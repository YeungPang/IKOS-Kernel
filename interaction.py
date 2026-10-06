import argparse
import ast
import contextlib
import hashlib
import io
import json
import logging
import os
import re
import tempfile
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json
from action_tool import ActionTool
from audit_compliance_manager import AuditComplianceManager
from dashboard_api import DashboardAPI
from event_bus import EventBus, handle_resource_conflict, handle_sla_breach, handle_task_overdue
from notifier import create_notifier_from_config
from notifier_engine import NotifierEngine
from project_simulator import ProjectSimulator, ScenarioModification
from scheduler import TaskScheduler
from workflow_monitor import WorkflowMonitor
from workflow_reliability_manager import WorkflowReliabilityManager
from workflow_state_machine import WorkflowStateMachine
from workflow_transition_executor import WorkflowTransitionExecutor
from runtime_logging import configure_logging
from llm_fallback import generate_content_with_openrouter_fallback, get_openrouter_client
import business_rules
import domain_db
import mutation_undo
import object_db
import nl_process_activation
import action_workflow_resolution
from source_database_integration import (
    discover_source_tables,
    load_source_mapping,
    register_source_database,
    sync_mapped_rows,
    upsert_source_table_inventory,
)

try:
    import spacy
except Exception:  # pragma: no cover - optional dependency
    spacy = None

adk = None


from ikos_config import (
    PROJECT_ID, LOCATION, CHAT_MODEL, EXTRACT_MODEL, FLASH_CONFIDENCE_THRESHOLD,
    OPENROUTER_SIMPLE_MODEL, OPENROUTER_COMPLEX_MODEL
)


@dataclass
class InteractionState:
    history: list[dict[str, str]] = field(default_factory=list)


class QueryResultCache:
    """Simple in-memory TTL-based cache for query results (1-hour default)."""
    
    def __init__(self, ttl_seconds: int = 3600):
        self.ttl_seconds = ttl_seconds
        self.cache: dict[str, tuple[dict[str, Any], float]] = {}
    
    def _hash_key(self, question: str) -> str:
        """Normalize question and return cache key."""
        normalized = " ".join(str(question or "").strip().lower().split())
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]
    
    def get(self, question: str) -> dict[str, Any] | None:
        """Retrieve cached result if fresh; return None otherwise."""
        key = self._hash_key(question)
        if key not in self.cache:
            return None
        result, timestamp = self.cache[key]
        if time.time() - timestamp > self.ttl_seconds:
            del self.cache[key]
            return None
        return result
    
    def put(self, question: str, result: dict[str, Any]) -> None:
        """Store query result with timestamp."""
        key = self._hash_key(question)
        self.cache[key] = (result, time.time())
    
    def clear(self) -> None:
        """Clear all cached results."""
        self.cache.clear()


def _should_skip_query_cache(question: str) -> bool:
    text = str(question or "").strip().lower()
    if not text:
        return True
    if any(token in text for token in ("customer", "customers", "client", "clients")):
        if any(token in text for token in (" from ", " in ", "country", "countries", "excluding", "except", "without")):
            return True
    return False


class IDMSInteractionTools:
    """Generic interaction tools that can be used directly or attached to ADK."""

    def __init__(self, enable_query_cache: bool = True) -> None:
        configure_logging()
        self.logger = logging.getLogger("idms.interaction")
        try:
            from query_engine import QueryEngine as _QueryEngine
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("query_engine dependencies are not available") from exc

        try:
            from sql_db import get_connection as _get_connection
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("sql_db dependencies are not available") from exc

        self.client = get_openrouter_client()
        if self.client is None:
            raise ValueError("OPENROUTER_API_KEY is required in environment")
        self.query_engine = _QueryEngine()
        self.get_connection = _get_connection
        self.state = InteractionState()
        self._command_nlp = None
        self._command_nlp_initialized = False
        self.model_usage_stats: dict[str, Any] = {
            "total_calls": 0,
            "flash_calls": 0,
            "chat_calls": 0,
            "escalations": 0,
            "by_call_name": {},
        }
        self._solf_init_lock = threading.Lock()
        self._solf_load_attempted = False
        self._solf_last_attempt_ts = 0.0
        self._solf_last_error: str | None = None
        self._solf_load_in_progress = False
        self._solf_load_done_event = threading.Event()
        self._solf_loader_thread: threading.Thread | None = None
        self._solf_runtime_rules_loaded = False
        self._solf_runtime_rules_in_progress = False
        self._solf_runtime_lock = threading.Lock()
        self._solf_retry_cooldown_seconds = max(
            0,
            int(str(os.getenv("IDMS_SOLF_RETRY_COOLDOWN_SEC", "30") or "30").strip() or "30"),
        )
        self._solf_load_timeout_seconds = max(
            1,
            int(str(os.getenv("IDMS_SOLF_LOAD_TIMEOUT_SEC", "8") or "8").strip() or "8"),
        )
        self._last_nl_request_text = ""
        try:
            threshold_raw = str(os.getenv("IDMS_SEMANTIC_BEST_EFFORT_THRESHOLD", "0.88") or "0.88").strip()
            threshold_val = float(threshold_raw)
        except Exception:
            threshold_val = 0.88
        self._semantic_best_effort_threshold = min(0.99, max(0.50, threshold_val))
        eager_solf_init = str(os.getenv("IDMS_EAGER_SOLF_POLICY_INIT", "false")).strip().lower() in {
            "1", "true", "yes", "on"
        }
        self.solf_interpreter = None
        self.query_cache = QueryResultCache() if enable_query_cache else None
        self.event_bus = EventBus(solf_interpreter=self.solf_interpreter, db_connection_fn=self.get_connection)
        self.notifier = create_notifier_from_config(db_connection_fn=self.get_connection)
        self.action_tool = ActionTool(
            db_connection_fn=self.get_connection,
            solf_interpreter=self.solf_interpreter,
            require_confirmation=False,
        )
        self.scheduler = TaskScheduler(
            autonomous_query_fn=self.autonomous_query,
            db_connection_fn=self.get_connection,
            event_bus=self.event_bus,
        )
        self.state_machine = WorkflowStateMachine(db_connection_fn=self.get_connection)
        self.transition_executor = WorkflowTransitionExecutor(
            state_machine=self.state_machine,
            event_bus=self.event_bus,
            solf_interpreter=self.solf_interpreter,
        )
        self.workflow_monitor = WorkflowMonitor(db_connection_fn=self.get_connection)
        self.workflow_reliability = WorkflowReliabilityManager(
            db_connection_fn=self.get_connection,
            monitor=self.workflow_monitor,
            event_bus=self.event_bus,
        )
        self.notifier_engine = NotifierEngine(db_connection_fn=self.get_connection)
        self.dashboard_api = DashboardAPI(
            db_connection_fn=self.get_connection,
            monitor=self.workflow_monitor,
            reliability_manager=self.workflow_reliability,
        )
        self.audit_compliance = AuditComplianceManager(
            db_connection_fn=self.get_connection,
            reliability_manager=self.workflow_reliability,
        )
        self.project_simulator = ProjectSimulator(db_connection_fn=self.get_connection)
        if eager_solf_init:
            self._ensure_solf_interpreter_loaded(force_retry=True)
        self._ensure_interaction_runtime_tables()
        self._wire_default_event_handlers()

    def _ensure_interaction_runtime_tables(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS workflow_action_log (
            id BIGSERIAL PRIMARY KEY,
            action_name TEXT NOT NULL,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            success BOOLEAN NOT NULL,
            message TEXT,
            result_data JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_workflow_action_log_name ON workflow_action_log(action_name);
        CREATE INDEX IF NOT EXISTS idx_workflow_action_log_created ON workflow_action_log(created_at);

        CREATE TABLE IF NOT EXISTS interaction_clarification_thread (
            thread_id BIGSERIAL PRIMARY KEY,
            session_id VARCHAR(128),
            channel VARCHAR(32) NOT NULL DEFAULT 'chat',
            user_ref VARCHAR(128),
            original_query TEXT NOT NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'pending_clarification'
                CHECK (status IN ('pending_clarification','resolved','abandoned')),
            reason_code VARCHAR(64) NOT NULL DEFAULT 'ambiguous_value',
            clarification_question TEXT NOT NULL,
            expected_input_type VARCHAR(64) NOT NULL DEFAULT 'free_text',
            ambiguity_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
            candidate_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
            provenance_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            resolved_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_clar_thread_status ON interaction_clarification_thread(status);
        CREATE INDEX IF NOT EXISTS idx_clar_thread_created_at ON interaction_clarification_thread(created_at DESC);

        CREATE TABLE IF NOT EXISTS interaction_clarification_turn (
            turn_id BIGSERIAL PRIMARY KEY,
            thread_id BIGINT NOT NULL REFERENCES interaction_clarification_thread(thread_id) ON DELETE CASCADE,
            turn_index INTEGER NOT NULL,
            role VARCHAR(16) NOT NULL CHECK (role IN ('user','assistant','system')),
            message_text TEXT NOT NULL,
            source_type VARCHAR(64),
            answer_source VARCHAR(64),
            confidence NUMERIC(5,4),
            ambiguity_flag BOOLEAN NOT NULL DEFAULT FALSE,
            ambiguity_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            candidate_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
            provenance_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
            linked_doc_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
            linked_object_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (thread_id, turn_index)
        );
        CREATE INDEX IF NOT EXISTS idx_clar_turn_thread ON interaction_clarification_turn(thread_id, turn_index);
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            object_db.ensure_tenant_isolation_for_tables(
                conn,
                ("workflow_action_log", "interaction_clarification_thread", "interaction_clarification_turn"),
            )

    @staticmethod
    def _contains_singular_intent(question: str) -> bool:
        text = str(question or "").strip().lower()
        if not text:
            return False
        singular_markers = (
            " current ", " last ", " latest ", " exact ", " full ",
            " aktuell ", " letzte ", " letzter ", " genau ", " vollstaendig ", " vollständig ",
        )
        wrapped = f" {text} "
        return any(marker in wrapped for marker in singular_markers)

    @staticmethod
    def _contains_plural_intent(question: str) -> bool:
        text = str(question or "").strip().lower()
        if not text:
            return False
        plural_markers = (
            " all ", " each ", " every ", " list ", " enumerate ", " options ", " candidates ",
            " alle ", " jeweils ", " liste ", " auflisten ", " mehrere ",
        )
        wrapped = f" {text} "
        return any(marker in wrapped for marker in plural_markers)

    @staticmethod
    def _attribute_expects_plural(attribute_name: str | None) -> bool:
        normalized = str(attribute_name or "").strip().lower()
        return normalized in {
            "shareholders",
            "documents",
            "document_list",
            "related_entities",
            "parties",
            "tags",
        }

    @staticmethod
    def _extract_customer_country_exclusion(question: str) -> str | None:
        text = str(question or "").strip()
        lowered = text.lower()
        if not text:
            return None
        if "customer" not in lowered and "customers" not in lowered:
            return None
        if "country" not in lowered and "countries" not in lowered:
            return None

        m = re.search(r"\b(?:except|excluding|exclude|without)\s+([A-Za-z][A-Za-z\-\s]{1,60})", text, flags=re.IGNORECASE)
        if not m:
            return None
        value = re.sub(r"[^A-Za-z\-\s]", " ", m.group(1)).strip()
        value = re.sub(r"\s+", " ", value)
        if not value:
            return None

        # Keep only the first token group to avoid trailing clauses (e.g., "Italy ordered by ...").
        parts = value.split(" ")
        if len(parts) > 3:
            value = " ".join(parts[:3]).strip()
        return value or None

    @staticmethod
    def _extract_customer_country_filter(question: str) -> dict[str, str] | None:
        text = str(question or "").strip()
        lowered = text.lower()
        if not text:
            return None
        if "customer" not in lowered and "customers" not in lowered and "client" not in lowered and "clients" not in lowered:
            return None

        exclusion = re.search(r"\b(?:except|excluding|exclude|without)\s+([A-Za-z][A-Za-z\-\s]{1,60})", text, flags=re.IGNORECASE)
        if exclusion:
            value = re.sub(r"[^A-Za-z\-\s]", " ", exclusion.group(1)).strip()
            value = re.sub(r"\s+", " ", value)
            if value:
                return {"mode": "exclude", "country": value.split(" ")[0:3] and " ".join(value.split(" ")[0:3]).strip()}

        include = re.search(r"\b(?:from|in)\s+([A-Za-z][A-Za-z\-\s]{1,60})", text, flags=re.IGNORECASE)
        if not include:
            return None
        value = re.sub(r"[^A-Za-z\-\s]", " ", include.group(1)).strip()
        value = re.sub(r"\s+", " ", value)
        if not value:
            return None

        # Trim trailing clauses such as "from Cyprus with contact persons".
        value = re.split(r"\b(?:with|and|or|where|whose|who|that|which|show|list|give|find|all)\b", value, maxsplit=1, flags=re.IGNORECASE)[0].strip()
        parts = value.split(" ")
        if len(parts) > 3:
            value = " ".join(parts[:3]).strip()
        return {"mode": "include", "country": value}

    @staticmethod
    def _extract_source_key_hint_from_question(question: str) -> str | None:
        text = str(question or "").strip()
        if not text:
            return None
        tail_match = re.search(r"\b(?:in|from)\s+([A-Za-z0-9_]+)\s*$", text, flags=re.IGNORECASE)
        if not tail_match:
            return None
        token = str(tail_match.group(1) or "").strip().lower()
        if not token:
            return None
        if token in {"italy", "germany", "switzerland", "china", "france", "spain", "austria"}:
            return None
        return token

    def _query_customers_excluding_country(self, excluded_country: str, limit: int = 200) -> dict[str, Any]:
        country = str(excluded_country or "").strip()
        if not country:
            return {"success": False, "count": 0, "rows": [], "excluded_country": ""}

        source_hint = self._extract_source_key_hint_from_question(self._last_nl_request_text)
        source = self._get_active_source_registration(source_key=source_hint) or self._get_active_source_registration(source_key=None)
        if not isinstance(source, dict):
            return {
                "success": False,
                "count": 0,
                "rows": [],
                "excluded_country": country,
                "message": "No active integrated source database registration found.",
            }

        source_id = int(source.get("source_id") or 0)
        source_key = str(source.get("source_key") or "").strip()
        if source_id <= 0:
            return {
                "success": False,
                "count": 0,
                "rows": [],
                "excluded_country": country,
                "message": "Active source registration is missing source_id.",
            }

        sql = """
        SELECT
            sem.target_object_id,
            COALESCE(
                NULLIF(TRIM(sem.target_metadata->'source_row'->>'customer_name'), ''),
                NULLIF(TRIM(sem.target_metadata->'source_row'->>'customer_code'), ''),
                NULLIF(TRIM(oi.object_name), ''),
                sem.source_pk_value
            ) AS customer_name,
            sem.target_class_name,
            TRIM(
                COALESCE(
                    sem.target_metadata->'source_row'->>'country',
                    sem.target_metadata->'source_row'->>'Country',
                    sem.target_metadata->'source_row'->>'country_name',
                    ''
                )
            ) AS country,
            sem.source_pk_value
        FROM source_entity_mapping sem
        LEFT JOIN object_instance oi
          ON oi.object_id = sem.target_object_id
        WHERE sem.source_id = %s
          AND LOWER(COALESCE(sem.table_name, '')) = 'customer'
          AND LOWER(COALESCE(sem.target_class_name, '')) IN ('company', 'organization', 'customer')
          AND TRIM(
                COALESCE(
                    sem.target_metadata->'source_row'->>'country',
                    sem.target_metadata->'source_row'->>'Country',
                    sem.target_metadata->'source_row'->>'country_name',
                    ''
                )
          ) <> ''
          AND LOWER(TRIM(
                COALESCE(
                    sem.target_metadata->'source_row'->>'country',
                    sem.target_metadata->'source_row'->>'Country',
                    sem.target_metadata->'source_row'->>'country_name',
                    ''
                )
          )) <> LOWER(%s)
        ORDER BY country ASC, oi.object_name ASC
        LIMIT %s
        """

        rows: list[dict[str, Any]] = []
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (source_id, country, max(1, int(limit))))
                    for rec in cur.fetchall():
                        rows.append(
                            {
                                "object_id": rec[0],
                                "customer_name": rec[1],
                                "class_name": rec[2],
                                "country": rec[3],
                                "source_pk_value": rec[4],
                            }
                        )
        except Exception as exc:
            self.logger.exception("Integrated customer-country query failed")
            return {
                "success": False,
                "count": 0,
                "rows": [],
                "excluded_country": country,
                "error": str(exc),
            }

        countries = sorted({str(item.get("country") or "").strip() for item in rows if str(item.get("country") or "").strip()})
        return {
            "success": True,
            "count": len(rows),
            "rows": rows,
            "excluded_country": country,
            "source_key": source_key,
            "source_id": source_id,
            "country_count": len(countries),
            "countries": countries,
        }

    def _query_customers_by_country(
        self,
        country_name: str,
        limit: int = 200,
        include_contact_persons: bool = False,
    ) -> dict[str, Any]:
        country = str(country_name or "").strip()
        if not country:
            return {"success": False, "count": 0, "rows": [], "country": ""}

        source_hint = self._extract_source_key_hint_from_question(self._last_nl_request_text)
        source = self._get_active_source_registration(source_key=source_hint) or self._get_active_source_registration(source_key=None)
        if not isinstance(source, dict):
            return {
                "success": False,
                "count": 0,
                "rows": [],
                "country": country,
                "message": "No active integrated source database registration found.",
            }

        source_id = int(source.get("source_id") or 0)
        source_key = str(source.get("source_key") or "").strip()
        if source_id <= 0:
            return {
                "success": False,
                "count": 0,
                "rows": [],
                "country": country,
                "message": "Active source registration is missing source_id.",
            }

        sql = """
        SELECT
            sem.target_object_id,
            COALESCE(
                NULLIF(TRIM(sem.target_metadata->'source_row'->>'customer_name'), ''),
                NULLIF(TRIM(sem.target_metadata->'source_row'->>'customer_code'), ''),
                NULLIF(TRIM(oi.object_name), ''),
                sem.source_pk_value
            ) AS customer_name,
            sem.target_class_name,
            TRIM(
                COALESCE(
                    sem.target_metadata->'source_row'->>'country',
                    sem.target_metadata->'source_row'->>'Country',
                    sem.target_metadata->'source_row'->>'country_name',
                    ''
                )
            ) AS country,
            sem.source_pk_value
        FROM source_entity_mapping sem
        LEFT JOIN object_instance oi
          ON oi.object_id = sem.target_object_id
        WHERE sem.source_id = %s
          AND LOWER(COALESCE(sem.table_name, '')) = 'customer'
          AND LOWER(COALESCE(sem.target_class_name, '')) IN ('company', 'organization', 'customer')
          AND TRIM(
                COALESCE(
                    sem.target_metadata->'source_row'->>'country',
                    sem.target_metadata->'source_row'->>'Country',
                    sem.target_metadata->'source_row'->>'country_name',
                    ''
                )
          ) <> ''
          AND LOWER(TRIM(
                COALESCE(
                    sem.target_metadata->'source_row'->>'country',
                    sem.target_metadata->'source_row'->>'Country',
                    sem.target_metadata->'source_row'->>'country_name',
                    ''
                )
          )) = LOWER(%s)
        ORDER BY country ASC, oi.object_name ASC
        LIMIT %s
        """

        rows: list[dict[str, Any]] = []
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (source_id, country, max(1, int(limit))))
                    for rec in cur.fetchall():
                        rows.append(
                            {
                                "object_id": rec[0],
                                "customer_name": rec[1],
                                "class_name": rec[2],
                                "country": rec[3],
                                "source_pk_value": rec[4],
                            }
                        )

                    if include_contact_persons and rows:
                        customer_ids = [int(item["object_id"]) for item in rows if item.get("object_id") is not None]
                        relationship_sql = """
                    SELECT
                        rel.tar_object_id,
                        rel.relationship_name,
                        contact.object_id,
                        contact.object_name,
                        contact.class_name,
                        contact.metadata
                    FROM object_relationship rel
                    JOIN object_instance contact
                      ON contact.object_id = rel.src_object_id
                    WHERE rel.tar_object_id = ANY(%s)
                      AND contact.status = 'active'
                      AND (
                            LOWER(rel.relationship_name) LIKE '%%contact%%'
                         OR LOWER(rel.relationship_name) LIKE '%%person%%'
                      )
                      AND LOWER(contact.class_name) IN ('person', 'contact_person', 'employee')
                    ORDER BY rel.tar_object_id, contact.object_name
                        """
                        cur.execute(relationship_sql, (customer_ids,))
                        contacts_by_customer: dict[int, list[dict[str, Any]]] = {}
                        for rec in cur.fetchall() or []:
                            metadata = rec[5] if isinstance(rec[5], dict) else {}
                            source_row = metadata.get("source_row") if isinstance(metadata.get("source_row"), dict) else {}
                            contact = {
                                "object_id": rec[2],
                                "name": rec[3],
                                "class_name": rec[4],
                                "relationship_name": rec[1],
                            }
                            for key in ("email_address", "email", "telephone_no", "phone", "mobile_no", "mobile", "role", "department"):
                                value = source_row.get(key)
                                if value not in (None, ""):
                                    contact[key] = value
                            contacts_by_customer.setdefault(int(rec[0]), []).append(contact)
                        for customer in rows:
                            customer["contact_persons"] = contacts_by_customer.get(int(customer["object_id"]), [])
        except Exception as exc:
            self.logger.exception("Integrated customer-country query failed")
            return {
                "success": False,
                "count": 0,
                "rows": [],
                "country": country,
                "error": str(exc),
            }

        countries = sorted({str(item.get("country") or "").strip() for item in rows if str(item.get("country") or "").strip()})
        return {
            "success": True,
            "count": len(rows),
            "rows": rows,
            "country": country,
            "source_key": source_key,
            "source_id": source_id,
            "country_count": len(countries),
            "countries": countries,
        }

    @staticmethod
    def _normalize_candidate_snapshot(candidates: list[Any], limit: int = 8) -> list[Any]:
        out: list[Any] = []
        seen: set[str] = set()
        for item in list(candidates or []):
            normalized: Any
            if isinstance(item, dict):
                normalized = {
                    "name": item.get("name") or item.get("entity_name") or item.get("value"),
                    "value": item.get("attribute_value") if "attribute_value" in item else item.get("value"),
                    "score": item.get("score"),
                    "object_id": item.get("object_id"),
                    "class_name": item.get("class_name"),
                    "similarity": item.get("similarity"),
                    "doc_id": item.get("doc_id"),
                    "doc_name": item.get("doc_name"),
                    "doc_path": item.get("doc_path"),
                    "relationship_name": item.get("relationship_name"),
                }
                normalized = {k: v for k, v in normalized.items() if v not in (None, "")}
            else:
                text = str(item or "").strip()
                if not text:
                    continue
                normalized = text

            dedupe_key = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            out.append(normalized)
            if len(out) >= max(1, int(limit)):
                break
        return out

    def _extract_provenance_snapshot(self, query_result: dict[str, Any] | None, limit: int = 8) -> list[dict[str, Any]]:
        if not isinstance(query_result, dict):
            return []

        grounding = query_result.get("grounding") if isinstance(query_result.get("grounding"), dict) else {}
        data = query_result.get("data") if isinstance(query_result.get("data"), dict) else {}
        attribute_result = grounding.get("attribute_result") if isinstance(grounding.get("attribute_result"), dict) else {}
        direct = grounding.get("direct_query_result") if isinstance(grounding.get("direct_query_result"), dict) else {}
        direct_data = direct.get("data") if isinstance(direct.get("data"), dict) else {}

        out: list[dict[str, Any]] = []
        seen: set[str] = set()

        def _append_from(payload: dict[str, Any], source_hint: str) -> None:
            if not isinstance(payload, dict):
                return
            entry = {
                "source": source_hint,
                "doc_id": payload.get("doc_id"),
                "doc_name": payload.get("doc_name"),
                "doc_path": payload.get("doc_path"),
                "doc_date": payload.get("doc_date"),
                "entity_name": payload.get("entity_name") or payload.get("source_entity_name"),
                "related_entity_name": payload.get("related_entity_name"),
                "attribute_name": payload.get("attribute_name"),
                "relationship_name": payload.get("relationship_name"),
            }
            entry = {k: v for k, v in entry.items() if v not in (None, "")}
            if len(entry) <= 1:
                return
            key = json.dumps(entry, sort_keys=True, ensure_ascii=False)
            if key in seen:
                return
            seen.add(key)
            out.append(entry)

        _append_from(attribute_result, "grounding.attribute_result")
        _append_from(direct_data, "grounding.direct_query_result")
        _append_from(data, "query_result.data")

        for item in list(grounding.get("matches") or []):
            if not isinstance(item, dict):
                continue
            _append_from(item, "grounding.matches")
            if len(out) >= max(1, int(limit)):
                break

        return out[: max(1, int(limit))]

    @staticmethod
    def _normalize_entity_token(value: Any) -> str:
        text = str(value or "").strip().lower()
        if not text:
            return ""
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = re.sub(r"\([^\)]*\)", " ", text)
        text = re.sub(r"[^a-z0-9äöüß]+", " ", text)
        return " ".join(text.split())

    def _candidate_labels_from_snapshot(self, candidate_snapshot: list[Any]) -> list[str]:
        labels: list[str] = []
        for item in list(candidate_snapshot or []):
            if isinstance(item, dict):
                label = str(item.get("name") or item.get("value") or item.get("entity_name") or "").strip()
            else:
                label = str(item or "").strip()
            if label:
                labels.append(label)
        return labels

    def _match_user_response_to_candidate(self, candidate_snapshot: list[Any], user_response: str) -> str | None:
        response_raw = str(user_response or "").strip()
        if not response_raw:
            return None

        response_norm = self._normalize_entity_token(response_raw)
        if not response_norm:
            return None

        labels = self._candidate_labels_from_snapshot(candidate_snapshot)
        if not labels:
            return None

        for label in labels:
            if self._normalize_entity_token(label) == response_norm:
                return label

        for label in labels:
            label_norm = self._normalize_entity_token(label)
            if not label_norm:
                continue
            if response_norm in label_norm or label_norm in response_norm:
                return label

        return None

    @staticmethod
    def _find_candidate_snapshot_entry(candidate_snapshot: list[Any], candidate_label: str) -> dict[str, Any] | None:
        label = str(candidate_label or "").strip()
        if not label:
            return None
        for item in list(candidate_snapshot or []):
            if not isinstance(item, dict):
                continue
            item_label = str(item.get("name") or item.get("value") or item.get("entity_name") or "").strip()
            if item_label == label:
                return item
        return None

    def _entity_matches_anchor(self, candidate: Any, anchor_entity: str) -> bool:
        anchor_norm = self._normalize_entity_token(anchor_entity)
        if not anchor_norm:
            return True

        candidate_norm = self._normalize_entity_token(candidate)
        if not candidate_norm:
            return True

        if candidate_norm == anchor_norm:
            return True

        anchor_parts = anchor_norm.split()
        candidate_parts = candidate_norm.split()
        if len(anchor_parts) >= 2 and len(candidate_parts) >= 2:
            if candidate_parts[: len(anchor_parts)] == anchor_parts:
                return True
            if candidate_parts[-len(anchor_parts):] == anchor_parts:
                return True
        return False

    def _sanitize_grounding_for_anchor(self, grounding: dict[str, Any], anchor_entity: str) -> dict[str, Any]:
        if not isinstance(grounding, dict) or not anchor_entity:
            return grounding if isinstance(grounding, dict) else {}

        filtered = dict(grounding)

        def _filter_item(item: Any) -> Any:
            if not isinstance(item, dict):
                return item

            entity_fields = [
                item.get("entity_name"),
                item.get("source_entity_name"),
                item.get("related_entity_name"),
                item.get("anchor_name"),
                item.get("company_name"),
                item.get("object_name"),
                item.get("target_name"),
                item.get("source_name"),
            ]
            candidate_entities = [value for value in entity_fields if str(value or "").strip()]
            if candidate_entities and not any(self._entity_matches_anchor(value, anchor_entity) for value in candidate_entities):
                return None

            nested_matches = item.get("matches")
            if isinstance(nested_matches, list):
                nested_filtered = [_filter_item(entry) for entry in nested_matches]
                item["matches"] = [entry for entry in nested_filtered if entry is not None]

            nested_results = item.get("results")
            if isinstance(nested_results, list):
                nested_filtered = [_filter_item(entry) for entry in nested_results]
                item["results"] = [entry for entry in nested_filtered if entry is not None]

            return item

        for key in ("attribute_result", "direct_query_result"):
            value = filtered.get(key)
            if isinstance(value, dict):
                sanitized = _filter_item(dict(value))
                if sanitized is None:
                    filtered[key] = None
                else:
                    filtered[key] = sanitized

        for key in ("criteria_result", "discovery_search", "scope"):
            value = filtered.get(key)
            if not isinstance(value, dict):
                continue
            sanitized_block = dict(value)
            for list_key in ("matches", "results", "top_candidates", "nodes", "objects", "relationships", "documents", "candidates", "discovery_results"):
                entries = sanitized_block.get(list_key)
                if not isinstance(entries, list):
                    continue
                new_entries = []
                for entry in entries:
                    sanitized = _filter_item(entry)
                    if sanitized is not None:
                        new_entries.append(sanitized)
                sanitized_block[list_key] = new_entries
            filtered[key] = sanitized_block

        return filtered

    @staticmethod
    def _build_generic_clarification_question(reason_code: str, candidates: list[Any]) -> str:
        count = len(candidates or [])
        if reason_code == "ambiguous_entity":
            return (
                "I found multiple possible entities for your request. "
                "Please specify which exact entity you mean."
            )
        if reason_code == "temporal_conflict":
            return (
                "I found multiple time-valid candidates. "
                "Please specify the timeframe or preferred source document/date."
            )
        if count > 1:
            return (
                "I found multiple possible answers. "
                "Please specify which candidate or scope should be used."
            )
        return "I need one more detail to answer precisely. Please clarify your intended scope."

    def _build_merge_clarification_question(self, slot_label: str, entity_text: str, reason_code: str) -> str:
        which = str(slot_label or "entity").replace("_", " ").strip()
        entity_hint = str(entity_text or "").strip()
        if reason_code == "ambiguous_entity":
            return (
                f"I found multiple possible entities for {which} '{entity_hint}' in your merge request. "
                "Please choose the exact entity candidate to use."
            )
        return (
            f"I could not resolve {which} '{entity_hint}' in your merge request. "
            "Please provide the exact entity name or object id."
        )

    def _build_document_clause_clarification_question(self, document_hint: str, reason_code: str) -> str:
        hint = str(document_hint or "").strip()
        if reason_code == "ambiguous_document":
            return (
                f"I found multiple documents matching '{hint}' for retrieve_document_file. "
                "Please choose the exact document candidate to use."
            )
        return (
            f"I could not resolve document '{hint}' for retrieve_document_file. "
            "Please provide the exact doc id, doc key, or document name."
        )

    def _build_update_clause_clarification_question(self, entity_hint: str, reason_code: str) -> str:
        hint = str(entity_hint or "").strip()
        if reason_code == "ambiguous_entity":
            return (
                f"I found multiple entities matching '{hint}' for update. "
                "Please choose the exact entity candidate to update."
            )
        return (
            f"I could not resolve entity '{hint}' for update. "
            "Please provide the exact entity name or object id."
        )

    def _resolve_merge_entity_reference(self, entity_value: Any) -> dict[str, Any]:
        raw = str(entity_value or "").strip()
        if not raw:
            return {"status": "not_found", "input": raw, "candidates": []}

        with self.get_connection() as conn:
            if re.fullmatch(r"\d+", raw):
                row = object_db.get_object_instance_by_id(conn, int(raw), include_inactive=False)
                if row:
                    return {
                        "status": "resolved",
                        "input": raw,
                        "object_id": int(row.get("object_id") or 0),
                        "object_name": str(row.get("object_name") or "").strip(),
                        "class_name": str(row.get("class_name") or "").strip(),
                    }
                return {"status": "not_found", "input": raw, "candidates": []}

            exact = object_db.get_object_instance(conn, raw, class_name=None, include_inactive=False)
            if exact:
                return {
                    "status": "resolved",
                    "input": raw,
                    "object_id": int(exact.get("object_id") or 0),
                    "object_name": str(exact.get("object_name") or "").strip(),
                    "class_name": str(exact.get("class_name") or "").strip(),
                }

            candidates = object_db.search_objects(
                connection=conn,
                name_query=raw,
                class_name=None,
                limit=8,
                include_inactive=False,
            )

        hint_tokens = set(self._normalize_entity_token(raw).split())
        filtered: list[dict[str, Any]] = []
        for candidate in list(candidates or []):
            candidate_name = str(candidate.get("object_name") or candidate.get("canonical_full_name") or "").strip()
            if not candidate_name:
                continue
            candidate_tokens = set(self._normalize_entity_token(candidate_name).split())
            overlap_count = len(hint_tokens.intersection(candidate_tokens))
            overlap_ratio = (overlap_count / max(1, len(hint_tokens))) if hint_tokens else 0.0
            similarity = float(candidate.get("similarity") or 0.0)
            if overlap_ratio <= 0.0 and similarity < 0.30:
                continue
            filtered.append(
                {
                    "object_id": int(candidate.get("object_id") or 0),
                    "name": candidate_name,
                    "class_name": candidate.get("class_name"),
                    "similarity": similarity,
                    "score": round((overlap_ratio * 100.0) + (similarity * 10.0), 4),
                }
            )

        filtered = self._normalize_candidate_snapshot(filtered, limit=8)
        if not filtered:
            return {"status": "not_found", "input": raw, "candidates": []}
        if len(filtered) == 1:
            item = filtered[0] if isinstance(filtered[0], dict) else {}
            if item.get("object_id"):
                return {
                    "status": "resolved",
                    "input": raw,
                    "object_id": int(item.get("object_id") or 0),
                    "object_name": str(item.get("name") or "").strip(),
                    "class_name": str(item.get("class_name") or "").strip(),
                }

        return {
            "status": "ambiguous",
            "input": raw,
            "candidates": filtered,
        }

    def _extract_entity_update_payload(self, raw_arg: Any, base_payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
        payload = dict(base_payload or {}) if isinstance(base_payload, dict) else {}
        if payload.get("object_name") and isinstance(payload.get("attributes"), dict) and payload.get("attributes"):
            return payload

        text = str(raw_arg or "").strip()
        if not text:
            return None

        match = re.match(
            r"^(?:entity\s+)?(.+?)\s+(?:set\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*(?:to|=)\s*(.+?)$",
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None

        object_name = str(match.group(1) or "").strip(" ,.;")
        field = str(match.group(2) or "").strip()
        new_value_text = str(match.group(3) or "").strip(" ,.;")
        if not object_name or not field or not new_value_text:
            return None

        lower_value = new_value_text.lower()
        if lower_value in {"true", "false"}:
            new_value: Any = lower_value == "true"
        elif re.fullmatch(r"-?\d+", new_value_text):
            new_value = int(new_value_text)
        elif re.fullmatch(r"-?\d+\.\d+", new_value_text):
            new_value = float(new_value_text)
        else:
            new_value = new_value_text.strip("\"'")

        out = dict(payload)
        out.setdefault("class_name", "entity")
        out["object_name"] = object_name
        attributes = out.get("attributes") if isinstance(out.get("attributes"), dict) else {}
        attributes = dict(attributes)
        attributes[field] = new_value
        out["attributes"] = attributes
        return out

    def _resolve_document_reference(self, document_value: Any) -> dict[str, Any]:
        raw = str(document_value or "").strip()
        if not raw:
            return {"status": "not_found", "input": raw, "candidates": []}

        with self.get_connection() as conn:
            if re.fullmatch(r"\d+", raw):
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT doc_id, doc_key, doc_name, doc_path
                        FROM document
                        WHERE doc_id = %s
                          AND COALESCE(status, 'active') = 'active'
                        LIMIT 1
                        """,
                        (int(raw),),
                    )
                    row = cur.fetchone()
                if row:
                    return {
                        "status": "resolved",
                        "input": raw,
                        "doc_id": int(row[0]),
                        "doc_key": str(row[1] or "").strip(),
                        "doc_name": str(row[2] or "").strip(),
                        "doc_path": str(row[3] or "").strip(),
                    }
                return {"status": "not_found", "input": raw, "candidates": []}

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT doc_id, doc_key, doc_name, doc_path
                    FROM document
                    WHERE COALESCE(status, 'active') = 'active'
                      AND (
                        LOWER(COALESCE(doc_key, '')) = LOWER(%s)
                        OR LOWER(COALESCE(doc_name, '')) = LOWER(%s)
                      )
                    ORDER BY doc_date DESC NULLS LAST, doc_id DESC
                    LIMIT 8
                    """,
                    (raw, raw),
                )
                exact_rows = cur.fetchall() or []

            if len(exact_rows) == 1:
                row = exact_rows[0]
                return {
                    "status": "resolved",
                    "input": raw,
                    "doc_id": int(row[0]),
                    "doc_key": str(row[1] or "").strip(),
                    "doc_name": str(row[2] or "").strip(),
                    "doc_path": str(row[3] or "").strip(),
                }

            exact_candidates = self._normalize_candidate_snapshot(
                [
                    {
                        "doc_id": int(row[0]),
                        "doc_key": str(row[1] or "").strip(),
                        "doc_name": str(row[2] or "").strip(),
                        "doc_path": str(row[3] or "").strip(),
                        "name": str(row[2] or row[1] or f"doc_{int(row[0])}").strip(),
                    }
                    for row in exact_rows
                ],
                limit=8,
            )
            if len(exact_candidates) > 1:
                return {
                    "status": "ambiguous",
                    "input": raw,
                    "candidates": exact_candidates,
                }

            fuzzy = object_db.search_documents_by_key(
                connection=conn,
                key_query=raw,
                limit=8,
                include_inactive=False,
            )

        candidates = self._normalize_candidate_snapshot(
            [
                {
                    "doc_id": int(item.get("doc_id") or 0),
                    "doc_key": str(item.get("doc_key") or "").strip(),
                    "doc_name": str(item.get("doc_name") or item.get("doc_key") or "").strip(),
                    "doc_path": str(item.get("doc_path") or "").strip(),
                    "name": str(item.get("doc_name") or item.get("doc_key") or f"doc_{int(item.get('doc_id') or 0)}").strip(),
                    "score": item.get("score"),
                }
                for item in list(fuzzy or [])
                if int(item.get("doc_id") or 0) > 0
            ],
            limit=8,
        )
        if not candidates:
            return {"status": "not_found", "input": raw, "candidates": []}
        if len(candidates) == 1:
            item = candidates[0] if isinstance(candidates[0], dict) else {}
            return {
                "status": "resolved",
                "input": raw,
                "doc_id": int(item.get("doc_id") or 0),
                "doc_key": str(item.get("doc_key") or "").strip(),
                "doc_name": str(item.get("doc_name") or item.get("name") or "").strip(),
                "doc_path": str(item.get("doc_path") or "").strip(),
            }

        return {
            "status": "ambiguous",
            "input": raw,
            "candidates": candidates,
        }

    def _resolve_task_reference(self, task_value: Any) -> dict[str, Any]:
        raw = str(task_value or "").strip()
        if not raw:
            return {"status": "not_found", "input": raw, "candidates": []}

        with self.get_connection() as conn:
            if re.fullmatch(r"\d+", raw):
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, name, project_id, status
                        FROM project_tasks
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (int(raw),),
                    )
                    row = cur.fetchone()
                if row:
                    return {
                        "status": "resolved",
                        "input": raw,
                        "task_id": int(row[0]),
                        "task_name": str(row[1] or "").strip(),
                        "project_id": row[2],
                        "task_status": str(row[3] or "").strip(),
                    }
                return {"status": "not_found", "input": raw, "candidates": []}

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, project_id, status
                    FROM project_tasks
                    WHERE LOWER(COALESCE(name, '')) = LOWER(%s)
                    ORDER BY id DESC
                    LIMIT 8
                    """,
                    (raw,),
                )
                exact_rows = cur.fetchall() or []

            if len(exact_rows) == 1:
                row = exact_rows[0]
                return {
                    "status": "resolved",
                    "input": raw,
                    "task_id": int(row[0]),
                    "task_name": str(row[1] or "").strip(),
                    "project_id": row[2],
                    "task_status": str(row[3] or "").strip(),
                }

            candidates = self._normalize_candidate_snapshot(
                [
                    {
                        "task_id": int(row[0]),
                        "name": str(row[1] or f"task_{int(row[0])}").strip(),
                        "project_id": row[2],
                        "task_status": str(row[3] or "").strip(),
                    }
                    for row in exact_rows
                ],
                limit=8,
            )
            if len(candidates) > 1:
                return {
                    "status": "ambiguous",
                    "input": raw,
                    "candidates": candidates,
                }

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, project_id, status,
                           similarity(LOWER(COALESCE(name, '')), LOWER(%s)) AS sim
                    FROM project_tasks
                    WHERE LOWER(COALESCE(name, '')) LIKE '%%' || LOWER(%s) || '%%'
                       OR LOWER(COALESCE(name, '')) %% LOWER(%s)
                    ORDER BY sim DESC, id DESC
                    LIMIT 8
                    """,
                    (raw, raw, raw),
                )
                fuzzy_rows = cur.fetchall() or []

        fuzzy_candidates = self._normalize_candidate_snapshot(
            [
                {
                    "task_id": int(row[0]),
                    "name": str(row[1] or f"task_{int(row[0])}").strip(),
                    "project_id": row[2],
                    "task_status": str(row[3] or "").strip(),
                    "score": float(row[4] or 0.0),
                }
                for row in fuzzy_rows
            ],
            limit=8,
        )
        if not fuzzy_candidates:
            return {"status": "not_found", "input": raw, "candidates": []}
        if len(fuzzy_candidates) == 1:
            item = fuzzy_candidates[0] if isinstance(fuzzy_candidates[0], dict) else {}
            return {
                "status": "resolved",
                "input": raw,
                "task_id": int(item.get("task_id") or 0),
                "task_name": str(item.get("name") or "").strip(),
                "project_id": item.get("project_id"),
                "task_status": str(item.get("task_status") or "").strip(),
            }

        return {
            "status": "ambiguous",
            "input": raw,
            "candidates": fuzzy_candidates,
        }

    def _resolve_project_reference(self, project_value: Any) -> dict[str, Any]:
        raw = str(project_value or "").strip()
        if not raw:
            return {"status": "not_found", "input": raw, "candidates": []}

        with self.get_connection() as conn:
            if re.fullmatch(r"\d+", raw):
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, project_code, name, status
                        FROM projects
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (int(raw),),
                    )
                    row = cur.fetchone()
                if row:
                    return {
                        "status": "resolved",
                        "input": raw,
                        "project_id": int(row[0]),
                        "project_code": str(row[1] or "").strip(),
                        "name": str(row[2] or "").strip(),
                        "project_status": str(row[3] or "").strip(),
                    }
                return {"status": "not_found", "input": raw, "candidates": []}

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, project_code, name, status
                    FROM projects
                    WHERE LOWER(COALESCE(project_code, '')) = LOWER(%s)
                       OR LOWER(COALESCE(name, '')) = LOWER(%s)
                    ORDER BY id DESC
                    LIMIT 8
                    """,
                    (raw, raw),
                )
                exact_rows = cur.fetchall() or []

            if len(exact_rows) == 1:
                row = exact_rows[0]
                return {
                    "status": "resolved",
                    "input": raw,
                    "project_id": int(row[0]),
                    "project_code": str(row[1] or "").strip(),
                    "name": str(row[2] or "").strip(),
                    "project_status": str(row[3] or "").strip(),
                }

            candidates = self._normalize_candidate_snapshot(
                [
                    {
                        "project_id": int(row[0]),
                        "project_code": str(row[1] or "").strip(),
                        "name": str(row[2] or f"project_{int(row[0])}").strip(),
                        "project_status": str(row[3] or "").strip(),
                    }
                    for row in exact_rows
                ],
                limit=8,
            )
            if len(candidates) > 1:
                return {
                    "status": "ambiguous",
                    "input": raw,
                    "candidates": candidates,
                }

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, project_code, name, status,
                           GREATEST(
                               similarity(LOWER(COALESCE(project_code, '')), LOWER(%s)),
                               similarity(LOWER(COALESCE(name, '')), LOWER(%s))
                           ) AS sim
                    FROM projects
                    WHERE LOWER(COALESCE(project_code, '')) LIKE '%%' || LOWER(%s) || '%%'
                       OR LOWER(COALESCE(name, '')) LIKE '%%' || LOWER(%s) || '%%'
                       OR LOWER(COALESCE(project_code, '')) %% LOWER(%s)
                       OR LOWER(COALESCE(name, '')) %% LOWER(%s)
                    ORDER BY sim DESC, id DESC
                    LIMIT 8
                    """,
                    (raw, raw, raw, raw, raw, raw),
                )
                fuzzy_rows = cur.fetchall() or []

        fuzzy_candidates = self._normalize_candidate_snapshot(
            [
                {
                    "project_id": int(row[0]),
                    "project_code": str(row[1] or "").strip(),
                    "name": str(row[2] or f"project_{int(row[0])}").strip(),
                    "project_status": str(row[3] or "").strip(),
                    "score": float(row[4] or 0.0),
                }
                for row in fuzzy_rows
            ],
            limit=8,
        )
        if not fuzzy_candidates:
            return {"status": "not_found", "input": raw, "candidates": []}
        if len(fuzzy_candidates) == 1:
            item = fuzzy_candidates[0] if isinstance(fuzzy_candidates[0], dict) else {}
            return {
                "status": "resolved",
                "input": raw,
                "project_id": int(item.get("project_id") or 0),
                "project_code": str(item.get("project_code") or "").strip(),
                "name": str(item.get("name") or "").strip(),
                "project_status": str(item.get("project_status") or "").strip(),
            }

        return {
            "status": "ambiguous",
            "input": raw,
            "candidates": fuzzy_candidates,
        }

    def _resolve_workflow_case_reference(self, case_value: Any) -> dict[str, Any]:
        raw = str(case_value or "").strip()
        if not raw:
            return {"status": "not_found", "input": raw, "candidates": []}

        with self.get_connection() as conn:
            if re.fullmatch(r"\d+", raw):
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, case_no, case_type, status
                        FROM workflow_cases
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (int(raw),),
                    )
                    row = cur.fetchone()
                if row:
                    return {
                        "status": "resolved",
                        "input": raw,
                        "case_ref": int(row[0]),
                        "case_no": str(row[1] or "").strip(),
                        "case_type": str(row[2] or "").strip(),
                        "case_status": str(row[3] or "").strip(),
                    }
                return {"status": "not_found", "input": raw, "candidates": []}

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, case_no, case_type, status
                    FROM workflow_cases
                    WHERE LOWER(COALESCE(case_no, '')) = LOWER(%s)
                    ORDER BY id DESC
                    LIMIT 8
                    """,
                    (raw,),
                )
                exact_rows = cur.fetchall() or []

            if len(exact_rows) == 1:
                row = exact_rows[0]
                return {
                    "status": "resolved",
                    "input": raw,
                    "case_ref": int(row[0]),
                    "case_no": str(row[1] or "").strip(),
                    "case_type": str(row[2] or "").strip(),
                    "case_status": str(row[3] or "").strip(),
                }

            candidates = self._normalize_candidate_snapshot(
                [
                    {
                        "case_ref": int(row[0]),
                        "name": str(row[1] or f"case_{int(row[0])}").strip(),
                        "case_no": str(row[1] or "").strip(),
                        "case_type": str(row[2] or "").strip(),
                        "case_status": str(row[3] or "").strip(),
                    }
                    for row in exact_rows
                ],
                limit=8,
            )
            if len(candidates) > 1:
                return {"status": "ambiguous", "input": raw, "candidates": candidates}

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, case_no, case_type, status,
                           similarity(LOWER(COALESCE(case_no, '')), LOWER(%s)) AS sim
                    FROM workflow_cases
                    WHERE LOWER(COALESCE(case_no, '')) LIKE '%%' || LOWER(%s) || '%%'
                       OR LOWER(COALESCE(case_no, '')) %% LOWER(%s)
                    ORDER BY sim DESC, id DESC
                    LIMIT 8
                    """,
                    (raw, raw, raw),
                )
                fuzzy_rows = cur.fetchall() or []

        fuzzy_candidates = self._normalize_candidate_snapshot(
            [
                {
                    "case_ref": int(row[0]),
                    "name": str(row[1] or f"case_{int(row[0])}").strip(),
                    "case_no": str(row[1] or "").strip(),
                    "case_type": str(row[2] or "").strip(),
                    "case_status": str(row[3] or "").strip(),
                    "score": float(row[4] or 0.0),
                }
                for row in fuzzy_rows
            ],
            limit=8,
        )
        if not fuzzy_candidates:
            return {"status": "not_found", "input": raw, "candidates": []}
        if len(fuzzy_candidates) == 1:
            item = fuzzy_candidates[0] if isinstance(fuzzy_candidates[0], dict) else {}
            return {
                "status": "resolved",
                "input": raw,
                "case_ref": int(item.get("case_ref") or 0),
                "case_no": str(item.get("case_no") or "").strip(),
                "case_type": str(item.get("case_type") or "").strip(),
                "case_status": str(item.get("case_status") or "").strip(),
            }

        return {"status": "ambiguous", "input": raw, "candidates": fuzzy_candidates}

    def _create_merge_clause_clarification(
        self,
        *,
        original_query: str,
        clause_name: str,
        raw_args: list[Any],
        pending_slot: str,
        resolution: dict[str, Any],
        resolved_ids: dict[str, Any],
    ) -> dict[str, Any]:
        entity_text = str(resolution.get("input") or "").strip()
        reason_code = "ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found"
        clarification_question = self._build_merge_clarification_question(pending_slot, entity_text, reason_code)
        candidates = list(resolution.get("candidates") or [])
        thread_id = self._create_clarification_thread(
            original_query=original_query,
            reason_code=reason_code,
            clarification_question=clarification_question,
            expected_input_type="entity_scope",
            ambiguity_summary={
                "clause_name": clause_name,
                "pending_slot": pending_slot,
                "entity_input": entity_text,
                "resolution_status": str(resolution.get("status") or "").strip(),
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
            metadata={
                "trigger": "solf_clause_merge_entities",
                "solf_clause_name": clause_name,
                "merge_resolution": {
                    "raw_args": [str(arg or "").strip() for arg in list(raw_args or [])[:2]],
                    "resolved_ids": dict(resolved_ids or {}),
                    "pending_slot": pending_slot,
                },
            },
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="user",
            message_text=original_query,
            source_type="clause_dispatch_input",
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="assistant",
            message_text=clarification_question,
            source_type="clarification_prompt",
            answer_source="pending_clarification",
            confidence=None,
            ambiguity_flag=True,
            ambiguity_payload={
                "clause_name": clause_name,
                "pending_slot": pending_slot,
                "entity_input": entity_text,
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
        )
        return {
            "intent": "solf_clause",
            "source": "pending_clarification",
            "question": original_query,
            "answer": f"[source: pending_clarification] {clarification_question}",
            "success": False,
            "status": "pending_clarification",
            "thread_id": thread_id,
            "clarification_question": clarification_question,
            "reason_code": reason_code,
            "expected_input_type": "entity_scope",
            "candidates": candidates,
            "data": {
                "clause_name": clause_name,
                "thread_id": thread_id,
                "pending_slot": pending_slot,
                "resolution": resolution,
            },
            "clause_name": clause_name,
        }

    def _create_document_clause_clarification(
        self,
        *,
        original_query: str,
        clause_name: str,
        raw_arg: Any,
        resolution: dict[str, Any],
    ) -> dict[str, Any]:
        document_hint = str(resolution.get("input") or raw_arg or "").strip()
        reason_code = "ambiguous_document" if str(resolution.get("status") or "") == "ambiguous" else "document_not_found"
        clarification_question = self._build_document_clause_clarification_question(document_hint, reason_code)
        candidates = list(resolution.get("candidates") or [])
        thread_id = self._create_clarification_thread(
            original_query=original_query,
            reason_code=reason_code,
            clarification_question=clarification_question,
            expected_input_type="document_scope",
            ambiguity_summary={
                "clause_name": clause_name,
                "document_input": document_hint,
                "resolution_status": str(resolution.get("status") or "").strip(),
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
            metadata={
                "trigger": "solf_clause_retrieve_document_file",
                "solf_clause_name": clause_name,
                "document_resolution": {
                    "raw_arg": str(raw_arg or "").strip(),
                },
            },
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="user",
            message_text=original_query,
            source_type="clause_dispatch_input",
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="assistant",
            message_text=clarification_question,
            source_type="clarification_prompt",
            answer_source="pending_clarification",
            confidence=None,
            ambiguity_flag=True,
            ambiguity_payload={
                "clause_name": clause_name,
                "document_input": document_hint,
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
        )
        return {
            "intent": "solf_clause",
            "source": "pending_clarification",
            "question": original_query,
            "answer": f"[source: pending_clarification] {clarification_question}",
            "success": False,
            "status": "pending_clarification",
            "thread_id": thread_id,
            "clarification_question": clarification_question,
            "reason_code": reason_code,
            "expected_input_type": "document_scope",
            "candidates": candidates,
            "data": {
                "clause_name": clause_name,
                "thread_id": thread_id,
                "resolution": resolution,
            },
            "clause_name": clause_name,
        }

    def _create_update_clause_clarification(
        self,
        *,
        original_query: str,
        clause_name: str,
        update_payload: dict[str, Any],
        resolution: dict[str, Any],
    ) -> dict[str, Any]:
        entity_hint = str(resolution.get("input") or update_payload.get("object_name") or "").strip()
        reason_code = "ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found"
        clarification_question = self._build_update_clause_clarification_question(entity_hint, reason_code)
        candidates = list(resolution.get("candidates") or [])
        thread_id = self._create_clarification_thread(
            original_query=original_query,
            reason_code=reason_code,
            clarification_question=clarification_question,
            expected_input_type="entity_scope",
            ambiguity_summary={
                "clause_name": clause_name,
                "entity_input": entity_hint,
                "resolution_status": str(resolution.get("status") or "").strip(),
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
            metadata={
                "trigger": "solf_clause_entity_update",
                "solf_clause_name": clause_name,
                "update_resolution": {
                    "update_payload": dict(update_payload or {}),
                },
            },
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="user",
            message_text=original_query,
            source_type="clause_dispatch_input",
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="assistant",
            message_text=clarification_question,
            source_type="clarification_prompt",
            answer_source="pending_clarification",
            confidence=None,
            ambiguity_flag=True,
            ambiguity_payload={
                "clause_name": clause_name,
                "entity_input": entity_hint,
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
        )
        return {
            "intent": "solf_clause",
            "source": "pending_clarification",
            "question": original_query,
            "answer": f"[source: pending_clarification] {clarification_question}",
            "success": False,
            "status": "pending_clarification",
            "thread_id": thread_id,
            "clarification_question": clarification_question,
            "reason_code": reason_code,
            "expected_input_type": "entity_scope",
            "candidates": candidates,
            "data": {
                "clause_name": clause_name,
                "thread_id": thread_id,
                "resolution": resolution,
            },
            "clause_name": clause_name,
        }

    def _create_update_action_clarification(
        self,
        *,
        action_name: str,
        payload: dict[str, Any],
        resolution: dict[str, Any],
    ) -> dict[str, Any]:
        entity_hint = str(resolution.get("input") or payload.get("natural_key_value") or "").strip()
        reason_code = "ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found"
        clarification_question = self._build_update_clause_clarification_question(entity_hint, reason_code)
        candidates = list(resolution.get("candidates") or [])
        thread_id = self._create_clarification_thread(
            original_query=f"workflow action {action_name}: {json.dumps(payload, ensure_ascii=False)}",
            reason_code=reason_code,
            clarification_question=clarification_question,
            expected_input_type="entity_scope",
            ambiguity_summary={
                "action": action_name,
                "entity_input": entity_hint,
                "resolution_status": str(resolution.get("status") or "").strip(),
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
            metadata={
                "trigger": "workflow_action_update_entity",
                "action_name": action_name,
                "update_action_resolution": {
                    "payload": dict(payload or {}),
                },
            },
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="assistant",
            message_text=clarification_question,
            source_type="clarification_prompt",
            answer_source="pending_clarification",
            confidence=None,
            ambiguity_flag=True,
            ambiguity_payload={
                "action": action_name,
                "entity_input": entity_hint,
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
        )
        return {
            "action": action_name,
            "source": "pending_clarification",
            "success": False,
            "status": "pending_clarification",
            "thread_id": thread_id,
            "clarification_question": clarification_question,
            "reason_code": reason_code,
            "expected_input_type": "entity_scope",
            "candidates": candidates,
            "message": clarification_question,
            "data": {
                "thread_id": thread_id,
                "resolution": resolution,
            },
        }

    def _create_create_workflow_case_action_clarification(
        self,
        *,
        action_name: str,
        payload: dict[str, Any],
        resolution: dict[str, Any],
        pending_field: str,
    ) -> dict[str, Any]:
        if pending_field == "subject":
            entity_hint = str(
                resolution.get("input")
                or payload.get("subject_name")
                or payload.get("subject_title")
                or payload.get("subject_ref")
                or ""
            ).strip()
            reason_code = "ambiguous_subject" if str(resolution.get("status") or "") == "ambiguous" else "subject_not_found"
            clarification_question = (
                f"I found multiple subjects matching '{entity_hint}'. Please choose the exact subject candidate."
                if reason_code == "ambiguous_subject"
                else f"I could not resolve subject '{entity_hint}'. Please provide the exact subject id or name."
            )
        else:
            entity_hint = str(
                resolution.get("input")
                or payload.get("initiated_by_name")
                or payload.get("initiated_by_title")
                or payload.get("initiated_by_ref")
                or ""
            ).strip()
            reason_code = "ambiguous_initiator" if str(resolution.get("status") or "") == "ambiguous" else "initiator_not_found"
            clarification_question = (
                f"I found multiple initiators matching '{entity_hint}'. Please choose the exact initiator candidate."
                if reason_code == "ambiguous_initiator"
                else f"I could not resolve initiator '{entity_hint}'. Please provide the exact initiator id or name."
            )

        candidates = list(resolution.get("candidates") or [])
        thread_id = self._create_clarification_thread(
            original_query=f"workflow action {action_name}: {json.dumps(payload, ensure_ascii=False)}",
            reason_code=reason_code,
            clarification_question=clarification_question,
            expected_input_type="entity_scope",
            ambiguity_summary={
                "action": action_name,
                "pending_field": pending_field,
                "resolution_status": str(resolution.get("status") or "").strip(),
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
            metadata={
                "trigger": "workflow_action_create_workflow_case_reference",
                "action_name": action_name,
                "create_workflow_case_action_resolution": {
                    "payload": dict(payload or {}),
                    "pending_field": pending_field,
                },
            },
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="assistant",
            message_text=clarification_question,
            source_type="clarification_prompt",
            answer_source="pending_clarification",
            confidence=None,
            ambiguity_flag=True,
            ambiguity_payload={
                "action": action_name,
                "pending_field": pending_field,
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
        )
        return {
            "action": action_name,
            "source": "pending_clarification",
            "success": False,
            "status": "pending_clarification",
            "thread_id": thread_id,
            "clarification_question": clarification_question,
            "reason_code": reason_code,
            "expected_input_type": "entity_scope",
            "candidates": candidates,
            "message": clarification_question,
            "data": {
                "thread_id": thread_id,
                "resolution": resolution,
                "pending_field": pending_field,
            },
        }

    def _create_record_approval_action_clarification(
        self,
        *,
        action_name: str,
        payload: dict[str, Any],
        resolution: dict[str, Any],
        pending_field: str,
    ) -> dict[str, Any]:
        if pending_field == "case":
            hint = str(
                resolution.get("input")
                or payload.get("case_no")
                or payload.get("case_ref")
                or ""
            ).strip()
            reason_code = "ambiguous_case" if str(resolution.get("status") or "") == "ambiguous" else "case_not_found"
            clarification_question = (
                f"I found multiple workflow cases matching '{hint}'. Please choose the exact case candidate."
                if reason_code == "ambiguous_case"
                else f"I could not resolve workflow case '{hint}'. Please provide the exact case id or case no."
            )
        else:
            hint = str(
                resolution.get("input")
                or payload.get("approver_name")
                or payload.get("approver_title")
                or payload.get("approver_ref")
                or ""
            ).strip()
            reason_code = "ambiguous_approver" if str(resolution.get("status") or "") == "ambiguous" else "approver_not_found"
            clarification_question = (
                f"I found multiple approvers matching '{hint}'. Please choose the exact approver candidate."
                if reason_code == "ambiguous_approver"
                else f"I could not resolve approver '{hint}'. Please provide the exact approver id or name."
            )

        candidates = list(resolution.get("candidates") or [])
        thread_id = self._create_clarification_thread(
            original_query=f"workflow action {action_name}: {json.dumps(payload, ensure_ascii=False)}",
            reason_code=reason_code,
            clarification_question=clarification_question,
            expected_input_type="entity_scope",
            ambiguity_summary={
                "action": action_name,
                "pending_field": pending_field,
                "resolution_status": str(resolution.get("status") or "").strip(),
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
            metadata={
                "trigger": "workflow_action_record_approval_reference",
                "action_name": action_name,
                "record_approval_action_resolution": {
                    "payload": dict(payload or {}),
                    "pending_field": pending_field,
                },
            },
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="assistant",
            message_text=clarification_question,
            source_type="clarification_prompt",
            answer_source="pending_clarification",
            confidence=None,
            ambiguity_flag=True,
            ambiguity_payload={"action": action_name, "pending_field": pending_field},
            candidate_snapshot=candidates,
            provenance_snapshot=[],
        )
        return {
            "action": action_name,
            "source": "pending_clarification",
            "success": False,
            "status": "pending_clarification",
            "thread_id": thread_id,
            "clarification_question": clarification_question,
            "reason_code": reason_code,
            "expected_input_type": "entity_scope",
            "candidates": candidates,
            "message": clarification_question,
            "data": {
                "thread_id": thread_id,
                "resolution": resolution,
                "pending_field": pending_field,
            },
        }

    def _create_retrieve_action_clarification(
        self,
        *,
        action_name: str,
        payload: dict[str, Any],
        resolution: dict[str, Any],
    ) -> dict[str, Any]:
        document_hint = str(
            resolution.get("input")
            or payload.get("doc_name")
            or payload.get("doc_key")
            or payload.get("doc_id")
            or ""
        ).strip()
        reason_code = "ambiguous_document" if str(resolution.get("status") or "") == "ambiguous" else "document_not_found"
        clarification_question = self._build_document_clause_clarification_question(document_hint, reason_code)
        candidates = list(resolution.get("candidates") or [])
        thread_id = self._create_clarification_thread(
            original_query=f"workflow action {action_name}: {json.dumps(payload, ensure_ascii=False)}",
            reason_code=reason_code,
            clarification_question=clarification_question,
            expected_input_type="document_scope",
            ambiguity_summary={
                "action": action_name,
                "document_input": document_hint,
                "resolution_status": str(resolution.get("status") or "").strip(),
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
            metadata={
                "trigger": "workflow_action_retrieve_document_file",
                "action_name": action_name,
                "retrieve_action_resolution": {
                    "payload": dict(payload or {}),
                },
            },
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="assistant",
            message_text=clarification_question,
            source_type="clarification_prompt",
            answer_source="pending_clarification",
            confidence=None,
            ambiguity_flag=True,
            ambiguity_payload={
                "action": action_name,
                "document_input": document_hint,
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
        )
        return {
            "action": action_name,
            "source": "pending_clarification",
            "success": False,
            "status": "pending_clarification",
            "thread_id": thread_id,
            "clarification_question": clarification_question,
            "reason_code": reason_code,
            "expected_input_type": "document_scope",
            "candidates": candidates,
            "message": clarification_question,
            "data": {
                "thread_id": thread_id,
                "resolution": resolution,
            },
        }

    def _create_task_action_clarification(
        self,
        *,
        action_name: str,
        payload: dict[str, Any],
        resolution: dict[str, Any],
    ) -> dict[str, Any]:
        task_hint = str(
            resolution.get("input")
            or payload.get("task_name")
            or payload.get("task_title")
            or payload.get("task_id")
            or ""
        ).strip()
        reason_code = "ambiguous_task" if str(resolution.get("status") or "") == "ambiguous" else "task_not_found"
        clarification_question = (
            f"I found multiple tasks matching '{task_hint}'. Please choose the exact task candidate."
            if reason_code == "ambiguous_task"
            else f"I could not resolve task '{task_hint}'. Please provide the exact task id or task name."
        )
        candidates = list(resolution.get("candidates") or [])
        thread_id = self._create_clarification_thread(
            original_query=f"workflow action {action_name}: {json.dumps(payload, ensure_ascii=False)}",
            reason_code=reason_code,
            clarification_question=clarification_question,
            expected_input_type="task_scope",
            ambiguity_summary={
                "action": action_name,
                "task_input": task_hint,
                "resolution_status": str(resolution.get("status") or "").strip(),
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
            metadata={
                "trigger": "workflow_action_task_reference",
                "action_name": action_name,
                "task_action_resolution": {
                    "payload": dict(payload or {}),
                },
            },
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="assistant",
            message_text=clarification_question,
            source_type="clarification_prompt",
            answer_source="pending_clarification",
            confidence=None,
            ambiguity_flag=True,
            ambiguity_payload={
                "action": action_name,
                "task_input": task_hint,
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
        )
        return {
            "action": action_name,
            "source": "pending_clarification",
            "success": False,
            "status": "pending_clarification",
            "thread_id": thread_id,
            "clarification_question": clarification_question,
            "reason_code": reason_code,
            "expected_input_type": "task_scope",
            "candidates": candidates,
            "message": clarification_question,
            "data": {
                "thread_id": thread_id,
                "resolution": resolution,
            },
        }

    def _create_create_workflow_case_action_clarification(
        self,
        *,
        action_name: str,
        payload: dict[str, Any],
        resolution: dict[str, Any],
        pending_field: str,
    ) -> dict[str, Any]:
        if pending_field == "subject":
            entity_hint = str(
                resolution.get("input")
                or payload.get("subject_name")
                or payload.get("subject_title")
                or payload.get("subject_ref")
                or ""
            ).strip()
            reason_code = "ambiguous_subject" if str(resolution.get("status") or "") == "ambiguous" else "subject_not_found"
            clarification_question = (
                f"I found multiple subjects matching '{entity_hint}'. Please choose the exact subject candidate."
                if reason_code == "ambiguous_subject"
                else f"I could not resolve subject '{entity_hint}'. Please provide the exact subject id or name."
            )
        else:
            entity_hint = str(
                resolution.get("input")
                or payload.get("initiated_by_name")
                or payload.get("initiated_by_title")
                or payload.get("initiated_by_ref")
                or ""
            ).strip()
            reason_code = "ambiguous_initiator" if str(resolution.get("status") or "") == "ambiguous" else "initiator_not_found"
            clarification_question = (
                f"I found multiple initiators matching '{entity_hint}'. Please choose the exact initiator candidate."
                if reason_code == "ambiguous_initiator"
                else f"I could not resolve initiator '{entity_hint}'. Please provide the exact initiator id or name."
            )

        candidates = list(resolution.get("candidates") or [])
        thread_id = self._create_clarification_thread(
            original_query=f"workflow action {action_name}: {json.dumps(payload, ensure_ascii=False)}",
            reason_code=reason_code,
            clarification_question=clarification_question,
            expected_input_type="entity_scope",
            ambiguity_summary={
                "action": action_name,
                "pending_field": pending_field,
                "resolution_status": str(resolution.get("status") or "").strip(),
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
            metadata={
                "trigger": "workflow_action_create_workflow_case_reference",
                "action_name": action_name,
                "create_workflow_case_action_resolution": {
                    "payload": dict(payload or {}),
                    "pending_field": pending_field,
                },
            },
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="assistant",
            message_text=clarification_question,
            source_type="clarification_prompt",
            answer_source="pending_clarification",
            confidence=None,
            ambiguity_flag=True,
            ambiguity_payload={
                "action": action_name,
                "pending_field": pending_field,
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
        )
        return {
            "action": action_name,
            "source": "pending_clarification",
            "success": False,
            "status": "pending_clarification",
            "thread_id": thread_id,
            "clarification_question": clarification_question,
            "reason_code": reason_code,
            "expected_input_type": "entity_scope",
            "candidates": candidates,
            "message": clarification_question,
            "data": {
                "thread_id": thread_id,
                "resolution": resolution,
                "pending_field": pending_field,
            },
        }

    def _create_create_task_action_clarification(
        self,
        *,
        action_name: str,
        payload: dict[str, Any],
        resolution: dict[str, Any],
        pending_field: str,
    ) -> dict[str, Any]:
        if pending_field == "project":
            project_hint = str(
                resolution.get("input")
                or payload.get("project_name")
                or payload.get("project_code")
                or payload.get("project_id")
                or ""
            ).strip()
            reason_code = "ambiguous_project" if str(resolution.get("status") or "") == "ambiguous" else "project_not_found"
            clarification_question = (
                f"I found multiple projects matching '{project_hint}'. Please choose the exact project candidate."
                if reason_code == "ambiguous_project"
                else f"I could not resolve project '{project_hint}'. Please provide the exact project id, code, or name."
            )
            expected_input_type = "project_scope"
        else:
            assignee_hint = str(
                resolution.get("input")
                or payload.get("assigned_to")
                or payload.get("assignee_name")
                or ""
            ).strip()
            reason_code = "ambiguous_assignee" if str(resolution.get("status") or "") == "ambiguous" else "assignee_not_found"
            clarification_question = (
                f"I found multiple assignees matching '{assignee_hint}'. Please choose the exact assignee candidate."
                if reason_code == "ambiguous_assignee"
                else f"I could not resolve assignee '{assignee_hint}'. Please provide the exact assignee id or name."
            )
            expected_input_type = "entity_scope"

        candidates = list(resolution.get("candidates") or [])
        thread_id = self._create_clarification_thread(
            original_query=f"workflow action {action_name}: {json.dumps(payload, ensure_ascii=False)}",
            reason_code=reason_code,
            clarification_question=clarification_question,
            expected_input_type=expected_input_type,
            ambiguity_summary={
                "action": action_name,
                "pending_field": pending_field,
                "resolution_status": str(resolution.get("status") or "").strip(),
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
            metadata={
                "trigger": "workflow_action_create_task_reference",
                "action_name": action_name,
                "create_task_action_resolution": {
                    "payload": dict(payload or {}),
                    "pending_field": pending_field,
                },
            },
        )
        self._append_clarification_turn(
            thread_id=thread_id,
            role="assistant",
            message_text=clarification_question,
            source_type="clarification_prompt",
            answer_source="pending_clarification",
            confidence=None,
            ambiguity_flag=True,
            ambiguity_payload={
                "action": action_name,
                "pending_field": pending_field,
            },
            candidate_snapshot=candidates,
            provenance_snapshot=[],
        )
        return {
            "action": action_name,
            "source": "pending_clarification",
            "success": False,
            "status": "pending_clarification",
            "thread_id": thread_id,
            "clarification_question": clarification_question,
            "reason_code": reason_code,
            "expected_input_type": expected_input_type,
            "candidates": candidates,
            "message": clarification_question,
            "data": {
                "thread_id": thread_id,
                "resolution": resolution,
                "pending_field": pending_field,
            },
        }

    def _dispatch_solf_clause_request(self, question: str, clause_request: dict[str, Any]) -> dict[str, Any]:
        clause_name = str(clause_request.get("clause_name") or "").strip()
        args = clause_request.get("args") if isinstance(clause_request.get("args"), list) else []

        if clause_name == "merge_entities" and len(args) >= 2:
            raw_args = [args[0], args[1]]
            resolved_ids: dict[str, Any] = {}
            for index, arg in enumerate(raw_args, start=1):
                slot = f"entity_{index}"
                resolution = self._resolve_merge_entity_reference(arg)
                if str(resolution.get("status") or "") != "resolved":
                    return self._create_merge_clause_clarification(
                        original_query=question,
                        clause_name=clause_name,
                        raw_args=raw_args,
                        pending_slot=slot,
                        resolution=resolution,
                        resolved_ids=resolved_ids,
                    )
                resolved_ids[slot] = int(resolution.get("object_id") or 0)

            evaluated = self.evaluate_solf_clause(
                clause_name=clause_name,
                payload=clause_request.get("payload") if isinstance(clause_request.get("payload"), dict) else {},
                args=[int(resolved_ids.get("entity_1") or 0), int(resolved_ids.get("entity_2") or 0)],
            )
            query_like = self._format_direct_solf_query_result(question, evaluated)
            data = query_like.get("data") if isinstance(query_like.get("data"), dict) else {}
            data["dispatch_mode"] = clause_request.get("dispatch_mode")
            data["resolved_ids"] = dict(resolved_ids)
            query_like["data"] = data
            return query_like

        if clause_name == "retrieve_document_file" and len(args) >= 1:
            raw_arg = args[0]
            resolution = self._resolve_document_reference(raw_arg)
            if str(resolution.get("status") or "") != "resolved":
                return self._create_document_clause_clarification(
                    original_query=question,
                    clause_name=clause_name,
                    raw_arg=raw_arg,
                    resolution=resolution,
                )

            resolved_doc_id = int(resolution.get("doc_id") or 0)
            evaluated = self.evaluate_solf_clause(
                clause_name=clause_name,
                payload=clause_request.get("payload") if isinstance(clause_request.get("payload"), dict) else {},
                args=[resolved_doc_id],
            )
            query_like = self._format_direct_solf_query_result(question, evaluated)
            data = query_like.get("data") if isinstance(query_like.get("data"), dict) else {}
            data["dispatch_mode"] = clause_request.get("dispatch_mode")
            data["resolved_doc_id"] = resolved_doc_id
            data["resolved_document"] = {
                "doc_id": resolved_doc_id,
                "doc_key": str(resolution.get("doc_key") or "").strip(),
                "doc_name": str(resolution.get("doc_name") or "").strip(),
                "doc_path": str(resolution.get("doc_path") or "").strip(),
            }
            query_like["data"] = data
            return query_like

        if clause_name in {"entity_update", "update"} and len(args) >= 1:
            base_payload = clause_request.get("payload") if isinstance(clause_request.get("payload"), dict) else {}
            update_payload = self._extract_entity_update_payload(args[0], base_payload=base_payload)
            if not isinstance(update_payload, dict) or not update_payload.get("object_name"):
                evaluated = {
                    "success": False,
                    "message": "Could not parse update target. Use format: <entity> <field> to <value>.",
                    "clause_name": clause_name,
                    "args": args,
                    "result": None,
                }
                return self._format_direct_solf_query_result(question, evaluated)

            resolution = self._resolve_merge_entity_reference(update_payload.get("object_name"))
            if str(resolution.get("status") or "") != "resolved":
                return self._create_update_clause_clarification(
                    original_query=question,
                    clause_name=clause_name,
                    update_payload=update_payload,
                    resolution=resolution,
                )

            resolved_payload = dict(update_payload)
            resolved_payload["resolved_object_id"] = int(resolution.get("object_id") or 0)
            resolved_payload["object_name"] = str(resolution.get("object_name") or resolved_payload.get("object_name") or "").strip()
            if str(resolution.get("class_name") or "").strip() and not resolved_payload.get("class_name"):
                resolved_payload["class_name"] = str(resolution.get("class_name") or "").strip()

            evaluated = self.evaluate_solf_clause(
                clause_name=clause_name,
                payload={},
                args=[resolved_payload],
            )
            query_like = self._format_direct_solf_query_result(question, evaluated)
            data = query_like.get("data") if isinstance(query_like.get("data"), dict) else {}
            data["dispatch_mode"] = clause_request.get("dispatch_mode")
            data["resolved_object_id"] = int(resolution.get("object_id") or 0)
            data["resolved_payload"] = resolved_payload
            query_like["data"] = data
            return query_like

        evaluated = self.evaluate_solf_clause(
            clause_name=clause_name,
            payload=clause_request.get("payload") if isinstance(clause_request.get("payload"), dict) else {},
            args=args,
        )
        query_like = self._format_direct_solf_query_result(question, evaluated)
        data = query_like.get("data") if isinstance(query_like.get("data"), dict) else {}
        data["dispatch_mode"] = clause_request.get("dispatch_mode")
        query_like["data"] = data
        return query_like

    def _extract_clarification_signal(self, user_message: str, query_result: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(query_result, dict):
            return None

        answer_source = str(query_result.get("answer_source") or "").strip().lower()
        result_data = query_result.get("data") if isinstance(query_result.get("data"), dict) else {}
        grounding = query_result.get("grounding") if isinstance(query_result.get("grounding"), dict) else {}
        parsed = grounding.get("parsed") if isinstance(grounding.get("parsed"), dict) else {}
        provenance_snapshot = self._extract_provenance_snapshot(query_result)

        candidates: list[Any] = []
        ambiguity_summary: dict[str, Any] = {}
        reason_code = ""

        direct = grounding.get("direct_query_result") if isinstance(grounding.get("direct_query_result"), dict) else {}
        direct_data = direct.get("data") if isinstance(direct.get("data"), dict) else {}
        resolution_data = direct_data if direct_data else result_data
        direct_resolution_status = str(resolution_data.get("resolution_status") or "").strip().lower()
        if direct_resolution_status.startswith("ambiguous"):
            reason_code = direct_resolution_status if direct_resolution_status in {"ambiguous_entity", "ambiguous_value"} else "ambiguous_value"
            candidates = list(
                resolution_data.get("entity_candidates")
                or resolution_data.get("candidate_values")
                or resolution_data.get("candidates")
                or []
            )
            ambiguity_summary = {
                "resolution_status": direct_resolution_status,
                "entity_hint": resolution_data.get("entity_hint"),
                "top_score": resolution_data.get("top_score"),
                "second_score": resolution_data.get("second_score"),
                "score_gap": resolution_data.get("score_gap"),
                "attribute_name": resolution_data.get("attribute_name"),
                "entity_name": resolution_data.get("entity_name"),
            }

        if not reason_code and "ambiguous" in answer_source:
            reason_code = "ambiguous_value"

        attribute_result = grounding.get("attribute_result") if isinstance(grounding.get("attribute_result"), dict) else {}
        attr_value = attribute_result.get("attribute_value")
        attr_name = attribute_result.get("attribute_name") or parsed.get("attribute_name")
        if (
            not reason_code
            and isinstance(attr_value, list)
            and len(attr_value) > 1
            and self._contains_singular_intent(user_message)
            and not self._contains_plural_intent(user_message)
            and not self._attribute_expects_plural(attr_name)
        ):
            reason_code = "ambiguous_value"
            candidates = list(attr_value)
            ambiguity_summary = {
                "resolution_status": "ambiguous_value",
                "attribute_name": attr_name,
                "entity_name": attribute_result.get("entity_name") or parsed.get("entity_name"),
                "candidate_count": len(attr_value),
            }

        if not reason_code:
            return None

        if reason_code == "ambiguous_value" and self._contains_plural_intent(user_message):
            return None

        return {
            "reason_code": reason_code,
            "expected_input_type": "entity_scope" if reason_code == "ambiguous_entity" else "free_text",
            "candidates": self._normalize_candidate_snapshot(candidates, limit=8),
            "provenance_snapshot": provenance_snapshot,
            "ambiguity_summary": ambiguity_summary,
            "answer_source": answer_source or "unknown",
            "confidence": parsed.get("confidence"),
        }

    def _create_clarification_thread(
        self,
        *,
        original_query: str,
        reason_code: str,
        clarification_question: str,
        expected_input_type: str,
        ambiguity_summary: dict[str, Any] | None,
        candidate_snapshot: list[Any] | None,
        provenance_snapshot: list[dict[str, Any]] | None,
        metadata: dict[str, Any] | None,
    ) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO interaction_clarification_thread (
                        original_query,
                        reason_code,
                        clarification_question,
                        expected_input_type,
                        ambiguity_summary,
                        candidate_snapshot,
                        provenance_snapshot,
                        metadata
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING thread_id
                    """,
                    (
                        str(original_query or "").strip(),
                        str(reason_code or "ambiguous_value").strip(),
                        str(clarification_question or "").strip(),
                        str(expected_input_type or "free_text").strip(),
                        Json(ambiguity_summary or {}),
                        Json(candidate_snapshot or []),
                        Json(provenance_snapshot or []),
                        Json(metadata or {}),
                    ),
                )
                row = cur.fetchone()
                return int(row[0])

    def _append_clarification_turn(
        self,
        *,
        thread_id: int,
        role: str,
        message_text: str,
        source_type: str | None = None,
        answer_source: str | None = None,
        confidence: float | None = None,
        ambiguity_flag: bool = False,
        ambiguity_payload: dict[str, Any] | None = None,
        candidate_snapshot: list[Any] | None = None,
        provenance_snapshot: list[dict[str, Any]] | None = None,
        linked_doc_ids: list[int] | None = None,
        linked_object_ids: list[int] | None = None,
    ) -> None:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(MAX(turn_index), 0) + 1 FROM interaction_clarification_turn WHERE thread_id = %s",
                    (int(thread_id),),
                )
                next_idx = int((cur.fetchone() or [1])[0] or 1)
                cur.execute(
                    """
                    INSERT INTO interaction_clarification_turn (
                        thread_id,
                        turn_index,
                        role,
                        message_text,
                        source_type,
                        answer_source,
                        confidence,
                        ambiguity_flag,
                        ambiguity_payload,
                        candidate_snapshot,
                        provenance_snapshot,
                        linked_doc_ids,
                        linked_object_ids
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        int(thread_id),
                        next_idx,
                        str(role or "assistant").strip().lower(),
                        str(message_text or "").strip(),
                        str(source_type or "").strip() or None,
                        str(answer_source or "").strip() or None,
                        float(confidence) if confidence is not None else None,
                        bool(ambiguity_flag),
                        Json(ambiguity_payload or {}),
                        Json(candidate_snapshot or []),
                        Json(provenance_snapshot or []),
                        Json([int(item) for item in (linked_doc_ids or []) if int(item) > 0]),
                        Json([int(item) for item in (linked_object_ids or []) if int(item) > 0]),
                    ),
                )
                cur.execute(
                    "UPDATE interaction_clarification_thread SET updated_at = NOW() WHERE thread_id = %s",
                    (int(thread_id),),
                )

    def _get_clarification_thread_row(self, thread_id: int) -> dict[str, Any] | None:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT thread_id, status, original_query, reason_code, clarification_question,
                           expected_input_type, ambiguity_summary, candidate_snapshot, provenance_snapshot,
                           metadata, created_at, updated_at, resolved_at
                    FROM interaction_clarification_thread
                    WHERE thread_id = %s
                    """,
                    (int(thread_id),),
                )
                row = cur.fetchone()
                if not row:
                    return None

                cur.execute(
                    """
                    SELECT turn_id, turn_index, role, message_text, source_type, answer_source,
                           confidence, ambiguity_flag, ambiguity_payload, candidate_snapshot,
                           provenance_snapshot, linked_doc_ids, linked_object_ids, created_at
                    FROM interaction_clarification_turn
                    WHERE thread_id = %s
                    ORDER BY turn_index ASC
                    """,
                    (int(thread_id),),
                )
                turns = cur.fetchall() or []

        return {
            "thread_id": int(row[0]),
            "status": row[1],
            "original_query": row[2],
            "reason_code": row[3],
            "clarification_question": row[4],
            "expected_input_type": row[5],
            "ambiguity_summary": row[6] if isinstance(row[6], dict) else {},
            "candidate_snapshot": row[7] if isinstance(row[7], list) else [],
            "provenance_snapshot": row[8] if isinstance(row[8], list) else [],
            "metadata": row[9] if isinstance(row[9], dict) else {},
            "created_at": row[10].isoformat() if row[10] else None,
            "updated_at": row[11].isoformat() if row[11] else None,
            "resolved_at": row[12].isoformat() if row[12] else None,
            "turns": [
                {
                    "turn_id": int(item[0]),
                    "turn_index": int(item[1]),
                    "role": item[2],
                    "message_text": item[3],
                    "source_type": item[4],
                    "answer_source": item[5],
                    "confidence": float(item[6]) if item[6] is not None else None,
                    "ambiguity_flag": bool(item[7]),
                    "ambiguity_payload": item[8] if isinstance(item[8], dict) else {},
                    "candidate_snapshot": item[9] if isinstance(item[9], list) else [],
                    "provenance_snapshot": item[10] if isinstance(item[10], list) else [],
                    "linked_doc_ids": item[11] if isinstance(item[11], list) else [],
                    "linked_object_ids": item[12] if isinstance(item[12], list) else [],
                    "created_at": item[13].isoformat() if item[13] else None,
                }
                for item in turns
            ],
        }

    def _set_clarification_thread_status(self, thread_id: int, status: str) -> None:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE interaction_clarification_thread
                    SET status = %s,
                        updated_at = NOW(),
                        resolved_at = CASE WHEN %s = 'resolved' THEN NOW() ELSE resolved_at END
                    WHERE thread_id = %s
                    """,
                    (str(status or "pending_clarification"), str(status or "pending_clarification"), int(thread_id)),
                )

    def _update_clarification_prompt(
        self,
        thread_id: int,
        *,
        reason_code: str,
        clarification_question: str,
        expected_input_type: str,
        ambiguity_summary: dict[str, Any] | None,
        candidate_snapshot: list[Any] | None,
        provenance_snapshot: list[dict[str, Any]] | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE interaction_clarification_thread
                    SET status = 'pending_clarification',
                        reason_code = %s,
                        clarification_question = %s,
                        expected_input_type = %s,
                        ambiguity_summary = %s,
                        candidate_snapshot = %s,
                        provenance_snapshot = %s,
                        metadata = COALESCE(metadata, '{}'::jsonb) || %s,
                        updated_at = NOW()
                    WHERE thread_id = %s
                    """,
                    (
                        str(reason_code or "ambiguous_value"),
                        str(clarification_question or "").strip(),
                        str(expected_input_type or "free_text").strip(),
                        Json(ambiguity_summary or {}),
                        Json(candidate_snapshot or []),
                        Json(provenance_snapshot or []),
                        Json(metadata or {}),
                        int(thread_id),
                    ),
                )

    def get_clarification_thread(self, thread_id: int) -> dict[str, Any] | None:
        return self._get_clarification_thread_row(int(thread_id))

    def respond_to_clarification(self, thread_id: int, user_response: str) -> dict[str, Any]:
        response_text = str(user_response or "").strip()
        if not response_text:
            raise ValueError("user_response must not be empty")

        thread = self._get_clarification_thread_row(int(thread_id))
        if not thread:
            raise ValueError(f"Clarification thread {thread_id} not found")
        if str(thread.get("status") or "") != "pending_clarification":
            raise ValueError(f"Clarification thread {thread_id} is not pending")

        self._append_clarification_turn(
            thread_id=int(thread_id),
            role="user",
            message_text=response_text,
            source_type="clarification_response",
        )

        thread_metadata = thread.get("metadata") if isinstance(thread.get("metadata"), dict) else {}
        if str(thread_metadata.get("trigger") or "") == "solf_clause_merge_entities":
            merge_meta = thread_metadata.get("merge_resolution") if isinstance(thread_metadata.get("merge_resolution"), dict) else {}
            raw_args = [str(arg or "").strip() for arg in list(merge_meta.get("raw_args") or [])[:2]]
            resolved_ids = dict(merge_meta.get("resolved_ids") or {})
            pending_slot = str(merge_meta.get("pending_slot") or "").strip() or "entity_1"
            candidate_snapshot = list(thread.get("candidate_snapshot") or [])
            selected_label = self._match_user_response_to_candidate(candidate_snapshot, response_text)
            selected_entry = self._find_candidate_snapshot_entry(candidate_snapshot, selected_label) if selected_label else None
            selected_object_id = int(selected_entry.get("object_id") or 0) if isinstance(selected_entry, dict) else 0

            if selected_object_id <= 0:
                resolution = self._resolve_merge_entity_reference(response_text)
                if str(resolution.get("status") or "") != "resolved":
                    clarification_question = self._build_merge_clarification_question(
                        pending_slot,
                        str(resolution.get("input") or response_text).strip(),
                        "ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                    )
                    self._update_clarification_prompt(
                        int(thread_id),
                        reason_code="ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                        clarification_question=clarification_question,
                        expected_input_type="entity_scope",
                        ambiguity_summary={
                            "clause_name": str(thread_metadata.get("solf_clause_name") or "merge_entities"),
                            "pending_slot": pending_slot,
                            "entity_input": str(resolution.get("input") or response_text).strip(),
                        },
                        candidate_snapshot=list(resolution.get("candidates") or []),
                        provenance_snapshot=[],
                        metadata={
                            "trigger": "solf_clause_merge_entities",
                            "solf_clause_name": str(thread_metadata.get("solf_clause_name") or "merge_entities"),
                            "merge_resolution": {
                                "raw_args": raw_args,
                                "resolved_ids": resolved_ids,
                                "pending_slot": pending_slot,
                            },
                        },
                    )
                    self._append_clarification_turn(
                        thread_id=int(thread_id),
                        role="assistant",
                        message_text=clarification_question,
                        source_type="clarification_prompt",
                        answer_source="pending_clarification",
                        ambiguity_flag=True,
                        ambiguity_payload={"pending_slot": pending_slot},
                        candidate_snapshot=list(resolution.get("candidates") or []),
                    )
                    return {
                        "status": "pending_clarification",
                        "thread_id": int(thread_id),
                        "clarification_question": clarification_question,
                        "reason_code": "ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                        "expected_input_type": "entity_scope",
                        "candidates": list(resolution.get("candidates") or []),
                        "provenance_snapshot": [],
                        "grounding": {"trigger": "solf_clause_merge_entities"},
                    }
                selected_object_id = int(resolution.get("object_id") or 0)

            resolved_ids[pending_slot] = selected_object_id
            for slot, idx in (("entity_1", 0), ("entity_2", 1)):
                if resolved_ids.get(slot):
                    continue
                if idx >= len(raw_args):
                    continue
                next_resolution = self._resolve_merge_entity_reference(raw_args[idx])
                if str(next_resolution.get("status") or "") == "resolved":
                    resolved_ids[slot] = int(next_resolution.get("object_id") or 0)
                    continue
                clarification_question = self._build_merge_clarification_question(
                    slot,
                    str(next_resolution.get("input") or raw_args[idx]).strip(),
                    "ambiguous_entity" if str(next_resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                )
                self._update_clarification_prompt(
                    int(thread_id),
                    reason_code="ambiguous_entity" if str(next_resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                    clarification_question=clarification_question,
                    expected_input_type="entity_scope",
                    ambiguity_summary={
                        "clause_name": str(thread_metadata.get("solf_clause_name") or "merge_entities"),
                        "pending_slot": slot,
                        "entity_input": str(next_resolution.get("input") or raw_args[idx]).strip(),
                    },
                    candidate_snapshot=list(next_resolution.get("candidates") or []),
                    provenance_snapshot=[],
                    metadata={
                        "trigger": "solf_clause_merge_entities",
                        "solf_clause_name": str(thread_metadata.get("solf_clause_name") or "merge_entities"),
                        "merge_resolution": {
                            "raw_args": raw_args,
                            "resolved_ids": resolved_ids,
                            "pending_slot": slot,
                        },
                    },
                )
                self._append_clarification_turn(
                    thread_id=int(thread_id),
                    role="assistant",
                    message_text=clarification_question,
                    source_type="clarification_prompt",
                    answer_source="pending_clarification",
                    ambiguity_flag=True,
                    ambiguity_payload={"pending_slot": slot},
                    candidate_snapshot=list(next_resolution.get("candidates") or []),
                )
                return {
                    "status": "pending_clarification",
                    "thread_id": int(thread_id),
                    "clarification_question": clarification_question,
                    "reason_code": "ambiguous_entity" if str(next_resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                    "expected_input_type": "entity_scope",
                    "candidates": list(next_resolution.get("candidates") or []),
                    "provenance_snapshot": [],
                    "grounding": {"trigger": "solf_clause_merge_entities"},
                }

            clause_name = str(thread_metadata.get("solf_clause_name") or "merge_entities")
            evaluated = self.evaluate_solf_clause(clause_name=clause_name, payload={}, args=[int(resolved_ids.get("entity_1") or 0), int(resolved_ids.get("entity_2") or 0)])
            query_result = self._format_direct_solf_query_result(original_query := str(thread.get("original_query") or "").strip(), evaluated)
            final_answer = str(query_result.get("answer") or "").strip()
            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=final_answer,
                source_type="clarification_resolution",
                answer_source="solf_clause",
                ambiguity_flag=False,
                linked_object_ids=[int(resolved_ids.get("entity_1") or 0), int(resolved_ids.get("entity_2") or 0)],
            )
            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": final_answer,
                "answer_source": "solf_clause",
                "grounding": query_result,
            }

        if str(thread_metadata.get("trigger") or "") == "solf_clause_retrieve_document_file":
            doc_meta = thread_metadata.get("document_resolution") if isinstance(thread_metadata.get("document_resolution"), dict) else {}
            raw_arg = str(doc_meta.get("raw_arg") or "").strip()
            clause_name = str(thread_metadata.get("solf_clause_name") or "retrieve_document_file")
            candidate_snapshot = list(thread.get("candidate_snapshot") or [])
            selected_label = self._match_user_response_to_candidate(candidate_snapshot, response_text)
            selected_entry = self._find_candidate_snapshot_entry(candidate_snapshot, selected_label) if selected_label else None
            selected_doc_id = int(selected_entry.get("doc_id") or 0) if isinstance(selected_entry, dict) else 0

            if selected_doc_id <= 0:
                resolution = self._resolve_document_reference(response_text)
                if str(resolution.get("status") or "") != "resolved":
                    clarification_question = self._build_document_clause_clarification_question(
                        str(resolution.get("input") or response_text).strip(),
                        "ambiguous_document" if str(resolution.get("status") or "") == "ambiguous" else "document_not_found",
                    )
                    self._update_clarification_prompt(
                        int(thread_id),
                        reason_code="ambiguous_document" if str(resolution.get("status") or "") == "ambiguous" else "document_not_found",
                        clarification_question=clarification_question,
                        expected_input_type="document_scope",
                        ambiguity_summary={
                            "clause_name": clause_name,
                            "document_input": str(resolution.get("input") or response_text).strip(),
                        },
                        candidate_snapshot=list(resolution.get("candidates") or []),
                        provenance_snapshot=[],
                        metadata={
                            "trigger": "solf_clause_retrieve_document_file",
                            "solf_clause_name": clause_name,
                            "document_resolution": {
                                "raw_arg": raw_arg,
                            },
                        },
                    )
                    self._append_clarification_turn(
                        thread_id=int(thread_id),
                        role="assistant",
                        message_text=clarification_question,
                        source_type="clarification_prompt",
                        answer_source="pending_clarification",
                        ambiguity_flag=True,
                        ambiguity_payload={"clause_name": clause_name},
                        candidate_snapshot=list(resolution.get("candidates") or []),
                    )
                    return {
                        "status": "pending_clarification",
                        "thread_id": int(thread_id),
                        "clarification_question": clarification_question,
                        "reason_code": "ambiguous_document" if str(resolution.get("status") or "") == "ambiguous" else "document_not_found",
                        "expected_input_type": "document_scope",
                        "candidates": list(resolution.get("candidates") or []),
                        "provenance_snapshot": [],
                        "grounding": {"trigger": "solf_clause_retrieve_document_file"},
                    }
                selected_doc_id = int(resolution.get("doc_id") or 0)

            evaluated = self.evaluate_solf_clause(clause_name=clause_name, payload={}, args=[selected_doc_id])
            query_result = self._format_direct_solf_query_result(str(thread.get("original_query") or "").strip(), evaluated)
            final_answer = str(query_result.get("answer") or "").strip()
            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=final_answer,
                source_type="clarification_resolution",
                answer_source="solf_clause",
                ambiguity_flag=False,
                linked_doc_ids=[selected_doc_id],
            )
            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": final_answer,
                "answer_source": "solf_clause",
                "grounding": query_result,
            }

        if str(thread_metadata.get("trigger") or "") == "solf_clause_entity_update":
            update_meta = thread_metadata.get("update_resolution") if isinstance(thread_metadata.get("update_resolution"), dict) else {}
            update_payload = dict(update_meta.get("update_payload") or {}) if isinstance(update_meta.get("update_payload"), dict) else {}
            clause_name = str(thread_metadata.get("solf_clause_name") or "entity_update")
            candidate_snapshot = list(thread.get("candidate_snapshot") or [])
            selected_label = self._match_user_response_to_candidate(candidate_snapshot, response_text)
            selected_entry = self._find_candidate_snapshot_entry(candidate_snapshot, selected_label) if selected_label else None
            selected_object_id = int(selected_entry.get("object_id") or 0) if isinstance(selected_entry, dict) else 0

            if selected_object_id <= 0:
                resolution = self._resolve_merge_entity_reference(response_text)
                if str(resolution.get("status") or "") != "resolved":
                    clarification_question = self._build_update_clause_clarification_question(
                        str(resolution.get("input") or response_text).strip(),
                        "ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                    )
                    self._update_clarification_prompt(
                        int(thread_id),
                        reason_code="ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                        clarification_question=clarification_question,
                        expected_input_type="entity_scope",
                        ambiguity_summary={
                            "clause_name": clause_name,
                            "entity_input": str(resolution.get("input") or response_text).strip(),
                        },
                        candidate_snapshot=list(resolution.get("candidates") or []),
                        provenance_snapshot=[],
                        metadata={
                            "trigger": "solf_clause_entity_update",
                            "solf_clause_name": clause_name,
                            "update_resolution": {
                                "update_payload": update_payload,
                            },
                        },
                    )
                    self._append_clarification_turn(
                        thread_id=int(thread_id),
                        role="assistant",
                        message_text=clarification_question,
                        source_type="clarification_prompt",
                        answer_source="pending_clarification",
                        ambiguity_flag=True,
                        ambiguity_payload={"clause_name": clause_name},
                        candidate_snapshot=list(resolution.get("candidates") or []),
                    )
                    return {
                        "status": "pending_clarification",
                        "thread_id": int(thread_id),
                        "clarification_question": clarification_question,
                        "reason_code": "ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                        "expected_input_type": "entity_scope",
                        "candidates": list(resolution.get("candidates") or []),
                        "provenance_snapshot": [],
                        "grounding": {"trigger": "solf_clause_entity_update"},
                    }
                selected_object_id = int(resolution.get("object_id") or 0)
                if str(resolution.get("object_name") or "").strip():
                    update_payload["object_name"] = str(resolution.get("object_name") or "").strip()

            resolved_payload = dict(update_payload)
            resolved_payload["resolved_object_id"] = selected_object_id
            evaluated = self.evaluate_solf_clause(clause_name=clause_name, payload={}, args=[resolved_payload])
            query_result = self._format_direct_solf_query_result(str(thread.get("original_query") or "").strip(), evaluated)
            final_answer = str(query_result.get("answer") or "").strip()
            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=final_answer,
                source_type="clarification_resolution",
                answer_source="solf_clause",
                ambiguity_flag=False,
                linked_object_ids=[selected_object_id],
            )
            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": final_answer,
                "answer_source": "solf_clause",
                "grounding": query_result,
            }

        if str(thread_metadata.get("trigger") or "") == "workflow_action_update_entity":
            action_name = str(thread_metadata.get("action_name") or "update_entity")
            action_meta = thread_metadata.get("update_action_resolution") if isinstance(thread_metadata.get("update_action_resolution"), dict) else {}
            action_payload = dict(action_meta.get("payload") or {}) if isinstance(action_meta.get("payload"), dict) else {}
            candidate_snapshot = list(thread.get("candidate_snapshot") or [])
            selected_label = self._match_user_response_to_candidate(candidate_snapshot, response_text)
            selected_entry = self._find_candidate_snapshot_entry(candidate_snapshot, selected_label) if selected_label else None
            selected_object_id = int(selected_entry.get("object_id") or 0) if isinstance(selected_entry, dict) else 0

            if selected_object_id <= 0:
                resolution = self._resolve_merge_entity_reference(response_text)
                if str(resolution.get("status") or "") != "resolved":
                    clarification_question = self._build_update_clause_clarification_question(
                        str(resolution.get("input") or response_text).strip(),
                        "ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                    )
                    self._update_clarification_prompt(
                        int(thread_id),
                        reason_code="ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                        clarification_question=clarification_question,
                        expected_input_type="entity_scope",
                        ambiguity_summary={
                            "action": action_name,
                            "entity_input": str(resolution.get("input") or response_text).strip(),
                        },
                        candidate_snapshot=list(resolution.get("candidates") or []),
                        provenance_snapshot=[],
                        metadata={
                            "trigger": "workflow_action_update_entity",
                            "action_name": action_name,
                            "update_action_resolution": {
                                "payload": action_payload,
                            },
                        },
                    )
                    self._append_clarification_turn(
                        thread_id=int(thread_id),
                        role="assistant",
                        message_text=clarification_question,
                        source_type="clarification_prompt",
                        answer_source="pending_clarification",
                        ambiguity_flag=True,
                        ambiguity_payload={"action": action_name},
                        candidate_snapshot=list(resolution.get("candidates") or []),
                    )
                    return {
                        "status": "pending_clarification",
                        "thread_id": int(thread_id),
                        "clarification_question": clarification_question,
                        "reason_code": "ambiguous_entity" if str(resolution.get("status") or "") == "ambiguous" else "entity_not_found",
                        "expected_input_type": "entity_scope",
                        "candidates": list(resolution.get("candidates") or []),
                        "provenance_snapshot": [],
                        "grounding": {"trigger": "workflow_action_update_entity"},
                    }
                selected_object_id = int(resolution.get("object_id") or 0)

            resolved_payload = dict(action_payload)
            resolved_payload["entity_id"] = selected_object_id
            action_result = self.perform_action(action_name, resolved_payload)
            if str(action_result.get("status") or "") == "pending_clarification":
                return action_result

            final_answer = str(action_result.get("message") or "").strip() or "Workflow action completed."
            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=final_answer,
                source_type="clarification_resolution",
                answer_source="workflow_action",
                ambiguity_flag=False,
                linked_object_ids=[selected_object_id],
            )
            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": final_answer,
                "answer_source": "workflow_action",
                "grounding": action_result,
            }

        if str(thread_metadata.get("trigger") or "") == "workflow_action_retrieve_document_file":
            action_name = str(thread_metadata.get("action_name") or "retrieve_document_file")
            action_meta = thread_metadata.get("retrieve_action_resolution") if isinstance(thread_metadata.get("retrieve_action_resolution"), dict) else {}
            action_payload = dict(action_meta.get("payload") or {}) if isinstance(action_meta.get("payload"), dict) else {}
            candidate_snapshot = list(thread.get("candidate_snapshot") or [])
            selected_label = self._match_user_response_to_candidate(candidate_snapshot, response_text)
            selected_entry = self._find_candidate_snapshot_entry(candidate_snapshot, selected_label) if selected_label else None
            selected_doc_id = int(selected_entry.get("doc_id") or 0) if isinstance(selected_entry, dict) else 0

            if selected_doc_id <= 0:
                resolution = self._resolve_document_reference(response_text)
                if str(resolution.get("status") or "") != "resolved":
                    clarification_question = self._build_document_clause_clarification_question(
                        str(resolution.get("input") or response_text).strip(),
                        "ambiguous_document" if str(resolution.get("status") or "") == "ambiguous" else "document_not_found",
                    )
                    self._update_clarification_prompt(
                        int(thread_id),
                        reason_code="ambiguous_document" if str(resolution.get("status") or "") == "ambiguous" else "document_not_found",
                        clarification_question=clarification_question,
                        expected_input_type="document_scope",
                        ambiguity_summary={
                            "action": action_name,
                            "document_input": str(resolution.get("input") or response_text).strip(),
                        },
                        candidate_snapshot=list(resolution.get("candidates") or []),
                        provenance_snapshot=[],
                        metadata={
                            "trigger": "workflow_action_retrieve_document_file",
                            "action_name": action_name,
                            "retrieve_action_resolution": {
                                "payload": action_payload,
                            },
                        },
                    )
                    self._append_clarification_turn(
                        thread_id=int(thread_id),
                        role="assistant",
                        message_text=clarification_question,
                        source_type="clarification_prompt",
                        answer_source="pending_clarification",
                        ambiguity_flag=True,
                        ambiguity_payload={"action": action_name},
                        candidate_snapshot=list(resolution.get("candidates") or []),
                    )
                    return {
                        "status": "pending_clarification",
                        "thread_id": int(thread_id),
                        "clarification_question": clarification_question,
                        "reason_code": "ambiguous_document" if str(resolution.get("status") or "") == "ambiguous" else "document_not_found",
                        "expected_input_type": "document_scope",
                        "candidates": list(resolution.get("candidates") or []),
                        "provenance_snapshot": [],
                        "grounding": {"trigger": "workflow_action_retrieve_document_file"},
                    }
                selected_doc_id = int(resolution.get("doc_id") or 0)

            resolved_payload = dict(action_payload)
            resolved_payload["doc_id"] = selected_doc_id
            action_result = self.perform_action(action_name, resolved_payload)
            if str(action_result.get("status") or "") == "pending_clarification":
                return action_result

            final_answer = str(action_result.get("message") or "").strip() or "Workflow action completed."
            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=final_answer,
                source_type="clarification_resolution",
                answer_source="workflow_action",
                ambiguity_flag=False,
                linked_doc_ids=[selected_doc_id],
            )
            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": final_answer,
                "answer_source": "workflow_action",
                "grounding": action_result,
            }

        if str(thread_metadata.get("trigger") or "") == "workflow_action_task_reference":
            action_name = str(thread_metadata.get("action_name") or "assign_task")
            action_meta = thread_metadata.get("task_action_resolution") if isinstance(thread_metadata.get("task_action_resolution"), dict) else {}
            action_payload = dict(action_meta.get("payload") or {}) if isinstance(action_meta.get("payload"), dict) else {}
            candidate_snapshot = list(thread.get("candidate_snapshot") or [])
            selected_label = self._match_user_response_to_candidate(candidate_snapshot, response_text)
            selected_entry = self._find_candidate_snapshot_entry(candidate_snapshot, selected_label) if selected_label else None
            selected_task_id = int(selected_entry.get("task_id") or 0) if isinstance(selected_entry, dict) else 0

            if selected_task_id <= 0:
                resolution = self._resolve_task_reference(response_text)
                if str(resolution.get("status") or "") != "resolved":
                    clarification_question = (
                        f"I found multiple tasks matching '{str(resolution.get('input') or response_text).strip()}'. Please choose the exact task candidate."
                        if str(resolution.get("status") or "") == "ambiguous"
                        else f"I could not resolve task '{str(resolution.get('input') or response_text).strip()}'. Please provide the exact task id or task name."
                    )
                    self._update_clarification_prompt(
                        int(thread_id),
                        reason_code="ambiguous_task" if str(resolution.get("status") or "") == "ambiguous" else "task_not_found",
                        clarification_question=clarification_question,
                        expected_input_type="task_scope",
                        ambiguity_summary={
                            "action": action_name,
                            "task_input": str(resolution.get("input") or response_text).strip(),
                        },
                        candidate_snapshot=list(resolution.get("candidates") or []),
                        provenance_snapshot=[],
                        metadata={
                            "trigger": "workflow_action_task_reference",
                            "action_name": action_name,
                            "task_action_resolution": {
                                "payload": action_payload,
                            },
                        },
                    )
                    self._append_clarification_turn(
                        thread_id=int(thread_id),
                        role="assistant",
                        message_text=clarification_question,
                        source_type="clarification_prompt",
                        answer_source="pending_clarification",
                        ambiguity_flag=True,
                        ambiguity_payload={"action": action_name},
                        candidate_snapshot=list(resolution.get("candidates") or []),
                    )
                    return {
                        "status": "pending_clarification",
                        "thread_id": int(thread_id),
                        "clarification_question": clarification_question,
                        "reason_code": "ambiguous_task" if str(resolution.get("status") or "") == "ambiguous" else "task_not_found",
                        "expected_input_type": "task_scope",
                        "candidates": list(resolution.get("candidates") or []),
                        "provenance_snapshot": [],
                        "grounding": {"trigger": "workflow_action_task_reference"},
                    }
                selected_task_id = int(resolution.get("task_id") or 0)

            resolved_payload = dict(action_payload)
            resolved_payload["task_id"] = selected_task_id
            action_result = self.perform_action(action_name, resolved_payload)
            if str(action_result.get("status") or "") == "pending_clarification":
                return action_result

            final_answer = str(action_result.get("message") or "").strip() or "Workflow action completed."
            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=final_answer,
                source_type="clarification_resolution",
                answer_source="workflow_action",
                ambiguity_flag=False,
            )
            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": final_answer,
                "answer_source": "workflow_action",
                "grounding": action_result,
            }

        if str(thread_metadata.get("trigger") or "") == "workflow_action_create_task_reference":
            action_name = str(thread_metadata.get("action_name") or "create_task")
            action_meta = thread_metadata.get("create_task_action_resolution") if isinstance(thread_metadata.get("create_task_action_resolution"), dict) else {}
            action_payload = dict(action_meta.get("payload") or {}) if isinstance(action_meta.get("payload"), dict) else {}
            pending_field = str(action_meta.get("pending_field") or "project").strip().lower()
            candidate_snapshot = list(thread.get("candidate_snapshot") or [])
            selected_label = self._match_user_response_to_candidate(candidate_snapshot, response_text)
            selected_entry = self._find_candidate_snapshot_entry(candidate_snapshot, selected_label) if selected_label else None

            selected_id = 0
            if isinstance(selected_entry, dict):
                if pending_field == "project":
                    selected_id = int(selected_entry.get("project_id") or 0)
                else:
                    selected_id = int(selected_entry.get("object_id") or 0)

            if selected_id <= 0:
                if pending_field == "project":
                    resolution = self._resolve_project_reference(response_text)
                    resolved_status = str(resolution.get("status") or "")
                    if resolved_status != "resolved":
                        clarification_question = (
                            f"I found multiple projects matching '{str(resolution.get('input') or response_text).strip()}'. Please choose the exact project candidate."
                            if resolved_status == "ambiguous"
                            else f"I could not resolve project '{str(resolution.get('input') or response_text).strip()}'. Please provide the exact project id, code, or name."
                        )
                        self._update_clarification_prompt(
                            int(thread_id),
                            reason_code="ambiguous_project" if resolved_status == "ambiguous" else "project_not_found",
                            clarification_question=clarification_question,
                            expected_input_type="project_scope",
                            ambiguity_summary={
                                "action": action_name,
                                "pending_field": pending_field,
                            },
                            candidate_snapshot=list(resolution.get("candidates") or []),
                            provenance_snapshot=[],
                            metadata={
                                "trigger": "workflow_action_create_task_reference",
                                "action_name": action_name,
                                "create_task_action_resolution": {
                                    "payload": action_payload,
                                    "pending_field": pending_field,
                                },
                            },
                        )
                        self._append_clarification_turn(
                            thread_id=int(thread_id),
                            role="assistant",
                            message_text=clarification_question,
                            source_type="clarification_prompt",
                            answer_source="pending_clarification",
                            ambiguity_flag=True,
                            ambiguity_payload={"action": action_name, "pending_field": pending_field},
                            candidate_snapshot=list(resolution.get("candidates") or []),
                        )
                        return {
                            "status": "pending_clarification",
                            "thread_id": int(thread_id),
                            "clarification_question": clarification_question,
                            "reason_code": "ambiguous_project" if resolved_status == "ambiguous" else "project_not_found",
                            "expected_input_type": "project_scope",
                            "candidates": list(resolution.get("candidates") or []),
                            "provenance_snapshot": [],
                            "grounding": {"trigger": "workflow_action_create_task_reference"},
                        }
                    selected_id = int(resolution.get("project_id") or 0)
                else:
                    resolution = self._resolve_merge_entity_reference(response_text)
                    resolved_status = str(resolution.get("status") or "")
                    if resolved_status != "resolved":
                        clarification_question = (
                            f"I found multiple assignees matching '{str(resolution.get('input') or response_text).strip()}'. Please choose the exact assignee candidate."
                            if resolved_status == "ambiguous"
                            else f"I could not resolve assignee '{str(resolution.get('input') or response_text).strip()}'. Please provide the exact assignee id or name."
                        )
                        self._update_clarification_prompt(
                            int(thread_id),
                            reason_code="ambiguous_assignee" if resolved_status == "ambiguous" else "assignee_not_found",
                            clarification_question=clarification_question,
                            expected_input_type="entity_scope",
                            ambiguity_summary={
                                "action": action_name,
                                "pending_field": pending_field,
                            },
                            candidate_snapshot=list(resolution.get("candidates") or []),
                            provenance_snapshot=[],
                            metadata={
                                "trigger": "workflow_action_create_task_reference",
                                "action_name": action_name,
                                "create_task_action_resolution": {
                                    "payload": action_payload,
                                    "pending_field": pending_field,
                                },
                            },
                        )
                        self._append_clarification_turn(
                            thread_id=int(thread_id),
                            role="assistant",
                            message_text=clarification_question,
                            source_type="clarification_prompt",
                            answer_source="pending_clarification",
                            ambiguity_flag=True,
                            ambiguity_payload={"action": action_name, "pending_field": pending_field},
                            candidate_snapshot=list(resolution.get("candidates") or []),
                        )
                        return {
                            "status": "pending_clarification",
                            "thread_id": int(thread_id),
                            "clarification_question": clarification_question,
                            "reason_code": "ambiguous_assignee" if resolved_status == "ambiguous" else "assignee_not_found",
                            "expected_input_type": "entity_scope",
                            "candidates": list(resolution.get("candidates") or []),
                            "provenance_snapshot": [],
                            "grounding": {"trigger": "workflow_action_create_task_reference"},
                        }
                    selected_id = int(resolution.get("object_id") or 0)

            resolved_payload = dict(action_payload)
            if pending_field == "project":
                resolved_payload["project_id"] = selected_id
            else:
                resolved_payload["assigned_to"] = selected_id

            action_result = self.perform_action(action_name, resolved_payload)
            if str(action_result.get("status") or "") == "pending_clarification":
                return action_result

            final_answer = str(action_result.get("message") or "").strip() or "Workflow action completed."
            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=final_answer,
                source_type="clarification_resolution",
                answer_source="workflow_action",
                ambiguity_flag=False,
                linked_object_ids=[selected_id] if pending_field == "assignee" and selected_id > 0 else [],
            )
            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": final_answer,
                "answer_source": "workflow_action",
                "grounding": action_result,
            }

        if str(thread_metadata.get("trigger") or "") == "workflow_action_create_workflow_case_reference":
            action_name = str(thread_metadata.get("action_name") or "create_workflow_case")
            action_meta = thread_metadata.get("create_workflow_case_action_resolution") if isinstance(thread_metadata.get("create_workflow_case_action_resolution"), dict) else {}
            action_payload = dict(action_meta.get("payload") or {}) if isinstance(action_meta.get("payload"), dict) else {}
            pending_field = str(action_meta.get("pending_field") or "subject").strip().lower()
            candidate_snapshot = list(thread.get("candidate_snapshot") or [])
            selected_label = self._match_user_response_to_candidate(candidate_snapshot, response_text)
            selected_entry = self._find_candidate_snapshot_entry(candidate_snapshot, selected_label) if selected_label else None

            selected_id = 0
            if isinstance(selected_entry, dict):
                selected_id = int(selected_entry.get("object_id") or selected_entry.get("entity_id") or 0)

            if selected_id <= 0:
                resolution = self._resolve_merge_entity_reference(response_text)
                resolved_status = str(resolution.get("status") or "")
                if resolved_status != "resolved":
                    clarification_question = (
                        f"I found multiple {pending_field}s matching '{str(resolution.get('input') or response_text).strip()}'. Please choose the exact {pending_field} candidate."
                        if resolved_status == "ambiguous"
                        else f"I could not resolve {pending_field} '{str(resolution.get('input') or response_text).strip()}'. Please provide the exact id or name."
                    )
                    self._update_clarification_prompt(
                        int(thread_id),
                        reason_code="ambiguous_subject" if pending_field == "subject" and resolved_status == "ambiguous" else (
                            "subject_not_found" if pending_field == "subject" else (
                                "ambiguous_initiator" if resolved_status == "ambiguous" else "initiator_not_found"
                            )
                        ),
                        clarification_question=clarification_question,
                        expected_input_type="entity_scope",
                        ambiguity_summary={
                            "action": action_name,
                            "pending_field": pending_field,
                        },
                        candidate_snapshot=list(resolution.get("candidates") or []),
                        provenance_snapshot=[],
                        metadata={
                            "trigger": "workflow_action_create_workflow_case_reference",
                            "action_name": action_name,
                            "create_workflow_case_action_resolution": {
                                "payload": action_payload,
                                "pending_field": pending_field,
                            },
                        },
                    )
                    self._append_clarification_turn(
                        thread_id=int(thread_id),
                        role="assistant",
                        message_text=clarification_question,
                        source_type="clarification_prompt",
                        answer_source="pending_clarification",
                        ambiguity_flag=True,
                        ambiguity_payload={"action": action_name, "pending_field": pending_field},
                        candidate_snapshot=list(resolution.get("candidates") or []),
                    )
                    return {
                        "status": "pending_clarification",
                        "thread_id": int(thread_id),
                        "clarification_question": clarification_question,
                        "reason_code": "ambiguous_subject" if pending_field == "subject" and resolved_status == "ambiguous" else (
                            "subject_not_found" if pending_field == "subject" else (
                                "ambiguous_initiator" if resolved_status == "ambiguous" else "initiator_not_found"
                            )
                        ),
                        "expected_input_type": "entity_scope",
                        "candidates": list(resolution.get("candidates") or []),
                        "provenance_snapshot": [],
                        "grounding": {"trigger": "workflow_action_create_workflow_case_reference"},
                    }
                selected_id = int(resolution.get("object_id") or resolution.get("entity_id") or 0)

            resolved_payload = dict(action_payload)
            if pending_field == "subject":
                resolved_payload["subject_ref"] = selected_id
            else:
                resolved_payload["initiated_by_ref"] = selected_id

            action_result = self.perform_action(action_name, resolved_payload)
            if str(action_result.get("status") or "") == "pending_clarification":
                return action_result

            final_answer = str(action_result.get("message") or "").strip() or "Workflow action completed."
            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=final_answer,
                source_type="clarification_resolution",
                answer_source="workflow_action",
                ambiguity_flag=False,
            )
            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": final_answer,
                "answer_source": "workflow_action",
                "grounding": action_result,
            }

        if str(thread_metadata.get("trigger") or "") == "workflow_action_record_approval_reference":
            action_name = str(thread_metadata.get("action_name") or "record_approval")
            action_meta = thread_metadata.get("record_approval_action_resolution") if isinstance(thread_metadata.get("record_approval_action_resolution"), dict) else {}
            action_payload = dict(action_meta.get("payload") or {}) if isinstance(action_meta.get("payload"), dict) else {}
            pending_field = str(action_meta.get("pending_field") or "case").strip().lower()
            candidate_snapshot = list(thread.get("candidate_snapshot") or [])
            selected_label = self._match_user_response_to_candidate(candidate_snapshot, response_text)
            selected_entry = self._find_candidate_snapshot_entry(candidate_snapshot, selected_label) if selected_label else None

            selected_id = 0
            if isinstance(selected_entry, dict):
                selected_id = int(selected_entry.get("case_ref") or selected_entry.get("object_id") or selected_entry.get("entity_id") or 0)

            if selected_id <= 0:
                if pending_field == "case":
                    resolution = self._resolve_workflow_case_reference(response_text)
                else:
                    resolution = self._resolve_merge_entity_reference(response_text)
                resolved_status = str(resolution.get("status") or "")
                if resolved_status != "resolved":
                    clarification_question = (
                        f"I found multiple workflow cases matching '{str(resolution.get('input') or response_text).strip()}'. Please choose the exact case candidate."
                        if pending_field == "case" and resolved_status == "ambiguous"
                        else (
                            f"I found multiple approvers matching '{str(resolution.get('input') or response_text).strip()}'. Please choose the exact approver candidate."
                            if pending_field == "approver" and resolved_status == "ambiguous"
                            else (
                                f"I could not resolve workflow case '{str(resolution.get('input') or response_text).strip()}'. Please provide the exact case id or case no."
                                if pending_field == "case"
                                else f"I could not resolve approver '{str(resolution.get('input') or response_text).strip()}'. Please provide the exact approver id or name."
                            )
                        )
                    )
                    self._update_clarification_prompt(
                        int(thread_id),
                        reason_code=("ambiguous_case" if pending_field == "case" and resolved_status == "ambiguous" else ("case_not_found" if pending_field == "case" else ("ambiguous_approver" if resolved_status == "ambiguous" else "approver_not_found"))),
                        clarification_question=clarification_question,
                        expected_input_type="entity_scope",
                        ambiguity_summary={
                            "action": action_name,
                            "pending_field": pending_field,
                        },
                        candidate_snapshot=list(resolution.get("candidates") or []),
                        provenance_snapshot=[],
                        metadata={
                            "trigger": "workflow_action_record_approval_reference",
                            "action_name": action_name,
                            "record_approval_action_resolution": {
                                "payload": action_payload,
                                "pending_field": pending_field,
                            },
                        },
                    )
                    self._append_clarification_turn(
                        thread_id=int(thread_id),
                        role="assistant",
                        message_text=clarification_question,
                        source_type="clarification_prompt",
                        answer_source="pending_clarification",
                        ambiguity_flag=True,
                        ambiguity_payload={"action": action_name, "pending_field": pending_field},
                        candidate_snapshot=list(resolution.get("candidates") or []),
                    )
                    return {
                        "status": "pending_clarification",
                        "thread_id": int(thread_id),
                        "clarification_question": clarification_question,
                        "reason_code": ("ambiguous_case" if pending_field == "case" and resolved_status == "ambiguous" else ("case_not_found" if pending_field == "case" else ("ambiguous_approver" if resolved_status == "ambiguous" else "approver_not_found"))),
                        "expected_input_type": "entity_scope",
                        "candidates": list(resolution.get("candidates") or []),
                        "provenance_snapshot": [],
                        "grounding": {"trigger": "workflow_action_record_approval_reference"},
                    }
                if pending_field == "case":
                    selected_id = int(resolution.get("case_ref") or 0)
                else:
                    selected_id = int(resolution.get("object_id") or 0)

            resolved_payload = dict(action_payload)
            if pending_field == "case":
                resolved_payload["case_ref"] = selected_id
            else:
                resolved_payload["approver_ref"] = selected_id

            action_result = self.perform_action(action_name, resolved_payload)
            if str(action_result.get("status") or "") == "pending_clarification":
                return action_result

            final_answer = str(action_result.get("message") or "").strip() or "Workflow action completed."
            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=final_answer,
                source_type="clarification_resolution",
                answer_source="workflow_action",
                ambiguity_flag=False,
            )
            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": final_answer,
                "answer_source": "workflow_action",
                "grounding": action_result,
            }

        original_query = str(thread.get("original_query") or "").strip()
        candidate_snapshot = list(thread.get("candidate_snapshot") or [])
        selected_candidate = self._match_user_response_to_candidate(candidate_snapshot, response_text)
        if selected_candidate:
            refined_question = (
                f"{original_query}\n"
                f"Clarification from user: selected candidate is '{selected_candidate}'. "
                f"Use exactly this entity and ignore other ambiguous candidates."
            ).strip()
        else:
            refined_question = f"{original_query}\nClarification from user: {response_text}".strip()
        query_result = self.autonomous_query(refined_question, max_steps=3, record_history=False)

        signal = self._extract_clarification_signal(refined_question, query_result)
        if signal and selected_candidate and str(signal.get("reason_code") or "") == "ambiguous_entity":
            # Consolidate after one clarification pass: avoid issuing a second
            # autonomous query that can create repeated-question loops.
            merged_candidates = self._normalize_candidate_snapshot(
                list(signal.get("candidates") or []) + list(candidate_snapshot or []),
                limit=8,
            )
            resolved_answer = str(query_result.get("answer") or "").strip()
            if not resolved_answer:
                resolved_answer = (
                    "[source: clarification_consolidated] "
                    "I applied your selected candidate, but remaining ambiguity still exists in retrieval evidence. "
                    "Returning the best consolidated result from this clarification turn."
                )
            else:
                resolved_answer = f"[source: clarification_consolidated] {resolved_answer}".strip()

            if merged_candidates:
                preview = ", ".join(
                    str(item.get("name") or item.get("value") or item.get("entity_name") or "").strip()
                    for item in merged_candidates
                    if isinstance(item, dict)
                )
                if preview:
                    resolved_answer = (
                        f"{resolved_answer}\n\n"
                        f"Consolidated candidates considered: {preview}."
                    )

            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=resolved_answer,
                source_type="clarification_resolution",
                answer_source="clarification_consolidated",
                confidence=None,
                ambiguity_flag=False,
                ambiguity_payload={
                    "reason_code": str(signal.get("reason_code") or "ambiguous_entity"),
                    "selected_candidate": selected_candidate,
                },
                candidate_snapshot=merged_candidates,
            )

            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": resolved_answer,
                "answer_source": "clarification_consolidated",
                "grounding": {
                    "query_result": query_result,
                    "ambiguity_summary": signal.get("ambiguity_summary") if isinstance(signal.get("ambiguity_summary"), dict) else {},
                    "selected_candidate": selected_candidate,
                    "candidates": merged_candidates,
                },
            }

        if signal:
            merged_candidates = self._normalize_candidate_snapshot(
                list(signal.get("candidates") or []) + list(candidate_snapshot or []),
                limit=8,
            )
            resolved_answer = str(query_result.get("answer") or "").strip()

            if not resolved_answer:
                if selected_candidate:
                    resolved_answer = (
                        "[source: clarification_consolidated] "
                        "I applied your selected candidate and completed this request with the best consolidated result available."
                    )
                else:
                    resolved_answer = (
                        "[source: clarification_consolidated] "
                        "No explicit candidate selection was detected, so I consolidated the current clarification response "
                        "and completed this request to avoid repeated question loops."
                    )
            else:
                resolved_answer = f"[source: clarification_consolidated] {resolved_answer}".strip()

            if merged_candidates:
                preview = ", ".join(
                    str(item.get("name") or item.get("value") or item.get("entity_name") or "").strip()
                    for item in merged_candidates
                    if isinstance(item, dict)
                )
                if preview:
                    resolved_answer = (
                        f"{resolved_answer}\n\n"
                        f"Consolidated candidates considered: {preview}."
                    )

            self._set_clarification_thread_status(int(thread_id), "resolved")
            self._append_clarification_turn(
                thread_id=int(thread_id),
                role="assistant",
                message_text=resolved_answer,
                source_type="clarification_resolution",
                answer_source="clarification_consolidated",
                confidence=None,
                ambiguity_flag=False,
                ambiguity_payload={
                    "reason_code": str(signal.get("reason_code") or "ambiguous_value"),
                    "selected_candidate": selected_candidate,
                    "auto_consolidated": True,
                },
                candidate_snapshot=merged_candidates,
                provenance_snapshot=list(signal.get("provenance_snapshot") or []),
            )
            return {
                "status": "resolved",
                "thread_id": int(thread_id),
                "answer": resolved_answer,
                "answer_source": "clarification_consolidated",
                "grounding": {
                    "query_result": query_result,
                    "ambiguity_summary": signal.get("ambiguity_summary") if isinstance(signal.get("ambiguity_summary"), dict) else {},
                    "selected_candidate": selected_candidate,
                    "candidates": merged_candidates,
                    "auto_consolidated": True,
                },
            }

        final_answer = str(query_result.get("answer") or "").strip()
        self._set_clarification_thread_status(int(thread_id), "resolved")
        self._append_clarification_turn(
            thread_id=int(thread_id),
            role="assistant",
            message_text=final_answer,
            source_type="clarification_resolution",
            answer_source=str(query_result.get("answer_source") or "resolved"),
            confidence=None,
            ambiguity_flag=False,
            ambiguity_payload={},
            candidate_snapshot=[],
        )

        return {
            "status": "resolved",
            "thread_id": int(thread_id),
            "answer": final_answer,
            "answer_source": str(query_result.get("answer_source") or "resolved"),
            "grounding": query_result,
        }

    def suggest_similar_entities(
        self,
        entity_name: str,
        class_name: str = "person",
        limit: int = 8,
    ) -> dict[str, Any]:
        name = str(entity_name or "").strip()
        klass = str(class_name or "person").strip().lower() or "person"
        if not name:
            raise ValueError("entity_name must not be empty")

        with self.get_connection() as conn:
            suggestions = object_db.suggest_similar_objects(
                connection=conn,
                object_name=name,
                class_name=klass,
                limit=max(1, int(limit)),
            )

        message = (
            "No similar active entities were found."
            if not suggestions
            else "Similar names were found. Consider alias linking first, then merge only after user confirmation."
        )
        return {
            "entity_name": name,
            "class_name": klass,
            "candidates": suggestions,
            "candidate_count": len(suggestions),
            "message": message,
            "recommended_actions": ["confirm_same_person", "link_alias", "merge_records"],
        }

    def link_entity_alias(
        self,
        primary_object_id: int,
        alias_object_id: int,
        alias_type: str = "same_entity",
        confidence: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.get_connection() as conn:
            row = object_db.link_object_alias(
                connection=conn,
                primary_object_id=int(primary_object_id),
                alias_object_id=int(alias_object_id),
                alias_type=str(alias_type or "same_entity"),
                confidence=confidence,
                metadata=metadata if isinstance(metadata, dict) else {},
                commit=True,
            )
        return {
            "status": "linked",
            "alias": row,
            "message": "Alias link saved. Query resolution will consider the linked entity group.",
        }

    def merge_entity_instances(
        self,
        canonical_object_id: int,
        duplicate_object_id: int,
        merge_note: str = "",
        keep_alias_link: bool = True,
    ) -> dict[str, Any]:
        with self.get_connection() as conn:
            summary = object_db.merge_object_instances(
                connection=conn,
                canonical_object_id=int(canonical_object_id),
                duplicate_object_id=int(duplicate_object_id),
                merge_note=str(merge_note or "").strip(),
                keep_alias_link=bool(keep_alias_link),
            )
        return {
            "status": "merged",
            "summary": summary,
            "message": "Records merged. Relationships, attributes, and parts were retargeted to the canonical object.",
        }

    def _log_action_execution(self, action_name: str, payload: dict[str, Any], result: dict[str, Any]) -> None:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO workflow_action_log (action_name, payload, success, message, result_data)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            str(action_name or "").strip().lower(),
                            Json(payload or {}),
                            bool(result.get("success")),
                            str(result.get("message") or ""),
                            Json(result or {}),
                        ),
                    )
        except Exception:
            pass

    def _wire_default_event_handlers(self) -> None:
        self.event_bus.subscribe("task.overdue", lambda e: handle_task_overdue(e, self.notifier))
        self.event_bus.subscribe("sla.breach", lambda e: handle_sla_breach(e, self.notifier))
        self.event_bus.subscribe("resource.conflict_detected", lambda e: handle_resource_conflict(e, self.notifier))
        self.transition_executor.register_default_rules()

    def _build_solf_policy_interpreter(self) -> Any:
        try:
            import solf_parser
            from solf_interpreter import SOLFInterpreter
        except Exception:
            return None

        script_path = Path(__file__).with_name("solf_script.txt")
        if not script_path.exists():
            return None

        try:
            solf_script = script_path.read_text(encoding="utf-8")
            interpreter = SOLFInterpreter()
            parser_adapter = type("Parser", (object,), {"parse": staticmethod(solf_parser.parse_script)})()
            interpreter.set_parser(parser_adapter)
            interpreter.set_debug(False)
            # Keep first policy load fast: load bundled script synchronously,
            # then hydrate runtime business-rule scripts asynchronously.
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                base_script = str(solf_script or "").strip()
                if base_script:
                    interpreter.load_program_script(base_script, clear_existing=True)
            self.logger.info("Loaded SOLF policy script from %s", script_path)
            return interpreter
        except Exception:
            self.logger.exception("Failed to load SOLF policy interpreter")
            return None

    def _default_policy(self, reason: str = "default_allow") -> dict[str, Any]:
        return {
            "mode": "allow",
            "reason": reason,
            "policy_clause": None,
        }

    def _attach_solf_interpreter(self, interpreter: Any) -> None:
        self.solf_interpreter = interpreter
        if hasattr(self, "query_engine") and self.query_engine is not None:
            def _infer_category_source(payload: dict[str, Any]) -> Any:
                hint = str(payload.get("source_hint") or "").strip().lower()
                clause_name = {
                    "document": "resolve_category_document",
                    "external": "resolve_category_external",
                }.get(hint, "resolve_category_internal")
                return interpreter._invoke_clause(clause_name, [payload])

            self.query_engine.solf_category_inference = _infer_category_source
        if hasattr(self, "event_bus") and self.event_bus is not None:
            self.event_bus.solf_interpreter = interpreter
        if hasattr(self, "action_tool") and self.action_tool is not None:
            self.action_tool.solf_interpreter = interpreter
        if hasattr(self, "transition_executor") and self.transition_executor is not None:
            self.transition_executor.solf_interpreter = interpreter
        self._start_solf_runtime_rule_hydration()

    def _start_solf_runtime_rule_hydration(self) -> None:
        if self.solf_interpreter is None or self._solf_runtime_rules_loaded:
            return

        with self._solf_runtime_lock:
            if self.solf_interpreter is None or self._solf_runtime_rules_loaded or self._solf_runtime_rules_in_progress:
                return
            self._solf_runtime_rules_in_progress = True

        def _hydrate() -> None:
            try:
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    runtime_pre_scripts, runtime_post_scripts = business_rules.load_active_business_rule_solf_script_chunks(limit=300)
                    runtime_script = "\n\n".join(
                        chunk
                        for chunk in [*list(runtime_pre_scripts or []), *list(runtime_post_scripts or [])]
                        if str(chunk or "").strip()
                    )
                    if runtime_script and self.solf_interpreter is not None:
                        self.solf_interpreter.load_program_script(runtime_script, clear_existing=False)
                self._solf_runtime_rules_loaded = True
                self.logger.info("Hydrated runtime SOLF business-rule scripts")
            except Exception:
                self.logger.exception("Failed to hydrate runtime SOLF business-rule scripts")
            finally:
                self._solf_runtime_rules_in_progress = False

        threading.Thread(target=_hydrate, name="solf_runtime_hydrator", daemon=True).start()

    def _solf_loader_worker(self) -> None:
        interpreter: Any = None
        build_error: str | None = None
        try:
            interpreter = self._build_solf_policy_interpreter()
        except Exception as exc:
            build_error = str(exc)

        with self._solf_init_lock:
            self._solf_load_in_progress = False
            self._solf_loader_thread = None
            if interpreter is None:
                self._solf_last_error = build_error or "build_returned_none"
            else:
                self._solf_last_error = None
                self._attach_solf_interpreter(interpreter)
                self.logger.info("SOLF policy interpreter loaded lazily")
            self._solf_load_done_event.set()

    def _ensure_solf_interpreter_loaded(self, force_retry: bool = False, wait_for_load: bool = True) -> bool:
        solf_interpreter = getattr(self, "solf_interpreter", None)
        if solf_interpreter is not None:
            return True

        if not hasattr(self, "_solf_init_lock"):
            self._solf_init_lock = threading.Lock()
        if not hasattr(self, "_solf_load_attempted"):
            self._solf_load_attempted = False
        if not hasattr(self, "_solf_last_attempt_ts"):
            self._solf_last_attempt_ts = 0.0
        if not hasattr(self, "_solf_retry_cooldown_seconds"):
            self._solf_retry_cooldown_seconds = 30
        if not hasattr(self, "_solf_load_timeout_seconds"):
            self._solf_load_timeout_seconds = 8
        if not hasattr(self, "_solf_load_in_progress"):
            self._solf_load_in_progress = False
        if not hasattr(self, "_solf_load_done_event"):
            self._solf_load_done_event = threading.Event()
        if not hasattr(self, "_solf_loader_thread"):
            self._solf_loader_thread = None
        if not hasattr(self, "_solf_last_error"):
            self._solf_last_error = None
        if not hasattr(self, "_solf_runtime_rules_loaded"):
            self._solf_runtime_rules_loaded = False
        if not hasattr(self, "_solf_runtime_rules_in_progress"):
            self._solf_runtime_rules_in_progress = False
        if not hasattr(self, "_solf_runtime_lock"):
            self._solf_runtime_lock = threading.Lock()

        if getattr(self, "_solf_load_in_progress", False):
            if wait_for_load:
                self._solf_load_done_event.wait(timeout=float(self._solf_load_timeout_seconds))
                return getattr(self, "solf_interpreter", None) is not None
            return False

        now_ts = time.time()
        if (
            not force_retry
            and getattr(self, "_solf_load_attempted", False)
            and self._solf_retry_cooldown_seconds > 0
            and (now_ts - self._solf_last_attempt_ts) < self._solf_retry_cooldown_seconds
        ):
            return False

        with self._solf_init_lock:
            solf_interpreter = getattr(self, "solf_interpreter", None)
            if solf_interpreter is not None:
                return True
            now_ts = time.time()
            if (
                not force_retry
                and getattr(self, "_solf_load_attempted", False)
                and self._solf_retry_cooldown_seconds > 0
                and (now_ts - self._solf_last_attempt_ts) < self._solf_retry_cooldown_seconds
            ):
                return False

            self._solf_load_attempted = True
            self._solf_last_attempt_ts = now_ts
            try:
                self._solf_load_in_progress = True
                self._solf_load_done_event.clear()
                self._solf_loader_thread = threading.Thread(
                    target=self._solf_loader_worker,
                    name="solf_loader",
                    daemon=True,
                )
                self._solf_loader_thread.start()
            except Exception as exc:
                self._solf_last_error = str(exc)
                self._solf_load_in_progress = False
                self._solf_loader_thread = None
                return False

        if not wait_for_load:
            return False
        self._solf_load_done_event.wait(timeout=float(self._solf_load_timeout_seconds))
        return getattr(self, "solf_interpreter", None) is not None

    def _invoke_solf_policy(self, clause_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        solf_interpreter = getattr(self, "solf_interpreter", None)
        if solf_interpreter is None:
            self._ensure_solf_interpreter_loaded(wait_for_load=False)
            solf_interpreter = getattr(self, "solf_interpreter", None)
        if solf_interpreter is None and not self._ensure_solf_interpreter_loaded(wait_for_load=True):
            return self._default_policy("solf_unavailable")

        try:
            self.logger.info("Invoking SOLF policy clause=%s", clause_name)
            result = solf_interpreter._invoke_clause(clause_name, [payload])
        except Exception:
            self.logger.exception("SOLF policy clause invocation failed: %s", clause_name)
            return self._default_policy("solf_invocation_failed")

        if not isinstance(result, dict):
            return self._default_policy("missing_or_non_dict_policy")

        mode = str(result.get("mode") or "allow").strip().lower()
        if mode not in {"allow", "deny", "escalate"}:
            mode = "allow"

        normalized = dict(result)
        normalized["mode"] = mode
        normalized["policy_clause"] = clause_name
        return normalized

    def _merge_workflow_payload(self, base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base or {})
        for key, value in (patch or {}).items():
            existing = merged.get(key)
            if isinstance(existing, dict) and isinstance(value, dict):
                merged[key] = self._merge_workflow_payload(existing, value)
            else:
                merged[key] = value
        return merged

    def _looks_like_file_query(self, question: str, parsed_payload: dict[str, Any] | None = None) -> bool:
        text = str(question or "")
        lowered = text.lower()
        if re.search(r"\b(?:file|filename|path|full\s+path|document\s+path|document\s+file)\b", lowered):
            return True
        parsed = parsed_payload if isinstance(parsed_payload, dict) else {}
        attr = str(parsed.get("attribute_name") or "").strip().lower()
        return attr in {"document_filename", "document_full_path", "document_file_info"}

    def _apply_workflow_rule_directives(
        self,
        *,
        stage: str,
        policy: dict[str, Any] | None,
        question: str | None = None,
        parsed_payload: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        policy_dict = policy if isinstance(policy, dict) else {}
        directives_raw = policy_dict.get("workflow_directives")
        if not isinstance(directives_raw, dict):
            directives_raw = policy_dict.get("directives") if isinstance(policy_dict.get("directives"), dict) else {}
        directives = dict(directives_raw or {})

        out: dict[str, Any] = {
            "parsed_payload": dict(parsed_payload or {}) if isinstance(parsed_payload, dict) else None,
            "payload": dict(payload or {}) if isinstance(payload, dict) else None,
            "applied": False,
            "applied_rules": [],
        }

        if not directives:
            return out

        if out["payload"] is not None:
            payload_set = directives.get("set_payload") if isinstance(directives.get("set_payload"), dict) else {}
            if payload_set:
                out["payload"] = dict(payload_set)
                out["applied"] = True
                out["applied_rules"].append(f"{stage}:set_payload")

            payload_merge = directives.get("merge_payload") if isinstance(directives.get("merge_payload"), dict) else {}
            if payload_merge:
                out["payload"] = self._merge_workflow_payload(out["payload"], payload_merge)
                out["applied"] = True
                out["applied_rules"].append(f"{stage}:merge_payload")

        if out["parsed_payload"] is not None:
            parsed_set = directives.get("set_parsed") if isinstance(directives.get("set_parsed"), dict) else {}
            if parsed_set:
                out["parsed_payload"] = self._merge_workflow_payload(out["parsed_payload"], parsed_set)
                out["applied"] = True
                out["applied_rules"].append(f"{stage}:set_parsed")

            if str(directives.get("set_intent") or "").strip():
                out["parsed_payload"]["intent"] = str(directives.get("set_intent") or "").strip()
                out["applied"] = True
                out["applied_rules"].append(f"{stage}:set_intent")

            if str(directives.get("set_entity_name") or "").strip():
                out["parsed_payload"]["entity_name"] = str(directives.get("set_entity_name") or "").strip()
                out["applied"] = True
                out["applied_rules"].append(f"{stage}:set_entity_name")

            if str(directives.get("set_attribute_name") or "").strip():
                out["parsed_payload"]["attribute_name"] = str(directives.get("set_attribute_name") or "").strip()
                out["applied"] = True
                out["applied_rules"].append(f"{stage}:set_attribute_name")

            if str(directives.get("set_relation_name") or "").strip():
                out["parsed_payload"]["relation_name"] = str(directives.get("set_relation_name") or "").strip()
                out["applied"] = True
                out["applied_rules"].append(f"{stage}:set_relation_name")

            if isinstance(directives.get("set_attribute_names"), list):
                out["parsed_payload"]["attribute_names"] = [
                    str(item).strip()
                    for item in list(directives.get("set_attribute_names") or [])
                    if str(item).strip()
                ]
                out["applied"] = True
                out["applied_rules"].append(f"{stage}:set_attribute_names")

            merge_criteria = directives.get("merge_criteria") if isinstance(directives.get("merge_criteria"), dict) else {}
            if merge_criteria:
                existing_criteria = out["parsed_payload"].get("criteria") if isinstance(out["parsed_payload"].get("criteria"), dict) else {}
                out["parsed_payload"]["criteria"] = self._merge_workflow_payload(existing_criteria, merge_criteria)
                out["applied"] = True
                out["applied_rules"].append(f"{stage}:merge_criteria")

            require_full_path = bool(directives.get("require_full_path_for_file_queries"))
            if require_full_path and self._looks_like_file_query(str(question or ""), out["parsed_payload"]):
                out["parsed_payload"]["attribute_name"] = "document_file_info"
                criteria = out["parsed_payload"].get("criteria") if isinstance(out["parsed_payload"].get("criteria"), dict) else {}
                criteria = dict(criteria)
                criteria["require_full_path"] = True
                criteria["parse_method"] = str(criteria.get("parse_method") or "workflow_rule_directive")
                out["parsed_payload"]["criteria"] = criteria
                out["applied"] = True
                out["applied_rules"].append(f"{stage}:require_full_path_for_file_queries")

        return out

    def _harden_transition_policy(self, payload: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
        entity_type = str(payload.get("entity_type") or "").strip().lower()
        to_state = str(payload.get("to_state") or "").strip().lower()
        event = str(payload.get("event") or "").strip().lower()
        actor_ref = str(payload.get("actor_ref") or "").strip().lower()

        # Enforce terminal workflow-case transitions only via approval events.
        if entity_type == "workflow_case" and to_state in {"approved", "rejected"} and event != "approval_recorded":
            return {
                "mode": "deny",
                "reason": "workflow_case_terminal_state_requires_approval_event",
                "allowed": False,
                "message": "workflow_case can move to approved/rejected only from approval_recorded event",
                "policy_clause": "workflow_transition_policy",
            }

        # Require explicit completion event for project tasks to become done.
        if entity_type == "project_task" and to_state == "done" and event != "task_completed":
            return {
                "mode": "deny",
                "reason": "project_task_done_requires_completion_event",
                "allowed": False,
                "message": "project_task can move to done only with task_completed event",
                "policy_clause": "workflow_transition_policy",
            }

        # Mark compliance escalations as escalate mode for downstream handling.
        if entity_type == "compliance_action" and to_state == "escalated" and event == "compliance_deadline_breach":
            hardened = dict(policy)
            hardened["mode"] = "escalate"
            hardened["reason"] = str(hardened.get("reason") or "compliance_deadline_breach_escalation")
            hardened["allowed"] = True
            return hardened

        # Prevent system/manual actor from forcing terminal workflow-case states.
        if entity_type == "workflow_case" and to_state in {"approved", "rejected"} and actor_ref == "system":
            return {
                "mode": "deny",
                "reason": "manual_terminal_transition_not_allowed",
                "allowed": False,
                "message": "manual transition to terminal workflow_case state is not allowed",
                "policy_clause": "workflow_transition_policy",
            }

        return policy

    def _invoke_solf_clause_raw(self, clause_name: str, call_args: list[Any]) -> Any:
        if self.solf_interpreter is None:
            self._ensure_solf_interpreter_loaded(wait_for_load=False)
        if self.solf_interpreter is None and not self._ensure_solf_interpreter_loaded(wait_for_load=True):
            return None
        try:
            return self.solf_interpreter._invoke_clause(clause_name, call_args)
        except Exception:
            return None

    def evaluate_solf_clause(
        self,
        clause_name: str,
        payload: dict[str, Any] | None = None,
        args: list[Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a specific SOLF clause/predicate and return raw evaluation result.

        This is an explicit interaction endpoint for users who want direct predicate
        execution, distinct from policy and workflow internal calls.
        """
        raw_clause_name = str(clause_name or "").strip()
        if not raw_clause_name:
            return {
                "success": False,
                "message": "clause_name is required",
                "clause_name": "",
                "args": [],
                "result": None,
            }

        try:
            safe_clause = self._sanitize_identifier(raw_clause_name)
        except Exception:
            return {
                "success": False,
                "message": f"Unsafe SOLF clause name: {raw_clause_name}",
                "clause_name": raw_clause_name,
                "args": [],
                "result": None,
            }

        if not self._ensure_solf_interpreter_loaded():
            return {
                "success": False,
                "message": "SOLF interpreter unavailable",
                "clause_name": safe_clause,
                "args": [],
                "result": None,
            }

        call_args: list[Any]
        if isinstance(args, list) and args:
            call_args = list(args)
        else:
            call_args = [payload if isinstance(payload, dict) else {}]

        try:
            result = self.solf_interpreter._invoke_clause(safe_clause, call_args)
        except Exception as exc:
            self.logger.exception("SOLF clause execution failed: %s", safe_clause)
            return {
                "success": False,
                "message": f"SOLF clause execution failed: {exc}",
                "clause_name": safe_clause,
                "args": call_args,
                "result": None,
            }

        return {
            "success": True,
            "message": "SOLF clause executed",
            "clause_name": safe_clause,
            "args": call_args,
            "result": result,
        }

    def _as_string_key_dict(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        return {str(k): v for k, v in value.items()}

    def _resolve_solf_placeholders(self, value: Any, payload: dict[str, Any]) -> Any:
        if isinstance(value, dict):
            return {str(k): self._resolve_solf_placeholders(v, payload) for k, v in value.items()}
        if isinstance(value, list):
            return [self._resolve_solf_placeholders(v, payload) for v in value]
        if isinstance(value, str) and value.startswith("_") and len(value) > 1:
            key = value[1:]
            if key in payload:
                return payload.get(key)
        return value

    def _sanitize_identifier(self, value: str) -> str:
        identifier = str(value or "").strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError(f"Unsafe SQL identifier: {identifier}")
        return identifier

    def _normalize_solf_identifier(self, value: str) -> str:
        """Normalize dynamic SOLF key forms like ['field'] into field."""
        text = str(value or "").strip()
        if text.startswith("['") and text.endswith("']") and len(text) > 4:
            return text[2:-2]
        if text.startswith('["') and text.endswith('"]') and len(text) > 4:
            return text[2:-2]
        if text.startswith("[") and text.endswith("]") and len(text) > 2:
            inner = text[1:-1].strip().strip("'\"")
            return inner
        return text

    def _execute_sql_plan(self, sql_plan: dict[str, Any]) -> dict[str, Any]:
        plan = self._as_string_key_dict(sql_plan)
        operation = str(plan.get("operation") or "").strip().lower()
        table = self._sanitize_identifier(str(plan.get("table") or "").strip())

        allowed_tables = {
            "projects",
            "project_tasks",
            "project_milestones",
            "project_members",
            "crm_tasks",
            "invoices",
            "workflow_cases",
            "approvals",
            "compliance_actions",
            "person",
            "organization",
            "company",
            "document",
            "invoice",
            "payment",
            "project",
            "project_task",
            "project_milestone",
            "project_risk",
            "workflow_case",
            "approval",
            "compliance_action",
            "employee_profile",
            "employment_contract",
            "party",
            "customer",
            "supplier",
            "product",
            "service",
            "warehouse",
            "inventory_item",
            "vat_return",
            "tax_declaration",
            "social_contribution_report",
        }
        if table not in allowed_tables:
            return {"success": False, "message": f"Table not allowed: {table}"}

        where = {
            self._normalize_solf_identifier(str(k)): v
            for k, v in self._as_string_key_dict(plan.get("where")).items()
        }
        values = {
            self._normalize_solf_identifier(str(k)): v
            for k, v in self._as_string_key_dict(plan.get("values")).items()
        }
        returning = plan.get("returning") if isinstance(plan.get("returning"), list) else []
        returning_cols = [self._sanitize_identifier(str(c)) for c in returning]

        if operation == "insert":
            if not values:
                return {"success": False, "message": "Insert plan missing values."}
            cols = [self._sanitize_identifier(k) for k in values.keys()]
            params = list(values.values())
            placeholders = ", ".join(["%s"] * len(cols))
            sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
            if returning_cols:
                sql += f" RETURNING {', '.join(returning_cols)}"
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, params)
                        row = cur.fetchone() if returning_cols else None
            except Exception as exc:
                return {
                    "success": False,
                    "operation": operation,
                    "table": table,
                    "message": f"SQL insert failed: {exc}",
                }
            return {
                "success": True,
                "operation": operation,
                "table": table,
                "returned": dict(zip(returning_cols, row)) if row and returning_cols else None,
            }

        if operation == "update":
            if not values:
                return {"success": False, "message": "Update plan missing values."}
            if not where:
                return {"success": False, "message": "Unsafe update without where clause."}

            set_cols = [self._sanitize_identifier(k) for k in values.keys()]
            where_cols = [self._sanitize_identifier(k) for k in where.keys()]
            set_expr = ", ".join([f"{c}=%s" for c in set_cols])
            where_expr = " AND ".join([f"{c}=%s" for c in where_cols])
            sql = f"UPDATE {table} SET {set_expr}, updated_at=NOW() WHERE {where_expr}"
            if returning_cols:
                sql += f" RETURNING {', '.join(returning_cols)}"
            params = list(values.values()) + list(where.values())
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, params)
                        row = cur.fetchone() if returning_cols else None
                        affected = cur.rowcount
            except Exception as exc:
                return {
                    "success": False,
                    "operation": operation,
                    "table": table,
                    "message": f"SQL update failed: {exc}",
                }
            return {
                "success": affected > 0,
                "operation": operation,
                "table": table,
                "affected_rows": affected,
                "returned": dict(zip(returning_cols, row)) if row and returning_cols else None,
            }

        if operation == "select":
            where_cols = [self._sanitize_identifier(k) for k in where.keys()]
            where_expr = " AND ".join([f"{c}=%s" for c in where_cols]) if where_cols else "TRUE"
            sql = f"SELECT * FROM {table} WHERE {where_expr}"
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, list(where.values()))
                        rows = cur.fetchall() or []
                        columns = [desc[0] for desc in cur.description or []]
            except Exception as exc:
                return {
                    "success": False,
                    "operation": operation,
                    "table": table,
                    "message": f"SQL select failed: {exc}",
                }
            return {
                "success": True,
                "operation": operation,
                "table": table,
                "row_count": len(rows),
                "rows": [dict(zip(columns, row)) for row in rows],
            }

        return {"success": False, "message": f"Unsupported operation in sql plan: {operation}"}

    def _compose_action_plan(self, action_name: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        action = str(action_name or "").strip().lower()
        raw = self._invoke_solf_clause_raw("action_plan", [action, payload])
        plan = self._as_string_key_dict(raw)
        return plan or None

    def _registered_interaction_capabilities(self) -> dict[str, Any]:
        script_path = Path(__file__).resolve().parent / "solf_script.txt"
        try:
            script_text = script_path.read_text(encoding="utf-8")
        except Exception:
            script_text = ""
        actions = sorted(set(re.findall(r"(?m)^action_plan\(([a-zA-Z0-9_]+),", script_text)))
        actions.extend(action for action in ("integrate_source_database", "sync_source_database_entities") if action not in actions)
        workflows = business_rules.list_solf_workflow_registry_entries(is_active=True, limit=500)
        return {
            "actions": actions,
            "workflows": [
                {
                    "workflow_key": item.get("workflow_key"),
                    "workflow_name": item.get("workflow_name"),
                    "description": item.get("description"),
                    "domain": item.get("domain"),
                }
                for item in workflows
                if str(item.get("workflow_key") or "").strip()
            ],
        }

    @staticmethod
    def _deterministic_mutation_plan(request_text: str, available_actions: set[str]) -> dict[str, Any] | None:
        text = str(request_text or "").strip()
        if "reverse_mutation" not in available_actions or not re.search(r"\b(?:reverse|reversal|undo)\b", text, re.IGNORECASE):
            return None
        reference_match = re.search(
            r"\b(?:invoice|document|booking|transaction|workflow|mutation|record)\s+(?:number|no\.?|#|reference|id|key)\s*[:#]?\s*([A-Za-z0-9][A-Za-z0-9_.:-]{3,})\b",
            text,
            re.IGNORECASE,
        )
        if not reference_match:
            reference_match = re.search(r"\b([0-9][0-9_.:-]{3,})\b", text)
        if not reference_match:
            return None
        return {
            "route_type": "action",
            "action_name": "reverse_mutation",
            "workflow_key": None,
            "payload": {"reference": str(reference_match.group(1)), "reason": text},
            "confidence": 0.98,
            "reason": "reversal command with a journal-resolvable business reference",
            "requires_confirmation": True,
        }

    def plan_nl_action_or_workflow(
        self,
        request_text: str,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Plan against registered SOLF actions and active workflows; never invent capabilities."""
        text = str(request_text or "").strip()
        if not text:
            return {"success": False, "route_type": "none", "message": "request text is required"}
        capabilities = self._registered_interaction_capabilities()
        available_actions = {str(item).strip() for item in capabilities.get("actions") or [] if str(item).strip()}
        deterministic = self._deterministic_mutation_plan(text, available_actions)
        if deterministic:
            return {"success": True, **deterministic, "capability_validated": True}

        client = getattr(self, "client", None)
        if client is None:
            return {"success": False, "route_type": "none", "message": "No registered action or workflow matched deterministically"}
        prompt = (
            "Map the user request to exactly one registered IDMS action, active workflow, query, or none.\n"
            "Return JSON only with keys: route_type, action_name, workflow_key, payload, confidence, reason, requires_confirmation.\n"
            "route_type must be action|workflow|query|none. Never invent an action_name or workflow_key.\n"
            "Use action for a concrete mutation/tool command, workflow for a multi-step registered process, query for information retrieval, and none when ambiguous.\n"
            f"Request: {json.dumps(text)}\n"
            f"Context: {json.dumps(context or {}, ensure_ascii=False)}\n"
            f"Registered capabilities: {json.dumps(capabilities, ensure_ascii=False)}"
        )
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: client.models.generate_content(model=EXTRACT_MODEL, contents=[prompt]),
            model=EXTRACT_MODEL,
            contents=[prompt],
            temperature=0.0,
            call_name="interaction_action_workflow_plan",
            complexity="simple",
        )
        planned = self._extract_json_object(str(getattr(response, "text", "") or ""))
        route_type = str(planned.get("route_type") or "none").strip().lower()
        action_name = str(planned.get("action_name") or "").strip().lower()
        workflow_key = str(planned.get("workflow_key") or "").strip()
        workflow_keys = {str(item.get("workflow_key") or "").strip() for item in capabilities.get("workflows") or []}
        if route_type == "action" and action_name not in available_actions:
            route_type = "none"
        if route_type == "workflow" and workflow_key not in workflow_keys:
            route_type = "none"
        if route_type not in {"action", "workflow", "query"}:
            return {"success": False, "route_type": "none", "message": "No validated action or workflow matched", "raw_plan": planned}
        return {
            "success": True,
            "route_type": route_type,
            "action_name": action_name or None,
            "workflow_key": workflow_key or None,
            "payload": planned.get("payload") if isinstance(planned.get("payload"), dict) else {},
            "confidence": max(0.0, min(1.0, float(planned.get("confidence") or 0.5))),
            "reason": str(planned.get("reason") or "registered capability match"),
            "requires_confirmation": route_type in {"action", "workflow"} or bool(planned.get("requires_confirmation")),
            "capability_validated": True,
        }

    def resolve_action_workflow(self, request_text: str) -> dict[str, Any]:
        """Return the canonical Action Object and registry-grounded execution plan."""
        entries = business_rules.list_solf_workflow_registry_entries(
            is_active=True,
            limit=1000,
        )

        def _load_active_version(entry: dict[str, Any]) -> dict[str, Any] | None:
            resolved = business_rules.get_solf_workflow_active_version_by_key(
                workflow_key=str(entry.get("workflow_key") or ""),
            )
            if not isinstance(resolved, dict):
                return None
            active_version = resolved.get("active_version")
            if not isinstance(active_version, dict):
                return resolved
            version = dict(active_version)
            version["workflow_version_id"] = resolved.get("workflow_version_id")
            connection = object_db.get_connection()
            try:
                version["steps"] = object_db.list_solf_workflow_steps(
                    connection,
                    workflow_version_id=int(resolved.get("workflow_version_id") or 0),
                )
            finally:
                connection.close()
            return version

        return action_workflow_resolution.resolve_action_workflow(
            request_text,
            entries,
            version_loader=_load_active_version,
        )

    def _reverse_mutation_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        journal_id = int(payload.get("journal_id") or 0)
        business_key = str(payload.get("business_key") or payload.get("reference") or "").strip()
        if journal_id <= 0 and not business_key:
            return {
                "action": "reverse_mutation",
                "success": False,
                "message": "journal_id, business_key, or reference is required",
            }

        confirmed = bool(payload.get("confirm"))
        requested_by = str(payload.get("requested_by") or "interaction:user")
        reason = str(payload.get("reason") or "interaction reversal")
        connection = object_db.get_connection()
        try:
            if journal_id > 0:
                result = mutation_undo.undo_journal_entry(
                    connection,
                    journal_id=journal_id,
                    requested_by=requested_by,
                    reason=reason,
                    dry_run=not confirmed,
                )
            else:
                result = mutation_undo.undo_by_business_key(
                    connection,
                    business_key=business_key,
                    requested_by=requested_by,
                    reason=reason,
                    dry_run=not confirmed,
                    operation_kind=str(payload.get("operation_kind") or "").strip() or None,
                    target_entity=str(payload.get("target_entity") or "").strip() or None,
                )

            matched = 1 if journal_id > 0 and result.get("ok") else int(result.get("matched") or 0)
            if matched == 0:
                return {
                    "action": "reverse_mutation",
                    "success": False,
                    "message": f"No reversible mutation matched {journal_id or business_key}",
                    "data": result,
                }
            if not confirmed:
                return {
                    "action": "reverse_mutation",
                    "success": False,
                    "requires_confirmation": True,
                    "confirmation_prompt": f"Execute {matched} recorded inverse operation(s)?",
                    "data": result,
                }
            return {
                "action": "reverse_mutation",
                "success": bool(result.get("ok")),
                "message": "Mutation reversed." if result.get("ok") else "Reversal failed.",
                "data": result,
            }
        finally:
            connection.close()

    def execute_nl_action_or_workflow_plan(
        self,
        plan: dict[str, Any],
        *,
        confirm: bool = False,
        requested_by: str = "interaction:user",
    ) -> dict[str, Any]:
        if not isinstance(plan, dict) or not plan.get("capability_validated"):
            return {"success": False, "message": "Plan was not validated against registered capabilities"}
        route_type = str(plan.get("route_type") or "").strip().lower()
        payload = dict(plan.get("payload") or {}) if isinstance(plan.get("payload"), dict) else {}
        payload["confirm"] = bool(confirm)
        payload.setdefault("requested_by", requested_by)
        if route_type == "action":
            return self.perform_action(str(plan.get("action_name") or ""), payload)
        if route_type == "workflow":
            return nl_process_activation.activate_process(
                text=str(plan.get("reason") or "NL workflow activation"),
                input_context=payload,
                workflow_key=str(plan.get("workflow_key") or ""),
                started_by=requested_by,
                dry_run=not bool(confirm),
            )
        if route_type == "query":
            return self.query(str(payload.get("question") or plan.get("request_text") or ""))
        return {"success": False, "message": f"Unsupported validated route_type: {route_type}"}

    def _normalize_integrate_source_action_request(self, action_name: str, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        def _extract_db_name_from_text(text: str) -> str:
            candidate_text = str(text or "").strip()
            if not candidate_text:
                return ""

            patterns = [
                r"integrate\s+(?:the\s+)?database\s+([a-zA-Z0-9_\-]+)\s+into\s+(?:the\s+)?system",
                r"integrate\s+(?:the\s+)?source\s+database\s+([a-zA-Z0-9_\-]+)",
                r"database\s+name\s*[:=]\s*([a-zA-Z0-9_\-]+)",
                r"db[_\s-]?name\s*[:=]\s*([a-zA-Z0-9_\-]+)",
                r"\bdatabase\s+([a-zA-Z0-9_\-]+)\b",
            ]
            for pattern in patterns:
                match = re.search(pattern, candidate_text, flags=re.IGNORECASE)
                if match:
                    return str(match.group(1) or "").strip()
            return ""

        action_raw = str(action_name or "").strip()
        action = action_raw.lower()
        resolved_payload = dict(payload or {}) if isinstance(payload, dict) else {}

        # Accept compact forms where db_name is embedded in action token.
        embedded_match = re.match(
            r"^\s*(?:integrate_source_database|integrate-source-database|integrate_database|integrate\s+database|integrate_db)\s+([a-zA-Z0-9_\-]+)\s*$",
            action_raw,
            flags=re.IGNORECASE,
        )
        if embedded_match and not resolved_payload.get("db_name"):
            resolved_payload["db_name"] = str(embedded_match.group(1) or "").strip()
        if embedded_match:
            action = "integrate_source_database"

        if not resolved_payload.get("db_name"):
            direct_candidates = [
                resolved_payload.get("database"),
                resolved_payload.get("source_database"),
                resolved_payload.get("db"),
            ]
            for candidate in direct_candidates:
                text_candidate = str(candidate or "").strip()
                if text_candidate:
                    resolved_payload["db_name"] = text_candidate
                    break

        if not resolved_payload.get("db_name") and isinstance(resolved_payload.get("args"), list):
            for raw_arg in list(resolved_payload.get("args") or []):
                arg = str(raw_arg or "").strip()
                if re.match(r"^[a-zA-Z0-9_\-]+$", arg):
                    resolved_payload["db_name"] = arg
                    break

        if not resolved_payload.get("db_name"):
            text_fields = [
                "text",
                "request",
                "instruction",
                "command",
                "message",
                "prompt",
                "query",
                "user_query",
                "user_request",
            ]
            merged_text = "\n".join(
                str(resolved_payload.get(field) or "").strip()
                for field in text_fields
                if str(resolved_payload.get(field) or "").strip()
            )
            inferred = _extract_db_name_from_text(merged_text)
            if inferred:
                resolved_payload["db_name"] = inferred

        integrate_action_aliases = {
            "integrate_source_database",
            "integrate-source-database",
            "integrate_database",
            "integrate database",
            "integrate_db",
        }

        if not resolved_payload.get("db_name") and action in integrate_action_aliases:
            # Fallback to last NL context captured in this tool session.
            history_candidates: list[str] = []
            if str(self._last_nl_request_text or "").strip():
                history_candidates.append(str(self._last_nl_request_text or "").strip())

            for turn in reversed(list(self.state.history or [])):
                if str(turn.get("role") or "").strip().lower() != "user":
                    continue
                text = str(turn.get("content") or "").strip()
                if text:
                    history_candidates.append(text)
                if len(history_candidates) >= 3:
                    break

            history_text = "\n".join(history_candidates)
            inferred = _extract_db_name_from_text(history_text)
            if inferred:
                resolved_payload["db_name"] = inferred

        if not resolved_payload.get("db_name") and action in integrate_action_aliases:
            # Last-resort LLM extraction when NL text exists but deterministic regex missed.
            llm_text_parts = [
                str(resolved_payload.get("message") or "").strip(),
                str(resolved_payload.get("text") or "").strip(),
                str(self._last_nl_request_text or "").strip(),
            ]
            llm_text = "\n".join(part for part in llm_text_parts if part)
            if llm_text:
                try:
                    prompt = (
                        "Extract the database name from this user request. "
                        "Return JSON only with key db_name. If missing, return {\"db_name\":\"\"}.\n\n"
                        f"Request:\n{llm_text}"
                    )
                    response = generate_content_with_openrouter_fallback(
                        primary_call=lambda: self.client.models.generate_content(
                            model=EXTRACT_MODEL,
                            contents=[prompt],
                        ),
                        model=EXTRACT_MODEL,
                        contents=[prompt],
                        temperature=0.0,
                        call_name="integrate_db_name_extraction",
                        complexity="simple",
                    )
                    parsed = self._extract_json_object(response.text or "")
                    candidate = str(parsed.get("db_name") or "").strip()
                    if re.match(r"^[a-zA-Z0-9_\-]+$", candidate):
                        resolved_payload["db_name"] = candidate
                except Exception:
                    pass

        if not resolved_payload.get("db_name") and action in integrate_action_aliases:
            # Stateless fallback for clients that call /api/action with empty payload.
            # Reuse the most recent successful integration target or active source entry.
            connection = None
            try:
                connection = self.get_connection()
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT COALESCE(payload->>'db_name', payload->>'database', '')
                        FROM workflow_action_log
                        WHERE action_name = 'integrate_source_database'
                          AND success = TRUE
                          AND COALESCE(payload->>'db_name', payload->>'database', '') <> ''
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    )
                    row = cursor.fetchone()
                    candidate = str(row[0] or "").strip() if row else ""
                    if candidate:
                        resolved_payload["db_name"] = candidate

                if not resolved_payload.get("db_name"):
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            SELECT db_name
                            FROM source_database_registry
                            WHERE status = 'active'
                              AND COALESCE(db_name, '') <> ''
                            ORDER BY updated_at DESC, source_id DESC
                            LIMIT 1
                            """
                        )
                        row = cursor.fetchone()
                        candidate = str(row[0] or "").strip() if row else ""
                        if candidate:
                            resolved_payload["db_name"] = candidate
            except Exception:
                pass
            finally:
                if connection is not None:
                    try:
                        connection.close()
                    except Exception:
                        pass

        if action in integrate_action_aliases:
            if not resolved_payload.get("db_name"):
                phrase_db = str(resolved_payload.get("database") or resolved_payload.get("source_database") or "").strip()
                if phrase_db:
                    resolved_payload["db_name"] = phrase_db
            return "integrate_source_database", resolved_payload

        match = re.match(
            r"^\s*integrate\s+(?:the\s+)?database\s+([a-zA-Z0-9_\-]+)\s+into\s+(?:the\s+)?system\s*[\.!?]?\s*$",
            action_raw,
            flags=re.IGNORECASE,
        )
        if match:
            if not resolved_payload.get("db_name"):
                resolved_payload["db_name"] = str(match.group(1) or "").strip()
            return "integrate_source_database", resolved_payload

        sync_action_aliases = {
            "sync_source_database_entities",
            "sync-source-database-entities",
            "sync_source_entities",
            "sync_db",
            "db_sync",
            "synchronize_db",
            "synchronize_source_database",
        }

        if action in sync_action_aliases:
            text_fields = [
                "text",
                "request",
                "instruction",
                "command",
                "message",
                "prompt",
                "query",
                "user_query",
                "user_request",
            ]
            merged_text = "\n".join(
                str(resolved_payload.get(field) or "").strip()
                for field in text_fields
                if str(resolved_payload.get(field) or "").strip()
            )

            if not resolved_payload.get("source_key") and merged_text:
                # Prefer explicit trailing source db mention: "... in aphotonix_test".
                tail_match = re.search(r"\bin\s+([a-zA-Z0-9_\-]+)\s*[\.!?]?\s*$", merged_text, flags=re.IGNORECASE)
                if tail_match:
                    resolved_payload["source_key"] = str(tail_match.group(1) or "").strip().lower()
                else:
                    stop_tokens = {
                        "customer",
                        "contact",
                        "contact_person",
                        "table",
                        "tables",
                        "customer_name",
                        "contact_name",
                        "system",
                        "database",
                        "source",
                    }
                    source_matches = re.findall(
                        r"\b(?:in|from|source|database)\s+([a-zA-Z0-9_\-]+)\b",
                        merged_text,
                        flags=re.IGNORECASE,
                    )
                    for candidate in reversed(source_matches):
                        token = str(candidate or "").strip().lower()
                        if token and token not in stop_tokens:
                            resolved_payload["source_key"] = token
                            break

            if not resolved_payload.get("use_default_customer_contact_mapping") and merged_text:
                if ("customer" in merged_text.lower()) and ("contact" in merged_text.lower()):
                    resolved_payload["use_default_customer_contact_mapping"] = True

            return "sync_source_database_entities", resolved_payload

        sync_match = re.match(
            r"^\s*synchroni(?:s|z)e\s+the\s+company_name\s+and\s+contact\s+persons\s+with\s+(?:the\s+)?system\s*$",
            action_raw,
            flags=re.IGNORECASE,
        )
        if sync_match:
            resolved_payload.setdefault("use_default_customer_contact_mapping", True)
            return "sync_source_database_entities", resolved_payload

        return action, resolved_payload

    def _resolve_source_db_password(self, source_key: str) -> str:
        sanitized = re.sub(r"[^A-Za-z0-9]", "_", str(source_key or "").upper())
        per_source_key = f"IDMS_SOURCE_DB_PASSWORD_{sanitized}" if sanitized else ""
        return (
            (os.getenv(per_source_key, "") if per_source_key else "")
            or os.getenv("IDMS_SOURCE_DB_PASSWORD", "")
            or os.getenv("IDMS_DB_PASSWORD", "")
        )

    def _build_integrate_source_database_preview(self, payload: dict[str, Any]) -> dict[str, Any]:
        db_name = str(payload.get("db_name") or payload.get("database") or "").strip()
        if not db_name:
            return {
                "action": "integrate_source_database",
                "success": False,
                "message": "db_name is required for integrate_source_database.",
            }

        source_key = str(payload.get("source_key") or db_name).strip().lower()
        source_name = str(payload.get("source_name") or db_name).strip() or db_name
        db_host = str(payload.get("db_host") or os.getenv("IDMS_DB_HOST", "localhost")).strip() or "localhost"
        db_port = int(payload.get("db_port") or os.getenv("IDMS_DB_PORT", "5432") or 5432)
        db_user = str(payload.get("db_user") or os.getenv("IDMS_DB_USER", "postgres")).strip() or "postgres"
        db_schema = str(payload.get("db_schema") or "").strip()
        include_views = bool(payload.get("include_views", False))
        schema_names = [str(item).strip() for item in list(payload.get("schema_names") or []) if str(item).strip()]

        return {
            "action": "integrate_source_database",
            "success": True,
            "dry_run": True,
            "message": "Integration preview prepared. Execution will register the source and inventory all non-system schemas/tables.",
            "plan": {
                "source_key": source_key,
                "source_name": source_name,
                "db_name": db_name,
                "db_host": db_host,
                "db_port": db_port,
                "db_user": db_user,
                "db_schema": db_schema or None,
                "schema_names": schema_names,
                "include_views": include_views,
                "sync_tables": False,
                "analyze_all_schemas": not bool(schema_names or db_schema),
            },
        }

    def _integrate_source_database_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        preview = self._build_integrate_source_database_preview(payload)
        if not preview.get("success"):
            return preview
        if bool(payload.get("dry_run")):
            return preview

        plan = preview.get("plan") if isinstance(preview.get("plan"), dict) else {}
        source_key = str(plan.get("source_key") or "").strip()
        source_name = str(plan.get("source_name") or source_key).strip() or source_key
        db_name = str(plan.get("db_name") or "").strip()
        db_host = str(plan.get("db_host") or "localhost").strip() or "localhost"
        db_port = int(plan.get("db_port") or 5432)
        db_user = str(plan.get("db_user") or "postgres").strip() or "postgres"
        db_schema = str(plan.get("db_schema") or "").strip() or None
        schema_names = [str(item).strip() for item in list(plan.get("schema_names") or []) if str(item).strip()]
        include_views = bool(plan.get("include_views"))
        db_password = str(payload.get("db_password") or "").strip() or self._resolve_source_db_password(source_key)
        if not db_password:
            return {
                "action": "integrate_source_database",
                "success": False,
                "message": "Source database password is missing. Provide db_password or configure IDMS_SOURCE_DB_PASSWORD(_<SOURCE_KEY>).",
                "plan": plan,
            }

        try:
            source_connection = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password,
                connect_timeout=5,
            )
        except Exception as exc:
            return {
                "action": "integrate_source_database",
                "success": False,
                "message": f"Unable to connect to source database: {exc}",
                "plan": plan,
            }

        metadata_connection = self.get_connection()
        try:
            domain_db.create_tables(metadata_connection, recreate=False)
            registration = register_source_database(
                metadata_connection,
                source_key=source_key,
                source_name=source_name,
                db_name=db_name,
                db_host=db_host,
                db_port=db_port,
                db_user=db_user,
                db_schema=db_schema,
                connection_hint=str(payload.get("connection_hint") or "action:integrate_source_database").strip() or None,
                metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
            )
            tables = discover_source_tables(
                source_connection,
                schema_names=schema_names or ([db_schema] if db_schema else None),
                include_views=include_views,
            )
            for table_info in tables:
                upsert_source_table_inventory(metadata_connection, int(registration["source_id"]), table_info)

            tables_by_schema: dict[str, int] = {}
            for table in tables:
                schema_name = str(table.schema_name or "").strip() or "public"
                tables_by_schema[schema_name] = tables_by_schema.get(schema_name, 0) + 1

            return {
                "action": "integrate_source_database",
                "success": True,
                "message": f"Integrated source database '{db_name}' and inventoried {len(tables)} schema objects.",
                "data": {
                    "source": registration,
                    "inventory_count": len(tables),
                    "tables_by_schema": tables_by_schema,
                    "sample_tables": [
                        {
                            "schema_name": table.schema_name,
                            "table_name": table.table_name,
                            "object_kind": table.object_kind,
                            "fingerprint": table.fingerprint,
                        }
                        for table in tables[:20]
                    ],
                    "sync_tables": False,
                },
                "plan": plan,
            }
        except Exception as exc:
            return {
                "action": "integrate_source_database",
                "success": False,
                "message": f"Source integration failed: {exc}",
                "plan": plan,
            }
        finally:
            try:
                source_connection.close()
            except Exception:
                pass
            try:
                metadata_connection.close()
            except Exception:
                pass

    def _get_active_source_registration(self, source_key: str | None = None) -> dict[str, Any] | None:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    if source_key:
                        cur.execute(
                            """
                            SELECT source_id, source_key, source_name, db_host, db_port, db_name, db_user, db_schema, connection_hint, status, metadata
                            FROM source_database_registry
                            WHERE status = 'active' AND source_key = %s
                            LIMIT 1
                            """,
                            (str(source_key).strip().lower(),),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT source_id, source_key, source_name, db_host, db_port, db_name, db_user, db_schema, connection_hint, status, metadata
                            FROM source_database_registry
                            WHERE status = 'active'
                            ORDER BY updated_at DESC, source_id DESC
                            LIMIT 1
                            """
                        )
                    row = cur.fetchone()
        except Exception:
            return None
        if not row:
            return None
        return {
            "source_id": int(row[0]),
            "source_key": str(row[1] or "").strip(),
            "source_name": str(row[2] or "").strip(),
            "db_host": str(row[3] or "").strip(),
            "db_port": int(row[4] or 5432),
            "db_name": str(row[5] or "").strip(),
            "db_user": str(row[6] or "").strip(),
            "db_schema": str(row[7] or "").strip() or None,
            "connection_hint": str(row[8] or "").strip() or None,
            "status": str(row[9] or "").strip(),
            "metadata": row[10] if isinstance(row[10], dict) else {},
        }

    def _list_source_inventory(self, source_id: int) -> list[dict[str, Any]]:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT schema_name, table_name, object_kind, metadata
                    FROM source_schema_inventory
                    WHERE source_id = %s
                    ORDER BY schema_name, table_name
                    """,
                    (int(source_id),),
                )
                rows = cur.fetchall() or []
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "schema_name": str(row[0] or "").strip(),
                    "table_name": str(row[1] or "").strip(),
                    "object_kind": str(row[2] or "").strip(),
                    "metadata": row[3] if isinstance(row[3], dict) else {},
                }
            )
        return out

    def _ensure_object_classes(self, connection: Any, class_names: list[str]) -> None:
        with connection.cursor() as cur:
            for class_name in class_names:
                normalized = str(class_name or "").strip().lower()
                if not normalized:
                    continue
                cur.execute(
                    """
                    INSERT INTO object_class (class_name, metadata)
                    VALUES (%s, '{}'::jsonb)
                    ON CONFLICT (class_name) DO NOTHING
                    """,
                    (normalized,),
                )
        connection.commit()

    def _json_safe_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): self._json_safe_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._json_safe_value(item) for item in value]
        return str(value)

    @staticmethod
    def _inventory_columns(entry: dict[str, Any]) -> set[str]:
        metadata = entry.get("metadata") if isinstance(entry.get("metadata"), dict) else {}
        cols = metadata.get("columns") if isinstance(metadata.get("columns"), list) else []
        return {str(item.get("column_name") or "").strip().lower() for item in cols if isinstance(item, dict)}

    def _llm_infer_customer_contact_aliases(self, inventory: list[dict[str, Any]]) -> dict[str, str] | None:
        if not inventory:
            return None
        if not getattr(self, "client", None):
            return None

        table_summary = []
        for entry in inventory:
            table_name = str(entry.get("table_name") or "").strip()
            columns = self._inventory_columns(entry)
            if not table_name:
                continue
            table_summary.append({
                "table_name": table_name,
                "columns": sorted(columns),
            })

        prompt = (
            "Identify the most likely parent customer/client/company table and the corresponding child contact/related-person table "
            "from the provided inventory. Return JSON only with keys 'parent_table' and 'child_table'. "
            "Use only table names that appear in the inventory. "
            "Prefer tables with names like customer/client/account/company for the parent and contact/person/related_person/contact_person for the child.\n\n"
            f"Inventory:\n{json.dumps(table_summary, ensure_ascii=False)}"
        )

        try:
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: self.client.models.generate_content(
                    model=OPENROUTER_SIMPLE_MODEL,
                    contents=[prompt],
                ),
                model=OPENROUTER_SIMPLE_MODEL,
                contents=[prompt],
                temperature=0.0,
                call_name="sync_table_alias_inference",
                complexity="simple",
            )
            payload = self._extract_json_object(response.text or "")
            parent_table = str(payload.get("parent_table") or "").strip()
            child_table = str(payload.get("child_table") or "").strip()
            if parent_table and child_table:
                return {"parent_table": parent_table, "child_table": child_table}
        except Exception:
            pass
        return None

    def _infer_default_customer_contact_sync_spec(self, source: dict[str, Any], inventory: list[dict[str, Any]]) -> dict[str, Any] | None:
        def _normalize_table_token(name: str) -> str:
            value = str(name or "").strip().lower()
            value = re.sub(r"[^a-z0-9]+", "_", value)
            value = re.sub(r"_+", "_", value).strip("_")
            if not value:
                return ""
            value = value.replace("_people", "_person")
            value = value.replace("_persons", "_person")
            value = value.replace("_contacts", "_contact")
            if value.endswith("_ies") and len(value) > 4:
                value = value[:-3] + "y"
            elif value.endswith("_ses") and len(value) > 4:
                value = value[:-2]
            elif value.endswith("s") and len(value) > 3 and not value.endswith("ss"):
                value = value[:-1]
            return value

        def _matches_table_alias(table_name: str, aliases: set[str]) -> bool:
            normalized = _normalize_table_token(table_name)
            if not normalized:
                return False
            if normalized in aliases:
                return True
            for alias in aliases:
                if alias == normalized or alias in normalized or normalized in alias:
                    return True
            return False

        parent_aliases = {"customer", "client", "account", "company", "business", "organisation", "organization"}
        child_aliases = {"contact_person", "contact", "person", "related_person", "people", "customer_contact"}

        customer_entry = None
        contact_entry = None
        for entry in inventory:
            table_name = str(entry.get("table_name") or "").strip()
            columns = self._inventory_columns(entry)
            if customer_entry is None and _matches_table_alias(table_name, parent_aliases):
                candidate_columns = {"customer_pk", "client_pk", "company_pk", "account_pk", "customer_name", "client_name", "company_name"}
                if candidate_columns & columns or any(col.endswith("_pk") for col in columns) and any(col.endswith("name") for col in columns):
                    customer_entry = entry
            if contact_entry is None and _matches_table_alias(table_name, child_aliases):
                fk_candidates = {"customer_pk", "client_pk", "company_pk", "account_pk", "customer_id", "client_id", "company_id"}
                name_candidates = {"contact_name", "person_name", "full_name", "name"}
                if fk_candidates & columns and (name_candidates & columns or {"contact_pk", "person_pk", "related_person_pk", "contact_id", "person_id"} & columns):
                    contact_entry = entry

        if not customer_entry or not contact_entry:
            llm_guess = self._llm_infer_customer_contact_aliases(inventory)
            if not llm_guess:
                return None
            parent_name = str(llm_guess.get("parent_table") or "").strip()
            child_name = str(llm_guess.get("child_table") or "").strip()
            if not parent_name or not child_name:
                return None
            matches_parent = [entry for entry in inventory if str(entry.get("table_name") or "").strip() == parent_name]
            matches_child = [entry for entry in inventory if str(entry.get("table_name") or "").strip() == child_name]
            if matches_parent and matches_child:
                customer_entry = matches_parent[0]
                contact_entry = matches_child[0]
            else:
                return None

        parent_pk = next((col for col in ["customer_pk", "client_pk", "company_pk", "account_pk"] if col in self._inventory_columns(customer_entry)), "customer_pk")
        parent_name = next((col for col in ["customer_name", "client_name", "company_name", "name"] if col in self._inventory_columns(customer_entry)), "customer_name")
        child_pk = next((col for col in ["contact_pk", "person_pk", "related_person_pk", "contact_id", "person_id"] if col in self._inventory_columns(contact_entry)), "contact_pk")
        child_name = next((col for col in ["contact_name", "person_name", "full_name", "name"] if col in self._inventory_columns(contact_entry)), "contact_name")
        parent_fk = next((col for col in ["customer_pk", "client_pk", "company_pk", "account_pk", "customer_id", "client_id", "company_id"] if col in self._inventory_columns(contact_entry)), "customer_pk")

        return {
            "source_key": source.get("source_key"),
            "parent": {
                "schema_name": customer_entry.get("schema_name") or "public",
                "table_name": customer_entry.get("table_name") or "customer",
                "pk_column": parent_pk,
                "name_column": parent_name,
                "class_name": "company",
                "updated_at_column": "last_update",
                "metadata_columns": [
                    parent_pk, "customer_code", "client_code", "company_code", parent_name, "country", "address",
                    "telephone_no", "mobile_no", "email_address", "website", "agency", "entry_date", "last_update",
                ],
            },
            "child": {
                "schema_name": contact_entry.get("schema_name") or "public",
                "table_name": contact_entry.get("table_name") or "contact_person",
                "pk_column": child_pk,
                "name_column": child_name,
                "class_name": "person",
                "updated_at_column": "last_update",
                "metadata_columns": [
                    child_pk, parent_fk, child_name, "telephone_no", "mobile_no",
                    "email_address", "is_primary", "source_row_no", "entry_date", "last_update",
                ],
                "parent_fk_column": parent_fk,
                "relationship_name": "contact_person_of",
                "relationship_cat": "business_contact",
            },
        }

    def _build_generic_relationship_rules_from_mapping(self, spec: dict[str, Any], *, source_key: str | None = None) -> list[dict[str, Any]]:
        if not isinstance(spec, dict):
            return []
        parent = spec.get("parent") if isinstance(spec.get("parent"), dict) else {}
        child = spec.get("child") if isinstance(spec.get("child"), dict) else {}
        if not parent or not child:
            return []

        def _table_base_name(value: Any) -> str:
            table_name = str(value or "").strip().lower()
            table_name = re.sub(r"[^a-z0-9]+", "_", table_name)
            table_name = re.sub(r"_+", "_", table_name).strip("_")
            return table_name or "entity"

        parent_table = str(parent.get("table_name") or parent.get("class_name") or "entity")
        child_table = str(child.get("table_name") or child.get("class_name") or "entity")
        parent_base = _table_base_name(parent_table)
        child_base = _table_base_name(child_table)

        relationship_name = str(child.get("relationship_name") or "").strip().lower() or None
        if not relationship_name:
            if "person" in child_base or "contact" in child_base or "employee" in child_base:
                relationship_name = f"has_{parent_base}_contact_person"
            else:
                relationship_name = f"has_{parent_base}_{child_base}"

        return [{
            "rule_kind": "source_relationship",
            "source_key": str(source_key or spec.get("source_key") or "").strip(),
            "source_schema": str(child.get("schema_name") or parent.get("schema_name") or "public"),
            "source_table": str(child.get("table_name") or child_table),
            "parent_table": str(parent.get("table_name") or parent_table),
            "parent_pk_column": str(parent.get("pk_column") or "id"),
            "child_table": str(child.get("table_name") or child_table),
            "child_pk_column": str(child.get("pk_column") or "id"),
            "parent_fk_column": str(child.get("parent_fk_column") or parent.get("pk_column") or "id"),
            "relationship_type": relationship_name,
            "relationship_cat": str(child.get("relationship_cat") or "source_link").strip().lower() or "source_link",
            "parent_class_name": str(parent.get("class_name") or parent_base),
            "child_class_name": str(child.get("class_name") or child_base),
        }]

    def _resolve_relationship_name_for_sync(self, plan: dict[str, Any], relationship_rules: list[dict[str, Any]] | None = None) -> tuple[str, str]:
        plan = plan if isinstance(plan, dict) else {}
        child = plan.get("child") if isinstance(plan.get("child"), dict) else {}
        parent = plan.get("parent") if isinstance(plan.get("parent"), dict) else {}

        if isinstance(relationship_rules, list):
            for rule in relationship_rules:
                if not isinstance(rule, dict):
                    continue
                value = str(rule.get("relationship_type") or "").strip()
                if value:
                    return value, str(rule.get("relationship_cat") or "source_link").strip().lower() or "source_link"

        explicit_name = str(child.get("relationship_name") or "").strip().lower()
        if explicit_name:
            return explicit_name, str(child.get("relationship_cat") or "source_link").strip().lower() or "source_link"

        def _table_base_name(value: Any) -> str:
            text = str(value or "").strip().lower()
            text = re.sub(r"[^a-z0-9]+", "_", text)
            text = re.sub(r"_+", "_", text).strip("_")
            return text or "entity"

        parent_table = str(parent.get("table_name") or parent.get("class_name") or "entity")
        child_table = str(child.get("table_name") or child.get("class_name") or "entity")
        parent_base = _table_base_name(parent_table)
        child_base = _table_base_name(child_table)
        if "person" in child_base or "contact" in child_base or "employee" in child_base:
            generated = f"has_{parent_base}_contact_person"
        else:
            generated = f"has_{parent_base}_{child_base}"
        return generated, str(child.get("relationship_cat") or "source_link").strip().lower() or "source_link"

    def _build_sync_source_database_entities_preview(self, payload: dict[str, Any]) -> dict[str, Any]:
        source_key = str(payload.get("source_key") or "").strip().lower() or None
        source = self._get_active_source_registration(source_key=source_key)
        if not source:
            return {
                "action": "sync_source_database_entities",
                "success": False,
                "message": "No active integrated source database found. Run integration first or provide source_key.",
            }
        inventory = self._list_source_inventory(int(source["source_id"]))
        spec = payload.get("mapping") if isinstance(payload.get("mapping"), dict) else None
        if not spec and bool(payload.get("use_default_customer_contact_mapping", False)):
            spec = self._infer_default_customer_contact_sync_spec(source, inventory)
        if not spec:
            return {
                "action": "sync_source_database_entities",
                "success": False,
                "message": "No sync mapping available. Provide mapping or use a supported default customer/contact schema.",
                "source": source,
            }
        relationship_rules = self._build_generic_relationship_rules_from_mapping(spec, source_key=source.get("source_key"))
        return {
            "action": "sync_source_database_entities",
            "success": True,
            "dry_run": True,
            "message": "Sync preview prepared. Execution will sync parent and child tables plus relationships.",
            "plan": spec,
            "source": source,
            "relationship_rules": relationship_rules,
        }

    def _sync_source_database_entities_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        preview = self._build_sync_source_database_entities_preview(payload)
        if not preview.get("success"):
            return preview
        if bool(payload.get("dry_run")):
            return preview

        source = preview.get("source") if isinstance(preview.get("source"), dict) else {}
        plan = preview.get("plan") if isinstance(preview.get("plan"), dict) else {}
        parent = plan.get("parent") if isinstance(plan.get("parent"), dict) else {}
        child = plan.get("child") if isinstance(plan.get("child"), dict) else {}
        source_key = str(source.get("source_key") or "").strip()
        db_password = str(payload.get("db_password") or "").strip() or self._resolve_source_db_password(source_key)
        if not db_password:
            return {
                "action": "sync_source_database_entities",
                "success": False,
                "message": "Source database password is missing. Provide db_password or configure IDMS_SOURCE_DB_PASSWORD(_<SOURCE_KEY>).",
                "source": source,
                "plan": plan,
            }

        source_connection = psycopg2.connect(
            host=str(source.get("db_host") or "localhost"),
            port=int(source.get("db_port") or 5432),
            database=str(source.get("db_name") or ""),
            user=str(source.get("db_user") or "postgres"),
            password=db_password,
            connect_timeout=5,
        )
        idms_connection = self.get_connection()
        try:
            object_db.create_tables(idms_connection, recreate=False)
            domain_db.create_tables(idms_connection, recreate=False)
            self._ensure_object_classes(idms_connection, [str(parent.get("class_name") or "company"), str(child.get("class_name") or "person")])

            def _map_parent_row(row: dict[str, Any]) -> dict[str, Any]:
                name_column = str(parent.get("name_column") or parent.get("pk_column") or "").strip()
                object_name = str(row.get(name_column) or row.get(parent.get("pk_column")) or "").strip()
                metadata = {"source_system": source_key, "source_schema": parent.get("schema_name"), "source_table": parent.get("table_name"), "source_row": self._json_safe_value(row)}
                return {"object_name": object_name, "class_name": str(parent.get("class_name") or "company").strip().lower(), "metadata": metadata, "status": "active"}

            parent_results = sync_mapped_rows(
                source_connection=source_connection,
                idms_connection=idms_connection,
                metadata_connection=idms_connection,
                source_id=int(source.get("source_id") or 0),
                schema_name=str(parent.get("schema_name") or "public"),
                table_name=str(parent.get("table_name") or ""),
                target_class_name=str(parent.get("class_name") or "company").strip().lower(),
                pk_column=str(parent.get("pk_column") or "id"),
                map_row_to_object=_map_parent_row,
                updated_at_column=str(parent.get("updated_at_column") or "").strip() or None,
                metadata_columns=list(parent.get("metadata_columns") or []) or None,
            )

            def _map_child_row(row: dict[str, Any]) -> dict[str, Any]:
                name_column = str(child.get("name_column") or child.get("pk_column") or "").strip()
                object_name = str(row.get(name_column) or row.get(child.get("pk_column")) or row.get("email_address") or "").strip()
                metadata = {"source_system": source_key, "source_schema": child.get("schema_name"), "source_table": child.get("table_name"), "source_row": self._json_safe_value(row)}
                return {"object_name": object_name, "class_name": str(child.get("class_name") or "person").strip().lower(), "metadata": metadata, "status": "active"}

            child_results = sync_mapped_rows(
                source_connection=source_connection,
                idms_connection=idms_connection,
                metadata_connection=idms_connection,
                source_id=int(source.get("source_id") or 0),
                schema_name=str(child.get("schema_name") or "public"),
                table_name=str(child.get("table_name") or ""),
                target_class_name=str(child.get("class_name") or "person").strip().lower(),
                pk_column=str(child.get("pk_column") or "id"),
                map_row_to_object=_map_child_row,
                updated_at_column=str(child.get("updated_at_column") or "").strip() or None,
                metadata_columns=list(child.get("metadata_columns") or []) or None,
            )

            relationship_count = 0
            parent_pk = str(parent.get("pk_column") or "id")
            child_pk = str(child.get("pk_column") or "id")
            parent_fk = str(child.get("parent_fk_column") or parent_pk)
            generated_rules = self._build_generic_relationship_rules_from_mapping(plan, source_key=source_key)
            relationship_name, relationship_cat = self._resolve_relationship_name_for_sync(plan, generated_rules)
            relationship_name = str(relationship_name or "related_to").strip().lower() or "related_to"
            relationship_cat = str(relationship_cat or "source_link").strip().lower() or "source_link"
            select_cols = list(dict.fromkeys([child_pk, parent_fk]))
            sql = (
                f"SELECT {', '.join('"' + col.replace('"','""') + '"' for col in select_cols)} "
                f"FROM {'"' + str(child.get('schema_name') or 'public').replace('"','""') + '"'}.{'"' + str(child.get('table_name') or '').replace('"','""') + '"'}"
            )
            with source_connection.cursor() as src_cur:
                src_cur.execute(sql)
                relation_rows = src_cur.fetchall() or []
            for rel_row in relation_rows:
                child_id = rel_row[0]
                parent_id = rel_row[1] if len(rel_row) > 1 else None
                if child_id in (None, "") or parent_id in (None, ""):
                    continue
                child_mapping = load_source_mapping(
                    idms_connection,
                    source_id=int(source.get("source_id") or 0),
                    schema_name=str(child.get("schema_name") or "public"),
                    table_name=str(child.get("table_name") or ""),
                    source_pk_value=str(child_id),
                    target_class_name=str(child.get("class_name") or "person").strip().lower(),
                )
                parent_mapping = load_source_mapping(
                    idms_connection,
                    source_id=int(source.get("source_id") or 0),
                    schema_name=str(parent.get("schema_name") or "public"),
                    table_name=str(parent.get("table_name") or ""),
                    source_pk_value=str(parent_id),
                    target_class_name=str(parent.get("class_name") or "company").strip().lower(),
                )
                if not child_mapping or not parent_mapping:
                    continue
                if not child_mapping.get("target_object_id") or not parent_mapping.get("target_object_id"):
                    continue
                object_db.upsert_object_relationship(
                    idms_connection,
                    relationship_name=relationship_name,
                    relationship_cat=relationship_cat,
                    src_object_id=int(child_mapping["target_object_id"]),
                    tar_object_id=int(parent_mapping["target_object_id"]),
                    metadata={"source_system": source_key, "source_parent_fk": str(parent_id)},
                )
                relationship_count += 1

            return {
                "action": "sync_source_database_entities",
                "success": True,
                "message": f"Synchronized source database entities from '{source_key}'.",
                "data": {
                    "source": source,
                    "plan": plan,
                    "parent_results": {"count": len(parent_results), "summary": [r.__dict__ for r in parent_results[:20]]},
                    "child_results": {"count": len(child_results), "summary": [r.__dict__ for r in child_results[:20]]},
                    "relationship_count": relationship_count,
                },
            }
        except Exception as exc:
            return {
                "action": "sync_source_database_entities",
                "success": False,
                "message": f"Source entity sync failed: {exc}",
                "source": source,
                "plan": plan,
            }
        finally:
            try:
                source_connection.close()
            except Exception:
                pass
            try:
                idms_connection.close()
            except Exception:
                pass

    def _compose_scheduler_command(self, command_name: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        command = str(command_name or "").strip().lower()
        raw = self._invoke_solf_clause_raw("scheduler_command", [command, payload])
        plan = self._as_string_key_dict(raw)
        return plan or None

    def _extract_json_array(self, raw_text: str) -> list[Any]:
        text = str(raw_text or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        return []

    def _extract_solf_clause_request(self, text: str) -> dict[str, Any] | None:
        raw = str(text or "").strip()
        if not raw:
            return None

        lowered = raw.lower()
        has_clause_token = bool(re.search(r"\b(predicate|clause|solf)\b", lowered))
        has_action_verb = bool(re.search(r"\b(run|invoke|execute|call|evaluate|trigger)\b", lowered))
        if not has_clause_token or not (has_action_verb or lowered.startswith("solf ")):
            return None

        clause_name = ""
        name_patterns = [
            r"\b(?:predicate|clause)\s+([A-Za-z_][A-Za-z0-9_]*)\b",
            r"\bsolf\s+(?:predicate|clause)\s+([A-Za-z_][A-Za-z0-9_]*)\b",
            r"\b(?:run|invoke|execute|call|evaluate|trigger)\s+([A-Za-z_][A-Za-z0-9_]*)\s+(?:predicate|clause)\b",
            r"\bsolf\s+([A-Za-z_][A-Za-z0-9_]*)\b",
        ]
        for pattern in name_patterns:
            match = re.search(pattern, raw, flags=re.IGNORECASE)
            if not match:
                continue
            candidate = str(match.group(1) or "").strip()
            if candidate.lower() not in {"predicate", "clause", "solf"}:
                clause_name = candidate
                break
        if not clause_name:
            return None

        args: list[Any] = []
        args_match = re.search(r"\bargs?\s*[:=]?\s*(\[[\s\S]*\])", raw, flags=re.IGNORECASE)
        if args_match:
            args = self._extract_json_array(args_match.group(1))

        payload = self._extract_json_object(raw)
        if not isinstance(payload, dict):
            payload = {}

        return {
            "clause_name": clause_name,
            "payload": payload,
            "args": args,
            "original_text": raw,
        }

    def _is_dispatchable_solf_clause_name(self, clause_name: str) -> bool:
        name = str(clause_name or "").strip().lower()
        if not name:
            return False
        blocked_prefixes = (
            "interaction_",
            "query_style_",
            "workflow_transition_",
            "scheduler_command",
            "event_",
            "action_plan",
            "sql_plan",
            "compose_",
            "can_",
            "adk_",
        )
        blocked_suffixes = (
            "_policy",
            "_plan",
            "_by_ref",
        )
        blocked_exact = {
            "resolve_policy",
            "entity_resolve_policy",
            "person_resolve_policy",
            "organization_resolve_policy",
            "company_resolve_policy",
            "run_regression_checks",
            "reset_policy_maps",
        }
        if name in blocked_exact:
            return False
        if any(name.startswith(prefix) for prefix in blocked_prefixes):
            return False
        if any(name.endswith(suffix) for suffix in blocked_suffixes):
            return False
        return True

    def _dispatchable_solf_clause_catalog(self) -> list[dict[str, Any]]:
        if not self._ensure_solf_interpreter_loaded():
            return []
        clauses = getattr(self.solf_interpreter, "clauses", {}) if self.solf_interpreter is not None else {}
        if not isinstance(clauses, dict):
            return []

        catalog: list[dict[str, Any]] = []
        for raw_name, defs in clauses.items():
            clause_name = str(raw_name or "").strip()
            if not self._is_dispatchable_solf_clause_name(clause_name):
                continue
            clause_defs = defs if isinstance(defs, list) else [defs]
            arities = sorted(
                {
                    len(item.get("args") or [])
                    for item in clause_defs
                    if isinstance(item, dict)
                }
            )
            if not arities:
                continue
            tokens = [tok for tok in re.split(r"_+", clause_name.lower()) if tok]
            catalog.append(
                {
                    "clause_name": clause_name,
                    "tokens": tokens,
                    "arities": arities,
                }
            )
        return catalog

    def _normalize_dispatch_text(self, text: str) -> str:
        lowered = str(text or "").strip().lower()
        lowered = re.sub(r"[^a-z0-9\s_\-]", " ", lowered)
        lowered = lowered.replace("_", " ")
        return re.sub(r"\s+", " ", lowered).strip()

    def _extract_generic_clause_args(self, text: str, clause_name: str, arity: int) -> list[Any] | None:
        raw = str(text or "").strip()
        if not raw:
            return None
        if arity == 0:
            return []

        quoted_matches = [a or b for a, b in re.findall(r'"([^"]+)"|\'([^\']+)\'', raw)]
        if len(quoted_matches) >= arity:
            return quoted_matches[:arity]

        tokens = [tok for tok in re.split(r"_+", str(clause_name or "").strip().lower()) if tok]
        if not tokens:
            return None
        verb = re.escape(tokens[0])
        tail_tokens = [re.escape(tok) for tok in tokens[1:]]
        optional_tail = rf"(?:\s+(?:{'|'.join(tail_tokens)}))?" if tail_tokens else ""

        if arity == 1:
            match = re.search(
                rf"\b{verb}\b{optional_tail}\s+(.+?)(?:[?.!]|$)",
                raw,
                flags=re.IGNORECASE,
            )
            if match:
                candidate = str(match.group(1) or "").strip(" ,.;")
                candidate = re.sub(r"\s+(?:together|please|now)$", "", candidate, flags=re.IGNORECASE).strip(" ,.;")
                return [candidate] if candidate else None
            return None

        if arity == 2:
            match = re.search(
                rf"\b{verb}\b{optional_tail}\s+(.+?)\s+(?:and|with|into|to)\s+(.+?)(?:\s+(?:together|as\s+one))?(?:[?.!]|$)",
                raw,
                flags=re.IGNORECASE,
            )
            if match:
                left = str(match.group(1) or "").strip(" ,.;")
                right = str(match.group(2) or "").strip(" ,.;")
                if left and right:
                    return [left, right]
            return None

        return None

    def _score_generic_solf_clause_match(self, text: str, clause_entry: dict[str, Any]) -> tuple[float, list[Any] | None]:
        normalized = self._normalize_dispatch_text(text)
        words = set(re.findall(r"[a-z0-9]+", normalized))
        tokens = [str(tok).strip().lower() for tok in list(clause_entry.get("tokens") or []) if str(tok).strip()]
        if not tokens:
            return 0.0, None

        verb = tokens[0]
        if verb not in words:
            return 0.0, None

        best_score = 0.0
        best_args: list[Any] | None = None
        overlap = len([tok for tok in tokens[1:] if tok in words])
        for arity in list(clause_entry.get("arities") or []):
            args = self._extract_generic_clause_args(text, str(clause_entry.get("clause_name") or ""), int(arity))
            if args is None:
                continue
            score = 1.0 + (0.35 * overlap) + (0.25 * min(len(args), 3))
            if best_args is None or score > best_score:
                best_score = score
                best_args = args

        return best_score, best_args

    def _resolve_solf_clause_request(self, text: str) -> dict[str, Any] | None:
        explicit = self._extract_solf_clause_request(text)
        if isinstance(explicit, dict):
            explicit["dispatch_mode"] = "explicit"
            return explicit

        catalog = self._dispatchable_solf_clause_catalog()
        best_request: dict[str, Any] | None = None
        best_score = 0.0
        for entry in catalog:
            score, args = self._score_generic_solf_clause_match(text, entry)
            if score < 1.2 or args is None:
                continue
            if score > best_score:
                best_score = score
                best_request = {
                    "clause_name": str(entry.get("clause_name") or "").strip(),
                    "payload": {},
                    "args": args,
                    "original_text": str(text or "").strip(),
                    "dispatch_mode": "generic_nl",
                    "dispatch_score": round(score, 4),
                }
        return best_request

    @staticmethod
    def _should_dispatch_solf_clause(question: str, clause_request: dict[str, Any] | None) -> bool:
        if not isinstance(clause_request, dict):
            return False

        dispatch_mode = str(clause_request.get("dispatch_mode") or "").strip().lower()
        if dispatch_mode == "explicit":
            return True
        if dispatch_mode != "generic_nl":
            return True

        text = re.sub(r"\s+", " ", str(question or "").strip().lower())
        if not text:
            return False

        # Keep generic SOLF dispatch for imperative/action-style requests, but
        # let ordinary information retrieval questions route through query_engine.
        info_request = bool(
            re.match(
                r"^(?:what|which|who|when|where|why|how|show|list|find|give me|tell me|"
                r"was|welche(?:r|s|n)?|wer|wann|wo|warum|wie|zeige|liste|finde|gib mir)\b",
                text,
                flags=re.IGNORECASE,
            )
        )
        return not info_request

    def _format_direct_solf_query_result(self, question: str, evaluated: dict[str, Any]) -> dict[str, Any]:
        success = bool(evaluated.get("success"))
        clause_name = str(evaluated.get("clause_name") or "").strip()
        message = str(evaluated.get("message") or "").strip()
        if success:
            result_text = json.dumps(evaluated.get("result"), ensure_ascii=False)
            answer = f"[source: solf_clause] SOLF clause {clause_name} executed. Result: {result_text}".strip()
        else:
            answer = f"[source: solf_clause] SOLF clause {clause_name or 'unknown'} failed: {message or 'execution failed'}".strip()
        return {
            "intent": "solf_clause",
            "source": "solf_clause",
            "question": question,
            "answer": answer,
            "success": success,
            "data": evaluated,
            "clause_name": clause_name,
        }

    def query(self, question: str) -> dict[str, Any]:
        """Tool: Resolve user query via SQL-first + Qdrant fallback query engine.
        
        Results are cached for 1 hour to avoid redundant embedding/search calls.
        """
        if self.query_cache:
            cached = self.query_cache.get(question)
            if cached is not None:
                # Mark cache hit in result metadata.
                cached = dict(cached)
                cached["_cache_hit"] = True
                return cached

        clause_request = self._resolve_solf_clause_request(question)
        if self._should_dispatch_solf_clause(question, clause_request):
            result = self._dispatch_solf_clause_request(question, clause_request)
            if self.query_cache:
                self.query_cache.put(question, result)
            return result
        
        result = self.query_engine.answer(question)
        if isinstance(result, dict):
            result = dict(result)
            result["evidence_bundle"] = self._build_query_evidence_bundle(question, result)
        
        if self.query_cache:
            self.query_cache.put(question, result)
        
        return result

    def ingest_document(
        self,
        source: str,
        description: str = "",
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Tool: Ingest a local or gs:// source through the existing ingest pipeline."""
        try:
            import ingest
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("ingest pipeline dependencies are not available") from exc

        user_context = {
            "description": description,
            "tags": tags or [],
            "metadata": metadata or {},
        }
        return ingest.run_ingest(source_path_or_uri=source, user_context=user_context)

    def ingest_information(
        self,
        information_text: str,
        description: str = "",
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Tool: Ingest raw information text without providing an external document file."""
        if not information_text.strip():
            raise ValueError("information_text must not be empty")

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".txt", delete=False) as tmp:
            tmp.write(information_text)
            tmp_path = tmp.name

        try:
            return self.ingest_document(
                source=tmp_path,
                description=description,
                tags=tags,
                metadata=metadata,
            )
        finally:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass

    def generate_document(
        self,
        document_type: str,
        subject: str,
        context: dict[str, Any] | None = None,
        template: str = "",
        output_path: str | None = None,
    ) -> dict[str, Any]:
        """Tool: Generate a simple structured document from payload context."""
        doc_type = str(document_type or "").strip().lower()
        doc_subject = str(subject or "").strip()
        if not doc_type:
            raise ValueError("document_type must not be empty")
        if not doc_subject:
            raise ValueError("subject must not be empty")

        ctx = context if isinstance(context, dict) else {}
        title = f"{doc_type.replace('_', ' ').title()}: {doc_subject}"
        lines = [
            f"# {title}",
            "",
            f"Generated At: {datetime.now(timezone.utc).isoformat()}",
            "",
        ]

        if template:
            lines.extend(["Template", "--------", template.strip(), ""])

        if ctx:
            lines.extend(["Context", "-------"])
            for key, value in ctx.items():
                if isinstance(value, (dict, list)):
                    rendered = json.dumps(value, ensure_ascii=False)
                else:
                    rendered = str(value)
                lines.append(f"- {key}: {rendered}")
            lines.append("")

        content = "\n".join(lines).strip() + "\n"
        out_path = None
        if output_path:
            out_file = Path(output_path)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_text(content, encoding="utf-8")
            out_path = str(out_file)

        return {
            "success": True,
            "document_type": doc_type,
            "subject": doc_subject,
            "content": content,
            "output_path": out_path,
        }

    def chat(self, user_message: str, use_query_tool: bool = True) -> dict[str, Any]:
        """Tool: Chat interaction with optional grounding through the query tool."""
        user_message = str(user_message or "").strip()
        if not user_message:
            raise ValueError("user_message must not be empty")
        self._last_nl_request_text = user_message

        clause_request = self._resolve_solf_clause_request(user_message)
        if isinstance(clause_request, dict):
            query_like = self._dispatch_solf_clause_request(user_message, clause_request)
            assistant_text = str(query_like.get("answer") or "").strip()
            self.state.history.append({"role": "user", "content": user_message})
            self.state.history.append({"role": "assistant", "content": assistant_text})
            return {
                "answer": assistant_text,
                "answer_source": str(query_like.get("source") or "solf_clause"),
                "answer_sources": [str(query_like.get("source") or "solf_clause")],
                "grounding": query_like,
                "model": "deterministic_solf" if str(query_like.get("source") or "") != "pending_clarification" else "deterministic_clarification",
                "status": query_like.get("status"),
                "thread_id": query_like.get("thread_id"),
                "clarification_question": query_like.get("clarification_question"),
                "candidates": query_like.get("candidates"),
            }

        query_result: dict[str, Any] | None = None
        if use_query_tool:
            query_result = self.autonomous_query(user_message, max_steps=3, record_history=False)

        clarification_signal = self._extract_clarification_signal(user_message, query_result)
        if clarification_signal:
            clarification_question = self._build_generic_clarification_question(
                str(clarification_signal.get("reason_code") or "ambiguous_value"),
                list(clarification_signal.get("candidates") or []),
            )
            thread_id = self._create_clarification_thread(
                original_query=user_message,
                reason_code=str(clarification_signal.get("reason_code") or "ambiguous_value"),
                clarification_question=clarification_question,
                expected_input_type=str(clarification_signal.get("expected_input_type") or "free_text"),
                ambiguity_summary=(
                    clarification_signal.get("ambiguity_summary")
                    if isinstance(clarification_signal.get("ambiguity_summary"), dict)
                    else {}
                ),
                candidate_snapshot=list(clarification_signal.get("candidates") or []),
                provenance_snapshot=list(clarification_signal.get("provenance_snapshot") or []),
                metadata={
                    "answer_source": str((query_result or {}).get("answer_source") or ""),
                    "trigger": "generic_ambiguity_guardrail",
                },
            )

            self._append_clarification_turn(
                thread_id=thread_id,
                role="user",
                message_text=user_message,
                source_type="chat_input",
            )
            self._append_clarification_turn(
                thread_id=thread_id,
                role="assistant",
                message_text=clarification_question,
                source_type="clarification_prompt",
                answer_source="pending_clarification",
                confidence=float(clarification_signal.get("confidence") or 0.0)
                if clarification_signal.get("confidence") is not None
                else None,
                ambiguity_flag=True,
                ambiguity_payload=(
                    clarification_signal.get("ambiguity_summary")
                    if isinstance(clarification_signal.get("ambiguity_summary"), dict)
                    else {}
                ),
                candidate_snapshot=list(clarification_signal.get("candidates") or []),
                provenance_snapshot=list(clarification_signal.get("provenance_snapshot") or []),
            )

            self.state.history.append({"role": "user", "content": user_message})
            self.state.history.append({"role": "assistant", "content": clarification_question})

            return {
                "answer": clarification_question,
                "status": "pending_clarification",
                "thread_id": thread_id,
                "clarification_question": clarification_question,
                "reason_code": str(clarification_signal.get("reason_code") or "ambiguous_value"),
                "expected_input_type": str(clarification_signal.get("expected_input_type") or "free_text"),
                "candidates": list(clarification_signal.get("candidates") or []),
                "provenance_snapshot": list(clarification_signal.get("provenance_snapshot") or []),
                "answer_source": "pending_clarification",
                "answer_sources": ["pending_clarification"],
                "grounding": query_result,
                "model": "deterministic_clarification",
            }

        history_text = "\n".join(
            f"{item['role']}: {item['content']}" for item in self.state.history[-10:]
        )
        grounding_text = json.dumps(query_result, ensure_ascii=False) if query_result else "{}"

        prompt = (
            "You are an IDMS assistant.\n"
            "Use the grounding result when present.\n"
            "If grounding source is sql_exact or qdrant_sql_fallback, prefer it over speculation.\n"
            "Be concise and factual.\n\n"
            f"Grounding:\n{grounding_text}\n\n"
            f"Recent conversation:\n{history_text or 'N/A'}\n\n"
            f"User: {user_message}"
        )

        # Select model based on query source: Flash (deterministic), Pro (complex)
        answer_source = (
            query_result.get("answer_source", "unknown")
            if isinstance(query_result, dict)
            else "model_only"
        )
        
        deterministic_sources = {
            "sql_exact", "sql_pattern_match",
            "keyword_payload_fallback", "keyword_candidates",
            "identifier_multilang_gate", "sql_lookup",
            "criteria_sql", "criteria_docs_sql",
            "federated_sql", "integrated_db_sql",
        }
        parsed_confidence = None
        if isinstance(query_result, dict):
            grounding = query_result.get("grounding") if isinstance(query_result.get("grounding"), dict) else {}
            parsed_payload = grounding.get("parsed") if isinstance(grounding.get("parsed"), dict) else {}
            parsed_confidence = parsed_payload.get("confidence")

        selected_model = self._select_model_by_confidence(parsed_confidence, default=EXTRACT_MODEL)
        if answer_source in deterministic_sources:
            selected_model = EXTRACT_MODEL
        self._record_model_usage("interaction_chat", selected_model, parsed_confidence)

        if isinstance(query_result, dict) and answer_source in deterministic_sources:
            query_sources = [
                str(
                    query_result.get("answer_source")
                    or query_result.get("source")
                    or "unknown"
                ).strip().lower()
            ]
            answer_text = str(query_result.get("answer") or "").strip()
            if answer_text:
                if not answer_text.lower().startswith("[source:"):
                    answer_text = f"[source: {answer_source}] {answer_text}".strip()
                self.state.history.append({"role": "user", "content": user_message})
                self.state.history.append({"role": "assistant", "content": answer_text})
                return {
                    "answer": answer_text,
                    "answer_source": answer_source,
                    "answer_sources": query_sources,
                    "grounding": query_result,
                    "evidence_bundle": self._build_query_evidence_bundle(user_message, query_result),
                    "model": selected_model,
                }
        
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: self.client.models.generate_content(
                model=selected_model,
                contents=[prompt],
            ),
            model=selected_model,
            contents=[prompt],
            temperature=0.0,
            call_name="interaction_chat",
            complexity="simple",
        )
        assistant_text = (response.text or "").strip()
        if isinstance(query_result, dict):
            query_sources = [
                str(
                    query_result.get("answer_source")
                    or query_result.get("source")
                    or "unknown"
                ).strip().lower()
            ]
        else:
            query_sources = ["model_only"]
        assistant_text = f"[source: {'+'.join(query_sources)}] {assistant_text}".strip()

        self.state.history.append({"role": "user", "content": user_message})
        self.state.history.append({"role": "assistant", "content": assistant_text})

        return {
            "answer": assistant_text,
            "answer_source": query_sources[0],
            "answer_sources": query_sources,
            "grounding": query_result,
            "evidence_bundle": self._build_query_evidence_bundle(user_message, query_result) if isinstance(query_result, dict) else None,
            "model": selected_model,
        }

    def _extract_json_object(self, raw_text: str) -> dict[str, Any]:
        text = str(raw_text or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

        # Fallback for fenced or mixed outputs.
        match = re.search(r"\{[\s\S]*\}", text)
        if not match:
            return {}
        try:
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _safe_confidence(self, value: Any, default: float = 1.0) -> float:
        try:
            score = float(value)
        except Exception:
            score = float(default)
        return max(0.0, min(1.0, score))

    def _select_model_by_confidence(self, confidence: Any, *, default: str = EXTRACT_MODEL) -> str:
        score = self._safe_confidence(confidence, default=1.0)
        return CHAT_MODEL if score < float(FLASH_CONFIDENCE_THRESHOLD) else str(default)

    def _record_model_usage(self, call_name: str, model: str, confidence: Any = None) -> dict[str, Any]:
        score = self._safe_confidence(confidence, default=1.0)
        stats = self.model_usage_stats
        stats["total_calls"] = int(stats.get("total_calls", 0)) + 1

        is_chat = str(model or "").strip() == str(CHAT_MODEL)
        if is_chat:
            stats["chat_calls"] = int(stats.get("chat_calls", 0)) + 1
        else:
            stats["flash_calls"] = int(stats.get("flash_calls", 0)) + 1

        escalated = is_chat and score < float(FLASH_CONFIDENCE_THRESHOLD)
        if escalated:
            stats["escalations"] = int(stats.get("escalations", 0)) + 1

        by_call = stats.get("by_call_name")
        if not isinstance(by_call, dict):
            by_call = {}
            stats["by_call_name"] = by_call

        call_key = str(call_name or "unknown").strip().lower()
        call_stats = by_call.get(call_key)
        if not isinstance(call_stats, dict):
            call_stats = {
                "total": 0,
                "flash": 0,
                "chat": 0,
                "escalations": 0,
            }
            by_call[call_key] = call_stats

        call_stats["total"] = int(call_stats.get("total", 0)) + 1
        if is_chat:
            call_stats["chat"] = int(call_stats.get("chat", 0)) + 1
        else:
            call_stats["flash"] = int(call_stats.get("flash", 0)) + 1
        if escalated:
            call_stats["escalations"] = int(call_stats.get("escalations", 0)) + 1

        total_calls = int(stats.get("total_calls", 0))
        chat_calls = int(stats.get("chat_calls", 0))
        escalation_count = int(stats.get("escalations", 0))
        chat_rate = (chat_calls / total_calls) if total_calls else 0.0
        escalation_rate = (escalation_count / total_calls) if total_calls else 0.0

        snapshot = {
            "total_calls": total_calls,
            "flash_calls": int(stats.get("flash_calls", 0)),
            "chat_calls": chat_calls,
            "escalations": escalation_count,
            "chat_rate": round(chat_rate, 4),
            "escalation_rate": round(escalation_rate, 4),
        }
        self.logger.info(
            "model_usage call=%s model=%s confidence=%.3f threshold=%.3f total=%s flash=%s chat=%s escalations=%s",
            call_key,
            model,
            score,
            float(FLASH_CONFIDENCE_THRESHOLD),
            snapshot["total_calls"],
            snapshot["flash_calls"],
            snapshot["chat_calls"],
            snapshot["escalations"],
        )
        return snapshot

    def get_model_usage_stats(self) -> dict[str, Any]:
        stats = self.model_usage_stats
        total_calls = int(stats.get("total_calls", 0))
        chat_calls = int(stats.get("chat_calls", 0))
        escalation_count = int(stats.get("escalations", 0))
        return {
            "total_calls": total_calls,
            "flash_calls": int(stats.get("flash_calls", 0)),
            "chat_calls": chat_calls,
            "escalations": escalation_count,
            "chat_rate": round((chat_calls / total_calls), 4) if total_calls else 0.0,
            "escalation_rate": round((escalation_count / total_calls), 4) if total_calls else 0.0,
            "by_call_name": stats.get("by_call_name", {}),
        }

    def _get_command_nlp(self) -> Any | None:
        if self._command_nlp_initialized:
            return self._command_nlp
        self._command_nlp_initialized = True
        if spacy is None:
            self._command_nlp = None
            return None

        for model_name in ("en_core_web_sm", "de_core_news_sm"):
            try:
                self._command_nlp = spacy.load(model_name)
                return self._command_nlp
            except Exception:
                continue
        self._command_nlp = None
        return None

    def _extract_domain_definition_json(self, payload: dict[str, Any], text: str) -> dict[str, Any]:
        candidates = [
            payload.get("definition"),
            payload.get("data"),
            payload.get("definition_json"),
            payload.get("json"),
            payload.get("payload"),
        ]
        for candidate in candidates:
            if isinstance(candidate, dict):
                return dict(candidate)
            if isinstance(candidate, str):
                parsed = self._extract_json_object(candidate)
                if parsed:
                    return parsed

        fenced = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
        if fenced:
            parsed = self._extract_json_object(fenced.group(1))
            if parsed:
                return parsed

        parsed = self._extract_json_object(text)
        return parsed if isinstance(parsed, dict) else {}

    def _resolve_domain_definition_operation(self, payload: dict[str, Any], text: str) -> str:
        explicit = str(payload.get("operation") or payload.get("crud_operation") or "").strip().lower()
        if explicit:
            mapping = {
                "add": "create",
                "insert": "create",
                "new": "create",
                "create": "create",
                "update": "update",
                "modify": "update",
                "edit": "update",
                "change": "update",
                "replace": "update",
                "delete": "delete",
                "remove": "delete",
                "drop": "delete",
                "read": "read",
                "get": "read",
                "show": "read",
                "view": "read",
                "list": "list",
            }
            if explicit in mapping:
                return mapping[explicit]

        lowered = str(text or "").strip().lower()
        op_aliases = {
            "create": ["create", "add", "insert", "new", "register"],
            "update": ["update", "modify", "edit", "change", "replace", "set"],
            "delete": ["delete", "remove", "drop", "deactivate"],
            "list": ["list", "all", "browse", "overview"],
            "read": ["get", "show", "read", "view", "find"],
        }
        for op_name, aliases in op_aliases.items():
            if any(re.search(rf"\b{re.escape(alias)}\b", lowered) for alias in aliases):
                return op_name

        nlp = self._get_command_nlp()
        if nlp is not None and lowered:
            try:
                lemmas = {str(token.lemma_ or token.text).strip().lower() for token in nlp(lowered)}
            except Exception:
                lemmas = set()
            for op_name, aliases in op_aliases.items():
                if lemmas.intersection(aliases):
                    return op_name
        return ""

    def _resolve_domain_definition_type(self, payload: dict[str, Any], text: str) -> str:
        explicit = str(payload.get("definition_type") or payload.get("type") or "").strip().lower()
        if explicit:
            if explicit in {"account_definition", "account", "coa", "chart_of_accounts", "chart of accounts", "ledger_account", "konto"}:
                return "account_definition"
            if explicit in {"hr_department_definition", "department", "department_definition", "dept", "abteilung"}:
                return "hr_department_definition"
            if explicit in {"hr_role_definition", "role", "role_definition", "position", "job_role", "funktion", "stelle"}:
                return "hr_role_definition"

        lowered = str(text or "").strip().lower()
        type_aliases = {
            "account_definition": ["account definition", "account", "coa", "chart of accounts", "ledger account", "gl account", "konto", "konten"],
            "hr_department_definition": ["department definition", "department", "hr department", "dept", "abteilung"],
            "hr_role_definition": ["role definition", "role", "job role", "position", "funktion", "stelle"],
        }
        for definition_type, aliases in type_aliases.items():
            if any(alias in lowered for alias in aliases):
                return definition_type

        nlp = self._get_command_nlp()
        if nlp is not None and lowered:
            try:
                lemmas = {str(token.lemma_ or token.text).strip().lower() for token in nlp(lowered)}
            except Exception:
                lemmas = set()
            if lemmas.intersection({"account", "ledger", "konto", "coa"}):
                return "account_definition"
            if lemmas.intersection({"department", "dept", "abteilung"}):
                return "hr_department_definition"
            if lemmas.intersection({"role", "position", "funktion", "stelle"}):
                return "hr_role_definition"

        return ""

    def _llm_extract_domain_definition_request(
        self,
        text: str,
        operation_hint: str,
        definition_type_hint: str,
    ) -> dict[str, Any]:
        prompt = (
            "You extract CRUD intent for domain definitions from user text.\n"
            "Return JSON only with keys: operation, definition_type, definition, confidence, errors.\n"
            "operation must be one of: create, update, delete, read, list.\n"
            "definition_type must be one of: account_definition, hr_department_definition, hr_role_definition.\n"
            "definition must be a JSON object with extracted fields.\n"
            "If uncertain, keep unknown fields empty and add a short error message to errors.\n"
            f"operation_hint: {operation_hint or 'none'}\n"
            f"definition_type_hint: {definition_type_hint or 'none'}\n"
            f"user_text:\n{text}\n"
        )
        selected_model = EXTRACT_MODEL
        self._record_model_usage("domain_definition_crud_parser", selected_model, 1.0)
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: self.client.models.generate_content(
                model=selected_model,
                contents=[prompt],
            ),
            model=selected_model,
            contents=[prompt],
            temperature=0.0,
            call_name="domain_definition_crud_parser",
            complexity="simple",
        )
        parsed = self._extract_json_object(response.text or "")
        return parsed if isinstance(parsed, dict) else {}

    def _get_solf_symbols_snapshot(self) -> dict[str, Any]:
        """Extract lightweight SOLF class/clause symbols for planner grounding."""
        script_path = Path(__file__).with_name("solf_script.txt")
        if not script_path.exists():
            return {
                "classes": [],
                "clauses": [],
                "source": str(script_path),
                "available": False,
            }

        try:
            text = script_path.read_text(encoding="utf-8")
        except Exception:
            return {
                "classes": [],
                "clauses": [],
                "source": str(script_path),
                "available": False,
            }

        class_names = sorted({
            str(match.group(1)).strip()
            for match in re.finditer(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*≔\s*\{", text, flags=re.MULTILINE)
        })
        clause_names = sorted({
            str(match.group(1)).strip()
            for match in re.finditer(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\([^\)]*\)\s*⦃", text, flags=re.MULTILINE)
        })
        return {
            "classes": class_names[:300],
            "clauses": clause_names[:600],
            "class_count": len(class_names),
            "clause_count": len(clause_names),
            "source": str(script_path),
            "available": True,
        }

    def _coerce_workflow_design_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        out = payload if isinstance(payload, dict) else {}
        workflow_steps = out.get("workflow_steps") if isinstance(out.get("workflow_steps"), list) else []
        gaps = out.get("gaps") if isinstance(out.get("gaps"), list) else []
        solf_candidates = out.get("solf_candidates") if isinstance(out.get("solf_candidates"), list) else []
        next_questions = out.get("next_questions") if isinstance(out.get("next_questions"), list) else []
        next_action = str(out.get("next_action") or "refine_with_context").strip().lower()
        if next_action not in {"finalize", "refine_with_context", "map_to_solf"}:
            next_action = "refine_with_context"
        return {
            "understanding": str(out.get("understanding") or "").strip(),
            "workflow_steps": workflow_steps,
            "gaps": gaps,
            "solf_candidates": solf_candidates,
            "next_questions": next_questions,
            "next_action": next_action,
        }

    def _workflow_design_iteration(
        self,
        request_text: str,
        iteration_index: int,
        context: dict[str, Any],
        previous_output: dict[str, Any] | None,
        solf_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        prompt = (
            "You are an IDMS workflow-design planner.\n"
            "Goal: iteratively produce a structured workflow plan for the request.\n"
            "Do NOT generate Python code. Focus on process steps, data requirements, SOLF mapping candidates, and open gaps.\n"
            "Return JSON only with keys: understanding, workflow_steps, gaps, solf_candidates, next_questions, next_action.\n"
            "next_action must be one of: finalize, refine_with_context, map_to_solf.\n"
            "workflow_steps: list of objects with keys: step_id, name, purpose, required_inputs, expected_outputs, retrieval_or_action.\n"
            "solf_candidates: list of objects with keys: class_name, clause_name, usage_reason, confidence.\n"
            "gaps: list of concise unresolved items.\n"
            "Use internal context and SOLF symbols where relevant.\n\n"
            f"Request:\n{request_text}\n\n"
            f"Iteration: {iteration_index}\n"
            f"Internal context JSON:\n{json.dumps(context or {}, ensure_ascii=False)}\n\n"
            f"Available SOLF symbols:\n{json.dumps(solf_snapshot, ensure_ascii=False)}\n\n"
            f"Previous iteration output JSON:\n{json.dumps(previous_output or {}, ensure_ascii=False)}"
        )
        selected_model = EXTRACT_MODEL
        self._record_model_usage("workflow_design_iteration", selected_model, 1.0)
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: self.client.models.generate_content(
                model=selected_model,
                contents=[prompt],
            ),
            model=selected_model,
            contents=[prompt],
            temperature=0.0,
            call_name="workflow_design_iteration",
            complexity="complex",
        )
        parsed = self._extract_json_object(response.text or "")
        return self._coerce_workflow_design_payload(parsed)

    def design_workflow_interaction(
        self,
        request_text: str,
        context: dict[str, Any] | None = None,
        max_iterations: int = 3,
    ) -> dict[str, Any]:
        """Iterative LLM planner for abstract workflow/process requests.

        Produces stepwise structured output that can be refined using internal context
        and SOLF symbol grounding across multiple iterations.
        """
        normalized_request = str(request_text or "").strip()
        if not normalized_request:
            raise ValueError("request_text must not be empty")
        self._last_nl_request_text = normalized_request

        iterations = min(max(1, int(max_iterations)), 8)
        working_context = context if isinstance(context, dict) else {}
        solf_snapshot = self._get_solf_symbols_snapshot()

        run_trace: list[dict[str, Any]] = []
        current_output: dict[str, Any] = {}
        for idx in range(iterations):
            iter_no = idx + 1
            current_output = self._workflow_design_iteration(
                request_text=normalized_request,
                iteration_index=iter_no,
                context=working_context,
                previous_output=current_output,
                solf_snapshot=solf_snapshot,
            )
            run_trace.append(
                {
                    "iteration": iter_no,
                    "next_action": current_output.get("next_action"),
                    "workflow_step_count": len(current_output.get("workflow_steps") or []),
                    "gap_count": len(current_output.get("gaps") or []),
                    "solf_candidate_count": len(current_output.get("solf_candidates") or []),
                    "payload": current_output,
                }
            )

            next_action = str(current_output.get("next_action") or "refine_with_context").strip().lower()
            has_steps = bool(current_output.get("workflow_steps"))
            has_gaps = bool(current_output.get("gaps"))
            if next_action == "finalize" and has_steps:
                break
            if idx >= iterations - 1:
                break

            if next_action in {"map_to_solf", "refine_with_context"}:
                working_context = {
                    **working_context,
                    "latest_workflow_steps": current_output.get("workflow_steps"),
                    "latest_gaps": current_output.get("gaps"),
                    "latest_solf_candidates": current_output.get("solf_candidates"),
                    "latest_next_questions": current_output.get("next_questions"),
                    "has_unresolved_gaps": has_gaps,
                }

        summary = {
            "request": normalized_request,
            "iterations_executed": len(run_trace),
            "final_next_action": str(current_output.get("next_action") or "").strip(),
            "workflow_step_count": len(current_output.get("workflow_steps") or []),
            "gap_count": len(current_output.get("gaps") or []),
            "solf_candidate_count": len(current_output.get("solf_candidates") or []),
        }
        return {
            "success": True,
            "summary": summary,
            "final_plan": current_output,
            "iterations": run_trace,
            "solf_snapshot": {
                "available": bool(solf_snapshot.get("available")),
                "class_count": int(solf_snapshot.get("class_count") or 0),
                "clause_count": int(solf_snapshot.get("clause_count") or 0),
            },
        }

    def _coerce_complex_interaction_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        out = payload if isinstance(payload, dict) else {}
        objectives = out.get("objectives") if isinstance(out.get("objectives"), list) else []
        planned_steps = out.get("planned_steps") if isinstance(out.get("planned_steps"), list) else []
        proposed_actions = out.get("proposed_actions") if isinstance(out.get("proposed_actions"), list) else []
        retrieval_plan = out.get("retrieval_plan") if isinstance(out.get("retrieval_plan"), list) else []
        solf_candidates = out.get("solf_candidates") if isinstance(out.get("solf_candidates"), list) else []
        missing_information = out.get("missing_information") if isinstance(out.get("missing_information"), list) else []
        next_action = str(out.get("next_action") or "refine_with_context").strip().lower()
        if next_action not in {
            "finalize",
            "refine_with_context",
            "execute_query",
            "execute_interaction",
            "ask_user_input",
            "map_to_solf",
        }:
            next_action = "refine_with_context"
        return {
            "understanding": str(out.get("understanding") or "").strip(),
            "objectives": objectives,
            "planned_steps": planned_steps,
            "proposed_actions": proposed_actions,
            "retrieval_plan": retrieval_plan,
            "solf_candidates": solf_candidates,
            "missing_information": missing_information,
            "user_prompt": str(out.get("user_prompt") or "").strip(),
            "response_draft": str(out.get("response_draft") or "").strip(),
            "next_action": next_action,
        }

    def _complex_interaction_iteration(
        self,
        request_text: str,
        iteration_index: int,
        context: dict[str, Any],
        previous_output: dict[str, Any] | None,
        solf_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        prompt = (
            "You are an IDMS complex interaction planner.\n"
            "Goal: iteratively resolve abstract/complex query or interaction requests.\n"
            "Do NOT generate Python code.\n"
            "Return JSON only with keys: understanding, objectives, planned_steps, proposed_actions, retrieval_plan, solf_candidates, missing_information, user_prompt, response_draft, next_action.\n"
            "next_action must be one of: finalize, refine_with_context, execute_query, execute_interaction, ask_user_input, map_to_solf.\n"
            "When information is missing, set next_action=ask_user_input and provide a concise user_prompt.\n"
            "planned_steps: structured list of step objects (step_id, name, purpose, inputs, outputs).\n"
            "proposed_actions: list of intended interaction/tool actions without code generation.\n"
            "retrieval_plan: list of data retrieval intents/filters needed to answer correctly.\n"
            "solf_candidates: list with class_name, clause_name, usage_reason, confidence.\n\n"
            f"Request:\n{request_text}\n\n"
            f"Iteration: {iteration_index}\n"
            f"Internal context JSON:\n{json.dumps(context or {}, ensure_ascii=False)}\n\n"
            f"Available SOLF symbols:\n{json.dumps(solf_snapshot, ensure_ascii=False)}\n\n"
            f"Previous iteration output JSON:\n{json.dumps(previous_output or {}, ensure_ascii=False)}"
        )
        selected_model = EXTRACT_MODEL
        self._record_model_usage("complex_interaction_iteration", selected_model, 1.0)
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: self.client.models.generate_content(
                model=selected_model,
                contents=[prompt],
            ),
            model=selected_model,
            contents=[prompt],
            temperature=0.0,
            call_name="complex_interaction_iteration",
            complexity="complex",
        )
        parsed = self._extract_json_object(response.text or "")
        return self._coerce_complex_interaction_payload(parsed)

    def resolve_complex_interaction(
        self,
        request_text: str,
        context: dict[str, Any] | None = None,
        max_iterations: int = 3,
    ) -> dict[str, Any]:
        """Iteratively resolve a complex query/interaction into actionable structured output.

        This planner is not workflow-only; it supports abstract interaction requests,
        query decomposition, SOLF mapping hints, and user clarification prompts.
        """
        normalized_request = str(request_text or "").strip()
        if not normalized_request:
            raise ValueError("request_text must not be empty")
        self._last_nl_request_text = normalized_request

        iterations = min(max(1, int(max_iterations)), 8)
        working_context = context if isinstance(context, dict) else {}
        solf_snapshot = self._get_solf_symbols_snapshot()

        run_trace: list[dict[str, Any]] = []
        current_output: dict[str, Any] = {}
        needs_user_input = False
        user_prompt = ""

        for idx in range(iterations):
            iter_no = idx + 1
            current_output = self._complex_interaction_iteration(
                request_text=normalized_request,
                iteration_index=iter_no,
                context=working_context,
                previous_output=current_output,
                solf_snapshot=solf_snapshot,
            )
            run_trace.append(
                {
                    "iteration": iter_no,
                    "next_action": current_output.get("next_action"),
                    "objective_count": len(current_output.get("objectives") or []),
                    "planned_step_count": len(current_output.get("planned_steps") or []),
                    "missing_information_count": len(current_output.get("missing_information") or []),
                    "payload": current_output,
                }
            )

            next_action = str(current_output.get("next_action") or "refine_with_context").strip().lower()
            if next_action == "ask_user_input":
                prompt_text = str(current_output.get("user_prompt") or "").strip()
                user_prompt = prompt_text or "Please provide the missing details so I can continue."
                needs_user_input = True
                break

            has_plan = bool(current_output.get("planned_steps")) or bool(current_output.get("response_draft"))
            if next_action == "finalize" and has_plan:
                break

            if idx >= iterations - 1:
                break

            working_context = {
                **working_context,
                "latest_objectives": current_output.get("objectives"),
                "latest_planned_steps": current_output.get("planned_steps"),
                "latest_proposed_actions": current_output.get("proposed_actions"),
                "latest_retrieval_plan": current_output.get("retrieval_plan"),
                "latest_solf_candidates": current_output.get("solf_candidates"),
                "latest_missing_information": current_output.get("missing_information"),
            }

        summary = {
            "request": normalized_request,
            "iterations_executed": len(run_trace),
            "final_next_action": str(current_output.get("next_action") or "").strip(),
            "needs_user_input": bool(needs_user_input),
            "objective_count": len(current_output.get("objectives") or []),
            "planned_step_count": len(current_output.get("planned_steps") or []),
            "missing_information_count": len(current_output.get("missing_information") or []),
        }
        result = {
            "success": True,
            "summary": summary,
            "final_resolution": current_output,
            "iterations": run_trace,
            "solf_snapshot": {
                "available": bool(solf_snapshot.get("available")),
                "class_count": int(solf_snapshot.get("class_count") or 0),
                "clause_count": int(solf_snapshot.get("clause_count") or 0),
            },
        }
        if needs_user_input:
            result["user_prompt"] = user_prompt
        return result

    def _build_interaction_capability_profile(self, solf_snapshot: dict[str, Any]) -> dict[str, Any]:
        action_capabilities = [
            "query",
            "chat",
            "perform_action",
            "evaluate_solf_clause",
            "design_workflow_interaction",
            "resolve_complex_interaction",
            "resolve_complex_interaction_production",
        ]
        return {
            "planner_scope": "generic_interaction_resolution",
            "non_goals": [
                "freeform legal compliance claims without sources",
                "final numeric computation outside deterministic code",
            ],
            "available_actions": action_capabilities,
            "generic_action_types": ["query", "interaction", "chat", "solf"],
            "deterministic_calculators": ["payroll_net_v1"],
            "required_external_fact_fields": [
                "canonical_key",
                "value",
                "source_url",
                "effective_date",
                "jurisdiction",
            ],
            "solf": {
                "available": bool(solf_snapshot.get("available")),
                "class_count": int(solf_snapshot.get("class_count") or 0),
                "clause_count": int(solf_snapshot.get("clause_count") or 0),
            },
        }

    def _coerce_complex_production_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        out = payload if isinstance(payload, dict) else {}
        next_action = str(out.get("next_action") or "refine_with_context").strip().lower()
        if next_action not in {
            "finalize",
            "refine_with_context",
            "ask_user_input",
            "retrieve_external",
            "calculate",
            "execute_query",
            "execute_interaction",
            "map_to_solf",
        }:
            next_action = "refine_with_context"
        return {
            "understanding": str(out.get("understanding") or "").strip(),
            "workflow_steps": out.get("workflow_steps") if isinstance(out.get("workflow_steps"), list) else [],
            "required_information": out.get("required_information") if isinstance(out.get("required_information"), list) else [],
            "external_retrieval_requests": out.get("external_retrieval_requests") if isinstance(out.get("external_retrieval_requests"), list) else [],
            "mapping_plan": out.get("mapping_plan") if isinstance(out.get("mapping_plan"), list) else [],
            "execution_actions": out.get("execution_actions") if isinstance(out.get("execution_actions"), list) else [],
            "missing_information": out.get("missing_information") if isinstance(out.get("missing_information"), list) else [],
            "user_prompt": str(out.get("user_prompt") or "").strip(),
            "response_draft": str(out.get("response_draft") or "").strip(),
            "calculator_id": str(out.get("calculator_id") or "").strip().lower(),
            "next_action": next_action,
        }

    def _complex_interaction_production_iteration(
        self,
        request_text: str,
        iteration_index: int,
        context: dict[str, Any],
        previous_output: dict[str, Any],
        capability_profile: dict[str, Any],
        llm_model: str,
    ) -> dict[str, Any]:
        prompt = (
            "You are an IDMS production-grade interaction planner.\\n"
            "Return JSON only with keys: understanding, workflow_steps, required_information, external_retrieval_requests, mapping_plan, execution_actions, missing_information, user_prompt, response_draft, calculator_id, next_action.\\n"
            "Use only these next_action values: finalize, refine_with_context, ask_user_input, retrieve_external, calculate, execute_query, execute_interaction, map_to_solf.\\n"
            "required_information items must include: canonical_key, label, source_preference (internal|user|external), required (true|false), reason.\\n"
            "external_retrieval_requests items must include: canonical_key, retrieval_query, jurisdiction, required_fields.\\n"
            "execution_actions items must include: action_type (query|interaction|chat|solf), name, payload, args.\\n"
            "Do not generate code. Keep workflow generic and reusable.\\n\\n"
            f"Request:\\n{request_text}\\n\\n"
            f"Iteration: {iteration_index}\\n\\n"
            f"IDMS capability profile JSON:\\n{json.dumps(capability_profile or {}, ensure_ascii=False)}\\n\\n"
            f"Current context JSON:\\n{json.dumps(context or {}, ensure_ascii=False)}\\n\\n"
            f"Previous output JSON:\\n{json.dumps(previous_output or {}, ensure_ascii=False)}"
        )
        selected_model = str(llm_model or EXTRACT_MODEL)
        self._record_model_usage("complex_interaction_production_iteration", selected_model, 1.0)
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: self.client.models.generate_content(
                model=selected_model,
                contents=[prompt],
            ),
            model=selected_model,
            contents=[prompt],
            temperature=0.0,
            call_name="complex_interaction_production_iteration",
            complexity="complex",
        )
        parsed = self._extract_json_object(response.text or "")
        return self._coerce_complex_production_payload(parsed)

    def _canonicalize_fact_key(self, key: Any) -> str:
        token = re.sub(r"[^a-z0-9]+", "_", str(key or "").strip().lower()).strip("_")
        aliases = {
            "gross_salary": "gross_salary_monthly",
            "gross_monthly_salary": "gross_salary_monthly",
            "gross_salary_chf": "gross_salary_monthly",
            "ahv_iv_eo": "ahv_iv_eo_rate_employee",
            "ahv_iv_eo_rate": "ahv_iv_eo_rate_employee",
            "alv": "alv_rate_employee",
            "alv_rate": "alv_rate_employee",
            "children_allowance": "children_allowance_monthly",
            "child_allowance": "children_allowance_monthly",
            "quellensteuer": "withholding_tax_rate_employee",
            "withholding_tax": "withholding_tax_rate_employee",
            "withholding_tax_rate": "withholding_tax_rate_employee",
        }
        return aliases.get(token, token)

    def _normalize_requirement_item(self, item: Any) -> dict[str, Any]:
        if isinstance(item, dict):
            raw_key = item.get("canonical_key") or item.get("field") or item.get("key") or item.get("name")
            source_pref = str(item.get("source_preference") or item.get("source") or "external").strip().lower()
            if source_pref not in {"internal", "user", "external"}:
                source_pref = "external"
            return {
                "canonical_key": self._canonicalize_fact_key(raw_key),
                "label": str(item.get("label") or raw_key or "").strip(),
                "source_preference": source_pref,
                "required": bool(item.get("required", True)),
                "reason": str(item.get("reason") or "").strip(),
            }
        key = self._canonicalize_fact_key(item)
        return {
            "canonical_key": key,
            "label": str(item or "").strip(),
            "source_preference": "external",
            "required": True,
            "reason": "",
        }

    def _collect_initial_facts_from_context(self, context: dict[str, Any]) -> dict[str, Any]:
        facts: dict[str, Any] = {}

        def _merge(container: Any) -> None:
            if not isinstance(container, dict):
                return
            for raw_key, value in container.items():
                if isinstance(value, (str, int, float, bool)):
                    key = self._canonicalize_fact_key(raw_key)
                    if key:
                        facts[key] = value

        _merge(context)
        _merge(context.get("known_facts"))
        _merge(context.get("international_data"))
        _merge(context.get("data"))
        return facts

    def _coerce_external_fact_payload(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        facts_raw = payload.get("facts") if isinstance(payload, dict) else []
        if not isinstance(facts_raw, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in facts_raw:
            if not isinstance(item, dict):
                continue
            normalized.append(
                {
                    "canonical_key": self._canonicalize_fact_key(
                        item.get("canonical_key") or item.get("field") or item.get("key")
                    ),
                    "value": item.get("value"),
                    "value_type": str(item.get("value_type") or "").strip(),
                    "unit": str(item.get("unit") or "").strip(),
                    "jurisdiction": str(item.get("jurisdiction") or "").strip(),
                    "effective_date": str(item.get("effective_date") or "").strip(),
                    "source_url": str(item.get("source_url") or "").strip(),
                    "source_title": str(item.get("source_title") or "").strip(),
                    "confidence": item.get("confidence"),
                    "notes": str(item.get("notes") or "").strip(),
                }
            )
        return normalized

    def _retrieve_external_facts_via_llm(
        self,
        request_text: str,
        retrieval_requests: list[dict[str, Any]],
        llm_model: str,
    ) -> list[dict[str, Any]]:
        prompt = (
            "You are an IDMS external fact retriever.\\n"
            "Return JSON only with key facts (array).\\n"
            "Each fact item must include: canonical_key, value, value_type, unit, jurisdiction, effective_date, source_url, source_title, confidence, notes.\\n"
            "Do not include facts without source_url and effective_date.\\n\\n"
            f"User request:\\n{request_text}\\n\\n"
            f"Retrieval requests JSON:\\n{json.dumps(retrieval_requests or [], ensure_ascii=False)}"
        )
        selected_model = str(llm_model or EXTRACT_MODEL)
        self._record_model_usage("complex_interaction_external_retrieval", selected_model, 1.0)
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: self.client.models.generate_content(
                model=selected_model,
                contents=[prompt],
            ),
            model=selected_model,
            contents=[prompt],
            temperature=0.0,
            call_name="complex_interaction_external_retrieval",
            complexity="complex",
        )
        payload = self._extract_json_object(response.text or "")
        return self._coerce_external_fact_payload(payload)

    def _is_valid_iso_date(self, value: str) -> bool:
        text = str(value or "").strip()
        if not text:
            return False
        try:
            datetime.strptime(text, "%Y-%m-%d")
            return True
        except Exception:
            return False

    def _verify_external_source_live(self, source_url: str, timeout_seconds: int = 5) -> dict[str, Any]:
        url = str(source_url or "").strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            return {
                "reachable": False,
                "http_status": None,
                "final_url": "",
                "content_type": "",
                "content_hash": "",
                "error": "invalid_url",
            }

        timeout = min(max(1, int(timeout_seconds)), 20)
        try:
            request = urllib.request.Request(
                url,
                method="GET",
                headers={"User-Agent": "IDMS-SourceVerifier/1.0"},
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status = int(getattr(response, "status", 200) or 200)
                final_url = str(getattr(response, "geturl", lambda: url)() or url)
                content_type = str(response.headers.get("Content-Type") or "").strip().lower()
                sample = response.read(2048)
                content_hash = hashlib.sha256(sample or b"").hexdigest()
                return {
                    "reachable": bool(200 <= status < 400),
                    "http_status": status,
                    "final_url": final_url,
                    "content_type": content_type,
                    "content_hash": content_hash,
                    "error": "",
                }
        except urllib.error.HTTPError as exc:
            return {
                "reachable": False,
                "http_status": int(getattr(exc, "code", 0) or 0) or None,
                "final_url": url,
                "content_type": "",
                "content_hash": "",
                "error": f"http_error:{int(getattr(exc, 'code', 0) or 0)}",
            }
        except Exception as exc:
            return {
                "reachable": False,
                "http_status": None,
                "final_url": url,
                "content_type": "",
                "content_hash": "",
                "error": str(exc),
            }

    def _is_source_domain_approved(self, source_url: str, approved_domains: list[str] | None) -> bool:
        domains = [str(d or "").strip().lower() for d in (approved_domains or []) if str(d or "").strip()]
        if not domains:
            return True
        url = str(source_url or "").strip()
        if not url:
            return False
        try:
            host = str(urllib.parse.urlparse(url).hostname or "").strip().lower()
        except Exception:
            return False
        if not host:
            return False
        for domain in domains:
            d = domain.lstrip(".")
            if host == d or host.endswith(f".{d}"):
                return True
        return False

    def _validate_external_fact(
        self,
        fact: dict[str, Any],
        strict_provenance: bool,
        verify_external_sources: bool = False,
        timeout_seconds: int = 5,
        enforce_approved_domains: bool = False,
        approved_source_domains: list[str] | None = None,
    ) -> dict[str, Any]:
        source_url = str(fact.get("source_url") or "").strip().lower()
        effective_date = str(fact.get("effective_date") or "").strip()
        provenance_ok = source_url.startswith("http://") or source_url.startswith("https://")
        effective_date_ok = self._is_valid_iso_date(effective_date)
        domain_approved = self._is_source_domain_approved(source_url, approved_source_domains)
        source_verification = {
            "checked": False,
            "reachable": None,
            "http_status": None,
            "final_url": "",
            "content_type": "",
            "content_hash": "",
            "error": "",
        }
        if verify_external_sources and provenance_ok:
            live = self._verify_external_source_live(source_url, timeout_seconds=timeout_seconds)
            source_verification = {
                "checked": True,
                **live,
            }
        usable = bool(fact.get("canonical_key")) and fact.get("value") is not None
        if strict_provenance and (not provenance_ok or not effective_date_ok):
            usable = False
        if strict_provenance and enforce_approved_domains and not domain_approved:
            usable = False
        if strict_provenance and verify_external_sources and source_verification.get("checked") and not source_verification.get("reachable"):
            usable = False
        return {
            **fact,
            "usable": usable,
            "validation": {
                "provenance_ok": provenance_ok,
                "effective_date_ok": effective_date_ok,
                "domain_approved": domain_approved,
                "source_verification": source_verification,
            },
        }

    def _execute_generic_complex_actions(
        self,
        request_text: str,
        next_action: str,
        execution_actions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        actions = [a for a in execution_actions if isinstance(a, dict)]
        if not actions:
            if next_action == "execute_query":
                actions = [{"action_type": "query", "payload": {"question": request_text}}]
            elif next_action == "map_to_solf":
                actions = [{"action_type": "solf", "name": "", "payload": {}, "args": []}]

        results: list[dict[str, Any]] = []
        executed = False

        for action in actions:
            action_type = str(action.get("action_type") or "").strip().lower()
            payload = action.get("payload") if isinstance(action.get("payload"), dict) else {}
            args = action.get("args") if isinstance(action.get("args"), list) else []
            name = str(action.get("name") or "").strip()

            if action_type == "query":
                question = str(payload.get("question") or request_text).strip()
                out = self.query(question)
                executed = True
                results.append({"action_type": action_type, "success": True, "result": out})
                continue

            if action_type == "chat":
                message = str(payload.get("message") or request_text).strip()
                use_query_tool = bool(payload.get("use_query_tool", True))
                out = self.chat(message, use_query_tool=use_query_tool)
                executed = True
                results.append({"action_type": action_type, "success": True, "result": out})
                continue

            if action_type == "interaction":
                action_name = str(name or payload.get("action_name") or "").strip().lower()
                action_payload = payload.get("action_payload") if isinstance(payload.get("action_payload"), dict) else payload
                if isinstance(action_payload, dict) and request_text:
                    action_payload.setdefault("message", request_text)
                if isinstance(action_payload, dict) and args and not action_payload.get("db_name"):
                    action_payload.setdefault("args", list(args))
                if action_name:
                    out = self.perform_action(action_name, action_payload)
                    executed = True
                    results.append({"action_type": action_type, "name": action_name, "success": bool(out.get("success", True)), "result": out})
                else:
                    results.append({"action_type": action_type, "success": False, "error": "missing_action_name"})
                continue

            if action_type == "solf":
                clause_name = str(name or payload.get("clause_name") or "").strip()
                clause_payload = payload.get("clause_payload") if isinstance(payload.get("clause_payload"), dict) else payload
                if clause_name:
                    out = self.evaluate_solf_clause(clause_name=clause_name, payload=clause_payload, args=args)
                    executed = True
                    results.append({"action_type": action_type, "clause_name": clause_name, "success": bool(out.get("success", True)), "result": out})
                else:
                    results.append({"action_type": action_type, "success": False, "error": "missing_clause_name"})
                continue

            results.append({"action_type": action_type or "unknown", "success": False, "error": "unsupported_action_type"})

        return {
            "executed": executed,
            "count": len(results),
            "results": results,
        }

    def _as_float(self, value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip().replace(",", "")
        if text.endswith("%"):
            text = text[:-1].strip()
            try:
                return float(text) / 100.0
            except Exception:
                return None
        try:
            return float(text)
        except Exception:
            return None

    def _calculate_payroll_net_v1(self, fact_map: dict[str, Any]) -> dict[str, Any]:
        gross = self._as_float(fact_map.get("gross_salary_monthly"))
        if gross is None:
            return {
                "success": False,
                "calculator_id": "payroll_net_v1",
                "missing_inputs": ["gross_salary_monthly"],
                "message": "gross_salary_monthly is required for payroll_net_v1",
            }

        rate_keys = [
            "ahv_iv_eo_rate_employee",
            "alv_rate_employee",
            "withholding_tax_rate_employee",
            "pension_rate_employee",
            "nbu_rate_employee",
        ]
        deductions: dict[str, float] = {}
        for key in rate_keys:
            raw = self._as_float(fact_map.get(key))
            if raw is None:
                continue
            rate = raw if raw <= 1.0 else raw / 100.0
            deductions[key] = round(gross * rate, 2)

        fixed_deductions = self._as_float(fact_map.get("fixed_deductions_monthly")) or 0.0
        children_allowance = self._as_float(fact_map.get("children_allowance_monthly")) or 0.0
        total_deductions = round(sum(deductions.values()) + fixed_deductions, 2)
        net_salary = round(gross - total_deductions + children_allowance, 2)

        return {
            "success": True,
            "calculator_id": "payroll_net_v1",
            "currency": str(fact_map.get("currency") or "CHF").strip() or "CHF",
            "gross_salary_monthly": round(gross, 2),
            "deductions": deductions,
            "fixed_deductions_monthly": round(fixed_deductions, 2),
            "children_allowance_monthly": round(children_allowance, 2),
            "total_deductions_monthly": total_deductions,
            "net_salary_monthly": net_salary,
        }

    def _run_deterministic_calculation(self, calculator_id: str, fact_map: dict[str, Any]) -> dict[str, Any]:
        selected = str(calculator_id or "").strip().lower()
        if not selected:
            has_payroll_signal = bool(
                fact_map.get("gross_salary_monthly")
                and (
                    fact_map.get("ahv_iv_eo_rate_employee")
                    or fact_map.get("alv_rate_employee")
                    or fact_map.get("withholding_tax_rate_employee")
                )
            )
            selected = "payroll_net_v1" if has_payroll_signal else ""

        if selected == "payroll_net_v1":
            return self._calculate_payroll_net_v1(fact_map)

        return {
            "success": False,
            "calculator_id": selected,
            "message": "No deterministic calculator executed",
        }

    def resolve_complex_interaction_production(
        self,
        request_text: str,
        context: dict[str, Any] | None = None,
        max_iterations: int = 3,
        llm_model: str = "",
        max_retrieval_rounds: int = 2,
        strict_provenance: bool = True,
        run_calculation: bool = True,
        execute_generic_actions: bool = True,
        verify_external_sources: bool = False,
        external_source_timeout_seconds: int = 5,
        enforce_approved_domains: bool = False,
        approved_source_domains: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized_request = str(request_text or "").strip()
        if not normalized_request:
            raise ValueError("request_text must not be empty")
        self._last_nl_request_text = normalized_request

        iterations = min(max(1, int(max_iterations)), 8)
        retrieval_rounds = min(max(1, int(max_retrieval_rounds)), 6)
        selected_model = str(llm_model or EXTRACT_MODEL)
        domains = [str(d or "").strip().lower() for d in (approved_source_domains or []) if str(d or "").strip()]
        if enforce_approved_domains and not domains:
            domains = ["admin.ch", "zg.ch", "ahv-iv.ch", "seco.admin.ch", "estv.admin.ch"]

        working_context = context if isinstance(context, dict) else {}
        solf_snapshot = self._get_solf_symbols_snapshot()
        capability_profile = self._build_interaction_capability_profile(solf_snapshot)

        run_trace: list[dict[str, Any]] = []
        current_output: dict[str, Any] = {}
        initial_facts = self._collect_initial_facts_from_context(working_context)
        fact_map = dict(initial_facts)
        external_facts: list[dict[str, Any]] = []
        generic_execution: dict[str, Any] = {"executed": False, "count": 0, "results": []}
        needs_user_input = False
        user_prompt = ""
        required_items: list[dict[str, Any]] = []

        for idx in range(iterations):
            iter_no = idx + 1
            current_output = self._complex_interaction_production_iteration(
                request_text=normalized_request,
                iteration_index=iter_no,
                context=working_context,
                previous_output=current_output,
                capability_profile=capability_profile,
                llm_model=selected_model,
            )

            required_items = [
                self._normalize_requirement_item(item)
                for item in (current_output.get("required_information") or [])
                if self._normalize_requirement_item(item).get("canonical_key")
            ]

            requested_external = current_output.get("external_retrieval_requests") or []
            retrieval_needed = [item for item in requested_external if isinstance(item, dict)]
            retrieval_attempted = False
            if retrieval_needed and iter_no <= retrieval_rounds:
                retrieval_attempted = True
                fetched = self._retrieve_external_facts_via_llm(
                    request_text=normalized_request,
                    retrieval_requests=retrieval_needed,
                    llm_model=selected_model,
                )
                validated = [
                    self._validate_external_fact(
                        item,
                        strict_provenance,
                        verify_external_sources=verify_external_sources,
                        timeout_seconds=external_source_timeout_seconds,
                        enforce_approved_domains=enforce_approved_domains,
                        approved_source_domains=domains,
                    )
                    for item in fetched
                ]
                for fact in validated:
                    if fact.get("usable") and fact.get("canonical_key"):
                        fact_map[str(fact.get("canonical_key"))] = fact.get("value")
                external_facts.extend(validated)

            missing_required: list[str] = []
            for item in required_items:
                if not item.get("required", True):
                    continue
                key = str(item.get("canonical_key") or "").strip()
                if not key:
                    continue
                if key not in fact_map or fact_map.get(key) in {None, ""}:
                    missing_required.append(key)

            next_action = str(current_output.get("next_action") or "refine_with_context").strip().lower()
            if next_action == "ask_user_input" or missing_required:
                needs_user_input = True
                prompt_text = str(current_output.get("user_prompt") or "").strip()
                if prompt_text:
                    user_prompt = prompt_text
                else:
                    labels: list[str] = []
                    for key in missing_required:
                        label = next(
                            (
                                str(entry.get("label") or key)
                                for entry in required_items
                                if str(entry.get("canonical_key") or "") == key
                            ),
                            key,
                        )
                        labels.append(label)
                    joined = ", ".join(labels) if labels else "the missing details"
                    user_prompt = f"Please provide {joined} so I can complete the response."

            run_trace.append(
                {
                    "iteration": iter_no,
                    "next_action": next_action,
                    "required_information_count": len(required_items),
                    "missing_required_count": len(missing_required),
                    "retrieval_attempted": retrieval_attempted,
                    "payload": current_output,
                }
            )

            if needs_user_input:
                break
            if execute_generic_actions and next_action in {"execute_query", "execute_interaction", "map_to_solf"}:
                generic_execution = self._execute_generic_complex_actions(
                    request_text=normalized_request,
                    next_action=next_action,
                    execution_actions=current_output.get("execution_actions") or [],
                )
                break
            if next_action in {"finalize", "calculate"}:
                break
            if idx >= iterations - 1:
                break

            working_context = {
                **working_context,
                "latest_required_information": required_items,
                "latest_external_retrieval_requests": retrieval_needed,
                "latest_known_fact_keys": sorted(list(fact_map.keys())),
                "latest_missing_information": current_output.get("missing_information"),
            }

        calculation_result = {
            "success": False,
            "calculator_id": str(current_output.get("calculator_id") or "").strip().lower(),
            "message": "Calculation skipped",
        }
        if run_calculation and not needs_user_input:
            calculation_result = self._run_deterministic_calculation(
                calculator_id=str(current_output.get("calculator_id") or "").strip().lower(),
                fact_map=fact_map,
            )

        summary = {
            "request": normalized_request,
            "iterations_executed": len(run_trace),
            "final_next_action": str(current_output.get("next_action") or "").strip(),
            "needs_user_input": bool(needs_user_input),
            "required_information_count": len(required_items),
            "fact_count": len(fact_map),
            "external_fact_count": len(external_facts),
            "calculation_executed": bool(calculation_result.get("success")),
            "generic_actions_executed": bool(generic_execution.get("executed")),
            "llm_model": selected_model,
        }

        result = {
            "success": True,
            "summary": summary,
            "final_resolution": current_output,
            "iterations": run_trace,
            "capability_profile": capability_profile,
            "mapped_facts": fact_map,
            "external_facts": external_facts,
            "generic_execution": generic_execution,
            "calculation": calculation_result,
            "provenance_policy": {
                "strict_provenance": bool(strict_provenance),
                "verify_external_sources": bool(verify_external_sources),
                "external_source_timeout_seconds": min(max(1, int(external_source_timeout_seconds)), 20),
                "enforce_approved_domains": bool(enforce_approved_domains),
                "approved_source_domains": domains,
                "required_fields": capability_profile.get("required_external_fact_fields"),
            },
            "solf_snapshot": {
                "available": bool(solf_snapshot.get("available")),
                "class_count": int(solf_snapshot.get("class_count") or 0),
                "clause_count": int(solf_snapshot.get("clause_count") or 0),
            },
        }
        if needs_user_input:
            result["user_prompt"] = user_prompt
        return result

    def _expected_fields_for_definition(self, definition_type: str, operation: str) -> list[str]:
        normalized_type = str(definition_type or "").strip().lower()
        normalized_op = str(operation or "").strip().lower()
        if normalized_type == "account_definition":
            if normalized_op in {"delete", "read"}:
                return ["legal_entity_ref", "account_number"]
            return ["legal_entity_ref", "account_number", "account_name", "account_type"]
        if normalized_type == "hr_department_definition":
            if normalized_op in {"delete", "read"}:
                return ["department_code"]
            return ["department_code", "department_name", "active"]
        if normalized_type == "hr_role_definition":
            if normalized_op in {"delete", "read"}:
                return ["role_code"]
            return ["role_code", "role_name", "role_family", "seniority_level", "active"]
        return []

    def _handle_domain_definition_crud_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            import domain_db
        except Exception as exc:
            return {
                "action": "domain_definition_crud",
                "success": False,
                "message": f"domain_db unavailable: {exc}",
            }

        raw_text_parts = [
            str(payload.get("text") or ""),
            str(payload.get("request") or ""),
            str(payload.get("instruction") or ""),
            str(payload.get("command") or ""),
            str(payload.get("message") or ""),
        ]
        raw_text = "\n".join(part for part in raw_text_parts if part.strip()).strip()

        operation = self._resolve_domain_definition_operation(payload, raw_text)
        definition_type = self._resolve_domain_definition_type(payload, raw_text)
        definition = self._extract_domain_definition_json(payload, raw_text)

        # Also accept direct key-value payloads when JSON block is not provided.
        if not definition:
            direct_keys = {
                "legal_entity_ref", "account_number", "account_name", "account_type",
                "department_code", "department_name",
                "role_code", "role_name", "role_family", "seniority_level",
                "active", "metadata",
            }
            direct_payload = {k: v for k, v in payload.items() if k in direct_keys}
            if direct_payload:
                definition = direct_payload

        needs_llm = (not operation) or (not definition_type) or (operation in {"create", "update", "delete", "read"} and not definition)
        llm_payload: dict[str, Any] = {}
        if needs_llm and raw_text:
            llm_payload = self._llm_extract_domain_definition_request(raw_text, operation, definition_type)
            if not operation:
                operation = str(llm_payload.get("operation") or "").strip().lower()
            if not definition_type:
                definition_type = str(llm_payload.get("definition_type") or "").strip().lower()
            llm_definition = llm_payload.get("definition") if isinstance(llm_payload.get("definition"), dict) else {}
            if llm_definition:
                merged = dict(llm_definition)
                merged.update(definition)
                definition = merged

        op_map = {
            "add": "create",
            "insert": "create",
            "new": "create",
            "modify": "update",
            "edit": "update",
            "change": "update",
            "remove": "delete",
            "drop": "delete",
            "get": "read",
            "show": "read",
            "view": "read",
        }
        operation = op_map.get(operation, operation)
        definition_type = domain_db.normalize_definition_type(definition_type)

        if operation not in {"create", "update", "delete", "read", "list"}:
            return {
                "action": "domain_definition_crud",
                "success": False,
                "message": "Could not resolve CRUD operation. Use one of create/update/delete/read/list.",
                "resolved": {
                    "operation": operation,
                    "definition_type": definition_type,
                },
            }

        if not definition_type:
            return {
                "action": "domain_definition_crud",
                "success": False,
                "message": "Could not resolve definition type. Specify account_definition, hr_department_definition, or hr_role_definition.",
                "resolved": {
                    "operation": operation,
                    "definition_type": definition_type,
                },
            }

        if operation == "list":
            with self.get_connection() as connection:
                result = domain_db.list_domain_definitions(
                    connection=connection,
                    definition_type=definition_type,
                    filters=definition,
                    limit=int(payload.get("limit") or 200),
                    offset=int(payload.get("offset") or 0),
                    include_inactive=bool(payload.get("include_inactive", False)),
                )
            return {
                "action": "domain_definition_crud",
                **result,
                "resolved": {
                    "operation": operation,
                    "definition_type": definition_type,
                    "definition": definition,
                },
            }

        validation = domain_db.validate_domain_definition_payload(
            definition_type=definition_type,
            payload=definition,
            operation=operation,
        )
        if not validation.get("valid"):
            return {
                "action": "domain_definition_crud",
                "success": False,
                "message": "Validation failed for domain definition payload.",
                "validation": validation,
                "expected_fields": self._expected_fields_for_definition(definition_type, operation),
                "resolved": {
                    "operation": operation,
                    "definition_type": definition_type,
                    "definition": definition,
                    "llm_payload": llm_payload,
                },
            }

        with self.get_connection() as connection:
            if operation == "create":
                result = domain_db.create_domain_definition(connection, definition_type, definition)
            elif operation == "update":
                result = domain_db.update_domain_definition(connection, definition_type, definition)
            elif operation == "delete":
                result = domain_db.delete_domain_definition(connection, definition_type, definition)
            else:
                result = domain_db.get_domain_definition(connection, definition_type, definition)

        return {
            "action": "domain_definition_crud",
            **result,
            "resolved": {
                "operation": operation,
                "definition_type": definition_type,
                "definition": definition,
            },
        }

    def _log_genai_json_response(self, call_name: str, model: str, payload: Any) -> None:
        try:
            payload_text = json.dumps(payload, ensure_ascii=False)
        except Exception:
            payload_text = str(payload)
        self.logger.info(
            "genai_json_response call=%s model=%s payload=%s",
            call_name,
            model,
            payload_text,
        )

    def _planner_step(self, question: str, state: dict[str, Any], step_index: int) -> dict[str, Any]:
        workflow_hint = self._as_string_key_dict(
            self._invoke_solf_clause_raw(
                "business_rule_workflow_execute",
                [{"process": "query_planning", "has_scope": bool(state.get("scope")), "candidates": state.get("scope") or {}}],
            )
        )
        control_flow = str(workflow_hint.get("control_flow") or "").strip().lower()
        if control_flow == "selection":
            selection_plan = self._as_string_key_dict(
                self._invoke_solf_clause_raw(
                    "business_rule_selection_plan",
                    [{"process": "query_planning", "has_scope": bool(state.get("scope"))}],
                )
            )
            next_action = str(selection_plan.get("next_action") or "").strip().lower()
            if next_action in {"resolve_scope", "lookup_attribute", "search_discovery", "search_criteria", "finalize"}:
                return {
                    "action": next_action,
                    "attribute_name": state.get("parsed", {}).get("attribute_name"),
                    "entity_name": state.get("parsed", {}).get("entity_name"),
                    "rationale": "Business-rule selection workflow routing.",
                }

        if control_flow == "sequence" and step_index > 0 and state.get("attribute_result"):
            return {
                "action": "finalize",
                "attribute_name": state.get("parsed", {}).get("attribute_name"),
                "entity_name": state.get("parsed", {}).get("entity_name"),
                "rationale": "Business-rule sequence workflow finalized after grounded attribute result.",
            }

        if control_flow == "iteration" and step_index == 0:
            return {
                "action": "search_criteria",
                "attribute_name": state.get("parsed", {}).get("attribute_name"),
                "entity_name": state.get("parsed", {}).get("entity_name"),
                "rationale": "Business-rule iteration workflow starts with criteria scan.",
            }

        if control_flow == "backtracking" and step_index == 0 and not state.get("scope"):
            return {
                "action": "resolve_scope",
                "attribute_name": state.get("parsed", {}).get("attribute_name"),
                "entity_name": state.get("parsed", {}).get("entity_name"),
                "rationale": "Business-rule backtracking workflow resolves broad scope first.",
            }

        style_data = state.get("query_style") if isinstance(state.get("query_style"), dict) else {}
        preferred_action = str(style_data.get("initial_action") or "").strip().lower()
        if step_index == 0 and preferred_action in {
            "resolve_scope",
            "search_discovery",
            "lookup_attribute",
            "search_criteria",
            "finalize",
        }:
            return {
                "action": preferred_action,
                "attribute_name": state.get("parsed", {}).get("attribute_name"),
                "entity_name": state.get("parsed", {}).get("entity_name"),
                "rationale": "Style-routed initial action.",
            }

        # Proactively route media-oriented questions to Discovery first.
        parsed_payload = state.get("parsed") if isinstance(state.get("parsed"), dict) else {}
        parsed_confidence = parsed_payload.get("confidence")
        media_hint = self._is_media_intent(question, confidence=parsed_confidence)
        if step_index == 0 and media_hint.get("media_intent"):
            return {
                "action": "search_discovery",
                "attribute_name": state.get("parsed", {}).get("attribute_name"),
                "entity_name": state.get("parsed", {}).get("entity_name"),
                "rationale": str(media_hint.get("rationale") or "Question appears media-oriented, so use Discovery search first."),
            }

        prompt = (
            "You are a retrieval planner for IDMS.\n"
            "Plan exactly one next action for a tool-using loop.\n"
            "Return JSON only with keys: action, attribute_name, entity_name, rationale.\n"
            "Allowed action values: resolve_scope, search_discovery, lookup_attribute, search_criteria, finalize.\n"
            "Rules:\n"
            "- Use resolve_scope first when scope is missing.\n"
            "- Use search_discovery when media-oriented or Discovery-specific results should be inspected directly.\n"
            "- Use lookup_attribute when a concrete attribute should be searched.\n"
            "- Use search_criteria when the query asks for list/filter matches.\n"
            "- Use finalize once enough grounded evidence exists.\n\n"
            f"Question: {question}\n"
            f"Step index: {step_index}\n"
            f"State JSON: {json.dumps(state, ensure_ascii=False)}"
        )
        selected_model = self._select_model_by_confidence(parsed_confidence, default=EXTRACT_MODEL)
        self._record_model_usage("interaction_planner_step", selected_model, parsed_confidence)
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: self.client.models.generate_content(
                model=selected_model,
                contents=[prompt],
            ),
            model=selected_model,
            contents=[prompt],
            temperature=0.0,
            call_name="interaction_planner_step",
            complexity="simple",
        )
        self.logger.info("GenAI planner call completed for step=%s", step_index)
        decision = self._extract_json_object(response.text or "")
        self._log_genai_json_response("planner", selected_model, decision)
        action = str(decision.get("action") or "").strip().lower()
        if action not in {"resolve_scope", "search_discovery", "lookup_attribute", "search_criteria", "finalize"}:
            if not state.get("scope"):
                action = "resolve_scope"
            elif self._scope_is_empty(state.get("scope")):
                action = "finalize"
            elif state.get("parsed", {}).get("intent") == "criteria_lookup" and not state.get("criteria_result"):
                action = "search_criteria"
            elif not state.get("attribute_result"):
                action = "lookup_attribute"
            else:
                action = "finalize"
        decision["action"] = action
        return decision

    def _classify_query_style(self, question: str, parsed: dict[str, Any]) -> dict[str, Any]:
        intent = str(parsed.get("intent") or "").strip().lower()
        attribute_name = str(parsed.get("attribute_name") or "").strip().lower()
        question_lower = str(question or "").strip().lower()
        criteria = parsed.get("criteria") if isinstance(parsed.get("criteria"), dict) else {}
        has_time_window = isinstance(criteria.get("time_window"), dict) and bool(criteria["time_window"])

        if intent == "criteria_lookup" and has_time_window:
            style_hint = "temporal_filtered_criteria"
            initial_action = "search_criteria"
        elif intent == "criteria_lookup":
            style_hint = "criteria_list"
            initial_action = "search_criteria"
        elif attribute_name in {"document_filename", "document_full_path", "document_file_info"} or "document" in question_lower:
            style_hint = "document_retrieval"
            initial_action = "lookup_attribute"
        elif intent == "attribute_lookup":
            style_hint = "attribute_value"
            initial_action = "lookup_attribute"
        else:
            style_hint = "semantic_description"
            initial_action = "resolve_scope"

        payload = {
            "question": question,
            "parsed": parsed,
            "style_hint": style_hint,
            "initial_action": initial_action,
        }
        result = self._invoke_solf_clause_raw("query_style_policy", [payload])
        style_result = self._as_string_key_dict(result)
        mode = str(style_result.get("mode") or "allow").strip().lower()
        if mode not in {"allow", "deny", "escalate"}:
            mode = "allow"

        return {
            "mode": mode,
            "reason": str(style_result.get("reason") or "default_query_style_policy"),
            "query_style": str(style_result.get("query_style") or style_hint),
            "initial_action": str(style_result.get("initial_action") or initial_action),
            "message": str(style_result.get("message") or ""),
            "policy_clause": "query_style_policy",
        }

    def _is_media_intent(self, question: str, confidence: Any = None) -> dict[str, Any]:
        lowered = str(question or "").strip().lower()
        media_tokens = {
            "image",
            "photo",
            "picture",
            "audio",
            "voice",
            "recording",
            "video",
            "clip",
            "transcript",
            "scene",
            "frame",
        }
        token_match = any(token in lowered for token in media_tokens)

        # If keyword match is clear, return immediately without LLM (saves embedding cost).
        if token_match:
            return {
                "media_intent": True,
                "rationale": "Detected media-oriented intent (keyword-based) for Discovery-first planning.",
            }

        # For ambiguous cases, use LLM only when keywords don't provide clear signal.
        prompt = (
            "Classify whether the question is primarily asking about media content "
            "(image/audio/video/transcript/scene/frame) rather than structured attributes.\n"
            "Return JSON only with keys: media_intent, rationale.\n"
            f"Question: {question}"
        )
        try:
            self.logger.info("GenAI media intent classification requested")
            selected_model = self._select_model_by_confidence(confidence, default=EXTRACT_MODEL)
            self._record_model_usage("interaction_media_intent", selected_model, confidence)
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: self.client.models.generate_content(
                    model=selected_model,
                    contents=[prompt],
                ),
                model=selected_model,
                contents=[prompt],
                temperature=0.0,
                call_name="interaction_media_intent",
                complexity="simple",
            )
            parsed = self._extract_json_object(response.text or "")
            self._log_genai_json_response("media_intent", selected_model, parsed)
            llm_value = bool(parsed.get("media_intent"))
            rationale = str(parsed.get("rationale") or "").strip()
            return {
                "media_intent": llm_value,
                "rationale": rationale or "LLM-classified media intent for Discovery-first planning.",
            }
        except Exception:
            return {
                "media_intent": False,
                "rationale": "Ambiguous intent; proceeding with default routing.",
            }

    def _answer_sources(self, grounding: dict[str, Any]) -> list[str]:
        attribute_result = grounding.get("attribute_result") if isinstance(grounding, dict) else None
        criteria_result = grounding.get("criteria_result") if isinstance(grounding, dict) else None
        criteria_contextual_result = grounding.get("criteria_contextual_result") if isinstance(grounding, dict) else None
        scope = grounding.get("scope") if isinstance(grounding, dict) else None
        discovery_search = grounding.get("discovery_search") if isinstance(grounding, dict) else None
        ordered_sources: list[str] = []

        def _append_source(value: str) -> None:
            if value not in ordered_sources:
                ordered_sources.append(value)

        if isinstance(attribute_result, dict):
            source = str(attribute_result.get("source") or "").strip().lower()
            if source == "discovery":
                _append_source("discovery")
            if attribute_result.get("doc_id") is not None:
                _append_source("sql")
            if attribute_result.get("node_id") is not None:
                _append_source("qdrant_sql")

        if isinstance(criteria_result, dict) and int(criteria_result.get("count", 0) or 0) > 0:
            _append_source("criteria_sql")

        if isinstance(criteria_contextual_result, dict) and str(criteria_contextual_result.get("answer") or "").strip():
            _append_source("criteria_contextual_llm")

        if isinstance(discovery_search, dict) and int(discovery_search.get("candidate_count", 0) or 0) > 0:
            _append_source("discovery")

        if isinstance(scope, dict) and int(scope.get("candidate_count", 0) or 0) > 0:
            _append_source("qdrant")

        if isinstance(scope, dict) and int(scope.get("discovery_candidate_count", 0) or 0) > 0:
            _append_source("discovery")

        if not ordered_sources:
            ordered_sources.append("not_found")

        return ordered_sources

    def _deterministic_criteria_answer(self, question: str, grounding: dict[str, Any]) -> str | None:
        """Build a deterministic list-style answer for criteria matches.

        This avoids count-only LLM summaries for explicit document/note listing asks.
        """
        if not isinstance(grounding, dict):
            return None

        criteria_result = grounding.get("criteria_result")
        if not isinstance(criteria_result, dict):
            return None
        matches = list(criteria_result.get("matches") or [])
        if not matches:
            return None

        parsed = grounding.get("parsed") if isinstance(grounding.get("parsed"), dict) else {}
        parsed_criteria = parsed.get("criteria") if isinstance(parsed.get("criteria"), dict) else {}
        response_format = str(parsed_criteria.get("response_format") or "").strip().lower()

        lines: list[str] = []
        for item in matches[:8]:
            if not isinstance(item, dict):
                continue
            label = str(
                item.get("entity_name")
                or item.get("doc_name")
                or item.get("title")
                or item.get("object_name")
                or ""
            ).strip()
            if not label:
                continue

            theme = str(item.get("entity_type") or item.get("theme") or "").strip()
            doc_name = str(item.get("doc_name") or "").strip()
            doc_path = str(item.get("doc_path") or "").strip()
            description = str(item.get("description") or item.get("doc_desc") or "").strip()
            matched_terms = item.get("matched_terms") if isinstance(item.get("matched_terms"), list) else []

            extras: list[str] = []
            if response_format == "doc_titles_and_files":
                if doc_name and doc_name.lower() != label.lower():
                    extras.append(f"file: {doc_name}")
                elif doc_name:
                    extras.append(f"file: {doc_name}")
                if doc_path:
                    extras.append(f"path: {doc_path}")
            else:
                if theme:
                    extras.append(theme)
                if doc_name and doc_name.lower() != label.lower():
                    extras.append(f"file: {doc_name}")
                if description:
                    extras.append(description[:180])
                elif matched_terms:
                    extras.append("matched: " + ", ".join(str(x) for x in matched_terms[:4]))

            if extras:
                lines.append(f"- {label} ({'; '.join(extras)})")
            else:
                lines.append(f"- {label}")

        if not lines:
            return None

        prompt_lower = str(question or "").lower()
        if "note" in prompt_lower or "notiz" in prompt_lower:
            intro = "Here are the matching notes:"
        else:
            intro = "Here are the matching documents:"

        return intro + "\n" + "\n".join(lines)

    def _is_value_question(self, question: str, parsed: Any | None = None) -> bool:
        normalized = str(question or "").strip().lower()
        if not normalized:
            return False

        value_pattern = re.compile(
            r"\b(?:how\s+much|amount|total|cost|price|value|due|sum|payable|balance|outstanding|net|gross|vat|tax|fee|charge)\b",
            flags=re.IGNORECASE,
        )
        asks_value = bool(value_pattern.search(normalized))

        attribute_hint = ""
        if parsed is not None:
            try:
                attribute_hint = str(getattr(parsed, "attribute_name", "") or "").strip().lower()
            except Exception:
                attribute_hint = ""
        if attribute_hint and value_pattern.search(attribute_hint):
            asks_value = True

        return asks_value

    def _is_phone_like_answer(self, text: str) -> bool:
        normalized = str(text or "").strip().lower()
        if not normalized:
            return False
        has_phone_hint = bool(
            re.search(
                r"\b(?:phone|telephone|telefon|mobile\s+number|telefonnummer|contact\s+number)\b",
                normalized,
                flags=re.IGNORECASE,
            )
        )
        has_phone_digits = bool(re.search(r"(?:\+\d[\d\s()./-]{6,}|\b\d{2,4}[\s-]\d{2,4}[\s-]\d{2,4}\b)", normalized))
        has_money_hint = bool(
            re.search(
                r"\b(?:chf|eur|usd|gbp|amount|total|due|cost|price|value|betrag|gesamt|kosten|preis)\b",
                normalized,
                flags=re.IGNORECASE,
            )
        )
        return bool((has_phone_hint or has_phone_digits) and not has_money_hint)

    def _is_value_like_answer(self, text: str) -> bool:
        normalized = str(text or "").strip().lower()
        if not normalized:
            return False
        if not re.search(r"\d", normalized):
            return False
        if self._is_phone_like_answer(normalized):
            return False

        has_value_hint = bool(
            re.search(
                r"(?:\b(?:chf|eur|usd|gbp|amount|total|due|cost|price|value|balance|outstanding|paid|payable|betrag|gesamt|kosten|preis)\b|[$€£])",
                normalized,
                flags=re.IGNORECASE,
            )
        )
        has_statement_hint = bool(re.search(r"\b(?:was|is|are|equals?|total(?:ed)?|amount)\b", normalized, flags=re.IGNORECASE))
        return bool(has_value_hint or has_statement_hint)

    def _is_generic_entity_value_answer(self, text: str, entity_name: str | None) -> bool:
        normalized = str(text or "").strip().lower()
        entity_norm = str(entity_name or "").strip().lower()
        if not normalized or not entity_norm:
            return False

        escaped_entity = re.escape(entity_norm)
        generic_patterns = [
            rf"\b(?:the\s+)?(?:total\s+)?amount\s+for\s+{escaped_entity}\s+(?:is|was)\b",
            rf"\b(?:the\s+)?(?:total\s+)?value\s+for\s+{escaped_entity}\s+(?:is|was)\b",
            rf"\b(?:the\s+)?(?:total\s+)?bill\s+for\s+{escaped_entity}\s+(?:is|was)\b",
            rf"\b{escaped_entity}\s*[-:]?\s*>?\s*(?:credit|debit)?\s*total\b",
        ]
        return any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in generic_patterns)

    def _build_generic_value_rewrite_candidates(self, question: str, parsed: Any | None = None) -> list[str]:
        normalized_question = str(question or "").strip()
        if not normalized_question:
            return []

        rewrites: list[str] = []
        patterns = [
            r"(?is)^\s*what\s+(?:was|is)\s+the\s+(.+?)\s+for\s+(.+?)\s*[?.!]?\s*$",
            r"(?is)^\s*what\s+(?:was|is)\s+(.+?)\s+for\s+(.+?)\s*[?.!]?\s*$",
            r"(?is)^\s*what\s+is\s+the\s+total\s+for\s+(.+?)\s*[?.!]?\s*$",
        ]
        for pattern in patterns:
            match = re.match(pattern, normalized_question)
            if not match:
                continue
            if len(match.groups()) == 2:
                phrase = str(match.group(1) or "").strip()
                entity = str(match.group(2) or "").strip()
                if phrase and entity:
                    rewrites.append(f"How much was the {phrase} for {entity}?")
            elif len(match.groups()) == 1:
                entity = str(match.group(1) or "").strip()
                if entity:
                    rewrites.append(f"How much was the total amount for {entity}?")

        attribute_hint = ""
        entity_hint = ""
        if parsed is not None:
            try:
                attribute_hint = str(getattr(parsed, "attribute_name", "") or "").strip()
            except Exception:
                attribute_hint = ""
            try:
                entity_hint = str(getattr(parsed, "entity_name", "") or "").strip()
            except Exception:
                entity_hint = ""

        if attribute_hint and entity_hint:
            rewrites.append(f"How much was the {attribute_hint} for {entity_hint}?")

        normalized_lower = normalized_question.lower()
        if entity_hint and re.search(r"\b(?:bill|invoice|receipt|payment|charge|fee|rechnung|beleg|zahlung|quittung)\b", normalized_lower, flags=re.IGNORECASE):
            qualifier_tokens = [
                token
                for token in re.findall(r"[a-zA-Z][a-zA-Z0-9_+-]{2,}", normalized_lower)
                if token
                not in {
                    "what", "was", "is", "the", "for", "how", "much", "amount", "value", "total",
                    "cost", "price", "due", "sum", "bill", "invoice", "receipt", "payment", "charge",
                    "fee", "rechnung", "beleg", "zahlung", "quittung",
                }
                and token not in {t.lower() for t in re.findall(r"[a-zA-Z][a-zA-Z0-9_+-]{2,}", entity_hint.lower())}
            ]
            qualifier_phrase = " ".join(qualifier_tokens[:3]).strip()
            if qualifier_phrase:
                rewrites.append(f"What was the {qualifier_phrase} invoice total for {entity_hint}?")
            rewrites.append(f"What was the invoice total for {entity_hint}?")

        parsed_criteria: dict[str, Any] = {}
        if parsed is not None:
            try:
                parsed_criteria = getattr(parsed, "criteria", {}) or {}
            except Exception:
                parsed_criteria = {}
        if isinstance(parsed_criteria, dict):
            must_contain_terms = [
                str(item).strip()
                for item in list(parsed_criteria.get("must_contain") or [])
                if str(item).strip()
            ]
            if must_contain_terms and entity_hint:
                compact_focus = " ".join(must_contain_terms[:4]).strip()
                if compact_focus:
                    rewrites.append(f"How much was the {compact_focus} amount for {entity_hint}?")

        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in rewrites:
            normalized = ""
            try:
                normalized = self.query_engine._normalize_semantic_text(candidate)
            except Exception:
                normalized = candidate.strip().lower()
            if not normalized or normalized in seen:
                continue
            try:
                same_as_original = (
                    normalized
                    == self.query_engine._normalize_semantic_text(normalized_question)
                )
            except Exception:
                same_as_original = normalized == normalized_question.strip().lower()
            if same_as_original:
                continue
            seen.add(normalized)
            deduped.append(candidate)

        return deduped

    def _extract_value_focus_terms(self, question: str, parsed: Any | None = None) -> list[str]:
        normalized_question = str(question or "").strip().lower()
        if not normalized_question:
            return []

        generic_value_terms = {
            "how",
            "much",
            "what",
            "was",
            "is",
            "the",
            "a",
            "an",
            "for",
            "of",
            "and",
            "or",
            "to",
            "from",
            "amount",
            "total",
            "cost",
            "price",
            "value",
            "due",
            "sum",
            "bill",
            "invoice",
            "payment",
            "charge",
            "fee",
            "rechnung",
            "betrag",
            "kosten",
            "preis",
            "gesamt",
            "services",
            "service",
        }

        entity_tokens: set[str] = set()
        attribute_tokens: list[str] = []
        criteria_tokens: list[str] = []
        if parsed is not None:
            try:
                entity_hint = str(getattr(parsed, "entity_name", "") or "").strip().lower()
            except Exception:
                entity_hint = ""
            if entity_hint:
                entity_tokens = {
                    token
                    for token in re.findall(r"[a-zA-Z][a-zA-Z0-9_+-]{1,}", entity_hint)
                    if token
                }

            try:
                attribute_hint = str(getattr(parsed, "attribute_name", "") or "").strip().lower()
            except Exception:
                attribute_hint = ""
            if attribute_hint:
                attribute_tokens.extend(re.findall(r"[a-zA-Z][a-zA-Z0-9_+-]{1,}", attribute_hint))

            try:
                parsed_criteria = getattr(parsed, "criteria", {}) or {}
            except Exception:
                parsed_criteria = {}
            if isinstance(parsed_criteria, dict):
                for item in list(parsed_criteria.get("must_contain") or []):
                    criteria_tokens.extend(re.findall(r"[a-zA-Z][a-zA-Z0-9_+-]{1,}", str(item).lower()))

        question_tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9_+-]{1,}", normalized_question)
        candidate_tokens = list(question_tokens) + attribute_tokens + criteria_tokens

        focus_terms: list[str] = []
        seen: set[str] = set()
        for token in candidate_tokens:
            if not token:
                continue
            if token in seen:
                continue
            if token in generic_value_terms:
                continue
            if token in entity_tokens:
                continue
            if len(token) < 3:
                continue
            seen.add(token)
            focus_terms.append(token)

        return focus_terms[:8]

    def _try_generic_value_contextual_resolution(
        self,
        question: str,
        parsed: Any,
        candidates: list[dict[str, Any]],
        discovery_results: list[dict[str, Any]],
        telemetry: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self._is_value_question(question, parsed):
            return None

        focus_terms = self._extract_value_focus_terms(question, parsed)
        has_qualified_focus = bool(focus_terms)

        try:
            entity_hint = str(getattr(parsed, "entity_name", "") or "").strip()
        except Exception:
            entity_hint = ""

        def _is_generic_entity_value_answer(text: str) -> bool:
            normalized = str(text or "").strip().lower()
            if not normalized or not entity_hint:
                return False
            entity_norm = entity_hint.strip().lower()
            if not entity_norm:
                return False
            escaped_entity = re.escape(entity_norm)
            generic_patterns = [
                rf"\b(?:the\s+)?(?:total\s+)?amount\s+for\s+{escaped_entity}\s+(?:is|was)\b",
                rf"\b(?:the\s+)?(?:total\s+)?value\s+for\s+{escaped_entity}\s+(?:is|was)\b",
                rf"\b(?:the\s+)?(?:total\s+)?bill\s+for\s+{escaped_entity}\s+(?:is|was)\b",
                rf"\b{escaped_entity}\s*[-:]?\s*>?\s*(?:credit|debit)?\s*total\b",
            ]
            return any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in generic_patterns)

        attempts = [str(question or "").strip()]
        attempts.extend(self._build_generic_value_rewrite_candidates(question, parsed))

        for attempt_question in attempts:
            if not attempt_question:
                continue
            try:
                resolved = self.query_engine._contextual_llm_resolution_fallback(
                    question=attempt_question,
                    parsed=parsed,
                    candidates=candidates,
                    discovery_results=discovery_results,
                )
                if isinstance(telemetry, dict):
                    telemetry["contextual_fallback_calls"] = int(telemetry.get("contextual_fallback_calls", 0)) + 1
            except Exception:
                resolved = None

            if not isinstance(resolved, dict):
                continue

            answer_text = str(resolved.get("answer") or "").strip()
            answer_source = str(resolved.get("source") or "").strip().lower()
            if not answer_text:
                continue
            if answer_source in {"criteria_sql", "criteria_docs_sql"}:
                continue
            if not self._is_value_like_answer(answer_text):
                continue
            if has_qualified_focus and _is_generic_entity_value_answer(answer_text):
                continue

            resolved_payload = dict(resolved)
            if attempt_question != str(question or "").strip():
                resolved_payload["rewrite_question"] = attempt_question
            return resolved_payload

        return None

    @staticmethod
    def _normalize_requirement_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        text = re.sub(r"[^a-z0-9]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def _collect_query_requirements(self, parsed: Any) -> list[dict[str, Any]]:
        """Derive the checkable requirements implied by a parsed query."""
        criteria = getattr(parsed, "criteria", None)
        criteria = criteria if isinstance(criteria, dict) else {}
        requirements: list[dict[str, Any]] = []
        seen: set[str] = set()

        def _add(kind: str, value: Any) -> None:
            normalized = self._normalize_requirement_text(value)
            if not normalized:
                return
            key = f"{kind}:{normalized}"
            if key in seen:
                return
            seen.add(key)
            requirements.append({"key": key, "kind": kind, "value": str(value).strip(), "normalized": normalized})

        _add("entity_type", criteria.get("entity_type"))
        _add("relationship", criteria.get("relationship_concept"))
        _add("relationship", getattr(parsed, "relation_name", None))
        _add("attribute", getattr(parsed, "attribute_name", None))
        _add("filter", criteria.get("include_country"))
        _add("filter", criteria.get("exclude_country"))

        for term in list(criteria.get("must_contain") or []):
            _add("term", term)

        for item in list(criteria.get("relation_filters") or []):
            if not isinstance(item, dict):
                continue
            for name in list(item.get("relationship_names") or []):
                _add("relationship", name)
            _add("term", item.get("target_name"))

        return requirements

    def _requirement_satisfied(self, requirement: dict[str, Any], haystack: str) -> bool:
        normalized = str(requirement.get("normalized") or "").strip()
        if not normalized:
            return True
        if normalized in haystack:
            return True
        tokens = [token for token in normalized.split(" ") if len(token) > 2]
        if not tokens:
            return False
        return all(token in haystack for token in tokens)

    # Echoed request context must never count as evidence that a requirement was met.
    _NON_EVIDENCE_GROUNDING_KEYS = {"parsed", "query_style", "policy", "criteria"}

    def _collect_evidence_payload(self, query_result: dict[str, Any] | None) -> dict[str, Any]:
        result = query_result if isinstance(query_result, dict) else {}
        grounding = result.get("grounding") if isinstance(result.get("grounding"), dict) else {}
        return {
            "answer": result.get("answer"),
            "data": result.get("data"),
            "grounding_evidence": {
                key: value
                for key, value in grounding.items()
                if key not in self._NON_EVIDENCE_GROUNDING_KEYS
            },
        }

    def _collect_evidence_rows(self, evidence_payload: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        def _walk(value: Any, depth: int = 0) -> None:
            if depth > 5 or len(rows) > 500:
                return
            if isinstance(value, dict):
                rows.append(value)
                for item in value.values():
                    _walk(item, depth + 1)
            elif isinstance(value, list):
                for item in value[:200]:
                    _walk(item, depth + 1)

        _walk(evidence_payload.get("data"))
        _walk(evidence_payload.get("grounding_evidence"))
        return rows

    @staticmethod
    def _value_is_present(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, (list, tuple, set, dict)):
            return bool(value)
        return bool(str(value).strip())

    def _requirement_has_structural_evidence(
        self,
        requirement: dict[str, Any],
        rows: list[dict[str, Any]],
    ) -> bool:
        normalized = str(requirement.get("normalized") or "").strip()
        tokens = [token for token in normalized.split(" ") if len(token) > 2] or [normalized]
        kind = str(requirement.get("kind") or "").strip()
        label_keys = ("relationship_name",) if kind == "relationship" else ("attribute_name", "attribute_key")
        value_keys = ("attribute_value", "value")

        for row in rows:
            if not isinstance(row, dict):
                continue
            if kind == "relationship":
                row_relationships = [
                    row.get("relationship_name"),
                    *[key for key, value in row.items() if isinstance(value, (list, dict)) and self._value_is_present(value)],
                ]
                direction_tokens = {"has", "of", "for", "from", "to", "is"}
                def _relationship_token(value: str) -> str:
                    token = str(value or "").strip()
                    if token.endswith("s") and len(token) > 3:
                        token = token[:-1]
                    return token
                required_tokens = {
                    _relationship_token(token) for token in tokens if token not in direction_tokens
                }
                for relationship in row_relationships:
                    relationship_tokens = {
                        _relationship_token(token)
                        for token in self._normalize_requirement_text(relationship).split(" ")
                        if token not in direction_tokens
                    }
                    if required_tokens and required_tokens.issubset(relationship_tokens):
                        return True
            for key, value in row.items():
                normalized_key = self._normalize_requirement_text(key)
                if not normalized_key:
                    continue
                if all(token in normalized_key for token in tokens) and self._value_is_present(value):
                    return True
            for label_key in label_keys:
                label = self._normalize_requirement_text(row.get(label_key))
                if not label or not all(token in label for token in tokens):
                    continue
                if kind == "relationship":
                    return True
                if any(self._value_is_present(row.get(value_key)) for value_key in value_keys):
                    return True
        return False

    def _assess_criteria_coverage(
        self,
        requirements: list[dict[str, Any]],
        query_result: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Report which parsed requirements are actually reflected in a query result."""
        if not requirements:
            return {"required": 0, "satisfied": [], "unmet": [], "complete": True}

        evidence_payload = self._collect_evidence_payload(query_result)
        rows = self._collect_evidence_rows(evidence_payload)
        try:
            haystack = self._normalize_requirement_text(json.dumps(evidence_payload, ensure_ascii=False, default=str))
        except Exception:
            haystack = self._normalize_requirement_text(str(evidence_payload))

        satisfied: list[dict[str, Any]] = []
        unmet: list[dict[str, Any]] = []
        for requirement in requirements:
            kind = str(requirement.get("kind") or "").strip()
            if kind in {"relationship", "attribute"}:
                # Retrieved data must prove these, since prose can mention them while lacking values.
                is_satisfied = self._requirement_has_structural_evidence(requirement, rows)
            else:
                is_satisfied = self._requirement_satisfied(requirement, haystack)
            if is_satisfied:
                satisfied.append(requirement)
            else:
                unmet.append(requirement)

        return {
            "required": len(requirements),
            "satisfied": satisfied,
            "unmet": unmet,
            "complete": not unmet,
        }

    @staticmethod
    def _requirement_subject(parsed: Any) -> str:
        criteria = getattr(parsed, "criteria", None)
        criteria = criteria if isinstance(criteria, dict) else {}
        subject = str(criteria.get("entity_type") or "").strip()
        if not subject:
            subject = str(getattr(parsed, "entity_name", "") or "").strip()
        return subject or "matched records"

    def _describe_unmet_requirement(self, requirement: dict[str, Any], subject: str) -> str:
        kind = str(requirement.get("kind") or "").strip()
        value = str(requirement.get("value") or "").strip()
        if not value:
            return ""
        if kind == "relationship":
            return f"no '{value}' found for the matched {subject}"
        if kind == "attribute":
            return f"attribute '{value}' was not available for the matched {subject}"
        if kind == "term":
            return f"no results matched '{value}'"
        if kind == "filter":
            return f"filter '{value}' could not be confirmed against the retrieved data"
        if kind == "entity_type":
            return f"no '{value}' records were found"
        return f"'{value}' could not be resolved"

    def _consolidate_query_result(
        self,
        question: str,
        result: dict[str, Any],
        parsed_override: Any | None = None,
    ) -> dict[str, Any]:
        """Attach evidence and explain any query criteria the result does not cover."""
        enriched = dict(result) if isinstance(result, dict) else {"answer": ""}

        try:
            parsed = parsed_override if parsed_override is not None else self.query_engine.parse_query(question)
        except Exception:
            parsed = None

        if parsed is None:
            return enriched

        try:
            bundle = self._build_query_evidence_bundle(question, enriched, parsed_override=parsed)
        except Exception:
            self.logger.exception("criteria_consolidation_failed question=%s", question)
            return enriched

        enriched["evidence_bundle"] = bundle

        coverage = bundle.get("criteria_coverage") if isinstance(bundle.get("criteria_coverage"), dict) else {}
        enriched["criteria_coverage"] = coverage

        unmet = [item for item in list(coverage.get("unmet") or []) if isinstance(item, dict)]
        if unmet and not enriched.get("_criteria_note_applied"):
            subject = self._requirement_subject(parsed)
            reasons = [text for text in (self._describe_unmet_requirement(item, subject) for item in unmet) if text]
            if reasons:
                answer_text = str(enriched.get("answer") or "").strip()
                note = "Unmet query criteria: " + "; ".join(reasons) + "."
                enriched["answer"] = f"{answer_text} {note}".strip()
                enriched["_criteria_note_applied"] = True

        return enriched

    def _build_query_evidence_bundle(
        self,
        question: str,
        query_result: dict[str, Any] | None,
        parsed_override: Any | None = None,
    ) -> dict[str, Any]:
        parsed = parsed_override if parsed_override is not None else self.query_engine.parse_query(question)
        parsed_data = {
            "intent": getattr(parsed, "intent", None),
            "entity_name": getattr(parsed, "entity_name", None),
            "attribute_name": getattr(parsed, "attribute_name", None),
            "relation_name": getattr(parsed, "relation_name", None),
            "confidence": getattr(parsed, "confidence", None),
            "language": getattr(parsed, "language", None),
            "criteria": getattr(parsed, "criteria", None) if isinstance(getattr(parsed, "criteria", None), dict) else {},
        }

        result_data = query_result.get("data") if isinstance(query_result, dict) and isinstance(query_result.get("data"), dict) else {}
        answer_sources: list[str] = []
        if isinstance(query_result, dict) and isinstance(query_result.get("answer_sources"), list):
            answer_sources = [str(item).strip() for item in query_result.get("answer_sources") if str(item).strip()]
        elif isinstance(query_result, dict):
            answer_sources = self._answer_sources(query_result.get("grounding") if isinstance(query_result.get("grounding"), dict) else {})

        provenance_snapshot = self._extract_provenance_snapshot(query_result)

        scope: dict[str, Any] = {}
        if isinstance(query_result, dict):
            grounding_payload = query_result.get("grounding") if isinstance(query_result.get("grounding"), dict) else {}
            if isinstance(grounding_payload.get("scope"), dict):
                scope = dict(grounding_payload.get("scope") or {})

        if not scope:
            semantic_sources = {
                "qdrant",
                "qdrant_sql",
                "qdrant_sql_fallback",
                "qdrant_semantic",
                "discovery",
                "discovery_fallback",
                "discovery_semantic",
                "semantic_llm_fallback",
                "contextual_llm_resolution_fallback",
                "criteria_contextual_llm",
            }
            source_hints = {
                str(item).strip().lower()
                for item in answer_sources
                if str(item).strip()
            }
            if isinstance(query_result, dict):
                source_hints.add(str(query_result.get("source") or "").strip().lower())
                source_hints.add(str(query_result.get("answer_source") or "").strip().lower())

            if any(item in semantic_sources for item in source_hints):
                try:
                    scope = self.query_engine.resolve_search_scope(question)
                except Exception:
                    scope = {}

        entities: list[str] = []
        attributes: list[str] = []
        relationships: list[str] = []

        def _append_unique(target: list[str], value: Any) -> None:
            text = str(value or "").strip()
            if not text or text in target:
                return
            target.append(text)

        _append_unique(entities, parsed_data.get("entity_name"))
        _append_unique(attributes, parsed_data.get("attribute_name"))
        _append_unique(relationships, parsed_data.get("relation_name"))

        if isinstance(result_data, dict):
            _append_unique(entities, result_data.get("entity_name"))
            _append_unique(entities, result_data.get("source_entity_name"))
            _append_unique(entities, result_data.get("related_entity_name"))
            _append_unique(attributes, result_data.get("attribute_name"))
            _append_unique(attributes, result_data.get("attribute_key"))
            _append_unique(relationships, result_data.get("relationship_name"))

        if isinstance(scope, dict):
            for item in list(scope.get("objects") or []):
                if isinstance(item, dict):
                    _append_unique(entities, item.get("object_name") or item.get("entity_name"))
            for item in list(scope.get("documents") or []):
                if isinstance(item, dict):
                    _append_unique(entities, item.get("doc_name") or item.get("doc_key"))
            for item in list(scope.get("relationships") or []):
                if isinstance(item, dict):
                    _append_unique(relationships, item.get("relationship_name"))

        for item in provenance_snapshot:
            if not isinstance(item, dict):
                continue
            _append_unique(entities, item.get("entity_name"))
            _append_unique(entities, item.get("related_entity_name"))
            _append_unique(attributes, item.get("attribute_name"))
            _append_unique(relationships, item.get("relationship_name"))

        requirements = self._collect_query_requirements(parsed)
        criteria_coverage = self._assess_criteria_coverage(requirements, query_result)
        if not criteria_coverage.get("complete"):
            self.logger.warning(
                "criteria_coverage_incomplete question=%s unmet=%s",
                question,
                [item.get("key") for item in criteria_coverage.get("unmet") or []],
            )

        return {
            "parsed": parsed_data,
            "criteria_coverage": criteria_coverage,
            "route": {
                "source": str(query_result.get("source") or "") if isinstance(query_result, dict) else "",
                "answer_source": str(query_result.get("answer_source") or "") if isinstance(query_result, dict) else "",
                "answer_sources": answer_sources,
                "candidate_count": int(query_result.get("candidate_count", 0) or 0) if isinstance(query_result, dict) else 0,
            },
            "telemetry": dict(query_result.get("request_telemetry") or {}) if isinstance(query_result, dict) and isinstance(query_result.get("request_telemetry"), dict) else {},
            "evidence": {
                "primary_data": result_data,
                "provenance_snapshot": provenance_snapshot,
                "search_scope": scope,
                "entities": entities,
                "attributes": attributes,
                "relationships": relationships,
            },
        }

    def _scope_is_empty(self, scope: Any) -> bool:
        if not isinstance(scope, dict):
            return False
        return (
            int(scope.get("candidate_count", 0) or 0) == 0
            and int(scope.get("discovery_candidate_count", 0) or 0) == 0
            and not list(scope.get("node_ids") or [])
            and not list(scope.get("edge_ids") or [])
            and not list(scope.get("doc_ids") or [])
        )

    def search_discovery(self, question: str, limit: int = 8) -> dict[str, Any]:
        """Tool: Search Discovery Engine directly for media-heavy results."""
        return self.query_engine.search_discovery(question, limit=limit)

    def search_criteria(
        self,
        question: str,
        scope: dict[str, Any] | None = None,
        parsed: Any | None = None,
    ) -> dict[str, Any]:
        """Tool: Execute list/filter criteria queries against structured records."""
        parsed = parsed if parsed is not None else self.query_engine.parse_query(question)
        criteria_result = self.query_engine.search_by_criteria(
            question=question,
            parsed=parsed,
            scope=scope,
            limit=20,
        )

        # Criteria list questions can be under-filled when initial scope resolution is narrow.
        # Retry once without scope constraints and prefer the richer grounded result.
        parsed_intent = str(getattr(parsed, "intent", "") or "").strip().lower()
        scoped_count = int((criteria_result or {}).get("count", 0) or 0) if isinstance(criteria_result, dict) else 0
        if parsed_intent == "criteria_lookup" and scoped_count <= 1:
            expanded_result = self.query_engine.search_by_criteria(
                question=question,
                parsed=parsed,
                scope=None,
                limit=20,
            )
            expanded_count = int((expanded_result or {}).get("count", 0) or 0) if isinstance(expanded_result, dict) else 0
            if isinstance(expanded_result, dict) and expanded_count > scoped_count:
                criteria_result = dict(expanded_result)
                criteria_result["scope_retry"] = "unscoped"

        # If lexical criteria remain too sparse, fall back to scoped document candidates
        # already resolved for this query so list-style asks can surface the evidence bundle.
        scoped_count = int((criteria_result or {}).get("count", 0) or 0) if isinstance(criteria_result, dict) else 0
        scope_docs = list((scope or {}).get("documents") or []) if isinstance(scope, dict) else []
        if parsed_intent == "criteria_lookup" and scoped_count <= 1 and len(scope_docs) > scoped_count:
            parsed_criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
            must_contain = [
                str(item).strip()
                for item in list(parsed_criteria.get("must_contain") or [])
                if str(item).strip()
            ]
            require_all_terms = bool(parsed_criteria.get("require_all_terms"))

            # Build tolerant criteria terms (for example travelling -> travel)
            # so scope fallback does not inject unrelated documents.
            phrase_terms: list[str] = []
            token_terms: list[str] = []
            if must_contain:
                try:
                    phrase_terms, token_terms = self.query_engine._criteria_terms(must_contain)
                except Exception:
                    phrase_terms = [str(item).strip().lower() for item in must_contain if str(item).strip()]
                    token_terms = list(phrase_terms)

            def _term_candidates(term: str) -> list[str]:
                base = str(term or "").strip().lower()
                if not base:
                    return []
                normalized = self.query_engine._normalize_semantic_text(base)
                candidates: set[str] = set(
                    self.query_engine._semantic_token_variants(normalized) if hasattr(self.query_engine, "_semantic_token_variants") else [normalized]
                )
                if normalized.endswith("ing") and len(normalized) > 6:
                    stem = normalized[:-3]
                    candidates.add(stem)
                    if stem.endswith("ll"):
                        candidates.add(stem[:-1])
                    if not stem.endswith("e"):
                        candidates.add(stem + "e")
                return [c for c in candidates if c]

            def _scope_doc_match_terms(item: dict[str, Any]) -> list[str]:
                if not must_contain:
                    return []
                blob_parts = [
                    item.get("doc_name"),
                    item.get("title"),
                    item.get("doc_path"),
                    item.get("file_path"),
                    item.get("doc_key"),
                    item.get("doc_desc"),
                    item.get("summary"),
                    item.get("keyword_text"),
                ]
                metadata = item.get("metadata")
                if isinstance(metadata, dict):
                    blob_parts.append(json.dumps(metadata, ensure_ascii=False))
                haystack = self.query_engine._normalize_semantic_text(
                    " ".join(str(part or "") for part in blob_parts)
                )
                matched: list[str] = []
                for term in list(phrase_terms) + list(token_terms):
                    candidates = _term_candidates(term)
                    if any(candidate in haystack for candidate in candidates):
                        matched.append(str(term))
                # Dedupe while preserving order
                deduped: list[str] = []
                seen: set[str] = set()
                for token in matched:
                    lowered = token.lower()
                    if lowered in seen:
                        continue
                    seen.add(lowered)
                    deduped.append(token)
                return deduped

            scope_matches: list[dict[str, Any]] = []
            seen_doc_ids: set[int] = set()
            for item in scope_docs:
                if not isinstance(item, dict):
                    continue
                doc_id = item.get("doc_id")
                if not isinstance(doc_id, int) or doc_id in seen_doc_ids:
                    continue
                matched_terms = _scope_doc_match_terms(item)
                if must_contain and not matched_terms:
                    continue
                if require_all_terms and must_contain:
                    required = {
                        self.query_engine._normalize_semantic_text(term)
                        for term in must_contain
                        if self.query_engine._normalize_semantic_text(term)
                    }
                    matched_norm = {
                        self.query_engine._normalize_semantic_text(term)
                        for term in matched_terms
                        if self.query_engine._normalize_semantic_text(term)
                    }
                    if not required.issubset(matched_norm):
                        continue
                seen_doc_ids.add(doc_id)
                label = str(item.get("doc_name") or item.get("title") or item.get("doc_path") or f"document_{doc_id}").strip()
                scope_matches.append(
                    {
                        "object_id": None,
                        "entity_name": label,
                        "entity_type": "document",
                        "doc_id": doc_id,
                        "doc_name": item.get("doc_name") or label,
                        "doc_path": item.get("doc_path") or item.get("file_path") or "",
                        "doc_date": item.get("doc_date"),
                        "matched_terms": (matched_terms[:4] if matched_terms else must_contain[:4]),
                        "score": 0.5,
                    }
                )

            # If lexical matching is too sparse but scope documents are already
            # resolved for this query, preserve that evidence bundle instead of
            # collapsing to a single sparse match.
            if not scope_matches and scope_docs:
                for item in scope_docs:
                    if not isinstance(item, dict):
                        continue
                    doc_id = item.get("doc_id")
                    if not isinstance(doc_id, int) or doc_id in seen_doc_ids:
                        continue
                    seen_doc_ids.add(doc_id)
                    label = str(item.get("doc_name") or item.get("title") or item.get("doc_path") or f"document_{doc_id}").strip()
                    scope_matches.append(
                        {
                            "object_id": None,
                            "entity_name": label,
                            "entity_type": "document",
                            "doc_id": doc_id,
                            "doc_name": item.get("doc_name") or label,
                            "doc_path": item.get("doc_path") or item.get("file_path") or "",
                            "doc_date": item.get("doc_date"),
                            "matched_terms": must_contain[:4],
                            "score": 0.4,
                        }
                    )

            if scope_matches:
                criteria_result = {
                    "criteria": parsed_criteria,
                    "time_window": parsed_criteria.get("time_window") if isinstance(parsed_criteria.get("time_window"), dict) else None,
                    "count": len(scope_matches),
                    "matches": scope_matches[:20],
                    "source": "criteria_scope_docs",
                    "scope_retry": "scope_documents",
                }

        requires_contextual_bridge = False
        if str(getattr(parsed, "intent", "") or "").strip().lower() == "attribute_lookup":
            try:
                requires_contextual_bridge = self.query_engine._requires_contextual_value_resolution(
                    question=question,
                    parsed=parsed,
                    criteria=parsed.criteria if isinstance(parsed.criteria, dict) else {},
                    direct_result={"attribute_name": getattr(parsed, "attribute_name", None)},
                )
            except Exception:
                requires_contextual_bridge = False

        if (
            not criteria_result
            and str(getattr(parsed, "intent", "") or "").strip().lower() == "attribute_lookup"
            and (
                self.query_engine._is_value_document_context_question(question)
                or requires_contextual_bridge
            )
        ):
            parsed_criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
            relation_filters: list[dict[str, Any]] = []
            if isinstance(parsed_criteria.get("relation_filters"), list):
                relation_filters.extend(item for item in parsed_criteria.get("relation_filters") if isinstance(item, dict))
            if isinstance(parsed_criteria.get("relationship_filters"), list):
                relation_filters.extend(item for item in parsed_criteria.get("relationship_filters") if isinstance(item, dict))

            must_terms: list[str] = []
            entity_name = str(getattr(parsed, "entity_name", "") or "").strip()
            if entity_name:
                must_terms.append(entity_name)

            for rel in relation_filters:
                for key in (
                    "target_name",
                    "target_entity_name",
                    "source_name",
                    "source_entity_name",
                    "relationship_type",
                    "relation",
                ):
                    value = str(rel.get(key) or "").strip()
                    if value and value not in must_terms:
                        must_terms.append(value)

            cue_tokens = [
                tok
                for tok in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]{4,}", str(question or "").lower())
                if tok
                not in {
                    "what", "which", "where", "when", "how", "much", "with", "from", "into", "about",
                    "cost", "price", "value", "amount", "total", "tell", "find", "related", "documents",
                    "show", "give", "query", "that", "this", "have", "does", "did", "were", "been",
                    "was", "sind", "ist", "sowie", "oder", "aber", "eine", "einer", "eines", "dieser",
                }
            ]
            for token in cue_tokens[:8]:
                if token and token not in must_terms:
                    must_terms.append(token)

            for hint in ("invoice", "bill", "receipt", "payment", "quotation", "quote", "offer", "rechnung", "beleg", "zahlung"):
                if hint not in must_terms:
                    must_terms.append(hint)

            if must_terms:
                synthetic_criteria = {
                    "entity_type": "",
                    "must_contain": must_terms,
                    "attribute_hints": ["description", "document", "metadata", "amount", "cost", "value", "due_date"],
                    "semantic_only": True,
                    "document_hint": "all",
                    "semantic_frame": "interaction_value_document_context_bridge",
                    "semantic_retrieval_mode": "related",
                    "allow_indirect_relationships": True,
                }
                synthetic_parsed = parsed.__class__(
                    intent="criteria_lookup",
                    entity_name=entity_name or None,
                    attribute_name=None,
                    confidence=max(0.70, float(getattr(parsed, "confidence", 0.0) or 0.0)),
                    language=str(getattr(parsed, "language", "en") or "en"),
                    criteria=synthetic_criteria,
                )
                criteria_result = self.query_engine.search_by_criteria(
                    question=question,
                    parsed=synthetic_parsed,
                    scope=scope,
                    limit=20,
                )

        # Some criteria SQL paths can return only count/source with sparse match rows.
        # For value-context attribute queries we hydrate top document matches from
        # semantic scope so contextual value extraction has grounded evidence.
        if (
            isinstance(criteria_result, dict)
            and int(criteria_result.get("count", 0) or 0) > 0
            and not list(criteria_result.get("matches") or [])
            and str(getattr(parsed, "intent", "") or "").strip().lower() == "attribute_lookup"
            and (
                self.query_engine._is_value_document_context_question(question)
                or requires_contextual_bridge
            )
        ):
            try:
                resolved_scope = scope if isinstance(scope, dict) and scope else self.query_engine.resolve_search_scope(question, limit=24)
                scope_docs = list((resolved_scope or {}).get("documents") or []) if isinstance(resolved_scope, dict) else []
                hydrated_matches: list[dict[str, Any]] = []
                seen_doc_ids: set[int] = set()
                must_terms = [
                    str(item).strip()
                    for item in list(((parsed.criteria or {}) if isinstance(parsed.criteria, dict) else {}).get("must_contain") or [])
                    if str(item).strip()
                ]

                for item in scope_docs:
                    if not isinstance(item, dict):
                        continue
                    doc_id = item.get("doc_id")
                    if not isinstance(doc_id, int) or doc_id in seen_doc_ids:
                        continue
                    seen_doc_ids.add(doc_id)
                    hydrated_matches.append(
                        {
                            "object_id": None,
                            "entity_name": str(item.get("doc_name") or item.get("title") or item.get("doc_path") or f"document_{doc_id}").strip(),
                            "entity_type": "document",
                            "doc_id": doc_id,
                            "doc_name": item.get("doc_name") or item.get("title"),
                            "doc_path": item.get("doc_path") or item.get("file_path") or "",
                            "doc_date": item.get("doc_date"),
                            "title": item.get("title"),
                            "file_name": item.get("file_name"),
                            "file_path": item.get("file_path") or item.get("doc_path") or "",
                            "matched_terms": must_terms[:6],
                            "score": float(item.get("score") or 0.0),
                            "doc_theme": item.get("doc_theme"),
                            "doc_cat": item.get("doc_cat"),
                            "doc_type": item.get("doc_type"),
                            "keyword_text": item.get("keyword_text"),
                        }
                    )
                    if len(hydrated_matches) >= 20:
                        break

                if hydrated_matches:
                    criteria_result = dict(criteria_result)
                    criteria_result["matches"] = hydrated_matches
                    criteria_result["source"] = str(criteria_result.get("source") or "criteria_sql")
                    criteria_result["scope_retry"] = "scope_documents_hydrated"
            except Exception:
                pass

        if not criteria_result:
            return {
                "success": False,
                "count": 0,
                "matches": [],
                "criteria": parsed.criteria if isinstance(parsed.criteria, dict) else {},
            }
        return {
            "success": True,
            **criteria_result,
        }

    def get_entity_attributes(self, entity_name: str) -> dict[str, Any]:
        """Tool: Return the attribute names actually stored in the database for a given entity.

        Use this before lookup_entity_attribute when the user's query does not clearly
        specify which attribute they want. The returned list provides grounded options
        for the agent to choose from.

        Args:
            entity_name: The company or person name exactly as it appears in the query.

        Returns:
            Dict with entity_name and a list of available_attributes (snake_case keys).
        """
        entity_name = str(entity_name or "").strip()
        if not entity_name:
            return {"entity_name": entity_name, "available_attributes": [], "count": 0}
        attrs = self.query_engine._get_entity_attribute_names(entity_name)
        return {
            "entity_name": entity_name,
            "available_attributes": attrs,
            "count": len(attrs),
        }

    def lookup_entity_attribute(
        self, entity_name: str, attribute_name: str
    ) -> dict[str, Any]:
        """Tool: Look up the value of a specific attribute for an entity using exact SQL.

        Call this after get_entity_attributes to retrieve the actual stored value.
        For shareholders, use attribute_name='shareholders'.

        Args:
            entity_name: The company or person name.
            attribute_name: The snake_case attribute key (e.g. 'registered_address',
                'registration_no', 'incorporation_date').

        Returns:
            Dict with entity_name, attribute_name, attribute_value, and source metadata.
        """
        entity_name = str(entity_name or "").strip()
        attribute_name = str(attribute_name or "").strip()
        if not entity_name or not attribute_name:
            return {"found": False, "entity_name": entity_name, "attribute_name": attribute_name}
        result = self.query_engine.lookup_attribute(attribute_name, entity_name)
        if not result:
            return {"found": False, "entity_name": entity_name, "attribute_name": attribute_name}
        return {"found": True, **result}

    def retrieve_document_file(
        self,
        doc_id: int | None = None,
        doc_key: str | None = None,
        doc_name: str | None = None,
    ) -> dict[str, Any]:
        """Tool: Retrieve document filename and path via SOLF-governed action flow.

        Resolution priority follows provided identifiers. Returns doc_name, doc_path,
        and original_local_path (when available) for downstream file retrieval.
        """
        payload: dict[str, Any] = {}
        if doc_id is not None:
            payload["doc_id"] = int(doc_id)
        if doc_key:
            payload["doc_key"] = str(doc_key).strip()
        if doc_name:
            payload["doc_name"] = str(doc_name).strip()
        result = self.perform_action("retrieve_document_file", payload)
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        rows = data.get("rows") if isinstance(data.get("rows"), list) else []
        row = rows[0] if rows else {}
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        user_metadata = metadata.get("user_metadata") if isinstance(metadata.get("user_metadata"), dict) else {}
        source_ref = user_metadata.get("source_reference") if isinstance(user_metadata.get("source_reference"), dict) else {}
        return {
            "success": bool(result.get("success")),
            "doc_id": row.get("doc_id"),
            "doc_name": row.get("doc_name"),
            "doc_key": row.get("doc_key"),
            "doc_path": row.get("doc_path"),
            "original_local_path": source_ref.get("original_local_path") or user_metadata.get("client_file_path"),
            "file_name": row.get("doc_name") or user_metadata.get("client_file_name") or row.get("doc_key"),
            "raw": result,
        }

    def perform_action(self, action_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Execute SOLF-guarded workflow action using ActionTool."""
        normalized_payload = payload if isinstance(payload, dict) else {}
        if isinstance(normalized_payload, dict) and not any(
            str(normalized_payload.get(field) or "").strip()
            for field in ("message", "text", "request", "instruction", "command", "prompt", "query")
        ):
            last_request = str(self._last_nl_request_text or "").strip()
            if last_request:
                normalized_payload = dict(normalized_payload)
                normalized_payload["message"] = last_request

        action, payload = self._normalize_integrate_source_action_request(action_name, normalized_payload)

        def _respond(out: dict[str, Any]) -> dict[str, Any]:
            self._log_action_execution(action, payload, out)
            return out

        if action == "integrate_source_database":
            return _respond(self._integrate_source_database_action(payload))
        if action == "sync_source_database_entities":
            return _respond(self._sync_source_database_entities_action(payload))

        # Compose and enforce SOLF semantic plan first.
        semantic_plan = self._compose_action_plan(action, payload)
        if semantic_plan:
            guard_clause = str(semantic_plan.get("guard") or "").strip()
            if guard_clause:
                guard_result = self._invoke_solf_policy(guard_clause, payload)
                directive_out = self._apply_workflow_rule_directives(
                    stage=guard_clause,
                    policy=guard_result,
                    payload=payload,
                )
                if isinstance(directive_out.get("payload"), dict):
                    payload = dict(directive_out.get("payload") or {})
                if str(guard_result.get("mode") or "allow").lower() == "deny":
                    return _respond({
                        "action": action,
                        "success": False,
                        "message": str(guard_result.get("message") or f"SOLF guard denied action: {guard_clause}"),
                        "policy": guard_result,
                        "semantic_plan": semantic_plan,
                        "trace": {
                            "action_clause": "action_plan",
                            "guard_clause": guard_clause,
                            "sql_clause": str(semantic_plan.get("sql_builder") or "") or None,
                        },
                    })

            if action == "reverse_mutation":
                return _respond(self._reverse_mutation_action(payload))

            sql_builder = str(semantic_plan.get("sql_builder") or "").strip()
            raw_plan_args = self._as_string_key_dict(semantic_plan.get("args")) or payload
            plan_args = self._as_string_key_dict(self._resolve_solf_placeholders(raw_plan_args, payload))
            if sql_builder:
                sql_plan_raw = self._invoke_solf_clause_raw(sql_builder, [plan_args])
                sql_plan = self._as_string_key_dict(self._resolve_solf_placeholders(sql_plan_raw, plan_args))
                if sql_plan:
                    if action == "retrieve_document_file":
                        where_payload: dict[str, Any] = {}
                        if payload.get("doc_id") is not None:
                            where_payload["doc_id"] = int(payload.get("doc_id"))
                        else:
                            doc_hint = payload.get("doc_key") if payload.get("doc_key") else payload.get("doc_name")
                            if doc_hint in (None, ""):
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "One of doc_id, doc_key, or doc_name is required.",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            resolution = self._resolve_document_reference(doc_hint)
                            if str(resolution.get("status") or "") != "resolved":
                                return _respond(
                                    self._create_retrieve_action_clarification(
                                        action_name=action,
                                        payload=payload,
                                        resolution=resolution,
                                    )
                                )

                            resolved_doc_id = int(resolution.get("doc_id") or 0)
                            if resolved_doc_id <= 0:
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "Unable to resolve document reference for retrieve_document_file.",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            payload = dict(payload)
                            payload["doc_id"] = resolved_doc_id
                            where_payload["doc_id"] = resolved_doc_id
                        sql_plan["operation"] = "select"
                        sql_plan["table"] = "document"
                        sql_plan["where"] = where_payload

                    if action == "update_entity":
                        entity_id_raw = payload.get("entity_id")
                        natural_key_field = str(payload.get("natural_key_field") or "").strip().lower()
                        natural_key_value = payload.get("natural_key_value")
                        if (entity_id_raw in (None, "")) and natural_key_field in {"name", "object_name", "canonical_full_name"} and (natural_key_value not in (None, "")):
                            resolution = self._resolve_merge_entity_reference(natural_key_value)
                            if str(resolution.get("status") or "") != "resolved":
                                return _respond(
                                    self._create_update_action_clarification(
                                        action_name=action,
                                        payload=payload,
                                        resolution=resolution,
                                    )
                                )

                            resolved_object_id = int(resolution.get("object_id") or 0)
                            field_name = str(payload.get("field") or "").strip()
                            entity_type = str(payload.get("entity_type") or "").strip()
                            if resolved_object_id <= 0 or not field_name or not entity_type:
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "Unable to resolve update_entity payload after entity disambiguation.",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })

                            payload = dict(payload)
                            payload["entity_id"] = resolved_object_id
                            sql_plan = {
                                "operation": "update",
                                "table": entity_type,
                                "where": {"id": resolved_object_id},
                                "values": {field_name: payload.get("new_value")},
                                "returning": ["id"],
                            }

                    if action in {"assign_task", "close_task"}:
                        task_id_raw = payload.get("task_id")
                        if task_id_raw in (None, ""):
                            task_hint = payload.get("task_name") if payload.get("task_name") else payload.get("task_title")
                            if task_hint not in (None, ""):
                                resolution = self._resolve_task_reference(task_hint)
                                if str(resolution.get("status") or "") != "resolved":
                                    return _respond(
                                        self._create_task_action_clarification(
                                            action_name=action,
                                            payload=payload,
                                            resolution=resolution,
                                        )
                                    )
                                resolved_task_id = int(resolution.get("task_id") or 0)
                                if resolved_task_id <= 0:
                                    return _respond({
                                        "action": action,
                                        "success": False,
                                        "message": f"Unable to resolve task reference for {action}.",
                                        "semantic_plan": semantic_plan,
                                        "sql_plan": sql_plan,
                                    })
                                payload = dict(payload)
                                payload["task_id"] = resolved_task_id
                                if action == "assign_task":
                                    assigned_to = payload.get("assigned_to")
                                    if assigned_to in (None, ""):
                                        return _respond({
                                            "action": action,
                                            "success": False,
                                            "message": "assigned_to is required for assign_task.",
                                            "semantic_plan": semantic_plan,
                                            "sql_plan": sql_plan,
                                        })
                                    sql_plan = {
                                        "operation": "update",
                                        "table": "project_tasks",
                                        "where": {"id": resolved_task_id},
                                        "values": {
                                            "assigned_to": int(assigned_to),
                                            "status": "in_progress",
                                        },
                                        "returning": ["id"],
                                    }

                    if action == "create_task":
                        project_id_raw = payload.get("project_id")
                        if project_id_raw in (None, ""):
                            project_hint = payload.get("project_name") if payload.get("project_name") else payload.get("project_code")
                            if project_hint in (None, ""):
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "project_id is required (or provide project_name/project_code).",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            project_resolution = self._resolve_project_reference(project_hint)
                            if str(project_resolution.get("status") or "") != "resolved":
                                return _respond(
                                    self._create_create_task_action_clarification(
                                        action_name=action,
                                        payload=payload,
                                        resolution=project_resolution,
                                        pending_field="project",
                                    )
                                )
                            resolved_project_id = int(project_resolution.get("project_id") or 0)
                            if resolved_project_id <= 0:
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "Unable to resolve create_task project reference.",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            payload = dict(payload)
                            payload["project_id"] = resolved_project_id
                        else:
                            payload = dict(payload)
                            payload["project_id"] = int(project_id_raw)

                        assigned_to_raw = payload.get("assigned_to")
                        if assigned_to_raw not in (None, ""):
                            assigned_to_text = str(assigned_to_raw).strip()
                            if re.fullmatch(r"\d+", assigned_to_text):
                                payload["assigned_to"] = int(assigned_to_text)
                            else:
                                assignee_resolution = self._resolve_merge_entity_reference(assigned_to_text)
                                if str(assignee_resolution.get("status") or "") != "resolved":
                                    return _respond(
                                        self._create_create_task_action_clarification(
                                            action_name=action,
                                            payload=payload,
                                            resolution=assignee_resolution,
                                            pending_field="assignee",
                                        )
                                    )
                                resolved_assignee_id = int(assignee_resolution.get("object_id") or 0)
                                if resolved_assignee_id <= 0:
                                    return _respond({
                                        "action": action,
                                        "success": False,
                                        "message": "Unable to resolve create_task assignee reference.",
                                        "semantic_plan": semantic_plan,
                                        "sql_plan": sql_plan,
                                    })
                                payload["assigned_to"] = resolved_assignee_id

                        title = str(payload.get("title") or "").strip()
                        if not title:
                            return _respond({
                                "action": action,
                                "success": False,
                                "message": "title is required for create_task.",
                                "semantic_plan": semantic_plan,
                                "sql_plan": sql_plan,
                            })

                        try:
                            estimated_hours = float(payload.get("hours_estimate") or 0.0)
                        except Exception:
                            estimated_hours = 0.0

                        sql_plan = {
                            "operation": "insert",
                            "table": "project_tasks",
                            "values": {
                                "project_id": int(payload.get("project_id")),
                                "name": title,
                                "estimated_hours": estimated_hours,
                                "assigned_to": payload.get("assigned_to"),
                                "status": "open",
                            },
                            "returning": ["id"],
                        }

                    if action == "create_workflow_case":
                        subject_ref_raw = payload.get("subject_ref")
                        if subject_ref_raw in (None, ""):
                            subject_hint = payload.get("subject_name") if payload.get("subject_name") else payload.get("subject_title")
                            if subject_hint in (None, ""):
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "subject_ref is required (or provide subject_name/subject_title).",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            subject_resolution = self._resolve_merge_entity_reference(subject_hint)
                            if str(subject_resolution.get("status") or "") != "resolved":
                                return _respond(
                                    self._create_create_workflow_case_action_clarification(
                                        action_name=action,
                                        payload=payload,
                                        resolution=subject_resolution,
                                        pending_field="subject",
                                    )
                                )
                            resolved_subject_ref = int(subject_resolution.get("object_id") or 0)
                            if resolved_subject_ref <= 0:
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "Unable to resolve create_workflow_case subject reference.",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            payload = dict(payload)
                            payload["subject_ref"] = resolved_subject_ref
                        else:
                            payload = dict(payload)
                            payload["subject_ref"] = int(subject_ref_raw)

                        initiated_by_raw = payload.get("initiated_by_ref")
                        if initiated_by_raw in (None, ""):
                            initiator_hint = payload.get("initiated_by_name") if payload.get("initiated_by_name") else payload.get("initiated_by_title")
                            if initiator_hint in (None, ""):
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "initiated_by_ref is required (or provide initiated_by_name/initiated_by_title).",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            initiator_resolution = self._resolve_merge_entity_reference(initiator_hint)
                            if str(initiator_resolution.get("status") or "") != "resolved":
                                return _respond(
                                    self._create_create_workflow_case_action_clarification(
                                        action_name=action,
                                        payload=payload,
                                        resolution=initiator_resolution,
                                        pending_field="initiator",
                                    )
                                )
                            resolved_initiator_ref = int(initiator_resolution.get("object_id") or 0)
                            if resolved_initiator_ref <= 0:
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "Unable to resolve create_workflow_case initiator reference.",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            payload["initiated_by_ref"] = resolved_initiator_ref
                        else:
                            payload["initiated_by_ref"] = int(initiated_by_raw)

                        case_no = str(payload.get("case_no") or "").strip()
                        case_type = str(payload.get("case_type") or "").strip()
                        current_step = str(payload.get("current_step") or "").strip()
                        status_value = str(payload.get("status") or "").strip()
                        if not case_no or not case_type or not current_step or not status_value:
                            return _respond({
                                "action": action,
                                "success": False,
                                "message": "case_no, case_type, current_step, and status are required for create_workflow_case.",
                                "semantic_plan": semantic_plan,
                                "sql_plan": sql_plan,
                            })

                        sql_plan = {
                            "operation": "insert",
                            "table": "workflow_cases",
                            "values": {
                                "case_no": case_no,
                                "case_type": case_type,
                                "subject_ref": int(payload.get("subject_ref")),
                                "initiated_by_ref": int(payload.get("initiated_by_ref")),
                                "current_step": current_step,
                                "sla_due_at": payload.get("sla_due_at"),
                                "status": status_value,
                            },
                            "returning": ["id"],
                        }

                    if action == "record_approval":
                        case_ref_raw = payload.get("case_ref")
                        if case_ref_raw in (None, ""):
                            case_hint = payload.get("case_no")
                            if case_hint in (None, ""):
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "case_ref is required (or provide case_no).",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            case_resolution = self._resolve_workflow_case_reference(case_hint)
                            if str(case_resolution.get("status") or "") != "resolved":
                                return _respond(
                                    self._create_record_approval_action_clarification(
                                        action_name=action,
                                        payload=payload,
                                        resolution=case_resolution,
                                        pending_field="case",
                                    )
                                )
                            resolved_case_ref = int(case_resolution.get("case_ref") or 0)
                            if resolved_case_ref <= 0:
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "Unable to resolve record_approval case reference.",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            payload = dict(payload)
                            payload["case_ref"] = resolved_case_ref
                        else:
                            payload = dict(payload)
                            payload["case_ref"] = int(case_ref_raw)

                        approver_raw = payload.get("approver_ref")
                        if approver_raw in (None, ""):
                            approver_hint = payload.get("approver_name") if payload.get("approver_name") else payload.get("approver_title")
                            if approver_hint in (None, ""):
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "approver_ref is required (or provide approver_name/approver_title).",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            approver_resolution = self._resolve_merge_entity_reference(approver_hint)
                            if str(approver_resolution.get("status") or "") != "resolved":
                                return _respond(
                                    self._create_record_approval_action_clarification(
                                        action_name=action,
                                        payload=payload,
                                        resolution=approver_resolution,
                                        pending_field="approver",
                                    )
                                )
                            resolved_approver_ref = int(approver_resolution.get("object_id") or 0)
                            if resolved_approver_ref <= 0:
                                return _respond({
                                    "action": action,
                                    "success": False,
                                    "message": "Unable to resolve record_approval approver reference.",
                                    "semantic_plan": semantic_plan,
                                    "sql_plan": sql_plan,
                                })
                            payload["approver_ref"] = resolved_approver_ref
                        else:
                            payload["approver_ref"] = int(approver_raw)

                        decision = str(payload.get("decision") or "").strip()
                        if not decision:
                            return _respond({
                                "action": action,
                                "success": False,
                                "message": "decision is required for record_approval.",
                                "semantic_plan": semantic_plan,
                                "sql_plan": sql_plan,
                            })
                        sql_plan = {
                            "operation": "insert",
                            "table": "approvals",
                            "values": {
                                "case_ref": int(payload.get("case_ref")),
                                "approver_ref": int(payload.get("approver_ref")),
                                "decision": decision,
                                "decision_at": payload.get("decision_at"),
                                "comment": payload.get("comment"),
                                "escalation_level": int(payload.get("escalation_level") or 0),
                            },
                            "returning": ["id"],
                        }

                    # validate_plan has multi-check semantics already implemented in ActionTool.
                    if action == "validate_plan":
                        result = self.action_tool.validate_plan(plan_id=int(payload.get("plan_id")))
                        out = {
                            "action": action,
                            "success": result.success,
                            "message": result.message,
                            "data": result.data,
                            "warnings": result.warnings,
                            "requires_confirmation": result.requires_confirmation,
                            "confirmation_prompt": result.confirmation_prompt,
                            "semantic_plan": semantic_plan,
                            "sql_plan": sql_plan,
                            "trace": {
                                "action_clause": "action_plan",
                                "guard_clause": guard_clause or None,
                                "sql_clause": sql_builder or None,
                            },
                        }
                        if isinstance(result.data, dict):
                            for conflict in (result.data.get("resource_conflicts") or []):
                                self.event_bus.emit(
                                    "resource.conflict_detected",
                                    source_entity="project_tasks",
                                    entity_id=int(payload.get("plan_id")),
                                    payload={
                                        "person_name": conflict.get("assigned_to"),
                                        "conflicting_tasks": conflict,
                                    },
                                    tags=["validation"],
                                )
                            for overdue in (result.data.get("overdue_tasks") or []):
                                self.event_bus.emit(
                                    "task.overdue",
                                    source_entity="project_tasks",
                                    entity_id=int(overdue.get("task_id")),
                                    payload={
                                        "title": overdue.get("name"),
                                        "days_overdue": 1,
                                    },
                                    tags=["validation"],
                                )
                        return _respond(out)

                    sql_exec = self._execute_sql_plan(sql_plan)
                    if sql_exec.get("success"):
                        if action == "create_task":
                            task_id = None
                            returned = self._as_string_key_dict(sql_exec.get("returned"))
                            if returned.get("id") is not None:
                                task_id = int(returned.get("id"))
                            self.event_bus.emit(
                                "task.created",
                                source_entity="project_tasks",
                                entity_id=int(task_id or 0),
                                payload={
                                    "project_id": payload.get("project_id"),
                                    "assigned_to": payload.get("assigned_to"),
                                    "title": payload.get("title"),
                                },
                                tags=["action"],
                            )
                        elif action == "assign_task":
                            self.event_bus.emit(
                                "task.assigned",
                                source_entity="project_tasks",
                                entity_id=int(payload.get("task_id")),
                                payload={"assigned_to": payload.get("assigned_to")},
                                tags=["action"],
                            )
                        elif action == "close_task":
                            self.event_bus.emit(
                                "task.closed",
                                source_entity="project_tasks",
                                entity_id=int(payload.get("task_id")),
                                payload={"status": "done"},
                                tags=["action"],
                            )
                        elif action == "create_workflow_case":
                            case_id = None
                            returned = self._as_string_key_dict(sql_exec.get("returned"))
                            if returned.get("id") is not None:
                                case_id = int(returned.get("id"))
                            self.event_bus.emit(
                                "workflow.case_created",
                                source_entity="workflow_cases",
                                entity_id=int(case_id or 0),
                                payload={
                                    "case_no": payload.get("case_no"),
                                    "case_type": payload.get("case_type"),
                                    "status": payload.get("status"),
                                },
                                tags=["action", "workflow"],
                            )
                        elif action == "record_approval":
                            approval_id = None
                            returned = self._as_string_key_dict(sql_exec.get("returned"))
                            if returned.get("id") is not None:
                                approval_id = int(returned.get("id"))
                            self.event_bus.emit(
                                "workflow.approval_recorded",
                                source_entity="approvals",
                                entity_id=int(approval_id or 0),
                                payload={
                                    "case_ref": payload.get("case_ref"),
                                    "decision": payload.get("decision"),
                                    "approver_ref": payload.get("approver_ref"),
                                },
                                tags=["action", "approval"],
                            )
                        elif action == "create_compliance_action":
                            compliance_id = None
                            returned = self._as_string_key_dict(sql_exec.get("returned"))
                            if returned.get("id") is not None:
                                compliance_id = int(returned.get("id"))
                            self.event_bus.emit(
                                "compliance.action_created",
                                source_entity="compliance_actions",
                                entity_id=int(compliance_id or 0),
                                payload={
                                    "action_no": payload.get("action_no"),
                                    "status": payload.get("status"),
                                    "priority": payload.get("priority"),
                                },
                                tags=["action", "compliance"],
                            )

                    return _respond({
                        "action": action,
                        "success": bool(sql_exec.get("success")),
                        "message": "Action executed via SOLF SQL plan." if sql_exec.get("success") else str(sql_exec.get("message") or "Action failed."),
                        "data": sql_exec,
                        "semantic_plan": semantic_plan,
                        "sql_plan": sql_plan,
                        "trace": {
                            "action_clause": "action_plan",
                            "guard_clause": guard_clause or None,
                            "sql_clause": sql_builder or None,
                        },
                    })

        if action in {"domain_definition_crud", "domain-definition-crud", "domain_definition", "domain-definition"}:
            out = self._handle_domain_definition_crud_action(payload)
            if not isinstance(out, dict):
                out = {
                    "action": "domain_definition_crud",
                    "success": False,
                    "message": "Unexpected response from domain definition CRUD handler.",
                }
            if not out.get("action"):
                out["action"] = "domain_definition_crud"
            return _respond(out)

        if action == "create_task":
            result = self.action_tool.create_task(
                project_id=int(payload.get("project_id")),
                title=str(payload.get("title") or "").strip(),
                hours_estimate=float(payload.get("hours_estimate") or 0),
                assigned_to=(int(payload["assigned_to"]) if payload.get("assigned_to") is not None else None),
                description=str(payload.get("description") or ""),
                priority=str(payload.get("priority") or "medium"),
                due_date=(str(payload.get("due_date")) if payload.get("due_date") else None),
            )
            if result.success and isinstance(result.data, dict):
                self.event_bus.emit(
                    "task.created",
                    source_entity="project_tasks",
                    entity_id=int(result.data.get("task_id") or 0),
                    payload={
                        "project_id": int(payload.get("project_id")),
                        "assigned_to": payload.get("assigned_to"),
                        "title": payload.get("title"),
                    },
                    tags=["action"],
                )
            return _respond({
                "action": action,
                "success": result.success,
                "message": result.message,
                "data": result.data,
                "warnings": result.warnings,
                "requires_confirmation": result.requires_confirmation,
                "confirmation_prompt": result.confirmation_prompt,
            })

        if action == "ingest_document":
            source = str(payload.get("source") or "").strip()
            if not source:
                return _respond({
                    "action": action,
                    "success": False,
                    "message": "source is required",
                })
            raw_tags = payload.get("tags")
            if isinstance(raw_tags, list):
                tags = [str(item).strip() for item in raw_tags if str(item).strip()]
            elif isinstance(raw_tags, str):
                tags = [item.strip() for item in raw_tags.split(",") if item.strip()]
            else:
                tags = []
            metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
            result = self.ingest_document(
                source=source,
                description=str(payload.get("description") or ""),
                tags=tags,
                metadata=metadata,
            )
            self.event_bus.emit(
                "document.ingested",
                source_entity="document",
                entity_id=0,
                payload={
                    "source": source,
                    "description": str(payload.get("description") or ""),
                    "tags": tags,
                },
                tags=["ingestion"],
            )
            return _respond({
                "action": action,
                "success": True,
                "message": "Document ingested.",
                "data": result,
            })

        if action == "ingest_information":
            information_text = str(payload.get("information_text") or payload.get("text") or "").strip()
            if not information_text:
                return _respond({
                    "action": action,
                    "success": False,
                    "message": "information_text is required",
                })
            raw_tags = payload.get("tags")
            if isinstance(raw_tags, list):
                tags = [str(item).strip() for item in raw_tags if str(item).strip()]
            elif isinstance(raw_tags, str):
                tags = [item.strip() for item in raw_tags.split(",") if item.strip()]
            else:
                tags = []
            metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
            result = self.ingest_information(
                information_text=information_text,
                description=str(payload.get("description") or ""),
                tags=tags,
                metadata=metadata,
            )
            self.event_bus.emit(
                "information.ingested",
                source_entity="document",
                entity_id=0,
                payload={
                    "description": str(payload.get("description") or ""),
                    "tags": tags,
                    "text_length": len(information_text),
                },
                tags=["ingestion"],
            )
            return _respond({
                "action": action,
                "success": True,
                "message": "Information text ingested.",
                "data": result,
            })

        if action == "generate_document":
            result = self.generate_document(
                document_type=str(payload.get("document_type") or ""),
                subject=str(payload.get("subject") or ""),
                context=(payload.get("context") if isinstance(payload.get("context"), dict) else {}),
                template=str(payload.get("template") or ""),
                output_path=(str(payload.get("output_path")).strip() if payload.get("output_path") else None),
            )
            self.event_bus.emit(
                "document.generated",
                source_entity="document",
                entity_id=0,
                payload={
                    "document_type": result.get("document_type"),
                    "subject": result.get("subject"),
                    "output_path": result.get("output_path"),
                },
                tags=["generation"],
            )
            return _respond({
                "action": action,
                "success": True,
                "message": "Document generated.",
                "data": result,
            })

        if action == "update_entity":
            entity_type = str(payload.get("entity_type") or "").strip()
            field = str(payload.get("field") or "").strip()
            new_value = payload.get("new_value")
            entity_id_raw = payload.get("entity_id")
            natural_key_field = str(payload.get("natural_key_field") or "").strip()
            natural_key_value = payload.get("natural_key_value")

            if (not entity_id_raw) and entity_type and field and natural_key_field and (natural_key_value is not None):
                sql_plan = {
                    "operation": "update",
                    "table": entity_type,
                    "where": {natural_key_field: natural_key_value},
                    "values": {field: new_value},
                    "returning": ["id"],
                }
                sql_exec = self._execute_sql_plan(sql_plan)
                return _respond({
                    "action": action,
                    "success": bool(sql_exec.get("success")),
                    "message": "Entity updated by natural key." if sql_exec.get("success") else str(sql_exec.get("message") or "Update failed."),
                    "data": sql_exec,
                    "sql_plan": sql_plan,
                })

            result = self.action_tool.update_entity(
                entity_type=entity_type,
                entity_id=int(payload.get("entity_id")),
                field=field,
                new_value=new_value,
            )
            return _respond({
                "action": action,
                "success": result.success,
                "message": result.message,
                "data": result.data,
                "warnings": result.warnings,
                "requires_confirmation": result.requires_confirmation,
                "confirmation_prompt": result.confirmation_prompt,
            })

        if action == "assign_task":
            result = self.action_tool.assign_task(
                task_id=int(payload.get("task_id")),
                assigned_to=int(payload.get("assigned_to")),
            )
            if result.success:
                self.event_bus.emit(
                    "task.assigned",
                    source_entity="project_tasks",
                    entity_id=int(payload.get("task_id")),
                    payload={"assigned_to": int(payload.get("assigned_to"))},
                    tags=["action"],
                )
            return _respond({
                "action": action,
                "success": result.success,
                "message": result.message,
                "data": result.data,
                "warnings": result.warnings,
                "requires_confirmation": result.requires_confirmation,
                "confirmation_prompt": result.confirmation_prompt,
            })

        if action == "close_task":
            result = self.action_tool.close_task(task_id=int(payload.get("task_id")))
            if result.success:
                self.event_bus.emit(
                    "task.closed",
                    source_entity="project_tasks",
                    entity_id=int(payload.get("task_id")),
                    payload={"status": "done"},
                    tags=["action"],
                )
            return _respond({
                "action": action,
                "success": result.success,
                "message": result.message,
                "data": result.data,
                "warnings": result.warnings,
                "requires_confirmation": result.requires_confirmation,
                "confirmation_prompt": result.confirmation_prompt,
            })

        if action == "validate_plan":
            result = self.action_tool.validate_plan(plan_id=int(payload.get("plan_id")))
            if isinstance(result.data, dict):
                for conflict in (result.data.get("resource_conflicts") or []):
                    self.event_bus.emit(
                        "resource.conflict_detected",
                        source_entity="project_tasks",
                        entity_id=int(payload.get("plan_id")),
                        payload={
                            "person_name": conflict.get("assigned_to"),
                            "conflicting_tasks": conflict,
                        },
                        tags=["validation"],
                    )
                for overdue in (result.data.get("overdue_tasks") or []):
                    self.event_bus.emit(
                        "task.overdue",
                        source_entity="project_tasks",
                        entity_id=int(overdue.get("task_id")),
                        payload={
                            "title": overdue.get("name"),
                            "days_overdue": 1,
                        },
                        tags=["validation"],
                    )
            return _respond({
                "action": action,
                "success": result.success,
                "message": result.message,
                "data": result.data,
                "warnings": result.warnings,
                "requires_confirmation": result.requires_confirmation,
                "confirmation_prompt": result.confirmation_prompt,
            })

        return _respond({
            "action": action,
            "success": False,
            "message": f"Unsupported action: {action}",
        })

    def preview_action_plan(self, action_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Dry-run SOLF composition: return semantic plan + SQL plan without execution."""
        action, payload = self._normalize_integrate_source_action_request(action_name, payload if isinstance(payload, dict) else {})
        if action == "integrate_source_database":
            preview_payload = dict(payload)
            preview_payload["dry_run"] = True
            return self._build_integrate_source_database_preview(preview_payload)
        if action == "sync_source_database_entities":
            preview_payload = dict(payload)
            preview_payload["dry_run"] = True
            return self._build_sync_source_database_entities_preview(preview_payload)
        semantic_plan = self._compose_action_plan(action, payload)
        if not semantic_plan:
            return {
                "action": action,
                "success": False,
                "message": "No semantic plan composed by SOLF.",
            }
        sql_builder = str(semantic_plan.get("sql_builder") or "").strip()
        raw_plan_args = self._as_string_key_dict(semantic_plan.get("args")) or payload
        plan_args = self._as_string_key_dict(self._resolve_solf_placeholders(raw_plan_args, payload))
        sql_plan = None
        if sql_builder:
            sql_plan_raw = self._invoke_solf_clause_raw(sql_builder, [plan_args])
            sql_plan = self._as_string_key_dict(self._resolve_solf_placeholders(sql_plan_raw, plan_args))
        return {
            "action": action,
            "success": True,
            "dry_run": True,
            "semantic_plan": semantic_plan,
            "sql_plan": sql_plan,
            "trace": {
                "action_clause": "action_plan",
                "sql_clause": sql_builder or None,
            },
        }

    def bootstrap_db(self, recreate: bool = False) -> dict[str, Any]:
        """Initialize DB tables from sql_db schema helper."""
        try:
            from sql_db import create_tables
        except Exception as exc:
            return {"success": False, "message": f"Unable to import create_tables: {exc}"}

        try:
            with self.get_connection() as conn:
                create_tables(conn, recreate=recreate)
            self.scheduler.ensure_tables()
            return {
                "success": True,
                "message": "Database schema bootstrap completed.",
                "recreate": bool(recreate),
            }
        except Exception as exc:
            return {"success": False, "message": f"Database bootstrap failed: {exc}", "recreate": bool(recreate)}

    def repair_schema(self) -> dict[str, Any]:
        """Repair known legacy schema drifts without dropping tables."""
        try:
            from sql_db import repair_legacy_schema
        except Exception as exc:
            return {"success": False, "message": f"Unable to import repair_legacy_schema: {exc}"}

        try:
            with self.get_connection() as conn:
                repair_legacy_schema(conn)
            self.scheduler.ensure_tables()
            self._ensure_interaction_runtime_tables()
            return {
                "success": True,
                "message": "Schema repair completed.",
            }
        except Exception as exc:
            return {"success": False, "message": f"Schema repair failed: {exc}"}

    def scheduler_command(self, command: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute scheduler commands from SOLF/agent semantics."""
        cmd = str(command or "").strip().lower()
        payload = payload if isinstance(payload, dict) else {}

        plan = self._compose_scheduler_command(cmd, payload)
        if plan:
            policy = self._invoke_solf_policy("scheduler_command_policy", payload)
            directive_out = self._apply_workflow_rule_directives(
                stage="scheduler_command_policy",
                policy=policy,
                payload=payload,
            )
            if isinstance(directive_out.get("payload"), dict):
                payload = dict(directive_out.get("payload") or {})
            if str(policy.get("mode") or "allow").lower() == "deny":
                return {
                    "command": cmd,
                    "success": False,
                    "message": str(policy.get("message") or "Scheduler command denied by policy."),
                    "policy": policy,
                    "semantic_plan": plan,
                    "trace": {
                        "scheduler_clause": "scheduler_command",
                        "policy_clause": "scheduler_command_policy",
                    },
                }
            plan_args = self._as_string_key_dict(plan.get("args"))
            if plan_args:
                payload = plan_args

        if cmd == "start":
            self.scheduler.start()
            return {
                "command": cmd,
                "success": True,
                "message": "Scheduler started.",
                "trace": {
                    "scheduler_clause": "scheduler_command",
                    "policy_clause": "scheduler_command_policy",
                },
            }
        if cmd == "stop":
            self.scheduler.stop()
            return {
                "command": cmd,
                "success": True,
                "message": "Scheduler stopped.",
                "trace": {
                    "scheduler_clause": "scheduler_command",
                    "policy_clause": "scheduler_command_policy",
                },
            }
        if cmd == "add_job":
            query = str(payload.get("query") or "").strip()
            cron_expression = str(payload.get("cron_expression") or "").strip()
            tags = payload.get("tags") if isinstance(payload.get("tags"), list) else []
            job = self.scheduler.add_job(query=query, cron_expression=cron_expression, tags=tags)
            return {
                "command": cmd,
                "success": True,
                "message": "Job added.",
                "job": {
                    "job_id": job.job_id,
                    "query": job.query,
                    "cron_expression": job.cron_expression,
                    "tags": job.tags,
                    "enabled": job.enabled,
                },
                "trace": {
                    "scheduler_clause": "scheduler_command",
                    "policy_clause": "scheduler_command_policy",
                },
            }
        if cmd == "remove_job":
            job_id = str(payload.get("job_id") or "").strip()
            ok = self.scheduler.remove_job(job_id=job_id)
            return {
                "command": cmd,
                "success": ok,
                "message": "Job removed." if ok else "Job not found.",
                "trace": {
                    "scheduler_clause": "scheduler_command",
                    "policy_clause": "scheduler_command_policy",
                },
            }
        if cmd == "pause_job":
            job_id = str(payload.get("job_id") or "").strip()
            ok = self.scheduler.pause_job(job_id=job_id)
            return {
                "command": cmd,
                "success": ok,
                "message": "Job paused." if ok else "Job not found.",
                "trace": {
                    "scheduler_clause": "scheduler_command",
                    "policy_clause": "scheduler_command_policy",
                },
            }
        if cmd == "resume_job":
            job_id = str(payload.get("job_id") or "").strip()
            ok = self.scheduler.resume_job(job_id=job_id)
            return {
                "command": cmd,
                "success": ok,
                "message": "Job resumed." if ok else "Job not found.",
                "trace": {
                    "scheduler_clause": "scheduler_command",
                    "policy_clause": "scheduler_command_policy",
                },
            }
        if cmd == "list_jobs":
            jobs = self.scheduler.list_jobs()
            return {
                "command": cmd,
                "success": True,
                "jobs": [
                    {
                        "job_id": j.job_id,
                        "query": j.query,
                        "cron_expression": j.cron_expression,
                        "tags": j.tags,
                        "enabled": j.enabled,
                        "last_run_at": str(j.last_run_at) if j.last_run_at else None,
                        "next_run_at": str(j.next_run_at) if j.next_run_at else None,
                    }
                    for j in jobs
                ],
                "trace": {
                    "scheduler_clause": "scheduler_command",
                    "policy_clause": "scheduler_command_policy",
                },
            }

        return {"command": cmd, "success": False, "message": f"Unsupported scheduler command: {cmd}"}

    def state_command(self, command: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """State machine command API for workflow entities."""
        cmd = str(command or "").strip().lower()
        payload = payload if isinstance(payload, dict) else {}

        entity_type = str(payload.get("entity_type") or "").strip().lower()
        entity_id = int(payload.get("entity_id") or 0)

        if cmd == "get":
            state = self.state_machine.get_state(entity_type=entity_type, entity_id=entity_id)
            return {"command": cmd, **state}

        if cmd == "can":
            to_state = str(payload.get("to_state") or "").strip().lower()
            res = self.state_machine.can_transition(entity_type=entity_type, entity_id=entity_id, to_state=to_state)
            return {
                "command": cmd,
                "success": bool(res.success),
                "allowed": bool(res.allowed),
                "entity_type": res.entity_type,
                "entity_id": res.entity_id,
                "from_state": res.from_state,
                "to_state": res.to_state,
                "reason": res.reason,
                "data": res.data,
            }

        if cmd == "transition":
            to_state = str(payload.get("to_state") or "").strip().lower()
            event = str(payload.get("event") or "manual_transition").strip().lower()
            actor_ref = str(payload.get("actor_ref") or "system").strip().lower()
            metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

            policy_payload = {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "to_state": to_state,
                "event": event,
                "actor_ref": actor_ref,
                "metadata": metadata,
            }
            policy = self._invoke_solf_policy("workflow_transition_policy", policy_payload)
            directive_out = self._apply_workflow_rule_directives(
                stage="workflow_transition_policy",
                policy=policy,
                payload=policy_payload,
            )
            if isinstance(directive_out.get("payload"), dict):
                policy_payload = dict(directive_out.get("payload") or {})
                entity_type = str(policy_payload.get("entity_type") or entity_type).strip().lower()
                entity_id = int(policy_payload.get("entity_id") or entity_id or 0)
                to_state = str(policy_payload.get("to_state") or to_state).strip().lower()
                event = str(policy_payload.get("event") or event).strip().lower()
                actor_ref = str(policy_payload.get("actor_ref") or actor_ref).strip().lower()
                metadata = policy_payload.get("metadata") if isinstance(policy_payload.get("metadata"), dict) else metadata
            policy = self._harden_transition_policy(policy_payload, policy)
            if str(policy.get("mode") or "allow").lower() == "deny":
                return {
                    "command": cmd,
                    "success": False,
                    "allowed": False,
                    "message": str(policy.get("message") or "Transition denied by policy."),
                    "policy": policy,
                }

            res = self.state_machine.transition(
                entity_type=entity_type,
                entity_id=entity_id,
                to_state=to_state,
                event=event,
                actor_ref=actor_ref,
                metadata=metadata,
            )
            if res.success:
                self.event_bus.emit(
                    event_type="workflow.state_changed",
                    source_entity=entity_type,
                    entity_id=entity_id,
                    payload={
                        "from_state": res.from_state,
                        "to_state": res.to_state,
                        "event": event,
                        "actor_ref": actor_ref,
                    },
                    tags=["state_machine", entity_type],
                )
            return {
                "command": cmd,
                "success": bool(res.success),
                "allowed": bool(res.allowed),
                "entity_type": res.entity_type,
                "entity_id": res.entity_id,
                "from_state": res.from_state,
                "to_state": res.to_state,
                "event": res.event,
                "reason": res.reason,
                "policy": policy,
            }

        if cmd == "history":
            limit = int(payload.get("limit") or 20)
            rows = self.state_machine.history(entity_type=entity_type, entity_id=entity_id, limit=limit)
            return {
                "command": cmd,
                "success": True,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "count": len(rows),
                "rows": rows,
            }

        if cmd == "maintenance":
            max_rows = int(payload.get("max_rows") or 200)
            emitted = {"task.overdue": 0, "compliance.deadline_breach": 0, "workflow.case_sla_breach": 0}
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, name, assigned_to, due_date
                        FROM project_tasks
                        WHERE status NOT IN ('done', 'cancelled')
                          AND due_date IS NOT NULL
                          AND due_date < CURRENT_DATE
                        ORDER BY due_date ASC
                        LIMIT %s
                        """,
                        (max_rows,),
                    )
                    overdue_tasks = cur.fetchall() or []

                    cur.execute(
                        """
                        SELECT id, due_date_key, status
                        FROM compliance_actions
                        WHERE status NOT IN ('closed', 'completed', 'submitted', 'cancelled')
                          AND due_date_key IS NOT NULL
                          AND due_date_key < CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS BIGINT)
                        ORDER BY due_date_key ASC
                        LIMIT %s
                        """,
                        (max_rows,),
                    )
                    overdue_actions = cur.fetchall() or []

                    cur.execute(
                        """
                        SELECT id, case_no, status, sla_due_at
                        FROM workflow_cases
                        WHERE status NOT IN ('approved', 'rejected', 'cancelled')
                          AND sla_due_at IS NOT NULL
                          AND sla_due_at < NOW()
                        ORDER BY sla_due_at ASC
                        LIMIT %s
                        """,
                        (max_rows,),
                    )
                    overdue_cases = cur.fetchall() or []

            for row in overdue_tasks:
                task_id = int(row[0])
                self.event_bus.emit(
                    event_type="task.overdue",
                    source_entity="project_tasks",
                    entity_id=task_id,
                    payload={
                        "title": row[1],
                        "assigned_to": row[2],
                        "due_date": str(row[3]) if row[3] is not None else None,
                        "days_overdue": 1,
                    },
                    tags=["maintenance", "sla"],
                )
                emitted["task.overdue"] += 1

            for row in overdue_actions:
                action_id = int(row[0])
                self.event_bus.emit(
                    event_type="compliance.deadline_breach",
                    source_entity="compliance_actions",
                    entity_id=action_id,
                    payload={
                        "due_date_key": int(row[1]) if row[1] is not None else None,
                        "status": row[2],
                        "days_overdue": 1,
                    },
                    tags=["maintenance", "compliance"],
                )
                emitted["compliance.deadline_breach"] += 1

            for row in overdue_cases:
                case_id = int(row[0])
                self.event_bus.emit(
                    event_type="workflow.case_sla_breach",
                    source_entity="workflow_cases",
                    entity_id=case_id,
                    payload={
                        "case_no": row[1],
                        "status": row[2],
                        "sla_due_at": str(row[3]) if row[3] is not None else None,
                        "days_overdue": 1,
                    },
                    tags=["maintenance", "workflow"],
                )
                emitted["workflow.case_sla_breach"] += 1

            return {
                "command": cmd,
                "success": True,
                "emitted": emitted,
                "total_emitted": int(sum(emitted.values())),
            }

        return {"command": cmd, "success": False, "message": f"Unsupported state command: {cmd}"}

    def monitor_command(self, command: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Operational monitoring commands for workflow/event health."""
        cmd = str(command or "").strip().lower()
        payload = payload if isinstance(payload, dict) else {}

        if cmd == "stats":
            window_hours = int(payload.get("window_hours") or 24)
            metrics = self.workflow_monitor.collect_metrics(window_hours=window_hours)
            return {
                "command": cmd,
                "success": True,
                "metrics": metrics,
            }

        if cmd == "health":
            window_hours = int(payload.get("window_hours") or 24)
            status = self.workflow_monitor.health(window_hours=window_hours)
            return {
                "command": cmd,
                "success": True,
                **status,
            }

        if cmd == "snapshot":
            label = str(payload.get("label") or "").strip()
            snap = self.workflow_monitor.snapshot(label=label)
            return {
                "command": cmd,
                "success": True,
                **snap,
            }

        if cmd == "history":
            limit = int(payload.get("limit") or 20)
            rows = self.workflow_monitor.history(limit=limit)
            return {
                "command": cmd,
                "success": True,
                "count": len(rows),
                "rows": rows,
            }

        if cmd == "alerts":
            window_hours = int(payload.get("window_hours") or 24)
            result = self.workflow_reliability.evaluate_alerts(window_hours=window_hours)
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        if cmd == "reconcile":
            window_hours = int(payload.get("window_hours") or 24)
            actor_ref = str(payload.get("actor_ref") or "system")
            result = self.workflow_reliability.reconcile_alerts(
                window_hours=window_hours,
                actor_ref=actor_ref,
            )
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        if cmd == "incidents":
            status = str(payload.get("status") or "open")
            limit = int(payload.get("limit") or 50)
            rows = self.workflow_reliability.list_incidents(status=status, limit=limit)
            return {
                "command": cmd,
                "success": True,
                "status": status,
                "count": len(rows),
                "rows": rows,
            }

        if cmd == "incident":
            incident_id = int(payload.get("incident_id") or 0)
            action = str(payload.get("incident_action") or "")
            actor_ref = str(payload.get("actor_ref") or "system")
            if incident_id <= 0:
                return {"command": cmd, "success": False, "message": "incident_id must be > 0"}
            result = self.workflow_reliability.update_incident_status(
                incident_id=incident_id,
                action=action,
                actor_ref=actor_ref,
            )
            return {
                "command": cmd,
                **result,
            }

        # Dashboard commands (enhanced Phase 4)
        if cmd == "health-summary":
            result = self.dashboard_api.get_health_summary()
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        if cmd == "incident-summary":
            result = self.dashboard_api.get_incident_summary()
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        if cmd == "incidents-by-status":
            status = str(payload.get("status") or "open")
            limit = int(payload.get("limit") or 50)
            offset = int(payload.get("offset") or 0)
            result = self.dashboard_api.get_incidents_by_status(status=status, limit=limit, offset=offset)
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        if cmd == "metrics-timeline":
            metric_key = str(payload.get("metric_key") or "")
            hours = int(payload.get("hours") or 168)
            result = self.dashboard_api.get_metrics_timeline(metric_key=metric_key, hours=hours)
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        if cmd == "incident-trends":
            days = int(payload.get("days") or 7)
            result = self.dashboard_api.get_incident_trend_analysis(days=days)
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        if cmd == "metric-distribution":
            result = self.dashboard_api.get_metric_distribution()
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        if cmd == "sla-compliance":
            result = self.dashboard_api.get_sla_compliance()
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        # Notification commands (Phase 4+ enhancement)
        if cmd == "subscribe-notifications":
            subscriber_ref = str(payload.get("subscriber_ref") or "")
            email = payload.get("email")
            webhook_url = payload.get("webhook_url")
            phone = payload.get("phone")
            result = self.notifier_engine.subscribe(
                subscriber_ref=subscriber_ref,
                email=email,
                webhook_url=webhook_url,
                phone=phone,
            )
            return {
                "command": cmd,
                **result,
            }

        # Audit commands (Phase 5)
        if cmd == "audit-trail":
            entity_type = payload.get("entity_type")
            entity_id = payload.get("entity_id")
            actor_ref = payload.get("actor_ref")
            hours = int(payload.get("hours") or 168)
            result = self.audit_compliance.get_audit_trail(
                entity_type=entity_type,
                entity_id=entity_id,
                actor_ref=actor_ref,
                hours=hours,
            )
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        if cmd == "compliance-status":
            compliance_type = str(payload.get("compliance_type") or "")
            if not compliance_type:
                return {"command": cmd, "success": False, "message": "compliance_type is required"}
            result = self.audit_compliance.get_compliance_status_by_type(compliance_type=compliance_type)
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        if cmd == "compliance-report":
            compliance_type = str(payload.get("compliance_type") or "")
            if not compliance_type:
                return {"command": cmd, "success": False, "message": "compliance_type is required"}
            result = self.audit_compliance.generate_compliance_report(compliance_type=compliance_type)
            return {
                "command": cmd,
                "success": True,
                **result,
            }

        # What-If Analysis / Simulation commands (Interaction Type 6)
        if cmd == "create-scenario":
            base_project_id = int(payload.get("base_project_id") or 0)
            scenario_name = str(payload.get("scenario_name") or "").strip()
            scenario_type = str(payload.get("scenario_type") or "what_if").strip() or "what_if"
            description = payload.get("description")
            actor_ref = str(payload.get("actor_ref") or "system")
            modifications_raw = payload.get("modifications") or {}
            if base_project_id <= 0 or not scenario_name:
                return {"command": cmd, "success": False, "message": "base_project_id and scenario_name are required"}
            try:
                if isinstance(modifications_raw, list):
                    normalized_mods: dict[str, Any] = {}
                    for mod in modifications_raw:
                        if not isinstance(mod, dict):
                            continue
                        scenario_mod = ScenarioModification(
                            param_type=str(mod.get("param_type") or mod.get("modification_type") or "custom"),
                            param_name=str(mod.get("param_name") or mod.get("parameter_name") or "custom_param"),
                            original_value=float(mod.get("original_value", 0) or 0),
                            modified_value=float(mod.get("modified_value", mod.get("value", 0)) or 0),
                            reason=str(mod.get("reason") or mod.get("description") or "")
                        )
                        normalized_mods[scenario_mod.param_name] = {
                            "type": scenario_mod.param_type,
                            "original": scenario_mod.original_value,
                            "modified": scenario_mod.modified_value,
                            "reason": scenario_mod.reason,
                        }
                elif isinstance(modifications_raw, dict):
                    normalized_mods = modifications_raw
                else:
                    normalized_mods = {}

                result = self.project_simulator.create_scenario(
                    base_project_id=base_project_id,
                    scenario_name=scenario_name,
                    scenario_type=scenario_type,
                    description=description,
                    modifications=normalized_mods,
                    actor_ref=actor_ref,
                )
                return {
                    "command": cmd,
                    "success": True,
                    **result,
                }
            except Exception as e:
                return {"command": cmd, "success": False, "message": f"Error creating scenario: {e}"}

        if cmd == "simulate":
            scenario_id = int(payload.get("scenario_id") or 0)
            include_resource_check = bool(payload.get("include_resource_check", True))
            if scenario_id <= 0:
                return {"command": cmd, "success": False, "message": "scenario_id is required"}
            try:
                result = self.project_simulator.simulate_project_schedule(
                    scenario_id=scenario_id,
                    include_resource_check=include_resource_check,
                )
                return {
                    "command": cmd,
                    "success": True,
                    **result,
                }
            except Exception as e:
                return {"command": cmd, "success": False, "message": f"Error simulating schedule: {e}"}

        if cmd == "budget-impact":
            scenario_id = int(payload.get("scenario_id") or 0)
            if scenario_id <= 0:
                return {"command": cmd, "success": False, "message": "scenario_id is required"}
            try:
                result = self.project_simulator.calculate_budget_impact(scenario_id=scenario_id)
                return {
                    "command": cmd,
                    "success": True,
                    **result,
                }
            except Exception as e:
                return {"command": cmd, "success": False, "message": f"Error calculating budget impact: {e}"}

        if cmd == "compare-scenarios":
            scenario1_id = int(payload.get("scenario1_id") or 0)
            scenario2_id = int(payload.get("scenario2_id") or 0)
            if scenario1_id <= 0 or scenario2_id <= 0:
                return {"command": cmd, "success": False, "message": "scenario1_id and scenario2_id are required"}
            try:
                result = self.project_simulator.compare_scenarios(
                    base_scenario_id=scenario1_id,
                    comparison_scenario_id=scenario2_id,
                )
                return {
                    "command": cmd,
                    "success": True,
                    **result,
                }
            except Exception as e:
                return {"command": cmd, "success": False, "message": f"Error comparing scenarios: {e}"}

        if cmd == "scenarios":
            project_id = int(payload.get("project_id") or 0)
            status = str(payload.get("status") or "draft").strip() or "draft"
            if project_id <= 0:
                return {"command": cmd, "success": False, "message": "project_id is required"}
            try:
                result = self.project_simulator.list_scenarios(
                    base_project_id=project_id,
                    status=status,
                )
                return {
                    "command": cmd,
                    "success": True,
                    **result,
                }
            except Exception as e:
                return {"command": cmd, "success": False, "message": f"Error listing scenarios: {e}"}

        if cmd == "scenario-details":
            scenario_id = int(payload.get("scenario_id") or 0)
            if scenario_id <= 0:
                return {"command": cmd, "success": False, "message": "scenario_id is required"}
            try:
                result = self.project_simulator.get_scenario_details(scenario_id=scenario_id)
                return {
                    "command": cmd,
                    "success": True,
                    **result,
                }
            except Exception as e:
                return {"command": cmd, "success": False, "message": f"Error retrieving scenario: {e}"}

        return {"command": cmd, "success": False, "message": f"Unsupported monitor command: {cmd}"}

    def autonomous_query(self, question: str, max_steps: int = 2, record_history: bool = True) -> dict[str, Any]:
        """Autonomous retrieval loop: resolve scope -> attribute lookup -> grounded answer.
        
        Args:
            question: Natural language query.
            max_steps: Maximum planner steps (default: 2 for cost efficiency; typical queries need 1-2).
                      Hard capped at 4 to prevent runaway costs.
        
        Results are cached for 1 hour to avoid redundant planner/search calls.
        """
        question = str(question or "").strip()
        if not question:
            raise ValueError("question must not be empty")
        self._last_nl_request_text = question

        clause_request = self._resolve_solf_clause_request(question)
        if self._should_dispatch_solf_clause(question, clause_request):
            query_like = self._dispatch_solf_clause_request(question, clause_request)
            answer_text = str(query_like.get("answer") or "").strip()
            if record_history:
                self.state.history.append({"role": "user", "content": question})
                self.state.history.append({"role": "assistant", "content": answer_text})
            return {
                "answer": answer_text,
                "answer_source": str(query_like.get("source") or "solf_clause"),
                "answer_sources": [str(query_like.get("source") or "solf_clause")],
                "grounding": {
                    "parsed": {
                        "intent": "solf_clause",
                        "entity_name": None,
                        "attribute_name": None,
                        "confidence": 1.0,
                        "criteria": None,
                    },
                    "query_style": {
                        "mode": "allow",
                        "query_style": "solf_clause",
                        "initial_action": "invoke_solf_clause",
                        "reason": "explicit_solf_predicate_request",
                        "policy_clause": "query_style_policy",
                    },
                    "scope": None,
                    "discovery_search": None,
                    "attribute_result": None,
                    "criteria_result": None,
                    "direct_query_result": query_like,
                    "solf_clause_result": query_like.get("data") if isinstance(query_like.get("data"), dict) else {},
                },
                "policy": {
                    "query": {"mode": "allow", "reason": "explicit_solf_predicate_request"},
                    "style": {"mode": "allow", "reason": "explicit_solf_predicate_request"},
                    "answer": None,
                },
                "trace": [
                    {
                        "step": -1,
                        "tool": "dispatch_solf_clause",
                        "clause_name": query_like.get("clause_name"),
                        "success": bool(query_like.get("success")),
                        "source": str(query_like.get("source") or "solf_clause"),
                    }
                ],
                "model": "deterministic_solf" if str(query_like.get("source") or "") != "pending_clarification" else "deterministic_clarification",
                "status": query_like.get("status"),
                "thread_id": query_like.get("thread_id"),
            }

        self.logger.info("Autonomous query started: max_steps=%s question=%s", max_steps, question)

        parse_memo: dict[str, Any] = {}

        def _parsed_once(strict: bool = True) -> Any:
            if "parsed" not in parse_memo:
                try:
                    parse_memo["parsed"] = self.query_engine.parse_query(question)
                except Exception:
                    if strict:
                        raise
                    return None
            return parse_memo["parsed"]
        
        # Check cache first (avoid expensive planner + retrieval for repeated queries).
        if self.query_cache and not _should_skip_query_cache(question):
            cached = self.query_cache.get(question)
            if cached is not None and "answer" in cached:
                cached = dict(cached)
                cached["_cache_hit"] = True
                self.logger.info("Autonomous query cache hit")
                return cached

        # Hard cap to prevent excessive planner costs.
        max_steps = min(max(1, int(max_steps)), 4)
        parsed = _parsed_once()

        normalized_attr_name = str(parsed.attribute_name or "").strip().lower()
        looks_like_billing_amount_attr = bool(
            re.search(r"\b(?:bill|invoice|receipt|payment|charge|fee|rechnung|beleg|zahlung|quittung)\b", normalized_attr_name)
            and re.search(r"\b(?:amount|total|value|cost|price|due|sum|betrag|gesamt|kosten|preis)\b", normalized_attr_name)
        )
        if parsed.intent in {"attribute_lookup", "criteria_lookup"} and looks_like_billing_amount_attr:
            parsed.attribute_name = "amount"
            parsed_criteria = dict(parsed.criteria) if isinstance(parsed.criteria, dict) else {}
            attr_hints = [str(item).strip() for item in list(parsed_criteria.get("attribute_hints") or []) if str(item).strip()]
            for hint in ["amount", "total_amount", "gross_amount", "amount_total_due", "debit_total", "credit_total", "currency"]:
                if hint not in attr_hints:
                    attr_hints.append(hint)
            parsed_criteria["attribute_hints"] = attr_hints[:12]
            parsed_criteria.setdefault("semantic_frame", "interaction_value_document_context_bridge")
            parsed_criteria.setdefault("response_shape", "scalar")
            parsed_criteria["preferred_amount_semantics"] = "document_total_due"
            parsed.criteria = parsed_criteria

        parsed_payload = {
            "intent": parsed.intent,
            "entity_name": parsed.entity_name,
            "attribute_name": parsed.attribute_name,
            "confidence": parsed.confidence,
            "criteria": parsed.criteria,
        }
        style_policy = self._classify_query_style(question, parsed_payload)
        selected_model = self._select_model_by_confidence(parsed_payload.get("confidence"), default=EXTRACT_MODEL)
        state: dict[str, Any] = {
            "question": question,
            "parsed": parsed_payload,
            "query_style": style_policy,
            "scope": None,
            "discovery_search": None,
            "attribute_result": None,
            "criteria_result": None,
            "criteria_contextual_result": None,
            "trace": [],
            "telemetry": {
                "parser_calls": 1,
                "planner_llm_calls": 0,
                "query_engine_answer_calls": 0,
                "criteria_contextual_calls": 0,
                "contextual_fallback_calls": 0,
                "media_intent_llm_calls": 0,
                "deterministic_loop_used": False,
            },
        }

        def _attach_evidence_bundle(result: dict[str, Any]) -> dict[str, Any]:
            return self._consolidate_query_result(question, result, parsed_override=parsed)

        query_policy_payload = {
            "question": question,
            "parsed": state["parsed"],
        }
        query_policy = self._invoke_solf_policy("interaction_query_policy", query_policy_payload)
        directive_out = self._apply_workflow_rule_directives(
            stage="interaction_query_policy",
            policy=query_policy,
            question=question,
            parsed_payload=parsed_payload,
        )
        if isinstance(directive_out.get("parsed_payload"), dict):
            parsed_payload = dict(directive_out.get("parsed_payload") or {})
            parsed.intent = str(parsed_payload.get("intent") or parsed.intent or "").strip() or parsed.intent
            parsed.entity_name = str(parsed_payload.get("entity_name") or parsed.entity_name or "").strip() or parsed.entity_name
            parsed.attribute_name = str(parsed_payload.get("attribute_name") or parsed.attribute_name or "").strip() or parsed.attribute_name
            parsed.relation_name = str(parsed_payload.get("relation_name") or parsed.relation_name or "").strip() or parsed.relation_name
            if isinstance(parsed_payload.get("criteria"), dict):
                parsed.criteria = dict(parsed_payload.get("criteria") or {})
            if isinstance(parsed_payload.get("attribute_names"), list):
                parsed.attribute_names = [str(item).strip() for item in list(parsed_payload.get("attribute_names") or []) if str(item).strip()]
        state["trace"].append(
            {
                "step": -1,
                "policy": "interaction_query_policy",
                "result": query_policy,
            }
        )
        if directive_out.get("applied"):
            state["trace"].append(
                {
                    "step": -1,
                    "policy": "workflow_rule_directives",
                    "result": {
                        "stage": "interaction_query_policy",
                        "applied_rules": list(directive_out.get("applied_rules") or []),
                    },
                }
            )
        state["trace"].append(
            {
                "step": -1,
                "policy": "query_style_policy",
                "result": style_policy,
            }
        )

        if style_policy.get("mode") == "deny":
            answer_text = str(style_policy.get("message") or "This query style is blocked by policy.").strip()
            answer_text = f"[source: policy_blocked] {answer_text}".strip()
            if record_history:
                self.state.history.append({"role": "user", "content": question})
                self.state.history.append({"role": "assistant", "content": answer_text})
            return {
                "answer": answer_text,
                "answer_source": "policy_blocked",
                "answer_sources": ["policy_blocked"],
                "grounding": {
                    "parsed": state.get("parsed"),
                    "query_style": state.get("query_style"),
                    "scope": None,
                    "discovery_search": None,
                    "attribute_result": None,
                    "criteria_result": None,
                },
                "policy": {
                    "query": query_policy,
                    "style": style_policy,
                    "answer": None,
                },
                "trace": state.get("trace", []),
                "model": selected_model,
            }

        if query_policy.get("mode") == "deny":
            answer_text = str(query_policy.get("message") or "This interaction is blocked by domain policy.").strip()
            answer_text = f"[source: policy_blocked] {answer_text}".strip()
            if record_history:
                self.state.history.append({"role": "user", "content": question})
                self.state.history.append({"role": "assistant", "content": answer_text})
            return {
                "answer": answer_text,
                "answer_source": "policy_blocked",
                "answer_sources": ["policy_blocked"],
                "grounding": {
                    "parsed": state.get("parsed"),
                    "scope": None,
                    "discovery_search": None,
                    "attribute_result": None,
                },
                "policy": {
                    "query": query_policy,
                    "style": style_policy,
                    "answer": None,
                },
                "trace": state.get("trace", []),
                "model": selected_model,
            }

        if query_policy.get("mode") == "escalate":
            max_steps = min(max(1, int(max_steps)) + 1, 8)

        require_contextual_resolution = False
        if parsed.intent in {"attribute_lookup", "criteria_lookup"}:
            try:
                require_contextual_resolution = self.query_engine._requires_contextual_value_resolution(
                    question=question,
                    parsed=parsed,
                    criteria=parsed.criteria if isinstance(parsed.criteria, dict) else {},
                    direct_result={"attribute_name": parsed.attribute_name},
                )
            except Exception:
                require_contextual_resolution = False

        # Direct factual fallback: if QueryEngine can already answer, use it and skip planner drift.
        # This prevents false [source: not_found] when resolve_scope returns empty but SQL facts exist.
        aggregate_value_question = bool(
            hasattr(self.query_engine, "_looks_like_travel_expense_total_question")
            and self.query_engine._looks_like_travel_expense_total_question(question)
        )
        if (
            parsed.intent in {"attribute_lookup", "semantic_lookup", "relationship_lookup", "criteria_lookup"}
            and (
                aggregate_value_question
                or (
                    not self.query_engine._is_value_document_context_question(question)
                    and not require_contextual_resolution
                )
            )
        ):
            state["telemetry"]["query_engine_answer_calls"] = int((state.get("telemetry") or {}).get("query_engine_answer_calls", 0)) + 1
            try:
                direct_answer = self.query_engine.answer(question)
            except Exception:
                direct_answer = None

            if isinstance(direct_answer, dict):
                direct_source = str(direct_answer.get("source") or "").strip().lower()
                direct_text = str(direct_answer.get("answer") or "").strip()
                if direct_source and direct_source != "not_found" and direct_text:
                    state["trace"].append(
                        {
                            "step": 0,
                            "tool": "query_engine_direct_fallback",
                            "source": direct_source,
                            "used": True,
                        }
                    )

                    grounding = {
                        "parsed": state.get("parsed"),
                        "query_style": state.get("query_style"),
                        "scope": None,
                        "discovery_search": None,
                        "attribute_result": None,
                        "criteria_result": None,
                        "direct_query_result": direct_answer,
                    }
                    answer_policy_payload = {
                        "question": question,
                        "grounding": grounding,
                    }
                    answer_policy = self._invoke_solf_policy("interaction_answer_policy", answer_policy_payload)
                    state["trace"].append(
                        {
                            "step": len(state.get("trace", [])),
                            "policy": "interaction_answer_policy",
                            "result": answer_policy,
                        }
                    )

                    if answer_policy.get("mode") == "deny":
                        answer_text = str(answer_policy.get("message") or "Answer generation is blocked by domain policy.").strip()
                        answer_text = f"[source: policy_blocked] {answer_text}".strip()
                        if record_history:
                            self.state.history.append({"role": "user", "content": question})
                            self.state.history.append({"role": "assistant", "content": answer_text})
                        return {
                            "answer": answer_text,
                            "answer_source": "policy_blocked",
                            "answer_sources": ["policy_blocked"],
                            "grounding": grounding,
                            "request_telemetry": dict(state.get("telemetry") or {}),
                            "policy": {
                                "query": query_policy,
                                "style": style_policy,
                                "answer": answer_policy,
                            },
                            "trace": state.get("trace", []),
                            "model": selected_model,
                        }

                    answer_text = direct_text
                    answer_sources = [direct_source]
                    result = {
                        "answer": answer_text,
                        "answer_source": direct_source,
                        "answer_sources": answer_sources,
                        "grounding": grounding,
                        "request_telemetry": dict(state.get("telemetry") or {}),
                        "policy": {
                            "query": query_policy,
                            "style": style_policy,
                            "answer": answer_policy,
                        },
                        "trace": state.get("trace", []),
                        "model": selected_model,
                    }
                    if record_history:
                        self.state.history.append({"role": "user", "content": question})
                        self.state.history.append({"role": "assistant", "content": answer_text})
                    if self.query_cache:
                        self.query_cache.put(question, _attach_evidence_bundle(result))
                    return _attach_evidence_bundle(result)

        # Fast path: direct attribute questions should hit SQL/object lookup first.
        # This avoids planner drift when Qdrant/Discovery scope is empty but structured data exists.
        # Keep contextual value questions on grounded criteria/LLM resolution paths.
        if parsed.intent == "attribute_lookup" and parsed.entity_name and parsed.attribute_name and not require_contextual_resolution:
            direct = self.query_engine.lookup_attribute(
                attribute_name=parsed.attribute_name,
                entity_name=parsed.entity_name,
                scope=None,
            )
            state["attribute_result"] = direct
            state["trace"].append(
                {
                    "step": 0,
                    "tool": "lookup_attribute_fast_path",
                    "attribute_name": parsed.attribute_name,
                    "entity_name": parsed.entity_name,
                    "found": bool(direct),
                }
            )
            if direct:
                grounding = {
                    "parsed": state.get("parsed"),
                    "query_style": state.get("query_style"),
                    "scope": None,
                    "discovery_search": None,
                    "attribute_result": direct,
                    "criteria_result": None,
                }
                answer_policy_payload = {
                    "question": question,
                    "grounding": grounding,
                }
                answer_policy = self._invoke_solf_policy("interaction_answer_policy", answer_policy_payload)
                state["trace"].append(
                    {
                        "step": len(state.get("trace", [])),
                        "policy": "interaction_answer_policy",
                        "result": answer_policy,
                    }
                )
                if answer_policy.get("mode") == "deny":
                    answer_text = str(answer_policy.get("message") or "Answer generation is blocked by domain policy.").strip()
                    answer_text = f"[source: policy_blocked] {answer_text}".strip()
                    if record_history:
                        self.state.history.append({"role": "user", "content": question})
                        self.state.history.append({"role": "assistant", "content": answer_text})
                    return {
                        "answer": answer_text,
                        "answer_source": "policy_blocked",
                        "answer_sources": ["policy_blocked"],
                        "grounding": grounding,
                        "policy": {
                            "query": query_policy,
                            "style": style_policy,
                            "answer": answer_policy,
                        },
                        "trace": state.get("trace", []),
                        "model": selected_model,
                    }

                display_attr = self.query_engine._get_attribute_display_name(direct["attribute_name"], parsed.language)
                display_value = self.query_engine._repair_mojibake_value(direct.get("attribute_value")) if hasattr(self.query_engine, "_repair_mojibake_value") else direct.get("attribute_value")
                if str(parsed.language or "en").lower() == "de":
                    answer_text = f"[source: sql_exact] {display_attr} für {direct['entity_name']} ist {display_value}."
                else:
                    answer_text = f"[source: sql_exact] {display_attr} for {direct['entity_name']} is {display_value}."

                if record_history:
                    self.state.history.append({"role": "user", "content": question})
                    self.state.history.append({"role": "assistant", "content": answer_text})

                result = {
                    "answer": answer_text,
                    "answer_source": "sql_exact",
                    "answer_sources": ["sql_exact"],
                    "grounding": grounding,
                    "request_telemetry": dict(state.get("telemetry") or {}),
                    "policy": {
                        "query": query_policy,
                        "style": style_policy,
                        "answer": answer_policy,
                    },
                    "trace": state.get("trace", []),
                    "model": selected_model,
                }
                if self.query_cache:
                    self.query_cache.put(question, _attach_evidence_bundle(result))
                return _attach_evidence_bundle(result)

        parsed_intent_hint = str((state.get("parsed") or {}).get("intent") or "").strip().lower()
        parsed_confidence_hint = float((state.get("parsed") or {}).get("confidence") or 0.0)
        use_deterministic_loop = bool(
            parsed_intent_hint in {"attribute_lookup", "criteria_lookup"}
            and (
                parsed_confidence_hint >= 0.85
                or require_contextual_resolution
            )
        )
        state["telemetry"]["deterministic_loop_used"] = bool(use_deterministic_loop)

        for step in range(max(1, int(max_steps))):
            self.logger.info("Autonomous query workflow step=%s", step)
            parsed_intent = str((state.get("parsed") or {}).get("intent") or "").strip().lower()
            has_criteria_result = bool(state.get("criteria_result"))

            if use_deterministic_loop:
                if (
                    not has_criteria_result
                    and (
                        parsed_intent == "criteria_lookup"
                        or self.query_engine._is_value_document_context_question(question)
                        or require_contextual_resolution
                    )
                ):
                    decision = {
                        "action": "search_criteria",
                        "attribute_name": str((state.get("parsed") or {}).get("attribute_name") or "").strip() or None,
                        "entity_name": str((state.get("parsed") or {}).get("entity_name") or "").strip() or None,
                        "rationale": "High-confidence parsed intent allows deterministic criteria retrieval without planner LLM.",
                    }
                elif (
                    parsed_intent == "attribute_lookup"
                    and not state.get("attribute_result")
                    and str((state.get("parsed") or {}).get("attribute_name") or "").strip()
                    and not (
                        require_contextual_resolution
                        or self.query_engine._is_value_document_context_question(question)
                    )
                ):
                    decision = {
                        "action": "lookup_attribute",
                        "attribute_name": str((state.get("parsed") or {}).get("attribute_name") or "").strip() or None,
                        "entity_name": str((state.get("parsed") or {}).get("entity_name") or "").strip() or None,
                        "rationale": "High-confidence parsed intent allows deterministic attribute lookup without planner LLM.",
                    }
                else:
                    decision = {
                        "action": "finalize",
                        "attribute_name": None,
                        "entity_name": None,
                        "rationale": "Deterministic retrieval path is complete; finalize without planner LLM.",
                    }
            elif (
                step == 0
                and (
                    self.query_engine._is_value_document_context_question(question)
                    or require_contextual_resolution
                )
                and not state.get("criteria_result")
            ):
                decision = {
                    "action": "search_criteria",
                    "attribute_name": str((state.get("parsed") or {}).get("attribute_name") or "").strip() or None,
                    "entity_name": str((state.get("parsed") or {}).get("entity_name") or "").strip() or None,
                    "rationale": "Value + document-context question detected; run criteria retrieval first to ground extraction.",
                }
            else:
                decision = self._planner_step(question, state, step)
                state["telemetry"]["planner_llm_calls"] = int((state.get("telemetry") or {}).get("planner_llm_calls", 0)) + 1
            action = decision["action"]

            # Criteria-intent guardrail: run SQL criteria retrieval before semantic-only
            # discovery loops when the parser has already identified a list/filter ask.
            if parsed_intent == "criteria_lookup" and not has_criteria_result and action in {"search_discovery"}:
                action = "search_criteria"
                decision["action"] = action
                decision["rationale"] = (
                    "Parsed intent is criteria_lookup; prioritize criteria retrieval first "
                    "to avoid discovery-only drift on document/note list queries."
                )

            if parsed_intent == "criteria_lookup" and not has_criteria_result and action == "finalize":
                action = "search_criteria"
                decision["action"] = action
                decision["rationale"] = (
                    "Parsed intent is criteria_lookup and no criteria_result exists yet; "
                    "run criteria retrieval before finalizing."
                )

            if action == "resolve_scope" and self._scope_is_empty(state.get("scope")):
                if parsed_intent == "criteria_lookup" and not has_criteria_result:
                    action = "search_criteria"
                    decision["action"] = action
                    decision["rationale"] = (
                        "Scope resolution returned no vector/discovery candidates, "
                        "so run SQL criteria retrieval without scope constraints before finalizing."
                    )
                else:
                    action = "finalize"
                    decision["action"] = action
                    decision["rationale"] = (
                        "Scope resolution already returned no Qdrant or Discovery candidates, "
                        "so repeating resolve_scope would not add new evidence."
                    )

            # Re-running scope resolution with the same question is redundant once
            # scope data already exists in state; route to the next productive step.
            if action == "resolve_scope" and isinstance(state.get("scope"), dict):
                if parsed_intent == "criteria_lookup" and not has_criteria_result:
                    action = "search_criteria"
                    decision["rationale"] = (
                        "Scope already resolved for this request; continue with criteria retrieval "
                        "instead of repeating vector scope search."
                    )
                elif parsed_intent == "attribute_lookup" and not state.get("attribute_result"):
                    action = "lookup_attribute"
                    decision["rationale"] = (
                        "Scope already resolved for this request; continue with attribute lookup "
                        "instead of repeating vector scope search."
                    )
                elif parsed_intent == "semantic_lookup" and not isinstance(state.get("discovery_search"), dict):
                    action = "search_discovery"
                    decision["rationale"] = (
                        "Scope already resolved for this request; continue with discovery search "
                        "instead of repeating vector scope search."
                    )
                else:
                    action = "finalize"
                    decision["rationale"] = (
                        "Scope was already resolved and no additional retrieval step is pending; "
                        "finalizing to avoid repeated identical searches."
                    )
                decision["action"] = action

            if action == "search_discovery" and isinstance(state.get("discovery_search"), dict):
                if parsed_intent == "criteria_lookup" and not has_criteria_result:
                    action = "search_criteria"
                    decision["rationale"] = (
                        "Discovery search already ran for this request; continue with criteria retrieval "
                        "instead of repeating discovery search."
                    )
                elif parsed_intent == "attribute_lookup" and not state.get("attribute_result"):
                    action = "lookup_attribute"
                    decision["rationale"] = (
                        "Discovery search already ran for this request; continue with attribute lookup "
                        "instead of repeating discovery search."
                    )
                else:
                    action = "finalize"
                    decision["rationale"] = (
                        "Discovery search was already executed and no new retrieval step is pending; "
                        "finalizing to avoid repeated identical searches."
                    )
                decision["action"] = action
            state["trace"].append({"step": step, "planner": decision})

            if action == "resolve_scope":
                scope_limit = 36 if parsed_intent == "criteria_lookup" else 12
                scope = self.query_engine.resolve_search_scope(question, limit=scope_limit)
                state["scope"] = {
                    "candidate_count": scope.get("candidate_count", 0),
                    "discovery_candidate_count": scope.get("discovery_candidate_count", 0),
                    "qdrant_search_mode": scope.get("qdrant_search_mode"),
                    "qdrant_collection": scope.get("qdrant_collection"),
                    "node_ids": scope.get("node_ids", scope.get("object_ids", [])),
                    "edge_ids": scope.get("edge_ids", scope.get("relationship_ids", [])),
                    "doc_ids": scope.get("doc_ids", []),
                    "top_candidates": (scope.get("candidates") or [])[:3],
                    "discovery_results": (scope.get("discovery_results") or [])[:5],
                    "nodes": (scope.get("nodes") or scope.get("objects") or [])[:5],
                    "edges": (scope.get("edges") or scope.get("relationships") or [])[:5],
                    "documents": (scope.get("documents") or [])[:36],
                }
                continue

            if action == "search_discovery":
                discovery_payload = self.search_discovery(question, limit=8)
                state["discovery_search"] = {
                    "candidate_count": discovery_payload.get("candidate_count", 0),
                    "summary": discovery_payload.get("summary"),
                    "results": (discovery_payload.get("results") or [])[:5],
                }
                state["trace"].append(
                    {
                        "step": step,
                        "tool": "search_discovery",
                        "candidate_count": discovery_payload.get("candidate_count", 0),
                    }
                )
                continue

            if action == "lookup_attribute":
                attribute_name = (
                    str(decision.get("attribute_name") or "").strip()
                    or parsed.attribute_name
                    or ""
                )
                entity_name = str(decision.get("entity_name") or "").strip() or parsed.entity_name
                if not attribute_name:
                    state["trace"].append(
                        {
                            "step": step,
                            "tool": "lookup_attribute",
                            "result": "skipped_missing_attribute_name",
                        }
                    )
                    continue

                lookup_criteria = dict(parsed.criteria) if isinstance(parsed.criteria, dict) else {}
                has_relation_context = bool(
                    str(parsed.relation_name or "").strip()
                    or list(lookup_criteria.get("relation_filters") or [])
                    or list(lookup_criteria.get("relationship_filters") or [])
                )
                if has_relation_context:
                    # Enforce relationship traversal semantics for relation-driven questions
                    # so broad fallbacks cannot drift to unrelated entities.
                    lookup_criteria["strict_relation_lookup"] = True

                result = self.query_engine.lookup_attribute(
                    attribute_name=attribute_name,
                    entity_name=entity_name,
                    relation_name=parsed.relation_name,
                    scope=state.get("scope") if isinstance(state.get("scope"), dict) else None,
                    criteria=lookup_criteria if lookup_criteria else None,
                )

                # Canonical recovery for person->companies shareholding questions when
                # planner emits non-canonical attributes such as shares_held/holds_shares_in.
                if not result:
                    normalized_attr = str(attribute_name or "").strip().lower()
                    relation_filters = list(lookup_criteria.get("relation_filters") or []) + list(lookup_criteria.get("relationship_filters") or [])

                    # LLM plans often keep relationship filters nested under llm_query_plan.filters.
                    llm_plan = lookup_criteria.get("llm_query_plan") if isinstance(lookup_criteria.get("llm_query_plan"), dict) else {}
                    plan_filters = llm_plan.get("filters") if isinstance(llm_plan.get("filters"), dict) else {}
                    nested_relationship_filters = plan_filters.get("relationship_filters") if isinstance(plan_filters.get("relationship_filters"), list) else []
                    for rf in nested_relationship_filters:
                        if not isinstance(rf, dict):
                            continue
                        rel_type = str(rf.get("relationship_type") or rf.get("relation") or "").strip()
                        target_name = str(rf.get("target_name") or rf.get("target_entity_name") or "").strip()
                        mapped = {
                            "relationship_names": [rel_type] if rel_type else [],
                            "target_name": target_name,
                        }
                        relation_filters.append(mapped)

                    relationship_blob = " ".join(
                        " ".join(
                            [
                                str(item.get("relationship_type") or ""),
                                " ".join(str(x) for x in (item.get("relationship_names") or [])),
                                str(item.get("target_name") or ""),
                            ]
                        )
                        for item in relation_filters
                        if isinstance(item, dict)
                    ).lower()
                    share_query_hint = bool(
                        re.search(r"\b(share|shares|shareholder|shareholders|ownership|anteil|gesellschafter|aktion[aä]r)\b", str(question or ""), flags=re.IGNORECASE)
                    )
                    non_canonical_share_attr = normalized_attr in {
                        "shares_held",
                        "holds_shares_in",
                        "shareholdings",
                        "shareholding",
                        "ownership",
                    }
                    relation_share_hint = bool(re.search(r"share|owner|ownership|gesellschafter|aktion", relationship_blob, flags=re.IGNORECASE))

                    if share_query_hint and (non_canonical_share_attr or relation_share_hint):
                        canonical_criteria = dict(lookup_criteria)
                        canonical_criteria.setdefault("question_type", "companies_held")
                        canonical_criteria.setdefault("semantic_frame", "shareholders_person")
                        canonical_criteria.setdefault("relationship_concept", "shareholder")
                        if relation_filters:
                            canonical_criteria["relation_filters"] = relation_filters
                            canonical_criteria["strict_relation_lookup"] = True
                        else:
                            canonical_criteria.pop("strict_relation_lookup", None)

                        recovered = self.query_engine.lookup_attribute(
                            attribute_name="shareholders",
                            entity_name=entity_name,
                            relation_name="shareholder",
                            scope=state.get("scope") if isinstance(state.get("scope"), dict) else None,
                            criteria=canonical_criteria,
                        )
                        if recovered:
                            result = recovered
                            attribute_name = "shareholders"
                            state["trace"].append(
                                {
                                    "step": step,
                                    "tool": "lookup_attribute_shareholding_recovery",
                                    "attribute_name": "shareholders",
                                    "entity_name": entity_name,
                                    "found": True,
                                }
                            )

                    # If canonical recovery still misses, ensure semantic scope is populated
                    # so final answer synthesis can use grounded qdrant/discovery evidence.
                    if not result and share_query_hint and not isinstance(state.get("scope"), dict):
                        try:
                            scope_question = str(question or "").strip()
                            resolved_scope_entity = ""
                            if str(entity_name or "").strip():
                                try:
                                    with self.query_engine._get_connection() as conn:
                                        details = self.query_engine._resolve_entity_name_for_lookup_details(conn, entity_name)
                                    if isinstance(details, dict):
                                        resolved_scope_entity = str(details.get("resolved_name") or "").strip()
                                except Exception:
                                    resolved_scope_entity = ""

                            if resolved_scope_entity and str(entity_name or "").strip():
                                try:
                                    scope_question = re.sub(
                                        re.escape(str(entity_name)),
                                        resolved_scope_entity,
                                        scope_question,
                                        count=1,
                                        flags=re.IGNORECASE,
                                    )
                                except Exception:
                                    # Safe fallback shape when replacement fails.
                                    scope_question = f"What companies does {resolved_scope_entity} hold shares in?"

                            scope = self.query_engine.resolve_search_scope(scope_question, limit=12)
                            state["scope"] = {
                                "candidate_count": scope.get("candidate_count", 0),
                                "discovery_candidate_count": scope.get("discovery_candidate_count", 0),
                                "qdrant_search_mode": scope.get("qdrant_search_mode"),
                                "qdrant_collection": scope.get("qdrant_collection"),
                                "node_ids": scope.get("node_ids", scope.get("object_ids", [])),
                                "edge_ids": scope.get("edge_ids", scope.get("relationship_ids", [])),
                                "doc_ids": scope.get("doc_ids", []),
                                "top_candidates": (scope.get("candidates") or [])[:5],
                                "discovery_results": (scope.get("discovery_results") or [])[:5],
                                "nodes": (scope.get("nodes") or scope.get("objects") or [])[:5],
                                "edges": (scope.get("edges") or scope.get("relationships") or [])[:5],
                                "documents": (scope.get("documents") or [])[:36],
                            }
                            state["trace"].append(
                                {
                                    "step": step,
                                    "tool": "shareholding_scope_recovery",
                                    "scope_question": scope_question,
                                    "resolved_scope_entity": resolved_scope_entity or None,
                                    "candidate_count": int(scope.get("candidate_count", 0) or 0),
                                    "discovery_candidate_count": int(scope.get("discovery_candidate_count", 0) or 0),
                                }
                            )
                        except Exception:
                            pass

                state["attribute_result"] = result
                state["trace"].append(
                    {
                        "step": step,
                        "tool": "lookup_attribute",
                        "attribute_name": attribute_name,
                        "entity_name": entity_name,
                        "found": bool(result),
                    }
                )
                continue

            if action == "search_criteria":
                scope_payload = state.get("scope") if isinstance(state.get("scope"), dict) else None
                criteria_result = self.search_criteria(question, scope=scope_payload, parsed=parsed)
                state["criteria_result"] = criteria_result if criteria_result.get("success") else None
                state["trace"].append(
                    {
                        "step": step,
                        "tool": "search_criteria",
                        "found": bool(criteria_result.get("success")),
                        "count": int(criteria_result.get("count", 0) or 0),
                    }
                )
                continue

            if action == "finalize":
                break

        grounding = {
            "parsed": state.get("parsed"),
            "query_style": state.get("query_style"),
            "scope": state.get("scope"),
            "discovery_search": state.get("discovery_search"),
            "attribute_result": state.get("attribute_result"),
            "criteria_result": state.get("criteria_result"),
            "criteria_contextual_result": state.get("criteria_contextual_result"),
        }

        # Generic bridge: when criteria retrieval finds key documents and the user asks
        # for a value-style answer (not a list request), run grounded contextual resolution.
        parsed_intent = str((state.get("parsed") or {}).get("intent") or "").strip().lower()
        parsed_criteria = (state.get("parsed") or {}).get("criteria") if isinstance((state.get("parsed") or {}).get("criteria"), dict) else {}
        response_format = str((parsed_criteria or {}).get("response_format") or "").strip().lower()
        parsed_attribute_name = str((state.get("parsed") or {}).get("attribute_name") or "").strip().lower()
        attr_value_like = bool(
            re.search(
                r"(?:amount|total|cost|price|value|due|balance|sum|fee|charge)",
                parsed_attribute_name,
                flags=re.IGNORECASE,
            )
        )
        criteria_result_payload = state.get("criteria_result") if isinstance(state.get("criteria_result"), dict) else None
        needs_value_evidence_hydration = bool(
            self._is_value_question(question, parsed)
            or self.query_engine._is_value_document_context_question(question)
            or require_contextual_resolution
        )
        if (
            isinstance(criteria_result_payload, dict)
            and bool(criteria_result_payload.get("success"))
            and (
                self.query_engine._is_value_document_context_question(question)
                or require_contextual_resolution
            )
            and (
                require_contextual_resolution
                or attr_value_like
                or not self.query_engine._criteria_question_prefers_listing(question, response_format)
            )
        ):
            criteria_for_context = dict(criteria_result_payload)
            if (
                needs_value_evidence_hydration
                and not list(criteria_for_context.get("matches") or [])
                and int(criteria_for_context.get("count", 0) or 0) > 0
            ):
                try:
                    scope_for_retry = state.get("scope") if isinstance(state.get("scope"), dict) else {}
                    if not scope_for_retry:
                        scope_for_retry = self.query_engine.resolve_search_scope(question)
                        if isinstance(scope_for_retry, dict):
                            state["scope"] = scope_for_retry
                            grounding["scope"] = scope_for_retry

                    enriched_criteria = self.search_criteria(
                        question,
                        scope=scope_for_retry if isinstance(scope_for_retry, dict) else None,
                        parsed=parsed,
                    )
                    if isinstance(enriched_criteria, dict) and bool(enriched_criteria.get("success")):
                        enriched_matches = list(enriched_criteria.get("matches") or [])
                        if enriched_matches:
                            criteria_for_context = dict(enriched_criteria)
                except Exception:
                    pass

            if (
                needs_value_evidence_hydration
                and not list(criteria_for_context.get("matches") or [])
                and int(criteria_for_context.get("count", 0) or 0) > 0
            ):
                try:
                    scope_for_docs = state.get("scope") if isinstance(state.get("scope"), dict) else {}
                    scope_documents = list(scope_for_docs.get("documents") or []) if isinstance(scope_for_docs, dict) else []
                    parsed_criteria_for_terms = parsed.criteria if isinstance(parsed.criteria, dict) else {}
                    must_contain_terms = [
                        str(item).strip()
                        for item in list(parsed_criteria_for_terms.get("must_contain") or [])
                        if str(item).strip()
                    ]

                    hydrated_matches: list[dict[str, Any]] = []
                    seen_doc_ids: set[int] = set()
                    for item in scope_documents:
                        if not isinstance(item, dict):
                            continue
                        doc_id = item.get("doc_id")
                        if not isinstance(doc_id, int) or doc_id in seen_doc_ids:
                            continue
                        doc_blob = " ".join(
                            [
                                str(item.get("doc_name") or item.get("title") or ""),
                                str(item.get("doc_path") or item.get("file_path") or ""),
                                str(item.get("doc_theme") or ""),
                                str(item.get("doc_cat") or ""),
                                str(item.get("doc_type") or ""),
                                str(item.get("keyword_text") or ""),
                            ]
                        ).lower()
                        matched_terms = [
                            term
                            for term in must_contain_terms
                            if str(term).strip() and str(term).strip().lower() in doc_blob
                        ]
                        seen_doc_ids.add(doc_id)
                        hydrated_matches.append(
                            {
                                "doc_id": doc_id,
                                "doc_name": item.get("doc_name") or item.get("title"),
                                "doc_path": item.get("doc_path") or item.get("file_path"),
                                "title": item.get("title") or item.get("doc_name"),
                                "matched_terms": matched_terms[:4],
                                "score": float(item.get("score") or 0.0),
                            }
                        )
                        if len(hydrated_matches) >= 12:
                            break

                    if hydrated_matches:
                        criteria_for_context = dict(criteria_for_context)
                        criteria_for_context["matches"] = hydrated_matches
                except Exception:
                    pass

            # Deterministic value-evidence hydration pass: even when planner did not
            # resolve scope, hydrate criteria matches from scope documents so
            # criteria contextual resolver can extract grounded totals.
            if (
                needs_value_evidence_hydration
                and not list(criteria_for_context.get("matches") or [])
                and int(criteria_for_context.get("count", 0) or 0) > 0
            ):
                try:
                    scope_for_docs = state.get("scope") if isinstance(state.get("scope"), dict) else {}
                    scope_documents = list(scope_for_docs.get("documents") or []) if isinstance(scope_for_docs, dict) else []

                    if not scope_documents:
                        resolved_scope = self.query_engine.resolve_search_scope(question, limit=24)
                        if isinstance(resolved_scope, dict):
                            scope_for_docs = resolved_scope
                            scope_documents = list(resolved_scope.get("documents") or [])
                            state["scope"] = resolved_scope
                            grounding["scope"] = resolved_scope

                    if scope_documents:
                        parsed_criteria_for_terms = parsed.criteria if isinstance(parsed.criteria, dict) else {}
                        must_contain_terms = [
                            str(item).strip()
                            for item in list(parsed_criteria_for_terms.get("must_contain") or [])
                            if str(item).strip()
                        ]

                        q_norm = str(question or "").strip().lower()
                        asks_telecom = bool(re.search(r"\b(?:telephone|phone|mobile|telecom|telefon|mobil)\b", q_norm, flags=re.IGNORECASE))
                        asks_billing = bool(re.search(r"\b(?:bill|invoice|receipt|payment|charge|fee|rechnung|beleg|zahlung|quittung)\b", q_norm, flags=re.IGNORECASE))

                        ranked_docs: list[tuple[float, dict[str, Any], list[str]]] = []
                        for item in scope_documents:
                            if not isinstance(item, dict):
                                continue
                            doc_blob = " ".join(
                                [
                                    str(item.get("doc_name") or item.get("title") or ""),
                                    str(item.get("doc_path") or item.get("file_path") or ""),
                                    str(item.get("doc_theme") or ""),
                                    str(item.get("doc_cat") or ""),
                                    str(item.get("doc_type") or ""),
                                    str(item.get("keyword_text") or ""),
                                ]
                            ).lower()
                            matched_terms = [
                                term
                                for term in must_contain_terms
                                if str(term).strip() and str(term).strip().lower() in doc_blob
                            ]
                            topic_score = float(item.get("score") or 0.0) + (0.6 * float(len(matched_terms)))

                            has_telecom = bool(re.search(r"\b(?:telephone|phone|mobile|telecom|telefon|mobil|yallo|sunrise)\b", doc_blob, flags=re.IGNORECASE))
                            has_billing = bool(re.search(r"\b(?:bill|invoice|receipt|payment|charge|fee|rechnung|beleg|zahlung|quittung)\b", doc_blob, flags=re.IGNORECASE))
                            looks_non_bill_transport = bool(re.search(r"\b(?:ticket|train|rail|sbb|booking)\b", doc_blob, flags=re.IGNORECASE))
                            looks_purchase_order = bool(re.search(r"\b(?:purchase\s*order|order\s*no\.|teyu|chiller)\b", doc_blob, flags=re.IGNORECASE))

                            if asks_telecom:
                                topic_score += 2.2 if has_telecom else -1.6
                            if asks_billing:
                                topic_score += 2.2 if has_billing else -1.6
                            if asks_billing and looks_non_bill_transport:
                                topic_score -= 2.4
                            if asks_billing and looks_purchase_order:
                                topic_score -= 2.0

                            ranked_docs.append((topic_score, item, matched_terms))

                        ranked_docs.sort(key=lambda entry: entry[0], reverse=True)

                        hydrated_matches: list[dict[str, Any]] = []
                        seen_doc_ids: set[int] = set()
                        for _rank, item, matched_terms in ranked_docs:
                            doc_id = item.get("doc_id")
                            if not isinstance(doc_id, int) or doc_id in seen_doc_ids:
                                continue
                            seen_doc_ids.add(doc_id)
                            hydrated_matches.append(
                                {
                                    "doc_id": doc_id,
                                    "doc_name": item.get("doc_name") or item.get("title"),
                                    "doc_path": item.get("doc_path") or item.get("file_path"),
                                    "title": item.get("title") or item.get("doc_name"),
                                    "matched_terms": matched_terms[:6],
                                    "score": float(item.get("score") or 0.0),
                                    "doc_theme": item.get("doc_theme"),
                                    "doc_cat": item.get("doc_cat"),
                                    "doc_type": item.get("doc_type"),
                                    "keyword_text": item.get("keyword_text"),
                                }
                            )
                            if len(hydrated_matches) >= 20:
                                break

                        if hydrated_matches:
                            criteria_for_context = dict(criteria_for_context)
                            criteria_for_context["matches"] = hydrated_matches
                            state["criteria_result"] = dict(criteria_for_context)
                            grounding["criteria_result"] = dict(criteria_for_context)
                            state["trace"].append(
                                {
                                    "step": len(state.get("trace", [])),
                                    "tool": "criteria_value_scope_hydration",
                                    "hydrated_match_count": len(hydrated_matches),
                                    "used": True,
                                }
                            )
                except Exception:
                    pass

            scope_payload = state.get("scope") if isinstance(state.get("scope"), dict) else {}
            scoped_candidates = list(scope_payload.get("top_candidates") or []) if isinstance(scope_payload, dict) else []
            if not scoped_candidates:
                try:
                    scoped_candidates = self.query_engine.qdrant_candidates(question, limit=8)
                except Exception:
                    scoped_candidates = []
            discovery_payload = state.get("discovery_search") if isinstance(state.get("discovery_search"), dict) else {}
            scoped_discovery = list(discovery_payload.get("results") or []) if isinstance(discovery_payload, dict) else []

            criteria_contextual = self.query_engine._criteria_contextual_llm_resolution_fallback(
                question=question,
                parsed=parsed,
                criteria_result=criteria_for_context,
                candidates=scoped_candidates,
                discovery_results=scoped_discovery,
            )
            state["telemetry"]["criteria_contextual_calls"] = int((state.get("telemetry") or {}).get("criteria_contextual_calls", 0)) + 1
            if isinstance(criteria_contextual, dict) and str(criteria_contextual.get("answer") or "").strip():
                state["criteria_contextual_result"] = criteria_contextual
                grounding["criteria_contextual_result"] = criteria_contextual
                state["trace"].append(
                    {
                        "step": len(state.get("trace", [])),
                        "tool": "criteria_contextual_llm_resolution",
                        "success": True,
                    }
                )
            elif require_contextual_resolution:
                contextual_fallback = self.query_engine._contextual_llm_resolution_fallback(
                    question=question,
                    parsed=parsed,
                    candidates=scoped_candidates,
                    discovery_results=scoped_discovery,
                )
                state["telemetry"]["contextual_fallback_calls"] = int((state.get("telemetry") or {}).get("contextual_fallback_calls", 0)) + 1
                if isinstance(contextual_fallback, dict) and str(contextual_fallback.get("answer") or "").strip():
                    state["criteria_contextual_result"] = contextual_fallback
                    grounding["criteria_contextual_result"] = contextual_fallback
                    state["trace"].append(
                        {
                            "step": len(state.get("trace", [])),
                            "tool": "contextual_llm_resolution_fallback",
                            "success": True,
                        }
                    )

        parsed_entity_anchor = str((grounding.get("parsed") or {}).get("entity_name") or "").strip()
        parsed_intent_anchor = str((grounding.get("parsed") or {}).get("intent") or "").strip().lower()
        if parsed_entity_anchor and parsed_intent_anchor != "criteria_lookup":
            grounding = self._sanitize_grounding_for_anchor(grounding, parsed_entity_anchor)

        answer_policy_payload = {
            "question": question,
            "grounding": grounding,
        }
        answer_policy = self._invoke_solf_policy("interaction_answer_policy", answer_policy_payload)
        state["trace"].append(
            {
                "step": len(state.get("trace", [])),
                "policy": "interaction_answer_policy",
                "result": answer_policy,
            }
        )

        if answer_policy.get("mode") == "deny":
            answer_text = str(answer_policy.get("message") or "Answer generation is blocked by domain policy.").strip()
            answer_text = f"[source: policy_blocked] {answer_text}".strip()
            if record_history:
                self.state.history.append({"role": "user", "content": question})
                self.state.history.append({"role": "assistant", "content": answer_text})
            return {
                "answer": answer_text,
                "answer_source": "policy_blocked",
                "answer_sources": ["policy_blocked"],
                "grounding": grounding,
                "policy": {
                    "query": query_policy,
                    "style": style_policy,
                    "answer": answer_policy,
                },
                "trace": state.get("trace", []),
                "model": selected_model,
            }

        is_value_question = self._is_value_question(question, parsed)
        value_focus_terms = self._extract_value_focus_terms(question, parsed) if is_value_question else []

        criteria_contextual_result = grounding.get("criteria_contextual_result") if isinstance(grounding, dict) else None
        if isinstance(criteria_contextual_result, dict) and str(criteria_contextual_result.get("answer") or "").strip():
            selected_contextual: dict[str, Any] | None = dict(criteria_contextual_result)
            if require_contextual_resolution:
                try:
                    scope_payload = grounding.get("scope") if isinstance(grounding.get("scope"), dict) else {}
                    scoped_candidates = list(scope_payload.get("top_candidates") or []) if isinstance(scope_payload, dict) else []
                    if not scoped_candidates:
                        scoped_candidates = self.query_engine.qdrant_candidates(question, limit=8)
                except Exception:
                    scoped_candidates = []

                try:
                    discovery_payload = grounding.get("discovery_search") if isinstance(grounding.get("discovery_search"), dict) else {}
                    scoped_discovery = list(discovery_payload.get("results") or []) if isinstance(discovery_payload, dict) else []
                except Exception:
                    scoped_discovery = []

                try:
                    contextual_override = self.query_engine._contextual_llm_resolution_fallback(
                        question=question,
                        parsed=parsed,
                        candidates=scoped_candidates,
                        discovery_results=scoped_discovery,
                    )
                    state["telemetry"]["contextual_fallback_calls"] = int((state.get("telemetry") or {}).get("contextual_fallback_calls", 0)) + 1
                except Exception:
                    contextual_override = None

                if not (isinstance(contextual_override, dict) and str(contextual_override.get("answer") or "").strip()):
                    normalized_question = str(question or "").strip()
                    match = re.match(
                        r"(?is)^\s*what\s+was\s+the\s+(?:total\s+)?(?:amount|cost|price|value)\s+for\s+(.+?)\s*[?.!]?\s*$",
                        normalized_question,
                    )
                    if match:
                        candidate_phrase = str(match.group(1) or "").strip()
                        if candidate_phrase:
                            normalized_question = f"How much was {candidate_phrase}"
                            try:
                                contextual_override = self.query_engine._contextual_llm_resolution_fallback(
                                    question=normalized_question,
                                    parsed=parsed,
                                    candidates=scoped_candidates,
                                    discovery_results=scoped_discovery,
                                )
                                state["telemetry"]["contextual_fallback_calls"] = int((state.get("telemetry") or {}).get("contextual_fallback_calls", 0)) + 1
                            except Exception:
                                contextual_override = None

                if isinstance(contextual_override, dict) and str(contextual_override.get("answer") or "").strip():
                    selected_contextual = dict(contextual_override)
                    state["criteria_contextual_result"] = selected_contextual
                    grounding["criteria_contextual_result"] = selected_contextual
                    state["trace"].append(
                        {
                            "step": len(state.get("trace", [])),
                            "tool": "contextual_value_source_preference",
                            "source": str(selected_contextual.get("source") or "contextual_llm_resolution_fallback"),
                            "used": True,
                        }
                    )

            if isinstance(selected_contextual, dict) and is_value_question:
                selected_answer = str(selected_contextual.get("answer") or "").strip()
                entity_hint = str((state.get("parsed") or {}).get("entity_name") or parsed.entity_name or "").strip()
                is_generic_entity_total = bool(
                    value_focus_terms
                    and self._is_generic_entity_value_answer(selected_answer, entity_hint)
                )
                if (not self._is_value_like_answer(selected_answer)) or is_generic_entity_total:
                    selected_contextual = None
                    state["criteria_contextual_result"] = None
                    grounding["criteria_contextual_result"] = None
                    state["trace"].append(
                        {
                            "step": len(state.get("trace", [])),
                            "tool": "generic_value_contextual_validation",
                            "accepted": False,
                            "focus_terms": value_focus_terms[:4],
                            "reason": "generic_entity_total" if is_generic_entity_total else "not_value_like",
                        }
                    )

            if isinstance(selected_contextual, dict):
                contextual_source = str(selected_contextual.get("source") or "contextual_llm_resolution_fallback").strip() or "contextual_llm_resolution_fallback"
                raw_contextual_answer = str(selected_contextual.get("answer") or "").strip()
                answer_text = f"[source: {contextual_source}] {raw_contextual_answer}".strip()

                if is_value_question:
                    try:
                        contextual_confidence = float(selected_contextual.get("confidence", 1.0))
                    except Exception:
                        contextual_confidence = 1.0
                    if contextual_confidence < self._semantic_best_effort_threshold and raw_contextual_answer:
                        criteria_docs = (
                            list(selected_contextual.get("criteria_documents") or [])
                            if isinstance(selected_contextual.get("criteria_documents"), list)
                            else []
                        )
                        best_doc = criteria_docs[0] if criteria_docs and isinstance(criteria_docs[0], dict) else {}
                        best_doc_label = str(best_doc.get("doc_name") or best_doc.get("doc_path") or "semantic search result").strip() or "semantic search result"
                        answer_text = (
                            f"[source: semantic_best_effort] Confidence is limited ({contextual_confidence:.2f} < {self._semantic_best_effort_threshold:.2f}). "
                            f"Based on the most relevant semantic match ({best_doc_label}), the best-effort value is: {raw_contextual_answer.rstrip('.')}. "
                            "This may be uncertain."
                        ).strip()
                        contextual_source = "semantic_best_effort"
                        state["trace"].append(
                            {
                                "step": len(state.get("trace", [])),
                                "tool": "low_confidence_semantic_best_effort",
                                "confidence": contextual_confidence,
                                "semantic_doc": best_doc_label,
                                "used": True,
                            }
                        )
                if record_history:
                    self.state.history.append({"role": "user", "content": question})
                    self.state.history.append({"role": "assistant", "content": answer_text})

                result = {
                    "answer": answer_text,
                    "answer_source": contextual_source,
                    "answer_sources": [contextual_source],
                    "grounding": grounding,
                    "request_telemetry": dict(state.get("telemetry") or {}),
                    "policy": {
                        "query": query_policy,
                        "style": style_policy,
                        "answer": answer_policy,
                    },
                    "trace": state.get("trace", []),
                    "model": "deterministic_contextual_value",
                }
                if self.query_cache:
                    self.query_cache.put(question, _attach_evidence_bundle(result))
                return _attach_evidence_bundle(result)

        def _resolve_scoped_context() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            scope_payload = grounding.get("scope") if isinstance(grounding.get("scope"), dict) else {}
            scoped_candidates = list(scope_payload.get("top_candidates") or []) if isinstance(scope_payload, dict) else []
            if not scoped_candidates:
                try:
                    scoped_candidates = self.query_engine.qdrant_candidates(question, limit=8)
                except Exception:
                    scoped_candidates = []

            discovery_payload = grounding.get("discovery_search") if isinstance(grounding.get("discovery_search"), dict) else {}
            scoped_discovery = list(discovery_payload.get("results") or []) if isinstance(discovery_payload, dict) else []
            return scoped_candidates, scoped_discovery

        # Generic value bridge: before criteria-list finalization, try contextual
        # grounded value resolution for all value-seeking questions.
        if (require_contextual_resolution or is_value_question) and not isinstance(criteria_contextual_result, dict):
            scoped_candidates, scoped_discovery = _resolve_scoped_context()
            contextual_fallback = self._try_generic_value_contextual_resolution(
                question=question,
                parsed=parsed,
                candidates=scoped_candidates,
                discovery_results=scoped_discovery,
                telemetry=state.get("telemetry"),
            )

            if isinstance(contextual_fallback, dict) and str(contextual_fallback.get("answer") or "").strip():
                contextual_source = str(contextual_fallback.get("source") or "contextual_llm_resolution_fallback").strip() or "contextual_llm_resolution_fallback"
                answer_text = f"[source: {contextual_source}] {str(contextual_fallback.get('answer') or '').strip()}".strip()
                grounding["criteria_contextual_result"] = contextual_fallback
                state["criteria_contextual_result"] = contextual_fallback

                if record_history:
                    self.state.history.append({"role": "user", "content": question})
                    self.state.history.append({"role": "assistant", "content": answer_text})

                result = {
                    "answer": answer_text,
                    "answer_source": contextual_source,
                    "answer_sources": [contextual_source],
                    "grounding": grounding,
                    "request_telemetry": dict(state.get("telemetry") or {}),
                    "policy": {
                        "query": query_policy,
                        "style": style_policy,
                        "answer": answer_policy,
                    },
                    "trace": state.get("trace", []) + [
                        {
                            "step": len(state.get("trace", [])),
                            "tool": "generic_value_contextual_bridge",
                            "source": contextual_source,
                            "rewrite_question": str(contextual_fallback.get("rewrite_question") or "").strip() or None,
                            "used": True,
                        }
                    ],
                    "model": "deterministic_generic_value_bridge",
                }
                if self.query_cache:
                    self.query_cache.put(question, _attach_evidence_bundle(result))
                return _attach_evidence_bundle(result)

        # Deterministic criteria-list answer to avoid count-only summaries for
        # explicit list/show document queries.
        deterministic_criteria_answer = self._deterministic_criteria_answer(question, grounding)
        if deterministic_criteria_answer:
            if require_contextual_resolution:
                try:
                    state["telemetry"]["query_engine_answer_calls"] = int((state.get("telemetry") or {}).get("query_engine_answer_calls", 0)) + 1
                    contextual_direct = self.query_engine.answer(question)
                except Exception:
                    contextual_direct = None

                if isinstance(contextual_direct, dict):
                    contextual_source = str(contextual_direct.get("source") or "").strip().lower()
                    contextual_answer = str(contextual_direct.get("answer") or "").strip()
                    contextual_acceptable = bool(contextual_answer)
                    if contextual_acceptable and is_value_question:
                        entity_hint = str((state.get("parsed") or {}).get("entity_name") or parsed.entity_name or "").strip()
                        contextual_acceptable = (
                            self._is_value_like_answer(contextual_answer)
                            and not (
                                bool(value_focus_terms)
                                and self._is_generic_entity_value_answer(contextual_answer, entity_hint)
                            )
                        )
                    if contextual_answer and contextual_source in {
                        "contextual_llm_resolution_fallback",
                        "criteria_contextual_llm",
                        "criteria_contextual_llm_resolution_fallback",
                    } and contextual_acceptable:
                        answer_text = contextual_answer
                        answer_sources = [contextual_source]
                        if record_history:
                            self.state.history.append({"role": "user", "content": question})
                            self.state.history.append({"role": "assistant", "content": answer_text})

                        result = {
                            "answer": answer_text,
                            "answer_source": contextual_source,
                            "answer_sources": answer_sources,
                            "grounding": grounding,
                            "request_telemetry": dict(state.get("telemetry") or {}),
                            "policy": {
                                "query": query_policy,
                                "style": style_policy,
                                "answer": answer_policy,
                            },
                            "trace": state.get("trace", []) + [
                                {
                                    "step": len(state.get("trace", [])),
                                    "tool": "query_engine_contextual_value_preference",
                                    "source": contextual_source,
                                    "used": True,
                                }
                            ],
                            "model": "deterministic_contextual_preference",
                        }
                        if self.query_cache:
                            self.query_cache.put(question, _attach_evidence_bundle(result))
                        return _attach_evidence_bundle(result)

            if is_value_question:
                scoped_candidates, scoped_discovery = _resolve_scoped_context()
                value_guard_result = self._try_generic_value_contextual_resolution(
                    question=question,
                    parsed=parsed,
                    candidates=scoped_candidates,
                    discovery_results=scoped_discovery,
                    telemetry=state.get("telemetry"),
                )
                if isinstance(value_guard_result, dict) and str(value_guard_result.get("answer") or "").strip():
                    contextual_source = str(value_guard_result.get("source") or "contextual_llm_resolution_fallback").strip() or "contextual_llm_resolution_fallback"
                    answer_text = f"[source: {contextual_source}] {str(value_guard_result.get('answer') or '').strip()}".strip()
                    grounding["criteria_contextual_result"] = value_guard_result
                    state["criteria_contextual_result"] = value_guard_result
                    if record_history:
                        self.state.history.append({"role": "user", "content": question})
                        self.state.history.append({"role": "assistant", "content": answer_text})

                    result = {
                        "answer": answer_text,
                        "answer_source": contextual_source,
                        "answer_sources": [contextual_source],
                        "grounding": grounding,
                        "request_telemetry": dict(state.get("telemetry") or {}),
                        "policy": {
                            "query": query_policy,
                            "style": style_policy,
                            "answer": answer_policy,
                        },
                        "trace": state.get("trace", []) + [
                            {
                                "step": len(state.get("trace", [])),
                                "tool": "generic_value_criteria_finalization_guard",
                                "source": contextual_source,
                                "rewrite_question": str(value_guard_result.get("rewrite_question") or "").strip() or None,
                                "used": True,
                            }
                        ],
                        "model": "deterministic_generic_value_guard",
                    }
                    if self.query_cache:
                        self.query_cache.put(question, _attach_evidence_bundle(result))
                    return _attach_evidence_bundle(result)

            criteria_result = grounding.get("criteria_result") if isinstance(grounding, dict) else None
            criteria_source = str((criteria_result or {}).get("source") or "criteria_sql").strip() or "criteria_sql"

            if (
                is_value_question
                and bool(value_focus_terms)
                and criteria_source in {"criteria_sql", "criteria_docs_sql"}
                and not isinstance(grounding.get("criteria_contextual_result"), dict)
            ):
                scoped_candidates, scoped_discovery = _resolve_scoped_context()
                best_effort_result = self._try_generic_value_contextual_resolution(
                    question=question,
                    parsed=parsed,
                    candidates=scoped_candidates,
                    discovery_results=scoped_discovery,
                    telemetry=state.get("telemetry"),
                )
                if isinstance(best_effort_result, dict) and str(best_effort_result.get("answer") or "").strip():
                    best_effort_answer = str(best_effort_result.get("answer") or "").strip().rstrip(".")
                    best_effort_docs = (
                        list(best_effort_result.get("criteria_documents") or [])
                        if isinstance(best_effort_result.get("criteria_documents"), list)
                        else []
                    )
                    best_doc = best_effort_docs[0] if best_effort_docs and isinstance(best_effort_docs[0], dict) else {}
                    best_doc_label = str(best_doc.get("doc_name") or best_doc.get("doc_path") or "semantic search result").strip() or "semantic search result"
                    try:
                        best_effort_confidence = float(best_effort_result.get("confidence", 0.75))
                    except Exception:
                        best_effort_confidence = 0.75

                    answer_text = (
                        f"[source: semantic_best_effort] Confidence is limited ({best_effort_confidence:.2f} < {self._semantic_best_effort_threshold:.2f}). "
                        f"Based on the most relevant semantic match ({best_doc_label}), the best-effort value is: {best_effort_answer}. "
                        "This may be uncertain."
                    ).strip()
                    if record_history:
                        self.state.history.append({"role": "user", "content": question})
                        self.state.history.append({"role": "assistant", "content": answer_text})

                    result = {
                        "answer": answer_text,
                        "answer_source": "semantic_best_effort",
                        "answer_sources": ["semantic_best_effort"],
                        "grounding": grounding,
                        "request_telemetry": dict(state.get("telemetry") or {}),
                        "policy": {
                            "query": query_policy,
                            "style": style_policy,
                            "answer": answer_policy,
                        },
                        "trace": state.get("trace", []) + [
                            {
                                "step": len(state.get("trace", [])),
                                "tool": "generic_value_semantic_best_effort_guard",
                                "source": str(best_effort_result.get("source") or "contextual_llm_resolution_fallback"),
                                "confidence": best_effort_confidence,
                                "semantic_doc": best_doc_label,
                                "used": True,
                            }
                        ],
                        "model": "deterministic_generic_value_guard",
                    }
                    if self.query_cache:
                        self.query_cache.put(question, _attach_evidence_bundle(result))
                    return _attach_evidence_bundle(result)

                focus_label = ", ".join(value_focus_terms[:3])
                clarification_text = (
                    f"[source: needs_clarification] I found matching records for {str((state.get('parsed') or {}).get('entity_name') or 'the requested entity').strip()}, "
                    f"but I could not reliably determine a specific value for '{focus_label}'. "
                    "Please ask with the exact billed item/attribute name shown in the document."
                ).strip()
                if record_history:
                    self.state.history.append({"role": "user", "content": question})
                    self.state.history.append({"role": "assistant", "content": clarification_text})

                result = {
                    "answer": clarification_text,
                    "answer_source": "needs_clarification",
                    "answer_sources": ["needs_clarification"],
                    "grounding": grounding,
                    "request_telemetry": dict(state.get("telemetry") or {}),
                    "policy": {
                        "query": query_policy,
                        "style": style_policy,
                        "answer": answer_policy,
                    },
                    "trace": state.get("trace", []) + [
                        {
                            "step": len(state.get("trace", [])),
                            "tool": "generic_value_resolution_guard",
                            "reason": "unresolved_qualified_value",
                            "focus_terms": value_focus_terms[:4],
                            "used": True,
                        }
                    ],
                    "model": "deterministic_generic_value_guard",
                }
                if self.query_cache:
                    self.query_cache.put(question, _attach_evidence_bundle(result))
                return _attach_evidence_bundle(result)

            answer_text = f"[source: {criteria_source}] {deterministic_criteria_answer}".strip()
            answer_sources = [criteria_source]
            if record_history:
                self.state.history.append({"role": "user", "content": question})
                self.state.history.append({"role": "assistant", "content": answer_text})

            result = {
                "answer": answer_text,
                "answer_source": criteria_source,
                "answer_sources": answer_sources,
                "grounding": grounding,
                "request_telemetry": dict(state.get("telemetry") or {}),
                "policy": {
                    "query": query_policy,
                    "style": style_policy,
                    "answer": answer_policy,
                },
                "trace": state.get("trace", []),
                "model": "deterministic_criteria_listing",
            }
            if self.query_cache:
                self.query_cache.put(question, _attach_evidence_bundle(result))
            return _attach_evidence_bundle(result)

        # Final generic safety gate before free-form answer generation.
        if is_value_question and not isinstance(grounding.get("criteria_contextual_result"), dict):
            scoped_candidates, scoped_discovery = _resolve_scoped_context()
            pre_answer_guard = self._try_generic_value_contextual_resolution(
                question=question,
                parsed=parsed,
                candidates=scoped_candidates,
                discovery_results=scoped_discovery,
                telemetry=state.get("telemetry"),
            )
            if isinstance(pre_answer_guard, dict) and str(pre_answer_guard.get("answer") or "").strip():
                contextual_source = str(pre_answer_guard.get("source") or "contextual_llm_resolution_fallback").strip() or "contextual_llm_resolution_fallback"
                answer_text = f"[source: {contextual_source}] {str(pre_answer_guard.get('answer') or '').strip()}".strip()
                grounding["criteria_contextual_result"] = pre_answer_guard
                state["criteria_contextual_result"] = pre_answer_guard
                if record_history:
                    self.state.history.append({"role": "user", "content": question})
                    self.state.history.append({"role": "assistant", "content": answer_text})

                result = {
                    "answer": answer_text,
                    "answer_source": contextual_source,
                    "answer_sources": [contextual_source],
                    "grounding": grounding,
                    "request_telemetry": dict(state.get("telemetry") or {}),
                    "policy": {
                        "query": query_policy,
                        "style": style_policy,
                        "answer": answer_policy,
                    },
                    "trace": state.get("trace", []) + [
                        {
                            "step": len(state.get("trace", [])),
                            "tool": "generic_value_pre_answer_guard",
                            "source": contextual_source,
                            "rewrite_question": str(pre_answer_guard.get("rewrite_question") or "").strip() or None,
                            "used": True,
                        }
                    ],
                    "model": "deterministic_generic_value_pre_answer_guard",
                }
                if self.query_cache:
                    self.query_cache.put(question, _attach_evidence_bundle(result))
                return _attach_evidence_bundle(result)

        criteria_payload_for_guard = grounding.get("criteria_result") if isinstance(grounding, dict) else None
        if (
            is_value_question
            and bool(value_focus_terms)
            and not isinstance(grounding.get("criteria_contextual_result"), dict)
            and isinstance(criteria_payload_for_guard, dict)
            and int(criteria_payload_for_guard.get("count", 0) or 0) > 0
        ):
            focus_label = ", ".join(value_focus_terms[:3])
            clarification_text = (
                f"[source: needs_clarification] I found matching records for {str((state.get('parsed') or {}).get('entity_name') or 'the requested entity').strip()}, "
                f"but I could not reliably determine a specific value for '{focus_label}'. "
                "Please ask with the exact billed item/attribute name shown in the document."
            ).strip()
            if record_history:
                self.state.history.append({"role": "user", "content": question})
                self.state.history.append({"role": "assistant", "content": clarification_text})

            result = {
                "answer": clarification_text,
                "answer_source": "needs_clarification",
                "answer_sources": ["needs_clarification"],
                "grounding": grounding,
                "request_telemetry": dict(state.get("telemetry") or {}),
                "policy": {
                    "query": query_policy,
                    "style": style_policy,
                    "answer": answer_policy,
                },
                "trace": state.get("trace", []) + [
                    {
                        "step": len(state.get("trace", [])),
                        "tool": "generic_value_pre_generation_guard",
                        "reason": "unresolved_qualified_value",
                        "focus_terms": value_focus_terms[:4],
                        "used": True,
                    }
                ],
                "model": "deterministic_generic_value_guard",
            }
            if self.query_cache:
                self.query_cache.put(question, _attach_evidence_bundle(result))
            return _attach_evidence_bundle(result)

        answer_prompt = (
            "You are an IDMS assistant. Use only grounded retrieval results.\n"
            "If attribute_result exists, answer directly with that value and mention whether it came from SQL, Qdrant-guided SQL, or Discovery.\n"
            "If criteria_result exists, list the strongest matches and mention which criteria terms matched.\n"
            "If criteria_contextual_result exists, prioritize that grounded value answer over listing results.\n"
            "If only semantic scope or Discovery search exists, summarize the best matching entities/documents/media and mention the retrieval source.\n"
            "Do not invent facts not in grounding.\n\n"
            f"Question: {question}\n"
            f"Grounding: {json.dumps(grounding, ensure_ascii=False)}"
        )
        answer_model = self._select_model_by_confidence(parsed_payload.get("confidence"), default=EXTRACT_MODEL)
        self._record_model_usage("interaction_answer_generation", answer_model, parsed_payload.get("confidence"))
        answer_response = generate_content_with_openrouter_fallback(
            primary_call=lambda: self.client.models.generate_content(
                model=answer_model,
                contents=[answer_prompt],
            ),
            model=answer_model,
            contents=[answer_prompt],
            temperature=0.0,
            call_name="interaction_answer_generation",
            complexity="simple",
        )
        answer_text = (answer_response.text or "").strip()

        answer_sources = self._answer_sources(grounding)
        source = answer_sources[0]
        answer_text = f"[source: {'+'.join(answer_sources)}] {answer_text}".strip()

        if record_history:
            self.state.history.append({"role": "user", "content": question})
            self.state.history.append({"role": "assistant", "content": answer_text})

        result = {
            "answer": answer_text,
            "answer_source": source,
            "answer_sources": answer_sources,
            "grounding": grounding,
            "request_telemetry": dict(state.get("telemetry") or {}),
            "policy": {
                "query": query_policy,
                "style": style_policy,
                "answer": answer_policy,
            },
            "trace": state.get("trace", []),
            "model": answer_model,
        }
        
        # Cache result for 1 hour to avoid re-running similar queries.
        if self.query_cache:
            self.query_cache.put(question, result)
        
        return result

    def build_adk_agent(self) -> Any:
        """Attempt to build an ADK agent wired with these tool callables."""
        if adk is None or not hasattr(adk, "Agent"):
            self.logger.warning("ADK unavailable; agent cannot be built")
            return None

        model_resource = (
            f"projects/{PROJECT_ID}/locations/{LOCATION}/publishers/google/models/{CHAT_MODEL}"
        )
        try:
            self.logger.info("Building ADK agent")
            return adk.Agent(
                name="IDMSInteractionAgent",
                model=model_resource,
                instruction=(
                    "Use tools for data-grounded responses. "
                    "For factual attribute questions: first call get_entity_attributes to discover "
                    "what attributes are available, then call lookup_entity_attribute with the best match. "
                    "For list/filter requests, call search_criteria. "
                    "For file retrieval requests, call retrieve_document_file to return doc_name/doc_path/original_local_path. "
                    "Prefer query tool for general questions before free-form answer generation."
                ),
                tools=[
                    self.query,
                    self.search_discovery,
                    self.search_criteria,
                    self.get_entity_attributes,
                    self.lookup_entity_attribute,
                    self.retrieve_document_file,
                    self.autonomous_query,
                    self.ingest_document,
                    self.ingest_information,
                    self.generate_document,
                    self.chat,
                ],
            )
        except Exception:
            self.logger.exception("Failed to build ADK agent")
            return None


def _parse_json_object(raw: str) -> dict[str, Any]:
    if not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    # Shell-friendly fallback for payloads like {'k': 'v'}
    try:
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    raise ValueError("Expected a JSON object")


def _parse_json_array(raw: str) -> list[Any]:
    if not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    try:
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass

    raise ValueError("Expected a JSON array")


def _parse_csv_list(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _extract_json_object_from_text(raw_text: str) -> dict[str, Any]:
    text = str(raw_text or "").strip()
    if not text:
        return {}

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    # Fallback: extract first JSON object-looking segment from free text.
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        segment = text[start : end + 1]
        try:
            parsed = json.loads(segment)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    return {}


def _extract_domain_definition_json(payload: dict[str, Any]) -> dict[str, Any] | None:
    data = payload if isinstance(payload, dict) else {}
    text = str(data.get("text") or "")
    candidates = [
        data.get("definition"),
        data.get("data"),
        data.get("definition_json"),
        data.get("json"),
        data.get("payload"),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return dict(candidate)
        if isinstance(candidate, str):
            parsed = _extract_json_object_from_text(candidate)
            if parsed:
                return parsed

    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    if fenced:
        parsed = _extract_json_object_from_text(fenced.group(1))
        if parsed:
            return parsed

    parsed = _extract_json_object_from_text(text)
    return parsed if parsed else None


def _resolve_domain_definition_operation(text: str) -> str:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return ""

    op_aliases = {
        "create": ["create", "add", "insert", "new", "register"],
        "update": ["update", "modify", "edit", "change", "replace", "set"],
        "delete": ["delete", "remove", "drop", "deactivate"],
        "list": ["list", "all", "browse", "overview"],
        "read": ["get", "show", "read", "view", "find"],
    }
    for op_name, aliases in op_aliases.items():
        if any(re.search(rf"\b{re.escape(alias)}\b", lowered) for alias in aliases):
            return op_name
    return ""


def _resolve_domain_definition_type(text: str) -> str:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return ""

    type_aliases = {
        "account_definition": [
            "account definition",
            "account",
            "coa",
            "chart of accounts",
            "chart_of_accounts",
            "ledger account",
            "gl account",
            "konto",
            "konten",
        ],
        "hr_department_definition": [
            "department definition",
            "department",
            "hr department",
            "dept",
            "abteilung",
        ],
        "hr_role_definition": [
            "role definition",
            "role",
            "job role",
            "position",
            "funktion",
            "stelle",
        ],
    }
    for definition_type, aliases in type_aliases.items():
        if any(alias in lowered for alias in aliases):
            return definition_type
    return ""


def _llm_extract_domain_definition_request(text: str, operation_hint: str = "", definition_type_hint: str = "") -> dict[str, Any]:
    # Lightweight compatibility shim used by tests that assert helper availability.
    return {
        "operation": str(operation_hint or "").strip().lower(),
        "definition_type": str(definition_type_hint or "").strip().lower(),
        "definition": _extract_domain_definition_json({"text": text}) or {},
        "confidence": 0.0,
        "errors": [],
    }


def run_cli() -> int:
    parser = argparse.ArgumentParser(description="IDMS interaction tools (query, chat, ingestion, actions, scheduler, monitor)")
    sub = parser.add_subparsers(dest="command", required=True)

    q = sub.add_parser("query", help="Run factual query against IDMS")
    q.add_argument("question")

    c = sub.add_parser("chat", help="Chat with optional grounding from query tool")
    c.add_argument("message")
    c.add_argument("--no-query-tool", action="store_true")

    auto = sub.add_parser("auto", help="Autonomous loop for retrieval + attribute lookup")
    auto.add_argument("question")
    auto.add_argument("--max-steps", type=int, default=4)

    d = sub.add_parser("discovery", help="Search Discovery Engine directly")
    d.add_argument("question")
    d.add_argument("--limit", type=int, default=8)

    i = sub.add_parser("ingest-doc", help="Ingest a local or gs:// document")
    i.add_argument("source")
    i.add_argument("--description", default="")
    i.add_argument("--tags", default="", help="Comma-separated tags")
    i.add_argument("--metadata-json", default="", help="JSON object")

    t = sub.add_parser("ingest-text", help="Ingest raw information text without document")
    t.add_argument("text")
    t.add_argument("--description", default="")
    t.add_argument("--tags", default="", help="Comma-separated tags")
    t.add_argument("--metadata-json", default="", help="JSON object")

    a = sub.add_parser("adk-agent", help="Create ADK agent and report availability")
    a.add_argument("--json", action="store_true")

    action = sub.add_parser("action", help="Execute SOLF-guarded workflow actions")
    action.add_argument("name", choices=[
        "create_task",
        "update_entity",
        "assign_task",
        "close_task",
        "validate_plan",
        "create_workflow_case",
        "record_approval",
        "create_compliance_action",
        "ingest_document",
        "ingest_information",
        "generate_document",
        "integrate_source_database",
        "sync_source_database_entities",
    ])
    action.add_argument("--payload-json", default="{}", help="JSON object payload for action")
    action.add_argument("--dry-run", action="store_true", help="Compose SOLF action/sql plan without executing")

    solf = sub.add_parser("solf", help="Execute a SOLF clause/predicate directly")
    solf.add_argument("clause_name", help="SOLF clause/predicate name")
    solf.add_argument("--payload-json", default="{}", help="JSON object used as single argument when --args-json is empty")
    solf.add_argument("--args-json", default="", help="Optional JSON array of positional arguments")

    wf = sub.add_parser("workflow-design", help="Iteratively design a workflow plan using LLM + SOLF context")
    wf.add_argument("request")
    wf.add_argument("--context-json", default="{}", help="Optional JSON object with internal rules/metadata context")
    wf.add_argument("--max-iterations", type=int, default=3)

    ix = sub.add_parser("interaction-resolve", help="Iteratively resolve complex query/interaction requests")
    ix.add_argument("request")
    ix.add_argument("--context-json", default="{}", help="Optional JSON object with internal rules/metadata context")
    ix.add_argument("--max-iterations", type=int, default=3)

    sch = sub.add_parser("scheduler", help="Control recurring autonomous jobs")
    sch.add_argument("name", choices=["start", "stop", "add_job", "remove_job", "pause_job", "resume_job", "list_jobs"])
    sch.add_argument("--payload-json", default="{}", help="JSON object payload for scheduler command")

    boot = sub.add_parser("bootstrap-db", help="Create DB schema tables for IDMS")
    boot.add_argument("--recreate", action="store_true", help="Drop and recreate known tables")

    sub.add_parser("repair-schema", help="Repair known legacy schema drift without dropping tables")

    st = sub.add_parser("state", help="Workflow state machine operations")
    st.add_argument("name", choices=["get", "can", "transition", "history", "maintenance"])
    st.add_argument("--entity-type", default="workflow_case", choices=["workflow_case", "compliance_action", "project_task"])
    st.add_argument("--entity-id", default=0, type=int)
    st.add_argument("--to-state", default="")
    st.add_argument("--event", default="manual_transition")
    st.add_argument("--actor-ref", default="system")
    st.add_argument("--metadata-json", default="{}")
    st.add_argument("--limit", type=int, default=20)
    st.add_argument("--max-rows", type=int, default=200)

    mon = sub.add_parser("monitor", help="Workflow operational monitoring, reliability alerts, incidents, and simulation")
    mon.add_argument(
        "name",
        choices=[
            "stats",
            "health",
            "snapshot",
            "history",
            "alerts",
            "reconcile",
            "incidents",
            "incident",
            "health-summary",
            "incident-summary",
            "incidents-by-status",
            "metrics-timeline",
            "incident-trends",
            "metric-distribution",
            "sla-compliance",
            "subscribe-notifications",
            "audit-trail",
            "compliance-status",
            "compliance-report",
            "create-scenario",
            "simulate",
            "budget-impact",
            "compare-scenarios",
            "scenarios",
            "scenario-details",
        ],
    )
    mon.add_argument("--window-hours", type=int, default=24)
    mon.add_argument("--label", default="")
    mon.add_argument("--limit", type=int, default=20)
    mon.add_argument("--status", default="open", choices=["open", "acknowledged", "resolved", "all"])
    mon.add_argument("--incident-id", type=int, default=0)
    mon.add_argument("--incident-action", default="", choices=["", "acknowledge", "resolve"])
    mon.add_argument("--actor-ref", default="system")
    mon.add_argument("--metric-key", default="")
    mon.add_argument("--hours", type=int, default=168)
    mon.add_argument("--days", type=int, default=7)
    mon.add_argument("--offset", type=int, default=0)
    mon.add_argument("--subscriber-ref", default="")
    mon.add_argument("--email", default="")
    mon.add_argument("--webhook-url", default="")
    mon.add_argument("--phone", default="")
    mon.add_argument("--entity-type", default="")
    mon.add_argument("--entity-id", type=int, default=0)
    mon.add_argument("--compliance-type", default="")
    mon.add_argument("--base-project-id", type=int, default=0)
    mon.add_argument("--scenario-name", default="")
    mon.add_argument("--scenario-type", default="what_if")
    mon.add_argument("--description", default="")
    mon.add_argument("--scenario-id", type=int, default=0)
    mon.add_argument("--scenario1-id", type=int, default=0)
    mon.add_argument("--scenario2-id", type=int, default=0)
    mon.add_argument("--project-id", type=int, default=0)
    mon.add_argument("--include-resource-check", action="store_true")
    mon.add_argument("--modifications-json", default="{}")

    args = parser.parse_args()
    tools = IDMSInteractionTools()

    if args.command == "query":
        print(json.dumps(tools.query(args.question), indent=2, ensure_ascii=False))
        return 0

    if args.command == "chat":
        result = tools.chat(args.message, use_query_tool=not args.no_query_tool)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "auto":
        result = tools.autonomous_query(args.question, max_steps=args.max_steps)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "discovery":
        result = tools.search_discovery(args.question, limit=args.limit)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "ingest-doc":
        result = tools.ingest_document(
            source=args.source,
            description=args.description,
            tags=_parse_csv_list(args.tags),
            metadata=_parse_json_object(args.metadata_json),
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "ingest-text":
        result = tools.ingest_information(
            information_text=args.text,
            description=args.description,
            tags=_parse_csv_list(args.tags),
            metadata=_parse_json_object(args.metadata_json),
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "adk-agent":
        agent = tools.build_adk_agent()
        payload = {
            "adk_available": bool(adk is not None),
            "agent_built": bool(agent is not None),
            "agent_name": getattr(agent, "name", None) if agent is not None else None,
        }
        if args.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False))
        else:
            print(payload)
        return 0

    if args.command == "action":
        payload = _parse_json_object(args.payload_json)
        result = tools.preview_action_plan(args.name, payload) if args.dry_run else tools.perform_action(args.name, payload)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "solf":
        payload = _parse_json_object(args.payload_json)
        call_args = _parse_json_array(args.args_json) if str(args.args_json or "").strip() else []
        result = tools.evaluate_solf_clause(
            clause_name=args.clause_name,
            payload=payload,
            args=call_args,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "workflow-design":
        result = tools.design_workflow_interaction(
            request_text=args.request,
            context=_parse_json_object(args.context_json),
            max_iterations=args.max_iterations,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "interaction-resolve":
        result = tools.resolve_complex_interaction(
            request_text=args.request,
            context=_parse_json_object(args.context_json),
            max_iterations=args.max_iterations,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "scheduler":
        payload = _parse_json_object(args.payload_json)
        result = tools.scheduler_command(args.name, payload)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "bootstrap-db":
        result = tools.bootstrap_db(recreate=args.recreate)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "repair-schema":
        result = tools.repair_schema()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "state":
        payload = {
            "entity_type": args.entity_type,
            "entity_id": args.entity_id,
            "to_state": args.to_state,
            "event": args.event,
            "actor_ref": args.actor_ref,
            "metadata": _parse_json_object(args.metadata_json),
            "limit": args.limit,
            "max_rows": args.max_rows,
        }
        result = tools.state_command(args.name, payload)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "monitor":
        payload = {
            "window_hours": args.window_hours,
            "label": args.label,
            "limit": args.limit,
            "status": args.status,
            "incident_id": args.incident_id,
            "incident_action": args.incident_action,
            "actor_ref": args.actor_ref,
            "metric_key": args.metric_key,
            "hours": args.hours,
            "days": args.days,
            "offset": args.offset,
            "subscriber_ref": args.subscriber_ref,
            "email": args.email,
            "webhook_url": args.webhook_url,
            "phone": args.phone,
            "entity_type": args.entity_type,
            "entity_id": args.entity_id,
            "compliance_type": args.compliance_type,
            "base_project_id": args.base_project_id,
            "scenario_name": args.scenario_name,
            "scenario_type": args.scenario_type,
            "description": args.description,
            "scenario_id": args.scenario_id,
            "scenario1_id": args.scenario1_id,
            "scenario2_id": args.scenario2_id,
            "project_id": args.project_id,
            "include_resource_check": args.include_resource_check,
            "modifications": _parse_json_object(args.modifications_json),
        }
        result = tools.monitor_command(args.name, payload)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(run_cli())
