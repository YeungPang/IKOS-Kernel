"""Workflow pipeline executor with pause/resume support.

A pipeline run executes a sequence of steps defined in solf_workflow_steps.
Any step may pause the run when:
  - user interaction is required (e.g. a confirmation or a missing field value)
  - insufficient data exists in the database (e.g. a document not yet ingested)
  - explicit approval is required before proceeding

When paused, the run stores the pause reason, what is needed, and resumes from
the same step once the caller supplies the missing information via resume_pipeline_run().

Step kinds supported
--------------------
clause          – Call a SOLF clause by name. The clause receives the current context dict
                  as its sole argument. Its return value is merged into the context.
python_binding  – Call module.function(context, config). The function may raise
                  PipelineStepPauseRequired to signal a pause.
                  capability_key/capability_version config dispatches to the trusted
                  registry first, without imports or generated scripts.
class_transform – Reserved for future use; treated as a no-op with a log warning.
class_generate  – Reserved for future use; treated as a no-op with a log warning.
class_iterate   – Reserved for future use; treated as a no-op with a log warning.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import hashlib
import logging
import re
import time
from types import MappingProxyType
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import object_db
from copy import deepcopy
from jsonschema import Draft202012Validator, FormatChecker
from mutation_undo import _reject_capability_authority, _validate_capability_action

LOGGER = logging.getLogger("idms.pipeline")

_BUILTIN_STEP_MODULE = "workflow_pipeline_builtin_steps"

_CAPABILITY_METADATA_FIELDS = frozenset({
    "tenant_id", "user_id", "permissions", "run_id", "step_key", "step_order",
    "step_kind", "run_status", "step_status", "workflow_version_id", "workflow_key",
    "started_by", "requested_by", "on_failure_policy", "paused_at_step_key",
    "idempotency_key", "status", "success", "paused", "pause_reason",
    "interaction_prompt", "required_doc_types", "missing_data_desc", "error",
    "error_message", "next_step_key", "handled_exception", "applied_clause",
    "security_context", "request_id", "roles", "workspace_ids", "platform_admin",
})


def _capability_payload(config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Only explicit literals/mappings cross the boundary; mapping is input -> context.

    Dotted dictionary paths are supported. Compensation uses output_snapshot only.
    """
    literals = config.get("payload", {})
    fields = config.get("payload_fields", {})
    if not isinstance(literals, dict) or not isinstance(fields, dict):
        raise ValueError("capability payload and payload_fields must be objects")
    payload = deepcopy(literals)
    for field_name, source in fields.items():
        if not isinstance(field_name, str) or not field_name or not isinstance(source, str) or not source:
            raise ValueError("payload_fields must map nonempty input fields to context fields")
        if field_name in payload:
            raise ValueError(f"Duplicate configured capability payload field: {field_name}")
        if source.split(".")[0] in {"tenant_id", "user_id", "permissions"}:
            raise ValueError("capability mapping cannot forward caller authority")
        value: Any = context
        if source in context:
            value = context[source]
        else:
            for part in source.split("."):
                if not isinstance(value, dict) or part not in value:
                    raise ValueError(f"Missing configured capability context field: {source}")
                value = value[part]
        payload[field_name] = deepcopy(value)
    _reject_capability_authority(payload)
    return payload


def _capability_inverse(config: Any, snapshot: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise ValueError("compensation_capability must be an object")
    return {"kind": "capability", "key": config.get("key"), "version": config.get("version"),
            "payload": _capability_payload(config, snapshot)}

_CLARIFICATION_TEXT_KEYS = (
    "clarification_note",
    "note_text",
    "explanation",
    "comment",
    "notes",
)


_GENERATED_SCRIPT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS workflow_generated_python_script (
    script_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    configuration_scope VARCHAR(16) NOT NULL DEFAULT 'tenant' CHECK (configuration_scope IN ('tenant', 'global')),
    script_key VARCHAR(256) NOT NULL,
    script_name VARCHAR(256) NOT NULL,
    script_source TEXT NOT NULL,
    entrypoint VARCHAR(128) NOT NULL DEFAULT 'run',
    approval_status VARCHAR(32) NOT NULL DEFAULT 'draft' CHECK (approval_status IN ('draft','approved','deprecated','archived')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_by VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_generated_script_scope_key ON workflow_generated_python_script(tenant_id, configuration_scope, script_key);
CREATE INDEX IF NOT EXISTS idx_wgps_script_key ON workflow_generated_python_script(tenant_id, script_key);
CREATE INDEX IF NOT EXISTS idx_wgps_approval_status ON workflow_generated_python_script(approval_status);
CREATE INDEX IF NOT EXISTS idx_wgps_is_active ON workflow_generated_python_script(is_active);
CREATE INDEX IF NOT EXISTS idx_wgps_metadata_gin ON workflow_generated_python_script USING GIN(metadata);
"""


_GENERATED_SCRIPT_AUDIT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS workflow_generated_python_script_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    run_id BIGINT,
    step_key VARCHAR(256),
    script_id BIGINT REFERENCES workflow_generated_python_script(script_id) ON DELETE SET NULL,
    script_key VARCHAR(256),
    entrypoint VARCHAR(128),
    execution_status VARCHAR(32) NOT NULL,
    duration_ms INTEGER,
    input_hash VARCHAR(64),
    output_hash VARCHAR(64),
    input_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key VARCHAR(256),
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wgps_audit_run_id ON workflow_generated_python_script_audit(run_id);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_step_key ON workflow_generated_python_script_audit(step_key);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_script_key ON workflow_generated_python_script_audit(script_key);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_status ON workflow_generated_python_script_audit(execution_status);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_executed_at ON workflow_generated_python_script_audit(executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_metadata_gin ON workflow_generated_python_script_audit USING GIN(metadata);
"""


def _ensure_generated_script_table(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(_GENERATED_SCRIPT_TABLE_DDL)
    object_db.ensure_tenant_isolation_for_tables(connection, ("workflow_generated_python_script",))


def _ensure_generated_script_audit_table(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(_GENERATED_SCRIPT_AUDIT_TABLE_DDL)
        cursor.execute(
            "ALTER TABLE IF EXISTS workflow_generated_python_script_audit ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(256)"
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_wgps_audit_idempotency
            ON workflow_generated_python_script_audit(run_id, step_key, script_key, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            """
        )
    object_db.ensure_tenant_isolation_for_tables(connection, ("workflow_generated_python_script_audit",))


def _stable_hash_payload(value: Any) -> str:
    try:
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        payload = repr(value)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _record_generated_script_execution_audit(
    connection: Any,
    *,
    run_id: int | None,
    step_key: str,
    script_id: int | None,
    script_key: str,
    entrypoint: str,
    execution_status: str,
    duration_ms: int,
    input_payload: dict[str, Any],
    output_payload: dict[str, Any],
    error_message: str | None,
    idempotency_key: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> bool:
    _ensure_generated_script_audit_table(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO workflow_generated_python_script_audit (
                run_id, step_key, script_id, script_key, entrypoint,
                execution_status, duration_ms, input_hash, output_hash,
                input_payload, output_payload, error_message, metadata, idempotency_key, executed_at
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, %s, %s::jsonb, %s, NOW()
            )
            ON CONFLICT (run_id, step_key, script_key, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            DO NOTHING
            """,
            (
                int(run_id) if run_id is not None else None,
                str(step_key or "").strip() or None,
                int(script_id) if script_id is not None else None,
                str(script_key or "").strip() or None,
                str(entrypoint or "").strip() or None,
                str(execution_status or "").strip().lower() or "unknown",
                int(duration_ms or 0),
                _stable_hash_payload(input_payload),
                _stable_hash_payload(output_payload),
                json.dumps(input_payload or {}),
                json.dumps(output_payload or {}),
                str(error_message or "").strip() or None,
                json.dumps(metadata or {}),
                str(idempotency_key or "").strip() or None,
            ),
        )
        return int(cursor.rowcount or 0) > 0


def _load_generated_script_for_execution(connection: Any, config: dict[str, Any]) -> dict[str, Any] | None:
    script_id_raw = config.get("generated_script_id")
    script_key_raw = config.get("generated_script_key")
    script_id: int | None = None
    try:
        if script_id_raw is not None and str(script_id_raw).strip() != "":
            script_id = int(script_id_raw)
    except (TypeError, ValueError):
        script_id = None

    script_key = str(script_key_raw or "").strip()
    if script_id is None and not script_key:
        return None

    _ensure_generated_script_table(connection)
    with connection.cursor() as cursor:
        if script_id is not None:
            cursor.execute(
                """
                SELECT script_id, script_key, script_name, script_source, entrypoint, approval_status, metadata, is_active
                FROM workflow_generated_python_script
                WHERE script_id = %s
                ORDER BY CASE WHEN configuration_scope = 'tenant' THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (int(script_id),),
            )
        else:
            cursor.execute(
                """
                SELECT script_id, script_key, script_name, script_source, entrypoint, approval_status, metadata, is_active
                FROM workflow_generated_python_script
                WHERE script_key = %s
                ORDER BY CASE WHEN configuration_scope = 'tenant' THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (script_key,),
            )
        row = cursor.fetchone()

    if not row:
        return None

    return {
        "script_id": int(row[0]),
        "script_key": row[1],
        "script_name": row[2],
        "script_source": row[3],
        "entrypoint": row[4],
        "approval_status": row[5],
        "metadata": row[6] or {},
        "is_active": bool(row[7]),
    }


def _extract_amount_candidates(text: str) -> list[str]:
    if not text:
        return []
    pattern = re.compile(r"(?:(?:CHF|EUR|USD)\s*)?[+-]?\d{1,3}(?:[',\s]\d{3})*(?:\.\d{1,2})?|[+-]?\d+(?:\.\d{1,2})")
    values: list[str] = []
    seen: set[str] = set()
    for match in pattern.findall(text):
        value = str(match).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        values.append(value)
    return values


def _build_clarification_context(
    payload: dict[str, Any] | None,
    doc_refs: list[Any] | None,
    source: str,
) -> dict[str, Any] | None:
    data = dict(payload or {})
    if not data and not doc_refs:
        return None

    note_fragments: list[str] = []
    for key in _CLARIFICATION_TEXT_KEYS:
        value = data.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            note_fragments.append(text)

    joined_note = "\n".join(note_fragments).strip()
    lowered = joined_note.lower()

    issue_flags: list[str] = []
    if any(token in lowered for token in ("allowance", "no receipt", "without receipt", "receipt missing")):
        issue_flags.append("unreceipted_allowance")
    if any(token in lowered for token in ("combined", "multiple receipts", "several receipts", "aggregation")):
        issue_flags.append("multi_receipt_allocation")
    if any(token in lowered for token in ("personal account", "personal transfer", "private account")):
        issue_flags.append("personal_account_transfer")
    if any(token in lowered for token in ("mismatch", "does not match", "unmatched", "difference")):
        issue_flags.append("amount_mismatch")

    clarification = {
        "source": source,
        "note_text": joined_note,
        "doc_refs": list(doc_refs or []),
        "provided_fields": sorted(list(data.keys())),
        "amount_candidates": _extract_amount_candidates(joined_note),
        "issue_flags": issue_flags,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    if not joined_note and not clarification["doc_refs"] and not clarification["provided_fields"]:
        return None
    return clarification


def _build_note_payload_from_doc_refs(db_connection_fn, doc_refs: list[Any] | None) -> dict[str, Any]:
    ids: list[int] = []
    seen: set[int] = set()
    for item in list(doc_refs or []):
        try:
            if isinstance(item, dict):
                raw = item.get("doc_id")
            else:
                raw = item
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value <= 0 or value in seen:
            continue
        seen.add(value)
        ids.append(value)

    if not ids:
        return {}

    note_fragments: list[str] = []
    tags: list[str] = []
    for doc_id in ids:
        with db_connection_fn() as conn:
            row = object_db.get_document_brief_by_id(conn, doc_id=doc_id)
        if not row:
            continue
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}

        for key in ("note_content", "user_description", "doc_desc", "description"):
            value = metadata.get(key)
            if value is None and key in ("doc_desc", "description"):
                value = row.get("doc_desc")
            text = str(value or "").strip()
            if text:
                note_fragments.append(text)
                break

        for key in ("note_tags", "user_tags"):
            value = metadata.get(key)
            if isinstance(value, list):
                for tag in value:
                    text = str(tag or "").strip()
                    if text:
                        tags.append(text)

    if not note_fragments:
        return {}

    joined = "\n\n".join(note_fragments)
    return {
        "clarification_note": joined,
        "notes": joined,
        "clarification_tags": sorted({str(tag).strip() for tag in tags if str(tag).strip()}),
    }

# ---------------------------------------------------------------------------
# Pause signal
# ---------------------------------------------------------------------------


class PipelineStepPauseRequired(Exception):
    """Raised by a python_binding step to signal that the run must be paused.

    Args:
        reason: 'user_interaction' | 'missing_data' | 'approval_required'
        prompt: Human-readable prompt to show the user (for user_interaction)
        missing_data_desc: Description of what data is missing (for missing_data)
        required_doc_types: List of document types that would provide the missing data
    """

    def __init__(
        self,
        reason: str = "user_interaction",
        prompt: str = "",
        missing_data_desc: str = "",
        required_doc_types: list[str] | None = None,
    ) -> None:
        super().__init__(reason)
        self.reason = str(reason or "user_interaction")
        self.prompt = str(prompt or "")
        self.missing_data_desc = str(missing_data_desc or "")
        self.required_doc_types: list[str] = list(required_doc_types or [])


# ---------------------------------------------------------------------------
# Internal step result
# ---------------------------------------------------------------------------


@dataclass
class _StepOutcome:
    success: bool = False
    output: dict[str, Any] = field(default_factory=dict)
    paused: bool = False
    pause_reason: str | None = None
    interaction_prompt: str | None = None
    missing_data_desc: str | None = None
    required_doc_types: list[str] = field(default_factory=list)
    error_message: str | None = None
    handled_exception: bool = False
    next_step_key: str | None = None
    applied_clause: str | None = None


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class WorkflowPipelineExecutor:
    """Execute workflow pipeline runs with pause/resume semantics."""

    def __init__(self, db_connection_fn, solf_interpreter=None):
        """
        Args:
            db_connection_fn: Callable returning a psycopg2 connection.
            solf_interpreter: Optional pre-loaded SOLFInterpreter instance.
                              Required when steps use step_kind='clause'.
        """
        self.db_connection_fn = db_connection_fn
        self.solf_interpreter = solf_interpreter

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute_ingestion_run(
        self, *, run_id: int, workflow_version_id: int,
        input_context: dict[str, Any], idempotency_key: str,
    ) -> dict[str, Any]:
        """Execute an outbox-reserved run; never allocate a second pipeline run."""
        from contextlib import closing
        from security_context import get_security_context

        caller = get_security_context()
        if (not caller or not caller.tenant_id or not caller.user_id
                or not caller.allows("workflows.run") or not caller.allows("documents.write")):
            raise PermissionError("Explicit tenant/principal, documents.write and workflows.run required")
        with closing(self.db_connection_fn()) as conn, conn:
            run = object_db.get_pipeline_run(conn, run_id)
            if not run or run.get("workflow_version_id") != workflow_version_id:
                raise ValueError("Reserved ingestion run not found for this workflow version")
            with conn.cursor() as cursor:
                cursor.execute("SELECT tenant_id::text FROM workflow_pipeline_run "
                               "WHERE run_id=%s AND tenant_id=%s::uuid", (run_id, caller.tenant_id))
                row = cursor.fetchone()
                if not row or row[0] != caller.tenant_id:
                    raise PermissionError("Reserved run belongs to another tenant")
            if run.get("input_context") != input_context or input_context.get("idempotency_key") != idempotency_key:
                raise ValueError("Reserved ingestion input does not match its immutable event")
            if run.get("run_status") == "completed":
                return run
            if run.get("run_status") != "pending":
                raise ValueError("Reserved run requires explicit reconciliation/resume; automatic replay is disabled")
            steps = object_db.list_solf_workflow_steps(conn, workflow_version_id=workflow_version_id)
            if not steps:
                raise ValueError("Reserved ingestion workflow has no executable steps")
            with conn.cursor() as cursor:
                cursor.execute("UPDATE workflow_pipeline_run SET run_status = 'running', modified_at=NOW() "
                               "WHERE run_id=%s AND tenant_id=%s::uuid AND run_status = 'pending' RETURNING run_id",
                               (run_id, caller.tenant_id))
                if not cursor.fetchone():
                    raise RuntimeError("Reserved run is already claimed; reconciliation required")
            conn.commit()  # Fence other adapters before helpers with internal commits.
            object_db.link_pipeline_run_documents(
                conn, run_id=run_id, doc_refs=input_context.get("doc_refs"),
                source="ingestion_event", linked_by=caller.user_id,
                metadata={"event": "ingestion_reserved_run"},
            )
        return self._execute_run(run_id, steps, dict(input_context))

    def start_pipeline_run(
        self,
        workflow_version_id: int,
        input_context: dict[str, Any],
        started_by: str | None = None,
        workflow_key: str | None = None,
    ) -> dict[str, Any]:
        """Create and immediately execute a pipeline run.

        Returns the final run state dict (possibly paused after the first blocking step).
        """
        with self.db_connection_fn() as conn:
            steps = object_db.list_solf_workflow_steps(conn, workflow_version_id=workflow_version_id)

        if not steps:
            return {"error": "no_steps", "message": f"No steps found for workflow_version_id={workflow_version_id}"}

        with self.db_connection_fn() as conn:
            run_id = object_db.create_pipeline_run(
                conn,
                workflow_version_id=workflow_version_id,
                workflow_key=workflow_key,
                input_context=input_context,
                started_by=started_by,
            )

            # Persist explicit run-to-document links for auditability and rerun checks.
            object_db.link_pipeline_run_documents(
                conn,
                run_id=run_id,
                doc_refs=input_context.get("doc_refs") if isinstance(input_context, dict) else None,
                source="input_context",
                linked_by=started_by,
                metadata={"event": "run_start"},
            )

        initial_context = dict(input_context)
        initial_clarification = _build_clarification_context(
            payload=input_context if isinstance(input_context, dict) else {},
            doc_refs=(input_context or {}).get("doc_refs") if isinstance(input_context, dict) else None,
            source="run_start",
        )
        if initial_clarification is not None:
            history = list(initial_context.get("clarification_history") or [])
            history.append(initial_clarification)
            initial_context["clarification_history"] = history
            initial_context["clarification_latest"] = initial_clarification

        LOGGER.info("pipeline_run start run_id=%s workflow_version_id=%s", run_id, workflow_version_id)
        return self._execute_run(run_id, steps, initial_context)

    def resume_pipeline_run(
        self,
        run_id: int,
        user_response: dict[str, Any] | None = None,
        doc_refs: list[Any] | None = None,
    ) -> dict[str, Any]:
        """Resume a paused run, optionally supplying user answers or new document refs.

        The paused step's status is reset to 'pending' and execution continues
        from that step with the enriched context.
        """
        with self.db_connection_fn() as conn:
            run = object_db.get_pipeline_run(conn, run_id)

        if run is None:
            return {"error": "not_found", "message": f"Pipeline run {run_id} not found"}

        if run["run_status"] != "paused":
            return {
                "error": "not_paused",
                "message": f"Run {run_id} is in status '{run['run_status']}', not paused",
            }

        paused_step_key = run.get("paused_at_step_key")
        if not paused_step_key:
            return {"error": "no_paused_step", "message": "Run is marked paused but has no paused_at_step_key"}

        # Store user response in the paused step row
        if user_response or doc_refs:
            with self.db_connection_fn() as conn:
                object_db.supply_pipeline_step_response(
                    conn,
                    run_id=run_id,
                    step_key=paused_step_key,
                    user_response=user_response,
                    doc_refs=doc_refs,
                )

                object_db.link_pipeline_run_documents(
                    conn,
                    run_id=run_id,
                    doc_refs=doc_refs,
                    source="resume",
                    step_key=paused_step_key,
                    metadata={"event": "run_resume"},
                )

        # Reload steps from DB (in case workflow version was updated)
        wv_id = run.get("workflow_version_id")
        with self.db_connection_fn() as conn:
            steps = object_db.list_solf_workflow_steps(conn, workflow_version_id=wv_id) if wv_id else []

        if not steps:
            return {"error": "no_steps", "message": "No steps found for this run's workflow version"}

        # Determine which steps remain (from paused_step_key onward)
        step_keys = [s["step_key"] for s in steps]
        try:
            resume_idx = step_keys.index(paused_step_key)
        except ValueError:
            resume_idx = 0

        remaining_steps = steps[resume_idx:]
        context = dict(run.get("current_context") or {})

        # Enrich context with user-supplied response
        if user_response:
            context.update(user_response)
        if doc_refs:
            context.setdefault("_resumed_doc_refs", [])
            context["_resumed_doc_refs"] = list(context["_resumed_doc_refs"]) + list(doc_refs)

        note_payload = _build_note_payload_from_doc_refs(self.db_connection_fn, doc_refs)
        merged_payload = dict(note_payload)
        if user_response:
            merged_payload.update(user_response)

        clarification = _build_clarification_context(
            payload=merged_payload,
            doc_refs=doc_refs,
            source="resume",
        )
        if clarification is not None:
            history = list(context.get("clarification_history") or [])
            history.append(clarification)
            context["clarification_history"] = history
            context["clarification_latest"] = clarification

        LOGGER.info("pipeline_run resume run_id=%s from_step=%s", run_id, paused_step_key)
        return self._execute_run(run_id, remaining_steps, context, resuming=True)

    def cancel_pipeline_run(self, run_id: int) -> bool:
        with self.db_connection_fn() as conn:
            run = object_db.get_pipeline_run(conn, run_id)
            if run is None:
                return False
            if run["run_status"] in {"completed", "failed", "cancelled"}:
                return False
            object_db.update_pipeline_run_status(conn, run_id, "cancelled", finished=True)
        LOGGER.info("pipeline_run cancelled run_id=%s", run_id)
        return True

    def get_run_state(self, run_id: int) -> dict[str, Any] | None:
        with self.db_connection_fn() as conn:
            run = object_db.get_pipeline_run(conn, run_id)
            if run is None:
                return None
            steps = object_db.get_pipeline_run_steps(conn, run_id)
        run["steps"] = steps
        return run

    def list_runs(
        self,
        run_status: str | None = None,
        workflow_key: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with self.db_connection_fn() as conn:
            return object_db.list_pipeline_runs(conn, run_status=run_status, workflow_key=workflow_key, limit=limit)

    def list_unfinished_runs(
        self,
        workflow_key: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with self.db_connection_fn() as conn:
            return object_db.list_unfinished_pipeline_runs(conn, workflow_key=workflow_key, limit=limit)

    def undo_pipeline_run(
        self,
        run_id: int,
        requested_by: str = "api:user",
        dry_run: bool = False,
        include_completed_only: bool = True,
    ) -> dict[str, Any]:
        with self.db_connection_fn() as conn:
            run = object_db.get_pipeline_run(conn, run_id)
            if run is None:
                return {"error": "not_found", "message": f"Pipeline run {run_id} not found"}

            steps = object_db.get_pipeline_run_steps(conn, run_id)
            workflow_version_id = int(run.get("workflow_version_id") or 0)
            version_steps = object_db.list_solf_workflow_steps(conn, workflow_version_id=workflow_version_id) if workflow_version_id > 0 else []

        step_by_key = {
            str(step.get("step_key") or "").strip(): step
            for step in version_steps
            if str(step.get("step_key") or "").strip()
        }

        eligible = [
            step for step in steps
            if (step.get("step_status") == "completed" if include_completed_only else step.get("step_status") in {"completed", "failed", "paused"})
        ]
        eligible.sort(key=lambda item: int(item.get("step_order") or 0), reverse=True)

        actions: list[dict[str, Any]] = []
        context = dict(run.get("current_context") or {})

        for step_state in eligible:
            step_key = str(step_state.get("step_key") or "").strip()
            step_order = int(step_state.get("step_order") or 0)
            step_def = step_by_key.get(step_key, {})
            step_config = step_def.get("config") if isinstance(step_def.get("config"), dict) else {}
            compensation_clause = str(step_config.get("compensation_clause") or "").strip()

            if "compensation_capability" in step_config:
                action = {"step_key": step_key, "step_order": step_order, "inverse_kind": "capability"}
                try:
                    snapshot = step_state.get("output_snapshot")
                    if not isinstance(snapshot, dict) or not snapshot:
                        raise ValueError("Original capability output_snapshot is required")
                    inverse = _capability_inverse(step_config["compensation_capability"], snapshot)
                    _validate_capability_action(inverse)
                    action.update({"capability_key": inverse["key"], "capability_version": inverse["version"]})
                    if dry_run:
                        action["status"] = "planned"
                    else:
                        from mutation_undo import _handle_capability
                        before_ctx = deepcopy(context)
                        with self.db_connection_fn() as conn:
                            outcome = _handle_capability(conn, inverse_action=inverse, entry=step_state,
                                                         requested_by=requested_by, reason=None)
                            if not outcome["ok"]:
                                raise RuntimeError(outcome["detail"])
                            context["compensation_result"] = outcome["result"]
                            object_db.append_workflow_mutation_journal(
                                conn, run_id=run_id, step_key=step_key, step_order=step_order,
                                operation_kind="compensation_execute", before_state=before_ctx,
                                after_state=deepcopy(context), inverse_action=None,
                                mutation_key=f"run:{run_id}:step:{step_key}:compensation",
                                metadata={"requested_by": requested_by, "capability_key": inverse["key"],
                                          "capability_version": inverse["version"]},
                            )
                        action["status"] = "compensated"
                except Exception as exc:
                    action.update({"status": "failed", "executable": False, "error": str(exc)})
                actions.append(action)
                continue

            if not compensation_clause:
                actions.append(
                    {
                        "step_key": step_key,
                        "step_order": step_order,
                        "status": "skipped",
                        "reason": "no_compensation_clause",
                    }
                )
                continue

            before_ctx = dict(context)
            if dry_run:
                actions.append(
                    {
                        "step_key": step_key,
                        "step_order": step_order,
                        "status": "planned",
                        "compensation_clause": compensation_clause,
                    }
                )
                continue

            outcome = self._execute_clause_by_name(
                clause_name=compensation_clause,
                context=context,
                config=step_config,
            )

            if outcome.success and isinstance(outcome.output, dict):
                context.update(outcome.output)
                with self.db_connection_fn() as conn:
                    object_db.append_workflow_mutation_journal(
                        conn,
                        run_id=run_id,
                        step_key=step_key,
                        step_order=step_order,
                        operation_kind="compensation_execute",
                        before_state=before_ctx,
                        after_state=dict(context),
                        inverse_action={"kind": "compensation_clause", "clause_name": compensation_clause},
                        target_store="sql",
                        target_entity="workflow_context",
                        mutation_key=f"run:{run_id}:step:{step_key}:compensation",
                        metadata={
                            "requested_by": str(requested_by or "api:user"),
                            "origin_step_status": step_state.get("step_status"),
                        },
                    )
                actions.append(
                    {
                        "step_key": step_key,
                        "step_order": step_order,
                        "status": "compensated",
                        "compensation_clause": compensation_clause,
                    }
                )
            else:
                actions.append(
                    {
                        "step_key": step_key,
                        "step_order": step_order,
                        "status": "failed",
                        "compensation_clause": compensation_clause,
                        "error": outcome.error_message or "compensation clause failed",
                    }
                )

        if not dry_run:
            with self.db_connection_fn() as conn:
                object_db.update_pipeline_run_status(
                    conn,
                    run_id,
                    run.get("run_status") or "completed",
                    current_context=context,
                )

        return {
            "run_id": run_id,
            "dry_run": bool(dry_run),
            "requested_by": str(requested_by or "api:user"),
            "actions": actions,
            "summary": {
                "planned": len([a for a in actions if a.get("status") == "planned"]),
                "compensated": len([a for a in actions if a.get("status") == "compensated"]),
                "skipped": len([a for a in actions if a.get("status") == "skipped"]),
                "failed": len([a for a in actions if a.get("status") == "failed"]),
            },
        }

    # ------------------------------------------------------------------
    # Internal execution loop
    # ------------------------------------------------------------------

    def _execute_run(
        self,
        run_id: int,
        steps: list[dict[str, Any]],
        context: dict[str, Any],
        resuming: bool = False,
    ) -> dict[str, Any]:
        # Mark run as running
        with self.db_connection_fn() as conn:
            object_db.update_pipeline_run_status(conn, run_id, "running", current_context=context)

        step_key_to_idx = {
            str(step.get("step_key") or "").strip(): idx
            for idx, step in enumerate(steps)
            if str(step.get("step_key") or "").strip()
        }

        idx = 0
        while idx < len(steps):
            step = steps[idx]
            step_key = step["step_key"]
            step_order = int(step.get("step_order") or 0)
            step_kind = str(step.get("step_kind") or "clause")

            LOGGER.info("pipeline_run execute run_id=%s step=%s kind=%s", run_id, step_key, step_kind)

            # Load user response / doc_refs stored for this step (resume case)
            prior_user_response: dict[str, Any] = {}
            prior_doc_refs: list[Any] = []
            if resuming:
                with self.db_connection_fn() as conn:
                    existing_steps = object_db.get_pipeline_run_steps(conn, run_id)
                    for s in existing_steps:
                        if s["step_key"] == step_key:
                            prior_user_response = s.get("user_response") or {}
                            prior_doc_refs = s.get("doc_refs") or []
                            break

            step_context = dict(context)
            if prior_user_response:
                step_context.update(prior_user_response)
            if prior_doc_refs:
                step_context.setdefault("_doc_refs", [])
                step_context["_doc_refs"] = list(step_context["_doc_refs"]) + list(prior_doc_refs)

            # Persist step as running
            with self.db_connection_fn() as conn:
                object_db.upsert_pipeline_run_step(
                    conn,
                    run_id=run_id,
                    step_key=step_key,
                    step_order=step_order,
                    step_status="running",
                    input_snapshot=step_context,
                    increment_attempt=True,
                )

            outcome = self._execute_step(step, step_context, run_id=run_id)

            if outcome.paused:
                # Persist paused state
                with self.db_connection_fn() as conn:
                    object_db.upsert_pipeline_run_step(
                        conn,
                        run_id=run_id,
                        step_key=step_key,
                        step_order=step_order,
                        step_status="paused",
                        output_snapshot={},
                        pause_reason=outcome.pause_reason,
                        interaction_prompt=outcome.interaction_prompt,
                        missing_data_desc=outcome.missing_data_desc,
                        required_doc_types=outcome.required_doc_types,
                    )
                    object_db.update_pipeline_run_status(
                        conn,
                        run_id,
                        "paused",
                        current_context=context,
                        paused_at_step_key=step_key,
                    )

                LOGGER.info(
                    "pipeline_run paused run_id=%s step=%s reason=%s",
                    run_id, step_key, outcome.pause_reason,
                )
                with self.db_connection_fn() as conn:
                    run = object_db.get_pipeline_run(conn, run_id)
                    run["steps"] = object_db.get_pipeline_run_steps(conn, run_id)
                return run

            if not outcome.success:
                handled = self._handle_step_exception_flow(
                    run_id=run_id,
                    step=step,
                    step_context=step_context,
                    context=context,
                    error_message=outcome.error_message or "step failed",
                )
                if handled.get("handled"):
                    branch_output = handled.get("output") if isinstance(handled.get("output"), dict) else {}
                    if branch_output:
                        context.update(branch_output)
                    with self.db_connection_fn() as conn:
                        object_db.upsert_pipeline_run_step(
                            conn,
                            run_id=run_id,
                            step_key=step_key,
                            step_order=step_order,
                            step_status="completed",
                            output_snapshot={
                                "exception_handled": True,
                                "error_message": outcome.error_message,
                                "exception_clause": handled.get("exception_clause"),
                                "exception_output": branch_output,
                            },
                        )
                        object_db.append_workflow_mutation_journal(
                            conn,
                            run_id=run_id,
                            step_key=step_key,
                            step_order=step_order,
                            operation_kind="exception_clause_execute",
                            before_state=step_context,
                            after_state=dict(context),
                            inverse_action={
                                "kind": "context_restore",
                                "context": step_context,
                            },
                            target_store="sql",
                            target_entity="workflow_context",
                            mutation_key=f"run:{run_id}:step:{step_key}:exception",
                            metadata={
                                "error_message": outcome.error_message,
                                "next_step_key": handled.get("next_step_key"),
                            },
                        )

                    next_step_key = str(handled.get("next_step_key") or "").strip()
                    if next_step_key and next_step_key in step_key_to_idx:
                        idx = int(step_key_to_idx[next_step_key])
                    else:
                        idx += 1
                    continue

                error_msg = outcome.error_message or "step failed"
                with self.db_connection_fn() as conn:
                    object_db.upsert_pipeline_run_step(
                        conn,
                        run_id=run_id,
                        step_key=step_key,
                        step_order=step_order,
                        step_status="failed",
                        output_snapshot={},
                        error_message=error_msg,
                    )
                    object_db.update_pipeline_run_status(
                        conn, run_id, "failed",
                        current_context=context, finished=True,
                    )

                LOGGER.error("pipeline_run failed run_id=%s step=%s error=%s", run_id, step_key, error_msg)

                on_failure_policy = str(context.get("on_failure_policy") or "").strip().lower()
                if on_failure_policy == "undo_all":
                    LOGGER.info("pipeline_run auto-undo triggered run_id=%s policy=on_failure_policy=undo_all", run_id)
                    auto_undo_result = self.undo_pipeline_run(run_id, requested_by="system:on_failure_policy")
                else:
                    auto_undo_result = None

                with self.db_connection_fn() as conn:
                    run = object_db.get_pipeline_run(conn, run_id)
                    run["steps"] = object_db.get_pipeline_run_steps(conn, run_id)
                if auto_undo_result is not None:
                    run["auto_undo"] = auto_undo_result
                return run

            # Step succeeded — merge outputs into running context
            if isinstance(outcome.output, dict):
                context.update(outcome.output)

            with self.db_connection_fn() as conn:
                object_db.upsert_pipeline_run_step(
                    conn,
                    run_id=run_id,
                    step_key=step_key,
                    step_order=step_order,
                    step_status="completed",
                    output_snapshot=outcome.output or {},
                )
                object_db.append_workflow_mutation_journal(
                    conn,
                    run_id=run_id,
                    step_key=step_key,
                    step_order=step_order,
                    operation_kind="step_output_merge",
                    before_state=step_context,
                    after_state=dict(context),
                    inverse_action={
                        "kind": "context_restore",
                        "context": step_context,
                    },
                    target_store="sql",
                    target_entity="workflow_context",
                    mutation_key=f"run:{run_id}:step:{step_key}:merge",
                    metadata={"step_kind": step_kind},
                )

            next_step_key = str(outcome.next_step_key or "").strip()
            if next_step_key and next_step_key in step_key_to_idx:
                idx = int(step_key_to_idx[next_step_key])
            else:
                idx += 1

        # All steps completed
        with self.db_connection_fn() as conn:
            object_db.update_pipeline_run_status(
                conn, run_id, "completed",
                current_context=context, output_context=context, finished=True,
            )

        LOGGER.info("pipeline_run completed run_id=%s", run_id)
        with self.db_connection_fn() as conn:
            run = object_db.get_pipeline_run(conn, run_id)
            run["steps"] = object_db.get_pipeline_run_steps(conn, run_id)
        return run

    def _execute_clause_by_name(self, clause_name: str, context: dict[str, Any], config: dict[str, Any] | None = None) -> _StepOutcome:
        cfg = dict(config or {})
        pseudo_step = {
            "step_key": f"clause::{clause_name}",
            "step_kind": "clause",
            "clause_name": str(clause_name or "").strip(),
            "config": cfg,
        }
        return self._execute_clause_step(pseudo_step, context, cfg)

    def _handle_step_exception_flow(
        self,
        *,
        run_id: int,
        step: dict[str, Any],
        step_context: dict[str, Any],
        context: dict[str, Any],
        error_message: str,
    ) -> dict[str, Any]:
        config = step.get("config") if isinstance(step.get("config"), dict) else {}
        exception_clause = str(config.get("exception_clause") or "").strip()
        next_step_key = str(config.get("on_failure_step_key") or "").strip() or None
        if not exception_clause and not next_step_key:
            return {"handled": False}

        if not exception_clause:
            return {
                "handled": True,
                "output": {
                    "exception_handled": True,
                    "exception_mode": "branch_only",
                    "source_step": step.get("step_key"),
                    "error_message": error_message,
                },
                "exception_clause": None,
                "next_step_key": next_step_key,
            }

        exception_context = dict(step_context)
        exception_context["_failure"] = {
            "run_id": int(run_id),
            "step_key": str(step.get("step_key") or "").strip(),
            "error_message": str(error_message or "").strip(),
        }

        outcome = self._execute_clause_by_name(exception_clause, exception_context, config)
        if not outcome.success:
            return {"handled": False}

        output = outcome.output if isinstance(outcome.output, dict) else {}
        if output:
            context.update(output)
        return {
            "handled": True,
            "output": output,
            "exception_clause": exception_clause,
            "next_step_key": next_step_key,
        }

    def _execute_step(self, step: dict[str, Any], context: dict[str, Any], run_id: int | None = None) -> _StepOutcome:
        step_kind = str(step.get("step_kind") or "clause")
        config: dict[str, Any] = step.get("config") or {}

        try:
            if step_kind == "clause":
                return self._execute_clause_step(step, context, config)
            elif step_kind == "python_binding":
                return self._execute_python_step(step, context, config, run_id=run_id)
            else:
                LOGGER.warning(
                    "pipeline step_kind=%s not yet implemented; treating as no-op (step_key=%s)",
                    step_kind, step.get("step_key"),
                )
                return _StepOutcome(success=True, output={})
        except PipelineStepPauseRequired as exc:
            return _StepOutcome(
                success=False,
                paused=True,
                pause_reason=exc.reason,
                interaction_prompt=exc.prompt or None,
                missing_data_desc=exc.missing_data_desc or None,
                required_doc_types=exc.required_doc_types,
            )
        except Exception as exc:
            LOGGER.exception("pipeline step error step_key=%s", step.get("step_key"))
            return _StepOutcome(success=False, error_message=str(exc))

    def _load_clause_body_from_db(self, clause_name: str) -> str:
        normalized = str(clause_name or "").strip()
        if not normalized:
            return ""

        with self.db_connection_fn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT clause_body
                    FROM solf_clauses
                    WHERE clause_name = %s
                      AND is_active = TRUE
                    ORDER BY CASE WHEN configuration_scope = 'tenant' THEN 0 ELSE 1 END,
                                                         modified_at DESC, clause_id DESC
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cursor.fetchone()

        return str(row[0] or "").strip() if row else ""

    def _hydrate_clause_from_db(self, clause_name: str) -> bool:
        clause_text = self._load_clause_body_from_db(clause_name)
        if not clause_text:
            return False

        loader = getattr(self.solf_interpreter, "load_program_script", None)
        if not callable(loader):
            return False

        try:
            loader(clause_text, clear_existing=False)
        except Exception:
            LOGGER.exception("Failed to hydrate SOLF clause from DB clause_name=%s", clause_name)
            return False

        clause_lookup = getattr(self.solf_interpreter, "get_clause_definitions", None)
        if callable(clause_lookup):
            try:
                return bool(clause_lookup(clause_name))
            except Exception:
                return False
        return True

    def _execute_clause_step(
        self, step: dict[str, Any], context: dict[str, Any], config: dict[str, Any]
    ) -> _StepOutcome:
        if self.solf_interpreter is None:
            return _StepOutcome(
                success=False,
                error_message="solf_interpreter not provided; cannot execute clause step",
            )

        clause_name = str(step.get("clause_name") or "").strip()
        if not clause_name:
            return _StepOutcome(success=False, error_message="clause step has no clause_name")

        clause_lookup = getattr(self.solf_interpreter, "get_clause_definitions", None)
        if callable(clause_lookup):
            try:
                loaded_defs = clause_lookup(clause_name)
            except Exception:
                loaded_defs = []
            if not loaded_defs:
                loaded_from_db = self._hydrate_clause_from_db(clause_name)
                if loaded_from_db:
                    try:
                        loaded_defs = clause_lookup(clause_name)
                    except Exception:
                        loaded_defs = []

            if not loaded_defs:
                source = str(config.get("source") or "").strip().lower()
                if source == "workflow_narrative":
                    # Narrative fallback steps can exist before a concrete SOLF clause is authored.
                    # Treat this as a successful no-op so the run can still persist output artifacts.
                    return _StepOutcome(
                        success=True,
                        output={
                            "workflow_execution_mode": "narrative_fallback",
                            "workflow_step_status": "skipped_missing_clause",
                            "missing_clause_name": clause_name,
                            "workflow_narrative": str(config.get("narrative") or "").strip(),
                        },
                    )

        try:
            result = self.solf_interpreter._invoke_clause(clause_name, [context])
        except Exception as exc:
            raise  # re-raised, caught by _execute_step wrapper

        if isinstance(result, dict):
            # A SOLF clause can signal a pause by returning {paused: true, ...}
            if result.get("paused") is True or result.get("paused") == "true":
                raise PipelineStepPauseRequired(
                    reason=str(result.get("reason") or "user_interaction"),
                    prompt=str(result.get("prompt") or result.get("interaction_prompt") or ""),
                    missing_data_desc=str(result.get("missing_data_desc") or ""),
                    required_doc_types=list(result.get("required_doc_types") or []),
                )
            if result.get("ok") is False:
                return _StepOutcome(
                    success=False,
                    error_message=str(result.get("error") or f"SOLF clause '{clause_name}' returned ok=false"),
                )
            return _StepOutcome(success=True, output=result)

        if result is None or result is False:
            return _StepOutcome(success=False, error_message=f"SOLF clause '{clause_name}' returned falsy result")

        return _StepOutcome(success=True, output={"result": result})

    def _execute_capability_step(
        self, step: dict[str, Any], context: dict[str, Any], config: dict[str, Any],
        run_id: int | None,
    ) -> _StepOutcome:
        from capability_registry import CAPABILITIES

        key, version = config.get("capability_key"), config.get("capability_version")
        metadata = CAPABILITIES.resolve(key, version)
        save_as = config.get("save_as", "capability_result")
        if (not isinstance(save_as, str) or not save_as.strip() or save_as != save_as.strip() or "." in save_as
                or save_as in _CAPABILITY_METADATA_FIELDS
                or save_as.startswith(("_", "workflow_", "execution_"))):
            raise ValueError("capability save_as cannot overwrite execution metadata")
        payload = _capability_payload(config, context)
        if run_id is not None:
            candidate = dict(payload, idempotency_key=f"run:{int(run_id)}:step:{step['step_key']}:capability")
            validator = Draft202012Validator(metadata["input_schema"], format_checker=FormatChecker())
            if validator.is_valid(candidate):
                payload = candidate
            elif "idempotency_key" in payload:
                raise ValueError("Configured idempotency_key cannot override the run/step key")
        compensation = config.get("compensation_capability")
        if "compensation_capability" in config:
            if not isinstance(compensation, dict):
                raise ValueError("compensation_capability must be an object")
            CAPABILITIES.resolve(compensation.get("key"), compensation.get("version"))
            # Check shape/authority before invoking a potentially mutating handler.
            literals, fields = compensation.get("payload", {}), compensation.get("payload_fields", {})
            if not isinstance(literals, dict) or not isinstance(fields, dict):
                raise ValueError("compensation payload and payload_fields must be objects")
            _reject_capability_authority(literals)
            for name, source in fields.items():
                if (not isinstance(name, str) or not name or not isinstance(source, str) or not source
                        or name in literals or name in {"tenant_id", "user_id", "permissions"}
                        or source.split(".")[0] in {"tenant_id", "user_id", "permissions"}):
                    raise ValueError("Invalid compensation capability mapping")
        result = CAPABILITIES.execute(key, version, payload)
        if isinstance(result, dict) and result.get("ok") is False:
            return _StepOutcome(success=False, error_message=str(result.get("error") or result.get("reason") or "capability failed"))
        output = deepcopy(context)
        output[save_as] = result
        inverse = _capability_inverse(compensation, output) if compensation is not None else None
        with self.db_connection_fn() as conn:
            object_db.append_workflow_mutation_journal(
                conn, run_id=run_id, step_key=str(step.get("step_key") or ""),
                step_order=int(step.get("step_order") or 0), operation_kind="capability_execute",
                before_state=deepcopy(context), after_state=deepcopy(output), inverse_action=inverse,
                target_store="capability", target_entity=key,
                mutation_key=f"run:{run_id}:step:{step.get('step_key')}:capability",
                metadata={"capability_key": key, "capability_version": version, "payload": payload,
                          "save_as": save_as, "source": "workflow_pipeline_executor"},
            )
        return _StepOutcome(success=True, output=output)

    def _execute_python_step(
        self,
        step: dict[str, Any],
        context: dict[str, Any],
        config: dict[str, Any],
        run_id: int | None = None,
    ) -> _StepOutcome:
        if "capability_key" in config or "capability_version" in config:
            return self._execute_capability_step(step, context, config, run_id)

        generated_script = None
        with self.db_connection_fn() as conn:
            generated_script = _load_generated_script_for_execution(conn, config)

        if generated_script is not None:
            audit_started = time.perf_counter()
            step_key = str(step.get("step_key") or "").strip()
            entrypoint = str(config.get("entrypoint") or generated_script.get("entrypoint") or "run").strip() or "run"
            audit_input_payload = {
                "context": context,
                "config": config,
            }
            audit_output_payload: dict[str, Any] = {}
            audit_status = "success"
            audit_error: str | None = None
            explicit_audit_key = str(config.get("audit_idempotency_key") or "").strip() or None
            auto_dedupe = bool(config.get("audit_idempotent", True))

            def _finalize_audit() -> None:
                duration = int((time.perf_counter() - audit_started) * 1000)
                try:
                    with self.db_connection_fn() as conn:
                        _record_generated_script_execution_audit(
                            conn,
                            run_id=run_id,
                            step_key=step_key,
                            script_id=int(generated_script.get("script_id")) if generated_script.get("script_id") is not None else None,
                            script_key=str(generated_script.get("script_key") or ""),
                            entrypoint=entrypoint,
                            execution_status=audit_status,
                            duration_ms=duration,
                            input_payload=audit_input_payload,
                            output_payload=audit_output_payload,
                            error_message=audit_error,
                            idempotency_key=(
                                explicit_audit_key
                                or (
                                    f"{int(run_id)}:{step_key}:{str(generated_script.get('script_key') or '')}:{_stable_hash_payload(audit_input_payload)}"
                                    if auto_dedupe and run_id is not None
                                    else None
                                )
                            ),
                            metadata={"source": "workflow_pipeline_executor", "step_kind": "python_binding"},
                        )
                        conn.commit()
                except Exception:
                    LOGGER.exception("Failed to persist generated script execution audit")

            if not bool(generated_script.get("is_active")):
                audit_status = "error"
                audit_error = f"Generated script '{generated_script.get('script_key')}' is inactive"
                _finalize_audit()
                return _StepOutcome(
                    success=False,
                    error_message=audit_error,
                )
            if str(generated_script.get("approval_status") or "").strip().lower() != "approved":
                audit_status = "error"
                audit_error = f"Generated script '{generated_script.get('script_key')}' is not approved"
                _finalize_audit()
                return _StepOutcome(
                    success=False,
                    error_message=audit_error,
                )

            script_source = str(generated_script.get("script_source") or "")
            script_filename = f"<workflow-generated-script:{generated_script.get('script_id')}>"
            namespace: dict[str, Any] = {
                "__name__": f"idms_generated_script_{generated_script.get('script_id')}",
                "__builtins__": __builtins__,
                "PipelineStepPauseRequired": PipelineStepPauseRequired,
                "SCRIPT_METADATA": MappingProxyType(dict(generated_script.get("metadata") or {})),
            }

            try:
                compiled = compile(script_source, script_filename, "exec")
                exec(compiled, namespace, namespace)
            except Exception as exc:
                audit_status = "error"
                audit_error = f"Generated script compile/exec failure: {exc}"
                _finalize_audit()
                return _StepOutcome(
                    success=False,
                    error_message=audit_error,
                )

            fn = namespace.get(entrypoint)
            if fn is None or not callable(fn):
                audit_status = "error"
                audit_error = f"Entrypoint '{entrypoint}' not found in generated script '{generated_script.get('script_key')}'"
                _finalize_audit()
                return _StepOutcome(
                    success=False,
                    error_message=audit_error,
                )
            try:
                result = fn(context, config)
                if isinstance(result, dict):
                    audit_output_payload = dict(result)
                    if result.get("paused") is True:
                        audit_status = "paused"
                        _finalize_audit()
                        raise PipelineStepPauseRequired(
                            reason=str(result.get("reason") or "user_interaction"),
                            prompt=str(result.get("prompt") or ""),
                            missing_data_desc=str(result.get("missing_data_desc") or ""),
                            required_doc_types=list(result.get("required_doc_types") or []),
                        )
                    _finalize_audit()
                    return _StepOutcome(success=True, output=result)

                audit_output_payload = {"result": result}
                _finalize_audit()
                return _StepOutcome(success=True, output={"result": result})
            except PipelineStepPauseRequired:
                raise
            except Exception as exc:
                audit_status = "error"
                audit_error = str(exc)
                _finalize_audit()
                return _StepOutcome(success=False, error_message=audit_error)

        python_module = str(step.get("python_module") or "").strip()
        python_function = str(step.get("python_function") or "").strip()

        # Allow concise builtin usage from workflow configs:
        # - step.python_function = "require_fields"
        # - step.config.builtin = "require_fields"
        if not python_function:
            python_function = str(config.get("builtin") or "").strip()
        if not python_module:
            python_module = str(config.get("python_module") or "").strip()
        if not python_module:
            python_module = _BUILTIN_STEP_MODULE

        if not python_function:
            return _StepOutcome(
                success=False,
                error_message="python_binding step missing python_module or python_function",
            )

        try:
            mod = importlib.import_module(python_module)
        except ImportError as exc:
            return _StepOutcome(success=False, error_message=f"Cannot import module '{python_module}': {exc}")

        fn = getattr(mod, python_function, None)
        if fn is None:
            return _StepOutcome(
                success=False,
                error_message=f"Function '{python_function}' not found in module '{python_module}'",
            )

        # PipelineStepPauseRequired raised here is caught by _execute_step wrapper
        result = fn(context, config)

        if isinstance(result, dict):
            # Python functions can also use the dict-signal convention
            if result.get("paused") is True:
                raise PipelineStepPauseRequired(
                    reason=str(result.get("reason") or "user_interaction"),
                    prompt=str(result.get("prompt") or ""),
                    missing_data_desc=str(result.get("missing_data_desc") or ""),
                    required_doc_types=list(result.get("required_doc_types") or []),
                )
            return _StepOutcome(success=True, output=result)

        return _StepOutcome(success=True, output={"result": result})
