"""Semantic pattern library for query pre-parsing and learning."""

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Optional

import psycopg2
from psycopg2.extras import Json
from security_context import LEGACY_TENANT_ID, get_tenant_id

logger = logging.getLogger(__name__)


@dataclass
class SemanticPattern:
    pattern_id: Optional[int]
    pattern_text: str
    semantic_concept: str
    mapped_attributes: dict[str, Any]
    computation_rule: Optional[str]
    entity_class: Optional[str]
    source_type: str
    confidence: float
    pattern_language: str = "en"
    metadata: Optional[dict[str, Any]] = None


class PatternLibrary:
    def __init__(self, connection: psycopg2.extensions.connection):
        self.connection = connection

    @staticmethod
    def _tenant_id() -> str:
        return get_tenant_id()

    def _normalize_for_matching(self, text: str) -> str:
        """Normalize text for tolerant multilingual matching.

        This keeps DB storage untouched but allows queries like "für" to match
        seeded ascii variants like "fur".
        """
        normalized = unicodedata.normalize("NFKC", str(text or "").lower().strip())
        normalized = (
            normalized
            .replace("ä", "a")
            .replace("ö", "o")
            .replace("ü", "u")
            .replace("ß", "ss")
        )
        return " ".join(normalized.split())

    def upsert_solf_clause(
        self,
        clause_name: str,
        clause_type: str,
        clause_body: str,
        entity_class: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        is_active: bool = True,
        created_by: Optional[str] = None,
        configuration_scope: str = "tenant",
        tenant_id: Optional[str] = None,
    ) -> Optional[int]:
        """Insert or update a SOLF clause stored in SQL, supporting private tenant and shared global records."""

        try:
            normalized_scope = str(configuration_scope or "tenant").strip().lower()
            if normalized_scope not in {"tenant", "global"}:
                raise ValueError("configuration_scope must be 'tenant' or 'global'")

            target_tenant_id = str(tenant_id or self._tenant_id()).strip() or LEGACY_TENANT_ID
            if normalized_scope == "global":
                target_tenant_id = LEGACY_TENANT_ID

            check_sql = """
            SELECT clause_id
            FROM solf_clauses
                        WHERE tenant_id = %s::uuid
                            AND configuration_scope = %s
                            AND clause_name = %s
              AND ((entity_class IS NULL AND %s IS NULL) OR entity_class = %s)
            LIMIT 1;
            """
            insert_sql = """
            INSERT INTO solf_clauses (
                tenant_id, configuration_scope, clause_name, clause_type, entity_class, clause_body,
                metadata, is_active, created_by, created_at, modified_at
            ) VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING clause_id;
            """
            update_sql = """
            UPDATE solf_clauses
            SET clause_type = %s,
                clause_body = %s,
                metadata = %s,
                is_active = %s,
                modified_at = NOW()
            WHERE tenant_id = %s::uuid
              AND configuration_scope = %s
              AND clause_id = %s
            RETURNING clause_id;
            """
            with self.connection.cursor() as cursor:
                cursor.execute(check_sql, (target_tenant_id, normalized_scope, clause_name, entity_class, entity_class))
                row = cursor.fetchone()
                if row:
                    clause_id = int(row[0])
                    cursor.execute(
                        update_sql,
                        (
                            clause_type,
                            clause_body,
                            Json(metadata or {}),
                            bool(is_active),
                            target_tenant_id,
                            normalized_scope,
                            clause_id,
                        ),
                    )
                    updated = cursor.fetchone()
                    self.connection.commit()
                    return int(updated[0]) if updated else clause_id

                cursor.execute(
                    insert_sql,
                    (
                        target_tenant_id,
                        normalized_scope,
                        clause_name,
                        clause_type,
                        entity_class,
                        clause_body,
                        Json(metadata or {}),
                        bool(is_active),
                        created_by,
                    ),
                )
                inserted = cursor.fetchone()
                self.connection.commit()
                return int(inserted[0]) if inserted else None
        except Exception as exc:
            logger.error("Failed to upsert SOLF clause: %s", exc)
            self.connection.rollback()
            return None

    def get_active_computation_rule(
        self,
        semantic_concept: str,
        entity_class: Optional[str] = None,
    ) -> Optional[str]:
        """
        Resolve an active computation rule for a semantic concept from SQL-stored SOLF clauses.
        Supports either:
        - clause_name == semantic_concept
        - metadata.semantic_concept == semantic_concept
        """

        try:
            tenant_id = self._tenant_id()
            sql = """
            SELECT clause_body
            FROM solf_clauses
            WHERE clause_type = 'computation_rule'
              AND is_active = TRUE
              AND (tenant_id = %s::uuid OR configuration_scope = 'global')
              AND (
                    LOWER(clause_name) = LOWER(%s)
                    OR LOWER(COALESCE(metadata->>'semantic_concept', '')) = LOWER(%s)
                  )
              AND (%s IS NULL OR entity_class IS NULL OR entity_class = %s)
                        ORDER BY CASE WHEN tenant_id = %s::uuid AND configuration_scope = 'tenant' THEN 0 ELSE 1 END,
                                         CASE WHEN entity_class = %s THEN 0 ELSE 1 END, modified_at DESC
            LIMIT 1;
            """
            with self.connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        tenant_id,
                        semantic_concept,
                        semantic_concept,
                        entity_class,
                        entity_class,
                        tenant_id,
                        entity_class,
                    ),
                )
                row = cursor.fetchone()
            if not row:
                return None
            body = str(row[0] or "").strip()
            if not body:
                return None

            # Accept plain expression or key-value style "compute: <expr>".
            compute_match = re.search(r"(?:^|\n)\s*compute\s*:\s*(.+?)\s*$", body, flags=re.IGNORECASE | re.MULTILINE)
            if compute_match:
                return compute_match.group(1).strip()
            return body
        except Exception as exc:
            logger.error("Failed to get active computation rule: %s", exc)
            return None

    def get_active_computation_clause_body(
        self,
        semantic_concept: str,
        entity_class: Optional[str] = None,
    ) -> Optional[str]:
        """Return the raw active SQL SOLF clause body for a computation semantic concept."""

        try:
            tenant_id = self._tenant_id()
            sql = """
            SELECT clause_body
            FROM solf_clauses
            WHERE clause_type = 'computation_rule'
              AND is_active = TRUE
              AND (tenant_id = %s::uuid OR configuration_scope = 'global')
              AND (
                    LOWER(clause_name) = LOWER(%s)
                    OR LOWER(COALESCE(metadata->>'semantic_concept', '')) = LOWER(%s)
                  )
              AND (%s IS NULL OR entity_class IS NULL OR entity_class = %s)
                        ORDER BY CASE WHEN tenant_id = %s::uuid AND configuration_scope = 'tenant' THEN 0 ELSE 1 END,
                                         CASE WHEN entity_class = %s THEN 0 ELSE 1 END, modified_at DESC
            LIMIT 1;
            """
            with self.connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        tenant_id,
                        semantic_concept,
                        semantic_concept,
                        entity_class,
                        entity_class,
                        tenant_id,
                        entity_class,
                    ),
                )
                row = cursor.fetchone()
            if not row:
                return None
            body = str(row[0] or "").strip()
            return body or None
        except Exception as exc:
            logger.error("Failed to get active computation clause body: %s", exc)
            return None

    def seed_default_solf_clauses(self) -> None:
        """Seed baseline computation rules in SQL-backed SOLF clauses."""

        defaults = [
            {
                "clause_name": "age",
                "entity_class": "person",
                "clause_body": "compute: AGE(birth_date)",
                "metadata": {"semantic_concept": "age", "version": 1},
            },
            {
                "clause_name": "age_at_event",
                "entity_class": "person",
                "clause_body": "compute: AGE_AT(birth_date, event_date)",
                "metadata": {"semantic_concept": "age_at_event", "version": 1},
            },
            {
                "clause_name": "duration",
                "entity_class": None,
                "clause_body": "compute: end_time - start_time",
                "metadata": {"semantic_concept": "duration", "version": 1},
            },
            {
                "clause_name": "travel_duration",
                "entity_class": None,
                "clause_body": "compute: arrival_time - departure_time",
                "metadata": {"semantic_concept": "travel_duration", "version": 1},
            },
            {
                "clause_name": "duration_years",
                "entity_class": None,
                "clause_body": "compute: YEAR_DIFF(end_date, start_date)",
                "metadata": {"semantic_concept": "duration_years", "version": 1},
            },
            {
                "clause_name": "tenure",
                "entity_class": None,
                "clause_body": "compute: tenure_end - tenure_start",
                "metadata": {"semantic_concept": "tenure", "version": 1},
            },
            {
                "clause_name": "crm_opportunity_cycle_time",
                "entity_class": "opportunity",
                "clause_body": "compute: close_date - create_date",
                "metadata": {"semantic_concept": "crm_opportunity_cycle_time", "domain": "crm", "version": 1},
            },
            {
                "clause_name": "crm_lead_response_time",
                "entity_class": "lead",
                "clause_body": "compute: first_response_time - lead_created_time",
                "metadata": {"semantic_concept": "crm_lead_response_time", "domain": "crm", "version": 1},
            },
            {
                "clause_name": "crm_case_resolution_time",
                "entity_class": "case",
                "clause_body": "compute: resolved_time - opened_time",
                "metadata": {"semantic_concept": "crm_case_resolution_time", "domain": "crm", "version": 1},
            },
            {
                "clause_name": "erp_order_cycle_time",
                "entity_class": "sales_order",
                "clause_body": "compute: delivery_date - order_date",
                "metadata": {"semantic_concept": "erp_order_cycle_time", "domain": "erp", "version": 1},
            },
            {
                "clause_name": "erp_procurement_lead_time",
                "entity_class": "purchase_order",
                "clause_body": "compute: goods_receipt_date - po_date",
                "metadata": {"semantic_concept": "erp_procurement_lead_time", "domain": "erp", "version": 1},
            },
            {
                "clause_name": "erp_production_cycle_time",
                "entity_class": "work_order",
                "clause_body": "compute: completion_date - release_date",
                "metadata": {"semantic_concept": "erp_production_cycle_time", "domain": "erp", "version": 1},
            },
            {
                "clause_name": "accounting_invoice_age",
                "entity_class": "invoice",
                "clause_body": "compute: CURRENT_DATE - invoice_date",
                "metadata": {"semantic_concept": "accounting_invoice_age", "domain": "accounting", "version": 1},
            },
            {
                "clause_name": "accounting_days_to_pay",
                "entity_class": "invoice",
                "clause_body": "compute: payment_date - invoice_date",
                "metadata": {"semantic_concept": "accounting_days_to_pay", "domain": "accounting", "version": 1},
            },
            {
                "clause_name": "accounting_days_overdue",
                "entity_class": "invoice",
                "clause_body": "compute: payment_due_date - invoice_date",
                "metadata": {"semantic_concept": "accounting_days_overdue", "domain": "accounting", "version": 1},
            },
            {
                "clause_name": "hr_tenure_days",
                "entity_class": "employee",
                "clause_body": "compute: end_date - hire_date",
                "metadata": {"semantic_concept": "hr_tenure_days", "domain": "hr", "version": 1},
            },
            {
                "clause_name": "hr_time_to_hire",
                "entity_class": "job_requisition",
                "clause_body": "compute: hire_date - requisition_open_date",
                "metadata": {"semantic_concept": "hr_time_to_hire", "domain": "hr", "version": 1},
            },
            {
                "clause_name": "hr_time_to_onboard",
                "entity_class": "employee",
                "clause_body": "compute: onboarding_complete_date - start_date",
                "metadata": {"semantic_concept": "hr_time_to_onboard", "domain": "hr", "version": 1},
            },
            {
                "clause_name": "pm_project_duration",
                "entity_class": "project",
                "clause_body": "compute: actual_end_date - start_date",
                "metadata": {"semantic_concept": "pm_project_duration", "domain": "project_management", "version": 1},
            },
            {
                "clause_name": "pm_task_cycle_time",
                "entity_class": "task",
                "clause_body": "compute: done_date - in_progress_date",
                "metadata": {"semantic_concept": "pm_task_cycle_time", "domain": "project_management", "version": 1},
            },
            {
                "clause_name": "pm_schedule_variance",
                "entity_class": "project",
                "clause_body": "compute: actual_end_date - planned_end_date",
                "metadata": {"semantic_concept": "pm_schedule_variance", "domain": "project_management", "version": 1},
            },
        ]

        for item in defaults:
            self.upsert_solf_clause(
                clause_name=item["clause_name"],
                clause_type="computation_rule",
                clause_body=item["clause_body"],
                entity_class=item["entity_class"],
                metadata=item["metadata"],
                is_active=True,
                created_by="system",
            )

    def seed_initial_patterns(self) -> None:
        initial_patterns = [
            {"pattern_text": "how old", "semantic_concept": "age", "mapped_attributes": {"age": "birth_date"}, "computation_rule": "AGE(birth_date)", "entity_class": "person", "confidence": 1.0},
            {"pattern_text": "how old is *", "semantic_concept": "age", "mapped_attributes": {"age": "birth_date"}, "computation_rule": "AGE(birth_date)", "entity_class": "person", "confidence": 0.95},
            {"pattern_text": "what is the age", "semantic_concept": "age", "mapped_attributes": {"age": "birth_date"}, "computation_rule": "AGE(birth_date)", "entity_class": "person", "confidence": 1.0},
            {"pattern_text": "what age is *", "semantic_concept": "age", "mapped_attributes": {"age": "birth_date"}, "computation_rule": "AGE(birth_date)", "entity_class": "person", "confidence": 0.90},
            {"pattern_text": "how many years old", "semantic_concept": "age", "mapped_attributes": {"age": "birth_date"}, "computation_rule": "AGE(birth_date)", "entity_class": "person", "confidence": 0.95},
            {"pattern_text": "how many years old is *", "semantic_concept": "age", "mapped_attributes": {"age": "birth_date"}, "computation_rule": "AGE(birth_date)", "entity_class": "person", "confidence": 0.95},
            {"pattern_text": "what is * age", "semantic_concept": "age", "mapped_attributes": {"age": "birth_date"}, "computation_rule": "AGE(birth_date)", "entity_class": "person", "confidence": 0.90},
            {"pattern_text": "when was * born", "semantic_concept": "birth_date", "mapped_attributes": {"birth_date": "birth_date"}, "computation_rule": "", "entity_class": "person", "confidence": 0.95},
            {"pattern_text": "where was * born", "semantic_concept": "birth_place", "mapped_attributes": {"birth_place": "birth_place"}, "computation_rule": "", "entity_class": "person", "confidence": 0.95},
            {"pattern_text": "what is * nationality", "semantic_concept": "nationality", "mapped_attributes": {"nationality": "nationality"}, "computation_rule": "", "entity_class": "person", "confidence": 0.90},
            {"pattern_text": "what age is", "semantic_concept": "age", "mapped_attributes": {"age": "birth_date"}, "computation_rule": "AGE(birth_date)", "entity_class": "person", "confidence": 0.90},
            {"pattern_text": "wie alt", "semantic_concept": "age", "mapped_attributes": {"age": "birth_date"}, "computation_rule": "AGE(birth_date)", "entity_class": "person", "confidence": 0.95, "pattern_language": "de"},
            {"pattern_text": "wie alt ist *", "semantic_concept": "age", "mapped_attributes": {"age": "birth_date"}, "computation_rule": "AGE(birth_date)", "entity_class": "person", "confidence": 0.95, "pattern_language": "de"},
            {"pattern_text": "wann wurde * geboren", "semantic_concept": "birth_date", "mapped_attributes": {"birth_date": "birth_date"}, "computation_rule": "", "entity_class": "person", "confidence": 0.90, "pattern_language": "de"},
            {"pattern_text": "wo wurde * geboren", "semantic_concept": "birth_place", "mapped_attributes": {"birth_place": "birth_place"}, "computation_rule": "", "entity_class": "person", "confidence": 0.90, "pattern_language": "de"},
            {"pattern_text": "born in what year", "semantic_concept": "birth_year", "mapped_attributes": {"birth_year": "birth_date"}, "computation_rule": "YEAR(birth_date)", "entity_class": "person", "confidence": 1.0},
            {"pattern_text": "how long", "semantic_concept": "duration", "mapped_attributes": {"duration": ["start_time", "end_time"]}, "computation_rule": "end_time - start_time", "entity_class": None, "confidence": 0.95},
            {"pattern_text": "how long did * take", "semantic_concept": "duration", "mapped_attributes": {"duration": ["start_time", "end_time"]}, "computation_rule": "end_time - start_time", "entity_class": None, "confidence": 0.92},
            {"pattern_text": "what is the duration", "semantic_concept": "duration", "mapped_attributes": {"duration": ["start_time", "end_time"]}, "computation_rule": "end_time - start_time", "entity_class": None, "confidence": 1.0},
            {"pattern_text": "what is the duration of *", "semantic_concept": "duration", "mapped_attributes": {"duration": ["start_time", "end_time"]}, "computation_rule": "end_time - start_time", "entity_class": None, "confidence": 0.95},
            {"pattern_text": "how long did it take", "semantic_concept": "duration", "mapped_attributes": {"duration": ["start_time", "end_time"]}, "computation_rule": "end_time - start_time", "entity_class": None, "confidence": 0.95},
            {"pattern_text": "how long between * and *", "semantic_concept": "duration", "mapped_attributes": {"duration": ["start_time", "end_time"]}, "computation_rule": "end_time - start_time", "entity_class": None, "confidence": 0.88},
            {"pattern_text": "duration of *", "semantic_concept": "duration", "mapped_attributes": {"duration": ["start_time", "end_time"]}, "computation_rule": "end_time - start_time", "entity_class": None, "confidence": 0.90},
            {"pattern_text": "travel time", "semantic_concept": "travel_duration", "mapped_attributes": {"travel_duration": ["departure_time", "arrival_time"]}, "computation_rule": "arrival_time - departure_time", "entity_class": None, "confidence": 1.0},
            {"pattern_text": "travel time from * to *", "semantic_concept": "travel_duration", "mapped_attributes": {"travel_duration": ["departure_time", "arrival_time"]}, "computation_rule": "arrival_time - departure_time", "entity_class": None, "confidence": 0.92},
            {"pattern_text": "how long is the trip", "semantic_concept": "travel_duration", "mapped_attributes": {"travel_duration": ["departure_time", "arrival_time"]}, "computation_rule": "arrival_time - departure_time", "entity_class": None, "confidence": 0.90},
            {"pattern_text": "how long did the journey take", "semantic_concept": "travel_duration", "mapped_attributes": {"travel_duration": ["departure_time", "arrival_time"]}, "computation_rule": "arrival_time - departure_time", "entity_class": None, "confidence": 0.95},
            {"pattern_text": "how long does", "semantic_concept": "tenure", "mapped_attributes": {"tenure": ["tenure_start", "tenure_end"]}, "computation_rule": "tenure_end - tenure_start", "entity_class": None, "confidence": 0.90},
            {"pattern_text": "how long has * been", "semantic_concept": "tenure", "mapped_attributes": {"tenure": ["tenure_start", "tenure_end"]}, "computation_rule": "tenure_end - tenure_start", "entity_class": None, "confidence": 0.88},
            {"pattern_text": "tenure of *", "semantic_concept": "tenure", "mapped_attributes": {"tenure": ["tenure_start", "tenure_end"]}, "computation_rule": "tenure_end - tenure_start", "entity_class": None, "confidence": 0.88},
            {"pattern_text": "how many years", "semantic_concept": "duration_years", "mapped_attributes": {"duration_years": ["start_date", "end_date"]}, "computation_rule": "YEAR_DIFF(end_date, start_date)", "entity_class": None, "confidence": 0.85},
            {"pattern_text": "how many years for *", "semantic_concept": "duration_years", "mapped_attributes": {"duration_years": ["start_date", "end_date"]}, "computation_rule": "YEAR_DIFF(end_date, start_date)", "entity_class": None, "confidence": 0.85},
            {"pattern_text": "for how many years", "semantic_concept": "duration_years", "mapped_attributes": {"duration_years": ["start_date", "end_date"]}, "computation_rule": "YEAR_DIFF(end_date, start_date)", "entity_class": None, "confidence": 0.82},
            {"pattern_text": "how long were together", "semantic_concept": "relationship_duration", "mapped_attributes": {"relationship_duration": ["relationship_start", "relationship_end"]}, "computation_rule": "relationship_end - relationship_start", "entity_class": None, "confidence": 0.90},
            {"pattern_text": "how long were * together", "semantic_concept": "relationship_duration", "mapped_attributes": {"relationship_duration": ["relationship_start", "relationship_end"]}, "computation_rule": "relationship_end - relationship_start", "entity_class": None, "confidence": 0.88},
            {"pattern_text": "relationship duration", "semantic_concept": "relationship_duration", "mapped_attributes": {"relationship_duration": ["relationship_start", "relationship_end"]}, "computation_rule": "relationship_end - relationship_start", "entity_class": None, "confidence": 0.85},
            {"pattern_text": "relationship duration of *", "semantic_concept": "relationship_duration", "mapped_attributes": {"relationship_duration": ["relationship_start", "relationship_end"]}, "computation_rule": "relationship_end - relationship_start", "entity_class": None, "confidence": 0.90},
            {"pattern_text": "how old was", "semantic_concept": "age_at_event", "mapped_attributes": {"age_at_event": ["birth_date", "event_date"]}, "computation_rule": "AGE_AT(birth_date, event_date)", "entity_class": "person", "confidence": 0.95},
            {"pattern_text": "how old was * when *", "semantic_concept": "age_at_event", "mapped_attributes": {"age_at_event": ["birth_date", "event_date"]}, "computation_rule": "AGE_AT(birth_date, event_date)", "entity_class": "person", "confidence": 0.90},
            {"pattern_text": "status of *", "semantic_concept": "status", "mapped_attributes": {"status": "status"}, "computation_rule": "", "entity_class": None, "confidence": 0.88},
            {"pattern_text": "is * active", "semantic_concept": "status", "mapped_attributes": {"status": "status"}, "computation_rule": "", "entity_class": None, "confidence": 0.86},
            {"pattern_text": "registration number of *", "semantic_concept": "registration_no", "mapped_attributes": {"registration_no": "registration_no"}, "computation_rule": "", "entity_class": None, "confidence": 0.88},
            {"pattern_text": "registered address of *", "semantic_concept": "registered_address", "mapped_attributes": {"registered_address": "registered_address"}, "computation_rule": "", "entity_class": None, "confidence": 0.88},

            # CRM domain
            {"pattern_text": "opportunity cycle time of *", "semantic_concept": "crm_opportunity_cycle_time", "mapped_attributes": {"crm_opportunity_cycle_time": ["create_date", "close_date"]}, "computation_rule": "close_date - create_date", "entity_class": "opportunity", "confidence": 0.90},
            {"pattern_text": "sales cycle time for *", "semantic_concept": "crm_opportunity_cycle_time", "mapped_attributes": {"crm_opportunity_cycle_time": ["create_date", "close_date"]}, "computation_rule": "close_date - create_date", "entity_class": "opportunity", "confidence": 0.88},
            {"pattern_text": "time to close opportunity *", "semantic_concept": "crm_opportunity_cycle_time", "mapped_attributes": {"crm_opportunity_cycle_time": ["create_date", "close_date"]}, "computation_rule": "close_date - create_date", "entity_class": "opportunity", "confidence": 0.87},
            {"pattern_text": "opportunity aging for *", "semantic_concept": "crm_opportunity_cycle_time", "mapped_attributes": {"crm_opportunity_cycle_time": ["create_date", "close_date"]}, "computation_rule": "close_date - create_date", "entity_class": "opportunity", "confidence": 0.85},
            {"pattern_text": "lead response time for *", "semantic_concept": "crm_lead_response_time", "mapped_attributes": {"crm_lead_response_time": ["lead_created_time", "first_response_time"]}, "computation_rule": "first_response_time - lead_created_time", "entity_class": "lead", "confidence": 0.90},
            {"pattern_text": "first response time for lead *", "semantic_concept": "crm_lead_response_time", "mapped_attributes": {"crm_lead_response_time": ["lead_created_time", "first_response_time"]}, "computation_rule": "first_response_time - lead_created_time", "entity_class": "lead", "confidence": 0.88},
            {"pattern_text": "lead first touch time for *", "semantic_concept": "crm_lead_response_time", "mapped_attributes": {"crm_lead_response_time": ["lead_created_time", "first_response_time"]}, "computation_rule": "first_response_time - lead_created_time", "entity_class": "lead", "confidence": 0.86},
            {"pattern_text": "how fast did we respond to lead *", "semantic_concept": "crm_lead_response_time", "mapped_attributes": {"crm_lead_response_time": ["lead_created_time", "first_response_time"]}, "computation_rule": "first_response_time - lead_created_time", "entity_class": "lead", "confidence": 0.84},
            {"pattern_text": "case resolution time for *", "semantic_concept": "crm_case_resolution_time", "mapped_attributes": {"crm_case_resolution_time": ["opened_time", "resolved_time"]}, "computation_rule": "resolved_time - opened_time", "entity_class": "case", "confidence": 0.90},
            {"pattern_text": "ticket resolution time for *", "semantic_concept": "crm_case_resolution_time", "mapped_attributes": {"crm_case_resolution_time": ["opened_time", "resolved_time"]}, "computation_rule": "resolved_time - opened_time", "entity_class": "case", "confidence": 0.88},
            {"pattern_text": "support case turnaround for *", "semantic_concept": "crm_case_resolution_time", "mapped_attributes": {"crm_case_resolution_time": ["opened_time", "resolved_time"]}, "computation_rule": "resolved_time - opened_time", "entity_class": "case", "confidence": 0.86},
            {"pattern_text": "time to resolve case *", "semantic_concept": "crm_case_resolution_time", "mapped_attributes": {"crm_case_resolution_time": ["opened_time", "resolved_time"]}, "computation_rule": "resolved_time - opened_time", "entity_class": "case", "confidence": 0.87},

            # ERP domain
            {"pattern_text": "order cycle time for *", "semantic_concept": "erp_order_cycle_time", "mapped_attributes": {"erp_order_cycle_time": ["order_date", "delivery_date"]}, "computation_rule": "delivery_date - order_date", "entity_class": "sales_order", "confidence": 0.90},
            {"pattern_text": "order to delivery time for *", "semantic_concept": "erp_order_cycle_time", "mapped_attributes": {"erp_order_cycle_time": ["order_date", "delivery_date"]}, "computation_rule": "delivery_date - order_date", "entity_class": "sales_order", "confidence": 0.88},
            {"pattern_text": "fulfillment cycle time for order *", "semantic_concept": "erp_order_cycle_time", "mapped_attributes": {"erp_order_cycle_time": ["order_date", "delivery_date"]}, "computation_rule": "delivery_date - order_date", "entity_class": "sales_order", "confidence": 0.87},
            {"pattern_text": "sales order lead time for *", "semantic_concept": "erp_order_cycle_time", "mapped_attributes": {"erp_order_cycle_time": ["order_date", "delivery_date"]}, "computation_rule": "delivery_date - order_date", "entity_class": "sales_order", "confidence": 0.85},
            {"pattern_text": "procurement lead time for *", "semantic_concept": "erp_procurement_lead_time", "mapped_attributes": {"erp_procurement_lead_time": ["po_date", "goods_receipt_date"]}, "computation_rule": "goods_receipt_date - po_date", "entity_class": "purchase_order", "confidence": 0.90},
            {"pattern_text": "purchase order lead time for *", "semantic_concept": "erp_procurement_lead_time", "mapped_attributes": {"erp_procurement_lead_time": ["po_date", "goods_receipt_date"]}, "computation_rule": "goods_receipt_date - po_date", "entity_class": "purchase_order", "confidence": 0.88},
            {"pattern_text": "po to receipt time for *", "semantic_concept": "erp_procurement_lead_time", "mapped_attributes": {"erp_procurement_lead_time": ["po_date", "goods_receipt_date"]}, "computation_rule": "goods_receipt_date - po_date", "entity_class": "purchase_order", "confidence": 0.86},
            {"pattern_text": "vendor lead time for po *", "semantic_concept": "erp_procurement_lead_time", "mapped_attributes": {"erp_procurement_lead_time": ["po_date", "goods_receipt_date"]}, "computation_rule": "goods_receipt_date - po_date", "entity_class": "purchase_order", "confidence": 0.84},
            {"pattern_text": "production cycle time for *", "semantic_concept": "erp_production_cycle_time", "mapped_attributes": {"erp_production_cycle_time": ["release_date", "completion_date"]}, "computation_rule": "completion_date - release_date", "entity_class": "work_order", "confidence": 0.88},
            {"pattern_text": "manufacturing cycle time for *", "semantic_concept": "erp_production_cycle_time", "mapped_attributes": {"erp_production_cycle_time": ["release_date", "completion_date"]}, "computation_rule": "completion_date - release_date", "entity_class": "work_order", "confidence": 0.87},
            {"pattern_text": "work order turnaround time for *", "semantic_concept": "erp_production_cycle_time", "mapped_attributes": {"erp_production_cycle_time": ["release_date", "completion_date"]}, "computation_rule": "completion_date - release_date", "entity_class": "work_order", "confidence": 0.86},
            {"pattern_text": "time to complete work order *", "semantic_concept": "erp_production_cycle_time", "mapped_attributes": {"erp_production_cycle_time": ["release_date", "completion_date"]}, "computation_rule": "completion_date - release_date", "entity_class": "work_order", "confidence": 0.85},

            # Accounting domain
            {"pattern_text": "invoice age for *", "semantic_concept": "accounting_invoice_age", "mapped_attributes": {"accounting_invoice_age": ["invoice_date", "current_date"]}, "computation_rule": "CURRENT_DATE - invoice_date", "entity_class": "invoice", "confidence": 0.90},
            {"pattern_text": "aging of invoice *", "semantic_concept": "accounting_invoice_age", "mapped_attributes": {"accounting_invoice_age": ["invoice_date", "current_date"]}, "computation_rule": "CURRENT_DATE - invoice_date", "entity_class": "invoice", "confidence": 0.88},
            {"pattern_text": "how old is invoice *", "semantic_concept": "accounting_invoice_age", "mapped_attributes": {"accounting_invoice_age": ["invoice_date", "current_date"]}, "computation_rule": "CURRENT_DATE - invoice_date", "entity_class": "invoice", "confidence": 0.87},
            {"pattern_text": "invoice aging days for *", "semantic_concept": "accounting_invoice_age", "mapped_attributes": {"accounting_invoice_age": ["invoice_date", "current_date"]}, "computation_rule": "CURRENT_DATE - invoice_date", "entity_class": "invoice", "confidence": 0.86},
            {"pattern_text": "days to pay for *", "semantic_concept": "accounting_days_to_pay", "mapped_attributes": {"accounting_days_to_pay": ["invoice_date", "payment_date"]}, "computation_rule": "payment_date - invoice_date", "entity_class": "invoice", "confidence": 0.90},
            {"pattern_text": "payment cycle time for invoice *", "semantic_concept": "accounting_days_to_pay", "mapped_attributes": {"accounting_days_to_pay": ["invoice_date", "payment_date"]}, "computation_rule": "payment_date - invoice_date", "entity_class": "invoice", "confidence": 0.88},
            {"pattern_text": "invoice to payment days for *", "semantic_concept": "accounting_days_to_pay", "mapped_attributes": {"accounting_days_to_pay": ["invoice_date", "payment_date"]}, "computation_rule": "payment_date - invoice_date", "entity_class": "invoice", "confidence": 0.86},
            {"pattern_text": "how many days until payment for *", "semantic_concept": "accounting_days_to_pay", "mapped_attributes": {"accounting_days_to_pay": ["invoice_date", "payment_date"]}, "computation_rule": "payment_date - invoice_date", "entity_class": "invoice", "confidence": 0.84},
            {"pattern_text": "within how many days must * be paid", "semantic_concept": "accounting_days_to_pay", "mapped_attributes": {"accounting_days_to_pay": ["invoice_date", "due_date"]}, "computation_rule": "due_date - invoice_date", "entity_class": "invoice", "confidence": 0.92},
            {"pattern_text": "how many days to pay for * to *", "semantic_concept": "accounting_days_to_pay_between_parties", "mapped_attributes": {"accounting_days_to_pay_between_parties": ["invoice_date", "due_date"]}, "computation_rule": "due_date - invoice_date", "entity_class": "invoice", "confidence": 0.93, "relation_filters": [{"relationship_names": ["supplier_ref", "issuer_ref", "creditor_ref", "billed_by"], "target": "$1"}, {"relationship_names": ["customer_ref", "debtor_ref", "billed_to"], "target": "$2"}]},
            {"pattern_text": "days overdue for *", "semantic_concept": "accounting_days_overdue", "mapped_attributes": {"accounting_days_overdue": ["invoice_date", "payment_due_date"]}, "computation_rule": "payment_due_date - invoice_date", "entity_class": "invoice", "confidence": 0.88},
            {"pattern_text": "overdue days for invoice *", "semantic_concept": "accounting_days_overdue", "mapped_attributes": {"accounting_days_overdue": ["invoice_date", "payment_due_date"]}, "computation_rule": "payment_due_date - invoice_date", "entity_class": "invoice", "confidence": 0.87},
            {"pattern_text": "days past due for *", "semantic_concept": "accounting_days_overdue", "mapped_attributes": {"accounting_days_overdue": ["invoice_date", "payment_due_date"]}, "computation_rule": "payment_due_date - invoice_date", "entity_class": "invoice", "confidence": 0.86},
            {"pattern_text": "how late is invoice *", "semantic_concept": "accounting_days_overdue", "mapped_attributes": {"accounting_days_overdue": ["invoice_date", "payment_due_date"]}, "computation_rule": "payment_due_date - invoice_date", "entity_class": "invoice", "confidence": 0.84},
            {"pattern_text": "invoice due date from * to *", "semantic_concept": "invoice_due_date_between_parties", "mapped_attributes": {"invoice_due_date_between_parties": "due_date", "relation_filters": [{"relationship_names": ["supplier_ref", "issuer_ref", "creditor_ref", "billed_by"], "target": "$1"}, {"relationship_names": ["customer_ref", "debtor_ref", "billed_to"], "target": "$2"}], "target_entity_class": "invoice"}, "computation_rule": "", "entity_class": "invoice", "confidence": 0.90},
            {"pattern_text": "what is the invoice due date of the invoice from * to *", "semantic_concept": "invoice_due_date_between_parties", "mapped_attributes": {"invoice_due_date_between_parties": "due_date", "relation_filters": [{"relationship_names": ["supplier_ref", "issuer_ref", "creditor_ref", "billed_by"], "target": "$1"}, {"relationship_names": ["customer_ref", "debtor_ref", "billed_to"], "target": "$2"}], "target_entity_class": "invoice"}, "computation_rule": "", "entity_class": "invoice", "confidence": 0.92},

            # HR domain
            {"pattern_text": "employee tenure of *", "semantic_concept": "hr_tenure_days", "mapped_attributes": {"hr_tenure_days": ["hire_date", "end_date"]}, "computation_rule": "end_date - hire_date", "entity_class": "employee", "confidence": 0.90},
            {"pattern_text": "employee length of service for *", "semantic_concept": "hr_tenure_days", "mapped_attributes": {"hr_tenure_days": ["hire_date", "end_date"]}, "computation_rule": "end_date - hire_date", "entity_class": "employee", "confidence": 0.88},
            {"pattern_text": "service duration of employee *", "semantic_concept": "hr_tenure_days", "mapped_attributes": {"hr_tenure_days": ["hire_date", "end_date"]}, "computation_rule": "end_date - hire_date", "entity_class": "employee", "confidence": 0.86},
            {"pattern_text": "how long has employee * worked", "semantic_concept": "hr_tenure_days", "mapped_attributes": {"hr_tenure_days": ["hire_date", "end_date"]}, "computation_rule": "end_date - hire_date", "entity_class": "employee", "confidence": 0.84},
            {"pattern_text": "time to hire for *", "semantic_concept": "hr_time_to_hire", "mapped_attributes": {"hr_time_to_hire": ["requisition_open_date", "hire_date"]}, "computation_rule": "hire_date - requisition_open_date", "entity_class": "job_requisition", "confidence": 0.90},
            {"pattern_text": "requisition to hire time for *", "semantic_concept": "hr_time_to_hire", "mapped_attributes": {"hr_time_to_hire": ["requisition_open_date", "hire_date"]}, "computation_rule": "hire_date - requisition_open_date", "entity_class": "job_requisition", "confidence": 0.88},
            {"pattern_text": "days to fill position *", "semantic_concept": "hr_time_to_hire", "mapped_attributes": {"hr_time_to_hire": ["requisition_open_date", "hire_date"]}, "computation_rule": "hire_date - requisition_open_date", "entity_class": "job_requisition", "confidence": 0.86},
            {"pattern_text": "time to fill vacancy *", "semantic_concept": "hr_time_to_hire", "mapped_attributes": {"hr_time_to_hire": ["requisition_open_date", "hire_date"]}, "computation_rule": "hire_date - requisition_open_date", "entity_class": "job_requisition", "confidence": 0.85},
            {"pattern_text": "time to onboard for *", "semantic_concept": "hr_time_to_onboard", "mapped_attributes": {"hr_time_to_onboard": ["start_date", "onboarding_complete_date"]}, "computation_rule": "onboarding_complete_date - start_date", "entity_class": "employee", "confidence": 0.88},
            {"pattern_text": "onboarding duration for *", "semantic_concept": "hr_time_to_onboard", "mapped_attributes": {"hr_time_to_onboard": ["start_date", "onboarding_complete_date"]}, "computation_rule": "onboarding_complete_date - start_date", "entity_class": "employee", "confidence": 0.87},
            {"pattern_text": "days to onboard employee *", "semantic_concept": "hr_time_to_onboard", "mapped_attributes": {"hr_time_to_onboard": ["start_date", "onboarding_complete_date"]}, "computation_rule": "onboarding_complete_date - start_date", "entity_class": "employee", "confidence": 0.86},
            {"pattern_text": "new hire ramp up time for *", "semantic_concept": "hr_time_to_onboard", "mapped_attributes": {"hr_time_to_onboard": ["start_date", "onboarding_complete_date"]}, "computation_rule": "onboarding_complete_date - start_date", "entity_class": "employee", "confidence": 0.84},

            # Project Management domain
            {"pattern_text": "project duration of *", "semantic_concept": "pm_project_duration", "mapped_attributes": {"pm_project_duration": ["start_date", "actual_end_date"]}, "computation_rule": "actual_end_date - start_date", "entity_class": "project", "confidence": 0.90},
            {"pattern_text": "project elapsed time for *", "semantic_concept": "pm_project_duration", "mapped_attributes": {"pm_project_duration": ["start_date", "actual_end_date"]}, "computation_rule": "actual_end_date - start_date", "entity_class": "project", "confidence": 0.88},
            {"pattern_text": "actual project timeline for *", "semantic_concept": "pm_project_duration", "mapped_attributes": {"pm_project_duration": ["start_date", "actual_end_date"]}, "computation_rule": "actual_end_date - start_date", "entity_class": "project", "confidence": 0.86},
            {"pattern_text": "how long did project * run", "semantic_concept": "pm_project_duration", "mapped_attributes": {"pm_project_duration": ["start_date", "actual_end_date"]}, "computation_rule": "actual_end_date - start_date", "entity_class": "project", "confidence": 0.85},
            {"pattern_text": "task cycle time for *", "semantic_concept": "pm_task_cycle_time", "mapped_attributes": {"pm_task_cycle_time": ["in_progress_date", "done_date"]}, "computation_rule": "done_date - in_progress_date", "entity_class": "task", "confidence": 0.90},
            {"pattern_text": "task turnaround time for *", "semantic_concept": "pm_task_cycle_time", "mapped_attributes": {"pm_task_cycle_time": ["in_progress_date", "done_date"]}, "computation_rule": "done_date - in_progress_date", "entity_class": "task", "confidence": 0.88},
            {"pattern_text": "time in progress for task *", "semantic_concept": "pm_task_cycle_time", "mapped_attributes": {"pm_task_cycle_time": ["in_progress_date", "done_date"]}, "computation_rule": "done_date - in_progress_date", "entity_class": "task", "confidence": 0.86},
            {"pattern_text": "how long did task * take", "semantic_concept": "pm_task_cycle_time", "mapped_attributes": {"pm_task_cycle_time": ["in_progress_date", "done_date"]}, "computation_rule": "done_date - in_progress_date", "entity_class": "task", "confidence": 0.85},
            {"pattern_text": "schedule variance for *", "semantic_concept": "pm_schedule_variance", "mapped_attributes": {"pm_schedule_variance": ["planned_end_date", "actual_end_date"]}, "computation_rule": "actual_end_date - planned_end_date", "entity_class": "project", "confidence": 0.88},
            {"pattern_text": "project delay for *", "semantic_concept": "pm_schedule_variance", "mapped_attributes": {"pm_schedule_variance": ["planned_end_date", "actual_end_date"]}, "computation_rule": "actual_end_date - planned_end_date", "entity_class": "project", "confidence": 0.87},
            {"pattern_text": "days behind schedule for *", "semantic_concept": "pm_schedule_variance", "mapped_attributes": {"pm_schedule_variance": ["planned_end_date", "actual_end_date"]}, "computation_rule": "actual_end_date - planned_end_date", "entity_class": "project", "confidence": 0.86},
            {"pattern_text": "planned vs actual end gap for *", "semantic_concept": "pm_schedule_variance", "mapped_attributes": {"pm_schedule_variance": ["planned_end_date", "actual_end_date"]}, "computation_rule": "actual_end_date - planned_end_date", "entity_class": "project", "confidence": 0.84},

            # German domain variants
            {"pattern_text": "vertriebszykluszeit fur *", "semantic_concept": "crm_opportunity_cycle_time", "mapped_attributes": {"crm_opportunity_cycle_time": ["create_date", "close_date"]}, "computation_rule": "close_date - create_date", "entity_class": "opportunity", "confidence": 0.86, "pattern_language": "de"},
            {"pattern_text": "reaktionszeit fur lead *", "semantic_concept": "crm_lead_response_time", "mapped_attributes": {"crm_lead_response_time": ["lead_created_time", "first_response_time"]}, "computation_rule": "first_response_time - lead_created_time", "entity_class": "lead", "confidence": 0.86, "pattern_language": "de"},
            {"pattern_text": "bearbeitungszeit fur fall *", "semantic_concept": "crm_case_resolution_time", "mapped_attributes": {"crm_case_resolution_time": ["opened_time", "resolved_time"]}, "computation_rule": "resolved_time - opened_time", "entity_class": "case", "confidence": 0.86, "pattern_language": "de"},
            {"pattern_text": "auftragsdurchlaufzeit fur *", "semantic_concept": "erp_order_cycle_time", "mapped_attributes": {"erp_order_cycle_time": ["order_date", "delivery_date"]}, "computation_rule": "delivery_date - order_date", "entity_class": "sales_order", "confidence": 0.86, "pattern_language": "de"},
            {"pattern_text": "beschaffungszeit fur *", "semantic_concept": "erp_procurement_lead_time", "mapped_attributes": {"erp_procurement_lead_time": ["po_date", "goods_receipt_date"]}, "computation_rule": "goods_receipt_date - po_date", "entity_class": "purchase_order", "confidence": 0.86, "pattern_language": "de"},
            {"pattern_text": "produktionszykluszeit fur *", "semantic_concept": "erp_production_cycle_time", "mapped_attributes": {"erp_production_cycle_time": ["release_date", "completion_date"]}, "computation_rule": "completion_date - release_date", "entity_class": "work_order", "confidence": 0.85, "pattern_language": "de"},
            {"pattern_text": "rechnungsalter fur *", "semantic_concept": "accounting_invoice_age", "mapped_attributes": {"accounting_invoice_age": ["invoice_date", "current_date"]}, "computation_rule": "CURRENT_DATE - invoice_date", "entity_class": "invoice", "confidence": 0.86, "pattern_language": "de"},
            {"pattern_text": "zahlungslaufzeit fur *", "semantic_concept": "accounting_days_to_pay", "mapped_attributes": {"accounting_days_to_pay": ["invoice_date", "payment_date"]}, "computation_rule": "payment_date - invoice_date", "entity_class": "invoice", "confidence": 0.85, "pattern_language": "de"},
            {"pattern_text": "uberfallige tage fur *", "semantic_concept": "accounting_days_overdue", "mapped_attributes": {"accounting_days_overdue": ["invoice_date", "payment_due_date"]}, "computation_rule": "payment_due_date - invoice_date", "entity_class": "invoice", "confidence": 0.85, "pattern_language": "de"},
            {"pattern_text": "falligkeitsdatum der rechnung von * an *", "semantic_concept": "invoice_due_date_between_parties", "mapped_attributes": {"invoice_due_date_between_parties": "due_date", "relation_filters": [{"relationship_names": ["supplier_ref", "issuer_ref", "creditor_ref", "billed_by"], "target": "$1"}, {"relationship_names": ["customer_ref", "debtor_ref", "billed_to"], "target": "$2"}], "target_entity_class": "invoice"}, "computation_rule": "", "entity_class": "invoice", "confidence": 0.90, "pattern_language": "de"},
            {"pattern_text": "betriebszugehorigkeit von *", "semantic_concept": "hr_tenure_days", "mapped_attributes": {"hr_tenure_days": ["hire_date", "end_date"]}, "computation_rule": "end_date - hire_date", "entity_class": "employee", "confidence": 0.86, "pattern_language": "de"},
            {"pattern_text": "zeit bis einstellung fur *", "semantic_concept": "hr_time_to_hire", "mapped_attributes": {"hr_time_to_hire": ["requisition_open_date", "hire_date"]}, "computation_rule": "hire_date - requisition_open_date", "entity_class": "job_requisition", "confidence": 0.85, "pattern_language": "de"},
            {"pattern_text": "onboarding dauer fur *", "semantic_concept": "hr_time_to_onboard", "mapped_attributes": {"hr_time_to_onboard": ["start_date", "onboarding_complete_date"]}, "computation_rule": "onboarding_complete_date - start_date", "entity_class": "employee", "confidence": 0.85, "pattern_language": "de"},
            {"pattern_text": "projektdauer von *", "semantic_concept": "pm_project_duration", "mapped_attributes": {"pm_project_duration": ["start_date", "actual_end_date"]}, "computation_rule": "actual_end_date - start_date", "entity_class": "project", "confidence": 0.86, "pattern_language": "de"},
            {"pattern_text": "durchlaufzeit fur aufgabe *", "semantic_concept": "pm_task_cycle_time", "mapped_attributes": {"pm_task_cycle_time": ["in_progress_date", "done_date"]}, "computation_rule": "done_date - in_progress_date", "entity_class": "task", "confidence": 0.85, "pattern_language": "de"},
            {"pattern_text": "terminabweichung fur *", "semantic_concept": "pm_schedule_variance", "mapped_attributes": {"pm_schedule_variance": ["planned_end_date", "actual_end_date"]}, "computation_rule": "actual_end_date - planned_end_date", "entity_class": "project", "confidence": 0.85, "pattern_language": "de"},
        ]
        for p in initial_patterns:
            self.add_pattern(
                pattern_text=p["pattern_text"],
                semantic_concept=p["semantic_concept"],
                mapped_attributes=p["mapped_attributes"],
                computation_rule=p["computation_rule"],
                entity_class=p["entity_class"],
                source_type="seeded",
                confidence=p["confidence"],
                pattern_language=p.get("pattern_language", "en"),
                created_by="system",
            )

    def add_pattern(
        self,
        pattern_text: str,
        semantic_concept: str,
        mapped_attributes: dict[str, Any],
        computation_rule: Optional[str] = None,
        entity_class: Optional[str] = None,
        source_type: str = "learned",
        confidence: float = 0.80,
        pattern_language: str = "en",
        metadata: Optional[dict[str, Any]] = None,
        created_by: Optional[str] = None,
    ) -> Optional[int]:
        try:
            check_sql = """
            SELECT pattern_id, confidence
            FROM semantic_patterns
                        WHERE tenant_id = %s::uuid
                            AND pattern_text = %s
              AND pattern_language = %s
              AND ((entity_class IS NULL AND %s IS NULL) OR entity_class = %s)
            LIMIT 1;
            """
            insert_sql = """
            INSERT INTO semantic_patterns (
                tenant_id, pattern_text, semantic_concept, mapped_attributes, computation_rule,
                entity_class, source_type, confidence, pattern_language, metadata,
                created_by, created_at, modified_at
            ) VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING pattern_id;
            """
            update_sql = """
            UPDATE semantic_patterns
            SET semantic_concept = %s,
                mapped_attributes = %s,
                computation_rule = %s,
                source_type = %s,
                confidence = %s,
                metadata = %s,
                modified_at = NOW()
            WHERE tenant_id = %s::uuid AND pattern_id = %s
            RETURNING pattern_id;
            """
            pattern_text_normalized = pattern_text.lower()
            semantic_concept_normalized = semantic_concept.lower()
            confidence_norm = float(max(0.0, min(1.0, confidence)))
            with self.connection.cursor() as cursor:
                tenant_id = self._tenant_id()
                cursor.execute(check_sql, (tenant_id, pattern_text_normalized, pattern_language, entity_class, entity_class))
                existing = cursor.fetchone()
                if existing:
                    pattern_id = int(existing[0])
                    current_conf = float(existing[1] or 0.0)
                    cursor.execute(
                        update_sql,
                        (
                            semantic_concept_normalized,
                            Json(mapped_attributes),
                            computation_rule,
                            source_type,
                            max(current_conf, confidence_norm),
                            Json(metadata or {}),
                            tenant_id,
                            pattern_id,
                        ),
                    )
                    row = cursor.fetchone()
                    self.connection.commit()
                    return row[0] if row else pattern_id

                cursor.execute(
                    insert_sql,
                    (
                        tenant_id,
                        pattern_text_normalized,
                        semantic_concept_normalized,
                        Json(mapped_attributes),
                        computation_rule,
                        entity_class,
                        source_type,
                        confidence_norm,
                        pattern_language,
                        Json(metadata or {}),
                        created_by,
                    ),
                )
                row = cursor.fetchone()
                self.connection.commit()
                return row[0] if row else None
        except Exception as exc:
            logger.error("Failed to add pattern: %s", exc)
            self.connection.rollback()
            return None

    def add_synonym(
        self,
        pattern_id: int,
        synonym_text: str,
        language: str = "en",
        semantic_distance: float = 0.0,
        match_type: str = "exact",
    ) -> bool:
        try:
            sql = """
            INSERT INTO pattern_synonyms (
                tenant_id, pattern_id, synonym_text, language, semantic_distance, match_type, created_at
            )
            SELECT %s::uuid, p.pattern_id, %s, %s, %s, %s, NOW()
            FROM semantic_patterns p
            WHERE p.tenant_id = %s::uuid AND p.pattern_id = %s
            ON CONFLICT (tenant_id, pattern_id, synonym_text, language) DO NOTHING;
            """
            with self.connection.cursor() as cursor:
                tenant_id = self._tenant_id()
                cursor.execute(
                    sql,
                    (
                        tenant_id,
                        synonym_text.lower(),
                        language,
                        float(max(0.0, min(1.0, semantic_distance))),
                        match_type,
                        tenant_id,
                        pattern_id,
                    ),
                )
                self.connection.commit()
                return cursor.rowcount > 0
        except Exception as exc:
            logger.error("Failed to add synonym: %s", exc)
            self.connection.rollback()
            return False

    def _row_to_pattern(self, row: tuple[Any, ...], score: float = 1.0) -> SemanticPattern:
        return SemanticPattern(
            pattern_id=row[0],
            pattern_text=row[1],
            semantic_concept=row[2],
            mapped_attributes=row[3] if isinstance(row[3], dict) else {},
            computation_rule=row[4],
            entity_class=row[5],
            source_type=row[6],
            confidence=float(row[7]) * score,
            pattern_language=row[8],
            metadata=row[9] if isinstance(row[9], dict) else {},
        )

    def match_pattern(self, query_text: str, entity_class: Optional[str] = None, language: str = "en") -> Optional[SemanticPattern]:
        text = self._normalize_for_matching(query_text)

        # Guardrail: deterministic pattern DB scans are meant for compact phrase-like
        # queries. For long natural-language questions this path can block parsing
        # under DB contention and should defer to other parsers.
        token_count = len(re.findall(r"\w+", text))
        if len(text) > 120 or token_count > 20:
            return None

        exact = self._match_exact(text, entity_class, language)
        if exact and exact.confidence >= 0.90:
            return exact
        templ = self._match_template(text, entity_class, language)
        if templ and templ.confidence >= 0.85:
            return templ
        regex = self._match_regex(text, entity_class, language)
        if regex and regex.confidence >= 0.80:
            return regex
        # Fallback: return the strongest candidate by confidence, not the first non-null.
        candidates = [item for item in (exact, templ, regex) if item is not None]
        if not candidates:
            return None
        return max(candidates, key=lambda item: float(item.confidence or 0.0))

    def _match_exact(self, query_lower: str, entity_class: Optional[str], language: str) -> Optional[SemanticPattern]:
        try:
            exact_sql = """
            SELECT p.pattern_id, p.pattern_text, p.semantic_concept, p.mapped_attributes,
                   p.computation_rule, p.entity_class, p.source_type, p.confidence,
                   p.pattern_language, p.metadata, 1.0 as match_score
            FROM semantic_patterns p
            WHERE LOWER(p.pattern_text) = %s
                            AND p.tenant_id = %s::uuid
              AND (%s IS NULL OR p.entity_class IS NULL OR p.entity_class = %s)
              AND p.pattern_language = %s
            LIMIT 1;
            """
            with self.connection.cursor() as cursor:
                # Prevent parser stalls if semantic pattern tables are blocked.
                cursor.execute("SET LOCAL lock_timeout = '1200ms';")
                cursor.execute("SET LOCAL statement_timeout = '1800ms';")
                cursor.execute(
                    exact_sql,
                    (
                        query_lower,
                        self._tenant_id(),
                        entity_class,
                        entity_class,
                        language,
                    ),
                )
                row = cursor.fetchone()
            if not row:
                # Best-effort synonym check. Keep this isolated so lock contention on
                # pattern_synonyms cannot break or delay normal exact matching.
                synonym_sql = """
                SELECT p.pattern_id, p.pattern_text, p.semantic_concept, p.mapped_attributes,
                       p.computation_rule, p.entity_class, p.source_type, p.confidence,
                       p.pattern_language, p.metadata, (1.0 - ps.semantic_distance) as match_score
                FROM pattern_synonyms ps
                JOIN semantic_patterns p ON ps.pattern_id = p.pattern_id
                WHERE LOWER(ps.synonym_text) = %s
                                    AND ps.tenant_id = %s::uuid
                                    AND p.tenant_id = ps.tenant_id
                  AND ps.language = %s
                  AND (%s IS NULL OR p.entity_class IS NULL OR p.entity_class = %s)
                ORDER BY match_score DESC, confidence DESC
                LIMIT 1;
                """
                try:
                    with self.connection.cursor() as cursor:
                        cursor.execute("SET LOCAL lock_timeout = '800ms';")
                        cursor.execute("SET LOCAL statement_timeout = '1200ms';")
                        cursor.execute(
                            synonym_sql,
                            (
                                query_lower,
                                self._tenant_id(),
                                language,
                                entity_class,
                                entity_class,
                            ),
                        )
                        row = cursor.fetchone()
                except Exception:
                    try:
                        self.connection.rollback()
                    except Exception:
                        pass

            if not row:
                # Guardrail: full fallback scan is only useful for short, nearly-exact
                # phrasing. For long natural-language questions, this path can become
                # expensive and block query parsing.
                query_token_count = len(re.findall(r"\w+", query_lower))
                if len(query_lower) > 120 or query_token_count > 20:
                    return None

                # Fallback: tolerant exact matching over normalized text (umlauts/spacing).
                scan_sql = """
                SELECT p.pattern_id, p.pattern_text, p.semantic_concept, p.mapped_attributes,
                       p.computation_rule, p.entity_class, p.source_type, p.confidence,
                       p.pattern_language, p.metadata, 1.0 as match_score
                FROM semantic_patterns p
                WHERE (%s IS NULL OR p.entity_class IS NULL OR p.entity_class = %s)
                                    AND p.tenant_id = %s::uuid
                  AND p.pattern_language = %s
                ORDER BY match_score DESC, confidence DESC;
                """
                with self.connection.cursor() as cursor:
                    cursor.execute("SET LOCAL lock_timeout = '1200ms';")
                    cursor.execute("SET LOCAL statement_timeout = '1800ms';")
                    cursor.execute(
                        scan_sql,
                        (
                            entity_class,
                            entity_class,
                            self._tenant_id(),
                            language,
                        ),
                    )
                    rows = cursor.fetchall()

                for scan_row in rows:
                    source_text = scan_row[1]
                    normalized_source = self._normalize_for_matching(source_text)
                    if normalized_source == query_lower:
                        return self._row_to_pattern(scan_row, float(scan_row[10]))
                return None
            return self._row_to_pattern(row, float(row[10]))
        except Exception as exc:
            logger.error("Exact match failed: %s", exc)
            try:
                self.connection.rollback()
            except Exception:
                pass
            return None

    def _match_template(self, query_lower: str, entity_class: Optional[str], language: str) -> Optional[SemanticPattern]:
        try:
            sql = """
            SELECT pattern_id, pattern_text, semantic_concept, mapped_attributes,
                   computation_rule, entity_class, source_type, confidence,
                   pattern_language, metadata
            FROM semantic_patterns
            WHERE (%s IS NULL OR entity_class IS NULL OR entity_class = %s)
                            AND tenant_id = %s::uuid
              AND pattern_language = %s
              AND pattern_text LIKE '%%*%%'
            ORDER BY confidence DESC, created_at DESC;
            """
            with self.connection.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '900ms';")
                cursor.execute("SET LOCAL statement_timeout = '1400ms';")
                cursor.execute(sql, (entity_class, entity_class, self._tenant_id(), language))
                rows = cursor.fetchall()
            for row in rows:
                if self._matches_template(query_lower, row[1]):
                    # Keep template confidence intact so specific wildcard patterns
                    # (e.g. "how long has * been") can outrank generic exact phrases.
                    return self._row_to_pattern(row, 1.0)

            synonym_sql = """
            SELECT p.pattern_id, p.pattern_text, p.semantic_concept, p.mapped_attributes,
                   p.computation_rule, p.entity_class, p.source_type, p.confidence,
                   p.pattern_language, p.metadata, ps.semantic_distance, ps.synonym_text
            FROM pattern_synonyms ps
            JOIN semantic_patterns p ON p.tenant_id = ps.tenant_id AND p.pattern_id = ps.pattern_id
            WHERE ps.tenant_id = %s::uuid
              AND ps.language = %s
              AND ps.match_type = 'template'
              AND ps.synonym_text LIKE '%%*%%'
              AND (%s IS NULL OR p.entity_class IS NULL OR p.entity_class = %s)
            ORDER BY p.confidence DESC, ps.semantic_distance ASC, p.created_at DESC
            """
            with self.connection.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '900ms';")
                cursor.execute("SET LOCAL statement_timeout = '1400ms';")
                cursor.execute(synonym_sql, (self._tenant_id(), language, entity_class, entity_class))
                synonym_rows = cursor.fetchall()
            for row in synonym_rows:
                if self._matches_template(query_lower, row[11]):
                    return self._row_to_pattern(row[:10], 1.0 - float(row[10] or 0.0))
            return None
        except Exception as exc:
            logger.error("Template match failed: %s", exc)
            try:
                self.connection.rollback()
            except Exception:
                pass
            return None

    def _match_regex(self, query_lower: str, entity_class: Optional[str], language: str) -> Optional[SemanticPattern]:
        try:
            sql = """
            SELECT pattern_id, pattern_text, semantic_concept, mapped_attributes,
                   computation_rule, entity_class, source_type, confidence,
                   pattern_language, metadata
            FROM semantic_patterns
            WHERE (%s IS NULL OR entity_class IS NULL OR entity_class = %s)
                            AND tenant_id = %s::uuid
              AND pattern_language = %s
            ORDER BY confidence DESC, created_at DESC
            LIMIT 20;
            """
            with self.connection.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '900ms';")
                cursor.execute("SET LOCAL statement_timeout = '1400ms';")
                cursor.execute(sql, (entity_class, entity_class, self._tenant_id(), language))
                rows = cursor.fetchall()

            best = None
            best_score = 0.5
            for row in rows:
                regex_pattern = re.escape(self._normalize_for_matching(row[1])).replace(r"\ ", r"\s+").replace(r"\*", r".*?")
                if re.search(regex_pattern, query_lower):
                    score = float(row[7])
                    if score > best_score:
                        best_score = score
                        best = (row, 0.85)

            if not best:
                return None
            row, factor = best
            return self._row_to_pattern(row, factor)
        except Exception as exc:
            logger.error("Regex match failed: %s", exc)
            try:
                self.connection.rollback()
            except Exception:
                pass
            return None

    def _matches_template(self, text: str, template: str) -> bool:
        regex = re.escape(self._normalize_for_matching(template)).replace(r"\ ", r"\s+").replace(r"\*", r".*?")
        return re.search(f"^{regex}$", text) is not None

    def learn_pattern(
        self,
        query_text: str,
        semantic_concept: str,
        mapped_attributes: dict[str, Any],
        entity_class: Optional[str] = None,
        computation_rule: Optional[str] = None,
        confidence: float = 0.80,
    ) -> Optional[int]:
        if confidence < 0.80:
            return None
        existing = self.match_pattern(query_text, entity_class)
        if existing and existing.confidence >= confidence:
            return existing.pattern_id
        return self.add_pattern(
            pattern_text=query_text,
            semantic_concept=semantic_concept,
            mapped_attributes=mapped_attributes,
            computation_rule=computation_rule,
            entity_class=entity_class,
            source_type="learned",
            confidence=confidence,
            created_by="system:llm_extractor",
        )

    def get_pattern(self, pattern_id: int) -> Optional[SemanticPattern]:
        try:
            sql = """
            SELECT pattern_id, pattern_text, semantic_concept, mapped_attributes,
                   computation_rule, entity_class, source_type, confidence,
                   pattern_language, metadata
            FROM semantic_patterns
            WHERE pattern_id = %s AND tenant_id = %s::uuid;
            """
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (pattern_id, self._tenant_id()))
                row = cursor.fetchone()
            return self._row_to_pattern(row) if row else None
        except Exception as exc:
            logger.error("Failed to get pattern: %s", exc)
            return None

    def list_patterns(
        self,
        entity_class: Optional[str] = None,
        language: str = "en",
        source_type: Optional[str] = None,
    ) -> list[SemanticPattern]:
        try:
            sql = """
            SELECT pattern_id, pattern_text, semantic_concept, mapped_attributes,
                   computation_rule, entity_class, source_type, confidence,
                   pattern_language, metadata
            FROM semantic_patterns
            WHERE (%s IS NULL OR entity_class IS NULL OR entity_class = %s)
                            AND tenant_id = %s::uuid
              AND pattern_language = %s
              AND (%s IS NULL OR source_type = %s)
            ORDER BY confidence DESC, created_at DESC;
            """
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (entity_class, entity_class, self._tenant_id(), language, source_type, source_type))
                rows = cursor.fetchall()
            return [self._row_to_pattern(r) for r in rows]
        except Exception as exc:
            logger.error("Failed to list patterns: %s", exc)
            return []
