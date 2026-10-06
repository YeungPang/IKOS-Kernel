"""Audit and compliance reporting for workflow automation (Phase 5)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


class AuditComplianceManager:
    """Manages audit trails, compliance records, and generates compliance reports."""

    def __init__(self, db_connection_fn, reliability_manager=None):
        self.db_connection_fn = db_connection_fn
        self.reliability_manager = reliability_manager
        self.ensure_tables()

    def ensure_tables(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS audit_trail (
            id BIGSERIAL PRIMARY KEY,
            entity_type TEXT NOT NULL,
            entity_id BIGINT,
            action TEXT NOT NULL,
            actor_ref TEXT NOT NULL,
            actor_role TEXT,
            changes JSONB NOT NULL DEFAULT '{}'::jsonb,
            reason TEXT,
            ip_address TEXT,
            user_agent TEXT,
            status TEXT NOT NULL DEFAULT 'success',
            error_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS compliance_record (
            id BIGSERIAL PRIMARY KEY,
            compliance_type TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id BIGINT,
            status TEXT NOT NULL DEFAULT 'open',
            severity TEXT NOT NULL DEFAULT 'info',
            requirement_ref TEXT NOT NULL,
            findings JSONB NOT NULL DEFAULT '[]'::jsonb,
            remediation_plan TEXT,
            remediation_due_at TIMESTAMPTZ,
            remediated_at TIMESTAMPTZ,
            certifier_ref TEXT,
            certification_date TIMESTAMPTZ,
            comments TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(compliance_type, requirement_ref, entity_id)
        );

        CREATE TABLE IF NOT EXISTS compliance_decision_trail (
            id BIGSERIAL PRIMARY KEY,
            decision_id BIGINT NOT NULL,
            decision_type TEXT NOT NULL,
            decision_date TIMESTAMPTZ NOT NULL,
            decided_by_ref TEXT NOT NULL,
            decided_by_role TEXT,
            for_entity_type TEXT NOT NULL,
            for_entity_id BIGINT,
            decision_reason TEXT,
            decision_outcome TEXT NOT NULL,
            approvals_required INT NOT NULL DEFAULT 1,
            approvals_received INT NOT NULL DEFAULT 0,
            approval_chain JSONB NOT NULL DEFAULT '[]'::jsonb,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS compliance_policy (
            id BIGSERIAL PRIMARY KEY,
            policy_name TEXT NOT NULL UNIQUE,
            policy_type TEXT NOT NULL,
            description TEXT,
            entity_type TEXT NOT NULL,
            scope TEXT NOT NULL DEFAULT 'global',
            requirements TEXT NOT NULL,
            enforcement_level TEXT NOT NULL DEFAULT 'mandatory',
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_by_ref TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_audit_trail_entity ON audit_trail(entity_type, entity_id);
        CREATE INDEX IF NOT EXISTS idx_audit_trail_created ON audit_trail(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_trail_actor ON audit_trail(actor_ref);
        CREATE INDEX IF NOT EXISTS idx_compliance_record_status ON compliance_record(status);
        CREATE INDEX IF NOT EXISTS idx_compliance_record_type ON compliance_record(compliance_type);
        CREATE INDEX IF NOT EXISTS idx_compliance_decision_status ON compliance_decision_trail(status);
        """
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)

    def record_audit_event(
        self,
        entity_type: str,
        action: str,
        actor_ref: str,
        entity_id: int | None = None,
        changes: dict[str, Any] | None = None,
        reason: str | None = None,
        status: str = "success",
        error_message: str | None = None,
    ) -> dict[str, Any]:
        """Record an audit event for compliance tracking."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO audit_trail (
                        entity_type, entity_id, action, actor_ref, changes, reason, status, error_message
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                    RETURNING id, created_at
                    """,
                    (
                        entity_type,
                        entity_id,
                        action,
                        actor_ref,
                        json.dumps(changes or {}),
                        reason,
                        status,
                        error_message,
                    ),
                )
                row = cur.fetchone()

        return {
            "audit_id": int(row[0]) if row else 0,
            "entity_type": entity_type,
            "action": action,
            "actor_ref": actor_ref,
            "recorded_at": row[1].isoformat() if row else None,
        }

    def create_compliance_record(
        self,
        compliance_type: str,
        entity_type: str,
        requirement_ref: str,
        entity_id: int | None = None,
        severity: str = "info",
        findings: list[str] | None = None,
    ) -> dict[str, Any]:
        """Create a compliance record for an entity."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO compliance_record (
                        compliance_type, entity_type, entity_id, requirement_ref, severity, findings, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, 'open')
                    ON CONFLICT (compliance_type, requirement_ref, entity_id)
                    DO UPDATE SET findings=%s::jsonb, status='open', updated_at=NOW()
                    RETURNING id, created_at
                    """,
                    (
                        compliance_type,
                        entity_type,
                        entity_id,
                        requirement_ref,
                        severity,
                        json.dumps(findings or []),
                        json.dumps(findings or []),
                    ),
                )
                row = cur.fetchone()

        return {
            "record_id": int(row[0]) if row else 0,
            "compliance_type": compliance_type,
            "requirement_ref": requirement_ref,
            "created_at": row[1].isoformat() if row else None,
        }

    def remediate_compliance_issue(
        self,
        record_id: int,
        remediation_plan: str,
        actor_ref: str,
        remediation_due_at: datetime | None = None,
    ) -> dict[str, Any]:
        """Log remediation plan for a compliance issue."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE compliance_record
                    SET remediation_plan=%s,
                        remediation_due_at=%s,
                        updated_at=NOW()
                    WHERE id=%s
                    RETURNING id, status
                    """,
                    (remediation_plan, remediation_due_at, record_id),
                )
                row = cur.fetchone()

        # Record audit event
        self.record_audit_event(
            entity_type="compliance_record",
            action="remediation_plan_set",
            actor_ref=actor_ref,
            entity_id=record_id,
            reason="Remediation plan submitted",
        )

        return {
            "record_id": int(row[0]) if row else 0,
            "status": str(row[1]) if row else "unknown",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def mark_compliance_remediated(
        self,
        record_id: int,
        actor_ref: str,
        certifier_ref: str | None = None,
    ) -> dict[str, Any]:
        """Mark compliance issue as remediated."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE compliance_record
                    SET status='resolved',
                        remediated_at=NOW(),
                        certifier_ref=%s,
                        certification_date=NOW(),
                        updated_at=NOW()
                    WHERE id=%s
                    RETURNING id, status, remediated_at
                    """,
                    (certifier_ref or actor_ref, record_id),
                )
                row = cur.fetchone()

        # Record audit event
        self.record_audit_event(
            entity_type="compliance_record",
            action="remediation_certified",
            actor_ref=certifier_ref or actor_ref,
            entity_id=record_id,
            reason="Issue remediated and certified",
        )

        return {
            "record_id": int(row[0]) if row else 0,
            "status": str(row[1]) if row else "unknown",
            "remediated_at": row[2].isoformat() if row else None,
        }

    def get_audit_trail(
        self,
        entity_type: str | None = None,
        entity_id: int | None = None,
        actor_ref: str | None = None,
        hours: int = 168,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Retrieve audit trail for compliance review."""
        filters = []
        params = []

        if entity_type:
            filters.append("entity_type=%s")
            params.append(entity_type)
        if entity_id:
            filters.append("entity_id=%s")
            params.append(entity_id)
        if actor_ref:
            filters.append("actor_ref=%s")
            params.append(actor_ref)

        filters.append("created_at >= NOW() - (%s * INTERVAL '1 hour')")
        params.append(hours)

        where_clause = " AND ".join(filters)
        query = f"""
            SELECT id, entity_type, entity_id, action, actor_ref, status, created_at
            FROM audit_trail
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT %s
        """
        params.append(limit)

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall() or []

        events = [
            {
                "audit_id": int(row[0]),
                "entity_type": str(row[1]),
                "entity_id": int(row[2]) if row[2] else None,
                "action": str(row[3]),
                "actor_ref": str(row[4]),
                "status": str(row[5]),
                "created_at": row[6].isoformat() if row[6] else None,
            }
            for row in rows
        ]

        return {
            "events": events,
            "count": len(events),
            "hours": hours,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_compliance_status_by_type(self, compliance_type: str) -> dict[str, Any]:
        """Get compliance status summary by type."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, COUNT(*) as count
                    FROM compliance_record
                    WHERE compliance_type=%s
                    GROUP BY status
                    """,
                    (compliance_type,),
                )
                rows = cur.fetchall() or []

                cur.execute(
                    """
                    SELECT COUNT(*) as total
                    FROM compliance_record
                    WHERE compliance_type=%s
                    """,
                    (compliance_type,),
                )
                total_row = cur.fetchone()

        summary = {
            "compliance_type": compliance_type,
            "by_status": {},
            "total": int(total_row[0]) if total_row else 0,
        }

        for status, count in rows:
            summary["by_status"][str(status)] = int(count)

        return summary

    def generate_compliance_report(
        self, compliance_type: str, start_date: datetime | None = None, end_date: datetime | None = None
    ) -> dict[str, Any]:
        """Generate compliance report for period."""
        if not start_date:
            start_date = datetime.now(timezone.utc).replace(day=1)
        if not end_date:
            end_date = datetime.now(timezone.utc)

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT requirement_ref, status, COUNT(*) as count, severity
                    FROM compliance_record
                    WHERE compliance_type=%s
                      AND created_at >= %s
                      AND created_at <= %s
                    GROUP BY requirement_ref, status, severity
                    ORDER BY severity DESC, requirement_ref ASC
                    """,
                    (compliance_type, start_date, end_date),
                )
                rows = cur.fetchall() or []

                cur.execute(
                    """
                    SELECT COUNT(*) as count
                    FROM compliance_record
                    WHERE compliance_type=%s
                      AND status='resolved'
                      AND remediated_at >= %s
                      AND remediated_at <= %s
                    """,
                    (compliance_type, start_date, end_date),
                )
                remediated_row = cur.fetchone()

        summary_by_requirement = {}
        for req_ref, status, count, severity in rows:
            key = str(req_ref)
            if key not in summary_by_requirement:
                summary_by_requirement[key] = {
                    "requirement": key,
                    "severity": str(severity),
                    "by_status": {},
                    "total": 0,
                }
            summary_by_requirement[key]["by_status"][str(status)] = int(count)
            summary_by_requirement[key]["total"] += int(count)

        remediated_count = int(remediated_row[0]) if remediated_row else 0

        return {
            "compliance_type": compliance_type,
            "period_start": start_date.isoformat(),
            "period_end": end_date.isoformat(),
            "total_requirements": len(summary_by_requirement),
            "total_records": sum(r["total"] for r in summary_by_requirement.values()),
            "remediated_count": remediated_count,
            "requirements": list(summary_by_requirement.values()),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def record_decision(
        self,
        decision_type: str,
        decided_by_ref: str,
        for_entity_type: str,
        for_entity_id: int,
        decision_outcome: str,
        decision_reason: str | None = None,
        approvals_required: int = 1,
    ) -> dict[str, Any]:
        """Record an important decision in audit trail."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH next_decision_id AS (
                        SELECT nextval(pg_get_serial_sequence('compliance_decision_trail', 'id')) AS decision_id
                    )
                    INSERT INTO compliance_decision_trail (
                        id, decision_id, decision_type, decision_date, decided_by_ref,
                        for_entity_type, for_entity_id, decision_outcome,
                        decision_reason, approvals_required, status
                    )
                    SELECT
                        decision_id, decision_id, %s, NOW(), %s,
                        %s, %s, %s,
                        %s, %s, 'pending'
                    FROM next_decision_id
                    RETURNING id, decision_date
                    """,
                    (
                        decision_type,
                        decided_by_ref,
                        for_entity_type,
                        for_entity_id,
                        decision_outcome,
                        decision_reason,
                        approvals_required,
                    ),
                )
                row = cur.fetchone()

        return {
            "decision_id": int(row[0]) if row else 0,
            "decision_type": decision_type,
            "status": "pending",
            "decision_date": row[1].isoformat() if row else None,
        }

    def get_decision_trail(
        self, for_entity_type: str, for_entity_id: int
    ) -> dict[str, Any]:
        """Retrieve decision trail for an entity."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, decision_type, decided_by_ref, decision_outcome,
                           decision_reason, status, decision_date
                    FROM compliance_decision_trail
                    WHERE for_entity_type=%s AND for_entity_id=%s
                    ORDER BY decision_date DESC
                    LIMIT 50
                    """,
                    (for_entity_type, for_entity_id),
                )
                rows = cur.fetchall() or []

        decisions = [
            {
                "decision_id": int(row[0]),
                "type": str(row[1]),
                "decided_by": str(row[2]),
                "outcome": str(row[3]),
                "reason": row[4],
                "status": str(row[5]),
                "date": row[6].isoformat() if row[6] else None,
            }
            for row in rows
        ]

        return {
            "entity_type": for_entity_type,
            "entity_id": for_entity_id,
            "decisions": decisions,
            "count": len(decisions),
        }
