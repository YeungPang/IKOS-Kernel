"""Reliability automation for workflow monitoring (Phase 4)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json


@dataclass(frozen=True)
class AlertSignal:
    metric_key: str
    label: str
    warning_threshold: float
    critical_threshold: float


DEFAULT_ALERT_SIGNALS = [
    # Overdue entities (Phase 4 baseline)
    AlertSignal("overdue.project_tasks", "Overdue project tasks", 1.0, 10.0),
    AlertSignal("overdue.compliance_actions", "Overdue compliance actions", 1.0, 5.0),
    AlertSignal("overdue.workflow_cases", "Overdue workflow cases", 1.0, 25.0),
    
    # Denied operations (Phase 4 baseline)
    AlertSignal("events.denied_ratio", "Denied event ratio", 0.10, 0.50),
    AlertSignal("transitions.denied_ratio", "Denied transition ratio", 0.25, 0.50),
    
    # Project metrics (Enhanced Phase 4)
    AlertSignal("project.resource_conflicts", "Active resource conflicts", 1.0, 5.0),
    AlertSignal("project.budget_overrun_count", "Projects exceeding budget", 0.0, 3.0),
    AlertSignal("project.milestone_delays", "Delayed project milestones", 1.0, 5.0),
    
    # Compliance metrics (Enhanced Phase 4)
    AlertSignal("compliance.escalated_count", "Escalated compliance actions", 0.0, 5.0),
    AlertSignal("compliance.incomplete_ratio", "Incomplete compliance ratio", 0.20, 0.50),
    
    # SLA metrics (Enhanced Phase 4)
    AlertSignal("sla.case_breach_ratio", "Workflow case SLA breach ratio", 0.05, 0.20),
    AlertSignal("sla.approval_delay_count", "Delayed approvals", 2.0, 10.0),
    AlertSignal("sla.approval_escalations", "Escalated approvals", 0.0, 5.0),
    
    # System health metrics (Enhanced Phase 4)
    AlertSignal("system.error_rate", "System error rate", 0.05, 0.20),
    AlertSignal("system.event_backlog", "Pending event count", 100.0, 500.0),
    AlertSignal("system.processing_delay_ms", "Avg processing delay (ms)", 5000.0, 15000.0),
]


class WorkflowReliabilityManager:
    """Evaluates alert policies and manages incident lifecycle records."""

    def __init__(self, db_connection_fn, monitor, event_bus=None):
        self.db_connection_fn = db_connection_fn
        self.monitor = monitor
        self.event_bus = event_bus
        self.ensure_tables()
        self.ensure_default_policies()

    def ensure_tables(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS workflow_alert_policies (
            metric_key TEXT PRIMARY KEY,
            metric_label TEXT NOT NULL,
            warning_threshold DOUBLE PRECISION NOT NULL,
            critical_threshold DOUBLE PRECISION NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS workflow_alert_incidents (
            id BIGSERIAL PRIMARY KEY,
            incident_key TEXT NOT NULL,
            metric_key TEXT NOT NULL,
            severity TEXT NOT NULL,
            title TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            current_value DOUBLE PRECISION,
            threshold_value DOUBLE PRECISION,
            occurrence_count INT NOT NULL DEFAULT 1,
            details JSONB NOT NULL DEFAULT '{}'::jsonb,
            opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            acknowledged_at TIMESTAMPTZ,
            resolved_at TIMESTAMPTZ,
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(incident_key, status)
        );

        CREATE INDEX IF NOT EXISTS idx_workflow_alert_incidents_status ON workflow_alert_incidents(status);
        CREATE INDEX IF NOT EXISTS idx_workflow_alert_incidents_metric ON workflow_alert_incidents(metric_key);
        CREATE INDEX IF NOT EXISTS idx_workflow_alert_incidents_last_seen ON workflow_alert_incidents(last_seen_at);
        """
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)

    def ensure_default_policies(self) -> None:
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                for signal in DEFAULT_ALERT_SIGNALS:
                    cur.execute(
                        """
                        INSERT INTO workflow_alert_policies (
                            metric_key,
                            metric_label,
                            warning_threshold,
                            critical_threshold,
                            enabled
                        )
                        VALUES (%s, %s, %s, %s, TRUE)
                        ON CONFLICT (metric_key)
                        DO NOTHING
                        """,
                        (
                            signal.metric_key,
                            signal.label,
                            float(signal.warning_threshold),
                            float(signal.critical_threshold),
                        ),
                    )

    def _metric_value(self, metrics: dict[str, Any], metric_key: str) -> float:
        # Overdue entities
        if metric_key == "overdue.project_tasks":
            return float((metrics.get("overdue") or {}).get("project_tasks") or 0)
        if metric_key == "overdue.compliance_actions":
            return float((metrics.get("overdue") or {}).get("compliance_actions") or 0)
        if metric_key == "overdue.workflow_cases":
            return float((metrics.get("overdue") or {}).get("workflow_cases") or 0)
        
        # Denied operations
        if metric_key == "events.denied_ratio":
            return float((metrics.get("events") or {}).get("denied_ratio") or 0.0)
        if metric_key == "transitions.denied_ratio":
            return float((metrics.get("transitions") or {}).get("denied_ratio") or 0.0)
        
        # Project metrics
        if metric_key == "project.resource_conflicts":
            return float((metrics.get("project") or {}).get("resource_conflicts") or 0)
        if metric_key == "project.budget_overrun_count":
            return float((metrics.get("project") or {}).get("budget_overrun_count") or 0)
        if metric_key == "project.milestone_delays":
            return float((metrics.get("project") or {}).get("milestone_delays") or 0)
        
        # Compliance metrics
        if metric_key == "compliance.escalated_count":
            return float((metrics.get("compliance") or {}).get("escalated_count") or 0)
        if metric_key == "compliance.incomplete_ratio":
            return float((metrics.get("compliance") or {}).get("incomplete_ratio") or 0.0)
        
        # SLA metrics
        if metric_key == "sla.case_breach_ratio":
            return float((metrics.get("sla") or {}).get("case_breach_ratio") or 0.0)
        if metric_key == "sla.approval_delay_count":
            return float((metrics.get("sla") or {}).get("approval_delay_count") or 0)
        if metric_key == "sla.approval_escalations":
            return float((metrics.get("sla") or {}).get("approval_escalations") or 0)
        
        # System health metrics
        if metric_key == "system.error_rate":
            return float((metrics.get("system") or {}).get("error_rate") or 0.0)
        if metric_key == "system.event_backlog":
            return float((metrics.get("system") or {}).get("event_backlog") or 0)
        if metric_key == "system.processing_delay_ms":
            return float((metrics.get("system") or {}).get("processing_delay_ms") or 0.0)
        
        return 0.0

    def _load_policies(self) -> list[dict[str, Any]]:
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT metric_key, metric_label, warning_threshold, critical_threshold, enabled
                    FROM workflow_alert_policies
                    ORDER BY metric_key ASC
                    """
                )
                rows = cur.fetchall() or []

        return [
            {
                "metric_key": str(row[0]),
                "metric_label": str(row[1]),
                "warning_threshold": float(row[2]),
                "critical_threshold": float(row[3]),
                "enabled": bool(row[4]),
            }
            for row in rows
        ]

    def evaluate_alerts(self, window_hours: int = 24) -> dict[str, Any]:
        metrics = self.monitor.collect_metrics(window_hours=window_hours)
        policies = self._load_policies()
        findings: list[dict[str, Any]] = []
        highest = "healthy"

        for policy in policies:
            if not policy.get("enabled"):
                continue
            key = str(policy["metric_key"])
            value = self._metric_value(metrics, key)
            warn = float(policy["warning_threshold"])
            crit = float(policy["critical_threshold"])

            severity = "healthy"
            threshold = 0.0
            if value >= crit:
                severity = "critical"
                threshold = crit
            elif value >= warn:
                severity = "warning"
                threshold = warn

            if severity == "critical":
                highest = "critical"
            elif severity == "warning" and highest == "healthy":
                highest = "warning"

            findings.append(
                {
                    "metric_key": key,
                    "metric_label": str(policy["metric_label"]),
                    "value": value,
                    "warning_threshold": warn,
                    "critical_threshold": crit,
                    "severity": severity,
                    "threshold_value": threshold,
                    "breached": severity in {"warning", "critical"},
                }
            )

        return {
            "window_hours": int(window_hours),
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "status": highest,
            "metrics": metrics,
            "findings": findings,
        }

    def _emit_incident_event(self, event_type: str, incident: dict[str, Any], actor_ref: str) -> None:
        if self.event_bus is None:
            return
        try:
            self.event_bus.emit(
                event_type=event_type,
                source_entity="workflow_alert_incidents",
                entity_id=int(incident.get("id") or 0),
                payload={
                    "incident_key": incident.get("incident_key"),
                    "metric_key": incident.get("metric_key"),
                    "severity": incident.get("severity"),
                    "status": incident.get("status"),
                    "title": incident.get("title"),
                    "actor_ref": actor_ref,
                },
                tags=["phase4", "reliability", "alerts"],
            )
        except Exception:
            # Event publication should not block incident persistence.
            pass

    def _upsert_open_incident(self, finding: dict[str, Any], actor_ref: str) -> dict[str, Any]:
        incident_key = str(finding.get("metric_key") or "unknown_metric")
        title = f"{finding.get('metric_label')} threshold breached"
        severity = str(finding.get("severity") or "warning")
        value = float(finding.get("value") or 0.0)
        threshold = float(finding.get("threshold_value") or 0.0)
        now = datetime.now(timezone.utc)

        created_new = False
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, status, severity, occurrence_count
                    FROM workflow_alert_incidents
                    WHERE incident_key=%s
                      AND status IN ('open', 'acknowledged')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (incident_key,),
                )
                existing = cur.fetchone()

                if existing:
                    incident_id = int(existing[0])
                    status = str(existing[1])
                    count = int(existing[3]) + 1
                    cur.execute(
                        """
                        UPDATE workflow_alert_incidents
                        SET severity=%s,
                            title=%s,
                            current_value=%s,
                            threshold_value=%s,
                            occurrence_count=%s,
                            details=%s,
                            last_seen_at=%s,
                            updated_at=%s
                        WHERE id=%s
                        RETURNING id, incident_key, metric_key, severity, status, title
                        """,
                        (
                            severity,
                            title,
                            value,
                            threshold,
                            count,
                            Json(
                                {
                                    "finding": finding,
                                    "actor_ref": actor_ref,
                                }
                            ),
                            now,
                            now,
                            incident_id,
                        ),
                    )
                else:
                    created_new = True
                    status = "open"
                    cur.execute(
                        """
                        INSERT INTO workflow_alert_incidents (
                            incident_key,
                            metric_key,
                            severity,
                            title,
                            status,
                            current_value,
                            threshold_value,
                            occurrence_count,
                            details,
                            opened_at,
                            last_seen_at,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, 'open', %s, %s, 1, %s, %s, %s, %s)
                        RETURNING id, incident_key, metric_key, severity, status, title
                        """,
                        (
                            incident_key,
                            incident_key,
                            severity,
                            title,
                            value,
                            threshold,
                            Json(
                                {
                                    "finding": finding,
                                    "actor_ref": actor_ref,
                                }
                            ),
                            now,
                            now,
                            now,
                        ),
                    )

                row = cur.fetchone()

        incident = {
            "id": int(row[0]),
            "incident_key": str(row[1]),
            "metric_key": str(row[2]),
            "severity": str(row[3]),
            "status": str(row[4]),
            "title": str(row[5]),
        }
        if created_new:
            self._emit_incident_event("workflow.alert_raised", incident, actor_ref=actor_ref)
        return incident

    def _resolve_cleared_incidents(self, active_keys: set[str], actor_ref: str) -> list[int]:
        now = datetime.now(timezone.utc)
        resolved_ids: list[int] = []
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, incident_key, metric_key, severity, status, title
                    FROM workflow_alert_incidents
                    WHERE status IN ('open', 'acknowledged')
                    """
                )
                rows = cur.fetchall() or []

                for row in rows:
                    incident_id = int(row[0])
                    incident_key = str(row[1])
                    if incident_key in active_keys:
                        continue

                    cur.execute(
                        """
                        UPDATE workflow_alert_incidents
                        SET status='resolved',
                            resolved_at=%s,
                            updated_at=%s,
                            details = COALESCE(details, '{}'::jsonb) || %s::jsonb
                        WHERE id=%s
                        """,
                        (
                            now,
                            now,
                            Json(
                                {
                                    "resolved_reason": "metric_back_to_normal",
                                    "resolved_by": actor_ref,
                                }
                            ),
                            incident_id,
                        ),
                    )

                    incident = {
                        "id": incident_id,
                        "incident_key": incident_key,
                        "metric_key": str(row[2]),
                        "severity": str(row[3]),
                        "status": "resolved",
                        "title": str(row[5]),
                    }
                    self._emit_incident_event("workflow.alert_resolved", incident, actor_ref=actor_ref)
                    resolved_ids.append(incident_id)
        return resolved_ids

    def reconcile_alerts(self, window_hours: int = 24, actor_ref: str = "system") -> dict[str, Any]:
        evaluation = self.evaluate_alerts(window_hours=window_hours)
        findings = list(evaluation.get("findings") or [])

        breached = [f for f in findings if bool(f.get("breached"))]
        active_keys = {str(f.get("metric_key") or "") for f in breached}

        created_or_updated: list[dict[str, Any]] = []
        for finding in breached:
            created_or_updated.append(self._upsert_open_incident(finding, actor_ref=actor_ref))

        resolved_ids = self._resolve_cleared_incidents(active_keys=active_keys, actor_ref=actor_ref)

        return {
            "result": "ok",
            "status": evaluation.get("status"),
            "window_hours": int(window_hours),
            "breached_count": len(breached),
            "opened_or_updated_count": len(created_or_updated),
            "resolved_count": len(resolved_ids),
            "active_incident_keys": sorted(active_keys),
            "incidents": created_or_updated,
            "resolved_incident_ids": resolved_ids,
            "evaluation": evaluation,
        }

    def list_incidents(self, status: str = "open", limit: int = 50) -> list[dict[str, Any]]:
        status = str(status or "open").strip().lower()
        limit = max(1, int(limit))

        sql = """
        SELECT id, incident_key, metric_key, severity, title, status,
               current_value, threshold_value, occurrence_count,
               opened_at, acknowledged_at, resolved_at, last_seen_at, updated_at, details
        FROM workflow_alert_incidents
        """
        params: list[Any] = []
        if status in {"open", "acknowledged", "resolved"}:
            sql += " WHERE status = %s"
            params.append(status)
        sql += " ORDER BY id DESC LIMIT %s"
        params.append(limit)

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall() or []

        return [
            {
                "id": int(r[0]),
                "incident_key": str(r[1]),
                "metric_key": str(r[2]),
                "severity": str(r[3]),
                "title": str(r[4]),
                "status": str(r[5]),
                "current_value": float(r[6]) if r[6] is not None else None,
                "threshold_value": float(r[7]) if r[7] is not None else None,
                "occurrence_count": int(r[8]),
                "opened_at": str(r[9]) if r[9] is not None else None,
                "acknowledged_at": str(r[10]) if r[10] is not None else None,
                "resolved_at": str(r[11]) if r[11] is not None else None,
                "last_seen_at": str(r[12]) if r[12] is not None else None,
                "updated_at": str(r[13]) if r[13] is not None else None,
                "details": r[14] or {},
            }
            for r in rows
        ]

    def update_incident_status(self, incident_id: int, action: str, actor_ref: str = "system") -> dict[str, Any]:
        incident_id = int(incident_id)
        action = str(action or "").strip().lower()
        if action not in {"acknowledge", "resolve"}:
            return {"success": False, "message": f"Unsupported incident action: {action}"}

        now = datetime.now(timezone.utc)
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, incident_key, metric_key, severity, status, title
                    FROM workflow_alert_incidents
                    WHERE id=%s
                    """,
                    (incident_id,),
                )
                row = cur.fetchone()
                if not row:
                    return {"success": False, "message": f"Incident not found: {incident_id}"}

                current_status = str(row[4])
                if action == "acknowledge":
                    if current_status == "resolved":
                        return {"success": False, "message": "Cannot acknowledge a resolved incident"}
                    cur.execute(
                        """
                        UPDATE workflow_alert_incidents
                        SET status='acknowledged', acknowledged_at=%s, updated_at=%s,
                            details = COALESCE(details, '{}'::jsonb) || %s::jsonb
                        WHERE id=%s
                        """,
                        (
                            now,
                            now,
                            Json({"acknowledged_by": actor_ref}),
                            incident_id,
                        ),
                    )
                    status = "acknowledged"
                    event_type = "workflow.alert_acknowledged"
                else:
                    cur.execute(
                        """
                        UPDATE workflow_alert_incidents
                        SET status='resolved', resolved_at=%s, updated_at=%s,
                            details = COALESCE(details, '{}'::jsonb) || %s::jsonb
                        WHERE id=%s
                        """,
                        (
                            now,
                            now,
                            Json({"resolved_by": actor_ref, "resolved_reason": "manual_resolve"}),
                            incident_id,
                        ),
                    )
                    status = "resolved"
                    event_type = "workflow.alert_resolved"

        incident = {
            "id": int(row[0]),
            "incident_key": str(row[1]),
            "metric_key": str(row[2]),
            "severity": str(row[3]),
            "status": status,
            "title": str(row[5]),
        }
        self._emit_incident_event(event_type, incident, actor_ref=actor_ref)
        return {"success": True, "incident": incident}