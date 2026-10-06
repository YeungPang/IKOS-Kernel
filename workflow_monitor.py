"""Operational monitoring utilities for workflow automation (Phase 3)."""

from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json


class WorkflowMonitor:
    """Collects workflow/event KPIs and persists snapshot records."""

    def __init__(self, db_connection_fn):
        self.db_connection_fn = db_connection_fn
        self.ensure_tables()

    def ensure_tables(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS workflow_metric_snapshots (
            id BIGSERIAL PRIMARY KEY,
            snapshot_label TEXT,
            metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
            health_status TEXT NOT NULL DEFAULT 'healthy',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_workflow_metric_snapshots_created ON workflow_metric_snapshots(created_at);
        CREATE INDEX IF NOT EXISTS idx_workflow_metric_snapshots_health ON workflow_metric_snapshots(health_status);
        """
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)

    def collect_metrics(self, window_hours: int = 24) -> dict[str, Any]:
        hours = max(1, int(window_hours))
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(1)
                    FROM event_log
                    WHERE created_at >= NOW() - make_interval(hours => %s)
                    """,
                    (hours,),
                )
                event_count = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(1)
                    FROM event_log
                    WHERE created_at >= NOW() - make_interval(hours => %s)
                      AND policy_mode='deny'
                    """,
                    (hours,),
                )
                denied_event_count = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(1)
                    FROM workflow_state_transition_log
                    WHERE created_at >= NOW() - make_interval(hours => %s)
                    """,
                    (hours,),
                )
                transition_count = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(1)
                    FROM workflow_state_transition_log
                    WHERE created_at >= NOW() - make_interval(hours => %s)
                      AND allowed=FALSE
                    """,
                    (hours,),
                )
                denied_transition_count = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(1)
                    FROM project_tasks
                    WHERE status NOT IN ('done', 'cancelled')
                      AND due_date IS NOT NULL
                      AND due_date < CURRENT_DATE
                    """
                )
                overdue_tasks = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(1)
                    FROM compliance_actions
                    WHERE status NOT IN ('closed', 'completed', 'submitted', 'cancelled')
                      AND due_date_key IS NOT NULL
                      AND due_date_key < CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS BIGINT)
                    """
                )
                overdue_compliance_actions = int(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT COUNT(1)
                    FROM workflow_cases
                    WHERE status NOT IN ('approved', 'rejected', 'cancelled')
                      AND sla_due_at IS NOT NULL
                      AND sla_due_at < NOW()
                    """
                )
                overdue_workflow_cases = int(cur.fetchone()[0])

        denied_event_ratio = (denied_event_count / event_count) if event_count else 0.0
        denied_transition_ratio = (denied_transition_count / transition_count) if transition_count else 0.0

        return {
            "window_hours": hours,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "events": {
                "total": event_count,
                "denied": denied_event_count,
                "denied_ratio": round(denied_event_ratio, 6),
            },
            "transitions": {
                "total": transition_count,
                "denied": denied_transition_count,
                "denied_ratio": round(denied_transition_ratio, 6),
            },
            "overdue": {
                "project_tasks": overdue_tasks,
                "compliance_actions": overdue_compliance_actions,
                "workflow_cases": overdue_workflow_cases,
            },
        }

    def health(self, window_hours: int = 24) -> dict[str, Any]:
        metrics = self.collect_metrics(window_hours=window_hours)
        overdue = metrics.get("overdue", {})
        transitions = metrics.get("transitions", {})
        events = metrics.get("events", {})

        reasons: list[str] = []
        status = "healthy"

        if int(overdue.get("workflow_cases", 0)) > 25:
            status = "critical"
            reasons.append("high_overdue_workflow_cases")
        if float(transitions.get("denied_ratio", 0.0)) > 0.5:
            status = "critical"
            reasons.append("high_denied_transition_ratio")

        if status != "critical":
            if int(overdue.get("project_tasks", 0)) > 0:
                status = "warning"
                reasons.append("overdue_project_tasks")
            if int(overdue.get("compliance_actions", 0)) > 0:
                status = "warning"
                reasons.append("overdue_compliance_actions")
            if float(events.get("denied_ratio", 0.0)) > 0.1:
                status = "warning"
                reasons.append("elevated_denied_event_ratio")

        return {
            "status": status,
            "reasons": reasons,
            "metrics": metrics,
        }

    def snapshot(self, label: str = "") -> dict[str, Any]:
        label = str(label or "").strip() or None
        health = self.health(window_hours=24)
        metrics = health.get("metrics", {})
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO workflow_metric_snapshots (snapshot_label, metrics, health_status)
                    VALUES (%s, %s, %s)
                    RETURNING id, created_at
                    """,
                    (label, Json(metrics), str(health.get("status") or "healthy")),
                )
                row = cur.fetchone()

        return {
            "snapshot_id": int(row[0]),
            "created_at": str(row[1]),
            "snapshot_label": label,
            "health_status": str(health.get("status") or "healthy"),
            "metrics": metrics,
        }

    def history(self, limit: int = 20) -> list[dict[str, Any]]:
        n = max(1, int(limit))
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, snapshot_label, metrics, health_status, created_at
                    FROM workflow_metric_snapshots
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (n,),
                )
                rows = cur.fetchall() or []

        return [
            {
                "id": int(r[0]),
                "snapshot_label": r[1],
                "metrics": r[2] or {},
                "health_status": r[3],
                "created_at": str(r[4]),
            }
            for r in rows
        ]