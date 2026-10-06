"""Dashboard and metrics API for real-time incident visualization (Phase 4+ enhancement)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


class DashboardAPI:
    """Provides real-time incident and health metrics endpoints."""

    def __init__(self, db_connection_fn, monitor, reliability_manager):
        self.db_connection_fn = db_connection_fn
        self.monitor = monitor
        self.reliability_manager = reliability_manager

    def get_health_summary(self) -> dict[str, Any]:
        """Get overall system health snapshot."""
        metrics = self.monitor.collect_metrics(window_hours=24)
        findings = self.reliability_manager.evaluate_alerts(window_hours=24)
        findings_list = findings.get("findings", [])

        breaches = [f for f in findings_list if f.get("breached")]
        critical_count = sum(1 for b in breaches if b.get("severity") == "critical")
        warning_count = sum(1 for b in breaches if b.get("severity") == "warning")

        status = "healthy"
        if critical_count > 0:
            status = "critical"
        elif warning_count > 0:
            status = "impaired"

        return {
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metrics_summary": {
                "critical_alerts": critical_count,
                "warning_alerts": warning_count,
                "healthy_metrics": sum(1 for f in findings_list if not f.get("breached")),
                "total_evaluated": len(findings_list),
            },
            "key_metrics": {
                "overdue_tasks": float((metrics.get("overdue") or {}).get("project_tasks") or 0),
                "overdue_compliance": float((metrics.get("overdue") or {}).get("compliance_actions") or 0),
                "overdue_cases": float((metrics.get("overdue") or {}).get("workflow_cases") or 0),
                "denied_events_ratio": float((metrics.get("events") or {}).get("denied_ratio") or 0.0),
            },
        }

    def get_incident_summary(self) -> dict[str, Any]:
        """Get incident summary by status and severity."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                # Count by status and severity
                cur.execute(
                    """
                    SELECT status, severity, COUNT(*) as count
                    FROM workflow_alert_incidents
                    WHERE status IN ('open', 'acknowledged', 'resolved')
                    GROUP BY status, severity
                    ORDER BY status, severity
                    """
                )
                rows = cur.fetchall() or []

        summary = {
            "open": {"critical": 0, "warning": 0, "healthy": 0},
            "acknowledged": {"critical": 0, "warning": 0, "healthy": 0},
            "resolved": {"critical": 0, "warning": 0, "healthy": 0},
        }

        for status, severity, count in rows:
            if status in summary and severity in summary[status]:
                summary[status][severity] = int(count)

        summary["total"] = sum(
            sum(v.values()) for v in summary.values() if isinstance(v, dict)
        )
        summary["critical_open"] = summary["open"]["critical"]
        summary["timestamp"] = datetime.now(timezone.utc).isoformat()

        return summary

    def get_incidents_by_status(
        self, status: str = "open", limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        """Get paginated incidents by status."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, incident_key, metric_key, severity, title, status, 
                           current_value, threshold_value, opened_at, acknowledged_at, resolved_at,
                           last_seen_at, occurrence_count
                    FROM workflow_alert_incidents
                    WHERE status = %s
                    ORDER BY severity DESC, last_seen_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (status, int(limit), int(offset)),
                )
                rows = cur.fetchall() or []

                cur.execute("SELECT COUNT(*) FROM workflow_alert_incidents WHERE status = %s", (status,))
                total = int(cur.fetchone()[0]) if cur.fetchone() else 0

        incidents = []
        for row in rows:
            incidents.append(
                {
                    "id": int(row[0]),
                    "incident_key": str(row[1]),
                    "metric_key": str(row[2]),
                    "severity": str(row[3]),
                    "title": str(row[4]),
                    "status": str(row[5]),
                    "current_value": float(row[6]) if row[6] else None,
                    "threshold_value": float(row[7]) if row[7] else None,
                    "opened_at": row[8].isoformat() if row[8] else None,
                    "acknowledged_at": row[9].isoformat() if row[9] else None,
                    "resolved_at": row[10].isoformat() if row[10] else None,
                    "last_seen_at": row[11].isoformat() if row[11] else None,
                    "occurrence_count": int(row[12]) if row[12] else 0,
                }
            )

        return {
            "status": status,
            "total": total,
            "limit": int(limit),
            "offset": int(offset),
            "incidents": incidents,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_metrics_timeline(self, metric_key: str, hours: int = 168) -> dict[str, Any]:
        """Get timeline of metric values over time."""
        # For now, return reconstructed timeline from recent incidents
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT last_seen_at, current_value, severity
                    FROM workflow_alert_incidents
                    WHERE metric_key = %s
                      AND last_seen_at >= NOW() - INTERVAL '%s hours'
                    ORDER BY last_seen_at ASC
                    LIMIT 100
                    """,
                    (metric_key, int(hours)),
                )
                rows = cur.fetchall() or []

        timeline = []
        for row in rows:
            timeline.append(
                {
                    "timestamp": row[0].isoformat() if row[0] else None,
                    "value": float(row[1]) if row[1] else None,
                    "severity": str(row[2]),
                }
            )

        return {
            "metric_key": metric_key,
            "hours": int(hours),
            "data_points": len(timeline),
            "timeline": timeline,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_incident_trend_analysis(self, days: int = 7) -> dict[str, Any]:
        """Analyze incident trends over time period."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                # Incidents opened per day
                cur.execute(
                    """
                    SELECT DATE(opened_at), COUNT(*) as count
                    FROM workflow_alert_incidents
                    WHERE opened_at >= %s
                    GROUP BY DATE(opened_at)
                    ORDER BY DATE(opened_at) ASC
                    """,
                    (cutoff,),
                )
                opened_rows = cur.fetchall() or []

                # Incidents resolved per day
                cur.execute(
                    """
                    SELECT DATE(resolved_at), COUNT(*) as count
                    FROM workflow_alert_incidents
                    WHERE resolved_at >= %s AND resolved_at IS NOT NULL
                    GROUP BY DATE(resolved_at)
                    ORDER BY DATE(resolved_at) ASC
                    """,
                    (cutoff,),
                )
                resolved_rows = cur.fetchall() or []

                # Average time to resolve
                cur.execute(
                    """
                    SELECT AVG(EXTRACT(epoch FROM (resolved_at - opened_at))) as avg_seconds
                    FROM workflow_alert_incidents
                    WHERE resolved_at IS NOT NULL AND opened_at >= %s
                    """,
                    (cutoff,),
                )
                avg_row = cur.fetchone()
                avg_time_seconds = float(avg_row[0]) if avg_row and avg_row[0] else 0.0

        opened_trend = [
            {"date": str(row[0]), "count": int(row[1])} for row in opened_rows
        ]
        resolved_trend = [
            {"date": str(row[0]), "count": int(row[1])} for row in resolved_rows
        ]

        total_opened = sum(int(row[1]) for row in opened_rows)
        total_resolved = sum(int(row[1]) for row in resolved_rows)

        return {
            "period_days": int(days),
            "opened_trend": opened_trend,
            "resolved_trend": resolved_trend,
            "total_opened": total_opened,
            "total_resolved": total_resolved,
            "avg_resolution_time_hours": round(avg_time_seconds / 3600, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_metric_distribution(self) -> dict[str, Any]:
        """Get distribution of active incidents by metric."""
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT metric_key, severity, COUNT(*) as count
                    FROM workflow_alert_incidents
                    WHERE status IN ('open', 'acknowledged')
                    GROUP BY metric_key, severity
                    ORDER BY metric_key, severity
                    """
                )
                rows = cur.fetchall() or []

        distribution = {}
        for metric_key, severity, count in rows:
            key = str(metric_key)
            if key not in distribution:
                distribution[key] = {"critical": 0, "warning": 0, "total": 0}
            distribution[key][str(severity)] = int(count)
            distribution[key]["total"] += int(count)

        return {
            "active_metrics": len(distribution),
            "by_metric": distribution,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_health_history(self, hours: int = 72) -> dict[str, Any]:
        """Get health status snapshots over time."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=int(hours))

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                # Sample hourly snapshots
                cur.execute(
                    """
                    SELECT DATE_TRUNC('hour', opened_at), COUNT(*) as count
                    FROM workflow_alert_incidents
                    WHERE opened_at >= %s
                    GROUP BY DATE_TRUNC('hour', opened_at)
                    ORDER BY DATE_TRUNC('hour', opened_at) ASC
                    """,
                    (cutoff,),
                )
                rows = cur.fetchall() or []

        history = [
            {
                "timestamp": row[0].isoformat() if row[0] else None,
                "incidents_opened": int(row[1]),
            }
            for row in rows
        ]

        return {
            "period_hours": int(hours),
            "hourly_snapshots": history,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_sla_compliance(self) -> dict[str, Any]:
        """Get SLA compliance metrics for incidents."""
        sla_hours = 4  # Default SLA: resolve within 4 hours

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                # Incidents resolved within SLA
                cur.execute(
                    """
                    SELECT COUNT(*) as count
                    FROM workflow_alert_incidents
                    WHERE resolved_at IS NOT NULL
                      AND EXTRACT(epoch FROM (resolved_at - opened_at)) <= %s
                    """,
                    (sla_hours * 3600,),
                )
                within_sla = int(cur.fetchone()[0]) if cur.fetchone() else 0

                # Total resolved
                cur.execute(
                    """
                    SELECT COUNT(*) as count
                    FROM workflow_alert_incidents
                    WHERE resolved_at IS NOT NULL
                    """
                )
                total_resolved = int(cur.fetchone()[0]) if cur.fetchone() else 0

        compliance_rate = (
            (100.0 * within_sla / total_resolved) if total_resolved > 0 else 0.0
        )

        return {
            "sla_hours": sla_hours,
            "resolved_within_sla": within_sla,
            "total_resolved": total_resolved,
            "compliance_rate_percent": round(compliance_rate, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
