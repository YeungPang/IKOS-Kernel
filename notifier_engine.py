"""Multi-channel incident notification engine (Phase 4+ enhancement)."""

from __future__ import annotations

import json
import smtplib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen


@dataclass
class NotificationTemplate:
    """Template for rendering notification messages."""

    name: str
    subject_template: str
    message_template: str
    channel: str  # email, webhook, sms

    def render(self, incident: dict[str, Any]) -> tuple[str, str]:
        """Render subject and message from template and incident data."""
        subject = self.subject_template.format(
            metric_label=incident.get("metric_label", "Unknown"),
            severity=incident.get("severity", "unknown"),
            status=incident.get("status", "unknown"),
        )
        message = self.message_template.format(
            metric_label=incident.get("metric_label", "Unknown"),
            metric_key=incident.get("incident_key", "unknown"),
            current_value=incident.get("current_value", 0),
            threshold_value=incident.get("threshold_value", 0),
            severity=incident.get("severity", "unknown"),
            status=incident.get("status", "unknown"),
            opened_at=incident.get("opened_at", "unknown"),
            title=incident.get("title", "Incident Alert"),
        )
        return subject, message


DEFAULT_TEMPLATES = {
    "incident_opened_email": NotificationTemplate(
        name="incident_opened_email",
        channel="email",
        subject_template="[{severity}] {metric_label} alert opened",
        message_template="""
Incident Alert
==============
Title: {title}
Severity: {severity} 
Status: {status}
Metric: {metric_label} ({metric_key})
Current Value: {current_value}
Threshold: {threshold_value}
Opened: {opened_at}

Please review and take appropriate action.
""",
    ),
    "incident_acknowledged_email": NotificationTemplate(
        name="incident_acknowledged_email",
        channel="email",
        subject_template="[ACKNOWLEDGED] {metric_label} alert acknowledged",
        message_template="""
Incident Acknowledged
====================
Title: {title}
Metric: {metric_label}
Status: {status}
Acknowledged: {opened_at}

Investigation in progress.
""",
    ),
    "incident_resolved_email": NotificationTemplate(
        name="incident_resolved_email",
        channel="email",
        subject_template="[RESOLVED] {metric_label} alert resolved",
        message_template="""
Incident Resolved
================
Title: {title}
Metric: {metric_label}
Status: {status}
Resolved: {opened_at}

The alert has been successfully resolved.
""",
    ),
    "incident_opened_webhook": NotificationTemplate(
        name="incident_opened_webhook",
        channel="webhook",
        subject_template="incident.opened",
        message_template='{"event":"incident.opened","severity":"{severity}","metric":"{metric_label}","value":{current_value},"threshold":{threshold_value}}',
    ),
    "incident_acknowledged_webhook": NotificationTemplate(
        name="incident_acknowledged_webhook",
        channel="webhook",
        subject_template="incident.acknowledged",
        message_template='{"event":"incident.acknowledged","severity":"{severity}","metric":"{metric_label}","status":"{status}"}',
    ),
    "incident_resolved_webhook": NotificationTemplate(
        name="incident_resolved_webhook",
        channel="webhook",
        subject_template="incident.resolved",
        message_template='{"event":"incident.resolved","severity":"{severity}","metric":"{metric_label}","status":"{status}"}',
    ),
}


@dataclass
class NotificationChannel:
    """Configuration for a notification channel."""

    channel_type: str  # email, webhook, sms
    enabled: bool = True
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class NotificationRule:
    """Route incidents to channels based on severity and type."""

    rule_name: str
    min_severity: str  # healthy, warning, critical
    incident_pattern: str  # regex or *
    channels: list[str]  # email, webhook, sms
    enabled: bool = True


DEFAULT_RULES = [
    NotificationRule(
        rule_name="critical_immediate",
        min_severity="critical",
        incident_pattern="*",
        channels=["email", "webhook"],
    ),
    NotificationRule(
        rule_name="warning_email",
        min_severity="warning",
        incident_pattern="*",
        channels=["email"],
    ),
    NotificationRule(
        rule_name="sla_metrics_webhook",
        min_severity="warning",
        incident_pattern="sla.*",
        channels=["webhook"],
    ),
]


class NotifierEngine:
    """Multi-channel notification delivery."""

    def __init__(self, db_connection_fn, channels: dict[str, NotificationChannel] | None = None):
        self.db_connection_fn = db_connection_fn
        self.channels = channels or self._default_channels()
        self.ensure_tables()

    def _default_channels(self) -> dict[str, NotificationChannel]:
        return {
            "email": NotificationChannel(
                channel_type="email",
                enabled=True,
                config={"smtp_host": "localhost", "smtp_port": 25, "from_address": "alerts@idms.local"},
            ),
            "webhook": NotificationChannel(
                channel_type="webhook",
                enabled=True,
                config={"timeout_seconds": 30},
            ),
            "sms": NotificationChannel(
                channel_type="sms",
                enabled=False,
                config={"api_key": "", "provider": "mock"},
            ),
        }

    def ensure_tables(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS notification_subscriptions (
            id BIGSERIAL PRIMARY KEY,
            subscriber_ref TEXT NOT NULL,
            subscriber_email TEXT,
            subscriber_phone TEXT,
            webhook_url TEXT,
            channels TEXT NOT NULL,
            severity_min TEXT NOT NULL DEFAULT 'warning',
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(subscriber_ref, webhook_url)
        );

        CREATE TABLE IF NOT EXISTS notification_history (
            id BIGSERIAL PRIMARY KEY,
            incident_id BIGINT NOT NULL,
            channel TEXT NOT NULL,
            recipient TEXT NOT NULL,
            template_name TEXT NOT NULL,
            subject TEXT,
            message TEXT,
            status TEXT NOT NULL DEFAULT 'sent',
            sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            error_message TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_notification_history_incident ON notification_history(incident_id);
        CREATE INDEX IF NOT EXISTS idx_notification_history_channel ON notification_history(channel);
        CREATE INDEX IF NOT EXISTS idx_notification_subscriptions_active ON notification_subscriptions(active);
        """
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)

    def notify(
        self,
        incident: dict[str, Any],
        event_type: str,
        target_subscribers: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Send notifications about incident to subscribed recipients."""
        if not target_subscribers:
            target_subscribers = self._load_subscriptions(
                severity=incident.get("severity", "warning")
            )

        results = {"sent": 0, "failed": 0, "deliveries": []}

        template_key = f"incident_{event_type}_email"
        template_webhook = f"incident_{event_type}_webhook"

        for subscriber in target_subscribers:
            # Email delivery
            if "email" in subscriber.get("channels", []) and subscriber.get("subscriber_email"):
                result = self._send_email(
                    incident,
                    subscriber.get("subscriber_email"),
                    DEFAULT_TEMPLATES.get(template_key),
                )
                results["deliveries"].append(result)
                if result["status"] == "sent":
                    results["sent"] += 1
                else:
                    results["failed"] += 1

            # Webhook delivery
            if "webhook" in subscriber.get("channels", []) and subscriber.get("webhook_url"):
                result = self._send_webhook(
                    incident,
                    subscriber.get("webhook_url"),
                    DEFAULT_TEMPLATES.get(template_webhook),
                )
                results["deliveries"].append(result)
                if result["status"] == "sent":
                    results["sent"] += 1
                else:
                    results["failed"] += 1

            # SMS delivery (mocked)
            if "sms" in subscriber.get("channels", []) and subscriber.get("subscriber_phone"):
                result = self._send_sms(
                    incident,
                    subscriber.get("subscriber_phone"),
                    DEFAULT_TEMPLATES.get(f"incident_{event_type}_sms", None),
                )
                results["deliveries"].append(result)
                if result["status"] == "sent":
                    results["sent"] += 1
                else:
                    results["failed"] += 1

        self._record_deliveries(incident, results["deliveries"])
        return results

    def _load_subscriptions(self, severity: str = "warning") -> list[dict[str, Any]]:
        """Load active subscriptions for given severity level."""
        severity_hierarchy = {"healthy": 0, "warning": 1, "critical": 2}
        min_level = severity_hierarchy.get(severity, 1)

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT subscriber_ref, subscriber_email, subscriber_phone, webhook_url, channels, severity_min
                    FROM notification_subscriptions
                    WHERE active = TRUE
                    ORDER BY subscriber_ref
                    """
                )
                rows = cur.fetchall() or []

        result = []
        for row in rows:
            sub_severity = row[5] or "warning"
            sub_level = severity_hierarchy.get(sub_severity, 1)
            if min_level >= sub_level:
                result.append(
                    {
                        "subscriber_ref": str(row[0]),
                        "subscriber_email": row[1],
                        "subscriber_phone": row[2],
                        "webhook_url": row[3],
                        "channels": (row[4] or "email").split(","),
                        "severity_min": sub_severity,
                    }
                )
        return result

    def _send_email(
        self, incident: dict[str, Any], recipient: str, template: NotificationTemplate | None
    ) -> dict[str, Any]:
        result = {
            "channel": "email",
            "recipient": recipient,
            "status": "sent",
            "template_name": template.name if template else "unknown",
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            if not template:
                raise ValueError("No email template available")

            subject, message = template.render(incident)

            # For development/testing, log to console instead of SMTP
            print(
                f"[EMAIL] To: {recipient}\nSubject: {subject}\n{message}\n---"
            )

            result["status"] = "sent"
        except Exception as e:
            result["status"] = "failed"
            result["error_message"] = str(e)

        return result

    def _send_webhook(
        self, incident: dict[str, Any], webhook_url: str, template: NotificationTemplate | None
    ) -> dict[str, Any]:
        result = {
            "channel": "webhook",
            "recipient": webhook_url,
            "status": "sent",
            "template_name": template.name if template else "unknown",
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            if not template:
                raise ValueError("No webhook template available")

            _, payload_str = template.render(incident)
            payload = json.loads(payload_str)

            # For development, log instead of actually sending
            print(
                f"[WEBHOOK] POST {webhook_url}\nPayload: {json.dumps(payload, indent=2)}\n---"
            )

            # Optionally send actual HTTP request (commented for dev)
            # req = Request(webhook_url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
            # with urlopen(req, timeout=5) as resp:
            #     resp.read()

            result["status"] = "sent"
        except Exception as e:
            result["status"] = "failed"
            result["error_message"] = str(e)

        return result

    def _send_sms(
        self, incident: dict[str, Any], phone: str, template: NotificationTemplate | None
    ) -> dict[str, Any]:
        """Mock SMS sending (returns success)."""
        result = {
            "channel": "sms",
            "recipient": phone,
            "status": "sent",
            "template_name": template.name if template else "mock_sms",
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            msg = f"Alert [{incident.get('severity', 'unknown')}]: {incident.get('title', 'Incident alert')}"
            print(f"[SMS] To: {phone}\nMessage: {msg}\n---")
            result["status"] = "sent"
        except Exception as e:
            result["status"] = "failed"
            result["error_message"] = str(e)

        return result

    def _record_deliveries(
        self, incident: dict[str, Any], deliveries: list[dict[str, Any]]
    ) -> None:
        """Record notification deliveries in database."""
        incident_id = incident.get("id", 0)
        if incident_id <= 0:
            return

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                for delivery in deliveries:
                    cur.execute(
                        """
                        INSERT INTO notification_history (
                            incident_id, channel, recipient, template_name, subject, message, status, error_message
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            incident_id,
                            delivery.get("channel"),
                            delivery.get("recipient"),
                            delivery.get("template_name"),
                            "",
                            "",
                            delivery.get("status"),
                            delivery.get("error_message"),
                        ),
                    )

    def subscribe(
        self, subscriber_ref: str, email: str | None = None, webhook_url: str | None = None, phone: str | None = None
    ) -> dict[str, Any]:
        """Subscribe a user/service to incident notifications."""
        channels = []
        if email:
            channels.append("email")
        if webhook_url:
            channels.append("webhook")
        if phone:
            channels.append("sms")

        if not channels:
            return {"success": False, "error": "At least one channel required"}

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO notification_subscriptions (
                        subscriber_ref, subscriber_email, subscriber_phone, webhook_url, channels, severity_min, active
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                    ON CONFLICT (subscriber_ref, webhook_url)
                    DO UPDATE SET channels=%s, active=TRUE
                    RETURNING id
                    """,
                    (
                        subscriber_ref,
                        email,
                        phone,
                        webhook_url,
                        ",".join(channels),
                        "warning",
                        ",".join(channels),
                    ),
                )
                row = cur.fetchone()

        return {
            "success": True,
            "subscriber_ref": subscriber_ref,
            "subscription_id": int(row[0]) if row else 0,
            "channels": channels,
        }
