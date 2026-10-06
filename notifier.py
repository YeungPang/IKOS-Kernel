"""Notification layer for events and scheduler outputs.

Safe default is log-only. Email/webhook can be enabled through env vars.
"""

import json
import os
import smtplib
from abc import ABC, abstractmethod
from email.message import EmailMessage
from enum import Enum
from typing import Any
from urllib import request

from psycopg2.extras import Json


class NotificationChannel(Enum):
    EMAIL = "email"
    WEBHOOK = "webhook"
    LOG = "log"


class NotificationBase(ABC):
    @abstractmethod
    def send(self, subject: str, message: str, recipient: str | None = None, **kwargs) -> bool:
        raise NotImplementedError


class LogNotifier(NotificationBase):
    def send(self, subject: str, message: str, recipient: str | None = None, **kwargs) -> bool:
        print(f"[NOTIFY][LOG] subject={subject} recipient={recipient or 'SYSTEM'} message={message}")
        return True


class EmailNotifier(NotificationBase):
    def __init__(self, smtp_host: str, smtp_port: int, from_address: str, username: str | None = None, password: str | None = None):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.from_address = from_address
        self.username = username
        self.password = password

    def send(self, subject: str, message: str, recipient: str | None = None, **kwargs) -> bool:
        if not recipient:
            return False
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.from_address
        msg["To"] = recipient
        msg.set_content(message)
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=10) as server:
                if self.username and self.password:
                    server.starttls()
                    server.login(self.username, self.password)
                server.send_message(msg)
            return True
        except Exception:
            return False


class WebhookNotifier(NotificationBase):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send(self, subject: str, message: str, recipient: str | None = None, **kwargs) -> bool:
        payload = {
            "subject": subject,
            "message": message,
            "recipient": recipient,
            "meta": kwargs,
        }
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=10) as resp:
                return 200 <= getattr(resp, "status", 500) < 300
        except Exception:
            return False


class Notifier:
    def __init__(self, channels: dict[NotificationChannel, NotificationBase] | None = None, db_connection_fn=None):
        self.channels = channels or {NotificationChannel.LOG: LogNotifier()}
        self.db_connection_fn = db_connection_fn
        self.ensure_tables()

    def ensure_tables(self) -> None:
        if self.db_connection_fn is None:
            return
        ddl = """
        CREATE TABLE IF NOT EXISTS notification_log (
            id BIGSERIAL PRIMARY KEY,
            channel TEXT NOT NULL,
            subject TEXT NOT NULL,
            message TEXT NOT NULL,
            recipient TEXT,
            success BOOLEAN NOT NULL,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_notification_log_channel ON notification_log(channel);
        CREATE INDEX IF NOT EXISTS idx_notification_log_success ON notification_log(success);
        CREATE INDEX IF NOT EXISTS idx_notification_log_created ON notification_log(created_at);
        """
        try:
            with self.db_connection_fn() as conn:
                with conn.cursor() as cur:
                    cur.execute(ddl)
        except Exception:
            pass

    def _persist_notification(
        self,
        channel: NotificationChannel,
        subject: str,
        message: str,
        recipient: str | None,
        success: bool,
        metadata: dict[str, Any],
    ) -> None:
        if self.db_connection_fn is None:
            return
        try:
            with self.db_connection_fn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO notification_log (channel, subject, message, recipient, success, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            channel.value,
                            subject,
                            message,
                            recipient,
                            bool(success),
                            Json(metadata or {}),
                        ),
                    )
        except Exception:
            pass

    def send(
        self,
        subject: str,
        message: str,
        recipient: str | None = None,
        channels: list[NotificationChannel] | None = None,
        **kwargs,
    ) -> dict[NotificationChannel, bool]:
        selected = channels or list(self.channels.keys())
        result: dict[NotificationChannel, bool] = {}
        for channel in selected:
            handler = self.channels.get(channel)
            if handler is None:
                result[channel] = False
                self._persist_notification(channel, subject, message, recipient, False, dict(kwargs or {}))
                continue
            try:
                result[channel] = handler.send(subject, message, recipient, **kwargs)
            except Exception:
                result[channel] = False
            self._persist_notification(channel, subject, message, recipient, result[channel], dict(kwargs or {}))
        return result

    def alert(self, subject: str, message: str, severity: str = "info", recipient: str | None = None) -> dict[NotificationChannel, bool]:
        formatted = f"[{severity.upper()}] {subject}"
        # Policy: critical goes all channels, warning/info defaults to log and webhook if configured.
        if severity.lower() == "critical":
            selected = list(self.channels.keys())
        else:
            selected = [c for c in [NotificationChannel.LOG, NotificationChannel.WEBHOOK] if c in self.channels]
            if not selected:
                selected = list(self.channels.keys())
        return self.send(formatted, message, recipient, channels=selected, severity=severity)

    def escalate(self, subject: str, message: str, recipient: str | None = None) -> dict[NotificationChannel, bool]:
        return self.alert(subject, message, severity="critical", recipient=recipient)


def create_notifier_from_config(db_connection_fn=None) -> Notifier:
    raw = str(os.getenv("IDMS_NOTIFICATION_CHANNELS", "log")).strip().lower()
    enabled = {x.strip() for x in raw.split(",") if x.strip()}

    channels: dict[NotificationChannel, NotificationBase] = {}

    if "log" in enabled or not enabled:
        channels[NotificationChannel.LOG] = LogNotifier()

    if "email" in enabled:
        smtp_host = os.getenv("IDMS_SMTP_HOST", "localhost")
        smtp_port = int(os.getenv("IDMS_SMTP_PORT", "587"))
        from_addr = os.getenv("IDMS_EMAIL_FROM", "idms-notifications@example.com")
        user = os.getenv("IDMS_EMAIL_USER", "") or None
        password = os.getenv("IDMS_EMAIL_PASSWORD", "") or None
        channels[NotificationChannel.EMAIL] = EmailNotifier(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            from_address=from_addr,
            username=user,
            password=password,
        )

    if "webhook" in enabled:
        webhook_url = os.getenv("IDMS_WEBHOOK_URL", "").strip()
        if webhook_url:
            channels[NotificationChannel.WEBHOOK] = WebhookNotifier(webhook_url=webhook_url)

    if not channels:
        channels[NotificationChannel.LOG] = LogNotifier()

    return Notifier(channels=channels, db_connection_fn=db_connection_fn)
