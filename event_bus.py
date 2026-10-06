"""Event bus with optional SOLF rule mediation.

SOLF can drive event routing decisions through subscription policies,
while Python executes callbacks safely.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from psycopg2.extras import Json


@dataclass
class Event:
    event_type: str
    source_entity: str
    entity_id: int
    payload: dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: list[str] = field(default_factory=list)


class EventBus:
    def __init__(self, solf_interpreter=None, db_connection_fn=None):
        self.listeners: dict[str, list[Callable[[Event], None]]] = {}
        self.event_history: list[Event] = []
        self.solf_interpreter = solf_interpreter
        self.db_connection_fn = db_connection_fn
        self.ensure_tables()

    def ensure_tables(self) -> None:
        if self.db_connection_fn is None:
            return
        ddl = """
        CREATE TABLE IF NOT EXISTS event_subscriptions (
            id BIGSERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            listener_name TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(event_type, listener_name)
        );
        CREATE TABLE IF NOT EXISTS event_log (
            id BIGSERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            source_entity TEXT NOT NULL,
            entity_id BIGINT,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            tags TEXT[],
            policy_mode TEXT,
            delivered BOOLEAN NOT NULL DEFAULT TRUE,
            listener_count INT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_event_subscriptions_event ON event_subscriptions(event_type);
        CREATE INDEX IF NOT EXISTS idx_event_subscriptions_enabled ON event_subscriptions(enabled);
        CREATE INDEX IF NOT EXISTS idx_event_log_type ON event_log(event_type);
        CREATE INDEX IF NOT EXISTS idx_event_log_entity ON event_log(source_entity, entity_id);
        CREATE INDEX IF NOT EXISTS idx_event_log_created ON event_log(created_at);
        """
        try:
            with self.db_connection_fn() as conn:
                with conn.cursor() as cur:
                    cur.execute(ddl)
        except Exception:
            pass

    def _solf_allows_subscription(self, event_type: str, listener_name: str) -> bool:
        if self.solf_interpreter is None:
            return True
        payload = {"event_type": event_type, "listener": listener_name}
        try:
            result = self.solf_interpreter._invoke_clause("event_subscription_policy", [payload])
        except Exception:
            return True
        if isinstance(result, dict):
            mode = str(result.get("mode") or "allow").strip().lower()
            if mode == "deny":
                return False
            return True
        if isinstance(result, bool):
            return result
        return True

    def _solf_publish_mode(self, event_type: str, payload: dict[str, Any]) -> str:
        if self.solf_interpreter is None:
            return "allow"
        policy_payload = {
            "event_type": event_type,
            "payload": payload,
        }
        try:
            result = self.solf_interpreter._invoke_clause("event_publish_policy", [policy_payload])
        except Exception:
            return "allow"
        if isinstance(result, dict):
            mode = str(result.get("mode") or "allow").strip().lower()
            return mode if mode in {"allow", "deny", "escalate"} else "allow"
        if isinstance(result, bool):
            return "allow" if result else "deny"
        return "allow"

    def _persist_subscription(self, event_type: str, listener_name: str) -> None:
        if self.db_connection_fn is None:
            return
        try:
            with self.db_connection_fn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO event_subscriptions (event_type, listener_name, enabled, metadata)
                        VALUES (%s, %s, TRUE, %s)
                        ON CONFLICT (event_type, listener_name)
                        DO UPDATE SET enabled=TRUE, updated_at=NOW()
                        """,
                        (event_type, listener_name, Json({})),
                    )
        except Exception:
            pass

    def _persist_event(self, event: Event, policy_mode: str, delivered: bool, listener_count: int) -> None:
        if self.db_connection_fn is None:
            return
        try:
            with self.db_connection_fn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO event_log (event_type, source_entity, entity_id, payload, tags, policy_mode, delivered, listener_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            event.event_type,
                            event.source_entity,
                            int(event.entity_id),
                            Json(event.payload or {}),
                            list(event.tags or []),
                            policy_mode,
                            bool(delivered),
                            int(listener_count),
                        ),
                    )
        except Exception:
            pass

    def subscribe(self, event_type: str, callback: Callable[[Event], None]) -> bool:
        listener_name = getattr(callback, "__name__", "anonymous_listener")
        if not self._solf_allows_subscription(event_type, listener_name):
            return False
        self.listeners.setdefault(event_type, []).append(callback)
        self._persist_subscription(event_type, listener_name)
        return True

    def unsubscribe(self, event_type: str, callback: Callable[[Event], None]) -> bool:
        if event_type not in self.listeners:
            return False
        if callback not in self.listeners[event_type]:
            return False
        self.listeners[event_type].remove(callback)
        return True

    def emit(
        self,
        event_type: str,
        source_entity: str,
        entity_id: int,
        payload: dict[str, Any],
        tags: list[str] | None = None,
    ) -> None:
        policy_mode = self._solf_publish_mode(event_type, payload)
        event = Event(
            event_type=event_type,
            source_entity=source_entity,
            entity_id=int(entity_id),
            payload=dict(payload or {}),
            tags=list(tags or []),
        )

        if policy_mode == "deny":
            self._persist_event(event, policy_mode=policy_mode, delivered=False, listener_count=0)
            return

        self.event_history.append(event)

        callbacks = list(self.listeners.get(event_type, []))
        for callback in callbacks:
            try:
                callback(event)
            except Exception as exc:
                print(f"[EVENT_BUS] listener error for {event_type}: {exc}")

        self._persist_event(
            event,
            policy_mode=policy_mode,
            delivered=True,
            listener_count=len(callbacks),
        )

    def emit_many(self, events: list[Event]) -> None:
        for event in events:
            self.emit(
                event_type=event.event_type,
                source_entity=event.source_entity,
                entity_id=event.entity_id,
                payload=event.payload,
                tags=event.tags,
            )

    def clear_history(self) -> None:
        self.event_history.clear()

    def get_events_for_entity(self, source_entity: str, entity_id: int) -> list[Event]:
        return [e for e in self.event_history if e.source_entity == source_entity and e.entity_id == entity_id]


def handle_task_overdue(event: Event, notifier) -> None:
    assigned_to = str(event.payload.get("assigned_to") or "")
    days_overdue = event.payload.get("days_overdue")
    title = str(event.payload.get("title") or f"Task {event.entity_id}")
    notifier.alert(
        subject=f"Task overdue: {title}",
        message=f"Task {event.entity_id} is overdue by {days_overdue} day(s).",
        severity="warning",
        recipient=assigned_to or None,
    )


def handle_sla_breach(event: Event, notifier) -> None:
    days = event.payload.get("days_overdue")
    notifier.escalate(
        subject=f"SLA breach on action {event.entity_id}",
        message=f"Compliance action is overdue by {days} day(s).",
        recipient=str(event.payload.get("owner") or "") or None,
    )


def handle_resource_conflict(event: Event, notifier) -> None:
    person = str(event.payload.get("person_name") or "unknown")
    conflicts = event.payload.get("conflicting_tasks")
    notifier.alert(
        subject=f"Resource conflict: {person}",
        message=f"Conflicting assignments detected: {conflicts}",
        severity="critical",
        recipient=str(event.payload.get("manager") or "") or None,
    )
