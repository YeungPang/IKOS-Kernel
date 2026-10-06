"""Workflow state machine model and transition APIs for Phase 2.

Provides deterministic state validation and audited transitions for key domain entities.
"""

from dataclasses import dataclass
from typing import Any

from psycopg2.extras import Json


@dataclass
class TransitionResult:
    success: bool
    entity_type: str
    entity_id: int
    from_state: str | None
    to_state: str | None
    event: str
    reason: str = ""
    allowed: bool = False
    data: dict[str, Any] | None = None


class WorkflowStateMachine:
    """DB-backed state machine for workflow entities."""

    _ENTITY_CONFIG: dict[str, dict[str, Any]] = {
        "workflow_case": {
            "table": "workflow_cases",
            "pk": "id",
            "state_col": "status",
            "step_col": "current_step",
            "default_state": "open",
            "transitions": {
                "open": {"in_review", "escalated", "cancelled"},
                "in_review": {"approved", "rejected", "escalated", "cancelled"},
                "escalated": {"in_review", "approved", "rejected", "cancelled"},
                "approved": set(),
                "rejected": set(),
                "cancelled": set(),
            },
        },
        "compliance_action": {
            "table": "compliance_actions",
            "pk": "id",
            "state_col": "status",
            "step_col": None,
            "default_state": "open",
            "transitions": {
                "open": {"in_progress", "submitted", "escalated", "cancelled"},
                "in_progress": {"submitted", "completed", "escalated", "cancelled"},
                "submitted": {"completed", "escalated", "cancelled"},
                "escalated": {"in_progress", "submitted", "completed", "cancelled"},
                "completed": {"closed"},
                "closed": set(),
                "cancelled": set(),
            },
        },
        "project_task": {
            "table": "project_tasks",
            "pk": "id",
            "state_col": "status",
            "step_col": None,
            "default_state": "open",
            "transitions": {
                "open": {"in_progress", "blocked", "cancelled"},
                "in_progress": {"blocked", "done", "cancelled"},
                "blocked": {"in_progress", "cancelled"},
                "done": set(),
                "cancelled": set(),
            },
        },
    }

    def __init__(self, db_connection_fn):
        self.db_connection_fn = db_connection_fn
        self.ensure_tables()

    def ensure_tables(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS workflow_state_transition_log (
            id BIGSERIAL PRIMARY KEY,
            entity_type TEXT NOT NULL,
            entity_id BIGINT NOT NULL,
            from_state TEXT,
            to_state TEXT,
            transition_event TEXT NOT NULL,
            actor_ref TEXT,
            allowed BOOLEAN NOT NULL,
            reason TEXT,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_state_log_entity ON workflow_state_transition_log(entity_type, entity_id);
        CREATE INDEX IF NOT EXISTS idx_state_log_allowed ON workflow_state_transition_log(allowed);
        CREATE INDEX IF NOT EXISTS idx_state_log_created ON workflow_state_transition_log(created_at);
        """
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)

    def _get_config(self, entity_type: str) -> dict[str, Any]:
        key = str(entity_type or "").strip().lower()
        cfg = self._ENTITY_CONFIG.get(key)
        if cfg is None:
            raise ValueError(f"Unsupported entity_type: {entity_type}")
        return cfg

    def get_state(self, entity_type: str, entity_id: int) -> dict[str, Any]:
        cfg = self._get_config(entity_type)
        sql = f"SELECT {cfg['state_col']} FROM {cfg['table']} WHERE {cfg['pk']}=%s"
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(entity_id),))
                row = cur.fetchone()
        if row is None:
            return {
                "success": False,
                "entity_type": str(entity_type).strip().lower(),
                "entity_id": int(entity_id),
                "message": "Entity not found.",
            }
        state = str(row[0] or cfg["default_state"]).strip().lower()
        return {
            "success": True,
            "entity_type": str(entity_type).strip().lower(),
            "entity_id": int(entity_id),
            "state": state,
        }

    def can_transition(self, entity_type: str, entity_id: int, to_state: str) -> TransitionResult:
        current = self.get_state(entity_type, entity_id)
        if not current.get("success"):
            return TransitionResult(
                success=False,
                entity_type=str(entity_type).strip().lower(),
                entity_id=int(entity_id),
                from_state=None,
                to_state=str(to_state or "").strip().lower() or None,
                event="check",
                reason=str(current.get("message") or "Entity not found."),
                allowed=False,
            )

        cfg = self._get_config(entity_type)
        from_state = str(current.get("state") or cfg["default_state"]).strip().lower()
        target = str(to_state or "").strip().lower()
        allowed_targets = cfg["transitions"].get(from_state, set())
        allowed = target in allowed_targets
        reason = "allowed" if allowed else f"Invalid transition from {from_state} to {target}"

        return TransitionResult(
            success=allowed,
            entity_type=str(entity_type).strip().lower(),
            entity_id=int(entity_id),
            from_state=from_state,
            to_state=target,
            event="check",
            reason=reason,
            allowed=allowed,
            data={"allowed_targets": sorted(allowed_targets)},
        )

    def transition(
        self,
        entity_type: str,
        entity_id: int,
        to_state: str,
        event: str,
        actor_ref: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TransitionResult:
        check = self.can_transition(entity_type, entity_id, to_state)
        if not check.allowed:
            self._log_transition(
                entity_type=check.entity_type,
                entity_id=check.entity_id,
                from_state=check.from_state,
                to_state=check.to_state,
                event=event,
                actor_ref=actor_ref,
                allowed=False,
                reason=check.reason,
                metadata=metadata or {},
            )
            return check

        cfg = self._get_config(entity_type)
        target = str(to_state or "").strip().lower()
        step_col = cfg.get("step_col")

        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                if step_col:
                    cur.execute(
                        f"UPDATE {cfg['table']} SET {cfg['state_col']}=%s, {step_col}=%s, updated_at=NOW() WHERE {cfg['pk']}=%s RETURNING {cfg['pk']}",
                        (target, target, int(entity_id)),
                    )
                else:
                    cur.execute(
                        f"UPDATE {cfg['table']} SET {cfg['state_col']}=%s, updated_at=NOW() WHERE {cfg['pk']}=%s RETURNING {cfg['pk']}",
                        (target, int(entity_id)),
                    )
                row = cur.fetchone()
                if row is None:
                    return TransitionResult(
                        success=False,
                        entity_type=check.entity_type,
                        entity_id=check.entity_id,
                        from_state=check.from_state,
                        to_state=target,
                        event=str(event or "transition"),
                        reason="Entity not found during update.",
                        allowed=False,
                    )

        self._log_transition(
            entity_type=check.entity_type,
            entity_id=check.entity_id,
            from_state=check.from_state,
            to_state=target,
            event=event,
            actor_ref=actor_ref,
            allowed=True,
            reason="transition_applied",
            metadata=metadata or {},
        )

        return TransitionResult(
            success=True,
            entity_type=check.entity_type,
            entity_id=check.entity_id,
            from_state=check.from_state,
            to_state=target,
            event=str(event or "transition"),
            reason="transition_applied",
            allowed=True,
        )

    def history(self, entity_type: str, entity_id: int, limit: int = 20) -> list[dict[str, Any]]:
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, entity_type, entity_id, from_state, to_state, transition_event, actor_ref, allowed, reason, metadata, created_at
                    FROM workflow_state_transition_log
                    WHERE entity_type=%s AND entity_id=%s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (str(entity_type).strip().lower(), int(entity_id), int(max(1, limit))),
                )
                rows = cur.fetchall() or []

        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "entity_type": r[1],
                    "entity_id": int(r[2]),
                    "from_state": r[3],
                    "to_state": r[4],
                    "event": r[5],
                    "actor_ref": r[6],
                    "allowed": bool(r[7]),
                    "reason": r[8],
                    "metadata": r[9] or {},
                    "created_at": str(r[10]),
                }
            )
        return out

    def _log_transition(
        self,
        entity_type: str,
        entity_id: int,
        from_state: str | None,
        to_state: str | None,
        event: str,
        actor_ref: str | None,
        allowed: bool,
        reason: str,
        metadata: dict[str, Any],
    ) -> None:
        with self.db_connection_fn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO workflow_state_transition_log (
                        entity_type, entity_id, from_state, to_state,
                        transition_event, actor_ref, allowed, reason, metadata
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(entity_type).strip().lower(),
                        int(entity_id),
                        from_state,
                        to_state,
                        str(event or "transition").strip().lower(),
                        str(actor_ref or "system").strip().lower(),
                        bool(allowed),
                        str(reason or ""),
                        Json(metadata or {}),
                    ),
                )
