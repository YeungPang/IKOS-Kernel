"""Event-driven workflow transition executor for Phase 2.

Maps domain events to state-machine transitions with optional SOLF policy mediation.
"""

from datetime import datetime
from typing import Any


class WorkflowTransitionExecutor:
    """Apply state transitions in response to emitted domain events."""

    def __init__(self, state_machine, event_bus, solf_interpreter=None):
        self.state_machine = state_machine
        self.event_bus = event_bus
        self.solf_interpreter = solf_interpreter

    def register_default_rules(self) -> None:
        self.event_bus.subscribe("task.overdue", self._on_task_overdue)
        self.event_bus.subscribe("workflow.approval_recorded", self._on_workflow_approval_recorded)
        self.event_bus.subscribe("compliance.deadline_breach", self._on_compliance_deadline_breach)
        self.event_bus.subscribe("workflow.case_sla_breach", self._on_workflow_case_sla_breach)

    def _policy_allows(self, payload: dict[str, Any]) -> dict[str, Any]:
        default = {
            "mode": "allow",
            "reason": "default_workflow_transition_policy",
            "allowed": True,
            "message": "",
        }
        if self.solf_interpreter is None:
            return self._harden_transition_policy(payload, default)
        try:
            result = self.solf_interpreter._invoke_clause("workflow_transition_policy", [payload])
        except Exception:
            return self._harden_transition_policy(payload, default)
        if not isinstance(result, dict):
            return self._harden_transition_policy(payload, default)
        mode = str(result.get("mode") or "allow").strip().lower()
        if mode not in {"allow", "deny", "escalate"}:
            mode = "allow"
        out = dict(result)
        out["mode"] = mode
        return self._harden_transition_policy(payload, out)

    def _harden_transition_policy(self, payload: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
        entity_type = str(payload.get("entity_type") or "").strip().lower()
        to_state = str(payload.get("to_state") or "").strip().lower()
        event = str(payload.get("event") or "").strip().lower()
        actor_ref = str(payload.get("actor_ref") or "").strip().lower()

        if entity_type == "workflow_case" and to_state in {"approved", "rejected"} and event != "approval_recorded":
            return {
                "mode": "deny",
                "reason": "workflow_case_terminal_state_requires_approval_event",
                "allowed": False,
                "message": "workflow_case can move to approved/rejected only from approval_recorded event",
            }

        if entity_type == "project_task" and to_state == "done" and event != "task_completed":
            return {
                "mode": "deny",
                "reason": "project_task_done_requires_completion_event",
                "allowed": False,
                "message": "project_task can move to done only with task_completed event",
            }

        if entity_type == "compliance_action" and to_state == "escalated" and event == "compliance_deadline_breach":
            hardened = dict(policy)
            hardened["mode"] = "escalate"
            hardened["reason"] = str(hardened.get("reason") or "compliance_deadline_breach_escalation")
            hardened["allowed"] = True
            return hardened

        if entity_type == "workflow_case" and to_state in {"approved", "rejected"} and actor_ref == "system":
            return {
                "mode": "deny",
                "reason": "manual_terminal_transition_not_allowed",
                "allowed": False,
                "message": "manual transition to terminal workflow_case state is not allowed",
            }

        return policy

    def _apply_transition(
        self,
        entity_type: str,
        entity_id: int,
        to_state: str,
        event: str,
        actor_ref: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        policy_payload = {
            "entity_type": str(entity_type).strip().lower(),
            "entity_id": int(entity_id),
            "to_state": str(to_state).strip().lower(),
            "event": str(event).strip().lower(),
            "actor_ref": str(actor_ref).strip().lower(),
            "metadata": dict(metadata or {}),
            "automatic": True,
        }
        policy = self._policy_allows(policy_payload)
        if str(policy.get("mode") or "allow").lower() == "deny":
            return {
                "success": False,
                "policy": policy,
                "message": str(policy.get("message") or "Transition denied by policy."),
            }

        result = self.state_machine.transition(
            entity_type=entity_type,
            entity_id=entity_id,
            to_state=to_state,
            event=event,
            actor_ref=actor_ref,
            metadata=metadata or {},
        )

        if result.success:
            self.event_bus.emit(
                event_type="workflow.state_changed",
                source_entity=str(entity_type).strip().lower(),
                entity_id=int(entity_id),
                payload={
                    "from_state": result.from_state,
                    "to_state": result.to_state,
                    "event": str(event).strip().lower(),
                    "actor_ref": str(actor_ref).strip().lower(),
                    "automatic": True,
                },
                tags=["state_machine", "automatic", str(entity_type).strip().lower()],
            )

        return {
            "success": bool(result.success),
            "allowed": bool(result.allowed),
            "reason": result.reason,
            "from_state": result.from_state,
            "to_state": result.to_state,
            "event": result.event,
            "policy": policy,
        }

    def _on_task_overdue(self, event_obj) -> None:
        task_id = int(event_obj.entity_id)
        self._apply_transition(
            entity_type="project_task",
            entity_id=task_id,
            to_state="blocked",
            event="task_overdue",
            actor_ref="automation",
            metadata={
                "source_event": event_obj.event_type,
                "detected_at": datetime.utcnow().isoformat(),
            },
        )

    def _on_workflow_approval_recorded(self, event_obj) -> None:
        payload = event_obj.payload or {}
        case_ref = payload.get("case_ref")
        decision = str(payload.get("decision") or "").strip().lower()
        if case_ref is None or decision not in {"approved", "rejected"}:
            return

        self._apply_transition(
            entity_type="workflow_case",
            entity_id=int(case_ref),
            to_state=decision,
            event="approval_recorded",
            actor_ref="automation",
            metadata={
                "source_event": event_obj.event_type,
                "approval_entity_id": int(event_obj.entity_id),
            },
        )

    def _on_compliance_deadline_breach(self, event_obj) -> None:
        self._apply_transition(
            entity_type="compliance_action",
            entity_id=int(event_obj.entity_id),
            to_state="escalated",
            event="compliance_deadline_breach",
            actor_ref="automation",
            metadata={
                "source_event": event_obj.event_type,
                "detected_at": datetime.utcnow().isoformat(),
            },
        )

    def _on_workflow_case_sla_breach(self, event_obj) -> None:
        self._apply_transition(
            entity_type="workflow_case",
            entity_id=int(event_obj.entity_id),
            to_state="escalated",
            event="workflow_case_sla_breach",
            actor_ref="automation",
            metadata={
                "source_event": event_obj.event_type,
                "detected_at": datetime.utcnow().isoformat(),
            },
        )
