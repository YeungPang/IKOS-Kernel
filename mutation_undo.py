"""Journal-driven undo for recorded mutations.

Reads workflow_mutation_journal entries, inspects the inverse_action captured when
each mutation was applied, and executes the matching compensation in reverse order.

Dispatch is keyed on inverse_action["kind"] against a fixed handler registry. The
module/function strings stored in the journal are treated as documentation only and
are never imported, so a tampered journal row cannot execute arbitrary code.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import object_db
from jsonschema import Draft202012Validator, FormatChecker
from security_context import SecurityContext, get_security_context

LOGGER = logging.getLogger("idms.mutation_undo")


def _reject_capability_authority(value: Any) -> None:
    """Configuration/journal data cannot supply caller authority, even nested."""
    if isinstance(value, dict):
        if {"tenant_id", "user_id", "permissions"}.intersection(value):
            raise ValueError("capability payload cannot override tenant_id/user_id/permissions")
        for child in value.values():
            _reject_capability_authority(child)
    elif isinstance(value, list):
        for child in value:
            _reject_capability_authority(child)


def _validate_capability_action(inverse_action: dict[str, Any]) -> None:
    """Resolve a fixed registration and validate without executing (also for plans)."""
    from capability_registry import CAPABILITIES

    metadata = CAPABILITIES.resolve(inverse_action.get("key"), inverse_action.get("version"))
    caller = get_security_context(required=True)
    if not isinstance(caller, SecurityContext) or not isinstance(caller.tenant_id, str) or not caller.tenant_id.strip():
        raise PermissionError("An active capability tenant is required")
    if (not isinstance(caller.permissions, (set, frozenset))
            or any(not isinstance(p, str) for p in caller.permissions)
            or not caller.allows(metadata["permission"])):
        raise PermissionError(f"Missing capability permission: {metadata['permission']}")
    payload = inverse_action.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("capability inverse payload must be an object")
    _reject_capability_authority(payload)
    Draft202012Validator(metadata["input_schema"], format_checker=FormatChecker()).validate(payload)


def _handle_capability(
    connection: Any, *, inverse_action: dict[str, Any], entry: dict[str, Any],
    requested_by: str, reason: str | None,
) -> dict[str, Any]:
    from capability_registry import CAPABILITIES

    _validate_capability_action(inverse_action)
    # execute rechecks the caller; requested_by and journal metadata grant no authority.
    result = CAPABILITIES.execute(inverse_action["key"], inverse_action["version"], inverse_action["payload"])
    if isinstance(result, dict) and result.get("ok") is False:
        return {"ok": False, "status": "failed", "detail": str(result.get("reason") or result.get("error") or "capability compensation failed"), "result": result}
    return {"ok": True, "status": "undone", "detail": "capability compensation executed", "result": result}


def _handle_reverse_transaction(
    connection: Any,
    *,
    inverse_action: dict[str, Any],
    entry: dict[str, Any],
    requested_by: str,
    reason: str | None,
) -> dict[str, Any]:
    import domain_db

    transaction_id = str(
        inverse_action.get("transaction_id") or entry.get("business_key") or ""
    ).strip()
    if not transaction_id:
        return {"ok": False, "status": "failed", "detail": "inverse_action is missing transaction_id"}

    result = domain_db.reverse_transaction(
        connection,
        transaction_id=transaction_id,
        reason=reason or "journal_undo",
        reversed_by=requested_by,
    )
    if bool(result.get("ok")):
        return {
            "ok": True,
            "status": "undone",
            "detail": f"posted reversal {result.get('reversal_transaction_id')}",
            "result": result,
        }

    failure_reason = str(result.get("reason") or "").strip()
    if failure_reason in {"already_reversed", "already_a_reversal"}:
        return {"ok": True, "status": "already_undone", "detail": failure_reason, "result": result}

    return {"ok": False, "status": "failed", "detail": failure_reason or "reversal failed", "result": result}


def _handle_cancel_accounting_booking_review(
    connection: Any,
    *,
    inverse_action: dict[str, Any],
    entry: dict[str, Any],
    requested_by: str,
    reason: str | None,
) -> dict[str, Any]:
    import domain_db

    review_id = int(inverse_action.get("review_id") or 0)
    if review_id <= 0:
        return {"ok": False, "status": "failed", "detail": "inverse_action is missing review_id"}

    result = domain_db.cancel_accounting_booking_review(
        connection,
        review_id=review_id,
        reviewed_by=requested_by,
        review_note=reason or "journal_undo",
    )
    if bool(result.get("ok")):
        return {"ok": True, "status": "undone", "detail": f"cancelled review {review_id}", "result": result}

    failure_reason = str(result.get("reason") or "").strip()
    if failure_reason == "not_pending":
        return {"ok": True, "status": "already_undone", "detail": failure_reason, "result": result}

    return {"ok": False, "status": "failed", "detail": failure_reason or "cancel failed", "result": result}


InverseHandler = Callable[..., dict[str, Any]]

INVERSE_ACTION_HANDLERS: dict[str, InverseHandler] = {
    "capability": _handle_capability,
    "reverse_transaction": _handle_reverse_transaction,
    "cancel_accounting_booking_review": _handle_cancel_accounting_booking_review,
}


def get_supported_inverse_actions() -> list[str]:
    return sorted(kind for kind in INVERSE_ACTION_HANDLERS if _handler_available(kind))


def _handler_available(kind: str) -> bool:
    if kind == "capability":
        try:
            from capability_registry import CAPABILITIES
            return bool(CAPABILITIES.inventory())
        except ImportError:
            return False
    if kind in {"reverse_transaction", "cancel_accounting_booking_review"}:
        try:
            import domain_db
            return callable(getattr(domain_db, kind, None))
        except ImportError:
            return False
    return kind in INVERSE_ACTION_HANDLERS


def _plan_entry(entry: dict[str, Any]) -> dict[str, Any]:
    inverse_action = entry.get("inverse_action") if isinstance(entry.get("inverse_action"), dict) else {}
    kind = str(inverse_action.get("kind") or "").strip()

    plan = {
        "journal_id": entry.get("journal_id"),
        "operation_kind": entry.get("operation_kind"),
        "business_key": entry.get("business_key"),
        "step_key": entry.get("step_key"),
        "inverse_kind": kind or None,
    }

    if not kind:
        plan.update({"executable": False, "status": "skipped", "reason": "no_inverse_action"})
    elif kind not in INVERSE_ACTION_HANDLERS:
        plan.update({"executable": False, "status": "skipped", "reason": "unsupported_inverse_action"})
    else:
        try:
            if kind == "capability":
                _validate_capability_action(inverse_action)
            elif not _handler_available(kind):
                raise LookupError("Fixed compensation handler is unavailable")
        except Exception as exc:
            plan.update({"executable": False, "status": "failed", "reason": str(exc)})
        else:
            plan.update({"executable": True, "status": "planned", "reason": None})
    return plan


def undo_journal_entry(
    connection: Any,
    journal_id: int,
    requested_by: str = "api:user",
    reason: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Undo a single recorded mutation using its captured inverse action."""
    entry = object_db.get_workflow_mutation_journal_entry(connection, journal_id)
    if entry is None:
        return {"ok": False, "error": "not_found", "journal_id": int(journal_id)}

    if str(entry.get("status") or "").strip().lower() == "undone":
        return {
            "ok": True,
            "journal_id": int(journal_id),
            "status": "already_undone",
            "detail": "journal entry already marked undone",
        }

    plan = _plan_entry(entry)
    if not plan.get("executable"):
        return {"ok": False, "journal_id": int(journal_id), **plan}

    if dry_run:
        return {"ok": True, "dry_run": True, "journal_id": int(journal_id), **plan}

    inverse_action = entry.get("inverse_action") or {}
    handler = INVERSE_ACTION_HANDLERS[str(inverse_action.get("kind"))]

    try:
        outcome = handler(
            connection,
            inverse_action=inverse_action,
            entry=entry,
            requested_by=requested_by,
            reason=reason,
        )
    except Exception as exc:
        LOGGER.exception("Undo handler failed for journal_id=%s", journal_id)
        outcome = {"ok": False, "status": "failed", "detail": str(exc)}

    object_db.mark_workflow_mutation_undone(
        connection,
        journal_id=int(journal_id),
        undone_by=requested_by,
        undo_error=None if outcome.get("ok") else str(outcome.get("detail") or "undo failed"),
        status="undone" if outcome.get("ok") else "undo_failed",
    )

    return {
        "ok": bool(outcome.get("ok")),
        "journal_id": int(journal_id),
        "inverse_kind": inverse_action.get("kind"),
        "status": outcome.get("status"),
        "detail": outcome.get("detail"),
        "result": outcome.get("result"),
    }


def undo_by_business_key(
    connection: Any,
    business_key: str,
    requested_by: str = "api:user",
    reason: str | None = None,
    dry_run: bool = False,
    operation_kind: str | None = None,
    target_entity: str | None = None,
    stop_on_error: bool = True,
    limit: int = 200,
) -> dict[str, Any]:
    """Resolve every mutation recorded for a business key and undo them newest-first."""
    key = str(business_key or "").strip()
    if not key:
        return {"ok": False, "error": "business_key_required"}

    entries = object_db.find_workflow_mutation_journal(
        connection,
        business_key=key,
        target_entity=target_entity,
        operation_kind=operation_kind,
        include_undone=False,
        limit=limit,
    )
    # find_workflow_mutation_journal already returns newest-first, which is undo order.
    actions: list[dict[str, Any]] = []

    for entry in entries:
        plan = _plan_entry(entry)
        if not plan.get("executable"):
            actions.append(plan)
            continue

        if dry_run:
            actions.append(plan)
            continue

        outcome = undo_journal_entry(
            connection,
            journal_id=int(entry.get("journal_id") or 0),
            requested_by=requested_by,
            reason=reason,
        )
        actions.append(
            {
                **plan,
                "status": outcome.get("status"),
                "detail": outcome.get("detail"),
                "ok": outcome.get("ok"),
            }
        )
        if not outcome.get("ok") and stop_on_error:
            break

    def _count(status: str) -> int:
        return len([item for item in actions if item.get("status") == status])

    return {
        "ok": all(item.get("status") != "failed" for item in actions),
        "business_key": key,
        "dry_run": bool(dry_run),
        "requested_by": requested_by,
        "matched": len(entries),
        "actions": actions,
        "summary": {
            "planned": _count("planned"),
            "undone": _count("undone"),
            "already_undone": _count("already_undone"),
            "skipped": _count("skipped"),
            "failed": _count("failed"),
        },
    }
