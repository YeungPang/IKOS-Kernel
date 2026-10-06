"""Registry-driven natural-language action and workflow resolution."""

from __future__ import annotations

import re
from typing import Any, Callable

_ACTION_VERBS = {
    "approve", "calculate", "cancel", "carry", "create", "execute", "generate",
    "prepare", "run", "start", "trigger", "update", "assign", "close", "ingest",
}
_STOPWORDS = {
    "a", "an", "and", "are", "do", "for", "from", "in", "into", "new", "of",
    "on", "please", "the", "this", "to", "with",
}


def _tokens(value: Any) -> set[str]:
    return {
        token for token in re.findall(r"[a-z0-9]+", str(value or "").lower())
        if len(token) > 1 and token not in _STOPWORDS
    }


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def interpret_action(text: str) -> dict[str, Any]:
    """Create a conservative Action Object from imperative text."""
    raw = str(text or "").strip()
    lowered = raw.lower()
    verb_match = re.search(r"\b(" + "|".join(sorted(_ACTION_VERBS, key=len, reverse=True)) + r")\b", lowered)
    action_type = verb_match.group(1) if verb_match else None
    if action_type == "carry":
        action_type = "execute"

    entities: list[str] = []
    for match in re.finditer(r"\b(?:for|from|of|to)\s+([A-Z][A-Za-z0-9&.' -]{1,80})", raw):
        candidate = re.split(r"\s+(?:with|and|on|using|please)\s+", match.group(1), maxsplit=1, flags=re.IGNORECASE)[0].strip(" .,;:")
        if candidate and candidate not in entities:
            entities.append(candidate)

    parameters: dict[str, Any] = {}
    for name, value in re.findall(r"\b([a-z][a-z0-9_]*)\s*[:=]\s*([^,;]+)", raw, flags=re.IGNORECASE):
        parameters[name.lower()] = value.strip()

    return {
        "intent": "action" if action_type else "unknown",
        "action_type": action_type,
        "action_name": None,
        "workflow_key": None,
        "entities": entities,
        "parameters": parameters,
        "raw": raw,
    }


def _workflow_terms(entry: dict[str, Any]) -> tuple[set[str], list[str], set[str]]:
    metadata = entry.get("metadata") if isinstance(entry.get("metadata"), dict) else {}
    aliases = _as_list(metadata.get("aliases")) + _as_list(metadata.get("trigger_phrases"))
    action_types = {
        str(item).strip().lower()
        for item in _as_list(metadata.get("action_types"))
        if str(item).strip()
    }
    terms = _tokens(" ".join([
        str(entry.get("workflow_key") or "").replace("::", " "),
        str(entry.get("workflow_name") or ""),
        str(entry.get("description") or ""),
        " ".join(str(item) for item in aliases),
    ]))
    return terms, [str(item) for item in aliases], action_types


def _required_parameters(input_contract: Any) -> list[dict[str, Any]]:
    contract = input_contract if isinstance(input_contract, dict) else {}
    raw = contract.get("required_parameters", contract.get("required", []))
    if isinstance(raw, dict):
        return [
            {"name": str(name), **(spec if isinstance(spec, dict) else {"type": str(spec)})}
            for name, spec in raw.items()
        ]
    return [{"name": str(item)} if not isinstance(item, dict) else item for item in _as_list(raw)]


def _provided_parameter_names(action: dict[str, Any]) -> set[str]:
    provided = {str(key).strip().lower() for key in (action.get("parameters") or {})}
    for entity in action.get("entities") or []:
        provided.add(str(entity).strip().lower())
    return provided


def resolve_workflow(
    action: dict[str, Any],
    registry_entries: list[dict[str, Any]],
    version_loader: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    """Resolve an Action Object against active registry entries only."""
    request = str(action.get("raw") or "")
    request_tokens = _tokens(request)
    candidates: list[dict[str, Any]] = []

    for entry in registry_entries:
        if not isinstance(entry, dict) or not entry.get("is_active", True):
            continue
        terms, aliases, action_types = _workflow_terms(entry)
        score = float(len(request_tokens & terms))
        reasons: list[str] = []
        if action.get("action_type") in action_types:
            score += 4.0
            reasons.append("action_type_match")
        if any(alias.lower() in request.lower() for alias in aliases if alias):
            score += 8.0
            reasons.append("trigger_phrase_match")
        key = str(entry.get("workflow_key") or "").strip()
        if key and key.lower() in request.lower():
            score += 12.0
            reasons.append("workflow_key_match")
        if score <= 0:
            continue
        candidates.append({
            "workflow_id": entry.get("workflow_id"),
            "workflow_key": key,
            "workflow_name": entry.get("workflow_name"),
            "description": entry.get("description") or "",
            "domain": entry.get("domain"),
            "score": round(score, 3),
            "reasons": reasons,
            "entry": entry,
        })

    candidates.sort(key=lambda item: (-float(item["score"]), str(item["workflow_key"])))
    selected = candidates[0] if candidates else None
    runner_up = candidates[1] if len(candidates) > 1 else None
    if selected is None:
        return {**action, "workflow": None, "candidates": [], "missing_parameters": [], "execution_plan": [], "resolved": False, "reason": "no_registered_workflow_match"}
    if runner_up and float(selected["score"]) == float(runner_up["score"]):
        return {**action, "workflow": None, "candidates": candidates[:5], "missing_parameters": [], "execution_plan": [], "resolved": False, "reason": "ambiguous_registered_workflow"}

    version = version_loader(selected["entry"]) if version_loader else None
    version = version if isinstance(version, dict) else {}
    required = _required_parameters(version.get("input_contract"))
    provided = _provided_parameter_names(action)
    missing = [item for item in required if str(item.get("name") or "").strip().lower() not in provided]
    steps = version.get("steps") if isinstance(version.get("steps"), list) else []
    execution_plan = [
        {
            "step": item.get("step_key") or item.get("step_order"),
            "step_type": item.get("step_kind"),
            "action": item.get("operation") or item.get("clause_name"),
            "requires_user_input": bool((item.get("config") or {}).get("requires_user_input")),
        }
        for item in steps if isinstance(item, dict)
    ]
    resolved_workflow = {
        key: selected.get(key)
        for key in ("workflow_id", "workflow_key", "workflow_name", "description", "domain")
    }
    return {
        **action,
        "workflow_key": selected.get("workflow_key"),
        "workflow_name": selected.get("workflow_name"),
        "workflow": resolved_workflow,
        "workflow_version_id": version.get("workflow_version_id"),
        "required_parameters": required,
        "missing_parameters": missing,
        "candidates": [{key: value for key, value in item.items() if key != "entry"} for item in candidates[:5]],
        "execution_plan": execution_plan,
        "resolved": True,
        "requires_confirmation": True,
        "reason": "; ".join(selected.get("reasons") or []) or "registered workflow match",
    }


def resolve_action_workflow(
    text: str,
    registry_entries: list[dict[str, Any]],
    version_loader: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    return resolve_workflow(interpret_action(text), registry_entries, version_loader)
