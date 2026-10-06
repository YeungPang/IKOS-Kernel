"""Resolve natural-language requests to registered workflow processes and activate them.

Ranking is deterministic and explainable: candidates are scored on token overlap against
the workflow registry plus taxonomy process hints, so an operator can see why a process
was chosen. Activation is deliberately refused when the top candidates are too close to
call, because starting the wrong business process is not a recoverable mistake.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import business_rules
import workflow_process_taxonomy

LOGGER = logging.getLogger("idms.nl_process_activation")

_STOPWORDS = {
    "a", "an", "and", "the", "for", "of", "to", "on", "in", "with", "please",
    "run", "start", "execute", "kick", "off", "do", "my", "our", "this", "that",
    "it", "is", "are", "be", "can", "you", "we", "i", "all", "new",
}


def _tokenize(text: Any) -> set[str]:
    tokens = re.findall(r"[a-z0-9]+", str(text or "").lower())
    return {token for token in tokens if token and token not in _STOPWORDS and len(token) > 1}


def score_registry_entry(
    text: str,
    entry: dict[str, Any],
    process_hints: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Score one workflow registry entry against the request text."""
    normalized_text = str(text or "").lower()
    text_tokens = _tokenize(text)

    workflow_key = str(entry.get("workflow_key") or "").strip()
    workflow_name = str(entry.get("workflow_name") or "").strip()
    description = str(entry.get("description") or "").strip()
    domain = str(entry.get("domain") or "").strip().lower()

    score = 0.0
    reasons: list[str] = []

    if workflow_key and workflow_key.lower() in normalized_text:
        score += 10.0
        reasons.append("workflow_key referenced explicitly")

    name_tokens = _tokenize(workflow_name) & text_tokens
    if name_tokens:
        score += 3.0 * len(name_tokens)
        reasons.append(f"name match: {', '.join(sorted(name_tokens))}")

    key_tokens = _tokenize(workflow_key.replace("::", " ")) & text_tokens
    if key_tokens:
        score += 2.0 * len(key_tokens)
        reasons.append(f"key match: {', '.join(sorted(key_tokens))}")

    description_tokens = _tokenize(description) & text_tokens
    if description_tokens:
        score += 1.0 * len(description_tokens)
        reasons.append(f"description match: {', '.join(sorted(description_tokens))}")

    matched_processes: list[str] = []
    for hint in process_hints or []:
        hint_domain = str(hint.get("domain") or "").strip().lower()
        if domain and hint_domain and hint_domain == domain:
            score += 2.0 * float(hint.get("confidence") or 0.0) * 5.0
            matched_processes.append(str(hint.get("process") or ""))

    if matched_processes:
        reasons.append(f"taxonomy domain match: {', '.join(sorted(set(matched_processes)))}")

    return {
        "workflow_id": entry.get("workflow_id"),
        "workflow_key": workflow_key,
        "workflow_name": workflow_name,
        "domain": domain or None,
        "score": round(score, 3),
        "confidence": min(0.99, round(score / 12.0, 3)),
        "matched_processes": sorted(set(p for p in matched_processes if p)),
        "reasons": reasons,
    }


def rank_workflow_candidates(
    text: str,
    registry_entries: list[dict[str, Any]],
    process_hints: list[dict[str, Any]] | None = None,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    """Rank registry entries for a request, dropping anything with no signal."""
    scored = [
        score_registry_entry(text, entry, process_hints)
        for entry in registry_entries
        if isinstance(entry, dict)
    ]
    scored = [item for item in scored if float(item.get("score") or 0.0) > 0.0]
    scored.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("workflow_key") or "")))
    return scored[: max(1, int(top_k or 5))]


def resolve_process(
    text: str,
    domain: str | None = None,
    top_k: int = 5,
    min_confidence: float = 0.3,
    ambiguity_margin: float = 0.15,
) -> dict[str, Any]:
    """Resolve an NL request to a ranked list of activatable workflow processes."""
    request_text = str(text or "").strip()
    if not request_text:
        return {"ok": False, "error": "text_required"}

    process_hints = workflow_process_taxonomy.suggest_workflow_processes(
        request_text, domain=domain, top_k=top_k
    )
    registry_entries = business_rules.list_solf_workflow_registry_entries(
        is_active=True, domain=domain, status=None, limit=500
    )
    candidates = rank_workflow_candidates(
        request_text, registry_entries, process_hints=process_hints, top_k=top_k
    )

    selected = candidates[0] if candidates else None
    runner_up = candidates[1] if len(candidates) > 1 else None

    blocked_reason: str | None = None
    if selected is None:
        blocked_reason = "no_matching_process"
    elif float(selected.get("confidence") or 0.0) < float(min_confidence):
        blocked_reason = "below_min_confidence"
    elif runner_up is not None and (
        float(selected.get("confidence") or 0.0) - float(runner_up.get("confidence") or 0.0)
    ) < float(ambiguity_margin):
        blocked_reason = "ambiguous_match"

    return {
        "ok": blocked_reason is None,
        "text": request_text,
        "process_hints": process_hints,
        "candidates": candidates,
        "selected": selected,
        "blocked_reason": blocked_reason,
        "requires_disambiguation": blocked_reason in {"ambiguous_match", "below_min_confidence"},
    }


def activate_process(
    text: str,
    input_context: dict[str, Any] | None = None,
    workflow_key: str | None = None,
    domain: str | None = None,
    started_by: str = "nl:user",
    top_k: int = 5,
    min_confidence: float = 0.3,
    ambiguity_margin: float = 0.15,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Resolve an NL request to a workflow and start its active version."""
    explicit_key = str(workflow_key or "").strip()
    if explicit_key:
        resolution = {
            "ok": True,
            "text": str(text or "").strip(),
            "process_hints": [],
            "candidates": [],
            "selected": {"workflow_key": explicit_key, "confidence": 1.0, "reasons": ["explicit workflow_key"]},
            "blocked_reason": None,
            "requires_disambiguation": False,
        }
    else:
        resolution = resolve_process(
            text,
            domain=domain,
            top_k=top_k,
            min_confidence=min_confidence,
            ambiguity_margin=ambiguity_margin,
        )

    if not resolution.get("ok"):
        return {**resolution, "activated": False}

    selected_key = str((resolution.get("selected") or {}).get("workflow_key") or "").strip()
    active_version = business_rules.get_solf_workflow_active_version_by_key(workflow_key=selected_key)
    if active_version is None or active_version.get("workflow_version_id") is None:
        return {
            **resolution,
            "activated": False,
            "ok": False,
            "blocked_reason": "no_active_version",
            "workflow_key": selected_key,
        }

    workflow_version_id = int(active_version.get("workflow_version_id"))
    if dry_run:
        return {
            **resolution,
            "activated": False,
            "dry_run": True,
            "workflow_key": selected_key,
            "workflow_version_id": workflow_version_id,
        }

    from workflow_pipeline_executor import WorkflowPipelineExecutor
    import object_db

    executor = WorkflowPipelineExecutor(db_connection_fn=object_db.get_connection)
    run = executor.start_pipeline_run(
        workflow_version_id=workflow_version_id,
        input_context=dict(input_context or {}),
        started_by=started_by,
        workflow_key=selected_key,
    )

    if isinstance(run, dict) and run.get("error"):
        return {
            **resolution,
            "activated": False,
            "ok": False,
            "blocked_reason": run.get("error"),
            "message": run.get("message"),
            "workflow_key": selected_key,
            "workflow_version_id": workflow_version_id,
        }

    return {
        **resolution,
        "activated": True,
        "workflow_key": selected_key,
        "workflow_version_id": workflow_version_id,
        "run": run,
    }
