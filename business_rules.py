from __future__ import annotations

import importlib
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from psycopg2.extras import Json

import object_db
from ikos_config import CLASSIFY_MODEL, EXTRACT_MODEL
from llm_fallback import generate_content_with_openrouter_fallback, get_openrouter_client
from pattern_library import PatternLibrary
from workflow_process_taxonomy import canonicalize_control_flow, canonicalize_workflow_process

try:
    import solf_parser
except Exception:  # pragma: no cover - parser import is optional in some runtimes
    solf_parser = None


INFERENCE_MAX_RULES = int(os.getenv("IDMS_INFERENCE_MAX_RULES", "25"))
INFERENCE_MAX_DIRECTIVES_PER_RULE = int(os.getenv("IDMS_INFERENCE_MAX_DIRECTIVES_PER_RULE", "8"))
INFERENCE_MAX_WORKFLOW_PROCESS_ADDITIONS = int(os.getenv("IDMS_INFERENCE_MAX_WORKFLOW_PROCESS_ADDITIONS", "10"))


def _slug(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _normalize_attr_token(value: str) -> str:
    token = _slug(value)
    alias_map = {
        "registration_number": "registration_no",
        "registration_no": "registration_no",
        "company_registration_number": "registration_no",
        "vat_number": "vat_no",
        "vat_no": "vat_no",
        "mwst_number": "vat_no",
        "mwst_no": "vat_no",
        "must_number": "vat_no",
        "must_no": "vat_no",
        "ust_id": "vat_no",
        "ust_idnr": "vat_no",
        "tax_id": "registration_no",
        # Runtime booking aliases from natural language.
        "account": "booking_debit_account_name",
        "debit_account": "booking_debit_account_name",
        "expense_account": "booking_debit_account_name",
        "booking_account": "booking_debit_account_name",
        "particulars": "booking_particulars",
        "line_particulars": "booking_particulars",
        "legal_entity": "legal_entity_ref",
        "legal_entity_reference": "legal_entity_ref",
        "company_reference": "company_ref",
        "employer_reference": "employer_ref",
        "current_employer_reference": "current_employer_ref",
    }
    return alias_map.get(token, token)


ACTIONABLE_RULE_ATTRIBUTE_KEYS = {
    "booking_debit_account_name",
    "booking_debit_account_number",
    "booking_particulars",
    "expense_account_name",
    "account_name",
    "legal_entity_ref",
    "company_ref",
    "employer_ref",
    "current_employer_ref",
}

REFERENCE_ATTRIBUTE_KEYS = {
    "legal_entity_ref",
    "company_ref",
    "employer_ref",
    "current_employer_ref",
    "person_ref",
    "employee_ref",
    "payer_ref",
    "owner_ref",
    "submitted_by_ref",
    "entered_by_ref",
}

ACTIONABLE_RULE_ATTRIBUTE_KEY_DESCRIPTIONS = {
    "booking_debit_account_name": "Name of the debit-side booking account, for example travelling expenses.",
    "booking_debit_account_number": "Number of the debit-side booking account, for example 6500.",
    "booking_particulars": "Booking memo or line particulars describing the reason or purpose of the booking.",
    "expense_account_name": "Expense account name used for expense classification.",
    "account_name": "General account name when rule text refers to account naming.",
    "legal_entity_ref": "Legal entity that the document or booking belongs to.",
    "company_ref": "Company reference associated with the booking or document.",
    "employer_ref": "Employer reference when a booking belongs to an employer context.",
    "current_employer_ref": "Current employer reference in employment-related booking contexts.",
}


class BusinessRuleClarificationNeeded(ValueError):
    def __init__(self, clarification: dict[str, Any]):
        self.clarification = clarification if isinstance(clarification, dict) else {}
        message = str(
            self.clarification.get("message")
            or self.clarification.get("clarification_question")
            or "Business rule requires clarification"
        )
        super().__init__(message)


def _build_booking_party_clarification(rule_text: str, directives: list[dict[str, Any]]) -> dict[str, Any] | None:
    text = str(rule_text or "").strip()
    if not text:
        return None

    if any(
        _normalize_attr_token(str(key or "")) in ACTIONABLE_RULE_ATTRIBUTE_KEYS
        for directive in directives
        if isinstance(directive, dict) and str(directive.get("target_scope") or "entity").strip().lower() == "entity"
        for key in (directive.get("set_attributes") or {})
        if isinstance(directive.get("set_attributes"), dict)
    ):
        return None

    if re.search(r"\blegal\s+entity\b|\blegal_entity_ref\b|\bcompany\s+reference\b|\bcompany_ref\b", text, flags=re.IGNORECASE):
        return None

    match = re.search(
        r"\b(?:booking|book(?:ed|ing)?)\s+(?:for|to|under)\s+(?:the\s+)?([A-Za-z0-9&().,'\-/ ]{2,120}?)(?=\s+(?:should|must|be|to|as|into|on|with|and|then)\b|[\.;,]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    target = re.sub(r"\s+", " ", str(match.group(1) or "")).strip(" ,.;")
    if not target:
        return None

    return {
        "type": "booking_party_clarification",
        "reason_code": "ambiguous_booking_party",
        "clarification_question": f"Should legal_entity_ref and company_ref be set to {target} for the booking?",
        "message": f"Clarification needed for booking party '{target}'.",
        "inferred_value": target,
        "expected_attributes": ["legal_entity_ref", "company_ref"],
        "suggested_set_attributes": {
            "legal_entity_ref": target,
            "company_ref": target,
        },
    }


def _raise_for_non_actionable_rule(actionability: dict[str, Any], *, operation: str) -> None:
    warnings = actionability.get("warnings") if isinstance(actionability.get("warnings"), list) else []
    clarifications = actionability.get("clarifications") if isinstance(actionability.get("clarifications"), list) else []
    if clarifications:
        raise BusinessRuleClarificationNeeded(clarifications[0])
    blocking = [item for item in warnings if isinstance(item, dict) and str(item.get("type") or "") in {"non_actionable_set_attribute", "document_scope_set_attributes"}]
    if not blocking:
        return

    details: list[str] = []
    for item in blocking:
        warning_type = str(item.get("type") or "").strip()
        if warning_type == "non_actionable_set_attribute":
            attribute = str(item.get("attribute") or "").strip()
            suggestions = item.get("suggestions") if isinstance(item.get("suggestions"), list) else []
            if suggestions:
                details.append(f"unsupported set_attributes key '{attribute}' (did you mean: {', '.join(str(s) for s in suggestions[:3])})")
            else:
                details.append(f"unsupported set_attributes key '{attribute}'")
        elif warning_type == "document_scope_set_attributes":
            keys = item.get("keys") if isinstance(item.get("keys"), list) else []
            if keys:
                details.append(
                    "document-scope set_attributes will not reliably drive entity-level workflows; "
                    f"move these keys to entity scope: {', '.join(str(k) for k in keys)}. "
                    "Clarification needed: should these overrides apply to invoice/receipt/bill entities, "
                    "or only to document metadata/workflow?"
                )
            else:
                details.append(
                    "document-scope set_attributes will not reliably drive entity-level workflows. "
                    "Clarification needed: should these overrides apply to invoice/receipt/bill entities, "
                    "or only to document metadata/workflow?"
                )

    if details:
        raise ValueError(f"{operation} rule validation failed: " + "; ".join(details))


def _normalize_rule_set_attribute_value(target_key: str, value: Any) -> Any:
    key = _normalize_attr_token(str(target_key or ""))
    if key in {"booking_debit_account_name", "expense_account_name", "account_name"} and isinstance(value, str):
        return re.sub(r"\s+", " ", value.replace("_", " ")).strip()
    return value


def _heuristic_resolve_booking_attribute_phrase(source_key: str, value: Any) -> dict[str, Any] | None:
    token = _slug(source_key)
    parts = {part for part in token.split("_") if part}
    text_value = re.sub(r"\s+", " ", str(value or "")).strip()
    if not token:
        return None

    if {"booking", "reference"}.issubset(parts) or ({"booking", "company"}.issubset(parts)):
        return {
            "resolved_keys": ["legal_entity_ref", "company_ref"],
            "target_scope": "entity",
            "confidence": 0.72,
            "reason": "booking/company reference phrases usually identify the booking party and map to company/legal entity refs",
            "resolved_value": text_value,
        }

    if {"company", "reference"}.issubset(parts):
        return {
            "resolved_keys": ["company_ref"],
            "target_scope": "entity",
            "confidence": 0.74,
            "reason": "company reference maps directly to company_ref",
            "resolved_value": text_value,
        }

    if {"legal", "entity"}.issubset(parts):
        return {
            "resolved_keys": ["legal_entity_ref"],
            "target_scope": "entity",
            "confidence": 0.8,
            "reason": "legal entity wording maps directly to legal_entity_ref",
            "resolved_value": text_value,
        }

    return None


def _resolve_non_actionable_rule_attributes_with_llm(unknown_entries: list[dict[str, Any]], rule_text: str) -> dict[str, Any]:
    if not unknown_entries:
        return {"resolutions": []}

    client = _make_genai_client()
    if client is None:
        return {"resolutions": []}

    prompt = (
        "Resolve ambiguous business-rule booking attribute phrases to known runtime booking keys. "
        "Return only JSON with this schema:\n"
        "{\n"
        "  \"resolutions\": [\n"
        "    {\n"
        "      \"directive_index\": number,\n"
        "      \"source_key\": string,\n"
        "      \"resolved_keys\": [string],\n"
        "      \"target_scope\": \"entity\" | \"document\",\n"
        "      \"confidence\": number,\n"
        "      \"reason\": string\n"
        "    }\n"
        "  ]\n"
        "}\n"
        "Rules:\n"
        "- Use only the allowed runtime keys listed below.\n"
        "- Multi-key resolution is allowed when one user phrase clearly implies several runtime fields.\n"
        "- For booking-party or company assignment phrases, prefer entity scope and consider both legal_entity_ref and company_ref when appropriate.\n"
        "- If uncertain, return an empty resolved_keys list for that entry.\n"
        f"Allowed runtime keys: {json.dumps(ACTIONABLE_RULE_ATTRIBUTE_KEY_DESCRIPTIONS, ensure_ascii=False)}\n"
        f"Original rule text: {json.dumps(str(rule_text or ''), ensure_ascii=False)}\n"
        f"Unknown entries: {json.dumps(unknown_entries, ensure_ascii=False)}"
    )

    try:
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: client.models.generate_content(
                model=CLASSIFY_MODEL,
                contents=[prompt],
            ),
            model=CLASSIFY_MODEL,
            contents=[prompt],
            temperature=0.0,
            call_name="business_rule_attribute_resolution",
            complexity="simple",
        )
        payload = _extract_json_object(str(getattr(response, "text", "") or ""))
        return payload if isinstance(payload, dict) else {"resolutions": []}
    except Exception:
        return {"resolutions": []}


def _repair_non_actionable_rule_attributes(structured_rule: dict[str, Any], *, rule_text: str = "") -> dict[str, Any]:
    normalized = _normalize_structured_rule(structured_rule or {})
    directives = normalized.get("post_extraction_directives") if isinstance(normalized.get("post_extraction_directives"), list) else []
    if not directives:
        return normalized

    unknown_entries: list[dict[str, Any]] = []
    for idx, directive in enumerate(directives, start=1):
        if not isinstance(directive, dict):
            continue
        set_attributes = directive.get("set_attributes") if isinstance(directive.get("set_attributes"), dict) else {}
        for raw_key, value in set_attributes.items():
            norm = _normalize_attr_token(str(raw_key or ""))
            if norm in ACTIONABLE_RULE_ATTRIBUTE_KEYS:
                continue
            unknown_entries.append(
                {
                    "directive_index": idx,
                    "source_key": str(raw_key or "").strip(),
                    "normalized_source_key": norm,
                    "value": value,
                    "target_scope": str(directive.get("target_scope") or "entity").strip().lower() or "entity",
                    "target_entity_classes": [str(v) for v in (directive.get("target_entity_classes") or []) if str(v).strip()],
                }
            )

    if not unknown_entries:
        return normalized

    llm_payload = _resolve_non_actionable_rule_attributes_with_llm(unknown_entries, rule_text)
    llm_resolutions = llm_payload.get("resolutions") if isinstance(llm_payload.get("resolutions"), list) else []
    resolution_map: dict[tuple[int, str], dict[str, Any]] = {}
    for item in llm_resolutions:
        if not isinstance(item, dict):
            continue
        idx = int(item.get("directive_index") or 0)
        source_key = str(item.get("source_key") or "").strip()
        if idx <= 0 or not source_key:
            continue
        resolution_map[(idx, source_key)] = item

    repaired_directives: list[dict[str, Any]] = []
    for idx, directive in enumerate(directives, start=1):
        if not isinstance(directive, dict):
            continue
        patched = dict(directive)
        set_attributes = patched.get("set_attributes") if isinstance(patched.get("set_attributes"), dict) else {}
        new_set_attributes: dict[str, Any] = {}
        for raw_key, value in set_attributes.items():
            source_key = str(raw_key or "").strip()
            norm = _normalize_attr_token(source_key)
            if norm in ACTIONABLE_RULE_ATTRIBUTE_KEYS:
                new_set_attributes[norm] = _normalize_rule_set_attribute_value(norm, value)
                continue

            resolution = resolution_map.get((idx, source_key))
            if not isinstance(resolution, dict) or not (resolution.get("resolved_keys") if isinstance(resolution.get("resolved_keys"), list) else []):
                resolution = _heuristic_resolve_booking_attribute_phrase(source_key, value)

            resolved_keys = resolution.get("resolved_keys") if isinstance(resolution, dict) and isinstance(resolution.get("resolved_keys"), list) else []
            target_scope = str(resolution.get("target_scope") or "").strip().lower() if isinstance(resolution, dict) else ""
            for resolved_key in resolved_keys:
                norm_resolved = _normalize_attr_token(str(resolved_key or ""))
                if norm_resolved in ACTIONABLE_RULE_ATTRIBUTE_KEYS:
                    new_set_attributes[norm_resolved] = _normalize_rule_set_attribute_value(norm_resolved, value)
            if resolved_keys and target_scope in {"entity", "document"}:
                patched["target_scope"] = "entity" if target_scope == "entity" else patched.get("target_scope")

        patched["set_attributes"] = new_set_attributes
        repaired_directives.append(patched)

    normalized["post_extraction_directives"] = repaired_directives
    return normalized


def _build_rule_actionability_report(structured_rule: dict[str, Any], *, rule_text: str = "") -> dict[str, Any]:
    directives = structured_rule.get("post_extraction_directives") if isinstance(structured_rule.get("post_extraction_directives"), list) else []
    warnings: list[dict[str, Any]] = []

    for idx, directive in enumerate(directives, start=1):
        if not isinstance(directive, dict):
            continue
        target_scope = str(directive.get("target_scope") or "entity").strip().lower() or "entity"
        set_attributes = directive.get("set_attributes") if isinstance(directive.get("set_attributes"), dict) else {}
        if target_scope == "document" and set_attributes:
            warnings.append(
                {
                    "directive_index": idx,
                    "type": "document_scope_set_attributes",
                    "message": "document-scope set_attributes may not propagate to entity-level booking fields",
                    "keys": sorted(str(k) for k in set_attributes.keys()),
                }
            )

        for raw_key in set_attributes.keys():
            key = _normalize_attr_token(str(raw_key or ""))
            if key in ACTIONABLE_RULE_ATTRIBUTE_KEYS:
                continue
            suggestions = difflib.get_close_matches(key, sorted(ACTIONABLE_RULE_ATTRIBUTE_KEYS), n=3, cutoff=0.55)
            warnings.append(
                {
                    "directive_index": idx,
                    "type": "non_actionable_set_attribute",
                    "attribute": key,
                    "message": "parsed attribute is not a known runtime booking override key",
                    "suggestions": suggestions,
                }
            )

    clarifications: list[dict[str, Any]] = []
    booking_party_clarification = _build_booking_party_clarification(rule_text, directives)
    if booking_party_clarification:
        clarifications.append(booking_party_clarification)

    return {
        "warning_count": len(warnings),
        "warnings": warnings,
        "clarification_count": len(clarifications),
        "clarifications": clarifications,
    }


def _normalize_class_token(value: str) -> str:
    return _slug(value)


def _extract_class_definitions_from_text(rule_text: str) -> list[dict[str, Any]]:
    text = str(rule_text or "")
    if not text.strip():
        return []

    class_defs: dict[str, dict[str, Any]] = {}

    create_pattern = re.compile(
        r"(?:define|create|add|introduce)\s+(?:a\s+)?(?:new\s+)?(?:solf\s+)?class\s+([a-zA-Z][a-zA-Z0-9_\- ]{1,80}?)(?=\s+(?:extends|inherits\s+from|with\s+attributes)|[\.,;]|$)",
        flags=re.IGNORECASE,
    )
    extend_pattern = re.compile(
        r"class\s+([a-zA-Z][a-zA-Z0-9_\- ]{1,80})\s+(?:extends|inherits\s+from)\s+([a-zA-Z][a-zA-Z0-9_\- ]{1,80}?)(?=\s+with\s+attributes|[\.,;]|$)",
        flags=re.IGNORECASE,
    )
    attrs_pattern = re.compile(
        r"class\s+([a-zA-Z][a-zA-Z0-9_\- ]{1,80}?)(?=\s+(?:extends|inherits\s+from|with\s+attributes)|[\.,;]|$).{0,160}?with\s+attributes?\s+([a-zA-Z0-9_,\- ]+)",
        flags=re.IGNORECASE,
    )

    for match in create_pattern.finditer(text):
        class_name = _normalize_class_token(match.group(1))
        if not class_name:
            continue
        class_defs.setdefault(
            class_name,
            {
                "class_name": class_name,
                "parent_class_name": None,
                "attributes": [],
            },
        )

    for match in extend_pattern.finditer(text):
        child = _normalize_class_token(match.group(1))
        parent = _normalize_class_token(match.group(2))
        if not child:
            continue
        payload = class_defs.setdefault(
            child,
            {
                "class_name": child,
                "parent_class_name": None,
                "attributes": [],
            },
        )
        if parent:
            payload["parent_class_name"] = parent
            class_defs.setdefault(
                parent,
                {
                    "class_name": parent,
                    "parent_class_name": None,
                    "attributes": [],
                },
            )

    for match in attrs_pattern.finditer(text):
        class_name = _normalize_class_token(match.group(1))
        attrs_raw = str(match.group(2) or "")
        if not class_name:
            continue
        payload = class_defs.setdefault(
            class_name,
            {
                "class_name": class_name,
                "parent_class_name": None,
                "attributes": [],
            },
        )
        attrs: list[str] = []
        for token in re.split(r"[,;]|\band\b", attrs_raw, flags=re.IGNORECASE):
            norm = _normalize_attr_token(token)
            if norm and norm not in attrs:
                attrs.append(norm)
        payload["attributes"] = sorted(set((payload.get("attributes") or []) + attrs))

    return [class_defs[name] for name in sorted(class_defs.keys())]


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    m = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        return {}
    try:
        parsed = json.loads(m.group(0))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _make_genai_client():
    return get_openrouter_client()


GENERAL_RULE_TEXT_PATTERNS = (
    r"\bin\s+general\b",
    r"\bfor\s+all\s+cases\b",
    r"\bfor\s+general\s+case\b",
    r"\bin\s+all\s+cases\b",
)


def _infer_rule_application_mode(rule_text: str) -> str:
    text = str(rule_text or "").strip()
    if not text:
        return "selected_only"
    for pattern in GENERAL_RULE_TEXT_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return "general"
    return "selected_only"


def _resolve_rule_application_mode(structured_rule: dict[str, Any] | None, rule_text: str = "") -> str:
    payload = structured_rule if isinstance(structured_rule, dict) else {}
    raw = str(payload.get("application_mode") or payload.get("rule_application_mode") or "").strip().lower()
    if raw in {"general", "selected_only"}:
        return raw
    return _infer_rule_application_mode(rule_text)


def _apply_application_mode_override(structured_rule: dict[str, Any], application_mode: str | None, rule_text: str = "") -> dict[str, Any]:
    payload = dict(structured_rule or {})
    raw = str(application_mode or "").strip().lower()
    if raw in {"general", "selected_only"}:
        payload["application_mode"] = raw
    else:
        payload["application_mode"] = _resolve_rule_application_mode(payload, rule_text)
    return payload


def _apply_overwrite_existing_override(
    structured_rule: dict[str, Any],
    overwrite_existing: bool | None,
) -> dict[str, Any]:
    if overwrite_existing is None:
        return dict(structured_rule or {})

    payload = dict(structured_rule or {})
    directives_raw = payload.get("post_extraction_directives") if isinstance(payload.get("post_extraction_directives"), list) else []
    patched_directives: list[dict[str, Any]] = []
    for item in directives_raw:
        if not isinstance(item, dict):
            continue
        patched = dict(item)
        patched["overwrite_existing"] = bool(overwrite_existing)
        patched_directives.append(patched)
    if patched_directives:
        payload["post_extraction_directives"] = patched_directives
    return payload


def _has_document_scope_actionable_set_attributes(directives: list[dict[str, Any]]) -> bool:
    for directive in directives:
        if not isinstance(directive, dict):
            continue
        if str(directive.get("target_scope") or "entity").strip().lower() != "document":
            continue
        set_attributes = directive.get("set_attributes") if isinstance(directive.get("set_attributes"), dict) else {}
        if any(_normalize_attr_token(str(key or "")) in ACTIONABLE_RULE_ATTRIBUTE_KEYS for key in set_attributes.keys()):
            return True
    return False


def _has_entity_scope_actionable_set_attributes(directives: list[dict[str, Any]]) -> bool:
    for directive in directives:
        if not isinstance(directive, dict):
            continue
        if str(directive.get("target_scope") or "entity").strip().lower() != "entity":
            continue
        set_attributes = directive.get("set_attributes") if isinstance(directive.get("set_attributes"), dict) else {}
        if any(_normalize_attr_token(str(key or "")) in ACTIONABLE_RULE_ATTRIBUTE_KEYS for key in set_attributes.keys()):
            return True
    return False


def _prefer_actionable_fallback_directives(payload: dict[str, Any], rule_text: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return payload

    llm_directives = payload.get("post_extraction_directives") if isinstance(payload.get("post_extraction_directives"), list) else []
    if not llm_directives:
        return payload

    fallback_directives = _extract_post_extraction_directives_from_rule_text(rule_text)
    if not fallback_directives:
        return payload

    if _has_document_scope_actionable_set_attributes(llm_directives) and _has_entity_scope_actionable_set_attributes(fallback_directives):
        patched = dict(payload)
        patched["post_extraction_directives"] = fallback_directives
        patched.setdefault("metadata", {})
        metadata = patched.get("metadata") if isinstance(patched.get("metadata"), dict) else {}
        metadata = dict(metadata)
        metadata["directive_resolution_source"] = "fallback_post_extraction_directives"
        patched["metadata"] = metadata
        return patched

    return payload


def parse_rule_text_to_structured(rule_text: str) -> dict[str, Any]:
    """Parse free-form business rule text into structured JSON for runtime usage."""
    text = str(rule_text or "").strip()
    if not text:
        return {"mappings": []}
    application_mode = _infer_rule_application_mode(text)

    prompt = (
        "Convert the business rule text into strict JSON for runtime policy execution. "
        "Return only JSON with this schema:\n"
        "{\n"
        "  \"rule_name\": string,\n"
        "  \"mappings\": [\n"
        "    {\n"
        "      \"source_attribute_terms\": [string],\n"
        "      \"target_attribute\": string,\n"
        "      \"relationship\": \"same_as\" | \"distinct\",\n"
        "      \"distinct_from\": [string],\n"
        "      \"scope\": {\"countries\":[string], \"document_types\":[string], \"entity_classes\":[string], \"operations\":[string]}\n"
        "    }\n"
        "  ]\n"
        "  \"workflow_hints\": [\n"
        "    {\"process\": string, \"control_flow\": \"sequence\"|\"selection\"|\"iteration\"|\"backtracking\", \"stop_on_error\": true|false}\n"
        "  ]\n"
        "  \"class_definitions\": [\n"
        "    {\"class_name\": string, \"parent_class_name\": string|null, \"attributes\": [string]}\n"
        "  ],\n"
        "  \"fact_assertions\": [\n"
        "    {\"fact_name\": string, \"subject\": string, \"predicate\": string, \"object\": string, \"value\": string, \"scope\": {\"countries\":[string], \"document_types\":[string], \"entity_classes\":[string], \"operations\":[string]}}\n"
        "  ],\n"
        "  \"multi_hop_dependencies\": [\n"
        "    {\"dependency_name\": string, \"source_fact\": string, \"target_fact\": string, \"via_relationship\": string, \"max_hops\": number, \"direction\": \"outgoing\"|\"incoming\"|\"any\", \"scope\": {\"countries\":[string], \"document_types\":[string], \"entity_classes\":[string], \"operations\":[string]}}\n"
        "  ],\n"
        "  \"quantified_conditions\": [\n"
        "    {\"condition_name\": string, \"quantifier\": \"any\"|\"all\"|\"none\"|\"count_at_least\"|\"count_at_most\"|\"exactly\", \"variable\": string, \"in_set\": [string], \"predicate\": string, \"threshold\": number, \"scope\": {\"countries\":[string], \"document_types\":[string], \"entity_classes\":[string], \"operations\":[string]}}\n"
        "  ],\n"
        "  \"post_extraction_directives\": [\n"
        "    {\n"
        "      \"target_scope\": \"entity\" | \"document\",\n"
        "      \"target_entity_classes\": [string],\n"
        "      \"conditions\": [\n"
        "        {\"attribute_terms\": [string], \"operator\": \"equals\"|\"equals_any\"|\"contains\"|\"contains_any\"|\"exists\", \"value\": string, \"values\": [string]}\n"
        "      ],\n"
        "      \"set_attributes\": {\"key\": \"value\"},\n"
        "      \"set_metadata\": {\"key\": \"value\"},\n"
        "      \"append_workflow_processes\": [string],\n"
        "      \"overwrite_existing\": true|false,\n"
        "      \"scope\": {\"countries\":[string], \"document_types\":[string], \"entity_classes\":[string], \"operations\":[string]}\n"
        "    }\n"
        "  ]\n"
        "}\n"
        "Normalize all tokens to snake_case lowercase."
        f"\nBusiness rule text: {json.dumps(text)}"
    )

    client = _make_genai_client()
    if client is not None:
        try:
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: client.models.generate_content(
                    model=EXTRACT_MODEL,
                    contents=[prompt],
                ),
                model=EXTRACT_MODEL,
                contents=[prompt],
                temperature=0.0,
                call_name="business_rule_parse",
                complexity="simple",
            )
            payload = _extract_json_object(str(getattr(response, "text", "") or ""))
            if payload:
                payload = _prefer_actionable_fallback_directives(payload, text)
                payload["application_mode"] = _resolve_rule_application_mode(payload, text)
                class_definitions_from_llm = payload.get("class_definitions") if isinstance(payload.get("class_definitions"), list) else []
                if class_definitions_from_llm:
                    payload["class_definition_source"] = "llm"
                else:
                    inferred_class_defs = _extract_class_definitions_from_text(text)
                    if inferred_class_defs:
                        payload["class_definitions"] = inferred_class_defs
                        payload["class_definition_source"] = "fallback"
                    else:
                        payload["class_definition_source"] = "none"
                return payload
        except Exception:
            pass

    lower = text.lower()
    mappings: list[dict[str, Any]] = []

    if "vat" in lower and "switzerland" in lower and "registration" in lower and ("same" in lower or "gleich" in lower):
        mappings.append(
            {
                "source_attribute_terms": ["vat_number", "vat_no", "mwst_number", "mwst_no", "must_number", "must_no"],
                "target_attribute": "registration_no",
                "relationship": "same_as",
                "distinct_from": [],
                "scope": {"countries": ["switzerland"], "document_types": [], "entity_classes": [], "operations": []},
            }
        )

    if "vat" in lower and "germany" in lower and ("distinct" in lower or "different" in lower):
        mappings.append(
            {
                "source_attribute_terms": ["vat_number", "vat_no", "ust_id", "ust_idnr"],
                "target_attribute": "vat_no",
                "relationship": "distinct",
                "distinct_from": ["registration_no"],
                "scope": {
                    "countries": ["germany"],
                    "document_types": ["invoice", "bill", "receipt"],
                    "entity_classes": ["company", "organization"],
                    "operations": [],
                },
            }
        )

    return {
        "rule_name": "runtime_business_rule",
        "application_mode": application_mode,
        "mappings": mappings,
        "workflow_hints": [],
        "class_definitions": _extract_class_definitions_from_text(text),
        "fact_assertions": [],
        "multi_hop_dependencies": [],
        "quantified_conditions": [],
        "post_extraction_directives": _extract_post_extraction_directives_from_rule_text(text),
        "class_definition_source": "fallback" if _extract_class_definitions_from_text(text) else "none",
    }


def _normalize_scope_block(scope_raw: Any) -> dict[str, list[str]]:
    scope_raw = scope_raw if isinstance(scope_raw, dict) else {}
    return {
        "countries": [_slug(str(v or "")) for v in (scope_raw.get("countries") or []) if _slug(str(v or ""))],
        "document_types": [_slug(str(v or "")) for v in (scope_raw.get("document_types") or []) if _slug(str(v or ""))],
        "entity_classes": [_slug(str(v or "")) for v in (scope_raw.get("entity_classes") or []) if _slug(str(v or ""))],
        "operations": [_slug(str(v or "")) for v in (scope_raw.get("operations") or []) if _slug(str(v or ""))],
    }


def _normalize_structured_rule(structured_rule: dict[str, Any]) -> dict[str, Any]:
    rule_name = _slug(str(structured_rule.get("rule_name") or "runtime_business_rule")) or "runtime_business_rule"
    application_mode = str(structured_rule.get("application_mode") or structured_rule.get("rule_application_mode") or "").strip().lower() or "selected_only"
    if application_mode not in {"general", "selected_only"}:
        application_mode = "selected_only"
    mappings_raw = structured_rule.get("mappings") if isinstance(structured_rule.get("mappings"), list) else []

    mappings: list[dict[str, Any]] = []
    for item in mappings_raw:
        if not isinstance(item, dict):
            continue

        source_terms = []
        for term in item.get("source_attribute_terms") or []:
            norm = _normalize_attr_token(str(term or ""))
            if norm and norm not in source_terms:
                source_terms.append(norm)

        target = _normalize_attr_token(str(item.get("target_attribute") or ""))
        relationship = str(item.get("relationship") or "same_as").strip().lower()
        if relationship not in {"same_as", "distinct"}:
            relationship = "same_as"

        if target in {"invoice", "bill", "receipt"} and any("vat" in term or "mwst" in term or "ust" in term for term in source_terms):
            target = "vat_no"

        distinct_from = []
        for term in item.get("distinct_from") or []:
            norm = _normalize_attr_token(str(term or ""))
            if norm and norm not in distinct_from:
                distinct_from.append(norm)

        distinct_from = [v for v in distinct_from if v not in {"invoice", "bill", "receipt"}]

        if relationship == "distinct" and not distinct_from and any("vat" in term for term in source_terms):
            distinct_from = ["registration_no"]

        scope = _normalize_scope_block(item.get("scope"))

        if not source_terms or not target:
            continue

        mappings.append(
            {
                "source_attribute_terms": source_terms,
                "target_attribute": target,
                "relationship": relationship,
                "distinct_from": distinct_from,
                "scope": scope,
            }
        )

    workflow_hints_raw = structured_rule.get("workflow_hints") if isinstance(structured_rule.get("workflow_hints"), list) else []
    workflow_hints: list[dict[str, Any]] = []
    for hint in workflow_hints_raw:
        if not isinstance(hint, dict):
            continue
        process_resolution = canonicalize_workflow_process(str(hint.get("process") or ""), domain=str(hint.get("domain") or ""))
        process = str(process_resolution.get("normalized_process") or "")
        control_flow = canonicalize_control_flow(str(hint.get("control_flow") or ""))
        if control_flow not in {"sequence", "selection", "iteration", "backtracking"}:
            continue
        workflow_hints.append(
            {
                "process": process or "generic",
                "domain": process_resolution.get("domain"),
                "process_resolution": {
                    "raw_text": process_resolution.get("raw_text"),
                    "matched": bool(process_resolution.get("matched")),
                    "confidence": float(process_resolution.get("confidence") or 0.0),
                },
                "control_flow": control_flow,
                "stop_on_error": bool(hint.get("stop_on_error", True)),
            }
        )

    class_definitions_raw = structured_rule.get("class_definitions") if isinstance(structured_rule.get("class_definitions"), list) else []
    class_definition_source = str(structured_rule.get("class_definition_source") or "").strip().lower()
    if class_definition_source not in {"llm", "fallback", "none"}:
        class_definition_source = "none"
    class_definitions: list[dict[str, Any]] = []
    seen_classes: set[str] = set()
    for item in class_definitions_raw:
        if not isinstance(item, dict):
            continue

        class_name = _normalize_class_token(str(item.get("class_name") or ""))
        if not class_name or class_name in seen_classes:
            continue

        parent_class_name = _normalize_class_token(str(item.get("parent_class_name") or "")) or None
        if parent_class_name == class_name:
            parent_class_name = None

        attrs_raw = item.get("attributes") if isinstance(item.get("attributes"), list) else item.get("allowed_attributes")
        attrs = [
            _normalize_attr_token(str(value or ""))
            for value in (attrs_raw or [])
            if _normalize_attr_token(str(value or ""))
        ]
        class_definitions.append(
            {
                "class_name": class_name,
                "parent_class_name": parent_class_name,
                "attributes": sorted(set(attrs)),
            }
        )
        seen_classes.add(class_name)

    fact_assertions_raw = structured_rule.get("fact_assertions") if isinstance(structured_rule.get("fact_assertions"), list) else []
    fact_assertions: list[dict[str, Any]] = []
    for item in fact_assertions_raw:
        if not isinstance(item, dict):
            continue
        fact_name = _slug(str(item.get("fact_name") or "")) or "runtime_fact"
        subject = _slug(str(item.get("subject") or ""))
        predicate = _slug(str(item.get("predicate") or ""))
        obj = _slug(str(item.get("object") or ""))
        value = str(item.get("value") or "").strip()
        if not fact_name or not predicate:
            continue
        fact_assertions.append(
            {
                "fact_name": fact_name,
                "subject": subject,
                "predicate": predicate,
                "object": obj,
                "value": value,
                "scope": _normalize_scope_block(item.get("scope")),
            }
        )

    multi_hop_raw = structured_rule.get("multi_hop_dependencies") if isinstance(structured_rule.get("multi_hop_dependencies"), list) else []
    multi_hop_dependencies: list[dict[str, Any]] = []
    for item in multi_hop_raw:
        if not isinstance(item, dict):
            continue
        dependency_name = _slug(str(item.get("dependency_name") or "")) or "dependency"
        source_fact = _slug(str(item.get("source_fact") or ""))
        target_fact = _slug(str(item.get("target_fact") or ""))
        via_relationship = _slug(str(item.get("via_relationship") or ""))
        direction = _slug(str(item.get("direction") or "any")) or "any"
        if direction not in {"outgoing", "incoming", "any"}:
            direction = "any"
        max_hops = int(item.get("max_hops") or 1)
        if max_hops < 1:
            max_hops = 1
        if max_hops > 10:
            max_hops = 10
        if not source_fact or not target_fact:
            continue
        multi_hop_dependencies.append(
            {
                "dependency_name": dependency_name,
                "source_fact": source_fact,
                "target_fact": target_fact,
                "via_relationship": via_relationship,
                "max_hops": max_hops,
                "direction": direction,
                "scope": _normalize_scope_block(item.get("scope")),
            }
        )

    quantified_raw = structured_rule.get("quantified_conditions") if isinstance(structured_rule.get("quantified_conditions"), list) else []
    quantified_conditions: list[dict[str, Any]] = []
    for item in quantified_raw:
        if not isinstance(item, dict):
            continue
        condition_name = _slug(str(item.get("condition_name") or "")) or "quantified_condition"
        quantifier = _slug(str(item.get("quantifier") or "any")) or "any"
        if quantifier not in {"any", "all", "none", "count_at_least", "count_at_most", "exactly"}:
            quantifier = "any"
        variable = _slug(str(item.get("variable") or "item")) or "item"
        in_set = [_slug(str(v or "")) for v in (item.get("in_set") or []) if _slug(str(v or ""))]
        predicate = _slug(str(item.get("predicate") or ""))
        threshold = int(item.get("threshold") or 0)
        if threshold < 0:
            threshold = 0
        if not predicate:
            continue
        quantified_conditions.append(
            {
                "condition_name": condition_name,
                "quantifier": quantifier,
                "variable": variable,
                "in_set": in_set,
                "predicate": predicate,
                "threshold": threshold,
                "scope": _normalize_scope_block(item.get("scope")),
            }
        )

    directives_raw = structured_rule.get("post_extraction_directives")
    if not isinstance(directives_raw, list):
        directives_raw = structured_rule.get("application_directives") if isinstance(structured_rule.get("application_directives"), list) else []

    post_extraction_directives: list[dict[str, Any]] = []
    for item in directives_raw:
        if not isinstance(item, dict):
            continue
        target_scope = str(item.get("target_scope") or "entity").strip().lower()
        if target_scope not in {"entity", "document"}:
            target_scope = "entity"

        target_entity_classes = []
        for raw_class in (item.get("target_entity_classes") or []):
            normalized_class = _normalize_class_token(str(raw_class or ""))
            if normalized_class and normalized_class not in target_entity_classes:
                target_entity_classes.append(normalized_class)

        conditions: list[dict[str, Any]] = []
        for cond in (item.get("conditions") or []):
            if not isinstance(cond, dict):
                continue
            attribute_terms = []
            for term in (cond.get("attribute_terms") or []):
                normalized_term = _normalize_attr_token(str(term or ""))
                if normalized_term and normalized_term not in attribute_terms:
                    attribute_terms.append(normalized_term)
            operator = str(cond.get("operator") or "contains_any").strip().lower() or "contains_any"
            if operator not in {"equals", "equals_any", "contains", "contains_any", "exists"}:
                operator = "contains_any"
            values = [str(v).strip() for v in (cond.get("values") or []) if str(v).strip()]
            if not values and cond.get("value") not in (None, ""):
                values = [str(cond.get("value")).strip()]
            if not attribute_terms:
                continue
            conditions.append(
                {
                    "attribute_terms": attribute_terms,
                    "operator": operator,
                    "values": values,
                }
            )

        set_attributes = item.get("set_attributes") if isinstance(item.get("set_attributes"), dict) else {}
        normalized_set_attributes = {
            (_normalize_attr_token(str(key or "")) or str(key or "").strip()): _normalize_rule_set_attribute_value(str(key or ""), value)
            for key, value in set_attributes.items()
            if str(key or "").strip()
        }
        normalized_set_attributes = {key: value for key, value in normalized_set_attributes.items() if key}

        set_metadata = item.get("set_metadata") if isinstance(item.get("set_metadata"), dict) else {}
        normalized_set_metadata = {
            _slug(str(key or "")) or str(key or "").strip(): value
            for key, value in set_metadata.items()
            if str(key or "").strip()
        }
        normalized_set_metadata = {key: value for key, value in normalized_set_metadata.items() if key}

        append_workflow_processes: list[str] = []
        for process in (item.get("append_workflow_processes") or []):
            process_resolution = canonicalize_workflow_process(str(process or ""), domain=str(item.get("domain") or ""))
            normalized_process = str(process_resolution.get("normalized_process") or "").strip()
            if normalized_process and normalized_process not in append_workflow_processes:
                append_workflow_processes.append(normalized_process)

        if not conditions and not normalized_set_attributes and not normalized_set_metadata and not append_workflow_processes:
            continue

        post_extraction_directives.append(
            {
                "target_scope": target_scope,
                "target_entity_classes": target_entity_classes,
                "conditions": conditions,
                "set_attributes": normalized_set_attributes,
                "set_metadata": normalized_set_metadata,
                "append_workflow_processes": append_workflow_processes,
                "overwrite_existing": bool(item.get("overwrite_existing", True)),
                "scope": _normalize_scope_block(item.get("scope")),
            }
        )

    return {
        "rule_name": rule_name,
        "application_mode": application_mode,
        "mappings": mappings,
        "workflow_hints": workflow_hints,
        "class_definitions": class_definitions,
        "fact_assertions": fact_assertions,
        "multi_hop_dependencies": multi_hop_dependencies,
        "quantified_conditions": quantified_conditions,
        "post_extraction_directives": post_extraction_directives,
        "class_definition_source": class_definition_source,
    }


def _sort_class_definitions_for_upsert(class_definitions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    class_map: dict[str, dict[str, Any]] = {
        str(item.get("class_name") or "").strip(): dict(item)
        for item in class_definitions
        if str(item.get("class_name") or "").strip()
    }

    # Ensure parent placeholders exist so child links can be resolved in one pass.
    for item in list(class_map.values()):
        parent = str(item.get("parent_class_name") or "").strip()
        if parent and parent not in class_map:
            class_map[parent] = {
                "class_name": parent,
                "parent_class_name": None,
                "attributes": [],
            }

    ordered: list[dict[str, Any]] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def _visit(name: str) -> None:
        if name in visited:
            return
        if name in visiting:
            return
        visiting.add(name)
        node = class_map.get(name) or {}
        parent = str(node.get("parent_class_name") or "").strip()
        if parent and parent in class_map:
            _visit(parent)
        visiting.remove(name)
        visited.add(name)
        ordered.append(node)

    for class_name in sorted(class_map.keys()):
        _visit(class_name)

    return ordered


def _upsert_runtime_solf_classes(
    connection: Any,
    class_definitions: list[dict[str, Any]],
    created_by: str | None = None,
) -> list[dict[str, Any]]:
    normalized = _sort_class_definitions_for_upsert(class_definitions)
    if not normalized:
        return []

    sql = """
    INSERT INTO object_class (class_name, parent_class_id, metadata)
    VALUES (
        %s,
        (
            SELECT class_id
            FROM object_class
            WHERE class_name = %s
            LIMIT 1
        ),
        %s::jsonb
    )
    ON CONFLICT (class_name)
    DO UPDATE SET
        parent_class_id = COALESCE(EXCLUDED.parent_class_id, object_class.parent_class_id),
        metadata = COALESCE(object_class.metadata, '{}'::jsonb) || EXCLUDED.metadata
    RETURNING class_id, class_name, parent_class_id, metadata
    """

    created: list[dict[str, Any]] = []
    with connection.cursor() as cursor:
        for item in normalized:
            class_name = str(item.get("class_name") or "").strip()
            parent_class_name = str(item.get("parent_class_name") or "").strip() or None
            attributes = sorted(
                {
                    _normalize_attr_token(str(v or ""))
                    for v in (item.get("attributes") or [])
                    if _normalize_attr_token(str(v or ""))
                }
            )
            metadata = {
                "source": "business_rule_runtime",
                "defined_by": str(created_by or "system:business_rules").strip() or "system:business_rules",
                "defined_attributes": attributes,
            }
            cursor.execute(
                sql,
                (
                    class_name,
                    parent_class_name,
                    json.dumps(metadata, ensure_ascii=False),
                ),
            )
            row = cursor.fetchone()
            if row is None:
                continue
            created.append(
                {
                    "class_id": int(row[0]),
                    "class_name": str(row[1] or "").strip().lower(),
                    "parent_class_id": int(row[2]) if row[2] is not None else None,
                    "metadata": row[3] if isinstance(row[3], dict) else {},
                }
            )

    connection.commit()
    return created


def compile_structured_rule_to_solf(structured_rule: dict[str, Any], rule_key: str) -> tuple[str, list[dict[str, Any]]]:
    """Compile a normalized structured rule into SOLF clauses + semantic terms."""
    normalized = _normalize_structured_rule(structured_rule)
    mappings = normalized.get("mappings") if isinstance(normalized.get("mappings"), list) else []

    clauses: list[str] = []
    semantic_terms: list[dict[str, Any]] = []

    for idx, mapping in enumerate(mappings, start=1):
        target = str(mapping.get("target_attribute") or "").strip()
        relationship = str(mapping.get("relationship") or "same_as").strip().lower()
        scope = mapping.get("scope") if isinstance(mapping.get("scope"), dict) else {}
        countries = [str(v).strip() for v in (scope.get("countries") or []) if str(v).strip()]
        doc_types = [str(v).strip() for v in (scope.get("document_types") or []) if str(v).strip()]
        entity_classes = [str(v).strip() for v in (scope.get("entity_classes") or []) if str(v).strip()]

        for source_term in mapping.get("source_attribute_terms") or []:
            source = str(source_term or "").strip()
            if not source or not target:
                continue

            conditions = [
                "(_requested ≔ _context[requested_attribute])",
                f"(_requested = {source})",
            ]
            if countries:
                conditions.append("(_country ≔ _context[country])")
                conditions.append(f"(_country = {countries[0]})")
            if doc_types:
                conditions.append("(_doc_type ≔ _context[document_type])")
                conditions.append(f"(_doc_type = {doc_types[0]})")
            if entity_classes:
                conditions.append("(_entity_class ≔ _context[entity_class])")
                conditions.append(f"(_entity_class = {entity_classes[0]})")

            body = " ⋀\n    ".join(conditions)
            clause = (
                "business_rule_attribute_alias(_context) ⦃\n"
                f"    {body} ⋀\n"
                "    ↲({"
                f"canonical_attribute: {target}, relationship: {relationship}, rule_key: {rule_key}_{idx}"
                "})\n"
                "⦄"
            )
            clauses.append(clause)

            semantic_terms.append(
                {
                    "kind": "attribute",
                    "canonical_name": target,
                    "term_text": source,
                    "language": "und",
                    "source_type": "llm_generated",
                    "metadata": {
                        "rule_key": rule_key,
                        "relationship": relationship,
                        "countries": countries,
                        "document_types": doc_types,
                    },
                }
            )

    workflow_hints = normalized.get("workflow_hints") if isinstance(normalized.get("workflow_hints"), list) else []
    for idx, hint in enumerate(workflow_hints, start=1):
        process = str(hint.get("process") or "generic").strip() or "generic"
        control_flow = str(hint.get("control_flow") or "sequence").strip() or "sequence"
        stop_on_error = "true" if bool(hint.get("stop_on_error", True)) else "false"
        clause = (
            "business_rule_workflow_hint(_context) ⦃\n"
            "    (_process ≔ _context[process]) ⋀\n"
            f"    (_process = {process}) ⋀\n"
            "    ↲({"
            f"control_flow: {control_flow}, stop_on_error: {stop_on_error}, rule_key: {rule_key}_wf_{idx}"
            "})\n"
            "⦄"
        )
        clauses.append(clause)

        execute_clause = (
            "business_rule_workflow_execute(_context) ⦃\n"
            "    (_process ≔ _context[process]) ⋀\n"
            f"    (_process = {process}) ⋀\n"
            "    ↲({"
            f"control_flow: {control_flow}, stop_on_error: {stop_on_error}, strategy: runtime_rule, rule_key: {rule_key}_exec_{idx}"
            "})\n"
            "⦄"
        )
        clauses.append(execute_clause)

        if control_flow == "sequence":
            sequence_clause = (
                "business_rule_sequence_plan(_context) ⦃\n"
                "    (_process ≔ _context[process]) ⋀\n"
                f"    (_process = {process}) ⋀\n"
                "    ↲({"
                "control_flow: sequence, steps: [resolve_scope, lookup_attribute, finalize],"
                f" stop_on_error: {stop_on_error}, rule_key: {rule_key}_seq_{idx}"
                "})\n"
                "⦄"
            )
            clauses.append(sequence_clause)

        if control_flow == "selection":
            selection_clause = (
                "business_rule_selection_plan(_context) ⦃\n"
                "    (_has_scope ≔ _context[has_scope]) ⋀\n"
                "    (_has_scope = true) ⋀\n"
                "    ↲({"
                "control_flow: selection, next_action: lookup_attribute,"
                f" stop_on_error: {stop_on_error}, rule_key: {rule_key}_sel_{idx}"
                "})\n"
                "⦄"
            )
            clauses.append(selection_clause)
            selection_default_clause = (
                "business_rule_selection_plan(_context) ⦃\n"
                "    ↲({"
                "control_flow: selection, next_action: resolve_scope,"
                f" stop_on_error: {stop_on_error}, rule_key: {rule_key}_sel_default_{idx}"
                "})\n"
                "⦄"
            )
            clauses.append(selection_default_clause)

        if control_flow == "iteration":
            iteration_clause = (
                "business_rule_iteration_plan(_context) ⦃\n"
                "    (_acc ≔ []) ⋀\n"
                "    ((∀(_i) ∈ [1‥10]) ⋀ (_acc ≪ _i)) ⋀\n"
                "    ↲({"
                "control_flow: iteration, iteration_mode: quantifier_range,"
                f" stop_on_error: {stop_on_error}, values: _acc, rule_key: {rule_key}_iter_{idx}"
                "})\n"
                "⦄"
            )
            clauses.append(iteration_clause)

        if control_flow == "backtracking":
            backtracking_clause = (
                "business_rule_backtracking_plan(_context) ⦃\n"
                "    (_candidates ≔ _context[candidates]) ⋀\n"
                "    ↲({"
                "control_flow: backtracking, strategy: prolog_style,"
                f" stop_on_error: {stop_on_error}, candidates: _candidates, rule_key: {rule_key}_backtrack_{idx}"
                "})\n"
                "⦄"
            )
            clauses.append(backtracking_clause)

    fact_assertions = normalized.get("fact_assertions") if isinstance(normalized.get("fact_assertions"), list) else []
    for idx, fact in enumerate(fact_assertions, start=1):
        scope = fact.get("scope") if isinstance(fact.get("scope"), dict) else {}
        scope_conditions: list[str] = []
        countries = [str(v).strip() for v in (scope.get("countries") or []) if str(v).strip()]
        doc_types = [str(v).strip() for v in (scope.get("document_types") or []) if str(v).strip()]
        entity_classes = [str(v).strip() for v in (scope.get("entity_classes") or []) if str(v).strip()]
        operations = [str(v).strip() for v in (scope.get("operations") or []) if str(v).strip()]
        if countries:
            scope_conditions.extend(["(_country ≔ _context[country])", f"(_country = {countries[0]})"])
        if doc_types:
            scope_conditions.extend(["(_doc_type ≔ _context[document_type])", f"(_doc_type = {doc_types[0]})"])
        if entity_classes:
            scope_conditions.extend(["(_entity_class ≔ _context[entity_class])", f"(_entity_class = {entity_classes[0]})"])
        if operations:
            scope_conditions.extend(["(_operation ≔ _context[operation])", f"(_operation = {operations[0]})"])

        predicate = str(fact.get("predicate") or "fact")
        fact_name = str(fact.get("fact_name") or f"fact_{idx}")
        subject = str(fact.get("subject") or "")
        obj = str(fact.get("object") or "")
        value = str(fact.get("value") or "")
        body_prefix = ""
        if scope_conditions:
            body_prefix = "    " + " ⋀\n    ".join(scope_conditions) + " ⋀\n"

        clause = (
            "business_rule_fact_assertion(_context) ⦃\n"
            f"{body_prefix}"
            "    ↲({"
            f"fact_name: {fact_name}, subject: {subject}, predicate: {predicate}, object: {obj}, value: {value}, rule_key: {rule_key}_fact_{idx}"
            "})\n"
            "⦄"
        )
        clauses.append(clause)

        semantic_terms.append(
            {
                "kind": "fact",
                "canonical_name": predicate,
                "term_text": fact_name,
                "language": "und",
                "source_type": "llm_generated",
                "metadata": {
                    "rule_key": rule_key,
                    "fact_name": fact_name,
                    "subject": subject,
                    "object": obj,
                },
            }
        )

    multi_hop_dependencies = normalized.get("multi_hop_dependencies") if isinstance(normalized.get("multi_hop_dependencies"), list) else []
    for idx, dependency in enumerate(multi_hop_dependencies, start=1):
        dependency_name = str(dependency.get("dependency_name") or f"dependency_{idx}")
        source_fact = str(dependency.get("source_fact") or "")
        target_fact = str(dependency.get("target_fact") or "")
        via_relationship = str(dependency.get("via_relationship") or "related_to")
        max_hops = int(dependency.get("max_hops") or 1)
        direction = str(dependency.get("direction") or "any")
        clause = (
            "business_rule_dependency_inference(_context) ⦃\n"
            "    ↲({"
            f"dependency_name: {dependency_name}, source_fact: {source_fact}, target_fact: {target_fact}, via_relationship: {via_relationship}, max_hops: {max_hops}, direction: {direction}, rule_key: {rule_key}_dep_{idx}"
            "})\n"
            "⦄"
        )
        clauses.append(clause)

    quantified_conditions = normalized.get("quantified_conditions") if isinstance(normalized.get("quantified_conditions"), list) else []
    for idx, condition in enumerate(quantified_conditions, start=1):
        condition_name = str(condition.get("condition_name") or f"quantified_{idx}")
        quantifier = str(condition.get("quantifier") or "any")
        variable = str(condition.get("variable") or "item")
        predicate = str(condition.get("predicate") or "condition")
        threshold = int(condition.get("threshold") or 0)
        value_set = [str(v).strip() for v in (condition.get("in_set") or []) if str(v).strip()]

        quant_clause = (
            "business_rule_quantified_guard(_context) ⦃\n"
            "    (_set ≔ _context[candidate_set]) ⋀\n"
            "    ↲({"
            f"condition_name: {condition_name}, quantifier: {quantifier}, variable: {variable}, predicate: {predicate}, threshold: {threshold}, in_set: {value_set}, rule_key: {rule_key}_quant_{idx}"
            "})\n"
            "⦄"
        )
        clauses.append(quant_clause)

    return "\n\n".join(clauses), semantic_terms


def _extract_scope_from_structured(structured_rule: dict[str, Any]) -> dict[str, Any]:
    mappings = structured_rule.get("mappings") if isinstance(structured_rule.get("mappings"), list) else []
    countries: set[str] = set()
    document_types: set[str] = set()
    entity_classes: set[str] = set()
    operations: set[str] = set()

    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        scope = mapping.get("scope") if isinstance(mapping.get("scope"), dict) else {}
        countries.update(str(v).strip() for v in (scope.get("countries") or []) if str(v).strip())
        document_types.update(str(v).strip() for v in (scope.get("document_types") or []) if str(v).strip())
        entity_classes.update(str(v).strip() for v in (scope.get("entity_classes") or []) if str(v).strip())
        operations.update(str(v).strip() for v in (scope.get("operations") or []) if str(v).strip())

    for fact in (structured_rule.get("fact_assertions") or []):
        if not isinstance(fact, dict):
            continue
        scope = fact.get("scope") if isinstance(fact.get("scope"), dict) else {}
        countries.update(str(v).strip() for v in (scope.get("countries") or []) if str(v).strip())
        document_types.update(str(v).strip() for v in (scope.get("document_types") or []) if str(v).strip())
        entity_classes.update(str(v).strip() for v in (scope.get("entity_classes") or []) if str(v).strip())
        operations.update(str(v).strip() for v in (scope.get("operations") or []) if str(v).strip())

    for dependency in (structured_rule.get("multi_hop_dependencies") or []):
        if not isinstance(dependency, dict):
            continue
        scope = dependency.get("scope") if isinstance(dependency.get("scope"), dict) else {}
        countries.update(str(v).strip() for v in (scope.get("countries") or []) if str(v).strip())
        document_types.update(str(v).strip() for v in (scope.get("document_types") or []) if str(v).strip())
        entity_classes.update(str(v).strip() for v in (scope.get("entity_classes") or []) if str(v).strip())
        operations.update(str(v).strip() for v in (scope.get("operations") or []) if str(v).strip())

    for condition in (structured_rule.get("quantified_conditions") or []):
        if not isinstance(condition, dict):
            continue
        scope = condition.get("scope") if isinstance(condition.get("scope"), dict) else {}
        countries.update(str(v).strip() for v in (scope.get("countries") or []) if str(v).strip())
        document_types.update(str(v).strip() for v in (scope.get("document_types") or []) if str(v).strip())
        entity_classes.update(str(v).strip() for v in (scope.get("entity_classes") or []) if str(v).strip())
        operations.update(str(v).strip() for v in (scope.get("operations") or []) if str(v).strip())

    return {
        "countries": sorted(countries),
        "document_types": sorted(document_types),
        "entity_classes": sorted(entity_classes),
        "operations": sorted(operations),
        "has_workflow_hints": bool(structured_rule.get("workflow_hints")),
        "has_fact_assertions": bool(structured_rule.get("fact_assertions")),
        "has_multi_hop_dependencies": bool(structured_rule.get("multi_hop_dependencies")),
        "has_quantified_conditions": bool(structured_rule.get("quantified_conditions")),
        "defined_classes": sorted(
            {
                str(item.get("class_name") or "").strip()
                for item in (structured_rule.get("class_definitions") or [])
                if isinstance(item, dict) and str(item.get("class_name") or "").strip()
            }
        ),
    }


def create_business_rule(
    rule_text: str,
    rule_name: str | None = None,
    created_by: str | None = None,
    is_active: bool = True,
    application_mode: str | None = None,
    overwrite_existing: bool | None = None,
) -> dict[str, Any]:
    structured = _normalize_structured_rule(
        _apply_application_mode_override(parse_rule_text_to_structured(rule_text), application_mode, rule_text)
    )
    structured = _normalize_structured_rule(_apply_overwrite_existing_override(structured, overwrite_existing))
    structured = _repair_non_actionable_rule_attributes(structured, rule_text=rule_text)
    actionability = _build_rule_actionability_report(structured, rule_text=rule_text)
    _raise_for_non_actionable_rule(actionability, operation="create")
    generated_rule_name = _slug(str(rule_name or structured.get("rule_name") or "runtime_business_rule")) or "runtime_business_rule"
    solf_script, semantic_terms = compile_structured_rule_to_solf(structured, generated_rule_name)
    scope = _extract_scope_from_structured(structured)

    connection = object_db.get_connection()
    try:
        created_classes = _upsert_runtime_solf_classes(
            connection=connection,
            class_definitions=structured.get("class_definitions") if isinstance(structured.get("class_definitions"), list) else [],
            created_by=created_by,
        )

        rule_id = object_db.upsert_business_rule(
            connection=connection,
            rule_name=generated_rule_name,
            rule_text=rule_text,
            structured_rule=structured,
            solf_script=solf_script,
            scope=scope,
            metadata={"source": "runtime_business_rules", "version": 1},
            is_active=is_active,
            created_by=created_by,
        )

        if semantic_terms:
            object_db.upsert_semantic_terms(connection, semantic_terms)

        # Persist each compiled clause into SQL-backed clause catalog as runtime rule clauses.
        pattern_library = PatternLibrary(connection)
        clauses = [c for c in solf_script.split("\n\n") if c.strip()]
        upserted_clause_ids: list[int] = []
        for idx, clause in enumerate(clauses, start=1):
            clause_name = f"business_rule_attribute_alias_{rule_id}_{idx}"
            clause_id = pattern_library.upsert_solf_clause(
                clause_name=clause_name,
                clause_type="resolve_policy",
                clause_body=clause,
                entity_class=None,
                metadata={
                    "source": "business_rule_runtime",
                    "rule_id": int(rule_id),
                    "rule_name": generated_rule_name,
                },
                is_active=True,
                created_by=created_by or "system:business_rules",
            )
            if isinstance(clause_id, int):
                upserted_clause_ids.append(clause_id)

        return {
            "rule_id": int(rule_id),
            "rule_name": generated_rule_name,
            "structured_rule": structured,
            "scope": scope,
            "class_definition_source": str(structured.get("class_definition_source") or "none"),
            "class_definitions_created": created_classes,
            "class_definitions_created_count": len(created_classes),
            "semantic_terms_upserted": len(semantic_terms),
            "solf_clauses_upserted": len(upserted_clause_ids),
            "solf_script": solf_script,
            "is_active": bool(is_active),
            "actionability": actionability,
        }
    finally:
        connection.close()


def _upsert_rule_clauses(
    connection: Any,
    rule_id: int,
    rule_name: str,
    solf_script: str,
    created_by: str | None,
    is_active: bool,
) -> int:
    pattern_library = PatternLibrary(connection)
    clauses = [c for c in str(solf_script or "").split("\n\n") if c.strip()]
    upserted_clause_ids: list[int] = []
    active_clause_names: list[str] = []
    for idx, clause in enumerate(clauses, start=1):
        clause_name = f"business_rule_attribute_alias_{rule_id}_{idx}"
        active_clause_names.append(clause_name)
        clause_id = pattern_library.upsert_solf_clause(
            clause_name=clause_name,
            clause_type="resolve_policy",
            clause_body=clause,
            entity_class=None,
            metadata={
                "source": "business_rule_runtime",
                "rule_id": int(rule_id),
                "rule_name": rule_name,
            },
            is_active=bool(is_active),
            created_by=created_by or "system:business_rules",
        )
        if isinstance(clause_id, int):
            upserted_clause_ids.append(clause_id)

    prefix = f"business_rule_attribute_alias_{int(rule_id)}_"
    with connection.cursor() as cursor:
        if active_clause_names:
            cursor.execute(
                """
                UPDATE solf_clauses
                SET is_active = FALSE,
                    modified_at = NOW(),
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                WHERE clause_name LIKE %s
                  AND clause_type = 'resolve_policy'
                  AND COALESCE(metadata->>'source', '') = 'business_rule_runtime'
                  AND COALESCE(metadata->>'rule_id', '') = %s
                  AND clause_name <> ALL(%s)
                """,
                (
                    Json({"deactivation_reason": "rule_overwrite_cleanup"}),
                    f"{prefix}%",
                    str(int(rule_id)),
                    active_clause_names,
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE solf_clauses
                SET is_active = FALSE,
                    modified_at = NOW(),
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                WHERE clause_name LIKE %s
                  AND clause_type = 'resolve_policy'
                  AND COALESCE(metadata->>'source', '') = 'business_rule_runtime'
                  AND COALESCE(metadata->>'rule_id', '') = %s
                """,
                (
                    Json({"deactivation_reason": "rule_overwrite_cleanup"}),
                    f"{prefix}%",
                    str(int(rule_id)),
                ),
            )

    return len(upserted_clause_ids)


def get_business_rule(rule_id: int) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        return object_db.get_business_rule_by_id(connection, rule_id=rule_id)
    finally:
        connection.close()


def update_business_rule(
    rule_id: int,
    rule_text: str,
    rule_name: str | None = None,
    updated_by: str | None = None,
    is_active: bool | None = None,
    application_mode: str | None = None,
    overwrite_existing: bool | None = None,
) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        current = object_db.get_business_rule_by_id(connection, rule_id=rule_id)
        if current is None:
            return None

        structured = _normalize_structured_rule(
            _apply_application_mode_override(parse_rule_text_to_structured(rule_text), application_mode, rule_text)
        )
        structured = _normalize_structured_rule(_apply_overwrite_existing_override(structured, overwrite_existing))
        structured = _repair_non_actionable_rule_attributes(structured, rule_text=rule_text)
        actionability = _build_rule_actionability_report(structured, rule_text=rule_text)
        _raise_for_non_actionable_rule(actionability, operation="update")
        generated_rule_name = _slug(str(rule_name or current.get("rule_name") or structured.get("rule_name") or "runtime_business_rule")) or "runtime_business_rule"
        solf_script, semantic_terms = compile_structured_rule_to_solf(structured, generated_rule_name)
        scope = _extract_scope_from_structured(structured)
        created_classes = _upsert_runtime_solf_classes(
            connection=connection,
            class_definitions=structured.get("class_definitions") if isinstance(structured.get("class_definitions"), list) else [],
            created_by=updated_by,
        )

        current_metadata = current.get("metadata") if isinstance(current.get("metadata"), dict) else {}
        metadata = dict(current_metadata)
        previous_version = int(metadata.get("version") or 1)
        metadata["source"] = "runtime_business_rules"
        metadata["version"] = previous_version + 1
        if updated_by:
            metadata["updated_by"] = str(updated_by)

        updated_active = bool(current.get("is_active")) if is_active is None else bool(is_active)
        previous_rule_text = str(current.get("rule_text") or "")
        previous_rule_name = str(current.get("rule_name") or "")
        previous_is_active = bool(current.get("is_active"))

        updated = object_db.update_business_rule(
            connection=connection,
            rule_id=rule_id,
            rule_name=generated_rule_name,
            rule_text=rule_text,
            structured_rule=structured,
            solf_script=solf_script,
            scope=scope,
            metadata=metadata,
            is_active=updated_active,
        )
        if not updated:
            return None

        if semantic_terms:
            object_db.upsert_semantic_terms(connection, semantic_terms)

        clauses_upserted = _upsert_rule_clauses(
            connection=connection,
            rule_id=int(rule_id),
            rule_name=generated_rule_name,
            solf_script=solf_script,
            created_by=updated_by,
            is_active=updated_active,
        )

        refreshed = object_db.get_business_rule_by_id(connection, rule_id=rule_id)
        connection.commit()

        change_summary = {
            "rule_text_changed": previous_rule_text != str(rule_text),
            "rule_name_changed": previous_rule_name != generated_rule_name,
            "is_active_changed": previous_is_active != bool(updated_active),
            "updated_by": str(updated_by or ""),
            "updated_at": (refreshed or {}).get("modified_at").isoformat() if (refreshed or {}).get("modified_at") is not None else None,
            "previous_rule_name": previous_rule_name,
            "previous_is_active": previous_is_active,
        }

        return {
            "rule_id": int(rule_id),
            "rule_name": generated_rule_name,
            "rule_text": str(rule_text),
            "structured_rule": structured,
            "scope": scope,
            "class_definition_source": str(structured.get("class_definition_source") or "none"),
            "class_definitions_created": created_classes,
            "class_definitions_created_count": len(created_classes),
            "semantic_terms_upserted": len(semantic_terms),
            "solf_clauses_upserted": int(clauses_upserted),
            "solf_script": solf_script,
            "is_active": bool(updated_active),
            "metadata": metadata,
            "created_at": (refreshed or {}).get("created_at").isoformat() if (refreshed or {}).get("created_at") is not None else None,
            "modified_at": (refreshed or {}).get("modified_at").isoformat() if (refreshed or {}).get("modified_at") is not None else None,
            "change_summary": change_summary,
            "actionability": actionability,
        }
    finally:
        connection.close()


def deactivate_business_rule(rule_id: int, updated_by: str | None = None) -> bool:
    connection = object_db.get_connection()
    try:
        updated = object_db.set_business_rule_active(connection, rule_id=rule_id, is_active=False)
        if not updated:
            return False

        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE solf_clauses
                SET is_active = FALSE,
                    modified_at = NOW(),
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                WHERE clause_type = 'resolve_policy'
                  AND COALESCE(metadata->>'source', '') = 'business_rule_runtime'
                  AND COALESCE(metadata->>'rule_id', '') = %s
                """,
                (
                    Json({
                        "deactivation_reason": "business_rule_deactivated",
                        "updated_by": str(updated_by or ""),
                    }),
                    str(int(rule_id)),
                ),
            )
        connection.commit()
        return True
    finally:
        connection.close()


def list_business_rules(is_active: bool | None = None, limit: int = 200) -> list[dict[str, Any]]:
    connection = object_db.get_connection()
    try:
        return object_db.get_business_rules(connection, is_active=is_active, limit=limit)
    finally:
        connection.close()


def activate_business_rule(rule_id: int, is_active: bool) -> bool:
    connection = object_db.get_connection()
    try:
        return object_db.set_business_rule_active(connection, rule_id=rule_id, is_active=is_active)
    finally:
        connection.close()


def load_active_business_rule_solf_script(limit: int = 200) -> str:
    pre_chunks, post_chunks = load_active_business_rule_solf_script_chunks(limit=limit)
    chunks = [*pre_chunks, *post_chunks]
    return "\n\n".join(chunks)


def load_active_business_rule_solf_script_chunks(limit: int = 200) -> tuple[list[str], list[str]]:
    rules = list_business_rules(is_active=True, limit=limit)
    pre_chunks: list[str] = []
    post_chunks: list[str] = []

    for row in rules:
        script = str(row.get("solf_script") or "").strip()
        if not script:
            continue

        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if metadata.get("overwrite_existing") is False:
            pre_chunks.append(script)
        else:
            post_chunks.append(script)

    return pre_chunks, post_chunks


def _scope_match(mapping_scope: dict[str, Any], context: dict[str, Any]) -> bool:
    country = _slug(str(context.get("country") or ""))
    document_type = _slug(str(context.get("document_type") or ""))
    entity_class = _slug(str(context.get("entity_class") or ""))
    operation = _slug(str(context.get("operation") or ""))

    countries = {_slug(str(v or "")) for v in (mapping_scope.get("countries") or []) if _slug(str(v or ""))}
    document_types = {_slug(str(v or "")) for v in (mapping_scope.get("document_types") or []) if _slug(str(v or ""))}
    entity_classes = {_slug(str(v or "")) for v in (mapping_scope.get("entity_classes") or []) if _slug(str(v or ""))}
    operations = {_slug(str(v or "")) for v in (mapping_scope.get("operations") or []) if _slug(str(v or ""))}

    if countries and country and country not in countries:
        return False
    if countries and not country:
        return False

    if document_types and document_type and document_type not in document_types:
        return False
    if document_types and not document_type:
        return False

    if entity_classes and entity_class and entity_class not in entity_classes:
        return False
    if entity_classes and not entity_class:
        return False

    if operations and operation and operation not in operations:
        return False
    if operations and not operation:
        return False

    return True


def resolve_attribute_via_business_rules(
    requested_attribute: str,
    context: dict[str, Any] | None = None,
    rule_id: int | None = None,
    include_inactive: bool = False,
) -> dict[str, Any] | None:
    req = _normalize_attr_token(str(requested_attribute or ""))
    if not req:
        return None

    ctx = dict(context or {})
    rules = list_business_rules(is_active=None if include_inactive else True, limit=400)
    if rule_id is not None:
        rules = [row for row in rules if int(row.get("rule_id") or 0) == int(rule_id)]

    for row in rules:
        structured = row.get("structured_rule") if isinstance(row.get("structured_rule"), dict) else {}
        mappings = structured.get("mappings") if isinstance(structured.get("mappings"), list) else []
        for mapping in mappings:
            if not isinstance(mapping, dict):
                continue
            source_terms = {_normalize_attr_token(str(v or "")) for v in (mapping.get("source_attribute_terms") or [])}
            source_terms.discard("")
            if req not in source_terms:
                continue
            scope = mapping.get("scope") if isinstance(mapping.get("scope"), dict) else {}
            if not _scope_match(scope, ctx):
                continue

            target = _normalize_attr_token(str(mapping.get("target_attribute") or ""))
            relationship = str(mapping.get("relationship") or "same_as").strip().lower()
            distinct_from = [
                _normalize_attr_token(str(v or ""))
                for v in (mapping.get("distinct_from") or [])
                if _normalize_attr_token(str(v or ""))
            ]

            return {
                "canonical_attribute": target or req,
                "relationship": relationship,
                "distinct_from": distinct_from,
                "rule_id": row.get("rule_id"),
                "rule_name": row.get("rule_name"),
            }

    return None


def apply_business_rules_to_attributes(
    attributes: dict[str, Any],
    context: dict[str, Any] | None = None,
    rule_id: int | None = None,
    include_inactive: bool = False,
) -> dict[str, Any]:
    attrs = dict(attributes or {})
    if not attrs:
        return attrs

    ctx = dict(context or {})
    normalized_keys = {_normalize_attr_token(str(k or "")): str(k) for k in attrs.keys()}

    for norm_key, original_key in list(normalized_keys.items()):
        resolved = resolve_attribute_via_business_rules(
            norm_key,
            ctx,
            rule_id=rule_id,
            include_inactive=include_inactive,
        )
        if not resolved:
            continue
        target = str(resolved.get("canonical_attribute") or "").strip()
        relationship = str(resolved.get("relationship") or "same_as").strip().lower()

        if relationship == "same_as" and target:
            source_val = attrs.get(original_key)
            if source_val is not None and target not in attrs:
                attrs[target] = source_val
        elif relationship == "distinct":
            distinct_from = [str(v).strip() for v in (resolved.get("distinct_from") or []) if str(v).strip()]
            if target and target not in attrs and original_key in attrs:
                attrs[target] = attrs.get(original_key)
            for attr_name in distinct_from:
                if attr_name in attrs and target in attrs and attrs[attr_name] == attrs[target]:
                    meta = attrs.get("_business_rule_flags") if isinstance(attrs.get("_business_rule_flags"), dict) else {}
                    meta[f"distinct_conflict_{target}_{attr_name}"] = True
                    attrs["_business_rule_flags"] = meta

    return attrs


_RULE_VALUE_ATTR_ALIASES: dict[str, set[str]] = {
    "vendor": {
        "vendor",
        "vendor_name",
        "vendor_ref",
        "merchant",
        "merchant_name",
        "merchant_ref",
        "supplier",
        "supplier_name",
        "supplier_ref",
        "issuer",
        "issuer_name",
        "issuer_ref",
        "payee",
        "payee_name",
        "payee_ref",
        "seller",
        "seller_name",
        "seller_ref",
        "creditor",
        "creditor_name",
        "creditor_ref",
        "legal_name",
        "counterparty_name",
        "counterparty_ref",
        "vendor_candidates",
    },
    "company": {"company", "company_name", "legal_entity_ref", "company_ref", "bill_to", "bill_to_name", "payer_company", "payable_by"},
    "particulars": {"particulars", "booking_particulars", "expense_particulars", "description", "memo", "purpose"},
}


def _normalize_rule_value(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _split_rule_condition_values(raw_value: str) -> list[str]:
    text = str(raw_value or "").strip()
    if not text:
        return []

    parts = [
        re.sub(r"\s+", " ", item).strip(" ,.;")
        for item in re.split(r",|\bor\b|/", text, flags=re.IGNORECASE)
    ]
    return [item for item in parts if item]


def _expand_rule_attribute_terms(attribute_terms: list[str]) -> set[str]:
    expanded: set[str] = set()
    for item in attribute_terms:
        token = _normalize_attr_token(str(item or ""))
        if not token:
            continue
        expanded.add(token)
        aliases = _RULE_VALUE_ATTR_ALIASES.get(token)
        if aliases:
            expanded.update({_normalize_attr_token(alias) for alias in aliases if _normalize_attr_token(alias)})
    return expanded


def _get_attribute_values_for_terms(attributes: dict[str, Any], attribute_terms: list[str]) -> list[str]:
    attrs = attributes if isinstance(attributes, dict) else {}
    if not attrs:
        return []

    expanded_terms = _expand_rule_attribute_terms(attribute_terms)
    values: list[str] = []
    for key, value in attrs.items():
        normalized_key = _normalize_attr_token(str(key or ""))
        if normalized_key not in expanded_terms:
            continue
        if isinstance(value, list):
            values.extend([str(item).strip() for item in value if str(item).strip()])
        elif value not in (None, "", {}, []):
            values.append(str(value).strip())
    return values


def _is_ticket_like_financial_entity(entity_class_norm: str, attributes: dict[str, Any]) -> bool:
    if entity_class_norm == "ticket":
        return True
    if entity_class_norm not in {"invoice", "bill", "receipt", "document", "entity"}:
        return False

    invoice_type = _normalize_class_token(str(attributes.get("invoice_type") or ""))
    if invoice_type in {"ticket", "travel_ticket", "rail_ticket"}:
        return True

    for key in (
        "ticket_id",
        "ticket_type",
        "fare_type",
        "journey_type",
        "origin",
        "destination",
        "departure_time",
        "arrival_time",
    ):
        if attributes.get(key) not in (None, "", [], {}):
            return True
    return False


def _matches_rule_condition(attributes: dict[str, Any], condition: dict[str, Any]) -> bool:
    attribute_terms = [str(item).strip() for item in (condition.get("attribute_terms") or []) if str(item).strip()]
    if not attribute_terms:
        return False

    operator = str(condition.get("operator") or "contains_any").strip().lower()
    expected_values = [
        _normalize_rule_value(item)
        for item in (condition.get("values") or condition.get("value") or [])
        if _normalize_rule_value(item)
    ]
    if not expected_values and condition.get("value") not in (None, ""):
        normalized_single = _normalize_rule_value(condition.get("value"))
        if normalized_single:
            expected_values = [normalized_single]

    candidate_values = [_normalize_rule_value(item) for item in _get_attribute_values_for_terms(attributes, attribute_terms)]
    candidate_values = [item for item in candidate_values if item]
    if not candidate_values:
        return False

    if operator in {"equals", "equals_any", "in", "is"}:
        return any(candidate == expected for candidate in candidate_values for expected in expected_values)

    if operator in {"contains", "contains_any", "matches"}:
        return any(expected in candidate for candidate in candidate_values for expected in expected_values)

    if operator in {"exists", "present"}:
        return bool(candidate_values)

    return any(expected in candidate for candidate in candidate_values for expected in expected_values)


def _extract_post_extraction_directives_from_rule_text(rule_text: str) -> list[dict[str, Any]]:
    text = str(rule_text or "").strip()
    if not text:
        return []

    lower_text = text.lower()
    target_entity_classes: list[str] = []
    for label, normalized in (("receipts", "receipt"), ("receipt", "receipt"), ("invoices", "invoice"), ("invoice", "invoice"), ("bills", "bill"), ("bill", "bill")):
        if re.search(rf"\b{label}\b", lower_text):
            if normalized not in target_entity_classes:
                target_entity_classes.append(normalized)

    generic_entity_match = re.search(
        r"\b(?:all|any|for)\s+([a-zA-Z_][a-zA-Z0-9_\- ]{2,80}?)(?=\s+(?:documents?|entities?|records?|profiles?|contracts?|receipts?|invoices?|bills?)\b)",
        text,
        flags=re.IGNORECASE,
    )
    if generic_entity_match:
        normalized_class = _normalize_class_token(str(generic_entity_match.group(1) or ""))
        if normalized_class and normalized_class not in target_entity_classes:
            target_entity_classes.append(normalized_class)

    conditions: list[dict[str, Any]] = []
    from_match = re.search(
        r"\bfrom\s+([A-Za-z0-9&().,'\-/ ]{2,120}?)(?=\s+(?:should|must|are|is|be|to|book(?:ed|ing)?)\b|[\.;,]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if from_match:
        vendor_name = str(from_match.group(1) or "").strip()
        if vendor_name:
            conditions.append(
                {
                    "attribute_terms": ["vendor", "merchant", "issuer", "supplier", "payee"],
                    "operator": "contains_any",
                    "values": [vendor_name],
                }
            )

    generic_conditions = re.findall(
        r"\b(?:if|when)\s+([a-zA-Z0-9_ ]{2,60}?)\s+(?:is|equals|contains|includes)\s+([A-Za-z0-9&().,'\-/ ]{1,120}?)(?=\s+(?:then|set|mark|route|send|append|add)\b|[\.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    for attribute_name, expected_value in generic_conditions:
        normalized_attr = _normalize_attr_token(str(attribute_name or ""))
        normalized_values = _split_rule_condition_values(str(expected_value or ""))
        if normalized_attr and normalized_values:
            conditions.append(
                {
                    "attribute_terms": [normalized_attr],
                    "operator": "contains_any",
                    "values": normalized_values,
                }
            )

    set_attributes: dict[str, Any] = {}
    set_metadata: dict[str, Any] = {}
    append_workflow_processes: list[str] = []
    target_scope = "entity"

    company_match = re.search(
        r"\bbook(?:ed|ing)?\s+(?:for|to)\s+(?:the\s+)?company\s+([A-Za-z0-9&().,'\-/ ]{2,120}?)(?=[\.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if company_match:
        company_name = str(company_match.group(1) or "").strip()
        if company_name:
            set_attributes.update(
                {
                    "legal_entity_ref": company_name,
                    "company_ref": company_name,
                    "employer_ref": company_name,
                    "current_employer_ref": company_name,
                }
            )

    account_number_match = re.search(
        r"\bbook(?:ed|ing)?\s+to\s+account\s*(?:number\s*)?(\d{3,6})\b",
        text,
        flags=re.IGNORECASE,
    )
    if account_number_match:
        set_attributes["booking_debit_account_number"] = int(account_number_match.group(1))
    else:
        account_name_match = re.search(
            r"\bbook(?:ed|ing)?\s+to\s+([A-Za-z][A-Za-z0-9&/,'\- ]{2,80}?)\s+account\b",
            text,
            flags=re.IGNORECASE,
        )
        if account_name_match:
            set_attributes["booking_debit_account_name"] = str(account_name_match.group(1) or "").strip()

    particulars_match = re.search(
        r"\bparticulars?\s+(?:are|is|should\s+be|must\s+be)\s+([A-Za-z0-9&/,'\- ]{2,120}?)(?=[\.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if particulars_match:
        particulars = str(particulars_match.group(1) or "").strip()
        if particulars:
            set_attributes["booking_particulars"] = particulars

    generic_set_clauses = re.findall(
        r"\bset\s+([a-zA-Z_][a-zA-Z0-9_\- ]{1,80}?)\s+to\s+([A-Za-z0-9&().,'\-/ ]{1,120}?)(?=\s+(?:and|then)\b|[\.;,]|$)",
        text,
        flags=re.IGNORECASE,
    )
    for raw_key, raw_value in generic_set_clauses:
        normalized_key = _normalize_attr_token(str(raw_key or ""))
        normalized_value = str(raw_value or "").strip()
        if normalized_key and normalized_value:
            set_attributes[normalized_key] = normalized_value

    generic_mark_clauses = re.findall(
        r"\bmark\s+([a-zA-Z_][a-zA-Z0-9_\- ]{1,80}?)\s+(?:as|to)\s+([A-Za-z0-9&().,'\-/ ]{1,120}?)(?=\s+(?:and|then)\b|[\.;,]|$)",
        text,
        flags=re.IGNORECASE,
    )
    for raw_key, raw_value in generic_mark_clauses:
        normalized_key = _normalize_attr_token(str(raw_key or ""))
        normalized_value = str(raw_value or "").strip()
        if normalized_key and normalized_value:
            set_attributes[normalized_key] = normalized_value

    route_match = re.search(
        r"\b(?:route|send)\s+(?:the\s+)?document\s+to\s+([A-Za-z0-9_\- ]{2,80}?)(?=[\.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if route_match:
        target_scope = "document"
        queue_name = _slug(str(route_match.group(1) or ""))
        if queue_name:
            set_metadata["review_queue"] = queue_name

    process_match = re.search(
        r"\b(?:append|add|run|trigger)\s+(?:workflow\s+)?process\s+([A-Za-z0-9_\- ]{2,80}?)(?=[\.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if process_match:
        process_name = str(process_match.group(1) or "").strip()
        if process_name:
            process_resolution = canonicalize_workflow_process(process_name)
            normalized_process = str(process_resolution.get("normalized_process") or "").strip()
            if normalized_process:
                append_workflow_processes.append(normalized_process)
                target_scope = "document"

    if not set_attributes and not set_metadata and not append_workflow_processes:
        return []

    return [
        {
            "target_scope": target_scope,
            "target_entity_classes": target_entity_classes,
            "conditions": conditions,
            "set_attributes": set_attributes,
            "set_metadata": set_metadata,
            "append_workflow_processes": append_workflow_processes,
            "overwrite_existing": True,
            "source": "rule_text_fallback",
        }
    ]


def _collect_post_extraction_directives(structured_rule: dict[str, Any], rule_text: str) -> list[dict[str, Any]]:
    normalized = _normalize_structured_rule(structured_rule or {})
    directives = normalized.get("post_extraction_directives") if isinstance(normalized.get("post_extraction_directives"), list) else []
    if directives:
        return [item for item in directives if isinstance(item, dict)][:INFERENCE_MAX_DIRECTIVES_PER_RULE]
    return _extract_post_extraction_directives_from_rule_text(rule_text)


def _looks_like_entity_reference_token(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return bool(text and re.fullmatch(r"e\d+", text))


def _build_entity_name_by_id(entities: list[Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        entity_id = str(entity.get("entity_id") or "").strip()
        entity_name = str(entity.get("entity_name") or entity.get("name") or "").strip()
        if entity_id and entity_name:
            out[entity_id] = entity_name
    return out


def _looks_like_reference_attribute_key(value: Any) -> bool:
    key = _normalize_attr_token(str(value or ""))
    return bool(key and (key in REFERENCE_ATTRIBUTE_KEYS or key.endswith("_ref")))


def _directive_matches_entity(
    directive: dict[str, Any],
    attributes: dict[str, Any],
    context: dict[str, Any],
    *,
    explicit_selected: bool,
) -> bool:
    scope = directive.get("scope") if isinstance(directive.get("scope"), dict) else {}
    if scope and not _scope_match(scope, context):
        return False

    target_entity_classes = {
        _normalize_class_token(str(item or ""))
        for item in (directive.get("target_entity_classes") or [])
        if _normalize_class_token(str(item or ""))
    }
    entity_class = _normalize_class_token(str(context.get("entity_class") or ""))
    document_type = _normalize_class_token(str(context.get("document_type") or ""))
    if target_entity_classes and entity_class not in target_entity_classes and document_type not in target_entity_classes:
        return False

    conditions = [item for item in (directive.get("conditions") or []) if isinstance(item, dict)]
    if not conditions:
        return explicit_selected or not target_entity_classes or entity_class in target_entity_classes or document_type in target_entity_classes

    return all(_matches_rule_condition(attributes, condition) for condition in conditions)


def _apply_rule_directive_attributes(
    attributes: dict[str, Any],
    set_attributes: dict[str, Any],
    overwrite_existing: bool,
    *,
    entity_name_by_id: dict[str, str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    attrs = dict(attributes or {})
    id_to_name = entity_name_by_id if isinstance(entity_name_by_id, dict) else {}
    changed_keys: list[str] = []
    for key, value in set_attributes.items():
        target_key = str(key or "").strip()
        if not target_key:
            continue
        normalized_key = _normalize_attr_token(target_key)
        resolved_value = value
        if normalized_key in REFERENCE_ATTRIBUTE_KEYS and _looks_like_entity_reference_token(value):
            candidate_name = str(id_to_name.get(str(value).strip()) or "").strip()
            if candidate_name:
                resolved_value = candidate_name
        if overwrite_existing or target_key not in attrs or attrs.get(target_key) in (None, "", [], {}):
            if attrs.get(target_key) != resolved_value:
                attrs[target_key] = resolved_value
                changed_keys.append(target_key)
    return attrs, changed_keys


def apply_post_extraction_business_rules(
    extracted: dict[str, Any],
    *,
    context: dict[str, Any] | None = None,
    selected_rules: list[dict[str, Any]] | None = None,
    include_inferred: bool = True,
    include_inactive: bool = False,
) -> dict[str, Any]:
    payload = dict(extracted or {})
    entities = payload.get("entities") if isinstance(payload.get("entities"), list) else []
    document = payload.get("document") if isinstance(payload.get("document"), dict) else {}
    metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}

    base_context = dict(context or {})
    base_context.setdefault("country", document.get("country") or metadata.get("country"))
    base_context.setdefault("document_type", document.get("doc_type"))
    base_context.setdefault("operation", "ingest")

    explicit_rules = [item for item in (selected_rules or []) if isinstance(item, dict)]
    explicit_rule_ids = {int(item.get("rule_id") or 0) for item in explicit_rules if int(item.get("rule_id") or 0) > 0}
    inferred_rules: list[dict[str, Any]] = []
    if include_inferred:
        inferred_rules = list_business_rules(is_active=None if include_inactive else True, limit=400)
        if explicit_rule_ids:
            inferred_rules = [row for row in inferred_rules if int(row.get("rule_id") or 0) not in explicit_rule_ids]
        inferred_rules = [
            row for row in inferred_rules
            if _resolve_rule_application_mode(
                row.get("structured_rule") if isinstance(row.get("structured_rule"), dict) else {},
                str(row.get("rule_text") or ""),
            ) == "general"
        ]
        inferred_rules = inferred_rules[:INFERENCE_MAX_RULES]

    applied_rules: list[dict[str, Any]] = []
    appended_workflow_processes: list[str] = []
    document_metadata = dict(metadata)
    document_attrs = dict(document)
    patched_entities: list[Any] = []
    entity_name_by_id = _build_entity_name_by_id(entities)
    peer_vendor_candidates = [
        str(entity.get("entity_name") or entity.get("name") or "").strip()
        for entity in entities
        if isinstance(entity, dict)
        and _normalize_class_token(str(entity.get("class_name") or entity.get("entity_type") or "")) in {"organization", "company", "service"}
        and str(entity.get("entity_name") or entity.get("name") or "").strip()
    ]
    explicit_rule_stats: dict[int, dict[str, Any]] = {}
    for rule in explicit_rules:
        rule_id = int(rule.get("rule_id") or 0)
        if rule_id <= 0:
            continue
        structured = rule.get("structured_rule") if isinstance(rule.get("structured_rule"), dict) else {}
        directives = _collect_post_extraction_directives(structured, str(rule.get("rule_text") or ""))
        explicit_rule_stats[rule_id] = {
            "rule_name": str(rule.get("rule_name") or ""),
            "directive_count": len([d for d in directives if isinstance(d, dict)]),
            "matched_count": 0,
        }

    def _record_application(rule: dict[str, Any], changed_keys: list[str], *, entity: dict[str, Any] | None, explicit_selected: bool, target_scope: str) -> None:
        applied_rules.append(
            {
                "rule_id": int(rule.get("rule_id") or 0) or None,
                "rule_name": str(rule.get("rule_name") or ""),
                "entity_name": str((entity or {}).get("entity_name") or (entity or {}).get("name") or ""),
                "entity_class": str((entity or {}).get("class_name") or (entity or {}).get("entity_type") or ""),
                "changed_keys": changed_keys,
                "selection_mode": "explicit" if explicit_selected else "inferred",
                "target_scope": target_scope,
            }
        )

    def _apply_document_directives(rule_set: list[dict[str, Any]], explicit_selected: bool) -> None:
        nonlocal document_metadata, document_attrs, appended_workflow_processes
        document_context = dict(base_context)
        document_context["entity_class"] = "document"
        document_condition_attrs = dict(document_attrs)
        document_condition_attrs.update(document_metadata)
        for rule in rule_set:
            structured = rule.get("structured_rule") if isinstance(rule.get("structured_rule"), dict) else {}
            directives = _collect_post_extraction_directives(structured, str(rule.get("rule_text") or ""))
            for directive in directives:
                if str(directive.get("target_scope") or "entity").strip().lower() != "document":
                    continue
                if not _directive_matches_entity(directive, document_condition_attrs, document_context, explicit_selected=explicit_selected):
                    continue
                if explicit_selected:
                    rid = int(rule.get("rule_id") or 0)
                    if rid > 0 and rid in explicit_rule_stats:
                        explicit_rule_stats[rid]["matched_count"] = int(explicit_rule_stats[rid].get("matched_count") or 0) + 1
                changed_keys: list[str] = []
                set_attributes = directive.get("set_attributes") if isinstance(directive.get("set_attributes"), dict) else {}
                if set_attributes:
                    updated_attrs, attr_changes = _apply_rule_directive_attributes(
                        document_attrs,
                        set_attributes,
                        bool(directive.get("overwrite_existing", True)),
                        entity_name_by_id=entity_name_by_id,
                    )
                    document_attrs = updated_attrs
                    changed_keys.extend([f"document.{key}" for key in attr_changes])
                set_metadata = directive.get("set_metadata") if isinstance(directive.get("set_metadata"), dict) else {}
                for key, value in set_metadata.items():
                    target_key = str(key or "").strip()
                    if not target_key:
                        continue
                    overwrite = bool(directive.get("overwrite_existing", True))
                    if overwrite or document_metadata.get(target_key) in (None, "", [], {}):
                        if document_metadata.get(target_key) != value:
                            document_metadata[target_key] = value
                            changed_keys.append(f"metadata.{target_key}")
                for process_name in (directive.get("append_workflow_processes") or [])[:INFERENCE_MAX_WORKFLOW_PROCESS_ADDITIONS]:
                    normalized_process = str(process_name or "").strip()
                    if normalized_process and normalized_process not in appended_workflow_processes:
                        appended_workflow_processes.append(normalized_process)
                        changed_keys.append(f"workflow_processes.{normalized_process}")
                document_condition_attrs = dict(document_attrs)
                document_condition_attrs.update(document_metadata)
                if changed_keys:
                    _record_application(rule, changed_keys, entity=None, explicit_selected=explicit_selected, target_scope="document")

    _apply_document_directives(explicit_rules, True)
    _apply_document_directives(inferred_rules, False)

    for entity in entities:
        if not isinstance(entity, dict):
            patched_entities.append(entity)
            continue

        item = dict(entity)
        attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
        attrs = dict(attrs)
        condition_attrs = dict(attrs)
        entity_context = dict(base_context)
        entity_context["entity_class"] = item.get("class_name") or item.get("entity_type")
        entity_class_norm = _normalize_class_token(str(entity_context.get("entity_class") or ""))
        entity_name_norm = _normalize_class_token(str(item.get("entity_name") or item.get("name") or ""))
        ticket_like = entity_class_norm in {"ticket", "paid_ticket"} or "ticket" in entity_name_norm or any(
            key in attrs
            for key in (
                "ticket_id",
                "ticket_type",
                "fare_type",
                "class_of_service",
                "journey_type",
                "origin",
                "destination",
                "valid_from",
                "valid_until",
            )
        )
        if _normalize_class_token(str(base_context.get("document_type") or "")) == "email" and entity_class_norm in {"invoice", "bill", "receipt"}:
            # For email-container documents, evaluate entity-scope document_type rules against
            # the embedded financial entity class so invoice/bill/receipt directives can match.
            entity_context["document_type"] = entity_class_norm
        elif _normalize_class_token(str(base_context.get("document_type") or "")) == "email" and ticket_like:
            # Some extractors emit ticket rows as generic entity/document classes; map these
            # to receipt scope so selected invoice/bill/receipt booking rules can still apply.
            entity_context["document_type"] = "receipt"

        # Some ticket-like financial entities do not carry explicit issuer/vendor fields,
        # while the vendor appears in sibling organization/service entities.
        # Expose those names as condition-only candidates so contains_any vendor rules can match.
        if _is_ticket_like_financial_entity(entity_class_norm, condition_attrs):
            has_direct_vendor_fields = bool(
                _get_attribute_values_for_terms(
                    condition_attrs,
                    ["vendor", "issuer", "supplier", "legal_name"],
                )
            )
            if not has_direct_vendor_fields and peer_vendor_candidates:
                condition_attrs["vendor_candidates"] = list(peer_vendor_candidates)

        for rule in explicit_rules:
            rule_id = int(rule.get("rule_id") or 0) or None
            attrs = apply_business_rules_to_attributes(attrs, context=entity_context, rule_id=rule_id, include_inactive=include_inactive)

        for rule_set, explicit_selected in ((explicit_rules, True), (inferred_rules, False)):
            for rule in rule_set:
                structured = rule.get("structured_rule") if isinstance(rule.get("structured_rule"), dict) else {}
                directives = _collect_post_extraction_directives(structured, str(rule.get("rule_text") or ""))
                if not directives:
                    continue
                for directive in directives:
                    if str(directive.get("target_scope") or "entity").strip().lower() != "entity":
                        continue
                    if not _directive_matches_entity(directive, condition_attrs, entity_context, explicit_selected=explicit_selected):
                        continue
                    if explicit_selected:
                        rid = int(rule.get("rule_id") or 0)
                        if rid > 0 and rid in explicit_rule_stats:
                            explicit_rule_stats[rid]["matched_count"] = int(explicit_rule_stats[rid].get("matched_count") or 0) + 1
                    attrs, changed_keys = _apply_rule_directive_attributes(
                        attrs,
                        directive.get("set_attributes") if isinstance(directive.get("set_attributes"), dict) else {},
                        bool(directive.get("overwrite_existing", True)),
                        entity_name_by_id=entity_name_by_id,
                    )
                    if changed_keys:
                        _record_application(rule, changed_keys, entity=item, explicit_selected=explicit_selected, target_scope="entity")

        item["attributes"] = attrs
        patched_entities.append(item)

    metadata = dict(document_metadata)
    if applied_rules:
        metadata["post_extraction_rule_applications"] = applied_rules
    if appended_workflow_processes:
        existing_processes = metadata.get("workflow_processes") if isinstance(metadata.get("workflow_processes"), list) else []
        merged_processes = [str(item).strip() for item in existing_processes if str(item).strip()]
        for process_name in appended_workflow_processes:
            if process_name.lower() not in {item.lower() for item in merged_processes}:
                merged_processes.append(process_name)
        metadata["workflow_processes"] = merged_processes

    unresolved_reference_tokens: list[dict[str, Any]] = []
    for application in applied_rules:
        if not isinstance(application, dict):
            continue
        entity_name = str(application.get("entity_name") or "").strip()
        target_scope = str(application.get("target_scope") or "entity").strip().lower() or "entity"
        changed_keys = application.get("changed_keys") if isinstance(application.get("changed_keys"), list) else []
        target_attrs: dict[str, Any] = {}
        if target_scope == "document":
            target_attrs = document_attrs
        elif entity_name:
            target_entity = next(
                (
                    e for e in patched_entities
                    if isinstance(e, dict) and str(e.get("entity_name") or e.get("name") or "").strip() == entity_name
                ),
                None,
            )
            target_attrs = target_entity.get("attributes") if isinstance((target_entity or {}).get("attributes"), dict) else {}
        for changed_key in changed_keys:
            key_text = str(changed_key or "").strip()
            if key_text.startswith("document."):
                key_text = key_text[9:]
            if key_text.startswith("attributes."):
                key_text = key_text[11:]
            if not _looks_like_reference_attribute_key(key_text):
                continue
            current_value = str((target_attrs or {}).get(key_text) or "").strip()
            if _looks_like_entity_reference_token(current_value):
                unresolved_reference_tokens.append(
                    {
                        "rule_name": str(application.get("rule_name") or ""),
                        "entity_name": entity_name,
                        "target_scope": target_scope,
                        "attribute": key_text,
                        "value": current_value,
                    }
                )

    selected_rules_not_applied = [
        {
            "rule_id": int(rule_id),
            "rule_name": str(stats.get("rule_name") or ""),
            "directive_count": int(stats.get("directive_count") or 0),
            "matched_count": int(stats.get("matched_count") or 0),
        }
        for rule_id, stats in explicit_rule_stats.items()
        if int(stats.get("directive_count") or 0) > 0 and int(stats.get("matched_count") or 0) == 0
    ]

    integrity_violations: list[dict[str, Any]] = []
    for item in selected_rules_not_applied:
        integrity_violations.append({"type": "selected_rule_not_applied", **item})
    for item in unresolved_reference_tokens:
        integrity_violations.append({"type": "unresolved_reference_token", **item})

    rule_integrity = {
        "selected_rule_stats": [
            {
                "rule_id": int(rule_id),
                "rule_name": str(stats.get("rule_name") or ""),
                "directive_count": int(stats.get("directive_count") or 0),
                "matched_count": int(stats.get("matched_count") or 0),
            }
            for rule_id, stats in explicit_rule_stats.items()
        ],
        "selected_rules_not_applied": selected_rules_not_applied,
        "unresolved_reference_tokens": unresolved_reference_tokens,
        "violations": integrity_violations,
        "violation_count": len(integrity_violations),
    }
    metadata["rule_application_integrity"] = rule_integrity

    document = dict(document_attrs)
    document["metadata"] = metadata
    payload["document"] = document
    payload["entities"] = patched_entities
    return {
        "extracted": payload,
        "applied_rules": applied_rules,
        "applied_count": len(applied_rules),
        "rule_integrity": rule_integrity,
    }


def resolve_workflow_hint(context: dict[str, Any] | None = None) -> dict[str, Any] | None:
    ctx = dict(context or {})
    process_resolution = canonicalize_workflow_process(
        str(ctx.get("process") or ""),
        domain=str(ctx.get("domain") or ""),
    )
    process = str(process_resolution.get("normalized_process") or "")
    if not process:
        return None

    rules = list_business_rules(is_active=True, limit=400)
    for row in rules:
        structured = row.get("structured_rule") if isinstance(row.get("structured_rule"), dict) else {}
        hints = structured.get("workflow_hints") if isinstance(structured.get("workflow_hints"), list) else []
        for hint in hints:
            if not isinstance(hint, dict):
                continue
            hint_process = str(hint.get("process") or "").strip().lower()
            if hint_process and hint_process != process:
                continue
            return {
                "control_flow": canonicalize_control_flow(str(hint.get("control_flow") or "sequence")) or "sequence",
                "stop_on_error": bool(hint.get("stop_on_error", True)),
                "rule_id": row.get("rule_id"),
                "rule_name": row.get("rule_name"),
                "resolved_process": process,
                "process_resolution": process_resolution,
            }
    return None


def simulate_business_rule_resolution(
    requested_attribute: str = "",
    context: dict[str, Any] | None = None,
    sample_attributes: dict[str, Any] | None = None,
    rule_id: int | None = None,
    include_inactive: bool = True,
) -> dict[str, Any]:
    ctx = dict(context or {})

    resolved_attribute = resolve_attribute_via_business_rules(
        requested_attribute=requested_attribute,
        context=ctx,
        rule_id=rule_id,
        include_inactive=include_inactive,
    )

    transformed_attributes = apply_business_rules_to_attributes(
        attributes=dict(sample_attributes or {}),
        context=ctx,
        rule_id=rule_id,
        include_inactive=include_inactive,
    )

    workflow_hint = None
    process_resolution = canonicalize_workflow_process(
        str(ctx.get("process") or ""),
        domain=str(ctx.get("domain") or ""),
    )
    process = str(process_resolution.get("normalized_process") or "")
    if process:
        rules = list_business_rules(is_active=None if include_inactive else True, limit=400)
        if rule_id is not None:
            rules = [row for row in rules if int(row.get("rule_id") or 0) == int(rule_id)]
        for row in rules:
            structured = row.get("structured_rule") if isinstance(row.get("structured_rule"), dict) else {}
            hints = structured.get("workflow_hints") if isinstance(structured.get("workflow_hints"), list) else []
            for hint in hints:
                if not isinstance(hint, dict):
                    continue
                hint_process = str(hint.get("process") or "").strip().lower()
                if hint_process and hint_process != process:
                    continue
                workflow_hint = {
                    "control_flow": canonicalize_control_flow(str(hint.get("control_flow") or "sequence")) or "sequence",
                    "stop_on_error": bool(hint.get("stop_on_error", True)),
                    "rule_id": row.get("rule_id"),
                    "rule_name": row.get("rule_name"),
                    "resolved_process": process,
                    "process_resolution": process_resolution,
                }
                break
            if workflow_hint:
                break

    return {
        "requested_attribute": requested_attribute,
        "context": ctx,
        "rule_id": int(rule_id) if rule_id is not None else None,
        "include_inactive": bool(include_inactive),
        "resolved_attribute": resolved_attribute,
        "workflow_hint": workflow_hint,
        "transformed_attributes": transformed_attributes,
        "matched": bool(resolved_attribute or workflow_hint),
    }


def _resolve_attribute_from_structured_rule(
    structured_rule: dict[str, Any],
    requested_attribute: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    req = _normalize_attr_token(str(requested_attribute or ""))
    if not req:
        return None

    ctx = dict(context or {})
    mappings = structured_rule.get("mappings") if isinstance(structured_rule.get("mappings"), list) else []
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        source_terms = {_normalize_attr_token(str(v or "")) for v in (mapping.get("source_attribute_terms") or [])}
        source_terms.discard("")
        if req not in source_terms:
            continue
        scope = mapping.get("scope") if isinstance(mapping.get("scope"), dict) else {}
        if not _scope_match(scope, ctx):
            continue
        target = _normalize_attr_token(str(mapping.get("target_attribute") or ""))
        relationship = str(mapping.get("relationship") or "same_as").strip().lower()
        distinct_from = [
            _normalize_attr_token(str(v or ""))
            for v in (mapping.get("distinct_from") or [])
            if _normalize_attr_token(str(v or ""))
        ]
        return {
            "canonical_attribute": target or req,
            "relationship": relationship,
            "distinct_from": distinct_from,
            "rule_id": None,
            "rule_name": structured_rule.get("rule_name") or "draft_rule",
        }
    return None


def _resolve_workflow_hint_from_structured_rule(
    structured_rule: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    ctx = dict(context or {})
    process_resolution = canonicalize_workflow_process(
        str(ctx.get("process") or ""),
        domain=str(ctx.get("domain") or ""),
    )
    process = str(process_resolution.get("normalized_process") or "")
    if not process:
        return None

    hints = structured_rule.get("workflow_hints") if isinstance(structured_rule.get("workflow_hints"), list) else []
    for hint in hints:
        if not isinstance(hint, dict):
            continue
        hint_process = str(hint.get("process") or "").strip().lower()
        if hint_process and hint_process != process:
            continue
        return {
            "control_flow": canonicalize_control_flow(str(hint.get("control_flow") or "sequence")) or "sequence",
            "stop_on_error": bool(hint.get("stop_on_error", True)),
            "rule_id": None,
            "rule_name": structured_rule.get("rule_name") or "draft_rule",
            "resolved_process": process,
            "process_resolution": process_resolution,
        }
    return None


def _apply_structured_rule_to_attributes(
    structured_rule: dict[str, Any],
    attributes: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    attrs = dict(attributes or {})
    if not attrs:
        return attrs

    ctx = dict(context or {})
    normalized_keys = {_normalize_attr_token(str(k or "")): str(k) for k in attrs.keys()}
    for norm_key, original_key in list(normalized_keys.items()):
        resolved = _resolve_attribute_from_structured_rule(structured_rule, norm_key, ctx)
        if not resolved:
            continue
        target = str(resolved.get("canonical_attribute") or "").strip()
        relationship = str(resolved.get("relationship") or "same_as").strip().lower()
        if relationship == "same_as" and target:
            source_val = attrs.get(original_key)
            if source_val is not None and target not in attrs:
                attrs[target] = source_val
        elif relationship == "distinct":
            distinct_from = [str(v).strip() for v in (resolved.get("distinct_from") or []) if str(v).strip()]
            if target and target not in attrs and original_key in attrs:
                attrs[target] = attrs.get(original_key)
            for attr_name in distinct_from:
                if attr_name in attrs and target in attrs and attrs[attr_name] == attrs[target]:
                    meta = attrs.get("_business_rule_flags") if isinstance(attrs.get("_business_rule_flags"), dict) else {}
                    meta[f"distinct_conflict_{target}_{attr_name}"] = True
                    attrs["_business_rule_flags"] = meta
    return attrs


def _extract_clause_name(clause_text: str) -> str:
    first_line = str(clause_text or "").strip().splitlines()
    if not first_line:
        return ""
    head = first_line[0].strip()
    match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", head)
    if match:
        return str(match.group(1) or "").strip()
    return ""


def _extract_solf_class_definitions_from_script(solf_script: str) -> list[dict[str, Any]]:
    """Extract SOLF class blocks in the `class_name ≔ { ... }` format."""
    classes: list[dict[str, Any]] = []
    lines = str(solf_script or "").splitlines()
    line_index = 0

    while line_index < len(lines):
        line = lines[line_index]
        match = re.match(r"^([a-z_][a-z0-9_]*)\s*≔\s*\{\s*$", line.strip())
        if not match:
            line_index += 1
            continue

        class_name = str(match.group(1) or "").strip().lower()
        body_lines: list[str] = []
        line_index += 1
        while line_index < len(lines):
            body_line = lines[line_index]
            if body_line.strip() == "}":
                break
            body_lines.append(body_line)
            line_index += 1
        line_index += 1

        body = "\n".join(body_lines)
        if "objectType: class" not in body:
            continue

        parent_match = re.search(r"extending:\s*([a-z_][a-z0-9_]*)", body)
        parent_class_name = str(parent_match.group(1) or "").strip().lower() if parent_match else None

        attrs: list[str] = []
        for body_line in body.splitlines():
            stripped = body_line.strip().rstrip(",")
            if not stripped or ":" not in stripped:
                continue
            key = str(stripped.split(":", 1)[0] or "").strip()
            if key in {"objectType", "extending"}:
                continue
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                norm = _normalize_attr_token(key)
                if norm and norm not in attrs:
                    attrs.append(norm)

        classes.append(
            {
                "class_name": class_name,
                "parent_class_name": parent_class_name,
                "attributes": sorted(attrs),
            }
        )

    classes.sort(key=lambda item: str(item.get("class_name") or ""))
    return classes


def _extract_solf_clauses_from_script(solf_script: str) -> list[dict[str, Any]]:
    """Extract SOLF clauses from script text while skipping class blocks."""
    text = str(solf_script or "")
    lines = text.splitlines()
    clauses: list[dict[str, Any]] = []
    current_lines: list[str] = []
    depth = 0
    in_clause = False
    line_index = 0

    while line_index < len(lines):
        line = lines[line_index]
        stripped = line.strip()

        # Skip class blocks (`name ≔ { ... }`) so they are not treated as clauses.
        if not in_clause and re.match(r"^[a-z_][a-z0-9_]*\s*≔\s*\{\s*$", stripped):
            line_index += 1
            while line_index < len(lines):
                if lines[line_index].strip() == "}":
                    break
                line_index += 1
            line_index += 1
            continue

        if not in_clause:
            starts_clause = bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*\(.*\)\s*⦃", stripped))
            if starts_clause:
                in_clause = True
                current_lines = [line]
                depth = line.count("⦃") - line.count("⦄")
                if depth <= 0:
                    clause_text = "\n".join(current_lines).strip()
                    if clause_text:
                        clauses.append(
                            {
                                "clause_name": _extract_clause_name(clause_text),
                                "clause_text": clause_text,
                            }
                        )
                    in_clause = False
                    current_lines = []
                    depth = 0
            line_index += 1
            continue

        current_lines.append(line)
        depth += line.count("⦃") - line.count("⦄")
        if depth <= 0:
            clause_text = "\n".join(current_lines).strip()
            if clause_text:
                clauses.append(
                    {
                        "clause_name": _extract_clause_name(clause_text),
                        "clause_text": clause_text,
                    }
                )
            in_clause = False
            current_lines = []
            depth = 0

        line_index += 1

    return clauses


def _infer_clause_type_for_script(
    clause_name: str,
    clause_text: str,
    default_clause_type: str,
) -> str:
    fallback = str(default_clause_type or "resolve_policy").strip().lower()
    if fallback not in {"resolve_policy", "ingest_rule", "computation_rule"}:
        fallback = "resolve_policy"

    name = str(clause_name or "").strip().lower()
    body = str(clause_text or "").strip().lower()
    if "compute:" in body or name.startswith("compute_") or name.endswith("_compute"):
        return "computation_rule"
    if "ingest" in name:
        return "ingest_rule"
    return fallback


def _extract_fact_preview_from_solf_clauses(clauses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for idx, clause in enumerate(clauses, start=1):
        clause_name = str(clause.get("clause_name") or "").strip().lower()
        clause_text = str(clause.get("clause_text") or "")
        if (
            "fact" not in clause_name
            and "assertion" not in clause_name
            and "fact_name:" not in clause_text
        ):
            continue
        facts.append(
            {
                "fact_type": "solf_clause",
                "fact_key": f"solf_fact_{idx}",
                "clause_name": clause_name,
                "clause_preview": "\n".join(clause_text.splitlines()[:3]),
            }
        )
    return facts


def _validate_solf_clause_syntax(clauses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    if solf_parser is None:
        return errors

    for idx, clause in enumerate(clauses, start=1):
        clause_text = str(clause.get("clause_text") or "")
        if not clause_text.strip():
            continue
        try:
            parsed = solf_parser.parse_script(clause_text)
            if parsed is None:
                errors.append(
                    {
                        "index": idx,
                        "clause_name": str(clause.get("clause_name") or ""),
                        "error": "Parser returned no AST",
                    }
                )
        except Exception as exc:
            errors.append(
                {
                    "index": idx,
                    "clause_name": str(clause.get("clause_name") or ""),
                    "error": str(exc),
                }
            )

    return errors


def review_solf_script_rule(
    solf_script: str,
    rule_name: str | None = None,
    default_clause_type: str = "resolve_policy",
) -> dict[str, Any]:
    classes = _extract_solf_class_definitions_from_script(solf_script)
    clauses = _extract_solf_clauses_from_script(solf_script)
    syntax_errors = _validate_solf_clause_syntax(clauses)
    facts_preview = _extract_fact_preview_from_solf_clauses(clauses)

    normalized_rule_name = _slug(str(rule_name or "")) or "solf_script_rule"
    scope = {
        "countries": [],
        "document_types": [],
        "entity_classes": sorted(
            {
                str(item.get("class_name") or "").strip()
                for item in classes
                if str(item.get("class_name") or "").strip()
            }
        ),
        "operations": [],
        "source": "solf_script",
    }

    artifacts = {
        "class_definition_source": "solf_script",
        "classes": classes,
        "class_count": len(classes),
        "facts_preview": facts_preview,
        "fact_count": len(facts_preview),
        "clauses": [
            {
                "index": idx,
                "clause_name": str(item.get("clause_name") or ""),
                "clause_text": str(item.get("clause_text") or ""),
                "clause_type": _infer_clause_type_for_script(
                    clause_name=str(item.get("clause_name") or ""),
                    clause_text=str(item.get("clause_text") or ""),
                    default_clause_type=default_clause_type,
                ),
            }
            for idx, item in enumerate(clauses, start=1)
        ],
        "clause_count": len(clauses),
        "scope": scope,
    }

    return {
        "rule_name": normalized_rule_name,
        "is_valid": len(syntax_errors) == 0,
        "syntax_error_count": len(syntax_errors),
        "syntax_errors": syntax_errors,
        "artifacts": artifacts,
    }


def create_business_rule_from_solf_script(
    solf_script: str,
    rule_name: str | None = None,
    created_by: str | None = None,
    is_active: bool = True,
    default_clause_type: str = "resolve_policy",
    overwrite_existing: bool | None = None,
) -> dict[str, Any]:
    review = review_solf_script_rule(
        solf_script=solf_script,
        rule_name=rule_name,
        default_clause_type=default_clause_type,
    )
    if not review.get("is_valid", False):
        raise ValueError(f"SOLF script contains syntax errors: {review.get('syntax_errors')}")

    artifacts = review.get("artifacts") if isinstance(review.get("artifacts"), dict) else {}
    classes = artifacts.get("classes") if isinstance(artifacts.get("classes"), list) else []
    clauses = artifacts.get("clauses") if isinstance(artifacts.get("clauses"), list) else []
    facts_preview = artifacts.get("facts_preview") if isinstance(artifacts.get("facts_preview"), list) else []
    scope = artifacts.get("scope") if isinstance(artifacts.get("scope"), dict) else {}

    generated_rule_name = _slug(str(review.get("rule_name") or rule_name or "solf_script_rule")) or "solf_script_rule"

    structured_rule = {
        "rule_name": generated_rule_name,
        "class_definition_source": "solf_script",
        "class_definitions": classes,
        "mappings": [],
        "workflow_hints": [],
        "fact_assertions": facts_preview,
        "multi_hop_dependencies": [],
        "quantified_conditions": [],
        "script_clause_names": [str(item.get("clause_name") or "") for item in clauses if str(item.get("clause_name") or "")],
    }

    connection = object_db.get_connection()
    try:
        object_db.create_tables(connection, recreate=False)

        # Persist class definitions extracted from SOLF class blocks.
        created_classes = _upsert_runtime_solf_classes(
            connection=connection,
            class_definitions=classes,
            created_by=created_by,
        )

        metadata = {
            "source": "solf_script",
            "version": 1,
            "default_clause_type": str(default_clause_type or "resolve_policy"),
        }
        if overwrite_existing is not None:
            metadata["overwrite_existing"] = bool(overwrite_existing)

        rule_id = object_db.upsert_business_rule(
            connection=connection,
            rule_name=generated_rule_name,
            rule_text=f"SOLF script rule: {generated_rule_name}",
            structured_rule=structured_rule,
            solf_script=str(solf_script or ""),
            scope=scope,
            metadata=metadata,
            is_active=is_active,
            created_by=created_by,
        )

        pattern_library = PatternLibrary(connection)
        upserted_clause_ids: list[int] = []
        semantic_terms: list[dict[str, Any]] = []

        for idx, clause in enumerate(clauses, start=1):
            clause_name = str(clause.get("clause_name") or "").strip() or f"script_clause_{idx}"
            clause_body = str(clause.get("clause_text") or "").strip()
            clause_type = _infer_clause_type_for_script(
                clause_name=clause_name,
                clause_text=clause_body,
                default_clause_type=default_clause_type,
            )
            stored_clause_name = f"business_rule_script_{int(rule_id)}_{idx}_{_slug(clause_name) or 'clause'}"
            clause_id = pattern_library.upsert_solf_clause(
                clause_name=stored_clause_name,
                clause_type=clause_type,
                clause_body=clause_body,
                entity_class=None,
                metadata={
                    "source": "business_rule_script",
                    "rule_id": int(rule_id),
                    "rule_name": generated_rule_name,
                    "original_clause_name": clause_name,
                },
                is_active=bool(is_active),
                created_by=created_by or "system:business_rules",
            )
            if isinstance(clause_id, int):
                upserted_clause_ids.append(clause_id)

            semantic_terms.append(
                {
                    "kind": "relationship",
                    "canonical_name": _slug(clause_name) or clause_name.lower(),
                    "term_text": clause_name,
                    "language": "und",
                    "source_type": "solf_clause",
                    "metadata": {
                        "rule_id": int(rule_id),
                        "rule_name": generated_rule_name,
                        "clause_type": clause_type,
                    },
                }
            )

        for class_item in classes:
            class_name = str(class_item.get("class_name") or "").strip().lower()
            if not class_name:
                continue
            semantic_terms.append(
                {
                    "kind": "relationship",
                    "canonical_name": class_name,
                    "term_text": class_name,
                    "language": "und",
                    "source_type": "solf_clause",
                    "metadata": {
                        "rule_id": int(rule_id),
                        "rule_name": generated_rule_name,
                        "source": "solf_class",
                    },
                }
            )
            for attr_name in class_item.get("attributes") or []:
                canonical_attr = _normalize_attr_token(str(attr_name or ""))
                if not canonical_attr:
                    continue
                semantic_terms.append(
                    {
                        "kind": "attribute",
                        "canonical_name": canonical_attr,
                        "term_text": canonical_attr,
                        "language": "und",
                        "source_type": "solf_clause",
                        "metadata": {
                            "rule_id": int(rule_id),
                            "rule_name": generated_rule_name,
                            "class_name": class_name,
                            "source": "solf_class_attribute",
                        },
                    }
                )

        if semantic_terms:
            object_db.upsert_semantic_terms(connection, semantic_terms)

        return {
            "rule_id": int(rule_id),
            "rule_name": generated_rule_name,
            "structured_rule": structured_rule,
            "scope": scope,
            "class_definition_source": "solf_script",
            "class_definitions_created": created_classes,
            "class_definitions_created_count": len(created_classes),
            "solf_clauses_upserted": len(upserted_clause_ids),
            "solf_script": str(solf_script or ""),
            "is_active": bool(is_active),
            "overwrite_existing": bool(overwrite_existing) if overwrite_existing is not None else None,
            "artifacts": artifacts,
            "persisted": True,
        }
    finally:
        connection.close()


def _deactivate_stale_solf_script_clauses(
    connection: Any,
    rule_id: int,
    active_clause_names: list[str],
    updated_by: str | None = None,
) -> int:
    metadata_patch = {
        "deactivation_reason": "solf_script_overwrite_cleanup",
        "updated_by": str(updated_by or ""),
    }
    with connection.cursor() as cursor:
        if active_clause_names:
            cursor.execute(
                """
                UPDATE solf_clauses
                SET is_active = FALSE,
                    modified_at = NOW(),
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                WHERE COALESCE(metadata->>'source', '') = 'business_rule_script'
                  AND COALESCE(metadata->>'rule_id', '') = %s
                  AND clause_name <> ALL(%s)
                """,
                (
                    Json(metadata_patch),
                    str(int(rule_id)),
                    active_clause_names,
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE solf_clauses
                SET is_active = FALSE,
                    modified_at = NOW(),
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                WHERE COALESCE(metadata->>'source', '') = 'business_rule_script'
                  AND COALESCE(metadata->>'rule_id', '') = %s
                """,
                (
                    Json(metadata_patch),
                    str(int(rule_id)),
                ),
            )
        return int(cursor.rowcount or 0)


def update_business_rule_from_solf_script(
    rule_id: int,
    solf_script: str,
    rule_name: str | None = None,
    updated_by: str | None = None,
    is_active: bool | None = None,
    default_clause_type: str = "resolve_policy",
    overwrite_existing: bool | None = None,
) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        current = object_db.get_business_rule_by_id(connection, rule_id=rule_id)
        if current is None:
            return None

        review = review_solf_script_rule(
            solf_script=solf_script,
            rule_name=rule_name or str(current.get("rule_name") or ""),
            default_clause_type=default_clause_type,
        )
        if not review.get("is_valid", False):
            raise ValueError(f"SOLF script contains syntax errors: {review.get('syntax_errors')}")

        artifacts = review.get("artifacts") if isinstance(review.get("artifacts"), dict) else {}
        classes = artifacts.get("classes") if isinstance(artifacts.get("classes"), list) else []
        clauses = artifacts.get("clauses") if isinstance(artifacts.get("clauses"), list) else []
        facts_preview = artifacts.get("facts_preview") if isinstance(artifacts.get("facts_preview"), list) else []
        scope = artifacts.get("scope") if isinstance(artifacts.get("scope"), dict) else {}

        generated_rule_name = _slug(
            str(review.get("rule_name") or rule_name or current.get("rule_name") or "solf_script_rule")
        ) or "solf_script_rule"
        updated_active = bool(current.get("is_active")) if is_active is None else bool(is_active)

        structured_rule = {
            "rule_name": generated_rule_name,
            "class_definition_source": "solf_script",
            "class_definitions": classes,
            "mappings": [],
            "workflow_hints": [],
            "fact_assertions": facts_preview,
            "multi_hop_dependencies": [],
            "quantified_conditions": [],
            "script_clause_names": [str(item.get("clause_name") or "") for item in clauses if str(item.get("clause_name") or "")],
        }

        current_metadata = current.get("metadata") if isinstance(current.get("metadata"), dict) else {}
        metadata = dict(current_metadata)
        previous_version = int(metadata.get("version") or 1)
        metadata["source"] = "solf_script"
        metadata["version"] = previous_version + 1
        metadata["default_clause_type"] = str(default_clause_type or "resolve_policy")
        if overwrite_existing is not None:
            metadata["overwrite_existing"] = bool(overwrite_existing)
        if updated_by:
            metadata["updated_by"] = str(updated_by)

        updated = object_db.update_business_rule(
            connection=connection,
            rule_id=int(rule_id),
            rule_name=generated_rule_name,
            rule_text=f"SOLF script rule: {generated_rule_name}",
            structured_rule=structured_rule,
            solf_script=str(solf_script or ""),
            scope=scope,
            metadata=metadata,
            is_active=updated_active,
        )
        if not updated:
            return None

        created_classes = _upsert_runtime_solf_classes(
            connection=connection,
            class_definitions=classes,
            created_by=updated_by,
        )

        pattern_library = PatternLibrary(connection)
        upserted_clause_ids: list[int] = []
        active_clause_names: list[str] = []
        semantic_terms: list[dict[str, Any]] = []

        for idx, clause in enumerate(clauses, start=1):
            clause_name = str(clause.get("clause_name") or "").strip() or f"script_clause_{idx}"
            clause_body = str(clause.get("clause_text") or "").strip()
            clause_type = _infer_clause_type_for_script(
                clause_name=clause_name,
                clause_text=clause_body,
                default_clause_type=default_clause_type,
            )
            stored_clause_name = f"business_rule_script_{int(rule_id)}_{idx}_{_slug(clause_name) or 'clause'}"
            active_clause_names.append(stored_clause_name)
            clause_id = pattern_library.upsert_solf_clause(
                clause_name=stored_clause_name,
                clause_type=clause_type,
                clause_body=clause_body,
                entity_class=None,
                metadata={
                    "source": "business_rule_script",
                    "rule_id": int(rule_id),
                    "rule_name": generated_rule_name,
                    "original_clause_name": clause_name,
                },
                is_active=bool(updated_active),
                created_by=updated_by or "system:business_rules",
            )
            if isinstance(clause_id, int):
                upserted_clause_ids.append(clause_id)

            semantic_terms.append(
                {
                    "kind": "relationship",
                    "canonical_name": _slug(clause_name) or clause_name.lower(),
                    "term_text": clause_name,
                    "language": "und",
                    "source_type": "solf_clause",
                    "metadata": {
                        "rule_id": int(rule_id),
                        "rule_name": generated_rule_name,
                        "clause_type": clause_type,
                    },
                }
            )

        for class_item in classes:
            class_name = str(class_item.get("class_name") or "").strip().lower()
            if not class_name:
                continue
            semantic_terms.append(
                {
                    "kind": "relationship",
                    "canonical_name": class_name,
                    "term_text": class_name,
                    "language": "und",
                    "source_type": "solf_clause",
                    "metadata": {
                        "rule_id": int(rule_id),
                        "rule_name": generated_rule_name,
                        "source": "solf_class",
                    },
                }
            )
            for attr_name in class_item.get("attributes") or []:
                canonical_attr = _normalize_attr_token(str(attr_name or ""))
                if not canonical_attr:
                    continue
                semantic_terms.append(
                    {
                        "kind": "attribute",
                        "canonical_name": canonical_attr,
                        "term_text": canonical_attr,
                        "language": "und",
                        "source_type": "solf_clause",
                        "metadata": {
                            "rule_id": int(rule_id),
                            "rule_name": generated_rule_name,
                            "class_name": class_name,
                            "source": "solf_class_attribute",
                        },
                    }
                )

        stale_deactivated = _deactivate_stale_solf_script_clauses(
            connection=connection,
            rule_id=int(rule_id),
            active_clause_names=active_clause_names,
            updated_by=updated_by,
        )

        if semantic_terms:
            object_db.upsert_semantic_terms(connection, semantic_terms)

        refreshed = object_db.get_business_rule_by_id(connection, rule_id=int(rule_id))
        connection.commit()

        return {
            "rule_id": int(rule_id),
            "rule_name": generated_rule_name,
            "structured_rule": structured_rule,
            "scope": scope,
            "class_definition_source": "solf_script",
            "class_definitions_created": created_classes,
            "class_definitions_created_count": len(created_classes),
            "solf_clauses_upserted": len(upserted_clause_ids),
            "solf_clauses_deactivated": int(stale_deactivated),
            "solf_script": str(solf_script or ""),
            "is_active": bool(updated_active),
            "metadata": metadata,
            "artifacts": artifacts,
            "created_at": (refreshed or {}).get("created_at").isoformat() if (refreshed or {}).get("created_at") is not None else None,
            "modified_at": (refreshed or {}).get("modified_at").isoformat() if (refreshed or {}).get("modified_at") is not None else None,
            "updated_by": str(updated_by or ""),
            "persisted": True,
        }
    finally:
        connection.close()


def _build_fact_preview_from_structured_rule(structured_rule: dict[str, Any]) -> list[dict[str, Any]]:
    mappings = structured_rule.get("mappings") if isinstance(structured_rule.get("mappings"), list) else []
    facts: list[dict[str, Any]] = []
    for idx, mapping in enumerate(mappings, start=1):
        if not isinstance(mapping, dict):
            continue
        facts.append(
            {
                "fact_type": "attribute_mapping",
                "fact_key": f"mapping_{idx}",
                "source_attribute_terms": [str(v) for v in (mapping.get("source_attribute_terms") or []) if str(v).strip()],
                "target_attribute": str(mapping.get("target_attribute") or ""),
                "relationship": str(mapping.get("relationship") or "same_as"),
                "distinct_from": [str(v) for v in (mapping.get("distinct_from") or []) if str(v).strip()],
                "scope": mapping.get("scope") if isinstance(mapping.get("scope"), dict) else {},
            }
        )

    workflow_hints = structured_rule.get("workflow_hints") if isinstance(structured_rule.get("workflow_hints"), list) else []
    for idx, hint in enumerate(workflow_hints, start=1):
        if not isinstance(hint, dict):
            continue
        facts.append(
            {
                "fact_type": "workflow_hint",
                "fact_key": f"workflow_{idx}",
                "process": str(hint.get("process") or ""),
                "control_flow": str(hint.get("control_flow") or ""),
                "stop_on_error": bool(hint.get("stop_on_error", True)),
            }
        )

    fact_assertions = structured_rule.get("fact_assertions") if isinstance(structured_rule.get("fact_assertions"), list) else []
    for idx, fact in enumerate(fact_assertions, start=1):
        if not isinstance(fact, dict):
            continue
        facts.append(
            {
                "fact_type": "fact_assertion",
                "fact_key": f"assertion_{idx}",
                "fact_name": str(fact.get("fact_name") or ""),
                "subject": str(fact.get("subject") or ""),
                "predicate": str(fact.get("predicate") or ""),
                "object": str(fact.get("object") or ""),
                "value": str(fact.get("value") or ""),
                "scope": fact.get("scope") if isinstance(fact.get("scope"), dict) else {},
            }
        )

    multi_hop_dependencies = structured_rule.get("multi_hop_dependencies") if isinstance(structured_rule.get("multi_hop_dependencies"), list) else []
    for idx, dependency in enumerate(multi_hop_dependencies, start=1):
        if not isinstance(dependency, dict):
            continue
        facts.append(
            {
                "fact_type": "multi_hop_dependency",
                "fact_key": f"dependency_{idx}",
                "dependency_name": str(dependency.get("dependency_name") or ""),
                "source_fact": str(dependency.get("source_fact") or ""),
                "target_fact": str(dependency.get("target_fact") or ""),
                "via_relationship": str(dependency.get("via_relationship") or ""),
                "max_hops": int(dependency.get("max_hops") or 1),
                "direction": str(dependency.get("direction") or "any"),
                "scope": dependency.get("scope") if isinstance(dependency.get("scope"), dict) else {},
            }
        )

    quantified_conditions = structured_rule.get("quantified_conditions") if isinstance(structured_rule.get("quantified_conditions"), list) else []
    for idx, condition in enumerate(quantified_conditions, start=1):
        if not isinstance(condition, dict):
            continue
        facts.append(
            {
                "fact_type": "quantified_condition",
                "fact_key": f"quantified_{idx}",
                "condition_name": str(condition.get("condition_name") or ""),
                "quantifier": str(condition.get("quantifier") or ""),
                "variable": str(condition.get("variable") or ""),
                "predicate": str(condition.get("predicate") or ""),
                "threshold": int(condition.get("threshold") or 0),
                "in_set": [str(v) for v in (condition.get("in_set") or []) if str(v).strip()],
                "scope": condition.get("scope") if isinstance(condition.get("scope"), dict) else {},
            }
        )

    post_extraction_directives = structured_rule.get("post_extraction_directives") if isinstance(structured_rule.get("post_extraction_directives"), list) else []
    for idx, directive in enumerate(post_extraction_directives, start=1):
        if not isinstance(directive, dict):
            continue
        facts.append(
            {
                "fact_type": "post_extraction_directive",
                "fact_key": f"directive_{idx}",
                "target_scope": str(directive.get("target_scope") or "entity"),
                "target_entity_classes": [str(v) for v in (directive.get("target_entity_classes") or []) if str(v).strip()],
                "conditions": directive.get("conditions") if isinstance(directive.get("conditions"), list) else [],
                "set_attributes": directive.get("set_attributes") if isinstance(directive.get("set_attributes"), dict) else {},
                "set_metadata": directive.get("set_metadata") if isinstance(directive.get("set_metadata"), dict) else {},
                "append_workflow_processes": [str(v) for v in (directive.get("append_workflow_processes") or []) if str(v).strip()],
            }
        )

    return facts


def build_rule_artifacts_view(
    structured_rule: dict[str, Any] | None,
    solf_script: str,
    scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = _normalize_structured_rule(structured_rule or {})
    classes = normalized.get("class_definitions") if isinstance(normalized.get("class_definitions"), list) else []
    facts_preview = _build_fact_preview_from_structured_rule(normalized)
    clauses_raw = [chunk for chunk in str(solf_script or "").split("\n\n") if str(chunk).strip()]
    clauses = [
        {
            "index": idx,
            "clause_name": _extract_clause_name(chunk),
            "clause_text": str(chunk),
        }
        for idx, chunk in enumerate(clauses_raw, start=1)
    ]

    return {
        "class_definition_source": str(normalized.get("class_definition_source") or "none"),
        "classes": classes,
        "class_count": len(classes),
        "facts_preview": facts_preview,
        "fact_count": len(facts_preview),
        "clauses": clauses,
        "clause_count": len(clauses),
        "scope": scope if isinstance(scope, dict) else _extract_scope_from_structured(normalized),
    }


def get_business_rule_artifacts(rule_id: int) -> dict[str, Any] | None:
    current = get_business_rule(rule_id=rule_id)
    if current is None:
        return None

    structured = current.get("structured_rule") if isinstance(current.get("structured_rule"), dict) else {}
    solf_script = str(current.get("solf_script") or "")
    scope = current.get("scope") if isinstance(current.get("scope"), dict) else {}

    return {
        "rule_id": int(current.get("rule_id") or 0),
        "rule_name": str(current.get("rule_name") or ""),
        "is_active": bool(current.get("is_active")),
        "artifacts": build_rule_artifacts_view(
            structured_rule=structured,
            solf_script=solf_script,
            scope=scope,
        ),
    }


def simulate_draft_business_rule(
    rule_text: str,
    rule_name: str | None = None,
    requested_attribute: str = "",
    application_mode: str | None = None,
    context: dict[str, Any] | None = None,
    sample_attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    structured = _normalize_structured_rule(
        _apply_application_mode_override(parse_rule_text_to_structured(rule_text), application_mode, rule_text)
    )
    structured = _repair_non_actionable_rule_attributes(structured, rule_text=rule_text)
    actionability = _build_rule_actionability_report(structured, rule_text=rule_text)
    if rule_name:
        structured["rule_name"] = _slug(str(rule_name)) or str(structured.get("rule_name") or "draft_rule")

    draft_key = _slug(str(structured.get("rule_name") or "draft_rule")) or "draft_rule"
    solf_script, semantic_terms = compile_structured_rule_to_solf(structured, draft_key)
    scope = _extract_scope_from_structured(structured)
    ctx = dict(context or {})

    resolved_attribute = _resolve_attribute_from_structured_rule(
        structured_rule=structured,
        requested_attribute=requested_attribute,
        context=ctx,
    )
    transformed_attributes = _apply_structured_rule_to_attributes(
        structured_rule=structured,
        attributes=dict(sample_attributes or {}),
        context=ctx,
    )
    workflow_hint = _resolve_workflow_hint_from_structured_rule(
        structured_rule=structured,
        context=ctx,
    )

    return {
        "rule_name": structured.get("rule_name"),
        "requested_attribute": requested_attribute,
        "context": ctx,
        "resolved_attribute": resolved_attribute,
        "workflow_hint": workflow_hint,
        "transformed_attributes": transformed_attributes,
        "matched": bool(resolved_attribute or workflow_hint),
        "scope": scope,
        "structured_rule": structured,
        "class_definition_source": str(structured.get("class_definition_source") or "none"),
        "class_definitions_preview": structured.get("class_definitions") if isinstance(structured.get("class_definitions"), list) else [],
        "class_definitions_preview_count": len(structured.get("class_definitions") or []),
        "solf_script": solf_script,
        "solf_clause_count": len([c for c in str(solf_script or "").split("\n\n") if c.strip()]),
        "semantic_terms_preview_count": len(semantic_terms),
        "actionability": actionability,
        "needs_clarification": bool(actionability.get("clarifications")),
        "clarification": (actionability.get("clarifications") or [None])[0],
        "persisted": False,
    }


def create_solf_workflow_registry_entry(
    workflow_key: str,
    workflow_name: str,
    description: str | None = None,
    domain: str | None = None,
    status: str = "draft",
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
    created_by: str | None = None,
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        workflow_id = object_db.upsert_solf_workflow_registry(
            connection=connection,
            workflow_key=workflow_key,
            workflow_name=workflow_name,
            description=description,
            domain=domain,
            status=status,
            metadata=metadata,
            is_active=is_active,
            created_by=created_by,
        )
        workflow = object_db.get_solf_workflow_registry_by_id(connection, workflow_id)
        return workflow or {"workflow_id": int(workflow_id)}
    finally:
        connection.close()


def list_solf_workflow_registry_entries(
    is_active: bool | None = None,
    domain: str | None = None,
    status: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    connection = object_db.get_connection()
    try:
        return object_db.list_solf_workflow_registry(
            connection=connection,
            is_active=is_active,
            domain=domain,
            status=status,
            limit=limit,
        )
    finally:
        connection.close()


def get_solf_workflow_registry_entry(workflow_id: int) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        return object_db.get_solf_workflow_registry_by_id(connection, workflow_id=workflow_id)
    finally:
        connection.close()


def get_solf_workflow_registry_entry_by_key(workflow_key: str) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        return object_db.get_solf_workflow_registry_by_key(connection, workflow_key=workflow_key)
    finally:
        connection.close()


def get_solf_workflow_active_version_by_key(workflow_key: str) -> dict[str, Any] | None:
    normalized_key = str(workflow_key or "").strip()
    if not normalized_key:
        return None

    connection = object_db.get_connection()
    try:
        workflow = object_db.get_solf_workflow_registry_by_key(connection, workflow_key=normalized_key)
        if workflow is None:
            return None

        workflow_id = int(workflow.get("workflow_id") or 0)
        active_version = object_db.get_workflow_active_version(connection, workflow_id=workflow_id)
        workflow_version_id = int(active_version.get("workflow_version_id") or 0) if active_version else None

        return {
            "workflow_id": workflow_id,
            "workflow_key": workflow.get("workflow_key") or normalized_key,
            "workflow_name": workflow.get("workflow_name"),
            "workflow_version_id": workflow_version_id,
            "has_active_version": active_version is not None,
            "active_version": active_version,
        }
    finally:
        connection.close()


def _extract_first_code_block(text: str) -> str:
    raw = str(text or "")
    match = re.search(r"```(?:[a-zA-Z0-9_+-]+)?\s*\n([\s\S]*?)```", raw)
    if match:
        return str(match.group(1) or "").strip()
    return raw.strip()


def _get_existing_clause_id_by_name(connection: Any, clause_name: str) -> int | None:
    normalized = str(clause_name or "").strip()
    if not normalized:
        return None
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT clause_id
            FROM solf_clauses
            WHERE clause_name = %s
            ORDER BY CASE WHEN configuration_scope = 'tenant' THEN 0 ELSE 1 END,
                     is_active DESC, modified_at DESC, clause_id DESC
            LIMIT 1
            """,
            (normalized,),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def _build_fallback_workflow_clause_body(
    *,
    clause_name: str,
    workflow_key: str,
    workflow_name: str,
    narrative: str,
) -> str:
    safe_clause = _slug(clause_name) or "auto_generated_workflow_clause"
    prompt_lines = [
        f"Workflow key: {workflow_key or 'workflow::auto'}",
        f"Workflow name: {workflow_name or 'workflow'}",
        f"Workflow narrative: {narrative or 'No narrative provided.'}",
        "Task: execute workflow steps from the narrative and return strict JSON.",
        "JSON contract: include fields result, assumptions, actions_taken, and artifact (format, title, columns, rows).",
        "When tabular output is suitable, provide artifact.format as xlsx and include artifact.columns/artifact.rows.",
        "Input context JSON: {context}",
    ]
    prompt_template = "\\n".join(prompt_lines).replace('"', '\\"')
    return (
        f"{safe_clause}(_ctx) ⦃\n"
        f"    (_workflow_prompt ≔ \"{prompt_template}\") ⋀\n"
        f"    ↲(workflow_compose_operations({{\n"
        f"        context: _ctx,\n"
        f"        stop_on_error: true,\n"
        f"        operations: [\n"
        f"            {{\n"
        f"                action: workflow_llm_call,\n"
        f"                payload: {{\n"
        f"                    prompt_template: _workflow_prompt,\n"
        f"                    variables: {{ context: _ctx }},\n"
        f"                    parse_json: true,\n"
        f"                    temperature: 0.0,\n"
        f"                    complexity: \"complex\"\n"
        f"                }},\n"
        f"                save_as: workflow_llm_result,\n"
        f"                merge_result_json: true\n"
        f"            }},\n"
        f"            {{\n"
        f"                action: workflow_generate_artifact,\n"
        f"                payload: {{ output_dir: \"generated/doc\", default_format: \"xlsx\", force_format: \"xlsx\" }},\n"
        f"                save_as: generated_artifact,\n"
        f"                merge_result_json: true\n"
        f"            }}\n"
        f"        ]\n"
        f"    }}))\n"
        f"⦄"
    )


def _generate_workflow_clause_body_from_narrative(
    *,
    clause_name: str,
    workflow_key: str,
    workflow_name: str,
    narrative: str,
) -> str:
    fallback = _build_fallback_workflow_clause_body(
        clause_name=clause_name,
        workflow_key=workflow_key,
        workflow_name=workflow_name,
        narrative=narrative,
    )

    client = _make_genai_client()
    if client is None:
        return fallback

    requires_excel = bool(re.search(r"\b(excel|xlsx|spreadsheet)\b", str(narrative or ""), flags=re.IGNORECASE))
    excel_requirement_line = (
        "3b) Because workflow narrative requires spreadsheet output, set workflow_generate_artifact payload force_format to xlsx.\\n"
        if requires_excel
        else ""
    )

    prompt = (
        "Generate exactly one executable SOLF clause for an IDMS workflow.\\n"
        "Hard requirements:\\n"
        f"1) Clause name MUST be exactly: {clause_name}\\n"
        "2) Single clause only, no class blocks, no markdown fences, no explanation text.\\n"
        "3) The clause must call workflow_compose_operations and include workflow_llm_call + workflow_generate_artifact operations.\\n"
        f"{excel_requirement_line}"
        "4) Return a JSON-like result via ↲(...).\\n"
        "5) Keep syntax valid for SOLF parser.\\n\\n"
        f"Workflow key: {workflow_key or 'workflow::auto'}\\n"
        f"Workflow name: {workflow_name or 'workflow'}\\n"
        f"Workflow narrative: {narrative or 'No narrative provided.'}\\n"
    )

    try:
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: client.models.generate_content(
                model=EXTRACT_MODEL,
                contents=[prompt],
            ),
            model=EXTRACT_MODEL,
            contents=[prompt],
            temperature=0.0,
            call_name="workflow_clause_generation",
            complexity="medium",
        )
        raw_text = str(getattr(response, "text", "") or "")
        candidate = _extract_first_code_block(raw_text)
        if not candidate:
            return fallback

        required_tokens = [
            "workflow_compose_operations",
            "workflow_llm_call",
            "workflow_generate_artifact",
        ]
        lowered_candidate = candidate.lower()
        if any(token.lower() not in lowered_candidate for token in required_tokens):
            return fallback

        if requires_excel:
            requires_excel_tokens = ["force_format", "xlsx"]
            if any(token not in lowered_candidate for token in requires_excel_tokens):
                return fallback

        parsed_clauses = _extract_solf_clauses_from_script(candidate)
        if not parsed_clauses:
            return fallback
        first_name = str(parsed_clauses[0].get("clause_name") or "").strip()
        if first_name != str(clause_name or "").strip():
            return fallback

        syntax_errors = _validate_solf_clause_syntax(parsed_clauses)
        if syntax_errors:
            return fallback
        return candidate
    except Exception:
        return fallback


def _ensure_workflow_clauses_for_steps(
    connection: Any,
    *,
    workflow_id: int,
    workflow_key: str,
    workflow_name: str,
    steps: list[dict[str, Any]],
    workflow_description: str | None,
    created_by: str | None,
) -> dict[str, Any]:
    pattern_library = PatternLibrary(connection)
    generated: list[dict[str, Any]] = []
    existing: list[str] = []

    for index, step in enumerate(steps or [], start=1):
        if not isinstance(step, dict):
            continue
        step_kind = str(step.get("step_kind") or "clause").strip().lower()
        if step_kind != "clause":
            continue

        clause_name = str(step.get("clause_name") or "").strip()
        if not clause_name:
            continue

        config = step.get("config") if isinstance(step.get("config"), dict) else {}
        source = str(config.get("source") or "").strip().lower()
        narrative = str(config.get("narrative") or workflow_description or "").strip()
        should_generate = source == "workflow_narrative" or clause_name.startswith("auto_generated_")

        clause_id = _get_existing_clause_id_by_name(connection, clause_name)
        if clause_id is not None and not should_generate:
            step["clause_id"] = clause_id
            existing.append(clause_name)
            continue

        if not should_generate:
            continue

        clause_body = _generate_workflow_clause_body_from_narrative(
            clause_name=clause_name,
            workflow_key=workflow_key,
            workflow_name=workflow_name,
            narrative=narrative,
        )
        clause_id = pattern_library.upsert_solf_clause(
            clause_name=clause_name,
            clause_type="resolve_policy",
            clause_body=clause_body,
            entity_class=None,
            metadata={
                "source": "workflow_registry_auto_clause",
                "workflow_id": int(workflow_id),
                "workflow_key": workflow_key,
                "workflow_name": workflow_name,
                "step_key": str(step.get("step_key") or f"step_{index}"),
            },
            is_active=True,
            created_by=created_by or "api:user",
        )
        if isinstance(clause_id, int):
            step["clause_id"] = clause_id
            generated.append(
                {
                    "step_key": str(step.get("step_key") or f"step_{index}"),
                    "clause_name": clause_name,
                    "clause_id": clause_id,
                }
            )

    return {
        "generated_clause_count": len(generated),
        "generated_clauses": generated,
        "existing_clause_count": len(existing),
    }


def _normalize_generated_extension_rule(rule: dict[str, Any], index: int) -> dict[str, Any]:
    hook_point = str(rule.get("hook_point") or "after_step").strip().lower()
    if hook_point not in {"on_run_start", "before_step", "after_step", "on_pause", "on_failure", "on_complete"}:
        hook_point = "after_step"

    conflict_policy = str(rule.get("conflict_policy") or "skip").strip().lower()
    if conflict_policy not in {"skip", "replace", "merge", "fail"}:
        conflict_policy = "skip"

    precedence_raw = rule.get("precedence")
    try:
        precedence = int(precedence_raw)
    except Exception:
        precedence = 100

    inserted_steps_raw = rule.get("inserted_steps_json") if isinstance(rule.get("inserted_steps_json"), list) else []
    inserted_steps: list[dict[str, Any]] = []
    for step_idx, step in enumerate(inserted_steps_raw, start=1):
        if not isinstance(step, dict):
            continue
        normalized = dict(step)
        if not str(normalized.get("step_key") or "").strip():
            normalized["step_key"] = f"extension_step_{index}_{step_idx}"
        if not str(normalized.get("step_kind") or "").strip():
            normalized["step_kind"] = "python_binding"
        inserted_steps.append(normalized)

    return {
        "rule_key": str(rule.get("rule_key") or f"auto_generated_rule_{index}").strip() or f"auto_generated_rule_{index}",
        "hook_point": hook_point,
        "target_step_key": str(rule.get("target_step_key") or "").strip() or None,
        "condition_json": rule.get("condition_json") if isinstance(rule.get("condition_json"), dict) else {},
        "precedence": precedence,
        "conflict_policy": conflict_policy,
        "inserted_steps_json": inserted_steps,
        "metadata": rule.get("metadata") if isinstance(rule.get("metadata"), dict) else {},
        "is_active": bool(rule.get("is_active", True)),
    }


def _derive_change_domain_hint(change_type: str, change_payload: dict[str, Any], explicit_domain_hint: str | None = None) -> str:
    explicit = str(explicit_domain_hint or "").strip().lower()
    if explicit:
        return explicit

    normalized_type = str(change_type or "").strip().lower()
    if normalized_type in {"account_definition", "chart_of_accounts", "accounting_rule", "accounting"}:
        return "accounting"
    if normalized_type in {"hr_rule", "department_definition", "role_definition", "hr"}:
        return "hr"

    payload_blob = " ".join(_collect_signal_strings(change_payload))
    if any(token in payload_blob for token in ("account", "ledger", "invoice", "payment", "travel", "expense", "vat", "book")):
        return "accounting"
    if any(token in payload_blob for token in ("employee", "department", "role", "employment", "payroll")):
        return "hr"
    return "workflow"


def analyze_workflow_impact_and_generate_extension(
    *,
    change_type: str,
    change_payload: dict[str, Any] | None = None,
    workflow_keys: list[str] | None = None,
    domain_hint: str | None = None,
    top_k: int = 8,
    created_by: str | None = "api:user",
    persist: bool = False,
    attach: bool = False,
    temperature: float = 0.0,
) -> dict[str, Any]:
    normalized_change_type = str(change_type or "").strip().lower()
    if not normalized_change_type:
        raise ValueError("change_type is required")

    payload = change_payload if isinstance(change_payload, dict) else {}
    resolved_domain = _derive_change_domain_hint(normalized_change_type, payload, explicit_domain_hint=domain_hint)
    normalized_top_k = max(1, min(int(top_k or 8), 50))

    candidate_rows = list_solf_workflow_registry_entries(is_active=True, limit=1000)
    requested_keys = {
        str(item or "").strip().lower()
        for item in (workflow_keys or [])
        if str(item or "").strip()
    }
    if requested_keys:
        candidate_rows = [
            row for row in candidate_rows
            if str(row.get("workflow_key") or "").strip().lower() in requested_keys
        ]

    workflow_catalog = [
        {
            "workflow_key": str(row.get("workflow_key") or "").strip(),
            "workflow_name": str(row.get("workflow_name") or "").strip(),
            "domain": str(row.get("domain") or "").strip().lower() or None,
            "description": str(row.get("description") or "").strip(),
            "status": str(row.get("status") or "").strip().lower(),
            "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
        }
        for row in candidate_rows
        if str(row.get("workflow_key") or "").strip()
    ]

    llm_parsed: dict[str, Any] = {}
    llm_raw_text = ""
    client = _make_genai_client()
    if client is not None and workflow_catalog:
        prompt = (
            "You are an IDMS workflow impact analyzer.\n"
            "Task: map a new/updated definition or rule to impacted workflow pipelines and generate extension rules to enforce it.\n"
            "Return JSON only with keys: impact_summary, workflow_impacts, extension.\n"
            "workflow_impacts: list of {workflow_key, reason, confidence}.\n"
            "extension: {extension_key, extension_name, description, scope_json, rules}.\n"
            "rules: list of {rule_key, hook_point, target_step_key, condition_json, precedence, conflict_policy, inserted_steps_json, metadata, is_active}.\n"
            "Each inserted step should prefer step_kind='python_binding' and may use workflow_pipeline_builtin_steps.invoke_solf_action.\n"
            "Use only workflow_key values from the provided catalog.\n"
            f"Change type: {normalized_change_type}\n"
            f"Domain hint: {resolved_domain}\n"
            f"Change payload JSON: {json.dumps(payload, ensure_ascii=False)}\n"
            f"Workflow catalog JSON: {json.dumps(workflow_catalog, ensure_ascii=False)}\n"
        )
        try:
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: client.models.generate_content(
                    model=EXTRACT_MODEL,
                    contents=[prompt],
                ),
                model=EXTRACT_MODEL,
                contents=[prompt],
                temperature=float(temperature),
                call_name="workflow_impact_analysis",
                complexity="medium",
            )
            llm_raw_text = str(getattr(response, "text", "") or "")
            llm_parsed = _extract_json_object(llm_raw_text)
        except Exception:
            llm_parsed = {}

    catalog_by_key = {
        str(item.get("workflow_key") or "").strip().lower(): item
        for item in workflow_catalog
        if str(item.get("workflow_key") or "").strip()
    }

    impacts_raw = llm_parsed.get("workflow_impacts") if isinstance(llm_parsed.get("workflow_impacts"), list) else []
    impacts: list[dict[str, Any]] = []
    for item in impacts_raw:
        if not isinstance(item, dict):
            continue
        key = str(item.get("workflow_key") or "").strip()
        key_norm = key.lower()
        if key_norm not in catalog_by_key:
            continue
        confidence_raw = item.get("confidence")
        try:
            confidence = float(confidence_raw)
        except Exception:
            confidence = 0.5
        confidence = max(0.0, min(confidence, 1.0))
        impacts.append(
            {
                "workflow_key": key,
                "workflow_name": catalog_by_key[key_norm].get("workflow_name"),
                "domain": catalog_by_key[key_norm].get("domain"),
                "reason": str(item.get("reason") or "").strip() or "LLM impact mapping",
                "confidence": confidence,
            }
        )

    if not impacts and workflow_catalog:
        fallback_impacts = [
            item for item in workflow_catalog
            if str(item.get("domain") or "").strip().lower() == resolved_domain
        ]
        if not fallback_impacts:
            fallback_impacts = workflow_catalog[: min(3, len(workflow_catalog))]
        impacts = [
            {
                "workflow_key": str(item.get("workflow_key") or ""),
                "workflow_name": item.get("workflow_name"),
                "domain": item.get("domain"),
                "reason": "Domain-based fallback mapping",
                "confidence": 0.45,
            }
            for item in fallback_impacts
            if str(item.get("workflow_key") or "")
        ]

    impacts.sort(key=lambda item: (-float(item.get("confidence") or 0.0), str(item.get("workflow_key") or "")))
    impacts = impacts[:normalized_top_k]

    extension_raw = llm_parsed.get("extension") if isinstance(llm_parsed.get("extension"), dict) else {}
    impact_keys = [str(item.get("workflow_key") or "") for item in impacts if str(item.get("workflow_key") or "")]

    extension_key = str(extension_raw.get("extension_key") or "").strip()
    if not extension_key:
        stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        extension_key = f"auto::impact::{_slug(normalized_change_type) or 'change'}::{stamp}"

    extension_name = str(extension_raw.get("extension_name") or "").strip() or f"Auto Impact: {normalized_change_type}"
    extension_description = str(extension_raw.get("description") or "").strip() or "Auto-generated workflow impact extension"
    scope_json = extension_raw.get("scope_json") if isinstance(extension_raw.get("scope_json"), dict) else {}
    if impact_keys and not isinstance(scope_json.get("workflow_keys"), list):
        scope_json = {**scope_json, "workflow_keys": impact_keys}

    rules_raw = extension_raw.get("rules") if isinstance(extension_raw.get("rules"), list) else []
    normalized_rules = [
        _normalize_generated_extension_rule(rule, idx)
        for idx, rule in enumerate(rules_raw, start=1)
        if isinstance(rule, dict)
    ]

    if not normalized_rules and impact_keys:
        normalized_rules = [
            {
                "rule_key": f"auto_enforce_{_slug(normalized_change_type) or 'change'}",
                "hook_point": "before_step",
                "target_step_key": None,
                "condition_json": {
                    "all": [
                        {"path": "run.status", "op": "in", "value": ["running", "pending"]},
                    ]
                },
                "precedence": 50,
                "conflict_policy": "merge",
                "inserted_steps_json": [
                    {
                        "step_key": f"extension_apply_{_slug(normalized_change_type) or 'change'}",
                        "step_kind": "python_binding",
                        "python_module": "workflow_pipeline_builtin_steps",
                        "python_function": "invoke_solf_action",
                        "config": {
                            "action": "workflow_domain_operation",
                            "payload": {"operation": "db_validate_ledger_payload"},
                            "save_as": "extension_validation_result",
                            "fail_on_error": False,
                        },
                    }
                ],
                "metadata": {
                    "source": "auto_impact_fallback",
                    "change_type": normalized_change_type,
                },
                "is_active": True,
            }
        ]

    generated_extension = {
        "extension_key": extension_key,
        "extension_name": extension_name,
        "description": extension_description,
        "scope_json": scope_json,
        "metadata": {
            "source": "workflow_impact_analysis",
            "change_type": normalized_change_type,
            "domain_hint": resolved_domain,
        },
        "rules": normalized_rules,
    }

    persisted: dict[str, Any] = {
        "persisted": False,
        "attached": False,
        "pack": None,
        "version": None,
        "attachments": [],
    }
    if persist and generated_extension.get("rules"):
        pack = create_workflow_extension_pack(
            extension_key=str(generated_extension.get("extension_key") or ""),
            extension_name=str(generated_extension.get("extension_name") or ""),
            description=str(generated_extension.get("description") or ""),
            scope_json=generated_extension.get("scope_json") if isinstance(generated_extension.get("scope_json"), dict) else {},
            metadata=generated_extension.get("metadata") if isinstance(generated_extension.get("metadata"), dict) else {},
            is_active=True,
            created_by=created_by,
        )
        extension_pack_id = int(pack.get("extension_pack_id") or 0)
        version_payload = create_workflow_extension_version(
            extension_pack_id=extension_pack_id,
            status="review",
            conditions_schema_version="v1",
            metadata={
                "source": "workflow_impact_analysis",
                "change_type": normalized_change_type,
            },
            is_active=True,
            rules=generated_extension.get("rules") if isinstance(generated_extension.get("rules"), list) else [],
            created_by=created_by,
        )
        version = version_payload.get("extension_version") if isinstance(version_payload.get("extension_version"), dict) else {}
        extension_version_id = int(version.get("extension_version_id") or 0)

        attachments: list[dict[str, Any]] = []
        if attach and extension_pack_id > 0 and extension_version_id > 0:
            for item in impacts:
                workflow_key = str(item.get("workflow_key") or "").strip()
                if not workflow_key:
                    continue
                attachment = attach_workflow_extension(
                    workflow_key=workflow_key,
                    workflow_version_id=None,
                    extension_pack_id=extension_pack_id,
                    extension_version_id=extension_version_id,
                    priority=50,
                    tenant_scope=None,
                    metadata={
                        "source": "workflow_impact_analysis",
                        "change_type": normalized_change_type,
                        "confidence": float(item.get("confidence") or 0.0),
                    },
                    is_active=True,
                    created_by=created_by,
                )
                attachments.append(attachment)

        persisted = {
            "persisted": True,
            "attached": bool(attachments),
            "pack": pack,
            "version": version_payload,
            "attachments": attachments,
        }

    return {
        "model": EXTRACT_MODEL,
        "change_type": normalized_change_type,
        "domain_hint": resolved_domain,
        "impact_summary": str(llm_parsed.get("impact_summary") or "").strip() or "Workflow impact analysis completed.",
        "workflow_catalog_count": len(workflow_catalog),
        "impacted_workflows": impacts,
        "generated_extension": generated_extension,
        "persistence": persisted,
        "llm": {
            "parsed": llm_parsed,
            "raw_text": llm_raw_text,
        },
    }


def create_workflow_extension_pack(
    extension_key: str,
    extension_name: str,
    description: str | None = None,
    scope_json: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
    created_by: str | None = None,
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        extension_pack_id = object_db.upsert_workflow_extension_pack(
            connection=connection,
            extension_key=extension_key,
            extension_name=extension_name,
            description=description,
            scope_json=scope_json,
            metadata=metadata,
            is_active=is_active,
            created_by=created_by,
        )
        rows = object_db.list_workflow_extension_packs(connection=connection, limit=500)
        for row in rows:
            if int(row.get("extension_pack_id") or 0) == int(extension_pack_id):
                return row
        return {"extension_pack_id": int(extension_pack_id)}
    finally:
        connection.close()


def list_workflow_extension_packs(
    is_active: bool | None = None,
    extension_key: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    connection = object_db.get_connection()
    try:
        return object_db.list_workflow_extension_packs(
            connection=connection,
            is_active=is_active,
            extension_key=extension_key,
            limit=limit,
        )
    finally:
        connection.close()


def get_workflow_extension_pack(extension_key: str) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        return object_db.get_workflow_extension_pack_by_key(connection=connection, extension_key=extension_key)
    finally:
        connection.close()


def create_workflow_extension_version(
    *,
    extension_pack_id: int,
    status: str = "draft",
    conditions_schema_version: str = "v1",
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
    rules: list[dict[str, Any]] | None = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        extension_version_id = object_db.create_workflow_extension_version(
            connection=connection,
            extension_pack_id=extension_pack_id,
            status=status,
            conditions_schema_version=conditions_schema_version,
            metadata=metadata,
            is_active=is_active,
            created_by=created_by,
        )
        rule_count = object_db.replace_workflow_extension_rules(
            connection=connection,
            extension_version_id=extension_version_id,
            rules=[item for item in (rules or []) if isinstance(item, dict)],
        )
        versions = object_db.list_workflow_extension_versions(
            connection=connection,
            extension_pack_id=extension_pack_id,
            is_active=None,
            limit=500,
        )
        version = next(
            (item for item in versions if int(item.get("extension_version_id") or 0) == int(extension_version_id)),
            None,
        )
        return {
            "extension_version": version or {"extension_version_id": int(extension_version_id)},
            "rule_count": int(rule_count),
            "rules": object_db.list_workflow_extension_rules(
                connection=connection,
                extension_version_id=extension_version_id,
                is_active=None,
            ),
        }
    finally:
        connection.close()


def list_workflow_extension_versions(
    extension_pack_id: int,
    is_active: bool | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    connection = object_db.get_connection()
    try:
        versions = object_db.list_workflow_extension_versions(
            connection=connection,
            extension_pack_id=extension_pack_id,
            is_active=is_active,
            limit=limit,
        )
        for version in versions:
            version["rules"] = object_db.list_workflow_extension_rules(
                connection=connection,
                extension_version_id=int(version.get("extension_version_id") or 0),
                is_active=True,
            )
        return versions
    finally:
        connection.close()


def attach_workflow_extension(
    *,
    workflow_key: str,
    workflow_version_id: int | None,
    extension_pack_id: int,
    extension_version_id: int | None = None,
    effective_from: str | None = None,
    effective_until: str | None = None,
    priority: int = 100,
    tenant_scope: str | None = None,
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
    created_by: str | None = None,
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        attachment_id = object_db.create_workflow_extension_attachment(
            connection=connection,
            workflow_key=workflow_key,
            workflow_version_id=workflow_version_id,
            extension_pack_id=extension_pack_id,
            extension_version_id=extension_version_id,
            effective_from=effective_from,
            effective_until=effective_until,
            priority=priority,
            tenant_scope=tenant_scope,
            metadata=metadata,
            is_active=is_active,
            created_by=created_by,
        )
        attachments = object_db.list_workflow_extension_attachments(
            connection=connection,
            workflow_key=workflow_key,
            workflow_version_id=workflow_version_id,
            extension_pack_id=extension_pack_id,
            tenant_scope=tenant_scope,
            is_active=None,
            only_effective_now=False,
            limit=500,
        )
        for item in attachments:
            if int(item.get("attachment_id") or 0) == int(attachment_id):
                return item
        return {"attachment_id": int(attachment_id)}
    finally:
        connection.close()


def detach_workflow_extension_attachment(attachment_id: int) -> bool:
    connection = object_db.get_connection()
    try:
        return object_db.set_workflow_extension_attachment_active(
            connection=connection,
            attachment_id=attachment_id,
            is_active=False,
        )
    finally:
        connection.close()


def list_workflow_extension_attachments(
    *,
    workflow_key: str | None = None,
    workflow_version_id: int | None = None,
    extension_pack_id: int | None = None,
    tenant_scope: str | None = None,
    is_active: bool | None = None,
    only_effective_now: bool = False,
    limit: int = 500,
) -> list[dict[str, Any]]:
    connection = object_db.get_connection()
    try:
        return object_db.list_workflow_extension_attachments(
            connection=connection,
            workflow_key=workflow_key,
            workflow_version_id=workflow_version_id,
            extension_pack_id=extension_pack_id,
            tenant_scope=tenant_scope,
            is_active=is_active,
            only_effective_now=only_effective_now,
            limit=limit,
        )
    finally:
        connection.close()


def _get_nested_value(container: Any, path: str) -> Any:
    current = container
    for token in [part for part in str(path or "").split(".") if part]:
        if isinstance(current, dict):
            current = current.get(token)
        else:
            return None
    return current


def _evaluate_extension_condition(condition: dict[str, Any], env: dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(condition, dict) or not condition:
        return True, "no_condition"

    if isinstance(condition.get("all"), list):
        for item in condition.get("all") or []:
            ok, reason = _evaluate_extension_condition(item if isinstance(item, dict) else {}, env)
            if not ok:
                return False, f"all_failed:{reason}"
        return True, "all_matched"

    if isinstance(condition.get("any"), list):
        any_items = condition.get("any") or []
        if not any_items:
            return True, "any_empty"
        for item in any_items:
            ok, reason = _evaluate_extension_condition(item if isinstance(item, dict) else {}, env)
            if ok:
                return True, f"any_matched:{reason}"
        return False, "any_failed"

    if isinstance(condition.get("not"), dict):
        ok, reason = _evaluate_extension_condition(condition.get("not") or {}, env)
        return (not ok), f"not:{reason}"

    path = str(condition.get("path") or "").strip()
    op = str(condition.get("op") or "eq").strip().lower()
    expected = condition.get("value")
    actual = _get_nested_value(env, path)

    if op == "exists":
        return (actual is not None), f"exists:{path}"
    if op == "not_exists":
        return (actual is None), f"not_exists:{path}"
    if op == "eq":
        return (actual == expected), f"eq:{path}"
    if op == "neq":
        return (actual != expected), f"neq:{path}"
    if op == "in":
        values = expected if isinstance(expected, list) else [expected]
        return (actual in values), f"in:{path}"
    if op in {"gt", "gte", "lt", "lte"}:
        try:
            lhs = float(actual)
            rhs = float(expected)
            if op == "gt":
                return (lhs > rhs), f"gt:{path}"
            if op == "gte":
                return (lhs >= rhs), f"gte:{path}"
            if op == "lt":
                return (lhs < rhs), f"lt:{path}"
            return (lhs <= rhs), f"lte:{path}"
        except Exception:
            return False, f"numeric_compare_failed:{path}"

    return False, f"unsupported_op:{op}"


def resolve_workflow_extension_preview(
    *,
    workflow_key: str,
    workflow_version_id: int | None = None,
    run_status: str = "running",
    current_context: dict[str, Any] | None = None,
    step_statuses: dict[str, Any] | None = None,
    tenant_scope: str | None = None,
    hook_point: str | None = None,
    target_step_key: str | None = None,
) -> dict[str, Any]:
    normalized_workflow_key = str(workflow_key or "").strip().lower()
    normalized_run_status = str(run_status or "running").strip().lower() or "running"
    normalized_hook_point = str(hook_point or "").strip().lower() or None
    normalized_target_step_key = str(target_step_key or "").strip() or None

    connection = object_db.get_connection()
    try:
        attachments = object_db.list_workflow_extension_attachments(
            connection=connection,
            workflow_key=normalized_workflow_key,
            workflow_version_id=workflow_version_id,
            extension_pack_id=None,
            tenant_scope=tenant_scope,
            is_active=True,
            only_effective_now=True,
            limit=1000,
        )

        env = {
            "workflow": {
                "workflow_key": normalized_workflow_key,
                "workflow_version_id": workflow_version_id,
            },
            "run": {
                "status": normalized_run_status,
            },
            "context": current_context if isinstance(current_context, dict) else {},
            "step_statuses": step_statuses if isinstance(step_statuses, dict) else {},
        }

        selected: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        considered_rules = 0

        for attachment in attachments:
            extension_pack_id = int(attachment.get("extension_pack_id") or 0)
            extension_version_id = attachment.get("extension_version_id")
            resolved_version_id = int(extension_version_id) if extension_version_id is not None else 0

            if resolved_version_id <= 0:
                versions = object_db.list_workflow_extension_versions(
                    connection=connection,
                    extension_pack_id=extension_pack_id,
                    is_active=True,
                    limit=1,
                )
                if not versions:
                    rejected.append(
                        {
                            "attachment_id": int(attachment.get("attachment_id") or 0),
                            "reason": "no_active_extension_version",
                        }
                    )
                    continue
                resolved_version_id = int(versions[0].get("extension_version_id") or 0)

            rules = object_db.list_workflow_extension_rules(
                connection=connection,
                extension_version_id=resolved_version_id,
                is_active=True,
            )
            for rule in rules:
                considered_rules += 1
                rule_hook_point = str(rule.get("hook_point") or "").strip().lower()
                if normalized_hook_point and rule_hook_point != normalized_hook_point:
                    rejected.append(
                        {
                            "attachment_id": int(attachment.get("attachment_id") or 0),
                            "extension_rule_id": int(rule.get("extension_rule_id") or 0),
                            "rule_key": rule.get("rule_key"),
                            "reason": "hook_point_mismatch",
                        }
                    )
                    continue

                rule_target_step_key = str(rule.get("target_step_key") or "").strip() or None
                if normalized_target_step_key and rule_target_step_key and rule_target_step_key != normalized_target_step_key:
                    rejected.append(
                        {
                            "attachment_id": int(attachment.get("attachment_id") or 0),
                            "extension_rule_id": int(rule.get("extension_rule_id") or 0),
                            "rule_key": rule.get("rule_key"),
                            "reason": "target_step_key_mismatch",
                        }
                    )
                    continue

                matched, reason = _evaluate_extension_condition(
                    rule.get("condition_json") if isinstance(rule.get("condition_json"), dict) else {},
                    env,
                )
                payload = {
                    "attachment_id": int(attachment.get("attachment_id") or 0),
                    "extension_pack_id": extension_pack_id,
                    "extension_version_id": resolved_version_id,
                    "extension_rule_id": int(rule.get("extension_rule_id") or 0),
                    "rule_key": rule.get("rule_key"),
                    "hook_point": rule.get("hook_point"),
                    "target_step_key": rule.get("target_step_key"),
                    "precedence": int(rule.get("precedence") or 100),
                    "conflict_policy": rule.get("conflict_policy"),
                    "inserted_steps_json": rule.get("inserted_steps_json") if isinstance(rule.get("inserted_steps_json"), list) else [],
                    "decision_reason": reason,
                }
                if matched:
                    selected.append(payload)
                else:
                    rejected.append(payload)

        selected.sort(
            key=lambda item: (
                int(item.get("precedence") or 100),
                int(item.get("attachment_id") or 0),
                int(item.get("extension_rule_id") or 0),
            )
        )

        return {
            "workflow_key": normalized_workflow_key,
            "workflow_version_id": workflow_version_id,
            "run_status": normalized_run_status,
            "hook_point": normalized_hook_point,
            "target_step_key": normalized_target_step_key,
            "attachments_considered": len(attachments),
            "rules_considered": int(considered_rules),
            "selected_rules": selected,
            "rejected_rules": rejected,
            "selected_count": len(selected),
            "rejected_count": len(rejected),
        }
    finally:
        connection.close()


def list_workflow_resource_aliases(
    workflow_id: int | None = None,
    workflow_key: str | None = None,
    workflow_version_id: int | None = None,
    resource_type: str | None = None,
) -> list[dict[str, Any]]:
    connection = object_db.get_connection()
    try:
        resolved_workflow_id = workflow_id
        if resolved_workflow_id is None and workflow_key:
            workflow = object_db.get_solf_workflow_registry_by_key(connection, workflow_key=workflow_key)
            resolved_workflow_id = int(workflow.get("workflow_id") or 0) if workflow else None
        return object_db.list_workflow_resource_aliases(
            connection=connection,
            workflow_id=resolved_workflow_id,
            workflow_version_id=workflow_version_id,
            resource_type=resource_type,
        )
    finally:
        connection.close()


def resolve_workflow_resource_alias(
    *,
    workflow_key: str,
    alias_name: str,
    resource_type: str | None = None,
    workflow_version_id: int | None = None,
) -> dict[str, Any] | None:
    normalized_workflow_key = str(workflow_key or "").strip()
    normalized_alias_name = str(alias_name or "").strip()
    normalized_resource_type = str(resource_type or "").strip().lower() or None
    if not normalized_workflow_key or not normalized_alias_name:
        return None

    connection = object_db.get_connection()
    try:
        workflow = object_db.get_solf_workflow_registry_by_key(connection, workflow_key=normalized_workflow_key)
        if workflow is None:
            return None

        version_id = workflow_version_id
        if version_id is None:
            active_version = object_db.get_workflow_active_version(connection, workflow_id=int(workflow.get("workflow_id") or 0))
            if active_version is not None:
                version_id = int(active_version.get("workflow_version_id") or 0)

        aliases = object_db.list_workflow_resource_aliases(
            connection=connection,
            workflow_id=int(workflow.get("workflow_id") or 0),
            workflow_version_id=version_id,
            resource_type=normalized_resource_type,
        )
        for alias in aliases:
            if str(alias.get("alias_name") or "").strip() == normalized_alias_name:
                resolved = dict(alias)
                resolved["workflow_key"] = normalized_workflow_key
                resolved["workflow_name"] = workflow.get("workflow_name")
                return resolved
        return None
    finally:
        connection.close()


def set_solf_workflow_registry_entry_active(workflow_id: int, is_active: bool) -> bool:
    connection = object_db.get_connection()
    try:
        return object_db.set_solf_workflow_registry_active(
            connection=connection,
            workflow_id=workflow_id,
            is_active=is_active,
        )
    finally:
        connection.close()


def _inject_default_policy_gate_steps(templates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Insert a policy-approval gate before user confirmation steps when missing.

    This keeps policy-gate behavior consistent across seeded templates without
    requiring each template block to duplicate the same step.
    """
    out: list[dict[str, Any]] = []

    default_flags = [
        "unreceipted_allowance",
        "personal_account_transfer",
        "amount_mismatch",
        "multi_receipt_allocation",
    ]

    for template in templates:
        original_steps = template.get("steps")
        if not isinstance(original_steps, list) or not original_steps:
            out.append(template)
            continue

        steps = [dict(step) if isinstance(step, dict) else step for step in original_steps]
        if not all(isinstance(step, dict) for step in steps):
            out.append(template)
            continue

        has_policy_gate = any(
            str(step.get("python_function") or "").strip() == "require_policy_approval_on_flags"
            for step in steps
        )
        if has_policy_gate:
            out.append(template)
            continue

        confirmation_indices = [
            idx
            for idx, step in enumerate(steps)
            if str(step.get("python_function") or "").strip() == "require_user_confirmation"
        ]
        if not confirmation_indices:
            out.append(template)
            continue

        first_confirmation_idx = confirmation_indices[0]
        approval_key = "policy_approved"
        prompt = (
            "Policy approval required when clarification risk flags are detected. "
            f"Set {approval_key}=yes and resume."
        )

        gate_step: dict[str, Any] = {
            "step_key": f"policy_gate_before_{approval_key}",
            "step_kind": "python_binding",
            "python_function": "require_policy_approval_on_flags",
            "config": {
                "flag_values": list(default_flags),
                "approval_key": approval_key,
                "accepted_values": ["yes", "approved", "true", "1"],
                "required_doc_types": ["approval_note"],
                "prompt": prompt,
            },
        }

        existing_keys = {
            str(step.get("step_key") or "").strip()
            for step in steps
            if isinstance(step, dict)
        }
        if gate_step["step_key"] in existing_keys:
            gate_step["step_key"] = f"policy_gate_{first_confirmation_idx + 1}"

        steps.insert(first_confirmation_idx, gate_step)

        for idx, step in enumerate(steps, start=1):
            step["step_order"] = idx

        updated_template = dict(template)
        updated_template["steps"] = steps
        out.append(updated_template)

    return out


def _workflow_template_specs() -> list[dict[str, Any]]:
    templates = [
        {
            "workflow_key": "template::llm_accounting_assist",
            "workflow_name": "Template - LLM Accounting Assist",
            "description": "Collect required fields, call LLM with prompt template, then pause for docs/approval before accounting action.",
            "domain": "accounting",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "llm_domain_pipeline",
            },
            "graph_spec": {
                "process": "llm_accounting_assist",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["document_text", "document_type", "extraction_goal"],
                "optional": ["approved", "doc_refs"],
            },
            "output_contract": {
                "produces": ["llm_extract", "domain_action_result"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_core_fields",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["document_text", "document_type", "extraction_goal"],
                        "required_doc_types": ["invoice", "receipt", "bill"],
                        "prompt": "Please provide document_text, document_type, and extraction_goal before continuing.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "llm_extract_json",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_llm_call",
                        "save_as": "llm_extract",
                        "payload": {
                            "prompt_template": "You are an accounting extraction assistant. Goal: {extraction_goal}. Document type: {document_type}. Extract structured accounting facts from:\n\n{document_text}",
                            "parse_json": True,
                            "temperature": 0.1,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "require_supporting_docs",
                    "step_kind": "python_binding",
                    "python_function": "require_document_refs",
                    "config": {
                        "min_count": 1,
                        "required_doc_types": ["supporting_document"],
                        "prompt": "Please ingest at least one supporting document and resume this run.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "require_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Review extraction output and approve to continue (approved=yes).",
                    },
                },
                {
                    "step_order": 5,
                    "step_key": "run_accounting_validation",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_domain_operation",
                        "save_as": "domain_action_result",
                        "fail_on_error": False,
                        "payload": {
                            "operation": "db_validate_ledger_payload",
                            "operation_payload": {
                                "entity": {
                                    "class_name": "accounting_transaction",
                                    "object_name": "pipeline_accounting_candidate",
                                    "attributes": {
                                        "ledger_lines": [
                                            {
                                                "account_number": "1100",
                                                "direction": "debit",
                                                "amount_source_currency": "100.00",
                                                "source_currency": "CHF",
                                                "exchange_rate_to_chf": "1.0",
                                                "amount_chf": "100.00",
                                            },
                                            {
                                                "account_number": "3000",
                                                "direction": "credit",
                                                "amount_source_currency": "100.00",
                                                "source_currency": "CHF",
                                                "exchange_rate_to_chf": "1.0",
                                                "amount_chf": "100.00",
                                            },
                                        ],
                                    },
                                },
                            },
                        },
                    },
                },
            ],
        },
        {
            "workflow_key": "template::schema_and_solf_generator",
            "workflow_name": "Template - Schema and SOLF Generator",
            "description": "Generate SQL DDL and SOLF class/input/output mappings, then optionally apply to DB after confirmation.",
            "domain": "schema",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "schema_generation",
            },
            "graph_spec": {
                "process": "schema_and_solf_generator",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["schema_name", "table_name", "class_name", "columns"],
                "optional": ["schema_apply"],
            },
            "output_contract": {
                "produces": ["schema_preview", "schema_apply_result"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_schema_inputs",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["schema_name", "table_name", "class_name", "columns"],
                        "prompt": "Provide schema_name, table_name, class_name, and columns before generating schema artifacts.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "preview_schema_and_solf",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_generate_schema_and_solf",
                        "save_as": "schema_preview",
                        "payload": {
                            "execute_ddl": False,
                            "register_class": False,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "confirm_schema_apply",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "schema_apply",
                        "accepted_values": ["yes", "apply", "true", "1"],
                        "prompt": "Review schema_preview and confirm schema_apply=yes to execute DDL.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "apply_schema_and_register_class",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_generate_schema_and_solf",
                        "save_as": "schema_apply_result",
                        "payload": {
                            "execute_ddl": True,
                            "register_class": True,
                        },
                    },
                },
            ],
        },
        {
            "workflow_key": "template::accounting_reconciliation_full",
            "workflow_name": "Template - Full Accounting Reconciliation",
            "description": "Full accounting reconciliation workflow with intake checks, reconciliation planning, booking-company resolution, double-entry validation, approval gating, and posting.",
            "domain": "accounting",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "accounting_reconciliation",
            },
            "graph_spec": {
                "process": "accounting_reconciliation",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["document_text", "document_type", "period_start", "period_end", "approved"],
                "optional": ["counterparty_name", "reconciliation_goal", "doc_refs"],
            },
            "output_contract": {
                "produces": [
                    "reconciliation_plan",
                    "booking_company_resolution",
                    "ledger_validation",
                    "posting_result",
                ],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_reconciliation_inputs",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["document_text", "document_type", "period_start", "period_end", "approved"],
                        "required_doc_types": ["bank_statement", "invoice", "general_ledger"],
                        "prompt": "Provide document_text, document_type, period_start, period_end, and approval flag before reconciliation.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "build_reconciliation_plan",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_llm_call",
                        "save_as": "reconciliation_plan",
                        "payload": {
                            "prompt_template": "You are a Swiss accounting reconciliation assistant. For period {period_start} to {period_end}, document type {document_type}, and goal {reconciliation_goal}, create a concise reconciliation plan with checks for completeness, double-entry balancing, VAT consistency, and posting readiness. Source text:\n\n{document_text}",
                            "parse_json": True,
                            "temperature": 0.0,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "require_reconciliation_documents",
                    "step_kind": "python_binding",
                    "python_function": "require_document_refs",
                    "config": {
                        "min_count": 2,
                        "required_doc_types": ["bank_statement", "invoice", "general_ledger"],
                        "prompt": "Please ingest reconciliation source documents (bank statement and ledger evidence), then resume.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "resolve_booking_company",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_domain_operation",
                        "save_as": "booking_company_resolution",
                        "payload": {
                            "operation": "resolve_accounting_booking_company",
                            "operation_payload": {
                                "entity": {
                                    "class_name": "accounting_transaction",
                                    "object_name": "reconciliation_candidate",
                                    "attributes": {
                                        "person_ref": "employee_ref_pending",
                                        "counterparty_name": "counterparty_pending",
                                        "description": "Reconciliation posting candidate",
                                        "ledger_lines": [
                                            {
                                                "account_number": "1100",
                                                "direction": "debit",
                                                "amount_source_currency": "100.00",
                                                "source_currency": "CHF",
                                                "exchange_rate_to_chf": "1.0",
                                                "amount_chf": "100.00",
                                                "swiss_vat_code": "NONE",
                                            },
                                            {
                                                "account_number": "2000",
                                                "direction": "credit",
                                                "amount_source_currency": "100.00",
                                                "source_currency": "CHF",
                                                "exchange_rate_to_chf": "1.0",
                                                "amount_chf": "100.00",
                                                "swiss_vat_code": "NONE",
                                            },
                                        ],
                                    },
                                },
                            },
                        },
                    },
                },
                {
                    "step_order": 5,
                    "step_key": "validate_double_entry",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_domain_operation",
                        "save_as": "ledger_validation",
                        "payload": {
                            "operation": "db_validate_ledger_payload",
                            "operation_payload": {
                                "entity": {
                                    "class_name": "accounting_transaction",
                                    "object_name": "reconciliation_candidate",
                                    "attributes": {
                                        "ledger_lines": [
                                            {
                                                "account_number": "1100",
                                                "direction": "debit",
                                                "amount_source_currency": "100.00",
                                                "source_currency": "CHF",
                                                "exchange_rate_to_chf": "1.0",
                                                "amount_chf": "100.00",
                                                "swiss_vat_code": "NONE",
                                            },
                                            {
                                                "account_number": "2000",
                                                "direction": "credit",
                                                "amount_source_currency": "100.00",
                                                "source_currency": "CHF",
                                                "exchange_rate_to_chf": "1.0",
                                                "amount_chf": "100.00",
                                                "swiss_vat_code": "NONE",
                                            },
                                        ],
                                    },
                                },
                            },
                        },
                    },
                },
                {
                    "step_order": 6,
                    "step_key": "require_policy_approval_for_risk_flags",
                    "step_kind": "python_binding",
                    "python_function": "require_policy_approval_on_flags",
                    "config": {
                        "flag_values": [
                            "unreceipted_allowance",
                            "personal_account_transfer",
                            "amount_mismatch",
                            "multi_receipt_allocation",
                        ],
                        "approval_key": "policy_approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "required_doc_types": ["approval_note", "manager_approval"],
                        "prompt": "High-risk clarification flags detected. Provide policy_approved=yes (and supporting approval note if required), then resume.",
                    },
                },
                {
                    "step_order": 7,
                    "step_key": "require_finance_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Review reconciliation_plan and ledger_validation, then approve to post (approved=yes).",
                    },
                },
                {
                    "step_order": 8,
                    "step_key": "post_reconciled_transaction",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_domain_operation",
                        "save_as": "posting_result",
                        "payload": {
                            "operation": "db_accounting_ingest",
                            "operation_payload": {
                                "entity": {
                                    "class_name": "accounting_transaction",
                                    "object_name": "reconciliation_candidate",
                                    "attributes": {
                                        "transaction_id": "recon_txn_001",
                                        "description": "Posted from full reconciliation template",
                                        "currency": "CHF",
                                        "ledger_lines": [
                                            {
                                                "account_number": "1100",
                                                "direction": "debit",
                                                "amount_source_currency": "100.00",
                                                "source_currency": "CHF",
                                                "exchange_rate_to_chf": "1.0",
                                                "amount_chf": "100.00",
                                                "swiss_vat_code": "NONE",
                                            },
                                            {
                                                "account_number": "2000",
                                                "direction": "credit",
                                                "amount_source_currency": "100.00",
                                                "source_currency": "CHF",
                                                "exchange_rate_to_chf": "1.0",
                                                "amount_chf": "100.00",
                                                "swiss_vat_code": "NONE",
                                            },
                                        ],
                                    },
                                },
                            },
                        },
                    },
                },
            ],
        },
        {
            "workflow_key": "template::crm_lead_pipeline",
            "workflow_name": "Template - CRM Lead to Opportunity",
            "description": "CRM lead workflow with qualification, evidence checks, and approval gating before opportunity advancement.",
            "domain": "crm",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "crm_pipeline",
            },
            "graph_spec": {
                "process": "crm_lead_pipeline",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["lead_name", "account_name", "opportunity_stage", "sales_approved"],
                "optional": ["lead_source", "contact_email", "doc_refs"],
            },
            "output_contract": {
                "produces": ["lead_qualification_result", "opportunity_recommendation"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_crm_lead_fields",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["lead_name", "account_name", "opportunity_stage", "sales_approved"],
                        "required_doc_types": ["lead_brief", "customer_note"],
                        "prompt": "Provide lead_name, account_name, opportunity_stage, and sales_approved before CRM processing.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "qualify_crm_lead",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_llm_call",
                        "save_as": "lead_qualification_result",
                        "payload": {
                            "prompt_template": "You are a CRM qualification assistant. Evaluate lead {lead_name} for account {account_name} at stage {opportunity_stage}. Return qualification summary, fit score, and next best actions.",
                            "parse_json": True,
                            "temperature": 0.1,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "require_crm_documents",
                    "step_kind": "python_binding",
                    "python_function": "require_document_refs",
                    "config": {
                        "min_count": 1,
                        "required_doc_types": ["lead_brief", "customer_note"],
                        "prompt": "Please ingest lead evidence documents and resume.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "require_sales_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "sales_approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Review qualification result and confirm opportunity advancement (sales_approved=yes).",
                    },
                },
            ],
        },
        {
            "workflow_key": "template::erp_procure_to_pay",
            "workflow_name": "Template - ERP Procure to Pay",
            "description": "ERP procure-to-pay workflow with controls extraction, 3-way evidence requirements, and payment approval gating.",
            "domain": "erp",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "erp_pipeline",
            },
            "graph_spec": {
                "process": "erp_procure_to_pay",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["procurement_request_id", "vendor_name", "total_amount", "currency", "finance_approved"],
                "optional": ["purchase_order_no", "invoice_no", "doc_refs"],
            },
            "output_contract": {
                "produces": ["erp_controls_extract", "erp_payment_recommendation"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_erp_fields",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["procurement_request_id", "vendor_name", "total_amount", "currency", "finance_approved"],
                        "required_doc_types": ["purchase_order", "invoice", "goods_receipt"],
                        "prompt": "Provide procurement_request_id, vendor_name, total_amount, currency, and finance_approved before ERP execution.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "extract_erp_controls",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_llm_call",
                        "save_as": "erp_controls_extract",
                        "payload": {
                            "prompt_template": "You are an ERP controls assistant. For procurement request {procurement_request_id}, vendor {vendor_name}, amount {total_amount} {currency}, summarize PO/invoice/GR controls and payment risks.",
                            "parse_json": True,
                            "temperature": 0.1,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "require_erp_three_way_docs",
                    "step_kind": "python_binding",
                    "python_function": "require_document_refs",
                    "config": {
                        "min_count": 2,
                        "required_doc_types": ["purchase_order", "invoice", "goods_receipt"],
                        "prompt": "Please ingest PO/invoice/GR evidence for 3-way check and resume.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "require_finance_payment_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "finance_approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Confirm payment readiness after ERP controls review (finance_approved=yes).",
                    },
                },
            ],
        },
        {
            "workflow_key": "template::project_delivery_governance",
            "workflow_name": "Template - Project Delivery Governance",
            "description": "Project management workflow for milestone health, risks/issues, evidence checks, and governance approval.",
            "domain": "project_management",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "project_management_pipeline",
            },
            "graph_spec": {
                "process": "project_delivery_governance",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["project_name", "milestone_name", "status_report", "pm_approved"],
                "optional": ["risk_register", "issue_log", "doc_refs"],
            },
            "output_contract": {
                "produces": ["project_governance_review", "delivery_decision"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_project_fields",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["project_name", "milestone_name", "status_report", "pm_approved"],
                        "required_doc_types": ["project_plan", "timesheet", "status_report"],
                        "prompt": "Provide project_name, milestone_name, status_report, and pm_approved before governance review.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "review_project_governance",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_llm_call",
                        "save_as": "project_governance_review",
                        "payload": {
                            "prompt_template": "You are a project governance assistant. Review project {project_name}, milestone {milestone_name}, and status report: {status_report}. Return milestone health, top risks, and go/no-go recommendation.",
                            "parse_json": True,
                            "temperature": 0.1,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "require_project_evidence_docs",
                    "step_kind": "python_binding",
                    "python_function": "require_document_refs",
                    "config": {
                        "min_count": 1,
                        "required_doc_types": ["project_plan", "timesheet", "status_report"],
                        "prompt": "Please ingest project evidence documents and resume.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "require_pm_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "pm_approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Confirm project delivery decision (pm_approved=yes).",
                    },
                },
            ],
        },
        {
            "workflow_key": "template::project_change_control",
            "workflow_name": "Template - Project Change Control",
            "description": "Project change-control workflow for impact analysis, evidence checks, and governance approval.",
            "domain": "project_management",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "project_management_pipeline",
            },
            "graph_spec": {
                "process": "project_change_control",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["project_name", "change_request_id", "change_summary", "governance_approved"],
                "optional": ["impact_scope", "impact_cost", "impact_schedule", "doc_refs"],
            },
            "output_contract": {
                "produces": ["change_impact_review", "change_control_decision"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_change_control_fields",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["project_name", "change_request_id", "change_summary", "governance_approved"],
                        "required_doc_types": ["change_request", "impact_assessment"],
                        "prompt": "Provide project_name, change_request_id, change_summary, and governance_approved before change-control review.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "review_change_impact",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_llm_call",
                        "save_as": "change_impact_review",
                        "payload": {
                            "prompt_template": "You are a project change-control assistant. Review change request {change_request_id} for project {project_name}: {change_summary}. Summarize impact on scope, cost, schedule, and top delivery risks.",
                            "parse_json": True,
                            "temperature": 0.1,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "require_change_docs",
                    "step_kind": "python_binding",
                    "python_function": "require_document_refs",
                    "config": {
                        "min_count": 1,
                        "required_doc_types": ["change_request", "impact_assessment"],
                        "prompt": "Please ingest change request and impact assessment documents, then resume.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "require_governance_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "governance_approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Confirm governance approval for this change request (governance_approved=yes).",
                    },
                },
            ],
        },
        {
            "workflow_key": "template::crm_case_management",
            "workflow_name": "Template - CRM Case Management",
            "description": "CRM service workflow for case triage, SLA risk analysis, evidence checks, and support approval.",
            "domain": "crm",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "crm_service_pipeline",
            },
            "graph_spec": {
                "process": "crm_case_management",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["case_id", "account_name", "case_priority", "case_summary", "support_approved"],
                "optional": ["sla_hours", "doc_refs"],
            },
            "output_contract": {
                "produces": ["case_triage_summary", "case_resolution_recommendation"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_case_fields",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["case_id", "account_name", "case_priority", "case_summary", "support_approved"],
                        "required_doc_types": ["ticket", "customer_email"],
                        "prompt": "Provide case_id, account_name, case_priority, case_summary, and support_approved before case management.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "triage_case",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_llm_call",
                        "save_as": "case_triage_summary",
                        "payload": {
                            "prompt_template": "You are a CRM support assistant. Triage case {case_id} for account {account_name} with priority {case_priority}. Case summary: {case_summary}. Return root-cause hypotheses, SLA risk, and next actions.",
                            "parse_json": True,
                            "temperature": 0.1,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "require_case_documents",
                    "step_kind": "python_binding",
                    "python_function": "require_document_refs",
                    "config": {
                        "min_count": 1,
                        "required_doc_types": ["ticket", "customer_email", "sla_policy"],
                        "prompt": "Please ingest case evidence documents and resume.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "require_support_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "support_approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Confirm support approval for resolution execution (support_approved=yes).",
                    },
                },
            ],
        },
        {
            "workflow_key": "template::erp_order_to_cash",
            "workflow_name": "Template - ERP Order to Cash",
            "description": "ERP order-to-cash workflow with order verification, credit/risk checks, evidence requirements, and approval.",
            "domain": "erp",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "erp_pipeline",
            },
            "graph_spec": {
                "process": "erp_order_to_cash",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["sales_order_no", "customer_name", "order_amount", "currency", "credit_approved"],
                "optional": ["invoice_no", "shipment_no", "doc_refs"],
            },
            "output_contract": {
                "produces": ["otc_risk_review", "collection_recommendation"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_otc_fields",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["sales_order_no", "customer_name", "order_amount", "currency", "credit_approved"],
                        "required_doc_types": ["sales_order", "delivery_note", "invoice"],
                        "prompt": "Provide sales_order_no, customer_name, order_amount, currency, and credit_approved before order-to-cash execution.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "review_otc_risk",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_llm_call",
                        "save_as": "otc_risk_review",
                        "payload": {
                            "prompt_template": "You are an ERP order-to-cash assistant. Review sales order {sales_order_no} for customer {customer_name}, amount {order_amount} {currency}. Return credit risk summary, invoicing readiness, and collection risks.",
                            "parse_json": True,
                            "temperature": 0.1,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "require_otc_documents",
                    "step_kind": "python_binding",
                    "python_function": "require_document_refs",
                    "config": {
                        "min_count": 2,
                        "required_doc_types": ["sales_order", "delivery_note", "invoice"],
                        "prompt": "Please ingest order-to-cash documents (SO/shipment/invoice evidence) and resume.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "require_credit_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "credit_approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Confirm credit/commercial approval for order fulfillment and collection (credit_approved=yes).",
                    },
                },
            ],
        },
        {
            "workflow_key": "template::sales_full_cycle",
            "workflow_name": "Template - Sales Full Cycle",
            "description": "Sales workflow from user request through quotation, order confirmation, delivery, invoice, and payment evidence closure.",
            "domain": "erp",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "sales_pipeline",
            },
            "graph_spec": {
                "process": "sales_full_cycle",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["customer_name", "service_type", "currency", "sales_approved"],
                "optional": ["quotation_no", "sales_order_no", "invoice_no", "payment_ref", "doc_refs"],
            },
            "output_contract": {
                "produces": ["sales_stage_review", "sales_cycle_readiness"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_sales_intake_fields",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["customer_name", "service_type", "currency", "sales_approved"],
                        "required_doc_types": [
                            "customer_request",
                            "quotation",
                            "order_confirmation",
                            "delivery_note",
                            "invoice",
                            "payment_receipt",
                        ],
                        "prompt": "Provide customer_name, service_type, currency, and sales_approved before running full sales workflow.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "review_sales_stage_gaps",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_llm_call",
                        "save_as": "sales_stage_review",
                        "payload": {
                            "prompt_template": "You are a sales operations assistant. For customer {customer_name} and service {service_type}, evaluate readiness across stages: request, quotation, order confirmation, delivery, invoice, payment. Return missing items and next actions.",
                            "parse_json": True,
                            "temperature": 0.1,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "require_sales_cycle_documents",
                    "step_kind": "python_binding",
                    "python_function": "require_document_refs",
                    "config": {
                        "min_count": 3,
                        "required_doc_types": [
                            "quotation",
                            "order_confirmation",
                            "delivery_note",
                            "invoice",
                            "payment_receipt",
                        ],
                        "prompt": "Please ingest sales-cycle evidence docs (quotation/order confirmation/delivery/invoice/payment) and resume.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "require_sales_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "sales_approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Confirm sales approval for progressing the full sales process (sales_approved=yes).",
                    },
                },
            ],
        },
        {
            "workflow_key": "template::hr_onboarding_composition",
            "workflow_name": "Template - HR Onboarding Composition",
            "description": "Compose LLM planning and HR domain validation/ingestion operations to drive onboarding workflows.",
            "domain": "hr",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "composed_domain_pipeline",
            },
            "graph_spec": {
                "process": "hr_onboarding_composition",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["employee_name", "employer_ref", "start_date"],
                "optional": ["approved", "doc_refs"],
            },
            "output_contract": {
                "produces": ["onboarding_plan", "hr_compose_result"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_onboarding_fields",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["employee_name", "employer_ref", "start_date"],
                        "required_doc_types": ["employment_contract"],
                        "prompt": "Please provide employee_name, employer_ref, and start_date.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "generate_onboarding_plan",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_llm_call",
                        "save_as": "onboarding_plan",
                        "payload": {
                            "prompt_template": "Build a concise HR onboarding checklist for employee {employee_name} joining {employer_ref} on {start_date}.",
                            "temperature": 0.1,
                        },
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "compose_hr_operations",
                    "step_kind": "python_binding",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_compose_operations",
                        "save_as": "hr_compose_result",
                        "fail_on_error": False,
                        "payload": {
                            "stop_on_error": False,
                            "operations": [
                                {
                                    "action": "workflow_domain_operation",
                                    "save_as": "hr_validation",
                                    "payload": {
                                        "operation": "db_hr_validate_payload",
                                        "operation_payload": {
                                            "entity": {
                                                "class_name": "employment_contract",
                                                "object_name": "hr_onboarding_candidate",
                                                "attributes": {
                                                    "person_ref": "employee_ref_pending",
                                                    "employer_ref": "employer_ref_pending",
                                                    "start_date": "2026-01-01",
                                                },
                                            },
                                        },
                                    },
                                },
                                {
                                    "action": "workflow_domain_operation",
                                    "save_as": "hr_ingest",
                                    "payload": {
                                        "operation": "db_hr_ingest",
                                        "operation_payload": {
                                            "entity": {
                                                "class_name": "employment_contract",
                                                "object_name": "hr_onboarding_candidate",
                                                "attributes": {
                                                    "person_ref": "employee_ref_pending",
                                                    "employer_ref": "employer_ref_pending",
                                                    "start_date": "2026-01-01",
                                                },
                                            },
                                        },
                                    },
                                },
                            ],
                        },
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "require_hr_docs",
                    "step_kind": "python_binding",
                    "python_function": "require_document_refs",
                    "config": {
                        "min_count": 1,
                        "required_doc_types": ["employment_contract", "id_document"],
                        "prompt": "Please ingest onboarding documents and resume.",
                    },
                },
                {
                    "step_order": 5,
                    "step_key": "final_hr_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Confirm HR onboarding workflow completion (approved=yes).",
                    },
                },
            ],
        },
        {
            "workflow_key": "template::generic_clarification_gate",
            "workflow_name": "Template - Generic Clarification Gate",
            "description": "Domain-agnostic workflow skeleton that enforces clarification notes and supporting evidence before approval.",
            "domain": "workflow",
            "status": "review",
            "metadata": {
                "source": "seed_templates",
                "template_family": "generic_workflow",
            },
            "graph_spec": {
                "process": "workflow_execute",
                "control_flow": "sequence",
                "stop_on_error": True,
            },
            "input_contract": {
                "required": ["case_summary", "approved"],
                "optional": ["clarification_note", "doc_refs"],
            },
            "output_contract": {
                "produces": ["clarification_check", "case_decision"],
            },
            "steps": [
                {
                    "step_order": 1,
                    "step_key": "require_case_fields",
                    "step_kind": "python_binding",
                    "python_function": "require_fields",
                    "config": {
                        "required_fields": ["case_summary", "approved"],
                        "required_doc_types": ["supporting_note"],
                        "prompt": "Provide case_summary and approved flag before proceeding.",
                    },
                },
                {
                    "step_order": 2,
                    "step_key": "require_case_clarification",
                    "step_kind": "python_binding",
                    "python_function": "require_clarification",
                    "config": {
                        "require_note": True,
                        "note_min_length": 30,
                        "min_doc_count": 1,
                        "required_doc_types": ["supporting_note", "supporting_receipt"],
                        "prompt": "Please provide a clear clarification note and at least one supporting document, then resume.",
                    },
                },
                {
                    "step_order": 3,
                    "step_key": "require_policy_gate",
                    "step_kind": "python_binding",
                    "python_function": "require_policy_approval_on_flags",
                    "config": {
                        "approval_key": "policy_approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "required_doc_types": ["approval_note"],
                        "prompt": "Policy approval required for detected clarification risk flags. Set policy_approved=yes and resume.",
                    },
                },
                {
                    "step_order": 4,
                    "step_key": "require_case_approval",
                    "step_kind": "python_binding",
                    "python_function": "require_user_confirmation",
                    "config": {
                        "confirmation_key": "approved",
                        "accepted_values": ["yes", "approved", "true", "1"],
                        "prompt": "Confirm approval after clarification review (approved=yes).",
                    },
                },
            ],
        },
    ]
    return _inject_default_policy_gate_steps(templates)


def seed_workflow_registry_templates(created_by: str | None = None) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        templates = _workflow_template_specs()
        seeded: list[dict[str, Any]] = []

        for template in templates:
            workflow_id = object_db.upsert_solf_workflow_registry(
                connection=connection,
                workflow_key=str(template.get("workflow_key") or "").strip(),
                workflow_name=str(template.get("workflow_name") or "").strip(),
                description=str(template.get("description") or "").strip(),
                domain=str(template.get("domain") or "").strip() or None,
                status=str(template.get("status") or "review").strip().lower() or "review",
                metadata=template.get("metadata") if isinstance(template.get("metadata"), dict) else {},
                is_active=True,
                created_by=created_by,
            )

            workflow_version_id = object_db.create_solf_workflow_version(
                connection=connection,
                workflow_id=int(workflow_id),
                rule_id=None,
                graph_spec=template.get("graph_spec") if isinstance(template.get("graph_spec"), dict) else {},
                input_contract=template.get("input_contract") if isinstance(template.get("input_contract"), dict) else {},
                output_contract=template.get("output_contract") if isinstance(template.get("output_contract"), dict) else {},
                metadata={
                    "source": "seed_workflow_registry_templates",
                    "template": True,
                },
                is_active=True,
                created_by=created_by,
            )

            step_count = object_db.replace_solf_workflow_steps(
                connection=connection,
                workflow_version_id=int(workflow_version_id),
                steps=template.get("steps") if isinstance(template.get("steps"), list) else [],
            )

            seeded.append(
                {
                    "workflow_id": int(workflow_id),
                    "workflow_version_id": int(workflow_version_id),
                    "workflow_key": str(template.get("workflow_key") or ""),
                    "workflow_name": str(template.get("workflow_name") or ""),
                    "step_count": int(step_count),
                }
            )

        connection.commit()
        return {
            "seeded_count": len(seeded),
            "seeded_workflows": seeded,
        }
    finally:
        connection.close()


def _preprogrammed_workflow_descriptions() -> list[dict[str, Any]]:
    return [
        {
            "workflow_key": "builtin::ingestion_pipeline",
            "workflow_name": "Built-in Ingestion Pipeline",
            "source": "preprogrammed",
            "description": "Document ingestion pipeline with routing, extraction, enrichment, indexing, and DB persistence.",
            "steps": [
                {"step_order": 1, "step_key": "load_solf", "step_kind": "builtin", "operation": "load_solf_classes"},
                {"step_order": 2, "step_key": "resolve_processing_rules", "step_kind": "builtin", "operation": "resolve_named_rules"},
                {"step_order": 3, "step_key": "upload_or_resolve_source", "step_kind": "builtin", "operation": "source_resolution"},
                {"step_order": 4, "step_key": "genai_router", "step_kind": "builtin", "operation": "doc_type_routing"},
                {"step_order": 5, "step_key": "extraction_source", "step_kind": "builtin", "operation": "markdown_or_source_selection"},
                {"step_order": 6, "step_key": "genai_extract", "step_kind": "builtin", "operation": "llm_entity_extraction"},
                {"step_order": 7, "step_key": "apply_domain_action_policy", "step_kind": "builtin", "operation": "domain_policy_application"},
                {"step_order": 8, "step_key": "db_ingest", "step_kind": "builtin", "operation": "persist_entities_relationships"},
                {"step_order": 9, "step_key": "qdrant_index", "step_kind": "builtin", "operation": "vector_indexing"},
                {"step_order": 10, "step_key": "discovery_index", "step_kind": "builtin", "operation": "discovery_datastore_indexing"},
            ],
        },
        {
            "workflow_key": "builtin::query_pipeline",
            "workflow_name": "Built-in Query Pipeline",
            "source": "preprogrammed",
            "description": "Grounded query pipeline with language detection, planner-assisted parsing, retrieval, ranking, and answer rendering.",
            "steps": [
                {"step_order": 1, "step_key": "detect_language", "step_kind": "builtin", "operation": "language_detection"},
                {"step_order": 2, "step_key": "parse_query", "step_kind": "builtin", "operation": "symbolic_query_parse"},
                {"step_order": 3, "step_key": "llm_structured_plan", "step_kind": "builtin", "operation": "planner_first_parse"},
                {"step_order": 4, "step_key": "repair_and_validate_plan", "step_kind": "builtin", "operation": "planner_sanitization"},
                {"step_order": 5, "step_key": "retrieve_candidates", "step_kind": "builtin", "operation": "object_document_retrieval"},
                {"step_order": 6, "step_key": "rank_and_filter", "step_kind": "builtin", "operation": "scoring_and_disambiguation"},
                {"step_order": 7, "step_key": "execute_query", "step_kind": "builtin", "operation": "db_and_graph_execution"},
                {"step_order": 8, "step_key": "render_answer", "step_kind": "builtin", "operation": "answer_formatting"},
            ],
        },
    ]


def _load_registry_workflow_descriptions(
    connection: Any,
    workflow_keys: list[str] | None = None,
) -> list[dict[str, Any]]:
    normalized_keys = {
        str(item or "").strip().lower()
        for item in (workflow_keys or [])
        if str(item or "").strip()
    }

    registry_rows = object_db.list_solf_workflow_registry(
        connection=connection,
        is_active=None,
        domain=None,
        status=None,
        limit=5000,
    )

    out: list[dict[str, Any]] = []
    for row in registry_rows:
        workflow_key = str(row.get("workflow_key") or "").strip()
        if normalized_keys and workflow_key.lower() not in normalized_keys:
            continue

        workflow_id = int(row.get("workflow_id") or 0)
        versions = object_db.list_solf_workflow_versions(
            connection=connection,
            workflow_id=workflow_id,
            is_active=None,
            limit=200,
        )
        if not versions:
            continue

        selected_version = next((v for v in versions if bool(v.get("is_active"))), versions[0])
        workflow_version_id = int(selected_version.get("workflow_version_id") or 0)
        steps = object_db.list_solf_workflow_steps(connection, workflow_version_id=workflow_version_id)

        out.append(
            {
                "workflow_key": workflow_key,
                "workflow_name": str(row.get("workflow_name") or workflow_key),
                "source": "user_defined",
                "description": str(row.get("description") or "").strip(),
                "domain": row.get("domain"),
                "status": row.get("status"),
                "workflow_id": workflow_id,
                "workflow_version_id": workflow_version_id,
                "steps": steps,
            }
        )
    return out


def _format_workflow_descriptions_markdown(workflows: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("# Workflow Pipeline Descriptions")
    lines.append("")
    lines.append(f"Generated at: {datetime.utcnow().isoformat()}Z")
    lines.append("")

    for workflow in workflows:
        lines.append(f"## {workflow.get('workflow_name')}")
        lines.append("")
        lines.append(f"- workflow_key: {workflow.get('workflow_key')}")
        lines.append(f"- source: {workflow.get('source')}")
        if workflow.get("domain"):
            lines.append(f"- domain: {workflow.get('domain')}")
        if workflow.get("status"):
            lines.append(f"- status: {workflow.get('status')}")
        if workflow.get("workflow_version_id"):
            lines.append(f"- workflow_version_id: {workflow.get('workflow_version_id')}")
        description = str(workflow.get("description") or "").strip()
        if description:
            lines.append(f"- description: {description}")
        lines.append("")
        lines.append("### Steps")
        lines.append("")

        steps = workflow.get("steps") if isinstance(workflow.get("steps"), list) else []
        if not steps:
            lines.append("- (no steps)")
        else:
            for step in sorted(steps, key=lambda item: int(item.get("step_order") or 0)):
                step_order = int(step.get("step_order") or 0)
                step_key = str(step.get("step_key") or "")
                step_kind = str(step.get("step_kind") or "")
                operation = str(step.get("operation") or "")
                clause_name = str(step.get("clause_name") or "")
                python_module = str(step.get("python_module") or "")
                python_function = str(step.get("python_function") or "")

                parts = [f"{step_order}. {step_key} [{step_kind}]"]
                if operation:
                    parts.append(f"operation={operation}")
                if clause_name:
                    parts.append(f"clause={clause_name}")
                if python_function:
                    parts.append(f"python={python_module + '.' if python_module else ''}{python_function}")

                lines.append(f"- {'; '.join(parts)}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _format_workflow_descriptions_text(workflows: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("WORKFLOW PIPELINE DESCRIPTIONS")
    lines.append(f"Generated at: {datetime.utcnow().isoformat()}Z")
    lines.append("")

    for workflow in workflows:
        lines.append(f"Workflow: {workflow.get('workflow_name')}")
        lines.append(f"  key: {workflow.get('workflow_key')}")
        lines.append(f"  source: {workflow.get('source')}")
        if workflow.get("domain"):
            lines.append(f"  domain: {workflow.get('domain')}")
        if workflow.get("status"):
            lines.append(f"  status: {workflow.get('status')}")
        if workflow.get("workflow_version_id"):
            lines.append(f"  version_id: {workflow.get('workflow_version_id')}")
        description = str(workflow.get("description") or "").strip()
        if description:
            lines.append(f"  description: {description}")
        lines.append("  steps:")

        steps = workflow.get("steps") if isinstance(workflow.get("steps"), list) else []
        if not steps:
            lines.append("    - (no steps)")
        else:
            for step in sorted(steps, key=lambda item: int(item.get("step_order") or 0)):
                step_order = int(step.get("step_order") or 0)
                step_key = str(step.get("step_key") or "")
                step_kind = str(step.get("step_kind") or "")
                operation = str(step.get("operation") or "")
                clause_name = str(step.get("clause_name") or "")
                python_module = str(step.get("python_module") or "")
                python_function = str(step.get("python_function") or "")

                detail_bits: list[str] = []
                if operation:
                    detail_bits.append(f"operation={operation}")
                if clause_name:
                    detail_bits.append(f"clause={clause_name}")
                if python_function:
                    detail_bits.append(f"python={python_module + '.' if python_module else ''}{python_function}")
                details = f" ({', '.join(detail_bits)})" if detail_bits else ""
                lines.append(f"    - {step_order}. {step_key} [{step_kind}]{details}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def export_workflow_pipeline_descriptions(
    *,
    file_format: str = "md",
    include_preprogrammed: bool = True,
    include_user_defined: bool = True,
    workflow_keys: list[str] | None = None,
    output_file_name: str | None = None,
) -> dict[str, Any]:
    normalized_format = str(file_format or "md").strip().lower()
    if normalized_format not in {"md", "txt"}:
        raise ValueError("file_format must be 'md' or 'txt'")

    workflows: list[dict[str, Any]] = []
    if include_preprogrammed:
        preprogrammed = _preprogrammed_workflow_descriptions()
        if workflow_keys:
            keys = {str(item).strip().lower() for item in workflow_keys if str(item).strip()}
            preprogrammed = [item for item in preprogrammed if str(item.get("workflow_key") or "").lower() in keys]
        workflows.extend(preprogrammed)

    if include_user_defined:
        connection = object_db.get_connection()
        try:
            workflows.extend(_load_registry_workflow_descriptions(connection, workflow_keys=workflow_keys))
        finally:
            connection.close()

    workflows.sort(key=lambda item: (str(item.get("source") or ""), str(item.get("workflow_name") or "").lower()))

    if normalized_format == "md":
        content = _format_workflow_descriptions_markdown(workflows)
    else:
        content = _format_workflow_descriptions_text(workflows)

    base_dir = Path(__file__).resolve().parent / "generated" / "workflow_docs"
    base_dir.mkdir(parents=True, exist_ok=True)

    safe_file_name = str(output_file_name or "").strip()
    if safe_file_name:
        safe_file_name = re.sub(r"[^a-zA-Z0-9._-]+", "_", safe_file_name)
        if not safe_file_name.lower().endswith(f".{normalized_format}"):
            safe_file_name = f"{safe_file_name}.{normalized_format}"
    else:
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        safe_file_name = f"workflow_pipelines_{stamp}.{normalized_format}"

    out_path = base_dir / safe_file_name
    out_path.write_text(content, encoding="utf-8")

    return {
        "success": True,
        "file_path": str(out_path),
        "file_format": normalized_format,
        "workflow_count": len(workflows),
        "workflows": [
            {
                "workflow_key": item.get("workflow_key"),
                "workflow_name": item.get("workflow_name"),
                "source": item.get("source"),
                "step_count": len(item.get("steps") or []),
            }
            for item in workflows
        ],
    }


def export_workflow_pipeline_descriptions_inline(
    *,
    file_format: str = "md",
    include_preprogrammed: bool = True,
    include_user_defined: bool = True,
    workflow_keys: list[str] | None = None,
    output_file_name: str | None = None,
) -> dict[str, Any]:
    """Generate workflow documentation file and return content inline."""
    export_result = export_workflow_pipeline_descriptions(
        file_format=file_format,
        include_preprogrammed=include_preprogrammed,
        include_user_defined=include_user_defined,
        workflow_keys=workflow_keys,
        output_file_name=output_file_name,
    )

    file_path = Path(str(export_result.get("file_path") or "")).resolve()
    content = file_path.read_text(encoding="utf-8")

    out = dict(export_result)
    out["content"] = content
    return out


def get_solf_llm_generation_pack(include_markdown: bool = True) -> dict[str, Any]:
    """Return a JSON-ready pack that helps LLMs generate executable SOLF artifacts."""
    markdown_path = Path(__file__).resolve().parent / "log" / "IDMS_SOLF_LLM_GENERATION_PACK.md"
    markdown_content = ""
    if include_markdown:
        try:
            markdown_content = markdown_path.read_text(encoding="utf-8")
        except Exception:
            markdown_content = ""

    allowed_operations = [
        "db_validate_ledger_payload",
        "resolve_accounting_booking_company",
        "db_accounting_ingest",
        "db_accounting_delete",
        "db_hr_validate_payload",
        "db_hr_ingest",
        "db_hr_delete",
    ]
    try:
        import solf_function as _solf_function

        getter = getattr(_solf_function, "get_allowed_workflow_domain_operations", None)
        if callable(getter):
            resolved = getter()
            if isinstance(resolved, list) and resolved:
                allowed_operations = [str(item).strip() for item in resolved if str(item).strip()]
    except Exception:
        pass

    return {
        "version": "1.0",
        "source": "idms",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "syntax": {
            "clause_wrapper": "clause_name(args) ⦃ ... ⦄",
            "return_operator": "↲(value)",
            "assignment_operator": "≔",
            "logical_and": "⋀",
            "logical_or": "⋁",
            "class_block": "class_name ≔ { attribute_name: type }",
        },
        "output_contract": {
            "type": "json_object",
            "required_keys": ["rule_name", "class_definitions", "solf_script"],
            "class_definition_shape": {
                "class_name": "snake_case_string",
                "parent_class_name": "snake_case_string_or_null",
                "attributes": ["snake_case_string"],
            },
        },
        "templates": {
            "domain_operation_clause": (
                "{{clause_name}}(_ctx) ⦃\n"
                "    ↲(workflow_domain_operation({\n"
                "        operation: {{operation_name}},\n"
                "        operation_payload: _ctx\n"
                "    }))\n"
                "⦄"
            ),
            "compose_clause": (
                "{{entry_clause}}(_ctx) ⦃\n"
                "    ↲(workflow_compose_operations({\n"
                "        context: _ctx,\n"
                "        stop_on_error: true,\n"
                "        operations: [\n"
                "            { action: workflow_domain_operation, payload: { operation: {{op1}}, operation_payload: _ctx }, save_as: {{save1}} },\n"
                "            { action: workflow_domain_operation, payload: { operation: {{op2}}, operation_payload: _ctx }, save_as: {{save2}} }\n"
                "        ]\n"
                "    }))\n"
                "⦄"
            ),
            "pause_clause": (
                "{{pause_clause}}(_ctx) ⦃\n"
                "    (_needs_pause ≔ _ctx[needs_user_confirmation]) ⋀\n"
                "    (_needs_pause = true) ⋀\n"
                "    ↲({ paused: true, reason: user_interaction, prompt: \"Multiple valid matches found. Please confirm preferred allocation.\" })\n"
                "⦄"
            ),
        },
        "allowed_operations": allowed_operations,
        "intake_flow": [
            "Generate JSON from this pack",
            "Extract solf_script",
            "Run business_rules.review_solf_script_rule",
            "Persist with business_rules.create_business_rule_from_solf_script",
        ],
        "documentation": {
            "markdown_path": str(markdown_path),
            "markdown": markdown_content if include_markdown else None,
        },
    }


def process_solf_llm_generation_output(
    *,
    llm_output: dict[str, Any],
    persist: bool = True,
    created_by: str | None = "api:user",
    is_active: bool = True,
    default_clause_type: str = "resolve_policy",
) -> dict[str, Any]:
    """Validate and optionally persist LLM-generated SOLF artifacts in one call."""
    payload = llm_output if isinstance(llm_output, dict) else {}
    if not payload:
        raise ValueError("llm_output must be a JSON object")

    rule_name = str(payload.get("rule_name") or "").strip() or None
    solf_script = str(payload.get("solf_script") or "").strip()
    if not solf_script:
        raise ValueError("llm_output.solf_script is required")

    provided_classes = payload.get("class_definitions") if isinstance(payload.get("class_definitions"), list) else []

    review = review_solf_script_rule(
        solf_script=solf_script,
        rule_name=rule_name,
        default_clause_type=default_clause_type,
    )

    artifacts = review.get("artifacts") if isinstance(review.get("artifacts"), dict) else {}
    parsed_classes = artifacts.get("classes") if isinstance(artifacts.get("classes"), list) else []

    class_alignment = {
        "provided_count": len(provided_classes),
        "parsed_count": len(parsed_classes),
        "provided_class_names": sorted(
            {
                str(item.get("class_name") or "").strip()
                for item in provided_classes
                if isinstance(item, dict) and str(item.get("class_name") or "").strip()
            }
        ),
        "parsed_class_names": sorted(
            {
                str(item.get("class_name") or "").strip()
                for item in parsed_classes
                if isinstance(item, dict) and str(item.get("class_name") or "").strip()
            }
        ),
    }

    result: dict[str, Any] = {
        "accepted": bool(review.get("is_valid", False)),
        "persist_requested": bool(persist),
        "review": review,
        "class_alignment": class_alignment,
        "warnings": [],
    }

    if provided_classes and not parsed_classes:
        result["warnings"].append(
            "class_definitions were provided in JSON but no class blocks were detected in solf_script"
        )

    if not review.get("is_valid", False):
        result["persisted"] = False
        return result

    if not persist:
        result["persisted"] = False
        return result

    created = create_business_rule_from_solf_script(
        solf_script=solf_script,
        rule_name=rule_name,
        created_by=created_by,
        is_active=is_active,
        default_clause_type=default_clause_type,
    )
    result["persisted"] = True
    result["created"] = created
    return result


def generate_and_process_solf_from_inspection(
    *,
    goal: str,
    inspection_context: dict[str, Any] | None = None,
    persist: bool = True,
    created_by: str | None = "api:user",
    is_active: bool = True,
    default_clause_type: str = "resolve_policy",
    temperature: float = 0.0,
    workflow_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Generate SOLF JSON from a goal plus web/internal/external inspection context and persist it."""
    goal_text = str(goal or "").strip()
    if not goal_text:
        raise ValueError("goal is required")

    pack = get_solf_llm_generation_pack(include_markdown=False)
    client = _make_genai_client()
    if client is None:
        raise RuntimeError("LLM client is unavailable")

    context_payload = inspection_context if isinstance(inspection_context, dict) else {}
    prompt = (
        "Generate executable SOLF artifacts for IDMS from the supplied inspection context. "
        "Return only a JSON object with keys: rule_name, class_definitions, solf_script.\n"
        "Follow this generation pack strictly:\n"
        f"{json.dumps(pack, ensure_ascii=False)}\n\n"
        "Business goal:\n"
        f"{goal_text}\n\n"
        "Inspection context:\n"
        f"{json.dumps(context_payload, ensure_ascii=False)}\n"
    )

    response = generate_content_with_openrouter_fallback(
        primary_call=lambda: client.models.generate_content(
            model=EXTRACT_MODEL,
            contents=[prompt],
        ),
        model=EXTRACT_MODEL,
        contents=[prompt],
        temperature=float(temperature),
        call_name="solf_generation_pack_generate",
        complexity="medium",
    )

    raw_text = str(getattr(response, "text", "") or "")
    llm_output = _extract_json_object(raw_text)
    if not llm_output:
        raise ValueError("LLM did not return a valid JSON object for SOLF generation")

    processed = process_solf_llm_generation_output(
        llm_output=llm_output,
        persist=persist,
        created_by=created_by,
        is_active=is_active,
        default_clause_type=default_clause_type,
    )

    workflow_registry_result: dict[str, Any] | None = None
    if isinstance(workflow_registry, dict) and workflow_registry:
        workflow_payload = dict(workflow_registry)
        workflow_key = str(workflow_payload.get("workflow_key") or "").strip()
        workflow_name = str(workflow_payload.get("workflow_name") or "").strip()
        if workflow_key and workflow_name and bool(processed.get("persisted")):
            workflow_registry_result = create_solf_workflow_registry_entry(
                workflow_key=workflow_key,
                workflow_name=workflow_name,
                description=str(workflow_payload.get("description") or "") or None,
                domain=str(workflow_payload.get("domain") or "") or None,
                status=str(workflow_payload.get("status") or "draft") or "draft",
                metadata=workflow_payload.get("metadata") if isinstance(workflow_payload.get("metadata"), dict) else {},
                is_active=bool(workflow_payload.get("is_active", True)),
                created_by=str(workflow_payload.get("created_by") or created_by or "api:user") or None,
            )

    return {
        "model": EXTRACT_MODEL,
        "goal": goal_text,
        "inspection_context": context_payload,
        "llm_output": llm_output,
        "processed": processed,
        "workflow_registry": workflow_registry_result or workflow_registry,
    }


def generate_and_process_solf_from_intent(
    *,
    intent: str,
    persist: bool = True,
    created_by: str | None = "api:user",
    is_active: bool = True,
    default_clause_type: str = "resolve_policy",
    temperature: float = 0.0,
) -> dict[str, Any]:
    """Generate SOLF JSON from plain intent text and run validate/persist pipeline."""
    intent_text = str(intent or "").strip()
    if not intent_text:
        raise ValueError("intent is required")

    pack = get_solf_llm_generation_pack(include_markdown=False)
    client = _make_genai_client()
    if client is None:
        raise RuntimeError("LLM client is unavailable")

    prompt = (
        "Generate executable SOLF artifacts for IDMS. "
        "Return only a JSON object with keys: rule_name, class_definitions, solf_script.\n"
        "Follow this generation pack strictly:\n"
        f"{json.dumps(pack, ensure_ascii=False)}\n\n"
        "Business intent:\n"
        f"{intent_text}\n"
    )

    response = generate_content_with_openrouter_fallback(
        primary_call=lambda: client.models.generate_content(
            model=EXTRACT_MODEL,
            contents=[prompt],
        ),
        model=EXTRACT_MODEL,
        contents=[prompt],
        temperature=float(temperature),
        call_name="solf_generation_pack_generate",
        complexity="medium",
    )

    raw_text = str(getattr(response, "text", "") or "")
    llm_output = _extract_json_object(raw_text)
    if not llm_output:
        raise ValueError("LLM did not return a valid JSON object for SOLF generation")

    processed = process_solf_llm_generation_output(
        llm_output=llm_output,
        persist=persist,
        created_by=created_by,
        is_active=is_active,
        default_clause_type=default_clause_type,
    )

    return {
        "model": EXTRACT_MODEL,
        "intent": intent_text,
        "llm_output": llm_output,
        "processed": processed,
    }


def _derive_workflow_entries_from_structured_rule(
    structured_rule: dict[str, Any],
    fallback_rule_name: str,
) -> list[dict[str, Any]]:
    workflow_hints = structured_rule.get("workflow_hints") if isinstance(structured_rule.get("workflow_hints"), list) else []
    entries: list[dict[str, Any]] = []

    for index, hint in enumerate(workflow_hints, start=1):
        if not isinstance(hint, dict):
            continue
        process = str(hint.get("process") or "").strip()
        domain = str(hint.get("domain") or "").strip().lower() or None
        resolved = canonicalize_workflow_process(process, domain=domain)
        process_key = str(resolved.get("process") or _slug(process) or "").strip()
        if not process_key:
            continue
        workflow_key = f"workflow::{process_key}"
        workflow_name = str(resolved.get("display") or process or process_key).strip()
        entries.append(
            {
                "workflow_key": workflow_key,
                "workflow_name": workflow_name,
                "domain": domain,
                "metadata": {
                    "source": "business_rule_workflow_hint",
                    "process": process_key,
                    "control_flow": str(hint.get("control_flow") or "sequence").strip().lower() or "sequence",
                    "stop_on_error": bool(hint.get("stop_on_error", True)),
                    "hint_index": index,
                },
                "graph_spec": {
                    "process": process_key,
                    "control_flow": str(hint.get("control_flow") or "sequence").strip().lower() or "sequence",
                    "stop_on_error": bool(hint.get("stop_on_error", True)),
                },
            }
        )

    if entries:
        return entries

    fallback_key = _slug(fallback_rule_name) or "runtime_business_rule"
    return [
        {
            "workflow_key": f"rule::{fallback_key}",
            "workflow_name": str(fallback_rule_name or fallback_key),
            "domain": None,
            "metadata": {
                "source": "business_rule_fallback",
                "reason": "missing_workflow_hints",
            },
            "graph_spec": {
                "process": fallback_key,
                "control_flow": "selection",
                "stop_on_error": True,
            },
        }
    ]


def _build_steps_for_rule_registry(
    structured_rule: dict[str, Any],
    clause_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []
    order = 1

    def _append_accounting_runtime_steps() -> None:
        nonlocal order
        workflow_hints = structured_rule.get("workflow_hints") if isinstance(structured_rule.get("workflow_hints"), list) else []
        if not workflow_hints:
            return

        has_accounting_reconciliation_hint = False
        for hint in workflow_hints:
            if not isinstance(hint, dict):
                continue
            domain = _slug(str(hint.get("domain") or ""))
            resolved = canonicalize_workflow_process(str(hint.get("process") or ""), domain=domain or None)
            process = _slug(str(resolved.get("process") or hint.get("process") or ""))
            if domain == "accounting" or any(
                token in process
                for token in (
                    "reconcile",
                    "reconciliation",
                    "bank_statement",
                    "ledger",
                    "booking",
                    "invoice",
                    "payment",
                    "accounting",
                )
            ):
                has_accounting_reconciliation_hint = True
                break

        if not has_accounting_reconciliation_hint:
            return

        operations = [
            "db_validate_ledger_payload",
            "resolve_accounting_booking_company",
            "db_accounting_ingest",
        ]
        for operation in operations:
            steps.append(
                {
                    "step_order": order,
                    "step_key": f"py_{_slug(operation) or order}",
                    "step_kind": "python_binding",
                    "operation": f"workflow_domain_operation::{operation}",
                    "python_module": "workflow_pipeline_builtin_steps",
                    "python_function": "invoke_solf_action",
                    "config": {
                        "action": "workflow_domain_operation",
                        "payload": {
                            "operation": operation,
                        },
                        "save_as": f"{_slug(operation)}_result",
                        "fail_on_error": True,
                    },
                }
            )
            order += 1

    class_definitions = structured_rule.get("class_definitions") if isinstance(structured_rule.get("class_definitions"), list) else []
    for class_item in class_definitions:
        if not isinstance(class_item, dict):
            continue
        class_name = str(class_item.get("class_name") or "").strip().lower()
        if not class_name:
            continue
        steps.append(
            {
                "step_order": order,
                "step_key": f"class_generate_{class_name}",
                "step_kind": "class_generate",
                "output_class": class_name,
                "operation": "class_definition",
                "config": {
                    "attributes": class_item.get("attributes") if isinstance(class_item.get("attributes"), list) else [],
                    "parent_class_name": class_item.get("parent_class_name"),
                },
            }
        )
        order += 1

    _append_accounting_runtime_steps()

    for clause_index, clause in enumerate(clause_rows, start=1):
        clause_name = str(clause.get("clause_name") or f"rule_clause_{clause_index}").strip()
        steps.append(
            {
                "step_order": order,
                "step_key": f"clause_{_slug(clause_name) or clause_index}",
                "step_kind": "clause",
                "clause_id": clause.get("clause_id"),
                "clause_name": clause_name,
                "operation": str(clause.get("clause_type") or "resolve_policy"),
                "config": {
                    "entity_class": clause.get("entity_class"),
                    "source": "business_rule_runtime",
                },
            }
        )
        order += 1

    return steps


def _collect_signal_strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = str(value).strip().lower()
        return [text] if text else []
    if isinstance(value, dict):
        values: list[str] = []
        for key, item in value.items():
            values.extend(_collect_signal_strings(key))
            values.extend(_collect_signal_strings(item))
        return values
    if isinstance(value, (list, tuple, set)):
        values: list[str] = []
        for item in value:
            values.extend(_collect_signal_strings(item))
        return values
    text = str(value).strip().lower()
    return [text] if text else []


def _infer_matching_purpose(signal_blob: str) -> str:
    if any(token in signal_blob for token in ("entity_resolution", "dedupe", "duplicate", "vendor", "supplier_merge", "customer_match", "merge_candidates")):
        return "entity_resolution"
    if any(token in signal_blob for token in ("reconcile", "reconciliation", "bank_statement", "ledger", "booking", "invoice", "payment", "accounting")):
        return "transaction_match"
    return "workflow_match"


def _derive_workflow_resource_aliases(
    *,
    workflow_key: str,
    domain: str | None,
    workflow_metadata: dict[str, Any],
    graph_spec: dict[str, Any],
    steps: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized_key = _slug(str(workflow_key or "")) or "workflow"
    aliases: list[dict[str, Any]] = [
        {
            "resource_type": "scope",
            "alias_name": f"{normalized_key}::workflow_scope",
            "canonical_name": f"workflow:{normalized_key}",
            "metadata": {"source": "workflow_registry_sync", "workflow_key": workflow_key},
        }
    ]

    explicit_aliases: list[dict[str, Any]] = []
    for container in (
        workflow_metadata,
        graph_spec,
        *((step.get("config") if isinstance(step.get("config"), dict) else {}) for step in steps),
    ):
        if not isinstance(container, dict):
            continue
        raw_aliases = container.get("resource_aliases") if isinstance(container.get("resource_aliases"), list) else []
        for alias in raw_aliases:
            if isinstance(alias, dict):
                explicit_aliases.append(alias)

    signal_values = [workflow_key, domain or "", workflow_metadata, graph_spec, steps]
    signal_blob = " ".join(_collect_signal_strings(signal_values))
    has_matching_capability = any(
        token in signal_blob
        for token in (
            "match",
            "matching",
            "reconcile",
            "reconciliation",
            "bank_statement",
            "ledger",
            "invoice",
            "payment",
            "entity_resolution",
            "dedupe",
        )
    )

    if has_matching_capability:
        purpose = str(workflow_metadata.get("matching_purpose") or graph_spec.get("matching_purpose") or _infer_matching_purpose(signal_blob)).strip().lower()
        collection_name = str(workflow_metadata.get("matching_collection") or graph_spec.get("matching_collection") or "default_matching_collection").strip()
        aliases.extend(
            [
                {
                    "resource_type": "endpoint",
                    "alias_name": f"{normalized_key}::matching_query",
                    "canonical_name": "/api/matching/query",
                    "metadata": {"source": "workflow_registry_sync", "workflow_key": workflow_key},
                },
                {
                    "resource_type": "endpoint",
                    "alias_name": f"{normalized_key}::matching_candidates",
                    "canonical_name": "/api/matching/candidates",
                    "metadata": {"source": "workflow_registry_sync", "workflow_key": workflow_key},
                },
                {
                    "resource_type": "purpose",
                    "alias_name": f"{normalized_key}::matching_purpose",
                    "canonical_name": purpose,
                    "metadata": {"source": "workflow_registry_sync", "workflow_key": workflow_key},
                },
                {
                    "resource_type": "collection",
                    "alias_name": f"{normalized_key}::matching_collection",
                    "canonical_name": collection_name,
                    "metadata": {"source": "workflow_registry_sync", "workflow_key": workflow_key},
                },
            ]
        )

        if any(token in signal_blob for token in ("retry", "pending", "index", "queue")):
            aliases.append(
                {
                    "resource_type": "queue",
                    "alias_name": f"{normalized_key}::matching_pending_queue",
                    "canonical_name": "tx_match_index_pending",
                    "metadata": {"source": "workflow_registry_sync", "workflow_key": workflow_key},
                }
            )

    for alias in explicit_aliases:
        resource_type = str(alias.get("resource_type") or "").strip().lower()
        alias_name = str(alias.get("alias_name") or "").strip()
        canonical_name = str(alias.get("canonical_name") or "").strip()
        if not resource_type or not alias_name or not canonical_name:
            continue
        aliases.append(
            {
                "resource_type": resource_type,
                "alias_name": alias_name,
                "canonical_name": canonical_name,
                "metadata": alias.get("metadata") if isinstance(alias.get("metadata"), dict) else {"source": "workflow_registry_sync_explicit"},
            }
        )

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for alias in aliases:
        resource_type = str(alias.get("resource_type") or "").strip().lower()
        alias_name = str(alias.get("alias_name") or "").strip()
        canonical_name = str(alias.get("canonical_name") or "").strip()
        if not resource_type or not alias_name or not canonical_name:
            continue
        key = (resource_type, alias_name)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(alias)
    return deduped


def sync_business_rule_workflow_registry(
    rule_id: int,
    created_by: str | None = None,
) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        rule = object_db.get_business_rule_by_id(connection, rule_id=rule_id)
        if rule is None:
            return None

        structured_rule = rule.get("structured_rule") if isinstance(rule.get("structured_rule"), dict) else {}
        rule_name = str(rule.get("rule_name") or "runtime_business_rule")
        workflow_entries = _derive_workflow_entries_from_structured_rule(structured_rule, fallback_rule_name=rule_name)
        clause_rows = object_db.get_solf_clauses_for_business_rule(connection, rule_id=rule_id)
        steps = _build_steps_for_rule_registry(structured_rule, clause_rows)

        linked: list[dict[str, Any]] = []
        for entry in workflow_entries:
            entry_metadata = entry.get("metadata") if isinstance(entry.get("metadata"), dict) else {}
            graph_spec = entry.get("graph_spec") if isinstance(entry.get("graph_spec"), dict) else {}
            workflow_id = object_db.upsert_solf_workflow_registry(
                connection=connection,
                workflow_key=str(entry.get("workflow_key") or "").strip(),
                workflow_name=str(entry.get("workflow_name") or "").strip() or str(entry.get("workflow_key") or "").strip(),
                description=f"Workflow registry entry generated from business rule {rule_id}",
                domain=str(entry.get("domain") or "").strip() or None,
                status="review",
                metadata=entry_metadata,
                is_active=True,
                created_by=created_by,
            )

            workflow_version_id = object_db.create_solf_workflow_version(
                connection=connection,
                workflow_id=workflow_id,
                rule_id=int(rule_id),
                graph_spec=graph_spec,
                input_contract={"source": "business_rules", "rule_id": int(rule_id)},
                output_contract={"type": "solf_runtime_artifacts"},
                metadata={
                    "source": "business_rule_registry_sync",
                    "rule_name": rule_name,
                },
                is_active=True,
                created_by=created_by,
            )

            step_count = object_db.replace_solf_workflow_steps(
                connection=connection,
                workflow_version_id=workflow_version_id,
                steps=steps,
            )

            resource_aliases = _derive_workflow_resource_aliases(
                workflow_key=str(entry.get("workflow_key") or "").strip(),
                domain=str(entry.get("domain") or "").strip() or None,
                workflow_metadata=entry_metadata,
                graph_spec=graph_spec,
                steps=steps,
            )
            alias_count = object_db.replace_workflow_resource_aliases(
                connection=connection,
                workflow_id=int(workflow_id),
                workflow_version_id=int(workflow_version_id),
                aliases=resource_aliases,
                created_by=created_by,
            )

            link_id = object_db.upsert_business_rule_workflow_link(
                connection=connection,
                rule_id=int(rule_id),
                workflow_id=int(workflow_id),
                workflow_version_id=int(workflow_version_id),
                link_type="uses",
                metadata={
                    "source": "business_rule_registry_sync",
                    "step_count": int(step_count),
                },
                created_by=created_by,
            )

            linked.append(
                {
                    "link_id": int(link_id),
                    "workflow_id": int(workflow_id),
                    "workflow_version_id": int(workflow_version_id),
                    "step_count": int(step_count),
                    "resource_aliases": resource_aliases,
                    "resource_alias_count": int(alias_count),
                    "workflow_key": str(entry.get("workflow_key") or ""),
                }
            )

        connection.commit()

        return {
            "rule_id": int(rule_id),
            "rule_name": rule_name,
            "workflows_linked": linked,
            "workflows_linked_count": len(linked),
            "class_definitions_count": len(structured_rule.get("class_definitions") or []),
            "clauses_count": len(clause_rows),
        }
    finally:
        connection.close()


def get_business_rule_workflow_registry(rule_id: int) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        rule = object_db.get_business_rule_by_id(connection, rule_id=rule_id)
        if rule is None:
            return None

        links = object_db.list_business_rule_workflow_links(connection, rule_id=rule_id)
        enriched_links: list[dict[str, Any]] = []
        for link in links:
            workflow_version_id = link.get("workflow_version_id")
            steps = []
            aliases = []
            if workflow_version_id is not None:
                steps = object_db.list_solf_workflow_steps(connection, workflow_version_id=int(workflow_version_id))
                aliases = object_db.list_workflow_resource_aliases(connection, workflow_version_id=int(workflow_version_id))
            enriched = dict(link)
            enriched["steps"] = steps
            enriched["resource_aliases"] = aliases
            enriched_links.append(enriched)

        return {
            "rule_id": int(rule_id),
            "rule_name": str(rule.get("rule_name") or ""),
            "links": enriched_links,
            "count": len(enriched_links),
        }
    finally:
        connection.close()


def create_business_rule_with_workflow_registry(
    rule_text: str,
    rule_name: str | None = None,
    created_by: str | None = None,
    is_active: bool = True,
) -> dict[str, Any]:
    rule_result = create_business_rule(
        rule_text=rule_text,
        rule_name=rule_name,
        created_by=created_by,
        is_active=is_active,
    )
    rule_id = int(rule_result.get("rule_id") or 0)

    registry_result = sync_business_rule_workflow_registry(
        rule_id=rule_id,
        created_by=created_by,
    )

    return {
        "rule_creation": rule_result,
        "workflow_registry_sync": registry_result,
        "combined_status": "success",
    }


def list_workflow_versions(
    workflow_id: int,
    is_active: bool | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    connection = object_db.get_connection()
    try:
        workflow = object_db.get_solf_workflow_registry_by_id(connection, workflow_id=workflow_id)
        if workflow is None:
            return []

        versions = object_db.list_solf_workflow_versions(
            connection=connection,
            workflow_id=int(workflow_id),
            is_active=is_active,
            limit=limit,
        )
        out: list[dict[str, Any]] = []
        for row in versions:
            version = dict(row)
            workflow_version_id = int(version.get("workflow_version_id") or 0)
            steps = object_db.list_solf_workflow_steps(connection, workflow_version_id=workflow_version_id)
            version["steps"] = steps
            version["step_count"] = len(steps)
            metadata = version.get("metadata") if isinstance(version.get("metadata"), dict) else {}
            version["status"] = str(metadata.get("status") or ("published" if bool(version.get("is_active")) else "draft"))
            out.append(version)
        return out
    finally:
        connection.close()


def create_workflow_version(
    workflow_id: int,
    *,
    steps: list[dict[str, Any]],
    graph_spec: dict[str, Any] | None = None,
    input_contract: dict[str, Any] | None = None,
    output_contract: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    status: str = "draft",
    created_by: str | None = None,
) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        workflow = object_db.get_solf_workflow_registry_by_id(connection, workflow_id=workflow_id)
        if workflow is None:
            return None

        normalized_steps = [dict(item) for item in (steps or []) if isinstance(item, dict)]
        clause_resolution = _ensure_workflow_clauses_for_steps(
            connection,
            workflow_id=int(workflow_id),
            workflow_key=str(workflow.get("workflow_key") or "").strip(),
            workflow_name=str(workflow.get("workflow_name") or "").strip(),
            steps=normalized_steps,
            workflow_description=str(workflow.get("description") or "").strip() or None,
            created_by=created_by,
        )

        workflow_version_id = object_db.create_solf_workflow_version(
            connection=connection,
            workflow_id=int(workflow_id),
            rule_id=None,
            graph_spec=graph_spec if isinstance(graph_spec, dict) else {},
            input_contract=input_contract if isinstance(input_contract, dict) else {},
            output_contract=output_contract if isinstance(output_contract, dict) else {},
            metadata=metadata if isinstance(metadata, dict) else {},
            is_active=False,
            created_by=created_by,
        )

        step_count = object_db.replace_solf_workflow_steps(
            connection=connection,
            workflow_version_id=int(workflow_version_id),
            steps=normalized_steps,
        )

        normalized_status = str(status or "draft").strip().lower() or "draft"
        if normalized_status != "draft":
            object_db.set_solf_workflow_version_status(
                connection=connection,
                workflow_version_id=int(workflow_version_id),
                status=normalized_status,
            )

        versions = object_db.list_solf_workflow_versions(
            connection=connection,
            workflow_id=int(workflow_id),
            is_active=None,
            limit=500,
        )
        created_version = next(
            (item for item in versions if int(item.get("workflow_version_id") or 0) == int(workflow_version_id)),
            None,
        )
        steps_row = object_db.list_solf_workflow_steps(connection, workflow_version_id=int(workflow_version_id))

        return {
            "workflow_id": int(workflow_id),
            "workflow_key": workflow.get("workflow_key"),
            "workflow_version_id": int(workflow_version_id),
            "step_count": int(step_count),
            "clause_resolution": clause_resolution,
            "status": normalized_status,
            "version": created_version,
            "steps": steps_row,
        }
    finally:
        connection.close()


def publish_workflow_version(
    workflow_id: int,
    workflow_version_id: int,
    published_by: str | None = None,
) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        workflow = object_db.get_solf_workflow_registry_by_id(connection, workflow_id=workflow_id)
        if workflow is None:
            return None

        version_list = object_db.list_solf_workflow_versions(
            connection=connection,
            workflow_id=workflow_id,
            is_active=None,
            limit=1000,
        )
        target_version = next(
            (v for v in version_list if int(v.get("workflow_version_id") or 0) == int(workflow_version_id)),
            None,
        )
        if target_version is None:
            return None

        version_steps = object_db.list_solf_workflow_steps(connection, workflow_version_id=int(workflow_version_id))
        clause_resolution = _ensure_workflow_clauses_for_steps(
            connection,
            workflow_id=int(workflow_id),
            workflow_key=str(workflow.get("workflow_key") or "").strip(),
            workflow_name=str(workflow.get("workflow_name") or "").strip(),
            steps=[dict(item) for item in version_steps if isinstance(item, dict)],
            workflow_description=str(workflow.get("description") or "").strip() or None,
            created_by=published_by,
        )

        object_db.deactivate_workflow_versions_except(
            connection=connection,
            workflow_id=workflow_id,
            except_version_id=workflow_version_id,
        )

        object_db.set_solf_workflow_version_status(
            connection=connection,
            workflow_version_id=workflow_version_id,
            status="published",
        )

        updated_workflow = object_db.get_solf_workflow_registry_by_id(connection, workflow_id=workflow_id)
        active_versions = object_db.list_solf_workflow_versions(
            connection=connection,
            workflow_id=workflow_id,
            is_active=True,
            limit=1,
        )
        updated_version = active_versions[0] if active_versions and len(active_versions) > 0 else None
        aliases = object_db.list_workflow_resource_aliases(connection, workflow_version_id=int(workflow_version_id))

        connection.commit()

        return {
            "workflow_id": workflow_id,
            "workflow_key": updated_workflow.get("workflow_key") if updated_workflow else None,
            "published_version_id": workflow_version_id,
            "published_version_no": int(target_version.get("version_no") or 0),
            "status": "published",
            "clause_resolution": clause_resolution,
            "active_version": updated_version,
            "resource_aliases": aliases,
            "resource_alias_count": len(aliases),
            "published_by": str(published_by or ""),
        }
    finally:
        connection.close()


def rollback_workflow_version(
    workflow_id: int,
    target_version_id: int,
    reason: str | None = None,
    rolled_back_by: str | None = None,
) -> dict[str, Any] | None:
    connection = object_db.get_connection()
    try:
        workflow = object_db.get_solf_workflow_registry_by_id(connection, workflow_id=workflow_id)
        if workflow is None:
            return None

        version_list = object_db.list_solf_workflow_versions(
            connection=connection,
            workflow_id=workflow_id,
            is_active=None,
            limit=1000,
        )
        target_version = next(
            (v for v in version_list if int(v.get("workflow_version_id") or 0) == int(target_version_id)),
            None,
        )
        if target_version is None:
            return None

        object_db.deactivate_workflow_versions_except(
            connection=connection,
            workflow_id=workflow_id,
            except_version_id=target_version_id,
        )

        object_db.set_solf_workflow_version_status(
            connection=connection,
            workflow_version_id=target_version_id,
            status="published",
        )

        updated_workflow = object_db.get_solf_workflow_registry_by_id(connection, workflow_id=workflow_id)
        active_versions = object_db.list_solf_workflow_versions(
            connection=connection,
            workflow_id=workflow_id,
            is_active=True,
            limit=1,
        )
        updated_version = active_versions[0] if active_versions and len(active_versions) > 0 else None

        connection.commit()

        return {
            "workflow_id": workflow_id,
            "workflow_key": updated_workflow.get("workflow_key") if updated_workflow else None,
            "rolled_back_to_version_id": target_version_id,
            "rolled_back_to_version_no": int(target_version.get("version_no") or 0),
            "status": "published",
            "reason": str(reason or "manual_rollback"),
            "rolled_back_by": str(rolled_back_by or ""),
            "active_version": updated_version,
        }
    finally:
        connection.close()


def _extract_step_python_target(step: dict[str, Any]) -> tuple[str, str]:
    config = step.get("config") if isinstance(step.get("config"), dict) else {}
    python_module = str(step.get("python_module") or "").strip() or str(config.get("python_module") or "").strip()
    python_function = str(step.get("python_function") or "").strip() or str(config.get("builtin") or "").strip()
    if not python_module:
        python_module = "workflow_pipeline_builtin_steps"
    return python_module, python_function


def _validate_step_capability(step: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    step_key = str(step.get("step_key") or "").strip() or f"step_{int(step.get('step_order') or 0)}"
    step_kind = str(step.get("step_kind") or "").strip().lower()
    issues: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    supported_step_kinds = {"clause", "python_binding", "class_generate", "class_transform", "class_iterate"}
    if step_kind not in supported_step_kinds:
        issues.append(
            {
                "code": "unsupported_step_kind",
                "step_key": step_key,
                "detail": f"step_kind '{step_kind}' is not supported by runtime executor",
                "suggested_fix": "Use clause/python_binding or add executor support for this step kind.",
            }
        )
        return issues, warnings

    if step_kind in {"class_generate", "class_transform", "class_iterate"}:
        warnings.append(
            {
                "code": "step_kind_noop_runtime",
                "step_key": step_key,
                "detail": f"step_kind '{step_kind}' currently executes as a no-op in workflow executor",
                "suggested_fix": "Implement runtime behavior in workflow_pipeline_executor for production use.",
            }
        )
        return issues, warnings

    if step_kind == "clause":
        clause_name = str(step.get("clause_name") or "").strip()
        if not clause_name:
            issues.append(
                {
                    "code": "missing_clause_name",
                    "step_key": step_key,
                    "detail": "clause step requires clause_name",
                    "suggested_fix": "Set step.clause_name to a valid SOLF clause.",
                }
            )
        return issues, warnings

    python_module, python_function = _extract_step_python_target(step)
    if not python_function:
        issues.append(
            {
                "code": "missing_python_function",
                "step_key": step_key,
                "detail": "python_binding step missing python_function/config.builtin",
                "suggested_fix": "Set python_function or config.builtin.",
            }
        )
        return issues, warnings

    try:
        mod = importlib.import_module(python_module)
    except Exception as exc:
        issues.append(
            {
                "code": "python_module_import_failed",
                "step_key": step_key,
                "detail": f"cannot import module '{python_module}': {exc}",
                "suggested_fix": "Use importable runtime module or deploy missing module.",
            }
        )
        return issues, warnings

    fn = getattr(mod, python_function, None)
    if fn is None or not callable(fn):
        issues.append(
            {
                "code": "python_function_not_found",
                "step_key": step_key,
                "detail": f"function '{python_function}' not found in module '{python_module}'",
                "suggested_fix": "Use a callable runtime function.",
            }
        )
        return issues, warnings

    if python_module == "workflow_pipeline_builtin_steps" and python_function == "invoke_solf_action":
        config = step.get("config") if isinstance(step.get("config"), dict) else {}
        action = str(config.get("action") or "").strip()
        if not action:
            issues.append(
                {
                    "code": "missing_solf_action",
                    "step_key": step_key,
                    "detail": "invoke_solf_action requires config.action",
                    "suggested_fix": "Set config.action to a callable in solf_function.py.",
                }
            )
            return issues, warnings

        try:
            import solf_function

            action_fn = getattr(solf_function, action, None)
            if action_fn is None or not callable(action_fn):
                issues.append(
                    {
                        "code": "unsupported_solf_action",
                        "step_key": step_key,
                        "detail": f"SOLF action '{action}' is not available in solf_function.py",
                        "suggested_fix": "Implement this action in solf_function.py or change config.action.",
                    }
                )
        except Exception as exc:
            issues.append(
                {
                    "code": "solf_function_import_failed",
                    "step_key": step_key,
                    "detail": f"cannot import solf_function while validating action '{action}': {exc}",
                    "suggested_fix": "Ensure solf_function is importable in runtime.",
                }
            )

    return issues, warnings


def _build_workflow_issue_template(
    *,
    workflow_name: str,
    workflow_key: str,
    workflow_id: int | None,
    workflow_version_id: int | None,
    missing_capabilities: list[dict[str, Any]],
    warning_capabilities: list[dict[str, Any]],
    steps: list[dict[str, Any]],
    requested_by: str,
) -> dict[str, Any]:
    safe_workflow = _slug(workflow_key or workflow_name) or "workflow"
    title = f"IDMS capability gap: {workflow_name or safe_workflow}"

    acceptance_criteria = [
        "Workflow validates as supported using capability validation endpoint.",
        "All missing capabilities are implemented or mapped to supported alternatives.",
        "Regression tests cover new runtime capabilities and failure cases.",
        "Workflow publish path remains backward compatible.",
    ]

    issue_json = {
        "title": title,
        "labels": ["idms", "workflow", "capability-gap", "enhancement"],
        "workflow": {
            "workflow_name": workflow_name,
            "workflow_key": workflow_key,
            "workflow_id": workflow_id,
            "workflow_version_id": workflow_version_id,
            "step_count": len(steps),
        },
        "reported_by": requested_by,
        "summary": "User-defined workflow requests capabilities not currently implemented by IDMS runtime.",
        "missing_capabilities": missing_capabilities,
        "warnings": warning_capabilities,
        "proposed_developer_actions": [
            "Add missing runtime step/function support.",
            "Add or extend required solf_function actions.",
            "Update capability validation tests and docs.",
        ],
        "acceptance_criteria": acceptance_criteria,
    }

    lines = [
        f"# {title}",
        "",
        "## Summary",
        issue_json["summary"],
        "",
        "## Workflow Context",
        f"- workflow_name: {workflow_name}",
        f"- workflow_key: {workflow_key}",
        f"- workflow_id: {workflow_id}",
        f"- workflow_version_id: {workflow_version_id}",
        f"- step_count: {len(steps)}",
        f"- reported_by: {requested_by}",
        "",
        "## Missing Capabilities",
    ]
    if missing_capabilities:
        for item in missing_capabilities:
            lines.append(f"- [{item.get('code')}] step={item.get('step_key')} detail={item.get('detail')}")
            lines.append(f"  - suggested_fix: {item.get('suggested_fix')}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("## Warnings")
    if warning_capabilities:
        for item in warning_capabilities:
            lines.append(f"- [{item.get('code')}] step={item.get('step_key')} detail={item.get('detail')}")
            lines.append(f"  - suggested_fix: {item.get('suggested_fix')}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("## Acceptance Criteria")
    for criterion in acceptance_criteria:
        lines.append(f"- {criterion}")

    markdown = "\n".join(lines)
    issue_json["markdown"] = markdown
    issue_json["github_issue"] = {
        "title": title,
        "body": markdown,
        "labels": issue_json["labels"],
        "assignees": [],
        "milestone": None,
        "projects": [],
    }
    return issue_json


def validate_workflow_implementation_capability(
    *,
    workflow_key: str | None = None,
    workflow_id: int | None = None,
    workflow_version_id: int | None = None,
    proposed_steps: list[dict[str, Any]] | None = None,
    workflow_name: str | None = None,
    requested_by: str | None = None,
) -> dict[str, Any]:
    normalized_workflow_key = str(workflow_key or "").strip()
    normalized_workflow_name = str(workflow_name or normalized_workflow_key or "workflow").strip()
    normalized_requested_by = str(requested_by or "api:user").strip() or "api:user"

    resolved_workflow_id = workflow_id
    resolved_version_id = workflow_version_id
    resolved_workflow_domain: str | None = None
    resolved_steps: list[dict[str, Any]] = [item for item in (proposed_steps or []) if isinstance(item, dict)]

    if not resolved_steps:
        connection = object_db.get_connection()
        try:
            if resolved_workflow_id is None and normalized_workflow_key:
                workflow = object_db.get_solf_workflow_registry_by_key(connection, workflow_key=normalized_workflow_key)
                if workflow is not None:
                    resolved_workflow_id = int(workflow.get("workflow_id") or 0)
                    resolved_workflow_domain = str(workflow.get("domain") or "").strip() or None
                    if not workflow_name:
                        normalized_workflow_name = str(workflow.get("workflow_name") or normalized_workflow_name)

            if resolved_workflow_id is not None and int(resolved_workflow_id) > 0:
                if resolved_version_id is None:
                    active_version = object_db.get_workflow_active_version(connection, workflow_id=int(resolved_workflow_id))
                    if active_version is not None:
                        resolved_version_id = int(active_version.get("workflow_version_id") or 0)
                if resolved_version_id is not None and int(resolved_version_id) > 0:
                    resolved_steps = object_db.list_solf_workflow_steps(connection, workflow_version_id=int(resolved_version_id))
        finally:
            connection.close()

    missing_capabilities: list[dict[str, Any]] = []
    warning_capabilities: list[dict[str, Any]] = []
    if not resolved_steps:
        missing_capabilities.append(
            {
                "code": "missing_workflow_steps",
                "step_key": "workflow",
                "detail": "No workflow steps provided or found for capability validation.",
                "suggested_fix": "Provide proposed_steps or create/sync workflow version before validation.",
            }
        )
    else:
        for step in resolved_steps:
            issues, warnings = _validate_step_capability(step)
            missing_capabilities.extend(issues)
            warning_capabilities.extend(warnings)

    supported = len(missing_capabilities) == 0
    issue_payload = _build_workflow_issue_template(
        workflow_name=normalized_workflow_name,
        workflow_key=normalized_workflow_key,
        workflow_id=resolved_workflow_id,
        workflow_version_id=resolved_version_id,
        missing_capabilities=missing_capabilities,
        warning_capabilities=warning_capabilities,
        steps=resolved_steps,
        requested_by=normalized_requested_by,
    )

    user_message = (
        "Workflow is supported by current IDMS runtime capabilities."
        if supported
        else "Workflow requires unsupported capabilities. Please pass the generated issue template to developers."
    )

    return {
        "supported": supported,
        "workflow_name": normalized_workflow_name,
        "workflow_key": normalized_workflow_key,
        "workflow_id": resolved_workflow_id,
        "workflow_version_id": resolved_version_id,
        "workflow_domain": resolved_workflow_domain,
        "step_count": len(resolved_steps),
        "missing_capabilities": missing_capabilities,
        "warnings": warning_capabilities,
        "developer_issue_template": issue_payload,
        "user_message": user_message,
    }
