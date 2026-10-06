from __future__ import annotations

import csv
import io
import json
import logging
import mimetypes
import re
from datetime import date
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

import document_generation
import document_template_pipeline
import object_db
import business_rules as business_rules_engine
from ikos_config import CLASSIFY_MODEL
from llm_fallback import generate_content_with_openrouter_fallback, get_openrouter_client
from workflow_pipeline_executor import WorkflowPipelineExecutor
from ikos_api_server.deps import parse_json_dict, require_permission
from ikos_api_server.schemas import (
    DocumentExportSaveRequest,
    TemplateReferenceGenerateRequest,
    InvoiceFromReferenceGenerateRequest,
    InvoiceFromInstructionGenerateRequest,
    SwissReferenceValidateRequest,
    SalesPipelineGenerateRequest,
    SalesPipelineStageSyncRequest,
    SalesPipelineStageBulkSyncRequest,
    SalesPipelineStageBulkSyncItem,
)
from swiss_qr_reference import SwissQRReferenceGenerator


router = APIRouter(prefix="/api/documents", tags=["documents"], dependencies=[Depends(require_permission("documents.read"))])
LOGGER = logging.getLogger("idms.api")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_REFERENCE_DIR = (PROJECT_ROOT / "generated" / "template_references").resolve()
TEMPLATE_REFERENCE_REGISTRY = (TEMPLATE_REFERENCE_DIR / "registry.json").resolve()


class TemplateReferencePdfAnalysisRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict, description="Optional payload/sample values to use for field rewrite inference")
    document_kind: str = Field(default="", description="Optional document kind override")
    include_llm_suggestions: bool = Field(default=True, description="Include LLM-ranked label suggestions for workflow overwrite fields")


class TemplateReferenceApplyFieldLabelsRequest(BaseModel):
    field_labels: dict[str, Any] = Field(default_factory=dict, description="Explicit field->label overrides to persist")
    suggestions: list[dict[str, Any]] = Field(default_factory=list, description="LLM/heuristic suggestions with field/chosen_label/confidence")
    min_confidence: float = Field(default=0.5, ge=0.0, le=1.0, description="Minimum confidence threshold for suggestion acceptance")
    only_workflow_targets: bool = Field(default=True, description="Apply only fields that are workflow-derived overwrite targets")


def _slugify(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(value or "").strip().lower())
    text = text.strip("-._")
    return text or "template-reference"


def _extract_explicit_output_filename(payload: dict[str, Any]) -> str:
    for key in ["output_filename", "filename", "file_name", "document_filename", "generated_filename", "output_name"]:
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def _normalize_generation_date_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return date.today().isoformat()
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if match:
        return match.group(0)
    return date.today().isoformat()


def _resolve_generated_filename(reference_name: str, payload: dict[str, Any], ext: str) -> str:
    explicit = _extract_explicit_output_filename(payload)
    if explicit:
        stem = Path(explicit).stem or explicit
    else:
        generation_date = _normalize_generation_date_token(
            payload.get("generation_date")
            or payload.get("issue_date")
            or payload.get("date")
        )
        stem = f"{reference_name}-{generation_date}"
    return f"{_slugify(stem)}.{str(ext or '').strip().lstrip('.') or 'pdf'}"


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_template_linked_workflow(workflow_key_raw: str, workflow_id_raw: str) -> dict[str, Any]:
    workflow_key = str(workflow_key_raw or "").strip()
    workflow_id_text = str(workflow_id_raw or "").strip()

    if not workflow_key and not workflow_id_text:
        return {}

    resolved_by_id: dict[str, Any] | None = None
    resolved_by_key: dict[str, Any] | None = None

    if workflow_id_text:
        try:
            workflow_id = int(workflow_id_text)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="workflow_id must be an integer") from exc
        if workflow_id <= 0:
            raise HTTPException(status_code=400, detail="workflow_id must be >= 1")

        resolved_by_id = business_rules_engine.get_solf_workflow_registry_entry(workflow_id=workflow_id)
        if not resolved_by_id:
            raise HTTPException(status_code=400, detail=f"workflow_id '{workflow_id}' not found in workflow registry")

    if workflow_key:
        resolved_by_key = business_rules_engine.get_solf_workflow_registry_entry_by_key(workflow_key=workflow_key)
        if not resolved_by_key:
            raise HTTPException(status_code=400, detail=f"workflow_key '{workflow_key}' not found in workflow registry")

    workflow = resolved_by_key or resolved_by_id
    if resolved_by_id and resolved_by_key:
        id_from_id = int(resolved_by_id.get("workflow_id") or 0)
        id_from_key = int(resolved_by_key.get("workflow_id") or 0)
        if id_from_id != id_from_key:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"workflow_id '{id_from_id}' and workflow_key '{workflow_key}' resolve to different workflows"
                ),
            )
        workflow = resolved_by_id

    if not workflow:
        return {}

    if not bool(workflow.get("is_active", True)):
        wf_id = int(workflow.get("workflow_id") or 0)
        raise HTTPException(status_code=400, detail=f"workflow_id '{wf_id}' is inactive")

    workflow_id = int(workflow.get("workflow_id") or 0)
    workflow_key_resolved = str(workflow.get("workflow_key") or "").strip()
    if not workflow_id or not workflow_key_resolved:
        raise HTTPException(status_code=400, detail="resolved workflow entry is missing workflow_id/workflow_key")

    active_version = business_rules_engine.get_solf_workflow_active_version_by_key(workflow_key=workflow_key_resolved)
    if not active_version or not bool(active_version.get("has_active_version")):
        raise HTTPException(
            status_code=400,
            detail=(
                f"workflow_key '{workflow_key_resolved}' has no active published version; "
                "publish/activate a workflow version before linking template ingestion"
            ),
        )

    workflow_version_id = int(active_version.get("workflow_version_id") or 0)
    if workflow_version_id <= 0:
        raise HTTPException(status_code=400, detail=f"workflow_key '{workflow_key_resolved}' has invalid active version")

    return {
        "linked_workflow_id": workflow_id,
        "linked_workflow_key": workflow_key_resolved,
        "linked_workflow_version_id": workflow_version_id,
        "linked_workflow_name": str(workflow.get("workflow_name") or "").strip() or None,
    }


def _validate_template_linked_workflow_for_generation(entry: dict[str, Any]) -> dict[str, Any] | None:
    workflow_key = str(entry.get("linked_workflow_key") or "").strip()
    workflow_id_raw = entry.get("linked_workflow_id")
    workflow_id = 0
    if isinstance(workflow_id_raw, (int, float, str)) and str(workflow_id_raw).strip():
        try:
            workflow_id = int(str(workflow_id_raw).strip())
        except ValueError:
            workflow_id = 0

    if not workflow_key and workflow_id <= 0:
        return None

    workflow: dict[str, Any] | None = None
    if workflow_key:
        workflow = business_rules_engine.get_solf_workflow_registry_entry_by_key(workflow_key=workflow_key)
    elif workflow_id > 0:
        workflow = business_rules_engine.get_solf_workflow_registry_entry(workflow_id=workflow_id)

    if not workflow:
        target = workflow_key or str(workflow_id)
        raise HTTPException(status_code=409, detail=f"Linked workflow '{target}' no longer exists in registry")

    if not bool(workflow.get("is_active", True)):
        wf_id = int(workflow.get("workflow_id") or workflow_id or 0)
        raise HTTPException(status_code=409, detail=f"Linked workflow '{wf_id}' is inactive")

    resolved_key = str(workflow.get("workflow_key") or workflow_key).strip()
    active_version = business_rules_engine.get_solf_workflow_active_version_by_key(workflow_key=resolved_key)
    if not active_version or not bool(active_version.get("has_active_version")):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Linked workflow '{resolved_key}' has no active published version. "
                "Publish/activate a workflow version before generation."
            ),
        )

    return {
        "linked_workflow_id": int(workflow.get("workflow_id") or 0),
        "linked_workflow_key": resolved_key,
        "linked_workflow_name": str(workflow.get("workflow_name") or "").strip() or None,
        "linked_workflow_version_id": int(active_version.get("workflow_version_id") or 0),
    }


def _run_template_generation_workflow(
    linked_workflow: dict[str, Any] | None,
    *,
    reference_name: str,
    document_kind: str,
    specification: str,
    payload: dict[str, Any],
    qr_data: dict[str, Any],
    request: InvoiceFromReferenceGenerateRequest,
) -> dict[str, Any]:
    if not linked_workflow:
        return {
            "payload": dict(payload),
            "qr_data": dict(qr_data),
            "reference_policy": {},
            "workflow_run": None,
        }

    workflow_version_id = int(linked_workflow.get("linked_workflow_version_id") or 0)
    if workflow_version_id <= 0:
        raise HTTPException(status_code=409, detail="Linked workflow has no executable active version")

    executor = WorkflowPipelineExecutor(db_connection_fn=object_db.get_connection, solf_interpreter=None)
    input_context = {
        "reference_name": reference_name,
        "document_kind": document_kind,
        "specification": specification,
        "payload": dict(payload),
        "qr_data": dict(qr_data),
        "service_period": payload.get("service_period"),
        "generation_date": date.today().isoformat(),
        "reference_mode": request.reference_mode,
        "current_reference": request.current_reference,
        "current_sequence": request.current_sequence,
        "sequence_step": request.sequence_step,
        "reference_prefix": request.reference_prefix,
        "sequence_value": request.sequence_value,
        "persist_reference_state": request.persist_reference_state,
    }
    run = executor.start_pipeline_run(
        workflow_version_id=workflow_version_id,
        input_context=input_context,
        started_by="api:template_generation",
        workflow_key=str(linked_workflow.get("linked_workflow_key") or "").strip() or None,
    )

    if isinstance(run, dict) and run.get("error"):
        raise HTTPException(status_code=400, detail=str(run.get("message") or run.get("error")))

    run_status = str((run or {}).get("run_status") or "").strip().lower()
    if run_status == "paused":
        raise HTTPException(
            status_code=409,
            detail={
                "workflow_paused": True,
                "workflow_key": linked_workflow.get("linked_workflow_key"),
                "run_id": run.get("run_id"),
                "paused_at_step_key": run.get("paused_at_step_key"),
                "message": "Linked workflow requires clarification before invoice generation can continue.",
            },
        )
    if run_status == "failed":
        raise HTTPException(
            status_code=409,
            detail={
                "workflow_failed": True,
                "workflow_key": linked_workflow.get("linked_workflow_key"),
                "run_id": run.get("run_id"),
                "message": str(run.get("error_message") or "Linked workflow execution failed"),
            },
        )

    output_context = dict(run.get("output_context") or run.get("current_context") or {})
    payload_out = dict(output_context.get("payload") or payload) if isinstance(output_context.get("payload") or payload, dict) else dict(payload)
    qr_out = dict(output_context.get("qr_data") or qr_data) if isinstance(output_context.get("qr_data") or qr_data, dict) else dict(qr_data)
    reference_policy = dict(output_context.get("reference_policy") or {}) if isinstance(output_context.get("reference_policy"), dict) else {}
    return {
        "payload": payload_out,
        "qr_data": qr_out,
        "reference_policy": reference_policy,
        "workflow_run": {
            "run_id": run.get("run_id"),
            "run_status": run.get("run_status"),
            "workflow_key": linked_workflow.get("linked_workflow_key"),
            "workflow_version_id": workflow_version_id,
        },
    }


def _normalized_override_fields(fields: list[str] | None) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in fields or []:
        field = str(raw or "").strip()
        if not field or field in seen:
            continue
        seen.add(field)
        result.append(field)
    return result


def _get_nested_value(payload: dict[str, Any], path: str) -> tuple[bool, Any]:
    if "." not in path:
        return (path in payload, payload.get(path))

    current: Any = payload
    for segment in path.split("."):
        key = str(segment or "").strip()
        if not key or not isinstance(current, dict) or key not in current:
            return (False, None)
        current = current.get(key)
    return (True, current)


def _set_nested_value(payload: dict[str, Any], path: str, value: Any) -> None:
    if "." not in path:
        payload[path] = value
        return

    parts = [str(segment or "").strip() for segment in path.split(".") if str(segment or "").strip()]
    if not parts:
        return

    current: dict[str, Any] = payload
    for key in parts[:-1]:
        existing = current.get(key)
        if not isinstance(existing, dict):
            existing = {}
            current[key] = existing
        current = existing
    current[parts[-1]] = value


def _merge_payload_with_override_fields(
    base_payload: dict[str, Any],
    override_payload: dict[str, Any],
    override_fields: list[str] | None,
) -> dict[str, Any]:
    merged = dict(base_payload or {})
    if not isinstance(override_payload, dict) or not override_payload:
        return merged

    fields = _normalized_override_fields(override_fields)
    if not fields:
        merged.update(override_payload)
        return merged

    for field in fields:
        found, value = _get_nested_value(override_payload, field)
        if not found:
            continue
        _set_nested_value(merged, field, value)
    return merged


def _flatten_payload_paths(payload: dict[str, Any], prefix: str = "") -> list[str]:
    paths: list[str] = []
    for key, value in (payload or {}).items():
        key_text = str(key or "").strip()
        if not key_text:
            continue
        path = f"{prefix}.{key_text}" if prefix else key_text
        paths.append(path)
        if isinstance(value, dict):
            paths.extend(_flatten_payload_paths(value, path))
    return paths


def _extract_line_item_columns(payload: dict[str, Any]) -> list[str]:
    line_items = payload.get("line_items") if isinstance(payload.get("line_items"), list) else []
    if not line_items:
        return []
    first = line_items[0] if isinstance(line_items[0], dict) else {}
    if not isinstance(first, dict):
        return []
    columns: list[str] = []
    for key in first.keys():
        key_text = str(key or "").strip()
        if key_text:
            columns.append(key_text)
    return columns


def _extract_linked_workflow_schema_fields_from_steps(steps: list[dict[str, Any]]) -> list[str]:
    fields: set[str] = set()
    for step in steps or []:
        if not isinstance(step, dict):
            continue
        config = step.get("config") if isinstance(step.get("config"), dict) else {}
        reference_policy = config.get("reference_policy") if isinstance(config.get("reference_policy"), dict) else {}
        reference_policy = config.get("reference_policy") if isinstance(config.get("reference_policy"), dict) else {}

        required_context_fields = config.get("required_context_fields")
        if isinstance(required_context_fields, list):
            for item in required_context_fields:
                name = str(item or "").strip()
                if name:
                    fields.add(name)

        mappings = config.get("context_to_payload_fields")
        if isinstance(mappings, dict):
            for payload_field in mappings.values():
                name = str(payload_field or "").strip()
                if name:
                    fields.add(name)
        elif isinstance(mappings, list):
            for mapping in mappings:
                if not isinstance(mapping, dict):
                    continue
                name = str(mapping.get("payload_field") or "").strip()
                if name:
                    fields.add(name)

        date_payload_fields = config.get("date_payload_fields")
        if isinstance(date_payload_fields, list):
            for item in date_payload_fields:
                name = str(item or "").strip()
                if name:
                    fields.add(name)

        derived_reference_fields = reference_policy.get("derived_reference_fields") if isinstance(reference_policy, dict) else None
        if isinstance(derived_reference_fields, list):
            for item in derived_reference_fields:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("field") or "").strip()
                if name:
                    fields.add(name)

    return sorted(fields)


def _extract_workflow_overwrite_targets_from_steps(steps: list[dict[str, Any]]) -> dict[str, Any]:
    targets: dict[str, dict[str, Any]] = {}

    def _touch(field_name: str, *, source: str, step_key: str, context_field: str | None = None) -> None:
        name = str(field_name or "").strip()
        if not name:
            return
        item = targets.setdefault(name, {"sources": set(), "step_keys": set(), "context_fields": set()})
        item["sources"].add(source)
        if step_key:
            item["step_keys"].add(step_key)
        if context_field:
            item["context_fields"].add(context_field)

    for step in steps or []:
        if not isinstance(step, dict):
            continue
        config = step.get("config") if isinstance(step.get("config"), dict) else {}
        reference_policy = config.get("reference_policy") if isinstance(config.get("reference_policy"), dict) else {}
        step_key = str(step.get("step_key") or step.get("step_name") or "").strip()

        required_context_fields = config.get("required_context_fields")
        if isinstance(required_context_fields, list):
            for item in required_context_fields:
                _touch(str(item or "").strip(), source="required_context_fields", step_key=step_key)

        mappings = config.get("context_to_payload_fields")
        if isinstance(mappings, dict):
            for payload_field, context_field in mappings.items():
                payload_name = str(payload_field or "").strip()
                context_name = str(context_field or "").strip() or None
                _touch(payload_name, source="context_to_payload_fields", step_key=step_key, context_field=context_name)
                if context_name:
                    _touch(context_name, source="context_to_payload_fields_context", step_key=step_key)
        elif isinstance(mappings, list):
            for mapping in mappings:
                if not isinstance(mapping, dict):
                    continue
                payload_name = str(mapping.get("payload_field") or "").strip()
                context_name = str(mapping.get("context_field") or "").strip() or None
                _touch(payload_name, source="context_to_payload_fields", step_key=step_key, context_field=context_name)
                if context_name:
                    _touch(context_name, source="context_to_payload_fields_context", step_key=step_key)

        date_payload_fields = config.get("date_payload_fields")
        if isinstance(date_payload_fields, list):
            for item in date_payload_fields:
                _touch(str(item or "").strip(), source="date_payload_fields", step_key=step_key)

        payload_updates = config.get("payload_updates")
        if isinstance(payload_updates, dict):
            for item in payload_updates.keys():
                _touch(str(item or "").strip(), source="payload_updates", step_key=step_key)

        derived_reference_fields = reference_policy.get("derived_reference_fields") if isinstance(reference_policy, dict) else None
        if isinstance(derived_reference_fields, list):
            for item in derived_reference_fields:
                if not isinstance(item, dict):
                    continue
                _touch(str(item.get("field") or "").strip(), source="derived_reference_fields", step_key=step_key)

    details: list[dict[str, Any]] = []
    for field_name in sorted(targets.keys()):
        item = targets[field_name]
        details.append(
            {
                "field": field_name,
                "sources": sorted(item["sources"]),
                "step_keys": sorted(item["step_keys"]),
                "context_fields": sorted(item["context_fields"]),
            }
        )

    return {
        "fields": [item["field"] for item in details],
        "details": details,
    }


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}

    fenced = re.search(r"```(?:json)?\s*(\{[\s\S]*\})\s*```", raw, flags=re.IGNORECASE)
    if fenced:
        raw = fenced.group(1).strip()

    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    first = raw.find("{")
    last = raw.rfind("}")
    if first >= 0 and last > first:
        try:
            parsed = json.loads(raw[first:last + 1])
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _build_pdf_label_candidates(spans: list[dict[str, Any]]) -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()
    for span in spans:
        text = str(span.get("text") or "").strip()
        if not text:
            continue
        if len(text) > 120:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        labels.append(text)
    return labels


def _normalize_label_values(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            text = str(item or "").strip()
            if text and text not in out:
                out.append(text)
        return out
    return []


def _merge_field_label_values(existing: Any, additions: list[str]) -> Any:
    merged = _normalize_label_values(existing)
    for label in additions:
        text = str(label or "").strip()
        if text and text not in merged:
            merged.append(text)
    if not merged:
        return existing
    if len(merged) == 1:
        return merged[0]
    return merged


def _build_field_labels_to_apply(
    payload: TemplateReferenceApplyFieldLabelsRequest,
    *,
    allowed_workflow_fields: set[str],
) -> tuple[dict[str, list[str]], list[str]]:
    apply_map: dict[str, list[str]] = {}
    skipped: list[str] = []

    def _allow(field_name: str) -> bool:
        if not payload.only_workflow_targets:
            return True
        return field_name in allowed_workflow_fields

    explicit = payload.field_labels if isinstance(payload.field_labels, dict) else {}
    for raw_field, raw_value in explicit.items():
        field = str(raw_field or "").strip()
        if not field:
            continue
        if not _allow(field):
            skipped.append(field)
            continue
        values = _normalize_label_values(raw_value)
        if values:
            apply_map[field] = values

    for item in payload.suggestions or []:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or "").strip()
        if not field:
            continue
        if not _allow(field):
            skipped.append(field)
            continue
        try:
            confidence = float(item.get("confidence") or 0.0)
        except Exception:
            confidence = 0.0
        if confidence < float(payload.min_confidence):
            continue
        chosen_label = str(item.get("chosen_label") or "").strip()
        candidates_raw = item.get("label_candidates") if isinstance(item.get("label_candidates"), list) else []
        candidates = [str(value or "").strip() for value in candidates_raw if str(value or "").strip()]
        values = [chosen_label] if chosen_label else candidates[:1]
        if not values:
            continue
        current = apply_map.get(field, [])
        for val in values:
            if val not in current:
                current.append(val)
        apply_map[field] = current

    return apply_map, sorted(set(skipped))


def _heuristic_overwrite_label_suggestions(
    overwrite_fields: list[str],
    existing_field_labels: dict[str, Any],
    candidate_labels: list[str],
) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    normalized_candidates = [str(item or "").strip() for item in candidate_labels if str(item or "").strip()]

    for field in overwrite_fields:
        field_name = str(field or "").strip()
        if not field_name:
            continue
        configured = existing_field_labels.get(field_name) if isinstance(existing_field_labels, dict) else None
        configured_list = [str(item).strip() for item in ([configured] if isinstance(configured, str) else (configured if isinstance(configured, list) else [])) if str(item).strip()]
        if configured_list:
            suggestions.append(
                {
                    "field": field_name,
                    "chosen_label": configured_list[0],
                    "label_candidates": configured_list,
                    "confidence": 0.98,
                    "reason": "existing_field_rule",
                }
            )
            continue

        tokens = [token for token in re.split(r"[^a-z0-9]+", field_name.lower()) if token]
        best_label = ""
        best_score = 0.0
        for label in normalized_candidates:
            lower = label.lower()
            score = 0.0
            for token in tokens:
                if token and token in lower:
                    score += 1.0
            if tokens:
                score = score / float(len(tokens))
            if score > best_score:
                best_score = score
                best_label = label

        if best_label and best_score >= 0.5:
            suggestions.append(
                {
                    "field": field_name,
                    "chosen_label": best_label,
                    "label_candidates": [best_label],
                    "confidence": round(min(0.9, 0.45 + best_score * 0.4), 2),
                    "reason": "token_similarity",
                }
            )
        else:
            suggestions.append(
                {
                    "field": field_name,
                    "chosen_label": None,
                    "label_candidates": [],
                    "confidence": 0.0,
                    "reason": "no_label_match",
                }
            )
    return suggestions


def _suggest_overwrite_labels_with_llm(
    *,
    reference_name: str,
    overwrite_target_details: list[dict[str, Any]],
    existing_field_labels: dict[str, Any],
    candidate_labels: list[str],
) -> dict[str, Any]:
    overwrite_fields = [str(item.get("field") or "").strip() for item in overwrite_target_details if isinstance(item, dict)]
    overwrite_fields = [field for field in overwrite_fields if field]
    if not overwrite_fields:
        return {
            "used_llm": False,
            "suggestions": [],
            "reason": "no_overwrite_fields",
        }

    heuristics = _heuristic_overwrite_label_suggestions(overwrite_fields, existing_field_labels, candidate_labels)
    client = get_openrouter_client()
    if client is None:
        return {
            "used_llm": False,
            "suggestions": heuristics,
            "reason": "openrouter_not_configured",
        }

    prompt = (
        "Map workflow overwrite fields to likely PDF labels for rewrite inference. "
        "Return JSON only with schema: "
        "{\"suggestions\":[{\"field\":string,\"chosen_label\":string|null,\"label_candidates\":[string],\"confidence\":number,\"reason\":string}]}. "
        "Rules: prefer configured labels when present; choose labels that are visible anchors next to replaceable values; "
        "confidence must be 0..1; include every overwrite field exactly once.\n"
        f"reference_name: {json.dumps(reference_name, ensure_ascii=False)}\n"
        f"workflow_overwrite_targets: {json.dumps(overwrite_target_details, ensure_ascii=False)}\n"
        f"existing_field_labels: {json.dumps(existing_field_labels, ensure_ascii=False)}\n"
        f"candidate_pdf_labels: {json.dumps(candidate_labels[:180], ensure_ascii=False)}\n"
        f"heuristic_baseline: {json.dumps(heuristics, ensure_ascii=False)}"
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
            call_name="template_overwrite_label_suggestions",
            complexity="simple",
        )
        payload = _extract_json_object(str(getattr(response, "text", "") or ""))
        parsed = payload.get("suggestions") if isinstance(payload.get("suggestions"), list) else []
        sanitized: list[dict[str, Any]] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            field_name = str(item.get("field") or "").strip()
            if not field_name:
                continue
            chosen_label_raw = item.get("chosen_label")
            chosen_label = str(chosen_label_raw).strip() if chosen_label_raw not in (None, "") else None
            label_candidates = item.get("label_candidates") if isinstance(item.get("label_candidates"), list) else []
            candidates = [str(label).strip() for label in label_candidates if str(label).strip()]
            confidence_raw = item.get("confidence")
            try:
                confidence = float(confidence_raw)
            except Exception:
                confidence = 0.0
            confidence = max(0.0, min(1.0, confidence))
            reason = str(item.get("reason") or "").strip() or "llm_suggestion"
            sanitized.append(
                {
                    "field": field_name,
                    "chosen_label": chosen_label,
                    "label_candidates": candidates,
                    "confidence": confidence,
                    "reason": reason,
                }
            )

        if not sanitized:
            return {
                "used_llm": False,
                "suggestions": heuristics,
                "reason": "llm_empty_result",
            }

        return {
            "used_llm": True,
            "suggestions": sanitized,
            "reason": "ok",
        }
    except Exception as exc:
        return {
            "used_llm": False,
            "suggestions": heuristics,
            "reason": "llm_error",
            "error": str(exc),
        }


def _auto_apply_workflow_overwrite_field_labels(
    *,
    reference_name: str,
    template_bytes: bytes,
    template_format: str,
    base_payload: dict[str, Any],
    linked_workflow_schema: dict[str, Any] | None,
    persist_registry_update: bool = False,
) -> dict[str, Any]:
    schema = dict(linked_workflow_schema or {})
    overwrite_targets = schema.get("overwrite_targets") if isinstance(schema.get("overwrite_targets"), dict) else {"fields": [], "details": []}
    overwrite_target_details = overwrite_targets.get("details") if isinstance(overwrite_targets.get("details"), list) else []
    overwrite_fields = overwrite_targets.get("fields") if isinstance(overwrite_targets.get("fields"), list) else []
    overwrite_fields = [str(item or "").strip() for item in overwrite_fields if str(item or "").strip()]

    field_rules = dict(base_payload.get("field_rules") if isinstance(base_payload.get("field_rules"), dict) else {})
    field_labels = dict(field_rules.get("field_labels") if isinstance(field_rules.get("field_labels"), dict) else {})
    if not overwrite_fields:
        return {
            "applied": [],
            "field_labels": field_labels,
            "workflow_overwrite_targets": overwrite_targets,
            "reason": "no_overwrite_fields",
        }

    candidate_labels: list[str] = []
    if str(template_format or "").strip().lower() == "pdf" and template_bytes:
        try:
            text_spans = document_template_pipeline.extract_pdf_text_spans(template_bytes)
            candidate_labels = _build_pdf_label_candidates(text_spans)
        except Exception as exc:
            LOGGER.warning("Failed to extract labels for automatic overwrite mapping on %s: %s", reference_name, exc)

    suggestions = _suggest_overwrite_labels_with_llm(
        reference_name=reference_name,
        overwrite_target_details=overwrite_target_details,
        existing_field_labels=field_labels,
        candidate_labels=candidate_labels,
    )
    apply_request = TemplateReferenceApplyFieldLabelsRequest(
        field_labels={},
        suggestions=suggestions.get("suggestions") if isinstance(suggestions.get("suggestions"), list) else [],
        only_workflow_targets=True,
        min_confidence=0.5,
    )
    apply_map, skipped = _build_field_labels_to_apply(apply_request, allowed_workflow_fields=set(overwrite_fields))

    applied_fields: list[str] = []
    for field_name, labels in apply_map.items():
        merged_value = _merge_field_label_values(field_labels.get(field_name), labels)
        if merged_value != field_labels.get(field_name):
            field_labels[field_name] = merged_value
            applied_fields.append(field_name)

    if applied_fields:
        field_rules["field_labels"] = field_labels
        base_payload["field_rules"] = field_rules

    if persist_registry_update and applied_fields:
        registry = _load_template_reference_registry()
        normalized = str(reference_name or "").strip().lower()
        for idx, row in enumerate(registry):
            if str(row.get("reference_name") or "").strip().lower() != normalized:
                continue
            updated_row = dict(row)
            updated_base_payload = dict(updated_row.get("base_payload") if isinstance(updated_row.get("base_payload"), dict) else {})
            updated_field_rules = dict(updated_base_payload.get("field_rules") if isinstance(updated_base_payload.get("field_rules"), dict) else {})
            updated_field_rules["field_labels"] = field_labels
            updated_base_payload["field_rules"] = updated_field_rules
            updated_row["base_payload"] = updated_base_payload
            updated_row["render_mode"] = _resolve_template_render_mode(updated_row, updated_base_payload)
            registry[idx] = updated_row
            _save_template_reference_registry(registry)
            break

    return {
        "applied": sorted(set(applied_fields)),
        "field_labels": field_labels,
        "workflow_overwrite_targets": overwrite_targets,
        "analysis": suggestions,
        "skipped": skipped,
    }


def _load_linked_workflow_schema_fields(entry: dict[str, Any]) -> dict[str, Any]:
    workflow_key = str(entry.get("linked_workflow_key") or "").strip()
    workflow_id_raw = entry.get("linked_workflow_id")
    workflow_id = 0
    if isinstance(workflow_id_raw, (int, float, str)) and str(workflow_id_raw).strip():
        try:
            workflow_id = int(str(workflow_id_raw).strip())
        except ValueError:
            workflow_id = 0

    if not workflow_key and workflow_id <= 0:
        return {
            "has_linked_workflow": False,
            "workflow_key": None,
            "workflow_version_id": None,
            "fields": [],
            "overwrite_targets": {"fields": [], "details": []},
        }

    if not workflow_key and workflow_id > 0:
        workflow = business_rules_engine.get_solf_workflow_registry_entry(workflow_id=workflow_id)
        workflow_key = str((workflow or {}).get("workflow_key") or "").strip()

    if not workflow_key:
        return {
            "has_linked_workflow": True,
            "workflow_key": None,
            "workflow_version_id": None,
            "fields": [],
            "overwrite_targets": {"fields": [], "details": []},
        }

    active = business_rules_engine.get_solf_workflow_active_version_by_key(workflow_key=workflow_key) or {}
    workflow_version_id = int(active.get("workflow_version_id") or 0)
    if workflow_version_id <= 0:
        return {
            "has_linked_workflow": True,
            "workflow_key": workflow_key,
            "workflow_version_id": None,
            "fields": [],
            "overwrite_targets": {"fields": [], "details": []},
        }

    connection = object_db.get_connection()
    try:
        steps = object_db.list_solf_workflow_steps(connection, workflow_version_id=workflow_version_id)
    finally:
        connection.close()

    overwrite_targets = _extract_workflow_overwrite_targets_from_steps(steps)

    return {
        "has_linked_workflow": True,
        "workflow_key": workflow_key,
        "workflow_version_id": workflow_version_id,
        "fields": _extract_linked_workflow_schema_fields_from_steps(steps),
        "overwrite_targets": overwrite_targets,
    }


def _build_template_field_schema(entry: dict[str, Any]) -> dict[str, Any]:
    base_payload = dict(entry.get("base_payload") if isinstance(entry.get("base_payload"), dict) else {})
    document_kind = str(entry.get("document_kind") or "document").strip().lower() or "document"

    discovered_fields = sorted(set(_flatten_payload_paths(base_payload)))
    linked_workflow_schema = _load_linked_workflow_schema_fields(entry)
    workflow_fields = linked_workflow_schema.get("fields") if isinstance(linked_workflow_schema.get("fields"), list) else []
    overwrite_targets = linked_workflow_schema.get("overwrite_targets") if isinstance(linked_workflow_schema.get("overwrite_targets"), dict) else {"fields": [], "details": []}
    overwrite_fields = overwrite_targets.get("fields") if isinstance(overwrite_targets.get("fields"), list) else []
    if workflow_fields:
        discovered_fields = sorted(set(discovered_fields).union({str(item).strip() for item in workflow_fields if str(item).strip()}))
    line_item_columns = _extract_line_item_columns(base_payload)

    suggested_common_fields = [
        "document_title",
        "document_number",
        "issue_date",
        "customer_name",
        "recipient_name",
        "recipient_address",
        "notes",
        "line_items",
        "subtotal",
        "tax_total",
        "grand_total",
    ]
    kind_specific_defaults: dict[str, list[str]] = {
        "invoice": ["invoice_number", "invoice_date", "due_date", "currency"],
        "quotation": ["quotation_number", "quotation_date", "valid_until", "currency"],
        "delivery_note": ["delivery_note_number", "delivery_date", "recipient_name"],
        "email": ["subject", "body_text", "recipient_name"],
        "letter": ["subject", "recipient_name", "body_text"],
    }
    suggested_override_fields: list[str] = []
    for field in suggested_common_fields + kind_specific_defaults.get(document_kind, []):
        if field in discovered_fields and field not in suggested_override_fields:
            suggested_override_fields.append(field)

    if "line_items" in discovered_fields and "line_items" not in suggested_override_fields:
        suggested_override_fields.append("line_items")

    for workflow_field in workflow_fields:
        field = str(workflow_field or "").strip()
        if field and field in discovered_fields and field not in suggested_override_fields:
            suggested_override_fields.append(field)

    return {
        "reference_name": str(entry.get("reference_name") or ""),
        "document_kind": document_kind,
        "template_format": str(entry.get("template_format") or ""),
        "discovered_fields": discovered_fields,
        "suggested_override_fields": suggested_override_fields,
        "line_items": {
            "supported": "line_items" in discovered_fields,
            "columns": line_item_columns,
            "recommended_columns": ["description", "quantity", "unit_price", "amount", "vat_rate", "vat_amount"],
        },
        "totals": {
            "subtotal_field": "subtotal",
            "tax_total_field": "tax_total",
            "grand_total_field": "grand_total",
            "supports_auto_compute": True,
        },
        "linked_workflow_schema": {
            "has_linked_workflow": bool(linked_workflow_schema.get("has_linked_workflow")),
            "workflow_key": linked_workflow_schema.get("workflow_key"),
            "workflow_version_id": linked_workflow_schema.get("workflow_version_id"),
            "derived_fields": workflow_fields,
            "derived_overwrite_fields": overwrite_fields,
        },
    }


def _ensure_sales_pipeline_tables(connection: Any) -> None:
    raise HTTPException(status_code=410, detail="Sales pipeline storage belongs in a domain-specific application, not IKOS-Kernel")


def _create_sales_pipeline_run(
    connection: Any,
    *,
    pipeline_name: str,
    stage_count: int,
    output_dir: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    run_key = f"{_slugify(pipeline_name)}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO sales_pipeline_run (
                run_key, pipeline_name, status, stage_count, output_dir, no_booking, payload
            )
            VALUES (%s, %s, 'running', %s, %s, TRUE, %s::jsonb)
            RETURNING run_id, run_key, created_at
            """,
            (run_key, pipeline_name, int(stage_count), output_dir, json.dumps(payload or {})),
        )
        row = cursor.fetchone()
    return {
        "run_id": int(row[0]),
        "run_key": str(row[1]),
        "created_at": row[2],
    }


def _upsert_sales_pipeline_stage(
    connection: Any,
    *,
    run_id: int,
    stage_order: int,
    stage_name: str,
    reference_name: str,
    status: str,
    error_message: str | None = None,
    result: dict[str, Any] | None = None,
) -> None:
    payload = result or {}
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO sales_pipeline_stage (
                run_id, stage_order, stage_name, reference_name, status,
                error_message, saved_path, filename, media_type, size_bytes, result, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
            ON CONFLICT (run_id, stage_order)
            DO UPDATE SET
                stage_name = EXCLUDED.stage_name,
                reference_name = EXCLUDED.reference_name,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                saved_path = EXCLUDED.saved_path,
                filename = EXCLUDED.filename,
                media_type = EXCLUDED.media_type,
                size_bytes = EXCLUDED.size_bytes,
                result = EXCLUDED.result,
                updated_at = NOW()
            """,
            (
                int(run_id),
                int(stage_order),
                stage_name,
                reference_name or None,
                status,
                error_message,
                payload.get("saved_path"),
                payload.get("filename"),
                payload.get("media_type"),
                int(payload.get("size_bytes") or 0) if payload.get("size_bytes") not in (None, "") else None,
                json.dumps(payload),
            ),
        )


def _finalize_sales_pipeline_run(
    connection: Any,
    *,
    run_id: int,
    generated_count: int,
    failed_count: int,
) -> None:
    status = "completed" if int(failed_count) == 0 else ("partial" if int(generated_count) > 0 else "failed")
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE sales_pipeline_run
            SET status = %s,
                generated_count = %s,
                failed_count = %s,
                updated_at = NOW()
            WHERE run_id = %s
            """,
            (status, int(generated_count), int(failed_count), int(run_id)),
        )


def _refresh_sales_pipeline_run_aggregate(connection: Any, *, run_id: int) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS stage_count,
                COUNT(*) FILTER (WHERE lower(status) = 'failed') AS failed_count,
                COUNT(*) FILTER (
                    WHERE lower(status) IN ('generated', 'completed', 'done', 'synced', 'booked', 'paid')
                ) AS success_count
            FROM sales_pipeline_stage
            WHERE run_id = %s
            """,
            (int(run_id),),
        )
        row = cursor.fetchone() or (0, 0, 0)

        stage_count = int(row[0] or 0)
        failed_count = int(row[1] or 0)
        generated_count = int(row[2] or 0)

        if stage_count <= 0:
            run_status = "running"
        elif failed_count == 0 and generated_count >= stage_count:
            run_status = "completed"
        elif failed_count > 0 and generated_count == 0:
            run_status = "failed"
        elif failed_count > 0 or generated_count > 0:
            run_status = "partial"
        else:
            run_status = "running"

        cursor.execute(
            """
            UPDATE sales_pipeline_run
            SET status = %s,
                stage_count = %s,
                generated_count = %s,
                failed_count = %s,
                updated_at = NOW()
            WHERE run_id = %s
            RETURNING run_id, run_key, pipeline_name, status, stage_count,
                      generated_count, failed_count, no_booking, output_dir,
                      created_at, updated_at
            """,
            (run_status, stage_count, generated_count, failed_count, int(run_id)),
        )
        run_row = cursor.fetchone()

    return {
        "run_id": int(run_row[0]),
        "run_key": run_row[1],
        "pipeline_name": run_row[2],
        "status": run_row[3],
        "stage_count": int(run_row[4] or 0),
        "generated_count": int(run_row[5] or 0),
        "failed_count": int(run_row[6] or 0),
        "no_booking": bool(run_row[7]),
        "output_dir": run_row[8],
        "created_at": run_row[9],
        "updated_at": run_row[10],
    }


def _sync_sales_pipeline_stage_internal(
    connection: Any,
    *,
    run_key: str,
    stage_name: str,
    payload: SalesPipelineStageSyncRequest | SalesPipelineStageBulkSyncItem,
) -> dict[str, Any]:
    key = str(run_key or "").strip()
    stage = str(stage_name or "").strip().lower()
    if not key:
        raise HTTPException(status_code=400, detail="run_key is required")
    if not stage:
        raise HTTPException(status_code=400, detail="stage_name is required")

    normalized_status = str(payload.status or "").strip().lower()
    if normalized_status and not re.fullmatch(r"[a-z0-9_-]+", normalized_status):
        raise HTTPException(status_code=400, detail="status must contain only letters, numbers, underscore, or hyphen")

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT r.run_id, s.stage_order, s.stage_name, s.reference_name, s.status, s.result
            FROM sales_pipeline_run r
            JOIN sales_pipeline_stage s ON s.run_id = r.run_id
            WHERE r.run_key = %s
              AND lower(s.stage_name) = %s
            ORDER BY s.stage_order ASC
            LIMIT 1
            """,
            (key, stage),
        )
        row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Stage '{stage}' not found for run '{key}'")

    run_id = int(row[0])
    stage_order = int(row[1])
    persisted_stage_name = str(row[2])
    reference_name = str(row[3] or "")
    current_status = str(row[4] or "pending").strip().lower() or "pending"
    current_result = row[5] if isinstance(row[5], dict) else {}

    idempotency_key = str(getattr(payload, "idempotency_key", "") or "").strip()
    processed_keys_raw = current_result.get("processed_idempotency_keys")
    processed_keys = [str(item).strip() for item in processed_keys_raw] if isinstance(processed_keys_raw, list) else []
    if idempotency_key and idempotency_key in processed_keys:
        return {
            "run_id": run_id,
            "stage_order": stage_order,
            "stage_name": persisted_stage_name,
            "reference_name": reference_name,
            "status": current_status,
            "error_message": None,
            "result": current_result,
            "idempotency_key": idempotency_key,
            "skipped": True,
            "skip_reason": "duplicate_idempotency_key",
        }

    merged_result = dict(current_result)
    if isinstance(payload.result, dict) and payload.result:
        merged_result.update(payload.result)
    if str(payload.external_event or "").strip():
        merged_result["external_event"] = str(payload.external_event).strip()
    if str(payload.external_reference or "").strip():
        merged_result["external_reference"] = str(payload.external_reference).strip()
    if idempotency_key:
        deduped = []
        for item in [*processed_keys, idempotency_key]:
            token = str(item).strip()
            if token and token not in deduped:
                deduped.append(token)
        merged_result["processed_idempotency_keys"] = deduped
    merged_result["last_synced_at"] = _utc_iso_now()

    next_status = normalized_status or current_status
    error_message = str(payload.error_message or "").strip() or None
    if next_status != "failed" and error_message:
        merged_result["sync_note"] = error_message
        error_message = None

    _upsert_sales_pipeline_stage(
        connection,
        run_id=run_id,
        stage_order=stage_order,
        stage_name=persisted_stage_name,
        reference_name=reference_name,
        status=next_status,
        error_message=error_message,
        result=merged_result,
    )

    return {
        "run_id": run_id,
        "stage_order": stage_order,
        "stage_name": persisted_stage_name,
        "reference_name": reference_name,
        "status": next_status,
        "error_message": error_message,
        "result": merged_result,
        "idempotency_key": idempotency_key or None,
        "skipped": False,
    }


def _load_template_reference_registry() -> list[dict[str, Any]]:
    if not TEMPLATE_REFERENCE_REGISTRY.exists():
        return []
    try:
        raw = json.loads(TEMPLATE_REFERENCE_REGISTRY.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def _save_template_reference_registry(rows: list[dict[str, Any]]) -> None:
    TEMPLATE_REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    TEMPLATE_REFERENCE_REGISTRY.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _find_template_reference(reference_name: str) -> dict[str, Any] | None:
    normalized = str(reference_name or "").strip().lower()
    if not normalized:
        return None
    for item in _load_template_reference_registry():
        if str(item.get("reference_name") or "").strip().lower() == normalized:
            return item
    return None


def _resolve_template_render_mode(entry: dict[str, Any], payload: dict[str, Any] | None = None) -> str:
    entry_mode = str(entry.get("render_mode") or entry.get("template_render_mode") or "").strip().lower()
    if entry_mode:
        return entry_mode

    base_payload = dict(entry.get("base_payload") if isinstance(entry.get("base_payload"), dict) else {})
    payload_mode = str(base_payload.get("render_mode") or base_payload.get("template_render_mode") or "").strip().lower()
    if payload_mode:
        return payload_mode

    rule_options = base_payload.get("template_rule_options") if isinstance(base_payload.get("template_rule_options"), dict) else {}
    options_mode = str(rule_options.get("render_mode") or "").strip().lower()
    if options_mode:
        return options_mode

    request_payload = payload if isinstance(payload, dict) else {}
    request_mode = str(request_payload.get("render_mode") or request_payload.get("template_render_mode") or "").strip().lower()
    if request_mode:
        return request_mode

    request_rule_options = request_payload.get("template_rule_options") if isinstance(request_payload.get("template_rule_options"), dict) else {}
    request_options_mode = str(request_rule_options.get("render_mode") or "").strip().lower()
    if request_options_mode:
        return request_options_mode

    linked_workflow_schema = _load_linked_workflow_schema_fields(entry)
    overwrite_targets = linked_workflow_schema.get("overwrite_targets") if isinstance(linked_workflow_schema.get("overwrite_targets"), dict) else {}
    overwrite_fields = overwrite_targets.get("fields") if isinstance(overwrite_targets.get("fields"), list) else []
    if any(str(field or "").strip() for field in overwrite_fields):
        return "preserve_template"

    return "overlay"


def _render_csv_from_context(payload: dict[str, Any]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["field", "value"])
    line_items = payload.get("line_items") if isinstance(payload.get("line_items"), list) else []
    for key, value in payload.items():
        if key == "line_items":
            continue
        writer.writerow([str(key), json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value or "")])
    if line_items:
        writer.writerow([])
        writer.writerow(["line_item_index", "description", "quantity", "unit_price", "amount", "vat_rate", "vat_amount"])
        for idx, item in enumerate(line_items, start=1):
            row = item if isinstance(item, dict) else {}
            writer.writerow([
                idx,
                str(row.get("description") or ""),
                str(row.get("quantity") or ""),
                str(row.get("unit_price") or ""),
                str(row.get("amount") or ""),
                str(row.get("vat_rate") or ""),
                str(row.get("vat_amount") or ""),
            ])
    return output.getvalue().encode("utf-8-sig")


def _apply_specification_overrides(payload: dict[str, Any], specification: str) -> dict[str, Any]:
    out = dict(payload)
    spec = str(specification or "").strip().lower()
    if not spec:
        return out
    if "current date" in spec or "invoice date" in spec:
        today = date.today().isoformat()
        out.setdefault("issue_date", today)
        out.setdefault("invoice_date", today)
    return out


def _generate_from_reference_internal(
    *,
    reference_name: str,
    specification: str,
    output_format: str,
    document_kind: str,
    payload: dict[str, Any],
    override_fields: list[str] | None,
    output_path: str,
    output_dir: str,
) -> dict[str, Any]:
    entry = _find_template_reference(reference_name)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Template reference '{reference_name}' not found")

    linked_workflow = _validate_template_linked_workflow_for_generation(entry)

    template_path = Path(str(entry.get("stored_path") or "").strip())
    if not template_path.exists():
        raise HTTPException(status_code=404, detail=f"Template file not found at {template_path}")

    template_format = str(entry.get("template_format") or template_path.suffix.lstrip(".") or "").strip().lower()
    doc_kind = str(document_kind or entry.get("document_kind") or "invoice").strip().lower() or "invoice"
    effective_payload = dict(entry.get("base_payload") if isinstance(entry.get("base_payload"), dict) else {})
    effective_payload = _merge_payload_with_override_fields(effective_payload, payload, override_fields)
    effective_payload = _apply_specification_overrides(effective_payload, specification)

    linked_workflow_schema = _load_linked_workflow_schema_fields(entry)
    auto_label_result = _auto_apply_workflow_overwrite_field_labels(
        reference_name=reference_name,
        template_bytes=template_path.read_bytes(),
        template_format=template_format,
        base_payload=effective_payload,
        linked_workflow_schema=linked_workflow_schema,
        persist_registry_update=True,
    )
    if auto_label_result.get("field_labels"):
        effective_payload = dict(effective_payload)
        field_rules = dict(effective_payload.get("field_rules") if isinstance(effective_payload.get("field_rules"), dict) else {})
        field_rules["field_labels"] = dict(auto_label_result.get("field_labels") or {})
        effective_payload["field_rules"] = field_rules
    render_mode = _resolve_template_render_mode(entry, effective_payload)

    requested_format = str(output_format or "pdf").strip().lower() or "pdf"

    if template_format in {"xlsx", "xls", "xlsm"}:
        if requested_format in {"excel", "csv", "xlsx"}:
            content = _render_csv_from_context(effective_payload)
            filename = _resolve_generated_filename(reference_name, effective_payload, "csv")
            media_type = "text/csv; charset=utf-8"
        else:
            raise HTTPException(
                status_code=400,
                detail="Excel references currently support csv/excel output only; format-preserving xlsx rendering is not implemented.",
            )
    else:
        if template_format not in {"pdf", "docx"}:
            raise HTTPException(status_code=400, detail=f"Unsupported template format: {template_format}")
        normalized_output_format = requested_format
        if normalized_output_format == "excel":
            normalized_output_format = "pdf"
        template_bytes = template_path.read_bytes()
        explicit_layout = effective_payload.get("layout") if isinstance(effective_payload.get("layout"), dict) else None
        resolved_layout = dict(explicit_layout or {})
        qr_payload = dict(effective_payload.get("qr_data") if isinstance(effective_payload.get("qr_data"), dict) else {})
        qr_generation_info: dict[str, Any] = {}
        if template_format == "pdf" and render_mode == "preserve_template":
            inferred_layout = document_template_pipeline.infer_pdf_rewrite_layout(
                template_bytes=template_bytes,
                payload=effective_payload,
                template_payload=entry.get("base_payload") if isinstance(entry.get("base_payload"), dict) else {},
            )
            if inferred_layout:
                merged_layout = dict(inferred_layout)
                merged_layout.update(resolved_layout)
                resolved_layout = merged_layout
            if not isinstance(resolved_layout.get("qr_bill_region"), dict):
                inferred_qr_region = document_template_pipeline.infer_pdf_qr_bill_region(
                    template_bytes,
                    entry.get("base_payload") if isinstance(entry.get("base_payload"), dict) else {},
                )
                if inferred_qr_region:
                    resolved_layout["qr_bill_region"] = inferred_qr_region
            inferred_qr_defaults = document_template_pipeline.infer_qr_data_from_pdf_text(
                template_bytes,
                entry.get("base_payload") if isinstance(entry.get("base_payload"), dict) else {},
            )
            if inferred_qr_defaults:
                merged_qr_payload = dict(inferred_qr_defaults)
                merged_qr_payload.update({key: value for key, value in qr_payload.items() if value not in (None, "", [], {})})
                qr_payload = merged_qr_payload
                effective_payload["qr_data"] = merged_qr_payload
        if isinstance(qr_payload, dict) and qr_payload:
            try:
                document_template_pipeline.build_swiss_qr_bill_payload(qr_payload, diagnostics_out=qr_generation_info)
            except Exception:
                qr_generation_info = {}
        try:
            content, filename, media_type = document_template_pipeline.render_template_document(
                template_bytes=template_bytes,
                template_format=template_format,
                output_format=normalized_output_format,
                document_kind=doc_kind,
                payload=effective_payload,
                qr_data=qr_payload,
                layout=resolved_layout,
                render_mode=render_mode,
                template_name=template_path.name,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    target = _resolve_output_file_path(
        filename=filename,
        output_path=output_path,
        output_dir=output_dir,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)

    return {
        "reference_name": reference_name,
        "doc_name": entry.get("doc_name") or reference_name,
        "document_kind": doc_kind,
        "specification": specification,
        "saved_path": str(target),
        "filename": filename,
        "media_type": media_type,
        "size_bytes": len(content),
        "no_booking": True,
        "render_mode": render_mode,
        "qr_generation": qr_generation_info if qr_generation_info else None,
        "source_template_path": str(template_path),
        "linked_workflow": linked_workflow,
    }


def _pick_reference_seed(payload: dict[str, Any], qr_data: dict[str, Any], explicit_current_reference: str) -> str:
    if str(explicit_current_reference or "").strip():
        return str(explicit_current_reference).strip()

    qr_reference = str(qr_data.get("reference") or qr_data.get("qr_reference") or "").strip()
    if qr_reference:
        return qr_reference

    payload_reference = str(payload.get("reference") or payload.get("qr_reference") or "").strip()
    if payload_reference:
        return payload_reference

    return ""


def _apply_fixed_segments_to_payload_digits(
    payload_digits: str,
    fixed_segments: list[dict[str, Any]] | None = None,
) -> str:
    digits = list(str(payload_digits or "").strip())
    if len(digits) != 26 or any(not ch.isdigit() for ch in digits):
        raise ValueError("payload seed must be exactly 26 digits")

    for segment in fixed_segments or []:
        if not isinstance(segment, dict):
            continue
        value_digits = "".join(ch for ch in str(segment.get("value") or "") if ch.isdigit())
        if not value_digits:
            continue
        try:
            start = int(segment.get("start"))
        except Exception as exc:
            raise ValueError("fixed segment start must be an integer") from exc
        if start < 1 or start > 26:
            raise ValueError("fixed segment start must be within payload positions 1..26")
        end = start + len(value_digits) - 1
        if end > 26:
            raise ValueError("fixed segment exceeds payload length 26")
        for offset, digit in enumerate(value_digits):
            digits[start - 1 + offset] = digit

    return "".join(digits)


def _build_seed_reference_from_sequence(
    sequence_seed: int,
    policy: dict[str, Any] | None = None,
) -> str:
    if int(sequence_seed) < 0:
        raise ValueError("sequence seed must be non-negative")
    payload_digits = f"{int(sequence_seed):026d}"
    normalized_policy = dict(policy or {})
    if str(normalized_policy.get("mode") or "").strip().lower() == "next_from_reference_fixed_segments":
        payload_digits = _apply_fixed_segments_to_payload_digits(
            payload_digits,
            fixed_segments=normalized_policy.get("fixed_segments") if isinstance(normalized_policy.get("fixed_segments"), list) else [],
        )
    generator = SwissQRReferenceGenerator()
    return generator.from_payload(payload_digits).storage


def _extract_default_sequence_seed_from_template_entry(entry: dict[str, Any]) -> int | None:
    candidates: list[str] = []
    source_reference = str((entry.get("base_payload") or {}).get("source_reference") or "").strip() if isinstance(entry.get("base_payload"), dict) else ""
    source_filename = str(entry.get("source_filename") or "").strip()
    reference_name = str(entry.get("reference_name") or "").strip()
    if source_reference:
        candidates.append(source_reference)
    if source_filename:
        candidates.append(source_filename)
    if reference_name:
        candidates.append(reference_name)

    best: tuple[int, str] | None = None
    for text in candidates:
        for token in re.findall(r"\d{3,}", text):
            normalized = token.lstrip("0") or "0"
            if best is None or len(normalized) > best[0] or (len(normalized) == best[0] and normalized > best[1]):
                best = (len(normalized), normalized)

    if best is None:
        return None
    try:
        return int(best[1])
    except Exception:
        return None


def _ensure_invoice_reference_state_table(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS invoice_reference_state (
                reference_name TEXT PRIMARY KEY,
                last_reference TEXT NOT NULL,
                last_sequence NUMERIC(26, 0),
                last_mode TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _build_generated_reference(
    request: InvoiceFromReferenceGenerateRequest,
    payload: dict[str, Any],
    qr_data: dict[str, Any],
    reference_policy: dict[str, Any] | None = None,
    *,
    default_current_reference: str = "",
    default_current_sequence: int | None = None,
) -> dict[str, Any]:
    generator = SwissQRReferenceGenerator()
    policy = dict(reference_policy or {})
    mode = str(request.reference_mode or "next_from_reference").strip().lower()

    if mode == "explicit":
        explicit_ref = _pick_reference_seed(payload, qr_data, request.current_reference)
        if not explicit_ref:
            raise HTTPException(status_code=400, detail="explicit mode requires current_reference or qr_data.reference")
        try:
            parsed = generator.parse(explicit_ref)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "storage_string": parsed.storage,
            "visual_string": parsed.visual,
            "mode": "explicit",
        }

    if mode == "next_from_sequence":
        sequence_seed = request.current_sequence if request.current_sequence is not None else default_current_sequence
        if sequence_seed is None:
            raise HTTPException(status_code=400, detail="next_from_sequence mode requires current_sequence")
        try:
            return {
                **generator.next_from_sequence(int(sequence_seed), step=int(request.sequence_step)),
                "mode": "next_from_sequence",
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    if mode == "prefix_and_sequence":
        if request.sequence_value is None:
            raise HTTPException(status_code=400, detail="prefix_and_sequence mode requires sequence_value")
        try:
            ref = generator.from_prefix_and_sequence(str(request.reference_prefix or ""), int(request.sequence_value))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "storage_string": ref.storage,
            "visual_string": ref.visual,
            "sequence_value": int(request.sequence_value),
            "mode": "prefix_and_sequence",
        }

    if mode != "next_from_reference":
        raise HTTPException(
            status_code=400,
            detail="reference_mode must be next_from_reference, next_from_sequence, prefix_and_sequence, or explicit",
        )

    seed_ref = _pick_reference_seed(payload, qr_data, request.current_reference or default_current_reference)
    if not seed_ref:
        sequence_seed = request.current_sequence if request.current_sequence is not None else default_current_sequence
        if sequence_seed is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "next_from_reference mode requires current_reference or existing qr_data.reference seed "
                    "(or a sequence seed from current_sequence/persisted state/template source)"
                ),
            )
        try:
            seed_ref = _build_seed_reference_from_sequence(int(sequence_seed), policy)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        if str(policy.get("mode") or "").strip().lower() == "next_from_reference_fixed_segments":
            ref = generator.next_from_reference_with_fixed_segments(
                seed_ref,
                fixed_segments=policy.get("fixed_segments") if isinstance(policy.get("fixed_segments"), list) else [],
                step=int(request.sequence_step),
            )
        else:
            ref = generator.next_from_reference(seed_ref, step=int(request.sequence_step))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "storage_string": ref.storage,
        "visual_string": ref.visual,
        "mode": "next_from_reference",
    }


def _reserve_generated_reference_atomic(
    request: InvoiceFromReferenceGenerateRequest,
    payload: dict[str, Any],
    qr_data: dict[str, Any],
    reference_policy: dict[str, Any] | None = None,
    *,
    default_current_reference: str = "",
    default_current_sequence: int | None = None,
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        _ensure_invoice_reference_state_table(connection)

        persisted_reference = ""
        persisted_sequence: int | None = None
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT last_reference, last_sequence
                FROM invoice_reference_state
                WHERE reference_name = %s
                FOR UPDATE
                """,
                (request.reference_name,),
            )
            row = cursor.fetchone()
            if row:
                persisted_reference = str(row[0] or "").strip()
                persisted_sequence = _to_int_or_none(row[1])

        effective_default_reference = persisted_reference or str(default_current_reference or "").strip()
        effective_default_sequence = persisted_sequence if persisted_sequence is not None else default_current_sequence

        generated = _build_generated_reference(
            request,
            payload,
            qr_data,
            reference_policy,
            default_current_reference=effective_default_reference,
            default_current_sequence=effective_default_sequence,
        )
        storage = str(generated.get("storage_string") or "").strip()
        if not storage:
            raise HTTPException(status_code=500, detail="failed to reserve Swiss reference")

        next_sequence = _to_int_or_none(generated.get("next_sequence_to_store"))
        if next_sequence is None:
            next_sequence = _to_int_or_none(storage[:26])

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO invoice_reference_state (reference_name, last_reference, last_sequence, last_mode, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (reference_name)
                DO UPDATE SET
                    last_reference = EXCLUDED.last_reference,
                    last_sequence = EXCLUDED.last_sequence,
                    last_mode = EXCLUDED.last_mode,
                    updated_at = NOW()
                """,
                (
                    request.reference_name,
                    storage,
                    next_sequence,
                    str(generated.get("mode") or ""),
                ),
            )

        connection.commit()
        generated["persisted"] = True
        return generated
    except HTTPException:
        connection.rollback()
        raise
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail=f"failed to reserve Swiss reference state: {exc}") from exc
    finally:
        connection.close()


def _merge_invoice_payload_with_generated_reference(
    payload: dict[str, Any],
    qr_data: dict[str, Any],
    generated_reference: dict[str, Any],
    reference_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out = dict(payload)
    merged_qr = dict(qr_data)
    policy = dict(reference_policy or {})

    storage = str(generated_reference.get("storage_string") or "").strip()
    visual = str(generated_reference.get("visual_string") or "").strip()
    merged_qr["reference_type"] = "QRR"
    merged_qr["reference"] = storage
    merged_qr["qr_reference"] = storage

    out["reference"] = storage
    out["reference_visual"] = visual
    out["qr_data"] = merged_qr

    derived_reference_fields = policy.get("derived_reference_fields")
    if isinstance(derived_reference_fields, list):
        payload_digits = storage[:26]
        for item in derived_reference_fields:
            if not isinstance(item, dict):
                continue
            field_name = str(item.get("field") or "").strip()
            if not field_name:
                continue
            source = str(item.get("source") or "payload_digits").strip().lower() or "payload_digits"
            if source != "payload_digits":
                continue
            start = int(item.get("start") or 1)
            end = int(item.get("end") or 26)
            if start < 1:
                start = 1
            if end > 26:
                end = 26
            if end < start:
                continue
            derived_value = payload_digits[start - 1:end]
            if bool(item.get("trim_leading_zeros")):
                derived_value = derived_value.lstrip("0") or "0"
            out[field_name] = derived_value

    if bool(policy.get("set_rechnung_from_payload")) and storage:
        rechnung_field = str(policy.get("rechnung_field") or "RECHNUNG").strip() or "RECHNUNG"
        out[rechnung_field] = storage[:26].lstrip("0") or "0"
    return out


def _find_active_solf_rule_by_name(rule_name: str) -> dict[str, Any] | None:
    target = str(rule_name or "").strip().lower()
    if not target:
        return None
    try:
        rules = business_rules_engine.list_business_rules(is_active=True, limit=500)
    except Exception:
        return None

    for row in rules:
        candidate = str(row.get("rule_name") or "").strip().lower()
        if candidate and candidate == target:
            return row
    return None


def _resolve_output_file_path(
    *,
    filename: str,
    output_path: str = "",
    output_dir: str = "",
) -> Path:
    requested_path = str(output_path or "").strip()
    requested_dir = str(output_dir or "").strip()

    if requested_path:
        candidate = Path(requested_path)
        if not candidate.is_absolute():
            candidate = (PROJECT_ROOT / candidate).resolve()
        return candidate

    if requested_dir:
        base_dir = Path(requested_dir)
        if not base_dir.is_absolute():
            base_dir = (PROJECT_ROOT / base_dir).resolve()
    else:
        base_dir = (PROJECT_ROOT / "generated" / "docs").resolve()

    return base_dir / filename


def _resolve_generated_file_download_path(path_text: str) -> Path:
    raw = str(path_text or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="path is required")

    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = (PROJECT_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()

    try:
        candidate.relative_to(PROJECT_ROOT)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="path must resolve inside the project root") from exc

    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail=f"generated file not found at {candidate}")
    return candidate


@router.get("/generated-file")
def download_generated_file(path: str, disposition: str = "attachment") -> Response:
    target = _resolve_generated_file_download_path(path)
    mode = str(disposition or "attachment").strip().lower() or "attachment"
    if mode not in {"attachment", "inline"}:
        raise HTTPException(status_code=400, detail="disposition must be attachment or inline")

    media_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
    return FileResponse(
        path=target,
        media_type=media_type,
        filename=target.name,
        content_disposition_type=mode,
    )


@router.delete("/{doc_id}")
def delete_document(doc_id: int) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT doc_id, doc_key, doc_name, doc_path, markdown_path FROM document WHERE doc_id = %s", (doc_id,))
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"document {doc_id} not found")

            document_row = {
                "doc_id": int(row[0]),
                "doc_key": row[1],
                "doc_name": row[2],
                "doc_path": row[3],
                "markdown_path": row[4],
            }

            markdown_path = str(row[4] or "").strip()
            if markdown_path:
                try:
                    markdown_file = Path(markdown_path)
                    if markdown_file.exists():
                        markdown_file.unlink()
                except Exception:
                    LOGGER.warning("Failed to remove markdown cache file for doc_id=%s path=%s", doc_id, markdown_path)

            cursor.execute("DELETE FROM document WHERE doc_id = %s", (doc_id,))
            deleted = int(cursor.rowcount)
        connection.commit()
        return {
            "success": True,
            "deleted": deleted,
            "document": document_row,
        }
    except HTTPException:
        connection.rollback()
        raise
    except Exception as exc:
        connection.rollback()
        LOGGER.exception("Failed to delete document %s", doc_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/{doc_id}/export")
def export_document(doc_id: int, format: str = "pdf") -> Response:
    connection = object_db.get_connection()
    try:
        try:
            payload = document_generation.load_document_export_payload(connection, doc_id)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"document {doc_id} not found")

        try:
            content, filename, media_type = document_generation.render_document_export(
                payload.get("document") or {},
                payload.get("parts") or [],
                format,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        return Response(content=content, media_type=media_type, headers=headers)
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to export document %s", doc_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/{doc_id}/export/save")
def export_document_to_path(doc_id: int, payload: DocumentExportSaveRequest) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        try:
            document_payload = document_generation.load_document_export_payload(connection, doc_id)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"document {doc_id} not found")

        try:
            content, filename, media_type = document_generation.render_document_export(
                document_payload.get("document") or {},
                document_payload.get("parts") or [],
                payload.format,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        target = _resolve_output_file_path(
            filename=filename,
            output_path=payload.output_path,
            output_dir=payload.output_dir,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)

        return {
            "success": True,
            "doc_id": int(doc_id),
            "format": str(payload.format or "pdf").strip().lower() or "pdf",
            "filename": filename,
            "media_type": media_type,
            "saved_path": str(target),
            "size_bytes": len(content),
            "path_mode": "absolute" if Path(str(payload.output_path or "")).is_absolute() else "project_relative_or_default",
            "project_root": str(PROJECT_ROOT),
        }
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to export document %s to path", doc_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/template-references/ingest")
async def ingest_template_reference(
    reference_file: UploadFile = File(...),
    reference_name: str = Form(...),
    doc_name: str = Form(default=""),
    document_kind: str = Form(default="invoice"),
    description: str = Form(default=""),
    payload_json: str = Form(default="{}"),
    workflow_key: str = Form(default=""),
    workflow_id: str = Form(default=""),
) -> dict[str, Any]:
    reference_name_clean = str(reference_name or "").strip()
    if not reference_name_clean:
        raise HTTPException(status_code=400, detail="reference_name is required")

    source_name = str(reference_file.filename or "template.bin").strip() or "template.bin"
    ext = Path(source_name).suffix.lower()
    if ext not in {".pdf", ".docx", ".xlsx", ".xls", ".xlsm"}:
        raise HTTPException(status_code=400, detail="reference_file must be pdf, docx, xlsx, xls, or xlsm")

    payload = parse_json_dict(payload_json, "payload_json")
    linked_workflow = _resolve_template_linked_workflow(workflow_key_raw=workflow_key, workflow_id_raw=workflow_id)
    raw = await reference_file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="reference_file is empty")

    TEMPLATE_REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = _slugify(reference_name_clean)
    stored_name = f"{safe_name}{ext}"
    stored_path = (TEMPLATE_REFERENCE_DIR / stored_name).resolve()
    stored_path.write_bytes(raw)

    registry = _load_template_reference_registry()
    entry = {
        "reference_name": reference_name_clean,
        "doc_name": str(doc_name or reference_name_clean).strip() or reference_name_clean,
        "document_kind": str(document_kind or "invoice").strip().lower() or "invoice",
        "description": str(description or "").strip(),
        "source_filename": source_name,
        "stored_path": str(stored_path),
        "template_format": ext.lstrip("."),
        "base_payload": payload,
        "no_booking": True,
    }
    if linked_workflow:
        entry.update(linked_workflow)

    linked_workflow_schema = _load_linked_workflow_schema_fields(entry)
    auto_label_result = _auto_apply_workflow_overwrite_field_labels(
        reference_name=reference_name_clean,
        template_bytes=raw,
        template_format=ext.lstrip("."),
        base_payload=payload,
        linked_workflow_schema=linked_workflow_schema,
        persist_registry_update=True,
    )
    if auto_label_result.get("field_labels"):
        payload = dict(payload)
        payload_field_rules = dict(payload.get("field_rules") if isinstance(payload.get("field_rules"), dict) else {})
        payload_field_rules["field_labels"] = dict(auto_label_result.get("field_labels") or {})
        payload["field_rules"] = payload_field_rules
        entry["base_payload"] = payload

    render_mode = _resolve_template_render_mode(entry, payload)
    entry["render_mode"] = render_mode

    updated = False
    for idx, item in enumerate(registry):
        if str(item.get("reference_name") or "").strip().lower() == reference_name_clean.lower():
            registry[idx] = entry
            updated = True
            break
    if not updated:
        registry.append(entry)
    _save_template_reference_registry(registry)

    return {
        "success": True,
        "message": "Template reference stored for future document generation.",
        "result": entry,
    }


@router.get("/template-references")
def list_template_references() -> dict[str, Any]:
    rows = _load_template_reference_registry()
    return {
        "success": True,
        "count": len(rows),
        "result": rows,
    }


@router.get("/template-references/{reference_name}/field-schema")
def get_template_reference_field_schema(reference_name: str) -> dict[str, Any]:
    entry = _find_template_reference(reference_name)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Template reference '{reference_name}' not found")
    return {
        "success": True,
        "result": _build_template_field_schema(entry),
    }


@router.post("/template-references/{reference_name}/analyze-pdf")
def analyze_template_reference_pdf(
    reference_name: str,
    payload: TemplateReferencePdfAnalysisRequest,
) -> dict[str, Any]:
    entry = _find_template_reference(reference_name)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Template reference '{reference_name}' not found")

    template_path = Path(str(entry.get("stored_path") or "").strip())
    if not template_path.exists():
        raise HTTPException(status_code=404, detail=f"Template file not found at {template_path}")

    template_format = str(entry.get("template_format") or template_path.suffix.lstrip(".") or "").strip().lower()
    if template_format != "pdf":
        raise HTTPException(status_code=400, detail="Template PDF analysis is only supported for pdf template references")

    base_payload = dict(entry.get("base_payload") if isinstance(entry.get("base_payload"), dict) else {})
    merged_payload = dict(base_payload)
    if isinstance(payload.payload, dict) and payload.payload:
        merged_payload = _merge_payload_with_override_fields(merged_payload, payload.payload, None)

    render_mode = _resolve_template_render_mode(entry, merged_payload)
    template_bytes = template_path.read_bytes()
    linked_workflow_schema = _load_linked_workflow_schema_fields(entry)
    overwrite_targets = linked_workflow_schema.get("overwrite_targets") if isinstance(linked_workflow_schema.get("overwrite_targets"), dict) else {"fields": [], "details": []}
    overwrite_target_details = overwrite_targets.get("details") if isinstance(overwrite_targets.get("details"), list) else []

    text_spans = document_template_pipeline.extract_pdf_text_spans(template_bytes)
    label_candidates = _build_pdf_label_candidates(text_spans)
    field_rules = base_payload.get("field_rules") if isinstance(base_payload.get("field_rules"), dict) else {}
    existing_field_labels = field_rules.get("field_labels") if isinstance(field_rules.get("field_labels"), dict) else {}

    inferred_layout = document_template_pipeline.infer_pdf_rewrite_layout(
        template_bytes=template_bytes,
        payload=merged_payload,
        template_payload=base_payload,
    )
    inferred_qr_region = document_template_pipeline.infer_pdf_qr_bill_region(template_bytes, base_payload)
    inferred_qr_defaults = document_template_pipeline.infer_qr_data_from_pdf_text(template_bytes, base_payload)
    merged_qr_data = dict(inferred_qr_defaults)
    payload_qr = merged_payload.get("qr_data") if isinstance(merged_payload.get("qr_data"), dict) else {}
    merged_qr_data.update({key: value for key, value in payload_qr.items() if value not in (None, "", [], {})})

    llm_overwrite_suggestions = {
        "used_llm": False,
        "suggestions": [],
        "reason": "disabled",
    }
    if bool(payload.include_llm_suggestions):
        llm_overwrite_suggestions = _suggest_overwrite_labels_with_llm(
            reference_name=reference_name,
            overwrite_target_details=overwrite_target_details,
            existing_field_labels=existing_field_labels,
            candidate_labels=label_candidates,
        )

    return {
        "success": True,
        "result": {
            "reference_name": reference_name,
            "document_kind": str(payload.document_kind or entry.get("document_kind") or "").strip().lower() or None,
            "render_mode": render_mode,
            "template_path": str(template_path),
            "field_labels": existing_field_labels,
            "workflow_overwrite_targets": overwrite_targets,
            "pdf_label_candidates": label_candidates,
            "llm_overwrite_suggestions": llm_overwrite_suggestions,
            "inferred_rewrite_layout": inferred_layout,
            "inferred_qr_bill_region": inferred_qr_region,
            "inferred_qr_defaults": inferred_qr_defaults,
            "resolved_qr_data": merged_qr_data,
        },
    }


@router.post("/template-references/{reference_name}/apply-field-label-suggestions")
def apply_template_reference_field_label_suggestions(
    reference_name: str,
    payload: TemplateReferenceApplyFieldLabelsRequest,
) -> dict[str, Any]:
    normalized = str(reference_name or "").strip().lower()
    if not normalized:
        raise HTTPException(status_code=400, detail="reference_name is required")

    registry = _load_template_reference_registry()
    target_index = -1
    for idx, row in enumerate(registry):
        if str(row.get("reference_name") or "").strip().lower() == normalized:
            target_index = idx
            break
    if target_index < 0:
        raise HTTPException(status_code=404, detail=f"Template reference '{reference_name}' not found")

    entry = dict(registry[target_index])
    linked_workflow_schema = _load_linked_workflow_schema_fields(entry)
    overwrite_targets = linked_workflow_schema.get("overwrite_targets") if isinstance(linked_workflow_schema.get("overwrite_targets"), dict) else {"fields": [], "details": []}
    workflow_fields = overwrite_targets.get("fields") if isinstance(overwrite_targets.get("fields"), list) else []
    allowed_workflow_fields = {str(item or "").strip() for item in workflow_fields if str(item or "").strip()}

    apply_map, skipped_fields = _build_field_labels_to_apply(payload, allowed_workflow_fields=allowed_workflow_fields)

    base_payload = dict(entry.get("base_payload") if isinstance(entry.get("base_payload"), dict) else {})
    field_rules = dict(base_payload.get("field_rules") if isinstance(base_payload.get("field_rules"), dict) else {})
    field_labels = dict(field_rules.get("field_labels") if isinstance(field_rules.get("field_labels"), dict) else {})

    applied_fields: list[str] = []
    for field_name, labels in apply_map.items():
        merged_value = _merge_field_label_values(field_labels.get(field_name), labels)
        field_labels[field_name] = merged_value
        applied_fields.append(field_name)

    field_rules["field_labels"] = field_labels
    base_payload["field_rules"] = field_rules
    entry["base_payload"] = base_payload
    registry[target_index] = entry
    _save_template_reference_registry(registry)

    return {
        "success": True,
        "result": {
            "reference_name": str(entry.get("reference_name") or reference_name),
            "applied_fields": sorted(set(applied_fields)),
            "skipped_fields": skipped_fields,
            "field_labels": field_labels,
            "workflow_overwrite_targets": overwrite_targets,
        },
    }


@router.post("/template-references/generate")
def generate_from_template_reference(payload: TemplateReferenceGenerateRequest) -> dict[str, Any]:
    result = _generate_from_reference_internal(
        reference_name=payload.reference_name,
        specification=payload.specification,
        output_format=payload.output_format,
        document_kind=payload.document_kind,
        payload=payload.payload,
        override_fields=payload.override_fields,
        output_path=payload.output_path,
        output_dir=payload.output_dir,
    )

    return {
        "success": True,
        "result": result,
    }


@router.post("/template-references/generate-invoice")
def generate_invoice_from_template_reference(payload: InvoiceFromReferenceGenerateRequest) -> dict[str, Any]:
    result = _generate_invoice_from_template_reference_internal(payload)
    return {
        "success": True,
        "result": result,
    }


def _generate_invoice_from_template_reference_internal(payload: InvoiceFromReferenceGenerateRequest) -> dict[str, Any]:
    entry = _find_template_reference(payload.reference_name)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Template reference '{payload.reference_name}' not found")

    linked_workflow = _validate_template_linked_workflow_for_generation(entry)

    base_payload = dict(entry.get("base_payload") if isinstance(entry.get("base_payload"), dict) else {})
    effective_payload = _merge_payload_with_override_fields(base_payload, payload.payload, payload.override_fields)

    base_qr = dict(base_payload.get("qr_data") if isinstance(base_payload.get("qr_data"), dict) else {})
    payload_qr = dict(effective_payload.get("qr_data") if isinstance(effective_payload.get("qr_data"), dict) else {})
    request_qr = dict(payload.qr_data if isinstance(payload.qr_data, dict) else {})
    merged_qr = dict(base_qr)
    merged_qr.update(payload_qr)
    merged_qr.update(request_qr)

    workflow_result = _run_template_generation_workflow(
        linked_workflow,
        reference_name=payload.reference_name,
        document_kind=payload.document_kind or "invoice",
        specification=payload.specification,
        payload=effective_payload,
        qr_data=merged_qr,
        request=payload,
    )
    effective_payload = dict(workflow_result.get("payload") or effective_payload)
    merged_qr = dict(workflow_result.get("qr_data") or merged_qr)
    reference_policy = dict(workflow_result.get("reference_policy") or {})
    default_sequence_seed = _extract_default_sequence_seed_from_template_entry(entry)

    if payload.persist_reference_state:
        generated_reference = _reserve_generated_reference_atomic(
            payload,
            effective_payload,
            merged_qr,
            reference_policy,
            default_current_reference="",
            default_current_sequence=default_sequence_seed,
        )
    else:
        generated_reference = _build_generated_reference(
            payload,
            effective_payload,
            merged_qr,
            reference_policy,
            default_current_reference="",
            default_current_sequence=default_sequence_seed,
        )
    final_payload = _merge_invoice_payload_with_generated_reference(effective_payload, merged_qr, generated_reference, reference_policy)

    result = _generate_from_reference_internal(
        reference_name=payload.reference_name,
        specification=payload.specification,
        output_format=payload.output_format,
        document_kind=payload.document_kind or "invoice",
        payload=final_payload,
        override_fields=[],
        output_path=payload.output_path,
        output_dir=payload.output_dir,
    )

    result["generated_reference"] = generated_reference
    if workflow_result.get("workflow_run") is not None:
        result["workflow_run"] = workflow_result.get("workflow_run")
    return result


@router.post("/template-references/generate-invoice-from-instruction")
def generate_invoice_from_instruction(payload: InvoiceFromInstructionGenerateRequest) -> dict[str, Any]:
    instruction_text = str(payload.instruction_text or "").strip()
    if not instruction_text:
        raise HTTPException(status_code=400, detail="instruction_text is required")

    solf_generation: dict[str, Any]
    reused_rule: dict[str, Any] | None = None

    if payload.reuse_existing_solf_rule:
        explicit_rule_name = str(payload.existing_rule_name or "").strip()
        if explicit_rule_name:
            reused_rule = _find_active_solf_rule_by_name(explicit_rule_name)

        if reused_rule is None:
            inferred_rule_name = str((payload.invoice_request.reference_name or "").strip())
            if inferred_rule_name:
                reused_rule = _find_active_solf_rule_by_name(inferred_rule_name)

    if reused_rule is not None:
        solf_generation = {
            "reused": True,
            "rule_id": reused_rule.get("rule_id"),
            "rule_name": reused_rule.get("rule_name"),
            "is_active": bool(reused_rule.get("is_active")),
            "source": "existing_active_rule",
        }
    else:
        try:
            solf_generation = business_rules_engine.generate_and_process_solf_from_intent(
                intent=instruction_text,
                persist=bool(payload.persist_generated_solf),
                created_by=str(payload.solf_created_by or "api:user"),
                is_active=bool(payload.solf_is_active),
                default_clause_type=str(payload.solf_default_clause_type or "resolve_policy"),
                temperature=float(payload.temperature),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:
            LOGGER.exception("Failed to generate runtime SOLF from instruction")
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    invoice_result = _generate_invoice_from_template_reference_internal(payload.invoice_request)
    return {
        "success": True,
        "result": {
            "instruction_text": instruction_text,
            "solf_generation": solf_generation,
            "qr_generation": (invoice_result.get("qr_generation") if isinstance(invoice_result, dict) else None),
            "invoice_generation": invoice_result,
        },
    }


@router.post("/template-references/validate-reference")
def validate_swiss_reference(payload: SwissReferenceValidateRequest) -> dict[str, Any]:
    generator = SwissQRReferenceGenerator()
    raw = str(payload.reference or "")
    is_valid = generator.validate(raw)
    if not is_valid:
        return {
            "success": True,
            "valid": False,
            "detail": "invalid Swiss QRR reference (must be 27 digits with valid Mod10 check digit)",
        }

    parsed = generator.parse(raw)
    return {
        "success": True,
        "valid": True,
        "storage_string": parsed.storage,
        "visual_string": parsed.visual,
    }


@router.post("/template-references/run-sales-pipeline")
def run_sales_pipeline_from_references(payload: SalesPipelineGenerateRequest) -> dict[str, Any]:
    stage_order = [str(item or "").strip().lower() for item in (payload.stages or []) if str(item or "").strip()]
    if not stage_order:
        stage_order = ["quotation", "order_confirmation", "delivery", "invoice", "payment"]

    pipeline_slug = _slugify(payload.pipeline_name or "sales_pipeline")
    base_output_dir = str(payload.output_dir or "").strip() or f"generated/docs/{pipeline_slug}"

    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    connection = object_db.get_connection()
    try:
        _ensure_sales_pipeline_tables(connection)
        run_record = _create_sales_pipeline_run(
            connection,
            pipeline_name=str(payload.pipeline_name or "sales_pipeline").strip() or "sales_pipeline",
            stage_count=len(stage_order),
            output_dir=base_output_dir,
            payload={
                "stages": stage_order,
                "stage_reference_names": payload.stage_reference_names,
                "specification": payload.specification,
                "stage_specifications": payload.stage_specifications,
                "output_format": payload.output_format,
                "common_payload": payload.common_payload,
                "stage_payloads": payload.stage_payloads,
                "continue_on_error": payload.continue_on_error,
            },
        )

        for index, stage in enumerate(stage_order, start=1):
            ref_name = str((payload.stage_reference_names or {}).get(stage) or "").strip()
            if not ref_name:
                error_text = f"No template reference specified for stage '{stage}'."
                failures.append({
                    "stage": stage,
                    "error": error_text,
                })
                _upsert_sales_pipeline_stage(
                    connection,
                    run_id=run_record["run_id"],
                    stage_order=index,
                    stage_name=stage,
                    reference_name=ref_name,
                    status="failed",
                    error_message=error_text,
                    result={"stage": stage, "error": error_text},
                )
                if not payload.continue_on_error:
                    break
                continue

            stage_payload = dict(payload.common_payload or {})
            stage_overlay = (payload.stage_payloads or {}).get(stage)
            if isinstance(stage_overlay, dict):
                stage_payload.update(stage_overlay)
            stage_payload.setdefault("sales_stage", stage)

            stage_spec = str((payload.stage_specifications or {}).get(stage) or payload.specification or "").strip()
            stage_out_dir = f"{base_output_dir}/{index:02d}-{_slugify(stage)}"

            _upsert_sales_pipeline_stage(
                connection,
                run_id=run_record["run_id"],
                stage_order=index,
                stage_name=stage,
                reference_name=ref_name,
                status="running",
                result={"stage": stage, "started_at": _utc_iso_now()},
            )

            try:
                generated = _generate_from_reference_internal(
                    reference_name=ref_name,
                    specification=stage_spec,
                    output_format=payload.output_format,
                    document_kind=stage,
                    payload=stage_payload,
                    override_fields=[],
                    output_path="",
                    output_dir=stage_out_dir,
                )
                results.append({"stage": stage, "result": generated})
                _upsert_sales_pipeline_stage(
                    connection,
                    run_id=run_record["run_id"],
                    stage_order=index,
                    stage_name=stage,
                    reference_name=ref_name,
                    status="generated",
                    result=generated,
                )
            except HTTPException as exc:
                error_text = str(exc.detail)
                failures.append({"stage": stage, "error": error_text})
                _upsert_sales_pipeline_stage(
                    connection,
                    run_id=run_record["run_id"],
                    stage_order=index,
                    stage_name=stage,
                    reference_name=ref_name,
                    status="failed",
                    error_message=error_text,
                    result={"stage": stage, "error": error_text},
                )
                if not payload.continue_on_error:
                    break
            except Exception as exc:
                error_text = str(exc)
                failures.append({"stage": stage, "error": error_text})
                _upsert_sales_pipeline_stage(
                    connection,
                    run_id=run_record["run_id"],
                    stage_order=index,
                    stage_name=stage,
                    reference_name=ref_name,
                    status="failed",
                    error_message=error_text,
                    result={"stage": stage, "error": error_text},
                )
                if not payload.continue_on_error:
                    break

        _finalize_sales_pipeline_run(
            connection,
            run_id=run_record["run_id"],
            generated_count=len(results),
            failed_count=len(failures),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return {
        "success": len(failures) == 0,
        "run_id": run_record.get("run_id"),
        "run_key": run_record.get("run_key"),
        "pipeline_name": payload.pipeline_name,
        "stages": stage_order,
        "generated_count": len(results),
        "failed_count": len(failures),
        "results": results,
        "failures": failures,
        "no_booking": True,
        "output_dir": base_output_dir,
    }


@router.get("/template-references/sales-pipeline-runs")
def list_sales_pipeline_runs(limit: int = 50) -> dict[str, Any]:
    effective_limit = max(1, min(int(limit or 50), 500))
    connection = object_db.get_connection()
    try:
        _ensure_sales_pipeline_tables(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id, run_key, pipeline_name, status, stage_count,
                       generated_count, failed_count, no_booking, output_dir,
                       created_at, updated_at
                FROM sales_pipeline_run
                ORDER BY run_id DESC
                LIMIT %s
                """,
                (effective_limit,),
            )
            rows = cursor.fetchall() or []

        result = [
            {
                "run_id": int(row[0]),
                "run_key": row[1],
                "pipeline_name": row[2],
                "status": row[3],
                "stage_count": int(row[4] or 0),
                "generated_count": int(row[5] or 0),
                "failed_count": int(row[6] or 0),
                "no_booking": bool(row[7]),
                "output_dir": row[8],
                "created_at": row[9],
                "updated_at": row[10],
            }
            for row in rows
        ]
        return {"success": True, "count": len(result), "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to list sales pipeline runs")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/template-references/sales-pipeline-runs/{run_key}")
def get_sales_pipeline_run(run_key: str) -> dict[str, Any]:
    key = str(run_key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="run_key is required")

    connection = object_db.get_connection()
    try:
        _ensure_sales_pipeline_tables(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id, run_key, pipeline_name, status, stage_count,
                       generated_count, failed_count, no_booking, output_dir,
                       payload, created_at, updated_at
                FROM sales_pipeline_run
                WHERE run_key = %s
                LIMIT 1
                """,
                (key,),
            )
            run_row = cursor.fetchone()
            if not run_row:
                raise HTTPException(status_code=404, detail=f"Sales pipeline run '{key}' not found")

            cursor.execute(
                """
                SELECT stage_order, stage_name, reference_name, status, error_message,
                       saved_path, filename, media_type, size_bytes, result,
                       created_at, updated_at
                FROM sales_pipeline_stage
                WHERE run_id = %s
                ORDER BY stage_order ASC
                """,
                (int(run_row[0]),),
            )
            stage_rows = cursor.fetchall() or []

        run_payload = run_row[9] if isinstance(run_row[9], dict) else {}
        stages = [
            {
                "stage_order": int(row[0]),
                "stage_name": row[1],
                "reference_name": row[2],
                "status": row[3],
                "error_message": row[4],
                "saved_path": row[5],
                "filename": row[6],
                "media_type": row[7],
                "size_bytes": int(row[8] or 0) if row[8] is not None else None,
                "result": row[9] if isinstance(row[9], dict) else {},
                "created_at": row[10],
                "updated_at": row[11],
            }
            for row in stage_rows
        ]

        return {
            "success": True,
            "result": {
                "run_id": int(run_row[0]),
                "run_key": run_row[1],
                "pipeline_name": run_row[2],
                "status": run_row[3],
                "stage_count": int(run_row[4] or 0),
                "generated_count": int(run_row[5] or 0),
                "failed_count": int(run_row[6] or 0),
                "no_booking": bool(run_row[7]),
                "output_dir": run_row[8],
                "payload": run_payload,
                "created_at": run_row[10],
                "updated_at": run_row[11],
                "stages": stages,
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to load sales pipeline run")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.patch("/template-references/sales-pipeline-runs/{run_key}/stages/{stage_name}")
def sync_sales_pipeline_stage(
    run_key: str,
    stage_name: str,
    payload: SalesPipelineStageSyncRequest,
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        _ensure_sales_pipeline_tables(connection)
        stage_result = _sync_sales_pipeline_stage_internal(
            connection,
            run_key=run_key,
            stage_name=stage_name,
            payload=payload,
        )
        run_summary = _refresh_sales_pipeline_run_aggregate(connection, run_id=stage_result["run_id"])
        connection.commit()

        return {
            "success": True,
            "run": run_summary,
            "stage": {k: v for k, v in stage_result.items() if k != "run_id"},
        }
    except HTTPException:
        connection.rollback()
        raise
    except Exception as exc:
        connection.rollback()
        LOGGER.exception("Failed to sync sales pipeline stage")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.patch("/template-references/sales-pipeline-runs/{run_key}/stages/sync-bulk")
def sync_sales_pipeline_stages_bulk(run_key: str, payload: SalesPipelineStageBulkSyncRequest) -> dict[str, Any]:
    updates = payload.updates or []
    if not updates:
        raise HTTPException(status_code=400, detail="updates must contain at least one stage update")

    connection = object_db.get_connection()
    try:
        _ensure_sales_pipeline_tables(connection)
        applied: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        skipped_count = 0
        run_id: int | None = None

        for item in updates:
            try:
                stage_result = _sync_sales_pipeline_stage_internal(
                    connection,
                    run_key=run_key,
                    stage_name=item.stage_name,
                    payload=item,
                )
                run_id = stage_result["run_id"]
                applied.append({k: v for k, v in stage_result.items() if k != "run_id"})
                if bool(stage_result.get("skipped")):
                    skipped_count += 1
            except HTTPException as exc:
                failures.append(
                    {
                        "stage_name": str(item.stage_name or "").strip().lower(),
                        "error": str(exc.detail),
                        "status_code": int(exc.status_code),
                    }
                )
                if not payload.continue_on_error:
                    break

        if run_id is None:
            raise HTTPException(status_code=404, detail=f"Sales pipeline run '{run_key}' not found or no valid stage update was applied")

        run_summary = _refresh_sales_pipeline_run_aggregate(connection, run_id=run_id)
        connection.commit()

        return {
            "success": len(failures) == 0,
            "run": run_summary,
            "applied_count": len(applied),
            "skipped_count": skipped_count,
            "failed_count": len(failures),
            "applied": applied,
            "failures": failures,
        }
    except HTTPException:
        connection.rollback()
        raise
    except Exception as exc:
        connection.rollback()
        LOGGER.exception("Failed to bulk sync sales pipeline stages")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/template-render")
async def render_document_template(
    template_file: UploadFile = File(...),
    template_format: str = Form(default="docx"),
    output_format: str = Form(default="pdf"),
    document_kind: str = Form(default="invoice"),
    payload_json: str = Form(default="{}"),
    qr_json: str = Form(default="{}"),
    layout_json: str = Form(default="{}"),
) -> Response:
    template_bytes = await template_file.read()
    payload = parse_json_dict(payload_json, "payload_json")
    qr_data = parse_json_dict(qr_json, "qr_json")
    layout = parse_json_dict(layout_json, "layout_json")

    try:
        content, filename, media_type = document_template_pipeline.render_template_document(
            template_bytes=template_bytes,
            template_format=template_format,
            output_format=output_format,
            document_kind=document_kind,
            payload=payload,
            qr_data=qr_data if qr_data else None,
            layout=layout if layout else None,
            template_name=template_file.filename or "template",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        LOGGER.exception("Failed to render template document")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
    }
    return Response(content=content, media_type=media_type, headers=headers)