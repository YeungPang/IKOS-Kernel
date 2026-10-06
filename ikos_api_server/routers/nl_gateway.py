from __future__ import annotations

import json
import re
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from ikos_config import EXTRACT_MODEL
from llm_fallback import generate_content_with_openrouter_fallback, get_openrouter_client


router = APIRouter(prefix="/api/nl", tags=["nl-gateway"])

_HTTP_METHODS = {"get", "post", "put", "delete", "patch"}


class NLExecuteRequest(BaseModel):
    text: str = Field(..., description="Natural-language intent for an IDMS API action")
    execute: bool = Field(default=False, description="When true, execute selected action. When false, return plan only")
    allow_mutation: bool = Field(
        default=False,
        description="Required true to execute non-GET actions",
    )
    context: dict[str, Any] = Field(default_factory=dict, description="Optional context to improve planning")
    max_candidates: int = Field(default=5, ge=1, le=20)
    temperature: float = Field(default=0.0, ge=0.0, le=1.0)


class NLCatalogRequest(BaseModel):
    include_methods: list[str] = Field(default_factory=list)


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    match = re.search(r"\{[\s\S]*\}", raw)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _safe_str(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text if text else default


def _build_openapi_catalog(request: Request) -> list[dict[str, Any]]:
    spec = request.app.openapi()
    paths = spec.get("paths") if isinstance(spec, dict) else {}
    if not isinstance(paths, dict):
        return []

    catalog: list[dict[str, Any]] = []
    for path, methods in paths.items():
        if not isinstance(methods, dict):
            continue
        for method, operation in methods.items():
            m = str(method or "").strip().lower()
            if m not in _HTTP_METHODS:
                continue
            op = operation if isinstance(operation, dict) else {}

            params = op.get("parameters") if isinstance(op.get("parameters"), list) else []
            path_params = []
            query_params = []
            for p in params:
                if not isinstance(p, dict):
                    continue
                name = _safe_str(p.get("name"))
                in_ = _safe_str(p.get("in")).lower()
                required = bool(p.get("required", False))
                item = {"name": name, "required": required}
                if in_ == "path":
                    path_params.append(item)
                elif in_ == "query":
                    query_params.append(item)

            body_keys: list[str] = []
            req_body = op.get("requestBody") if isinstance(op.get("requestBody"), dict) else {}
            content = req_body.get("content") if isinstance(req_body.get("content"), dict) else {}
            json_content = content.get("application/json") if isinstance(content.get("application/json"), dict) else {}
            schema = json_content.get("schema") if isinstance(json_content.get("schema"), dict) else {}
            props = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
            body_keys = sorted([str(key) for key in props.keys()])

            catalog.append(
                {
                    "method": m.upper(),
                    "path": str(path),
                    "summary": _safe_str(op.get("summary")),
                    "tags": op.get("tags") if isinstance(op.get("tags"), list) else [],
                    "path_params": path_params,
                    "query_params": query_params,
                    "body_keys": body_keys,
                }
            )

    catalog.sort(key=lambda item: (item["path"], item["method"]))
    return catalog


def _heuristic_account_definition_plan(text: str) -> dict[str, Any] | None:
    lower = str(text or "").lower()
    if "account" in lower and "number" in lower and "account type" in lower:
        return {
            "intent_summary": "Create account definition from natural language",
            "candidate_actions": [
                {
                    "method": "POST",
                    "path": "/api/domain-definitions/account-definition/from-nl",
                    "path_params": {},
                    "query_params": {},
                    "body": {
                        "text": str(text or "").strip(),
                        "legal_entity_ref": "global",
                        "created_by": "nl:user",
                    },
                    "confidence": 0.92,
                    "reason": "Matched account-definition creation phrasing",
                }
            ],
        }
    return None


def _plan_with_llm(
    *,
    text: str,
    context: dict[str, Any],
    catalog: list[dict[str, Any]],
    max_candidates: int,
    temperature: float,
) -> dict[str, Any]:
    heuristic = _heuristic_account_definition_plan(text)
    if heuristic:
        return heuristic

    client = get_openrouter_client()
    if client is None:
        return {
            "intent_summary": "LLM unavailable; no action plan generated",
            "candidate_actions": [],
        }

    trimmed_catalog = catalog[:400]
    prompt = (
        "You map natural-language user requests to HTTP API actions.\n"
        "Return JSON only with keys: intent_summary, candidate_actions.\n"
        "candidate_actions must be a list of objects with keys:\n"
        "method, path, path_params, query_params, body, confidence, reason.\n"
        "Use only method/path pairs from the catalog.\n"
        "Do not invent endpoints.\n"
        f"Max candidates: {int(max_candidates)}\n"
        f"User request: {json.dumps(str(text or '').strip())}\n"
        f"Context JSON: {json.dumps(context or {}, ensure_ascii=False)}\n"
        f"Catalog JSON: {json.dumps(trimmed_catalog, ensure_ascii=False)}\n"
    )

    response = generate_content_with_openrouter_fallback(
        primary_call=lambda: client.models.generate_content(
            model=EXTRACT_MODEL,
            contents=[prompt],
        ),
        model=EXTRACT_MODEL,
        contents=[prompt],
        temperature=float(temperature),
        call_name="nl_api_gateway_plan",
        complexity="medium",
    )
    parsed = _extract_json_object(getattr(response, "text", "") or "")
    if not parsed:
        return {
            "intent_summary": "LLM returned no valid JSON plan",
            "candidate_actions": [],
        }
    if not isinstance(parsed.get("candidate_actions"), list):
        parsed["candidate_actions"] = []
    return parsed


def _validate_and_normalize_plan(plan: dict[str, Any], catalog: list[dict[str, Any]]) -> dict[str, Any]:
    valid_pairs = {(item["method"], item["path"]) for item in catalog}

    candidates_raw = plan.get("candidate_actions") if isinstance(plan.get("candidate_actions"), list) else []
    candidates: list[dict[str, Any]] = []
    for item in candidates_raw:
        if not isinstance(item, dict):
            continue
        method = _safe_str(item.get("method")).upper()
        path = _safe_str(item.get("path"))
        if (method, path) not in valid_pairs:
            continue

        confidence_raw = item.get("confidence")
        try:
            confidence = float(confidence_raw)
        except Exception:
            confidence = 0.5
        confidence = max(0.0, min(confidence, 1.0))

        candidates.append(
            {
                "method": method,
                "path": path,
                "path_params": item.get("path_params") if isinstance(item.get("path_params"), dict) else {},
                "query_params": item.get("query_params") if isinstance(item.get("query_params"), dict) else {},
                "body": item.get("body") if isinstance(item.get("body"), dict) else {},
                "confidence": confidence,
                "reason": _safe_str(item.get("reason"), "LLM suggestion"),
            }
        )

    candidates.sort(key=lambda c: (-float(c.get("confidence") or 0.0), str(c.get("path") or "")))
    return {
        "intent_summary": _safe_str(plan.get("intent_summary"), "Planned API actions"),
        "candidate_actions": candidates,
    }


def _materialize_path(path_template: str, path_params: dict[str, Any]) -> str:
    out = str(path_template or "")
    tokens = re.findall(r"\{([^}]+)\}", out)
    for token in tokens:
        if token not in path_params:
            raise ValueError(f"Missing path param: {token}")
        value = urllib_parse.quote(str(path_params[token]))
        out = out.replace("{" + token + "}", value)
    return out


@router.post("/catalog")
def get_nl_catalog(payload: NLCatalogRequest, request: Request) -> dict[str, Any]:
    catalog = _build_openapi_catalog(request)
    methods = {str(item).strip().upper() for item in (payload.include_methods or []) if str(item).strip()}
    if methods:
        catalog = [item for item in catalog if str(item.get("method") or "").upper() in methods]
    return {"success": True, "count": len(catalog), "result": catalog}


@router.post("/execute")
def execute_nl_request(payload: NLExecuteRequest, request: Request) -> dict[str, Any]:
    text = _safe_str(payload.text)
    if not text:
        raise HTTPException(status_code=400, detail="text is required")

    catalog = _build_openapi_catalog(request)
    plan_raw = _plan_with_llm(
        text=text,
        context=payload.context if isinstance(payload.context, dict) else {},
        catalog=catalog,
        max_candidates=int(payload.max_candidates),
        temperature=float(payload.temperature),
    )
    plan = _validate_and_normalize_plan(plan_raw, catalog)

    if not plan.get("candidate_actions"):
        return {
            "success": True,
            "execute": False,
            "result": {
                "intent_summary": plan.get("intent_summary"),
                "requires_confirmation": False,
                "message": "No valid API action could be mapped from NL request.",
                "candidate_actions": [],
            },
        }

    selected = dict(plan["candidate_actions"][0])
    method = str(selected.get("method") or "GET").upper()
    path_template = str(selected.get("path") or "")

    if path_template.startswith("/api/nl/"):
        raise HTTPException(status_code=400, detail="NL gateway cannot recursively call /api/nl/* endpoints")

    requires_confirmation = method != "GET"
    if not payload.execute:
        return {
            "success": True,
            "execute": False,
            "result": {
                "intent_summary": plan.get("intent_summary"),
                "requires_confirmation": requires_confirmation,
                "message": "Plan generated. Set execute=true to run selected action.",
                "selected_action": selected,
                "candidate_actions": plan.get("candidate_actions"),
            },
        }

    if requires_confirmation and not bool(payload.allow_mutation):
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Executing non-GET actions requires allow_mutation=true",
                "selected_action": selected,
            },
        )

    try:
        final_path = _materialize_path(path_template, selected.get("path_params") if isinstance(selected.get("path_params"), dict) else {})
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    query_params = selected.get("query_params") if isinstance(selected.get("query_params"), dict) else {}
    qs = urllib_parse.urlencode(query_params, doseq=True)
    final_url = str(request.base_url).rstrip("/") + final_path + (("?" + qs) if qs else "")

    headers = {"Accept": "application/json"}
    data: bytes | None = None
    if method != "GET":
        headers["Content-Type"] = "application/json; charset=utf-8"
        data = json.dumps(selected.get("body") if isinstance(selected.get("body"), dict) else {}).encode("utf-8")

    req = urllib_request.Request(url=final_url, data=data, method=method, headers=headers)
    try:
        with urllib_request.urlopen(req, timeout=60) as resp:
            status_code = int(resp.status)
            text_out = resp.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as exc:
        status_code = int(getattr(exc, "code", 500) or 500)
        text_out = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else str(exc)
    except urllib_error.URLError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to call planned endpoint: {exc}") from exc

    try:
        response_payload: Any = json.loads(text_out)
    except Exception:
        response_payload = text_out

    ok = 200 <= status_code < 300
    if not ok:
        raise HTTPException(
            status_code=status_code,
            detail={
                "message": "Planned action execution failed",
                "selected_action": selected,
                "response": response_payload,
            },
        )

    return {
        "success": True,
        "execute": True,
        "result": {
            "intent_summary": plan.get("intent_summary"),
            "selected_action": selected,
            "status_code": status_code,
            "response": response_payload,
        },
    }
