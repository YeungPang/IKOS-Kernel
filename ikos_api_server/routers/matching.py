from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import business_rules
import tx_match_index


router = APIRouter(prefix="/api/matching", tags=["matching"])


def _workflow_alias_prefix(workflow_key: str) -> str:
    value = str(workflow_key or "").strip().lower()
    value = re.sub(r"[^a-z0-9_\-]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "workflow"


class MatchingQueryRequest(BaseModel):
    query_text: str = Field(..., description="Natural-language query text for semantic matching")
    purpose: str | None = Field(default=None, description="Mandatory purpose filter (e.g., transaction_match, entity_resolution)")
    workflow_scope: str | None = Field(default=None, description="Mandatory workflow scope filter")
    tenant_scope: str = Field(..., description="Mandatory tenant scope filter")
    top_k: int = Field(default=10, ge=1, le=50, description="Maximum results")
    collection_name: str | None = Field(default=None, description="Optional collection override")
    workflow_key: str | None = Field(default=None, description="Optional workflow key used for alias resolution")
    purpose_alias: str | None = Field(default=None, description="Optional workflow resource alias for purpose")
    collection_alias: str | None = Field(default=None, description="Optional workflow resource alias for collection")
    workflow_scope_alias: str | None = Field(default=None, description="Optional workflow resource alias for workflow scope")


class MatchingCandidatesListRequest(BaseModel):
    purpose: str | None = Field(default=None, description="Mandatory purpose filter")
    workflow_scope: str | None = Field(default=None, description="Mandatory workflow scope filter")
    tenant_scope: str = Field(..., description="Mandatory tenant scope filter")
    limit: int = Field(default=50, ge=1, le=200, description="Maximum number of candidates to return")
    cursor: str | None = Field(default=None, description="Optional pagination cursor from previous response")
    collection_name: str | None = Field(default=None, description="Optional collection override")
    doc_type: str | None = Field(default=None, description="Optional document type filter")
    workflow_key: str | None = Field(default=None, description="Optional workflow key used for alias resolution")
    purpose_alias: str | None = Field(default=None, description="Optional workflow resource alias for purpose")
    collection_alias: str | None = Field(default=None, description="Optional workflow resource alias for collection")
    workflow_scope_alias: str | None = Field(default=None, description="Optional workflow resource alias for workflow scope")


def _resolve_alias_value(workflow_key: str, alias_name: str, resource_type: str) -> str:
    alias = business_rules.resolve_workflow_resource_alias(
        workflow_key=workflow_key,
        alias_name=alias_name,
        resource_type=resource_type,
    )
    if alias is None:
        raise HTTPException(status_code=404, detail=f"Workflow alias not found: {alias_name}")
    return str(alias.get("canonical_name") or "").strip()


def _resolve_matching_scope(
    *,
    workflow_key: str,
    explicit_scope: str | None,
    scope_alias: str | None,
) -> str:
    if explicit_scope and str(explicit_scope).strip():
        return str(explicit_scope).strip()
    if workflow_key and scope_alias:
        return _resolve_alias_value(workflow_key, str(scope_alias).strip(), "scope")
    if workflow_key:
        normalized = _workflow_alias_prefix(workflow_key)
        default_alias = f"{normalized}::workflow_scope"
        resolved = business_rules.resolve_workflow_resource_alias(
            workflow_key=workflow_key,
            alias_name=default_alias,
            resource_type="scope",
        )
        if resolved is not None:
            return str(resolved.get("canonical_name") or "").strip()
    return ""


def _resolve_matching_purpose(
    *,
    workflow_key: str,
    explicit_purpose: str | None,
    purpose_alias: str | None,
) -> str:
    if explicit_purpose and str(explicit_purpose).strip():
        return str(explicit_purpose).strip().lower()
    if workflow_key and purpose_alias:
        return _resolve_alias_value(workflow_key, str(purpose_alias).strip(), "purpose").lower()
    if workflow_key:
        normalized = _workflow_alias_prefix(workflow_key)
        default_alias = f"{normalized}::matching_purpose"
        resolved = business_rules.resolve_workflow_resource_alias(
            workflow_key=workflow_key,
            alias_name=default_alias,
            resource_type="purpose",
        )
        if resolved is not None:
            return str(resolved.get("canonical_name") or "").strip().lower()
    return ""


def _resolve_matching_collection(
    *,
    workflow_key: str,
    explicit_collection: str | None,
    collection_alias: str | None,
) -> str:
    if explicit_collection and str(explicit_collection).strip():
        return str(explicit_collection).strip()
    if workflow_key and collection_alias:
        return _resolve_alias_value(workflow_key, str(collection_alias).strip(), "collection")
    if workflow_key:
        normalized = _workflow_alias_prefix(workflow_key)
        default_alias = f"{normalized}::matching_collection"
        resolved = business_rules.resolve_workflow_resource_alias(
            workflow_key=workflow_key,
            alias_name=default_alias,
            resource_type="collection",
        )
        if resolved is not None:
            return str(resolved.get("canonical_name") or "").strip()
    return ""


@router.post("/query")
def query_matching(payload: MatchingQueryRequest) -> dict[str, Any]:
    workflow_key = str(payload.workflow_key or "").strip()
    purpose = _resolve_matching_purpose(
        workflow_key=workflow_key,
        explicit_purpose=payload.purpose,
        purpose_alias=payload.purpose_alias,
    )
    workflow_scope = _resolve_matching_scope(
        workflow_key=workflow_key,
        explicit_scope=payload.workflow_scope,
        scope_alias=payload.workflow_scope_alias,
    )
    collection_name = _resolve_matching_collection(
        workflow_key=workflow_key,
        explicit_collection=payload.collection_name,
        collection_alias=payload.collection_alias,
    )
    if not purpose or not workflow_scope:
        raise HTTPException(status_code=400, detail="purpose and workflow_scope are required, directly or via workflow aliases")
    if not tx_match_index.is_allowed_purpose(purpose):
        raise HTTPException(status_code=403, detail=f"Purpose not allowed: {purpose}")

    result = tx_match_index.query_rows(
        query_text=payload.query_text,
        purpose=purpose,
        workflow_scope=workflow_scope,
        tenant_scope=str(payload.tenant_scope or "").strip(),
        top_k=int(payload.top_k),
        collection_name=collection_name,
    )

    if not bool(result.get("ok")):
        raise HTTPException(status_code=400, detail=str(result.get("reason") or "matching_query_failed"))

    return {"success": True, "result": result}


@router.post("/workflow/{workflow_key}/query")
def query_matching_for_workflow(workflow_key: str, payload: MatchingQueryRequest) -> dict[str, Any]:
    merged = payload.model_copy(update={"workflow_key": workflow_key})
    return query_matching(merged)


@router.post("/candidates")
def list_matching_candidates(payload: MatchingCandidatesListRequest) -> dict[str, Any]:
    workflow_key = str(payload.workflow_key or "").strip()
    purpose = _resolve_matching_purpose(
        workflow_key=workflow_key,
        explicit_purpose=payload.purpose,
        purpose_alias=payload.purpose_alias,
    )
    workflow_scope = _resolve_matching_scope(
        workflow_key=workflow_key,
        explicit_scope=payload.workflow_scope,
        scope_alias=payload.workflow_scope_alias,
    )
    collection_name = _resolve_matching_collection(
        workflow_key=workflow_key,
        explicit_collection=payload.collection_name,
        collection_alias=payload.collection_alias,
    )
    if not purpose or not workflow_scope:
        raise HTTPException(status_code=400, detail="purpose and workflow_scope are required, directly or via workflow aliases")
    if not tx_match_index.is_allowed_purpose(purpose):
        raise HTTPException(status_code=403, detail=f"Purpose not allowed: {purpose}")

    result = tx_match_index.list_candidates(
        purpose=purpose,
        workflow_scope=workflow_scope,
        tenant_scope=str(payload.tenant_scope or "").strip(),
        limit=int(payload.limit),
        cursor=str(payload.cursor or "").strip(),
        collection_name=collection_name,
        doc_type=str(payload.doc_type or "").strip().lower(),
    )

    if not bool(result.get("ok")):
        raise HTTPException(status_code=400, detail=str(result.get("reason") or "matching_candidates_failed"))

    return {"success": True, "result": result}


@router.post("/workflow/{workflow_key}/candidates")
def list_matching_candidates_for_workflow(workflow_key: str, payload: MatchingCandidatesListRequest) -> dict[str, Any]:
    merged = payload.model_copy(update={"workflow_key": workflow_key})
    return list_matching_candidates(merged)
