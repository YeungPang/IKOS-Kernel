from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from ikos_api_server.schemas import SolfGenerationFromInspectionRequest

import business_rules
from ikos_api_server import deps
from ikos_api_server.schemas import (
    BusinessRuleActivationRequest,
    BusinessRuleCreateRequest,
    BusinessRuleDraftSimulationRequest,
    BusinessRuleSimulationRequest,
    SolfScriptRuleCreateRequest,
    SolfScriptRuleReviewRequest,
    SolfScriptRuleUpdateRequest,
    BusinessRuleUpdateRequest,
)
from workflow_process_taxonomy import list_workflow_process_taxonomy, suggest_workflow_processes


router = APIRouter(prefix="/api/business-rules", tags=["business-rules"])
LOGGER = logging.getLogger("idms.api")


class WorkflowProcessSuggestionRequest(BaseModel):
    text: str = Field(..., description="Natural language description of where rule should apply")
    domain: str | None = Field(default=None, description="Optional domain filter (e.g., accounting, hr, workflow)")
    top_k: int = Field(default=5, ge=1, le=20, description="Max number of candidates to return")


class WorkflowRegistryCreateRequest(BaseModel):
    workflow_key: str = Field(..., description="Stable workflow key")
    workflow_name: str = Field(..., description="Display name")
    description: str | None = Field(default=None)
    domain: str | None = Field(default=None)
    status: str = Field(default="draft")
    metadata: dict[str, Any] = Field(default_factory=dict)
    is_active: bool = Field(default=True)
    created_by: str | None = Field(default="api:user")


class WorkflowRegistryActivationRequest(BaseModel):
    is_active: bool = Field(...)


class WorkflowRegistrySyncRequest(BaseModel):
    created_by: str | None = Field(default="api:user")


class WorkflowRegistrySeedTemplatesRequest(BaseModel):
    created_by: str | None = Field(default="api:user")


class WorkflowDescriptionExportRequest(BaseModel):
    file_format: str = Field(default="md", description="Output format: md or txt")
    include_preprogrammed: bool = Field(default=True, description="Include built-in ingestion/query workflows")
    include_user_defined: bool = Field(default=True, description="Include user-defined workflows from registry")
    workflow_keys: list[str] | None = Field(default=None, description="Optional workflow_key filter")
    output_file_name: str | None = Field(default=None, description="Optional output file name")


class CreateBusinessRuleWithRegistryRequest(BaseModel):
    rule_text: str = Field(..., description="Natural language rule description")
    rule_name: str | None = Field(default=None, description="Optional rule name")
    created_by: str | None = Field(default="api:user")
    is_active: bool = Field(default=True)


class WorkflowVersionPublishRequest(BaseModel):
    workflow_version_id: int = Field(..., description="Version to publish")
    published_by: str | None = Field(default="api:user")


class WorkflowVersionCreateRequest(BaseModel):
    steps: list[dict[str, Any]] = Field(default_factory=list, description="Workflow steps for the new version")
    graph_spec: dict[str, Any] = Field(default_factory=dict, description="Optional workflow graph specification")
    input_contract: dict[str, Any] = Field(default_factory=dict, description="Optional input contract")
    output_contract: dict[str, Any] = Field(default_factory=dict, description="Optional output contract")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Optional version metadata")
    status: str = Field(default="draft", description="Version status: draft|review|published|deprecated|archived")
    created_by: str | None = Field(default="api:user", description="Creator label")


class WorkflowVersionRollbackRequest(BaseModel):
    target_version_id: int = Field(..., description="Version to roll back to")
    reason: str | None = Field(default=None, description="Reason for rollback")
    rolled_back_by: str | None = Field(default="api:user")


class WorkflowResourceAliasResolveRequest(BaseModel):
    workflow_key: str = Field(..., description="Workflow key")
    alias_name: str = Field(..., description="Logical alias name to resolve")
    resource_type: str | None = Field(default=None, description="Optional resource type filter")
    workflow_version_id: int | None = Field(default=None, description="Optional workflow version filter")


class WorkflowCapabilityValidationRequest(BaseModel):
    workflow_key: str | None = Field(default=None, description="Workflow key to validate")
    workflow_id: int | None = Field(default=None, description="Workflow id to validate")
    workflow_version_id: int | None = Field(default=None, description="Workflow version id to validate")
    workflow_name: str | None = Field(default=None, description="Optional workflow display name")
    proposed_steps: list[dict[str, Any]] | None = Field(default=None, description="Optional ad-hoc workflow steps to validate")
    requested_by: str | None = Field(default="api:user", description="Requester identity for issue template")


class WorkflowCapabilityIssueJsonRequest(WorkflowCapabilityValidationRequest):
    github_repository: str | None = Field(default=None, description="Optional repository in owner/repo form")
    github_owner: str | None = Field(default=None, description="Optional GitHub owner override")
    github_repo: str | None = Field(default=None, description="Optional GitHub repo name override")
    labels: list[str] | None = Field(default=None, description="Optional labels override")
    assignees: list[str] | None = Field(default=None, description="Optional assignees")
    milestone: int | None = Field(default=None, description="Optional milestone id")
    projects: list[str] | None = Field(default=None, description="Optional project identifiers")
    include_domain_label: bool = Field(default=True, description="Add domain:<workflow_domain> label when available")


class WorkflowCapabilityIssueCreateRequest(WorkflowCapabilityIssueJsonRequest):
    dry_run: bool = Field(default=True, description="When true, only returns payload and does not call GitHub")
    github_token: str | None = Field(default=None, description="Optional GitHub token override; env IKOS_GITHUB_TOKEN preferred")
    github_api_base_url: str | None = Field(default=None, description="Optional GitHub API base URL override")


class WorkflowExtensionPackCreateRequest(BaseModel):
    extension_key: str = Field(..., description="Stable extension key")
    extension_name: str = Field(..., description="Display name")
    description: str | None = Field(default=None)
    scope_json: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    is_active: bool = Field(default=True)
    created_by: str | None = Field(default="api:user")


class WorkflowExtensionVersionCreateRequest(BaseModel):
    extension_pack_id: int | None = Field(default=None)
    extension_key: str | None = Field(default=None)
    status: str = Field(default="draft")
    conditions_schema_version: str = Field(default="v1")
    metadata: dict[str, Any] = Field(default_factory=dict)
    is_active: bool = Field(default=True)
    rules: list[dict[str, Any]] = Field(default_factory=list)
    created_by: str | None = Field(default="api:user")


class WorkflowExtensionAttachmentCreateRequest(BaseModel):
    workflow_key: str = Field(..., description="Prime workflow key")
    workflow_version_id: int | None = Field(default=None)
    extension_pack_id: int | None = Field(default=None)
    extension_key: str | None = Field(default=None)
    extension_version_id: int | None = Field(default=None)
    effective_from: str | None = Field(default=None)
    effective_until: str | None = Field(default=None)
    priority: int = Field(default=100)
    tenant_scope: str | None = Field(default=None)
    metadata: dict[str, Any] = Field(default_factory=dict)
    is_active: bool = Field(default=True)
    created_by: str | None = Field(default="api:user")


class WorkflowExtensionAttachmentDetachRequest(BaseModel):
    attachment_id: int = Field(..., description="Attachment id to deactivate")


class WorkflowExtensionResolvePreviewRequest(BaseModel):
    workflow_key: str = Field(..., description="Prime workflow key")
    workflow_version_id: int | None = Field(default=None)
    run_status: str = Field(default="running")
    current_context: dict[str, Any] = Field(default_factory=dict)
    step_statuses: dict[str, Any] = Field(default_factory=dict)
    tenant_scope: str | None = Field(default=None)
    hook_point: str | None = Field(default=None)
    target_step_key: str | None = Field(default=None)


class WorkflowImpactAutoPlanRequest(BaseModel):
    change_type: str = Field(..., description="Type of new change, for example account_definition, business_rule, domain_definition")
    change_payload: dict[str, Any] = Field(default_factory=dict, description="Normalized change payload to analyze")
    workflow_keys: list[str] | None = Field(default=None, description="Optional workflow_key allowlist")
    domain_hint: str | None = Field(default=None, description="Optional explicit domain hint")
    top_k: int = Field(default=8, ge=1, le=50, description="Maximum impacted workflows to return")
    created_by: str | None = Field(default="api:user", description="Actor for optional persisted extension artifacts")
    persist: bool = Field(default=False, description="When true, persist generated extension pack + version")
    attach: bool = Field(default=False, description="When true (with persist), attach generated extension to impacted workflows")
    temperature: float = Field(default=0.0, ge=0.0, le=1.0, description="LLM temperature for impact planning")


class SolfGenerationOutputIngestRequest(BaseModel):
    llm_output: dict[str, Any] = Field(
        ...,
        description="LLM JSON output with rule_name, class_definitions, and solf_script",
    )
    persist: bool = Field(default=True, description="When true, persist validated SOLF into business rules")
    created_by: str | None = Field(default="api:user", description="Creator identifier when persisting")
    is_active: bool = Field(default=True, description="Activation flag applied when persisting")
    default_clause_type: str = Field(
        default="resolve_policy",
        description="Fallback SQL clause type for persisted clauses: resolve_policy | ingest_rule | computation_rule",
    )


class SolfGenerationFromIntentRequest(BaseModel):
    intent: str = Field(..., description="Plain-language business intent to convert into executable SOLF artifacts")
    persist: bool = Field(default=True, description="When true, persist validated SOLF into business rules")
    created_by: str | None = Field(default="api:user", description="Creator identifier when persisting")
    is_active: bool = Field(default=True, description="Activation flag applied when persisting")
    default_clause_type: str = Field(
        default="resolve_policy",
        description="Fallback SQL clause type for persisted clauses: resolve_policy | ingest_rule | computation_rule",
    )
    temperature: float = Field(default=0.0, ge=0.0, le=1.0, description="LLM sampling temperature")


class SolfGenerationFromInspectionRequest(BaseModel):
    goal: str = Field(..., description="Business goal or workflow intent to convert into executable SOLF artifacts")
    inspection_context: dict[str, Any] = Field(
        default_factory=dict,
        description="Structured inspection context gathered from web, internal DB, and external DB sources",
    )
    persist: bool = Field(default=True, description="When true, persist validated SOLF into business rules")
    created_by: str | None = Field(default="api:user", description="Creator identifier when persisting")
    is_active: bool = Field(default=True, description="Activation flag applied when persisting")
    default_clause_type: str = Field(
        default="resolve_policy",
        description="Fallback SQL clause type for persisted clauses: resolve_policy | ingest_rule | computation_rule",
    )
    temperature: float = Field(default=0.0, ge=0.0, le=1.0, description="LLM sampling temperature")
    workflow_registry: dict[str, Any] | None = Field(
        default=None,
        description="Optional workflow-registry payload to create or update alongside the generated SOLF artifacts",
    )


@router.post("")
def create_business_rule(payload: BusinessRuleCreateRequest) -> dict[str, Any]:
    try:
        result = business_rules.create_business_rule(
            rule_text=payload.rule_text,
            rule_name=payload.rule_name or None,
            created_by=payload.created_by,
            is_active=payload.is_active,
            application_mode=payload.application_mode or None,
            overwrite_existing=payload.overwrite_existing,
        )
        # Rebuild singleton tools so interaction interpreter sees newly activated rules.
        deps.get_tools.cache_clear()
        return {"success": True, "result": result}
    except business_rules.BusinessRuleClarificationNeeded as exc:
        raise HTTPException(
            status_code=409,
            detail={"clarification_required": True, "result": exc.clarification, "message": str(exc)},
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        LOGGER.exception("Failed to create business rule")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/solf-script/review")
def review_solf_script_rule(payload: SolfScriptRuleReviewRequest) -> dict[str, Any]:
    try:
        result = business_rules.review_solf_script_rule(
            solf_script=payload.solf_script,
            rule_name=payload.rule_name or None,
            default_clause_type=payload.default_clause_type,
        )
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to review SOLF script rule")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/solf-script")
def create_solf_script_rule(payload: SolfScriptRuleCreateRequest) -> dict[str, Any]:
    try:
        result = business_rules.create_business_rule_from_solf_script(
            solf_script=payload.solf_script,
            rule_name=payload.rule_name or None,
            created_by=payload.created_by,
            is_active=payload.is_active,
            default_clause_type=payload.default_clause_type,
            overwrite_existing=payload.overwrite_existing,
        )
        deps.get_tools.cache_clear()
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to create SOLF script rule")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.put("/{rule_id:int}/solf-script")
def update_solf_script_rule(rule_id: int, payload: SolfScriptRuleUpdateRequest) -> dict[str, Any]:
    try:
        result = business_rules.update_business_rule_from_solf_script(
            rule_id=rule_id,
            solf_script=payload.solf_script,
            rule_name=payload.rule_name or None,
            updated_by=payload.updated_by,
            is_active=payload.is_active,
            default_clause_type=payload.default_clause_type,
            overwrite_existing=payload.overwrite_existing,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"business rule {rule_id} not found")
        deps.get_tools.cache_clear()
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to update SOLF script rule")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("")
def list_business_rules(is_active: bool | None = None, limit: int = 200) -> dict[str, Any]:
    try:
        rules = business_rules.list_business_rules(is_active=is_active, limit=limit)
        return {"success": True, "count": len(rules), "result": rules}
    except Exception as exc:
        LOGGER.exception("Failed to list business rules")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{rule_id:int}")
def get_business_rule(rule_id: int) -> dict[str, Any]:
    try:
        rule = business_rules.get_business_rule(rule_id=rule_id)
        if rule is None:
            raise HTTPException(status_code=404, detail=f"business rule {rule_id} not found")
        return {"success": True, "result": rule}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to get business rule")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{rule_id:int}/nl")
def get_business_rule_nl_input(rule_id: int) -> dict[str, Any]:
    try:
        rule = business_rules.get_business_rule(rule_id=rule_id)
        if rule is None:
            raise HTTPException(status_code=404, detail=f"business rule {rule_id} not found")

        created_at = rule.get("created_at")
        modified_at = rule.get("modified_at")
        return {
            "success": True,
            "result": {
                "rule_id": int(rule.get("rule_id") or 0),
                "rule_name": str(rule.get("rule_name") or ""),
                "rule_text": str(rule.get("rule_text") or ""),
                "is_active": bool(rule.get("is_active")),
                "created_by": str(rule.get("created_by") or ""),
                "created_at": created_at.isoformat() if created_at is not None else None,
                "modified_at": modified_at.isoformat() if modified_at is not None else None,
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to get business rule NL input")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.put("/{rule_id:int}")
def update_business_rule(rule_id: int, payload: BusinessRuleUpdateRequest) -> dict[str, Any]:
    try:
        result = business_rules.update_business_rule(
            rule_id=rule_id,
            rule_text=payload.rule_text,
            rule_name=payload.rule_name or None,
            updated_by=payload.updated_by,
            is_active=payload.is_active,
            application_mode=payload.application_mode or None,
            overwrite_existing=payload.overwrite_existing,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"business rule {rule_id} not found")
        deps.get_tools.cache_clear()
        return {"success": True, "result": result}
    except business_rules.BusinessRuleClarificationNeeded as exc:
        raise HTTPException(
            status_code=409,
            detail={"clarification_required": True, "result": exc.clarification, "message": str(exc)},
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to update business rule")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/{rule_id:int}")
def deactivate_business_rule(rule_id: int, updated_by: str = "api:user") -> dict[str, Any]:
    try:
        updated = business_rules.deactivate_business_rule(rule_id=rule_id, updated_by=updated_by)
        if not updated:
            raise HTTPException(status_code=404, detail=f"business rule {rule_id} not found")
        deps.get_tools.cache_clear()
        return {
            "success": True,
            "rule_id": int(rule_id),
            "is_active": False,
            "action": "deactivated",
        }
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to deactivate business rule")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{rule_id:int}/artifacts")
def get_business_rule_artifacts(rule_id: int) -> dict[str, Any]:
    try:
        result = business_rules.get_business_rule_artifacts(rule_id=rule_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"business rule {rule_id} not found")
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to get business rule artifacts")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{rule_id:int}/activation")
def set_business_rule_activation(rule_id: int, payload: BusinessRuleActivationRequest) -> dict[str, Any]:
    try:
        updated = business_rules.activate_business_rule(rule_id=rule_id, is_active=payload.is_active)
        deps.get_tools.cache_clear()
        return {
            "success": bool(updated),
            "rule_id": int(rule_id),
            "is_active": bool(payload.is_active),
        }
    except Exception as exc:
        LOGGER.exception("Failed to update business rule activation")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/simulate")
def simulate_business_rule(payload: BusinessRuleSimulationRequest) -> dict[str, Any]:
    try:
        result = business_rules.simulate_business_rule_resolution(
            requested_attribute=payload.requested_attribute,
            context=payload.context,
            sample_attributes=payload.sample_attributes,
            rule_id=payload.rule_id,
            include_inactive=payload.include_inactive,
        )
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to simulate business rule")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/simulate-draft")
def simulate_draft_business_rule(payload: BusinessRuleDraftSimulationRequest) -> dict[str, Any]:
    try:
        result = business_rules.simulate_draft_business_rule(
            rule_text=payload.rule_text,
            rule_name=payload.rule_name or None,
            requested_attribute=payload.requested_attribute,
            application_mode=payload.application_mode or None,
            context=payload.context,
            sample_attributes=payload.sample_attributes,
        )
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to simulate draft business rule")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/review-draft")
def review_draft_business_rule(payload: BusinessRuleDraftSimulationRequest) -> dict[str, Any]:
    try:
        draft = business_rules.simulate_draft_business_rule(
            rule_text=payload.rule_text,
            rule_name=payload.rule_name or None,
            requested_attribute=payload.requested_attribute,
            context=payload.context,
            sample_attributes=payload.sample_attributes,
        )
        artifacts = business_rules.build_rule_artifacts_view(
            structured_rule=draft.get("structured_rule") if isinstance(draft.get("structured_rule"), dict) else {},
            solf_script=str(draft.get("solf_script") or ""),
            scope=draft.get("scope") if isinstance(draft.get("scope"), dict) else {},
        )
        return {
            "success": True,
            "result": {
                "rule_name": str(draft.get("rule_name") or ""),
                "class_definition_source": str(draft.get("class_definition_source") or "none"),
                "artifacts": artifacts,
            },
        }
    except Exception as exc:
        LOGGER.exception("Failed to review draft business rule")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/solf-generation-pack")
def get_solf_generation_pack(include_markdown: bool = True) -> dict[str, Any]:
    try:
        result = business_rules.get_solf_llm_generation_pack(include_markdown=include_markdown)
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to get SOLF generation pack")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/solf-generation-pack/ingest")
def ingest_solf_generation_output(payload: SolfGenerationOutputIngestRequest) -> dict[str, Any]:
    try:
        result = business_rules.process_solf_llm_generation_output(
            llm_output=payload.llm_output,
            persist=payload.persist,
            created_by=payload.created_by,
            is_active=payload.is_active,
            default_clause_type=payload.default_clause_type,
        )
        if bool(result.get("persisted")):
            deps.get_tools.cache_clear()
        return {"success": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        LOGGER.exception("Failed to ingest SOLF generation output")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/solf-generation-pack/generate-and-ingest")
def generate_and_ingest_solf_from_intent(payload: SolfGenerationFromIntentRequest) -> dict[str, Any]:
    try:
        result = business_rules.generate_and_process_solf_from_intent(
            intent=payload.intent,
            persist=payload.persist,
            created_by=payload.created_by,
            is_active=payload.is_active,
            default_clause_type=payload.default_clause_type,
            temperature=payload.temperature,
        )
        processed = result.get("processed") if isinstance(result.get("processed"), dict) else {}
        if bool(processed.get("persisted")):
            deps.get_tools.cache_clear()
        return {"success": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        LOGGER.exception("Failed to generate and ingest SOLF from intent")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/solf-generation-pack/generate-and-ingest-from-inspection")
def generate_and_ingest_solf_from_inspection(payload: SolfGenerationFromInspectionRequest) -> dict[str, Any]:
    try:
        result = business_rules.generate_and_process_solf_from_inspection(
            goal=payload.goal,
            inspection_context=payload.inspection_context,
            persist=payload.persist,
            created_by=payload.created_by,
            is_active=payload.is_active,
            default_clause_type=payload.default_clause_type,
            temperature=payload.temperature,
            workflow_registry=payload.workflow_registry,
        )
        processed = result.get("processed") if isinstance(result.get("processed"), dict) else {}
        if bool(processed.get("persisted")):
            deps.get_tools.cache_clear()
        return {"success": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        LOGGER.exception("Failed to generate and ingest SOLF from inspection")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/process-taxonomy")
def get_workflow_process_taxonomy(domain: str | None = None) -> dict[str, Any]:
    try:
        return {
            "success": True,
            "result": list_workflow_process_taxonomy(domain=domain),
        }
    except Exception as exc:
        LOGGER.exception("Failed to get workflow process taxonomy")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/process-taxonomy/suggest")
def suggest_workflow_process_candidates(payload: WorkflowProcessSuggestionRequest) -> dict[str, Any]:
    try:
        candidates = suggest_workflow_processes(
            text=payload.text,
            domain=payload.domain,
            top_k=payload.top_k,
        )
        return {
            "success": True,
            "result": {
                "input_text": payload.text,
                "domain": payload.domain,
                "candidates": candidates,
            },
        }
    except Exception as exc:
        LOGGER.exception("Failed to suggest workflow process candidates")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry")
def create_workflow_registry_entry(payload: WorkflowRegistryCreateRequest) -> dict[str, Any]:
    try:
        result = business_rules.create_solf_workflow_registry_entry(
            workflow_key=payload.workflow_key,
            workflow_name=payload.workflow_name,
            description=payload.description,
            domain=payload.domain,
            status=payload.status,
            metadata=payload.metadata,
            is_active=payload.is_active,
            created_by=payload.created_by,
        )
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to create workflow registry entry")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/workflow-registry")
def list_workflow_registry_entries(
    is_active: bool | None = None,
    domain: str | None = None,
    status: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    try:
        result = business_rules.list_solf_workflow_registry_entries(
            is_active=is_active,
            domain=domain,
            status=status,
            limit=limit,
        )
        return {"success": True, "count": len(result), "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to list workflow registry entries")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/workflow-registry/list")
def list_workflow_registry_entries_explicit(
    is_active: bool | None = None,
    domain: str | None = None,
    status: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Explicit list route that avoids collision with /{rule_id} legacy route ordering."""
    return list_workflow_registry_entries(
        is_active=is_active,
        domain=domain,
        status=status,
        limit=limit,
    )


@router.get("/workflow-registry/{workflow_id}")
def get_workflow_registry_entry(workflow_id: int) -> dict[str, Any]:
    try:
        result = business_rules.get_solf_workflow_registry_entry(workflow_id=workflow_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"workflow {workflow_id} not found")
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to get workflow registry entry")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/workflow-registry/by-key/{workflow_key}/active-version")
def get_workflow_registry_active_version_by_key(workflow_key: str) -> dict[str, Any]:
    try:
        result = business_rules.get_solf_workflow_active_version_by_key(workflow_key=workflow_key)
        if result is None:
            raise HTTPException(status_code=404, detail=f"workflow key {workflow_key} not found")
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to get active workflow version by key")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/workflow-registry/{workflow_id}/resource-aliases")
def get_workflow_resource_aliases(
    workflow_id: int,
    workflow_version_id: int | None = None,
    resource_type: str | None = None,
) -> dict[str, Any]:
    try:
        workflow = business_rules.get_solf_workflow_registry_entry(workflow_id=workflow_id)
        if workflow is None:
            raise HTTPException(status_code=404, detail=f"workflow {workflow_id} not found")
        result = business_rules.list_workflow_resource_aliases(
            workflow_id=workflow_id,
            workflow_version_id=workflow_version_id,
            resource_type=resource_type,
        )
        return {"success": True, "count": len(result), "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to list workflow resource aliases")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/workflow-registry/by-key/{workflow_key}/resource-aliases")
def get_workflow_resource_aliases_by_key(
    workflow_key: str,
    workflow_version_id: int | None = None,
    resource_type: str | None = None,
) -> dict[str, Any]:
    try:
        workflow = business_rules.get_solf_workflow_registry_entry_by_key(workflow_key=workflow_key)
        if workflow is None:
            raise HTTPException(status_code=404, detail=f"workflow key {workflow_key} not found")
        result = business_rules.list_workflow_resource_aliases(
            workflow_key=workflow_key,
            workflow_version_id=workflow_version_id,
            resource_type=resource_type,
        )
        return {"success": True, "count": len(result), "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to list workflow resource aliases by key")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/resolve-alias")
def resolve_workflow_resource_alias(payload: WorkflowResourceAliasResolveRequest) -> dict[str, Any]:
    try:
        result = business_rules.resolve_workflow_resource_alias(
            workflow_key=payload.workflow_key,
            alias_name=payload.alias_name,
            resource_type=payload.resource_type,
            workflow_version_id=payload.workflow_version_id,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"workflow alias {payload.alias_name} not found")
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to resolve workflow resource alias")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-extensions")
def create_workflow_extension(payload: WorkflowExtensionPackCreateRequest) -> dict[str, Any]:
    try:
        result = business_rules.create_workflow_extension_pack(
            extension_key=payload.extension_key,
            extension_name=payload.extension_name,
            description=payload.description,
            scope_json=payload.scope_json,
            metadata=payload.metadata,
            is_active=payload.is_active,
            created_by=payload.created_by,
        )
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to create workflow extension pack")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/workflow-extensions")
def list_workflow_extensions(
    is_active: bool | None = None,
    extension_key: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    try:
        result = business_rules.list_workflow_extension_packs(
            is_active=is_active,
            extension_key=extension_key,
            limit=limit,
        )
        return {"success": True, "count": len(result), "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to list workflow extension packs")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/workflow-extensions/key/{extension_key}")
def get_workflow_extension(extension_key: str) -> dict[str, Any]:
    try:
        result = business_rules.get_workflow_extension_pack(extension_key=extension_key)
        if result is None:
            raise HTTPException(status_code=404, detail=f"workflow extension {extension_key} not found")
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to get workflow extension pack")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-extensions/key/{extension_key}/versions")
def create_workflow_extension_version(extension_key: str, payload: WorkflowExtensionVersionCreateRequest) -> dict[str, Any]:
    try:
        extension_pack_id = payload.extension_pack_id
        if extension_pack_id is None:
            extension = business_rules.get_workflow_extension_pack(extension_key=payload.extension_key or extension_key)
            if extension is None:
                raise HTTPException(status_code=404, detail=f"workflow extension {extension_key} not found")
            extension_pack_id = int(extension.get("extension_pack_id") or 0)
        if extension_pack_id is None or int(extension_pack_id) <= 0:
            raise HTTPException(status_code=400, detail="extension_pack_id is required")

        result = business_rules.create_workflow_extension_version(
            extension_pack_id=int(extension_pack_id),
            status=payload.status,
            conditions_schema_version=payload.conditions_schema_version,
            metadata=payload.metadata,
            is_active=payload.is_active,
            rules=payload.rules,
            created_by=payload.created_by,
        )
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to create workflow extension version")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/workflow-extensions/key/{extension_key}/versions")
def list_workflow_extension_versions(
    extension_key: str,
    is_active: bool | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    try:
        extension = business_rules.get_workflow_extension_pack(extension_key=extension_key)
        if extension is None:
            raise HTTPException(status_code=404, detail=f"workflow extension {extension_key} not found")
        extension_pack_id = int(extension.get("extension_pack_id") or 0)
        result = business_rules.list_workflow_extension_versions(
            extension_pack_id=extension_pack_id,
            is_active=is_active,
            limit=limit,
        )
        return {"success": True, "count": len(result), "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to list workflow extension versions")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-extensions/attachments")
def attach_workflow_extension(payload: WorkflowExtensionAttachmentCreateRequest) -> dict[str, Any]:
    try:
        extension_pack_id = payload.extension_pack_id
        if extension_pack_id is None:
            if not payload.extension_key:
                raise HTTPException(status_code=400, detail="extension_pack_id or extension_key is required")
            extension = business_rules.get_workflow_extension_pack(extension_key=payload.extension_key)
            if extension is None:
                raise HTTPException(status_code=404, detail=f"workflow extension {payload.extension_key} not found")
            extension_pack_id = int(extension.get("extension_pack_id") or 0)
        if extension_pack_id is None or int(extension_pack_id) <= 0:
            raise HTTPException(status_code=400, detail="invalid extension pack id")

        result = business_rules.attach_workflow_extension(
            workflow_key=payload.workflow_key,
            workflow_version_id=payload.workflow_version_id,
            extension_pack_id=int(extension_pack_id),
            extension_version_id=payload.extension_version_id,
            effective_from=payload.effective_from,
            effective_until=payload.effective_until,
            priority=payload.priority,
            tenant_scope=payload.tenant_scope,
            metadata=payload.metadata,
            is_active=payload.is_active,
            created_by=payload.created_by,
        )
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to attach workflow extension")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-extensions/attachments/detach")
def detach_workflow_extension(payload: WorkflowExtensionAttachmentDetachRequest) -> dict[str, Any]:
    try:
        detached = business_rules.detach_workflow_extension_attachment(attachment_id=payload.attachment_id)
        if not detached:
            raise HTTPException(status_code=404, detail=f"attachment {payload.attachment_id} not found")
        return {"success": True, "attachment_id": int(payload.attachment_id), "is_active": False}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to detach workflow extension")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/workflow-extensions/attachments")
def list_workflow_extension_attachments(
    workflow_key: str | None = None,
    workflow_version_id: int | None = None,
    extension_pack_id: int | None = None,
    tenant_scope: str | None = None,
    is_active: bool | None = None,
    only_effective_now: bool = False,
    limit: int = 500,
) -> dict[str, Any]:
    try:
        result = business_rules.list_workflow_extension_attachments(
            workflow_key=workflow_key,
            workflow_version_id=workflow_version_id,
            extension_pack_id=extension_pack_id,
            tenant_scope=tenant_scope,
            is_active=is_active,
            only_effective_now=only_effective_now,
            limit=limit,
        )
        return {"success": True, "count": len(result), "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to list workflow extension attachments")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-extensions/resolve-preview")
def resolve_workflow_extension_preview(payload: WorkflowExtensionResolvePreviewRequest) -> dict[str, Any]:
    try:
        result = business_rules.resolve_workflow_extension_preview(
            workflow_key=payload.workflow_key,
            workflow_version_id=payload.workflow_version_id,
            run_status=payload.run_status,
            current_context=payload.current_context,
            step_statuses=payload.step_statuses,
            tenant_scope=payload.tenant_scope,
            hook_point=payload.hook_point,
            target_step_key=payload.target_step_key,
        )
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to resolve workflow extension preview")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/impact-auto-plan")
def workflow_registry_impact_auto_plan(payload: WorkflowImpactAutoPlanRequest) -> dict[str, Any]:
    try:
        result = business_rules.analyze_workflow_impact_and_generate_extension(
            change_type=payload.change_type,
            change_payload=payload.change_payload,
            workflow_keys=payload.workflow_keys,
            domain_hint=payload.domain_hint,
            top_k=payload.top_k,
            created_by=payload.created_by,
            persist=payload.persist,
            attach=payload.attach,
            temperature=payload.temperature,
        )
        return {
            "success": True,
            "result": result,
        }
    except Exception as exc:
        LOGGER.exception("Failed to generate workflow impact auto plan")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/validate-capability")
def validate_workflow_capability(payload: WorkflowCapabilityValidationRequest) -> dict[str, Any]:
    try:
        result = business_rules.validate_workflow_implementation_capability(
            workflow_key=payload.workflow_key,
            workflow_id=payload.workflow_id,
            workflow_version_id=payload.workflow_version_id,
            proposed_steps=payload.proposed_steps,
            workflow_name=payload.workflow_name,
            requested_by=payload.requested_by,
        )
        return {
            "success": True,
            "supported": bool(result.get("supported")),
            "result": result,
        }
    except Exception as exc:
        LOGGER.exception("Failed to validate workflow capability")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/validate-capability/issue-markdown", response_class=PlainTextResponse)
def validate_workflow_capability_issue_markdown(payload: WorkflowCapabilityValidationRequest) -> str:
    try:
        result = business_rules.validate_workflow_implementation_capability(
            workflow_key=payload.workflow_key,
            workflow_id=payload.workflow_id,
            workflow_version_id=payload.workflow_version_id,
            proposed_steps=payload.proposed_steps,
            workflow_name=payload.workflow_name,
            requested_by=payload.requested_by,
        )
        issue_payload = result.get("developer_issue_template") if isinstance(result, dict) else {}
        markdown = str(issue_payload.get("markdown") or "").strip()
        if not markdown:
            raise HTTPException(status_code=500, detail="Issue markdown could not be generated")
        return markdown
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to generate workflow capability issue markdown")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _resolve_workflow_capability_validation_result(
    payload: WorkflowCapabilityValidationRequest,
) -> tuple[dict[str, Any], dict[str, Any]]:
    result = business_rules.validate_workflow_implementation_capability(
        workflow_key=payload.workflow_key,
        workflow_id=payload.workflow_id,
        workflow_version_id=payload.workflow_version_id,
        proposed_steps=payload.proposed_steps,
        workflow_name=payload.workflow_name,
        requested_by=payload.requested_by,
    )
    issue_payload = result.get("developer_issue_template") if isinstance(result, dict) else {}
    github_issue = issue_payload.get("github_issue") if isinstance(issue_payload, dict) else {}
    if not isinstance(github_issue, dict) or not github_issue.get("title") or not github_issue.get("body"):
        raise HTTPException(status_code=500, detail="Issue JSON payload could not be generated")
    return result, github_issue


def _build_github_issue_payload(
    payload: WorkflowCapabilityIssueJsonRequest,
    validation_result: dict[str, Any],
    github_issue: dict[str, Any],
) -> dict[str, Any]:
    workflow_domain = str(validation_result.get("workflow_domain") or "").strip()
    owner = str(payload.github_owner or os.getenv("IKOS_GITHUB_OWNER") or "").strip()
    repo = str(payload.github_repo or os.getenv("IKOS_GITHUB_REPO") or "").strip()
    repository = str(payload.github_repository or os.getenv("IKOS_GITHUB_REPOSITORY") or "").strip()
    if repository and "/" in repository:
        parsed_owner, parsed_repo = repository.split("/", 1)
        owner = owner or parsed_owner.strip()
        repo = repo or parsed_repo.strip()

    labels = [str(item).strip() for item in (payload.labels or github_issue.get("labels") or []) if str(item).strip()]
    if payload.include_domain_label and workflow_domain:
        domain_label = f"domain:{workflow_domain.lower().replace(' ', '-')}"
        if domain_label not in labels:
            labels.append(domain_label)

    create_issue_payload = {
        "owner": owner or None,
        "repo": repo or None,
        "title": str(github_issue.get("title") or "").strip(),
        "body": str(github_issue.get("body") or "").strip(),
        "labels": labels,
        "assignees": [str(item).strip() for item in (payload.assignees or github_issue.get("assignees") or []) if str(item).strip()],
        "milestone": payload.milestone if payload.milestone is not None else github_issue.get("milestone"),
        "projects": [str(item).strip() for item in (payload.projects or github_issue.get("projects") or []) if str(item).strip()],
    }

    return {
        "repository": {
            "owner": owner or None,
            "repo": repo or None,
            "full_name": f"{owner}/{repo}" if owner and repo else None,
        },
        "create_issue_payload": create_issue_payload,
    }


@router.post("/workflow-registry/validate-capability/issue-json")
def validate_workflow_capability_issue_json(payload: WorkflowCapabilityIssueJsonRequest) -> dict[str, Any]:
    try:
        result, github_issue = _resolve_workflow_capability_validation_result(payload)
        issue_payload = _build_github_issue_payload(payload, result, github_issue)

        return {
            "success": True,
            "supported": bool(result.get("supported")),
            "result": issue_payload,
        }
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to generate workflow capability issue JSON")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/validate-capability/issue-create")
def validate_workflow_capability_issue_create(payload: WorkflowCapabilityIssueCreateRequest) -> dict[str, Any]:
    try:
        result, github_issue = _resolve_workflow_capability_validation_result(payload)
        issue_payload = _build_github_issue_payload(payload, result, github_issue)
        create_issue_payload = issue_payload.get("create_issue_payload") if isinstance(issue_payload, dict) else {}

        owner = str(create_issue_payload.get("owner") or "").strip()
        repo = str(create_issue_payload.get("repo") or "").strip()
        if not owner or not repo:
            raise HTTPException(status_code=400, detail="owner and repo are required to create a GitHub issue")

        if payload.dry_run:
            return {
                "success": True,
                "supported": bool(result.get("supported")),
                "dry_run": True,
                "result": issue_payload,
            }

        token = str(payload.github_token or os.getenv("IKOS_GITHUB_TOKEN") or "").strip()
        if not token:
            raise HTTPException(status_code=400, detail="GitHub token required (set IKOS_GITHUB_TOKEN or provide github_token)")

        api_base = str(payload.github_api_base_url or os.getenv("IKOS_GITHUB_API_BASE_URL") or "https://api.github.com").strip().rstrip("/")
        github_request_body = {
            "title": str(create_issue_payload.get("title") or "").strip(),
            "body": str(create_issue_payload.get("body") or "").strip(),
        }
        if create_issue_payload.get("labels"):
            github_request_body["labels"] = create_issue_payload.get("labels")
        if create_issue_payload.get("assignees"):
            github_request_body["assignees"] = create_issue_payload.get("assignees")
        if create_issue_payload.get("milestone") is not None:
            github_request_body["milestone"] = create_issue_payload.get("milestone")

        request_data = json.dumps(github_request_body).encode("utf-8")
        request_url = f"{api_base}/repos/{owner}/{repo}/issues"
        github_request = urllib_request.Request(
            url=request_url,
            data=request_data,
            method="POST",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "idms-workflow-capability-agent",
            },
        )

        try:
            with urllib_request.urlopen(github_request, timeout=20) as response:
                response_text = response.read().decode("utf-8", errors="replace")
        except urllib_error.HTTPError as exc:
            error_text = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else str(exc)
            raise HTTPException(status_code=502, detail=f"GitHub issue creation failed: {error_text}") from exc
        except urllib_error.URLError as exc:
            raise HTTPException(status_code=502, detail=f"GitHub API is unreachable: {exc}") from exc

        created_issue = json.loads(response_text) if response_text else {}
        return {
            "success": True,
            "supported": bool(result.get("supported")),
            "dry_run": False,
            "result": {
                "repository": issue_payload.get("repository"),
                "issue": {
                    "id": created_issue.get("id"),
                    "number": created_issue.get("number"),
                    "node_id": created_issue.get("node_id"),
                    "title": created_issue.get("title"),
                    "state": created_issue.get("state"),
                    "html_url": created_issue.get("html_url"),
                },
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to create workflow capability GitHub issue")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/{workflow_id}/activation")
def set_workflow_registry_entry_activation(
    workflow_id: int,
    payload: WorkflowRegistryActivationRequest,
) -> dict[str, Any]:
    try:
        updated = business_rules.set_solf_workflow_registry_entry_active(
            workflow_id=workflow_id,
            is_active=payload.is_active,
        )
        if not updated:
            raise HTTPException(status_code=404, detail=f"workflow {workflow_id} not found")
        return {
            "success": True,
            "workflow_id": int(workflow_id),
            "is_active": bool(payload.is_active),
        }
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to set workflow registry entry activation")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/workflow-registry/{workflow_id}/versions")
def list_workflow_versions(
    workflow_id: int,
    is_active: bool | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    try:
        result = business_rules.list_workflow_versions(
            workflow_id=workflow_id,
            is_active=is_active,
            limit=limit,
        )
        if result == []:
            workflow = business_rules.get_solf_workflow_registry_entry(workflow_id=workflow_id)
            if workflow is None:
                raise HTTPException(status_code=404, detail=f"workflow {workflow_id} not found")
        return {"success": True, "count": len(result), "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to list workflow versions")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/{workflow_id}/versions")
def create_workflow_version(
    workflow_id: int,
    payload: WorkflowVersionCreateRequest,
) -> dict[str, Any]:
    try:
        result = business_rules.create_workflow_version(
            workflow_id=workflow_id,
            steps=payload.steps,
            graph_spec=payload.graph_spec,
            input_contract=payload.input_contract,
            output_contract=payload.output_contract,
            metadata=payload.metadata,
            status=payload.status,
            created_by=payload.created_by,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"workflow {workflow_id} not found")
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to create workflow version")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{rule_id:int}/workflow-registry/sync")
def sync_rule_workflow_registry(rule_id: int, payload: WorkflowRegistrySyncRequest) -> dict[str, Any]:
    try:
        result = business_rules.sync_business_rule_workflow_registry(
            rule_id=rule_id,
            created_by=payload.created_by,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"business rule {rule_id} not found")
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to sync rule workflow registry")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/seed-templates")
def seed_workflow_registry_templates(payload: WorkflowRegistrySeedTemplatesRequest) -> dict[str, Any]:
    try:
        result = business_rules.seed_workflow_registry_templates(
            created_by=payload.created_by,
        )
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to seed workflow registry templates")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/export-descriptions")
def export_workflow_descriptions(payload: WorkflowDescriptionExportRequest) -> dict[str, Any]:
    try:
        result = business_rules.export_workflow_pipeline_descriptions(
            file_format=payload.file_format,
            include_preprogrammed=payload.include_preprogrammed,
            include_user_defined=payload.include_user_defined,
            workflow_keys=payload.workflow_keys,
            output_file_name=payload.output_file_name,
        )
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to export workflow descriptions")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/export-descriptions-inline")
def export_workflow_descriptions_inline(payload: WorkflowDescriptionExportRequest) -> dict[str, Any]:
    try:
        result = business_rules.export_workflow_pipeline_descriptions_inline(
            file_format=payload.file_format,
            include_preprogrammed=payload.include_preprogrammed,
            include_user_defined=payload.include_user_defined,
            workflow_keys=payload.workflow_keys,
            output_file_name=payload.output_file_name,
        )
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to export workflow descriptions inline")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{rule_id:int}/workflow-registry")
def get_rule_workflow_registry(rule_id: int) -> dict[str, Any]:
    try:
        result = business_rules.get_business_rule_workflow_registry(rule_id=rule_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"business rule {rule_id} not found")
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to get rule workflow registry")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/create-with-registry")
def create_business_rule_with_workflow_registry(
    payload: CreateBusinessRuleWithRegistryRequest,
) -> dict[str, Any]:
    try:
        result = business_rules.create_business_rule_with_workflow_registry(
            rule_text=payload.rule_text,
            rule_name=payload.rule_name,
            created_by=payload.created_by,
            is_active=payload.is_active,
        )
        deps.get_tools.cache_clear()
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to create business rule with workflow registry")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/{workflow_id}/publish")
def publish_workflow_version(
    workflow_id: int,
    payload: WorkflowVersionPublishRequest,
) -> dict[str, Any]:
    try:
        result = business_rules.publish_workflow_version(
            workflow_id=workflow_id,
            workflow_version_id=payload.workflow_version_id,
            published_by=payload.published_by,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"workflow {workflow_id} or version not found")
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to publish workflow version")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/workflow-registry/{workflow_id}/rollback")
def rollback_workflow_version(
    workflow_id: int,
    payload: WorkflowVersionRollbackRequest,
) -> dict[str, Any]:
    try:
        result = business_rules.rollback_workflow_version(
            workflow_id=workflow_id,
            target_version_id=payload.target_version_id,
            reason=payload.reason,
            rolled_back_by=payload.rolled_back_by,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"workflow {workflow_id} or version not found")
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to rollback workflow version")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
