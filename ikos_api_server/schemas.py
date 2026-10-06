from __future__ import annotations

from typing import Any
from typing import Literal

from pydantic import BaseModel, Field, ConfigDict


class IngestPathRequest(BaseModel):
    source_path: str = Field(..., description="Absolute or relative local path or gs:// URI")
    file_name: str = Field(default="", description="Optional client-side file name")
    note: str = Field(default="", description="Additional processing note/instructions")
    processing_rule_names: list[str] = Field(
        default_factory=list,
        description="Optional list of active business rule names to enforce during ingestion",
    )
    workflow_processes: list[str] = Field(
        default_factory=list,
        description="Optional list of workflow process names to trigger during ingestion (e.g., booking_process, vat_classification). Processes are auto-derived from document_type; use this to override or add custom processes.",
    )
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    prefer_markdown_input: bool = Field(
        default=False,
        description="When True, use the cached Markdown artifact (if available) as the extraction source instead of the original PDF/GCS file. Improves table header-to-row mapping accuracy.",
    )


class ReprocessRequest(BaseModel):
    """Reprocess a previously ingested document with optional parameter overrides."""
    doc_id: int = Field(..., description="Document ID from a previous ingest run")
    prefer_markdown_input: bool | None = Field(
        default=None,
        description="Override: use cached Markdown for extraction. If None, reuses original setting.",
    )
    reuse_markdown: bool | None = Field(default=None, description="Override: reuse cached markdown if available")
        # persist_markdown has been removed to avoid dangerous reprocess markdown persistence override


RepairMode = Literal["markdown_only", "metadata_only", "full_reextract"]


class RepairDocumentsRequest(BaseModel):
    """Maintenance repair request for document recovery modes."""

    mode: RepairMode = Field(
        default="markdown_only",
        description="Repair mode: markdown_only, metadata_only, full_reextract",
    )
    doc_ids: list[int] = Field(
        default_factory=list,
        description="Optional explicit list of document IDs. If empty, auto-selects documents by mode.",
    )
    only_missing: bool = Field(
        default=True,
        description="When true and doc_ids is empty, target only rows with missing fields relevant to the selected mode.",
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=1000,
        description="Maximum number of auto-selected document rows when doc_ids is empty.",
    )
    allow_persist_markdown: bool = Field(
        default=False,
        description=(
            "When true, allows repair modes to persist newly generated markdown artifacts. "
            "Requires a valid maintenance admin_token when IDMS_MAINTENANCE_TOKEN is configured."
        ),
    )
    admin_token: str = Field(
        default="",
        description="Optional maintenance token used for restricted operations.",
    )


class TableProvenanceBackfillRequest(BaseModel):
    """Backfill normalized table-cell provenance rows from persisted document metadata."""

    doc_ids: list[int] = Field(
        default_factory=list,
        description="Optional explicit list of document IDs to backfill. If empty, auto-selects rows.",
    )
    only_missing: bool = Field(
        default=True,
        description="When true and doc_ids is empty, target only docs with structured tables and no provenance rows.",
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=2000,
        description="Maximum number of auto-selected document rows when doc_ids is empty.",
    )


class DocumentExportSaveRequest(BaseModel):
    format: str = Field(default="pdf", description="Export format: pdf, docx, csv, excel")
    output_path: str = Field(
        default="",
        description="Optional full file path (absolute or project-relative). Overrides output_dir when provided.",
    )
    output_dir: str = Field(
        default="",
        description="Optional output directory (absolute or project-relative). Defaults to generated/docs when empty.",
    )


class TemplateReferenceGenerateRequest(BaseModel):
    reference_name: str = Field(..., description="Registered template reference name")
    specification: str = Field(default="", description="Natural-language generation instruction")
    output_format: str = Field(default="pdf", description="Requested output format")
    document_kind: str = Field(default="invoice", description="Document kind context (invoice, quotation, etc.)")
    payload: dict[str, Any] = Field(default_factory=dict, description="Context overrides for the generated document")
    override_fields: list[str] = Field(
        default_factory=list,
        description="Optional allow-list of payload fields to overwrite (supports dot paths, e.g. recipient.address.city)",
    )
    output_path: str = Field(default="", description="Optional full output file path")
    output_dir: str = Field(default="", description="Optional output directory; defaults to generated/docs")


class InvoiceFromReferenceGenerateRequest(BaseModel):
    reference_name: str = Field(..., description="Registered template reference name")
    specification: str = Field(default="", description="Natural-language generation instruction")
    output_format: str = Field(default="pdf", description="Requested output format")
    document_kind: str = Field(default="invoice", description="Document kind context; defaults to invoice")
    payload: dict[str, Any] = Field(default_factory=dict, description="Invoice payload overrides")
    override_fields: list[str] = Field(
        default_factory=list,
        description="Optional allow-list of payload fields to overwrite (supports dot paths, e.g. recipient.address.city)",
    )
    qr_data: dict[str, Any] = Field(default_factory=dict, description="Optional QR payload overrides")
    output_path: str = Field(default="", description="Optional full output file path")
    output_dir: str = Field(default="", description="Optional output directory; defaults to generated/docs")
    reference_mode: str = Field(
        default="next_from_reference",
        description=(
            "Reference generation mode: next_from_reference, next_from_sequence, "
            "prefix_and_sequence, explicit"
        ),
    )
    current_reference: str = Field(
        default="",
        description="Current 27-digit reference used for next_from_reference mode",
    )
    current_sequence: int | None = Field(
        default=None,
        ge=0,
        description="Current sequence used for next_from_sequence mode",
    )
    sequence_step: int = Field(
        default=1,
        ge=1,
        description="Increment step for next_from_reference or next_from_sequence",
    )
    reference_prefix: str = Field(
        default="",
        description="Prefix digits used for prefix_and_sequence mode",
    )
    sequence_value: int | None = Field(
        default=None,
        ge=0,
        description="Sequence value used for prefix_and_sequence mode",
    )
    persist_reference_state: bool = Field(
        default=True,
        description="When true, reserve next reference atomically in DB-backed state.",
    )


class SwissReferenceValidateRequest(BaseModel):
    reference: str = Field(..., description="Swiss QRR reference string (27 digits, spaces allowed)")


class InvoiceFromInstructionGenerateRequest(BaseModel):
    instruction_text: str = Field(
        ...,
        description="Natural-language instruction used to generate runtime SOLF artifacts.",
    )
    invoice_request: InvoiceFromReferenceGenerateRequest = Field(
        ...,
        description="Invoice generation request executed after SOLF generation.",
    )
    persist_generated_solf: bool = Field(
        default=True,
        description="Persist generated SOLF artifacts for reuse.",
    )
    reuse_existing_solf_rule: bool = Field(
        default=True,
        description="When true, reuse an active existing SOLF business rule if matched instead of regenerating.",
    )
    existing_rule_name: str = Field(
        default="",
        description="Optional explicit active business rule name to reuse.",
    )
    solf_created_by: str = Field(default="api:user", description="Creator label for persisted SOLF artifacts")
    solf_is_active: bool = Field(default=True, description="Whether generated SOLF artifacts are active")
    solf_default_clause_type: str = Field(
        default="resolve_policy",
        description="Default clause type used when persisting generated SOLF clauses",
    )
    temperature: float = Field(default=0.0, ge=0.0, le=1.0, description="Generation temperature for runtime SOLF creation")


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


class SalesPipelineGenerateRequest(BaseModel):
    pipeline_name: str = Field(default="sales_pipeline", description="Pipeline run label used in output paths")
    stages: list[str] = Field(
        default_factory=lambda: ["quotation", "order_confirmation", "delivery", "invoice", "payment"],
        description="Ordered stage list to generate",
    )
    stage_reference_names: dict[str, str] = Field(
        default_factory=dict,
        description="Per-stage template reference mapping, e.g. {invoice: 'Bahm invoice', quotation: 'Generic quotation'}",
    )
    specification: str = Field(default="", description="Global instruction applied to all stages")
    stage_specifications: dict[str, str] = Field(
        default_factory=dict,
        description="Optional per-stage instruction override",
    )
    output_format: str = Field(default="pdf", description="Requested output format for generated artifacts")
    output_dir: str = Field(default="", description="Output directory; defaults to generated/docs")
    common_payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Shared payload applied to all stages (customer/service/date context)",
    )
    stage_payloads: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="Optional stage-specific payload overlays",
    )
    continue_on_error: bool = Field(default=False, description="Continue remaining stages when one stage fails")


class SalesPipelineStageSyncRequest(BaseModel):
    status: str = Field(
        default="",
        description="Optional stage status update (for example: generated, failed, paid, booked)",
    )
    error_message: str = Field(default="", description="Optional error detail when status indicates failure")
    result: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional stage result fields to merge into the persisted stage result JSON",
    )
    external_event: str = Field(default="", description="Optional external event label (ERP/CRM webhook action)")
    external_reference: str = Field(default="", description="Optional external system reference/id")
    idempotency_key: str = Field(
        default="",
        description="Optional idempotency key; repeated keys for the same run+stage are ignored",
    )


class SalesPipelineStageBulkSyncItem(BaseModel):
    stage_name: str = Field(..., description="Pipeline stage name (quotation, invoice, payment, etc.)")
    status: str = Field(default="", description="Optional stage status update")
    error_message: str = Field(default="", description="Optional error message when status indicates failure")
    result: dict[str, Any] = Field(default_factory=dict, description="Optional result fields merged into stage result JSON")
    external_event: str = Field(default="", description="Optional external event label")
    external_reference: str = Field(default="", description="Optional external system reference/id")
    idempotency_key: str = Field(default="", description="Optional idempotency key for webhook/event dedupe")


class SalesPipelineStageBulkSyncRequest(BaseModel):
    updates: list[SalesPipelineStageBulkSyncItem] = Field(
        default_factory=list,
        description="List of stage updates to apply in order",
    )
    continue_on_error: bool = Field(default=True, description="Continue applying remaining updates if one update fails")


class ChatRequest(BaseModel):
    message: str
    use_query_tool: bool = True


class ClarificationResponseRequest(BaseModel):
    thread_id: int = Field(..., ge=1, description="Pending clarification thread ID")
    user_response: str = Field(..., description="User clarification response text")


class SimilarEntityRequest(BaseModel):
    entity_name: str = Field(..., description="Entity/person name to inspect for similar records")
    class_name: str = Field(default="person", description="Entity class filter (defaults to person)")
    limit: int = Field(default=8, ge=1, le=20, description="Maximum number of similar candidates")


class EntityAliasLinkRequest(BaseModel):
    primary_object_id: int = Field(..., ge=1, description="Primary/canonical object_id")
    alias_object_id: int = Field(..., ge=1, description="Alias/linked object_id")
    alias_type: str = Field(default="same_entity", description="Alias semantics, e.g. same_entity, merged_record")
    confidence: float | None = Field(default=None, ge=0.0, le=1.0, description="Optional confidence score")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Optional alias metadata")


class EntityMergeRequest(BaseModel):
    canonical_object_id: int = Field(..., ge=1, description="Canonical target object_id")
    duplicate_object_id: int = Field(..., ge=1, description="Duplicate object_id to merge into canonical")
    merge_note: str = Field(default="", description="Optional merge note for audit trail")
    keep_alias_link: bool = Field(default=True, description="Keep an alias record after merge")


class QueryRequest(BaseModel):
    question: str
    max_steps: int = 3


class ActionRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    action_name: str
    payload: dict[str, Any] = Field(default_factory=dict)
    request: str = Field(default="", description="Optional natural-language request text")
    message: str = Field(default="", description="Optional natural-language message text")
    text: str = Field(default="", description="Optional natural-language text")


class SolfClauseQueryRequest(BaseModel):
    clause_name: str = Field(..., description="SOLF clause/predicate name to execute")
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional single payload object. Used when args is empty.",
    )
    args: list[Any] = Field(
        default_factory=list,
        description="Optional explicit positional arguments passed to the clause.",
    )


class WorkflowDesignRequest(BaseModel):
    request: str = Field(..., description="High-level workflow/process request to design")
    context: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional internal metadata/rules/context to ground iterative refinement",
    )
    max_iterations: int = Field(
        default=3,
        ge=1,
        le=8,
        description="Maximum LLM refinement iterations",
    )


class ComplexInteractionRequest(BaseModel):
    request: str = Field(..., description="Complex query or interaction request")
    context: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional internal metadata/rules/context to ground iterative refinement",
    )
    max_iterations: int = Field(
        default=3,
        ge=1,
        le=8,
        description="Maximum LLM refinement iterations",
    )
    production_mode: bool = Field(
        default=False,
        description="When true, use production-grade resolution with capability profiling, schema mapping, external fact retrieval, and deterministic calculation",
    )
    llm_model: str = Field(
        default="",
        description="Optional explicit LLM model id for production-mode planning/retrieval calls",
    )
    max_retrieval_rounds: int = Field(
        default=2,
        ge=1,
        le=6,
        description="Maximum external fact retrieval rounds when production_mode is enabled",
    )
    strict_provenance: bool = Field(
        default=True,
        description="Require valid source URL and effective date for externally retrieved facts",
    )
    run_calculation: bool = Field(
        default=True,
        description="Execute deterministic calculator when enough mapped inputs are available",
    )
    execute_generic_actions: bool = Field(
        default=True,
        description="Execute generic planner actions (query/interaction/chat/solf) for non-domain-specific complex requests",
    )
    verify_external_sources: bool = Field(
        default=False,
        description="When true, perform live URL verification for externally retrieved facts",
    )
    external_source_timeout_seconds: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Timeout in seconds for live external source verification",
    )
    enforce_approved_domains: bool = Field(
        default=False,
        description="When true, external fact sources must match approved_source_domains",
    )
    approved_source_domains: list[str] = Field(
        default_factory=list,
        description="Optional source-domain allowlist (e.g. admin.ch, zg.ch) for external fact validation",
    )


class NoteIngestRequest(BaseModel):
    """Ingest a note, memo, or info entry directly from chat/web client."""
    title: str = Field(..., description="Title or memo name for the note")
    content: str = Field(..., description="Main content of the note/memo/info")
    note_date: str = Field(default="", description="Optional date for the note (YYYY-MM-DD format)")
    tags: list[str] = Field(default_factory=list, description="Optional list of tags/themes for the note")
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Optional additional metadata. Clarification linking keys supported: "
            "workflow_run_id/workflow_run_ids, clarification_for_run_id/clarification_for_run_ids, "
            "workflow_key/clarification_for_workflow_key. "
            "If omitted, the API may infer target workflow from note title/content/tags and link to likely in-process runs."
        ),
    )
    source: str = Field(default="web", description="Source of the note (web, chat, etc.)")


class SchemaEmbeddingsMaintenanceRequest(BaseModel):
    recreate_collection: bool = Field(
        default=False,
        description="When true, drop and recreate the schema embedding collection before rebuild.",
    )
    include_relationships: bool = Field(
        default=True,
        description="When true, include current relationship names from DB in the rebuild.",
    )
    admin_token: str = Field(
        default="",
        description="Optional maintenance token for restricted environments.",
    )


class BusinessRuleCreateRequest(BaseModel):
    rule_text: str = Field(..., description="Natural-language business rule specification")
    rule_name: str = Field(default="", description="Optional stable rule name")
    created_by: str = Field(default="api:user", description="Creator identifier")
    is_active: bool = Field(default=True, description="Whether the rule is active immediately")
    application_mode: str = Field(default="", description="Optional override: general | selected_only")
    overwrite_existing: bool | None = Field(
        default=None,
        description="Optional override for post-extraction directives; when omitted, the compiler default is preserved",
    )


class BusinessRuleActivationRequest(BaseModel):
    is_active: bool = Field(..., description="Set true to activate, false to deactivate")


class BusinessRuleUpdateRequest(BaseModel):
    rule_text: str = Field(..., description="Updated natural-language business rule specification")
    rule_name: str = Field(default="", description="Optional updated rule name")
    updated_by: str = Field(default="api:user", description="Updater identifier")
    is_active: bool | None = Field(
        default=None,
        description="Optional activation override; when omitted, existing activation state is preserved",
    )
    application_mode: str = Field(default="", description="Optional override: general | selected_only")
    overwrite_existing: bool | None = Field(
        default=None,
        description="Optional override for post-extraction directives; when omitted, the compiler default is preserved",
    )


class BusinessRuleSimulationRequest(BaseModel):
    requested_attribute: str = Field(default="", description="Attribute term to resolve via business rules")
    context: dict[str, Any] = Field(
        default_factory=dict,
        description="Context payload: country, document_type, entity_class, operation, process, etc.",
    )
    sample_attributes: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional sample attribute map to test transformation logic",
    )
    rule_id: int | None = Field(default=None, description="Optional specific rule id to simulate")
    include_inactive: bool = Field(
        default=True,
        description="When true, simulation may evaluate inactive rules (useful before activation)",
    )


class BusinessRuleDraftSimulationRequest(BaseModel):
    rule_text: str = Field(..., description="Natural-language rule text to parse and simulate without saving")
    rule_name: str = Field(default="", description="Optional draft rule name used for SOLF key generation")
    requested_attribute: str = Field(default="", description="Attribute term to resolve in draft simulation")
    application_mode: str = Field(default="", description="Optional override: general | selected_only")
    context: dict[str, Any] = Field(
        default_factory=dict,
        description="Context payload: country, document_type, entity_class, operation, process, etc.",
    )
    sample_attributes: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional sample attribute map to test draft transformation logic",
    )


class SolfScriptRuleReviewRequest(BaseModel):
    rule_name: str = Field(default="", description="Optional stable rule name for this SOLF script")
    solf_script: str = Field(..., description="Raw SOLF script including class blocks and/or clauses")
    default_clause_type: str = Field(
        default="resolve_policy",
        description="Fallback SQL clause type for persisted clauses: resolve_policy | ingest_rule | computation_rule",
    )


class SolfScriptRuleCreateRequest(BaseModel):
    rule_name: str = Field(default="", description="Optional stable rule name for this SOLF script")
    solf_script: str = Field(..., description="Raw SOLF script including class blocks and/or clauses")
    created_by: str = Field(default="api:user", description="Creator identifier")
    is_active: bool = Field(default=True, description="Whether the rule is active immediately")
    default_clause_type: str = Field(
        default="resolve_policy",
        description="Fallback SQL clause type for persisted clauses: resolve_policy | ingest_rule | computation_rule",
    )
    overwrite_existing: bool | None = Field(
        default=None,
        description="Optional persisted overwrite preference for this SOLF rule; when omitted, the default is preserved",
    )


class SolfScriptRuleUpdateRequest(BaseModel):
    rule_name: str = Field(default="", description="Optional updated stable rule name for this SOLF script")
    solf_script: str = Field(..., description="Raw SOLF script including class blocks and/or clauses")
    updated_by: str = Field(default="api:user", description="Updater identifier")
    is_active: bool | None = Field(
        default=None,
        description="Optional activation override; when omitted, existing activation state is preserved",
    )
    default_clause_type: str = Field(
        default="resolve_policy",
        description="Fallback SQL clause type for persisted clauses: resolve_policy | ingest_rule | computation_rule",
    )
    overwrite_existing: bool | None = Field(
        default=None,
        description="Optional persisted overwrite preference for this SOLF rule; when omitted, the default is preserved",
    )


class SolfAttributeProposalStatusRequest(BaseModel):
    status: str = Field(..., description="One of proposed, approved, rejected")
    reviewed_by: str = Field(default="api:user", description="Reviewer identifier")
    note: str = Field(default="", description="Optional review note")


class SolfSchemaPromotionBatchCreateRequest(BaseModel):
    batch_name: str = Field(default="", description="Optional batch name")
    class_name: str = Field(default="", description="Optional class filter for approved proposals")
    proposal_ids: list[int] = Field(default_factory=list, description="Optional explicit approved proposal ids to include")
    created_by: str = Field(default="api:user", description="Creator identifier")
    note: str = Field(default="", description="Optional batch note")


class SolfSchemaPromotionBatchStatusRequest(BaseModel):
    status: str = Field(..., description="One of draft, reviewed, exported, applied")
    reviewed_by: str = Field(default="api:user", description="Reviewer identifier")
    note: str = Field(default="", description="Optional status note")


class SolfSchemaPromotionBatchAuditLinkRequest(BaseModel):
    linked_by: str = Field(default="api:user", description="User linking the batch to a SOLF update")
    exported_artifact_path: str = Field(default="", description="Optional path to exported .solf artifact")
    solf_script_path: str = Field(default="", description="Optional explicit path to canonical solf_script.txt")
    solf_script_hash: str = Field(default="", description="Optional explicit hash for canonical SOLF script content")
    note: str = Field(default="", description="Optional governance/audit note")
