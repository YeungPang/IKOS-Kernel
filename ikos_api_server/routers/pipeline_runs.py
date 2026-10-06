"""FastAPI router for workflow pipeline run management.

Endpoints
---------
POST   /api/pipeline-runs                    Start a new pipeline run
POST   /api/pipeline-runs/{run_id}/resume    Resume a paused run
POST   /api/pipeline-runs/{run_id}/cancel    Cancel a run
GET    /api/pipeline-runs/{run_id}           Get run state + all step states
GET    /api/pipeline-runs                    List runs (filterable)
"""

from __future__ import annotations

import ast
import importlib.metadata
import inspect
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import business_rules
import object_db
from ikos_api_server import deps
from workflow_pipeline_executor import WorkflowPipelineExecutor

router = APIRouter(prefix="/api/pipeline-runs", tags=["pipeline-runs"])
LOGGER = logging.getLogger("idms.api")
PROJECT_ROOT = Path(__file__).resolve().parents[2]


_GENERATED_SCRIPT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS workflow_generated_python_script (
    script_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    configuration_scope VARCHAR(16) NOT NULL DEFAULT 'tenant' CHECK (configuration_scope IN ('tenant', 'global')),
    script_key VARCHAR(256) NOT NULL,
    script_name VARCHAR(256) NOT NULL,
    script_source TEXT NOT NULL,
    entrypoint VARCHAR(128) NOT NULL DEFAULT 'run',
    approval_status VARCHAR(32) NOT NULL DEFAULT 'draft' CHECK (approval_status IN ('draft','approved','deprecated','archived')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_by VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_generated_script_scope_key ON workflow_generated_python_script(tenant_id, configuration_scope, script_key);
CREATE INDEX IF NOT EXISTS idx_wgps_script_key ON workflow_generated_python_script(tenant_id, script_key);
CREATE INDEX IF NOT EXISTS idx_wgps_approval_status ON workflow_generated_python_script(approval_status);
CREATE INDEX IF NOT EXISTS idx_wgps_is_active ON workflow_generated_python_script(is_active);
CREATE INDEX IF NOT EXISTS idx_wgps_metadata_gin ON workflow_generated_python_script USING GIN(metadata);
"""


_GENERATED_SCRIPT_AUDIT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS workflow_generated_python_script_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    run_id BIGINT,
    step_key VARCHAR(256),
    script_id BIGINT REFERENCES workflow_generated_python_script(script_id) ON DELETE SET NULL,
    script_key VARCHAR(256),
    entrypoint VARCHAR(128),
    execution_status VARCHAR(32) NOT NULL,
    duration_ms INTEGER,
    input_hash VARCHAR(64),
    output_hash VARCHAR(64),
    input_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wgps_audit_run_id ON workflow_generated_python_script_audit(run_id);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_step_key ON workflow_generated_python_script_audit(step_key);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_script_key ON workflow_generated_python_script_audit(script_key);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_status ON workflow_generated_python_script_audit(execution_status);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_executed_at ON workflow_generated_python_script_audit(executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_wgps_audit_metadata_gin ON workflow_generated_python_script_audit USING GIN(metadata);
"""


_SCRIPT_KEY_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{2,127}$")


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
        base_dir = (PROJECT_ROOT / "generated" / "workflow-runs").resolve()

    return base_dir / filename


def _sanitize_output_filename(value: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(value or "").strip())
    base = base.strip("._-")
    return base or "workflow_run_output"


def _ensure_generated_script_table(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(_GENERATED_SCRIPT_TABLE_DDL)
    object_db.ensure_tenant_isolation_for_tables(connection, ("workflow_generated_python_script",))


def _ensure_generated_script_audit_table(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(_GENERATED_SCRIPT_AUDIT_TABLE_DDL)
        cursor.execute(
            "ALTER TABLE IF EXISTS workflow_generated_python_script_audit ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(256)"
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_wgps_audit_idempotency
            ON workflow_generated_python_script_audit(run_id, step_key, script_key, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            """
        )
    object_db.ensure_tenant_isolation_for_tables(connection, ("workflow_generated_python_script_audit",))


_DENYLIST_IMPORTS = {
    "subprocess",
    "socket",
    "requests",
    "httpx",
    "urllib",
    "ftplib",
    "telnetlib",
    "paramiko",
    "ctypes",
    "shutil",
    "multiprocessing",
}
_DENYLIST_CALLS = {"eval", "exec", "compile", "open", "input", "breakpoint", "__import__"}


def _analyze_script_policy(script_source: str) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    try:
        tree = ast.parse(script_source or "")
    except SyntaxError as exc:
        return {
            "allowed": False,
            "violations": [{"code": "syntax_error", "message": str(exc), "line": int(exc.lineno or 0)}],
            "warnings": warnings,
        }

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                base = str(alias.name or "").split(".")[0]
                if base in _DENYLIST_IMPORTS:
                    violations.append(
                        {
                            "code": "blocked_import",
                            "message": f"Import '{alias.name}' is blocked by execution policy",
                            "line": int(getattr(node, "lineno", 0) or 0),
                        }
                    )
        elif isinstance(node, ast.ImportFrom):
            base = str(node.module or "").split(".")[0]
            if base in _DENYLIST_IMPORTS:
                violations.append(
                    {
                        "code": "blocked_import",
                        "message": f"Import from '{node.module}' is blocked by execution policy",
                        "line": int(getattr(node, "lineno", 0) or 0),
                    }
                )
        elif isinstance(node, ast.Call):
            func_name = None
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                func_name = node.func.attr

            if func_name in _DENYLIST_CALLS:
                violations.append(
                    {
                        "code": "blocked_call",
                        "message": f"Call '{func_name}' is blocked by execution policy",
                        "line": int(getattr(node, "lineno", 0) or 0),
                    }
                )

            if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
                mod_name = node.func.value.id
                attr = node.func.attr
                if mod_name == "os" and attr == "system":
                    violations.append(
                        {
                            "code": "blocked_call",
                            "message": "Call 'os.system' is blocked by execution policy",
                            "line": int(getattr(node, "lineno", 0) or 0),
                        }
                    )

    if "def run(" not in str(script_source or ""):
        warnings.append(
            {
                "code": "missing_default_entrypoint",
                "message": "No 'def run(context, config)' entrypoint found; ensure entrypoint is provided explicitly.",
            }
        )

    return {
        "allowed": len(violations) == 0,
        "violations": violations,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class PipelineRunStartRequest(BaseModel):
    workflow_version_id: int = Field(..., description="ID of the solf_workflow_versions row to execute")
    input_context: dict[str, Any] = Field(default_factory=dict, description="Initial context payload passed to all steps")
    started_by: str | None = Field(default=None, description="User or system actor starting the run")
    workflow_key: str | None = Field(default=None, description="Optional workflow key label")


class PipelineRunResumeRequest(BaseModel):
    user_response: dict[str, Any] | None = Field(
        default=None,
        description="Answers / field values provided by the user for the paused step",
    )
    doc_refs: list[Any] | None = Field(
        default=None,
        description="List of doc_ids of newly ingested documents that unblock the paused step",
    )


class PipelineRunUndoRequest(BaseModel):
    requested_by: str = Field(default="api:user", description="Actor requesting undo execution")
    dry_run: bool = Field(default=True, description="When true, only plan compensation steps")
    include_completed_only: bool = Field(
        default=True,
        description="When true, only include completed steps; when false include completed/failed/paused for planning",
    )


class PipelineRunStartByKeyRequest(BaseModel):
    workflow_key: str = Field(..., description="Stable workflow key")
    input_context: dict[str, Any] = Field(default_factory=dict, description="Initial context payload passed to all steps")
    started_by: str | None = Field(default=None, description="User or system actor starting the run")


class PipelineRunRerunExecuteRequest(BaseModel):
    workflow_key: str | None = Field(default=None, description="Optional workflow key filter")
    limit: int = Field(default=100, ge=1, le=500, description="Maximum workflow groups to inspect")
    include_non_rerun: bool = Field(default=False, description="Include non-rerun candidates in planning")
    started_by: str = Field(default="api:rerun-recommendation", description="Actor used for new rerun starts")
    max_runs: int | None = Field(default=None, ge=1, le=500, description="Optional cap on number of runs to execute")
    continue_on_error: bool = Field(default=True, description="Continue processing if one rerun fails")
    dry_run: bool = Field(default=False, description="When true, return planned execution only")


class PipelineRunOutputSaveRequest(BaseModel):
    output_path: str = Field(default="", description="Optional explicit output file path")
    output_dir: str = Field(default="", description="Optional output directory when output_path is omitted")
    file_name: str | None = Field(default=None, description="Optional base file name (without extension)")
    format: str = Field(default="json", description="Output format: json or txt")
    payload_source: str = Field(
        default="auto",
        description="Payload source: auto, output_context, current_context, run",
    )
    include_steps: bool = Field(default=False, description="Include run steps in saved artifact")
    pretty: bool = Field(default=True, description="Pretty-print JSON output")


class GeneratedScriptUpsertRequest(BaseModel):
    script_key: str = Field(..., description="Stable script key used by workflow step config")
    script_name: str = Field(..., description="Human-readable script name")
    script_source: str = Field(..., description="Python source code implementing the configured entrypoint")
    entrypoint: str = Field(default="run", description="Callable name in script_source, default run")
    approval_status: str = Field(default="draft", description="draft, approved, deprecated, archived")
    is_active: bool = Field(default=True, description="Whether this script is selectable for execution")
    created_by: str | None = Field(default=None, description="User/system actor saving this script")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional script metadata")


class GeneratedScriptActivationRequest(BaseModel):
    approval_status: str = Field(default="approved", description="draft, approved, deprecated, archived")
    is_active: bool = Field(default=True, description="Activate/deactivate this script")


class GeneratedScriptPolicyCheckRequest(BaseModel):
    script_source: str = Field(default="", description="Python source to validate against IDMS execution policy")
    script_key: str = Field(default="", description="Optional script key; when provided and script_source empty, validates DB script")


class GeneratedScriptPromptPackageRequest(BaseModel):
    workflow_version_id: int | None = Field(default=None, ge=1, description="Optional workflow version id to include contracts")
    step_key: str | None = Field(default=None, description="Optional workflow step key to include step config")
    objective: str = Field(default="", description="Optional objective text embedded in the prompt package")
    entrypoint: str = Field(default="run", description="Target entrypoint function name for generated script")


class GeneratedScriptCleanupRequest(BaseModel):
    script_keys: list[str] = Field(default_factory=list, description="Generated script keys to clean up")
    workflow_keys: list[str] = Field(default_factory=list, description="Workflow keys to clean up")
    deactivate_only: bool = Field(default=True, description="When true, only deactivate/deprecate scripts")
    remove_runs: bool = Field(default=False, description="When true, remove pipeline run rows for listed workflow keys")
    remove_workflow_versions: bool = Field(default=False, description="When true, remove workflow version rows")
    remove_workflow_registry: bool = Field(default=False, description="When true, remove workflow registry rows")
    hard_purge_audits: bool = Field(
        default=False,
        description="When true, permanently delete generated-script audit rows for matching script_keys and workflow-linked runs",
    )
    dry_run: bool = Field(default=True, description="Preview changes without mutating data")


# ---------------------------------------------------------------------------
# Executor factory (lazy singleton per process)
# ---------------------------------------------------------------------------


def _get_executor() -> WorkflowPipelineExecutor:
    """Build executor using the same DB connection factory as the rest of the API.

    We reuse the interaction tools singleton to obtain the loaded SOLF interpreter
    (base script + active runtime business rules), so clause-steps in workflow
    runs are executable.
    """
    solf_interpreter = None
    try:
        tools = deps.get_tools()
        ensure_loaded = getattr(tools, "_ensure_solf_interpreter_loaded", None)
        if callable(ensure_loaded):
            try:
                ensure_loaded(wait_for_load=True)
            except Exception:
                LOGGER.exception("Failed to force-load SOLF interpreter for pipeline executor")
        solf_interpreter = getattr(tools, "solf_interpreter", None)
    except Exception:
        LOGGER.exception("Failed to obtain SOLF interpreter for pipeline executor")

    return WorkflowPipelineExecutor(
        db_connection_fn=object_db.get_connection,
        solf_interpreter=solf_interpreter,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("", summary="Start a new pipeline run")
def start_pipeline_run(request: PipelineRunStartRequest) -> dict[str, Any]:
    """Create and immediately begin executing a workflow pipeline.

    The response contains the run state. If a step requires user interaction
    or missing data, the run will be in `paused` status with the blocking step
    details populated.
    """
    executor = _get_executor()
    try:
        result = executor.start_pipeline_run(
            workflow_version_id=request.workflow_version_id,
            input_context=request.input_context,
            started_by=request.started_by,
            workflow_key=request.workflow_key,
        )
    except Exception as exc:
        LOGGER.exception("pipeline start failed workflow_version_id=%s", request.workflow_version_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if isinstance(result, dict) and result.get("error"):
        raise HTTPException(status_code=400, detail=result.get("message", result["error"]))

    return {"success": True, "run": result}


@router.post("/start-by-key", summary="Start a new pipeline run by workflow key")
def start_pipeline_run_by_key(request: PipelineRunStartByKeyRequest) -> dict[str, Any]:
    """Resolve the active workflow_version_id for the key, then start the run."""
    resolved = business_rules.get_solf_workflow_active_version_by_key(workflow_key=request.workflow_key)
    if resolved is None:
        raise HTTPException(status_code=404, detail=f"workflow key {request.workflow_key} not found")

    workflow_version_id = resolved.get("workflow_version_id")
    if workflow_version_id is None:
        raise HTTPException(status_code=409, detail=f"workflow key {request.workflow_key} has no active version")

    executor = _get_executor()
    try:
        result = executor.start_pipeline_run(
            workflow_version_id=int(workflow_version_id),
            input_context=request.input_context,
            started_by=request.started_by,
            workflow_key=request.workflow_key,
        )
    except Exception as exc:
        LOGGER.exception("pipeline start-by-key failed workflow_key=%s", request.workflow_key)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if isinstance(result, dict) and result.get("error"):
        raise HTTPException(status_code=400, detail=result.get("message", result["error"]))

    return {
        "success": True,
        "resolved": {
            "workflow_key": resolved.get("workflow_key") or request.workflow_key,
            "workflow_name": resolved.get("workflow_name"),
            "workflow_id": resolved.get("workflow_id"),
            "workflow_version_id": int(workflow_version_id),
        },
        "run": result,
    }


@router.post("/{run_id}/resume", summary="Resume a paused pipeline run")
def resume_pipeline_run(run_id: int, request: PipelineRunResumeRequest) -> dict[str, Any]:
    """Supply the information needed by the paused step and continue execution.

    - Pass `user_response` to provide field values or answers requested by an
      interaction step.
    - Pass `doc_refs` (list of doc_ids) to indicate that new documents have been
      ingested and the step should retry with the updated database state.
    """
    executor = _get_executor()
    try:
        result = executor.resume_pipeline_run(
            run_id=run_id,
            user_response=request.user_response,
            doc_refs=request.doc_refs,
        )
    except Exception as exc:
        LOGGER.exception("pipeline resume failed run_id=%s", run_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if isinstance(result, dict) and result.get("error"):
        err = result["error"]
        status_code = 404 if err == "not_found" else 400
        raise HTTPException(status_code=status_code, detail=result.get("message", err))

    return {"success": True, "run": result}


@router.get("/unfinished", summary="List unfinished pipeline tasks")
def list_unfinished_pipeline_runs(
    workflow_key: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Return pending, running, and paused runs for client-side task recovery."""
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    executor = _get_executor()
    try:
        runs = executor.list_unfinished_runs(workflow_key=workflow_key, limit=limit)
    except Exception as exc:
        LOGGER.exception("pipeline unfinished runs failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "success": True,
        "count": len(runs),
        "limit": limit,
        "unfinished_statuses": ["pending", "running", "paused"],
        "runs": runs,
    }


@router.get("/{run_id}/mutation-journal", summary="List workflow mutation journal entries for a run")
def get_pipeline_run_mutation_journal(
    run_id: int,
    include_undone: bool = True,
    limit: int = 500,
) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit), 5000))
    with object_db.get_connection() as conn:
        run = object_db.get_pipeline_run(conn, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"Pipeline run {run_id} not found")
        rows = object_db.list_workflow_mutation_journal(
            conn,
            run_id=run_id,
            include_undone=bool(include_undone),
            limit=safe_limit,
        )
    return {
        "success": True,
        "run_id": int(run_id),
        "count": len(rows),
        "limit": safe_limit,
        "include_undone": bool(include_undone),
        "journal": rows,
    }


@router.post("/{run_id}/undo-execute", summary="Execute workflow compensation undo for a run")
def undo_pipeline_run(run_id: int, request: PipelineRunUndoRequest) -> dict[str, Any]:
    executor = _get_executor()
    try:
        result = executor.undo_pipeline_run(
            run_id=run_id,
            requested_by=request.requested_by,
            dry_run=bool(request.dry_run),
            include_completed_only=bool(request.include_completed_only),
        )
    except Exception as exc:
        LOGGER.exception("pipeline undo failed run_id=%s", run_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if isinstance(result, dict) and result.get("error"):
        err = result["error"]
        status_code = 404 if err == "not_found" else 400
        raise HTTPException(status_code=status_code, detail=result.get("message", err))

    return {"success": True, "result": result}


@router.post("/{run_id}/cancel", summary="Cancel a pipeline run")
def cancel_pipeline_run(run_id: int) -> dict[str, Any]:
    executor = _get_executor()
    try:
        cancelled = executor.cancel_pipeline_run(run_id)
    except Exception as exc:
        LOGGER.exception("pipeline cancel failed run_id=%s", run_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not cancelled:
        raise HTTPException(
            status_code=409,
            detail=f"Run {run_id} could not be cancelled (not found or already in terminal state)",
        )

    return {"success": True, "run_id": run_id, "run_status": "cancelled"}


@router.get("/{run_id}", summary="Get pipeline run state and step details")
def get_pipeline_run(run_id: int) -> dict[str, Any]:
    executor = _get_executor()
    try:
        state = executor.get_run_state(run_id)
    except Exception as exc:
        LOGGER.exception("pipeline get_state failed run_id=%s", run_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if state is None:
        raise HTTPException(status_code=404, detail=f"Pipeline run {run_id} not found")

    return {"success": True, "run": state}


@router.post("/{run_id}/output/save", summary="Persist workflow run output to a file")
def save_pipeline_run_output(run_id: int, request: PipelineRunOutputSaveRequest) -> dict[str, Any]:
    executor = _get_executor()
    try:
        state = executor.get_run_state(run_id)
    except Exception as exc:
        LOGGER.exception("pipeline get_state failed run_id=%s", run_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if state is None:
        raise HTTPException(status_code=404, detail=f"Pipeline run {run_id} not found")

    source = str(request.payload_source or "auto").strip().lower()
    if source not in {"auto", "output_context", "current_context", "run"}:
        raise HTTPException(status_code=400, detail="payload_source must be one of auto, output_context, current_context, run")

    output_context = state.get("output_context") if isinstance(state.get("output_context"), dict) else {}
    current_context = state.get("current_context") if isinstance(state.get("current_context"), dict) else {}

    resolved_source = source
    if source == "auto":
        if output_context:
            payload = output_context
            resolved_source = "output_context"
        elif current_context:
            payload = current_context
            resolved_source = "current_context"
        else:
            payload = state
            resolved_source = "run"
    elif source == "output_context":
        payload = output_context
    elif source == "current_context":
        payload = current_context
    else:
        payload = state

    fmt = str(request.format or "json").strip().lower()
    if fmt not in {"json", "txt"}:
        raise HTTPException(status_code=400, detail="format must be json or txt")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_status = str(state.get("run_status") or "unknown").strip().lower() or "unknown"
    file_base = request.file_name or f"workflow_run_{run_id}_{run_status}_{timestamp}"
    safe_base = _sanitize_output_filename(file_base)
    extension = ".json" if fmt == "json" else ".txt"
    filename = f"{safe_base}{extension}"

    envelope: dict[str, Any] = {
        "saved_at_utc": timestamp,
        "run_id": int(run_id),
        "workflow_key": state.get("workflow_key"),
        "workflow_version_id": state.get("workflow_version_id"),
        "run_status": state.get("run_status"),
        "payload_source": resolved_source,
        "payload": payload,
    }
    if bool(request.include_steps):
        envelope["steps"] = state.get("steps") if isinstance(state.get("steps"), list) else []

    if fmt == "json":
        content_text = json.dumps(
            envelope,
            ensure_ascii=False,
            indent=2 if bool(request.pretty) else None,
            default=str,
        )
        media_type = "application/json"
    else:
        if isinstance(payload, str):
            payload_text = payload
        else:
            payload_text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
        lines = [
            f"run_id: {run_id}",
            f"workflow_key: {state.get('workflow_key')}",
            f"workflow_version_id: {state.get('workflow_version_id')}",
            f"run_status: {state.get('run_status')}",
            f"payload_source: {resolved_source}",
            "",
            payload_text,
        ]
        content_text = "\n".join(lines)
        media_type = "text/plain"

    target = _resolve_output_file_path(
        filename=filename,
        output_path=request.output_path,
        output_dir=request.output_dir,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content_text, encoding="utf-8")

    return {
        "success": True,
        "run_id": int(run_id),
        "run_status": state.get("run_status"),
        "workflow_key": state.get("workflow_key"),
        "workflow_version_id": state.get("workflow_version_id"),
        "format": fmt,
        "media_type": media_type,
        "payload_source": resolved_source,
        "saved_path": str(target),
        "size_bytes": len(content_text.encode("utf-8")),
        "project_root": str(PROJECT_ROOT),
    }


@router.get("/{run_id}/documents", summary="List documents linked to a pipeline run")
def get_pipeline_run_documents(run_id: int) -> dict[str, Any]:
    executor = _get_executor()
    try:
        state = executor.get_run_state(run_id)
    except Exception as exc:
        LOGGER.exception("pipeline get_state failed run_id=%s", run_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if state is None:
        raise HTTPException(status_code=404, detail=f"Pipeline run {run_id} not found")

    with object_db.get_connection() as conn:
        links = object_db.list_pipeline_run_document_links(conn, run_id=run_id)

    return {"success": True, "run_id": run_id, "count": len(links), "documents": links}


@router.get("/by-document/{document_id}", summary="List pipeline runs that processed a document")
def list_pipeline_runs_by_document(
    document_id: int,
    run_status: str | None = None,
    workflow_key: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    with object_db.get_connection() as conn:
        runs = object_db.list_pipeline_runs_by_document(
            conn,
            document_id=document_id,
            run_status=run_status,
            workflow_key=workflow_key,
            limit=limit,
        )
    return {"success": True, "document_id": document_id, "count": len(runs), "runs": runs}


def _build_rerun_decision(item: dict[str, Any]) -> dict[str, Any]:
    status = str(item.get("run_status") or "").strip().lower()
    should_rerun = False
    action = "review"
    priority = "low"
    reason = "No strong signal available; review workflow intent and changes."

    if status in {"running", "pending"}:
        should_rerun = False
        action = "defer"
        priority = "low"
        reason = "Workflow has an active run; wait for completion before rerun."
    elif status == "paused":
        should_rerun = False
        action = "resume"
        priority = "medium"
        reason = "Workflow is paused; resume with updated context/documents first."
    elif status == "failed":
        should_rerun = True
        action = "rerun"
        priority = "high"
        reason = "Latest run failed; rerun after applying document changes and checking failure cause."
    elif status in {"completed", "cancelled"}:
        should_rerun = True
        action = "rerun"
        priority = "medium"
        reason = "Latest run is terminal; rerun to propagate document changes."

    return {
        "workflow_group_key": item.get("workflow_group_key"),
        "workflow_key": item.get("workflow_key"),
        "workflow_version_id": item.get("workflow_version_id"),
        "latest_run_id": item.get("run_id"),
        "latest_run_status": item.get("run_status"),
        "latest_run_created_at": item.get("created_at"),
        "should_rerun": should_rerun,
        "recommended_action": action,
        "priority": priority,
        "reason": reason,
    }


def _build_start_by_key_payload(
    item: dict[str, Any],
    document_id: int,
    started_by: str,
) -> dict[str, Any] | None:
    workflow_key = str(item.get("workflow_key") or "").strip()
    if not workflow_key:
        return None

    input_context = dict(item.get("input_context") or {})
    existing_doc_refs = list(input_context.get("doc_refs") or [])
    if int(document_id) not in [int(d) for d in existing_doc_refs if str(d).isdigit()]:
        existing_doc_refs.append(int(document_id))
    input_context["doc_refs"] = existing_doc_refs
    input_context["_rerun"] = {
        "trigger": "document_change",
        "document_id": int(document_id),
        "source_run_id": item.get("run_id"),
    }

    return {
        "workflow_key": workflow_key,
        "input_context": input_context,
        "started_by": started_by,
    }


def _execute_start_by_key_payload(executor: WorkflowPipelineExecutor, payload: dict[str, Any]) -> dict[str, Any]:
    workflow_key = str(payload.get("workflow_key") or "").strip()
    if not workflow_key:
        return {"success": False, "error": "missing_workflow_key", "message": "workflow_key is required"}

    resolved = business_rules.get_solf_workflow_active_version_by_key(workflow_key=workflow_key)
    if resolved is None:
        return {"success": False, "error": "workflow_not_found", "message": f"workflow key {workflow_key} not found"}

    workflow_version_id = resolved.get("workflow_version_id")
    if workflow_version_id is None:
        return {
            "success": False,
            "error": "no_active_version",
            "message": f"workflow key {workflow_key} has no active version",
        }

    result = executor.start_pipeline_run(
        workflow_version_id=int(workflow_version_id),
        input_context=dict(payload.get("input_context") or {}),
        started_by=payload.get("started_by"),
        workflow_key=workflow_key,
    )

    if isinstance(result, dict) and result.get("error"):
        return {
            "success": False,
            "error": str(result.get("error")),
            "message": str(result.get("message") or result.get("error")),
            "workflow_key": workflow_key,
            "workflow_version_id": int(workflow_version_id),
        }

    return {
        "success": True,
        "workflow_key": workflow_key,
        "workflow_version_id": int(workflow_version_id),
        "run": result,
    }


@router.get("/by-document/{document_id}/rerun-recommendations", summary="Rerun recommendations for a changed document")
def get_rerun_recommendations_for_document(
    document_id: int,
    workflow_key: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    with object_db.get_connection() as conn:
        latest = object_db.list_latest_pipeline_runs_by_document_workflow(
            conn,
            document_id=document_id,
            workflow_key=workflow_key,
            limit=limit,
        )

    recommendations = [_build_rerun_decision(item) for item in latest]

    return {
        "success": True,
        "document_id": document_id,
        "count": len(recommendations),
        "recommendations": recommendations,
    }


@router.get("/by-document/{document_id}/rerun-payloads", summary="Executable start-by-key payloads for recommended reruns")
def get_rerun_payloads_for_document(
    document_id: int,
    workflow_key: str | None = None,
    limit: int = 100,
    include_non_rerun: bool = False,
    started_by: str = "api:rerun-recommendation",
) -> dict[str, Any]:
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    with object_db.get_connection() as conn:
        latest = object_db.list_latest_pipeline_runs_by_document_workflow(
            conn,
            document_id=document_id,
            workflow_key=workflow_key,
            limit=limit,
        )

    payloads: list[dict[str, Any]] = []
    for item in latest:
        decision = _build_rerun_decision(item)
        if not include_non_rerun and not bool(decision.get("should_rerun")):
            continue

        start_payload = _build_start_by_key_payload(item, document_id=document_id, started_by=started_by)
        if start_payload is None:
            payloads.append(
                {
                    "workflow_group_key": decision.get("workflow_group_key"),
                    "workflow_key": decision.get("workflow_key"),
                    "latest_run_id": decision.get("latest_run_id"),
                    "latest_run_status": decision.get("latest_run_status"),
                    "should_rerun": decision.get("should_rerun"),
                    "recommended_action": decision.get("recommended_action"),
                    "priority": decision.get("priority"),
                    "reason": "Cannot build start-by-key payload because workflow_key is missing.",
                    "executable": False,
                    "start_by_key_payload": None,
                }
            )
            continue

        payloads.append(
            {
                "workflow_group_key": decision.get("workflow_group_key"),
                "workflow_key": decision.get("workflow_key"),
                "latest_run_id": decision.get("latest_run_id"),
                "latest_run_status": decision.get("latest_run_status"),
                "should_rerun": decision.get("should_rerun"),
                "recommended_action": decision.get("recommended_action"),
                "priority": decision.get("priority"),
                "reason": decision.get("reason"),
                "executable": True,
                "start_by_key_payload": start_payload,
            }
        )

    return {
        "success": True,
        "document_id": document_id,
        "count": len(payloads),
        "payloads": payloads,
    }


@router.post("/by-document/{document_id}/rerun-execute", summary="Execute reruns for a changed document")
def execute_reruns_for_document(
    document_id: int,
    request: PipelineRunRerunExecuteRequest,
) -> dict[str, Any]:
    planned = get_rerun_payloads_for_document(
        document_id=document_id,
        workflow_key=request.workflow_key,
        limit=request.limit,
        include_non_rerun=request.include_non_rerun,
        started_by=request.started_by,
    )
    payloads = list(planned.get("payloads") or [])

    if request.max_runs is not None:
        payloads = payloads[: int(request.max_runs)]

    if request.dry_run:
        return {
            "success": True,
            "document_id": document_id,
            "dry_run": True,
            "planned_count": len(payloads),
            "planned": payloads,
        }

    executor = _get_executor()
    executed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for item in payloads:
        payload = item.get("start_by_key_payload")
        if not bool(item.get("executable")) or not isinstance(payload, dict):
            skipped.append(
                {
                    "workflow_key": item.get("workflow_key"),
                    "reason": item.get("reason") or "payload_not_executable",
                }
            )
            continue

        try:
            execution = _execute_start_by_key_payload(executor, payload)
        except Exception as exc:
            execution = {"success": False, "error": "execution_exception", "message": str(exc)}

        if bool(execution.get("success")):
            run = execution.get("run") or {}
            executed.append(
                {
                    "workflow_key": execution.get("workflow_key"),
                    "workflow_version_id": execution.get("workflow_version_id"),
                    "run_id": run.get("run_id"),
                    "run_status": run.get("run_status"),
                }
            )
            continue

        failed.append(
            {
                "workflow_key": item.get("workflow_key") or execution.get("workflow_key"),
                "error": execution.get("error") or "execution_failed",
                "message": execution.get("message"),
            }
        )
        if not request.continue_on_error:
            break

    return {
        "success": True,
        "document_id": document_id,
        "dry_run": False,
        "planned_count": len(payloads),
        "executed_count": len(executed),
        "failed_count": len(failed),
        "skipped_count": len(skipped),
        "executed": executed,
        "failed": failed,
        "skipped": skipped,
    }


@router.get("", summary="List pipeline runs")
def list_pipeline_runs(
    run_status: str | None = None,
    workflow_key: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """List pipeline runs, optionally filtered by status or workflow key.

    Common `run_status` values: `pending`, `running`, `paused`, `completed`, `failed`, `cancelled`.
    """
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    executor = _get_executor()
    try:
        runs = executor.list_runs(run_status=run_status, workflow_key=workflow_key, limit=limit)
    except Exception as exc:
        LOGGER.exception("pipeline list_runs failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {"success": True, "count": len(runs), "runs": runs}


@router.get("/scripts/generated/codegen-context", summary="Get IDMS python codegen context for LLM prompting")
def get_generated_script_codegen_context(limit_packages: int = 300) -> dict[str, Any]:
    max_packages = max(10, min(int(limit_packages or 300), 1000))
    try:
        import workflow_pipeline_builtin_steps as builtin_steps

        builtins_catalog: list[dict[str, Any]] = []
        for name, fn in inspect.getmembers(builtin_steps, inspect.isfunction):
            if name.startswith("_"):
                continue
            try:
                sig = str(inspect.signature(fn))
            except Exception:
                sig = "(context, config)"
            builtins_catalog.append({
                "name": name,
                "signature": sig,
                "doc": str(inspect.getdoc(fn) or "").strip(),
            })
    except Exception:
        builtins_catalog = []

    dists = sorted(importlib.metadata.distributions(), key=lambda d: str(d.metadata.get("Name") or "").lower())
    packages: list[dict[str, str]] = []
    for dist in dists[:max_packages]:
        name = str(dist.metadata.get("Name") or "").strip()
        version = str(dist.version or "").strip()
        if name:
            packages.append({"name": name, "version": version})

    return {
        "success": True,
        "result": {
            "python_version": sys.version,
            "entrypoint_contract": {
                "signature": "def run(context: dict, config: dict) -> dict|Any",
                "pause_exception": "PipelineStepPauseRequired(reason, prompt, missing_data_desc, required_doc_types)",
                "pause_dict_signal": {"paused": True, "reason": "user_interaction|missing_data|approval_required"},
            },
            "workflow_step_config": {
                "step_kind": "python_binding",
                "config_keys": ["generated_script_id", "generated_script_key", "entrypoint"],
            },
            "builtin_pipeline_functions": builtins_catalog,
            "installed_packages": packages,
        },
    }


@router.post("/scripts/generated", summary="Create or update generated workflow python script")
def upsert_generated_script(request: GeneratedScriptUpsertRequest) -> dict[str, Any]:
    script_key = str(request.script_key or "").strip()
    if not _SCRIPT_KEY_PATTERN.fullmatch(script_key):
        raise HTTPException(status_code=400, detail="script_key must match ^[a-zA-Z0-9][a-zA-Z0-9._:-]{2,127}$")

    script_name = str(request.script_name or "").strip()
    script_source = str(request.script_source or "")
    entrypoint = str(request.entrypoint or "run").strip() or "run"
    approval_status = str(request.approval_status or "draft").strip().lower() or "draft"
    if approval_status not in {"draft", "approved", "deprecated", "archived"}:
        raise HTTPException(status_code=400, detail="approval_status must be one of draft, approved, deprecated, archived")
    if not script_name:
        raise HTTPException(status_code=400, detail="script_name is required")
    if not script_source.strip():
        raise HTTPException(status_code=400, detail="script_source is required")

    try:
        compile(script_source, f"<generated-script:{script_key}>", "exec")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"script_source compile failed: {exc}") from exc

    policy = _analyze_script_policy(script_source)
    if approval_status == "approved" and not bool(policy.get("allowed")):
        raise HTTPException(
            status_code=400,
            detail={
                "message": "script_source violates execution policy and cannot be approved",
                "policy": policy,
            },
        )

    with object_db.get_connection() as conn:
        _ensure_generated_script_table(conn)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO workflow_generated_python_script (
                    tenant_id, configuration_scope, script_key, script_name, script_source, entrypoint,
                    approval_status, metadata, is_active, created_by, created_at, modified_at
                )
                VALUES (%s::uuid, 'tenant', %s, %s, %s, %s, %s, %s::jsonb, %s, %s, NOW(), NOW())
                ON CONFLICT (tenant_id, configuration_scope, script_key)
                DO UPDATE SET
                    script_name = EXCLUDED.script_name,
                    script_source = EXCLUDED.script_source,
                    entrypoint = EXCLUDED.entrypoint,
                    approval_status = EXCLUDED.approval_status,
                    metadata = COALESCE(workflow_generated_python_script.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                    is_active = EXCLUDED.is_active,
                    created_by = COALESCE(EXCLUDED.created_by, workflow_generated_python_script.created_by),
                    modified_at = NOW()
                RETURNING script_id, script_key, script_name, entrypoint, approval_status,
                          metadata, is_active, created_by, created_at, modified_at
                """,
                (
                    object_db.get_tenant_id(),
                    script_key,
                    script_name,
                    script_source,
                    entrypoint,
                    approval_status,
                    json.dumps(request.metadata or {}),
                    bool(request.is_active),
                    str(request.created_by or "").strip() or None,
                ),
            )
            row = cursor.fetchone()
        conn.commit()

    return {
        "success": True,
        "result": {
            "script_id": int(row[0]),
            "script_key": row[1],
            "script_name": row[2],
            "entrypoint": row[3],
            "approval_status": row[4],
            "metadata": row[5] or {},
            "is_active": bool(row[6]),
            "created_by": row[7],
            "created_at": row[8],
            "modified_at": row[9],
        },
        "policy": policy,
    }


@router.get("/scripts/generated", summary="List generated workflow python scripts")
def list_generated_scripts(
    approval_status: str | None = None,
    include_inactive: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    effective_limit = max(1, min(int(limit or 100), 500))
    normalized_status = str(approval_status or "").strip().lower() or None
    if normalized_status is not None and normalized_status not in {"draft", "approved", "deprecated", "archived"}:
        raise HTTPException(status_code=400, detail="approval_status must be one of draft, approved, deprecated, archived")

    with object_db.get_connection() as conn:
        _ensure_generated_script_table(conn)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT script_id, script_key, script_name, entrypoint, approval_status,
                       metadata, is_active, created_by, created_at, modified_at
                FROM workflow_generated_python_script
                WHERE (%s IS NULL OR approval_status = %s)
                  AND (%s = TRUE OR is_active = TRUE)
                ORDER BY modified_at DESC, script_id DESC
                LIMIT %s
                """,
                (normalized_status, normalized_status, bool(include_inactive), effective_limit),
            )
            rows = cursor.fetchall() or []

    result = [
        {
            "script_id": int(r[0]),
            "script_key": r[1],
            "script_name": r[2],
            "entrypoint": r[3],
            "approval_status": r[4],
            "metadata": r[5] or {},
            "is_active": bool(r[6]),
            "created_by": r[7],
            "created_at": r[8],
            "modified_at": r[9],
        }
        for r in rows
    ]
    return {"success": True, "count": len(result), "result": result}


@router.patch("/scripts/generated/{script_key}/activation", summary="Update script approval status and active flag")
def set_generated_script_activation(script_key: str, request: GeneratedScriptActivationRequest) -> dict[str, Any]:
    key = str(script_key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="script_key is required")

    approval_status = str(request.approval_status or "approved").strip().lower() or "approved"
    if approval_status not in {"draft", "approved", "deprecated", "archived"}:
        raise HTTPException(status_code=400, detail="approval_status must be one of draft, approved, deprecated, archived")

    with object_db.get_connection() as conn:
        _ensure_generated_script_table(conn)
        if approval_status == "approved":
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT script_source
                    FROM workflow_generated_python_script
                    WHERE script_key = %s
                    ORDER BY CASE WHEN configuration_scope = 'tenant' THEN 0 ELSE 1 END
                    LIMIT 1
                    """,
                    (key,),
                )
                source_row = cursor.fetchone()
            if not source_row:
                raise HTTPException(status_code=404, detail=f"generated script '{key}' not found")
            policy = _analyze_script_policy(str(source_row[0] or ""))
            if not bool(policy.get("allowed")):
                raise HTTPException(
                    status_code=400,
                    detail={
                        "message": "Cannot set approval_status=approved because script violates execution policy",
                        "policy": policy,
                    },
                )

        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE workflow_generated_python_script
                SET approval_status = %s,
                    is_active = %s,
                    modified_at = NOW()
                WHERE tenant_id = %s::uuid AND script_key = %s AND configuration_scope = 'tenant'
                RETURNING script_id, script_key, script_name, entrypoint, approval_status,
                          metadata, is_active, created_by, created_at, modified_at
                """,
                (approval_status, bool(request.is_active), object_db.get_tenant_id(), key),
            )
            row = cursor.fetchone()
        conn.commit()

    if not row:
        raise HTTPException(status_code=404, detail=f"generated script '{key}' not found")

    return {
        "success": True,
        "result": {
            "script_id": int(row[0]),
            "script_key": row[1],
            "script_name": row[2],
            "entrypoint": row[3],
            "approval_status": row[4],
            "metadata": row[5] or {},
            "is_active": bool(row[6]),
            "created_by": row[7],
            "created_at": row[8],
            "modified_at": row[9],
        },
    }


@router.post("/scripts/generated/policy-check", summary="Validate generated script source against IDMS execution policy")
def check_generated_script_policy(request: GeneratedScriptPolicyCheckRequest) -> dict[str, Any]:
    source = str(request.script_source or "")
    key = str(request.script_key or "").strip()

    if not source.strip() and key:
        with object_db.get_connection() as conn:
            _ensure_generated_script_table(conn)
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT script_source
                    FROM workflow_generated_python_script
                    WHERE script_key = %s
                                            AND tenant_id = %s::uuid
                    LIMIT 1
                    """,
                                        (key, object_db.get_tenant_id()),
                )
                row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"generated script '{key}' not found")
        source = str(row[0] or "")

    if not source.strip():
        raise HTTPException(status_code=400, detail="Provide script_source or script_key")

    policy = _analyze_script_policy(source)
    return {
        "success": True,
        "result": {
            "allowed": bool(policy.get("allowed")),
            "violations": policy.get("violations") or [],
            "warnings": policy.get("warnings") or [],
        },
    }


@router.post("/scripts/generated/prompt-package", summary="Build LLM prompt package for workflow script generation")
def build_generated_script_prompt_package(request: GeneratedScriptPromptPackageRequest) -> dict[str, Any]:
    codegen_context = get_generated_script_codegen_context().get("result") or {}

    workflow_contract: dict[str, Any] = {}
    step_schema: dict[str, Any] = {}

    if request.workflow_version_id is not None:
        with object_db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT workflow_version_id, input_contract, output_contract, metadata
                    FROM solf_workflow_versions
                    WHERE workflow_version_id = %s
                    LIMIT 1
                    """,
                    (int(request.workflow_version_id),),
                )
                version_row = cursor.fetchone()
                if not version_row:
                    raise HTTPException(status_code=404, detail=f"workflow_version_id {request.workflow_version_id} not found")

                workflow_contract = {
                    "workflow_version_id": int(version_row[0]),
                    "input_contract": version_row[1] or {},
                    "output_contract": version_row[2] or {},
                    "metadata": version_row[3] or {},
                }

                if str(request.step_key or "").strip():
                    cursor.execute(
                        """
                        SELECT step_key, step_order, step_kind, config, python_module, python_function
                        FROM solf_workflow_steps
                        WHERE workflow_version_id = %s
                          AND step_key = %s
                        LIMIT 1
                        """,
                        (int(request.workflow_version_id), str(request.step_key or "").strip()),
                    )
                    step_row = cursor.fetchone()
                    if not step_row:
                        raise HTTPException(
                            status_code=404,
                            detail=f"step_key '{request.step_key}' not found for workflow_version_id {request.workflow_version_id}",
                        )
                    step_schema = {
                        "step_key": step_row[0],
                        "step_order": int(step_row[1] or 0),
                        "step_kind": step_row[2],
                        "config": step_row[3] or {},
                        "python_module": step_row[4],
                        "python_function": step_row[5],
                    }

    objective = str(request.objective or "").strip()
    objective_line = objective if objective else "No explicit objective supplied; infer from workflow contracts and step schema."

    prompt_text = "\n".join(
        [
            "Generate Python code for an IDMS workflow python_binding step.",
            f"Entrypoint must be: {str(request.entrypoint or 'run').strip() or 'run'}(context, config)",
            "Return a dict output suitable for context merge; use pause signaling when needed.",
            f"Objective: {objective_line}",
            "Use only installed packages and runtime capabilities described in codegen_context.",
            "Do not use blocked imports/calls from policy violations.",
        ]
    )

    return {
        "success": True,
        "result": {
            "prompt_text": prompt_text,
            "codegen_context": codegen_context,
            "workflow_contract": workflow_contract,
            "step_schema": step_schema,
        },
    }


@router.get("/scripts/generated/audits", summary="List generated script execution audit events")
def list_generated_script_audits(
    run_id: int | None = None,
    script_key: str | None = None,
    execution_status: str | None = None,
    idempotency_key: str | None = None,
    executed_from: str | None = None,
    executed_to: str | None = None,
    page: int = 1,
    page_size: int = 100,
) -> dict[str, Any]:
    effective_page = max(1, int(page or 1))
    effective_page_size = max(1, min(int(page_size or 100), 1000))
    offset = (effective_page - 1) * effective_page_size
    normalized_from = str(executed_from or "").strip().replace(" ", "+") or None
    normalized_to = str(executed_to or "").strip().replace(" ", "+") or None

    with object_db.get_connection() as conn:
        _ensure_generated_script_table(conn)
        _ensure_generated_script_audit_table(conn)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM workflow_generated_python_script_audit
                WHERE (%s IS NULL OR run_id = %s)
                  AND (%s IS NULL OR script_key = %s)
                  AND (%s IS NULL OR execution_status = %s)
                  AND (%s IS NULL OR idempotency_key = %s)
                  AND (%s IS NULL OR executed_at >= %s::timestamptz)
                  AND (%s IS NULL OR executed_at <= %s::timestamptz)
                """,
                (
                    int(run_id) if run_id is not None else None,
                    int(run_id) if run_id is not None else None,
                    str(script_key or "").strip() or None,
                    str(script_key or "").strip() or None,
                    str(execution_status or "").strip().lower() or None,
                    str(execution_status or "").strip().lower() or None,
                    str(idempotency_key or "").strip() or None,
                    str(idempotency_key or "").strip() or None,
                    normalized_from,
                    normalized_from,
                    normalized_to,
                    normalized_to,
                ),
            )
            total = int((cursor.fetchone() or [0])[0] or 0)

            cursor.execute(
                """
                SELECT audit_id, run_id, step_key, script_id, script_key, entrypoint,
                       execution_status, duration_ms, input_hash, output_hash,
                       error_message, metadata, idempotency_key, executed_at
                FROM workflow_generated_python_script_audit
                WHERE (%s IS NULL OR run_id = %s)
                  AND (%s IS NULL OR script_key = %s)
                  AND (%s IS NULL OR execution_status = %s)
                  AND (%s IS NULL OR idempotency_key = %s)
                  AND (%s IS NULL OR executed_at >= %s::timestamptz)
                  AND (%s IS NULL OR executed_at <= %s::timestamptz)
                ORDER BY executed_at DESC, audit_id DESC
                LIMIT %s OFFSET %s
                """,
                (
                    int(run_id) if run_id is not None else None,
                    int(run_id) if run_id is not None else None,
                    str(script_key or "").strip() or None,
                    str(script_key or "").strip() or None,
                    str(execution_status or "").strip().lower() or None,
                    str(execution_status or "").strip().lower() or None,
                    str(idempotency_key or "").strip() or None,
                    str(idempotency_key or "").strip() or None,
                    normalized_from,
                    normalized_from,
                    normalized_to,
                    normalized_to,
                    effective_page_size,
                    offset,
                ),
            )
            rows = cursor.fetchall() or []

    result = [
        {
            "audit_id": int(row[0]),
            "run_id": int(row[1]) if row[1] is not None else None,
            "step_key": row[2],
            "script_id": int(row[3]) if row[3] is not None else None,
            "script_key": row[4],
            "entrypoint": row[5],
            "execution_status": row[6],
            "duration_ms": int(row[7] or 0),
            "input_hash": row[8],
            "output_hash": row[9],
            "error_message": row[10],
            "metadata": row[11] or {},
            "idempotency_key": row[12],
            "executed_at": row[13],
        }
        for row in rows
    ]
    return {
        "success": True,
        "count": len(result),
        "total": total,
        "page": effective_page,
        "page_size": effective_page_size,
        "result": result,
    }


@router.post("/scripts/generated/test-artifacts/cleanup", summary="Deactivate/delete generated-script test artifacts")
def cleanup_generated_script_test_artifacts(request: GeneratedScriptCleanupRequest) -> dict[str, Any]:
    script_keys = [str(item or "").strip() for item in (request.script_keys or []) if str(item or "").strip()]
    workflow_keys = [str(item or "").strip() for item in (request.workflow_keys or []) if str(item or "").strip()]

    if not script_keys and not workflow_keys:
        raise HTTPException(status_code=400, detail="Provide at least one script_key or workflow_key")

    planned: list[dict[str, Any]] = []
    for key in script_keys:
        planned.append({
            "target": "generated_script",
            "script_key": key,
            "action": "deactivate+deprecate" if request.deactivate_only else "delete",
        })
    for key in workflow_keys:
        if request.hard_purge_audits:
            planned.append({"target": "generated_script_audits", "workflow_key": key, "action": "hard_delete"})
        if request.remove_runs:
            planned.append({"target": "workflow_runs", "workflow_key": key, "action": "delete"})
        if request.remove_workflow_versions:
            planned.append({"target": "workflow_versions", "workflow_key": key, "action": "delete"})
        if request.remove_workflow_registry:
            planned.append({"target": "workflow_registry", "workflow_key": key, "action": "delete"})

    for key in script_keys:
        if request.hard_purge_audits:
            planned.append({"target": "generated_script_audits", "script_key": key, "action": "hard_delete"})

    cleanup_commands = [
        "# PowerShell cleanup preview",
        "Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/pipeline-runs/scripts/generated/test-artifacts/cleanup' -ContentType 'application/json' -Body '<payload-json>'",
    ]

    if request.dry_run:
        return {
            "success": True,
            "dry_run": True,
            "planned_count": len(planned),
            "planned": planned,
            "cleanup_commands": cleanup_commands,
        }

    mutated: list[dict[str, Any]] = []
    with object_db.get_connection() as conn:
        _ensure_generated_script_table(conn)
        _ensure_generated_script_audit_table(conn)
        with conn.cursor() as cursor:
            if request.hard_purge_audits:
                for key in workflow_keys:
                    cursor.execute(
                        """
                        DELETE FROM workflow_generated_python_script_audit a
                        USING workflow_pipeline_run r
                        WHERE a.run_id = r.run_id
                          AND r.workflow_key = %s
                        """,
                        (key,),
                    )
                    mutated.append(
                        {
                            "target": "generated_script_audits",
                            "workflow_key": key,
                            "affected": int(cursor.rowcount or 0),
                            "action": "hard_delete",
                        }
                    )

                for key in script_keys:
                    cursor.execute(
                        "DELETE FROM workflow_generated_python_script_audit WHERE script_key = %s",
                        (key,),
                    )
                    mutated.append(
                        {
                            "target": "generated_script_audits",
                            "script_key": key,
                            "affected": int(cursor.rowcount or 0),
                            "action": "hard_delete",
                        }
                    )

            for key in script_keys:
                if request.deactivate_only:
                    cursor.execute(
                        """
                        UPDATE workflow_generated_python_script
                        SET is_active = FALSE,
                            approval_status = 'deprecated',
                            modified_at = NOW()
                        WHERE script_key = %s
                        """,
                        (key,),
                    )
                    mutated.append({"target": "generated_script", "script_key": key, "affected": int(cursor.rowcount or 0), "action": "deactivate+deprecate"})
                else:
                    cursor.execute("DELETE FROM workflow_generated_python_script WHERE script_key = %s", (key,))
                    mutated.append({"target": "generated_script", "script_key": key, "affected": int(cursor.rowcount or 0), "action": "delete"})

            for key in workflow_keys:
                if request.remove_runs:
                    cursor.execute("DELETE FROM workflow_pipeline_run WHERE workflow_key = %s", (key,))
                    mutated.append({"target": "workflow_runs", "workflow_key": key, "affected": int(cursor.rowcount or 0), "action": "delete"})

                if request.remove_workflow_versions or request.remove_workflow_registry:
                    cursor.execute("SELECT workflow_id FROM solf_workflow_registry WHERE workflow_key = %s", (key,))
                    wf_row = cursor.fetchone()
                    if wf_row:
                        workflow_id = int(wf_row[0])
                        if request.remove_workflow_versions:
                            cursor.execute("DELETE FROM solf_workflow_versions WHERE workflow_id = %s", (workflow_id,))
                            mutated.append({"target": "workflow_versions", "workflow_key": key, "affected": int(cursor.rowcount or 0), "action": "delete"})
                        if request.remove_workflow_registry:
                            cursor.execute("DELETE FROM solf_workflow_registry WHERE workflow_id = %s", (workflow_id,))
                            mutated.append({"target": "workflow_registry", "workflow_key": key, "affected": int(cursor.rowcount or 0), "action": "delete"})
        conn.commit()

    return {
        "success": True,
        "dry_run": False,
        "mutated_count": len(mutated),
        "mutated": mutated,
        "cleanup_commands": cleanup_commands,
    }
