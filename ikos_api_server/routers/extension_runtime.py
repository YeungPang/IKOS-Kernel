"""Tenant-authorized domain capability execution and ingestion outbox dispatch."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from jsonschema.exceptions import ValidationError

import object_db
from capability_registry import CAPABILITIES, UnknownCapabilityError
from ingestion_triggers import dispatch_one, enqueue_persisted_document, retry_failed
from ikos_api_server.deps import require_permission
from security_context import SecurityContext
from workflow_pipeline_executor import WorkflowPipelineExecutor

router = APIRouter(prefix="/api/extensions", tags=["extension-runtime"])


class CapabilityExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    key: str = Field(min_length=1, max_length=128)
    version: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)


@router.get("/capabilities")
def capability_inventory(context: SecurityContext = Depends(require_permission("tenant.configuration.manage"))) -> dict[str, Any]:
    # Descriptions only, never handler objects or server import paths.
    return {"success": True, "result": CAPABILITIES.inventory()}


@router.post("/capabilities/execute")
def execute_capability(request: CapabilityExecuteRequest,
                       context: SecurityContext = Depends(require_permission("workflows.run"))) -> dict[str, Any]:
    try:
        return {"success": True, "result": CAPABILITIES.execute(request.key, request.version, request.payload)}
    except UnknownCapabilityError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.message) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/ingestion-deliveries/dispatch-one")
def dispatch_ingestion_delivery(context: SecurityContext = Depends(require_permission("workflows.run"))) -> dict[str, Any]:
    if not context.allows("documents.write"):
        raise HTTPException(status_code=403, detail="documents.write is also required")
    connection = object_db.get_connection()
    try:
        result = dispatch_one(connection, lambda: WorkflowPipelineExecutor(object_db.get_connection))
        return {"success": True, "result": result}
    except PermissionError as exc:
        connection.rollback()
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/ingestion-deliveries/{delivery_id}/retry")
def retry_ingestion_delivery(delivery_id: int,
                             context: SecurityContext = Depends(require_permission("workflows.run"))) -> dict[str, Any]:
    if not context.allows("documents.write"):
        raise HTTPException(status_code=403, detail="documents.write is also required")
    connection = object_db.get_connection()
    try:
        if not retry_failed(connection, delivery_id):
            raise HTTPException(status_code=409, detail="No failed delivery available in this tenant")
        connection.commit()
        return {"success": True, "result": {"delivery_id": delivery_id, "status": "queued"}}
    except PermissionError as exc:
        connection.rollback()
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


@router.post("/documents/{doc_id}/enqueue")
def reconcile_document_event(doc_id: int,
                              context: SecurityContext = Depends(require_permission("documents.write"))) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        result = enqueue_persisted_document(connection, doc_id)
        connection.commit()
        return {"success": True, "result": result}
    except PermissionError as exc:
        connection.rollback()
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        connection.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
