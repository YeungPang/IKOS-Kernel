from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

import domain_db
from ingest import parse_solf_classes
import object_db
from ikos_api_server.schemas import (
    SolfAttributeProposalStatusRequest,
    SolfSchemaPromotionBatchAuditLinkRequest,
    SolfSchemaPromotionBatchCreateRequest,
    SolfSchemaPromotionBatchStatusRequest,
)


router = APIRouter(prefix="/api/schema-proposals", tags=["schema-proposals"])
LOGGER = logging.getLogger("idms.api")


def _solf_script_path() -> Path:
    return Path(__file__).resolve().parents[2] / "solf_script.txt"


def _sha256_text(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


def _build_solf_patch_preview(class_name: str, missing_attributes: list[str]) -> str:
    if not missing_attributes:
        return ""

    body = "\n".join(f"    {attribute_name}: Ø," for attribute_name in missing_attributes)
    return (
        f"{class_name} ≔ {{\n"
        f"    ... existing class fields ...\n"
        f"{body}\n"
        f"}}"
    )


def _build_patch_draft_result(
    approved_extensions: dict[str, list[str]],
    class_defs: dict[str, Any],
    class_name: str | None = None,
) -> dict[str, Any]:
    requested_class = str(class_name or "").strip().lower()
    class_names = sorted(approved_extensions.keys())
    if requested_class:
        class_names = [name for name in class_names if name == requested_class]

    classes: list[dict[str, Any]] = []
    patch_blocks: list[str] = []
    for current_class_name in class_names:
        approved_attributes = sorted({str(item).strip().lower() for item in approved_extensions.get(current_class_name, []) if str(item).strip()})
        existing_attributes = sorted(class_defs.get(current_class_name).allowed_attributes) if current_class_name in class_defs else []
        missing_attributes = sorted(attribute for attribute in approved_attributes if attribute not in set(existing_attributes))

        patch_preview = _build_solf_patch_preview(current_class_name, missing_attributes)
        classes.append(
            {
                "class_name": current_class_name,
                "approved_attributes": approved_attributes,
                "existing_attributes": existing_attributes,
                "missing_attributes": missing_attributes,
                "patch_preview": patch_preview,
                "class_exists_in_script": current_class_name in class_defs,
            }
        )
        if patch_preview:
            patch_blocks.append(patch_preview)

    return {
        "classes": classes,
        "solf_patch_preview": "\n\n".join(block for block in patch_blocks if block),
    }


def _build_schema_governance_summary(proposals: list[dict[str, Any]], batches: list[dict[str, Any]]) -> dict[str, Any]:
    proposal_status_counts = {"proposed": 0, "approved": 0, "rejected": 0}
    high_priority_open = 0
    suggested_for_approval = 0
    open_by_class: dict[str, int] = {}

    for proposal in proposals:
        status = str(proposal.get("status") or "").strip().lower()
        if status in proposal_status_counts:
            proposal_status_counts[status] += 1

        is_open = status == "proposed"
        class_name = str(proposal.get("class_name") or "").strip().lower()
        if is_open and class_name:
            open_by_class[class_name] = open_by_class.get(class_name, 0) + 1

        if is_open and str(proposal.get("priority_bucket") or "").strip().lower() == "high":
            high_priority_open += 1

        if is_open and bool(proposal.get("suggested_for_approval")):
            suggested_for_approval += 1

    top_open_classes = [
        {"class_name": item[0], "open_proposal_count": item[1]}
        for item in sorted(open_by_class.items(), key=lambda row: (-int(row[1]), row[0]))[:10]
    ]

    batch_status_counts = {"draft": 0, "reviewed": 0, "exported": 0, "applied": 0}
    pending_audit_links = 0
    open_batches = 0
    for batch in batches:
        status = str(batch.get("status") or "").strip().lower()
        if status in batch_status_counts:
            batch_status_counts[status] += 1
        if status in {"draft", "reviewed"}:
            open_batches += 1

        if status in {"exported", "applied"}:
            metadata = batch.get("metadata") if isinstance(batch.get("metadata"), dict) else {}
            if not str(metadata.get("solf_script_hash") or "").strip():
                pending_audit_links += 1

    return {
        "proposal_summary": {
            "total": len(proposals),
            "by_status": proposal_status_counts,
            "high_priority_open": high_priority_open,
            "suggested_for_approval": suggested_for_approval,
            "top_open_classes": top_open_classes,
        },
        "batch_summary": {
            "total": len(batches),
            "by_status": batch_status_counts,
            "open_batches": open_batches,
            "pending_audit_links": pending_audit_links,
        },
        "suggested_proposals_preview": [
            {
                "proposal_id": int(item.get("proposal_id") or 0),
                "class_name": str(item.get("class_name") or ""),
                "attribute_name": str(item.get("attribute_name") or ""),
                "occurrence_count": int(item.get("occurrence_count") or 0),
                "priority_score": int(item.get("priority_score") or 0),
                "priority_bucket": str(item.get("priority_bucket") or ""),
                "suggestion_reason": str(item.get("suggestion_reason") or ""),
            }
            for item in proposals
            if str(item.get("status") or "").strip().lower() == "proposed" and bool(item.get("suggested_for_approval"))
        ][:10],
    }


@router.get("")
def list_schema_proposals(
    status: str | None = None,
    class_name: str | None = None,
    priority_bucket: str | None = None,
    suggested_only: bool = False,
    limit: int = 200,
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        proposals = domain_db.list_solf_attribute_proposals(
            connection,
            status=status,
            class_name=class_name,
            priority_bucket=priority_bucket,
            suggested_only=suggested_only,
            limit=limit,
        )
        return {"success": True, "count": len(proposals), "result": proposals}
    except Exception as exc:
        LOGGER.exception("Failed to list schema proposals")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/summary")
def get_schema_governance_summary(proposals_limit: int = 500, batches_limit: int = 200) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        proposals = domain_db.list_solf_attribute_proposals(
            connection,
            status=None,
            class_name=None,
            priority_bucket=None,
            suggested_only=False,
            limit=proposals_limit,
        )
        batches = domain_db.list_solf_schema_promotion_batches(connection, limit=batches_limit)
        summary = _build_schema_governance_summary(proposals=proposals, batches=batches)
        return {
            "success": True,
            "result": summary,
        }
    except Exception as exc:
        LOGGER.exception("Failed to build schema governance summary")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/{proposal_id}/status")
def set_schema_proposal_status(proposal_id: int, payload: SolfAttributeProposalStatusRequest) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        updated = domain_db.update_solf_attribute_proposal_status(
            connection,
            proposal_id=proposal_id,
            status=payload.status,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
        if updated is None:
            raise HTTPException(status_code=404, detail=f"schema proposal {proposal_id} not found")
        return {"success": True, "result": updated}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to update schema proposal status")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/solf-patch-draft")
def get_solf_patch_draft(class_name: str | None = None) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        approved_extensions = domain_db.get_approved_solf_attribute_extensions(connection)
        script_text = _solf_script_path().read_text(encoding="utf-8")
        class_defs = parse_solf_classes(script_text)
        result = _build_patch_draft_result(approved_extensions, class_defs, class_name=class_name)

        return {
            "success": True,
            "count": len(result.get("classes") or []),
            "result": result,
        }
    except Exception as exc:
        LOGGER.exception("Failed to build SOLF patch draft")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/promotion-batches")
def list_schema_promotion_batches(limit: int = 100) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        batches = domain_db.list_solf_schema_promotion_batches(connection, limit=limit)
        return {"success": True, "count": len(batches), "result": batches}
    except Exception as exc:
        LOGGER.exception("Failed to list schema promotion batches")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/promotion-batches")
def create_schema_promotion_batch(payload: SolfSchemaPromotionBatchCreateRequest) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        proposals = domain_db.list_solf_attribute_proposals(
            connection,
            status="approved",
            class_name=payload.class_name or None,
            limit=1000,
        )

        requested_ids = {int(item) for item in (payload.proposal_ids or []) if int(item) > 0}
        if requested_ids:
            proposals = [item for item in proposals if int(item.get("proposal_id") or 0) in requested_ids]

        approved_extensions: dict[str, list[str]] = {}
        proposal_ids: list[int] = []
        for proposal in proposals:
            proposal_id = int(proposal.get("proposal_id") or 0)
            current_class_name = str(proposal.get("class_name") or "").strip().lower()
            attribute_name = str(proposal.get("attribute_name") or "").strip().lower()
            if proposal_id <= 0 or not current_class_name or not attribute_name:
                continue
            proposal_ids.append(proposal_id)
            approved_extensions.setdefault(current_class_name, []).append(attribute_name)

        script_text = _solf_script_path().read_text(encoding="utf-8")
        class_defs = parse_solf_classes(script_text)
        patch_result = _build_patch_draft_result(approved_extensions, class_defs, class_name=payload.class_name or None)
        patch_preview = str(patch_result.get("solf_patch_preview") or "")

        batch = domain_db.create_solf_schema_promotion_batch(
            connection,
            batch_name=payload.batch_name or "solf_schema_promotion_batch",
            proposal_ids=proposal_ids,
            patch_preview=patch_preview,
            class_name=payload.class_name or None,
            created_by=payload.created_by,
            note=payload.note,
        )
        return {
            "success": True,
            "result": {
                **batch,
                "classes": patch_result.get("classes") or [],
            },
        }
    except Exception as exc:
        LOGGER.exception("Failed to create schema promotion batch")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/promotion-batches/{batch_id}/status")
def set_schema_promotion_batch_status(batch_id: int, payload: SolfSchemaPromotionBatchStatusRequest) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        updated = domain_db.update_solf_schema_promotion_batch_status(
            connection,
            batch_id=batch_id,
            status=payload.status,
            reviewed_by=payload.reviewed_by,
            note=payload.note,
        )
        if updated is None:
            raise HTTPException(status_code=404, detail=f"schema promotion batch {batch_id} not found")
        return {"success": True, "result": updated}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to update schema promotion batch status")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/promotion-batches/{batch_id}/export")
def export_schema_promotion_batch(batch_id: int) -> PlainTextResponse:
    connection = object_db.get_connection()
    try:
        batch = domain_db.get_solf_schema_promotion_batch(connection, batch_id=batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail=f"schema promotion batch {batch_id} not found")

        batch_name = str(batch.get("batch_name") or f"solf-batch-{batch_id}").strip() or f"solf-batch-{batch_id}"
        safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in batch_name).strip("-") or f"solf-batch-{batch_id}"
        content = str(batch.get("patch_preview") or "").strip()
        headers = {
            "Content-Disposition": f'attachment; filename="{safe_name}.solf"',
        }
        return PlainTextResponse(content=content, headers=headers, media_type="text/plain; charset=utf-8")
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to export schema promotion batch")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/promotion-batches/{batch_id}/audit-link")
def link_schema_promotion_batch_audit(batch_id: int, payload: SolfSchemaPromotionBatchAuditLinkRequest) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        solf_path = Path(payload.solf_script_path).expanduser() if str(payload.solf_script_path or "").strip() else _solf_script_path()
        solf_hash = str(payload.solf_script_hash or "").strip()
        if not solf_hash:
            solf_hash = _sha256_text(solf_path.read_text(encoding="utf-8"))

        audit_metadata = {
            "audit_linked_by": str(payload.linked_by or "api:user").strip() or "api:user",
            "audit_note": str(payload.note or "").strip(),
            "solf_script_path": str(solf_path),
            "solf_script_hash": solf_hash,
            "exported_artifact_path": str(payload.exported_artifact_path or "").strip(),
        }

        updated = domain_db.update_solf_schema_promotion_batch_audit_link(
            connection,
            batch_id=batch_id,
            audit_metadata=audit_metadata,
        )
        if updated is None:
            raise HTTPException(status_code=404, detail=f"schema promotion batch {batch_id} not found")
        return {"success": True, "result": updated}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to link schema promotion batch audit metadata")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()