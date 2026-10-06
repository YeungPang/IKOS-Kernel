from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

import object_db
from ikos_api_server.deps import require_permission
from security_context import SecurityContext

router = APIRouter(prefix="/api/security", tags=["security"])


class TenantMemberUpsertRequest(BaseModel):
    external_subject: str = Field(..., min_length=1, max_length=256)
    display_name: str | None = Field(default=None, max_length=256)
    role_keys: list[str] = Field(..., min_length=1)


@router.post("/members", summary="Add or update a tenant member's roles")
def upsert_tenant_member(
    request: TenantMemberUpsertRequest,
    context: SecurityContext = Depends(require_permission("tenant.members.manage")),
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        membership_id = object_db.upsert_tenant_membership(
            connection,
            tenant_id=context.tenant_id,
            external_subject=request.external_subject,
            display_name=request.display_name,
            role_keys=request.role_keys,
        )
        connection.commit()
        return {
            "success": True,
            "membership_id": membership_id,
            "tenant_id": context.tenant_id,
            "external_subject": request.external_subject,
            "role_keys": sorted({str(role).strip().lower() for role in request.role_keys}),
        }
    except PermissionError as exc:
        connection.rollback()
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        connection.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Unable to update tenant membership") from exc
    finally:
        connection.close()
