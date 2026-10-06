from __future__ import annotations

import json
from dataclasses import replace
from functools import lru_cache

from fastapi import Depends, HTTPException, Request

from interaction import IDMSInteractionTools
import object_db
from security_context import SecurityContext, get_security_context as _get_security_context


@lru_cache(maxsize=1)
def get_tools() -> IDMSInteractionTools:
    return IDMSInteractionTools()


def get_security_context(request: Request) -> SecurityContext:
    context = _get_security_context(required=False)
    if context is None:
        raise HTTPException(status_code=401, detail="Tenant authentication is required")
    if context.external_permissions:
        permissions = context.permissions
    else:
        try:
            permissions = object_db.get_permissions_for_subject(context.tenant_id, context.user_id)
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Unable to resolve tenant permissions") from exc
    if not permissions:
        raise HTTPException(status_code=403, detail="User is not an active member of this tenant")
    return replace(context, permissions=permissions)


def require_permission(permission: str):
    def check(context: SecurityContext = Depends(get_security_context)) -> SecurityContext:
        if not context.allows(permission):
            raise HTTPException(status_code=403, detail=f"Missing permission: {permission}")
        return context
    return check


def require_platform_admin() -> SecurityContext:
    """Require an identity allowlisted in deployment configuration."""
    context = _get_security_context(required=False)
    if context is None or not context.platform_admin:
        raise HTTPException(status_code=403, detail="Platform administrator access is required")
    return context


def parse_json_dict(raw: str, field_name: str) -> dict:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail=f"{field_name} must decode to a JSON object")
    return parsed


def normalize_tags(raw_tags: str) -> list[str]:
    return [part.strip() for part in str(raw_tags or "").split(",") if part.strip()]
