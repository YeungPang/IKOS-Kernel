from __future__ import annotations

import logging
import hmac
import os
from pathlib import Path
import uuid
from integration_auth import IntegrationAssertionError, validate_bearer_assertion
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from ikos_config import PLATFORM_ADMIN_SUBJECTS
from security_context import SecurityContext, reset_security_context, set_security_context

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


def _bootstrap_env() -> None:
    """Load project .env once for API entrypoints.

    This keeps environment resolution stable even when modules that import
    ikos_config are not imported first.
    """
    if load_dotenv is None:
        return
    base_dir = Path(__file__).resolve().parents[2]
    load_dotenv(base_dir / ".env")


_bootstrap_env()

from ikos_api_server.routers.ingestion import router as ingestion_router
from ikos_api_server.routers.interaction import router as interaction_router
from ikos_api_server.routers.system import router as system_router
from ikos_api_server.routers.notes import router as notes_router
from ikos_api_server.routers.business_rules import router as business_rules_router
from ikos_api_server.routers.documents import router as documents_router
from ikos_api_server.routers.schema_proposals import router as schema_proposals_router
from ikos_api_server.routers.accounting import router as accounting_router
from ikos_api_server.routers.rule_ingestion import router as rule_ingestion_router
from ikos_api_server.routers.domain_definitions import router as domain_definitions_router
from ikos_api_server.routers.pipeline_runs import router as pipeline_runs_router
from ikos_api_server.routers.matching import router as matching_router
from ikos_api_server.routers.nl_gateway import router as nl_gateway_router
from ikos_api_server.routers.source_databases import router as source_databases_router
from ikos_api_server.routers.security import router as security_router
from ikos_api_server.routers.platform_admin import router as platform_admin_router
from ikos_api_server.routers.extension_runtime import router as extension_runtime_router

logger = logging.getLogger(__name__)


app = FastAPI(
    title="IDMS API",
    version="1.0.0",
    description="Unified API for IDMS ingestion, chat, actions, and grounded queries.",
)


@app.middleware("http")
async def tenant_security_context(request: Request, call_next):
    """Accept verified identities only from the configured trusted auth proxy."""
    if request.url.path in {"/docs", "/openapi.json", "/redoc"}:
        return await call_next(request)
    expected_secret = str(os.getenv("IKOS_TRUSTED_AUTH_PROXY_SECRET") or "")
    supplied_secret = str(request.headers.get("X-IKOS-Auth-Secret") or "")
    if not expected_secret or not hmac.compare_digest(expected_secret, supplied_secret):
        return JSONResponse(status_code=401, content={"success": False, "detail": "Trusted authentication is required"})

    request_id = str(request.headers.get("X-IKOS-Request-ID") or uuid.uuid4()).strip()
    authorization = str(request.headers.get("Authorization") or "").strip()
    signed_context: dict[str, object] | None = None
    if authorization.lower().startswith("bearer "):
        try:
            signed_context = validate_bearer_assertion(authorization[7:].strip())
        except IntegrationAssertionError as exc:
            return JSONResponse(status_code=401, content={"success": False, "detail": str(exc)})

    tenant_id = str((signed_context or {}).get("tenant_id") or request.headers.get("X-IKOS-Tenant-ID") or "").strip()
    user_id = str((signed_context or {}).get("subject") or request.headers.get("X-IKOS-User-ID") or "").strip()
    if not user_id:
        return JSONResponse(status_code=401, content={"success": False, "detail": "Tenant authentication is required"})

    # Platform administrators have no implicit tenant context and can only use
    # the isolated control-plane provisioning routes.
    if request.url.path.startswith("/api/platform/"):
        if user_id not in PLATFORM_ADMIN_SUBJECTS:
            return JSONResponse(status_code=403, content={"success": False, "detail": "Platform administrator access is required"})
        token = set_security_context(
            SecurityContext(
                tenant_id="",
                user_id=user_id,
                permissions=frozenset({"platform.configuration.publish"}),
                request_id=request_id,
                authorization_source="platform_allowlist",
                platform_admin=True,
            )
        )
        try:
            response = await call_next(request)
            from object_db import record_security_event

            record_security_event(
                tenant_id=None,
                subject=user_id,
                request_id=request_id,
                endpoint=request.url.path,
                outcome="allowed" if response.status_code < 400 else "denied",
                authorization_source="platform_allowlist",
                status_code=response.status_code,
            )
            return response
        finally:
            reset_security_context(token)

    if not tenant_id:
        return JSONResponse(status_code=401, content={"success": False, "detail": "Tenant authentication is required"})
    try:
        tenant_id = str(uuid.UUID(tenant_id))
    except ValueError:
        return JSONResponse(status_code=401, content={"success": False, "detail": "Invalid tenant identity"})

    if signed_context is not None:
        from object_db import is_active_tenant

        try:
            if not is_active_tenant(tenant_id):
                return JSONResponse(status_code=403, content={"success": False, "detail": "Tenant is not active"})
        except Exception:
            return JSONResponse(status_code=503, content={"success": False, "detail": "Unable to validate tenant"})

    token = set_security_context(
        SecurityContext(
            tenant_id=tenant_id,
            user_id=user_id,
            permissions=(signed_context or {}).get("permissions", frozenset()),
            roles=(signed_context or {}).get("roles", frozenset()),
            request_id=str((signed_context or {}).get("request_id") or request_id),
            authorization_source="external_assertion" if signed_context else "ikos_membership",
            external_permissions=bool(signed_context),
        )
    )
    try:
        from object_db import get_permissions_for_subject

        permissions = (
            signed_context["permissions"]
            if signed_context is not None
            else get_permissions_for_subject(tenant_id, user_id)
        )
        if not permissions:
            return JSONResponse(
                status_code=403,
                content={"success": False, "detail": "User is not an active member of this tenant"},
            )
        reset_security_context(token)
        token = set_security_context(
            SecurityContext(
                tenant_id=tenant_id,
                user_id=user_id,
                permissions=permissions,
                roles=(signed_context or {}).get("roles", frozenset()),
                request_id=str((signed_context or {}).get("request_id") or request_id),
                authorization_source="external_assertion" if signed_context else "ikos_membership",
                external_permissions=bool(signed_context),
            )
        )
        response = await call_next(request)
        from object_db import record_security_event

        record_security_event(
            tenant_id=tenant_id,
            subject=user_id,
            request_id=request_id,
            endpoint=request.url.path,
            outcome="allowed" if response.status_code < 400 else "denied",
            authorization_source="external_assertion" if signed_context else "ikos_membership",
            status_code=response.status_code,
        )
        return response
    finally:
        reset_security_context(token)

app.include_router(system_router)
app.include_router(ingestion_router)
app.include_router(interaction_router)
app.include_router(notes_router)
app.include_router(business_rules_router)
app.include_router(documents_router)
app.include_router(schema_proposals_router)
app.include_router(accounting_router)
app.include_router(rule_ingestion_router)
app.include_router(domain_definitions_router)
app.include_router(pipeline_runs_router)
app.include_router(matching_router)
app.include_router(nl_gateway_router)
app.include_router(source_databases_router)
app.include_router(security_router)
app.include_router(platform_admin_router)
app.include_router(extension_runtime_router)


@app.on_event("startup")
async def startup_event():
    """Setup credentials and other initialization on API startup."""
    try:
        from credentials_path_resolver import setup_google_credentials, get_credentials_path_info
        
        creds_path = setup_google_credentials()
        logger.info(f"Google credentials configured: {creds_path}")
        
        # Log configuration info for debugging
        info = get_credentials_path_info()
        logger.debug(f"Credentials configuration: source={info['priority_source']}, exists={info['file_exists']}")
    except Exception as e:
        logger.warning(f"Could not setup credentials: {e}")
        # Don't fail startup, some endpoints may not need credentials


@app.exception_handler(HTTPException)
async def http_exception_handler(_request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"success": False, "detail": exc.detail})
