"""Validation of signed external-application authorization assertions."""

from __future__ import annotations

import base64
import json
import time
import uuid
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from ikos_config import (
    INTEGRATION_JWT_AUDIENCE,
    INTEGRATION_JWT_CLOCK_SKEW_SECONDS,
    INTEGRATION_JWT_ISSUER,
    INTEGRATION_JWT_PUBLIC_KEY_PATH,
    INTEGRATION_JWT_PUBLIC_KEY_PEM,
)


class IntegrationAssertionError(ValueError):
    """Raised when an external authorization assertion is invalid."""


def _decode_segment(segment: str) -> bytes:
    try:
        return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
    except Exception as exc:
        raise IntegrationAssertionError("Malformed signed assertion") from exc


def _load_public_key() -> Any:
    pem = INTEGRATION_JWT_PUBLIC_KEY_PEM
    if not pem and INTEGRATION_JWT_PUBLIC_KEY_PATH:
        pem = Path(INTEGRATION_JWT_PUBLIC_KEY_PATH).read_text(encoding="utf-8")
    if not pem:
        raise IntegrationAssertionError("Signed integration assertions are not configured")
    try:
        return serialization.load_pem_public_key(pem.encode("utf-8"))
    except Exception as exc:
        raise IntegrationAssertionError("Invalid integration public key") from exc


def validate_bearer_assertion(token: str) -> dict[str, Any]:
    parts = str(token or "").split(".")
    if len(parts) != 3:
        raise IntegrationAssertionError("Malformed bearer assertion")

    header_raw, payload_raw, signature_raw = parts
    try:
        header = json.loads(_decode_segment(header_raw))
        claims = json.loads(_decode_segment(payload_raw))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrationAssertionError("Malformed bearer assertion JSON") from exc
    if not isinstance(header, dict) or not isinstance(claims, dict):
        raise IntegrationAssertionError("Malformed bearer assertion payload")
    if header.get("alg") != "RS256" or header.get("typ", "JWT") != "JWT":
        raise IntegrationAssertionError("Unsupported bearer assertion algorithm")

    try:
        _load_public_key().verify(
            _decode_segment(signature_raw),
            f"{header_raw}.{payload_raw}".encode("ascii"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
    except IntegrationAssertionError:
        raise
    except (InvalidSignature, ValueError, TypeError) as exc:
        raise IntegrationAssertionError("Invalid bearer assertion signature") from exc

    now = int(time.time())
    skew = max(0, int(INTEGRATION_JWT_CLOCK_SKEW_SECONDS))
    issuer = str(claims.get("iss") or "")
    if INTEGRATION_JWT_ISSUER and issuer != INTEGRATION_JWT_ISSUER:
        raise IntegrationAssertionError("Invalid bearer assertion issuer")
    audience = claims.get("aud")
    audiences = audience if isinstance(audience, list) else [audience]
    if INTEGRATION_JWT_AUDIENCE and INTEGRATION_JWT_AUDIENCE not in audiences:
        raise IntegrationAssertionError("Invalid bearer assertion audience")
    if not claims.get("sub"):
        raise IntegrationAssertionError("Bearer assertion subject is required")
    try:
        issued_at = int(claims["iat"])
        expires_at = int(claims["exp"])
    except (KeyError, TypeError, ValueError) as exc:
        raise IntegrationAssertionError("Bearer assertion iat and exp are required") from exc
    if issued_at > now + skew or expires_at < now - skew or expires_at <= issued_at:
        raise IntegrationAssertionError("Bearer assertion is expired or not yet valid")

    tenant_id = str(claims.get("tenant_id") or "").strip()
    if not tenant_id:
        raise IntegrationAssertionError("Bearer assertion tenant_id is required")
    try:
        tenant_id = str(uuid.UUID(tenant_id))
    except ValueError as exc:
        raise IntegrationAssertionError("Bearer assertion tenant_id is invalid") from exc

    permissions = claims.get("permissions")
    roles = claims.get("roles")
    if not isinstance(permissions, list) or not all(isinstance(item, str) for item in permissions):
        raise IntegrationAssertionError("Bearer assertion permissions must be a string array")
    if roles is not None and (not isinstance(roles, list) or not all(isinstance(item, str) for item in roles)):
        raise IntegrationAssertionError("Bearer assertion roles must be a string array")

    return {
        "subject": str(claims["sub"]),
        "tenant_id": tenant_id,
        "permissions": frozenset(str(item).strip() for item in permissions if str(item).strip()),
        "roles": frozenset(str(item).strip() for item in (roles or []) if str(item).strip()),
        "request_id": str(claims.get("request_id") or claims.get("jti") or "").strip(),
        "issuer": issuer,
    }
