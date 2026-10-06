"""Request-scoped tenant and authorization context for IKOS services."""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import FrozenSet


@dataclass(frozen=True)
class SecurityContext:
    tenant_id: str
    user_id: str
    permissions: FrozenSet[str] = frozenset()
    workspace_ids: FrozenSet[str] = frozenset()
    platform_admin: bool = False
    roles: FrozenSet[str] = frozenset()
    request_id: str = ""
    authorization_source: str = "ikos_membership"
    external_permissions: bool = False

    def allows(self, permission: str) -> bool:
        return "*" in self.permissions or permission in self.permissions


def infer_document_access_level(doc_cat: str | None, doc_type: str | None) -> str:
    """Classify common financial document kinds for default-deny finance access."""
    finance_markers = {
        "accounting", "accounting_transaction", "bank_statement", "balance_sheet",
        "financial", "finance", "invoice", "ledger", "payroll", "receipt",
        "tax", "vat", "profit_and_loss", "payment", "expense",
    }
    tokens = {
        str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
        for value in (doc_cat, doc_type)
    }
    return "finance" if any(marker in token for marker in finance_markers for token in tokens) else "tenant"


_SECURITY_CONTEXT: ContextVar[SecurityContext | None] = ContextVar(
    "ikos_security_context",
    default=None,
)

LEGACY_TENANT_ID = "00000000-0000-0000-0000-000000000001"


def set_security_context(context: SecurityContext) -> Token[SecurityContext | None]:
    return _SECURITY_CONTEXT.set(context)


def reset_security_context(token: Token[SecurityContext | None]) -> None:
    _SECURITY_CONTEXT.reset(token)


def get_security_context(required: bool = True) -> SecurityContext | None:
    context = _SECURITY_CONTEXT.get()
    if required and context is None:
        raise RuntimeError("No IKOS security context is active")
    return context


def get_tenant_id() -> str:
    """Return the active tenant, or the explicit legacy tenant for maintenance jobs."""
    context = get_security_context(required=False)
    if context is None:
        return LEGACY_TENANT_ID
    return str(context.tenant_id or "")