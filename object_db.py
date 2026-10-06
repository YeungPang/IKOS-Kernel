import logging
import json
import re
import hashlib
import tempfile
import zipfile
import uuid
from datetime import date, datetime
from collections import defaultdict
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Callable

import psycopg2
from psycopg2.extras import Json
from ikos_config import DB_NAME, DB_HOST, DB_USER, DB_PASSWORD, DB_PORT
from security_context import LEGACY_TENANT_ID, get_security_context, get_tenant_id


def get_connection() -> psycopg2.extensions.connection:
    connection = psycopg2.connect(
        database=DB_NAME,
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )
    context = get_security_context(required=False)
    tenant_id = context.tenant_id if context is not None else "00000000-0000-0000-0000-000000000001"
    user_id = context.user_id if context is not None else ""
    permissions = ",".join(sorted(context.permissions)) if context is not None else ""
    with connection.cursor() as cursor:
        cursor.execute("SELECT set_config('ikos.tenant_id', %s, false)", (tenant_id,))
        cursor.execute("SELECT set_config('ikos.user_id', %s, false)", (user_id,))
        cursor.execute("SELECT set_config('ikos.permissions', %s, false)", (permissions,))
    return connection


def record_security_event(
    *,
    tenant_id: str | None,
    subject: str,
    request_id: str,
    endpoint: str,
    outcome: str,
    authorization_source: str,
    status_code: int,
) -> None:
    """Persist a security decision without storing credentials or assertions."""
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS security_audit_event (
                    event_id BIGSERIAL PRIMARY KEY,
                    tenant_id UUID,
                    subject VARCHAR(512) NOT NULL,
                    request_id VARCHAR(256) NOT NULL,
                    endpoint VARCHAR(512) NOT NULL,
                    outcome VARCHAR(32) NOT NULL,
                    authorization_source VARCHAR(64) NOT NULL,
                    status_code INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_security_audit_event_tenant ON security_audit_event(tenant_id);
                CREATE INDEX IF NOT EXISTS idx_security_audit_event_subject ON security_audit_event(subject);
                CREATE INDEX IF NOT EXISTS idx_security_audit_event_created ON security_audit_event(created_at DESC);
                """
            )
            cursor.execute(
                """
                INSERT INTO security_audit_event (
                    tenant_id, subject, request_id, endpoint, outcome,
                    authorization_source, status_code
                ) VALUES (NULLIF(%s, '')::uuid, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(tenant_id or ""),
                    str(subject or "")[:512],
                    str(request_id or "")[:256],
                    str(endpoint or "")[:512],
                    str(outcome or "unknown")[:32],
                    str(authorization_source or "unknown")[:64],
                    int(status_code),
                ),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        logging.getLogger(__name__).exception("Unable to persist security audit event")
    finally:
        connection.close()


def _canonical_person_full_name(value: Any) -> str:
    """Normalize a person name into a canonical full-name form."""
    text = str(value or "").strip()
    if not text:
        return ""

    text = re.sub(
        r"^(?:dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|herr|frau)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()

    if "," in text:
        last_name, first_names = [part.strip() for part in text.split(",", 1)]
        if last_name and first_names:
            text = f"{first_names} {last_name}"

    text = re.sub(r"[.,;:_\-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_semantic_term_for_compare(value: Any) -> str:
    """Normalize semantic alias text to a stable comparison key."""
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = re.sub(r"[\-/]+", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.replace(" ", "_")


def _load_seeded_business_terms_from_file() -> list[dict[str, Any]]:
    """Return the canonical seeded registry used by schema sync and query-time matching."""
    seed_path = Path(__file__).resolve().parent / "seeded_business_terms.json"
    try:
        payload = json.loads(seed_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []

    rows: list[dict[str, Any]] = []
    for canonical_name, aliases in payload.items():
        if not isinstance(aliases, list):
            continue
        for alias in aliases:
            alias_text = str(alias or "").strip()
            if not alias_text:
                continue
            rows.append(
                {
                    "kind": "category",
                    "canonical_name": str(canonical_name).strip(),
                    "term_text": alias_text,
                    "language": "en",
                    "source_type": "seeded",
                    "metadata": {"source": "seed_file"},
                }
            )
    return rows


_RELATIONSHIP_CANONICAL_MAP = {
    "shareholder": "shareholder_of",
    "shareholders": "shareholder_of",
    "shareholder_of": "shareholder_of",
    "stockholder": "shareholder_of",
    "stockholders": "shareholder_of",
    "stockholder_of": "shareholder_of",
    "president": "president_of",
    "presidents": "president_of",
    "president_of": "president_of",
}


def canonicalize_relationship_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "related_to"

    text = text.replace("&", " and ")
    text = re.sub(r"[\s\-]+", "_", text)
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        return "related_to"

    return _RELATIONSHIP_CANONICAL_MAP.get(text, text)


def expand_relationship_names(value: Any) -> list[str]:
    text = str(value or "").strip().lower()
    if not text:
        return ["related_to"]

    normalized = text.replace("&", " and ")
    normalized = re.sub(r"[\s\-]+", "_", normalized)
    normalized = re.sub(r"[^a-z0-9_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        return ["related_to"]

    parts = [normalized]
    if "_and_" in normalized:
        split_parts = [part.strip("_") for part in normalized.split("_and_") if part.strip("_")]
        if split_parts:
            parts = split_parts

    expanded: list[str] = []
    for part in parts:
        canonical = canonicalize_relationship_name(part)
        if canonical and canonical not in expanded:
            expanded.append(canonical)

    return expanded or ["related_to"]


def _relationship_semantic_aliases(relationship_name: str) -> list[str]:
    canonical = canonicalize_relationship_name(relationship_name)
    aliases = {canonical}

    for alias, mapped in _RELATIONSHIP_CANONICAL_MAP.items():
        if mapped == canonical:
            aliases.add(alias)

    if canonical.endswith("_of"):
        base = canonical[:-3]
        aliases.add(base)
        aliases.add(f"{base}s")

    return sorted(alias for alias in aliases if alias)


def _find_semantic_equivalent_relationship(
    connection: psycopg2.extensions.connection,
    relationship_name: str,
    relationship_cat: str,
    src_object_id: int,
    tar_object_id: int,
) -> dict[str, Any] | None:
    aliases = _relationship_semantic_aliases(relationship_name)
    if not aliases:
        return None

    sql = """
    SELECT relationship_id, relationship_name, relationship_cat, src_object_id, tar_object_id, confidence, metadata, valid_from, valid_until, entry_date
    FROM object_relationship
    WHERE src_object_id = %s
      AND tar_object_id = %s
      AND relationship_cat = %s
      AND LOWER(relationship_name) = ANY(%s)
    ORDER BY CASE WHEN LOWER(relationship_name) = %s THEN 0 ELSE 1 END, relationship_id
    LIMIT 1
    """

    canonical = canonicalize_relationship_name(relationship_name)
    with connection.cursor() as cursor:
        cursor.execute(sql, (src_object_id, tar_object_id, relationship_cat, aliases, canonical))
        row = cursor.fetchone()

    if not row:
        return None

    return {
        "relationship_id": row[0],
        "relationship_name": row[1],
        "relationship_cat": row[2],
        "src_object_id": row[3],
        "tar_object_id": row[4],
        "confidence": row[5],
        "metadata": row[6] or {},
        "valid_from": row[7],
        "valid_until": row[8],
        "entry_date": row[9],
    }


def _table_exists(connection: psycopg2.extensions.connection, table_name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        return cursor.fetchone()[0] is not None


def _ensure_object_alias_table(connection: psycopg2.extensions.connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(create_object_alias_table)


def get_permissions_for_subject(tenant_id: str, external_subject: str) -> frozenset[str]:
    """Resolve active permissions for a tenant membership from assigned RBAC roles."""
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT rp.permission_key
                FROM tenant_membership tm
                JOIN ikos_user u ON u.user_id = tm.user_id
                JOIN membership_role mr ON mr.membership_id = tm.membership_id
                JOIN role_permission rp ON rp.role_id = mr.role_id
                WHERE tm.tenant_id = %s::uuid
                  AND u.external_subject = %s
                  AND tm.status = 'active'
                  AND u.status = 'active'
                """,
                (str(tenant_id), str(external_subject)),
            )
            return frozenset(str(row[0]) for row in cursor.fetchall())
    finally:
        connection.close()


def is_active_tenant(tenant_id: str) -> bool:
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM tenant WHERE tenant_id = %s::uuid AND status = 'active'",
                (str(tenant_id),),
            )
            return cursor.fetchone() is not None
    finally:
        connection.close()


def create_tenant_with_initial_admin(
    connection: psycopg2.extensions.connection,
    *,
    tenant_key: str,
    display_name: str,
    admin_external_subject: str,
    admin_display_name: str | None = None,
) -> dict[str, str | int]:
    """Create a tenant and its first tenant administrator in the caller's transaction."""
    normalized_key = str(tenant_key or "").strip().lower()
    normalized_name = str(display_name or "").strip()
    normalized_subject = str(admin_external_subject or "").strip()
    if not normalized_key or not normalized_name or not normalized_subject:
        raise ValueError("tenant_key, display_name, and admin_external_subject are required")
    if re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,126}[a-z0-9])?", normalized_key) is None:
        raise ValueError("tenant_key must be a lowercase slug containing letters, digits, and internal hyphens")

    tenant_id = uuid.uuid4()
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO tenant (tenant_id, tenant_key, display_name) VALUES (%s, %s, %s)",
            (tenant_id, normalized_key, normalized_name),
        )
        cursor.execute(
            """
            INSERT INTO ikos_user (user_id, external_subject, display_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (external_subject)
            DO UPDATE SET display_name = COALESCE(EXCLUDED.display_name, ikos_user.display_name), status = 'active'
            RETURNING user_id
            """,
            (
                uuid.uuid4(),
                normalized_subject,
                str(admin_display_name or "").strip() or None,
            ),
        )
        user_id = cursor.fetchone()[0]
        cursor.execute(
            """
            INSERT INTO tenant_membership (tenant_id, user_id, status)
            VALUES (%s, %s, 'active')
            RETURNING membership_id
            """,
            (tenant_id, user_id),
        )
        membership_id = int(cursor.fetchone()[0])
        cursor.execute(
            """
            INSERT INTO membership_role (membership_id, role_id)
            SELECT %s, role_id FROM role WHERE role_key = 'tenant_admin'
            RETURNING role_id
            """,
            (membership_id,),
        )
        if cursor.fetchone() is None:
            raise RuntimeError("tenant_admin role is not seeded; initialize the IKOS schema first")

    return {
        "tenant_id": str(tenant_id),
        "tenant_key": normalized_key,
        "user_id": str(user_id),
        "membership_id": membership_id,
        "role_key": "tenant_admin",
    }


def upsert_tenant_membership(
    connection: psycopg2.extensions.connection,
    *,
    tenant_id: str,
    external_subject: str,
    display_name: str | None,
    role_keys: list[str],
) -> int:
    """Provision or update one user membership and replace its tenant-scoped roles."""
    context = get_security_context()
    assert context is not None
    if str(context.tenant_id) != str(tenant_id):
        raise PermissionError("Cannot manage memberships outside the active tenant")
    normalized_roles = sorted({str(role).strip().lower() for role in role_keys if str(role).strip()})
    if not normalized_roles:
        raise ValueError("At least one role is required")
    with connection.cursor() as cursor:
        cursor.execute("SELECT role_key FROM role WHERE role_key = ANY(%s)", (normalized_roles,))
        known_roles = {str(row[0]) for row in cursor.fetchall()}
        missing = sorted(set(normalized_roles) - known_roles)
        if missing:
            raise ValueError(f"Unknown role(s): {', '.join(missing)}")
        cursor.execute(
            """
            INSERT INTO ikos_user (user_id, external_subject, display_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (external_subject)
            DO UPDATE SET display_name = COALESCE(EXCLUDED.display_name, ikos_user.display_name), status = 'active'
            RETURNING user_id
            """,
            (str(uuid.uuid4()), str(external_subject).strip(), str(display_name or "").strip() or None),
        )
        user_id = cursor.fetchone()[0]
        cursor.execute(
            """
            INSERT INTO tenant_membership (tenant_id, user_id, status)
            VALUES (%s::uuid, %s, 'active')
            ON CONFLICT (tenant_id, user_id) DO UPDATE SET status = 'active'
            RETURNING membership_id
            """,
            (str(tenant_id), user_id),
        )
        membership_id = int(cursor.fetchone()[0])
        cursor.execute("DELETE FROM membership_role WHERE membership_id = %s", (membership_id,))
        cursor.execute(
            """
            INSERT INTO membership_role (membership_id, role_id)
            SELECT %s, role_id FROM role WHERE role_key = ANY(%s)
            ON CONFLICT DO NOTHING
            """,
            (membership_id, normalized_roles),
        )
    return membership_id


def save_document_content(
    connection: psycopg2.extensions.connection,
    *,
    doc_id: int,
    content_type: str,
    content_text: str | None = None,
    content_bytes: bytes | None = None,
    object_uri: str | None = None,
    source_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    """Persist a new content version linked to a document; never commits the caller's transaction."""
    normalized_type = str(content_type or "").strip().lower()
    if not normalized_type:
        raise ValueError("content_type is required")
    if content_text is None and content_bytes is None and not object_uri:
        raise ValueError("content_text, content_bytes, or object_uri is required")

    if content_text is not None:
        checksum = hashlib.sha256(content_text.encode("utf-8")).hexdigest()
    elif content_bytes is not None:
        checksum = hashlib.sha256(content_bytes).hexdigest()
    else:
        checksum = hashlib.sha256(str(object_uri).encode("utf-8")).hexdigest()

    context = get_security_context(required=False)
    requested_tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
    with connection.cursor() as cursor:
        cursor.execute("SELECT tenant_id FROM document WHERE doc_id = %s AND tenant_id = %s::uuid FOR UPDATE", (int(doc_id), requested_tenant_id))
        document_row = cursor.fetchone()
        if document_row is None:
            raise LookupError(f"Document {doc_id} was not found")
        tenant_id = str(document_row[0])
        if tenant_id != requested_tenant_id:
            raise PermissionError("Document does not belong to the active tenant")

        cursor.execute(
            """
            SELECT content_id, content_version, checksum_sha256
            FROM document_content
            WHERE tenant_id = %s AND doc_id = %s AND content_type = %s AND status = 'active'
            ORDER BY content_version DESC
            LIMIT 1
            """,
            (tenant_id, int(doc_id), normalized_type),
        )
        current = cursor.fetchone()
        if current and current[2] == checksum:
            return int(current[0])
        cursor.execute(
            """
            SELECT COALESCE(MAX(content_version), 0) + 1
            FROM document_content
            WHERE tenant_id = %s AND doc_id = %s AND content_type = %s
            """,
            (tenant_id, int(doc_id), normalized_type),
        )
        next_version = int(cursor.fetchone()[0])
        cursor.execute(
            """
            UPDATE document_content
            SET status = 'superseded'
            WHERE tenant_id = %s AND doc_id = %s AND content_type = %s AND status = 'active'
            """,
            (tenant_id, int(doc_id), normalized_type),
        )
        cursor.execute(
            """
            INSERT INTO document_content (
                tenant_id, doc_id, content_type, content_version, content_text,
                content_bytes, object_uri, checksum_sha256, source_name, metadata, created_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING content_id
            """,
            (
                tenant_id,
                int(doc_id),
                normalized_type,
                next_version,
                content_text,
                psycopg2.Binary(content_bytes) if content_bytes is not None else None,
                object_uri,
                checksum,
                source_name,
                Json(metadata or {}),
                context.user_id if context is not None else None,
            ),
        )
        row = cursor.fetchone()
        return int(row[0])


def get_document_content(
    connection: psycopg2.extensions.connection,
    *,
    doc_id: int,
    content_type: str = "markdown",
) -> dict[str, Any] | None:
    """Return the active latest tenant-matched content version for a document."""
    context = get_security_context(required=False)
    requested_tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.content_id, c.content_version, c.content_text, c.content_bytes,
                   c.object_uri, c.checksum_sha256, c.source_name, c.metadata, c.created_at
            FROM document_content c
            JOIN document d ON d.doc_id = c.doc_id AND d.tenant_id = c.tenant_id
            WHERE c.doc_id = %s AND c.content_type = %s AND c.status = 'active'
                              AND c.tenant_id = %s::uuid
            ORDER BY c.content_version DESC
            LIMIT 1
            """,
            (
                int(doc_id),
                str(content_type or "markdown").strip().lower(),
                requested_tenant_id,
            ),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return {
        "content_id": int(row[0]),
        "content_version": int(row[1]),
        "content_text": row[2],
        "content_bytes": bytes(row[3]) if row[3] is not None else None,
        "object_uri": row[4],
        "checksum_sha256": row[5],
        "source_name": row[6],
        "metadata": row[7] or {},
        "created_at": row[8],
    }


create_class_table = """
CREATE TABLE IF NOT EXISTS object_class (
    class_id            BIGSERIAL PRIMARY KEY,
    class_name          VARCHAR(256) NOT NULL UNIQUE,
    parent_class_id     BIGINT REFERENCES object_class(class_id) ON DELETE SET NULL,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    entry_date          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_object_class_name ON object_class(class_name);
CREATE INDEX IF NOT EXISTS idx_object_class_parent ON object_class(parent_class_id);
CREATE INDEX IF NOT EXISTS idx_object_class_metadata_gin ON object_class USING GIN(metadata);
"""


create_tenant_security_tables = """
CREATE TABLE IF NOT EXISTS tenant (
    tenant_id UUID PRIMARY KEY,
    tenant_key VARCHAR(128) NOT NULL UNIQUE,
    display_name VARCHAR(256) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO tenant (tenant_id, tenant_key, display_name)
VALUES ('00000000-0000-0000-0000-000000000001', 'legacy', 'Legacy tenant')
ON CONFLICT (tenant_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS ikos_user (
    user_id UUID PRIMARY KEY,
    external_subject VARCHAR(256) NOT NULL UNIQUE,
    display_name VARCHAR(256),
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_membership (
    membership_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES ikos_user(user_id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, user_id)
);

CREATE TABLE IF NOT EXISTS role (
    role_id BIGSERIAL PRIMARY KEY,
    role_key VARCHAR(128) NOT NULL UNIQUE,
    display_name VARCHAR(256) NOT NULL
);

CREATE TABLE IF NOT EXISTS permission (
    permission_key VARCHAR(128) PRIMARY KEY,
    description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS role_permission (
    role_id BIGINT NOT NULL REFERENCES role(role_id) ON DELETE CASCADE,
    permission_key VARCHAR(128) NOT NULL REFERENCES permission(permission_key) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_key)
);

CREATE TABLE IF NOT EXISTS membership_role (
    membership_id BIGINT NOT NULL REFERENCES tenant_membership(membership_id) ON DELETE CASCADE,
    role_id BIGINT NOT NULL REFERENCES role(role_id) ON DELETE CASCADE,
    PRIMARY KEY (membership_id, role_id)
);

INSERT INTO role (role_key, display_name) VALUES
    ('employee', 'Employee'),
    ('finance_viewer', 'Finance Viewer'),
    ('finance_operator', 'Finance Operator'),
    ('tenant_admin', 'Tenant Administrator')
ON CONFLICT (role_key) DO NOTHING;
INSERT INTO permission (permission_key, description) VALUES
    ('documents.read', 'Read tenant documents'),
    ('documents.write', 'Create or update tenant documents'),
    ('workflows.run', 'Execute tenant workflows and dispatch ingestion deliveries'),
    ('finance.read', 'Read finance resources'),
    ('finance.write', 'Modify finance resources'),
    ('documents.restricted.read', 'Read restricted tenant documents'),
    ('tenant.members.manage', 'Manage tenant membership and roles'),
    ('tenant.configuration.manage', 'Manage configuration for the active tenant'),
    ('platform.configuration.publish', 'Publish shared configuration for all tenants')
ON CONFLICT (permission_key) DO NOTHING;
INSERT INTO role_permission (role_id, permission_key)
SELECT r.role_id, p.permission_key
FROM role r CROSS JOIN permission p
WHERE (r.role_key = 'employee' AND p.permission_key = 'documents.read')
   OR (r.role_key = 'finance_viewer' AND p.permission_key IN ('documents.read', 'finance.read'))
   OR (r.role_key = 'finance_operator' AND p.permission_key IN ('documents.read', 'documents.write', 'finance.read', 'finance.write'))
    OR (r.role_key = 'tenant_admin' AND p.permission_key IN ('documents.read', 'documents.write', 'documents.restricted.read', 'finance.read', 'finance.write', 'tenant.members.manage', 'tenant.configuration.manage', 'workflows.run'))
ON CONFLICT DO NOTHING;
"""


create_object_table = """
CREATE TABLE IF NOT EXISTS object_instance (
    object_id           BIGSERIAL PRIMARY KEY,
    tenant_id           UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    object_name         VARCHAR(256) NOT NULL,
    canonical_full_name VARCHAR(256),
    class_name          VARCHAR(256) NOT NULL REFERENCES object_class(class_name) ON DELETE RESTRICT,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    status              VARCHAR(32) NOT NULL DEFAULT 'active',
    valid_from          DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until         DATE,
    valid_daterange     daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED,
    entry_date          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, object_name, class_name)
);

CREATE INDEX IF NOT EXISTS idx_object_instance_name ON object_instance(object_name);
CREATE INDEX IF NOT EXISTS idx_object_instance_tenant_name ON object_instance(tenant_id, LOWER(object_name));
CREATE INDEX IF NOT EXISTS idx_object_instance_name_lower ON object_instance (LOWER(object_name));
CREATE INDEX IF NOT EXISTS idx_object_instance_canonical_full_name ON object_instance(canonical_full_name);
CREATE INDEX IF NOT EXISTS idx_object_instance_canonical_full_name_lower ON object_instance (LOWER(canonical_full_name));
CREATE INDEX IF NOT EXISTS idx_object_instance_class_name ON object_instance(class_name);
CREATE INDEX IF NOT EXISTS idx_object_instance_metadata_gin ON object_instance USING GIN(metadata);
CREATE INDEX IF NOT EXISTS idx_object_instance_name_trgm ON object_instance USING GIN (LOWER(object_name) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_object_instance_canonical_full_name_trgm ON object_instance USING GIN (LOWER(canonical_full_name) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_object_instance_status ON object_instance(status);
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'object_instance' AND column_name = 'valid_daterange'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_object_instance_valid_daterange ON object_instance USING GIST (valid_daterange);
    END IF;
END $$;
"""


create_relationship_table = """
CREATE TABLE IF NOT EXISTS object_relationship (
    relationship_id     BIGSERIAL PRIMARY KEY,
    tenant_id           UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    relationship_name   VARCHAR(64) NOT NULL,
    relationship_cat    VARCHAR(64) NOT NULL,
    src_object_id       BIGINT NOT NULL REFERENCES object_instance(object_id) ON DELETE CASCADE,
    tar_object_id       BIGINT NOT NULL REFERENCES object_instance(object_id) ON DELETE CASCADE,
    confidence          NUMERIC(5,4),
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    valid_from          DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until         DATE,
    valid_daterange     daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED,
    entry_date          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, relationship_name, src_object_id, tar_object_id)
);

CREATE INDEX IF NOT EXISTS idx_object_relationship_name ON object_relationship(relationship_name);
CREATE INDEX IF NOT EXISTS idx_object_relationship_tenant ON object_relationship(tenant_id, relationship_name);
CREATE INDEX IF NOT EXISTS idx_object_relationship_src ON object_relationship(src_object_id);
CREATE INDEX IF NOT EXISTS idx_object_relationship_tar ON object_relationship(tar_object_id);
CREATE INDEX IF NOT EXISTS idx_object_relationship_cat ON object_relationship(relationship_cat);
CREATE INDEX IF NOT EXISTS idx_object_relationship_metadata_gin ON object_relationship USING GIN(metadata);
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'object_relationship' AND column_name = 'valid_daterange'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_object_relationship_valid_daterange ON object_relationship USING GIST (valid_daterange);
    END IF;
END $$;
"""


create_document_table = """
CREATE TABLE IF NOT EXISTS document (
    doc_id                BIGSERIAL PRIMARY KEY,
        tenant_id             UUID NOT NULL REFERENCES tenant(tenant_id),
    doc_name              VARCHAR(512),
    doc_key               VARCHAR(512),
    doc_path              VARCHAR(1024),
    markdown_path         VARCHAR(1024),
    doc_cat               VARCHAR(128),
    doc_type              VARCHAR(128),
    doc_date              DATE,
    doc_desc              TEXT,
    doc_theme             TEXT,
    keyword_text          TEXT,
    keyword_vec           TSVECTOR,
    identifiers_kv        JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    status                VARCHAR(32) NOT NULL DEFAULT 'active',
    access_level          VARCHAR(32) NOT NULL DEFAULT 'tenant' CHECK (access_level IN ('tenant','finance','restricted')),
    valid_from            DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until           DATE,
    valid_daterange       daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED,
    entry_date            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_docs_cat ON document(doc_cat);
CREATE INDEX IF NOT EXISTS idx_docs_tenant_created ON document(tenant_id, entry_date DESC);
CREATE INDEX IF NOT EXISTS idx_docs_type ON document(doc_type);
CREATE INDEX IF NOT EXISTS idx_docs_key ON document(doc_key);
CREATE INDEX IF NOT EXISTS idx_docs_key_lower ON document (LOWER(doc_key));
CREATE INDEX IF NOT EXISTS idx_docs_key_trgm ON document USING GIN (LOWER(doc_key) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_docs_path ON document(doc_path);
CREATE INDEX IF NOT EXISTS idx_docs_markdown_path ON document(markdown_path);
CREATE UNIQUE INDEX IF NOT EXISTS uq_document_tenant_doc_path_nonnull ON document(tenant_id, doc_path) WHERE doc_path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_docs_date ON document(doc_date);
CREATE INDEX IF NOT EXISTS idx_docs_identifiers_gin ON document USING GIN(identifiers_kv);
CREATE INDEX IF NOT EXISTS idx_docs_metadata_gin ON document USING GIN(metadata);
CREATE INDEX IF NOT EXISTS idx_docs_keyword_vec ON document USING GIN(keyword_vec);
CREATE INDEX IF NOT EXISTS idx_docs_keyword_trgm ON document USING GIN (LOWER(keyword_text) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_docs_user_description_trgm ON document USING GIN (LOWER(metadata->>'user_description') gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_docs_user_tags_trgm ON document USING GIN (LOWER(metadata->>'user_tags') gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_docs_status ON document(status);
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'document' AND column_name = 'valid_daterange'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_docs_valid_daterange ON document USING GIST (valid_daterange);
    END IF;
END $$;
"""


create_document_content_table = """
CREATE TABLE IF NOT EXISTS document_content (
    content_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(tenant_id) ON DELETE CASCADE,
    doc_id BIGINT NOT NULL REFERENCES document(doc_id) ON DELETE CASCADE,
    content_type VARCHAR(64) NOT NULL,
    content_version INTEGER NOT NULL DEFAULT 1,
    content_text TEXT,
    content_bytes BYTEA,
    object_uri TEXT,
    checksum_sha256 VARCHAR(64),
    source_name VARCHAR(512),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_by VARCHAR(256),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (content_text IS NOT NULL OR content_bytes IS NOT NULL OR object_uri IS NOT NULL),
    UNIQUE (tenant_id, doc_id, content_type, content_version)
);

CREATE INDEX IF NOT EXISTS idx_document_content_tenant_doc ON document_content(tenant_id, doc_id);
CREATE INDEX IF NOT EXISTS idx_document_content_active ON document_content(tenant_id, doc_id, content_type, content_version DESC) WHERE status = 'active';
"""

create_part_table = """
CREATE TABLE IF NOT EXISTS part (
    part_id               BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    doc_id                BIGINT NOT NULL REFERENCES document(doc_id) ON DELETE CASCADE,
    part_key              VARCHAR(128) NOT NULL,
    class_id              BIGINT REFERENCES object_class(class_id) ON DELETE CASCADE,
    object_id             BIGINT REFERENCES object_instance(object_id) ON DELETE CASCADE,
    relationship_id       BIGINT REFERENCES object_relationship(relationship_id) ON DELETE CASCADE,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    entry_date            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, doc_id, part_key)
);

CREATE INDEX IF NOT EXISTS idx_part_doc ON part(doc_id);
CREATE INDEX IF NOT EXISTS idx_part_class ON part(class_id);
CREATE INDEX IF NOT EXISTS idx_part_object ON part(object_id);
CREATE INDEX IF NOT EXISTS idx_part_relationship ON part(relationship_id);
CREATE INDEX IF NOT EXISTS idx_part_metadata_gin ON part USING GIN(metadata);
"""


create_document_table_cell_table = """
CREATE TABLE IF NOT EXISTS document_table_cell (
    cell_id               BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    doc_id                BIGINT NOT NULL REFERENCES document(doc_id) ON DELETE CASCADE,
    table_index           INTEGER NOT NULL,
    row_index             INTEGER NOT NULL,
    row_label             TEXT,
    column_index          INTEGER NOT NULL,
    column_label          TEXT,
    cell_value            TEXT,
    source_kind           VARCHAR(32) NOT NULL DEFAULT 'document',
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, doc_id, table_index, row_index, column_index)
);

CREATE INDEX IF NOT EXISTS idx_doc_table_cell_doc_id ON document_table_cell(doc_id);
CREATE INDEX IF NOT EXISTS idx_doc_table_cell_row_label_trgm ON document_table_cell USING GIN (LOWER(COALESCE(row_label, '')) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_doc_table_cell_column_label_trgm ON document_table_cell USING GIN (LOWER(COALESCE(column_label, '')) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_doc_table_cell_metadata_gin ON document_table_cell USING GIN(metadata);
"""


create_document_geographic_facet_table = """
CREATE TABLE IF NOT EXISTS document_geographic_facet (
    facet_id              BIGSERIAL PRIMARY KEY,
    doc_id                BIGINT NOT NULL REFERENCES document(doc_id) ON DELETE CASCADE,
    raw_name              VARCHAR(512) NOT NULL,
    canonical_name        VARCHAR(512) NOT NULL,
    place_kind            VARCHAR(32) NOT NULL DEFAULT 'unknown'
                         CHECK (place_kind IN ('country','region','city','address','venue','unknown')),
    country_code          VARCHAR(8),
    role_name             VARCHAR(64) NOT NULL DEFAULT 'document_subject'
                         CHECK (role_name IN ('incident_location','issuer_location','sender_address','recipient_address','document_subject','service_location','jurisdiction','unknown')),
    confidence            NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (doc_id, canonical_name, place_kind, country_code, role_name)
);

CREATE INDEX IF NOT EXISTS idx_doc_geo_facet_doc ON document_geographic_facet(doc_id);
CREATE INDEX IF NOT EXISTS idx_doc_geo_facet_canonical ON document_geographic_facet(LOWER(canonical_name));
CREATE INDEX IF NOT EXISTS idx_doc_geo_facet_country ON document_geographic_facet(country_code);
CREATE INDEX IF NOT EXISTS idx_doc_geo_facet_role ON document_geographic_facet(role_name);
"""


create_document_action_item_table = """
CREATE TABLE IF NOT EXISTS document_action_item (
    action_item_id        BIGSERIAL PRIMARY KEY,
    doc_id                BIGINT NOT NULL REFERENCES document(doc_id) ON DELETE CASCADE,
    action_type           VARCHAR(128) NOT NULL DEFAULT 'general_action',
    instruction           TEXT NOT NULL,
    subject_name          VARCHAR(512),
    assignee_name         VARCHAR(512),
    due_date              DATE,
    priority              VARCHAR(32) NOT NULL DEFAULT 'medium'
                         CHECK (priority IN ('low','medium','high','urgent')),
    status                VARCHAR(32) NOT NULL DEFAULT 'open'
                         CHECK (status IN ('open','in_progress','completed','cancelled','superseded')),
    confidence            NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (doc_id, action_type, instruction)
);

CREATE INDEX IF NOT EXISTS idx_doc_action_doc ON document_action_item(doc_id);
CREATE INDEX IF NOT EXISTS idx_doc_action_status_due ON document_action_item(status, due_date);
CREATE INDEX IF NOT EXISTS idx_doc_action_subject_lower ON document_action_item(LOWER(subject_name));
CREATE INDEX IF NOT EXISTS idx_doc_action_type ON document_action_item(action_type);
CREATE INDEX IF NOT EXISTS idx_doc_action_metadata_gin ON document_action_item USING GIN(metadata);
"""


# Extended attribute table variant supporting class/object/relationship in addition to graph types
create_attribute_table = """
CREATE TABLE IF NOT EXISTS attribute (
    attr_id              BIGSERIAL PRIMARY KEY,
    tenant_id            UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    src_id               BIGINT NOT NULL,
    src_type             VARCHAR(16) NOT NULL CHECK (src_type IN ('part','document','class','object','relationship')),
    attr_type            VARCHAR(64) NOT NULL,
    valid_from           DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_until          DATE,
    valid_range tsrange GENERATED ALWAYS AS (tsrange(valid_from, valid_until)) STORED,
    valid_daterange      daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED,
    attr_json            JSONB NOT NULL,
    search_txt           TEXT,
    search_vec           TSVECTOR,
    entry_date           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_attr_src ON attribute(src_type, src_id);
CREATE INDEX IF NOT EXISTS idx_attr_tenant_src ON attribute(tenant_id, src_type, src_id);
CREATE INDEX IF NOT EXISTS idx_attr_type ON attribute(attr_type);
CREATE INDEX IF NOT EXISTS idx_attr_json_gin ON attribute USING GIN(attr_json);
CREATE INDEX IF NOT EXISTS idx_attr_search_vec ON attribute USING GIN(search_vec);
CREATE INDEX IF NOT EXISTS idx_attributes_valid_range ON attribute USING GIST (valid_range);
CREATE INDEX IF NOT EXISTS idx_attributes_valid_daterange ON attribute USING GIST (valid_daterange);
"""


create_resolution_ambiguity_queue_table = """
CREATE TABLE IF NOT EXISTS resolution_ambiguity_queue (
    queue_id             BIGSERIAL PRIMARY KEY,
    entity_id            VARCHAR(128),
    object_name          VARCHAR(256) NOT NULL,
    class_name           VARCHAR(256) NOT NULL,
    doc_id               BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    payload              JSONB NOT NULL DEFAULT '{}'::jsonb,
    resolution_result    JSONB NOT NULL DEFAULT '{}'::jsonb,
    status               VARCHAR(32) NOT NULL DEFAULT 'pending',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_status ON resolution_ambiguity_queue(status);
CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_doc_id ON resolution_ambiguity_queue(doc_id);
CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_class_name ON resolution_ambiguity_queue(class_name);
CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_created_at ON resolution_ambiguity_queue(created_at);
CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_payload_gin ON resolution_ambiguity_queue USING GIN(payload);
CREATE INDEX IF NOT EXISTS idx_resolution_ambiguity_result_gin ON resolution_ambiguity_queue USING GIN(resolution_result);
"""


create_tx_match_index_pending_table = """
CREATE TABLE IF NOT EXISTS tx_match_index_pending (
    pending_id            BIGSERIAL PRIMARY KEY,
    doc_id                BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    source                VARCHAR(64) NOT NULL DEFAULT 'ingestion',
    status                VARCHAR(32) NOT NULL DEFAULT 'pending',
    payload               JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message         TEXT,
    retry_count           INTEGER NOT NULL DEFAULT 0,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tx_match_pending_status ON tx_match_index_pending(status);
CREATE INDEX IF NOT EXISTS idx_tx_match_pending_doc_id ON tx_match_index_pending(doc_id);
CREATE INDEX IF NOT EXISTS idx_tx_match_pending_created_at ON tx_match_index_pending(created_at);
CREATE INDEX IF NOT EXISTS idx_tx_match_pending_payload_gin ON tx_match_index_pending USING GIN(payload);
"""


create_object_alias_table = """
CREATE TABLE IF NOT EXISTS object_alias (
    alias_id              BIGSERIAL PRIMARY KEY,
    primary_object_id     BIGINT NOT NULL REFERENCES object_instance(object_id) ON DELETE CASCADE,
    alias_object_id       BIGINT NOT NULL REFERENCES object_instance(object_id) ON DELETE CASCADE,
    alias_type            VARCHAR(64) NOT NULL DEFAULT 'same_entity',
    confidence            NUMERIC(5,4),
    status                VARCHAR(32) NOT NULL DEFAULT 'active',
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (primary_object_id <> alias_object_id),
    UNIQUE (primary_object_id, alias_object_id)
);

CREATE INDEX IF NOT EXISTS idx_object_alias_primary ON object_alias(primary_object_id);
CREATE INDEX IF NOT EXISTS idx_object_alias_alias ON object_alias(alias_object_id);
CREATE INDEX IF NOT EXISTS idx_object_alias_status ON object_alias(status);
CREATE INDEX IF NOT EXISTS idx_object_alias_type ON object_alias(alias_type);
CREATE INDEX IF NOT EXISTS idx_object_alias_metadata_gin ON object_alias USING GIN(metadata);
"""


create_semantic_patterns_table = """
CREATE TABLE IF NOT EXISTS semantic_patterns (
    pattern_id           BIGSERIAL PRIMARY KEY,
    tenant_id            UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    pattern_text         VARCHAR(512) NOT NULL,
    semantic_concept     VARCHAR(128) NOT NULL,
    mapped_attributes    JSONB NOT NULL DEFAULT '{}'::jsonb,
    computation_rule     TEXT,
    entity_class         VARCHAR(128),
    source_type          VARCHAR(32) NOT NULL DEFAULT 'seeded' CHECK (source_type IN ('seeded','learned','manual')),
    confidence           NUMERIC(5,4) NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    pattern_language     VARCHAR(32) NOT NULL DEFAULT 'en',
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by           VARCHAR(128),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, pattern_text, entity_class, pattern_language),
    UNIQUE (tenant_id, pattern_id)
);

CREATE INDEX IF NOT EXISTS idx_semantic_patterns_text ON semantic_patterns(pattern_text);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_text_lower ON semantic_patterns (LOWER(pattern_text));
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_concept ON semantic_patterns(semantic_concept);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_entity_class ON semantic_patterns(entity_class);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_source_type ON semantic_patterns(source_type);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_confidence ON semantic_patterns(confidence);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_language ON semantic_patterns(pattern_language);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_metadata_gin ON semantic_patterns USING GIN(metadata);
CREATE INDEX IF NOT EXISTS idx_semantic_patterns_mapped_attr_gin ON semantic_patterns USING GIN(mapped_attributes);
CREATE UNIQUE INDEX IF NOT EXISTS uq_semantic_patterns_tenant_pattern_id ON semantic_patterns(tenant_id, pattern_id);
"""


create_pattern_synonyms_table = """
CREATE TABLE IF NOT EXISTS pattern_synonyms (
    synonym_id           BIGSERIAL PRIMARY KEY,
    tenant_id            UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    pattern_id           BIGINT NOT NULL,
    synonym_text         VARCHAR(512) NOT NULL,
    language             VARCHAR(32) NOT NULL DEFAULT 'en',
    semantic_distance    NUMERIC(5,4) NOT NULL DEFAULT 0.0 CHECK (semantic_distance >= 0 AND semantic_distance <= 1),
    match_type           VARCHAR(32) NOT NULL DEFAULT 'exact' CHECK (match_type IN ('exact','template','fuzzy','semantic')),
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, pattern_id, synonym_text, language),
    FOREIGN KEY (tenant_id, pattern_id) REFERENCES semantic_patterns(tenant_id, pattern_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_pattern_id ON pattern_synonyms(pattern_id);
CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_text ON pattern_synonyms(synonym_text);
CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_text_lower ON pattern_synonyms (LOWER(synonym_text));
CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_language ON pattern_synonyms(language);
CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_match_type ON pattern_synonyms(match_type);
CREATE INDEX IF NOT EXISTS idx_pattern_synonyms_semantic_distance ON pattern_synonyms(semantic_distance);
"""


create_semantic_terms_table = """
CREATE TABLE IF NOT EXISTS semantic_terms (
    term_id              BIGSERIAL PRIMARY KEY,
    tenant_id            UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    kind                 VARCHAR(32) NOT NULL CHECK (kind IN ('attribute','relationship','entity_type','category')),
    canonical_name       VARCHAR(256) NOT NULL,
    term_text            VARCHAR(512) NOT NULL,
    language             VARCHAR(32) NOT NULL DEFAULT 'und',
    source_type          VARCHAR(64) NOT NULL DEFAULT 'ingest' CHECK (source_type IN ('seeded','ingest','solf_clause','manual','llm_generated')),
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, kind, canonical_name, term_text, language)
);

CREATE INDEX IF NOT EXISTS idx_semantic_terms_kind ON semantic_terms(kind);
CREATE INDEX IF NOT EXISTS idx_semantic_terms_canonical_name ON semantic_terms(canonical_name);
CREATE INDEX IF NOT EXISTS idx_semantic_terms_term_text ON semantic_terms(term_text);
CREATE INDEX IF NOT EXISTS idx_semantic_terms_term_text_lower ON semantic_terms (LOWER(term_text));
CREATE INDEX IF NOT EXISTS idx_semantic_terms_source_type ON semantic_terms(source_type);
CREATE INDEX IF NOT EXISTS idx_semantic_terms_metadata_gin ON semantic_terms USING GIN(metadata);
"""


create_query_term_alias_table = """
CREATE TABLE IF NOT EXISTS query_term_alias (
    alias_id              BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    alias_text            VARCHAR(512) NOT NULL,
    canonical_name        VARCHAR(256) NOT NULL,
    kind                  VARCHAR(32) NOT NULL CHECK (kind IN ('attribute','relationship','entity_type','category')),
    language              VARCHAR(32) NOT NULL DEFAULT 'und',
    priority              INTEGER NOT NULL DEFAULT 100,
    source_type           VARCHAR(64) NOT NULL DEFAULT 'manual',
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT true,
    created_by            VARCHAR(128),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, alias_text, kind, language)
);

CREATE INDEX IF NOT EXISTS idx_query_term_alias_text ON query_term_alias(alias_text);
CREATE INDEX IF NOT EXISTS idx_query_term_alias_text_lower ON query_term_alias (LOWER(alias_text));
CREATE INDEX IF NOT EXISTS idx_query_term_alias_canonical_name ON query_term_alias(canonical_name);
CREATE INDEX IF NOT EXISTS idx_query_term_alias_kind ON query_term_alias(kind);
CREATE INDEX IF NOT EXISTS idx_query_term_alias_is_active ON query_term_alias(is_active);
CREATE INDEX IF NOT EXISTS idx_query_term_alias_priority ON query_term_alias(priority);
CREATE INDEX IF NOT EXISTS idx_query_term_alias_metadata_gin ON query_term_alias USING GIN(metadata);
"""


create_query_alias_candidate_table = """
CREATE TABLE IF NOT EXISTS query_alias_candidate (
    candidate_id          BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    alias_text            VARCHAR(512) NOT NULL,
    canonical_name        VARCHAR(256) NOT NULL,
    kind                  VARCHAR(32) NOT NULL CHECK (kind IN ('attribute','relationship','entity_type','category','document_hint')),
    language              VARCHAR(32) NOT NULL DEFAULT 'und',
    confidence            NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    occurrence_count      INTEGER NOT NULL DEFAULT 1,
    first_seen_doc_id     BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    last_seen_doc_id      BIGINT REFERENCES document(doc_id) ON DELETE SET NULL,
    status                VARCHAR(32) NOT NULL DEFAULT 'proposed' CHECK (status IN ('proposed','approved','rejected')),
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, alias_text, canonical_name, kind, language)
);

CREATE INDEX IF NOT EXISTS idx_query_alias_candidate_status ON query_alias_candidate(status);
CREATE INDEX IF NOT EXISTS idx_query_alias_candidate_kind ON query_alias_candidate(kind);
CREATE INDEX IF NOT EXISTS idx_query_alias_candidate_occurrence ON query_alias_candidate(occurrence_count DESC);
"""


create_source_schema_taxonomy_table = """
CREATE TABLE IF NOT EXISTS source_schema_taxonomy (
    taxonomy_id           BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    source_id             BIGINT NOT NULL,
    schema_name           VARCHAR(256) NOT NULL,
    table_name            VARCHAR(256) NOT NULL,
    column_name           VARCHAR(256),
    category_name         VARCHAR(256) NOT NULL,
    role_name             VARCHAR(64) NOT NULL DEFAULT 'entity_candidate'
                         CHECK (role_name IN ('entity_candidate','identity','attribute','filter','measure','temporal','relationship_key')),
    confidence            NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, source_id, schema_name, table_name, column_name, category_name, role_name)
);

CREATE INDEX IF NOT EXISTS idx_source_schema_taxonomy_source ON source_schema_taxonomy(source_id);
CREATE INDEX IF NOT EXISTS idx_source_schema_taxonomy_category ON source_schema_taxonomy(category_name);
CREATE INDEX IF NOT EXISTS idx_source_schema_taxonomy_role ON source_schema_taxonomy(role_name);
CREATE INDEX IF NOT EXISTS idx_source_schema_taxonomy_lookup ON source_schema_taxonomy(category_name, role_name, is_active);
CREATE INDEX IF NOT EXISTS idx_source_schema_taxonomy_metadata_gin ON source_schema_taxonomy USING GIN(metadata);
"""


create_category_registry_table = """
CREATE TABLE IF NOT EXISTS category_registry (
    category_id           BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    category_name         VARCHAR(256) NOT NULL,
    source_scope          VARCHAR(32) NOT NULL CHECK (source_scope IN ('internal', 'external', 'document_kb')),
    access_method         VARCHAR(32) NOT NULL CHECK (access_method IN ('sql', 'semantic_lookup', 'rag', 'document_facet')),
    source_id             BIGINT,
    schema_name           VARCHAR(256),
    table_name            VARCHAR(256),
    column_name           VARCHAR(256),
    entity_types          JSONB NOT NULL DEFAULT '[]'::jsonb,
    attributes            JSONB NOT NULL DEFAULT '[]'::jsonb,
    document_types        JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence            NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, category_name, source_scope, access_method, source_id, schema_name, table_name, column_name)
);

CREATE INDEX IF NOT EXISTS idx_category_registry_name ON category_registry(category_name);
CREATE INDEX IF NOT EXISTS idx_category_registry_scope_method ON category_registry(source_scope, access_method);
CREATE INDEX IF NOT EXISTS idx_category_registry_source ON category_registry(source_id);
CREATE INDEX IF NOT EXISTS idx_category_registry_entity_types ON category_registry USING GIN(entity_types);
CREATE INDEX IF NOT EXISTS idx_category_registry_attributes ON category_registry USING GIN(attributes);
CREATE INDEX IF NOT EXISTS idx_category_registry_document_types ON category_registry USING GIN(document_types);
CREATE INDEX IF NOT EXISTS idx_category_registry_metadata ON category_registry USING GIN(metadata);
"""


create_categories_table = """
CREATE TABLE IF NOT EXISTS categories (
    category_id           BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    name                  VARCHAR(256) NOT NULL,
    description           TEXT,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name),
    UNIQUE (tenant_id, category_id)
);

CREATE INDEX IF NOT EXISTS idx_categories_name ON categories(name);
CREATE INDEX IF NOT EXISTS idx_categories_metadata ON categories USING GIN(metadata);
"""


create_category_sources_table = """
CREATE TABLE IF NOT EXISTS category_sources (
    category_source_id    BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    category_id           BIGINT NOT NULL,
    source_type           VARCHAR(32) NOT NULL CHECK (source_type IN ('internal_db', 'external_db', 'documents')),
    source_id             BIGINT,
    access_method         VARCHAR(32) NOT NULL CHECK (access_method IN ('sql', 'semantic_lookup', 'rag', 'document_facet')),
    priority              INTEGER NOT NULL DEFAULT 100,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, category_id) REFERENCES categories(tenant_id, category_id) ON DELETE CASCADE,
    UNIQUE (tenant_id, category_source_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_category_sources_identity
ON category_sources(tenant_id, category_id, source_type, COALESCE(source_id, 0), access_method);
CREATE INDEX IF NOT EXISTS idx_category_sources_category ON category_sources(category_id);
CREATE INDEX IF NOT EXISTS idx_category_sources_source ON category_sources(source_id);
"""


create_category_entities_table = """
CREATE TABLE IF NOT EXISTS category_entities (
    category_entity_id    BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    category_source_id    BIGINT NOT NULL,
    entity_type           VARCHAR(256) NOT NULL,
    source_table          VARCHAR(256),
    source_column         VARCHAR(256),
    role_name             VARCHAR(64) NOT NULL DEFAULT 'entity_candidate',
    confidence            NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    FOREIGN KEY (tenant_id, category_source_id) REFERENCES category_sources(tenant_id, category_source_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_category_entities_identity
ON category_entities(tenant_id, category_source_id, entity_type, COALESCE(source_table, ''), COALESCE(source_column, ''), role_name);
CREATE INDEX IF NOT EXISTS idx_category_entities_type ON category_entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_category_entities_source ON category_entities(category_source_id);
"""


create_category_attributes_table = """
CREATE TABLE IF NOT EXISTS category_attributes (
    category_attribute_id BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    category_source_id    BIGINT NOT NULL,
    attribute_name        VARCHAR(256) NOT NULL,
    canonical_name        VARCHAR(256) NOT NULL,
    source_table          VARCHAR(256),
    source_column         VARCHAR(256),
    role_name             VARCHAR(64) NOT NULL DEFAULT 'attribute',
    confidence            NUMERIC(4,3) NOT NULL DEFAULT 0.500,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    FOREIGN KEY (tenant_id, category_source_id) REFERENCES category_sources(tenant_id, category_source_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_category_attributes_identity
ON category_attributes(tenant_id, category_source_id, canonical_name, COALESCE(source_table, ''), COALESCE(source_column, ''), role_name);
CREATE INDEX IF NOT EXISTS idx_category_attributes_canonical ON category_attributes(canonical_name);
CREATE INDEX IF NOT EXISTS idx_category_attributes_source ON category_attributes(category_source_id);
"""


create_query_pattern_registry_table = """
CREATE TABLE IF NOT EXISTS query_pattern_registry (
    pattern_id            BIGSERIAL PRIMARY KEY,
    pattern_key           VARCHAR(128) NOT NULL UNIQUE,
    display_name          VARCHAR(256) NOT NULL,
    intent_key            VARCHAR(128) NOT NULL,
    description           TEXT,
    allowed_source_scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
    allowed_access_methods JSONB NOT NULL DEFAULT '[]'::jsonb,
    priority              INTEGER NOT NULL DEFAULT 100,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_query_pattern_registry_intent ON query_pattern_registry(intent_key);
CREATE INDEX IF NOT EXISTS idx_query_pattern_registry_active ON query_pattern_registry(is_active);
"""


create_solf_clauses_table = """
CREATE TABLE IF NOT EXISTS solf_clauses (
    clause_id            BIGSERIAL PRIMARY KEY,
    tenant_id            UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    configuration_scope  VARCHAR(16) NOT NULL DEFAULT 'tenant' CHECK (configuration_scope IN ('tenant', 'global')),
    clause_name          VARCHAR(256) NOT NULL,
    clause_type          VARCHAR(32) NOT NULL CHECK (clause_type IN ('query_pattern','resolve_policy','ingest_rule','computation_rule')),
    entity_class         VARCHAR(128),
    clause_body          TEXT NOT NULL,
    referenced_patterns  BIGINT[] DEFAULT '{}',
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active            BOOLEAN NOT NULL DEFAULT true,
    created_by           VARCHAR(128),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_solf_clauses_name ON solf_clauses(clause_name);
CREATE INDEX IF NOT EXISTS idx_solf_clauses_type ON solf_clauses(clause_type);
CREATE INDEX IF NOT EXISTS idx_solf_clauses_entity_class ON solf_clauses(entity_class);
CREATE INDEX IF NOT EXISTS idx_solf_clauses_is_active ON solf_clauses(is_active);
CREATE INDEX IF NOT EXISTS idx_solf_clauses_created_at ON solf_clauses(created_at);
CREATE INDEX IF NOT EXISTS idx_solf_clauses_metadata_gin ON solf_clauses USING GIN(metadata);
"""


create_business_rules_table = """
CREATE TABLE IF NOT EXISTS business_rules (
    rule_id               BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    configuration_scope   VARCHAR(16) NOT NULL DEFAULT 'tenant' CHECK (configuration_scope IN ('tenant', 'global')),
    rule_name             VARCHAR(256) NOT NULL,
    rule_text             TEXT NOT NULL,
    structured_rule       JSONB NOT NULL DEFAULT '{}'::jsonb,
    solf_script           TEXT NOT NULL DEFAULT '',
    scope                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT true,
    created_by            VARCHAR(128),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_business_rules_active ON business_rules(is_active);
CREATE INDEX IF NOT EXISTS idx_business_rules_name ON business_rules(rule_name);
CREATE INDEX IF NOT EXISTS idx_business_rules_scope_gin ON business_rules USING GIN(scope);
CREATE INDEX IF NOT EXISTS idx_business_rules_structured_gin ON business_rules USING GIN(structured_rule);
CREATE INDEX IF NOT EXISTS idx_business_rules_metadata_gin ON business_rules USING GIN(metadata);
"""


create_solf_workflow_registry_table = """
CREATE TABLE IF NOT EXISTS solf_workflow_registry (
    workflow_id           BIGSERIAL PRIMARY KEY,
    tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    configuration_scope   VARCHAR(16) NOT NULL DEFAULT 'tenant' CHECK (configuration_scope IN ('tenant', 'global')),
    workflow_key          VARCHAR(256) NOT NULL,
    workflow_name         VARCHAR(256) NOT NULL,
    description           TEXT NOT NULL DEFAULT '',
    domain                VARCHAR(64),
    status                VARCHAR(32) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','review','published','deprecated','archived')),
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_by            VARCHAR(128),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_solf_workflow_registry_status ON solf_workflow_registry(status);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_registry_domain ON solf_workflow_registry(domain);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_registry_active ON solf_workflow_registry(is_active);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_registry_metadata_gin ON solf_workflow_registry USING GIN(metadata);
"""


create_solf_workflow_versions_table = """
CREATE TABLE IF NOT EXISTS solf_workflow_versions (
    workflow_version_id   BIGSERIAL PRIMARY KEY,
    workflow_id           BIGINT NOT NULL REFERENCES solf_workflow_registry(workflow_id) ON DELETE CASCADE,
    version_no            INTEGER NOT NULL,
    rule_id               BIGINT REFERENCES business_rules(rule_id) ON DELETE SET NULL,
    graph_spec            JSONB NOT NULL DEFAULT '{}'::jsonb,
    input_contract        JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_contract       JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_by            VARCHAR(128),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_id, version_no)
);

CREATE INDEX IF NOT EXISTS idx_solf_workflow_versions_workflow_id ON solf_workflow_versions(workflow_id);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_versions_rule_id ON solf_workflow_versions(rule_id);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_versions_active ON solf_workflow_versions(is_active);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_versions_metadata_gin ON solf_workflow_versions USING GIN(metadata);
"""


create_solf_workflow_steps_table = """
CREATE TABLE IF NOT EXISTS solf_workflow_steps (
    step_id               BIGSERIAL PRIMARY KEY,
    workflow_version_id   BIGINT NOT NULL REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE CASCADE,
    step_order            INTEGER NOT NULL,
    step_key              VARCHAR(256) NOT NULL,
    step_kind             VARCHAR(32) NOT NULL CHECK (step_kind IN ('clause','class_transform','class_generate','class_iterate','python_binding')),
    clause_id             BIGINT REFERENCES solf_clauses(clause_id) ON DELETE SET NULL,
    clause_name           VARCHAR(256),
    input_class           VARCHAR(128),
    output_class          VARCHAR(128),
    operation             VARCHAR(64),
    python_module         VARCHAR(256),
    python_function       VARCHAR(256),
    config                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_version_id, step_order),
    UNIQUE (workflow_version_id, step_key)
);

CREATE INDEX IF NOT EXISTS idx_solf_workflow_steps_version_id ON solf_workflow_steps(workflow_version_id);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_steps_kind ON solf_workflow_steps(step_kind);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_steps_clause_id ON solf_workflow_steps(clause_id);
CREATE INDEX IF NOT EXISTS idx_solf_workflow_steps_config_gin ON solf_workflow_steps USING GIN(config);
"""


create_workflow_pipeline_run_tables = """
CREATE TABLE IF NOT EXISTS workflow_pipeline_run (
    run_id              BIGSERIAL PRIMARY KEY,
    tenant_id           UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenant(tenant_id),
    workflow_version_id BIGINT REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE SET NULL,
    workflow_key        VARCHAR(256),
    run_status          VARCHAR(32) NOT NULL DEFAULT 'pending'
                        CHECK (run_status IN ('pending','running','paused','completed','failed','cancelled')),
    paused_at_step_key  VARCHAR(256),
    input_context       JSONB NOT NULL DEFAULT '{}'::jsonb,
    current_context     JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_context      JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_by          VARCHAR(128),
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_status ON workflow_pipeline_run(run_status);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_tenant_status ON workflow_pipeline_run(tenant_id, run_status);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_workflow_version ON workflow_pipeline_run(workflow_version_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_workflow_key ON workflow_pipeline_run(workflow_key);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_created_at ON workflow_pipeline_run(created_at);

CREATE TABLE IF NOT EXISTS workflow_pipeline_run_step (
    step_run_id         BIGSERIAL PRIMARY KEY,
    run_id              BIGINT NOT NULL REFERENCES workflow_pipeline_run(run_id) ON DELETE CASCADE,
    step_key            VARCHAR(256) NOT NULL,
    step_order          INTEGER NOT NULL,
    step_status         VARCHAR(32) NOT NULL DEFAULT 'pending'
                        CHECK (step_status IN ('pending','running','completed','paused','failed','skipped')),
    pause_reason        VARCHAR(64),
    interaction_prompt  TEXT,
    missing_data_desc   TEXT,
    required_doc_types  JSONB NOT NULL DEFAULT '[]'::jsonb,
    input_snapshot      JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_snapshot     JSONB NOT NULL DEFAULT '{}'::jsonb,
    user_response       JSONB NOT NULL DEFAULT '{}'::jsonb,
    doc_refs            JSONB NOT NULL DEFAULT '[]'::jsonb,
    error_message       TEXT,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, step_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_step_run_id ON workflow_pipeline_run_step(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_step_status ON workflow_pipeline_run_step(step_status);
CREATE INDEX IF NOT EXISTS idx_pipeline_step_order ON workflow_pipeline_run_step(run_id, step_order);
"""


create_workflow_pipeline_run_document_links_table = """
CREATE TABLE IF NOT EXISTS workflow_pipeline_run_document_link (
    link_id              BIGSERIAL PRIMARY KEY,
    run_id               BIGINT NOT NULL REFERENCES workflow_pipeline_run(run_id) ON DELETE CASCADE,
    workflow_version_id  BIGINT REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE SET NULL,
    workflow_key         VARCHAR(256),
    document_id          BIGINT NOT NULL,
    source               VARCHAR(32) NOT NULL DEFAULT 'system'
                         CHECK (source IN ('input_context','resume','step_response','system')),
    step_key             VARCHAR(256) NOT NULL DEFAULT '',
    linked_by            VARCHAR(128),
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, document_id, source, step_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_doc_link_run_id ON workflow_pipeline_run_document_link(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_doc_link_doc_id ON workflow_pipeline_run_document_link(document_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_doc_link_workflow_key ON workflow_pipeline_run_document_link(workflow_key);
CREATE INDEX IF NOT EXISTS idx_pipeline_doc_link_source ON workflow_pipeline_run_document_link(source);
CREATE INDEX IF NOT EXISTS idx_pipeline_doc_link_metadata_gin ON workflow_pipeline_run_document_link USING GIN(metadata);
"""


create_workflow_mutation_journal_table = """
CREATE TABLE IF NOT EXISTS workflow_mutation_journal (
    journal_id            BIGSERIAL PRIMARY KEY,
    run_id                BIGINT REFERENCES workflow_pipeline_run(run_id) ON DELETE CASCADE,
    workflow_version_id   BIGINT REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE SET NULL,
    workflow_key          VARCHAR(256),
    step_key              VARCHAR(256),
    step_order            INTEGER,
    operation_kind        VARCHAR(64) NOT NULL,
    target_store          VARCHAR(32) NOT NULL DEFAULT 'sql',
    target_entity         VARCHAR(128) NOT NULL DEFAULT 'workflow_context',
    mutation_key          VARCHAR(256),
    business_key          VARCHAR(256),
    business_key_type     VARCHAR(64),
    before_state          JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_state           JSONB NOT NULL DEFAULT '{}'::jsonb,
    inverse_action        JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    status                VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    undone_at             TIMESTAMPTZ,
    undone_by             VARCHAR(128),
    undo_error            TEXT
);

CREATE INDEX IF NOT EXISTS idx_workflow_mutation_run_id ON workflow_mutation_journal(run_id);
CREATE INDEX IF NOT EXISTS idx_workflow_mutation_step ON workflow_mutation_journal(run_id, step_order, step_key);
CREATE INDEX IF NOT EXISTS idx_workflow_mutation_status ON workflow_mutation_journal(status);
CREATE INDEX IF NOT EXISTS idx_workflow_mutation_applied_at ON workflow_mutation_journal(applied_at DESC);
CREATE INDEX IF NOT EXISTS idx_workflow_mutation_business_key ON workflow_mutation_journal(business_key);
CREATE INDEX IF NOT EXISTS idx_workflow_mutation_target_entity ON workflow_mutation_journal(target_entity);
"""


create_action_resolution_plan_table = """
CREATE TABLE IF NOT EXISTS action_resolution_plan (
    plan_id                UUID PRIMARY KEY,
    request_text           TEXT NOT NULL,
    action_object          JSONB NOT NULL DEFAULT '{}'::jsonb,
    workflow_snapshot      JSONB NOT NULL DEFAULT '{}'::jsonb,
    execution_plan         JSONB NOT NULL DEFAULT '[]'::jsonb,
    parameters             JSONB NOT NULL DEFAULT '{}'::jsonb,
    missing_parameters     JSONB NOT NULL DEFAULT '[]'::jsonb,
    status                 VARCHAR(32) NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending','awaiting_input','confirmed','executing','completed','rejected','expired')),
    requested_by           VARCHAR(128) NOT NULL DEFAULT 'api:user',
    expires_at             TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '30 minutes'),
    confirmed_at           TIMESTAMPTZ,
    completed_at           TIMESTAMPTZ,
    result                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_action_resolution_plan_status ON action_resolution_plan(status);
CREATE INDEX IF NOT EXISTS idx_action_resolution_plan_expires ON action_resolution_plan(expires_at);
CREATE INDEX IF NOT EXISTS idx_action_resolution_plan_created ON action_resolution_plan(created_at DESC);
"""


create_business_rule_workflow_links_table = """
CREATE TABLE IF NOT EXISTS business_rule_workflow_links (
    link_id               BIGSERIAL PRIMARY KEY,
    rule_id               BIGINT NOT NULL REFERENCES business_rules(rule_id) ON DELETE CASCADE,
    workflow_id           BIGINT NOT NULL REFERENCES solf_workflow_registry(workflow_id) ON DELETE CASCADE,
    workflow_version_id   BIGINT REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE CASCADE,
    link_type             VARCHAR(32) NOT NULL DEFAULT 'uses' CHECK (link_type IN ('uses','creates','extends','overrides')),
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by            VARCHAR(128),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (rule_id, workflow_id, workflow_version_id, link_type)
);

CREATE INDEX IF NOT EXISTS idx_business_rule_workflow_links_rule_id ON business_rule_workflow_links(rule_id);
CREATE INDEX IF NOT EXISTS idx_business_rule_workflow_links_workflow_id ON business_rule_workflow_links(workflow_id);
CREATE INDEX IF NOT EXISTS idx_business_rule_workflow_links_version_id ON business_rule_workflow_links(workflow_version_id);
CREATE INDEX IF NOT EXISTS idx_business_rule_workflow_links_metadata_gin ON business_rule_workflow_links USING GIN(metadata);
"""


create_workflow_resource_alias_table = """
CREATE TABLE IF NOT EXISTS workflow_resource_alias (
    alias_id               BIGSERIAL PRIMARY KEY,
    workflow_id            BIGINT NOT NULL REFERENCES solf_workflow_registry(workflow_id) ON DELETE CASCADE,
    workflow_version_id    BIGINT NOT NULL REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE CASCADE,
    resource_type          VARCHAR(32) NOT NULL CHECK (resource_type IN ('endpoint','queue','collection','purpose','scope','policy')),
    alias_name             VARCHAR(256) NOT NULL,
    canonical_name         VARCHAR(256) NOT NULL,
    metadata               JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by             VARCHAR(128),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_version_id, resource_type, alias_name)
);

CREATE INDEX IF NOT EXISTS idx_workflow_resource_alias_workflow_id ON workflow_resource_alias(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_resource_alias_version_id ON workflow_resource_alias(workflow_version_id);
CREATE INDEX IF NOT EXISTS idx_workflow_resource_alias_type ON workflow_resource_alias(resource_type);
CREATE INDEX IF NOT EXISTS idx_workflow_resource_alias_alias_name ON workflow_resource_alias(alias_name);
CREATE INDEX IF NOT EXISTS idx_workflow_resource_alias_metadata_gin ON workflow_resource_alias USING GIN(metadata);
"""


create_workflow_extension_tables = """
CREATE TABLE IF NOT EXISTS workflow_extension_pack (
    extension_pack_id      BIGSERIAL PRIMARY KEY,
    extension_key          VARCHAR(256) NOT NULL UNIQUE,
    extension_name         VARCHAR(256) NOT NULL,
    description            TEXT NOT NULL DEFAULT '',
    scope_json             JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata               JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active              BOOLEAN NOT NULL DEFAULT TRUE,
    created_by             VARCHAR(128),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflow_extension_pack_active ON workflow_extension_pack(is_active);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_pack_scope_gin ON workflow_extension_pack USING GIN(scope_json);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_pack_metadata_gin ON workflow_extension_pack USING GIN(metadata);

CREATE TABLE IF NOT EXISTS workflow_extension_version (
    extension_version_id   BIGSERIAL PRIMARY KEY,
    extension_pack_id      BIGINT NOT NULL REFERENCES workflow_extension_pack(extension_pack_id) ON DELETE CASCADE,
    version_no             INTEGER NOT NULL,
    status                 VARCHAR(32) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','review','published','deprecated','archived')),
    conditions_schema_version VARCHAR(32) NOT NULL DEFAULT 'v1',
    metadata               JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active              BOOLEAN NOT NULL DEFAULT TRUE,
    created_by             VARCHAR(128),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (extension_pack_id, version_no)
);

CREATE INDEX IF NOT EXISTS idx_workflow_extension_version_pack_id ON workflow_extension_version(extension_pack_id);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_version_status ON workflow_extension_version(status);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_version_active ON workflow_extension_version(is_active);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_version_metadata_gin ON workflow_extension_version USING GIN(metadata);

CREATE TABLE IF NOT EXISTS workflow_extension_rule (
    extension_rule_id      BIGSERIAL PRIMARY KEY,
    extension_version_id   BIGINT NOT NULL REFERENCES workflow_extension_version(extension_version_id) ON DELETE CASCADE,
    rule_key               VARCHAR(256) NOT NULL,
    hook_point             VARCHAR(32) NOT NULL CHECK (hook_point IN ('on_run_start','before_step','after_step','on_pause','on_failure','on_complete')),
    target_step_key        VARCHAR(256),
    condition_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    precedence             INTEGER NOT NULL DEFAULT 100,
    conflict_policy        VARCHAR(16) NOT NULL DEFAULT 'skip' CHECK (conflict_policy IN ('skip','replace','merge','fail')),
    inserted_steps_json    JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata               JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active              BOOLEAN NOT NULL DEFAULT TRUE,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (extension_version_id, rule_key)
);

CREATE INDEX IF NOT EXISTS idx_workflow_extension_rule_version_id ON workflow_extension_rule(extension_version_id);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_rule_hook_point ON workflow_extension_rule(hook_point);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_rule_condition_gin ON workflow_extension_rule USING GIN(condition_json);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_rule_steps_gin ON workflow_extension_rule USING GIN(inserted_steps_json);

CREATE TABLE IF NOT EXISTS workflow_extension_attachment (
    attachment_id          BIGSERIAL PRIMARY KEY,
    workflow_key           VARCHAR(256) NOT NULL,
    workflow_version_id    BIGINT REFERENCES solf_workflow_versions(workflow_version_id) ON DELETE CASCADE,
    extension_pack_id      BIGINT NOT NULL REFERENCES workflow_extension_pack(extension_pack_id) ON DELETE CASCADE,
    extension_version_id   BIGINT REFERENCES workflow_extension_version(extension_version_id) ON DELETE CASCADE,
    effective_from         DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_until        DATE,
    priority               INTEGER NOT NULL DEFAULT 100,
    tenant_scope           VARCHAR(128),
    metadata               JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active              BOOLEAN NOT NULL DEFAULT TRUE,
    created_by             VARCHAR(128),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflow_extension_attachment_workflow_key ON workflow_extension_attachment(workflow_key);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_attachment_workflow_version_id ON workflow_extension_attachment(workflow_version_id);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_attachment_pack_id ON workflow_extension_attachment(extension_pack_id);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_attachment_version_id ON workflow_extension_attachment(extension_version_id);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_attachment_tenant_scope ON workflow_extension_attachment(tenant_scope);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_attachment_active ON workflow_extension_attachment(is_active);
CREATE INDEX IF NOT EXISTS idx_workflow_extension_attachment_metadata_gin ON workflow_extension_attachment USING GIN(metadata);
"""


create_document_removal_log_table = """
CREATE TABLE IF NOT EXISTS document_removal_log (
    removal_id            BIGSERIAL PRIMARY KEY,
    doc_id                BIGINT NOT NULL,
    removed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    removed_by            VARCHAR(128),
    reason                TEXT,
    document_row          JSONB NOT NULL,
    part_rows             JSONB NOT NULL DEFAULT '[]'::jsonb,
    table_cell_rows       JSONB NOT NULL DEFAULT '[]'::jsonb,
    restored_at           TIMESTAMPTZ,
    restored_by           VARCHAR(128),
    restore_status        VARCHAR(32) NOT NULL DEFAULT 'pending',
    restore_error         TEXT,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_document_removal_log_doc_id ON document_removal_log(doc_id);
CREATE INDEX IF NOT EXISTS idx_document_removal_log_removed_at ON document_removal_log(removed_at DESC);
CREATE INDEX IF NOT EXISTS idx_document_removal_log_restore_status ON document_removal_log(restore_status);
"""


create_extensions = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
"""


create_security_audit_event_table = """
CREATE TABLE IF NOT EXISTS security_audit_event (
    event_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID,
    subject VARCHAR(512) NOT NULL,
    request_id VARCHAR(256) NOT NULL,
    endpoint VARCHAR(512) NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    authorization_source VARCHAR(64) NOT NULL,
    status_code INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_security_audit_event_tenant ON security_audit_event(tenant_id);
CREATE INDEX IF NOT EXISTS idx_security_audit_event_subject ON security_audit_event(subject);
CREATE INDEX IF NOT EXISTS idx_security_audit_event_created ON security_audit_event(created_at DESC);
"""


create_extension_package_tables = """
CREATE TABLE IF NOT EXISTS ikos_extension_package (
    package_id BIGSERIAL PRIMARY KEY,
    extension_key VARCHAR(128) NOT NULL UNIQUE,
    display_name VARCHAR(256) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_by VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ikos_extension_package_version (
    package_version_id BIGSERIAL PRIMARY KEY,
    package_id BIGINT NOT NULL REFERENCES ikos_extension_package(package_id) ON DELETE RESTRICT,
    version VARCHAR(64) NOT NULL,
    manifest JSONB NOT NULL,
    sha256 CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'validated'
        CHECK (status IN ('validated','deprecated')),
    created_by VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (package_id, version)
);
CREATE INDEX IF NOT EXISTS idx_ikos_extension_version_created
    ON ikos_extension_package_version(package_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ikos_extension_deployment (
    deployment_id BIGSERIAL PRIMARY KEY,
    package_version_id BIGINT NOT NULL REFERENCES ikos_extension_package_version(package_version_id) ON DELETE RESTRICT,
    target_scope VARCHAR(16) NOT NULL CHECK (target_scope IN ('tenant','global')),
    target_tenant_id UUID NOT NULL REFERENCES tenant(tenant_id) ON DELETE RESTRICT,
    package_sha256 CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('applied','failed')),
    change_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    deployed_by VARCHAR(256) NOT NULL,
    deployed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((target_scope = 'tenant' AND target_tenant_id <> '00000000-0000-0000-0000-000000000001'::uuid)
        OR (target_scope = 'global' AND target_tenant_id = '00000000-0000-0000-0000-000000000001'::uuid)),
    UNIQUE (package_version_id, target_scope, target_tenant_id)
);
CREATE INDEX IF NOT EXISTS idx_ikos_extension_deployment_target
    ON ikos_extension_deployment(target_scope, target_tenant_id, deployed_at DESC);

DO $ikos_extension_rls$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'ikos_extension_package', 'ikos_extension_package_version', 'ikos_extension_deployment'
    ] LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE format('DROP POLICY IF EXISTS ikos_platform_extension_admin ON %I', table_name);
        EXECUTE format(
            'CREATE POLICY ikos_platform_extension_admin ON %I USING (''platform.configuration.publish'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '',''))) WITH CHECK (''platform.configuration.publish'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '','')))',
            table_name
        );
    END LOOP;
END
$ikos_extension_rls$;
"""


# Tenant-owned executable configuration and operational action records. Shared
# kernel definitions (for example query_pattern_registry and RBAC role catalogs)
# are intentionally excluded from this list.
TENANT_SCOPED_RUNTIME_TABLES = (
    "business_rules",
    "solf_clauses",
    "solf_workflow_registry",
    "solf_workflow_versions",
    "solf_workflow_steps",
    "business_rule_workflow_links",
    "workflow_resource_alias",
    "workflow_extension_pack",
    "workflow_extension_version",
    "workflow_extension_rule",
    "workflow_extension_attachment",
    "workflow_generated_python_script",
    "workflow_generated_python_script_audit",
    "workflow_pipeline_run_step",
    "workflow_pipeline_run_document_link",
    "workflow_mutation_journal",
    "workflow_pipeline_run",
    "action_resolution_plan",
    "workflow_action_log",
    "interaction_clarification_thread",
    "interaction_clarification_turn",
)

SHARED_CONFIGURATION_TABLES = (
    "business_rules",
    "solf_clauses",
    "solf_workflow_registry",
    "solf_workflow_versions",
    "solf_workflow_steps",
    "business_rule_workflow_links",
    "workflow_resource_alias",
    "workflow_extension_pack",
    "workflow_extension_version",
    "workflow_extension_rule",
    "workflow_extension_attachment",
    "workflow_generated_python_script",
)


def ensure_tenant_isolation_for_tables(
    connection: psycopg2.extensions.connection,
    table_names: tuple[str, ...] | list[str] = TENANT_SCOPED_RUNTIME_TABLES,
) -> None:
    """Add tenant ownership, contextual defaults, indexes, and fail-closed RLS.

    Existing unscoped rows are assigned to the legacy tenant during migration;
    they are not silently shared with newly provisioned tenants.
    """
    allowed_tables = set(TENANT_SCOPED_RUNTIME_TABLES)
    names = tuple(name for name in table_names if name in allowed_tables)
    with connection.cursor() as cursor:
        for table_name in names:
            cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
            if cursor.fetchone()[0] is None:
                continue
            cursor.execute(
                f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'"
            )
            cursor.execute(
                f"ALTER TABLE IF EXISTS {table_name} ALTER COLUMN tenant_id SET DEFAULT COALESCE(NULLIF(current_setting('ikos.tenant_id', true), '')::uuid, '00000000-0000-0000-0000-000000000001'::uuid)"
            )
            if table_name in SHARED_CONFIGURATION_TABLES:
                cursor.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS configuration_scope VARCHAR(16) NOT NULL DEFAULT 'tenant'"
                )
                cursor.execute(
                    f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS ck_{table_name}_configuration_scope"
                )
                cursor.execute(
                    f"ALTER TABLE {table_name} ADD CONSTRAINT ck_{table_name}_configuration_scope CHECK (configuration_scope IN ('tenant', 'global'))"
                )

        parent_backfills = (
            ("solf_workflow_versions", "solf_workflow_registry", "workflow_id"),
            ("solf_workflow_steps", "solf_workflow_versions", "workflow_version_id"),
            ("workflow_resource_alias", "solf_workflow_versions", "workflow_version_id"),
            ("workflow_extension_version", "workflow_extension_pack", "extension_pack_id"),
            ("workflow_extension_rule", "workflow_extension_version", "extension_version_id"),
            ("workflow_extension_attachment", "workflow_extension_pack", "extension_pack_id"),
            ("workflow_generated_python_script_audit", "workflow_generated_python_script", "script_id"),
            ("workflow_pipeline_run_step", "workflow_pipeline_run", "run_id"),
            ("workflow_pipeline_run_document_link", "workflow_pipeline_run", "run_id"),
            ("workflow_mutation_journal", "workflow_pipeline_run", "run_id"),
            ("interaction_clarification_turn", "interaction_clarification_thread", "thread_id"),
        )
        for child_table, parent_table, join_key in parent_backfills:
            if child_table not in names or parent_table not in names:
                continue
            cursor.execute(
                "SELECT to_regclass(%s), to_regclass(%s)",
                (f"public.{child_table}", f"public.{parent_table}"),
            )
            child_regclass, parent_regclass = cursor.fetchone() or (None, None)
            if child_regclass is None or parent_regclass is None:
                continue
            cursor.execute(
                f"UPDATE {child_table} child SET tenant_id = parent.tenant_id FROM {parent_table} parent WHERE child.{join_key} = parent.{join_key} AND child.tenant_id IS DISTINCT FROM parent.tenant_id"
            )

        scoped_unique_indexes = {
            "business_rules": (
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_business_rules_scope_name ON business_rules(tenant_id, configuration_scope, rule_name)",
            ),
            "solf_clauses": (
                "ALTER TABLE solf_clauses DROP CONSTRAINT IF EXISTS solf_clauses_clause_name_entity_class_key",
                "DROP INDEX IF EXISTS uq_solf_clauses_tenant_name",
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_clauses_scope_name ON solf_clauses(tenant_id, configuration_scope, clause_name, (COALESCE(entity_class, '')))" ,
            ),
            "solf_workflow_registry": (
                "ALTER TABLE solf_workflow_registry DROP CONSTRAINT IF EXISTS solf_workflow_registry_workflow_key_key",
                "DROP INDEX IF EXISTS uq_solf_workflow_registry_tenant_key",
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_solf_workflow_registry_scope_key ON solf_workflow_registry(tenant_id, configuration_scope, workflow_key)",
            ),
            "workflow_extension_pack": (
                "ALTER TABLE workflow_extension_pack DROP CONSTRAINT IF EXISTS workflow_extension_pack_extension_key_key",
                "DROP INDEX IF EXISTS uq_workflow_extension_pack_tenant_key",
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_extension_pack_scope_key ON workflow_extension_pack(tenant_id, configuration_scope, extension_key)",
            ),
            "workflow_generated_python_script": (
                "ALTER TABLE workflow_generated_python_script DROP CONSTRAINT IF EXISTS workflow_generated_python_script_script_key_key",
                "DROP INDEX IF EXISTS uq_workflow_generated_script_tenant_key",
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_generated_script_scope_key ON workflow_generated_python_script(tenant_id, configuration_scope, script_key)",
            ),
        }
        for table_name in names:
            cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
            if cursor.fetchone()[0] is None:
                continue
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_tenant_id ON {table_name}(tenant_id)"
            )
            cursor.execute(f"ALTER TABLE IF EXISTS {table_name} ENABLE ROW LEVEL SECURITY")
            cursor.execute(f"ALTER TABLE IF EXISTS {table_name} FORCE ROW LEVEL SECURITY")
            cursor.execute(f"DROP POLICY IF EXISTS ikos_tenant_isolation ON {table_name}")
            tenant_match = "tenant_id = COALESCE(NULLIF(current_setting('ikos.tenant_id', true), '')::uuid, '00000000-0000-0000-0000-000000000001'::uuid)"
            if table_name in SHARED_CONFIGURATION_TABLES:
                cursor.execute(
                    f"""CREATE POLICY ikos_tenant_isolation ON {table_name}
                        USING (({tenant_match} AND configuration_scope = 'tenant') OR configuration_scope = 'global')
                        WITH CHECK ({tenant_match} AND (
                                (configuration_scope = 'tenant' AND 'tenant.configuration.manage' = ANY(string_to_array(current_setting('ikos.permissions', true), ',')))
                                OR (configuration_scope = 'global' AND 'platform.configuration.publish' = ANY(string_to_array(current_setting('ikos.permissions', true), ',')))
                            ))"""
                )
            else:
                cursor.execute(
                    f"""CREATE POLICY ikos_tenant_isolation ON {table_name}
                        USING ({tenant_match})
                        WITH CHECK ({tenant_match})"""
                )
            for statement in scoped_unique_indexes.get(table_name, ()):
                cursor.execute(statement)

        tenant_key_indexes = {
            "business_rules": ("rule_id",),
            "solf_clauses": ("clause_id",),
            "solf_workflow_registry": ("workflow_id",),
            "solf_workflow_versions": ("workflow_version_id",),
            "workflow_extension_pack": ("extension_pack_id",),
            "workflow_extension_version": ("extension_version_id",),
            "workflow_generated_python_script": ("script_id",),
            "workflow_pipeline_run": ("run_id",),
        }
        for table_name, id_columns in tenant_key_indexes.items():
            if table_name not in names:
                continue
            cursor.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{table_name}_tenant_id ON {table_name}(tenant_id, {', '.join(id_columns)})"
            )

        cursor.execute(
            """DO $$ BEGIN
            IF to_regclass('public.solf_workflow_versions') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_solf_workflow_versions_tenant_workflow') THEN
                ALTER TABLE solf_workflow_versions ADD CONSTRAINT fk_solf_workflow_versions_tenant_workflow
                    FOREIGN KEY (tenant_id, workflow_id) REFERENCES solf_workflow_registry(tenant_id, workflow_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.solf_workflow_versions') IS NOT NULL
               AND to_regclass('public.business_rules') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_solf_workflow_versions_tenant_rule') THEN
                ALTER TABLE solf_workflow_versions ADD CONSTRAINT fk_solf_workflow_versions_tenant_rule
                    FOREIGN KEY (tenant_id, rule_id) REFERENCES business_rules(tenant_id, rule_id);
            END IF;
            IF to_regclass('public.solf_workflow_steps') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_solf_workflow_steps_tenant_version') THEN
                ALTER TABLE solf_workflow_steps ADD CONSTRAINT fk_solf_workflow_steps_tenant_version
                    FOREIGN KEY (tenant_id, workflow_version_id) REFERENCES solf_workflow_versions(tenant_id, workflow_version_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.solf_workflow_steps') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_solf_workflow_steps_tenant_clause') THEN
                ALTER TABLE solf_workflow_steps ADD CONSTRAINT fk_solf_workflow_steps_tenant_clause
                    FOREIGN KEY (tenant_id, clause_id) REFERENCES solf_clauses(tenant_id, clause_id);
            END IF;
            IF to_regclass('public.business_rule_workflow_links') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_rule_workflow_links_tenant_rule') THEN
                ALTER TABLE business_rule_workflow_links ADD CONSTRAINT fk_rule_workflow_links_tenant_rule
                    FOREIGN KEY (tenant_id, rule_id) REFERENCES business_rules(tenant_id, rule_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.business_rule_workflow_links') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_rule_workflow_links_tenant_workflow') THEN
                ALTER TABLE business_rule_workflow_links ADD CONSTRAINT fk_rule_workflow_links_tenant_workflow
                    FOREIGN KEY (tenant_id, workflow_id) REFERENCES solf_workflow_registry(tenant_id, workflow_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.business_rule_workflow_links') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_rule_workflow_links_tenant_version') THEN
                ALTER TABLE business_rule_workflow_links ADD CONSTRAINT fk_rule_workflow_links_tenant_version
                    FOREIGN KEY (tenant_id, workflow_version_id) REFERENCES solf_workflow_versions(tenant_id, workflow_version_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.workflow_resource_alias') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_resource_alias_tenant_workflow') THEN
                ALTER TABLE workflow_resource_alias ADD CONSTRAINT fk_workflow_resource_alias_tenant_workflow
                    FOREIGN KEY (tenant_id, workflow_id) REFERENCES solf_workflow_registry(tenant_id, workflow_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.workflow_resource_alias') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_resource_alias_tenant_version') THEN
                ALTER TABLE workflow_resource_alias ADD CONSTRAINT fk_workflow_resource_alias_tenant_version
                    FOREIGN KEY (tenant_id, workflow_version_id) REFERENCES solf_workflow_versions(tenant_id, workflow_version_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.workflow_extension_version') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_extension_version_tenant_pack') THEN
                ALTER TABLE workflow_extension_version ADD CONSTRAINT fk_workflow_extension_version_tenant_pack
                    FOREIGN KEY (tenant_id, extension_pack_id) REFERENCES workflow_extension_pack(tenant_id, extension_pack_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.workflow_extension_rule') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_extension_rule_tenant_version') THEN
                ALTER TABLE workflow_extension_rule ADD CONSTRAINT fk_workflow_extension_rule_tenant_version
                    FOREIGN KEY (tenant_id, extension_version_id) REFERENCES workflow_extension_version(tenant_id, extension_version_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.workflow_extension_attachment') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_extension_attachment_tenant_pack') THEN
                ALTER TABLE workflow_extension_attachment ADD CONSTRAINT fk_workflow_extension_attachment_tenant_pack
                    FOREIGN KEY (tenant_id, extension_pack_id) REFERENCES workflow_extension_pack(tenant_id, extension_pack_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.workflow_extension_attachment') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_extension_attachment_tenant_extension_version') THEN
                ALTER TABLE workflow_extension_attachment ADD CONSTRAINT fk_workflow_extension_attachment_tenant_extension_version
                    FOREIGN KEY (tenant_id, extension_version_id) REFERENCES workflow_extension_version(tenant_id, extension_version_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.workflow_extension_attachment') IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_extension_attachment_tenant_workflow_version') THEN
                ALTER TABLE workflow_extension_attachment ADD CONSTRAINT fk_workflow_extension_attachment_tenant_workflow_version
                    FOREIGN KEY (tenant_id, workflow_version_id) REFERENCES solf_workflow_versions(tenant_id, workflow_version_id) ON DELETE CASCADE;
            END IF;
            IF to_regclass('public.workflow_generated_python_script_audit') IS NOT NULL THEN
                ALTER TABLE workflow_generated_python_script_audit DROP CONSTRAINT IF EXISTS fk_generated_script_audit_tenant_script;
            END IF;
                    IF to_regclass('public.workflow_pipeline_run_step') IS NOT NULL
                       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_pipeline_step_tenant_run') THEN
                    ALTER TABLE workflow_pipeline_run_step ADD CONSTRAINT fk_workflow_pipeline_step_tenant_run
                        FOREIGN KEY (tenant_id, run_id) REFERENCES workflow_pipeline_run(tenant_id, run_id) ON DELETE CASCADE;
                    END IF;
                    IF to_regclass('public.workflow_pipeline_run_document_link') IS NOT NULL
                       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_pipeline_document_link_tenant_run') THEN
                    ALTER TABLE workflow_pipeline_run_document_link ADD CONSTRAINT fk_workflow_pipeline_document_link_tenant_run
                        FOREIGN KEY (tenant_id, run_id) REFERENCES workflow_pipeline_run(tenant_id, run_id) ON DELETE CASCADE;
                    END IF;
                    IF to_regclass('public.workflow_mutation_journal') IS NOT NULL
                       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_workflow_mutation_journal_tenant_run') THEN
                    ALTER TABLE workflow_mutation_journal ADD CONSTRAINT fk_workflow_mutation_journal_tenant_run
                        FOREIGN KEY (tenant_id, run_id) REFERENCES workflow_pipeline_run(tenant_id, run_id) ON DELETE CASCADE;
                    END IF;
            END $$;"""
        )


def _to_search_tokens(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip().lower() for item in value if str(item).strip()]
    return [str(value).strip().lower()] if str(value).strip() else []


def _load_object_attributes(
    connection: psycopg2.extensions.connection,
    object_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    if not object_ids:
        return {}

    sql = """
    SELECT src_id, attr_type, attr_json
    FROM attribute
    WHERE src_type = 'object'
      AND src_id = ANY(%s)
    """

    attribute_map: dict[int, list[dict[str, Any]]] = defaultdict(list)
    with connection.cursor() as cursor:
        cursor.execute(sql, (object_ids,))
        for src_id, attr_type, attr_json in cursor.fetchall():
            attribute_map[int(src_id)].append(
                {
                    "attr_type": str(attr_type).lower(),
                    "attr_json_text": str(attr_json).lower(),
                }
            )
    return dict(attribute_map)


def _attribute_match_count(
    object_attrs: list[dict[str, Any]],
    expected_attributes: dict[str, Any],
) -> int:
    if not expected_attributes:
        return 0

    score = 0
    normalized_expectations = {
        str(attr_type).lower(): _to_search_tokens(attr_value)
        for attr_type, attr_value in expected_attributes.items()
    }

    for attr_type, expected_tokens in normalized_expectations.items():
        if not expected_tokens:
            continue

        has_match = any(
            row["attr_type"] == attr_type
            and all(token in row["attr_json_text"] for token in expected_tokens)
            for row in object_attrs
        )
        if has_match:
            score += 1
    return score


def search_objects(
    connection: psycopg2.extensions.connection,
    name_query: str,
    class_name: str | None = None,
    attributes: dict[str, Any] | None = None,
    limit: int = 20,
    solf_matcher: Callable[[dict[str, Any]], bool] | None = None,
    as_of_date: date | None = None,
    include_inactive: bool = False,
    include_merged_variants: bool = True,
) -> list[dict[str, Any]]:
    """Search objects by partial name and optionally disambiguate with attributes.

    When multiple candidates are returned, attribute matching boosts and reorders results.
    Optionally pass a SOLF matcher callback for clause-based filtering.
    When include_merged_variants=True (default), also searches for merged objects that match
    the name and resolves them to their canonical forms transparently.
    """
    if not name_query.strip():
        return []

    # Build the LIKE pattern in Python to avoid %s-in-string confusion
    like_prefix = name_query + "%"
    like_contains = "%" + name_query + "%"
    name_lower = name_query.lower()
    like_prefix_lower = name_lower + "%"
    like_contains_lower = "%" + name_lower + "%"

    sql = """
    SELECT
        oi.object_id,
        oi.object_name,
        oi.canonical_full_name,
        oi.class_name,
        oi.metadata,
        oi.status,
        similarity(LOWER(COALESCE(oi.canonical_full_name, oi.object_name)), LOWER(%s)) AS sim_score,
        CASE
            WHEN LOWER(oi.object_name) = %s OR LOWER(COALESCE(oi.canonical_full_name, '')) = %s THEN 1
            WHEN LOWER(oi.object_name) LIKE %s OR LOWER(COALESCE(oi.canonical_full_name, '')) LIKE %s THEN 2
            WHEN LOWER(oi.object_name) LIKE %s OR LOWER(COALESCE(oi.canonical_full_name, '')) LIKE %s THEN 3
            ELSE 4
        END AS name_rank
    FROM object_instance oi
    WHERE (%s IS NULL OR oi.class_name = %s)
            AND (%s OR COALESCE(oi.status, 'active') = 'active')
            AND (oi.valid_from <= %s)
            AND (oi.valid_until IS NULL OR oi.valid_until > %s)
            AND (
                LOWER(oi.object_name) LIKE %s
                OR LOWER(COALESCE(oi.canonical_full_name, '')) LIKE %s
            )
        ORDER BY name_rank, sim_score DESC, oi.object_name
    LIMIT %s
    """

    with connection.cursor() as cursor:
        effective_date = as_of_date or date.today()
        cursor.execute(
            sql,
            (
                name_query,         # for similarity()
                name_lower,         # CASE exact match
                name_lower,         # CASE exact match canonical
                like_prefix_lower,  # CASE prefix match
                like_prefix_lower,  # CASE prefix match canonical
                like_contains_lower, # CASE contains match
                like_contains_lower, # CASE contains match canonical
                class_name,         # WHERE class_name check 1
                class_name,         # WHERE class_name check 2
                include_inactive,   # WHERE status filter
                effective_date,     # WHERE valid_from
                effective_date,     # WHERE valid_until
                like_contains_lower, # WHERE contains check (object_name)
                like_contains_lower, # WHERE contains check (canonical_full_name)
                limit,              # LIMIT
            ),
        )
        rows = cursor.fetchall()

    candidates = [
        {
            "object_id": row[0],
            "object_name": row[1],
            "canonical_full_name": row[2],
            "class_name": row[3],
            "metadata": row[4] or {},
            "status": row[5],
            "similarity": float(row[6]),
            "name_rank": row[7],
            "attribute_matches": 0,
            "score": 0,
        }
        for row in rows
    ]

    # Note: do NOT early-return here if candidates is empty.
    # include_merged_variants may still find and add candidates below.

    expected_attributes = attributes or {}
    if expected_attributes and len(candidates) > 1:
        object_ids = [int(candidate["object_id"]) for candidate in candidates]
        attribute_map = _load_object_attributes(connection, object_ids)
        total_expected = len([key for key in expected_attributes if str(key).strip()])

        for candidate in candidates:
            object_id = int(candidate["object_id"])
            object_attrs = attribute_map.get(object_id, [])
            match_count = _attribute_match_count(object_attrs, expected_attributes)
            candidate["attribute_matches"] = match_count
            candidate["attributes_checked"] = total_expected
            # Prioritize attribute evidence first, then name quality.
            candidate["score"] = (match_count * 100) - int(candidate["name_rank"])

        candidates.sort(
            key=lambda item: (
                -int(item["attribute_matches"]),
                int(item["name_rank"]),
                str(item["object_name"]).lower(),
            )
        )
    else:
        for candidate in candidates:
            candidate["score"] = (candidate["similarity"] * 100.0) - int(candidate["name_rank"])

        candidates.sort(
            key=lambda item: (
                -float(item["similarity"]),
                int(item["name_rank"]),
                str(item["object_name"]).lower(),
            )
        )

    if solf_matcher is not None:
        filtered_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            try:
                if solf_matcher(candidate):
                    filtered_candidates.append(candidate)
            except Exception:
                logging.exception("SOLF matcher failed for candidate %s", candidate.get("object_name"))
        candidates = filtered_candidates

    # Search for merged object variants that match the name and add them to candidates
    if include_merged_variants:
        merged_variant_sql = """
        SELECT
            oi.object_id,
            oi.object_name,
            oi.canonical_full_name,
            oi.class_name,
            oi.metadata,
            oi.status,
            similarity(LOWER(COALESCE(oi.canonical_full_name, oi.object_name)), LOWER(%s)) AS sim_score
        FROM object_instance oi
        WHERE (%s IS NULL OR oi.class_name = %s)
            AND COALESCE(oi.status, 'active') = 'merged'
        ORDER BY sim_score DESC, oi.object_name
        LIMIT %s
        """
        
        with connection.cursor() as cursor:
            cursor.execute(
                merged_variant_sql,
                (
                    name_query,  # for similarity()
                    class_name,  # WHERE class_name check 1
                    class_name,  # WHERE class_name check 2
                    limit,  # LIMIT
                ),
            )
            merged_rows = cursor.fetchall()
        
        # Filter by similarity threshold (same as in active objects search)
        for row in merged_rows:
            similarity = float(row[6])
            if similarity < 0.12:  # min_similarity threshold
                continue
            merged_variant = {
                "object_id": row[0],
                "object_name": row[1],
                "canonical_full_name": row[2],
                "class_name": row[3],
                "metadata": row[4] or {},
                "status": row[5],
                "similarity": similarity,
                "name_rank": 4,  # merged variants get lower rank
                "attribute_matches": 0,
                "score": 0,
            }
            # Mark that this will be resolved
            merged_variant["_is_merged_variant"] = True
            candidates.append(merged_variant)

    # Resolve merged objects to their canonical forms for transparency
    resolved_candidates: dict[int, dict[str, Any]] = {}
    for candidate in candidates:
        original_object_id = int(candidate["object_id"])
        canonical_object_id = resolve_merged_object_to_canonical(connection, original_object_id)
        
        if canonical_object_id != original_object_id:
            # Object was merged, fetch the canonical object details
            canonical_row = get_object_instance_by_id(
                connection,
                canonical_object_id,
                as_of_date=as_of_date,
                include_inactive=include_inactive,
            )
            if canonical_row:
                # Replace with canonical object, but preserve original similarity score
                resolved_candidate = {
                    "object_id": canonical_row["object_id"],
                    "object_name": canonical_row["object_name"],
                    "canonical_full_name": canonical_row["canonical_full_name"],
                    "class_name": canonical_row["class_name"],
                    "metadata": canonical_row["metadata"],
                    "status": canonical_row["status"],
                    "similarity": candidate.get("similarity", 0.0),
                    "name_rank": candidate.get("name_rank", 4),
                    "attribute_matches": candidate.get("attribute_matches", 0),
                    "score": candidate.get("score", 0),
                    "resolved_from_merged": original_object_id,  # Track what we resolved from
                }
                resolved_candidates[canonical_object_id] = resolved_candidate
            else:
                # Canonical object not found, keep original
                resolved_candidates[original_object_id] = candidate
        else:
            # Not merged, keep as-is
            resolved_candidates[original_object_id] = candidate
    
    # Return deduplicated list (in case multiple merged variants resolved to same canonical)
    return list(resolved_candidates.values())


def search_documents_by_key(
    connection: psycopg2.extensions.connection,
    key_query: str,
    doc_cat: str | None = None,
    doc_type: str | None = None,
    limit: int = 20,
    as_of_date: date | None = None,
    include_inactive: bool = False,
) -> list[dict[str, Any]]:
    """Search documents by key and user metadata with weighted lexical ranking.

    Ranking preference: key exact/prefix > uploader description/tags > keyword text > trigram similarity.
    """
    if not key_query.strip():
        return []

    sql = """
    SELECT
        d.doc_id,
        d.doc_key,
        d.doc_path,
        d.doc_cat,
        d.doc_type,
        d.doc_date,
        d.identifiers_kv,
        d.metadata,
        similarity(LOWER(COALESCE(d.doc_key, '')), LOWER(%s)) AS sim_score,
        similarity(LOWER(COALESCE(d.keyword_text, '')), LOWER(%s)) AS keyword_sim_score,
        CASE
            WHEN LOWER(COALESCE(d.doc_key, '')) = LOWER(%s) THEN 1
            WHEN LOWER(COALESCE(d.doc_key, '')) LIKE LOWER(%s) || '%%' THEN 2
            WHEN LOWER(COALESCE(d.doc_key, '')) LIKE '%%' || LOWER(%s) || '%%' THEN 3
            ELSE 4
        END AS key_rank,
        (
            CASE
                WHEN LOWER(COALESCE(d.doc_key, '')) = LOWER(%s) THEN 35
                WHEN LOWER(COALESCE(d.doc_key, '')) LIKE LOWER(%s) || '%%' THEN 24
                WHEN LOWER(COALESCE(d.doc_key, '')) LIKE '%%' || LOWER(%s) || '%%' THEN 14
                ELSE 0
            END
            + CASE WHEN LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%' || LOWER(%s) || '%%' THEN 12 ELSE 0 END
            + CASE WHEN LOWER(COALESCE(d.metadata->>'user_tags', '')) LIKE '%%' || LOWER(%s) || '%%' THEN 10 ELSE 0 END
            + CASE WHEN LOWER(COALESCE(d.keyword_text, '')) LIKE '%%' || LOWER(%s) || '%%' THEN 8 ELSE 0 END
        ) AS lexical_boost
    FROM document d
    WHERE (%s IS NULL OR d.doc_cat = %s)
      AND (%s IS NULL OR d.doc_type = %s)
            AND (%s OR COALESCE(d.status, 'active') = 'active')
            AND (d.valid_from <= %s)
            AND (d.valid_until IS NULL OR d.valid_until > %s)
      AND (
            LOWER(COALESCE(d.doc_key, '')) LIKE '%%' || LOWER(%s) || '%%'
            OR LOWER(COALESCE(d.doc_key, '')) %% LOWER(%s)
            OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%' || LOWER(%s) || '%%'
            OR LOWER(COALESCE(d.metadata->>'user_tags', '')) LIKE '%%' || LOWER(%s) || '%%'
            OR LOWER(COALESCE(d.keyword_text, '')) LIKE '%%' || LOWER(%s) || '%%'
            OR LOWER(COALESCE(d.keyword_text, '')) %% LOWER(%s)
          )
    ORDER BY lexical_boost DESC, key_rank, sim_score DESC, keyword_sim_score DESC, d.doc_key
    LIMIT %s
    """

    with connection.cursor() as cursor:
        effective_date = as_of_date or date.today()
        cursor.execute(
            sql,
            (
                key_query,
                key_query,
                key_query,
                key_query,
                key_query,
                key_query,
                key_query,
                key_query,
                key_query,
                key_query,
                doc_cat,
                doc_cat,
                doc_type,
                doc_type,
                include_inactive,
                effective_date,
                effective_date,
                key_query,
                key_query,
                key_query,
                key_query,
                key_query,
                key_query,
                limit,
            ),
        )
        rows = cursor.fetchall()

    return [
        {
            "doc_id": row[0],
            "doc_key": row[1],
            "doc_path": row[2],
            "doc_cat": row[3],
            "doc_type": row[4],
            "doc_date": row[5],
            "identifiers_kv": row[6] or {},
            "metadata": row[7] or {},
            "similarity": float(row[8]),
            "keyword_similarity": float(row[9]),
            "key_rank": int(row[10]),
            "lexical_boost": int(row[11]),
            "score": int(row[11]) + (float(row[8]) * 100.0) + (float(row[9]) * 25.0) - int(row[10]),
        }
        for row in rows
    ]


def replace_document_geographic_facets(
    connection: psycopg2.extensions.connection,
    doc_id: int,
    facets: list[dict[str, Any]],
) -> int:
    """Replace extracted geographic facets for one document."""
    allowed_kinds = {"country", "region", "city", "address", "venue", "unknown"}
    allowed_roles = {"incident_location", "issuer_location", "sender_address", "recipient_address", "document_subject", "service_location", "jurisdiction", "unknown"}
    rows: list[tuple[Any, ...]] = []
    for item in facets:
        if not isinstance(item, dict):
            continue
        raw_name = str(item.get("raw_name") or item.get("name") or "").strip()
        canonical_name = str(item.get("canonical_name") or raw_name).strip()
        place_kind = str(item.get("place_kind") or item.get("kind") or "unknown").strip().lower()
        role_name = str(item.get("role_name") or item.get("role") or "document_subject").strip().lower()
        if not raw_name or not canonical_name or place_kind not in allowed_kinds or role_name not in allowed_roles:
            continue
        confidence = max(0.0, min(1.0, float(item.get("confidence") or 0.5)))
        rows.append((int(doc_id), raw_name, canonical_name, place_kind, str(item.get("country_code") or "").strip().upper() or None, role_name, confidence, Json(item.get("metadata") if isinstance(item.get("metadata"), dict) else {})))

    with connection.cursor() as cursor:
        cursor.execute(create_document_geographic_facet_table)
        cursor.execute("DELETE FROM document_geographic_facet WHERE doc_id = %s", (int(doc_id),))
        if rows:
            cursor.executemany(
                """
                INSERT INTO document_geographic_facet (
                    doc_id, raw_name, canonical_name, place_kind, country_code, role_name, confidence, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_id, canonical_name, place_kind, country_code, role_name)
                DO UPDATE SET raw_name = EXCLUDED.raw_name, confidence = EXCLUDED.confidence,
                              metadata = COALESCE(document_geographic_facet.metadata, '{}'::jsonb) || EXCLUDED.metadata
                """,
                rows,
            )
    connection.commit()
    return len(rows)


def search_documents_by_geography(
    connection: psycopg2.extensions.connection,
    location: str,
    country_code: str | None = None,
    role_name: str | None = None,
    doc_type: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    normalized_location = str(location or "").strip()
    normalized_country = str(country_code or "").strip().upper()
    if not normalized_location and not normalized_country:
        return []
    conditions = ["d.status = 'active'"]
    params: list[Any] = []
    if normalized_location:
        conditions.append("(f.canonical_name ILIKE %s OR f.raw_name ILIKE %s)")
        params.extend([normalized_location, normalized_location])
    if normalized_country:
        conditions.append("f.country_code = %s")
        params.append(normalized_country)
    if str(role_name or "").strip():
        conditions.append("f.role_name = %s")
        params.append(str(role_name).strip().lower())
    if str(doc_type or "").strip():
        conditions.append("d.doc_type = %s")
        params.append(str(doc_type).strip().lower())
    params.append(max(1, min(int(limit), 200)))
    with connection.cursor() as cursor:
        cursor.execute(create_document_geographic_facet_table)
        cursor.execute(
            f"""
            SELECT d.doc_id, d.doc_key, d.doc_name, d.doc_type, d.doc_date,
                   f.raw_name, f.canonical_name, f.place_kind, f.country_code, f.role_name, f.confidence
            FROM document_geographic_facet f
            JOIN document d ON d.doc_id = f.doc_id
            WHERE {' AND '.join(conditions)}
            ORDER BY d.doc_date DESC NULLS LAST, f.confidence DESC, d.doc_id DESC
            LIMIT %s
            """,
            tuple(params),
        )
        rows = cursor.fetchall() or []
    return [
        {"doc_id": int(row[0]), "doc_key": row[1], "doc_name": row[2], "doc_type": row[3], "doc_date": str(row[4]) if row[4] else None,
         "raw_name": row[5], "canonical_name": row[6], "place_kind": row[7], "country_code": row[8], "role_name": row[9], "confidence": float(row[10])}
        for row in rows
    ]


def replace_document_action_items(
    connection: psycopg2.extensions.connection,
    doc_id: int,
    action_items: list[dict[str, Any]],
) -> int:
    """Replace extracted action items for one document without creating operational tasks."""
    allowed_priorities = {"low", "medium", "high", "urgent"}
    allowed_statuses = {"open", "in_progress", "completed", "cancelled", "superseded"}
    rows: list[tuple[Any, ...]] = []
    for item in action_items:
        if not isinstance(item, dict):
            continue
        instruction = str(item.get("instruction") or item.get("action") or "").strip()
        action_type = str(item.get("action_type") or "general_action").strip().lower() or "general_action"
        priority = str(item.get("priority") or "medium").strip().lower()
        status = str(item.get("status") or "open").strip().lower()
        if not instruction or priority not in allowed_priorities or status not in allowed_statuses:
            continue
        confidence = max(0.0, min(1.0, float(item.get("confidence") or 0.5)))
        rows.append((
            int(doc_id), action_type, instruction,
            str(item.get("subject_name") or item.get("subject") or "").strip() or None,
            str(item.get("assignee_name") or item.get("assignee") or "").strip() or None,
            item.get("due_date") or None, priority, status, confidence,
            Json(item.get("metadata") if isinstance(item.get("metadata"), dict) else {}),
        ))
    with connection.cursor() as cursor:
        cursor.execute(create_document_action_item_table)
        cursor.execute("DELETE FROM document_action_item WHERE doc_id = %s", (int(doc_id),))
        if rows:
            cursor.executemany(
                """
                INSERT INTO document_action_item (
                    doc_id, action_type, instruction, subject_name, assignee_name,
                    due_date, priority, status, confidence, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
    connection.commit()
    return len(rows)


def search_document_action_items(
    connection: psycopg2.extensions.connection,
    subject_query: str | None = None,
    document_query: str | None = None,
    status: str | None = "open",
    limit: int = 50,
) -> list[dict[str, Any]]:
    conditions = ["d.status = 'active'"]
    params: list[Any] = []
    subject = str(subject_query or "").strip()
    document = str(document_query or "").strip()
    if subject:
        conditions.append("(a.subject_name ILIKE %s OR a.instruction ILIKE %s)")
        params.extend([f"%{subject}%", f"%{subject}%"])
    if document:
        conditions.append("(d.doc_key ILIKE %s OR d.doc_name ILIKE %s OR d.doc_type ILIKE %s OR d.doc_desc ILIKE %s)")
        params.extend([f"%{document}%"] * 4)
    if str(status or "").strip():
        conditions.append("a.status = %s")
        params.append(str(status).strip().lower())
    params.append(max(1, min(int(limit), 200)))
    with connection.cursor() as cursor:
        cursor.execute(create_document_action_item_table)
        cursor.execute(
            f"""
            SELECT a.action_item_id, a.action_type, a.instruction, a.subject_name, a.assignee_name,
                   a.due_date, a.priority, a.status, a.confidence, a.metadata,
                   d.doc_id, d.doc_key, d.doc_name, d.doc_type, d.doc_date
            FROM document_action_item a
            JOIN document d ON d.doc_id = a.doc_id
            WHERE {' AND '.join(conditions)}
            ORDER BY a.due_date ASC NULLS LAST,
                     CASE a.priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
                     d.doc_date DESC NULLS LAST, a.action_item_id DESC
            LIMIT %s
            """,
            tuple(params),
        )
        rows = cursor.fetchall() or []
    return [
        {"action_item_id": int(row[0]), "action_type": row[1], "instruction": row[2], "subject_name": row[3],
         "assignee_name": row[4], "due_date": str(row[5]) if row[5] else None, "priority": row[6], "status": row[7],
         "confidence": float(row[8]), "metadata": row[9] if isinstance(row[9], dict) else {}, "doc_id": int(row[10]),
         "doc_key": row[11], "doc_name": row[12], "doc_type": row[13], "doc_date": str(row[14]) if row[14] else None}
        for row in rows
    ]


def get_document_brief_by_id(
    connection: psycopg2.extensions.connection,
    doc_id: int,
) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT doc_id, doc_key, doc_cat, doc_type, doc_date, doc_desc, doc_theme,
                   keyword_text, metadata, status, valid_from, valid_until, entry_date
            FROM document
            WHERE doc_id = %s
            """,
            (int(doc_id),),
        )
        row = cursor.fetchone()

    if row is None:
        return None

    return {
        "doc_id": int(row[0]),
        "doc_key": row[1],
        "doc_cat": row[2],
        "doc_type": row[3],
        "doc_date": row[4],
        "doc_desc": row[5],
        "doc_theme": row[6],
        "keyword_text": row[7],
        "metadata": row[8] or {},
        "status": row[9],
        "valid_from": row[10],
        "valid_until": row[11],
        "entry_date": row[12],
    }


def upsert_document_table_cells(
    connection: psycopg2.extensions.connection,
    doc_id: int,
    tables: list[dict[str, Any]] | None,
    source_kind: str = "document",
    replace_existing: bool = True,
) -> int:
    context = get_security_context(required=False)
    tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
    with connection.cursor() as cursor:
        cursor.execute(create_document_table_cell_table)
    normalized_tables = [table for table in (tables or []) if isinstance(table, dict)]
    if replace_existing:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM document_table_cell WHERE tenant_id = %s::uuid AND doc_id = %s",
                (tenant_id, int(doc_id)),
            )

    inserted = 0
    with connection.cursor() as cursor:
        for table_idx, table in enumerate(normalized_tables):
            headers = [str(value or "").strip() for value in list(table.get("headers") or [])]
            rows = table.get("rows") if isinstance(table.get("rows"), list) else []
            records = table.get("records") if isinstance(table.get("records"), list) else []

            if not rows and headers and records:
                rebuilt_rows: list[list[str]] = []
                for record in records:
                    if not isinstance(record, dict):
                        continue
                    rebuilt_rows.append([str(record.get(header) or "").strip() for header in headers])
                rows = rebuilt_rows

            if not rows:
                continue

            for row_idx, row in enumerate(rows):
                if not isinstance(row, list):
                    continue
                row_label = str(row[0] or "").strip() if row else ""
                for col_idx, value in enumerate(row):
                    cell_value = str(value or "").strip()
                    if not cell_value:
                        continue
                    column_label = str(headers[col_idx] if col_idx < len(headers) else f"col_{col_idx + 1}").strip()
                    metadata = {
                        "table_index": int(table_idx),
                        "row_index": int(row_idx),
                        "column_index": int(col_idx),
                        "source_kind": str(source_kind or "document").strip().lower() or "document",
                    }
                    cursor.execute(
                        """
                        INSERT INTO document_table_cell (
                            tenant_id, doc_id, table_index, row_index, row_label,
                            column_index, column_label, cell_value, source_kind, metadata
                        )
                        VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (tenant_id, doc_id, table_index, row_index, column_index)
                        DO UPDATE SET
                            row_label = EXCLUDED.row_label,
                            column_label = EXCLUDED.column_label,
                            cell_value = EXCLUDED.cell_value,
                            source_kind = EXCLUDED.source_kind,
                            metadata = EXCLUDED.metadata
                        """,
                        (
                            tenant_id,
                            int(doc_id),
                            int(table_idx),
                            int(row_idx),
                            row_label or None,
                            int(col_idx),
                            column_label or None,
                            cell_value,
                            metadata["source_kind"],
                            json.dumps(metadata),
                        ),
                    )
                    inserted += 1

    connection.commit()
    return inserted


def list_document_table_cells(
    connection: psycopg2.extensions.connection,
    doc_id: int,
    limit: int = 200,
) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(create_document_table_cell_table)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT cell_id, doc_id, table_index, row_index, row_label,
                   column_index, column_label, cell_value, source_kind, metadata, created_at
            FROM document_table_cell
            WHERE doc_id = %s
            ORDER BY table_index, row_index, column_index
            LIMIT %s
            """,
            (int(doc_id), int(limit)),
        )
        rows = cursor.fetchall() or []

    return [
        {
            "cell_id": int(row[0]),
            "doc_id": int(row[1]),
            "table_index": int(row[2]),
            "row_index": int(row[3]),
            "row_label": row[4],
            "column_index": int(row[5]),
            "column_label": row[6],
            "cell_value": row[7],
            "source_kind": row[8],
            "metadata": row[9] or {},
            "created_at": row[10],
        }
        for row in rows
    ]


def backfill_document_table_cells_from_document_metadata(
    connection: psycopg2.extensions.connection,
    doc_id: int,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT metadata FROM document WHERE doc_id = %s",
            (int(doc_id),),
        )
        row = cursor.fetchone()

    if not row:
        return 0
    metadata = row[0] if isinstance(row[0], dict) else {}
    structured_tables = metadata.get("structured_tables") if isinstance(metadata.get("structured_tables"), dict) else {}
    tables = structured_tables.get("tables") if isinstance(structured_tables.get("tables"), list) else []
    source_kind = str(structured_tables.get("source_kind") or "document").strip().lower() or "document"
    if not tables:
        return upsert_document_table_cells(connection, int(doc_id), [], source_kind=source_kind, replace_existing=True)
    return upsert_document_table_cells(connection, int(doc_id), tables, source_kind=source_kind, replace_existing=True)


def select_document_ids_for_table_provenance_backfill(
    connection: psycopg2.extensions.connection,
    doc_ids: list[int] | None = None,
    only_missing: bool = True,
    limit: int = 100,
) -> list[int]:
    with connection.cursor() as cursor:
        cursor.execute(create_document_table_cell_table)

    explicit_ids = [int(item) for item in (doc_ids or []) if int(item) > 0]
    if explicit_ids:
        return sorted(set(explicit_ids))

    with connection.cursor() as cursor:
        if only_missing:
            cursor.execute(
                """
                SELECT d.doc_id
                FROM document d
                WHERE CASE
                        WHEN jsonb_typeof(d.metadata->'structured_tables'->'tables') = 'array'
                        THEN jsonb_array_length(d.metadata->'structured_tables'->'tables')
                        ELSE 0
                      END > 0
                  AND NOT EXISTS (
                    SELECT 1 FROM document_table_cell c WHERE c.doc_id = d.doc_id
                  )
                ORDER BY d.doc_id DESC
                LIMIT %s
                """,
                (int(limit),),
            )
        else:
            cursor.execute(
                """
                SELECT d.doc_id
                FROM document d
                WHERE CASE
                        WHEN jsonb_typeof(d.metadata->'structured_tables'->'tables') = 'array'
                        THEN jsonb_array_length(d.metadata->'structured_tables'->'tables')
                        ELSE 0
                      END > 0
                ORDER BY d.doc_id DESC
                LIMIT %s
                """,
                (int(limit),),
            )
        rows = cursor.fetchall() or []
    return [int(row[0]) for row in rows if row and int(row[0]) > 0]


def _rows_to_dicts(cursor: psycopg2.extensions.cursor, rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    columns = [str(col[0]) for col in list(cursor.description or [])]
    out: list[dict[str, Any]] = []
    for row in rows or []:
        out.append({columns[idx]: row[idx] for idx in range(len(columns))})
    return out


def _set_serial_sequence_to_max(
    connection: psycopg2.extensions.connection,
    table_name: str,
    id_column: str,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_get_serial_sequence(%s, %s)", (table_name, id_column))
        row = cursor.fetchone()
        sequence_name = str(row[0] or "").strip() if row else ""
        if not sequence_name:
            return
        cursor.execute(
            f"SELECT setval(%s, COALESCE((SELECT MAX({id_column}) FROM {table_name}), 1), true)",
            (sequence_name,),
        )


def log_document_removal_snapshot(
    connection: psycopg2.extensions.connection,
    doc_id: int,
    removed_by: str | None = None,
    reason: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Persist a full document-removal snapshot before deletion for undo support."""
    with connection.cursor() as cursor:
        cursor.execute(create_document_removal_log_table)
        cursor.execute(
            """
            SELECT doc_id, tenant_id, doc_name, doc_key, doc_path, markdown_path, doc_cat, doc_type,
                   doc_date, doc_desc, doc_theme, keyword_text, identifiers_kv, metadata,
                   status, access_level, valid_from, valid_until, entry_date
            FROM document
            WHERE doc_id = %s
            """,
            (int(doc_id),),
        )
        doc_row = cursor.fetchone()
        if doc_row is None:
            return None
        doc_columns = [str(col[0]) for col in list(cursor.description or [])]
        document_payload = {doc_columns[idx]: doc_row[idx] for idx in range(len(doc_columns))}

        cursor.execute(
            """
            SELECT part_id, doc_id, part_key, class_id, object_id, relationship_id, metadata, entry_date
            FROM part
            WHERE doc_id = %s
            ORDER BY part_id
            """,
            (int(doc_id),),
        )
        part_rows = _rows_to_dicts(cursor, cursor.fetchall() or [])

        cursor.execute(
            """
            SELECT cell_id, doc_id, table_index, row_index, row_label, column_index,
                   column_label, cell_value, source_kind, metadata, created_at
            FROM document_table_cell
            WHERE doc_id = %s
            ORDER BY cell_id
            """,
            (int(doc_id),),
        )
        table_cell_rows = _rows_to_dicts(cursor, cursor.fetchall() or [])

        cursor.execute(
            """
            INSERT INTO document_removal_log (
                doc_id, removed_by, reason, document_row, part_rows, table_cell_rows, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING removal_id, removed_at
            """,
            (
                int(doc_id),
                str(removed_by or "").strip() or None,
                str(reason or "").strip() or None,
                Json(document_payload),
                Json(part_rows),
                Json(table_cell_rows),
                Json(metadata or {}),
            ),
        )
        inserted = cursor.fetchone()
        removal_id = int(inserted[0]) if inserted else 0
        removed_at = inserted[1] if inserted else None

    return {
        "removal_id": removal_id,
        "doc_id": int(doc_id),
        "removed_at": removed_at,
        "removed_by": str(removed_by or "").strip() or None,
        "reason": str(reason or "").strip() or None,
        "part_row_count": len(part_rows),
        "table_cell_row_count": len(table_cell_rows),
    }


def get_latest_document_removal_snapshot(
    connection: psycopg2.extensions.connection,
    doc_id: int,
) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(create_document_removal_log_table)
        cursor.execute(
            """
            SELECT removal_id, doc_id, removed_at, removed_by, reason, document_row,
                   part_rows, table_cell_rows, restored_at, restored_by, restore_status,
                   restore_error, metadata
            FROM document_removal_log
            WHERE doc_id = %s
            ORDER BY removal_id DESC
            LIMIT 1
            """,
            (int(doc_id),),
        )
        row = cursor.fetchone()

    if row is None:
        return None

    return {
        "removal_id": int(row[0]),
        "doc_id": int(row[1]),
        "removed_at": row[2],
        "removed_by": row[3],
        "reason": row[4],
        "document_row": row[5] if isinstance(row[5], dict) else {},
        "part_rows": row[6] if isinstance(row[6], list) else [],
        "table_cell_rows": row[7] if isinstance(row[7], list) else [],
        "restored_at": row[8],
        "restored_by": row[9],
        "restore_status": row[10],
        "restore_error": row[11],
        "metadata": row[12] if isinstance(row[12], dict) else {},
    }


def restore_document_from_removal_log(
    connection: psycopg2.extensions.connection,
    removal_id: int,
    restored_by: str | None = None,
) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute(create_document_removal_log_table)
        cursor.execute(
            """
            SELECT removal_id, doc_id, document_row, part_rows, table_cell_rows,
                   restore_status, restored_at
            FROM document_removal_log
            WHERE removal_id = %s
            FOR UPDATE
            """,
            (int(removal_id),),
        )
        row = cursor.fetchone()

    if row is None:
        raise ValueError(f"Removal snapshot {int(removal_id)} not found")

    snapshot_doc_id = int(row[1])
    document_row = row[2] if isinstance(row[2], dict) else {}
    part_rows = row[3] if isinstance(row[3], list) else []
    table_cell_rows = row[4] if isinstance(row[4], list) else []
    restore_status = str(row[5] or "").strip().lower()
    restored_at = row[6]

    if restore_status == "restored" and restored_at is not None:
        return {
            "restored": False,
            "reason": "already_restored",
            "removal_id": int(removal_id),
            "doc_id": snapshot_doc_id,
        }

    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM document WHERE doc_id = %s", (snapshot_doc_id,))
        if cursor.fetchone() is not None:
            raise ValueError(f"Document {snapshot_doc_id} already exists; cannot restore snapshot")

        cursor.execute(
            """
            INSERT INTO document (
                doc_id, tenant_id, doc_name, doc_key, doc_path, markdown_path, doc_cat, doc_type,
                doc_date, doc_desc, doc_theme, keyword_text, identifiers_kv, metadata,
                status, access_level, valid_from, valid_until, entry_date
            )
            VALUES (%s, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(document_row.get("doc_id") or snapshot_doc_id),
                str(document_row.get("tenant_id") or (get_security_context(required=False).tenant_id if get_security_context(required=False) else "00000000-0000-0000-0000-000000000001")),
                document_row.get("doc_name"),
                document_row.get("doc_key"),
                document_row.get("doc_path"),
                document_row.get("markdown_path"),
                document_row.get("doc_cat"),
                document_row.get("doc_type"),
                document_row.get("doc_date"),
                document_row.get("doc_desc"),
                document_row.get("doc_theme"),
                document_row.get("keyword_text"),
                Json(document_row.get("identifiers_kv") if isinstance(document_row.get("identifiers_kv"), dict) else {}),
                Json(document_row.get("metadata") if isinstance(document_row.get("metadata"), dict) else {}),
                document_row.get("status") or "active",
                document_row.get("access_level") or "tenant",
                document_row.get("valid_from"),
                document_row.get("valid_until"),
                document_row.get("entry_date"),
            ),
        )

        restored_parts = 0
        for part in part_rows:
            if not isinstance(part, dict):
                continue
            context = get_security_context(required=False)
            tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
            cursor.execute(
                """
                INSERT INTO part (
                    tenant_id, part_id, doc_id, part_key, class_id, object_id, relationship_id, metadata, entry_date
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (part_id)
                DO NOTHING
                """,
                (
                    tenant_id,
                    part.get("part_id"),
                    int(part.get("doc_id") or snapshot_doc_id),
                    part.get("part_key"),
                    part.get("class_id"),
                    part.get("object_id"),
                    part.get("relationship_id"),
                    Json(part.get("metadata") if isinstance(part.get("metadata"), dict) else {}),
                    part.get("entry_date"),
                ),
            )
            restored_parts += int(cursor.rowcount or 0)

        restored_cells = 0
        context = get_security_context(required=False)
        tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
        for cell in table_cell_rows:
            if not isinstance(cell, dict):
                continue
            cursor.execute(
                """
                INSERT INTO document_table_cell (
                    tenant_id, cell_id, doc_id, table_index, row_index, row_label, column_index,
                    column_label, cell_value, source_kind, metadata, created_at
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cell_id) DO NOTHING
                """,
                (
                    tenant_id,
                    cell.get("cell_id"),
                    int(cell.get("doc_id") or snapshot_doc_id),
                    int(cell.get("table_index") or 0),
                    int(cell.get("row_index") or 0),
                    cell.get("row_label"),
                    int(cell.get("column_index") or 0),
                    cell.get("column_label"),
                    cell.get("cell_value"),
                    str(cell.get("source_kind") or "document").strip().lower() or "document",
                    Json(cell.get("metadata") if isinstance(cell.get("metadata"), dict) else {}),
                    cell.get("created_at"),
                ),
            )
            restored_cells += int(cursor.rowcount or 0)

        cursor.execute(
            """
            UPDATE document_removal_log
            SET restored_at = NOW(),
                restored_by = %s,
                restore_status = 'restored',
                restore_error = NULL
            WHERE removal_id = %s
            """,
            (str(restored_by or "").strip() or "api:user", int(removal_id)),
        )

    _set_serial_sequence_to_max(connection, "document", "doc_id")
    _set_serial_sequence_to_max(connection, "part", "part_id")
    _set_serial_sequence_to_max(connection, "document_table_cell", "cell_id")
    _set_serial_sequence_to_max(connection, "document_removal_log", "removal_id")

    connection.commit()
    return {
        "restored": True,
        "removal_id": int(removal_id),
        "doc_id": snapshot_doc_id,
        "parts_restored": int(restored_parts),
        "table_cells_restored": int(restored_cells),
    }


def _normalize_date_key(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, (date, datetime)):
        return value.year * 10000 + value.month * 100 + value.day
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            return int(stripped)
        for pattern in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                parsed = datetime.strptime(stripped, pattern)
            except ValueError:
                continue
            return parsed.year * 10000 + parsed.month * 100 + parsed.day
    return None


def _extract_date_key(metadata: dict[str, Any] | None, candidate_fields: tuple[str, ...]) -> int | None:
    if not metadata:
        return None

    for field_name in candidate_fields:
        if field_name not in metadata:
            continue
        normalized = _normalize_date_key(metadata.get(field_name))
        if normalized is not None:
            return normalized
    return None


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for pattern in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(text, pattern).date()
            except ValueError:
                continue
    return None


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def _normalize_attr_type_name(attr_type: str) -> str:
    """Normalize attribute keys so semantically identical fields share one attr_type."""
    normalized = str(attr_type or "").strip().lower()
    aliases = {
        "zweck": "purpose",
        "gesellschaftszweck": "purpose",
        "unternehmenszweck": "purpose",
        "business_purpose": "purpose",
        "company_purpose": "purpose",
    }
    return aliases.get(normalized, normalized)


def upsert_temporal_attribute(
    connection: psycopg2.extensions.connection,
    src_id: int,
    src_type: str,
    attr_type: str,
    attr_json: Any,
    valid_from: date | str | None = None,
    valid_until: date | str | None = None,
    close_previous: bool = True,
) -> dict[str, Any] | None:
    context = get_security_context(required=False)
    tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
    normalized_src_type = str(src_type).strip().lower()
    normalized_attr_type = _normalize_attr_type_name(attr_type)
    if not normalized_src_type or not normalized_attr_type:
        return None

    if isinstance(attr_json, dict):
        payload = attr_json
    else:
        payload = {"value": attr_json}

    start_date = _coerce_date(valid_from) or date.today()
    end_date = _coerce_date(valid_until)

    select_sql = """
    SELECT attr_id, attr_json, valid_from, valid_until
    FROM attribute
        WHERE tenant_id = %s::uuid
            AND src_id = %s
      AND src_type = %s
      AND attr_type = %s
      AND (valid_until IS NULL OR valid_until > %s)
    ORDER BY valid_from DESC, attr_id DESC
    """

    update_sql = """
    UPDATE attribute
    SET valid_until = %s
    WHERE tenant_id = %s::uuid AND attr_id = %s
    """

    insert_sql = """
    INSERT INTO attribute (tenant_id, src_id, src_type, attr_type, valid_from, valid_until, attr_json, search_txt)
    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s::jsonb, %s)
    RETURNING attr_id, src_id, src_type, attr_type, valid_from, valid_until, attr_json, entry_date
    """

    with connection.cursor() as cursor:
        cursor.execute(select_sql, (tenant_id, src_id, normalized_src_type, normalized_attr_type, start_date))
        current_rows = cursor.fetchall() or []

        unchanged_row: dict[str, Any] | None = None
        for current in current_rows:
            current_json = current[1]
            current_start = current[2]
            current_end = current[3]
            if (
                _canonical_json(current_json) == _canonical_json(payload)
                and current_start == start_date
                and current_end == end_date
            ):
                unchanged_row = {
                    "attr_id": int(current[0]),
                    "src_id": int(src_id),
                    "src_type": normalized_src_type,
                    "attr_type": normalized_attr_type,
                    "valid_from": current_start,
                    "valid_until": current_end,
                    "attr_json": current_json,
                    "unchanged": True,
                }
                break

        if close_previous:
            for current in current_rows:
                if unchanged_row is not None and int(current[0]) == int(unchanged_row["attr_id"]):
                    continue
                current_start = current[2]
                current_end = current[3]
                if not (current_end is None or current_end > start_date):
                    continue

                # Prevent invalid ranges when backfilling older dates.
                # The closed upper bound must be >= the existing lower bound.
                close_at = start_date
                if current_start and close_at < current_start:
                    close_at = current_start
                if current_end is None or close_at < current_end:
                    cursor.execute(update_sql, (close_at, tenant_id, int(current[0])))

        if unchanged_row is not None:
            return unchanged_row

        search_txt = _canonical_json(payload)
        cursor.execute(
            insert_sql,
            (
                tenant_id,
                int(src_id),
                normalized_src_type,
                normalized_attr_type,
                start_date,
                end_date,
                _canonical_json(payload),
                search_txt,
            ),
        )
        row = cursor.fetchone()

    return {
        "attr_id": int(row[0]),
        "src_id": int(row[1]),
        "src_type": row[2],
        "attr_type": row[3],
        "valid_from": row[4],
        "valid_until": row[5],
        "attr_json": row[6] or {},
        "entry_date": row[7],
    }


def sync_object_temporal_attributes(
    connection: psycopg2.extensions.connection,
    object_id: int,
    attributes: dict[str, Any],
    valid_from: date | str | None = None,
    valid_until: date | str | None = None,
    close_previous: bool = True,
    commit: bool = True,
) -> list[dict[str, Any]]:
    return sync_temporal_attributes(
        connection=connection,
        src_id=int(object_id),
        src_type="object",
        attributes=attributes,
        valid_from=valid_from,
        valid_until=valid_until,
        close_previous=close_previous,
        commit=commit,
    )


def sync_temporal_attributes(
    connection: psycopg2.extensions.connection,
    src_id: int,
    src_type: str,
    attributes: dict[str, Any],
    valid_from: date | str | None = None,
    valid_until: date | str | None = None,
    close_previous: bool = True,
    commit: bool = True,
) -> list[dict[str, Any]]:
    if not attributes:
        return []

    rows: list[dict[str, Any]] = []
    for attr_name, attr_value in attributes.items():
        if not str(attr_name).strip():
            continue
        row = upsert_temporal_attribute(
            connection=connection,
            src_id=int(src_id),
            src_type=src_type,
            attr_type=str(attr_name),
            attr_json=attr_value,
            valid_from=valid_from,
            valid_until=valid_until,
            close_previous=close_previous,
        )
        if row is not None:
            rows.append(row)

    if commit and rows:
        connection.commit()

    return rows


def get_object_relationship(
    connection: psycopg2.extensions.connection,
    relationship_name: str,
    relationship_cat: str,
    src_object_id: int,
    tar_object_id: int,
) -> dict[str, Any] | None:
    sql = """
    SELECT relationship_id, relationship_name, relationship_cat, src_object_id, tar_object_id, confidence, metadata, entry_date
    FROM object_relationship
    WHERE relationship_name = %s
      AND relationship_cat = %s
      AND src_object_id = %s
      AND tar_object_id = %s
    LIMIT 1
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, (relationship_name, relationship_cat, src_object_id, tar_object_id))
        row = cursor.fetchone()

    if not row:
        return None

    return {
        "relationship_id": int(row[0]),
        "relationship_name": row[1],
        "relationship_cat": row[2],
        "src_object_id": int(row[3]),
        "tar_object_id": int(row[4]),
        "confidence": row[5],
        "metadata": row[6] or {},
        "entry_date": row[7],
    }


def _find_conflicting_relationship_ids(
    connection: psycopg2.extensions.connection,
    relationship_name: str,
    relationship_cat: str,
    src_object_id: int,
    tar_object_id: int,
    exclusivity_scope: str,
    exclude_relationship_id: int,
) -> list[int]:
    scope = str(exclusivity_scope).strip().lower()
    if scope not in {"by_target", "by_source"}:
        return []

    filters = [
        "relationship_name = %s",
        "relationship_cat = %s",
        "relationship_id <> %s",
    ]
    params: list[Any] = [relationship_name, relationship_cat, exclude_relationship_id]

    if scope == "by_target":
        filters.append("tar_object_id = %s")
        params.append(tar_object_id)
    else:
        filters.append("src_object_id = %s")
        params.append(src_object_id)

    sql = f"""
    SELECT relationship_id
    FROM object_relationship
    WHERE {' AND '.join(filters)}
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
    return [int(row[0]) for row in rows]


def deactivate_object_relationship(
    connection: psycopg2.extensions.connection,
    relationship_name: str,
    relationship_cat: str,
    src_object_id: int,
    tar_object_id: int,
    effective_from: date | str | None = None,
    metadata: dict[str, Any] | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    existing = get_object_relationship(
        connection=connection,
        relationship_name=relationship_name,
        relationship_cat=relationship_cat,
        src_object_id=src_object_id,
        tar_object_id=tar_object_id,
    )
    if not existing:
        return {
            "deleted": False,
            "relationship_name": relationship_name,
            "relationship_cat": relationship_cat,
            "src_object_id": int(src_object_id),
            "tar_object_id": int(tar_object_id),
        }

    attrs = {"active": False}
    if metadata:
        attrs.update(metadata)

    sync_temporal_attributes(
        connection=connection,
        src_id=int(existing["relationship_id"]),
        src_type="relationship",
        attributes=attrs,
        valid_from=effective_from,
        valid_until=None,
        close_previous=True,
        commit=False,
    )

    if commit:
        connection.commit()

    return {
        "deleted": True,
        "relationship_id": int(existing["relationship_id"]),
        "relationship_name": relationship_name,
        "relationship_cat": relationship_cat,
        "src_object_id": int(src_object_id),
        "tar_object_id": int(tar_object_id),
    }


def upsert_temporal_relationship(
    connection: psycopg2.extensions.connection,
    relationship_name: str,
    relationship_cat: str,
    src_object_id: int,
    tar_object_id: int,
    confidence: float | None = None,
    metadata: dict[str, Any] | None = None,
    temporal_attributes: dict[str, Any] | None = None,
    effective_from: date | str | None = None,
    effective_until: date | str | None = None,
    exclusivity_scope: str | None = None,
) -> dict[str, Any]:
    relationship = upsert_object_relationship(
        connection=connection,
        relationship_name=relationship_name,
        relationship_cat=relationship_cat,
        src_object_id=src_object_id,
        tar_object_id=tar_object_id,
        confidence=confidence,
        metadata=metadata,
    )

    rel_attrs = dict(temporal_attributes or {})
    rel_attrs["active"] = True

    temporal_rows = sync_temporal_attributes(
        connection=connection,
        src_id=int(relationship["relationship_id"]),
        src_type="relationship",
        attributes=rel_attrs,
        valid_from=effective_from,
        valid_until=effective_until,
        close_previous=True,
        commit=False,
    )

    deactivated = 0
    if exclusivity_scope in {"by_target", "by_source"}:
        conflicts = _find_conflicting_relationship_ids(
            connection=connection,
            relationship_name=relationship_name,
            relationship_cat=relationship_cat,
            src_object_id=src_object_id,
            tar_object_id=tar_object_id,
            exclusivity_scope=exclusivity_scope,
            exclude_relationship_id=int(relationship["relationship_id"]),
        )
        for conflict_id in conflicts:
            sync_temporal_attributes(
                connection=connection,
                src_id=conflict_id,
                src_type="relationship",
                attributes={"active": False, "deactivation_reason": f"exclusivity:{exclusivity_scope}"},
                valid_from=effective_from,
                valid_until=None,
                close_previous=True,
                commit=False,
            )
            deactivated += 1

    connection.commit()

    relationship["temporal_attributes_written"] = len(temporal_rows)
    relationship["deactivated_conflicts"] = deactivated
    return relationship


def get_relationships(
    connection: psycopg2.extensions.connection,
    relationship_name: str | None = None,
    relationship_cat: str | None = None,
    src_object_name: str | None = None,
    src_class_name: str | None = None,
    tar_object_name: str | None = None,
    tar_class_name: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    sql = """
    SELECT
        rel.relationship_id,
        rel.relationship_name,
        rel.relationship_cat,
        rel.confidence,
        rel.metadata,
        rel.entry_date,
        src.object_id,
        src.object_name,
        src.class_name,
        src.metadata,
        tar.object_id,
        tar.object_name,
        tar.class_name,
        tar.metadata
    FROM object_relationship rel
    JOIN object_instance src ON src.object_id = rel.src_object_id
    JOIN object_instance tar ON tar.object_id = rel.tar_object_id
    WHERE (%s IS NULL OR rel.relationship_name = %s)
      AND (%s IS NULL OR rel.relationship_cat = %s)
      AND (%s IS NULL OR LOWER(src.object_name) = LOWER(%s))
      AND (%s IS NULL OR src.class_name = %s)
      AND (%s IS NULL OR LOWER(tar.object_name) = LOWER(%s))
      AND (%s IS NULL OR tar.class_name = %s)
    ORDER BY rel.entry_date DESC, rel.relationship_id DESC
    LIMIT %s
    """

    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                relationship_name,
                relationship_name,
                relationship_cat,
                relationship_cat,
                src_object_name,
                src_object_name,
                src_class_name,
                src_class_name,
                tar_object_name,
                tar_object_name,
                tar_class_name,
                tar_class_name,
                limit,
            ),
        )
        rows = cursor.fetchall()

    return [
        {
            "relationship_id": row[0],
            "relationship_name": row[1],
            "relationship_cat": row[2],
            "confidence": row[3],
            "relationship_metadata": row[4] or {},
            "entry_date": row[5],
            "src_object_id": row[6],
            "src_object_name": row[7],
            "src_class_name": row[8],
            "src_metadata": row[9] or {},
            "tar_object_id": row[10],
            "tar_object_name": row[11],
            "tar_class_name": row[12],
            "tar_metadata": row[13] or {},
        }
        for row in rows
    ]


def upsert_semantic_terms(
    connection: psycopg2.extensions.connection,
    terms: list[dict[str, Any]],
) -> int:
    """Upsert semantic terms that map user wording to canonical schema names."""
    if not terms:
        return 0

    rows: list[tuple[str, str, str, str, str, str, Any]] = []
    tenant_id = get_tenant_id()
    allowed_source_types = {"seeded", "ingest", "solf_clause", "manual", "llm_generated"}
    source_type_aliases = {
        "ingest_llm_alias": "llm_generated",
    }
    allowed_kinds = {"attribute", "relationship", "entity_type", "category"}
    for item in terms:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or "").strip().lower()
        canonical_name = str(item.get("canonical_name") or "").strip().lower()
        term_text = str(item.get("term_text") or "").strip()
        language = str(item.get("language") or "und").strip().lower() or "und"
        raw_source_type = str(item.get("source_type") or "ingest").strip().lower() or "ingest"
        source_type = source_type_aliases.get(raw_source_type, raw_source_type)
        if source_type not in allowed_source_types:
            source_type = "ingest"
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}

        if kind not in allowed_kinds:
            continue
        if not canonical_name or not term_text:
            continue

        rows.append((tenant_id, kind, canonical_name, term_text, language, source_type, Json(metadata)))

    if not rows:
        return 0

    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO semantic_terms (tenant_id, kind, canonical_name, term_text, language, source_type, metadata)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, kind, canonical_name, term_text, language)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                metadata = COALESCE(semantic_terms.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb),
                modified_at = NOW()
            """,
            rows,
        )
    connection.commit()
    return len(rows)


def upsert_query_term_alias(
    connection: psycopg2.extensions.connection,
    aliases: list[dict[str, Any]],
) -> int:
    """Upsert query-time alias rows for canonical terms and general business categories."""
    if not aliases:
        return 0

    rows: list[tuple[str, str, str, str, str, int, str, Any]] = []
    tenant_id = get_tenant_id()
    allowed_kinds = {"attribute", "relationship", "entity_type", "category"}
    allowed_source_types = {"manual", "ingest", "seeded", "solf_clause", "llm_generated"}
    for item in aliases:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or "").strip().lower()
        alias_text = str(item.get("alias_text") or "").strip()
        canonical_name = str(item.get("canonical_name") or "").strip().lower()
        language = str(item.get("language") or "und").strip().lower() or "und"
        source_type = str(item.get("source_type") or "manual").strip().lower() or "manual"
        priority = int(item.get("priority") or 100)
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}

        if kind not in allowed_kinds:
            continue
        if source_type not in allowed_source_types:
            source_type = "manual"
        if not alias_text or not canonical_name:
            continue

        rows.append((tenant_id, alias_text, canonical_name, kind, language, priority, source_type, Json(metadata)))

    if not rows:
        return 0

    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO query_term_alias (tenant_id, alias_text, canonical_name, kind, language, priority, source_type, metadata)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, alias_text, kind, language)
            DO UPDATE SET
                canonical_name = EXCLUDED.canonical_name,
                priority = EXCLUDED.priority,
                source_type = EXCLUDED.source_type,
                metadata = COALESCE(query_term_alias.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb),
                is_active = TRUE,
                modified_at = NOW()
            """,
            rows,
        )
    connection.commit()
    return len(rows)


def upsert_query_alias_candidates(
    connection: psycopg2.extensions.connection,
    candidates: list[dict[str, Any]],
) -> int:
    """Record learned aliases for review; this never activates query routing aliases."""
    rows: list[tuple[Any, ...]] = []
    tenant_id = get_tenant_id()
    allowed_kinds = {"attribute", "relationship", "entity_type", "category", "document_hint"}
    for item in candidates:
        if not isinstance(item, dict):
            continue
        alias_text = str(item.get("alias_text") or item.get("term_text") or "").strip().lower()
        canonical_name = str(item.get("canonical_name") or "").strip().lower()
        kind = str(item.get("kind") or "").strip().lower()
        if not alias_text or not canonical_name or kind not in allowed_kinds or alias_text == canonical_name:
            continue
        rows.append((
            tenant_id, alias_text, canonical_name, kind, str(item.get("language") or "und").strip().lower() or "und",
            max(0.0, min(1.0, float(item.get("confidence") or 0.5))),
            int(item.get("doc_id") or 0) or None, Json(item.get("metadata") if isinstance(item.get("metadata"), dict) else {}),
        ))
    if not rows:
        return 0
    with connection.cursor() as cursor:
        cursor.execute(create_query_alias_candidate_table)
        cursor.executemany(
            """
            INSERT INTO query_alias_candidate (tenant_id, alias_text, canonical_name, kind, language, confidence, first_seen_doc_id, last_seen_doc_id, metadata)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, alias_text, canonical_name, kind, language)
            DO UPDATE SET occurrence_count = query_alias_candidate.occurrence_count + 1,
                          last_seen_doc_id = EXCLUDED.last_seen_doc_id,
                          confidence = GREATEST(query_alias_candidate.confidence, EXCLUDED.confidence),
                          metadata = COALESCE(query_alias_candidate.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                          modified_at = NOW()
            """,
            [(t, a, c, k, l, confidence, doc_id, doc_id, metadata) for t, a, c, k, l, confidence, doc_id, metadata in rows],
        )
    connection.commit()
    return len(rows)


def promote_query_alias_candidate(
    connection: psycopg2.extensions.connection,
    candidate_id: int,
    approved_by: str = "api:user",
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(create_query_alias_candidate_table)
        cursor.execute(
            "SELECT alias_text, canonical_name, kind, language, confidence, metadata FROM query_alias_candidate WHERE tenant_id = %s::uuid AND candidate_id = %s AND status = 'proposed'",
            (get_tenant_id(), int(candidate_id)),
        )
        row = cursor.fetchone()
        if row is None:
            return False
        alias_kind = "category" if row[2] == "document_hint" else row[2]
        upsert_query_term_alias(connection, [{"alias_text": row[0], "canonical_name": row[1], "kind": alias_kind, "language": row[3], "priority": 90, "source_type": "llm_generated", "metadata": {**(row[5] or {}), "approved_by": approved_by, "candidate_id": int(candidate_id)}}])
        cursor.execute("UPDATE query_alias_candidate SET status = 'approved', modified_at = NOW() WHERE tenant_id = %s::uuid AND candidate_id = %s", (get_tenant_id(), int(candidate_id)))
    connection.commit()
    return True


def list_query_alias_candidates(
    connection: psycopg2.extensions.connection,
    status: str | None = "proposed",
    limit: int = 200,
) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(create_query_alias_candidate_table)
        cursor.execute(
            """
            SELECT candidate_id, alias_text, canonical_name, kind, language, confidence,
                   occurrence_count, first_seen_doc_id, last_seen_doc_id, status, metadata,
                   created_at, modified_at
            FROM query_alias_candidate
                 WHERE tenant_id = %s::uuid AND (%s IS NULL OR status = %s)
            ORDER BY confidence DESC, occurrence_count DESC, candidate_id DESC
            LIMIT %s
            """,
            (get_tenant_id(), str(status).strip().lower() if status else None, str(status).strip().lower() if status else None, max(1, min(int(limit), 1000))),
        )
        rows = cursor.fetchall() or []
    return [
        {"candidate_id": int(row[0]), "alias_text": row[1], "canonical_name": row[2], "kind": row[3],
         "language": row[4], "confidence": float(row[5]), "occurrence_count": int(row[6]),
         "first_seen_doc_id": int(row[7]) if row[7] is not None else None, "last_seen_doc_id": int(row[8]) if row[8] is not None else None,
         "status": row[9], "metadata": row[10] or {}, "created_at": row[11], "modified_at": row[12]}
        for row in rows
    ]


def upsert_source_schema_taxonomy(
    connection: psycopg2.extensions.connection,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert semantic source-schema classifications without syncing source rows."""
    if not rows:
        return 0

    values: list[tuple[Any, ...]] = []
    tenant_id = get_tenant_id()
    allowed_roles = {"entity_candidate", "identity", "attribute", "filter", "measure", "temporal", "relationship_key"}
    for item in rows:
        if not isinstance(item, dict):
            continue
        source_id = int(item.get("source_id") or 0)
        schema_name = str(item.get("schema_name") or "").strip()
        table_name = str(item.get("table_name") or "").strip()
        category_name = str(item.get("category_name") or "").strip().lower()
        role_name = str(item.get("role_name") or "entity_candidate").strip().lower()
        if source_id <= 0 or not schema_name or not table_name or not category_name or role_name not in allowed_roles:
            continue
        confidence = max(0.0, min(1.0, float(item.get("confidence") or 0.5)))
        values.append(
            (
                tenant_id,
                source_id,
                schema_name,
                table_name,
                str(item.get("column_name") or "").strip() or None,
                category_name,
                role_name,
                confidence,
                Json(item.get("metadata") if isinstance(item.get("metadata"), dict) else {}),
            )
        )
    if not values:
        return 0

    with connection.cursor() as cursor:
        cursor.execute(create_source_schema_taxonomy_table)
        cursor.execute(
            "SELECT source_id FROM source_database_registry WHERE tenant_id = %s::uuid AND source_id = ANY(%s)",
            (tenant_id, sorted({int(value[1]) for value in values})),
        )
        owned_source_ids = {int(row[0]) for row in cursor.fetchall() or []}
        values = [value for value in values if int(value[1]) in owned_source_ids]
        if not values:
            return 0
        cursor.executemany(
            """
            INSERT INTO source_schema_taxonomy (
                tenant_id, source_id, schema_name, table_name, column_name, category_name,
                role_name, confidence, metadata, is_active, modified_at
            )
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW())
            ON CONFLICT (tenant_id, source_id, schema_name, table_name, (COALESCE(column_name, '')), category_name, role_name)
            DO UPDATE SET confidence = EXCLUDED.confidence,
                          metadata = COALESCE(source_schema_taxonomy.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                          is_active = TRUE,
                          modified_at = NOW()
            """,
            values,
        )
    connection.commit()
    return len(values)


def find_source_schema_taxonomy(
    connection: psycopg2.extensions.connection,
    category_name: str | None = None,
    role_name: str | None = None,
    source_id: int | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    conditions = ["t.is_active = TRUE", "t.tenant_id = %s::uuid"]
    params: list[Any] = [get_tenant_id()]
    if str(category_name or "").strip():
        conditions.append("t.category_name = %s")
        params.append(str(category_name).strip().lower())
    if str(role_name or "").strip():
        conditions.append("t.role_name = %s")
        params.append(str(role_name).strip().lower())
    if source_id is not None:
        conditions.append("t.source_id = %s")
        params.append(int(source_id))
    params.append(max(1, min(int(limit), 1000)))

    with connection.cursor() as cursor:
        cursor.execute(create_source_schema_taxonomy_table)
        cursor.execute(
            f"""
            SELECT t.taxonomy_id, t.source_id, r.source_key, t.schema_name, t.table_name,
                   t.column_name, t.category_name, t.role_name, t.confidence, t.metadata
            FROM source_schema_taxonomy t
            JOIN source_database_registry r
              ON r.source_id = t.source_id AND r.tenant_id = t.tenant_id
            WHERE {' AND '.join(conditions)}
            ORDER BY t.confidence DESC, r.source_key, t.schema_name, t.table_name, t.column_name NULLS FIRST
            LIMIT %s
            """,
            tuple(params),
        )
        results = cursor.fetchall() or []
    return [
        {
            "taxonomy_id": int(row[0]), "source_id": int(row[1]), "source_key": row[2],
            "schema_name": row[3], "table_name": row[4], "column_name": row[5],
            "category_name": row[6], "role_name": row[7], "confidence": float(row[8]),
            "metadata": row[9] if isinstance(row[9], dict) else {},
        }
        for row in results
    ]


def upsert_category_registry_rows(
    connection: psycopg2.extensions.connection,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert unified category coverage projections from internal or external metadata."""
    allowed_scopes = {"internal", "external", "document_kb"}
    allowed_methods = {"sql", "semantic_lookup", "rag", "document_facet"}
    tenant_id = get_tenant_id()
    values: list[tuple[Any, ...]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        category_name = str(item.get("category_name") or "").strip().lower()
        source_scope = str(item.get("source_scope") or "").strip().lower()
        access_method = str(item.get("access_method") or "").strip().lower()
        if not category_name or source_scope not in allowed_scopes or access_method not in allowed_methods:
            continue
        confidence = max(0.0, min(1.0, float(item.get("confidence") or 0.5)))
        values.append((
            tenant_id,
            category_name, source_scope, access_method,
            int(item["source_id"]) if item.get("source_id") is not None else None,
            str(item.get("schema_name") or "").strip() or None,
            str(item.get("table_name") or "").strip() or None,
            str(item.get("column_name") or "").strip() or None,
            Json(sorted({str(v).strip().lower() for v in item.get("entity_types") or [] if str(v).strip()})),
            Json(sorted({str(v).strip().lower() for v in item.get("attributes") or [] if str(v).strip()})),
            Json(sorted({str(v).strip().lower() for v in item.get("document_types") or [] if str(v).strip()})),
            Json(item.get("metadata") if isinstance(item.get("metadata"), dict) else {}),
            confidence,
        ))
    if not values:
        return 0
    with connection.cursor() as cursor:
        cursor.execute(create_category_registry_table)
        cursor.executemany(
            """
            INSERT INTO category_registry (
                tenant_id, category_name, source_scope, access_method, source_id, schema_name, table_name,
                column_name, entity_types, attributes, document_types, metadata, confidence
            ) VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (
                tenant_id, category_name, source_scope, access_method,
                (COALESCE(source_id, 0)), (COALESCE(schema_name, '')),
                (COALESCE(table_name, '')), (COALESCE(column_name, ''))
            )
            DO UPDATE SET entity_types = EXCLUDED.entity_types,
                          attributes = EXCLUDED.attributes,
                          document_types = EXCLUDED.document_types,
                          metadata = COALESCE(category_registry.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                          confidence = EXCLUDED.confidence,
                          is_active = TRUE,
                          modified_at = NOW()
            """,
            values,
        )
    connection.commit()
    return len(values)


def resolve_category_registry(
    connection: psycopg2.extensions.connection,
    category_name: str,
    entity_type: str | None = None,
    attribute_name: str | None = None,
    document_type: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Resolve a query concept to ranked locations and access methods."""
    normalized = str(category_name or "").strip().lower()
    if not normalized:
        return []
    conditions = ["tenant_id = %s::uuid", "is_active = TRUE", "(category_name = %s OR category_name ILIKE %s)"]
    params: list[Any] = [get_tenant_id(), normalized, f"%{normalized}%"]
    for column, value in (("entity_types", entity_type), ("attributes", attribute_name), ("document_types", document_type)):
        if str(value or "").strip():
            conditions.append(f"{column} @> %s::jsonb")
            params.append(json.dumps([str(value).strip().lower()]))
    params.append(max(1, min(int(limit), 200)))
    with connection.cursor() as cursor:
        cursor.execute(create_category_registry_table)
        cursor.execute(
            f"""
            SELECT category_id, category_name, source_scope, access_method, source_id,
                   schema_name, table_name, column_name, entity_types, attributes,
                   document_types, metadata, confidence
            FROM category_registry
            WHERE {' AND '.join(conditions)}
            ORDER BY confidence DESC, category_id
            LIMIT %s
            """,
            tuple(params),
        )
        rows = cursor.fetchall() or []
    return [
        {"category_id": int(row[0]), "category_name": row[1], "source_scope": row[2], "access_method": row[3],
         "source_id": int(row[4]) if row[4] is not None else None, "schema_name": row[5], "table_name": row[6],
         "column_name": row[7], "entity_types": row[8] or [], "attributes": row[9] or [],
         "document_types": row[10] or [], "metadata": row[11] or {}, "confidence": float(row[12])}
        for row in rows
    ]


def upsert_category_definition(
    connection: psycopg2.extensions.connection,
    *,
    name: str,
    source_type: str,
    access_method: str,
    source_id: int | None = None,
    description: str | None = None,
    entity_mappings: list[dict[str, Any]] | None = None,
    attribute_mappings: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
    priority: int = 100,
) -> dict[str, int]:
    """Persist category mappings while referencing, rather than copying, source credentials."""
    category_name = str(name or "").strip().lower()
    normalized_source_type = str(source_type or "").strip().lower()
    normalized_access = str(access_method or "").strip().lower()
    if not category_name:
        raise ValueError("category name is required")
    tenant_id = get_tenant_id()
    if normalized_source_type not in {"internal_db", "external_db", "documents"}:
        raise ValueError("invalid category source_type")
    if normalized_access not in {"sql", "semantic_lookup", "rag", "document_facet"}:
        raise ValueError("invalid category access_method")
    if normalized_source_type == "external_db" and source_id is None:
        raise ValueError("external_db category source requires source_id")
    with connection.cursor() as cursor:
        if normalized_source_type == "external_db":
            cursor.execute(
                "SELECT 1 FROM source_database_registry WHERE tenant_id = %s::uuid AND source_id = %s",
                (tenant_id, int(source_id)),
            )
            if cursor.fetchone() is None:
                raise ValueError("external_db category source is not registered for the active tenant")
        cursor.execute(create_categories_table)
        cursor.execute(create_category_sources_table)
        cursor.execute(create_category_entities_table)
        cursor.execute(create_category_attributes_table)
        cursor.execute(
            """
            INSERT INTO categories (tenant_id, name, description, metadata, is_active, modified_at)
            VALUES (%s::uuid, %s, %s, %s, TRUE, NOW())
            ON CONFLICT (tenant_id, name) DO UPDATE SET
                description = COALESCE(EXCLUDED.description, categories.description),
                metadata = COALESCE(categories.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                is_active = TRUE, modified_at = NOW()
            RETURNING category_id
            """,
            (tenant_id, category_name, str(description or "").strip() or None, Json(metadata or {})),
        )
        category_id = int(cursor.fetchone()[0])
        cursor.execute(
            """
            INSERT INTO category_sources (tenant_id, category_id, source_type, source_id, access_method, priority, metadata, is_active, modified_at)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, TRUE, NOW())
            ON CONFLICT (tenant_id, category_id, source_type, (COALESCE(source_id, 0)), access_method) DO UPDATE SET
                priority = EXCLUDED.priority,
                metadata = COALESCE(category_sources.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                is_active = TRUE, modified_at = NOW()
            RETURNING category_source_id
            """,
            (tenant_id, category_id, normalized_source_type, int(source_id) if source_id is not None else None, normalized_access, int(priority), Json(metadata or {})),
        )
        category_source_id = int(cursor.fetchone()[0])

        entity_count = 0
        for item in entity_mappings or []:
            if not isinstance(item, dict):
                continue
            entity_type = str(item.get("entity_type") or "").strip().lower()
            if not entity_type:
                continue
            cursor.execute(
                """
                INSERT INTO category_entities (tenant_id, category_source_id, entity_type, source_table, source_column, role_name, confidence, metadata)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, category_source_id, entity_type, (COALESCE(source_table, '')), (COALESCE(source_column, '')), role_name)
                DO UPDATE SET confidence = EXCLUDED.confidence,
                              metadata = COALESCE(category_entities.metadata, '{}'::jsonb) || EXCLUDED.metadata
                """,
                (tenant_id, category_source_id, entity_type, item.get("source_table"), item.get("source_column"), str(item.get("role_name") or "entity_candidate").strip().lower(), max(0.0, min(1.0, float(item.get("confidence") or 0.5))), Json(item.get("metadata") if isinstance(item.get("metadata"), dict) else {})),
            )
            entity_count += 1

        attribute_count = 0
        for item in attribute_mappings or []:
            if not isinstance(item, dict):
                continue
            attribute_name = str(item.get("attribute_name") or item.get("canonical_name") or "").strip().lower()
            canonical_name = str(item.get("canonical_name") or attribute_name).strip().lower()
            if not attribute_name or not canonical_name:
                continue
            cursor.execute(
                """
                INSERT INTO category_attributes (tenant_id, category_source_id, attribute_name, canonical_name, source_table, source_column, role_name, confidence, metadata)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, category_source_id, canonical_name, (COALESCE(source_table, '')), (COALESCE(source_column, '')), role_name)
                DO UPDATE SET attribute_name = EXCLUDED.attribute_name,
                              confidence = EXCLUDED.confidence,
                              metadata = COALESCE(category_attributes.metadata, '{}'::jsonb) || EXCLUDED.metadata
                """,
                (tenant_id, category_source_id, attribute_name, canonical_name, item.get("source_table"), item.get("source_column"), str(item.get("role_name") or "attribute").strip().lower(), max(0.0, min(1.0, float(item.get("confidence") or 0.5))), Json(item.get("metadata") if isinstance(item.get("metadata"), dict) else {})),
            )
            attribute_count += 1
    connection.commit()
    return {"category_id": category_id, "category_source_id": category_source_id, "entity_mappings": entity_count, "attribute_mappings": attribute_count}


_UNIVERSAL_QUERY_PATTERNS = [
    # Canonical enterprise query taxonomy. Legacy aliases below remain active
    # for callers that persisted the original registry keys.
    ("entity_attribute", "Entity attribute/property lookup", "attribute_lookup", ["internal", "external", "document_kb"], ["semantic_lookup", "sql", "rag"]),
    ("temporal_schedule", "Temporal and schedule inquiry", "time_lookup", ["internal", "external", "document_kb"], ["semantic_lookup", "sql", "rag"]),
    ("spatial_location", "Spatial and location inquiry", "location_lookup", ["internal", "external", "document_kb"], ["semantic_lookup", "sql", "document_facet"]),
    ("existence_boolean", "Existence and boolean verification", "existence_lookup", ["internal", "external", "document_kb"], ["semantic_lookup", "sql", "rag"]),
    ("latest_document_state", "Latest chronological document state", "latest_document", ["document_kb"], ["rag", "document_facet"]),
    ("spatial_clause_extraction", "Spatial and clause-specific extraction", "document_clause", ["document_kb"], ["rag", "document_facet"]),
    ("document_synthesis", "Document summarization and synthesis", "document_synthesis", ["document_kb"], ["rag"]),
    ("metric_aggregation", "Metric aggregation and calculation", "aggregate", ["internal", "external", "document_kb"], ["sql", "semantic_lookup", "rag"]),
    ("comparative_analysis", "Comparative analysis", "compare", ["internal", "external", "document_kb"], ["sql", "semantic_lookup", "rag"]),
    ("procedural_sop", "Technical procedure and SOP guidance", "procedural_guidance", ["internal", "document_kb"], ["semantic_lookup", "rag"]),
    ("capability_feasibility", "System capability and feasibility", "capability_lookup", ["internal", "document_kb"], ["semantic_lookup", "rag"]),
    ("root_cause_diagnostic", "Root-cause and diagnostic analysis", "diagnostic_analysis", ["internal", "external", "document_kb"], ["sql", "semantic_lookup", "rag"]),
    ("relational_graph", "Relational graph traversal and ownership", "relationship_lookup", ["internal", "external", "document_kb"], ["semantic_lookup", "sql", "rag"]),
    ("federated_join", "Cross-entity federated join", "federated_read", ["external"], ["sql"]),
    ("workflow_action", "Workflow trigger and action invocation", "workflow_execution", ["internal"], ["semantic_lookup"]),
    ("clarification_ambiguity", "Clarification and ambiguity resolution", "clarification", ["internal", "document_kb"], ["semantic_lookup", "rag"]),
    ("identity", "Identity query", "identity_lookup", ["internal", "external", "document_kb"], ["semantic_lookup", "sql", "rag"]),
    ("attribute", "Attribute lookup", "attribute_lookup", ["internal", "external", "document_kb"], ["semantic_lookup", "sql", "rag", "document_facet"]),
    ("relationship", "Relationship query", "relationship_lookup", ["internal", "external", "document_kb"], ["semantic_lookup", "sql", "rag"]),
    ("collection", "Collection query", "collection_retrieval", ["external", "internal", "document_kb"], ["sql", "semantic_lookup", "rag"]),
    ("conditional_collection", "Conditional collection query", "conditional_collection", ["external", "internal", "document_kb"], ["sql", "semantic_lookup", "rag"]),
    ("document_reference", "Document reference query", "document_retrieval", ["document_kb"], ["rag", "document_facet"]),
    ("document_attribute", "Document-derived attribute query", "document_attribute", ["document_kb"], ["rag", "document_facet"]),
    ("amount", "Amount query", "numeric_value", ["external", "internal", "document_kb"], ["sql", "semantic_lookup", "rag"]),
    ("duration", "Duration query", "duration_value", ["internal", "external"], ["semantic_lookup", "sql"]),
    ("responsibility", "Responsibility routing query", "responsibility_routing", ["internal", "document_kb"], ["semantic_lookup", "rag"]),
    ("status", "Status query", "status_lookup", ["external", "internal"], ["sql", "semantic_lookup"]),
    ("workflow_action", "Workflow action query", "workflow_execution", ["internal"], ["semantic_lookup"]),
    ("location", "Location query", "location_lookup", ["document_kb", "internal", "external"], ["document_facet", "semantic_lookup", "sql"]),
    ("time", "Time query", "time_lookup", ["internal", "external", "document_kb"], ["semantic_lookup", "sql", "rag"]),
]


def seed_query_pattern_registry(connection: psycopg2.extensions.connection) -> int:
    with connection.cursor() as cursor:
        cursor.execute(create_query_pattern_registry_table)
        cursor.executemany(
            """
            INSERT INTO query_pattern_registry (
                pattern_key, display_name, intent_key, allowed_source_scopes, allowed_access_methods, priority, metadata
            ) VALUES (%s, %s, %s, %s, %s, 100, %s)
            ON CONFLICT (pattern_key) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                intent_key = EXCLUDED.intent_key,
                allowed_source_scopes = EXCLUDED.allowed_source_scopes,
                allowed_access_methods = EXCLUDED.allowed_access_methods,
                is_active = TRUE,
                modified_at = NOW()
            """,
            [
                (key, label, intent, Json(scopes), Json(methods), Json({"source": "universal_query_patterns"}))
                for key, label, intent, scopes, methods in _UNIVERSAL_QUERY_PATTERNS
            ],
        )
    connection.commit()
    return len(_UNIVERSAL_QUERY_PATTERNS)


def get_query_pattern_definition(
    connection: psycopg2.extensions.connection,
    pattern_key: str,
) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(create_query_pattern_registry_table)
        cursor.execute(
            """
            SELECT pattern_key, display_name, intent_key, description, allowed_source_scopes,
                   allowed_access_methods, priority, metadata
            FROM query_pattern_registry
            WHERE pattern_key = %s AND is_active = TRUE
            """,
            (str(pattern_key or "").strip().lower(),),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return {"pattern_key": row[0], "display_name": row[1], "intent_key": row[2], "description": row[3],
            "allowed_source_scopes": row[4] or [], "allowed_access_methods": row[5] or [],
            "priority": int(row[6]), "metadata": row[7] or {}}


def list_query_pattern_definitions(connection: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    seed_query_pattern_registry(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pattern_key, display_name, intent_key, allowed_source_scopes, allowed_access_methods, priority FROM query_pattern_registry WHERE is_active = TRUE ORDER BY priority, pattern_key"
        )
        rows = cursor.fetchall() or []
    return [{"pattern_key": row[0], "display_name": row[1], "intent_key": row[2], "allowed_source_scopes": row[3] or [], "allowed_access_methods": row[4] or [], "priority": int(row[5])} for row in rows]


def resolve_category_registry_for_query(
    connection: psycopg2.extensions.connection,
    entity_type: str | None = None,
    attribute_names: list[str] | None = None,
    document_type: str | None = None,
    category_names: list[str] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return ranked category locations for a parsed query without executing retrieval."""
    predicates: list[str] = ["tenant_id = %s::uuid", "is_active = TRUE"]
    params: list[Any] = [get_tenant_id()]
    concepts = [str(item).strip().lower() for item in (category_names or []) if str(item).strip()]
    if concepts:
        predicates.append("(" + " OR ".join(["category_name = %s", "category_name ILIKE %s"] * len(concepts)) + ")")
        for concept in concepts:
            params.extend([concept, f"%{concept}%"])

    coverage_predicates: list[str] = []
    normalized_entity = str(entity_type or "").strip().lower()
    if normalized_entity:
        coverage_predicates.append("entity_types @> %s::jsonb")
        params.append(json.dumps([normalized_entity]))
    for attribute in attribute_names or []:
        normalized_attribute = str(attribute or "").strip().lower()
        if normalized_attribute:
            coverage_predicates.append("attributes @> %s::jsonb")
            params.append(json.dumps([normalized_attribute]))
    normalized_document_type = str(document_type or "").strip().lower()
    if normalized_document_type:
        coverage_predicates.append("document_types @> %s::jsonb")
        params.append(json.dumps([normalized_document_type]))
    if coverage_predicates:
        predicates.append("(" + " OR ".join(coverage_predicates) + ")")
    params.append(max(1, min(int(limit), 500)))

    with connection.cursor() as cursor:
        cursor.execute(create_category_registry_table)
        cursor.execute(
            f"""
            SELECT category_id, category_name, source_scope, access_method, source_id,
                   schema_name, table_name, column_name, entity_types, attributes,
                   document_types, metadata, confidence
            FROM category_registry
            WHERE {' AND '.join(predicates)}
            ORDER BY CASE source_scope WHEN 'external' THEN 1 WHEN 'document_kb' THEN 2 ELSE 3 END,
                     confidence DESC, category_id
            LIMIT %s
            """,
            tuple(params),
        )
        rows = cursor.fetchall() or []
    return [
        {"category_id": int(row[0]), "category_name": row[1], "source_scope": row[2], "access_method": row[3],
         "source_id": int(row[4]) if row[4] is not None else None, "schema_name": row[5], "table_name": row[6],
         "column_name": row[7], "entity_types": row[8] or [], "attributes": row[9] or [],
         "document_types": row[10] or [], "metadata": row[11] or {}, "confidence": float(row[12])}
        for row in rows
    ]


def rebuild_internal_category_registry(
    connection: psycopg2.extensions.connection,
) -> dict[str, int]:
    """Project current internal KB and document coverage into the unified category registry."""
    marker = "category_registry_internal_backfill"
    tenant_id = get_tenant_id()
    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM category_registry WHERE tenant_id = %s::uuid AND metadata->>'source' = %s",
            (tenant_id, marker),
        )
        cursor.execute(
            """
            SELECT oi.class_name, array_agg(DISTINCT a.attr_type) FILTER (WHERE a.attr_type IS NOT NULL)
            FROM object_instance oi
            LEFT JOIN attribute a
              ON a.tenant_id = oi.tenant_id AND a.src_type = 'object' AND a.src_id = oi.object_id
            WHERE oi.tenant_id = %s::uuid AND COALESCE(oi.status, 'active') = 'active'
            GROUP BY oi.class_name
            """,
            (tenant_id,),
        )
        class_rows = cursor.fetchall() or []
        cursor.execute(
            """
            SELECT d.doc_cat, d.doc_type,
                   array_agg(DISTINCT facet.place_kind) FILTER (WHERE facet.place_kind IS NOT NULL),
                   array_agg(DISTINCT action.action_type) FILTER (WHERE action.action_type IS NOT NULL)
            FROM document d
            LEFT JOIN document_geographic_facet facet ON facet.doc_id = d.doc_id
            LEFT JOIN document_action_item action ON action.doc_id = d.doc_id
            WHERE d.tenant_id = %s::uuid AND COALESCE(d.status, 'active') = 'active'
            GROUP BY d.doc_cat, d.doc_type
            """,
            (tenant_id,),
        )
        document_rows = cursor.fetchall() or []
    connection.commit()

    registry_rows: list[dict[str, Any]] = []
    for class_name, attribute_names in class_rows:
        entity_type = str(class_name or "").strip().lower()
        if not entity_type:
            continue
        attributes = [str(value).strip().lower() for value in attribute_names or [] if str(value).strip()]
        registry_rows.append(
            {
                "category_name": entity_type,
                "source_scope": "internal",
                "access_method": "semantic_lookup",
                "entity_types": [entity_type],
                "attributes": attributes,
                "metadata": {"source": marker},
                "confidence": 0.9,
            }
        )
        upsert_category_definition(
            connection,
            name=entity_type,
            source_type="internal_db",
            access_method="semantic_lookup",
            entity_mappings=[{"entity_type": entity_type, "source_table": "object_instance", "confidence": 0.9}],
            attribute_mappings=[
                {"attribute_name": attribute, "canonical_name": attribute, "source_table": "attribute", "confidence": 0.9}
                for attribute in attributes
            ],
            metadata={"source": marker},
        )

    for doc_cat, doc_type, place_kinds, action_types in document_rows:
        categories = {str(doc_cat or "").strip().lower(), str(doc_type or "").strip().lower()} - {""}
        document_type = str(doc_type or "").strip().lower()
        attributes = [str(value).strip().lower() for value in place_kinds or [] if str(value).strip()]
        attributes.extend(str(value).strip().lower() for value in action_types or [] if str(value).strip())
        for category in categories:
            registry_rows.append(
                {
                    "category_name": category,
                    "source_scope": "document_kb",
                    "access_method": "rag",
                    "document_types": [document_type] if document_type else [],
                    "attributes": sorted(set(attributes)),
                    "metadata": {"source": marker},
                    "confidence": 0.85,
                }
            )
            upsert_category_definition(
                connection,
                name=category,
                source_type="documents",
                access_method="rag",
                attribute_mappings=[
                    {"attribute_name": attribute, "canonical_name": attribute, "source_table": "document", "confidence": 0.85}
                    for attribute in sorted(set(attributes))
                ],
                metadata={"source": marker, "document_type": document_type},
            )

    written = upsert_category_registry_rows(connection, registry_rows)
    return {"internal_classes": len(class_rows), "document_categories": len(document_rows), "rows_written": written}


def list_category_definitions(
    connection: psycopg2.extensions.connection,
    name: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    conditions = ["c.is_active = TRUE", "c.tenant_id = %s::uuid", "cs.tenant_id = %s::uuid"]
    params: list[Any] = [get_tenant_id(), get_tenant_id()]
    if str(name or "").strip():
        conditions.append("c.name ILIKE %s")
        params.append(f"%{str(name).strip().lower()}%")
    params.append(max(1, min(int(limit), 500)))
    with connection.cursor() as cursor:
        cursor.execute(create_categories_table)
        cursor.execute(create_category_sources_table)
        cursor.execute(
            f"""
            SELECT c.category_id, c.name, c.description, c.metadata,
                   cs.category_source_id, cs.source_type, cs.source_id, cs.access_method, cs.priority, cs.metadata
            FROM categories c
            JOIN category_sources cs ON cs.category_id = c.category_id AND cs.tenant_id = c.tenant_id AND cs.is_active = TRUE
            WHERE {' AND '.join(conditions)}
            ORDER BY c.name, cs.priority, cs.category_source_id
            LIMIT %s
            """,
            tuple(params),
        )
        rows = cursor.fetchall() or []
    return [
        {"category_id": int(row[0]), "name": row[1], "description": row[2], "metadata": row[3] or {},
         "category_source_id": int(row[4]), "source_type": row[5], "source_id": int(row[6]) if row[6] is not None else None,
         "access_method": row[7], "priority": int(row[8]), "source_metadata": row[9] or {}}
        for row in rows
    ]


def get_semantic_terms(
    connection: psycopg2.extensions.connection,
    kind: str | None = None,
    canonical_name: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Fetch semantic term mappings from DB."""
    kind_norm = str(kind).strip().lower() if kind is not None else None
    canonical_norm = str(canonical_name).strip().lower() if canonical_name is not None else None

    sql = """
    SELECT kind, canonical_name, term_text, language, source_type, metadata
    FROM semantic_terms
        WHERE tenant_id = %s::uuid
            AND (%s IS NULL OR kind = %s)
      AND (%s IS NULL OR canonical_name = %s)
    ORDER BY canonical_name, term_text
    LIMIT %s
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, (get_tenant_id(), kind_norm, kind_norm, canonical_norm, canonical_norm, int(limit)))
        rows = cursor.fetchall()

    return [
        {
            "kind": row[0],
            "canonical_name": row[1],
            "term_text": row[2],
            "language": row[3],
            "source_type": row[4],
            "metadata": row[5] or {},
        }
        for row in rows
    ]


def get_semantic_terms_by_kind(
    connection: psycopg2.extensions.connection | str | None = None,
    kind: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Fetch semantic terms for a single kind, including category aliases.

    Accepts either get_semantic_terms_by_kind(kind="category") or
    get_semantic_terms_by_kind(connection, "category").
    """
    if isinstance(connection, str) and kind is None:
        kind = connection
        connection = None
    if kind is None and isinstance(connection, str):
        kind = connection
        connection = None

    if connection is None:
        seed_rows = _load_seeded_business_terms_from_file()
        if kind:
            seed_rows = [row for row in seed_rows if str(row.get("kind") or "").lower() == str(kind).strip().lower()]
        if seed_rows:
            return seed_rows[:limit]

    db_conn = connection if connection is not None else get_connection()
    try:
        rows = get_semantic_terms(connection=db_conn, kind=kind, limit=limit)
        seed_rows = _load_seeded_business_terms_from_file()
        if kind:
            seed_rows = [row for row in seed_rows if str(row.get("kind") or "").lower() == str(kind).strip().lower()]
        combined: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        # Tenant/database terms override an identical universal seed term.
        for row in rows + seed_rows:
            key = (
                str(row.get("kind") or "").strip().lower(),
                str(row.get("canonical_name") or "").strip().lower(),
                str(row.get("term_text") or "").strip().lower(),
            )
            if not key[1] or not key[2] or key in seen:
                continue
            seen.add(key)
            combined.append(row)
        return combined[:limit]
    finally:
        if connection is None:
            try:
                db_conn.close()
            except Exception:
                pass


def upsert_business_rule(
    connection: psycopg2.extensions.connection,
    rule_name: str,
    rule_text: str,
    structured_rule: dict[str, Any] | None = None,
    solf_script: str | None = None,
    scope: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
    created_by: str | None = None,
    configuration_scope: str = "tenant",
    tenant_id: str | None = None,
) -> int:
    """Insert or update a business rule row, respecting tenant-private vs published-global scope."""
    normalized_scope = str(configuration_scope or "tenant").strip().lower()
    if normalized_scope not in {"tenant", "global"}:
        raise ValueError("configuration_scope must be 'tenant' or 'global'")

    target_tenant_id = str(tenant_id or get_tenant_id()).strip() or LEGACY_TENANT_ID
    if normalized_scope == "global":
        target_tenant_id = LEGACY_TENANT_ID

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO business_rules (
                tenant_id, configuration_scope, rule_name, rule_text, structured_rule,
                solf_script, scope, metadata, is_active, created_by, created_at, modified_at
            )
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (tenant_id, configuration_scope, rule_name)
            DO UPDATE SET
                rule_text = EXCLUDED.rule_text,
                structured_rule = EXCLUDED.structured_rule,
                solf_script = EXCLUDED.solf_script,
                scope = EXCLUDED.scope,
                metadata = COALESCE(business_rules.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                is_active = EXCLUDED.is_active,
                modified_at = NOW()
            RETURNING rule_id
            """,
            (
                target_tenant_id,
                normalized_scope,
                str(rule_name or "").strip() or "business_rule",
                str(rule_text or "").strip(),
                Json(structured_rule or {}),
                str(solf_script or ""),
                Json(scope or {}),
                Json(metadata or {}),
                bool(is_active),
                str(created_by or "").strip() or None,
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return int(row[0])


def set_business_rule_active(
    connection: psycopg2.extensions.connection,
    rule_id: int,
    is_active: bool,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE business_rules
            SET is_active = %s,
                modified_at = NOW()
            WHERE rule_id = %s
            """,
            (bool(is_active), int(rule_id)),
        )
        updated = int(cursor.rowcount or 0)
    connection.commit()
    return updated > 0


def get_business_rules(
    connection: psycopg2.extensions.connection,
    is_active: bool | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    tenant_id = get_tenant_id()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                rule_id,
                rule_name,
                rule_text,
                structured_rule,
                solf_script,
                scope,
                metadata,
                is_active,
                created_by,
                created_at,
                modified_at
            FROM business_rules
            WHERE (%s IS NULL OR is_active = %s)
              AND (tenant_id = %s::uuid OR configuration_scope = 'global')
            ORDER BY CASE WHEN configuration_scope = 'tenant' AND tenant_id = %s::uuid THEN 0 ELSE 1 END,
                     modified_at DESC, rule_id DESC
            LIMIT %s
            """,
            (is_active, is_active, tenant_id, tenant_id, int(limit)),
        )
        rows = cursor.fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "rule_id": row[0],
                "rule_name": row[1],
                "rule_text": row[2],
                "structured_rule": row[3] or {},
                "solf_script": row[4] or "",
                "scope": row[5] or {},
                "metadata": row[6] or {},
                "is_active": bool(row[7]),
                "created_by": row[8],
                "created_at": row[9],
                "modified_at": row[10],
            }
        )
    return out


def get_active_business_rules_by_names(
    connection: psycopg2.extensions.connection,
    rule_names: list[str],
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_names = []
    for name in rule_names or []:
        token = str(name or "").strip().lower()
        if token and token not in normalized_names:
            normalized_names.append(token)

    if not normalized_names:
        return []

    tenant_id = get_tenant_id()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT ON (LOWER(TRIM(rule_name)))
                rule_id,
                rule_name,
                rule_text,
                structured_rule,
                solf_script,
                scope,
                metadata,
                is_active,
                created_by,
                created_at,
                modified_at
            FROM business_rules
            WHERE is_active = TRUE
              AND (tenant_id = %s::uuid OR configuration_scope = 'global')
              AND LOWER(TRIM(rule_name)) = ANY(%s)
              AND configuration_scope IN ('tenant', 'global')
            ORDER BY LOWER(TRIM(rule_name)),
                     CASE WHEN configuration_scope = 'tenant' AND tenant_id = %s::uuid THEN 0 ELSE 1 END,
                     modified_at DESC, rule_id DESC
            LIMIT %s
            """,
            (tenant_id, normalized_names, tenant_id, int(limit)),
        )
        rows = cursor.fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "rule_id": row[0],
                "rule_name": row[1],
                "rule_text": row[2],
                "structured_rule": row[3] or {},
                "solf_script": row[4] or "",
                "scope": row[5] or {},
                "metadata": row[6] or {},
                "is_active": bool(row[7]),
                "created_by": row[8],
                "created_at": row[9],
                "modified_at": row[10],
            }
        )
    return out


def get_business_rule_by_id(
    connection: psycopg2.extensions.connection,
    rule_id: int,
) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                rule_id,
                rule_name,
                rule_text,
                structured_rule,
                solf_script,
                scope,
                metadata,
                is_active,
                created_by,
                created_at,
                modified_at
            FROM business_rules
            WHERE rule_id = %s
            LIMIT 1
            """,
            (int(rule_id),),
        )
        row = cursor.fetchone()

    if row is None:
        return None

    return {
        "rule_id": row[0],
        "rule_name": row[1],
        "rule_text": row[2],
        "structured_rule": row[3] or {},
        "solf_script": row[4] or "",
        "scope": row[5] or {},
        "metadata": row[6] or {},
        "is_active": bool(row[7]),
        "created_by": row[8],
        "created_at": row[9],
        "modified_at": row[10],
    }


def update_business_rule(
    connection: psycopg2.extensions.connection,
    rule_id: int,
    rule_name: str,
    rule_text: str,
    structured_rule: dict[str, Any] | None = None,
    solf_script: str | None = None,
    scope: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE business_rules
            SET rule_name = %s,
                rule_text = %s,
                structured_rule = %s,
                solf_script = %s,
                scope = %s,
                metadata = %s,
                is_active = %s,
                modified_at = NOW()
            WHERE rule_id = %s
            """,
            (
                str(rule_name or "").strip() or "business_rule",
                str(rule_text or "").strip(),
                Json(structured_rule or {}),
                str(solf_script or ""),
                Json(scope or {}),
                Json(metadata or {}),
                bool(is_active),
                int(rule_id),
            ),
        )
        updated = int(cursor.rowcount or 0)
    connection.commit()
    return updated > 0


def upsert_solf_workflow_registry(
    connection: psycopg2.extensions.connection,
    workflow_key: str,
    workflow_name: str,
    description: str | None = None,
    domain: str | None = None,
    status: str = "draft",
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
    created_by: str | None = None,
    configuration_scope: str = "tenant",
    tenant_id: str | None = None,
) -> int:
    key = str(workflow_key or "").strip().lower()
    if not key:
        raise ValueError("workflow_key is required")

    normalized_scope = str(configuration_scope or "tenant").strip().lower()
    if normalized_scope not in {"tenant", "global"}:
        raise ValueError("configuration_scope must be 'tenant' or 'global'")

    valid_statuses = {"draft", "review", "published", "deprecated", "archived"}
    normalized_status = str(status or "draft").strip().lower()
    if normalized_status not in valid_statuses:
        normalized_status = "draft"

    target_tenant_id = str(tenant_id or get_tenant_id()).strip() or LEGACY_TENANT_ID
    if normalized_scope == "global":
        target_tenant_id = LEGACY_TENANT_ID

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO solf_workflow_registry (
                    tenant_id, configuration_scope, workflow_key, workflow_name, description, domain, status, metadata,
                is_active, created_by, created_at, modified_at
            )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (tenant_id, configuration_scope, workflow_key)
            DO UPDATE SET
                workflow_name = EXCLUDED.workflow_name,
                description = EXCLUDED.description,
                domain = EXCLUDED.domain,
                status = EXCLUDED.status,
                metadata = COALESCE(solf_workflow_registry.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                is_active = EXCLUDED.is_active,
                modified_at = NOW()
            RETURNING workflow_id
            """,
            (
                target_tenant_id,
                normalized_scope,
                key,
                str(workflow_name or key).strip(),
                str(description or "").strip(),
                str(domain or "").strip() or None,
                normalized_status,
                Json(metadata or {}),
                bool(is_active),
                str(created_by or "").strip() or None,
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return int(row[0])


def list_solf_workflow_registry(
    connection: psycopg2.extensions.connection,
    is_active: bool | None = None,
    domain: str | None = None,
    status: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    tenant_id = get_tenant_id()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT workflow_id, workflow_key, workflow_name, description, domain, status,
                   metadata, is_active, created_by, created_at, modified_at
            FROM solf_workflow_registry
            WHERE (%s IS NULL OR is_active = %s)
              AND (%s IS NULL OR domain = %s)
              AND (%s IS NULL OR status = %s)
              AND (tenant_id = %s::uuid OR configuration_scope = 'global')
            ORDER BY CASE WHEN configuration_scope = 'tenant' AND tenant_id = %s::uuid THEN 0 ELSE 1 END,
                     modified_at DESC, workflow_id DESC
            LIMIT %s
            """,
            (
                is_active,
                is_active,
                str(domain or "").strip() or None,
                str(domain or "").strip() or None,
                str(status or "").strip().lower() or None,
                str(status or "").strip().lower() or None,
                tenant_id,
                tenant_id,
                int(limit),
            ),
        )
        rows = cursor.fetchall()

    return [
        {
            "workflow_id": int(row[0]),
            "workflow_key": row[1],
            "workflow_name": row[2],
            "description": row[3] or "",
            "domain": row[4],
            "status": row[5],
            "metadata": row[6] or {},
            "is_active": bool(row[7]),
            "created_by": row[8],
            "created_at": row[9],
            "modified_at": row[10],
        }
        for row in rows
    ]


def get_solf_workflow_registry_by_id(
    connection: psycopg2.extensions.connection,
    workflow_id: int,
) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT workflow_id, workflow_key, workflow_name, description, domain, status,
                   metadata, is_active, created_by, created_at, modified_at
            FROM solf_workflow_registry
            WHERE workflow_id = %s
            LIMIT 1
            """,
            (int(workflow_id),),
        )
        row = cursor.fetchone()

    if row is None:
        return None

    return {
        "workflow_id": int(row[0]),
        "workflow_key": row[1],
        "workflow_name": row[2],
        "description": row[3] or "",
        "domain": row[4],
        "status": row[5],
        "metadata": row[6] or {},
        "is_active": bool(row[7]),
        "created_by": row[8],
        "created_at": row[9],
        "modified_at": row[10],
    }


def get_solf_workflow_registry_by_key(
    connection: psycopg2.extensions.connection,
    workflow_key: str,
) -> dict[str, Any] | None:
    normalized_key = str(workflow_key or "").strip()
    if not normalized_key:
        return None

    tenant_id = get_tenant_id()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT workflow_id, workflow_key, workflow_name, description, domain, status,
                   metadata, is_active, created_by, created_at, modified_at
            FROM solf_workflow_registry
            WHERE workflow_key = %s
              AND (tenant_id = %s::uuid OR configuration_scope = 'global')
            ORDER BY CASE WHEN configuration_scope = 'tenant' AND tenant_id = %s::uuid THEN 0 ELSE 1 END,
                     modified_at DESC, workflow_id DESC
            LIMIT 1
            """,
            (normalized_key, tenant_id, tenant_id),
        )
        row = cursor.fetchone()

    if row is None:
        return None

    return {
        "workflow_id": int(row[0]),
        "workflow_key": row[1],
        "workflow_name": row[2],
        "description": row[3] or "",
        "domain": row[4],
        "status": row[5],
        "metadata": row[6] or {},
        "is_active": bool(row[7]),
        "created_by": row[8],
        "created_at": row[9],
        "modified_at": row[10],
    }


def set_solf_workflow_registry_active(
    connection: psycopg2.extensions.connection,
    workflow_id: int,
    is_active: bool,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE solf_workflow_registry
            SET is_active = %s,
                modified_at = NOW()
            WHERE workflow_id = %s
            """,
            (bool(is_active), int(workflow_id)),
        )
        updated = int(cursor.rowcount or 0)
    connection.commit()
    return updated > 0


def create_solf_workflow_version(
    connection: psycopg2.extensions.connection,
    workflow_id: int,
    rule_id: int | None = None,
    graph_spec: dict[str, Any] | None = None,
    input_contract: dict[str, Any] | None = None,
    output_contract: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
    created_by: str | None = None,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COALESCE(MAX(version_no), 0) + 1 FROM solf_workflow_versions WHERE workflow_id = %s",
            (int(workflow_id),),
        )
        next_version_no = int((cursor.fetchone() or [1])[0] or 1)

        cursor.execute(
            """
            INSERT INTO solf_workflow_versions (
                workflow_id, version_no, rule_id, graph_spec, input_contract,
                output_contract, metadata, is_active, created_by, created_at, modified_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING workflow_version_id
            """,
            (
                int(workflow_id),
                next_version_no,
                int(rule_id) if rule_id is not None else None,
                Json(graph_spec or {}),
                Json(input_contract or {}),
                Json(output_contract or {}),
                Json(metadata or {}),
                bool(is_active),
                str(created_by or "").strip() or None,
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return int(row[0])


def list_solf_workflow_versions(
    connection: psycopg2.extensions.connection,
    workflow_id: int,
    is_active: bool | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT workflow_version_id, workflow_id, version_no, rule_id, graph_spec,
                   input_contract, output_contract, metadata, is_active, created_by,
                   created_at, modified_at
            FROM solf_workflow_versions
            WHERE workflow_id = %s
              AND (%s IS NULL OR is_active = %s)
            ORDER BY version_no DESC
            LIMIT %s
            """,
            (int(workflow_id), is_active, is_active, int(limit)),
        )
        rows = cursor.fetchall()

    return [
        {
            "workflow_version_id": int(row[0]),
            "workflow_id": int(row[1]),
            "version_no": int(row[2]),
            "rule_id": row[3],
            "graph_spec": row[4] or {},
            "input_contract": row[5] or {},
            "output_contract": row[6] or {},
            "metadata": row[7] or {},
            "is_active": bool(row[8]),
            "created_by": row[9],
            "created_at": row[10],
            "modified_at": row[11],
        }
        for row in rows
    ]


def replace_solf_workflow_steps(
    connection: psycopg2.extensions.connection,
    workflow_version_id: int,
    steps: list[dict[str, Any]],
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM solf_workflow_steps WHERE workflow_version_id = %s",
            (int(workflow_version_id),),
        )

        inserted = 0
        for index, step in enumerate(steps or [], start=1):
            step_key = str(step.get("step_key") or f"step_{index}").strip().lower()
            if not step_key:
                step_key = f"step_{index}"
            step_kind = str(step.get("step_kind") or "clause").strip().lower()
            if step_kind not in {"clause", "class_transform", "class_generate", "class_iterate", "python_binding"}:
                step_kind = "clause"

            cursor.execute(
                """
                INSERT INTO solf_workflow_steps (
                    workflow_version_id, step_order, step_key, step_kind, clause_id,
                    clause_name, input_class, output_class, operation, python_module,
                    python_function, config, created_at, modified_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (
                    int(workflow_version_id),
                    int(step.get("step_order") or index),
                    step_key,
                    step_kind,
                    int(step.get("clause_id")) if step.get("clause_id") is not None else None,
                    str(step.get("clause_name") or "").strip() or None,
                    str(step.get("input_class") or "").strip() or None,
                    str(step.get("output_class") or "").strip() or None,
                    str(step.get("operation") or "").strip() or None,
                    str(step.get("python_module") or "").strip() or None,
                    str(step.get("python_function") or "").strip() or None,
                    Json(step.get("config") if isinstance(step.get("config"), dict) else {}),
                ),
            )
            inserted += 1

    connection.commit()
    return inserted


def list_solf_workflow_steps(
    connection: psycopg2.extensions.connection,
    workflow_version_id: int,
) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT step_id, workflow_version_id, step_order, step_key, step_kind,
                   clause_id, clause_name, input_class, output_class, operation,
                   python_module, python_function, config, created_at, modified_at
            FROM solf_workflow_steps
            WHERE workflow_version_id = %s
            ORDER BY step_order ASC, step_id ASC
            """,
            (int(workflow_version_id),),
        )
        rows = cursor.fetchall()

    return [
        {
            "step_id": int(row[0]),
            "workflow_version_id": int(row[1]),
            "step_order": int(row[2]),
            "step_key": row[3],
            "step_kind": row[4],
            "clause_id": row[5],
            "clause_name": row[6],
            "input_class": row[7],
            "output_class": row[8],
            "operation": row[9],
            "python_module": row[10],
            "python_function": row[11],
            "config": row[12] or {},
            "created_at": row[13],
            "modified_at": row[14],
        }
        for row in rows
    ]


def upsert_workflow_extension_pack(
    connection: psycopg2.extensions.connection,
    extension_key: str,
    extension_name: str,
    description: str | None = None,
    scope_json: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
    created_by: str | None = None,
    configuration_scope: str = "tenant",
    tenant_id: str | None = None,
) -> int:
    key = str(extension_key or "").strip().lower()
    if not key:
        raise ValueError("extension_key is required")

    normalized_scope = str(configuration_scope or "tenant").strip().lower()
    if normalized_scope not in {"tenant", "global"}:
        raise ValueError("configuration_scope must be 'tenant' or 'global'")

    target_tenant_id = str(tenant_id or get_tenant_id()).strip() or LEGACY_TENANT_ID
    if normalized_scope == "global":
        target_tenant_id = LEGACY_TENANT_ID

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO workflow_extension_pack (
                    tenant_id, configuration_scope, extension_key, extension_name, description, scope_json, metadata,
                is_active, created_by, created_at, modified_at
            )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (tenant_id, configuration_scope, extension_key)
            DO UPDATE SET
                extension_name = EXCLUDED.extension_name,
                description = EXCLUDED.description,
                scope_json = COALESCE(workflow_extension_pack.scope_json, '{}'::jsonb) || EXCLUDED.scope_json,
                metadata = COALESCE(workflow_extension_pack.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                is_active = EXCLUDED.is_active,
                modified_at = NOW()
            RETURNING extension_pack_id
            """,
            (
                target_tenant_id,
                normalized_scope,
                key,
                str(extension_name or key).strip(),
                str(description or "").strip(),
                Json(scope_json or {}),
                Json(metadata or {}),
                bool(is_active),
                str(created_by or "").strip() or None,
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return int(row[0])


def get_workflow_extension_pack_by_key(
    connection: psycopg2.extensions.connection,
    extension_key: str,
) -> dict[str, Any] | None:
    key = str(extension_key or "").strip().lower()
    if not key:
        return None

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT extension_pack_id, extension_key, extension_name, description,
                   scope_json, metadata, is_active, created_by, created_at, modified_at
            FROM workflow_extension_pack
            WHERE extension_key = %s
            ORDER BY CASE WHEN configuration_scope = 'tenant' THEN 0 ELSE 1 END,
                     modified_at DESC, extension_pack_id DESC
            LIMIT 1
            """,
            (key,),
        )
        row = cursor.fetchone()

    if row is None:
        return None

    return {
        "extension_pack_id": int(row[0]),
        "extension_key": row[1],
        "extension_name": row[2],
        "description": row[3] or "",
        "scope_json": row[4] or {},
        "metadata": row[5] or {},
        "is_active": bool(row[6]),
        "created_by": row[7],
        "created_at": row[8],
        "modified_at": row[9],
    }


def list_workflow_extension_packs(
    connection: psycopg2.extensions.connection,
    is_active: bool | None = None,
    extension_key: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    normalized_key = str(extension_key or "").strip().lower() or None
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT extension_pack_id, extension_key, extension_name, description,
                   scope_json, metadata, is_active, created_by, created_at, modified_at
            FROM workflow_extension_pack
            WHERE (%s IS NULL OR is_active = %s)
              AND (%s IS NULL OR extension_key = %s)
                        ORDER BY CASE WHEN configuration_scope = 'tenant' THEN 0 ELSE 1 END,
                                         modified_at DESC, extension_pack_id DESC
            LIMIT %s
            """,
            (is_active, is_active, normalized_key, normalized_key, int(limit)),
        )
        rows = cursor.fetchall()

    return [
        {
            "extension_pack_id": int(row[0]),
            "extension_key": row[1],
            "extension_name": row[2],
            "description": row[3] or "",
            "scope_json": row[4] or {},
            "metadata": row[5] or {},
            "is_active": bool(row[6]),
            "created_by": row[7],
            "created_at": row[8],
            "modified_at": row[9],
        }
        for row in rows
    ]


def create_workflow_extension_version(
    connection: psycopg2.extensions.connection,
    extension_pack_id: int,
    status: str = "draft",
    conditions_schema_version: str = "v1",
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
    created_by: str | None = None,
) -> int:
    valid_statuses = {"draft", "review", "published", "deprecated", "archived"}
    normalized_status = str(status or "draft").strip().lower()
    if normalized_status not in valid_statuses:
        normalized_status = "draft"

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COALESCE(MAX(version_no), 0) + 1 FROM workflow_extension_version WHERE extension_pack_id = %s",
            (int(extension_pack_id),),
        )
        next_version_no = int((cursor.fetchone() or [1])[0] or 1)

        cursor.execute(
            """
            INSERT INTO workflow_extension_version (
                extension_pack_id, version_no, status, conditions_schema_version,
                metadata, is_active, created_by, created_at, modified_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING extension_version_id
            """,
            (
                int(extension_pack_id),
                int(next_version_no),
                normalized_status,
                str(conditions_schema_version or "v1").strip() or "v1",
                Json(metadata or {}),
                bool(is_active),
                str(created_by or "").strip() or None,
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return int(row[0])


def list_workflow_extension_versions(
    connection: psycopg2.extensions.connection,
    extension_pack_id: int,
    is_active: bool | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT extension_version_id, extension_pack_id, version_no, status,
                   conditions_schema_version, metadata, is_active, created_by,
                   created_at, modified_at
            FROM workflow_extension_version
            WHERE extension_pack_id = %s
              AND (%s IS NULL OR is_active = %s)
            ORDER BY version_no DESC, extension_version_id DESC
            LIMIT %s
            """,
            (int(extension_pack_id), is_active, is_active, int(limit)),
        )
        rows = cursor.fetchall()

    return [
        {
            "extension_version_id": int(row[0]),
            "extension_pack_id": int(row[1]),
            "version_no": int(row[2]),
            "status": row[3],
            "conditions_schema_version": row[4],
            "metadata": row[5] or {},
            "is_active": bool(row[6]),
            "created_by": row[7],
            "created_at": row[8],
            "modified_at": row[9],
        }
        for row in rows
    ]


def replace_workflow_extension_rules(
    connection: psycopg2.extensions.connection,
    extension_version_id: int,
    rules: list[dict[str, Any]],
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM workflow_extension_rule WHERE extension_version_id = %s",
            (int(extension_version_id),),
        )

        inserted = 0
        for index, rule in enumerate(rules or [], start=1):
            rule_key = str(rule.get("rule_key") or f"rule_{index}").strip().lower()
            if not rule_key:
                rule_key = f"rule_{index}"

            hook_point = str(rule.get("hook_point") or "after_step").strip().lower()
            if hook_point not in {"on_run_start", "before_step", "after_step", "on_pause", "on_failure", "on_complete"}:
                hook_point = "after_step"

            conflict_policy = str(rule.get("conflict_policy") or "skip").strip().lower()
            if conflict_policy not in {"skip", "replace", "merge", "fail"}:
                conflict_policy = "skip"

            cursor.execute(
                """
                INSERT INTO workflow_extension_rule (
                    extension_version_id, rule_key, hook_point, target_step_key,
                    condition_json, precedence, conflict_policy, inserted_steps_json,
                    metadata, is_active, created_at, modified_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (
                    int(extension_version_id),
                    rule_key,
                    hook_point,
                    str(rule.get("target_step_key") or "").strip() or None,
                    Json(rule.get("condition_json") if isinstance(rule.get("condition_json"), dict) else {}),
                    int(rule.get("precedence") or 100),
                    conflict_policy,
                    Json(rule.get("inserted_steps_json") if isinstance(rule.get("inserted_steps_json"), list) else []),
                    Json(rule.get("metadata") if isinstance(rule.get("metadata"), dict) else {}),
                    bool(rule.get("is_active", True)),
                ),
            )
            inserted += 1

    connection.commit()
    return inserted


def list_workflow_extension_rules(
    connection: psycopg2.extensions.connection,
    extension_version_id: int,
    is_active: bool | None = None,
) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT extension_rule_id, extension_version_id, rule_key, hook_point,
                   target_step_key, condition_json, precedence, conflict_policy,
                   inserted_steps_json, metadata, is_active, created_at, modified_at
            FROM workflow_extension_rule
            WHERE extension_version_id = %s
              AND (%s IS NULL OR is_active = %s)
            ORDER BY precedence ASC, extension_rule_id ASC
            """,
            (int(extension_version_id), is_active, is_active),
        )
        rows = cursor.fetchall()

    return [
        {
            "extension_rule_id": int(row[0]),
            "extension_version_id": int(row[1]),
            "rule_key": row[2],
            "hook_point": row[3],
            "target_step_key": row[4],
            "condition_json": row[5] or {},
            "precedence": int(row[6]),
            "conflict_policy": row[7],
            "inserted_steps_json": row[8] or [],
            "metadata": row[9] or {},
            "is_active": bool(row[10]),
            "created_at": row[11],
            "modified_at": row[12],
        }
        for row in rows
    ]


def create_workflow_extension_attachment(
    connection: psycopg2.extensions.connection,
    workflow_key: str,
    workflow_version_id: int | None,
    extension_pack_id: int,
    extension_version_id: int | None,
    effective_from: str | None = None,
    effective_until: str | None = None,
    priority: int = 100,
    tenant_scope: str | None = None,
    metadata: dict[str, Any] | None = None,
    is_active: bool = True,
    created_by: str | None = None,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO workflow_extension_attachment (
                workflow_key, workflow_version_id, extension_pack_id, extension_version_id,
                effective_from, effective_until, priority, tenant_scope, metadata,
                is_active, created_by, created_at, modified_at
            )
            VALUES (
                %s, %s, %s, %s,
                COALESCE(%s::date, CURRENT_DATE), %s::date, %s, %s, %s,
                %s, %s, NOW(), NOW()
            )
            RETURNING attachment_id
            """,
            (
                str(workflow_key or "").strip().lower(),
                int(workflow_version_id) if workflow_version_id is not None else None,
                int(extension_pack_id),
                int(extension_version_id) if extension_version_id is not None else None,
                str(effective_from or "").strip() or None,
                str(effective_until or "").strip() or None,
                int(priority),
                str(tenant_scope or "").strip() or None,
                Json(metadata or {}),
                bool(is_active),
                str(created_by or "").strip() or None,
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return int(row[0])


def list_workflow_extension_attachments(
    connection: psycopg2.extensions.connection,
    workflow_key: str | None = None,
    workflow_version_id: int | None = None,
    extension_pack_id: int | None = None,
    tenant_scope: str | None = None,
    is_active: bool | None = None,
    only_effective_now: bool = False,
    limit: int = 500,
) -> list[dict[str, Any]]:
    normalized_workflow_key = str(workflow_key or "").strip().lower() or None
    normalized_tenant_scope = str(tenant_scope or "").strip() or None

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT attachment_id, workflow_key, workflow_version_id, extension_pack_id,
                   extension_version_id, effective_from, effective_until, priority,
                   tenant_scope, metadata, is_active, created_by, created_at, modified_at
            FROM workflow_extension_attachment
            WHERE (%s IS NULL OR workflow_key = %s)
              AND (%s IS NULL OR workflow_version_id = %s)
              AND (%s IS NULL OR extension_pack_id = %s)
              AND (%s IS NULL OR tenant_scope = %s)
              AND (%s IS NULL OR is_active = %s)
              AND (
                    %s = FALSE OR (
                        effective_from <= CURRENT_DATE
                        AND (effective_until IS NULL OR effective_until >= CURRENT_DATE)
                    )
                  )
            ORDER BY priority ASC, attachment_id ASC
            LIMIT %s
            """,
            (
                normalized_workflow_key,
                normalized_workflow_key,
                int(workflow_version_id) if workflow_version_id is not None else None,
                int(workflow_version_id) if workflow_version_id is not None else None,
                int(extension_pack_id) if extension_pack_id is not None else None,
                int(extension_pack_id) if extension_pack_id is not None else None,
                normalized_tenant_scope,
                normalized_tenant_scope,
                is_active,
                is_active,
                bool(only_effective_now),
                int(limit),
            ),
        )
        rows = cursor.fetchall()

    return [
        {
            "attachment_id": int(row[0]),
            "workflow_key": row[1],
            "workflow_version_id": row[2],
            "extension_pack_id": int(row[3]),
            "extension_version_id": row[4],
            "effective_from": row[5],
            "effective_until": row[6],
            "priority": int(row[7]),
            "tenant_scope": row[8],
            "metadata": row[9] or {},
            "is_active": bool(row[10]),
            "created_by": row[11],
            "created_at": row[12],
            "modified_at": row[13],
        }
        for row in rows
    ]


def set_workflow_extension_attachment_active(
    connection: psycopg2.extensions.connection,
    attachment_id: int,
    is_active: bool,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE workflow_extension_attachment
            SET is_active = %s,
                modified_at = NOW()
            WHERE attachment_id = %s
            """,
            (bool(is_active), int(attachment_id)),
        )
        updated = int(cursor.rowcount or 0)
    connection.commit()
    return updated > 0


def replace_workflow_resource_aliases(
    connection: psycopg2.extensions.connection,
    workflow_id: int,
    workflow_version_id: int,
    aliases: list[dict[str, Any]],
    created_by: str | None = None,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM workflow_resource_alias WHERE workflow_version_id = %s",
            (int(workflow_version_id),),
        )

        inserted = 0
        for alias in aliases or []:
            resource_type = str(alias.get("resource_type") or "").strip().lower()
            if resource_type not in {"endpoint", "queue", "collection", "purpose", "scope", "policy"}:
                continue
            alias_name = str(alias.get("alias_name") or "").strip()
            canonical_name = str(alias.get("canonical_name") or "").strip()
            if not alias_name or not canonical_name:
                continue

            cursor.execute(
                """
                INSERT INTO workflow_resource_alias (
                    workflow_id, workflow_version_id, resource_type, alias_name,
                    canonical_name, metadata, created_by, created_at, modified_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (
                    int(workflow_id),
                    int(workflow_version_id),
                    resource_type,
                    alias_name,
                    canonical_name,
                    Json(alias.get("metadata") if isinstance(alias.get("metadata"), dict) else {}),
                    str(created_by or "").strip() or None,
                ),
            )
            inserted += 1

    connection.commit()
    return inserted


def list_workflow_resource_aliases(
    connection: psycopg2.extensions.connection,
    workflow_id: int | None = None,
    workflow_version_id: int | None = None,
    resource_type: str | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []

    if workflow_id is not None:
        filters.append("workflow_id = %s")
        params.append(int(workflow_id))
    if workflow_version_id is not None:
        filters.append("workflow_version_id = %s")
        params.append(int(workflow_version_id))
    if resource_type:
        filters.append("resource_type = %s")
        params.append(str(resource_type).strip().lower())

    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT alias_id, workflow_id, workflow_version_id, resource_type,
                   alias_name, canonical_name, metadata, created_by,
                   created_at, modified_at
            FROM workflow_resource_alias
            {where_sql}
            ORDER BY resource_type ASC, alias_name ASC, alias_id ASC
            """,
            tuple(params),
        )
        rows = cursor.fetchall()

    return [
        {
            "alias_id": int(row[0]),
            "workflow_id": int(row[1]),
            "workflow_version_id": int(row[2]),
            "resource_type": row[3],
            "alias_name": row[4],
            "canonical_name": row[5],
            "metadata": row[6] or {},
            "created_by": row[7],
            "created_at": row[8],
            "modified_at": row[9],
        }
        for row in rows
    ]


def upsert_business_rule_workflow_link(
    connection: psycopg2.extensions.connection,
    rule_id: int,
    workflow_id: int,
    workflow_version_id: int | None = None,
    link_type: str = "uses",
    metadata: dict[str, Any] | None = None,
    created_by: str | None = None,
) -> int:
    normalized_link_type = str(link_type or "uses").strip().lower()
    if normalized_link_type not in {"uses", "creates", "extends", "overrides"}:
        normalized_link_type = "uses"

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO business_rule_workflow_links (
                rule_id, workflow_id, workflow_version_id, link_type, metadata,
                created_by, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (rule_id, workflow_id, workflow_version_id, link_type)
            DO UPDATE SET
                metadata = COALESCE(business_rule_workflow_links.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                created_by = COALESCE(EXCLUDED.created_by, business_rule_workflow_links.created_by)
            RETURNING link_id
            """,
            (
                int(rule_id),
                int(workflow_id),
                int(workflow_version_id) if workflow_version_id is not None else None,
                normalized_link_type,
                Json(metadata or {}),
                str(created_by or "").strip() or None,
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return int(row[0])


def list_business_rule_workflow_links(
    connection: psycopg2.extensions.connection,
    rule_id: int,
) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT l.link_id, l.rule_id, l.workflow_id, l.workflow_version_id, l.link_type,
                   l.metadata, l.created_by, l.created_at,
                   w.workflow_key, w.workflow_name,
                   v.version_no
            FROM business_rule_workflow_links l
            JOIN solf_workflow_registry w ON w.workflow_id = l.workflow_id
            LEFT JOIN solf_workflow_versions v ON v.workflow_version_id = l.workflow_version_id
            WHERE l.rule_id = %s
            ORDER BY l.created_at DESC, l.link_id DESC
            """,
            (int(rule_id),),
        )
        rows = cursor.fetchall()

    return [
        {
            "link_id": int(row[0]),
            "rule_id": int(row[1]),
            "workflow_id": int(row[2]),
            "workflow_version_id": row[3],
            "link_type": row[4],
            "metadata": row[5] or {},
            "created_by": row[6],
            "created_at": row[7],
            "workflow_key": row[8],
            "workflow_name": row[9],
            "version_no": row[10],
        }
        for row in rows
    ]


def get_solf_clauses_for_business_rule(
    connection: psycopg2.extensions.connection,
    rule_id: int,
) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT clause_id, clause_name, clause_type, entity_class, clause_body, metadata
            FROM solf_clauses
            WHERE COALESCE(metadata->>'source', '') = 'business_rule_runtime'
              AND COALESCE(metadata->>'rule_id', '') = %s
            ORDER BY clause_name ASC
            """,
            (str(int(rule_id)),),
        )
        rows = cursor.fetchall()

    return [
        {
            "clause_id": int(row[0]),
            "clause_name": row[1],
            "clause_type": row[2],
            "entity_class": row[3],
            "clause_body": row[4],
            "metadata": row[5] or {},
        }
        for row in rows
    ]


def set_solf_workflow_version_status(
    connection: psycopg2.extensions.connection,
    workflow_version_id: int,
    status: str,
) -> bool:
    valid_statuses = {"draft", "review", "published", "deprecated", "archived"}
    normalized_status = str(status or "draft").strip().lower()
    if normalized_status not in valid_statuses:
        normalized_status = "draft"

    with connection.cursor() as cursor:
        is_active = normalized_status == "published"
        cursor.execute(
            """
            UPDATE solf_workflow_versions
            SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                is_active = %s,
                modified_at = NOW()
            WHERE workflow_version_id = %s
            """,
            (
                Json({"status": normalized_status, "status_updated_at": datetime.utcnow().isoformat()}),
                is_active,
                int(workflow_version_id),
            ),
        )
        updated = int(cursor.rowcount or 0)
    connection.commit()
    return updated > 0


def get_workflow_active_version(
    connection: psycopg2.extensions.connection,
    workflow_id: int,
) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT workflow_version_id, workflow_id, version_no, rule_id, graph_spec,
                   input_contract, output_contract, metadata, is_active, created_by,
                   created_at, modified_at
            FROM solf_workflow_versions
            WHERE workflow_id = %s
              AND is_active = TRUE
            ORDER BY version_no DESC
            LIMIT 1
            """,
            (int(workflow_id),),
        )
        row = cursor.fetchone()

    if row is None:
        return None

    return {
        "workflow_version_id": int(row[0]),
        "workflow_id": int(row[1]),
        "version_no": int(row[2]),
        "rule_id": row[3],
        "graph_spec": row[4] or {},
        "input_contract": row[5] or {},
        "output_contract": row[6] or {},
        "metadata": row[7] or {},
        "is_active": bool(row[8]),
        "created_by": row[9],
        "created_at": row[10],
        "modified_at": row[11],
    }


def deactivate_workflow_versions_except(
    connection: psycopg2.extensions.connection,
    workflow_id: int,
    except_version_id: int | None = None,
) -> int:
    with connection.cursor() as cursor:
        if except_version_id is not None:
            cursor.execute(
                """
                UPDATE solf_workflow_versions
                SET is_active = FALSE,
                    modified_at = NOW()
                WHERE workflow_id = %s
                  AND workflow_version_id <> %s
                  AND is_active = TRUE
                """,
                (int(workflow_id), int(except_version_id)),
            )
        else:
            cursor.execute(
                """
                UPDATE solf_workflow_versions
                SET is_active = FALSE,
                    modified_at = NOW()
                WHERE workflow_id = %s
                  AND is_active = TRUE
                """,
                (int(workflow_id),),
            )
        deactivated = int(cursor.rowcount or 0)
    connection.commit()
    return deactivated


def get_temporal_attributes_as_of(
    connection: psycopg2.extensions.connection,
    src_id: int,
    src_type: str,
    as_of: date | str | None = None,
) -> dict[str, Any]:
    as_of_date = _coerce_date(as_of) or date.today()
    normalized_src_type = str(src_type).strip().lower()

    sql = """
    SELECT attr_type, attr_json, valid_from, valid_until
    FROM attribute
    WHERE src_id = %s
      AND src_type = %s
      AND valid_from <= %s
      AND (valid_until IS NULL OR valid_until > %s)
    ORDER BY attr_type, valid_from DESC, attr_id DESC
    """

    values: dict[str, Any] = {}
    with connection.cursor() as cursor:
        cursor.execute(sql, (int(src_id), normalized_src_type, as_of_date, as_of_date))
        rows = cursor.fetchall()

    for attr_type, attr_json, _valid_from, _valid_until in rows:
        attr_key = str(attr_type).strip().lower()
        if attr_key in values:
            continue
        payload = attr_json if isinstance(attr_json, dict) else {"value": attr_json}
        if isinstance(payload, dict) and "value" in payload and len(payload) == 1:
            values[attr_key] = payload.get("value")
        else:
            values[attr_key] = payload
    return values


def get_current_temporal_attributes(
    connection: psycopg2.extensions.connection,
    src_id: int,
    src_type: str,
) -> dict[str, Any]:
    return get_temporal_attributes_as_of(
        connection=connection,
        src_id=src_id,
        src_type=src_type,
        as_of=date.today(),
    )


def get_attribute_history(
    connection: psycopg2.extensions.connection,
    src_id: int,
    src_type: str,
    attr_type: str | None = None,
) -> list[dict[str, Any]]:
    normalized_src_type = str(src_type).strip().lower()
    normalized_attr_type = str(attr_type).strip().lower() if attr_type is not None else None

    sql = """
    SELECT attr_id, src_id, src_type, attr_type, valid_from, valid_until, attr_json, entry_date
    FROM attribute
    WHERE src_id = %s
      AND src_type = %s
      AND (%s IS NULL OR attr_type = %s)
    ORDER BY attr_type, valid_from DESC, attr_id DESC
    """

    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                int(src_id),
                normalized_src_type,
                normalized_attr_type,
                normalized_attr_type,
            ),
        )
        rows = cursor.fetchall()

    return [
        {
            "attr_id": int(row[0]),
            "src_id": int(row[1]),
            "src_type": row[2],
            "attr_type": row[3],
            "valid_from": row[4],
            "valid_until": row[5],
            "attr_json": row[6] or {},
            "entry_date": row[7],
        }
        for row in rows
    ]


def get_active_relationships_as_of(
    connection: psycopg2.extensions.connection,
    as_of: date | str | None = None,
    relationship_name: str | None = None,
    relationship_cat: str | None = None,
    src_object_id: int | None = None,
    tar_object_id: int | None = None,
) -> list[dict[str, Any]]:
    as_of_date = _coerce_date(as_of) or date.today()

    sql = """
    SELECT
        rel.relationship_id,
        rel.relationship_name,
        rel.relationship_cat,
        rel.src_object_id,
        rel.tar_object_id,
        rel.confidence,
        rel.metadata,
        rel.entry_date,
        src.object_name,
        src.class_name,
        tar.object_name,
        tar.class_name
    FROM object_relationship rel
    JOIN object_instance src ON src.object_id = rel.src_object_id
    JOIN object_instance tar ON tar.object_id = rel.tar_object_id
    WHERE (%s IS NULL OR rel.relationship_name = %s)
      AND (%s IS NULL OR rel.relationship_cat = %s)
      AND (%s IS NULL OR rel.src_object_id = %s)
      AND (%s IS NULL OR rel.tar_object_id = %s)
    ORDER BY rel.relationship_id
    """

    results: list[dict[str, Any]] = []
    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                relationship_name,
                relationship_name,
                relationship_cat,
                relationship_cat,
                src_object_id,
                src_object_id,
                tar_object_id,
                tar_object_id,
            ),
        )
        rows = cursor.fetchall()

    for row in rows:
        rel_id = int(row[0])
        rel_attrs = get_temporal_attributes_as_of(
            connection=connection,
            src_id=rel_id,
            src_type="relationship",
            as_of=as_of_date,
        )
        is_active = bool(rel_attrs.get("active", False))
        if not is_active:
            continue

        results.append(
            {
                "relationship_id": rel_id,
                "relationship_name": row[1],
                "relationship_cat": row[2],
                "src_object_id": int(row[3]),
                "tar_object_id": int(row[4]),
                "confidence": row[5],
                "metadata": row[6] or {},
                "entry_date": row[7],
                "src_object_name": row[8],
                "src_class_name": row[9],
                "tar_object_name": row[10],
                "tar_class_name": row[11],
                "temporal_attributes": rel_attrs,
            }
        )

    return results


def get_relationship_timeline(
    connection: psycopg2.extensions.connection,
    relationship_name: str | None = None,
    relationship_cat: str | None = None,
    src_object_id: int | None = None,
    tar_object_id: int | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    sql = """
    SELECT
        rel.relationship_id,
        rel.relationship_name,
        rel.relationship_cat,
        rel.src_object_id,
        rel.tar_object_id,
        rel.confidence,
        rel.metadata,
        rel.entry_date,
        src.object_name,
        src.class_name,
        tar.object_name,
        tar.class_name
    FROM object_relationship rel
    JOIN object_instance src ON src.object_id = rel.src_object_id
    JOIN object_instance tar ON tar.object_id = rel.tar_object_id
    WHERE (%s IS NULL OR rel.relationship_name = %s)
      AND (%s IS NULL OR rel.relationship_cat = %s)
      AND (%s IS NULL OR rel.src_object_id = %s)
      AND (%s IS NULL OR rel.tar_object_id = %s)
    ORDER BY rel.relationship_id DESC
    LIMIT %s
    """

    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                relationship_name,
                relationship_name,
                relationship_cat,
                relationship_cat,
                src_object_id,
                src_object_id,
                tar_object_id,
                tar_object_id,
                limit,
            ),
        )
        rows = cursor.fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        relationship_id = int(row[0])
        history = get_attribute_history(
            connection=connection,
            src_id=relationship_id,
            src_type="relationship",
        )
        active_periods = [
            {
                "valid_from": item.get("valid_from"),
                "valid_until": item.get("valid_until"),
                "active": bool((item.get("attr_json") or {}).get("value") if isinstance(item.get("attr_json"), dict) else item.get("attr_json")),
            }
            for item in history
            if str(item.get("attr_type") or "").lower() == "active"
        ]

        results.append(
            {
                "relationship_id": relationship_id,
                "relationship_name": row[1],
                "relationship_cat": row[2],
                "src_object_id": int(row[3]),
                "tar_object_id": int(row[4]),
                "confidence": row[5],
                "metadata": row[6] or {},
                "entry_date": row[7],
                "src_object_name": row[8],
                "src_class_name": row[9],
                "tar_object_name": row[10],
                "tar_class_name": row[11],
                "active_periods": active_periods,
                "attribute_history": history,
            }
        )

    return results


def build_children_snapshot(
    children_rows: list[dict[str, Any]],
    date_key_fields: tuple[str, ...] = ("birth_date_key", "birth_date", "dob", "date_of_birth", "birthdate"),
) -> list[tuple[str, int | None]]:
    snapshot: list[tuple[str, int | None]] = []
    for child_row in children_rows:
        child_name = str(child_row.get("src_object_name") or child_row.get("object_name") or "").strip()
        if not child_name:
            continue

        child_metadata = child_row.get("src_metadata") or child_row.get("metadata") or {}
        relationship_metadata = child_row.get("relationship_metadata") or {}
        date_key = _extract_date_key(child_metadata, date_key_fields)
        if date_key is None:
            date_key = _extract_date_key(relationship_metadata, date_key_fields)
        snapshot.append((child_name, date_key))
    return snapshot


def seed_children_snapshot_policy(
    interpreter: Any,
    parent_ref: Any,
    children_rows: list[dict[str, Any]],
    date_key_fields: tuple[str, ...] = ("birth_date_key", "birth_date", "dob", "date_of_birth", "birthdate"),
    evaluate_clause: bool = True,
) -> dict[str, Any]:
    if not hasattr(interpreter, "variables"):
        raise TypeError("seed_children_snapshot_policy() requires an interpreter with variables")

    snapshot = build_children_snapshot(children_rows, date_key_fields=date_key_fields)
    parent_key = parent_ref.strip() if isinstance(parent_ref, str) else parent_ref

    variables = interpreter.variables
    children_snapshot_map = variables.setdefault("children_snapshot_by_parent_ref", {})
    children_count_map = variables.setdefault("children_count_by_parent_ref", {})
    children_snapshot_map[parent_key] = snapshot
    children_count_map[parent_key] = len(snapshot)

    result: dict[str, Any] = {
        "parent_ref": parent_key,
        "children_snapshot": snapshot,
        "children_count": len(snapshot),
    }

    if evaluate_clause and hasattr(interpreter, "parser") and hasattr(interpreter, "execute_predicate"):
        clause_expression = f"child_allowance_input_complete({parent_key})"
        ast = interpreter.parser.parse(clause_expression)
        result["input_complete"] = bool(interpreter.execute_predicate(ast))

    return result


def load_children_snapshot_policy(
    connection: psycopg2.extensions.connection,
    interpreter: Any,
    parent_ref: Any,
    parent_object_name: str,
    parent_class_name: str | None = None,
    relationship_name: str = "child_of",
    relationship_cat: str | None = None,
    date_key_fields: tuple[str, ...] = ("birth_date_key", "birth_date", "dob", "date_of_birth", "birthdate"),
    evaluate_clause: bool = True,
) -> dict[str, Any]:
    children_rows = get_relationships(
        connection=connection,
        relationship_name=relationship_name,
        relationship_cat=relationship_cat,
        tar_object_name=parent_object_name,
        tar_class_name=parent_class_name,
    )

    result = seed_children_snapshot_policy(
        interpreter=interpreter,
        parent_ref=parent_ref,
        children_rows=children_rows,
        date_key_fields=date_key_fields,
        evaluate_clause=evaluate_clause,
    )
    result["children_rows"] = children_rows
    return result


def get_object_instance(
    connection: psycopg2.extensions.connection,
    object_name: str,
    class_name: str | None = None,
        as_of_date: date | None = None,
        include_inactive: bool = False,
) -> dict[str, Any] | None:
    sql = """
    SELECT object_id, object_name, canonical_full_name, class_name, metadata, status, valid_from, valid_until, entry_date
    FROM object_instance
    WHERE (LOWER(object_name) = LOWER(%s) OR LOWER(COALESCE(canonical_full_name, '')) = LOWER(%s))
      AND (%s IS NULL OR class_name = %s)
      AND (%s OR COALESCE(status, 'active') = 'active')
      AND (valid_from <= %s)
      AND (valid_until IS NULL OR valid_until > %s)
    ORDER BY entry_date DESC
    LIMIT 1
    """

    with connection.cursor() as cursor:
        effective_date = as_of_date or date.today()
        cursor.execute(
            sql,
            (object_name, object_name, class_name, class_name, include_inactive, effective_date, effective_date),
        )
        row = cursor.fetchone()

    if not row:
        return None

    return {
        "object_id": row[0],
        "object_name": row[1],
        "canonical_full_name": row[2],
        "class_name": row[3],
        "metadata": row[4] or {},
        "status": row[5],
        "valid_from": row[6],
        "valid_until": row[7],
        "entry_date": row[8],
    }


def get_object_instance_by_id(
    connection: psycopg2.extensions.connection,
    object_id: int,
    as_of_date: date | None = None,
    include_inactive: bool = False,
) -> dict[str, Any] | None:
    object_id = int(object_id)
    
    # Resolve merged objects to canonical form
    canonical_id = resolve_merged_object_to_canonical(connection, object_id)
    if canonical_id != object_id:
        # Object was merged, use canonical ID instead
        object_id = canonical_id
    
    sql = """
    SELECT object_id, object_name, canonical_full_name, class_name, metadata, status, valid_from, valid_until, entry_date
    FROM object_instance
    WHERE object_id = %s
      AND (%s OR COALESCE(status, 'active') = 'active')
      AND (valid_from <= %s)
      AND (valid_until IS NULL OR valid_until > %s)
    LIMIT 1
    """

    with connection.cursor() as cursor:
        effective_date = as_of_date or date.today()
        cursor.execute(sql, (int(object_id), include_inactive, effective_date, effective_date))
        row = cursor.fetchone()

    if not row:
        return None

    return {
        "object_id": row[0],
        "object_name": row[1],
        "canonical_full_name": row[2],
        "class_name": row[3],
        "metadata": row[4] or {},
        "status": row[5],
        "valid_from": row[6],
        "valid_until": row[7],
        "entry_date": row[8],
    }


def get_alias_group_object_ids(
    connection: psycopg2.extensions.connection,
    object_id: int,
    include_self: bool = True,
) -> list[int]:
    _ensure_object_alias_table(connection)

    sql = """
    WITH RECURSIVE alias_graph AS (
        SELECT %s::BIGINT AS object_id
        UNION
        SELECT CASE
            WHEN oa.primary_object_id = g.object_id THEN oa.alias_object_id
            ELSE oa.primary_object_id
        END AS object_id
        FROM object_alias oa
        JOIN alias_graph g
          ON oa.primary_object_id = g.object_id
          OR oa.alias_object_id = g.object_id
        WHERE COALESCE(oa.status, 'active') = 'active'
    )
    SELECT DISTINCT object_id
    FROM alias_graph
    ORDER BY object_id
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, (int(object_id),))
        rows = [int(row[0]) for row in cursor.fetchall() or []]

    if include_self:
        return rows if rows else [int(object_id)]
    return [item for item in rows if int(item) != int(object_id)]


def get_alias_group_for_name(
    connection: psycopg2.extensions.connection,
    object_name: str,
    class_name: str | None = None,
) -> dict[str, Any] | None:
    seed = get_object_instance(connection, object_name=object_name, class_name=class_name)
    if not seed:
        return None

    object_ids = get_alias_group_object_ids(connection, int(seed["object_id"]), include_self=True)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT object_id, object_name, canonical_full_name, class_name, metadata
            FROM object_instance
            WHERE object_id = ANY(%s)
            ORDER BY object_name, object_id
            """,
            (object_ids,),
        )
        rows = cursor.fetchall() or []

    members = [
        {
            "object_id": int(row[0]),
            "object_name": row[1],
            "canonical_full_name": row[2],
            "class_name": row[3],
            "metadata": row[4] if isinstance(row[4], dict) else {},
        }
        for row in rows
    ]
    return {
        "seed_object_id": int(seed["object_id"]),
        "seed_object_name": seed["object_name"],
        "class_name": seed["class_name"],
        "object_ids": object_ids,
        "members": members,
    }


def resolve_merged_object_to_canonical(
    connection: psycopg2.extensions.connection,
    object_id: int,
) -> int:
    """Resolve a merged object to its canonical object via merged_record alias link.
    
    If the object is merged, follows the merged_record alias to find the canonical.
    If not merged or no alias found, returns the original object_id.
    
    Args:
        connection: Database connection
        object_id: Object ID to resolve
        
    Returns:
        The canonical object_id (same as input if not merged, or the primary_object_id of the merged_record alias)
    """
    object_id = int(object_id)
    
    # Check if object is merged
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT status
            FROM object_instance
            WHERE object_id = %s
            LIMIT 1
            """,
            (object_id,)
        )
        row = cursor.fetchone()
    
    if not row or row[0] != 'merged':
        return object_id
    
    # Object is merged, look for merged_record alias link
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT primary_object_id
            FROM object_alias
            WHERE alias_object_id = %s
              AND alias_type = 'merged_record'
              AND COALESCE(status, 'active') = 'active'
            LIMIT 1
            """,
            (object_id,)
        )
        row = cursor.fetchone()
    
    if row:
        canonical_id = int(row[0])
        return canonical_id
    
    # No merged_record alias found, return original
    return object_id


def suggest_similar_objects(
    connection: psycopg2.extensions.connection,
    object_name: str,
    class_name: str = "person",
    limit: int = 8,
    min_similarity: float = 0.12,
) -> list[dict[str, Any]]:
    candidates = search_objects(
        connection=connection,
        name_query=object_name,
        class_name=class_name,
        limit=max(3, int(limit)),
    )
    out: list[dict[str, Any]] = []
    normalized_hint = _canonical_person_full_name(object_name) if str(class_name).strip().lower() == "person" else str(object_name or "").strip()
    hint_tokens = [tok for tok in re.findall(r"[A-Za-z0-9ÄÖÜäöüß]+", normalized_hint.lower()) if tok]

    for candidate in candidates:
        similarity = float(candidate.get("similarity") or 0.0)
        if similarity < float(min_similarity):
            continue
        candidate_name = str(candidate.get("object_name") or "").strip()
        candidate_tokens = [tok for tok in re.findall(r"[A-Za-z0-9ÄÖÜäöüß]+", candidate_name.lower()) if tok]
        overlap = sorted(set(hint_tokens).intersection(candidate_tokens))
        out.append(
            {
                "object_id": int(candidate.get("object_id") or 0),
                "object_name": candidate_name,
                "class_name": candidate.get("class_name"),
                "similarity": similarity,
                "token_overlap": overlap,
                "overlap_count": len(overlap),
                "metadata": candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {},
            }
        )

    out.sort(key=lambda item: (-float(item.get("similarity") or 0.0), -int(item.get("overlap_count") or 0), item.get("object_name") or ""))
    return out[: max(1, int(limit))]


def link_object_alias(
    connection: psycopg2.extensions.connection,
    primary_object_id: int,
    alias_object_id: int,
    alias_type: str = "same_entity",
    confidence: float | None = None,
    metadata: dict[str, Any] | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    _ensure_object_alias_table(connection)

    primary = int(primary_object_id)
    alias = int(alias_object_id)
    if primary == alias:
        raise ValueError("primary_object_id and alias_object_id must be different")

    left = min(primary, alias)
    right = max(primary, alias)
    payload = metadata or {}

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO object_alias (
                primary_object_id,
                alias_object_id,
                alias_type,
                confidence,
                status,
                metadata
            )
            VALUES (%s, %s, %s, %s, 'active', %s::jsonb)
            ON CONFLICT (primary_object_id, alias_object_id)
            DO UPDATE
            SET alias_type = EXCLUDED.alias_type,
                confidence = COALESCE(EXCLUDED.confidence, object_alias.confidence),
                status = 'active',
                metadata = COALESCE(object_alias.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                updated_at = NOW()
            RETURNING alias_id, primary_object_id, alias_object_id, alias_type, confidence, status, metadata, created_at, updated_at
            """,
            (left, right, str(alias_type or "same_entity").strip().lower(), confidence, json.dumps(payload)),
        )
        row = cursor.fetchone()

    if commit:
        connection.commit()

    return {
        "alias_id": int(row[0]),
        "primary_object_id": int(row[1]),
        "alias_object_id": int(row[2]),
        "alias_type": row[3],
        "confidence": row[4],
        "status": row[5],
        "metadata": row[6] if isinstance(row[6], dict) else {},
        "created_at": row[7],
        "updated_at": row[8],
    }


def merge_object_instances(
    connection: psycopg2.extensions.connection,
    canonical_object_id: int,
    duplicate_object_id: int,
    merge_note: str = "",
    keep_alias_link: bool = True,
) -> dict[str, Any]:
    canonical = int(canonical_object_id)
    duplicate = int(duplicate_object_id)
    if canonical == duplicate:
        raise ValueError("canonical_object_id and duplicate_object_id must be different")

    canonical_row = get_object_instance_by_id(connection, canonical, include_inactive=True)
    duplicate_row = get_object_instance_by_id(connection, duplicate, include_inactive=True)
    if not canonical_row or not duplicate_row:
        raise ValueError("Both canonical and duplicate object ids must exist")
    if str(canonical_row.get("class_name") or "") != str(duplicate_row.get("class_name") or ""):
        raise ValueError("Cannot merge objects with different class_name values")

    rels_retargeted = 0
    rels_deleted_as_duplicate = 0

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT relationship_id, relationship_name, src_object_id, tar_object_id, metadata, confidence
            FROM object_relationship
            WHERE src_object_id = %s OR tar_object_id = %s
            ORDER BY relationship_id
            """,
            (duplicate, duplicate),
        )
        rel_rows = cursor.fetchall() or []

        for rel_id, rel_name, src_id, tar_id, rel_meta, rel_conf in rel_rows:
            new_src = canonical if int(src_id) == duplicate else int(src_id)
            new_tar = canonical if int(tar_id) == duplicate else int(tar_id)

            if new_src == new_tar:
                cursor.execute("DELETE FROM object_relationship WHERE relationship_id = %s", (int(rel_id),))
                rels_deleted_as_duplicate += 1
                continue

            cursor.execute(
                """
                SELECT relationship_id
                FROM object_relationship
                WHERE relationship_name = %s
                  AND src_object_id = %s
                  AND tar_object_id = %s
                  AND relationship_id <> %s
                LIMIT 1
                """,
                (rel_name, new_src, new_tar, int(rel_id)),
            )
            existing = cursor.fetchone()

            if existing:
                keeper_rel_id = int(existing[0])
                cursor.execute(
                    """
                    UPDATE object_relationship
                    SET metadata = COALESCE(object_relationship.metadata, '{}'::jsonb) || %s::jsonb,
                        confidence = GREATEST(COALESCE(object_relationship.confidence, 0), COALESCE(%s, 0))
                    WHERE relationship_id = %s
                    """,
                    (json.dumps(rel_meta if isinstance(rel_meta, dict) else {}), rel_conf, keeper_rel_id),
                )
                cursor.execute(
                    """
                    UPDATE attribute
                    SET src_id = %s
                    WHERE src_type = 'relationship'
                      AND src_id = %s
                    """,
                    (keeper_rel_id, int(rel_id)),
                )
                cursor.execute("DELETE FROM object_relationship WHERE relationship_id = %s", (int(rel_id),))
                rels_deleted_as_duplicate += 1
                continue

            cursor.execute(
                "UPDATE object_relationship SET src_object_id = %s, tar_object_id = %s WHERE relationship_id = %s",
                (new_src, new_tar, int(rel_id)),
            )
            rels_retargeted += 1

        cursor.execute(
            "UPDATE attribute SET src_id = %s WHERE src_type = 'object' AND src_id = %s",
            (canonical, duplicate),
        )
        attrs_retargeted = int(cursor.rowcount or 0)

        cursor.execute(
            "UPDATE part SET object_id = %s WHERE object_id = %s",
            (canonical, duplicate),
        )
        parts_retargeted = int(cursor.rowcount or 0)

        merged_meta = {
            "merged_into": canonical,
            "merge_note": str(merge_note or "").strip(),
            "merged_at": datetime.utcnow().isoformat() + "Z",
        }
        cursor.execute(
            """
            UPDATE object_instance
            SET status = 'merged',
                valid_until = COALESCE(valid_until, CURRENT_DATE),
                metadata = COALESCE(object_instance.metadata, '{}'::jsonb) || %s::jsonb
            WHERE object_id = %s
            """,
            (json.dumps(merged_meta), duplicate),
        )

    alias_row = None
    if keep_alias_link:
        alias_row = link_object_alias(
            connection=connection,
            primary_object_id=canonical,
            alias_object_id=duplicate,
            alias_type="merged_record",
            confidence=1.0,
            metadata={"merge_note": str(merge_note or "").strip()},
            commit=False,
        )

    connection.commit()
    return {
        "canonical_object_id": canonical,
        "duplicate_object_id": duplicate,
        "canonical_object_name": canonical_row.get("object_name"),
        "duplicate_object_name": duplicate_row.get("object_name"),
        "class_name": canonical_row.get("class_name"),
        "relationships_retargeted": rels_retargeted,
        "relationships_deleted_as_duplicate": rels_deleted_as_duplicate,
        "attributes_retargeted": attrs_retargeted,
        "parts_retargeted": parts_retargeted,
        "alias_link": alias_row,
    }


def update_object_instance_by_id(
    connection: psycopg2.extensions.connection,
    object_id: int,
    metadata: dict[str, Any] | None = None,
    object_name: str | None = None,
) -> dict[str, Any] | None:
    payload = metadata or {}

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT class_name, object_name FROM object_instance WHERE object_id = %s",
            (int(object_id),),
        )
        existing_row = cursor.fetchone()

    if not existing_row:
        return None

    existing_class_name = str(existing_row[0] or "").strip().lower()
    effective_name = str(object_name or existing_row[1] or "").strip()
    canonical_full_name = _canonical_person_full_name(effective_name) if existing_class_name == "person" else None

    if object_name and object_name.strip():
        sql = """
        UPDATE object_instance
        SET object_name = %s,
            canonical_full_name = %s,
            metadata = COALESCE(object_instance.metadata, '{}'::jsonb) || %s::jsonb
        WHERE object_id = %s
        RETURNING object_id, object_name, canonical_full_name, class_name, metadata, entry_date
        """
        params = (object_name.strip(), canonical_full_name, json.dumps(payload), int(object_id))
    else:
        sql = """
        UPDATE object_instance
        SET canonical_full_name = %s,
            metadata = COALESCE(object_instance.metadata, '{}'::jsonb) || %s::jsonb
        WHERE object_id = %s
        RETURNING object_id, object_name, canonical_full_name, class_name, metadata, entry_date
        """
        params = (canonical_full_name, json.dumps(payload), int(object_id))

    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        row = cursor.fetchone()

    if not row:
        connection.rollback()
        return None

    connection.commit()
    return {
        "object_id": row[0],
        "object_name": row[1],
        "canonical_full_name": row[2],
        "class_name": row[3],
        "metadata": row[4] or {},
        "entry_date": row[5],
    }


def upsert_object_instance(
    connection: psycopg2.extensions.connection,
    object_name: str,
    class_name: str,
    metadata: dict[str, Any] | None = None,
    status: str = "active",
    valid_from: date | str | None = None,
    valid_until: date | str | None = None,
) -> dict[str, Any]:
    context = get_security_context(required=False)
    tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
    canonical_full_name = _canonical_person_full_name(object_name) if str(class_name or "").strip().lower() == "person" else None
    sql = """
    INSERT INTO object_instance (tenant_id, object_name, canonical_full_name, class_name, metadata, status, valid_from, valid_until)
    VALUES (%s::uuid, %s, %s, %s, %s::jsonb, %s, COALESCE(%s, CURRENT_DATE), %s)
    ON CONFLICT (tenant_id, object_name, class_name)
    DO UPDATE SET
        canonical_full_name = COALESCE(object_instance.canonical_full_name, EXCLUDED.canonical_full_name),
        metadata = COALESCE(object_instance.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        status = EXCLUDED.status,
        valid_from = COALESCE(object_instance.valid_from, EXCLUDED.valid_from),
        valid_until = EXCLUDED.valid_until
    RETURNING object_id, object_name, canonical_full_name, class_name, metadata, status, valid_from, valid_until, entry_date
    """

    payload = metadata or {}
    with connection.cursor() as cursor:
        cursor.execute(sql, (tenant_id, object_name, canonical_full_name, class_name, json.dumps(payload), status, valid_from, valid_until))
        row = cursor.fetchone()
    connection.commit()

    return {
        "object_id": row[0],
        "object_name": row[1],
        "canonical_full_name": row[2],
        "class_name": row[3],
        "metadata": row[4] or {},
        "status": row[5],
        "valid_from": row[6],
        "valid_until": row[7],
        "entry_date": row[8],
    }


def enqueue_resolution_ambiguity(
    connection: psycopg2.extensions.connection,
    entity_payload: dict[str, Any],
    resolution_result: dict[str, Any],
    status: str = "pending",
) -> dict[str, Any]:
    sql = """
    INSERT INTO resolution_ambiguity_queue (
        entity_id,
        object_name,
        class_name,
        doc_id,
        payload,
        resolution_result,
        status
    )
    VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
    RETURNING queue_id, entity_id, object_name, class_name, doc_id, payload, resolution_result, status, created_at, updated_at
    """

    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                entity_payload.get("entity_id"),
                str(entity_payload.get("object_name") or entity_payload.get("name") or "").strip(),
                str(entity_payload.get("class_name") or "entity").strip().lower(),
                entity_payload.get("doc_id"),
                json.dumps(entity_payload or {}),
                json.dumps(resolution_result or {}),
                status,
            ),
        )
        row = cursor.fetchone()

    connection.commit()
    return {
        "queue_id": row[0],
        "entity_id": row[1],
        "object_name": row[2],
        "class_name": row[3],
        "doc_id": row[4],
        "payload": row[5] or {},
        "resolution_result": row[6] or {},
        "status": row[7],
        "created_at": row[8],
        "updated_at": row[9],
    }


def enqueue_tx_match_index_pending(
    connection: psycopg2.extensions.connection,
    payload: dict[str, Any],
    doc_id: int | None = None,
    error_message: str | None = None,
    source: str = "ingestion",
    status: str = "pending",
) -> dict[str, Any]:
    sql = """
    INSERT INTO tx_match_index_pending (
        doc_id,
        source,
        status,
        payload,
        error_message
    )
    VALUES (%s, %s, %s, %s::jsonb, %s)
    RETURNING pending_id, doc_id, source, status, payload, error_message, retry_count, created_at, updated_at
    """

    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                int(doc_id) if doc_id else None,
                str(source or "ingestion").strip() or "ingestion",
                str(status or "pending").strip() or "pending",
                json.dumps(payload or {}),
                str(error_message or "").strip() or None,
            ),
        )
        row = cursor.fetchone()

    connection.commit()
    return {
        "pending_id": row[0],
        "doc_id": row[1],
        "source": row[2],
        "status": row[3],
        "payload": row[4] or {},
        "error_message": row[5],
        "retry_count": int(row[6] or 0),
        "created_at": row[7],
        "updated_at": row[8],
    }


def list_tx_match_index_pending(
    connection: psycopg2.extensions.connection,
    statuses: list[str] | None = None,
    limit: int = 100,
    doc_id: int | None = None,
) -> list[dict[str, Any]]:
    normalized_statuses = [str(item).strip().lower() for item in (statuses or []) if str(item).strip()]
    params: list[Any] = []

    sql = """
    SELECT pending_id, doc_id, source, status, payload, error_message, retry_count, created_at, updated_at
    FROM tx_match_index_pending
    WHERE 1=1
    """

    if normalized_statuses:
        sql += " AND status = ANY(%s)"
        params.append(normalized_statuses)

    if doc_id is not None:
        sql += " AND doc_id = %s"
        params.append(int(doc_id))

    sql += " ORDER BY created_at ASC LIMIT %s"
    params.append(max(1, int(limit)))

    with connection.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()

    return [
        {
            "pending_id": int(row[0]),
            "doc_id": row[1],
            "source": row[2],
            "status": row[3],
            "payload": row[4] or {},
            "error_message": row[5],
            "retry_count": int(row[6] or 0),
            "created_at": row[7],
            "updated_at": row[8],
        }
        for row in rows
    ]


def get_tx_match_index_pending_by_id(
    connection: psycopg2.extensions.connection,
    pending_id: int,
) -> dict[str, Any] | None:
    sql = """
    SELECT pending_id, doc_id, source, status, payload, error_message, retry_count, created_at, updated_at
    FROM tx_match_index_pending
    WHERE pending_id = %s
    LIMIT 1
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, (int(pending_id),))
        row = cursor.fetchone()

    if not row:
        return None

    return {
        "pending_id": int(row[0]),
        "doc_id": row[1],
        "source": row[2],
        "status": row[3],
        "payload": row[4] or {},
        "error_message": row[5],
        "retry_count": int(row[6] or 0),
        "created_at": row[7],
        "updated_at": row[8],
    }


def update_tx_match_index_pending_status(
    connection: psycopg2.extensions.connection,
    pending_id: int,
    status: str,
    error_message: str | None = None,
    retry_increment: bool = False,
    last_result: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    current = get_tx_match_index_pending_by_id(connection, pending_id)
    if current is None:
        return None

    payload = dict(current.get("payload") or {})
    if isinstance(last_result, dict):
        payload["last_retry_result"] = last_result

    sql = """
    UPDATE tx_match_index_pending
    SET status = %s,
        payload = %s::jsonb,
        error_message = %s,
        retry_count = retry_count + %s,
        updated_at = NOW()
    WHERE pending_id = %s
    RETURNING pending_id, doc_id, source, status, payload, error_message, retry_count, created_at, updated_at
    """

    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                str(status or "pending").strip().lower() or "pending",
                json.dumps(payload),
                str(error_message or "").strip() or None,
                1 if retry_increment else 0,
                int(pending_id),
            ),
        )
        row = cursor.fetchone()

    connection.commit()
    if not row:
        return None

    return {
        "pending_id": int(row[0]),
        "doc_id": row[1],
        "source": row[2],
        "status": row[3],
        "payload": row[4] or {},
        "error_message": row[5],
        "retry_count": int(row[6] or 0),
        "created_at": row[7],
        "updated_at": row[8],
    }


def upsert_object_relationship(
    connection: psycopg2.extensions.connection,
    relationship_name: str,
    relationship_cat: str,
    src_object_id: int,
    tar_object_id: int,
    confidence: float | None = None,
    metadata: dict[str, Any] | None = None,
    valid_from: date | str | None = None,
    valid_until: date | str | None = None,
) -> dict[str, Any]:
    context = get_security_context(required=False)
    tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
    relationship_name = canonicalize_relationship_name(relationship_name)
    payload = metadata or {}

    existing = _find_semantic_equivalent_relationship(
        connection=connection,
        relationship_name=relationship_name,
        relationship_cat=relationship_cat,
        src_object_id=src_object_id,
        tar_object_id=tar_object_id,
    )
    if existing is not None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE object_relationship
                SET relationship_name = %s,
                    relationship_cat = %s,
                    confidence = COALESCE(%s, object_relationship.confidence),
                    metadata = COALESCE(object_relationship.metadata, '{}'::jsonb) || %s::jsonb,
                    valid_from = COALESCE(object_relationship.valid_from, COALESCE(%s, CURRENT_DATE)),
                    valid_until = COALESCE(%s, object_relationship.valid_until)
                WHERE relationship_id = %s
                RETURNING relationship_id, relationship_name, relationship_cat, src_object_id, tar_object_id, confidence, metadata, valid_from, valid_until, entry_date
                """,
                (
                    relationship_name,
                    relationship_cat,
                    confidence,
                    json.dumps(payload),
                    valid_from,
                    valid_until,
                    int(existing["relationship_id"]),
                ),
            )
            row = cursor.fetchone()
        connection.commit()

        return {
            "relationship_id": row[0],
            "relationship_name": row[1],
            "relationship_cat": row[2],
            "src_object_id": row[3],
            "tar_object_id": row[4],
            "confidence": row[5],
            "metadata": row[6] or {},
            "valid_from": row[7],
            "valid_until": row[8],
            "entry_date": row[9],
        }

    sql = """
    INSERT INTO object_relationship (
        tenant_id,
        relationship_name,
        relationship_cat,
        src_object_id,
        tar_object_id,
        confidence,
        metadata,
        valid_from,
        valid_until
    )
    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s::jsonb, COALESCE(%s, CURRENT_DATE), %s)
    ON CONFLICT (tenant_id, relationship_name, src_object_id, tar_object_id)
    DO UPDATE SET
        relationship_cat = EXCLUDED.relationship_cat,
        confidence = EXCLUDED.confidence,
        metadata = COALESCE(object_relationship.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        valid_from = COALESCE(object_relationship.valid_from, EXCLUDED.valid_from),
        valid_until = EXCLUDED.valid_until
    RETURNING relationship_id, relationship_name, relationship_cat, src_object_id, tar_object_id, confidence, metadata, valid_from, valid_until, entry_date
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, (tenant_id, relationship_name, relationship_cat, src_object_id, tar_object_id, confidence, json.dumps(payload), valid_from, valid_until))
        row = cursor.fetchone()
    connection.commit()

    return {
        "relationship_id": row[0],
        "relationship_name": row[1],
        "relationship_cat": row[2],
        "src_object_id": row[3],
        "tar_object_id": row[4],
        "confidence": row[5],
        "metadata": row[6] or {},
        "valid_from": row[7],
        "valid_until": row[8],
        "entry_date": row[9],
    }


def relate_objects_by_name(
    connection: psycopg2.extensions.connection,
    relationship_name: str,
    relationship_cat: str,
    src_object_name: str,
    src_class_name: str | None,
    tar_object_name: str,
    tar_class_name: str | None,
    confidence: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    src = get_object_instance(connection, src_object_name, src_class_name)
    if not src:
        raise LookupError(f"Source object not found: {src_object_name}")

    tar = get_object_instance(connection, tar_object_name, tar_class_name)
    if not tar:
        raise LookupError(f"Target object not found: {tar_object_name}")

    relationship = upsert_object_relationship(
        connection=connection,
        relationship_name=relationship_name,
        relationship_cat=relationship_cat,
        src_object_id=int(src["object_id"]),
        tar_object_id=int(tar["object_id"]),
        confidence=confidence,
        metadata=metadata,
    )

    relationship["src_object_name"] = src["object_name"]
    relationship["tar_object_name"] = tar["object_name"]
    return relationship


def get_children_of_parent(
    connection: psycopg2.extensions.connection,
    parent_object_name: str,
    parent_class_name: str | None = None,
    relationship_name: str = "child_of",
) -> list[dict[str, Any]]:
    relationships = get_relationships(
        connection=connection,
        relationship_name=relationship_name,
        tar_object_name=parent_object_name,
        tar_class_name=parent_class_name,
    )

    return [
        {
            "object_id": row["src_object_id"],
            "object_name": row["src_object_name"],
            "class_name": row["src_class_name"],
            "metadata": row["src_metadata"],
            "relationship_id": row["relationship_id"],
            "relationship_name": row["relationship_name"],
            "relationship_cat": row["relationship_cat"],
            "confidence": row["confidence"],
            "relationship_metadata": row["relationship_metadata"],
            "entry_date": row["entry_date"],
        }
        for row in relationships
    ]


def build_solf_candidate_matcher(
    interpreter: Any,
    predicate_template: str,
    result_var: str = "_ok",
) -> Callable[[dict[str, Any]], bool]:
    """Create a candidate matcher using SOLF clause execution.

    Example predicate_template:
    "({_ok} ? sameCompanyName {object_name} \"ExampleCo\")"
    The template can reference: object_id, object_name, and _ok.
    """

    def _matcher(candidate: dict[str, Any]) -> bool:
        predicate = predicate_template.format(
            object_id=candidate.get("object_id"),
            object_name=candidate.get("object_name"),
            _ok=result_var,
        )
        if not hasattr(interpreter, "execute_predicate") or not hasattr(interpreter, "parser"):
            return False

        ast = interpreter.parser.parse(predicate)
        interpreter.execute_predicate(ast)
        return bool(getattr(interpreter, "variables", {}).get(result_var))

    return _matcher


def _migrate_legacy_object_class_schema(connection: psycopg2.extensions.connection) -> None:
    """Backfill legacy object_class layouts to the current class_id-based schema.

    Older databases may only have class_name without class_id/parent_class_id.
    Dependent tables (for example part.class_id) require object_class.class_id to exist
    and be unique/referencable.
    """

    if not _table_exists(connection, "object_class"):
        return

    sql_statements = [
        "ALTER TABLE IF EXISTS object_class ADD COLUMN IF NOT EXISTS class_id BIGINT GENERATED BY DEFAULT AS IDENTITY;",
        "UPDATE object_class SET class_id = DEFAULT WHERE class_id IS NULL;",
        "ALTER TABLE IF EXISTS object_class ALTER COLUMN class_id SET NOT NULL;",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_object_class_class_id ON object_class(class_id);",
        "ALTER TABLE IF EXISTS object_class ADD COLUMN IF NOT EXISTS parent_class_id BIGINT;",
        "ALTER TABLE IF EXISTS object_class ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;",
        "ALTER TABLE IF EXISTS object_class ADD COLUMN IF NOT EXISTS entry_date TIMESTAMPTZ NOT NULL DEFAULT NOW();",
        "ALTER TABLE IF EXISTS object_class DROP CONSTRAINT IF EXISTS fk_object_class_parent;",
        "ALTER TABLE IF EXISTS object_class ADD CONSTRAINT fk_object_class_parent FOREIGN KEY (parent_class_id) REFERENCES object_class(class_id) ON DELETE SET NULL;",
    ]

    with connection.cursor() as cursor:
        for sql in sql_statements:
            cursor.execute(sql)


def _migrate_legacy_part_schema(connection: psycopg2.extensions.connection) -> None:
    """Backfill legacy part table columns used by current ingestion pipeline."""

    if not _table_exists(connection, "part"):
        return

    sql_statements = [
        "DROP TABLE IF EXISTS edge CASCADE;",
        "DROP TABLE IF EXISTS node CASCADE;",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS class_id BIGINT;",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS object_id BIGINT;",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS relationship_id BIGINT;",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';",
        "ALTER TABLE IF EXISTS part ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;",
        "ALTER TABLE IF EXISTS part DROP COLUMN IF EXISTS node_id CASCADE;",
        "ALTER TABLE IF EXISTS part DROP COLUMN IF EXISTS edge_id CASCADE;",
        "ALTER TABLE IF EXISTS part DROP COLUMN IF EXISTS part_type CASCADE;",
        "ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS part_doc_id_part_type_part_key_key;",
        "ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS part_part_type_check;",
        "ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS ck_part_part_type;",
        "ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS part_doc_id_part_key_key;",
        "DROP INDEX IF EXISTS uq_part_doc_key;",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_part_tenant_doc_key ON part(tenant_id, doc_id, part_key);",
        "ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS part_part_type_check;",
        "ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS ck_part_part_type;",
        "ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS fk_part_class;",
        "ALTER TABLE IF EXISTS part ADD CONSTRAINT fk_part_class FOREIGN KEY (class_id) REFERENCES object_class(class_id) ON DELETE CASCADE;",
        "ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS fk_part_object;",
        "ALTER TABLE IF EXISTS part ADD CONSTRAINT fk_part_object FOREIGN KEY (object_id) REFERENCES object_instance(object_id) ON DELETE CASCADE;",
        "ALTER TABLE IF EXISTS part DROP CONSTRAINT IF EXISTS fk_part_relationship;",
        "ALTER TABLE IF EXISTS part ADD CONSTRAINT fk_part_relationship FOREIGN KEY (relationship_id) REFERENCES object_relationship(relationship_id) ON DELETE CASCADE;",
    ]

    with connection.cursor() as cursor:
        for sql in sql_statements:
            cursor.execute(sql)


def _migrate_legacy_attribute_schema(connection: psycopg2.extensions.connection) -> None:
    """Backfill legacy attribute table columns expected by current DDL/indexes."""

    if not _table_exists(connection, "attribute"):
        return

    sql_statements = [
        "ALTER TABLE IF EXISTS attribute ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';",
        "CREATE INDEX IF NOT EXISTS idx_attr_tenant_src ON attribute(tenant_id, src_type, src_id);",
        "ALTER TABLE IF EXISTS attribute ADD COLUMN IF NOT EXISTS valid_daterange daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED;",
    ]

    with connection.cursor() as cursor:
        for sql in sql_statements:
            cursor.execute(sql)


def _migrate_document_schema(connection: psycopg2.extensions.connection) -> None:
    """Backfill document constraints/indexes expected by current ingestion pipeline."""

    if not _table_exists(connection, "document"):
        return

    sql_statements = [
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS doc_name VARCHAR(512);",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS doc_desc TEXT;",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS status VARCHAR(32) NOT NULL DEFAULT 'active';",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS access_level VARCHAR(32) NOT NULL DEFAULT 'tenant';",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS valid_from DATE;",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS valid_until DATE;",
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='document' AND column_name='source_path') AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='document' AND column_name='markdown_path') THEN ALTER TABLE document RENAME COLUMN source_path TO markdown_path; END IF; END $$;",
        "ALTER TABLE IF EXISTS document ADD COLUMN IF NOT EXISTS markdown_path VARCHAR(1024);",
        "UPDATE document SET markdown_path = NULL WHERE markdown_path = doc_path;",
        "UPDATE document SET markdown_path = metadata->>'markdown_path' WHERE markdown_path IS NULL AND metadata->>'markdown_path' IS NOT NULL;",
        "UPDATE document SET valid_from = COALESCE(valid_from, doc_date, entry_date::date, CURRENT_DATE);",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_document_tenant_doc_path_nonnull ON document(tenant_id, doc_path) WHERE doc_path IS NOT NULL;",
        "CREATE INDEX IF NOT EXISTS idx_docs_status ON document(status);",
        "DROP INDEX IF EXISTS idx_docs_source_path;",
        "CREATE INDEX IF NOT EXISTS idx_docs_markdown_path ON document(markdown_path);",
        "CREATE INDEX IF NOT EXISTS idx_docs_tenant_created ON document(tenant_id, entry_date DESC);",
        "DROP INDEX IF EXISTS uq_document_doc_path_nonnull;",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_document_tenant_doc_path_nonnull ON document(tenant_id, doc_path) WHERE doc_path IS NOT NULL;",
    ]

    with connection.cursor() as cursor:
        for sql in sql_statements:
            cursor.execute(sql)


def _migrate_object_instance_schema(connection: psycopg2.extensions.connection) -> None:
    if not _table_exists(connection, "object_instance"):
        return

    sql_statements = [
        "ALTER TABLE IF EXISTS object_instance ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';",
        "ALTER TABLE IF EXISTS object_instance DROP CONSTRAINT IF EXISTS object_instance_object_name_class_name_key;",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_object_instance_tenant_name_class ON object_instance(tenant_id, object_name, class_name);",
        "ALTER TABLE IF EXISTS object_instance ADD COLUMN IF NOT EXISTS status VARCHAR(32) NOT NULL DEFAULT 'active';",
        "ALTER TABLE IF EXISTS object_instance ADD COLUMN IF NOT EXISTS valid_from DATE;",
        "ALTER TABLE IF EXISTS object_instance ADD COLUMN IF NOT EXISTS valid_until DATE;",
        "ALTER TABLE IF EXISTS object_instance ADD COLUMN IF NOT EXISTS canonical_full_name VARCHAR(256);",
        "UPDATE object_instance SET valid_from = COALESCE(valid_from, entry_date::date, CURRENT_DATE);",
        "UPDATE object_instance SET canonical_full_name = CASE WHEN LOWER(class_name) = 'person' THEN COALESCE(canonical_full_name, object_name) ELSE canonical_full_name END;",
        "ALTER TABLE IF EXISTS object_instance ALTER COLUMN valid_from SET DEFAULT CURRENT_DATE;",
        "ALTER TABLE IF EXISTS object_instance ALTER COLUMN valid_from SET NOT NULL;",
        "ALTER TABLE IF EXISTS object_instance ADD COLUMN IF NOT EXISTS valid_daterange daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED;",
        "CREATE INDEX IF NOT EXISTS idx_object_instance_status ON object_instance(status);",
        "CREATE INDEX IF NOT EXISTS idx_object_instance_canonical_full_name ON object_instance(canonical_full_name);",
        "CREATE INDEX IF NOT EXISTS idx_object_instance_canonical_full_name_lower ON object_instance (LOWER(canonical_full_name));",
        "CREATE INDEX IF NOT EXISTS idx_object_instance_canonical_full_name_trgm ON object_instance USING GIN (LOWER(canonical_full_name) gin_trgm_ops);",
        "CREATE INDEX IF NOT EXISTS idx_object_instance_valid_daterange ON object_instance USING GIST (valid_daterange);",
    ]

    with connection.cursor() as cursor:
        for sql in sql_statements:
            cursor.execute(sql)


def _migrate_object_relationship_schema(connection: psycopg2.extensions.connection) -> None:
    if not _table_exists(connection, "object_relationship"):
        return

    sql_statements = [
        "ALTER TABLE IF EXISTS object_relationship ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';",
        "ALTER TABLE IF EXISTS object_relationship DROP CONSTRAINT IF EXISTS object_relationship_relationship_name_src_object_id_tar_object_id_key;",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_object_relationship_tenant_key ON object_relationship(tenant_id, relationship_name, src_object_id, tar_object_id);",
        "ALTER TABLE IF EXISTS object_relationship ADD COLUMN IF NOT EXISTS valid_from DATE;",
        "ALTER TABLE IF EXISTS object_relationship ADD COLUMN IF NOT EXISTS valid_until DATE;",
        "UPDATE object_relationship SET valid_from = COALESCE(valid_from, entry_date::date, CURRENT_DATE);",
        "ALTER TABLE IF EXISTS object_relationship ALTER COLUMN valid_from SET DEFAULT CURRENT_DATE;",
        "ALTER TABLE IF EXISTS object_relationship ALTER COLUMN valid_from SET NOT NULL;",
        "ALTER TABLE IF EXISTS object_relationship ADD COLUMN IF NOT EXISTS valid_daterange daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED;",
        "CREATE INDEX IF NOT EXISTS idx_object_relationship_valid_daterange ON object_relationship USING GIST (valid_daterange);",
    ]

    with connection.cursor() as cursor:
        for sql in sql_statements:
            cursor.execute(sql)


def create_tables(connection: psycopg2.extensions.connection, recreate: bool = False) -> None:
    from ingestion_triggers import TRIGGER_DDL

    ddl_statements = [
        create_extensions,
        create_tenant_security_tables,
        create_document_table,
        create_document_content_table,
        create_document_table_cell_table,
        create_document_geographic_facet_table,
        create_document_action_item_table,
        create_class_table,
        create_object_table,
        create_relationship_table,
        create_part_table,
        create_attribute_table,
        create_object_alias_table,
        create_resolution_ambiguity_queue_table,
        create_tx_match_index_pending_table,
        create_semantic_patterns_table,
        create_pattern_synonyms_table,
        create_semantic_terms_table,
        create_query_term_alias_table,
        create_query_alias_candidate_table,
        create_source_schema_taxonomy_table,
        create_category_registry_table,
        create_categories_table,
        create_category_sources_table,
        create_category_entities_table,
        create_category_attributes_table,
        create_query_pattern_registry_table,
        create_solf_clauses_table,
        create_business_rules_table,
        create_solf_workflow_registry_table,
        create_solf_workflow_versions_table,
        create_solf_workflow_steps_table,
        create_business_rule_workflow_links_table,
        create_workflow_resource_alias_table,
        create_workflow_extension_tables,
        create_workflow_pipeline_run_tables,
        create_workflow_pipeline_run_document_links_table,
        create_workflow_mutation_journal_table,
        create_action_resolution_plan_table,
        create_document_removal_log_table,
        create_security_audit_event_table,
        create_extension_package_tables,
        TRIGGER_DDL,
    ]

    vocabulary_tables = (
        "semantic_patterns", "pattern_synonyms", "semantic_terms", "query_term_alias",
        "query_alias_candidate", "source_schema_taxonomy", "category_registry", "categories",
        "category_sources", "category_entities", "category_attributes",
    )

    drop_statements = [
        "DROP TABLE IF EXISTS ikos_ingestion_delivery;",
        "DROP TABLE IF EXISTS ikos_ingestion_trigger;",
        "DROP TABLE IF EXISTS business_rule_workflow_links CASCADE;",
        "DROP TABLE IF EXISTS solf_workflow_steps CASCADE;",
        "DROP TABLE IF EXISTS solf_workflow_versions CASCADE;",
        "DROP TABLE IF EXISTS solf_workflow_registry CASCADE;",
        "DROP TABLE IF EXISTS workflow_extension_attachment CASCADE;",
        "DROP TABLE IF EXISTS workflow_extension_rule CASCADE;",
        "DROP TABLE IF EXISTS workflow_extension_version CASCADE;",
        "DROP TABLE IF EXISTS workflow_extension_pack CASCADE;",
        "DROP TABLE IF EXISTS workflow_mutation_journal CASCADE;",
        "DROP TABLE IF EXISTS ikos_extension_deployment CASCADE;",
        "DROP TABLE IF EXISTS ikos_extension_package_version CASCADE;",
        "DROP TABLE IF EXISTS ikos_extension_package CASCADE;",
        "DROP TABLE IF EXISTS workflow_pipeline_run_document_link CASCADE;",
        "DROP TABLE IF EXISTS workflow_resource_alias CASCADE;",
        "DROP TABLE IF EXISTS pattern_synonyms CASCADE;",
        "DROP TABLE IF EXISTS semantic_patterns CASCADE;",
        "DROP TABLE IF EXISTS semantic_terms CASCADE;",
        "DROP TABLE IF EXISTS query_term_alias CASCADE;",
        "DROP TABLE IF EXISTS solf_clauses CASCADE;",
        "DROP TABLE IF EXISTS business_rules CASCADE;",
        "DROP TABLE IF EXISTS object_alias CASCADE;",
        "DROP TABLE IF EXISTS resolution_ambiguity_queue CASCADE;",
        "DROP TABLE IF EXISTS tx_match_index_pending CASCADE;",
        "DROP TABLE IF EXISTS document_removal_log CASCADE;",
        "DROP TABLE IF EXISTS document_table_cell CASCADE;",
        "DROP TABLE IF EXISTS part CASCADE;",
        "DROP TABLE IF EXISTS object_relationship CASCADE;",
        "DROP TABLE IF EXISTS object_instance CASCADE;",
        "DROP TABLE IF EXISTS object_class CASCADE;",
        "DROP TABLE IF EXISTS document CASCADE;",
    ]

    try:
        with connection.cursor() as cursor:
            if recreate:
                for sql in drop_statements:
                    cursor.execute(sql)
            else:
                cursor.execute(create_tenant_security_tables)

        if not recreate:
            _migrate_legacy_object_class_schema(connection)
            _migrate_legacy_part_schema(connection)
            _migrate_legacy_attribute_schema(connection)
            _migrate_object_instance_schema(connection)
            _migrate_object_relationship_schema(connection)
            _migrate_document_schema(connection)

        with connection.cursor() as cursor:
            for table_name in vocabulary_tables:
                cursor.execute(
                    f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';"
                )
            for sql in ddl_statements:
                cursor.execute(sql)
        # These tables hold executable/configuration artifacts. Scope them at
        # the database boundary so forgotten tenant predicates cannot leak
        # workflows, rules, actions, or SOLF between tenants.
        ensure_tenant_isolation_for_tables(connection)
        with connection.cursor() as cursor:
            for table_name in ("document", "object_instance", "object_relationship", "part", "attribute", "document_table_cell", "workflow_pipeline_run"):
                cursor.execute(
                    f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';"
                )
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_tenant_id ON {table_name}(tenant_id);")
            for table_name in vocabulary_tables:
                cursor.execute(
                    f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';"
                )
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_tenant_id ON {table_name}(tenant_id);")

            legacy_constraints = (
                ("semantic_patterns", "semantic_patterns_pattern_text_entity_class_pattern_language_key"),
                ("pattern_synonyms", "pattern_synonyms_pattern_id_synonym_text_language_key"),
                ("semantic_terms", "semantic_terms_kind_canonical_name_term_text_language_key"),
                ("query_term_alias", "query_term_alias_alias_text_kind_language_key"),
                ("query_alias_candidate", "query_alias_candidate_alias_text_canonical_name_kind_language_key"),
                ("source_schema_taxonomy", "source_schema_taxonomy_source_id_schema_name_table_name_column_name_category_name_role_name_key"),
                ("category_registry", "category_registry_category_name_source_scope_access_method_source_id_schema_name_table_name_column_name_key"),
                ("categories", "categories_name_key"),
            )
            for table_name, constraint_name in legacy_constraints:
                cursor.execute(f"ALTER TABLE IF EXISTS {table_name} DROP CONSTRAINT IF EXISTS {constraint_name};")
            cursor.execute("DROP INDEX IF EXISTS uq_category_sources_identity;")
            cursor.execute("DROP INDEX IF EXISTS uq_category_entities_identity;")
            cursor.execute("DROP INDEX IF EXISTS uq_category_attributes_identity;")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_semantic_patterns_tenant_key ON semantic_patterns(tenant_id, pattern_text, entity_class, pattern_language);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_semantic_patterns_tenant_pattern_id ON semantic_patterns(tenant_id, pattern_id);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_pattern_synonyms_tenant_key ON pattern_synonyms(tenant_id, pattern_id, synonym_text, language);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_semantic_terms_tenant_key ON semantic_terms(tenant_id, kind, canonical_name, term_text, language);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_query_term_alias_tenant_key ON query_term_alias(tenant_id, alias_text, kind, language);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_query_alias_candidate_tenant_key ON query_alias_candidate(tenant_id, alias_text, canonical_name, kind, language);")
            cursor.execute(
                """DELETE FROM source_schema_taxonomy a USING source_schema_taxonomy b
                WHERE a.tenant_id = b.tenant_id AND a.source_id = b.source_id
                  AND a.schema_name = b.schema_name AND a.table_name = b.table_name
                  AND COALESCE(a.column_name, '') = COALESCE(b.column_name, '')
                  AND a.category_name = b.category_name AND a.role_name = b.role_name
                  AND a.taxonomy_id < b.taxonomy_id"""
            )
            cursor.execute(
                """DELETE FROM category_registry a USING category_registry b
                WHERE a.tenant_id = b.tenant_id AND a.category_name = b.category_name
                  AND a.source_scope = b.source_scope AND a.access_method = b.access_method
                  AND COALESCE(a.source_id, 0) = COALESCE(b.source_id, 0)
                  AND COALESCE(a.schema_name, '') = COALESCE(b.schema_name, '')
                  AND COALESCE(a.table_name, '') = COALESCE(b.table_name, '')
                  AND COALESCE(a.column_name, '') = COALESCE(b.column_name, '')
                  AND a.category_id < b.category_id"""
            )
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_source_schema_taxonomy_tenant_key ON source_schema_taxonomy(tenant_id, source_id, schema_name, table_name, (COALESCE(column_name, '')), category_name, role_name);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_category_registry_tenant_key ON category_registry(tenant_id, category_name, source_scope, access_method, (COALESCE(source_id, 0)), (COALESCE(schema_name, '')), (COALESCE(table_name, '')), (COALESCE(column_name, '')));")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_categories_tenant_name ON categories(tenant_id, name);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_categories_tenant_id ON categories(tenant_id, category_id);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_category_sources_tenant_id ON category_sources(tenant_id, category_source_id);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_category_sources_tenant_identity ON category_sources(tenant_id, category_id, source_type, COALESCE(source_id, 0), access_method);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_category_entities_tenant_identity ON category_entities(tenant_id, category_source_id, entity_type, COALESCE(source_table, ''), COALESCE(source_column, ''), role_name);")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_category_attributes_tenant_identity ON category_attributes(tenant_id, category_source_id, canonical_name, COALESCE(source_table, ''), COALESCE(source_column, ''), role_name);")
            cursor.execute(
                """DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_pattern_synonyms_tenant_pattern') THEN
                    ALTER TABLE pattern_synonyms ADD CONSTRAINT fk_pattern_synonyms_tenant_pattern
                    FOREIGN KEY (tenant_id, pattern_id) REFERENCES semantic_patterns(tenant_id, pattern_id) ON DELETE CASCADE;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_category_sources_tenant_category') THEN
                    ALTER TABLE category_sources ADD CONSTRAINT fk_category_sources_tenant_category
                    FOREIGN KEY (tenant_id, category_id) REFERENCES categories(tenant_id, category_id) ON DELETE CASCADE;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_category_entities_tenant_source') THEN
                    ALTER TABLE category_entities ADD CONSTRAINT fk_category_entities_tenant_source
                    FOREIGN KEY (tenant_id, category_source_id) REFERENCES category_sources(tenant_id, category_source_id) ON DELETE CASCADE;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_category_attributes_tenant_source') THEN
                    ALTER TABLE category_attributes ADD CONSTRAINT fk_category_attributes_tenant_source
                    FOREIGN KEY (tenant_id, category_source_id) REFERENCES category_sources(tenant_id, category_source_id) ON DELETE CASCADE;
                END IF;
                END $$;"""
            )
            cursor.execute(
                "ALTER TABLE IF EXISTS document_content ALTER COLUMN created_by TYPE VARCHAR(256) USING created_by::text;"
            )
            cursor.execute(
                "ALTER TABLE IF EXISTS document_table_cell DROP CONSTRAINT IF EXISTS document_table_cell_doc_id_table_index_row_index_column_index_key;"
            )
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_doc_table_cell_tenant_key ON document_table_cell(tenant_id, doc_id, table_index, row_index, column_index);"
            )
            for table_name in (
                "document", "document_content", "object_instance", "object_relationship",
                "part", "attribute", "document_table_cell", "workflow_pipeline_run",
                *vocabulary_tables,
            ):
                finance_predicate = ""
                finance_check_predicate = ""
                if table_name == "document":
                    finance_predicate = " AND (access_level <> ''finance'' OR ''finance.read'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '',''))) AND (access_level <> ''restricted'' OR ''documents.restricted.read'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '','')))"
                    finance_check_predicate = " AND (access_level <> ''finance'' OR ''finance.write'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '',''))) AND (access_level <> ''restricted'' OR ''documents.restricted.read'' = ANY(string_to_array(current_setting(''ikos.permissions'', true), '','')))"
                elif table_name == "document_content":
                    finance_predicate = " AND EXISTS (SELECT 1 FROM document d WHERE d.doc_id = document_content.doc_id AND d.tenant_id = document_content.tenant_id)"
                    finance_check_predicate = " AND EXISTS (SELECT 1 FROM document d WHERE d.doc_id = document_content.doc_id AND d.tenant_id = document_content.tenant_id)"
                cursor.execute(f"ALTER TABLE IF EXISTS {table_name} ENABLE ROW LEVEL SECURITY;")
                cursor.execute(f"ALTER TABLE IF EXISTS {table_name} FORCE ROW LEVEL SECURITY;")
                # Replace policies created by earlier versions so upgraded
                # installations cannot retain weaker tenant predicates.
                cursor.execute(f"DROP POLICY IF EXISTS ikos_tenant_isolation ON {table_name};")
                cursor.execute(
                    f"""DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_policies
                        WHERE schemaname = current_schema()
                          AND tablename = '{table_name}'
                          AND policyname = 'ikos_tenant_isolation'
                    ) THEN
                        EXECUTE 'CREATE POLICY ikos_tenant_isolation ON {table_name}
                            USING (tenant_id = NULLIF(current_setting(''ikos.tenant_id'', true), '''')::uuid{finance_predicate})
                            WITH CHECK (tenant_id = NULLIF(current_setting(''ikos.tenant_id'', true), '''')::uuid{finance_check_predicate})';
                    END IF;
                    END $$;"""
                )
            for table_name, run_column in (
                ("workflow_pipeline_run_step", "run_id"),
                ("workflow_pipeline_run_document_link", "run_id"),
            ):
                cursor.execute(f"ALTER TABLE IF EXISTS {table_name} ENABLE ROW LEVEL SECURITY;")
                cursor.execute(f"ALTER TABLE IF EXISTS {table_name} FORCE ROW LEVEL SECURITY;")
                cursor.execute(f"DROP POLICY IF EXISTS ikos_tenant_isolation ON {table_name};")
                cursor.execute(
                    f"""DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_policies
                        WHERE schemaname = current_schema()
                          AND tablename = '{table_name}'
                          AND policyname = 'ikos_tenant_isolation'
                    ) THEN
                        EXECUTE 'CREATE POLICY ikos_tenant_isolation ON {table_name}
                            USING (EXISTS (SELECT 1 FROM workflow_pipeline_run r WHERE r.run_id = {table_name}.{run_column} AND r.tenant_id = NULLIF(current_setting(''ikos.tenant_id'', true), '''')::uuid))
                            WITH CHECK (EXISTS (SELECT 1 FROM workflow_pipeline_run r WHERE r.run_id = {table_name}.{run_column} AND r.tenant_id = NULLIF(current_setting(''ikos.tenant_id'', true), '''')::uuid))';
                    END IF;
                    END $$;"""
                )
            if not recreate:
                cursor.execute("ALTER TABLE IF EXISTS attribute DROP CONSTRAINT IF EXISTS attribute_src_type_check;")
                cursor.execute(
                    """
                    ALTER TABLE IF EXISTS attribute
                    ADD CONSTRAINT attribute_src_type_check
                    CHECK (src_type IN ('part','document','class','object','relationship'));
                    """
                )
        connection.commit()
        seed_query_pattern_registry(connection)
        logging.info("Object schema tables created successfully")
    except Exception:
        connection.rollback()
        logging.exception("Failed while creating object schema tables")
        raise


# ---------------------------------------------------------------------------
# Pipeline run DB helpers
# ---------------------------------------------------------------------------

def create_pipeline_run(
    connection: psycopg2.extensions.connection,
    workflow_version_id: int | None,
    workflow_key: str | None,
    input_context: dict[str, Any],
    started_by: str | None = None,
) -> int:
    context = get_security_context(required=False)
    tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO workflow_pipeline_run
                (tenant_id, workflow_version_id, workflow_key, run_status, input_context, current_context,
                 output_context, started_by, started_at, created_at, modified_at)
            VALUES (%s::uuid, %s, %s, 'pending', %s, %s, '{}'::jsonb, %s, NOW(), NOW(), NOW())
            RETURNING run_id
            """,
            (
                tenant_id,
                int(workflow_version_id) if workflow_version_id is not None else None,
                str(workflow_key or "").strip() or None,
                Json(input_context or {}),
                Json(input_context or {}),
                str(started_by or "").strip() or None,
            ),
        )
        run_id = int(cur.fetchone()[0])
    connection.commit()
    return run_id


def get_pipeline_run(
    connection: psycopg2.extensions.connection,
    run_id: int,
) -> dict[str, Any] | None:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, workflow_version_id, workflow_key, run_status, paused_at_step_key,
                   input_context, current_context, output_context, started_by,
                   started_at, finished_at, created_at, modified_at
            FROM workflow_pipeline_run
            WHERE run_id = %s
            """,
            (int(run_id),),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "run_id": int(row[0]),
        "workflow_version_id": row[1],
        "workflow_key": row[2],
        "run_status": row[3],
        "paused_at_step_key": row[4],
        "input_context": row[5] or {},
        "current_context": row[6] or {},
        "output_context": row[7] or {},
        "started_by": row[8],
        "started_at": row[9],
        "finished_at": row[10],
        "created_at": row[11],
        "modified_at": row[12],
    }


def update_pipeline_run_status(
    connection: psycopg2.extensions.connection,
    run_id: int,
    run_status: str,
    current_context: dict[str, Any] | None = None,
    output_context: dict[str, Any] | None = None,
    paused_at_step_key: str | None = None,
    finished: bool = False,
) -> None:
    fields = ["run_status = %s", "modified_at = NOW()"]
    params: list[Any] = [run_status]

    if current_context is not None:
        fields.append("current_context = %s")
        params.append(Json(current_context))

    if output_context is not None:
        fields.append("output_context = %s")
        params.append(Json(output_context))

    if paused_at_step_key is not None:
        fields.append("paused_at_step_key = %s")
        params.append(str(paused_at_step_key))
    elif run_status in {"completed", "failed", "cancelled"}:
        fields.append("paused_at_step_key = NULL")

    if finished:
        fields.append("finished_at = NOW()")

    params.append(int(run_id))
    with connection.cursor() as cur:
        cur.execute(
            f"UPDATE workflow_pipeline_run SET {', '.join(fields)} WHERE run_id = %s",
            params,
        )
    connection.commit()


def list_pipeline_runs(
    connection: psycopg2.extensions.connection,
    run_status: str | None = None,
    workflow_key: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, workflow_version_id, workflow_key, run_status, paused_at_step_key,
                   input_context, current_context, output_context, started_by,
                   started_at, finished_at, created_at, modified_at
            FROM workflow_pipeline_run
            WHERE (%s IS NULL OR run_status = %s)
              AND (%s IS NULL OR workflow_key = %s)
            ORDER BY created_at DESC, run_id DESC
            LIMIT %s
            """,
            (run_status, run_status, workflow_key, workflow_key, int(limit)),
        )
        rows = cur.fetchall()
    return [
        {
            "run_id": int(r[0]),
            "workflow_version_id": r[1],
            "workflow_key": r[2],
            "run_status": r[3],
            "paused_at_step_key": r[4],
            "input_context": r[5] or {},
            "current_context": r[6] or {},
            "output_context": r[7] or {},
            "started_by": r[8],
            "started_at": r[9],
            "finished_at": r[10],
            "created_at": r[11],
            "modified_at": r[12],
        }
        for r in rows
    ]


def list_unfinished_pipeline_runs(
    connection: psycopg2.extensions.connection,
    workflow_key: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List runs that still require execution or user action."""
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, workflow_version_id, workflow_key, run_status, paused_at_step_key,
                   input_context, current_context, output_context, started_by,
                   started_at, finished_at, created_at, modified_at
            FROM workflow_pipeline_run
            WHERE run_status IN ('pending', 'running', 'paused')
              AND (%s IS NULL OR workflow_key = %s)
            ORDER BY created_at DESC, run_id DESC
            LIMIT %s
            """,
            (workflow_key, workflow_key, int(limit)),
        )
        rows = cur.fetchall()
    return [
        {
            "run_id": int(r[0]),
            "workflow_version_id": r[1],
            "workflow_key": r[2],
            "run_status": r[3],
            "paused_at_step_key": r[4],
            "input_context": r[5] or {},
            "current_context": r[6] or {},
            "output_context": r[7] or {},
            "started_by": r[8],
            "started_at": r[9],
            "finished_at": r[10],
            "created_at": r[11],
            "modified_at": r[12],
        }
        for r in rows
    ]


def upsert_pipeline_run_step(
    connection: psycopg2.extensions.connection,
    run_id: int,
    step_key: str,
    step_order: int,
    step_status: str,
    input_snapshot: dict[str, Any] | None = None,
    output_snapshot: dict[str, Any] | None = None,
    pause_reason: str | None = None,
    interaction_prompt: str | None = None,
    missing_data_desc: str | None = None,
    required_doc_types: list[str] | None = None,
    user_response: dict[str, Any] | None = None,
    doc_refs: list[Any] | None = None,
    error_message: str | None = None,
    increment_attempt: bool = False,
) -> int:
    with connection.cursor() as cur:
        cur.execute(
            "SELECT step_run_id, attempt_count FROM workflow_pipeline_run_step WHERE run_id=%s AND step_key=%s",
            (int(run_id), str(step_key)),
        )
        existing = cur.fetchone()

        if existing is None:
            cur.execute(
                """
                INSERT INTO workflow_pipeline_run_step
                    (run_id, step_key, step_order, step_status,
                     input_snapshot, output_snapshot, pause_reason,
                     interaction_prompt, missing_data_desc, required_doc_types,
                     user_response, doc_refs, error_message, attempt_count,
                     started_at, completed_at, created_at, modified_at)
                VALUES (%s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        NOW(), NULL, NOW(), NOW())
                RETURNING step_run_id
                """,
                (
                    int(run_id), str(step_key), int(step_order), str(step_status),
                    Json(input_snapshot or {}), Json(output_snapshot or {}), pause_reason,
                    interaction_prompt, missing_data_desc, Json(required_doc_types or []),
                    Json(user_response or {}), Json(doc_refs or []), error_message, 1,
                ),
            )
            step_run_id = int(cur.fetchone()[0])
        else:
            step_run_id = int(existing[0])
            old_attempts = int(existing[1] or 0)
            new_attempts = (old_attempts + 1) if increment_attempt else old_attempts

            completed_clause = "completed_at = NOW()," if step_status in {"completed", "failed", "skipped"} else ""
            cur.execute(
                f"""
                UPDATE workflow_pipeline_run_step
                SET step_status = %s,
                    {completed_clause}
                    output_snapshot = COALESCE(%s, output_snapshot),
                    pause_reason = %s,
                    interaction_prompt = %s,
                    missing_data_desc = %s,
                    required_doc_types = COALESCE(%s, required_doc_types),
                    user_response = COALESCE(%s, user_response),
                    doc_refs = COALESCE(%s, doc_refs),
                    error_message = %s,
                    attempt_count = %s,
                    modified_at = NOW()
                WHERE step_run_id = %s
                """,
                (
                    str(step_status),
                    Json(output_snapshot) if output_snapshot is not None else None,
                    pause_reason,
                    interaction_prompt,
                    missing_data_desc,
                    Json(required_doc_types) if required_doc_types is not None else None,
                    Json(user_response) if user_response is not None else None,
                    Json(doc_refs) if doc_refs is not None else None,
                    error_message,
                    new_attempts,
                    step_run_id,
                ),
            )
    connection.commit()
    return step_run_id


def get_pipeline_run_steps(
    connection: psycopg2.extensions.connection,
    run_id: int,
) -> list[dict[str, Any]]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT step_run_id, run_id, step_key, step_order, step_status,
                   pause_reason, interaction_prompt, missing_data_desc,
                   required_doc_types, input_snapshot, output_snapshot,
                   user_response, doc_refs, error_message, attempt_count,
                   started_at, completed_at, created_at, modified_at
            FROM workflow_pipeline_run_step
            WHERE run_id = %s
            ORDER BY step_order, step_run_id
            """,
            (int(run_id),),
        )
        rows = cur.fetchall()
    return [
        {
            "step_run_id": int(r[0]),
            "run_id": int(r[1]),
            "step_key": r[2],
            "step_order": int(r[3]),
            "step_status": r[4],
            "pause_reason": r[5],
            "interaction_prompt": r[6],
            "missing_data_desc": r[7],
            "required_doc_types": r[8] or [],
            "input_snapshot": r[9] or {},
            "output_snapshot": r[10] or {},
            "user_response": r[11] or {},
            "doc_refs": r[12] or [],
            "error_message": r[13],
            "attempt_count": int(r[14] or 0),
            "started_at": r[15],
            "completed_at": r[16],
            "created_at": r[17],
            "modified_at": r[18],
        }
        for r in rows
    ]


def supply_pipeline_step_response(
    connection: psycopg2.extensions.connection,
    run_id: int,
    step_key: str,
    user_response: dict[str, Any] | None = None,
    doc_refs: list[Any] | None = None,
) -> bool:
    """Store user-supplied response or new document refs for a paused step, reset to pending."""
    with connection.cursor() as cur:
        cur.execute(
            """
            UPDATE workflow_pipeline_run_step
            SET step_status = 'pending',
                pause_reason = NULL,
                user_response = COALESCE(%s, user_response),
                doc_refs = COALESCE(%s, doc_refs),
                modified_at = NOW()
            WHERE run_id = %s AND step_key = %s AND step_status = 'paused'
            """,
            (
                Json(user_response) if user_response is not None else None,
                Json(doc_refs) if doc_refs is not None else None,
                int(run_id),
                str(step_key),
            ),
        )
        updated = int(cur.rowcount or 0)
    connection.commit()
    return updated > 0


def _normalize_document_id(doc_ref: Any) -> int | None:
    if isinstance(doc_ref, dict):
        candidate = doc_ref.get("doc_id")
    else:
        candidate = doc_ref

    if candidate is None:
        return None
    try:
        value = int(candidate)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def link_pipeline_run_documents(
    connection: psycopg2.extensions.connection,
    run_id: int,
    doc_refs: list[Any] | None,
    source: str = "system",
    step_key: str | None = None,
    linked_by: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    refs = list(doc_refs or [])
    if not refs:
        return 0

    normalized_source = str(source or "system").strip().lower() or "system"
    if normalized_source not in {"input_context", "resume", "step_response", "system"}:
        normalized_source = "system"

    normalized_step_key = str(step_key or "").strip()

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT workflow_version_id, workflow_key
            FROM workflow_pipeline_run
            WHERE run_id = %s
            """,
            (int(run_id),),
        )
        run_row = cur.fetchone()
        if run_row is None:
            return 0

        workflow_version_id = run_row[0]
        workflow_key = run_row[1]

        inserted = 0
        for ref in refs:
            document_id = _normalize_document_id(ref)
            if document_id is None:
                continue

            cur.execute(
                """
                INSERT INTO workflow_pipeline_run_document_link (
                    run_id, workflow_version_id, workflow_key, document_id,
                    source, step_key, linked_by, metadata, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (run_id, document_id, source, step_key)
                DO NOTHING
                """,
                (
                    int(run_id),
                    int(workflow_version_id) if workflow_version_id is not None else None,
                    workflow_key,
                    int(document_id),
                    normalized_source,
                    normalized_step_key,
                    str(linked_by or "").strip() or None,
                    Json(metadata or {}),
                ),
            )
            inserted += int(cur.rowcount or 0)

    connection.commit()
    return inserted


def list_pipeline_run_document_links(
    connection: psycopg2.extensions.connection,
    run_id: int,
) -> list[dict[str, Any]]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT link_id, run_id, workflow_version_id, workflow_key, document_id,
                   source, step_key, linked_by, metadata, created_at
            FROM workflow_pipeline_run_document_link
            WHERE run_id = %s
            ORDER BY created_at, link_id
            """,
            (int(run_id),),
        )
        rows = cur.fetchall()

    return [
        {
            "link_id": int(row[0]),
            "run_id": int(row[1]),
            "workflow_version_id": int(row[2]) if row[2] is not None else None,
            "workflow_key": row[3],
            "document_id": int(row[4]),
            "source": row[5],
            "step_key": row[6],
            "linked_by": row[7],
            "metadata": row[8] or {},
            "created_at": row[9],
        }
        for row in rows
    ]


def _ensure_workflow_mutation_journal_table(cursor: Any) -> None:
    cursor.execute(create_workflow_mutation_journal_table)
    cursor.execute("ALTER TABLE workflow_mutation_journal ADD COLUMN IF NOT EXISTS business_key VARCHAR(256)")
    cursor.execute("ALTER TABLE workflow_mutation_journal ADD COLUMN IF NOT EXISTS business_key_type VARCHAR(64)")
    cursor.execute("ALTER TABLE workflow_mutation_journal ALTER COLUMN run_id DROP NOT NULL")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_mutation_business_key ON workflow_mutation_journal(business_key)"
    )


def append_workflow_mutation_journal(
    connection: psycopg2.extensions.connection,
    *,
    run_id: int | None = None,
    step_key: str,
    step_order: int,
    operation_kind: str,
    before_state: dict[str, Any] | None = None,
    after_state: dict[str, Any] | None = None,
    inverse_action: dict[str, Any] | None = None,
    target_store: str = "sql",
    target_entity: str = "workflow_context",
    mutation_key: str | None = None,
    business_key: str | None = None,
    business_key_type: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    with connection.cursor() as cur:
        _ensure_workflow_mutation_journal_table(cur)

        workflow_version_id = None
        workflow_key = None
        if run_id is not None:
            cur.execute(
                """
                SELECT workflow_version_id, workflow_key
                FROM workflow_pipeline_run
                WHERE run_id = %s
                """,
                (int(run_id),),
            )
            run_row = cur.fetchone()
            workflow_version_id = run_row[0] if run_row else None
            workflow_key = run_row[1] if run_row else None

        cur.execute(
            """
            INSERT INTO workflow_mutation_journal (
                run_id, workflow_version_id, workflow_key, step_key, step_order,
                operation_kind, target_store, target_entity, mutation_key,
                business_key, business_key_type,
                before_state, after_state, inverse_action, metadata, status, applied_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'applied', NOW())
            RETURNING journal_id
            """,
            (
                int(run_id) if run_id is not None else None,
                int(workflow_version_id) if workflow_version_id is not None else None,
                workflow_key,
                str(step_key or "").strip() or None,
                int(step_order),
                str(operation_kind or "").strip() or "unknown",
                str(target_store or "sql").strip().lower() or "sql",
                str(target_entity or "workflow_context").strip() or "workflow_context",
                str(mutation_key or "").strip() or None,
                str(business_key or "").strip() or None,
                str(business_key_type or "").strip() or None,
                Json(before_state or {}),
                Json(after_state or {}),
                Json(inverse_action or {}),
                Json(metadata or {}),
            ),
        )
        journal_id = int(cur.fetchone()[0])

    connection.commit()
    return journal_id


_WORKFLOW_MUTATION_JOURNAL_COLUMNS = """
    journal_id, run_id, workflow_version_id, workflow_key, step_key, step_order,
    operation_kind, target_store, target_entity, mutation_key,
    business_key, business_key_type,
    before_state, after_state, inverse_action, metadata,
    status, applied_at, undone_at, undone_by, undo_error
"""


def _map_workflow_mutation_journal_row(r: Any) -> dict[str, Any]:
    return {
        "journal_id": int(r[0]),
        "run_id": int(r[1]) if r[1] is not None else None,
        "workflow_version_id": int(r[2]) if r[2] is not None else None,
        "workflow_key": r[3],
        "step_key": r[4],
        "step_order": int(r[5]) if r[5] is not None else None,
        "operation_kind": r[6],
        "target_store": r[7],
        "target_entity": r[8],
        "mutation_key": r[9],
        "business_key": r[10],
        "business_key_type": r[11],
        "before_state": r[12] or {},
        "after_state": r[13] or {},
        "inverse_action": r[14] or {},
        "metadata": r[15] or {},
        "status": r[16],
        "applied_at": r[17],
        "undone_at": r[18],
        "undone_by": r[19],
        "undo_error": r[20],
    }


def list_workflow_mutation_journal(
    connection: psycopg2.extensions.connection,
    run_id: int,
    include_undone: bool = True,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    with connection.cursor() as cur:
        _ensure_workflow_mutation_journal_table(cur)
        status_filter = "" if include_undone else "AND status <> 'undone'"
        cur.execute(
            f"""
            SELECT {_WORKFLOW_MUTATION_JOURNAL_COLUMNS}
            FROM workflow_mutation_journal
            WHERE run_id = %s
              {status_filter}
            ORDER BY journal_id DESC
            LIMIT %s
            """,
            (int(run_id), int(limit)),
        )
        rows = cur.fetchall() or []

    return [_map_workflow_mutation_journal_row(r) for r in rows]


def find_workflow_mutation_journal(
    connection: psycopg2.extensions.connection,
    business_key: str | None = None,
    business_key_type: str | None = None,
    target_entity: str | None = None,
    operation_kind: str | None = None,
    include_undone: bool = True,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Resolve journal entries by business identifier (e.g. an invoice/transaction id)."""
    conditions: list[str] = []
    params: list[Any] = []

    normalized_business_key = str(business_key or "").strip()
    if normalized_business_key:
        conditions.append("business_key ILIKE %s")
        params.append(f"%{normalized_business_key}%")

    for column, value in (
        ("business_key_type", business_key_type),
        ("target_entity", target_entity),
        ("operation_kind", operation_kind),
    ):
        normalized = str(value or "").strip()
        if normalized:
            conditions.append(f"{column} = %s")
            params.append(normalized)

    if not include_undone:
        conditions.append("status <> 'undone'")

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(max(1, min(int(limit), 2000)))

    with connection.cursor() as cur:
        _ensure_workflow_mutation_journal_table(cur)
        cur.execute(
            f"""
            SELECT {_WORKFLOW_MUTATION_JOURNAL_COLUMNS}
            FROM workflow_mutation_journal
            {where_sql}
            ORDER BY journal_id DESC
            LIMIT %s
            """,
            tuple(params),
        )
        rows = cur.fetchall() or []

    return [_map_workflow_mutation_journal_row(r) for r in rows]


def get_workflow_mutation_journal_entry(
    connection: psycopg2.extensions.connection,
    journal_id: int,
) -> dict[str, Any] | None:
    with connection.cursor() as cur:
        _ensure_workflow_mutation_journal_table(cur)
        cur.execute(
            f"""
            SELECT {_WORKFLOW_MUTATION_JOURNAL_COLUMNS}
            FROM workflow_mutation_journal
            WHERE journal_id = %s
            LIMIT 1
            """,
            (int(journal_id),),
        )
        row = cur.fetchone()

    return _map_workflow_mutation_journal_row(row) if row is not None else None


def mark_workflow_mutation_undone(
    connection: psycopg2.extensions.connection,
    journal_id: int,
    undone_by: str,
    undo_error: str | None = None,
    status: str = "undone",
) -> bool:
    normalized_status = str(status or "undone").strip().lower() or "undone"
    with connection.cursor() as cur:
        _ensure_workflow_mutation_journal_table(cur)
        cur.execute(
            """
            UPDATE workflow_mutation_journal
            SET status = %s,
                undone_at = NOW(),
                undone_by = %s,
                undo_error = %s
            WHERE journal_id = %s
            """,
            (
                normalized_status,
                str(undone_by or "").strip() or "api:user",
                str(undo_error or "").strip() or None,
                int(journal_id),
            ),
        )
        updated = int(cur.rowcount or 0)

    connection.commit()
    return updated > 0


def _ensure_action_resolution_plan_table(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(create_action_resolution_plan_table)
    connection.commit()


def create_action_resolution_plan(
    connection: Any,
    *,
    request_text: str,
    action_object: dict[str, Any],
    workflow_snapshot: dict[str, Any] | None = None,
    execution_plan: list[dict[str, Any]] | None = None,
    parameters: dict[str, Any] | None = None,
    missing_parameters: list[dict[str, Any]] | None = None,
    requested_by: str = "api:user",
    ttl_minutes: int = 30,
) -> dict[str, Any]:
    """Persist a registry/version-grounded plan without executing it."""
    plan_id = uuid.uuid4()
    status = "awaiting_input" if missing_parameters else "pending"
    with connection.cursor() as cursor:
        cursor.execute(create_action_resolution_plan_table)
        cursor.execute(
            """
            INSERT INTO action_resolution_plan (
                plan_id, request_text, action_object, workflow_snapshot,
                execution_plan, parameters, missing_parameters, status,
                requested_by, expires_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                    NOW() + (%s * INTERVAL '1 minute'))
            """,
            (
                plan_id,
                str(request_text or "").strip(),
                Json(action_object or {}),
                Json(workflow_snapshot or {}),
                Json(execution_plan or []),
                Json(parameters or {}),
                Json(missing_parameters or []),
                status,
                str(requested_by or "api:user").strip() or "api:user",
                max(1, min(int(ttl_minutes), 10080)),
            ),
        )
    connection.commit()
    return get_action_resolution_plan(connection, plan_id) or {"plan_id": str(plan_id), "status": status}


def get_action_resolution_plan(connection: Any, plan_id: Any) -> dict[str, Any] | None:
    try:
        normalized_id = uuid.UUID(str(plan_id))
    except (TypeError, ValueError, AttributeError):
        return None
    with connection.cursor() as cursor:
        cursor.execute(create_action_resolution_plan_table)
        cursor.execute(
            """
            SELECT plan_id, request_text, action_object, workflow_snapshot,
                   execution_plan, parameters, missing_parameters, status,
                   requested_by, expires_at, confirmed_at, completed_at,
                   result, created_at, modified_at
            FROM action_resolution_plan
            WHERE plan_id = %s
            LIMIT 1
            """,
            (normalized_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        if row[7] in {"pending", "awaiting_input", "confirmed"}:
            cursor.execute(
                """
                UPDATE action_resolution_plan
                SET status = 'expired', modified_at = NOW()
                WHERE plan_id = %s AND expires_at <= NOW()
                RETURNING status
                """,
                (normalized_id,),
            )
            if cursor.fetchone():
                row = list(row)
                row[7] = "expired"
    return {
        "plan_id": str(row[0]),
        "request_text": row[1],
        "action_object": row[2] or {},
        "workflow_snapshot": row[3] or {},
        "execution_plan": row[4] or [],
        "parameters": row[5] or {},
        "missing_parameters": row[6] or [],
        "status": row[7],
        "requested_by": row[8],
        "expires_at": row[9],
        "confirmed_at": row[10],
        "completed_at": row[11],
        "result": row[12] or {},
        "created_at": row[13],
        "modified_at": row[14],
    }


def confirm_action_resolution_plan(connection: Any, plan_id: Any) -> dict[str, Any] | None:
    try:
        normalized_id = uuid.UUID(str(plan_id))
    except (TypeError, ValueError, AttributeError):
        return None
    with connection.cursor() as cursor:
        cursor.execute(create_action_resolution_plan_table)
        cursor.execute(
            """
            UPDATE action_resolution_plan
            SET status = 'confirmed', confirmed_at = NOW(), modified_at = NOW()
            WHERE plan_id = %s
              AND status = 'pending'
              AND expires_at > NOW()
              AND jsonb_array_length(missing_parameters) = 0
            RETURNING plan_id
            """,
            (normalized_id,),
        )
        if cursor.fetchone() is None:
            connection.rollback()
            return get_action_resolution_plan(connection, normalized_id)
    connection.commit()
    return get_action_resolution_plan(connection, normalized_id)


def list_pipeline_runs_by_document(
    connection: psycopg2.extensions.connection,
    document_id: int,
    run_status: str | None = None,
    workflow_key: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT r.run_id, r.workflow_version_id, r.workflow_key, r.run_status,
                            r.paused_at_step_key, r.input_context, r.current_context,
                            r.output_context, r.started_by, r.started_at, r.finished_at,
                            r.created_at, r.modified_at
            FROM workflow_pipeline_run r
            JOIN workflow_pipeline_run_document_link l ON l.run_id = r.run_id
            WHERE l.document_id = %s
              AND (%s IS NULL OR r.run_status = %s)
              AND (%s IS NULL OR r.workflow_key = %s)
            ORDER BY r.created_at DESC, r.run_id DESC
            LIMIT %s
            """,
            (int(document_id), run_status, run_status, workflow_key, workflow_key, int(limit)),
        )
        rows = cur.fetchall()

    return [
        {
            "run_id": int(r[0]),
            "workflow_version_id": r[1],
            "workflow_key": r[2],
            "run_status": r[3],
            "paused_at_step_key": r[4],
            "input_context": r[5] or {},
            "current_context": r[6] or {},
            "output_context": r[7] or {},
            "started_by": r[8],
            "started_at": r[9],
            "finished_at": r[10],
            "created_at": r[11],
            "modified_at": r[12],
        }
        for r in rows
    ]


def list_latest_pipeline_runs_by_document_workflow(
    connection: psycopg2.extensions.connection,
    document_id: int,
    workflow_key: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return latest run per workflow (or workflow version fallback) for a document."""
    with connection.cursor() as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    COALESCE(NULLIF(r.workflow_key, ''), CONCAT('__workflow_version__', COALESCE(r.workflow_version_id::text, 'unknown'))) AS workflow_group_key,
                    r.run_id,
                    r.workflow_version_id,
                    r.workflow_key,
                    r.run_status,
                    r.paused_at_step_key,
                    r.input_context,
                    r.current_context,
                    r.output_context,
                    r.started_by,
                    r.started_at,
                    r.finished_at,
                    r.created_at,
                    r.modified_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(NULLIF(r.workflow_key, ''), CONCAT('__workflow_version__', COALESCE(r.workflow_version_id::text, 'unknown')))
                        ORDER BY r.created_at DESC, r.run_id DESC
                    ) AS rn
                FROM workflow_pipeline_run r
                JOIN workflow_pipeline_run_document_link l ON l.run_id = r.run_id
                WHERE l.document_id = %s
                  AND (%s IS NULL OR r.workflow_key = %s)
            )
            SELECT workflow_group_key, run_id, workflow_version_id, workflow_key, run_status,
                   paused_at_step_key, input_context, current_context, output_context,
                   started_by, started_at, finished_at, created_at, modified_at
            FROM ranked
            WHERE rn = 1
            ORDER BY created_at DESC, run_id DESC
            LIMIT %s
            """,
            (int(document_id), workflow_key, workflow_key, int(limit)),
        )
        rows = cur.fetchall()

    return [
        {
            "workflow_group_key": r[0],
            "run_id": int(r[1]),
            "workflow_version_id": r[2],
            "workflow_key": r[3],
            "run_status": r[4],
            "paused_at_step_key": r[5],
            "input_context": r[6] or {},
            "current_context": r[7] or {},
            "output_context": r[8] or {},
            "started_by": r[9],
            "started_at": r[10],
            "finished_at": r[11],
            "created_at": r[12],
            "modified_at": r[13],
        }
        for r in rows
    ]


_DB_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def list_exportable_tables(
    connection: psycopg2.extensions.connection,
    schema_name: str = "public",
) -> list[str]:
    schema = str(schema_name or "public").strip().lower() or "public"
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname = %s
            ORDER BY tablename
            """,
            (schema,),
        )
        rows = cur.fetchall()
    return [str(row[0]) for row in rows if row and row[0]]


def export_tables_to_csv_bundle(
    connection: psycopg2.extensions.connection,
    *,
    table_names: list[str] | None = None,
    include_all: bool = False,
    schema_name: str = "public",
    output_file_name: str | None = None,
) -> dict[str, Any]:
    """Export one, multiple, or all tables as CSV files.

    - Single table: writes one CSV and returns its path.
    - Multiple/all tables: writes CSVs and a ZIP bundle for migration portability.
    """
    schema = str(schema_name or "public").strip().lower() or "public"
    if not _DB_IDENTIFIER_PATTERN.match(schema):
        raise ValueError(f"Invalid schema_name: {schema}")

    available = list_exportable_tables(connection, schema_name=schema)
    available_set = {item.lower() for item in available}

    requested = [str(item or "").strip().lower() for item in (table_names or []) if str(item or "").strip()]

    if include_all or not requested:
        selected = list(available)
    else:
        selected = []
        for table in requested:
            if not _DB_IDENTIFIER_PATTERN.match(table):
                raise ValueError(f"Invalid table name: {table}")
            if table not in available_set:
                raise ValueError(f"Table not found in schema '{schema}': {table}")
            if table not in selected:
                selected.append(table)

    if not selected:
        raise ValueError(f"No tables available for export in schema '{schema}'")

    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    export_root = Path(__file__).resolve().parent / "generated" / "db_exports"
    run_dir = export_root / f"db_export_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    exported_files: list[dict[str, Any]] = []
    for table in selected:
        csv_path = run_dir / f"{table}.csv"
        table_ref = f'"{schema}"."{table}"'
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            with connection.cursor() as cur:
                cur.copy_expert(
                    f"COPY {table_ref} TO STDOUT WITH (FORMAT CSV, HEADER TRUE)",
                    handle,
                )
        exported_files.append(
            {
                "table": table,
                "schema": schema,
                "file_path": str(csv_path),
                "file_name": csv_path.name,
                "file_size_bytes": csv_path.stat().st_size,
            }
        )

    bundle_path: Path | None = None
    if len(exported_files) > 1:
        bundle_base = str(output_file_name or "").strip()
        if bundle_base:
            bundle_base = re.sub(r"[^A-Za-z0-9._-]+", "_", bundle_base)
            if not bundle_base.lower().endswith(".zip"):
                bundle_base = f"{bundle_base}.zip"
        else:
            bundle_base = f"db_export_{stamp}.zip"
        bundle_path = run_dir / bundle_base
        with zipfile.ZipFile(bundle_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for item in exported_files:
                zf.write(item["file_path"], arcname=item["file_name"])

    return {
        "schema_name": schema,
        "include_all": bool(include_all),
        "selected_tables": selected,
        "table_count": len(selected),
        "export_dir": str(run_dir),
        "files": exported_files,
        "bundle_path": str(bundle_path) if bundle_path is not None else None,
    }


def _load_fk_edges(
    connection: psycopg2.extensions.connection,
    schema_name: str,
) -> list[tuple[str, str]]:
    """Return FK dependency edges as (parent_table, child_table)."""
    sql = """
    SELECT
        p.relname AS parent_table,
        c.relname AS child_table
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace nc ON nc.oid = c.relnamespace
    JOIN pg_class p ON p.oid = con.confrelid
    JOIN pg_namespace np ON np.oid = p.relnamespace
    WHERE con.contype = 'f'
      AND nc.nspname = %s
      AND np.nspname = %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, (schema_name, schema_name))
        rows = cur.fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def _topological_import_order(
    selected_tables: list[str],
    fk_edges: list[tuple[str, str]],
) -> list[str]:
    selected = [str(t) for t in selected_tables]
    selected_set = set(selected)
    indegree: dict[str, int] = {t: 0 for t in selected}
    graph: dict[str, list[str]] = {t: [] for t in selected}

    for parent, child in fk_edges:
        if parent not in selected_set or child not in selected_set:
            continue
        graph[parent].append(child)
        indegree[child] += 1

    queue = sorted([t for t, deg in indegree.items() if deg == 0])
    ordered: list[str] = []
    while queue:
        node = queue.pop(0)
        ordered.append(node)
        for child in sorted(graph.get(node, [])):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(ordered) < len(selected):
        remaining = sorted([t for t in selected if t not in ordered])
        ordered.extend(remaining)
    return ordered


def import_tables_from_csv_bundle(
    connection: psycopg2.extensions.connection,
    *,
    source_zip_path: str | None = None,
    source_dir: str | None = None,
    table_names: list[str] | None = None,
    include_all: bool = False,
    schema_name: str = "public",
    truncate_before_import: bool = False,
    continue_on_error: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Import CSV data into IDMS tables from a zip bundle or directory.

    One of source_zip_path or source_dir must be provided.
    """
    schema = str(schema_name or "public").strip().lower() or "public"
    if not _DB_IDENTIFIER_PATTERN.match(schema):
        raise ValueError(f"Invalid schema_name: {schema}")

    if not source_zip_path and not source_dir:
        raise ValueError("source_zip_path or source_dir is required")

    db_tables = list_exportable_tables(connection, schema_name=schema)
    db_table_set = {t.lower() for t in db_tables}

    source_csv_map: dict[str, str] = {}
    source_kind = "directory"
    zip_path_obj: Path | None = None
    dir_path_obj: Path | None = None

    if source_zip_path:
        source_kind = "zip"
        zip_path_obj = Path(str(source_zip_path)).resolve()
        if not zip_path_obj.exists() or not zip_path_obj.is_file():
            raise ValueError(f"source_zip_path not found: {zip_path_obj}")
        with zipfile.ZipFile(zip_path_obj, mode="r") as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".csv"):
                    continue
                table = Path(name).name[:-4].strip().lower()
                if table and _DB_IDENTIFIER_PATTERN.match(table):
                    source_csv_map[table] = name
    else:
        dir_path_obj = Path(str(source_dir)).resolve()
        if not dir_path_obj.exists() or not dir_path_obj.is_dir():
            raise ValueError(f"source_dir not found: {dir_path_obj}")
        for item in sorted(dir_path_obj.iterdir()):
            if not item.is_file() or item.suffix.lower() != ".csv":
                continue
            table = item.stem.strip().lower()
            if table and _DB_IDENTIFIER_PATTERN.match(table):
                source_csv_map[table] = str(item)

    if not source_csv_map:
        raise ValueError("No CSV files found in source")

    requested = [str(item or "").strip().lower() for item in (table_names or []) if str(item or "").strip()]
    if include_all or not requested:
        selected = sorted([table for table in source_csv_map.keys() if table in db_table_set])
    else:
        selected = []
        for table in requested:
            if not _DB_IDENTIFIER_PATTERN.match(table):
                raise ValueError(f"Invalid table name: {table}")
            if table not in db_table_set:
                raise ValueError(f"Table not found in DB schema '{schema}': {table}")
            if table not in source_csv_map:
                raise ValueError(f"CSV for table '{table}' not found in source")
            if table not in selected:
                selected.append(table)

    if not selected:
        raise ValueError("No matching DB tables found for import")

    fk_edges = _load_fk_edges(connection, schema_name=schema)
    import_order = _topological_import_order(selected, fk_edges)

    if dry_run:
        return {
            "schema_name": schema,
            "source_kind": source_kind,
            "source_zip_path": str(zip_path_obj) if zip_path_obj else None,
            "source_dir": str(dir_path_obj) if dir_path_obj else None,
            "selected_tables": selected,
            "import_order": import_order,
            "table_count": len(selected),
            "dry_run": True,
        }

    imported: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    if truncate_before_import:
        quoted = [f'"{schema}"."{table}"' for table in selected]
        with connection.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {', '.join(quoted)} RESTART IDENTITY CASCADE")
        connection.commit()

    try:
        if source_kind == "zip" and zip_path_obj is not None:
            with zipfile.ZipFile(zip_path_obj, mode="r") as zf:
                for table in import_order:
                    member_name = source_csv_map.get(table)
                    if not member_name:
                        continue
                    try:
                        with zf.open(member_name, mode="r") as raw:
                            with TextIOWrapper(raw, encoding="utf-8", newline="") as text_stream:
                                with connection.cursor() as cur:
                                    cur.copy_expert(
                                        f'COPY "{schema}"."{table}" FROM STDIN WITH (FORMAT CSV, HEADER TRUE)',
                                        text_stream,
                                    )
                        connection.commit()
                        imported.append({"table": table, "status": "imported", "source": member_name})
                    except Exception as exc:
                        connection.rollback()
                        errors.append({"table": table, "error": str(exc)})
                        if not continue_on_error:
                            break
        else:
            for table in import_order:
                path_text = source_csv_map.get(table)
                if not path_text:
                    continue
                csv_path = Path(path_text)
                try:
                    with csv_path.open("r", encoding="utf-8", newline="") as handle:
                        with connection.cursor() as cur:
                            cur.copy_expert(
                                f'COPY "{schema}"."{table}" FROM STDIN WITH (FORMAT CSV, HEADER TRUE)',
                                handle,
                            )
                    connection.commit()
                    imported.append({"table": table, "status": "imported", "source": str(csv_path)})
                except Exception as exc:
                    connection.rollback()
                    errors.append({"table": table, "error": str(exc)})
                    if not continue_on_error:
                        break
    except Exception:
        connection.rollback()
        raise

    success = len(errors) == 0
    return {
        "schema_name": schema,
        "source_kind": source_kind,
        "source_zip_path": str(zip_path_obj) if zip_path_obj else None,
        "source_dir": str(dir_path_obj) if dir_path_obj else None,
        "selected_tables": selected,
        "import_order": import_order,
        "table_count": len(selected),
        "imported_count": len(imported),
        "error_count": len(errors),
        "imported": imported,
        "errors": errors,
        "success": success,
        "dry_run": False,
    }


if __name__ == "__main__":
    connection = get_connection()
    try:
        create_tables(connection, recreate=False)
    finally:
        connection.close()
