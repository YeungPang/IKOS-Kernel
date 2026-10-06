from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable

import psycopg2
from psycopg2.extras import RealDictCursor

import object_db


@dataclass(frozen=True)
class SourceTableInfo:
    schema_name: str
    table_name: str
    object_kind: str
    fingerprint: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class SourceRowMappingResult:
    source_pk_value: str
    action: str
    target_object_id: int | None
    source_row_hash: str
    source_row_fingerprint: str
    source_row_updated_at: str | None


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _normalize_identifier(value: Any) -> str:
    return str(value or "").strip().lower()


def _coerce_iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt_value = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt_value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=timezone.utc)
    return dt_value.isoformat()


def discover_source_tables(
    connection: psycopg2.extensions.connection,
    schema_names: Iterable[str] | None = None,
    include_views: bool = False,
) -> list[SourceTableInfo]:
    schema_filter = [str(item).strip() for item in (schema_names or []) if str(item).strip()]
    object_types = ["BASE TABLE"]
    if include_views:
        object_types.append("VIEW")

    sql = """
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
      AND table_type = ANY(%s)
    """
    params: list[Any] = [object_types]
    if schema_filter:
        sql += " AND table_schema = ANY(%s)"
        params.append(schema_filter)
    sql += " ORDER BY table_schema, table_name"

    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, tuple(params))
        tables = cursor.fetchall() or []

    results: list[SourceTableInfo] = []
    for table in tables:
        schema_name = _normalize_identifier(table.get("table_schema"))
        table_name = _normalize_identifier(table.get("table_name"))
        object_kind = _normalize_identifier(table.get("table_type")) or "table"

        column_sql = """
        SELECT column_name, data_type, is_nullable, ordinal_position, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(column_sql, (schema_name, table_name))
            columns = cursor.fetchall() or []

        foreign_key_sql = """
        SELECT
            kcu.column_name AS column_name,
            ccu.table_schema AS foreign_schema_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            tc.constraint_name AS constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = %s
          AND tc.table_name = %s
        ORDER BY tc.constraint_name, kcu.ordinal_position
        """
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(foreign_key_sql, (schema_name, table_name))
            foreign_keys = cursor.fetchall() or []

        column_signature = [
            {
                "column_name": _normalize_identifier(column.get("column_name")),
                "data_type": _normalize_identifier(column.get("data_type")),
                "is_nullable": str(column.get("is_nullable") or "").strip().lower(),
                "ordinal_position": int(column.get("ordinal_position") or 0),
                "column_default": str(column.get("column_default") or "").strip(),
            }
            for column in columns
        ]
        foreign_key_signature = [
            {
                "column_name": _normalize_identifier(item.get("column_name")),
                "foreign_schema_name": _normalize_identifier(item.get("foreign_schema_name")),
                "foreign_table_name": _normalize_identifier(item.get("foreign_table_name")),
                "foreign_column_name": _normalize_identifier(item.get("foreign_column_name")),
                "constraint_name": _normalize_identifier(item.get("constraint_name")),
            }
            for item in foreign_keys
        ]
        category_tokens = table_name.lower()
        descriptive_column_tokens = " ".join(
            str(column.get("column_name") or "").lower()
            for column in column_signature
            if not str(column.get("column_name") or "").lower().endswith(("_id", "_pk"))
            and str(column.get("column_name") or "").lower() not in {"id", "pk"}
        )
        query_categories = sorted(
            {
                category
                for category, terms in {
                    "customer": ("customer", "client", "account"),
                    "contact": ("contact", "person", "telephone", "phone", "email"),
                    "purchase_order": ("purchase_order", "order", "ordered", "product"),
                    "invoice": ("invoice", "bill", "vat"),
                    "address": ("country", "city", "address", "region"),
                }.items()
                if any(term in category_tokens for term in terms)
                or any(term in descriptive_column_tokens for term in terms)
            }
        )
        calendar_profile = _calendar_profile(table_name, column_signature)
        if calendar_profile:
            query_categories = sorted(set(query_categories).union({"calendar_event"}))
        fingerprint = _sha256_text(_stable_json({"schema": schema_name, "table": table_name, "kind": object_kind, "columns": column_signature, "foreign_keys": foreign_key_signature}))
        results.append(
            SourceTableInfo(
                schema_name=schema_name,
                table_name=table_name,
                object_kind=object_kind,
                fingerprint=fingerprint,
                metadata={
                    "columns": column_signature,
                    "foreign_keys": foreign_key_signature,
                    "query_categories": query_categories,
                    **({"calendar_profile": calendar_profile} if calendar_profile else {}),
                },
            )
        )

    return results


_SEED_FILE_PATH = os.path.join(os.path.dirname(__file__), "seeded_business_terms.json")


def _load_seeded_business_term_aliases() -> dict[str, list[str]]:
    try:
        with open(_SEED_FILE_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            cleaned: dict[str, list[str]] = {}
            for key, aliases in payload.items():
                if isinstance(aliases, list):
                    cleaned[str(key)] = [str(item).strip() for item in aliases if str(item).strip()]
            return cleaned
    except Exception:
        pass
    return {}


GENERAL_SCHEMA_CATEGORY_ALIASES: dict[str, list[str]] = _load_seeded_business_term_aliases()


def _calendar_profile(table_name: str, columns: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Infer a reusable calendar-event schema profile from catalog metadata."""
    table_tokens = _normalize_schema_term_string(table_name).replace(" ", "_")
    column_names = {
        _normalize_schema_term_string(item.get("column_name") or "").replace(" ", "_")
        for item in columns
        if isinstance(item, dict)
    }
    table_signal = any(token in table_tokens for token in ("calendar", "event", "appointment", "meeting", "schedule"))
    temporal_signal = bool(column_names.intersection({
        "starts_at", "start_at", "start_time", "starts_on", "ends_at", "end_at",
        "end_time", "event_date", "recurrence_rule", "rrule", "recurrence",
    }))
    if not (table_signal and temporal_signal):
        return None

    def first_match(names: tuple[str, ...]) -> str | None:
        return next((name for name in names if name in column_names), None)

    return {
        "entity_kind": "calendar_event",
        "start_column": first_match(("starts_at", "start_at", "start_time", "starts_on", "event_date")),
        "end_column": first_match(("ends_at", "end_at", "end_time", "ends_on")),
        "title_column": first_match(("title", "event_title", "subject", "name")),
        "status_column": first_match(("status", "event_status")),
        "event_type_column": first_match(("event_type", "type", "category")),
        "recurrence_column": first_match(("recurrence_rule", "rrule", "recurrence", "repeat_rule")),
        "updated_at_column": first_match(("updated_at", "modified_at", "last_modified")),
    }


def _normalize_schema_term_string(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("_", " ")
    text = text.replace("-", " ")
    text = text.replace("/", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_general_category_terms(table_info: SourceTableInfo) -> list[dict[str, Any]]:
    if not table_info:
        return []

    candidate_strings: list[str] = [
        table_info.schema_name,
        table_info.table_name,
    ]
    for column in (table_info.metadata or {}).get("columns") or []:
        if isinstance(column, dict):
            candidate_strings.append(str(column.get("column_name") or ""))

    combined_text = " ".join(_normalize_schema_term_string(item) for item in candidate_strings if _normalize_schema_term_string(item))
    if not combined_text:
        return []

    rows: list[dict[str, Any]] = []
    for canonical_name, aliases in GENERAL_SCHEMA_CATEGORY_ALIASES.items():
        for alias in aliases:
            alias_norm = _normalize_schema_term_string(alias)
            if not alias_norm:
                continue
            if alias_norm in combined_text or alias_norm.replace(" ", "") in combined_text.replace(" ", ""):
                rows.append(
                    {
                        "kind": "category",
                        "canonical_name": canonical_name,
                        "term_text": alias,
                        "language": "en",
                        "source_type": "ingest",
                        "metadata": {
                            "source": "external_schema_sync",
                            "schema_name": table_info.schema_name,
                            "table_name": table_info.table_name,
                        },
                    }
                )
                break
    return rows


def _extract_schema_alias_rows(table_info: SourceTableInfo) -> list[dict[str, Any]]:
    if not table_info:
        return []

    table_name = str(table_info.table_name or "").strip()
    if not table_name:
        return []

    def _canonical_entity_name(name: str) -> str:
        text = str(name or "").strip().lower()
        text = text.replace("_", " ")
        text = text.replace("-", " ")
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return "entity"
        if text.endswith("s") and len(text) > 3 and not text.endswith("ss"):
            text = text[:-1]
        text = text.replace(" people", " person")
        text = text.replace(" persons", " person")
        text = text.replace(" contacts", " contact")
        if text.endswith(" person"):
            return "contact_person"
        if "customer" in text or "client" in text or "company" in text:
            return "client"
        if "supplier" in text:
            return "supplier"
        return text.replace(" ", "_")

    def _canonical_column_name(column_name: str, *, default_kind: str) -> str:
        text = str(column_name or "").strip().lower()
        if not text:
            return ""
        if "customer" in text or "client" in text or "company" in text:
            return "client"
        if "contact" in text or "person" in text or "related" in text:
            return "contact_person"
        if text.endswith("_id") or text.endswith("id"):
            return default_kind
        return default_kind

    entity_canonical = _canonical_entity_name(table_name)
    rows: list[dict[str, Any]] = [
        {
            "alias_text": table_name,
            "canonical_name": entity_canonical,
            "kind": "entity_type",
            "language": "en",
            "source_type": "ingest",
            "priority": 85,
            "metadata": {
                "source": "external_schema_sync",
                "schema_name": table_info.schema_name,
                "table_name": table_info.table_name,
            },
        }
    ]

    for column in (table_info.metadata or {}).get("columns") or []:
        if not isinstance(column, dict):
            continue
        column_name = str(column.get("column_name") or "").strip()
        if not column_name:
            continue
        canonical_kind = _canonical_column_name(column_name, default_kind=entity_canonical)
        rows.append(
            {
                "alias_text": column_name,
                "canonical_name": canonical_kind,
                "kind": "attribute",
                "language": "en",
                "source_type": "ingest",
                "priority": 80,
                "metadata": {
                    "source": "external_schema_sync",
                    "schema_name": table_info.schema_name,
                    "table_name": table_info.table_name,
                    "column_name": column_name,
                },
            }
        )

    return rows


def build_source_schema_taxonomy_rows(source_id: int, table_info: SourceTableInfo) -> list[dict[str, Any]]:
    """Classify table and columns for query routing without reading source rows."""
    if int(source_id or 0) <= 0 or not table_info:
        return []

    schema_name = str(table_info.schema_name or "").strip()
    table_name = str(table_info.table_name or "").strip()
    if not schema_name or not table_name:
        return []

    table_terms = _normalize_schema_term_string(table_name)
    category_names = {table_terms.replace(" ", "_") or "entity"}
    for canonical_name, aliases in GENERAL_SCHEMA_CATEGORY_ALIASES.items():
        if any(_normalize_schema_term_string(alias) in table_terms for alias in aliases if _normalize_schema_term_string(alias)):
            category_names.add(_normalize_schema_term_string(canonical_name).replace(" ", "_"))

    metadata = table_info.metadata if isinstance(table_info.metadata, dict) else {}
    calendar_profile = metadata.get("calendar_profile") if isinstance(metadata.get("calendar_profile"), dict) else _calendar_profile(
        table_name,
        metadata.get("columns") if isinstance(metadata.get("columns"), list) else [],
    )
    if calendar_profile:
        category_names.add("calendar_event")
    rows: list[dict[str, Any]] = []
    for category_name in sorted(category_names):
        rows.append(
            {
                "source_id": int(source_id), "schema_name": schema_name, "table_name": table_name,
                "category_name": category_name, "role_name": "entity_candidate", "confidence": 0.8,
                "metadata": {"source": "external_schema_discovery", "basis": "table_name"},
            }
        )

    for column in metadata.get("columns") or []:
        if not isinstance(column, dict):
            continue
        column_name = str(column.get("column_name") or "").strip()
        if not column_name:
            continue
        normalized = _normalize_schema_term_string(column_name)
        data_type = str(column.get("data_type") or "").strip().lower()
        roles = {"attribute"}
        is_bookkeeping = normalized in {"source row no", "row number", "row no", "line number", "line no"}
        if normalized.endswith(" id") or normalized.endswith(" pk") or normalized == "id":
            roles.add("identity")
        if any(token in normalized for token in ("date", "time", "timestamp", "created", "updated")) or "date" in data_type or "time" in data_type:
            roles.update({"temporal", "filter"})
        if calendar_profile:
            start_column = str(calendar_profile.get("start_column") or "").lower()
            end_column = str(calendar_profile.get("end_column") or "").lower()
            recurrence_column = str(calendar_profile.get("recurrence_column") or "").lower()
            if column_name.lower() in {start_column, end_column, recurrence_column} - {""}:
                roles.update({"temporal", "filter"})
        if not is_bookkeeping and any(token in normalized for token in ("amount", "total", "value", "price", "cost", "quantity", "balance")):
            roles.update({"measure", "filter"})
        if any(token in normalized for token in ("country", "status", "type", "category", "name", "code", "email", "phone", "city", "region")):
            roles.add("filter")
        if any(str(fk.get("column_name") or "").strip().lower() == column_name.lower() for fk in metadata.get("foreign_keys") or [] if isinstance(fk, dict)):
            roles.add("relationship_key")

        for category_name in sorted(category_names):
            for role_name in sorted(roles):
                rows.append(
                    {
                        "source_id": int(source_id), "schema_name": schema_name, "table_name": table_name,
                        "column_name": column_name, "category_name": category_name, "role_name": role_name,
                        "confidence": 0.7 if role_name == "attribute" else 0.85,
                        "metadata": {"source": "external_schema_discovery", "data_type": data_type, "basis": "column_name"},
                    }
                )
    return rows


def upsert_source_table_inventory(
    metadata_connection: psycopg2.extensions.connection,
    source_id: int,
    table_info: SourceTableInfo,
) -> None:
    sql = """
    INSERT INTO source_schema_inventory (
        source_id,
        schema_name,
        table_name,
        object_kind,
        fingerprint,
        metadata
    )
    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
    ON CONFLICT (source_id, schema_name, table_name)
    DO UPDATE SET
        object_kind = EXCLUDED.object_kind,
        fingerprint = EXCLUDED.fingerprint,
        metadata = EXCLUDED.metadata,
        discovered_at = NOW()
    """
    with metadata_connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                int(source_id),
                table_info.schema_name,
                table_info.table_name,
                table_info.object_kind,
                table_info.fingerprint,
                _stable_json(table_info.metadata),
            ),
        )
    metadata_connection.commit()

    general_aliases = _extract_general_category_terms(table_info)
    schema_alias_rows = _extract_schema_alias_rows(table_info)
    alias_rows = []
    if general_aliases:
        alias_rows.extend(
            {
                "alias_text": row["term_text"],
                "canonical_name": row["canonical_name"],
                "kind": row["kind"],
                "language": row["language"],
                "source_type": row["source_type"],
                "metadata": row["metadata"],
                "priority": 80,
            }
            for row in general_aliases
        )
    if schema_alias_rows:
        alias_rows.extend(schema_alias_rows)
    if alias_rows:
        try:
            object_db.upsert_query_term_alias(metadata_connection, alias_rows)
            if general_aliases:
                object_db.upsert_semantic_terms(metadata_connection, general_aliases)
        except Exception:
            pass

    taxonomy_rows = build_source_schema_taxonomy_rows(source_id, table_info)
    if taxonomy_rows:
        with metadata_connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM source_schema_taxonomy
                WHERE tenant_id = %s::uuid AND source_id = %s
                  AND schema_name = %s AND table_name = %s
                """,
                (object_db.get_tenant_id(), int(source_id), table_info.schema_name, table_info.table_name),
            )
        metadata_connection.commit()
        object_db.upsert_source_schema_taxonomy(metadata_connection, taxonomy_rows)
        category_rows: list[dict[str, Any]] = []
        for taxonomy in taxonomy_rows:
            category_name = str(taxonomy.get("category_name") or "").strip().lower()
            if not category_name:
                continue
            role_name = str(taxonomy.get("role_name") or "").strip().lower()
            category_rows.append(
                {
                    "category_name": category_name,
                    "source_scope": "external",
                    "access_method": "sql",
                    "source_id": int(source_id),
                    "schema_name": taxonomy.get("schema_name"),
                    "table_name": taxonomy.get("table_name"),
                    "column_name": taxonomy.get("column_name"),
                    "entity_types": [category_name] if role_name == "entity_candidate" else [],
                    "attributes": [taxonomy.get("column_name")] if role_name in {"attribute", "filter", "measure", "temporal", "relationship_key", "identity"} and taxonomy.get("column_name") else [],
                    "metadata": {"source": "external_schema_taxonomy", "role_name": role_name},
                    "confidence": taxonomy.get("confidence") or 0.5,
                }
            )
        object_db.upsert_category_registry_rows(metadata_connection, category_rows)
        for category_name in sorted({str(row.get("category_name") or "").strip().lower() for row in taxonomy_rows if str(row.get("category_name") or "").strip()}):
            category_taxonomy = [row for row in taxonomy_rows if str(row.get("category_name") or "").strip().lower() == category_name]
            object_db.upsert_category_definition(
                metadata_connection,
                name=category_name,
                source_type="external_db",
                access_method="sql",
                source_id=int(source_id),
                entity_mappings=[
                    {
                        "entity_type": category_name,
                        "source_table": row.get("table_name"),
                        "source_column": row.get("column_name"),
                        "role_name": row.get("role_name"),
                        "confidence": row.get("confidence"),
                    }
                    for row in category_taxonomy
                    if str(row.get("role_name") or "") == "entity_candidate"
                ],
                attribute_mappings=[
                    {
                        "attribute_name": row.get("column_name"),
                        "canonical_name": row.get("column_name"),
                        "source_table": row.get("table_name"),
                        "source_column": row.get("column_name"),
                        "role_name": row.get("role_name"),
                        "confidence": row.get("confidence"),
                    }
                    for row in category_taxonomy
                    if row.get("column_name")
                ],
                metadata={"source": "external_schema_discovery"},
            )


def register_source_database(
    metadata_connection: psycopg2.extensions.connection,
    *,
    source_key: str,
    source_name: str,
    db_name: str,
    db_host: str | None = None,
    db_port: int | None = None,
    db_user: str | None = None,
    db_schema: str | None = None,
    connection_hint: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sql = """
    INSERT INTO source_database_registry (
        tenant_id,
        source_key,
        source_name,
        db_host,
        db_port,
        db_name,
        db_user,
        db_schema,
        connection_hint,
        metadata,
        updated_at
    )
    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
    ON CONFLICT (tenant_id, source_key)
    DO UPDATE SET
        source_name = EXCLUDED.source_name,
        db_host = EXCLUDED.db_host,
        db_port = EXCLUDED.db_port,
        db_name = EXCLUDED.db_name,
        db_user = EXCLUDED.db_user,
        db_schema = EXCLUDED.db_schema,
        connection_hint = EXCLUDED.connection_hint,
        metadata = COALESCE(source_database_registry.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        status = 'active',
        updated_at = NOW()
    RETURNING source_id, source_key, source_name, db_host, db_port, db_name, db_user, db_schema, connection_hint, status, metadata, created_at, updated_at
    """

    with metadata_connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                object_db.get_tenant_id(),
                str(source_key or "").strip(),
                str(source_name or "").strip(),
                db_host,
                db_port,
                str(db_name or "").strip(),
                db_user,
                db_schema,
                connection_hint,
                _stable_json(metadata or {}),
            ),
        )
        row = cursor.fetchone()
    metadata_connection.commit()

    return {
        "source_id": int(row[0]),
        "source_key": row[1],
        "source_name": row[2],
        "db_host": row[3],
        "db_port": row[4],
        "db_name": row[5],
        "db_user": row[6],
        "db_schema": row[7],
        "connection_hint": row[8],
        "status": row[9],
        "metadata": row[10] if isinstance(row[10], dict) else {},
        "created_at": row[11].isoformat() if row[11] is not None else None,
        "updated_at": row[12].isoformat() if row[12] is not None else None,
    }


def load_source_mapping(
    metadata_connection: psycopg2.extensions.connection,
    source_id: int,
    schema_name: str,
    table_name: str,
    source_pk_value: str,
    target_class_name: str,
) -> dict[str, Any] | None:
    sql = """
    SELECT mapping_id, source_id, schema_name, table_name, source_pk_value, target_class_name,
           target_object_id, target_metadata, source_row_hash, source_row_updated_at, source_row_fingerprint,
           mapped_at, updated_at
    FROM source_entity_mapping
    WHERE source_id = %s
      AND schema_name = %s
      AND table_name = %s
      AND source_pk_value = %s
      AND target_class_name = %s
    LIMIT 1
    """
    with metadata_connection.cursor() as cursor:
        cursor.execute(sql, (int(source_id), schema_name, table_name, str(source_pk_value), target_class_name))
        row = cursor.fetchone()

    if row is None:
        return None

    return {
        "mapping_id": int(row[0]),
        "source_id": int(row[1]),
        "schema_name": row[2],
        "table_name": row[3],
        "source_pk_value": row[4],
        "target_class_name": row[5],
        "target_object_id": row[6],
        "target_metadata": row[7] if isinstance(row[7], dict) else {},
        "source_row_hash": row[8],
        "source_row_updated_at": _coerce_iso_datetime(row[9]),
        "source_row_fingerprint": row[10],
        "mapped_at": row[11].isoformat() if row[11] is not None else None,
        "updated_at": row[12].isoformat() if row[12] is not None else None,
    }


def upsert_source_mapping(
    metadata_connection: psycopg2.extensions.connection,
    *,
    source_id: int,
    schema_name: str,
    table_name: str,
    source_pk_value: str,
    target_class_name: str,
    target_object_id: int | None,
    target_metadata: dict[str, Any],
    source_row_hash: str,
    source_row_updated_at: str | None,
    source_row_fingerprint: str,
) -> dict[str, Any]:
    sql = """
    INSERT INTO source_entity_mapping (
        source_id,
        schema_name,
        table_name,
        source_pk_value,
        target_class_name,
        target_object_id,
        target_metadata,
        source_row_hash,
        source_row_updated_at,
        source_row_fingerprint,
        mapped_at,
        updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, NOW(), NOW())
    ON CONFLICT (source_id, schema_name, table_name, source_pk_value, target_class_name)
    DO UPDATE SET
        target_object_id = EXCLUDED.target_object_id,
        target_metadata = COALESCE(source_entity_mapping.target_metadata, '{}'::jsonb) || EXCLUDED.target_metadata,
        source_row_hash = EXCLUDED.source_row_hash,
        source_row_updated_at = EXCLUDED.source_row_updated_at,
        source_row_fingerprint = EXCLUDED.source_row_fingerprint,
        updated_at = NOW()
    RETURNING mapping_id, source_id, schema_name, table_name, source_pk_value, target_class_name,
              target_object_id, target_metadata, source_row_hash, source_row_updated_at,
              source_row_fingerprint, mapped_at, updated_at
    """
    with metadata_connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                int(source_id),
                schema_name,
                table_name,
                str(source_pk_value),
                target_class_name,
                target_object_id,
                _stable_json(target_metadata or {}),
                source_row_hash,
                source_row_updated_at,
                source_row_fingerprint,
            ),
        )
        row = cursor.fetchone()
    metadata_connection.commit()

    return {
        "mapping_id": int(row[0]),
        "source_id": int(row[1]),
        "schema_name": row[2],
        "table_name": row[3],
        "source_pk_value": row[4],
        "target_class_name": row[5],
        "target_object_id": row[6],
        "target_metadata": row[7] if isinstance(row[7], dict) else {},
        "source_row_hash": row[8],
        "source_row_updated_at": _coerce_iso_datetime(row[9]),
        "source_row_fingerprint": row[10],
        "mapped_at": row[11].isoformat() if row[11] is not None else None,
        "updated_at": row[12].isoformat() if row[12] is not None else None,
    }


def _row_hash(row: dict[str, Any]) -> str:
    normalized = {key: row[key] for key in sorted(row.keys())}
    return _sha256_text(_stable_json(normalized))


def _row_fingerprint(row: dict[str, Any], pk_column: str, updated_at_column: str | None) -> str:
    payload = {
        "pk": str(row.get(pk_column) or ""),
        "updated_at": _coerce_iso_datetime(row.get(updated_at_column)) if updated_at_column else None,
        "hash": _row_hash(row),
    }
    return _sha256_text(_stable_json(payload))


def sync_mapped_rows(
    *,
    source_connection: psycopg2.extensions.connection,
    idms_connection: psycopg2.extensions.connection,
    metadata_connection: psycopg2.extensions.connection,
    source_id: int,
    schema_name: str,
    table_name: str,
    target_class_name: str,
    pk_column: str,
    map_row_to_object: Callable[[dict[str, Any]], dict[str, Any]],
    updated_at_column: str | None = "updated_at",
    metadata_columns: list[str] | None = None,
) -> list[SourceRowMappingResult]:
    column_list = metadata_columns or ["*"]
    if column_list != ["*"]:
        with source_connection.cursor() as schema_cursor:
            schema_cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema_name, table_name),
            )
            available_columns = {str(row[0]).strip() for row in schema_cursor.fetchall() or []}
        column_list = [column for column in column_list if str(column).strip() in available_columns]

    if not column_list:
        raise ValueError(f"No requested metadata columns exist in {schema_name}.{table_name}")

    selected_columns = ", ".join('"' + str(column).replace('"', '""') + '"' for column in column_list)
    quoted_schema = '"' + str(schema_name).replace('"', '""') + '"'
    quoted_table = '"' + str(table_name).replace('"', '""') + '"'
    sql = f"SELECT {selected_columns} FROM {quoted_schema}.{quoted_table}"

    with source_connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall() or []

    results: list[SourceRowMappingResult] = []
    for row in rows:
        normalized_row = dict(row)
        source_pk_value = str(normalized_row.get(pk_column) or "").strip()
        if not source_pk_value:
            continue

        source_row_hash = _row_hash(normalized_row)
        source_row_updated_at = _coerce_iso_datetime(normalized_row.get(updated_at_column)) if updated_at_column else None
        source_row_fingerprint = _row_fingerprint(normalized_row, pk_column=pk_column, updated_at_column=updated_at_column)

        existing = load_source_mapping(
            metadata_connection,
            source_id=source_id,
            schema_name=schema_name,
            table_name=table_name,
            source_pk_value=source_pk_value,
            target_class_name=target_class_name,
        )
        existing_object = None
        if existing and existing.get("target_object_id"):
            existing_object = object_db.get_object_instance_by_id(
                idms_connection,
                int(existing["target_object_id"]),
                include_inactive=True,
            )
        if existing and existing.get("source_row_hash") == source_row_hash and existing_object:
            results.append(
                SourceRowMappingResult(
                    source_pk_value=source_pk_value,
                    action="skipped_unchanged",
                    target_object_id=int(existing["target_object_id"]) if existing.get("target_object_id") else None,
                    source_row_hash=source_row_hash,
                    source_row_fingerprint=source_row_fingerprint,
                    source_row_updated_at=source_row_updated_at,
                )
            )
            continue

        if existing and source_row_updated_at and existing.get("source_row_updated_at"):
            try:
                current_updated_at = datetime.fromisoformat(source_row_updated_at)
                previous_updated_at = datetime.fromisoformat(str(existing.get("source_row_updated_at") or ""))
                if current_updated_at <= previous_updated_at and existing.get("source_row_hash") == source_row_hash and existing_object:
                    results.append(
                        SourceRowMappingResult(
                            source_pk_value=source_pk_value,
                            action="skipped_not_newer",
                            target_object_id=int(existing["target_object_id"]) if existing.get("target_object_id") else None,
                            source_row_hash=source_row_hash,
                            source_row_fingerprint=source_row_fingerprint,
                            source_row_updated_at=source_row_updated_at,
                        )
                    )
                    continue
            except Exception:
                pass

        target_payload = map_row_to_object(normalized_row)
        object_name = str(target_payload.get("object_name") or normalized_row.get(pk_column) or "").strip()
        if not object_name:
            continue
        class_name = str(target_payload.get("class_name") or target_class_name or "entity").strip().lower()
        target_metadata = target_payload.get("metadata") if isinstance(target_payload.get("metadata"), dict) else {}
        status = str(target_payload.get("status") or "active").strip().lower() or "active"
        valid_from = target_payload.get("valid_from")
        valid_until = target_payload.get("valid_until")

        upserted = object_db.upsert_object_instance(
            idms_connection,
            object_name=object_name,
            class_name=class_name,
            metadata=target_metadata,
            status=status,
            valid_from=valid_from,
            valid_until=valid_until,
        )
        mapping = upsert_source_mapping(
            metadata_connection,
            source_id=source_id,
            schema_name=schema_name,
            table_name=table_name,
            source_pk_value=source_pk_value,
            target_class_name=class_name,
            target_object_id=int(upserted["object_id"]),
            target_metadata=target_metadata,
            source_row_hash=source_row_hash,
            source_row_updated_at=source_row_updated_at,
            source_row_fingerprint=source_row_fingerprint,
        )
        results.append(
            SourceRowMappingResult(
                source_pk_value=source_pk_value,
                action="upserted",
                target_object_id=int(mapping["target_object_id"]) if mapping.get("target_object_id") else None,
                source_row_hash=source_row_hash,
                source_row_fingerprint=source_row_fingerprint,
                source_row_updated_at=source_row_updated_at,
            )
        )

    return results
