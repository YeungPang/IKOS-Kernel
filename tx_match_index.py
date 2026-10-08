from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import datetime
from typing import Any

from ikos_config import (
    TX_MATCH_DOC_TYPES,
    TX_MATCH_EMBEDDING_MODEL,
    TX_MATCH_MIN_FIELDS,
    TX_MATCH_QDRANT_API_KEY,
    TX_MATCH_QDRANT_COLLECTION,
    TX_MATCH_QDRANT_HOST,
    TX_MATCH_QDRANT_PORT,
    TX_MATCH_VECTOR_SIZE,
)
from llm_fallback import get_openrouter_client

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import (
        Distance,
        FieldCondition,
        Filter,
        MatchValue,
        PointStruct,
        SparseVector,
        SparseVectorParams,
        VectorParams,
    )
except Exception:  # pragma: no cover - optional dependency
    QdrantClient = None
    PointStruct = None
    VectorParams = None
    Distance = None
    Filter = None
    FieldCondition = None
    MatchValue = None
    SparseVector = None
    SparseVectorParams = None

try:
    from hybrid_search import encode_sparse_vector
except Exception:  # pragma: no cover - optional dependency
    encode_sparse_vector = None


LOGGER = logging.getLogger("idms.tx_match_index")

COLLECTION_NAME_REGEX = re.compile(r"^[A-Za-z0-9_\-]{3,96}$")


def _normalize_collection_name(value: Any) -> str:
    raw = str(value or "").strip()
    if raw and COLLECTION_NAME_REGEX.match(raw):
        return raw
    return ""


def _resolve_default_collection() -> str:
    configured = str(os.getenv("IKOS_MATCH_QDRANT_COLLECTION", "")).strip()
    if configured:
        return configured
    return TX_MATCH_QDRANT_COLLECTION


def _allow_collection_override() -> bool:
    return str(os.getenv("IKOS_MATCH_ALLOW_COLLECTION_OVERRIDE", "true")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _allowed_purposes() -> set[str]:
    raw = str(os.getenv("IKOS_MATCH_ALLOWED_PURPOSES", "transaction_match")).strip()
    if not raw:
        return {"transaction_match"}
    return {
        token.strip().lower()
        for token in raw.split(",")
        if token.strip()
    }


def is_allowed_purpose(value: Any) -> bool:
    purpose = str(value or "").strip().lower()
    if not purpose:
        return False
    allowed = _allowed_purposes()
    if "*" in allowed:
        return True
    return purpose in allowed


def resolve_collection_name(requested: Any = None) -> str:
    requested_name = _normalize_collection_name(requested)
    if requested_name and _allow_collection_override():
        return requested_name
    return _normalize_collection_name(_resolve_default_collection()) or TX_MATCH_QDRANT_COLLECTION


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def _pick_first(attrs: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in attrs and attrs.get(key) not in (None, "", [], {}):
            return attrs.get(key)
    return None


def _doc_type(ingested: dict[str, Any], routed: dict[str, Any]) -> str:
    document = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
    return str(document.get("doc_type") or routed.get("document_type") or "").strip().lower()


def _eligible_doc_type(doc_type: str) -> bool:
    return doc_type in set(TX_MATCH_DOC_TYPES)


def is_eligible_doc_type(doc_type: str) -> bool:
    return _eligible_doc_type(str(doc_type or "").strip().lower())


def _build_feature_text(row: dict[str, Any]) -> str:
    return (
        f"desc={row.get('description') or ''}; "
        f"party={row.get('party') or ''}; "
        f"ref={row.get('reference') or ''}; "
        f"amount={row.get('amount') if row.get('amount') is not None else ''}; "
        f"currency={row.get('currency') or ''}; "
        f"date={row.get('date') or ''}; "
        f"doc_type={row.get('doc_type') or ''}; "
        f"class_name={row.get('class_name') or ''}"
    ).strip()


def _has_required_fields(row: dict[str, Any]) -> bool:
    required = set(TX_MATCH_MIN_FIELDS)
    checks = {
        "amount": row.get("amount") is not None,
        "currency": bool(str(row.get("currency") or "").strip()),
        "date": bool(str(row.get("date") or "").strip()),
        "description": bool(str(row.get("description") or "").strip()),
    }
    return all(checks.get(name, True) for name in required)


def build_tx_match_rows(
    *,
    ingested: dict[str, Any],
    routed: dict[str, Any],
    db_summary: dict[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    doc_type = _doc_type(ingested, routed)
    if not _eligible_doc_type(doc_type):
        return doc_type, []

    document = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
    metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}
    processing_directives = metadata.get("processing_directives") if isinstance(metadata.get("processing_directives"), dict) else {}

    requested_collection = (
        processing_directives.get("matching_collection")
        or metadata.get("matching_collection")
        or document.get("matching_collection")
    )
    purpose = (
        processing_directives.get("matching_purpose")
        or metadata.get("matching_purpose")
        or document.get("matching_purpose")
        or "transaction_match"
    )
    workflow_scope = (
        processing_directives.get("matching_workflow_scope")
        or metadata.get("matching_workflow_scope")
        or document.get("workflow_id")
        or document.get("rule_id")
    )
    tenant_scope = (
        processing_directives.get("matching_tenant_scope")
        or metadata.get("matching_tenant_scope")
        or document.get("legal_entity_ref")
        or metadata.get("legal_entity_ref")
    )

    resolved_collection = ""
    if _allow_collection_override():
        resolved_collection = _normalize_collection_name(requested_collection)

    doc_key = str(document.get("doc_key") or "").strip()
    doc_id = int(db_summary.get("doc_id") or 0)

    rows: list[dict[str, Any]] = []
    entities = ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []

    for entity in entities:
        if not isinstance(entity, dict):
            continue
        class_name = str(entity.get("class_name") or "").strip().lower()
        if not class_name or class_name.endswith("_line"):
            continue

        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        amount = _to_float(
            _pick_first(
                attrs,
                [
                    "amount",
                    "amount_chf",
                    "amount_source_currency",
                    "invoice_total",
                    "total_amount",
                    "gross_amount",
                    "net_amount",
                ],
            )
        )
        currency = _normalize_text(
            _pick_first(attrs, ["currency", "source_currency", "invoice_currency", "payment_currency"])
            or document.get("currency")
        )
        date_value = _normalize_text(
            _pick_first(attrs, ["transaction_date", "invoice_date", "payment_date", "value_date", "date"])
            or document.get("doc_date")
        )
        description = _normalize_text(
            _pick_first(
                attrs,
                [
                    "line_description",
                    "particulars",
                    "description",
                    "booking_text",
                    "merchant_name",
                    "expense_purpose",
                ],
            )
            or entity.get("name")
            or document.get("doc_desc")
        )
        reference = _normalize_text(
            _pick_first(attrs, ["invoice_no", "bill_no", "receipt_no", "reference", "transaction_id", "booking_ref"])
            or document.get("doc_key")
        )
        party = _normalize_text(
            _pick_first(attrs, ["vendor_name", "supplier_name", "merchant", "customer_name", "payee", "payer", "company"])
        )

        row = {
            "record_id": f"doc:{doc_id}:entity:{str(entity.get('entity_id') or entity.get('name') or class_name)}",
            "doc_id": doc_id,
            "doc_key": doc_key,
            "doc_type": doc_type,
            "class_name": class_name,
            "source": "document_ingestion",
            "amount": amount,
            "currency": currency,
            "date": date_value,
            "description": description,
            "reference": reference,
            "party": party,
            "purpose": str(purpose or "transaction_match").strip().lower() or "transaction_match",
            "workflow_scope": str(workflow_scope or "").strip(),
            "tenant_scope": str(tenant_scope or "").strip(),
        }
        if resolved_collection:
            row["matching_collection"] = resolved_collection
        if _has_required_fields(row):
            row["feature_text"] = _build_feature_text(row)
            rows.append(row)

    return doc_type, rows


def _ensure_collection(qdrant: Any, collection_name: str) -> None:
    existing = {c.name for c in qdrant.get_collections().collections}
    if collection_name in existing:
        return

    vectors_config = {
        "dense": VectorParams(size=TX_MATCH_VECTOR_SIZE, distance=Distance.COSINE),
    }

    if SparseVectorParams is not None:
        qdrant.create_collection(
            collection_name=collection_name,
            vectors_config=vectors_config,
            sparse_vectors_config={"sparse": SparseVectorParams()},
        )
    else:
        qdrant.create_collection(
            collection_name=collection_name,
            vectors_config=vectors_config,
        )


def _delete_existing_rows(qdrant: Any, doc_id: int, collection_name: str) -> int:
    if Filter is None or FieldCondition is None or MatchValue is None:
        return 0

    points, _ = qdrant.scroll(
        collection_name=collection_name,
        scroll_filter=Filter(must=[FieldCondition(key="doc_id", match=MatchValue(value=int(doc_id)))]),
        with_payload=False,
        with_vectors=False,
        limit=10_000,
    )
    existing_ids = [point.id for point in points]
    if existing_ids:
        qdrant.delete(collection_name=collection_name, points_selector=existing_ids)
    return len(existing_ids)


def index_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"indexed": False, "reason": "no_rows"}
    if QdrantClient is None:
        return {"indexed": False, "reason": "qdrant_client_unavailable"}

    client = get_openrouter_client()
    if client is None:
        return {"indexed": False, "reason": "embedding_client_unavailable"}

    qdrant = QdrantClient(
        host=TX_MATCH_QDRANT_HOST,
        port=TX_MATCH_QDRANT_PORT,
        api_key=TX_MATCH_QDRANT_API_KEY or None,
    )
    requested_collection = rows[0].get("matching_collection") if rows else None
    collection_name = resolve_collection_name(requested_collection)
    _ensure_collection(qdrant, collection_name)

    doc_id = int(rows[0].get("doc_id") or 0)
    replaced_count = _delete_existing_rows(qdrant, doc_id=doc_id, collection_name=collection_name) if doc_id > 0 else 0

    points: list[Any] = []
    for row in rows:
        text = str(row.get("feature_text") or "").strip()
        if not text:
            continue

        emb = client.models.embed_content(
            model=TX_MATCH_EMBEDDING_MODEL,
            contents=text,
        )
        dense = list(emb.embeddings[0].values)

        vector: Any = {"dense": dense}
        if encode_sparse_vector is not None and SparseVector is not None:
            indices, values = encode_sparse_vector(text)
            if indices and values:
                vector["sparse"] = SparseVector(indices=indices, values=values)

        payload = dict(row)
        payload["purpose"] = str(payload.get("purpose") or "transaction_match").strip().lower() or "transaction_match"
        payload["matching_collection"] = collection_name
        payload["indexed_at"] = datetime.utcnow().isoformat() + "Z"

        point_id = hashlib.sha256(str(row.get("record_id") or text).encode("utf-8")).hexdigest()
        points.append(
            PointStruct(
                id=point_id,
                vector=vector,
                payload=payload,
            )
        )

    if not points:
        return {"indexed": False, "reason": "no_points"}

    qdrant.upsert(collection_name=collection_name, points=points)
    return {
        "indexed": True,
        "collection": collection_name,
        "row_count": len(points),
        "replaced_existing_rows": replaced_count,
        "doc_id": doc_id,
    }


def query_rows(
    *,
    query_text: str,
    purpose: str,
    workflow_scope: str,
    tenant_scope: str,
    top_k: int = 10,
    collection_name: str = "",
) -> dict[str, Any]:
    text = str(query_text or "").strip()
    normalized_purpose = str(purpose or "").strip().lower()
    normalized_workflow_scope = str(workflow_scope or "").strip()
    normalized_tenant_scope = str(tenant_scope or "").strip()
    limit = max(1, min(int(top_k or 10), 50))

    if not text:
        return {"ok": False, "reason": "empty_query_text"}
    if not normalized_purpose or not normalized_workflow_scope or not normalized_tenant_scope:
        return {"ok": False, "reason": "missing_required_filters"}
    if not is_allowed_purpose(normalized_purpose):
        return {"ok": False, "reason": "purpose_not_allowed", "purpose": normalized_purpose}
    if QdrantClient is None:
        return {"ok": False, "reason": "qdrant_client_unavailable"}

    client = get_openrouter_client()
    if client is None:
        return {"ok": False, "reason": "embedding_client_unavailable"}

    emb = client.models.embed_content(
        model=TX_MATCH_EMBEDDING_MODEL,
        contents=text,
    )
    dense = list(emb.embeddings[0].values)

    qdrant = QdrantClient(
        host=TX_MATCH_QDRANT_HOST,
        port=TX_MATCH_QDRANT_PORT,
        api_key=TX_MATCH_QDRANT_API_KEY or None,
    )
    resolved_collection = resolve_collection_name(collection_name)

    if Filter is None or FieldCondition is None or MatchValue is None:
        return {"ok": False, "reason": "qdrant_filter_models_unavailable"}

    query_filter = Filter(
        must=[
            FieldCondition(key="purpose", match=MatchValue(value=normalized_purpose)),
            FieldCondition(key="workflow_scope", match=MatchValue(value=normalized_workflow_scope)),
            FieldCondition(key="tenant_scope", match=MatchValue(value=normalized_tenant_scope)),
        ]
    )

    points: list[Any] = []
    if hasattr(qdrant, "query_points"):
        response = qdrant.query_points(
            collection_name=resolved_collection,
            query=dense,
            query_filter=query_filter,
            limit=limit,
            with_payload=True,
            with_vectors=False,
        )
        points = list(getattr(response, "points", []) or [])
    else:
        points = list(
            qdrant.search(
                collection_name=resolved_collection,
                query_vector=dense,
                query_filter=query_filter,
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            or []
        )

    items: list[dict[str, Any]] = []
    for point in points:
        payload = dict(getattr(point, "payload", {}) or {})
        items.append(
            {
                "id": str(getattr(point, "id", "") or ""),
                "score": float(getattr(point, "score", 0.0) or 0.0),
                "payload": payload,
            }
        )

    return {
        "ok": True,
        "collection": resolved_collection,
        "purpose": normalized_purpose,
        "workflow_scope": normalized_workflow_scope,
        "tenant_scope": normalized_tenant_scope,
        "query_text": text,
        "top_k": limit,
        "count": len(items),
        "items": items,
    }


def list_candidates(
    *,
    purpose: str,
    workflow_scope: str,
    tenant_scope: str,
    limit: int = 50,
    collection_name: str = "",
    doc_type: str = "",
    cursor: str = "",
) -> dict[str, Any]:
    normalized_purpose = str(purpose or "").strip().lower()
    normalized_workflow_scope = str(workflow_scope or "").strip()
    normalized_tenant_scope = str(tenant_scope or "").strip()
    normalized_doc_type = str(doc_type or "").strip().lower()
    normalized_cursor = str(cursor or "").strip()
    page_size = max(1, min(int(limit or 50), 200))

    if not normalized_purpose or not normalized_workflow_scope or not normalized_tenant_scope:
        return {"ok": False, "reason": "missing_required_filters"}
    if not is_allowed_purpose(normalized_purpose):
        return {"ok": False, "reason": "purpose_not_allowed", "purpose": normalized_purpose}
    if QdrantClient is None:
        return {"ok": False, "reason": "qdrant_client_unavailable"}
    if Filter is None or FieldCondition is None or MatchValue is None:
        return {"ok": False, "reason": "qdrant_filter_models_unavailable"}

    qdrant = QdrantClient(
        host=TX_MATCH_QDRANT_HOST,
        port=TX_MATCH_QDRANT_PORT,
        api_key=TX_MATCH_QDRANT_API_KEY or None,
    )
    resolved_collection = resolve_collection_name(collection_name)

    must_conditions = [
        FieldCondition(key="purpose", match=MatchValue(value=normalized_purpose)),
        FieldCondition(key="workflow_scope", match=MatchValue(value=normalized_workflow_scope)),
        FieldCondition(key="tenant_scope", match=MatchValue(value=normalized_tenant_scope)),
    ]
    if normalized_doc_type:
        must_conditions.append(FieldCondition(key="doc_type", match=MatchValue(value=normalized_doc_type)))

    scroll_kwargs: dict[str, Any] = {
        "collection_name": resolved_collection,
        "scroll_filter": Filter(must=must_conditions),
        "with_payload": True,
        "with_vectors": False,
        "limit": page_size,
    }
    if normalized_cursor:
        if normalized_cursor.isdigit():
            scroll_kwargs["offset"] = int(normalized_cursor)
        else:
            scroll_kwargs["offset"] = normalized_cursor

    points, next_page_offset = qdrant.scroll(**scroll_kwargs)

    items: list[dict[str, Any]] = []
    for point in points or []:
        payload = dict(getattr(point, "payload", {}) or {})
        items.append(
            {
                "id": str(getattr(point, "id", "") or ""),
                "payload": payload,
            }
        )

    return {
        "ok": True,
        "collection": resolved_collection,
        "purpose": normalized_purpose,
        "workflow_scope": normalized_workflow_scope,
        "tenant_scope": normalized_tenant_scope,
        "doc_type": normalized_doc_type,
        "limit": page_size,
        "cursor": normalized_cursor,
        "next_cursor": str(next_page_offset) if next_page_offset is not None else "",
        "count": len(items),
        "items": items,
    }
