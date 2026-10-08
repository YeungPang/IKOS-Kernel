import argparse
from decimal import Decimal, InvalidOperation
import hashlib
import mimetypes
import json
import logging
import os
import re
import time
import uuid
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable
from xml.etree import ElementTree as ET

from dotenv import load_dotenv

# Google/Discovery integration is disabled in IDMS-Demo.
ClientOptions = None
NotFound = Exception
storage = None
discoveryengine = None

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import (
        Distance,
        PointStruct,
        VectorParams,
        Filter,
        FieldCondition,
        MatchValue,
        SparseVector,
        SparseVectorParams,
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
    import domain_function
except Exception:  # pragma: no cover - optional dependency
    domain_function = None

try:
    import domain_db
except Exception:  # pragma: no cover - optional dependency
    domain_db = None

try:
    import spacy_preparser
except Exception:  # pragma: no cover - optional dependency
    spacy_preparser = None

try:
    import pypdf
except Exception:  # pragma: no cover - optional dependency
    pypdf = None

try:
    import md_gen as _md_gen
except Exception:  # pragma: no cover - optional dependency
    _md_gen = None

import object_db
from security_context import get_security_context, infer_document_access_level
import solf_function
import solf_parser
from solf_interpreter import SOLFInterpreter
import business_rules
import markdown_manager

try:
    from action_tool import ActionTool
except Exception:  # pragma: no cover - optional dependency
    ActionTool = None

try:
    from hybrid_search import encode_sparse_vector
except Exception:  # pragma: no cover - optional dependency
    encode_sparse_vector = None

try:
    from attribute_embedding_index import AttributeEmbeddingIndex
except Exception:  # pragma: no cover - optional dependency
    AttributeEmbeddingIndex = None

try:
    import tx_match_index
except Exception:  # pragma: no cover - optional dependency
    tx_match_index = None


from ikos_config import (
    PROJECT_ID,
    LOCATION,
    INGEST_BUCKET,
    ROUTER_MODEL,
    EXTRACT_MODEL,
    ENABLE_DISCOVERY_INDEX,
    DISCOVERY_DATA_STORE_ID,
    DISCOVERY_LOCATION,
    DISCOVERY_BRANCH,
    ENABLE_QDRANT_INDEX,
    QDRANT_HOST,
    QDRANT_PORT,
    QDRANT_API_KEY,
    QDRANT_COLLECTION,
    QDRANT_EMBEDDING_MODEL,
    QDRANT_CHUNK_SIZE,
    QDRANT_CHUNK_OVERLAP,
    QDRANT_VECTOR_SIZE,
    MARKDOWN_CACHE_DIR,
    ENABLE_ATTRIBUTE_EMBEDDING_UPSERT,
    ENABLE_ATTRIBUTE_EMBEDDING_UPSERT_FOR_NOTES,
    ENABLE_INGEST_LLM_TERM_ALIAS_EXPANSION,
    INGEST_LLM_TERM_ALIAS_MAX_CANONICAL,
    INGEST_LLM_TERM_ALIAS_MAX_ALIASES_PER_TERM,
    INGEST_LLM_TERM_ALIAS_LANGUAGES,
    ENABLE_INGEST_TERM_IMPORTANCE_LLM_RERANK,
    INGEST_TERM_IMPORTANCE_BORDERLINE_MIN,
    INGEST_TERM_IMPORTANCE_BORDERLINE_MAX,
    INGEST_TERM_IMPORTANCE_PRIMARY_THRESHOLD,
    ENABLE_TX_MATCH_INDEX,
)
from runtime_logging import configure_logging
from llm_fallback import (
    generate_content_with_openrouter_fallback,
    get_openrouter_client,
    is_quota_exhausted_error,
)


LOGGER = logging.getLogger("idms.ingest")

# Timing instrumentation (dev/prod configurable)
ENABLE_TIMING = str(os.getenv("IKOS_ENABLE_TIMING", "false")).strip().lower() in {"1", "true", "yes", "on"}
ENABLE_RELATION_TABLE_ENRICHMENT = str(os.getenv("IKOS_ENABLE_RELATION_TABLE_ENRICHMENT", "false")).strip().lower() in {"1", "true", "yes", "on"}


def _ensure_google_credentials_env_path() -> str:
    raw = str(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "") or "").strip()
    if not raw:
        return ""

    candidate = Path(raw)
    if not candidate.is_absolute():
        # Resolve relative credential paths against the repository root,
        # so browser/server cwd differences cannot break ingestion.
        candidate = (Path(__file__).resolve().parent / candidate).resolve()

    resolved = str(candidate)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = resolved
    return resolved


def _looks_like_uri(path_text: str) -> bool:
    text = str(path_text or "").strip()
    if not text:
        return False
    return bool(re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", text))


def _resolve_original_local_path(*candidates: str) -> str:
    """Return best absolute local path from candidate source hints.

    Priority: first absolute local path wins; otherwise resolve an existing
    relative local path to absolute. URI-like values are ignored.
    """
    for raw in candidates:
        value = str(raw or "").strip()
        if not value or _looks_like_uri(value):
            continue

        path_obj = Path(value)
        if path_obj.is_absolute():
            return str(path_obj)

        try:
            if path_obj.exists():
                return str(path_obj.resolve())
        except Exception:
            continue

    return ""


def _build_attribute_embedding_index_if_available(skip_upsert: bool = False) -> dict[str, Any]:
    """Incrementally upsert schema keyword embeddings during ingestion (best-effort)."""
    if not ENABLE_ATTRIBUTE_EMBEDDING_UPSERT:
        return {
            "built": False,
            "skipped": True,
            "reason": "attribute_embedding_upsert_disabled",
        }
    if skip_upsert:
        return {
            "built": False,
            "skipped": True,
            "reason": "attribute_embedding_upsert_skipped_for_note_ingest",
        }
    if AttributeEmbeddingIndex is None:
        return {"built": False, "skipped": True, "reason": "attribute_embedding_index_unavailable"}
    try:
        relationship_names: list[str] = []
        dynamic_attribute_names: list[str] = []
        idx = AttributeEmbeddingIndex.get_shared_instance()

        try:
            connection = object_db.get_connection()
            try:
                dynamic_attribute_names = [
                    str(name or "").strip()
                    for name in (idx._fetch_distinct_attribute_types() or [])
                    if str(name or "").strip()
                ]
                relationship_rows = object_db.get_relationships(connection, limit=5000)
                relationship_names = sorted({str(row.get("relationship_name") or "").strip() for row in relationship_rows if str(row.get("relationship_name") or "").strip()})
            finally:
                connection.close()
        except Exception:
            dynamic_attribute_names = []
            relationship_names = []

        signature = idx.build_ingest_upsert_signature(
            dynamic_attribute_names=dynamic_attribute_names,
            relationship_names=relationship_names,
        )
        if idx.should_skip_ingest_upsert(signature):
            return {
                "built": False,
                "skipped": True,
                "reason": "attribute_embedding_upsert_unchanged",
                "mode": "incremental",
                "upserted": 0,
                "upserted_attributes": 0,
                "upserted_dynamic_attributes": 0,
                "upserted_relationships": 0,
            }

        upserted_attributes = int(idx.upsert_default_attributes(recreate_collection=False))
        upserted_dynamic_attributes = int(idx.upsert_dynamic_attribute_terms(dynamic_attribute_names))

        upserted_relationships = int(idx.upsert_relationship_terms(relationship_names)) if relationship_names else 0
        upserted = upserted_attributes + upserted_dynamic_attributes + upserted_relationships
        if upserted > 0 or idx.refresh_ready_state():
            idx.mark_ingest_upsert_signature(signature)
        return {
            "built": upserted > 0,
            "skipped": False,
            "upserted": upserted,
            "upserted_attributes": upserted_attributes,
            "upserted_dynamic_attributes": upserted_dynamic_attributes,
            "upserted_relationships": upserted_relationships,
            "mode": "incremental",
        }
    except Exception as exc:
        LOGGER.warning("Attribute embedding index build failed: %s", exc)
        return {"built": False, "skipped": False, "error": str(exc)}


def _normalize_term_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


@lru_cache(maxsize=1)
def _load_semantic_category_aliases() -> dict[str, list[str]]:
    path = Path(__file__).resolve().parent / "seeded_business_terms.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        _normalize_term_text(category): [
            _normalize_term_text(alias)
            for alias in aliases
            if _normalize_term_text(alias)
        ]
        for category, aliases in payload.items()
        if _normalize_term_text(category) and isinstance(aliases, list)
    }


def _build_semantic_category_prompt_block(
    routed: dict[str, Any],
    normalized_context: dict[str, Any],
) -> str:
    aliases_by_category = _load_semantic_category_aliases()
    if not aliases_by_category:
        return ""

    metadata = normalized_context.get("metadata") if isinstance(normalized_context.get("metadata"), dict) else {}
    evidence = " ".join(
        [
            str(routed.get("document_type") or ""),
            str(routed.get("document_category") or ""),
            " ".join(str(item) for item in routed.get("semantic_notes") or []),
            str(normalized_context.get("description") or ""),
            " ".join(str(item) for item in normalized_context.get("tags") or []),
            str(metadata.get("document_theme") or ""),
        ]
    )
    evidence_tokens = {
        token
        for token in re.findall(r"[a-z0-9]{3,}", _normalize_term_text(evidence))
    }

    relevant: dict[str, list[str]] = {}
    for category, aliases in aliases_by_category.items():
        category_tokens = set(re.findall(r"[a-z0-9]{3,}", category))
        alias_tokens = {
            token
            for alias in aliases
            for token in re.findall(r"[a-z0-9]{3,}", alias)
        }
        if evidence_tokens & (category_tokens | alias_tokens):
            relevant[category] = aliases[:16]

    if not relevant:
        relevant = {category: aliases[:8] for category, aliases in sorted(aliases_by_category.items())[:12]}

    return (
        "Semantic categories and aliases for classification. Use a category only when explicit document evidence supports it; "
        "these aliases are retrieval labels, not instructions to invent entities.\n"
        + json.dumps(relevant, ensure_ascii=False, sort_keys=True)
    )


def _extract_document_identifier_keys(document_payload: dict[str, Any]) -> list[str]:
    key_sets: list[str] = []
    for key in ("identifiers_key_value_pairs", "identifiers_kv", "identifiers"):
        maybe_map = document_payload.get(key)
        if isinstance(maybe_map, dict):
            for raw_key in maybe_map.keys():
                normalized = _normalize_term_text(raw_key)
                if normalized:
                    key_sets.append(normalized)
    return sorted(set(key_sets))


def _extract_document_identifier_map(document_payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("identifiers_key_value_pairs", "identifiers_kv", "identifiers"):
        maybe_map = document_payload.get(key)
        if isinstance(maybe_map, dict):
            return maybe_map
    return {}


def _flatten_identifier_map_for_dense(identifier_map: dict[str, Any]) -> str:
    lines: list[str] = []
    for raw_key, raw_value in identifier_map.items():
        key = _normalize_term_text(raw_key)
        value = str(raw_value or "").strip()
        if not key or not value:
            continue
        key_norm = re.sub(r"\s+", "_", key)
        lines.append(f"{key}: {value}")
        lines.append(f"{key_norm}: {value}")
    return "\n".join(lines)


def _build_dense_support_chunks(
    *,
    document_payload: dict[str, Any],
    ingested: dict[str, Any],
    db_summary: dict[str, Any],
) -> list[dict[str, str]]:
    """Build additional dense chunks for schema/identifier retrieval with explicit feature types."""
    chunks: list[dict[str, str]] = []

    keywords = [
        _normalize_term_text(item)
        for item in (document_payload.get("keywords") or [])
        if _normalize_term_text(item)
    ]
    if keywords:
        chunks.append(
            {
                "feature_type": "keyword_terms",
                "text": "keyword terms\n" + "\n".join(sorted(set(keywords))[:160]),
            }
        )

    identifier_map = _extract_document_identifier_map(document_payload)
    flattened_identifiers = _flatten_identifier_map_for_dense(identifier_map)
    if flattened_identifiers:
        chunks.append(
            {
                "feature_type": "identifier_terms",
                "text": "identifier terms\n" + flattened_identifiers[:8000],
            }
        )

    canonical_attributes: set[str] = set()
    for entity in (ingested.get("solf_entities") or []):
        if not isinstance(entity, dict):
            continue
        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        for attr_name in attrs.keys():
            attr = _normalize_term_text(attr_name)
            if attr:
                canonical_attributes.add(attr)

    if canonical_attributes:
        chunks.append(
            {
                "feature_type": "attribute_schema_terms",
                "text": "attribute schema terms\n" + "\n".join(sorted(canonical_attributes)[:220]),
            }
        )

    canonical_relationships = {
        _normalize_term_text(rel.get("relationship_type"))
        for rel in (ingested.get("solf_relationships") or [])
        if isinstance(rel, dict) and _normalize_term_text(rel.get("relationship_type"))
    }
    if canonical_relationships:
        chunks.append(
            {
                "feature_type": "relationship_schema_terms",
                "text": "relationship schema terms\n" + "\n".join(sorted(canonical_relationships)[:220]),
            }
        )

    alias_rows = db_summary.get("semantic_alias_terms") if isinstance(db_summary.get("semantic_alias_terms"), list) else []
    if alias_rows:
        attribute_alias_lines: list[str] = []
        relationship_alias_lines: list[str] = []
        for row in alias_rows[:600]:
            if not isinstance(row, dict):
                continue
            kind = _normalize_term_text(row.get("kind"))
            canonical_name = _normalize_term_text(row.get("canonical_name"))
            term_text = _normalize_term_text(row.get("term_text"))
            if not canonical_name or not term_text:
                continue
            line = f"{canonical_name} | {term_text}"
            if kind == "attribute":
                attribute_alias_lines.append(line)
            elif kind == "relationship":
                relationship_alias_lines.append(line)

        if attribute_alias_lines:
            chunks.append(
                {
                    "feature_type": "attribute_alias_terms",
                    "text": "attribute alias terms\n" + "\n".join(sorted(set(attribute_alias_lines))[:260]),
                }
            )
        if relationship_alias_lines:
            chunks.append(
                {
                    "feature_type": "relationship_alias_terms",
                    "text": "relationship alias terms\n" + "\n".join(sorted(set(relationship_alias_lines))[:260]),
                }
            )

    return [item for item in chunks if str(item.get("text") or "").strip()]


def _expand_semantic_terms_with_llm_aliases(
    ingested: dict[str, Any],
    semantic_terms: list[dict[str, Any]],
    doc_id: int,
) -> list[dict[str, Any]]:
    if not ENABLE_INGEST_LLM_TERM_ALIAS_EXPANSION:
        return []

    canonical_by_kind: dict[str, set[str]] = {"attribute": set(), "relationship": set(), "category": set()}
    for term in semantic_terms:
        kind = _normalize_term_text(term.get("kind"))
        canonical_name = _normalize_term_text(term.get("canonical_name"))
        if kind in canonical_by_kind and canonical_name:
            canonical_by_kind[kind].add(canonical_name)

    canonical_attributes = sorted(canonical_by_kind["attribute"])[: max(1, int(INGEST_LLM_TERM_ALIAS_MAX_CANONICAL))]
    canonical_relationships = sorted(canonical_by_kind["relationship"])[: max(1, int(INGEST_LLM_TERM_ALIAS_MAX_CANONICAL))]
    canonical_categories = sorted(canonical_by_kind["category"])[: max(1, int(INGEST_LLM_TERM_ALIAS_MAX_CANONICAL))]
    if not canonical_attributes and not canonical_relationships and not canonical_categories:
        return []

    document_payload = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
    keywords = [
        _normalize_term_text(item)
        for item in (document_payload.get("keywords") or [])
        if _normalize_term_text(item)
    ]
    keywords = sorted(set(keywords))[:80]
    identifier_keys = _extract_document_identifier_keys(document_payload)[:80]
    allowed_languages = {
        lang for lang in (str(item or "").strip().lower() for item in INGEST_LLM_TERM_ALIAS_LANGUAGES)
        if lang in {"en", "de"}
    }
    if not allowed_languages:
        allowed_languages = {"en", "de"}
    looks_german = bool(
        re.search(r"[äöüß]", " ".join(keywords + identifier_keys), flags=re.IGNORECASE)
        or any(
            token in " ".join(keywords + identifier_keys)
            for token in ("nummer", "anschrift", "zoll", "ansprechpartner", "unternehmen")
        )
    )

    prompt_payload = {
        "task": "Map document terms to canonical schema terms and propose concise aliases.",
        "rules": [
            "Return JSON only.",
            "Do not invent canonical names not provided in candidates.",
            "Only include aliases that are plausible synonyms or naming variants.",
            "Keep aliases concise and lowercase.",
            "Language must be one of: en, de.",
            "If evidence appears German, include German aliases where possible.",
        ],
        "canonical_candidates": {
            "attribute": canonical_attributes,
            "relationship": canonical_relationships,
            "category": canonical_categories,
        },
        "document_evidence": {
            "keywords": keywords,
            "identifier_keys": identifier_keys,
        },
        "response_schema": {
            "aliases": [
                {
                    "kind": "attribute|relationship",
                    "canonical_name": "string",
                    "terms": ["string"],
                    "language": "en|de",
                    "confidence": "0..1",
                }
            ]
        },
        "preferred_languages": ["de", "en"] if looks_german else ["en", "de"],
        "allowed_languages": sorted(allowed_languages),
    }

    prompt = (
        "You normalize schema terms for retrieval.\n"
        "Return only valid JSON with key 'aliases'.\n"
        f"Input: {json.dumps(prompt_payload, ensure_ascii=False)}"
    )

    try:
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: None,
            model=EXTRACT_MODEL,
            contents=prompt,
            temperature=0.0,
            call_name="ingest_term_alias_expansion",
            complexity="simple",
        )
    except Exception as exc:
        LOGGER.debug("Ingest alias expansion skipped due to LLM error: %s", exc)
        return []

    text = str(getattr(response, "text", "") or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:]) if len(lines) > 1 else text
        if text.endswith("```"):
            text = text[:-3].strip()

    parsed: dict[str, Any]
    try:
        parsed = json.loads(text)
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
    aliases = parsed.get("aliases") if isinstance(parsed.get("aliases"), list) else []
    max_aliases = max(1, int(INGEST_LLM_TERM_ALIAS_MAX_ALIASES_PER_TERM))
    for item in aliases:
        if not isinstance(item, dict):
            continue
        kind = _normalize_term_text(item.get("kind"))
        canonical_name = _normalize_term_text(item.get("canonical_name"))
        if kind not in canonical_by_kind:
            continue
        if canonical_name not in canonical_by_kind[kind]:
            continue

        language = _normalize_term_text(item.get("language"))
        if language not in allowed_languages:
            language = "de" if (looks_german and "de" in allowed_languages) else "en"
        try:
            confidence = float(item.get("confidence") or 0.0)
        except Exception:
            confidence = 0.0

        seen_terms: set[str] = set()
        for raw_term in (item.get("terms") or [])[: max_aliases * 2]:
            term_text = _normalize_term_text(raw_term)
            if not term_text or term_text == canonical_name:
                continue
            if term_text in seen_terms:
                continue
            seen_terms.add(term_text)
            rows.append(
                {
                    "kind": kind,
                    "canonical_name": canonical_name,
                    "term_text": term_text,
                    "language": language,
                    "source_type": "ingest_llm_alias",
                    "metadata": {
                        "doc_id": int(doc_id),
                        "source": "ingest_llm_alias",
                        "confidence": confidence,
                    },
                }
            )
            if len(seen_terms) >= max_aliases:
                break

    return rows


def _term_matches_identifier_pattern(value: str) -> bool:
    text = _normalize_term_text(value)
    if not text:
        return False
    if re.search(r"\b(eori|vat|uid|iban|bic|swift|tax|invoice|receipt|po|order|registration|register)\b", text):
        return True
    if re.search(r"\b(id|no|nr|number|code|ref|reference|uuid|key)\b", text):
        return True
    if re.search(r"[a-z]{2,6}[\-_]?\d{2,}", text):
        return True
    if re.search(r"\b\d{6,}\b", text):
        return True
    return False


def _build_term_scoring_context(ingested: dict[str, Any]) -> dict[str, set[str]]:
    document_payload = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
    keywords = {
        _normalize_term_text(item)
        for item in (document_payload.get("keywords") or [])
        if _normalize_term_text(item)
    }
    identifier_keys = set(_extract_document_identifier_keys(document_payload))

    entity_attributes: set[str] = set()
    for entity in ingested.get("solf_entities") or []:
        if not isinstance(entity, dict):
            continue
        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        for attr_name in attrs.keys():
            normalized = _normalize_term_text(attr_name)
            if normalized:
                entity_attributes.add(normalized)

    relationship_names = {
        _normalize_term_text(rel.get("relationship_type"))
        for rel in (ingested.get("solf_relationships") or [])
        if isinstance(rel, dict) and _normalize_term_text(rel.get("relationship_type"))
    }

    return {
        "keywords": keywords,
        "identifier_keys": identifier_keys,
        "entity_attributes": entity_attributes,
        "relationship_names": relationship_names,
    }


def _default_term_role(kind: str, score: float, is_identifier: bool) -> str:
    threshold = float(INGEST_TERM_IMPORTANCE_PRIMARY_THRESHOLD)
    if score >= threshold:
        if is_identifier:
            return "primary_identifier"
        if kind == "relationship":
            return "relationship_anchor"
        return "primary_attribute"
    if score >= 0.5:
        return "supporting_context"
    return "noise"


def _deterministic_term_importance(
    *,
    kind: str,
    canonical_name: str,
    term_text: str,
    context: dict[str, set[str]],
) -> tuple[float, str, list[str]]:
    score = 0.0
    reasons: list[str] = []

    is_identifier = _term_matches_identifier_pattern(term_text) or _term_matches_identifier_pattern(canonical_name)
    if is_identifier:
        score += 0.30
        reasons.append("identifier_pattern")

    if term_text in context["identifier_keys"] or canonical_name in context["identifier_keys"]:
        score += 0.25
        reasons.append("identifier_key_match")

    if term_text in context["keywords"] or canonical_name in context["keywords"]:
        score += 0.15
        reasons.append("document_keyword_match")

    if kind == "attribute" and canonical_name in context["entity_attributes"]:
        score += 0.20
        reasons.append("entity_attribute_match")

    if kind == "relationship" and canonical_name in context["relationship_names"]:
        score += 0.25
        reasons.append("relationship_match")

    if re.search(r"\b(date|email|phone|address|country|currency|amount|total|vat|tax)\b", canonical_name):
        score += 0.10
        reasons.append("business_field_hint")

    if len(term_text) <= 2:
        score -= 0.10
        reasons.append("very_short_term_penalty")

    score = max(0.0, min(1.0, score))
    role = _default_term_role(kind, score, is_identifier)
    return score, role, reasons


def _llm_rerank_term_importance_borderline(
    *,
    ingested: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    if not ENABLE_INGEST_TERM_IMPORTANCE_LLM_RERANK:
        return {}
    if not candidates:
        return {}

    document_payload = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
    evidence = {
        "doc_type": _normalize_term_text(document_payload.get("doc_type")),
        "keywords": [
            _normalize_term_text(item)
            for item in (document_payload.get("keywords") or [])
            if _normalize_term_text(item)
        ][:80],
        "identifier_keys": _extract_document_identifier_keys(document_payload)[:80],
    }

    prompt_payload = {
        "task": "Classify term importance for retrieval and identifier resolution.",
        "rules": [
            "Return JSON only.",
            "Use provided candidate terms only.",
            "importance_score must be between 0 and 1.",
            "term_role must be one of primary_identifier, primary_attribute, relationship_anchor, supporting_context, noise.",
        ],
        "evidence": evidence,
        "candidates": candidates,
        "response_schema": {
            "items": [
                {
                    "kind": "attribute|relationship",
                    "canonical_name": "string",
                    "term_text": "string",
                    "importance_score": "0..1",
                    "term_role": "primary_identifier|primary_attribute|relationship_anchor|supporting_context|noise",
                    "reasons": ["string"],
                }
            ]
        },
    }

    prompt = (
        "You rank semantic terms for query resolution.\n"
        "Return valid JSON only with key 'items'.\n"
        f"Input: {json.dumps(prompt_payload, ensure_ascii=False)}"
    )

    try:
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: None,
            model=EXTRACT_MODEL,
            contents=prompt,
            temperature=0.0,
            call_name="ingest_term_importance_rerank",
            complexity="simple",
        )
    except Exception:
        return {}

    text = str(getattr(response, "text", "") or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:]) if len(lines) > 1 else text
        if text.endswith("```"):
            text = text[:-3].strip()

    try:
        payload = json.loads(text)
    except Exception:
        return {}

    by_key: dict[str, dict[str, Any]] = {}
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    valid_roles = {
        "primary_identifier",
        "primary_attribute",
        "relationship_anchor",
        "supporting_context",
        "noise",
    }
    for item in items:
        if not isinstance(item, dict):
            continue
        kind = _normalize_term_text(item.get("kind"))
        canonical_name = _normalize_term_text(item.get("canonical_name"))
        term_text = _normalize_term_text(item.get("term_text"))
        if not kind or not canonical_name or not term_text:
            continue
        try:
            score = float(item.get("importance_score") or 0.0)
        except Exception:
            continue
        score = max(0.0, min(1.0, score))
        role = _normalize_term_text(item.get("term_role"))
        if role not in valid_roles:
            role = _default_term_role(kind, score, _term_matches_identifier_pattern(term_text) or _term_matches_identifier_pattern(canonical_name))
        reasons = [
            _normalize_term_text(reason)
            for reason in (item.get("reasons") or [])
            if _normalize_term_text(reason)
        ]
        key = f"{kind}|{canonical_name}|{term_text}"
        by_key[key] = {
            "importance_score": score,
            "term_role": role,
            "reasons": reasons,
        }
    return by_key


def _annotate_semantic_terms_with_importance(
    *,
    ingested: dict[str, Any],
    semantic_terms: list[dict[str, Any]],
    doc_id: int,
) -> list[dict[str, Any]]:
    if not semantic_terms:
        return []

    context = _build_term_scoring_context(ingested)
    annotated: list[dict[str, Any]] = []
    borderline: list[dict[str, Any]] = []
    low = min(float(INGEST_TERM_IMPORTANCE_BORDERLINE_MIN), float(INGEST_TERM_IMPORTANCE_BORDERLINE_MAX))
    high = max(float(INGEST_TERM_IMPORTANCE_BORDERLINE_MIN), float(INGEST_TERM_IMPORTANCE_BORDERLINE_MAX))

    for row in semantic_terms:
        kind = _normalize_term_text(row.get("kind"))
        canonical_name = _normalize_term_text(row.get("canonical_name"))
        term_text = _normalize_term_text(row.get("term_text"))
        if kind not in {"attribute", "relationship", "entity_type", "category"} or not canonical_name or not term_text:
            annotated.append(row)
            continue

        score, role, reasons = _deterministic_term_importance(
            kind=kind,
            canonical_name=canonical_name,
            term_text=term_text,
            context=context,
        )

        updated = dict(row)
        metadata = dict(updated.get("metadata") if isinstance(updated.get("metadata"), dict) else {})
        metadata.update(
            {
                "doc_id": int(doc_id),
                "importance_score": round(score, 4),
                "term_role": role,
                "importance_reasons": reasons,
                "importance_method": "deterministic",
            }
        )
        updated["metadata"] = metadata
        annotated.append(updated)

        if low <= score <= high:
            borderline.append(
                {
                    "kind": kind,
                    "canonical_name": canonical_name,
                    "term_text": term_text,
                    "importance_score": score,
                    "term_role": role,
                    "reasons": reasons,
                }
            )

    reranked = _llm_rerank_term_importance_borderline(ingested=ingested, candidates=borderline)
    if not reranked:
        return annotated

    final_rows: list[dict[str, Any]] = []
    for row in annotated:
        kind = _normalize_term_text(row.get("kind"))
        canonical_name = _normalize_term_text(row.get("canonical_name"))
        term_text = _normalize_term_text(row.get("term_text"))
        key = f"{kind}|{canonical_name}|{term_text}"
        rerank = reranked.get(key)
        if not rerank:
            final_rows.append(row)
            continue

        updated = dict(row)
        metadata = dict(updated.get("metadata") if isinstance(updated.get("metadata"), dict) else {})
        metadata.update(
            {
                "importance_score": round(float(rerank.get("importance_score") or 0.0), 4),
                "term_role": _normalize_term_text(rerank.get("term_role")) or metadata.get("term_role") or "supporting_context",
                "importance_reasons": [
                    _normalize_term_text(item)
                    for item in (rerank.get("reasons") or [])
                    if _normalize_term_text(item)
                ],
                "importance_method": "llm_rerank",
            }
        )
        updated["metadata"] = metadata
        final_rows.append(updated)

    return final_rows


class TimedOperation:
    """Context manager for timing operations during development."""
    
    def __init__(self, operation_name: str, run_id: str | None = None):
        self.operation_name = operation_name
        self.run_id = run_id
        self.start_ms = 0
        self.elapsed_ms = 0
    
    def __enter__(self):
        self.start_ms = time.time() * 1000
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed_ms = time.time() * 1000 - self.start_ms
        if ENABLE_TIMING:
            LOGGER.info("timing run_id=%s operation=%s elapsed_ms=%.2f", self.run_id or "n/a", self.operation_name, self.elapsed_ms)


@dataclass
class SolfClassDef:
    class_name: str
    parent_class_name: str | None
    allowed_attributes: set[str]
    callable_fields: set[str]


CRUD_FIELD_NAMES = {"ingest", "update", "delete"}
ENTITY_ACTIONS = {"ingest", "update", "delete"}

TEXT_EXTENSIONS = {".txt", ".md", ".csv", ".tsv", ".json", ".xml", ".html", ".htm"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tif", ".tiff"}
OFFICE_TEXT_EXTENSIONS = {".docx", ".xlsx", ".xlsm"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}
AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".wma"}
SUPPORTED_URI_EXTENSIONS = TEXT_EXTENSIONS | IMAGE_EXTENSIONS | OFFICE_TEXT_EXTENSIONS | {".pdf", ".xls"}
MAX_STRUCTURED_TABLE_ROWS = max(1, int(os.getenv("IKOS_STRUCTURED_TABLE_ROW_LIMIT", "100")))

MIME_BY_EXTENSION = {
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
    ".tif": "image/tiff",
    ".tiff": "image/tiff",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".csv": "text/csv",
    ".tsv": "text/tab-separated-values",
    ".json": "application/json",
    ".xml": "application/xml",
    ".html": "text/html",
    ".htm": "text/html",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xlsm": "application/vnd.ms-excel.sheet.macroEnabled.12",
    ".xls": "application/vnd.ms-excel",
}


def _normalize_discovery_document_id(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]+", "-", value).strip("-")
    if not safe:
        safe = "idms-doc"
    return safe[:128]


def _first_non_empty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _to_decimal_amount(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value

    text = str(value).strip()
    if not text:
        return Decimal("0")

    text = text.replace("'", "").replace(" ", "")
    text = re.sub(r"[^0-9,\.\-]", "", text)
    if text.count(",") > 0 and text.count(".") == 0:
        text = text.replace(",", ".")
    elif text.count(",") > 0 and text.count(".") > 0:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "")
            text = text.replace(",", ".")
        else:
            text = text.replace(",", "")

    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")


def _ticket_has_payable_fare(document_payload: dict[str, Any], solf_entities: list[dict[str, Any]] | None = None) -> bool:
    amount_keys = (
        "ticket_amount",
        "fare_amount",
        "gross_amount",
        "total_amount",
        "amount",
        "price",
        "fare",
    )

    for key in amount_keys:
        if _to_decimal_amount(document_payload.get(key)) > 0:
            return True

    for entity in (solf_entities or []):
        if not isinstance(entity, dict):
            continue
        entity_class = str(entity.get("class_name") or entity.get("entity_type") or "").strip().lower()
        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        entity_name = str(entity.get("entity_name") or "").strip().lower()
        ticket_like = entity_class == "ticket" or "ticket" in entity_name or any(
            key in attrs
            for key in (
                "ticket_id",
                "ticket_type",
                "fare_type",
                "class_of_service",
                "journey_type",
                "origin",
                "destination",
                "valid_from",
                "valid_until",
            )
        )
        if not ticket_like:
            continue
        for key in amount_keys:
            if _to_decimal_amount(attrs.get(key)) > 0:
                return True

    return False


def _is_booking_relevant_financial_entity(entity: dict[str, Any] | None) -> bool:
    if not isinstance(entity, dict):
        return False
    entity_class = str(entity.get("class_name") or entity.get("entity_type") or "").strip().lower()
    if entity_class in {"invoice", "bill", "receipt"}:
        return True
    attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
    entity_name = str(entity.get("entity_name") or "").strip().lower()
    ticket_like = entity_class == "ticket" or "ticket" in entity_name or any(
        key in attrs
        for key in (
            "ticket_id",
            "ticket_type",
            "fare_type",
            "class_of_service",
            "journey_type",
            "origin",
            "destination",
            "valid_from",
            "valid_until",
        )
    )
    if not ticket_like:
        return False
    for key in ("ticket_amount", "fare_amount", "gross_amount", "total_amount", "amount", "price", "fare"):
        if _to_decimal_amount(attrs.get(key)) > 0:
            return True
    return False


def _document_requires_default_booking_process(document_payload: dict[str, Any], solf_entities: list[dict[str, Any]] | None = None) -> bool:
    doc_type = str(document_payload.get("doc_type") or "").strip().lower()
    if doc_type in {"invoice", "bill", "receipt"}:
        return True
    if doc_type == "ticket" or _ticket_has_payable_fare(document_payload, solf_entities):
        return _ticket_has_payable_fare(document_payload, solf_entities)
    return any(_is_booking_relevant_financial_entity(entity) for entity in (solf_entities or []))


def _build_search_boost_text(metadata: dict[str, Any] | None) -> str:
    if not isinstance(metadata, dict):
        return ""

    description = str(metadata.get("user_description") or "").strip()
    tags = metadata.get("user_tags") if isinstance(metadata.get("user_tags"), list) else []
    tag_text = " ".join(str(tag).strip() for tag in tags if str(tag).strip())

    # Repeat high-value context terms once to bias lexical retrieval without over-inflating index text.
    pieces = [description, description, tag_text, tag_text]
    return " ".join(piece for piece in pieces if piece).strip()


def _build_document_semantic_profile(
    doc_cat: Any,
    doc_type: Any,
    doc_theme: Any,
) -> dict[str, Any]:
    """Build normalized document-semantic terms for retrieval and filtering."""
    cat_norm = _normalize_term_text(doc_cat)
    type_norm = _normalize_term_text(doc_type)
    theme_norm = _normalize_term_text(doc_theme)

    field_values = [cat_norm, type_norm, theme_norm]

    terms: set[str] = set()
    for value in field_values:
        if not value:
            continue
        terms.add(value)
        for fragment in re.split(r"[,;/|]+", value):
            frag = _normalize_term_text(fragment)
            if not frag:
                continue
            terms.add(frag)
            for token in frag.split():
                if len(token) >= 3:
                    terms.add(token)

    concept_rules: list[tuple[str, set[str], set[str]]] = [
        (
            "billing_document",
            {"invoice", "bill", "billing", "receipt", "statement", "rechnung", "beleg", "quittung"},
            {"invoice", "bill", "billing", "receipt", "payment", "charge", "rechnung", "beleg"},
        ),
        (
            "telecom_service",
            {"telecom", "telecommunication", "telephone", "telefon", "mobile", "mobil", "cell", "cellular"},
            {"telecom", "telephone", "phone", "mobile", "telefon", "mobil", "communications"},
        ),
        (
            "quotation_offer",
            {"quotation", "quote", "offer", "offerte", "angebot"},
            {"quotation", "quote", "offer", "offerte", "proposal"},
        ),
        (
            "contract_document",
            {"contract", "agreement", "policy", "vertrag", "vereinbarung"},
            {"contract", "agreement", "policy", "terms"},
        ),
        (
            "logistics_document",
            {"shipment", "delivery", "transport", "logistics", "cargo", "fracht", "lieferung"},
            {"shipment", "delivery", "transport", "logistics", "cargo"},
        ),
    ]

    concepts: list[str] = []
    haystack = " ".join(v for v in field_values if v)
    for concept, triggers, expansions in concept_rules:
        if any(trigger in haystack for trigger in triggers):
            concepts.append(concept)
            terms.update(expansions)

    ordered_terms = sorted(terms)
    ordered_concepts = sorted(set(concepts))
    return {
        "doc_cat": cat_norm,
        "doc_type": type_norm,
        "doc_theme": theme_norm,
        "terms": ordered_terms,
        "concepts": ordered_concepts,
    }


def _build_retrieval_hints(routed: dict[str, Any], ingested: dict[str, Any]) -> dict[str, Any]:
    document = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
    metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}
    doc_key = str(document.get("doc_key") or "").strip()
    doc_type = str(document.get("doc_type") or routed.get("document_type") or "").strip()
    doc_cat = str(document.get("doc_cat") or "").strip()
    doc_theme = str(document.get("doc_theme") or "").strip()
    description = str(metadata.get("user_description") or "").strip()
    tags = [str(tag).strip() for tag in (metadata.get("user_tags") or []) if str(tag).strip()]
    semantic_profile = _build_document_semantic_profile(doc_cat, doc_type, doc_theme)
    semantic_terms = [str(item).strip() for item in (semantic_profile.get("terms") or []) if str(item).strip()]
    semantic_concepts = [str(item).strip() for item in (semantic_profile.get("concepts") or []) if str(item).strip()]

    entity_names = sorted({
        str(entity.get("name") or "").strip()
        for entity in (ingested.get("solf_entities") or [])
        if isinstance(entity, dict) and str(entity.get("name") or "").strip()
    })
    entity_classes = sorted({
        str(entity.get("class_name") or "").strip().lower()
        for entity in (ingested.get("solf_entities") or [])
        if isinstance(entity, dict) and str(entity.get("class_name") or "").strip()
    })
    relationship_types = sorted({
        str(rel.get("relationship_type") or "").strip().lower()
        for rel in (ingested.get("solf_relationships") or [])
        if isinstance(rel, dict) and str(rel.get("relationship_type") or "").strip()
    })

    entity_name_text = " ".join(entity_names)
    entity_class_text = " ".join(entity_classes)
    relationship_text = " ".join(relationship_types)

    template_tokens = [
        token
        for token in [
            doc_key,
            doc_cat,
            doc_type,
            doc_theme,
            description,
            " ".join(tags),
            " ".join(semantic_terms),
            entity_name_text,
            entity_class_text,
            relationship_text,
        ]
        if token
    ]
    boosted_template = " ".join(template_tokens)

    return {
        "boost_weights": {
            "doc_key": 2.2,
            "user_description": 2.0,
            "user_tags": 1.8,
            "entity_names": 2.0,
            "entity_classes": 1.6,
            "relationship_types": 1.6,
            "doc_semantic_terms": 1.7,
            "keyword_text": 1.2,
        },
        "boosted_query_templates": [
            "{question}",
            f"{{question}} {boosted_template}".strip(),
            f"{doc_key} {{question}}".strip() if doc_key else "{question}",
            f"{description} {{question}}".strip() if description else "{question}",
            f"{' '.join(tags)} {{question}}".strip() if tags else "{question}",
            f"{entity_name_text} {{question}}".strip() if entity_name_text else "{question}",
            f"{entity_class_text} {{question}}".strip() if entity_class_text else "{question}",
            f"{relationship_text} {{question}}".strip() if relationship_text else "{question}",
        ],
        "boosted_filter_terms": {
            "doc_key": doc_key,
            "doc_cat": doc_cat,
            "doc_type": doc_type,
            "doc_theme": doc_theme,
            "doc_semantic_terms": semantic_terms,
            "doc_semantic_concepts": semantic_concepts,
            "user_description": description,
            "user_tags": tags,
            "entity_names": entity_names,
            "entity_classes": entity_classes,
            "relationship_types": relationship_types,
        },
    }


def build_domain_extraction_guidance(class_defs: dict[str, SolfClassDef], routed: dict[str, Any]) -> str:
    if domain_function is None or not hasattr(domain_function, "build_extraction_guidance"):
        return ""
    try:
        return str(domain_function.build_extraction_guidance(class_defs, routed) or "")
    except Exception:
        LOGGER.exception("Domain extraction guidance hook failed")
        return ""


def _upsert_document_category_registry(
    connection: Any,
    doc_id: int,
    document: dict[str, Any],
    entities: list[dict[str, Any]],
) -> int:
    metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}
    profile = metadata.get("semantic_doc_profile") if isinstance(metadata.get("semantic_doc_profile"), dict) else {}
    categories = {
        str(document.get("doc_cat") or "").strip().lower(),
        str(document.get("doc_type") or "").strip().lower(),
        *[str(item).strip().lower() for item in profile.get("concepts") or [] if str(item).strip()],
        *[str(item).strip().lower() for item in metadata.get("query_categories") or [] if str(item).strip()],
    } - {""}
    entity_types = {
        str(entity.get("class_name") or entity.get("entity_type") or "").strip().lower()
        for entity in entities
        if isinstance(entity, dict) and str(entity.get("class_name") or entity.get("entity_type") or "").strip()
    }
    attributes = {
        str(attribute).strip().lower()
        for entity in entities
        if isinstance(entity, dict)
        for attribute in ((entity.get("attributes") or {}) if isinstance(entity.get("attributes"), dict) else {})
        if str(attribute).strip()
    }
    rows: list[dict[str, Any]] = []
    for category in sorted(categories):
        rows.append(
            {
                "category_name": category,
                "source_scope": "document_kb",
                "access_method": "rag",
                "document_types": [str(document.get("doc_type") or "").strip().lower()] if str(document.get("doc_type") or "").strip() else [],
                "entity_types": sorted(entity_types),
                "attributes": sorted(attributes),
                "metadata": {"source": "document_ingestion", "doc_id": int(doc_id)},
                "confidence": 0.8,
            }
        )
    if entity_types or attributes:
        rows.append(
            {
                "category_name": "internal_entity_registry",
                "source_scope": "internal",
                "access_method": "semantic_lookup",
                "entity_types": sorted(entity_types),
                "attributes": sorted(attributes),
                "document_types": [],
                "metadata": {"source": "document_ingestion", "doc_id": int(doc_id)},
                "confidence": 0.75,
            }
        )
    projection_count = object_db.upsert_category_registry_rows(connection, rows)
    for category in sorted(categories):
        object_db.upsert_category_definition(
            connection,
            name=category,
            source_type="documents",
            access_method="rag",
            entity_mappings=[
                {"entity_type": entity_type, "source_table": "document", "role_name": "entity_candidate", "confidence": 0.8}
                for entity_type in sorted(entity_types)
            ],
            attribute_mappings=[
                {"attribute_name": attribute, "canonical_name": attribute, "source_table": "attribute", "role_name": "attribute", "confidence": 0.8}
                for attribute in sorted(attributes)
            ],
            metadata={"source": "document_ingestion", "doc_id": int(doc_id)},
        )
    if entity_types or attributes:
        object_db.upsert_category_definition(
            connection,
            name="internal_entity_registry",
            source_type="internal_db",
            access_method="semantic_lookup",
            entity_mappings=[
                {"entity_type": entity_type, "source_table": "object_instance", "role_name": "entity_candidate", "confidence": 0.75}
                for entity_type in sorted(entity_types)
            ],
            attribute_mappings=[
                {"attribute_name": attribute, "canonical_name": attribute, "source_table": "attribute", "role_name": "attribute", "confidence": 0.75}
                for attribute in sorted(attributes)
            ],
            metadata={"source": "document_ingestion", "doc_id": int(doc_id)},
        )
    return projection_count


def build_discovery_struct_data(
    routed: dict[str, Any],
    ingested: dict[str, Any],
    db_summary: dict[str, Any],
) -> dict[str, Any]:
    document = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
    metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}
    semantic_profile = _build_document_semantic_profile(
        document.get("doc_cat"),
        document.get("doc_type") or routed.get("document_type"),
        document.get("doc_theme"),
    )
    semantic_terms = [str(item).strip() for item in (semantic_profile.get("terms") or []) if str(item).strip()]
    semantic_concepts = [str(item).strip() for item in (semantic_profile.get("concepts") or []) if str(item).strip()]
    entity_types = sorted({
        str(entity.get("class_name") or "").strip().lower()
        for entity in (ingested.get("solf_entities") or [])
        if isinstance(entity, dict)
    })

    return {
        "doc_key": _first_non_empty(document.get("doc_key"), Path(str(document.get("doc_path") or "")).name),
        "doc_cat": _first_non_empty(document.get("doc_cat"), routed.get("document_type"), "general_information"),
        "doc_type": _first_non_empty(document.get("doc_type"), routed.get("document_type"), "general_information"),
        "doc_date": _first_non_empty(document.get("doc_date")),
        "doc_theme": _first_non_empty(document.get("doc_theme")),
        "language": _first_non_empty(routed.get("language")),
        "entity_type_hints": routed.get("entity_type_hints") if isinstance(routed.get("entity_type_hints"), list) else [],
        "semantic_notes": routed.get("semantic_notes") if isinstance(routed.get("semantic_notes"), list) else [],
        "solf_entity_count": len(ingested.get("solf_entities") or []),
        "solf_relationship_count": len(ingested.get("solf_relationships") or []),
        "solf_entity_types": entity_types,
        "objects_upserted": int(db_summary.get("objects_upserted") or 0),
        "relationships_upserted": int(db_summary.get("relationships_upserted") or 0),
        "ambiguities_queued": int(db_summary.get("ambiguities_queued") or 0),
        "user_description": _first_non_empty(metadata.get("user_description")),
        "user_tags": metadata.get("user_tags") if isinstance(metadata.get("user_tags"), list) else [],
        "user_metadata": metadata.get("user_metadata") if isinstance(metadata.get("user_metadata"), dict) else {},
        "search_boost_text": _build_search_boost_text(metadata),
        "doc_semantic_terms": semantic_terms,
        "doc_semantic_concepts": semantic_concepts,
    }


def index_document_in_discovery_engine(
    gcs_uri: str,
    mime_type: str,
    struct_data: dict[str, Any],
    document_id: str,
) -> dict[str, Any]:
    if discoveryengine is None:
        raise RuntimeError("google-cloud-discoveryengine is not installed")
    if not PROJECT_ID:
        raise ValueError("PROJECT_ID is required for Discovery Engine indexing")
    if not DISCOVERY_DATA_STORE_ID:
        raise ValueError("IKOS_DISCOVERY_DATA_STORE_ID is required when Discovery indexing is enabled")

    discovery_location = str(DISCOVERY_LOCATION or "").strip() or "global"
    api_endpoint = "discoveryengine.googleapis.com"
    if discovery_location != "global":
        api_endpoint = f"{discovery_location}-discoveryengine.googleapis.com"

    client_options = ClientOptions(api_endpoint=api_endpoint)
    document_client = discoveryengine.DocumentServiceClient(client_options=client_options)
    datastore_client = discoveryengine.DataStoreServiceClient(client_options=client_options)

    parent = document_client.branch_path(PROJECT_ID, discovery_location, DISCOVERY_DATA_STORE_ID, DISCOVERY_BRANCH)
    datastore_name = datastore_client.data_store_path(PROJECT_ID, discovery_location, DISCOVERY_DATA_STORE_ID)

    try:
        datastore_client.get_data_store(name=datastore_name)
    except NotFound as exc:
        raise RuntimeError(
            "Discovery datastore not found for backfill/indexing: "
            f"{datastore_name}. Check PROJECT_ID, IKOS_DISCOVERY_LOCATION, and IKOS_DISCOVERY_DATA_STORE_ID."
        ) from exc

    document = discoveryengine.Document(
        content=discoveryengine.Document.Content(uri=gcs_uri, mime_type=mime_type),
        struct_data=struct_data,
    )

    try:
        request = discoveryengine.CreateDocumentRequest(
            parent=parent,
            document=document,
            document_id=document_id,
        )
        response = document_client.create_document(request=request)
        return {
            "indexed": True,
            "operation": "create",
            "document_id": document_id,
            "branch": DISCOVERY_BRANCH,
            "data_store_id": DISCOVERY_DATA_STORE_ID,
            "resource_name": response.name,
        }
    except Exception as exc:
        error_text = str(exc).lower()
        if "already exists" not in error_text and "409" not in error_text:
            raise

        document.name = f"{parent}/documents/{document_id}"
        response = document_client.update_document(document=document)
        return {
            "indexed": True,
            "operation": "update",
            "document_id": document_id,
            "branch": DISCOVERY_BRANCH,
            "data_store_id": DISCOVERY_DATA_STORE_ID,
            "resource_name": response.name,
        }


def generate_markdown_from_document(
    client: Any,
    source_path_or_uri: str,
    gcs_uri: str,
    extracted: dict[str, Any] | None = None,
    parser_rule_override: str | None = None,
    markdown_run_guard: Callable[[str], None] | None = None,
) -> str:
    """Generate markdown from document.
    
    For product_reference documents, generates synthetic markdown from extracted fields.
    For other documents, uses Gemini to convert to markdown.
    """
    # Check if this is a product reference that should use synthetic markdown
    if extracted:
        doc_payload = extracted.get("document") if isinstance(extracted.get("document"), dict) else {}
        doc_type = str(doc_payload.get("doc_type") or doc_payload.get("document_type") or "").strip().lower()
        if doc_type == "product_reference":
            return domain_function.generate_product_reference_markdown(extracted)
    
    source_mime_type = infer_source_mime_type(source_path_or_uri)
    extension = _source_extension(source_path_or_uri)

    # Media normalization: produce text artifacts so media can also be indexed in Qdrant.
    if extension in IMAGE_EXTENSIONS or source_mime_type.startswith("image/"):
        prompt = (
            "Analyze this product/business image and output clean Markdown for retrieval indexing. "
            "Include visible text, inferred caption, object/product hints, labels, numbers, and any identifiers. "
            "If uncertain, mark as possible/unknown. Output only Markdown."
        )
    elif extension in AUDIO_EXTENSIONS or source_mime_type.startswith("audio/"):
        prompt = (
            "Create a retrieval-friendly Markdown transcript summary for this audio. "
            "Include detected language, key topics, named entities, numbers/identifiers, and important statements. "
            "Include timestamp-style sections when possible. Output only Markdown."
        )
    elif extension in VIDEO_EXTENSIONS or source_mime_type.startswith("video/"):
        prompt = (
            "Create a retrieval-friendly Markdown video summary. "
            "Include scene/topic timeline, key entities, spoken content summary, visible text, and important identifiers. "
            "Include timestamp-style sections when possible. Output only Markdown."
        )
    else:
        prompt = (
            "Convert this document to clean Markdown. "
            "Preserve all text content, headings, tables, and lists faithfully. "
            "Do not summarize or omit any content. "
            "Output only the Markdown text with no additional commentary."
        )

    # Delegate PDF and image markdown generation to md_gen which has proper
    # routing: scanned PDFs → vision model, text-layer PDFs → text model,
    # standalone images → vision model.
    ext = _source_extension(source_path_or_uri)
    if ext in (".pdf",) or ext in IMAGE_EXTENSIONS:
        source_local = Path(source_path_or_uri)
        if source_local.exists() and _md_gen is not None:
            if markdown_run_guard is not None:
                markdown_run_guard("generate_markdown_from_document")
            with TimedOperation("genai_call:markdown_md_gen", run_id=None) as timer:
                result = _md_gen.file_to_markdown_openrouter(
                    str(source_local),
                    parser_rule_override=parser_rule_override,
                )
            if ENABLE_TIMING and timer.elapsed_ms > 0:
                LOGGER.info("genai_call_timing call=markdown_md_gen elapsed_ms=%.2f", timer.elapsed_ms)
            return (result or "").strip()

    contents = build_model_contents(source_path_or_uri, gcs_uri, prompt)
    with TimedOperation("genai_call:markdown", run_id=None) as timer:
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: client.models.generate_content(
                model=EXTRACT_MODEL,
                contents=contents,
                temperature=0.0,
            ),
            model=EXTRACT_MODEL,
            contents=contents,
            temperature=0.0,
            call_name="markdown",
        )
    if ENABLE_TIMING and timer.elapsed_ms > 0:
        LOGGER.info("genai_call_timing call=markdown model=%s elapsed_ms=%.2f", EXTRACT_MODEL, timer.elapsed_ms)
    return (response.text or "").strip()


def _markdown_cache_key(source_path_or_uri: str, gcs_uri: str, extracted: dict[str, Any] | None = None) -> str:
    source = str(source_path_or_uri or "").strip()
    if source.startswith("gs://"):
        basis = source
    else:
        source_path = Path(source)
        if source_path.exists():
            stat = source_path.stat()
            basis = f"{source_path.resolve()}|{stat.st_mtime_ns}|{stat.st_size}"
        else:
            basis = source

    doc_payload = extracted.get("document") if isinstance(extracted, dict) and isinstance(extracted.get("document"), dict) else {}
    doc_type = str(doc_payload.get("doc_type") or doc_payload.get("document_type") or "").strip().lower()
    digest_source = f"{basis}|{gcs_uri}|{doc_type}"
    return hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:16]


def _markdown_cache_path(source_path_or_uri: str, gcs_uri: str, extracted: dict[str, Any] | None = None) -> Path:
    doc_payload = extracted.get("document") if isinstance(extracted, dict) and isinstance(extracted.get("document"), dict) else {}
    doc_type = str(doc_payload.get("doc_type") or doc_payload.get("document_type") or "document").strip().lower() or "document"
    safe_doc_type = re.sub(r"[^A-Za-z0-9_.-]+", "-", doc_type).strip("-") or "document"
    return MARKDOWN_CACHE_DIR / f"{safe_doc_type}-{_markdown_cache_key(source_path_or_uri, gcs_uri, extracted)}.md"


def _normalized_markdown_hash(markdown_text: str) -> str:
    normalized = str(markdown_text or "").replace("\r\n", "\n").strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def _markdown_cache_path_from_content(markdown_text: str, extracted: dict[str, Any] | None = None) -> Path:
    doc_payload = extracted.get("document") if isinstance(extracted, dict) and isinstance(extracted.get("document"), dict) else {}
    doc_type = str(doc_payload.get("doc_type") or doc_payload.get("document_type") or "document").strip().lower() or "document"
    safe_doc_type = re.sub(r"[^A-Za-z0-9_.-]+", "-", doc_type).strip("-") or "document"
    return MARKDOWN_CACHE_DIR / f"{safe_doc_type}-{_normalized_markdown_hash(markdown_text)}.md"


def load_or_generate_markdown(
    client: Any,
    source_path_or_uri: str,
    gcs_uri: str,
    extracted: dict[str, Any] | None = None,
    reuse_markdown: bool = True,
    persist_markdown: bool = True,
    parser_rule_override: str | None = None,
    markdown_run_guard: Callable[[str], None] | None = None,
) -> tuple[str, Path | None, bool]:
    cache_path = _markdown_cache_path(source_path_or_uri, gcs_uri, extracted)
    if reuse_markdown and cache_path.exists():
        LOGGER.info("Reusing cached markdown artifact: %s", cache_path)
        return cache_path.read_text(encoding="utf-8"), cache_path, True

    LOGGER.info("Generating markdown artifact for reuse: %s", source_path_or_uri)
    markdown_text = generate_markdown_from_document(
        client,
        source_path_or_uri,
        gcs_uri,
        extracted,
        parser_rule_override=parser_rule_override,
        markdown_run_guard=markdown_run_guard,
    )
    if not str(markdown_text or "").strip():
        LOGGER.warning("Generated markdown is empty for source=%s gcs_uri=%s; skipping canonical empty-markdown reuse", source_path_or_uri, gcs_uri)
        return "", None, False

    if persist_markdown:
        LOGGER.info("Markdown persistence is deferred until document_content is written for the document row")
    return markdown_text, None, False


def _find_existing_markdown_path_by_doc_path(doc_path: str) -> str | None:
    """Lookup previously persisted markdown path for an existing document path."""
    path_value = str(doc_path or "").strip()
    if not path_value:
        return None

    try:
        with object_db.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT doc_id, doc_key, COALESCE(markdown_path, metadata->>'markdown_path')
                    FROM document
                    WHERE doc_path = %s
                    ORDER BY doc_id
                    LIMIT 1
                    """,
                    (path_value,),
                )
                row = cursor.fetchone()
                if row:
                    doc_id = int(row[0]) if row[0] is not None else 0
                    doc_key = str(row[1] or "").strip() or None
                    stored_path = str(row[2] or "").strip()
                    if stored_path:
                        return stored_path

                    # Backward-compatible fallback when markdown_path was not yet persisted.
                    if doc_id > 0:
                        deterministic_path = markdown_manager.get_markdown_path(doc_id, doc_key)
                        if deterministic_path.exists():
                            return str(deterministic_path)
    except Exception as exc:
        LOGGER.debug("Existing markdown lookup failed for doc_path=%s: %s", path_value, exc)

    return None


def chunk_markdown(text: str, chunk_size: int = 800, overlap: int = 100) -> list[str]:
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]
    chunks: list[str] = []
    current_parts: list[str] = []
    current_len = 0

    for para in paragraphs:
        para_len = len(para)
        if current_len + para_len > chunk_size and current_parts:
            chunks.append("\n\n".join(current_parts))
            while current_parts and current_len > overlap:
                removed = current_parts.pop(0)
                current_len -= len(removed) + 2
            current_len = max(0, current_len)
        current_parts.append(para)
        current_len += para_len + 2

    if current_parts:
        chunks.append("\n\n".join(current_parts))

    return chunks or [text[:chunk_size]]


def embed_text_chunks(client: Any, chunks: list[str]) -> list[list[float]]:
    """Generate dense embeddings for text chunks using small embedding model."""
    if not chunks:
        return []

    batch_size = max(1, int(os.getenv("IKOS_INGEST_EMBEDDING_BATCH_SIZE", "32")))
    embeddings: list[list[float]] = []
    for start in range(0, len(chunks), batch_size):
        batch = [str(chunk or "") for chunk in chunks[start : start + batch_size] if str(chunk or "").strip()]
        if not batch:
            continue
        try:
            response = client.models.embed_content(
                model=QDRANT_EMBEDDING_MODEL,
                contents=batch,
            )
            batch_embeddings = list(getattr(response, "embeddings", []) or [])
            if len(batch_embeddings) != len(batch):
                raise RuntimeError(
                    f"Embedding batch size mismatch: expected {len(batch)}, got {len(batch_embeddings)}"
                )
            for embedding_obj in batch_embeddings:
                embeddings.append(list(getattr(embedding_obj, "values", []) or []))
        except Exception:
            # Fall back to single-text calls for this batch so one malformed response
            # does not lose the rest of the document's embeddings.
            for chunk in batch:
                response = client.models.embed_content(
                    model=QDRANT_EMBEDDING_MODEL,
                    contents=chunk,
                )
                embeddings.append(list(response.embeddings[0].values))
    return embeddings


def _extract_dense_vector_size(vectors_cfg: Any, preferred_name: str = "dense") -> int | None:
    """Best-effort extraction of dense vector size from Qdrant vectors config."""
    if vectors_cfg is None:
        return None

    direct_size = getattr(vectors_cfg, "size", None)
    if direct_size is not None:
        try:
            return int(direct_size)
        except Exception:
            return None

    cfg_dict: dict[str, Any] | None = None
    if isinstance(vectors_cfg, dict):
        cfg_dict = vectors_cfg
    else:
        model_dump = getattr(vectors_cfg, "model_dump", None)
        if callable(model_dump):
            try:
                dumped = model_dump()
                if isinstance(dumped, dict):
                    cfg_dict = dumped
            except Exception:
                cfg_dict = None

    if not cfg_dict:
        return None

    preferred = cfg_dict.get(preferred_name)
    if isinstance(preferred, dict) and preferred.get("size") is not None:
        try:
            return int(preferred.get("size"))
        except Exception:
            return None

    if hasattr(preferred, "size"):
        try:
            return int(getattr(preferred, "size"))
        except Exception:
            return None

    for value in cfg_dict.values():
        if isinstance(value, dict) and value.get("size") is not None:
            try:
                return int(value.get("size"))
            except Exception:
                continue
        if hasattr(value, "size"):
            try:
                return int(getattr(value, "size"))
            except Exception:
                continue
    return None


def encode_chunks_sparse(chunks: list[str]) -> list[tuple[list[int], list[float]]]:
    """Generate sparse vectors for text chunks using token frequency (BM25-style)."""
    if encode_sparse_vector is None:
        return [([], []) for _ in chunks]
    
    sparse_vectors = []
    for chunk in chunks:
        indices, values = encode_sparse_vector(chunk)
        sparse_vectors.append((indices, values))
    return sparse_vectors


def _ensure_qdrant_collection(
    qdrant: Any,
    collection_name: str,
    expected_vector_size: int | None = None,
) -> None:
    """Ensure Qdrant collection exists with both dense and sparse vector support."""
    recreate_on_mismatch = str(
        os.getenv("IKOS_QDRANT_AUTO_RECREATE_ON_DIM_MISMATCH", "false")
    ).strip().lower() in {"1", "true", "yes", "on"}

    expected_size = int(expected_vector_size or QDRANT_VECTOR_SIZE)

    existing = {c.name for c in qdrant.get_collections().collections}
    if collection_name in existing:
        info = qdrant.get_collection(collection_name)
        vectors_cfg = getattr(getattr(info, "config", None), "params", None)
        vectors_cfg = getattr(vectors_cfg, "vectors", None)
        current_size = _extract_dense_vector_size(vectors_cfg)

        if current_size is None:
            raise RuntimeError(
                f"Qdrant collection '{collection_name}' has no readable dense vector size; "
                "cannot validate ingestion embeddings."
            )

        if int(current_size) != int(expected_size):
            message = (
                f"Qdrant collection '{collection_name}' dense vector size mismatch: "
                f"collection={current_size}, expected={expected_size}. "
                "Set IKOS_QDRANT_EMBEDDING_DIMENSION/model consistently or rebuild the collection."
            )
            if not recreate_on_mismatch:
                raise RuntimeError(message)

            LOGGER.warning("%s Auto-recreating collection because IKOS_QDRANT_AUTO_RECREATE_ON_DIM_MISMATCH=true", message)
            qdrant.delete_collection(collection_name)
            existing.discard(collection_name)

    if collection_name not in existing:
        # Configure collection with named dense vectors and optional sparse config.
        vectors_config = {
            "dense": VectorParams(size=expected_size, distance=Distance.COSINE),
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


def index_chunks_in_qdrant(
    qdrant: Any,
    chunks: list[str],
    embeddings: list[list[float]],
    doc_id: int,
    doc_key: str,
    doc_type: str,
    gcs_uri: str,
    doc_cat: str | None = None,
    doc_theme: str | None = None,
    doc_keywords: list[str] | None = None,
    semantic_doc_terms: list[str] | None = None,
    entity_names: list[str] | None = None,
    chunk_feature_types: list[str] | None = None,
) -> dict[str, Any]:
    security_context = get_security_context(required=False)
    tenant_id = str(security_context.tenant_id) if security_context is not None else "00000000-0000-0000-0000-000000000001"
    expected_vector_size = len(embeddings[0]) if embeddings else QDRANT_VECTOR_SIZE
    _ensure_qdrant_collection(
        qdrant,
        QDRANT_COLLECTION,
        expected_vector_size=expected_vector_size,
    )

    replaced_existing_chunks = 0
    if Filter is not None and FieldCondition is not None and MatchValue is not None and doc_key:
        existing_points, _ = qdrant.scroll(
            collection_name=QDRANT_COLLECTION,
            scroll_filter=Filter(
                must=[
                    FieldCondition(
                        key="doc_key",
                        match=MatchValue(value=doc_key),
                    ),
                    FieldCondition(
                        key="tenant_id",
                        match=MatchValue(value=tenant_id),
                    ),
                ]
            ),
            with_payload=False,
            with_vectors=False,
            limit=10_000,
        )
        existing_ids = [point.id for point in existing_points]
        replaced_existing_chunks = len(existing_ids)
        if existing_ids:
            qdrant.delete(
                collection_name=QDRANT_COLLECTION,
                points_selector=existing_ids,
            )

    # Generate sparse vectors for hybrid search
    sparse_vectors = encode_chunks_sparse(chunks) if encode_sparse_vector else [([], []) for _ in chunks]
    
    points = []
    for idx, (chunk, embedding, (sparse_indices, sparse_values)) in enumerate(zip(chunks, embeddings, sparse_vectors)):
        feature_type = "document_chunk"
        if isinstance(chunk_feature_types, list) and idx < len(chunk_feature_types):
            candidate_feature = str(chunk_feature_types[idx] or "").strip()
            if candidate_feature:
                feature_type = candidate_feature
        # Build vector dict with both dense and sparse if available
        vector_dict = {"dense": embedding}
        
        if sparse_indices and sparse_values and SparseVector is not None:
            vector_dict["sparse"] = SparseVector(indices=sparse_indices, values=sparse_values)
        
        # Use named vectors if sparse available, otherwise just dense
        point_vector = vector_dict if len(vector_dict) > 1 else embedding
        
        points.append(
            PointStruct(
                id=str(uuid.uuid4()),
                vector=point_vector,
                payload={
                    "tenant_id": tenant_id,
                    "access_level": infer_document_access_level(doc_cat, doc_type),
                    "doc_id": doc_id,
                    "doc_key": doc_key,
                    "doc_cat": str(doc_cat or "").strip(),
                    "doc_type": doc_type,
                    "doc_theme": str(doc_theme or "").strip(),
                    "gcs_uri": gcs_uri,
                    "doc_keywords": [str(item).strip() for item in (doc_keywords or []) if str(item).strip()],
                    "doc_semantic_terms": [str(item).strip() for item in (semantic_doc_terms or []) if str(item).strip()],
                    "entity_names": [str(item).strip() for item in (entity_names or []) if str(item).strip()],
                    "feature_type": feature_type,
                    "chunk_index": idx,
                    "chunk_text": chunk,
                },
            )
        )

    qdrant.upsert(collection_name=QDRANT_COLLECTION, points=points)
    return {
        "indexed": True,
        "collection": QDRANT_COLLECTION,
        "chunk_count": len(points),
        "replaced_existing_chunks": replaced_existing_chunks,
        "doc_id": doc_id,
    }


def build_adk_context_payload(
    source_path_or_uri: str,
    gcs_uri: str,
    routed: dict[str, Any],
    ingested: dict[str, Any],
    db_summary: dict[str, Any],
    discovery_index: dict[str, Any] | None = None,
) -> dict[str, Any]:
    retrieval_hints = _build_retrieval_hints(routed, ingested)
    return {
        "ingestion": {
            "source": source_path_or_uri,
            "gcs_uri": gcs_uri,
            "doc_id": db_summary.get("doc_id"),
            "document_type": routed.get("document_type"),
            "language": routed.get("language"),
            "objects_upserted": db_summary.get("objects_upserted"),
            "relationships_upserted": db_summary.get("relationships_upserted"),
            "ambiguities_queued": db_summary.get("ambiguities_queued"),
        },
        "entity_ids": [
            entity.get("entity_id")
            for entity in (ingested.get("solf_entities") or [])
            if isinstance(entity, dict)
        ],
        "discovery": discovery_index or {"indexed": False},
        "tool_hints": {
            "preferred_lookup": "sql_first_then_rag",
            "allow_action_tools": True,
            "allow_report_tools": True,
            "retrieval": retrieval_hints,
        },
    }


def parse_solf_classes(solf_script: str) -> dict[str, SolfClassDef]:
    classes: dict[str, SolfClassDef] = {}
    lines = solf_script.splitlines()
    class_blocks: list[tuple[str, list[str]]] = []
    line_index = 0

    while line_index < len(lines):
        line = lines[line_index]
        match = re.match(r"^([a-z_][a-z0-9_]*)\s*≔\s*\{\s*$", line.strip())
        if not match:
            line_index += 1
            continue

        class_name = match.group(1)
        body_lines: list[str] = []
        line_index += 1
        while line_index < len(lines):
            body_line = lines[line_index]
            if body_line.strip() == "}":
                break
            body_lines.append(body_line)
            line_index += 1

        class_blocks.append((class_name, body_lines))
        line_index += 1

    for class_name, body_lines in class_blocks:
        body = "\n".join(body_lines)
        if "objectType: class" not in body:
            continue

        parent_match = re.search(r"extending:\s*([a-z_][a-z0-9_]*)", body)
        parent = parent_match.group(1) if parent_match else None

        attrs: set[str] = set()
        callables: set[str] = set()
        for line in body.splitlines():
            stripped = line.strip().rstrip(",")
            if not stripped or ":" not in stripped:
                continue
            key = stripped.split(":", 1)[0].strip()
            if key in {"objectType", "extending"}:
                continue
            if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
                if key in CRUD_FIELD_NAMES:
                    callables.add(key)
                else:
                    attrs.add(key)

        classes[class_name] = SolfClassDef(
            class_name=class_name,
            parent_class_name=parent,
            allowed_attributes=attrs,
            callable_fields=callables,
        )

    return classes


def _normalize_user_context(user_context: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(user_context, dict):
        return {"description": "", "tags": [], "metadata": {}}

    description = str(user_context.get("description") or "").strip()
    tags_raw = user_context.get("tags") if isinstance(user_context.get("tags"), list) else []
    tags = [str(tag).strip() for tag in tags_raw if str(tag).strip()]
    metadata = user_context.get("metadata") if isinstance(user_context.get("metadata"), dict) else {}
    return {
        "description": description,
        "tags": tags,
        "metadata": metadata,
    }


def _normalize_rule_name_token(value: Any) -> str:
    token = str(value or "").strip()
    token = re.sub(r"[\[\](){}'\"]", "", token)
    token = re.sub(r"\s+", " ", token).strip()
    return token


def _normalize_note_date(raw_value: Any) -> str | None:
    """Normalize note-provided dates into YYYY-MM-DD."""
    raw = str(raw_value or "").strip()
    if not raw:
        return None

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Support 2-digit year forms such as 01.09.06
    for fmt in ("%d.%m.%y", "%d/%m/%y", "%d-%m-%y"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def _coerce_note_directive_value(raw_value: str) -> Any:
    text = str(raw_value or "").strip()
    if not text:
        return ""

    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None

    if re.fullmatch(r"-?\d+", text):
        try:
            return int(text)
        except ValueError:
            pass
    if re.fullmatch(r"-?\d+\.\d+", text):
        try:
            return float(text)
        except ValueError:
            pass

    normalized_date = _normalize_note_date(text)
    if normalized_date:
        return normalized_date

    if text.startswith("{") or text.startswith("["):
        try:
            return json.loads(text)
        except Exception:
            return text
    return text


def _parse_note_injections(description: str) -> dict[str, Any]:
    """Parse generic metadata injections from note text.

    Supported forms:
    - inject: key=value, key2=value2
    - inject key=value; key2=value2
    """
    injections: dict[str, Any] = {}
    patterns = (
        r"\binject\s*:\s*([^\n\r]+)",
        r"\binject\s+([^\n\r]+)",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, description, flags=re.IGNORECASE):
            block = str(match.group(1) or "")
            for token in re.split(r"[,;|]", block):
                piece = str(token).strip()
                if not piece:
                    continue
                if "=" not in piece:
                    if ":" not in piece:
                        continue
                    key, raw_value = piece.split(":", 1)
                else:
                    key, raw_value = piece.split("=", 1)
                normalized_key = str(key or "").strip().lower()
                normalized_key = re.sub(r"[^a-z0-9_\-.]+", "_", normalized_key)
                normalized_key = re.sub(r"_+", "_", normalized_key).strip("_")
                if not normalized_key:
                    continue
                injections[normalized_key] = _coerce_note_directive_value(raw_value)
    return injections


_INGEST_DIRECTIVE_PATTERNS: tuple[str, ...] = (
    r"\brules?\s*:\s*[^\n\r]+",
    r"\brule_names?\s*:\s*[^\n\r]+",
    r"\binject\s*:\s*[^\n\r]+",
    r"\binject\s+[^\n\r]+",
    r"\baction\s*:\s*[^\n\r]+",
    r"\bworkflow\s+process(?:es)?\s*:\s*[^\n\r]+",
    r"\bprocess(?:es)?\s*:\s*[^\n\r]+",
)


def _strip_ingestion_directive_text(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""

    stripped = cleaned
    for pattern in _INGEST_DIRECTIVE_PATTERNS:
        stripped = re.sub(pattern, " ", stripped, flags=re.IGNORECASE)

    stripped = re.sub(r"\s+", " ", stripped).strip(" ;,.-")
    return stripped


def _is_directive_only_text(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    return not bool(_strip_ingestion_directive_text(raw))


def _load_semantic_markdown_excerpt(metadata: dict[str, Any], max_chars: int = 700) -> str:
    if not isinstance(metadata, dict):
        return ""

    user_metadata = metadata.get("user_metadata") if isinstance(metadata.get("user_metadata"), dict) else {}
    source_reference = user_metadata.get("source_reference") if isinstance(user_metadata.get("source_reference"), dict) else {}

    candidate_paths = [
        str(source_reference.get("markdown_cache_path") or "").strip(),
        str(user_metadata.get("markdown_path") or "").strip(),
        str(metadata.get("markdown_path") or "").strip(),
    ]

    seen_paths: set[str] = set()
    for raw_path in candidate_paths:
        if not raw_path:
            continue
        norm_path = os.path.normpath(raw_path)
        if norm_path in seen_paths:
            continue
        seen_paths.add(norm_path)
        if not os.path.exists(norm_path):
            continue

        try:
            text = Path(norm_path).read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        lines: list[str] = []
        for line in text.splitlines():
            piece = str(line or "").strip()
            if not piece:
                continue
            if piece.startswith("#"):
                continue
            if re.fullmatch(r"[|\-: ]+", piece):
                continue
            if any(re.search(pattern, piece, flags=re.IGNORECASE) for pattern in _INGEST_DIRECTIVE_PATTERNS):
                continue
            if len(piece) < 12:
                continue
            lines.append(piece)
            if sum(len(item) for item in lines) >= max_chars:
                break

        excerpt = " ".join(lines)
        excerpt = re.sub(r"\s+", " ", excerpt).strip()
        if excerpt:
            return excerpt[:max_chars]

    return ""


def _parse_note_action_requests(description: str) -> list[dict[str, Any]]:
    """Parse generic action declarations from note text.

    Supported forms:
    - action: <action_name> {"json":"payload"}
    - action: <action_name>
    """
    requests: list[dict[str, Any]] = []
    for match in re.finditer(r"\baction\s*:\s*([a-z_][a-z0-9_]*)\s*([^\n\r]*)", description, flags=re.IGNORECASE):
        action_name = str(match.group(1) or "").strip().lower()
        trailing = str(match.group(2) or "").strip()
        payload: dict[str, Any] = {}
        if trailing:
            json_block_match = re.search(r"(\{.*\})", trailing)
            if json_block_match:
                json_text = str(json_block_match.group(1) or "").strip()
                try:
                    parsed = json.loads(json_text)
                    if isinstance(parsed, dict):
                        payload = parsed
                except Exception:
                    payload = {}
        requests.append(
            {
                "action": action_name,
                "payload": payload,
                "source": "note:action",
            }
        )
    return requests


def _derive_note_directives(user_context: dict[str, Any]) -> dict[str, Any]:
    """Parse free-form note text into structured ingest directives."""
    description = str(user_context.get("description") or "").strip()
    metadata = user_context.get("metadata") if isinstance(user_context.get("metadata"), dict) else {}

    directives: dict[str, Any] = {
        "metadata_updates": {},
        "additional_rule_names": [],
        "workflow_processes": [],
        "action_requests": [],
        "execute_actions": False,
        "effective_from": None,
    }
    if not description:
        return directives

    lowered = description.lower()
    metadata_updates = directives["metadata_updates"]

    update_markers = [
        "update",
        "change",
        "changed",
        "replace",
        "amend",
        "correct",
        "set validity",
    ]
    if any(marker in lowered for marker in update_markers):
        metadata_updates["note_update_intent"] = True

    if re.search(r"\b(reference image|use .* as reference|referenzbild)\b", lowered):
        metadata_updates["note_reference_image"] = True

    # Generic metadata injection from note text (runtime-configurable behavior).
    text_injections = _parse_note_injections(description)
    if text_injections:
        metadata_updates.update(text_injections)

    # Parse ad-hoc workflow process declarations from notes.
    for pattern in (
        r"\bworkflow\s+process(?:es)?\s*:\s*([^\n\r]+)",
        r"\bprocess(?:es)?\s*:\s*([^\n\r]+)",
        r"\btrigger\s+workflow\s*:\s*([^\n\r]+)",
    ):
        for match in re.finditer(pattern, description, flags=re.IGNORECASE):
            for token in re.split(r"[,;|]", str(match.group(1) or "")):
                normalized_process = str(token).strip().lower()
                normalized_process = re.sub(r"[^a-z0-9_\-]+", "_", normalized_process)
                normalized_process = re.sub(r"_+", "_", normalized_process).strip("_")
                if not normalized_process:
                    continue
                if normalized_process not in {str(item).strip().lower() for item in directives.get("workflow_processes", [])}:
                    directives.setdefault("workflow_processes", []).append(normalized_process)

    focus_items: list[str] = []
    for pattern in (
        r"\bextract\s+(?:only\s+)?(?:the\s+)?(?:crucial|key|important)?\s*information\s*:\s*([^\n\r]+)",
        r"\bextract\s+critical\s+fields\s*:\s*([^\n\r]+)",
    ):
        for match in re.finditer(pattern, description, flags=re.IGNORECASE):
            for token in re.split(r"[,;|]", str(match.group(1) or "")):
                cleaned = str(token).strip()
                if cleaned and cleaned.lower() not in {item.lower() for item in focus_items}:
                    focus_items.append(cleaned)
    if focus_items:
        metadata_updates["note_extract_focus"] = focus_items

    # Parse validity clauses from natural language, e.g. "set validity at 01.09.2006".
    validity_patterns = (
        r"\bset\s+valid(?:ity|_from)?\s*(?:at|from|to)?\s*([0-9]{1,4}[./-][0-9]{1,2}[./-][0-9]{2,4})\b",
        r"\bvalid\s+from\s+([0-9]{1,4}[./-][0-9]{1,2}[./-][0-9]{2,4})\b",
        r"\beffective\s+from\s+([0-9]{1,4}[./-][0-9]{1,2}[./-][0-9]{2,4})\b",
    )
    note_valid_from = None
    for pattern in validity_patterns:
        match = re.search(pattern, description, flags=re.IGNORECASE)
        if match:
            note_valid_from = _normalize_note_date(match.group(1))
            if note_valid_from:
                break
    if note_valid_from:
        directives["effective_from"] = note_valid_from
        metadata_updates["note_valid_from"] = note_valid_from
        metadata_updates["note_update_intent"] = True
        clause_inputs = metadata_updates.get("solf_clause_inputs") if isinstance(metadata_updates.get("solf_clause_inputs"), dict) else {}
        clause_inputs = dict(clause_inputs)
        clause_inputs["set_validity"] = note_valid_from
        metadata_updates["solf_clause_inputs"] = clause_inputs
        if "set_validity" not in {name.lower() for name in directives["additional_rule_names"]}:
            directives["additional_rule_names"].append("set_validity")

    # Parse explicit SOLF rule/clause directives from the note.
    solf_patterns = (
        r"\b(?:run|execute|apply)\s+(?:the\s+)?(?:solf\s+)?rules?\s*:\s*([^\n\r]+)",
        r"\b(?:solf\s+rules?|rules?)\s*:\s*([^\n\r]+)",
        r"\bsolf\s+clause\s*:\s*([^\n\r]+)",
    )
    additional_rules = [str(name) for name in directives["additional_rule_names"]]
    for pattern in solf_patterns:
        for match in re.finditer(pattern, description, flags=re.IGNORECASE):
            for token in re.split(r"[,;|]", str(match.group(1) or "")):
                normalized = _normalize_rule_name_token(token)
                if normalized and normalized.lower() not in {name.lower() for name in additional_rules}:
                    additional_rules.append(normalized)
    directives["additional_rule_names"] = additional_rules

    # Parse generic action requests from note text.
    action_requests: list[dict[str, Any]] = _parse_note_action_requests(description)

    # Merge caller-provided action requests.
    if isinstance(metadata.get("action_requests"), list):
        for item in metadata.get("action_requests"):
            if not isinstance(item, dict):
                continue
            action_name = str(item.get("action") or "").strip().lower()
            action_payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
            if not action_name:
                continue
            action_requests.append(
                {
                    "action": action_name,
                    "payload": dict(action_payload),
                    "source": str(item.get("source") or "metadata:action_requests"),
                }
            )

    deduped_actions: list[dict[str, Any]] = []
    seen_action_keys: set[str] = set()
    for request in action_requests:
        action_name = str(request.get("action") or "").strip().lower()
        payload_obj = request.get("payload") if isinstance(request.get("payload"), dict) else {}
        dedupe_key = f"{action_name}:{json.dumps(payload_obj, sort_keys=True, ensure_ascii=False)}"
        if action_name and dedupe_key not in seen_action_keys:
            seen_action_keys.add(dedupe_key)
            deduped_actions.append({
                "action": action_name,
                "payload": payload_obj,
                "source": str(request.get("source") or "note"),
            })
    directives["action_requests"] = deduped_actions

    execute_actions_flag = bool(metadata.get("execute_action_requests", False))
    if re.search(r"\b(?:execute|trigger|activate|run)\s+actions?\b", lowered):
        execute_actions_flag = True
    directives["execute_actions"] = execute_actions_flag

    if directives.get("workflow_processes"):
        metadata_updates["workflow_processes"] = directives.get("workflow_processes")
    if deduped_actions:
        metadata_updates["action_requests"] = deduped_actions
        metadata_updates["execute_action_requests"] = execute_actions_flag

    # Preserve any caller-provided structured clause inputs.
    if isinstance(metadata.get("solf_clause_inputs"), dict):
        existing_clause_inputs = metadata_updates.get("solf_clause_inputs") if isinstance(metadata_updates.get("solf_clause_inputs"), dict) else {}
        merged_clause_inputs = dict(existing_clause_inputs)
        merged_clause_inputs.update(metadata.get("solf_clause_inputs"))
        metadata_updates["solf_clause_inputs"] = merged_clause_inputs

    return directives


def _normalize_clause_name_token(value: Any) -> str:
    token = str(value or "").strip().lower()
    token = re.sub(r"[^a-z0-9_]+", "_", token)
    token = re.sub(r"_+", "_", token).strip("_")
    if not token:
        return ""
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,127}", token):
        return ""
    return token


def _invoke_note_solf_clauses(
    interpreter: SOLFInterpreter,
    note_directives: dict[str, Any],
    routed: dict[str, Any],
    extracted: dict[str, Any],
    ingested: dict[str, Any],
    run_id: str,
) -> list[dict[str, Any]]:
    """Execute explicit SOLF clauses requested from note directives."""
    clause_names_raw = note_directives.get("additional_rule_names") if isinstance(note_directives.get("additional_rule_names"), list) else []
    clause_names: list[str] = []
    for name in clause_names_raw:
        normalized = _normalize_clause_name_token(name)
        if normalized and normalized not in clause_names:
            clause_names.append(normalized)

    if not clause_names:
        return []

    clause_inputs = note_directives.get("metadata_updates") if isinstance(note_directives.get("metadata_updates"), dict) else {}
    payload = {
        "process": "ingest",
        "run_id": run_id,
        "note_directives": note_directives,
        "clause_inputs": clause_inputs.get("solf_clause_inputs") if isinstance(clause_inputs.get("solf_clause_inputs"), dict) else {},
        "effective_from": note_directives.get("effective_from"),
        "routed": routed,
        "extracted": extracted,
        "solf_objects": ingested,
    }

    results: list[dict[str, Any]] = []
    for clause_name in clause_names:
        try:
            clause_result = interpreter._invoke_clause(clause_name, [payload])
            status = "ok" if clause_result is not None and clause_result is not False else "no_effect"
            results.append(
                {
                    "clause": clause_name,
                    "status": status,
                    "result": clause_result,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "clause": clause_name,
                    "status": "error",
                    "error": str(exc),
                }
            )

    return results


def _extract_processing_rule_names(user_context: dict[str, Any]) -> list[str]:
    description = str(user_context.get("description") or "")
    metadata = user_context.get("metadata") if isinstance(user_context.get("metadata"), dict) else {}

    names: list[str] = []

    from_metadata = metadata.get("processing_rule_names")
    if isinstance(from_metadata, list):
        for item in from_metadata:
            token = _normalize_rule_name_token(item)
            if token and token.lower() not in {n.lower() for n in names}:
                names.append(token)
    elif isinstance(from_metadata, str):
        for item in re.split(r"[,;|]", from_metadata):
            token = _normalize_rule_name_token(item)
            if token and token.lower() not in {n.lower() for n in names}:
                names.append(token)

    patterns = [
        r"\[\s*rules?\s*:\s*([^\]]+)\]",
        r"\brules?\s*:\s*([^\n\r]+)",
        r"\brule_names?\s*:\s*([^\n\r]+)",
        r"\b(?:run|execute|apply)\s+(?:the\s+)?(?:solf\s+)?rules?\s*:\s*([^\n\r]+)",
        r"\bsolf\s+clause\s*:\s*([^\n\r]+)",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, description, flags=re.IGNORECASE):
            candidate_block = str(match.group(1) or "")
            for item in re.split(r"[,;|]", candidate_block):
                token = _normalize_rule_name_token(item)
                if token and token.lower() not in {n.lower() for n in names}:
                    names.append(token)

    return names


SYSTEM_INGESTION_PROCESSING_RULES: dict[str, dict[str, Any]] = {
    "update_existing_document": {
        "rule_name": "update_existing_document",
        "rule_text": "When this source identity/path matches an existing document, update that existing document in place.",
        "structured_rule": {
            "document_lifecycle_policy": "update_existing",
        },
        "is_system_rule": True,
        "scope": "ingestion",
        "description": "Update existing document row for matched source identity/path.",
    },
    "replace_existing_document": {
        "rule_name": "replace_existing_document",
        "rule_text": "When this source identity/path matches an existing document, delete existing document and re-ingest as new.",
        "structured_rule": {
            "document_lifecycle_policy": "replace_existing",
        },
        "is_system_rule": True,
        "scope": "ingestion",
        "description": "Delete matched existing document (cascade) and ingest new row.",
    },
}


def list_system_ingestion_processing_rules() -> list[dict[str, Any]]:
    return [
        dict(rule)
        for rule in SYSTEM_INGESTION_PROCESSING_RULES.values()
    ]


def _resolve_active_processing_rules(rule_names: list[str]) -> dict[str, Any]:
    requested_names = [_normalize_rule_name_token(name) for name in (rule_names or []) if _normalize_rule_name_token(name)]
    if not requested_names:
        return {"requested_names": [], "matched_rules": [], "missing_names": []}

    system_rule_rows: dict[str, dict[str, Any]] = {
        key.lower(): dict(value)
        for key, value in SYSTEM_INGESTION_PROCESSING_RULES.items()
    }

    db_requested_names = [name for name in requested_names if name.lower() not in system_rule_rows]

    connection = object_db.get_connection()
    try:
        rules = object_db.get_active_business_rules_by_names(connection, db_requested_names, limit=max(20, len(db_requested_names) * 2)) if db_requested_names else []
    finally:
        connection.close()

    by_name = {
        str(item.get("rule_name") or "").strip().lower(): item
        for item in rules
        if str(item.get("rule_name") or "").strip()
    }

    matched_rules: list[dict[str, Any]] = []
    missing_names: list[str] = []
    seen_match_names: set[str] = set()
    for name in requested_names:
        key = name.lower()
        system_row = system_rule_rows.get(key)
        if system_row is not None:
            if key in seen_match_names:
                continue
            seen_match_names.add(key)
            matched_rules.append(
                {
                    "rule_id": 0,
                    "rule_name": str(system_row.get("rule_name") or ""),
                    "rule_text": str(system_row.get("rule_text") or ""),
                    "structured_rule": system_row.get("structured_rule") if isinstance(system_row.get("structured_rule"), dict) else {},
                    "is_system_rule": True,
                    "scope": str(system_row.get("scope") or "ingestion"),
                }
            )
            continue

        row = by_name.get(key)
        if row is None:
            missing_names.append(name)
            continue
        if key in seen_match_names:
            continue
        seen_match_names.add(key)
        matched_rules.append(
            {
                "rule_id": int(row.get("rule_id") or 0),
                "rule_name": str(row.get("rule_name") or ""),
                "rule_text": str(row.get("rule_text") or ""),
                "structured_rule": row.get("structured_rule") if isinstance(row.get("structured_rule"), dict) else {},
                "is_system_rule": False,
                "scope": "business_rule",
            }
        )

    return {
        "requested_names": requested_names,
        "matched_rules": matched_rules,
        "missing_names": missing_names,
    }


def _summarize_processing_rule_for_prompt(rule: dict[str, Any]) -> dict[str, Any]:
    structured = rule.get("structured_rule") if isinstance(rule.get("structured_rule"), dict) else {}
    mappings = structured.get("mappings") if isinstance(structured.get("mappings"), list) else []
    workflow_hints = structured.get("workflow_hints") if isinstance(structured.get("workflow_hints"), list) else []
    return {
        "rule_name": str(rule.get("rule_name") or ""),
        "rule_text": str(rule.get("rule_text") or ""),
        "mappings": mappings[:10],
        "workflow_hints": workflow_hints[:10],
    }


def _build_processing_rules_prompt_block(matched_rules: list[dict[str, Any]]) -> str:
    if not matched_rules:
        return ""

    serialized = [_summarize_processing_rule_for_prompt(rule) for rule in matched_rules]
    return (
        "\nExplicit processing rules were selected by name and must be applied as mandatory constraints.\n"
        "If a selected rule conflicts with default extraction behavior, selected rules take precedence.\n"
        f"Selected processing rules: {json.dumps(serialized, ensure_ascii=False)}\n"
    )


def _build_note_directives_prompt_block(metadata: dict[str, Any]) -> str:
    if not isinstance(metadata, dict):
        return ""

    directives: dict[str, Any] = {}
    if metadata.get("note_reference_image"):
        directives["reference_image"] = True
    if str(metadata.get("note_invoice_account") or "").strip():
        directives["invoice_account"] = str(metadata.get("note_invoice_account")).strip()
    focus = metadata.get("note_extract_focus") if isinstance(metadata.get("note_extract_focus"), list) else []
    if focus:
        directives["extract_focus"] = [str(item).strip() for item in focus if str(item).strip()]
    if str(metadata.get("note_valid_from") or "").strip():
        directives["valid_from"] = str(metadata.get("note_valid_from")).strip()
    clause_inputs = metadata.get("solf_clause_inputs") if isinstance(metadata.get("solf_clause_inputs"), dict) else {}
    if clause_inputs:
        directives["solf_clause_inputs"] = clause_inputs
    injected_fields: dict[str, Any] = {}
    for key, value in metadata.items():
        if not str(key).startswith("note_"):
            continue
        if key in {"note_reference_image", "note_extract_focus", "note_valid_from"}:
            continue
        if value in (None, "", [], {}):
            continue
        injected_fields[str(key)] = value
    if injected_fields:
        directives["injected_fields"] = injected_fields
    workflow_processes = metadata.get("workflow_processes") if isinstance(metadata.get("workflow_processes"), list) else []
    if workflow_processes:
        directives["workflow_processes"] = [str(item).strip() for item in workflow_processes if str(item).strip()]
    action_requests = metadata.get("action_requests") if isinstance(metadata.get("action_requests"), list) else []
    if action_requests:
        directives["action_requests"] = action_requests
        directives["execute_action_requests"] = bool(metadata.get("execute_action_requests", False))

    if not directives:
        return ""

    return (
        "\nExplicit note directives were provided by the user and must be respected when supported by document facts.\n"
        f"Note directives: {json.dumps(directives, ensure_ascii=False)}\n"
    )


def _apply_note_directives_to_extracted(extracted: dict[str, Any], note_directives: dict[str, Any]) -> dict[str, Any]:
    out = dict(extracted)
    document = out.get("document") if isinstance(out.get("document"), dict) else {}
    document = dict(document)

    metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}
    metadata = dict(metadata)
    metadata_updates = note_directives.get("metadata_updates") if isinstance(note_directives.get("metadata_updates"), dict) else {}
    for key, value in metadata_updates.items():
        if value in (None, "", [], {}):
            continue
        metadata[str(key)] = value

    valid_from = str(note_directives.get("effective_from") or "").strip()
    if valid_from and not str(document.get("document_effective_date") or "").strip():
        document["document_effective_date"] = valid_from
    if metadata:
        document["metadata"] = metadata
    out["document"] = document

    entities = out.get("entities") if isinstance(out.get("entities"), list) else []
    patched_entities: list[Any] = []
    for entity in entities:
        if not isinstance(entity, dict):
            patched_entities.append(entity)
            continue
        item = dict(entity)
        if valid_from and not str(item.get("effective_from") or "").strip():
            item["effective_from"] = valid_from

        patched_entities.append(item)
    out["entities"] = patched_entities

    relationships = out.get("relationships") if isinstance(out.get("relationships"), list) else []
    patched_relationships: list[Any] = []
    for rel in relationships:
        if not isinstance(rel, dict):
            patched_relationships.append(rel)
            continue
        item = dict(rel)
        if valid_from and not str(item.get("effective_from") or "").strip():
            item["effective_from"] = valid_from
        patched_relationships.append(item)
    out["relationships"] = patched_relationships

    return out


def _execute_note_action_requests(
    interpreter: SOLFInterpreter,
    note_directives: dict[str, Any],
    doc_id: int,
    run_id: str,
    source_path_or_uri: str,
) -> dict[str, Any]:
    action_requests = note_directives.get("action_requests") if isinstance(note_directives.get("action_requests"), list) else []
    execute_actions = bool(note_directives.get("execute_actions", False))
    if not action_requests:
        return {
            "requested": 0,
            "executed": 0,
            "skipped": 0,
            "execute_actions": execute_actions,
            "results": [],
        }

    if not execute_actions:
        return {
            "requested": len(action_requests),
            "executed": 0,
            "skipped": len(action_requests),
            "execute_actions": False,
            "results": [
                {
                    "action": str(item.get("action") or ""),
                    "status": "planned",
                    "reason": "execute_actions_disabled",
                    "payload": item.get("payload") if isinstance(item.get("payload"), dict) else {},
                }
                for item in action_requests
            ],
        }

    if ActionTool is None:
        return {
            "requested": len(action_requests),
            "executed": 0,
            "skipped": len(action_requests),
            "execute_actions": True,
            "results": [
                {
                    "action": str(item.get("action") or ""),
                    "status": "skipped",
                    "reason": "action_tool_unavailable",
                }
                for item in action_requests
            ],
        }

    tool = ActionTool(
        db_connection_fn=object_db.get_connection,
        solf_interpreter=interpreter,
        require_confirmation=False,
    )

    results: list[dict[str, Any]] = []
    executed_count = 0
    skipped_count = 0
    for item in action_requests:
        action = str(item.get("action") or "").strip().lower()
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        payload = dict(payload)
        payload.setdefault("doc_id", int(doc_id))
        payload.setdefault("run_id", run_id)
        payload.setdefault("source_path_or_uri", source_path_or_uri)

        if action == "create_task":
            if not str(payload.get("project_id") or "").isdigit():
                skipped_count += 1
                results.append({
                    "action": action,
                    "status": "skipped",
                    "reason": "project_id_required",
                    "payload": payload,
                })
                continue
            action_result = tool.create_task(
                project_id=int(payload.get("project_id")),
                title=str(payload.get("title") or "Generated note task").strip(),
                hours_estimate=float(payload.get("hours_estimate") or 1),
                assigned_to=(int(payload.get("assigned_to")) if str(payload.get("assigned_to") or "").isdigit() else None),
                description=str(payload.get("description") or "Generated from note directives."),
                priority=str(payload.get("priority") or "medium"),
                due_date=(str(payload.get("due_date")) if payload.get("due_date") else None),
            )
        elif action == "update_entity":
            if not str(payload.get("entity_id") or "").isdigit():
                skipped_count += 1
                results.append({
                    "action": action,
                    "status": "skipped",
                    "reason": "entity_id_required",
                    "payload": payload,
                })
                continue
            action_result = tool.update_entity(
                entity_type=str(payload.get("entity_type") or "project_tasks"),
                entity_id=int(payload.get("entity_id")),
                field=str(payload.get("field") or "due_date"),
                new_value=payload.get("new_value"),
            )
        elif action == "assign_task":
            if not str(payload.get("task_id") or "").isdigit() or not str(payload.get("assigned_to") or "").isdigit():
                skipped_count += 1
                results.append({
                    "action": action,
                    "status": "skipped",
                    "reason": "task_id_and_assigned_to_required",
                    "payload": payload,
                })
                continue
            action_result = tool.assign_task(task_id=int(payload.get("task_id")), assigned_to=int(payload.get("assigned_to")))
        elif action == "close_task":
            if not str(payload.get("task_id") or "").isdigit():
                skipped_count += 1
                results.append({
                    "action": action,
                    "status": "skipped",
                    "reason": "task_id_required",
                    "payload": payload,
                })
                continue
            action_result = tool.close_task(task_id=int(payload.get("task_id")))
        elif action == "validate_plan":
            if not str(payload.get("plan_id") or "").isdigit():
                skipped_count += 1
                results.append({
                    "action": action,
                    "status": "skipped",
                    "reason": "plan_id_required",
                    "payload": payload,
                })
                continue
            action_result = tool.validate_plan(plan_id=int(payload.get("plan_id")))
        else:
            skipped_count += 1
            results.append(
                {
                    "action": action,
                    "status": "skipped",
                    "reason": "unsupported_action",
                    "payload": payload,
                }
            )
            continue

        if action_result.success:
            executed_count += 1
            status = "ok"
        else:
            skipped_count += 1
            status = "error"
        results.append(
            {
                "action": action,
                "status": status,
                "message": action_result.message,
                "data": action_result.data,
                "warnings": action_result.warnings,
                "requires_confirmation": action_result.requires_confirmation,
                "confirmation_prompt": action_result.confirmation_prompt,
                "payload": payload,
            }
        )

    return {
        "requested": len(action_requests),
        "executed": executed_count,
        "skipped": skipped_count,
        "execute_actions": True,
        "results": results,
    }


def _normalize_runtime_token(value: Any) -> str:
    token = str(value or "").strip().lower()
    token = re.sub(r"[^a-z0-9_\-]+", "_", token)
    token = re.sub(r"_+", "_", token).strip("_")
    return token


def _scope_match_for_trigger(mapping_scope: dict[str, Any], context: dict[str, Any]) -> bool:
    country = _normalize_runtime_token(context.get("country"))
    document_type = _normalize_runtime_token(context.get("document_type"))
    entity_class = _normalize_runtime_token(context.get("entity_class"))
    operation = _normalize_runtime_token(context.get("operation"))

    countries = {_normalize_runtime_token(v) for v in (mapping_scope.get("countries") or []) if _normalize_runtime_token(v)}
    document_types = {_normalize_runtime_token(v) for v in (mapping_scope.get("document_types") or []) if _normalize_runtime_token(v)}
    entity_classes = {_normalize_runtime_token(v) for v in (mapping_scope.get("entity_classes") or []) if _normalize_runtime_token(v)}
    operations = {_normalize_runtime_token(v) for v in (mapping_scope.get("operations") or []) if _normalize_runtime_token(v)}

    if countries and country and country not in countries:
        return False
    if countries and not country:
        return False

    if document_types and document_type and document_type not in document_types:
        return False
    if document_types and not document_type:
        return False

    if entity_classes and entity_class and entity_class not in entity_classes:
        return False
    if entity_classes and not entity_class:
        return False

    if operations and operation and operation not in operations:
        return False
    if operations and not operation:
        return False

    return True


def _evaluate_fact_assertion_for_trigger(
    fact: dict[str, Any],
    *,
    document_type: str,
    entity_classes: set[str],
    attribute_keys: set[str],
    relationship_types: set[str],
    active_processes: set[str],
) -> bool:
    predicate = _normalize_runtime_token(fact.get("predicate"))
    value_token = _normalize_runtime_token(fact.get("value") or fact.get("object"))
    subject_token = _normalize_runtime_token(fact.get("subject"))
    fact_name = _normalize_runtime_token(fact.get("fact_name"))

    if not predicate and fact_name in {
        "has_entity_class",
        "has_attribute",
        "has_relationship",
        "document_type_is",
        "process_is",
    }:
        predicate = fact_name

    if not predicate:
        return False

    if predicate in {"has_entity_class", "entity_class_is", "entity_class"}:
        candidate = value_token or subject_token
        return bool(candidate and candidate in entity_classes)

    if predicate in {"has_attribute", "attribute_exists", "attribute_is"}:
        candidate = value_token or subject_token
        return bool(candidate and candidate in attribute_keys)

    if predicate in {"has_relationship", "relationship_type", "relationship_is"}:
        candidate = value_token or subject_token
        return bool(candidate and candidate in relationship_types)

    if predicate in {"document_type_is", "doc_type", "is_document_type"}:
        candidate = value_token or subject_token
        return bool(candidate and candidate == document_type)

    if predicate in {"process_is", "workflow_process", "has_process"}:
        candidate = value_token or subject_token
        return bool(candidate and candidate in active_processes)

    # Generic containment fallback for custom predicates.
    candidate = value_token or subject_token
    all_tokens = set(entity_classes) | set(attribute_keys) | set(relationship_types) | set(active_processes) | {document_type}
    return bool(candidate and candidate in all_tokens)


def evaluate_runtime_rule_triggers(
    *,
    routed: dict[str, Any],
    extracted: dict[str, Any],
    metadata_context: dict[str, Any],
) -> dict[str, Any]:
    document_payload = extracted.get("document") if isinstance(extracted.get("document"), dict) else {}
    document_type = _normalize_runtime_token(
        document_payload.get("doc_type")
        or routed.get("document_type")
    )

    entities = extracted.get("entities") if isinstance(extracted.get("entities"), list) else []
    relationships = extracted.get("relationships") if isinstance(extracted.get("relationships"), list) else []

    entity_classes: set[str] = set()
    attribute_keys: set[str] = set()
    relationship_types: set[str] = set()
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        class_name = _normalize_runtime_token(entity.get("class_name") or entity.get("entity_type"))
        if class_name:
            entity_classes.add(class_name)
        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        for key in attrs.keys():
            normalized_attr = _normalize_runtime_token(key)
            if normalized_attr:
                attribute_keys.add(normalized_attr)

    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        rel_name = _normalize_runtime_token(rel.get("relationship_type"))
        if rel_name:
            relationship_types.add(rel_name)

    existing_processes = metadata_context.get("workflow_processes") if isinstance(metadata_context.get("workflow_processes"), list) else []
    active_processes = {_normalize_runtime_token(p) for p in existing_processes if _normalize_runtime_token(p)}

    base_context = {
        "country": str(document_payload.get("country") or metadata_context.get("country") or ""),
        "document_type": document_type,
        "operation": "ingest",
    }

    try:
        rules = business_rules.list_business_rules(is_active=True, limit=400)
    except Exception:
        return {
            "matched_rules": [],
            "triggered_processes": [],
            "document_type": document_type,
            "entity_classes": sorted(entity_classes),
            "attribute_keys": sorted(attribute_keys),
            "relationship_types": sorted(relationship_types),
            "rule_evaluation_error": True,
        }

    matched_rules: list[dict[str, Any]] = []
    triggered_processes: list[str] = []
    for row in rules:
        structured = row.get("structured_rule") if isinstance(row.get("structured_rule"), dict) else {}
        scope = row.get("scope") if isinstance(row.get("scope"), dict) else {}

        scope_matched = False
        if entity_classes:
            for entity_class in sorted(entity_classes):
                ctx = dict(base_context)
                ctx["entity_class"] = entity_class
                if _scope_match_for_trigger(scope, ctx):
                    scope_matched = True
                    break
        else:
            scope_matched = _scope_match_for_trigger(scope, base_context)

        if not scope_matched:
            continue

        mappings = structured.get("mappings") if isinstance(structured.get("mappings"), list) else []
        workflow_hints = structured.get("workflow_hints") if isinstance(structured.get("workflow_hints"), list) else []
        fact_assertions = structured.get("fact_assertions") if isinstance(structured.get("fact_assertions"), list) else []

        trigger_reasons: list[str] = []

        for mapping in mappings:
            if not isinstance(mapping, dict):
                continue
            source_terms = {
                _normalize_runtime_token(item)
                for item in (mapping.get("source_attribute_terms") or [])
                if _normalize_runtime_token(item)
            }
            if source_terms and source_terms.intersection(attribute_keys):
                trigger_reasons.append("mapping_source_attribute_match")
                break

        for fact in fact_assertions:
            if not isinstance(fact, dict):
                continue
            if _evaluate_fact_assertion_for_trigger(
                fact,
                document_type=document_type,
                entity_classes=entity_classes,
                attribute_keys=attribute_keys,
                relationship_types=relationship_types,
                active_processes=active_processes,
            ):
                trigger_reasons.append("fact_assertion_match")
                break

        if not trigger_reasons and workflow_hints:
            trigger_reasons.append("scope_workflow_hint_match")

        if not trigger_reasons and not mappings and not fact_assertions and not workflow_hints:
            trigger_reasons.append("scope_only_match")

        if not trigger_reasons:
            continue

        for hint in workflow_hints:
            if not isinstance(hint, dict):
                continue
            hinted_process = _normalize_runtime_token(hint.get("process"))
            if hinted_process and hinted_process not in active_processes and hinted_process not in triggered_processes:
                triggered_processes.append(hinted_process)

        matched_rules.append(
            {
                "rule_id": int(row.get("rule_id") or 0),
                "rule_name": str(row.get("rule_name") or ""),
                "trigger_reasons": trigger_reasons,
                "scope": scope,
                "workflow_hints": workflow_hints,
            }
        )

    return {
        "matched_rules": matched_rules,
        "triggered_processes": triggered_processes,
        "document_type": document_type,
        "entity_classes": sorted(entity_classes),
        "attribute_keys": sorted(attribute_keys),
        "relationship_types": sorted(relationship_types),
        "rule_evaluation_error": False,
    }


def _derive_processing_directives(matched_rules: list[dict[str, Any]]) -> dict[str, Any]:
    directives = {
        "summary_only": False,
        "keep_total_amount": False,
        "document_lifecycle_policy": None,
    }
    if not matched_rules:
        return directives

    for rule in matched_rules:
        rule_name = str(rule.get("rule_name") or "").strip().lower()
        structured_rule = rule.get("structured_rule") if isinstance(rule.get("structured_rule"), dict) else {}
        lifecycle_policy = str(structured_rule.get("document_lifecycle_policy") or "").strip().lower()

        if lifecycle_policy in {"update_existing", "replace_existing"}:
            directives["document_lifecycle_policy"] = lifecycle_policy

        if rule_name == "update_existing_document":
            directives["document_lifecycle_policy"] = "update_existing"
        elif rule_name == "replace_existing_document":
            directives["document_lifecycle_policy"] = "replace_existing"

        rule_text = str(rule.get("rule_text") or "").lower()
        if (
            ("ignore" in rule_text and ("detail item" in rule_text or "line item" in rule_text or "positions" in rule_text))
            or ("just the total" in rule_text)
            or ("total sum" in rule_text)
        ):
            directives["summary_only"] = True
            directives["keep_total_amount"] = True
    return directives


def _prune_detail_item_attributes(payload: Any, removed_counter: dict[str, int]) -> Any:
    if isinstance(payload, dict):
        blocked_tokens = {
            "line_items",
            "invoice_line_items",
            "detail_items",
            "detail_lines",
            "invoice_lines",
            "item_lines",
            "positions",
            "positionen",
        }
        out: dict[str, Any] = {}
        for key, value in payload.items():
            key_norm = str(key or "").strip().lower()
            if key_norm in blocked_tokens:
                removed_counter["count"] = int(removed_counter.get("count") or 0) + 1
                continue
            out[key] = _prune_detail_item_attributes(value, removed_counter)
        return out

    if isinstance(payload, list):
        return [_prune_detail_item_attributes(item, removed_counter) for item in payload]

    return payload


def _apply_processing_directives(extracted: dict[str, Any], directives: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if not directives.get("summary_only"):
        return extracted, {"removed_detail_item_fields": 0}

    removed_counter = {"count": 0}
    pruned = _prune_detail_item_attributes(extracted, removed_counter)
    return pruned, {"removed_detail_item_fields": int(removed_counter.get("count") or 0)}


def _trim_log_text(value: Any, limit: int = 300) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "...[truncated]"


def _build_user_input_trace_payload(
    source_path_or_uri: str,
    user_context: dict[str, Any],
) -> dict[str, Any]:
    metadata = user_context.get("metadata") if isinstance(user_context.get("metadata"), dict) else {}
    metadata_keys = sorted(str(key) for key in metadata.keys())
    tags = user_context.get("tags") if isinstance(user_context.get("tags"), list) else []

    return {
        "source": _trim_log_text(source_path_or_uri, limit=512),
        "description": _trim_log_text(user_context.get("description") or "", limit=500),
        "tags": [_trim_log_text(tag, limit=100) for tag in tags[:50]],
        "metadata_keys": metadata_keys[:100],
        "metadata_count": len(metadata_keys),
    }


def parse_note_command_from_chat(message: str) -> dict[str, Any] | None:
    """Parse a note command from a chat message.
    
    Supported formats:
    1. @note Title: content -- Use @note marker followed by title and content separated by colon
    2. /note --title <title> --content <content> [--tags <tag1,tag2>] -- Use /note command with flags
    
    Returns: dict with keys (title, content, tags) if note command found, None otherwise
    """
    message = str(message or "").strip()
    if not message:
        return None
    
    # Format 1: @note Title: content
    if message.startswith("@note "):
        rest = message[6:].strip()
        if ":" in rest:
            parts = rest.split(":", 1)
            title = parts[0].strip()
            content = parts[1].strip()
            if title and content:
                return {
                    "title": title,
                    "content": content,
                    "tags": [],
                }
        return None
    
    # Format 2: /note --title <title> --content <content> [--tags <tags>]
    if message.startswith("/note "):
        note_data: dict[str, Any] = {"title": "", "content": "", "tags": []}
        rest = message[6:].strip()
        
        # Parse --title flag
        title_match = re.search(r'--title\s+([^-]+?)(?=--|\s*$)', rest)
        if title_match:
            note_data["title"] = title_match.group(1).strip()
        
        # Parse --content flag
        content_match = re.search(r'--content\s+([^-]+?)(?=--|\s*$)', rest)
        if content_match:
            note_data["content"] = content_match.group(1).strip()
        
        # Parse --tags flag (optional)
        tags_match = re.search(r'--tags\s+([^-]+?)(?=--|\s*$)', rest)
        if tags_match:
            tags_str = tags_match.group(1).strip()
            note_data["tags"] = [tag.strip() for tag in tags_str.split(",") if tag.strip()]
        
        if note_data["title"] and note_data["content"]:
            return note_data
        return None
    
    return None


def _normalize_language_tag(value: Any) -> str:
    language = str(value or "").strip().lower()
    if not language:
        return ""

    aliases = {
        "de": "german",
        "de-de": "german",
        "ger": "german",
        "deutsch": "german",
        "en": "english",
        "en-us": "english",
        "en-gb": "english",
        "fr": "french",
        "it": "italian",
    }
    return aliases.get(language, language)


def localize_user_description(
    client: Any,
    description: str,
    target_language: str,
    run_id: str | None = None,
) -> str:
    source_text = str(description or "").strip()
    normalized_language = _normalize_language_tag(target_language)
    if not source_text or not normalized_language:
        return source_text

    prompt = (
        "Rewrite the input text in the target language while preserving accounting meaning. "
        "If already in the target language, return it unchanged. Return JSON only.\n"
        f"target_language: {normalized_language}\n"
        f"input_text: {source_text}\n\n"
        "Return exactly:\n"
        "{\n"
        "  \"localized_description\": \"...\"\n"
        "}\n"
    )

    try:
        response = generate_content_with_openrouter_fallback(
            primary_call=lambda: client.models.generate_content(
                model=ROUTER_MODEL,
                contents=[prompt],
                temperature=0.0,
            ),
            model=ROUTER_MODEL,
            contents=[prompt],
            temperature=0.0,
            call_name="localize_user_description",
            complexity="simple",
        )
        parsed = json.loads(response.text or "{}")
        LOGGER.info(
            "genai_json_response run_id=%s call=%s model=%s payload=%s",
            run_id or "n/a",
            "localize_user_description",
            ROUTER_MODEL,
            json.dumps(parsed, ensure_ascii=False),
        )
        localized = str(parsed.get("localized_description") or "").strip()
        return localized or source_text
    except Exception:
        LOGGER.exception("Failed to localize user description to %s", normalized_language)
        return source_text


def ensure_solf_classes_in_db(connection: Any, class_defs: dict[str, SolfClassDef]) -> None:
    sql = """
    INSERT INTO object_class (class_name, parent_class_id, metadata)
    VALUES (
        %s,
        (
            SELECT class_id
            FROM object_class
            WHERE class_name = %s
            LIMIT 1
        ),
        %s::jsonb
    )
    ON CONFLICT (class_name)
    DO UPDATE SET
        parent_class_id = EXCLUDED.parent_class_id,
        metadata = COALESCE(object_class.metadata, '{}'::jsonb) || EXCLUDED.metadata
    """

    sorted_defs = sorted(class_defs.values(), key=lambda c: (c.parent_class_name is not None, c.class_name))
    with connection.cursor() as cursor:
        for class_def in sorted_defs:
            metadata = {
                "allowed_attributes": sorted(class_def.allowed_attributes),
                "callable_fields": sorted(class_def.callable_fields),
                "source": "solf_script",
            }
            cursor.execute(sql, (class_def.class_name, class_def.parent_class_name, json.dumps(metadata)))
    connection.commit()


def build_document_type_values(class_defs: dict[str, SolfClassDef]) -> list[str]:
    document_like_values = {"general_information"}
    for class_name in class_defs:
        lowered = class_name.lower()
        if lowered == "document" or lowered.endswith("_document"):
            document_like_values.add(lowered)
        if lowered.endswith("_record") or lowered.endswith("_contract") or lowered.endswith("_agreement"):
            document_like_values.add(lowered)
        if lowered in {
            "invoice",
            "bill",
            "receipt",
            "quotation",
            "email",
            "policy",
            "tax_form",
            "tax_declaration",
            "vat_return",
            "social_contribution_report",
            "workflow_case",
            "product_reference",
        }:
            document_like_values.add(lowered)
    return sorted(document_like_values)


def build_router_schema(class_defs: dict[str, SolfClassDef]) -> dict[str, Any]:
    return {
        "document_type_values": build_document_type_values(class_defs),
        "entity_type_values": sorted(name.upper() for name in class_defs.keys()),
    }


def build_router_prompt(class_defs: dict[str, SolfClassDef], user_context: dict[str, Any] | None = None) -> str:
    router_schema = build_router_schema(class_defs)
    normalized_context = _normalize_user_context(user_context)
    context_block = (
        f"Uploader description: {normalized_context['description'] or 'N/A'}\n"
        f"Uploader tags: {json.dumps(normalized_context['tags'], ensure_ascii=False)}\n"
        f"Uploader metadata: {json.dumps(normalized_context['metadata'], ensure_ascii=False)}\n\n"
    )
    return (
        "You are a document router for IDMS. "
        "IDMS core is generic. SOLF defines the semantic classes. "
        "Classify the document and identify likely SOLF entity types. "
        "Return JSON only.\n\n"
        f"{context_block}"
        f"Allowed document_type values: {', '.join(router_schema['document_type_values'])}.\n"
        f"Allowed entity_type_hints values: {', '.join(router_schema['entity_type_values'])}.\n\n"
        "Return exactly this JSON object shape:\n"
        "{\n"
        "  \"document_type\": \"...\",\n"
        "  \"language\": \"...\",\n"
        "  \"entity_type_hints\": [\"...\"],\n"
        "  \"semantic_notes\": [\"...\"],\n"
        "  \"confidence\": 0.0\n"
        "}\n"
    )


def load_approved_solf_attribute_extensions() -> dict[str, list[str]]:
    if domain_db is None or not hasattr(domain_db, "get_approved_solf_attribute_extensions"):
        return {}

    connection = None
    try:
        connection = object_db.get_connection()
        return domain_db.get_approved_solf_attribute_extensions(connection)
    except Exception:
        LOGGER.exception("Failed to load approved SOLF attribute extensions")
        return {}
    finally:
        if connection is not None:
            connection.close()


def build_extraction_prompt(
    class_defs: dict[str, SolfClassDef],
    routed: dict[str, Any],
    user_context: dict[str, Any] | None = None,
    selected_processing_rules: list[dict[str, Any]] | None = None,
) -> str:
    approved_extensions = load_approved_solf_attribute_extensions()
    class_schema = {
        name: {
            "allowed_attributes": sorted(
                set(class_defs[name].allowed_attributes)
                | set(approved_extensions.get(str(name).strip().lower(), []))
            ),
            "callable_fields": sorted(class_defs[name].callable_fields),
        }
        for name in sorted(class_defs.keys())
    }

    normalized_context = _normalize_user_context(user_context)
    domain_guidance = build_domain_extraction_guidance(class_defs, routed)
    semantic_category_block = _build_semantic_category_prompt_block(routed, normalized_context)
    processing_rules_block = _build_processing_rules_prompt_block(selected_processing_rules or [])
    note_directives_block = _build_note_directives_prompt_block(normalized_context.get("metadata") if isinstance(normalized_context.get("metadata"), dict) else {})
    return (
        "You are an IDMS extraction engine. Return JSON only.\n"
        "IDMS core is generic. Do not inject business-domain assumptions beyond the SOLF classes provided.\n"
        "Use explicit facts from the document. Do not hallucinate.\n"
        "For entity attributes: use the canonical snake_case name from the allowed attributes list when the document value matches a known attribute.\n"
        "For document attributes NOT in the allowed list: include them if they are legally or factually significant "
        "(e.g. formal dates, statutory designations, official identifiers, policy notes, publication references). "
        "Use descriptive snake_case names derived from the document header or label.\n"
        "Skip attributes that are irrelevant or non-informational: row/entry markers (Ei, Lö, Ae, Ref numbers), "
        "blank cells, formatting artifacts, sequential counters, and purely structural table metadata.\n"
        "For tabular content, always map each row to the correct header columns.\n"
        "Do not detach cell values from their column names. If a table has multi-line headers, normalize them into stable snake_case attribute names.\n"
        "When a table row describes an entity, place row values in entity.attributes using the corresponding header semantics.\n"
        "When a table row links a person and a company (for example Gesellschafter + Stammanteile), represent the shareholding values as relationship attributes on the person->company relationship, not as standalone person attributes.\n"
        "For shareholding rows, include relationship attributes such as share_allocation_raw, share_count, share_nominal_value_chf, and share_total_nominal_value_chf whenever derivable.\n"
        "If a value is missing for a header in a row, omit that attribute instead of shifting values to the wrong column.\n"
        "Extract identity evidence needed for deduplication and updates: legal name, registration and tax IDs, email, phone, birth_date, birth_place, and address details when available.\n"
        "When names are similar but potentially different entities, keep identifiers explicit to support disambiguation.\n"
        "For multilingual documents, keep descriptive/narrative values in the document language.\n"
        "For document.geography, extract only locations explicitly stated or unambiguously identified in the document. "
        "Use role_name to distinguish event, sender, recipient, issuer, service, jurisdiction, and document-subject locations. "
        "Do not create geography entries merely from an entity's inferred country.\n"
        "For document.action_items, extract only explicit obligations, required responses, approvals, submissions, payments, or deadlines. "
        "Do not create an action item for background information or an inferred recommendation. Keep instruction concise, preserve due_date only when explicit, and default status to open.\n"
        f"Routed document_type: {routed.get('document_type', 'general_information')}\n"
        f"Semantic notes: {json.dumps(routed.get('semantic_notes') or [], ensure_ascii=False)}\n"
        f"Uploader description: {normalized_context['description'] or 'N/A'}\n"
        f"Uploader tags: {json.dumps(normalized_context['tags'], ensure_ascii=False)}\n"
        f"Uploader metadata: {json.dumps(normalized_context['metadata'], ensure_ascii=False)}\n"
        f"{semantic_category_block}\n"
        f"{processing_rules_block}"
        f"{note_directives_block}"
        f"{domain_guidance}\n"
        "\nSCHEMA — canonical attribute names (use these exact names when values match; extend with additional important attributes as needed):\n"
        f"{json.dumps(class_schema, ensure_ascii=False)}\n\n"
        "If a business-domain object is present, represent it as an entity only when its class exists in the allowed SOLF classes.\n"
        "Do not manufacture domain_payloads. Put all extracted objects into entities and relationships.\n"
        "Infer whether each entity item is a new ingest, an update to an existing object, or a delete instruction.\n"
        "Extract effective dates whenever the document states when a change becomes valid.\n"
        + (
            "IMPORTANT: The user explicitly indicates this note describes a CHANGE or UPDATE to an existing entity. "
            "For any entity that already exists (person, company, etc.), set operation='update' unless it is clearly a new object. "
            "When the note mentions attribute changes (e.g. 'new address', 'address change', 'moved to', 'now lives in', 'changed phone'), "
            "set operation='update' and include only the changed attributes on that entity. "
            "Do not create a new entity record for a name that is clearly referencing an existing known entity.\n"
            if (user_context or {}).get("metadata", {}).get("note_update_intent")
            else ""
        )
        +
        "Return exactly this JSON object shape:\n"
        "{\n"
        "  \"document\": {\n"
        "    \"doc_key\": \"...\",\n"
        "    \"doc_cat\": \"...\",\n"
        "    \"doc_type\": \"...\",\n"
        "    \"doc_date\": \"YYYY-MM-DD\",\n"
        "    \"doc_theme\": \"...\",\n"
        "    \"document_effective_date\": \"YYYY-MM-DD\",\n"
        "    \"document_recorded_date\": \"YYYY-MM-DD\",\n"
        "    \"keywords\": [\"...\"],\n"
        "    \"identifiers\": {},\n"
        "    \"metadata\": {\n"
        "      \"description\": \"Narrative description in the same language as the document\"\n"
        "    },\n"
        "    \"geography\": [\n"
        "      {\"raw_name\": \"...\", \"canonical_name\": \"...\", \"place_kind\": \"country|region|city|address|venue\", \"country_code\": \"ISO alpha-2 when explicit or certain\", \"role_name\": \"incident_location|issuer_location|sender_address|recipient_address|document_subject|service_location|jurisdiction\", \"confidence\": 0.0}\n"
        "    ],\n"
        "    \"action_items\": [\n"
        "      {\"action_type\": \"...\", \"instruction\": \"...\", \"subject_name\": \"...\", \"assignee_name\": \"...\", \"due_date\": \"YYYY-MM-DD|null\", \"priority\": \"low|medium|high|urgent\", \"status\": \"open\", \"confidence\": 0.0}\n"
        "    ]\n"
        "  },\n"
        "  \"entities\": [\n"
        "    {\n"
        "      \"entity_id\": \"e1\",\n"
        "      \"entity_name\": \"...\",\n"
        "      \"class_name\": \"...\",\n"
        "      \"operation\": \"ingest|update|delete\",\n"
        "      \"effective_from\": \"YYYY-MM-DD\",\n"
        "      \"effective_until\": \"YYYY-MM-DD|null\",\n"
        "      \"attributes\": {},\n"
        "      \"confidence\": 0.0\n"
        "    }\n"
        "  ]\n"
        ",\n"
        "  \"relationships\": [\n"
        "    {\n"
        "      \"source_entity_id\": \"e1\",\n"
        "      \"relationship_type\": \"...\",\n"
        "      \"target_entity_id\": \"e2\",\n"
        "      \"relationship_cat\": \"semantic\",\n"
        "      \"operation\": \"upsert|delete\",\n"
        "      \"exclusivity_scope\": \"none|by_target|by_source\",\n"
        "      \"effective_from\": \"YYYY-MM-DD\",\n"
        "      \"effective_until\": \"YYYY-MM-DD|null\",\n"
        "      \"attributes\": {},\n"
        "      \"confidence\": 0.0\n"
        "    }\n"
        "  ]\n"
        "}\n"
    )


def upload_if_local(storage_client: Any, source_path_or_uri: str, bucket_name: str) -> str:
    if source_path_or_uri.startswith("gs://"):
        return source_path_or_uri

    # In IDMS-Demo, local ingestion should work without requiring GCS.
    if storage_client is None:
        return source_path_or_uri

    if not bucket_name:
        return source_path_or_uri

    src_path = Path(source_path_or_uri)
    if not src_path.exists():
        raise FileNotFoundError(f"Input file not found: {source_path_or_uri}")

    object_name = f"ingest/{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{src_path.name}"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(src_path))
    return f"gs://{bucket_name}/{object_name}"


def _source_extension(source_path_or_uri: str) -> str:
    if source_path_or_uri.startswith("gs://"):
        return Path(source_path_or_uri.split("?", 1)[0]).suffix.lower()
    return Path(source_path_or_uri).suffix.lower()


def infer_source_mime_type(source_path_or_uri: str) -> str:
    extension = _source_extension(source_path_or_uri)
    if extension in MIME_BY_EXTENSION:
        return MIME_BY_EXTENSION[extension]

    guessed, _ = mimetypes.guess_type(source_path_or_uri)
    return guessed or "application/octet-stream"


def should_use_discovery_for_source(source_path_or_uri: str, source_mime_type: str) -> bool:
    extension = _source_extension(source_path_or_uri)
    if extension in VIDEO_EXTENSIONS or extension in AUDIO_EXTENSIONS:
        return True

    lowered_mime = str(source_mime_type or "").strip().lower()
    if lowered_mime.startswith("video/") or lowered_mime.startswith("audio/"):
        return True

    return False


def select_indexing_backend(prefer_discovery_for_media: bool) -> tuple[bool, bool, bool, str]:
    if not ENABLE_QDRANT_INDEX and not ENABLE_DISCOVERY_INDEX:
        raise ValueError(
            "Indexing is required but both IKOS_ENABLE_QDRANT_INDEX and "
            "IKOS_ENABLE_DISCOVERY_INDEX are disabled"
        )

    if prefer_discovery_for_media:
        # For media, prefer Discovery but also index in Qdrant when available
        # so retrieval can be unified across modalities.
        if ENABLE_DISCOVERY_INDEX and ENABLE_QDRANT_INDEX:
            return True, True, False, "discovery+qdrant"
        if ENABLE_DISCOVERY_INDEX:
            return False, True, False, "discovery"
        return True, False, True, "qdrant"

    if ENABLE_QDRANT_INDEX:
        return True, False, False, "qdrant"
    return False, True, True, "discovery"


def _truncate_text(text: str, limit: int = 120_000) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n[TRUNCATED]"


def _extract_docx_text(source_path: Path) -> str:
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraphs: list[str] = []

    with zipfile.ZipFile(source_path) as archive:
        try:
            document_xml = archive.read("word/document.xml")
        except KeyError:
            return ""

    root = ET.fromstring(document_xml)
    for paragraph in root.findall(".//w:p", namespace):
        parts = [node.text for node in paragraph.findall(".//w:t", namespace) if node.text]
        if parts:
            paragraphs.append("".join(parts))

    return "\n".join(paragraphs)


def _extract_xlsx_text(source_path: Path) -> str:
    namespace = {
        "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }

    def read_xml(archive: zipfile.ZipFile, path: str) -> ET.Element | None:
        try:
            return ET.fromstring(archive.read(path))
        except KeyError:
            return None

    with zipfile.ZipFile(source_path) as archive:
        shared_strings: list[str] = []
        shared_root = read_xml(archive, "xl/sharedStrings.xml")
        if shared_root is not None:
            for item in shared_root.findall(".//main:si", namespace):
                text_parts = [node.text for node in item.findall(".//main:t", namespace) if node.text]
                shared_strings.append("".join(text_parts))

        sheet_paths = sorted(
            name for name in archive.namelist()
            if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")
        )

        sheet_texts: list[str] = []
        for sheet_path in sheet_paths:
            sheet_root = read_xml(archive, sheet_path)
            if sheet_root is None:
                continue

            rows: list[str] = []
            for row in sheet_root.findall(".//main:row", namespace):
                cells: list[str] = []
                for cell in row.findall("main:c", namespace):
                    cell_type = cell.attrib.get("t")
                    value_node = cell.find("main:v", namespace)
                    inline_node = cell.find("main:is/main:t", namespace)
                    value_text = ""
                    if inline_node is not None and inline_node.text:
                        value_text = inline_node.text
                    elif value_node is not None and value_node.text:
                        if cell_type == "s":
                            try:
                                value_text = shared_strings[int(value_node.text)]
                            except (ValueError, IndexError):
                                value_text = value_node.text
                        else:
                            value_text = value_node.text
                    cells.append(value_text)
                if cells:
                    rows.append("\t".join(cells))

            if rows:
                sheet_texts.append(f"[{sheet_path}]\n" + "\n".join(rows))

    return "\n\n".join(sheet_texts)


def _extract_pdf_text(source_path: Path) -> str | None:
    """Extract plain text from a PDF using pypdf (pure-Python, no external deps)."""
    if pypdf is None:
        return None
    try:
        reader = pypdf.PdfReader(str(source_path))
        pages: list[str] = []
        for page in reader.pages:
            text = page.extract_text() or ""
            if text.strip():
                pages.append(text)
        return "\n\n".join(pages) if pages else None
    except Exception:
        LOGGER.exception("Failed to extract PDF text from %s", source_path)
        return None


def _extract_pdf_page_images_base64(source_path: Path) -> list[str]:
    """Extract page images from a scanned PDF as data URIs (base64-encoded).

    For image-scanned PDFs the entire page is stored as an embedded image object.
    Returns a list of data: URIs suitable for the OpenRouter vision API.
    """
    if pypdf is None:
        return []
    import base64
    data_uris: list[str] = []
    try:
        reader = pypdf.PdfReader(str(source_path))
        for page in reader.pages:
            try:
                for img in page.images:
                    raw = bytes(img.data) if img.data else b""
                    if not raw:
                        continue
                    # Detect JPEG vs PNG by magic bytes
                    if raw[:2] == b"\xff\xd8":
                        mime = "image/jpeg"
                    elif raw[:8] == b"\x89PNG\r\n\x1a\n":
                        mime = "image/png"
                    else:
                        mime = "image/jpeg"
                    b64 = base64.b64encode(raw).decode("ascii")
                    data_uris.append(f"data:{mime};base64,{b64}")
            except Exception:
                continue
    except Exception:
        LOGGER.exception("Failed to extract page images from PDF %s", source_path)
    return data_uris


def extract_local_text(source_path: Path) -> str | None:
    extension = source_path.suffix.lower()

    try:
        if extension in {".txt", ".md", ".csv", ".tsv", ".json", ".xml", ".html", ".htm"}:
            return source_path.read_text(encoding="utf-8", errors="replace")

        if extension == ".pdf":
            return _extract_pdf_text(source_path)

        if extension == ".docx":
            return _extract_docx_text(source_path)

        if extension in {".xlsx", ".xlsm"}:
            return _extract_xlsx_text(source_path)
    except Exception:
        LOGGER.exception("Failed to extract local text from %s", source_path)
        return None

    return None


def get_direct_markdown_text(source_path_or_uri: str) -> str | None:
    """Return local text directly for sources that do not need markdown generation."""
    extension = _source_extension(source_path_or_uri)
    if extension not in TEXT_EXTENSIONS and extension not in OFFICE_TEXT_EXTENSIONS:
        return None

    source_path = Path(source_path_or_uri)
    if not source_path.exists():
        return None

    extracted_text = extract_local_text(source_path)
    if not extracted_text or not extracted_text.strip():
        return None
    return extracted_text


def _normalize_table_header_name(value: Any, fallback_index: int) -> str:
    text = str(value or "").strip()
    if not text:
        return f"column_{fallback_index}"

    normalized = unicodedata.normalize("NFKD", text)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or f"column_{fallback_index}"


def _split_table_row(line: str, delimiter: str) -> list[str]:
    if delimiter == "\t":
        return [cell.strip() for cell in line.rstrip("\n\r").split("\t")]

    text = line.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]
    return [cell.strip() for cell in text.split("|")]


def _is_markdown_separator_row(cells: list[str]) -> bool:
    non_empty_cells = [cell for cell in cells if cell]
    if not non_empty_cells:
        return False
    return all(re.fullmatch(r":?-{2,}:?", cell) for cell in non_empty_cells)


def _build_structured_table_payload(
    rows: list[list[str]],
    *,
    source_label: str | None,
    source_kind: str,
    table_index: int,
    delimiter: str,
) -> dict[str, Any] | None:
    cleaned_rows = [list(row) for row in rows if any(str(cell).strip() for cell in row)]
    if not cleaned_rows:
        return None

    if delimiter == "|" and len(cleaned_rows) > 1 and _is_markdown_separator_row(cleaned_rows[1]):
        cleaned_rows = [cleaned_rows[0], *cleaned_rows[2:]]

    header_row = cleaned_rows[0]
    data_rows = cleaned_rows[1:] if len(cleaned_rows) > 1 else []
    column_count = max(len(header_row), *(len(row) for row in data_rows)) if data_rows else len(header_row)
    if column_count <= 0:
        return None

    headers = [str(header_row[idx]).strip() if idx < len(header_row) else "" for idx in range(column_count)]
    normalized_headers = [_normalize_table_header_name(header, idx + 1) for idx, header in enumerate(headers)]

    records: list[dict[str, Any]] = []
    raw_rows: list[list[str]] = []
    for row_index, row in enumerate(data_rows[:MAX_STRUCTURED_TABLE_ROWS], start=1):
        padded_row = [str(row[idx]).strip() if idx < len(row) else "" for idx in range(column_count)]
        raw_rows.append(padded_row)
        record = {
            normalized_headers[idx]: cell
            for idx, cell in enumerate(padded_row)
            if cell
        }
        if record:
            record["row_index"] = row_index
            records.append(record)

    table_dimension = "1d" if column_count <= 2 else "2d"
    if column_count == 1:
        table_kind = "single_column_list"
    elif column_count == 2:
        table_kind = "key_value"
    else:
        table_kind = "row_records"

    return {
        "table_index": table_index,
        "source": source_label,
        "source_kind": source_kind,
        "table_kind": table_kind,
        "table_dimension": table_dimension,
        "delimiter": "tab" if delimiter == "\t" else "pipe",
        "column_count": column_count,
        "row_count": len(data_rows),
        "headers": headers,
        "normalized_headers": normalized_headers,
        "rows": raw_rows,
        "records": records,
        "truncated": len(data_rows) > MAX_STRUCTURED_TABLE_ROWS,
    }


def extract_structured_tables(markdown_text: str) -> list[dict[str, Any]]:
    if not markdown_text or not markdown_text.strip():
        return []

    lines = [line.rstrip() for line in markdown_text.splitlines()]
    tables: list[dict[str, Any]] = []
    table_index = 1
    current_source: str | None = None
    i = 0

    while i < len(lines):
        stripped = lines[i].strip()
        if not stripped:
            i += 1
            continue

        source_match = re.fullmatch(r"\[(.+)\]", stripped)
        if source_match and stripped.lower().startswith("[xl/worksheets/"):
            current_source = source_match.group(1)
            i += 1
            continue

        if stripped.startswith("|") and stripped.count("|") >= 2:
            block: list[list[str]] = [_split_table_row(lines[i], "|")]
            i += 1
            while i < len(lines):
                next_stripped = lines[i].strip()
                if next_stripped.startswith("|") and next_stripped.count("|") >= 2:
                    block.append(_split_table_row(lines[i], "|"))
                    i += 1
                    continue
                break
            table = _build_structured_table_payload(
                block,
                source_label=current_source,
                source_kind="markdown",
                table_index=table_index,
                delimiter="|",
            )
            if table:
                tables.append(table)
                table_index += 1
            continue

        if "\t" in lines[i]:
            block = [_split_table_row(lines[i], "\t")]
            i += 1
            while i < len(lines):
                next_line = lines[i]
                if "\t" in next_line and not next_line.strip().startswith("|"):
                    block.append(_split_table_row(next_line, "\t"))
                    i += 1
                    continue
                break
            table = _build_structured_table_payload(
                block,
                source_label=current_source,
                source_kind="tabular",
                table_index=table_index,
                delimiter="\t",
            )
            if table:
                tables.append(table)
                table_index += 1
            continue

        i += 1

    return tables


def _structured_table_candidate_entity_classes(document_type: str) -> set[str]:
    normalized = str(document_type or "").strip().lower()
    candidates = {normalized}
    if normalized in {"bill", "quotation", "receipt"}:
        candidates.add("invoice")
    if normalized in {"invoice", "bill", "quotation", "receipt"}:
        candidates.update({"invoice", "bill", "quotation", "receipt"})
    return {candidate for candidate in candidates if candidate}


def _find_structured_table_main_entity(entities: list[dict[str, Any]], document_type: str) -> dict[str, Any] | None:
    candidate_classes = _structured_table_candidate_entity_classes(document_type)
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        class_name = str(entity.get("class_name") or entity.get("entity_type") or "").strip().lower()
        if class_name in candidate_classes:
            return entity
    return None


def _map_table_row_to_line_item_attributes(row: dict[str, Any], row_index: int) -> dict[str, Any]:
    normalized = {str(key).strip().lower(): value for key, value in row.items() if str(key).strip()}
    description = ""
    for key in ("description", "item", "product", "service", "name", "label"):
        value = str(normalized.get(key) or "").strip()
        if value:
            description = value
            break

    quantity = ""
    for key in ("quantity", "qty", "count", "units"):
        value = str(normalized.get(key) or "").strip()
        if value:
            quantity = value
            break

    unit_price = ""
    for key in ("unit_price", "unitprice", "price", "rate", "unit_rate"):
        value = str(normalized.get(key) or "").strip()
        if value:
            unit_price = value
            break

    net_amount = ""
    for key in ("net_amount", "amount", "total", "line_total", "subtotal"):
        value = str(normalized.get(key) or "").strip()
        if value:
            net_amount = value
            break

    tax_rate = str(normalized.get("tax_rate") or normalized.get("vat_rate") or "").strip()
    tax_amount = str(normalized.get("tax_amount") or normalized.get("vat_amount") or "").strip()
    account_ref = str(normalized.get("account_ref") or normalized.get("account") or "").strip()

    attrs: dict[str, Any] = {"line_no": row_index}
    if description:
        attrs["description"] = description
    if quantity:
        attrs["quantity"] = quantity
    if unit_price:
        attrs["unit_price"] = unit_price
    if net_amount:
        attrs["net_amount"] = net_amount
    if tax_rate:
        attrs["tax_rate"] = tax_rate
    if tax_amount:
        attrs["tax_amount"] = tax_amount
    if account_ref:
        attrs["account_ref"] = account_ref
    attrs["row_index"] = row_index
    attrs["row_data"] = dict(row)
    return attrs


def _table_is_key_value(table: dict[str, Any]) -> bool:
    headers = [str(item).strip().lower() for item in (table.get("normalized_headers") or [])]
    if len(headers) != 2:
        return False
    left, right = headers
    key_names = {"field", "key", "label", "attribute", "name"}
    value_names = {"value", "val", "text", "content", "details"}
    if left in key_names and right in value_names:
        return True
    if left in {"invoice_no", "invoice_date", "due_date", "currency", "status", "supplier_ref", "customer_ref", "quotation_no"}:
        return True
    return False


def _table_looks_like_line_items(table: dict[str, Any]) -> bool:
    headers = {str(item).strip().lower() for item in (table.get("normalized_headers") or [])}
    if not headers:
        return False
    line_item_markers = {"description", "item", "product", "service", "quantity", "qty", "unit_price", "price", "amount", "total", "net_amount", "tax_rate", "tax_amount"}
    return len(headers & line_item_markers) >= 2 or ("description" in headers and ("amount" in headers or "net_amount" in headers or "unit_price" in headers))


def _apply_structured_table_enrichment(
    extracted: dict[str, Any],
    *,
    document_type: str,
    class_defs: dict[str, SolfClassDef],
) -> dict[str, Any]:
    document = extracted.get("document") if isinstance(extracted.get("document"), dict) else {}
    metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}
    structured_tables = metadata.get("structured_tables") if isinstance(metadata.get("structured_tables"), dict) else {}
    tables = structured_tables.get("tables") if isinstance(structured_tables.get("tables"), list) else []
    if not tables:
        return extracted

    entities = extracted.get("entities") if isinstance(extracted.get("entities"), list) else []
    relationships = extracted.get("relationships") if isinstance(extracted.get("relationships"), list) else []

    main_entity = _find_structured_table_main_entity(entities, document_type)
    if main_entity is None and any(_table_looks_like_line_items(table) for table in tables):
        main_entity = {
            "entity_id": "structured_document_main",
            "entity_name": str(document.get("doc_key") or document.get("doc_name") or document_type or "document"),
            "class_name": document_type if document_type in class_defs else "entity",
            "attributes": {},
            "confidence": 0.6,
        }
        entities.append(main_entity)

    main_attrs = main_entity.get("attributes") if isinstance(main_entity.get("attributes"), dict) else None

    line_item_class_exists = "invoice_line" in class_defs
    line_item_counter = 0
    enriched_tables: list[dict[str, Any]] = []

    for table in tables:
        if not isinstance(table, dict):
            continue

        records = table.get("records") if isinstance(table.get("records"), list) else []
        if not records:
            enriched_tables.append(table)
            continue

        if _table_is_key_value(table) and main_attrs is not None:
            merged = dict(main_attrs)
            for record in records:
                if not isinstance(record, dict):
                    continue
                values = [str(value).strip() for key, value in record.items() if key != "row_index" and str(value).strip()]
                if len(values) < 2:
                    continue
                key_name = _normalize_table_header_name(values[0], 1)
                merged.setdefault(key_name, values[1])
            main_entity["attributes"] = merged
            table["mapped_to"] = "main_entity_attributes"
            enriched_tables.append(table)
            continue

        if _table_looks_like_line_items(table) and main_entity is not None and line_item_class_exists:
            main_entity_name = str(main_entity.get("entity_name") or main_entity.get("entity_id") or document.get("doc_key") or document_type or "document").strip()
            for record in records:
                if not isinstance(record, dict):
                    continue
                line_item_counter += 1
                line_item_id = f"structured_line_{line_item_counter}"
                line_item_attrs = _map_table_row_to_line_item_attributes(record, line_item_counter)
                if main_entity_name:
                    line_item_attrs.setdefault("invoice_ref", main_entity_name)
                entities.append(
                    {
                        "entity_id": line_item_id,
                        "entity_name": line_item_attrs.get("description") or f"line_item_{line_item_counter}",
                        "class_name": "invoice_line",
                        "operation": "ingest",
                        "attributes": line_item_attrs,
                        "confidence": 0.75,
                    }
                )
                relationships.append(
                    {
                        "source_entity_id": str(main_entity.get("entity_id") or "structured_document_main"),
                        "target_entity_id": line_item_id,
                        "relationship_type": "has_line_item",
                        "relationship_cat": "semantic",
                        "operation": "ingest",
                        "attributes": {"source": "structured_table"},
                        "confidence": 0.85,
                    }
                )
            table["mapped_to"] = "invoice_line_entities"
            enriched_tables.append(table)
            continue

        enriched_tables.append(table)

    if enriched_tables:
        structured_tables["tables"] = enriched_tables
        metadata["structured_tables"] = structured_tables
        document["metadata"] = metadata
        extracted["document"] = document
    extracted["entities"] = entities
    extracted["relationships"] = relationships
    return extracted


def build_model_contents(source_path_or_uri: str, gcs_uri: str, prompt: str) -> list[Any]:
    del gcs_uri
    source_path = Path(source_path_or_uri)
    if source_path.exists():
        extracted_text = extract_local_text(source_path)
        if extracted_text:
            return [f"{prompt}\n\nDocument text:\n{_truncate_text(extracted_text)}"]

    # Fallback to text-only prompting for binary sources when raw text extraction
    # is unavailable. This keeps ingestion runnable without provider-specific file parts.
    return [
        f"{prompt}\n\nSource path: {source_path_or_uri}\n"
        "No direct text extraction was available for this source."
    ]


def _strip_json_fence(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text.startswith("```"):
        return text
    if text.lower().startswith("```json"):
        text = text[7:]
    else:
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def _iter_json_candidates(raw_text: str) -> list[str]:
    text = str(raw_text or "").strip()
    candidates: list[str] = []
    if text:
        candidates.append(text)

    stripped = _strip_json_fence(text)
    if stripped and stripped not in candidates:
        candidates.append(stripped)

    for candidate in (text, stripped):
        open_idx = candidate.find("{")
        close_idx = candidate.rfind("}")
        if open_idx != -1 and close_idx != -1 and close_idx > open_idx:
            sliced = candidate[open_idx:close_idx + 1].strip()
            if sliced and sliced not in candidates:
                candidates.append(sliced)

    return candidates


def _autoclose_truncated_json_object(candidate: str) -> str | None:
    text = str(candidate or "").strip()
    if not text:
        return None
    first_brace = text.find("{")
    if first_brace == -1:
        return None
    text = text[first_brace:]
    if "```" in text:
        text = text.split("```", 1)[0].strip()

    stack: list[str] = []
    in_string = False
    escape = False
    for ch in text:
        if in_string:
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            stack.append("}")
            continue
        if ch == "[":
            stack.append("]")
            continue
        if ch in {"}", "]"}:
            if not stack or stack[-1] != ch:
                return None
            stack.pop()

    repaired = text
    if in_string:
        last_quote = repaired.rfind('"')
        if last_quote <= 0:
            return None
        repaired = repaired[:last_quote]

    repaired = repaired.rstrip()
    while repaired and repaired[-1] in {",", ":"}:
        repaired = repaired[:-1].rstrip()

    if not repaired or not repaired.startswith("{"):
        return None
    return repaired + "".join(reversed(stack))


def _parse_json_object_best_effort(raw_text: str) -> dict[str, Any] | None:
    for candidate in _iter_json_candidates(raw_text):
        if not candidate:
            continue
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            value = None
        if isinstance(value, dict):
            return value

        repaired = _autoclose_truncated_json_object(candidate)
        if not repaired:
            continue
        try:
            repaired_value = json.loads(repaired)
        except json.JSONDecodeError:
            continue
        if isinstance(repaired_value, dict):
            return repaired_value
    return None


def generate_json(
    client: Any,
    model: str,
    source_path_or_uri: str,
    gcs_uri: str,
    prompt: str,
    call_name: str = "unspecified",
    run_id: str | None = None,
    complexity: str = "complex",
) -> dict[str, Any]:
    contents = build_model_contents(source_path_or_uri, gcs_uri, prompt)
    response = None
    last_exc: Exception | None = None
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            with TimedOperation(f"genai_call:{call_name}", run_id=run_id) as timer:
                response = generate_content_with_openrouter_fallback(
                    primary_call=lambda: client.models.generate_content(
                        model=model,
                        contents=contents,
                        temperature=0.0,
                    ),
                    model=model,
                    contents=contents,
                    temperature=0.0,
                    call_name=call_name,
                    complexity=complexity,
                )
            if ENABLE_TIMING and timer.elapsed_ms > 0:
                LOGGER.info("genai_call_timing run_id=%s call=%s model=%s elapsed_ms=%.2f", 
                            run_id or "n/a", call_name, model, timer.elapsed_ms)
            break
        except Exception as exc:
            last_exc = exc
            error_text = str(exc).upper()
            is_quota = is_quota_exhausted_error(exc)
            is_transient = any(
                token in error_text
                for token in ("500", "INTERNAL", "503", "UNAVAILABLE", "429", "DEADLINE_EXCEEDED")
            )
            if not is_transient or attempt >= max_attempts:
                if is_quota:
                    LOGGER.error(
                        "genai_quota_exhausted run_id=%s call=%s model=%s attempt=%s/%s error=%s",
                        run_id or "n/a",
                        call_name,
                        model,
                        attempt,
                        max_attempts,
                        str(exc),
                    )
                raise

            # Backoff more aggressively for quota errors to reduce immediate retry storms.
            delay_seconds = min(2 ** attempt, 8) if is_quota else min(2 ** (attempt - 1), 4)
            LOGGER.warning(
                "genai_request_retry run_id=%s call=%s model=%s attempt=%s/%s delay_seconds=%s error=%s",
                run_id or "n/a",
                call_name,
                model,
                attempt,
                max_attempts,
                delay_seconds,
                str(exc),
            )
            time.sleep(delay_seconds)

    if response is None:
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Model request failed before receiving a response")

    text = str(response.text or "{}").strip()
    parsed = _parse_json_object_best_effort(text)

    if not isinstance(parsed, dict):
        # One best-effort JSON repair pass: ask the model to only fix formatting
        # and return strict JSON without changing semantic content.
        repair_prompt = (
            "Repair the following malformed JSON into valid JSON. "
            "Do not add commentary, do not use code fences, and do not invent new fields. "
            "Return a JSON object only.\n\n"
            f"Malformed JSON:\n{text[:40000]}"
        )
        try:
            repair_response = generate_content_with_openrouter_fallback(
                primary_call=lambda: client.models.generate_content(
                    model=model,
                    contents=[repair_prompt],
                    temperature=0.0,
                ),
                model=model,
                contents=[repair_prompt],
                temperature=0.0,
                call_name=f"{call_name}_json_repair",
                complexity="simple",
            )
            repaired_text = str(repair_response.text or "").strip()

            parsed = _parse_json_object_best_effort(repaired_text)
        except Exception as repair_exc:
            LOGGER.warning(
                "genai_json_repair_failed run_id=%s call=%s model=%s error=%s",
                run_id or "n/a",
                call_name,
                model,
                repair_exc,
            )

    if not isinstance(parsed, dict):
        raise RuntimeError(f"Model response was not valid JSON: {text[:500]}")

    LOGGER.info(
        "genai_json_response run_id=%s call=%s model=%s payload=%s",
        run_id or "n/a",
        call_name,
        model,
        json.dumps(parsed, ensure_ascii=False),
    )
    return parsed


def normalize_entity_class_name(raw_class_name: Any, class_defs: dict[str, SolfClassDef]) -> str:
    if not raw_class_name:
        return "entity"

    key = str(raw_class_name).strip().lower()
    aliases = {
        "organisation": "organization",
        "org": "organization",
    }
    key = aliases.get(key, key)

    # Canonicalize class families so semantically equivalent labels do not
    # create class drift across documents (e.g., legal_entity vs company).
    canonical_family_map = {
        "legal_entity": "company",
    }
    candidate = canonical_family_map.get(key, key)
    if candidate in class_defs:
        key = candidate

    return key if key in class_defs else "entity"


def normalize_entity_operation(raw_operation: Any) -> str:
    if not raw_operation:
        return "ingest"
    operation = str(raw_operation).strip().lower()
    return operation if operation in ENTITY_ACTIONS else "ingest"


def normalize_person_entity_name(raw_name: Any) -> str:
    text = str(raw_name or "").strip()
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


def _normalize_person_name_key(raw_name: Any) -> str:
    """Normalize person names for duplicate/similarity comparisons."""
    text = normalize_person_entity_name(raw_name)
    text = str(text or "").strip().lower()
    text = re.sub(r"[^a-z0-9äöüß]+", " ", text)
    return " ".join(text.split())


def _person_name_tokens(raw_name: Any) -> list[str]:
    key = _normalize_person_name_key(raw_name)
    return [token for token in key.split() if token]


def _person_name_initial_tokens(raw_name: Any) -> list[str]:
    return [token[0] for token in _person_name_tokens(raw_name)[:-1] if token]


COMMON_PERSON_SURNAMES = {
    "miller",
    "johnson",
    "smith",
    "williams",
    "brown",
    "jones",
    "garcia",
    "davis",
    "rodriguez",
    "wilson",
    "martin",
    "anderson",
    "thomas",
    "taylor",
    "moore",
    "jackson",
    "martinez",
    "lee",
}


def _person_surname_token(raw_name: Any) -> str:
    tokens = _person_name_tokens(raw_name)
    return tokens[-1] if tokens else ""


def _person_initial_prefix_aligned(person_name: Any, candidate_name: Any) -> bool:
    current_tokens = _person_name_tokens(person_name)[:-1]
    candidate_tokens = _person_name_tokens(candidate_name)[:-1]
    if not current_tokens or not candidate_tokens:
        return False
    prefix_len = min(len(current_tokens), len(candidate_tokens))
    if prefix_len <= 0:
        return False

    for current_token, candidate_token in zip(current_tokens[:prefix_len], candidate_tokens[:prefix_len]):
        if current_token == candidate_token:
            continue
        if len(current_token) == 1 and candidate_token.startswith(current_token):
            continue
        if len(candidate_token) == 1 and current_token.startswith(candidate_token):
            continue
        return False
    return True


def _classify_person_similarity_confidence(
    person_name: Any,
    candidate_name: Any,
    token_overlap: list[str] | None = None,
) -> str:
    current_key = _normalize_person_name_key(person_name)
    candidate_key = _normalize_person_name_key(candidate_name)
    if current_key and current_key == candidate_key:
        return "high"

    overlap = {str(item or "").strip().lower() for item in (token_overlap or []) if str(item or "").strip()}
    surname = _person_surname_token(person_name)
    candidate_surname = _person_surname_token(candidate_name)
    strong_non_surname_overlap = {
        token
        for token in overlap
        if token not in {surname, candidate_surname} and len(token) > 1
    }
    common_surname = bool(surname and surname == candidate_surname and surname in COMMON_PERSON_SURNAMES)

    if len(overlap) >= 3:
        return "high"
    if common_surname and not strong_non_surname_overlap and not _person_initial_prefix_aligned(person_name, candidate_name):
        return "low"
    if len(overlap) >= 2:
        return "medium"

    if overlap and _person_initial_prefix_aligned(person_name, candidate_name):
        return "medium"

    return "low"


def _is_meaningful_person_similarity_candidate(
    person_name: Any,
    candidate_name: Any,
    token_overlap: list[str] | None = None,
) -> bool:
    return _classify_person_similarity_confidence(person_name, candidate_name, token_overlap) in {"medium", "high"}


def _build_person_similarity_review(
    ingested: dict[str, Any],
    db_summary: dict[str, Any],
) -> dict[str, Any]:
    """Build a high-visibility review block for likely same-person candidates.

    This is advisory only (no auto-merge), because person merge is a sensitive operation.
    """
    entities = ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []
    if not entities:
        return {
            "attention_required": False,
            "has_potential_duplicates": False,
            "alert_count": 0,
            "headline": "",
            "alerts": [],
            "recommended_actions": [],
        }

    entity_rows = db_summary.get("entity_rows") if isinstance(db_summary.get("entity_rows"), list) else []
    entity_id_to_object_id: dict[str, int] = {}
    for row in entity_rows:
        if not isinstance(row, dict):
            continue
        entity_id = str(row.get("entity_id") or "").strip()
        object_id = int(row.get("object_id") or 0)
        if entity_id and object_id > 0:
            entity_id_to_object_id[entity_id] = object_id

    person_entities: list[dict[str, Any]] = []
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        if str(entity.get("class_name") or "").strip().lower() != "person":
            continue
        person_name = str(entity.get("name") or "").strip()
        if not person_name:
            continue
        person_entities.append(entity)

    if not person_entities:
        return {
            "attention_required": False,
            "has_potential_duplicates": False,
            "alert_count": 0,
            "headline": "",
            "alerts": [],
            "recommended_actions": [],
        }

    alerts: list[dict[str, Any]] = []
    try:
        with object_db.get_connection() as connection:
            for entity in person_entities:
                entity_id = str(entity.get("entity_id") or "").strip()
                person_name = str(entity.get("name") or "").strip()
                if not person_name:
                    continue

                current_object_id = int(entity_id_to_object_id.get(entity_id) or 0)
                current_key = _normalize_person_name_key(person_name)

                candidates = object_db.suggest_similar_objects(
                    connection=connection,
                    object_name=person_name,
                    class_name="person",
                    limit=8,
                    min_similarity=0.14,
                )

                reviewed_candidates: list[dict[str, Any]] = []
                for candidate in candidates:
                    if not isinstance(candidate, dict):
                        continue
                    candidate_id = int(candidate.get("object_id") or 0)
                    candidate_name = str(candidate.get("object_name") or "").strip()
                    if not candidate_name:
                        continue
                    if current_object_id > 0 and candidate_id == current_object_id:
                        continue

                    candidate_key = _normalize_person_name_key(candidate_name)
                    if not candidate_key:
                        continue

                    overlap_tokens = candidate.get("token_overlap") if isinstance(candidate.get("token_overlap"), list) else []
                    confidence_level = _classify_person_similarity_confidence(
                        person_name,
                        candidate_name,
                        overlap_tokens,
                    )
                    if not _is_meaningful_person_similarity_candidate(person_name, candidate_name, overlap_tokens):
                        continue

                    match_type = "exact_name_duplicate" if candidate_key == current_key else "similar_name_possible_duplicate"
                    reviewed_candidates.append(
                        {
                            "object_id": candidate_id,
                            "object_name": candidate_name,
                            "similarity": float(candidate.get("similarity") or 0.0),
                            "token_overlap": overlap_tokens,
                            "confidence_level": confidence_level,
                            "match_type": match_type,
                        }
                    )

                if not reviewed_candidates:
                    continue

                reviewed_candidates.sort(
                    key=lambda item: (
                        0 if str(item.get("match_type") or "") == "exact_name_duplicate" else 1,
                        -float(item.get("similarity") or 0.0),
                        str(item.get("object_name") or ""),
                    )
                )

                alerts.append(
                    {
                        "entity_name": person_name,
                        "entity_id": entity_id,
                        "current_object_id": current_object_id if current_object_id > 0 else None,
                        "message": (
                            f"Potential same-person match detected for '{person_name}'. "
                            "Review candidates and merge/link aliases if they refer to the same real person."
                        ),
                        "candidate_count": len(reviewed_candidates),
                        "candidates": reviewed_candidates[:5],
                    }
                )
    except Exception as exc:
        LOGGER.warning("Failed to build person similarity review: %s", exc)
        return {
            "attention_required": False,
            "has_potential_duplicates": False,
            "alert_count": 0,
            "headline": "",
            "alerts": [],
            "recommended_actions": [],
            "error": str(exc),
        }

    if not alerts:
        return {
            "attention_required": False,
            "has_potential_duplicates": False,
            "alert_count": 0,
            "headline": "",
            "alerts": [],
            "recommended_actions": [],
        }

    summary_lines: list[str] = []
    for alert in alerts[:5]:
        if not isinstance(alert, dict):
            continue
        entity_name = str(alert.get("entity_name") or "").strip()
        candidates = alert.get("candidates") if isinstance(alert.get("candidates"), list) else []
        candidate_names = [
            str(item.get("object_name") or "").strip()
            for item in candidates[:3]
            if isinstance(item, dict) and str(item.get("object_name") or "").strip()
        ]
        if entity_name and candidate_names:
            summary_lines.append(f"{entity_name} -> {', '.join(candidate_names)}")
        elif entity_name:
            summary_lines.append(entity_name)

    return {
        "attention_required": True,
        "has_potential_duplicates": True,
        "alert_count": len(alerts),
        "headline": "ATTENTION REQUIRED: POSSIBLE SAME-PERSON RECORDS DETECTED",
        "message": (
            "The ingested document contains person names that may refer to existing people in the database. "
            "Please review and merge/link aliases where appropriate."
        ),
        "alerts": alerts,
        "summary_lines": summary_lines,
        "recommended_actions": [
            "Review each candidate in person_similarity_review.alerts",
            "If same person: create alias link or merge records",
            "If not same person: keep separate records",
        ],
    }


def _normalized_evidence_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def prune_unsupported_identifier_attributes_from_markdown(ingested: dict[str, Any], markdown_text: str) -> int:
    """Drop identifier-like attributes whose values are not evidenced in the source markdown.

    This is a generic anti-hallucination guardrail: values such as registration numbers,
    tax IDs, EORI, UID, etc. must be traceable to the current document text.
    """
    entities = ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []
    if not entities:
        return 0

    plain = _markdown_to_plain_text(markdown_text or "")
    plain_norm = _normalized_evidence_text(plain)
    if not plain_norm:
        return 0

    removed = 0
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        if not attrs:
            continue

        filtered: dict[str, Any] = {}
        for key, value in attrs.items():
            key_norm = str(key or "").strip().lower()
            is_identifier_key = bool(
                re.search(r"(^|_)(?:id|identifier|no|nr|number|num|registration|reg|tax|vat|uid|eori|ahv|iban|bic|swift)($|_)", key_norm)
            )
            if not is_identifier_key:
                filtered[key] = value
                continue

            value_text = str(value or "").strip()
            value_norm = _normalized_evidence_text(value_text)

            # Require non-empty evidence for identifier-like values.
            if not value_norm:
                removed += 1
                LOGGER.warning(
                    "Dropping unsupported identifier attribute key=%s value=%s reason=empty_value",
                    key,
                    value_text,
                )
                continue

            has_direct_match = value_text.lower() in plain.lower()
            has_normalized_match = len(value_norm) >= 4 and value_norm in plain_norm

            if has_direct_match or has_normalized_match:
                filtered[key] = value
                continue

            removed += 1
            LOGGER.warning(
                "Dropping unsupported identifier attribute key=%s value=%s reason=no_document_evidence",
                key,
                value_text[:120],
            )

        entity["attributes"] = filtered

    return removed


def normalize_effective_date(raw_date: Any) -> str | None:
    if raw_date is None:
        return None
    date_text = str(raw_date).strip()
    if not date_text:
        return None
    try:
        parsed = datetime.strptime(date_text, "%Y-%m-%d")
        return parsed.strftime("%Y-%m-%d")
    except ValueError:
        return None


def _normalize_dotted_date_to_iso(raw_date: str) -> str | None:
    date_text = str(raw_date or "").strip()
    if not date_text:
        return None
    try:
        return datetime.strptime(date_text, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def normalize_relationship_operation(raw_operation: Any) -> str:
    if not raw_operation:
        return "upsert"
    operation = str(raw_operation).strip().lower()
    return operation if operation in {"upsert", "delete"} else "upsert"


def _merge_relationship_payload(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)

    existing_attrs = merged.get("attributes") if isinstance(merged.get("attributes"), dict) else {}
    incoming_attrs = incoming.get("attributes") if isinstance(incoming.get("attributes"), dict) else {}
    if incoming_attrs:
        combined_attrs = dict(existing_attrs)
        combined_attrs.update(incoming_attrs)
        merged["attributes"] = combined_attrs

    existing_conf = merged.get("confidence")
    incoming_conf = incoming.get("confidence")
    try:
        if incoming_conf is not None and (existing_conf is None or float(incoming_conf) > float(existing_conf)):
            merged["confidence"] = incoming_conf
    except (TypeError, ValueError):
        pass

    if not merged.get("effective_from") and incoming.get("effective_from"):
        merged["effective_from"] = incoming.get("effective_from")
    if not merged.get("effective_until") and incoming.get("effective_until"):
        merged["effective_until"] = incoming.get("effective_until")
    if not merged.get("exclusivity_scope") and incoming.get("exclusivity_scope"):
        merged["exclusivity_scope"] = incoming.get("exclusivity_scope")

    return merged


def normalize_exclusivity_scope(raw_scope: Any) -> str | None:
    if raw_scope is None:
        return None
    scope = str(raw_scope).strip().lower()
    if scope in {"by_target", "by_source"}:
        return scope
    return None


def filter_attributes_by_class(
    class_name: str,
    attributes: dict[str, Any],
    class_defs: dict[str, SolfClassDef],
) -> dict[str, Any]:
    allowed = class_defs.get(class_name, SolfClassDef(class_name, None, set(), set())).allowed_attributes
    if not attributes:
        return {}
    if not allowed:
        return dict(attributes)

    out: dict[str, Any] = {}
    for k, v in attributes.items():
        if k in allowed:
            # Schema-defined attribute: keep as-is (canonical name preserved)
            out[k] = v
        elif v not in (None, "", [], {}):
            # Guardrail for hallucinated identifier-like fields:
            # if attribute name looks like an ID/number key but is not in schema,
            # drop it instead of persisting a potentially fabricated identifier.
            key_norm = str(k or "").strip().lower()
            looks_identifier_key = bool(
                re.search(r"(^|_)(?:id|identifier|no|nr|number|num|registration|reg|tax|vat|uid|eori)($|_)", key_norm)
            )
            if looks_identifier_key:
                LOGGER.warning(
                    "Dropping non-schema identifier-like attribute class=%s key=%s value=%s",
                    class_name,
                    k,
                    str(v)[:120],
                )
                continue

            # Non-schema non-identifier attribute: keep if non-empty.
            out[k] = v
    return out


def collect_solf_attribute_proposals(
    ingested: dict[str, Any],
    class_defs: dict[str, SolfClassDef],
    doc_id: int,
) -> list[dict[str, Any]]:
    entities = ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []
    proposals_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    for entity in entities:
        if not isinstance(entity, dict):
            continue

        class_name = str(entity.get("class_name") or "").strip().lower()
        if not class_name or class_name not in class_defs:
            continue

        allowed = class_defs[class_name].allowed_attributes
        attributes = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        entity_name = str(entity.get("name") or "").strip()

        for attribute_name, attribute_value in attributes.items():
            normalized_attribute = str(attribute_name or "").strip().lower()
            if not normalized_attribute or normalized_attribute in allowed:
                continue
            if attribute_value in (None, "", [], {}):
                continue

            key = (class_name, normalized_attribute)
            proposal = proposals_by_key.get(key)
            if proposal is None:
                proposals_by_key[key] = {
                    "class_name": class_name,
                    "attribute_name": normalized_attribute,
                    "first_seen_doc_id": int(doc_id),
                    "last_seen_doc_id": int(doc_id),
                    "sample_value": attribute_value,
                    "occurrence_count": 1,
                    "status": "proposed",
                    "metadata": {
                        "source": "ingest_non_schema_attribute",
                        "entity_name": entity_name,
                        "entity_id": entity.get("entity_id"),
                    },
                }
                continue

            proposal["occurrence_count"] = int(proposal.get("occurrence_count") or 0) + 1
            proposal["last_seen_doc_id"] = int(doc_id)

    return list(proposals_by_key.values())


def apply_domain_action_policy(routed: dict[str, Any], extracted: dict[str, Any], class_defs: dict[str, SolfClassDef]) -> dict[str, Any]:
    if domain_function is None or not hasattr(domain_function, "apply_document_action_policy"):
        return extracted
    try:
        result = domain_function.apply_document_action_policy(routed, extracted, class_defs)
        return result if isinstance(result, dict) else extracted
    except Exception:
        LOGGER.exception("Domain action policy hook failed")
        return extracted


def enforce_domain_action_policy(
    routed: dict[str, Any],
    extracted: dict[str, Any],
    class_defs: dict[str, SolfClassDef],
    enforce_required_actions: bool | None = None,
    required_action_policy_text: str | None = None,
) -> None:
    if domain_function is None or not hasattr(domain_function, "enforce_required_domain_actions"):
        return
    domain_function.enforce_required_domain_actions(
        routed,
        extracted,
        class_defs,
        enforce_required_actions=enforce_required_actions,
        required_action_policy_text=required_action_policy_text,
    )


def ensure_domain_schema(connection: Any) -> None:
    if domain_function is None or not hasattr(domain_function, "ensure_domain_tables"):
        return
    domain_function.ensure_domain_tables(connection)


def to_solf_objects(payload: dict[str, Any], class_defs: dict[str, SolfClassDef]) -> dict[str, Any]:
    entities = payload.get("entities") or []
    relationships = payload.get("relationships") or []
    document_payload = payload.get("document") if isinstance(payload.get("document"), dict) else {}

    document_effective_date = normalize_effective_date(
        document_payload.get("document_effective_date") or document_payload.get("doc_date")
    )
    document_recorded_date = normalize_effective_date(
        document_payload.get("document_recorded_date") or document_payload.get("doc_date")
    )

    solf_entities: list[dict[str, Any]] = []
    id_to_name: dict[str, str] = {}

    for idx, entity in enumerate(entities, start=1):
        if not isinstance(entity, dict):
            continue
        entity_id = str(entity.get("entity_id") or f"e{idx}")
        entity_name = str(entity.get("entity_name") or entity_id)
        class_name = normalize_entity_class_name(entity.get("class_name") or entity.get("entity_type"), class_defs)
        if class_name == "person":
            canonical_person_name = normalize_person_entity_name(entity_name)
            if canonical_person_name:
                entity_name = canonical_person_name
        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        attrs = filter_attributes_by_class(class_name, attrs, class_defs)
        operation = normalize_entity_operation(entity.get("operation"))
        effective_from = normalize_effective_date(entity.get("effective_from")) or document_effective_date
        effective_until = normalize_effective_date(entity.get("effective_until"))

        solf_entities.append(
            {
                "entity_id": entity_id,
                "name": entity_name,
                "class_name": class_name,
                "operation": operation,
                "effective_from": effective_from,
                "effective_until": effective_until,
                "attributes": attrs,
                "confidence": entity.get("confidence"),
            }
        )
        id_to_name[entity_id] = entity_name

    solf_relationships: list[dict[str, Any]] = []
    relationship_index: dict[tuple[str, str, str, str, str], int] = {}
    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        src_id = str(rel.get("source_entity_id") or "")
        tar_id = str(rel.get("target_entity_id") or "")
        src_name = id_to_name.get(src_id, src_id)
        tar_name = id_to_name.get(tar_id, tar_id)
        if not src_name or not tar_name:
            continue

        relationship_cat = str(rel.get("relationship_cat") or "semantic")
        operation = normalize_relationship_operation(rel.get("operation"))
        exclusivity_scope = normalize_exclusivity_scope(rel.get("exclusivity_scope"))
        effective_from = normalize_effective_date(rel.get("effective_from")) or document_effective_date
        effective_until = normalize_effective_date(rel.get("effective_until"))
        attributes = rel.get("attributes") if isinstance(rel.get("attributes"), dict) else {}
        confidence = rel.get("confidence")

        for relationship_type in object_db.expand_relationship_names(rel.get("relationship_type") or "related_to"):
            payload = {
                "source_entity_id": src_id,
                "target_entity_id": tar_id,
                "source_name": src_name,
                "target_name": tar_name,
                "relationship_type": relationship_type,
                "relationship_cat": relationship_cat,
                "operation": operation,
                "exclusivity_scope": exclusivity_scope,
                "effective_from": effective_from,
                "effective_until": effective_until,
                "attributes": dict(attributes),
                "confidence": confidence,
            }
            key = (src_id, tar_id, relationship_type, relationship_cat, operation)
            existing_idx = relationship_index.get(key)
            if existing_idx is None:
                relationship_index[key] = len(solf_relationships)
                solf_relationships.append(payload)
            else:
                solf_relationships[existing_idx] = _merge_relationship_payload(solf_relationships[existing_idx], payload)

    return {
        "document": document_payload,
        "document_effective_date": document_effective_date,
        "document_recorded_date": document_recorded_date,
        "solf_entities": solf_entities,
        "solf_relationships": solf_relationships,
    }


def insert_document_record(
    connection: Any,
    gcs_uri: str,
    document: dict[str, Any],
    document_effective_date: Any = None,
    document_recorded_date: Any = None,
) -> int:
    security_context = get_security_context(required=False)
    tenant_id = str(security_context.tenant_id) if security_context is not None else "00000000-0000-0000-0000-000000000001"
    metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}
    user_metadata = metadata.get("user_metadata") if isinstance(metadata.get("user_metadata"), dict) else {}
    source_reference = user_metadata.get("source_reference") if isinstance(user_metadata.get("source_reference"), dict) else {}
    stable_source_identity = str(
        source_reference.get("stable_source_identity")
        or user_metadata.get("stable_source_identity")
        or ""
    ).strip()
    source_identity = str(
        stable_source_identity
        or source_reference.get("source_path_or_uri")
        or source_reference.get("gcs_uri")
        or gcs_uri
        or ""
    ).strip()
    document_path = str(source_reference.get("source_path_or_uri") or gcs_uri or "").strip()

    update_by_path_sql = """
    UPDATE document
    SET
        doc_name = %s,
        doc_key = %s,
        doc_path = %s,
        doc_cat = %s,
        doc_type = %s,
        doc_date = %s,
        doc_desc = %s,
        doc_theme = %s,
        keyword_text = %s,
        identifiers_kv = %s::jsonb,
        metadata = %s::jsonb,
        status = %s,
        access_level = %s,
        valid_from = COALESCE(%s, valid_from),
        valid_until = %s
    WHERE tenant_id = %s::uuid AND doc_path = %s
    RETURNING doc_id
    """

    update_by_identity_sql = """
    UPDATE document
    SET
        doc_name = %s,
        doc_key = %s,
        doc_path = %s,
        doc_cat = %s,
        doc_type = %s,
        doc_date = %s,
        doc_desc = %s,
        doc_theme = %s,
        keyword_text = %s,
        identifiers_kv = %s::jsonb,
        metadata = %s::jsonb,
        status = %s,
        access_level = %s,
        valid_from = COALESCE(%s, valid_from),
        valid_until = %s
    WHERE doc_id = (
        SELECT doc_id
        FROM document
          WHERE tenant_id = %s::uuid
             AND (metadata #>> '{user_metadata,source_reference,stable_source_identity}' = %s
              OR metadata #>> '{user_metadata,stable_source_identity}' = %s
              OR metadata #>> '{user_metadata,source_reference,source_path_or_uri}' = %s)
        ORDER BY doc_id
        LIMIT 1
    )
    RETURNING doc_id
    """

    sql = """
    INSERT INTO document (
        tenant_id, doc_name, doc_key, doc_path, doc_cat, doc_type, doc_date, doc_desc, doc_theme,
        keyword_text, identifiers_kv, metadata, status, access_level, valid_from, valid_until
    )
    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, COALESCE(%s, CURRENT_DATE), %s)
    RETURNING doc_id
    """

    keywords = document.get("keywords") if isinstance(document.get("keywords"), list) else []
    user_description_raw = str(metadata.get("user_description") or "").strip()
    user_description = _strip_ingestion_directive_text(user_description_raw)
    user_tags = metadata.get("user_tags") if isinstance(metadata.get("user_tags"), list) else []
    client_file_name = str(user_metadata.get("client_file_name") or "").strip()
    client_file_path = str(user_metadata.get("client_file_path") or "").strip()
    keyword_parts = [str(k) for k in keywords]
    semantic_profile = _build_document_semantic_profile(
        document.get("doc_cat"),
        document.get("doc_type"),
        document.get("doc_theme"),
    )
    semantic_terms = [str(item).strip() for item in (semantic_profile.get("terms") or []) if str(item).strip()]
    if user_description:
        keyword_parts.append(user_description)
    keyword_parts.extend(str(tag) for tag in user_tags if str(tag).strip())
    if str(document.get("doc_cat") or "").strip():
        keyword_parts.append(str(document.get("doc_cat")))
    if str(document.get("doc_theme") or "").strip():
        keyword_parts.append(str(document.get("doc_theme")))
    if str(document.get("doc_type") or "").strip():
        keyword_parts.append(str(document.get("doc_type")))
    if semantic_terms:
        keyword_parts.append(" ".join(semantic_terms))
    metadata_description_raw = str(metadata.get("description") or "").strip()
    metadata_description = _strip_ingestion_directive_text(metadata_description_raw)
    semantic_markdown_excerpt = ""
    if not user_description and not metadata_description:
        semantic_markdown_excerpt = _load_semantic_markdown_excerpt(metadata)

    if metadata_description:
        keyword_parts.append(metadata_description)
    if semantic_markdown_excerpt:
        keyword_parts.append(semantic_markdown_excerpt)
    identifiers = document.get("identifiers") if isinstance(document.get("identifiers"), dict) else {}
    for key, value in identifiers.items():
        if str(key or "").strip():
            keyword_parts.append(str(key))
        if value not in (None, "", [], {}):
            keyword_parts.append(str(value))

    deduped_keyword_parts: list[str] = []
    seen_keyword_tokens: set[str] = set()
    for token in keyword_parts:
        cleaned = str(token or "").strip()
        if not cleaned:
            continue
        normalized = re.sub(r"\s+", " ", cleaned).strip().lower()
        if normalized in seen_keyword_tokens:
            continue
        seen_keyword_tokens.add(normalized)
        deduped_keyword_parts.append(cleaned)

    keyword_text = " ".join(deduped_keyword_parts)

    source_path_or_uri = str(source_reference.get("source_path_or_uri") or "").strip()
    fallback_name = ""
    if source_path_or_uri:
        fallback_name = Path(source_path_or_uri).name
    if not fallback_name:
        fallback_name = Path(gcs_uri).name
    doc_name_value = client_file_name or fallback_name
    if user_description_raw and _is_directive_only_text(user_description_raw):
        metadata["ingestion_directive_text"] = user_description_raw
    if metadata_description_raw and _is_directive_only_text(metadata_description_raw):
        metadata["ingestion_directive_description"] = metadata_description_raw
    if semantic_terms or semantic_profile.get("concepts"):
        metadata["semantic_doc_profile"] = {
            "terms": semantic_terms,
            "concepts": [
                str(item).strip()
                for item in (semantic_profile.get("concepts") or [])
                if str(item).strip()
            ],
        }
        metadata["query_categories"] = sorted(
            {
                str(document.get("doc_cat") or "").strip().lower(),
                str(document.get("doc_type") or "").strip().lower(),
                *[str(item).strip().lower() for item in (semantic_profile.get("concepts") or []) if str(item).strip()],
            }
            - {""}
        )

    fallback_user_desc = user_description_raw if user_description_raw and not _is_directive_only_text(user_description_raw) else ""
    fallback_metadata_desc = (
        metadata_description_raw
        if metadata_description_raw and not _is_directive_only_text(metadata_description_raw)
        else ""
    )

    doc_desc_value = (
        user_description
        or metadata_description
        or semantic_markdown_excerpt
        or fallback_user_desc
        or fallback_metadata_desc
        or None
    )
    doc_status = str(document.get("status") or "active").strip().lower() or "active"
    inferred_access = infer_document_access_level(document.get("doc_cat"), document.get("doc_type"))
    requested_access = str(document.get("access_level") or metadata.get("access_level") or "").strip().lower()
    access_level = "restricted" if requested_access == "restricted" else (
        "finance" if inferred_access == "finance" or requested_access == "finance" else "tenant"
    )
    doc_valid_from = normalize_effective_date(
        document.get("valid_from") or document_effective_date or document.get("doc_date") or document_recorded_date
    )
    doc_valid_until = normalize_effective_date(document.get("valid_until"))

    with connection.cursor() as cursor:
        params = (
            tenant_id,
            doc_name_value,
            document.get("doc_key") or Path(gcs_uri).name,
            document_path,
            document.get("doc_cat"),
            document.get("doc_type"),
            document.get("doc_date"),
            doc_desc_value,
            document.get("doc_theme"),
            keyword_text,
            json.dumps(document.get("identifiers") or {}),
            json.dumps(metadata),
            doc_status,
            access_level,
            doc_valid_from,
            doc_valid_until,
        )

        if document_path:
            cursor.execute(update_by_path_sql, params[1:] + (tenant_id, document_path))
            row = cursor.fetchone()
            if row is not None:
                doc_id = int(row[0])
                LOGGER.warning(
                    "Document persistence collision detected; existing row will be updated doc_id=%s matched_by=%s match_value=%s new_doc_key=%s new_doc_name=%s",
                    doc_id,
                    "doc_path",
                    document_path,
                    document.get("doc_key") or Path(gcs_uri).name,
                    client_file_name or Path(gcs_uri).name,
                )
                connection.commit()
                return doc_id

        if source_identity:
            cursor.execute(update_by_identity_sql, params[1:] + (tenant_id, source_identity, source_identity, source_identity))
            row = cursor.fetchone()
            if row is not None:
                doc_id = int(row[0])
                LOGGER.warning(
                    "Document persistence collision detected; existing row will be updated doc_id=%s matched_by=%s match_value=%s new_doc_key=%s new_doc_name=%s",
                    doc_id,
                    "source_identity",
                    source_identity,
                    document.get("doc_key") or Path(gcs_uri).name,
                    client_file_name or Path(gcs_uri).name,
                )
                connection.commit()
                return doc_id

        cursor.execute(
            sql,
            params,
        )
        doc_id = int(cursor.fetchone()[0])
    connection.commit()
    return doc_id


def detect_document_persistence_mode(
    connection: Any,
    gcs_uri: str,
    document: dict[str, Any],
    document_effective_date: Any = None,
    document_recorded_date: Any = None,
) -> dict[str, Any]:
    """Detect whether document persistence will insert a new row or update an existing one."""
    metadata = document.get("metadata") if isinstance(document.get("metadata"), dict) else {}
    user_metadata = metadata.get("user_metadata") if isinstance(metadata.get("user_metadata"), dict) else {}
    source_reference = user_metadata.get("source_reference") if isinstance(user_metadata.get("source_reference"), dict) else {}
    stable_source_identity = str(
        source_reference.get("stable_source_identity")
        or user_metadata.get("stable_source_identity")
        or ""
    ).strip()
    source_identity = str(
        stable_source_identity
        or source_reference.get("source_path_or_uri")
        or source_reference.get("gcs_uri")
        or gcs_uri
        or ""
    ).strip()
    document_path = str(source_reference.get("source_path_or_uri") or gcs_uri or "").strip()

    try:
        with connection.cursor() as cursor:
            if document_path:
                cursor.execute(
                    "SELECT doc_id, doc_key, doc_name FROM document WHERE doc_path = %s ORDER BY doc_id LIMIT 1",
                    (document_path,),
                )
                row = cursor.fetchone()
                if row is not None:
                    return {
                        "persistence_action": "update_existing",
                        "matched_by": "doc_path",
                        "doc_id": int(row[0]),
                        "doc_key": row[1],
                        "doc_name": row[2],
                        "match_value": document_path,
                    }
            if source_identity:
                cursor.execute(
                    "SELECT doc_id, doc_key, doc_name FROM document WHERE metadata #>> '{user_metadata,source_reference,stable_source_identity}' = %s OR metadata #>> '{user_metadata,stable_source_identity}' = %s OR metadata #>> '{user_metadata,source_reference,source_path_or_uri}' = %s ORDER BY doc_id LIMIT 1",
                    (source_identity, source_identity, source_identity),
                )
                row = cursor.fetchone()
                if row is not None:
                    return {
                        "persistence_action": "update_existing",
                        "matched_by": "source_identity",
                        "doc_id": int(row[0]),
                        "doc_key": row[1],
                        "doc_name": row[2],
                        "match_value": source_identity,
                    }
    except Exception as exc:
        LOGGER.warning("Failed to detect document persistence mode for gcs_uri=%s: %s", gcs_uri, exc)

    return {
        "persistence_action": "insert_new",
        "matched_by": None,
        "doc_id": None,
        "doc_key": document.get("doc_key") or Path(gcs_uri).name,
        "doc_name": str(user_metadata.get("client_file_name") or Path(gcs_uri).name),
        "match_value": None,
    }


def _delete_existing_document_for_replace(connection: Any, doc_id: int) -> dict[str, Any]:
    deleted_doc_id = int(doc_id or 0)
    if deleted_doc_id <= 0:
        return {"deleted": False, "doc_id": None, "markdown_path": None}

    removal_snapshot = object_db.log_document_removal_snapshot(
        connection,
        deleted_doc_id,
        removed_by="system:replace_existing_document",
        reason="replace_existing_document lifecycle policy",
        metadata={"source": "ingest.replace_existing_document"},
    )
    if not removal_snapshot:
        return {"deleted": False, "doc_id": deleted_doc_id, "markdown_path": None}

    markdown_path = None
    with connection.cursor() as cursor:
        cursor.execute("SELECT markdown_path FROM document WHERE doc_id = %s", (deleted_doc_id,))
        row = cursor.fetchone()
        markdown_path = str(row[0] or "").strip() if row else ""

        cursor.execute("DELETE FROM document WHERE doc_id = %s", (deleted_doc_id,))
        deleted = int(cursor.rowcount or 0) > 0

    if deleted and markdown_path:
        try:
            markdown_file = Path(markdown_path)
            if markdown_file.exists():
                markdown_file.unlink()
        except Exception as exc:
            LOGGER.warning("Failed to remove markdown file after replace doc_id=%s path=%s error=%s", deleted_doc_id, markdown_path, exc)

    return {
        "deleted": deleted,
        "doc_id": deleted_doc_id,
        "markdown_path": markdown_path,
        "removal_snapshot": removal_snapshot,
    }


def _normalize_person_name_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""

    if "," in text:
        last_name, first_names = [part.strip() for part in text.split(",", 1)]
        if last_name and first_names:
            text = f"{first_names} {last_name}"

    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_shareholder_rows_from_markdown(markdown_text: str) -> dict[str, dict[str, Any]]:
    if not markdown_text.strip():
        return {}

    lines = [line.rstrip() for line in markdown_text.splitlines()]
    share_rows: dict[str, dict[str, Any]] = {}

    holder_idx = -1
    share_idx = -1
    in_share_table = False

    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("|"):
            if in_share_table:
                in_share_table = False
            continue

        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        lower_cells = [cell.lower() for cell in cells]

        if "stammanteile" in lower_cells and any("gesellschafter" in cell for cell in lower_cells):
            share_idx = lower_cells.index("stammanteile")
            holder_idx = next(i for i, cell in enumerate(lower_cells) if "gesellschafter" in cell)
            in_share_table = True
            continue

        if not in_share_table:
            continue

        if all(re.fullmatch(r":?-{2,}:?", cell) for cell in lower_cells if cell):
            continue

        if holder_idx < 0 or share_idx < 0 or len(cells) <= max(holder_idx, share_idx):
            continue

        holder_name = cells[holder_idx].strip()
        share_text = cells[share_idx].strip()
        if not holder_name or not share_text:
            continue

        key = _normalize_person_name_key(holder_name)
        if not key:
            continue

        attrs: dict[str, Any] = {"share_allocation_raw": share_text}
        match = re.search(r"(\d+)\s*x\s*CHF\s*([0-9'.,]+)", share_text, flags=re.IGNORECASE)
        if match:
            share_count = int(match.group(1))
            nominal_raw = match.group(2).replace("'", "").replace(",", "")
            try:
                nominal_value = float(nominal_raw)
                attrs["share_count"] = share_count
                attrs["share_nominal_value_chf"] = nominal_value
                attrs["share_total_nominal_value_chf"] = round(share_count * nominal_value, 2)
            except ValueError:
                attrs["share_count"] = share_count

        share_rows[key] = attrs

    return share_rows


def _extract_company_purpose_from_markdown(markdown_text: str) -> str | None:
    """Extract company purpose text from a table row with a Zweck header."""
    if not markdown_text.strip():
        return None

    lines = [line.rstrip() for line in markdown_text.splitlines()]
    purpose_idx = -1
    in_purpose_table = False

    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("|"):
            if in_purpose_table:
                in_purpose_table = False
            continue

        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        lower_cells = [cell.lower() for cell in cells]

        if "zweck" in lower_cells:
            purpose_idx = lower_cells.index("zweck")
            in_purpose_table = True
            continue

        if not in_purpose_table:
            continue

        if all(re.fullmatch(r":?-{2,}:?", cell) for cell in lower_cells if cell):
            continue

        if purpose_idx < 0 or len(cells) <= purpose_idx:
            continue

        purpose_text = cells[purpose_idx].strip()
        if purpose_text:
            return purpose_text

    return None


def _extract_company_registration_metadata_from_markdown(markdown_text: str) -> dict[str, Any]:
    """Extract registration metadata from common commercial-register table headers.

    Works off semantic headers rather than doc-specific row positions.
    """
    if not markdown_text.strip():
        return {}

    lines = [line.rstrip() for line in markdown_text.splitlines()]
    out: dict[str, Any] = {}
    registration_date_match = re.search(
        r"\bEintragung\b.*?(?P<date>\d{2}\.\d{2}\.\d{4})",
        markdown_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    registration_date_iso = (
        _normalize_dotted_date_to_iso(registration_date_match.group("date"))
        if registration_date_match
        else None
    )

    # Table 1: publication organ (Publikationsorgan)
    pub_idx = -1
    in_pub_table = False
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("|"):
            if in_pub_table:
                in_pub_table = False
            continue

        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        lower_cells = [cell.lower() for cell in cells]

        if "publikationsorgan" in lower_cells:
            pub_idx = lower_cells.index("publikationsorgan")
            in_pub_table = True
            continue

        if not in_pub_table:
            continue
        if all(re.fullmatch(r":?-{2,}:?", cell) for cell in lower_cells if cell):
            continue
        if pub_idx >= 0 and len(cells) > pub_idx:
            value = cells[pub_idx].strip()
            if value:
                out["publication_organ"] = value
                break

    # Table 2: TR-Nr / TR-Datum
    tr_no_idx = -1
    tr_date_idx = -1
    in_tr_table = False
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("|"):
            if in_tr_table:
                in_tr_table = False
            continue

        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        lower_cells = [cell.lower() for cell in cells]

        if "tr-nr" in lower_cells and "tr-datum" in lower_cells:
            tr_no_idx = lower_cells.index("tr-nr")
            tr_date_idx = lower_cells.index("tr-datum")
            in_tr_table = True
            continue

        if not in_tr_table:
            continue
        if all(re.fullmatch(r":?-{2,}:?", cell) for cell in lower_cells if cell):
            continue

        if tr_no_idx >= 0 and len(cells) > tr_no_idx:
            tr_no = cells[tr_no_idx].strip()
            if tr_no:
                out["tr_number"] = tr_no

        if tr_date_idx >= 0 and len(cells) > tr_date_idx:
            tr_date_text = cells[tr_date_idx].strip()
            if tr_date_text:
                normalized = tr_date_text
                m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", tr_date_text)
                if m:
                    normalized = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
                elif registration_date_iso and re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", registration_date_match.group("date")):
                    normalized = registration_date_iso
                out["tr_date"] = normalized

        if "tr_number" in out or "tr_date" in out:
            break

    return out


def _markdown_to_plain_text(markdown_text: str) -> str:
    text = re.sub(r"^#+\s*", "", markdown_text or "", flags=re.MULTILINE)
    text = text.replace("**", "")
    text = text.replace("\r", "\n")
    return re.sub(r"\s+", " ", text).strip()


def _normalize_profile_date(raw_value: str) -> str | None:
    normalized = normalize_effective_date(raw_value)
    if normalized:
        return normalized

    cleaned = str(raw_value or "").strip()
    if not cleaned:
        return None

    cleaned = re.sub(r"\bthe\b\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+of\s+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.;")

    for fmt in ("%d %B %Y", "%d %b %Y", "%B %d %Y", "%b %d %Y", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _extract_profile_subject_name(markdown_text: str) -> str | None:
    for pattern in (
        r"(?:^|\n)#+\s+(?:personal\s+profile|profile)\s+of\s+(?P<name>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,5})",
        r"(?:^|\n)Title:\s*(?:personal\s+profile|profile)\s+of\s+(?P<name>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,5})",
    ):
        match = re.search(pattern, markdown_text or "", flags=re.IGNORECASE)
        if match:
            return str(match.group("name") or "").strip()
    return None


def _extract_person_profile_facts_from_markdown(markdown_text: str) -> dict[str, Any]:
    text = _markdown_to_plain_text(markdown_text)
    if not text:
        return {}

    facts: dict[str, Any] = {}

    born_on_in = re.search(
        r"\bborn\s+on\s+(?P<date>[^.]+?)\s+in\s+(?P<place>[^.;]+?)(?:\s+with\b|\s+and\b|[.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if born_on_in:
        birth_date = _normalize_profile_date(born_on_in.group("date"))
        birth_place = str(born_on_in.group("place") or "").strip(" ,.;")
        if birth_date:
            facts["birth_date"] = birth_date
        if birth_place:
            facts["birth_place"] = birth_place
    else:
        born_in = re.search(
            r"\bborn\s+in\s+(?P<place>[^.;]+?)(?:\s+with\b|\s+and\b|[.;]|$)",
            text,
            flags=re.IGNORECASE,
        )
        if born_in:
            birth_place = str(born_in.group("place") or "").strip(" ,.;")
            if birth_place:
                facts["birth_place"] = birth_place

        born_on = re.search(
            r"\bborn\s+on\s+(?P<date>[^.;]+?)(?:\s+in\b|\s+with\b|\s+and\b|[.;]|$)",
            text,
            flags=re.IGNORECASE,
        )
        if born_on:
            birth_date = _normalize_profile_date(born_on.group("date"))
            if birth_date:
                facts["birth_date"] = birth_date

    citizen = re.search(
        r"\b(?:is|was)(?:\s+now)?\s+a[n]?\s+(?P<nationality>[A-Za-z][A-Za-z\s-]+?)\s+citizen\b",
        text,
        flags=re.IGNORECASE,
    )
    if citizen:
        nationality = str(citizen.group("nationality") or "").strip(" ,.;")
        if nationality:
            facts["nationality"] = nationality

    residence = re.search(
        r"\b(?:live|lives|reside|resides|living)\s+in\s+(?P<address>[^.;]+?)(?:\s+with\s+heimat\s+ort\b|\s+and\b|[.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if residence:
        address = str(residence.group("address") or "").strip(" ,.;")
        if address:
            facts["address"] = address

    heimat_ort = re.search(r"\bheimat\s+ort\s+(?P<heimat>[^.;,]+)", text, flags=re.IGNORECASE)
    if heimat_ort:
        hometown = str(heimat_ort.group("heimat") or "").strip(" ,.;")
        if hometown:
            facts["heimat_ort"] = hometown

    return facts


def enrich_person_profile_attributes_from_markdown(ingested: dict[str, Any], markdown_text: str) -> int:
    extracted = _extract_person_profile_facts_from_markdown(markdown_text)
    if not extracted:
        return 0

    entities = ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []
    person_entities = [
        entity for entity in entities
        if isinstance(entity, dict) and str(entity.get("class_name") or "").strip().lower() == "person"
    ]
    if not person_entities:
        return 0

    subject_name = _extract_profile_subject_name(markdown_text)
    target_entities = person_entities
    if subject_name:
        normalized_subject = _normalize_person_name_key(subject_name)
        matched_entities = [
            entity for entity in person_entities
            if _normalize_person_name_key(str(entity.get("name") or "")) == normalized_subject
        ]
        if matched_entities:
            target_entities = matched_entities
    elif len(person_entities) != 1:
        return 0

    updated = 0
    for entity in target_entities:
        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        merged = dict(attrs)
        changed = False
        for key, value in extracted.items():
            if key not in merged and value not in (None, "", [], {}):
                merged[key] = value
                changed = True
        if changed:
            entity["attributes"] = merged
            updated += 1

    return updated


def enrich_shareholder_relationships_from_markdown(ingested: dict[str, Any], markdown_text: str) -> int:
    rels = ingested.get("solf_relationships") if isinstance(ingested.get("solf_relationships"), list) else []
    if not rels:
        return 0

    extracted_rows = _extract_shareholder_rows_from_markdown(markdown_text)
    if not extracted_rows:
        return 0

    updated = 0
    for rel in rels:
        rel_type = str(rel.get("relationship_type") or "").strip().lower()
        if "shareholder" not in rel_type:
            continue

        src_name = str(rel.get("source_name") or "")
        key = _normalize_person_name_key(src_name)
        attrs = extracted_rows.get(key)
        if not attrs:
            continue

        rel_attrs = rel.get("attributes") if isinstance(rel.get("attributes"), dict) else {}
        merged = dict(rel_attrs)
        for attr_key, attr_value in attrs.items():
            if attr_key not in merged:
                merged[attr_key] = attr_value
        rel["attributes"] = merged
        updated += 1

    return updated


def enrich_company_purpose_from_markdown(ingested: dict[str, Any], markdown_text: str) -> int:
    """Populate company purpose attribute from markdown when extraction misses it."""
    purpose_text = _extract_company_purpose_from_markdown(markdown_text)
    if not purpose_text:
        return 0

    entities = ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []
    if not entities:
        return 0

    updated = 0
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        class_name = str(entity.get("class_name") or "").strip().lower()
        # Company inherits legal_entity in SOLF; keep this focused to company-like entities.
        if class_name not in {"company", "legal_entity", "organization"}:
            continue

        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        if attrs.get("purpose"):
            continue

        merged = dict(attrs)
        merged["purpose"] = purpose_text
        entity["attributes"] = merged
        updated += 1

    return updated


def enrich_company_registration_metadata_from_markdown(ingested: dict[str, Any], markdown_text: str) -> int:
    """Populate publication/tracking metadata from markdown table headers."""
    extracted = _extract_company_registration_metadata_from_markdown(markdown_text)
    if not extracted:
        return 0

    entities = ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []
    if not entities:
        return 0

    updated = 0
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        class_name = str(entity.get("class_name") or "").strip().lower()
        if class_name not in {"company", "legal_entity", "organization"}:
            continue

        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        merged = dict(attrs)
        changed = False
        for key, value in extracted.items():
            if not merged.get(key):
                merged[key] = value
                changed = True
        if changed:
            entity["attributes"] = merged
            updated += 1

    return updated


def _extract_eori_from_markdown(markdown_text: str) -> str | None:
    def _is_likely_eori(token: str) -> bool:
        t = str(token or "").strip().upper()
        # Typical EORI: 2-letter country prefix + alnum body and at least one digit.
        return bool(re.fullmatch(r"[A-Z]{2}[A-Z0-9]{8,17}", t) and re.search(r"\d", t))

    # Prefer strict DE format first because this deployment commonly ingests DE customs letters.
    strict_de = re.findall(r"\bDE\d{15}\b", markdown_text or "")
    if strict_de:
        return strict_de[0]

    # Generic EORI-like fallback: only from lines that mention EORI and with numeric body.
    eori_lines = [
        line
        for line in (markdown_text or "").splitlines()
        if re.search(r"\beori\b", line, flags=re.IGNORECASE)
    ]
    scoped_text = "\n".join(eori_lines) if eori_lines else (markdown_text or "")
    generic = re.findall(r"\b[A-Z]{2}[A-Z0-9]{8,17}\b", scoped_text)
    generic = [token for token in generic if _is_likely_eori(token)]
    if not generic:
        return None

    for token in generic:
        if token.upper().startswith("DE"):
            return token
    return generic[0]


def enrich_company_eori_from_markdown(ingested: dict[str, Any], markdown_text: str) -> int:
    """Populate/repair company eori_no from markdown when extraction misses or truncates it."""
    extracted_eori = _extract_eori_from_markdown(markdown_text)
    if not extracted_eori:
        return 0

    entities = ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []
    if not entities:
        return 0

    updated = 0
    for entity in entities:
        if not isinstance(entity, dict):
            continue

        class_name = str(entity.get("class_name") or "").strip().lower()
        if class_name not in {"company", "legal_entity", "organization"}:
            continue

        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        current_eori = str(attrs.get("eori_no") or "").strip()

        def _quality(value: str) -> int:
            token = str(value or "").strip().upper()
            if not token:
                return 0
            if re.fullmatch(r"DE\d{15}", token):
                return 3
            if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{8,17}", token) and re.search(r"\d", token):
                return 2
            return 1

        current_quality = _quality(current_eori)
        extracted_quality = _quality(extracted_eori)

        should_replace = (
            extracted_quality > current_quality
            or (
                extracted_quality == current_quality
                and (
                    not current_eori
                    or len(current_eori) < len(extracted_eori)
                    or (current_eori and extracted_eori.startswith(current_eori))
                )
            )
        )
        if not should_replace:
            continue

        merged = dict(attrs)
        merged["eori_no"] = extracted_eori
        entity["attributes"] = merged
        updated += 1

    return updated


def _normalize_entity_name_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_process_family_contact_candidates_from_markdown(markdown_text: str) -> list[dict[str, Any]]:
    if not str(markdown_text or "").strip():
        return []

    process_patterns: list[tuple[str, re.Pattern[str]]] = [
        ("eori", re.compile(r"\b(?:eori)\b", flags=re.IGNORECASE)),
        ("customs", re.compile(r"\b(?:customs|zoll)\b", flags=re.IGNORECASE)),
        ("vat", re.compile(r"\b(?:vat|mwst|ust)\b", flags=re.IGNORECASE)),
        ("license", re.compile(r"\b(?:license|licensing|lizenz)\b", flags=re.IGNORECASE)),
        ("permit", re.compile(r"\b(?:permit|bewilligung|genehmigung)\b", flags=re.IGNORECASE)),
    ]
    contact_cue = re.compile(
        r"\b(?:contact|contact person|ansprechpartner(?:in)?|kontaktperson|kontakt|responsible person|zust(?:aendig|ändig))\b",
        flags=re.IGNORECASE,
    )
    email_pat = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", flags=re.IGNORECASE)

    candidates: list[dict[str, Any]] = []
    for raw_line in str(markdown_text or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue

        if not contact_cue.search(line):
            continue

        process_name = ""
        for token, pattern in process_patterns:
            if pattern.search(line):
                process_name = token
                break
        if not process_name:
            continue

        emails = email_pat.findall(line)
        candidates.append(
            {
                "process_name": process_name,
                "line_text": line,
                "emails": emails,
            }
        )

    return candidates


def enrich_process_family_relationships_from_markdown(ingested: dict[str, Any], markdown_text: str) -> dict[str, int]:
    """Enrich generic process-family contact graph facts from markdown evidence.

    This adds reusable facts (relationship + missing contact email) and avoids
    query-facing semantic attributes at ingestion time.
    """
    entities = ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []
    relationships = ingested.get("solf_relationships") if isinstance(ingested.get("solf_relationships"), list) else []
    if not entities:
        return {"relationships_added": 0, "person_emails_added": 0, "candidates_detected": 0}

    people = [
        entity for entity in entities
        if isinstance(entity, dict)
        and str(entity.get("class_name") or "").strip().lower() == "person"
        and str(entity.get("entity_id") or "").strip()
    ]
    companies = [
        entity for entity in entities
        if isinstance(entity, dict)
        and str(entity.get("class_name") or "").strip().lower() in {"company", "legal_entity", "organization"}
        and str(entity.get("entity_id") or "").strip()
    ]
    if not people or not companies:
        return {"relationships_added": 0, "person_emails_added": 0, "candidates_detected": 0}

    candidates = _extract_process_family_contact_candidates_from_markdown(markdown_text)
    if not candidates:
        return {"relationships_added": 0, "person_emails_added": 0, "candidates_detected": 0}

    existing_rel_keys = {
        (
            str(rel.get("source_entity_id") or "").strip(),
            str(rel.get("target_entity_id") or "").strip(),
            str(rel.get("relationship_type") or "").strip().lower(),
        )
        for rel in relationships
        if isinstance(rel, dict)
    }

    people_index: list[tuple[int, str, dict[str, Any]]] = []
    for entity in people:
        name_key = _normalize_person_name_key(entity.get("name"))
        if name_key:
            people_index.append((len(name_key), name_key, entity))
    people_index.sort(reverse=True, key=lambda item: item[0])

    company_index: list[tuple[int, str, dict[str, Any]]] = []
    for entity in companies:
        name_key = _normalize_entity_name_key(entity.get("name"))
        if name_key:
            company_index.append((len(name_key), name_key, entity))
    company_index.sort(reverse=True, key=lambda item: item[0])

    relationships_added = 0
    person_emails_added = 0

    for candidate in candidates:
        line_text = str(candidate.get("line_text") or "").strip()
        normalized_line = _normalize_entity_name_key(line_text)
        if not normalized_line:
            continue

        matched_person = None
        for _, person_key, person_entity in people_index:
            if person_key and person_key in normalized_line:
                matched_person = person_entity
                break
        if not matched_person:
            continue

        matched_company = None
        for _, company_key, company_entity in company_index:
            if company_key and company_key in normalized_line:
                matched_company = company_entity
                break
        if not matched_company and len(companies) == 1:
            matched_company = companies[0]
        if not matched_company:
            continue

        process_name = str(candidate.get("process_name") or "").strip().lower()
        if not process_name:
            continue
        rel_type = f"has_{process_name}_contact_person"

        src_id = str(matched_company.get("entity_id") or "").strip()
        tar_id = str(matched_person.get("entity_id") or "").strip()
        rel_key = (src_id, tar_id, rel_type)
        if rel_key not in existing_rel_keys:
            relationships.append(
                {
                    "source_entity_id": src_id,
                    "target_entity_id": tar_id,
                    "source_name": str(matched_company.get("name") or "").strip(),
                    "target_name": str(matched_person.get("name") or "").strip(),
                    "relationship_type": rel_type,
                    "relationship_cat": "process",
                    "operation": "upsert",
                    "exclusivity_scope": "none",
                    "effective_from": ingested.get("document_effective_date"),
                    "effective_until": None,
                    "attributes": {
                        "source": "markdown_process_family_enrichment",
                        "process_concept": process_name,
                    },
                    "confidence": 0.8,
                }
            )
            existing_rel_keys.add(rel_key)
            relationships_added += 1

        emails = [str(item).strip() for item in (candidate.get("emails") or []) if str(item).strip()]
        if emails:
            attrs = matched_person.get("attributes") if isinstance(matched_person.get("attributes"), dict) else {}
            current_email = str(attrs.get("email") or "").strip()
            if not current_email:
                merged = dict(attrs)
                merged["email"] = emails[0]
                matched_person["attributes"] = merged
                person_emails_added += 1

    ingested["solf_relationships"] = relationships
    ingested["solf_entities"] = entities

    return {
        "relationships_added": relationships_added,
        "person_emails_added": person_emails_added,
        "candidates_detected": len(candidates),
    }


def build_solf_interpreter(solf_script: str) -> SOLFInterpreter:
    interpreter = SOLFInterpreter()
    parser_adapter = type("Parser", (object,), {"parse": staticmethod(solf_parser.parse_script)})()
    interpreter.set_parser(parser_adapter)
    interpreter.set_debug(False)
    try:
        runtime_pre_scripts, runtime_post_scripts = business_rules.load_active_business_rule_solf_script_chunks(limit=300)
    except Exception:
        runtime_pre_scripts, runtime_post_scripts = [], []
    # Expand before prepending runtime rules: the fixed marker is first-line only.
    from solf_program import expand_bundled_program

    first_script = "\n\n".join(
        chunk for chunk in [*runtime_pre_scripts, expand_bundled_program(str(solf_script or "").strip())] if chunk
    )
    if first_script:
        interpreter.load_program_script(first_script, clear_existing=True)

    runtime_post_script = "\n\n".join(chunk for chunk in runtime_post_scripts if chunk)
    if runtime_post_script:
        interpreter.load_program_script(runtime_post_script, clear_existing=False)
    return interpreter


def clause_candidates_for_action(
    class_name: str,
    action: str,
    class_defs: dict[str, SolfClassDef],
) -> list[str]:
    lineage: list[str] = []
    seen: set[str] = set()
    current = class_name

    while current and current not in seen and current in class_defs:
        lineage.append(current)
        seen.add(current)
        current = class_defs[current].parent_class_name

    candidates: list[str] = []
    for name in lineage:
        candidates.append(f"{name}_{action}")
    candidates.append(action)

    unique_candidates: list[str] = []
    seen_candidates: set[str] = set()
    for name in candidates:
        if name not in seen_candidates:
            unique_candidates.append(name)
            seen_candidates.add(name)
    return unique_candidates


def invoke_solf_entity_action(
    interpreter: SOLFInterpreter,
    class_defs: dict[str, SolfClassDef],
    action: str,
    entity_payload: dict[str, Any],
) -> tuple[Any, str | None]:
    class_name = str(entity_payload.get("class_name") or "entity").lower()
    candidates = clause_candidates_for_action(class_name, action, class_defs)
    last_clause: str | None = None

    for clause_name in candidates:
        last_clause = clause_name
        result = interpreter._invoke_clause(clause_name, [entity_payload])
        if result is not None and result is not False:
            return result, clause_name

    return None, last_clause


def invoke_solf_entity_validation(
    interpreter: SOLFInterpreter,
    class_defs: dict[str, SolfClassDef],
    entity_payload: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    class_name = str(entity_payload.get("class_name") or "entity").lower()
    candidates = clause_candidates_for_action(class_name, "validate", class_defs)
    last_clause: str | None = None

    for clause_name in candidates:
        last_clause = clause_name
        result = interpreter._invoke_clause(clause_name, [entity_payload])
        if isinstance(result, dict):
            merged = {"valid": True}
            merged.update(result)
            return merged, clause_name
        if isinstance(result, bool):
            return {"valid": bool(result)}, clause_name

    return {"valid": True}, None


def invoke_solf_resolution_policy(
    interpreter: SOLFInterpreter,
    class_defs: dict[str, SolfClassDef],
    entity_payload: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    class_name = str(entity_payload.get("class_name") or "entity").lower()
    candidates = clause_candidates_for_action(class_name, "resolve_policy", class_defs)
    default_policy = {
        "threshold": 0.72,
        "low_threshold": 0.72,
        "high_threshold": 0.72,
        "margin": 0.08,
        "max_candidates": 20,
        "name_similarity_weight": 0.35,
        "strong_match_score": 0.45,
        "medium_match_score": 0.20,
        "strong_keys": [],
        "medium_keys": [],
    }

    last_clause: str | None = None
    for clause_name in candidates:
        last_clause = clause_name
        result = interpreter._invoke_clause(clause_name, [entity_payload])
        if isinstance(result, dict):
            merged = dict(default_policy)
            merged.update(result)
            return merged, clause_name

    return default_policy, last_clause


def resolve_entity_before_action(
    interpreter: SOLFInterpreter,
    class_defs: dict[str, SolfClassDef],
    entity_payload: dict[str, Any],
) -> dict[str, Any]:
    resolution_policy, policy_clause = invoke_solf_resolution_policy(
        interpreter=interpreter,
        class_defs=class_defs,
        entity_payload=entity_payload,
    )

    resolver_payload = dict(entity_payload)
    resolver_payload["resolution_policy"] = resolution_policy
    resolver_result = solf_function.db_resolve_entity(resolver_payload)

    if not isinstance(resolver_result, dict):
        resolver_result = {"resolved": False, "decision": "create_new", "reason": "resolver_failed"}

    entity_payload["resolution_policy"] = resolution_policy
    entity_payload["resolution_policy_clause"] = policy_clause
    entity_payload["resolution_result"] = resolver_result

    if resolver_result.get("resolved"):
        entity_payload["resolved_object_id"] = resolver_result.get("object_id")
        entity_payload["resolved_object_name"] = resolver_result.get("object_name")

    return entity_payload


def _upsert_document_parts(
    connection: Any,
    doc_id: int,
    entity_results: list[dict[str, Any]],
    relationship_results: list[dict[str, Any]],
) -> dict[str, int]:
    """Populate part links for fast document/entity/relationship traversal.

    This is idempotent through (doc_id, part_key) conflict handling.
    """

    context = get_security_context(required=False)
    tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
    object_ids: set[int] = set()
    class_names: set[str] = set()
    relationship_ids: set[int] = set()

    for row in entity_results:
        raw_object_id = row.get("object_id")
        if isinstance(raw_object_id, int):
            object_ids.add(raw_object_id)

        class_name = str(row.get("class_name") or "").strip().lower()
        if class_name:
            class_names.add(class_name)

    for row in relationship_results:
        raw_relationship_id = row.get("relationship_id")
        if isinstance(raw_relationship_id, int):
            relationship_ids.add(raw_relationship_id)

    class_id_by_name: dict[str, int] = {}
    if class_names:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT class_id, class_name
                FROM object_class
                WHERE LOWER(class_name) = ANY(%s)
                """,
                (list(class_names),),
            )
            for class_id, class_name in cursor.fetchall() or []:
                normalized = str(class_name or "").strip().lower()
                if normalized and isinstance(class_id, int):
                    class_id_by_name[normalized] = class_id

    rows: list[tuple[Any, ...]] = []

    rows.append(
        (
            int(doc_id),
            f"document:{int(doc_id)}",
            None,
            None,
            None,
            json.dumps({"doc_id": int(doc_id), "link_source": "ingest"}),
        )
    )

    for class_name in sorted(class_names):
        class_id = class_id_by_name.get(class_name)
        if class_id is None:
            continue
        rows.append(
            (
                int(doc_id),
                f"class:{class_id}",
                int(class_id),
                None,
                None,
                json.dumps({"class_name": class_name, "link_source": "ingest"}),
            )
        )

    for object_id in sorted(object_ids):
        rows.append(
            (
                int(doc_id),
                f"object:{object_id}",
                None,
                int(object_id),
                None,
                json.dumps({"object_id": int(object_id), "link_source": "ingest"}),
            )
        )

    for relationship_id in sorted(relationship_ids):
        rows.append(
            (
                int(doc_id),
                f"relationship:{relationship_id}",
                None,
                None,
                int(relationship_id),
                json.dumps({"relationship_id": int(relationship_id), "link_source": "ingest"}),
            )
        )

    if not rows:
        return {
            "parts_linked": 0,
            "object_parts_linked": 0,
            "relationship_parts_linked": 0,
            "class_parts_linked": 0,
        }

    with connection.cursor() as cursor:
        # Drop stale ingest-generated part links for this document so reprocess runs
        # do not keep obsolete object/class/relationship links from older payloads.
        current_part_keys = [str(row[1]) for row in rows]
        cursor.execute(
            """
            DELETE FROM part
            WHERE doc_id = %s
                            AND tenant_id = %s::uuid
              AND COALESCE(metadata->>'link_source', '') = 'ingest'
              AND NOT (part_key = ANY(%s))
            """,
                        (int(doc_id), tenant_id, current_part_keys),
        )
        cursor.executemany(
            """
            INSERT INTO part (
                tenant_id,
                doc_id,
                part_key,
                class_id,
                object_id,
                relationship_id,
                metadata
            )
            VALUES (%s::uuid, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (tenant_id, doc_id, part_key)
            DO UPDATE SET
                class_id = COALESCE(EXCLUDED.class_id, part.class_id),
                object_id = COALESCE(EXCLUDED.object_id, part.object_id),
                relationship_id = COALESCE(EXCLUDED.relationship_id, part.relationship_id),
                metadata = COALESCE(part.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb)
            """,
            [(tenant_id, *row) for row in rows],
        )

    connection.commit()
    return {
        "parts_linked": len(rows),
        "object_parts_linked": len(object_ids),
        "relationship_parts_linked": len(relationship_ids),
        "class_parts_linked": len(class_id_by_name),
    }


def persist_solf_objects(
    connection: Any,
    interpreter: SOLFInterpreter,
    class_defs: dict[str, SolfClassDef],
    ingested: dict[str, Any],
    doc_id: int,
) -> dict[str, Any]:
    def _normalize_process_token(value: Any) -> str:
        token = str(value or "").strip().lower()
        token = re.sub(r"[^a-z0-9_\-]+", "_", token)
        token = re.sub(r"_+", "_", token).strip("_")
        return token

    def _extract_process_tokens(raw_value: Any) -> list[str]:
        values: list[str] = []
        if isinstance(raw_value, list):
            values = [str(item or "") for item in raw_value]
        elif isinstance(raw_value, str):
            values = re.split(r"[,;|]", raw_value)
        return [token for token in (_normalize_process_token(item) for item in values) if token]

    def _derive_ingest_processes(document_payload: dict[str, Any]) -> list[str]:
        ordered: list[str] = ["ingest"]
        seen = {"ingest"}

        if _document_requires_default_booking_process(document_payload, ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []):
            for process_name in (
                "booking_process",
                "booking_validation",
                "account_selection",
                "vat_classification",
                "posting_finalize",
            ):
                if process_name not in seen:
                    ordered.append(process_name)
                    seen.add(process_name)

        metadata = document_payload.get("metadata") if isinstance(document_payload.get("metadata"), dict) else {}
        user_metadata = metadata.get("user_metadata") if isinstance(metadata.get("user_metadata"), dict) else {}
        processing_directives = metadata.get("processing_directives") if isinstance(metadata.get("processing_directives"), dict) else {}

        for container in (metadata, user_metadata, processing_directives):
            for key in ("workflow_processes", "processes"):
                for process_name in _extract_process_tokens(container.get(key)):
                    if process_name not in seen:
                        ordered.append(process_name)
                        seen.add(process_name)

        return ordered

    def _invoke_ingest_workflow_hooks(document_payload: dict[str, Any]) -> dict[str, Any]:
        metadata = document_payload.get("metadata") if isinstance(document_payload.get("metadata"), dict) else {}
        processes = _derive_ingest_processes(document_payload)
        context_payload = {
            "document_type": str(document_payload.get("doc_type") or "").strip().lower(),
            "country": str(metadata.get("country") or "").strip().lower(),
            "operation": "ingest",
            "has_scope": bool(metadata.get("scope")),
            "candidates": metadata.get("scope") if isinstance(metadata.get("scope"), dict) else {},
        }

        hooks: list[dict[str, Any]] = []
        any_stop_on_error = False
        for process_name in processes:
            payload = dict(context_payload)
            payload["process"] = process_name
            hint: dict[str, Any] = {}
            execute: dict[str, Any] = {}
            plan: dict[str, Any] = {}
            status = "ok"

            try:
                hint_raw = interpreter._invoke_clause("business_rule_workflow_hint", [payload])
                hint = hint_raw if isinstance(hint_raw, dict) else {}
            except Exception as exc:
                status = "hint_error"
                hint = {"error": str(exc)}

            try:
                execute_raw = interpreter._invoke_clause("business_rule_workflow_execute", [payload])
                execute = execute_raw if isinstance(execute_raw, dict) else {}
            except Exception as exc:
                status = "execute_error" if status == "ok" else status
                execute = {"error": str(exc)}

            control_flow = str(execute.get("control_flow") or hint.get("control_flow") or "").strip().lower()
            if control_flow == "selection":
                try:
                    plan_raw = interpreter._invoke_clause("business_rule_selection_plan", [payload])
                    plan = plan_raw if isinstance(plan_raw, dict) else {}
                except Exception as exc:
                    status = "plan_error" if status == "ok" else status
                    plan = {"error": str(exc)}
            elif control_flow == "sequence":
                try:
                    plan_raw = interpreter._invoke_clause("business_rule_sequence_plan", [payload])
                    plan = plan_raw if isinstance(plan_raw, dict) else {}
                except Exception:
                    # Sequence plans are optional for older rule sets.
                    plan = {}
            elif control_flow == "iteration":
                try:
                    plan_raw = interpreter._invoke_clause("business_rule_iteration_plan", [payload])
                    plan = plan_raw if isinstance(plan_raw, dict) else {}
                except Exception:
                    plan = {}
            elif control_flow == "backtracking":
                try:
                    plan_raw = interpreter._invoke_clause("business_rule_backtracking_plan", [payload])
                    plan = plan_raw if isinstance(plan_raw, dict) else {}
                except Exception:
                    plan = {}

            any_stop_on_error = any_stop_on_error or bool(hint.get("stop_on_error", False)) or bool(execute.get("stop_on_error", False))
            hooks.append(
                {
                    "process": process_name,
                    "status": status,
                    "hint": hint,
                    "execute": execute,
                    "plan": plan,
                }
            )

        return {
            "processes": processes,
            "hooks": hooks,
            "stop_on_error": any_stop_on_error,
        }

    def _sync_domain_record_for_entity(entity_payload: dict[str, Any], object_row: dict[str, Any], action: str) -> dict[str, Any]:
        class_name = str(entity_payload.get("class_name") or "").strip().lower()
        if class_name not in {"accounting_transaction", "employee_profile", "employment_contract"}:
            return {
                "attempted": False,
                "class_name": class_name,
                "reason": "not_domain_managed",
            }
        if domain_db is None:
            return {
                "attempted": True,
                "class_name": class_name,
                "ok": False,
                "reason": "domain_db_unavailable",
            }

        object_id = int(object_row.get("object_id") or 0)
        if object_id <= 0:
            return {
                "attempted": True,
                "class_name": class_name,
                "ok": False,
                "reason": "missing_object_id",
            }

        if class_name == "accounting_transaction":
            if action == "delete":
                deleted_count = int(domain_db.delete_transaction_by_object_id(connection, object_id))
                return {
                    "attempted": True,
                    "class_name": class_name,
                    "ok": True,
                    "mode": "delete",
                    "transactions_deleted": deleted_count,
                }

            attrs = entity_payload.get("attributes") if isinstance(entity_payload.get("attributes"), dict) else {}
            source_type = str(attrs.get("source_type") or attrs.get("document_type") or "").strip().lower()
            review_mode = str(attrs.get("booking_review_mode") or "required").strip().lower()
            requires_review = source_type in {"invoice", "bill", "receipt"} and review_mode not in {"skip", "bypass", "disabled"}

            if requires_review and hasattr(domain_db, "enqueue_accounting_booking_review"):
                queued = domain_db.enqueue_accounting_booking_review(
                    connection=connection,
                    payload=entity_payload,
                    object_row=object_row,
                )
                return {
                    "attempted": True,
                    "class_name": class_name,
                    "ok": True,
                    "mode": "queued_for_review",
                    "accounting_review_required": True,
                    "accounting_review_status": "pending",
                    "accounting_review_id": queued.get("review_id"),
                    "validation": queued.get("validation") or {},
                    "transaction_id": None,
                    "ledger_lines_written": 0,
                }

            domain_result = domain_db.upsert_transaction_and_lines(
                connection=connection,
                payload=entity_payload,
                object_row=object_row,
            )
            return {
                "attempted": True,
                "class_name": class_name,
                "ok": bool(domain_result.get("ok")),
                "mode": "upsert",
                "transaction_id": domain_result.get("transaction_id"),
                "ledger_lines_written": int(domain_result.get("ledger_lines_written") or 0),
                "validation": domain_result.get("validation"),
            }

        if action == "delete":
            deleted = domain_db.delete_hr_records_by_object_id(connection, object_id)
            return {
                "attempted": True,
                "class_name": class_name,
                "ok": True,
                "mode": "delete",
                "hr_deleted": deleted,
            }

        domain_result = domain_db.upsert_hr_domain_record(
            connection=connection,
            payload=entity_payload,
            object_row=object_row,
        )
        return {
            "attempted": True,
            "class_name": class_name,
            "ok": bool(domain_result.get("written")),
            "mode": "upsert",
            "domain_table": domain_result.get("domain_table"),
            "domain_id": domain_result.get("domain_id"),
        }

    entity_results: list[dict[str, Any]] = []
    ambiguity_results: list[dict[str, Any]] = []
    relationship_results: list[dict[str, Any]] = []
    domain_sync_results: list[dict[str, Any]] = []
    id_to_db_object: dict[str, dict[str, Any]] = {}
    document_payload = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
    workflow_runtime = _invoke_ingest_workflow_hooks(document_payload)
    stop_on_error = bool(workflow_runtime.get("stop_on_error", False))

    solf_function.set_connection(connection)
    try:
        for entity in ingested["solf_entities"]:
            action = normalize_entity_operation(entity.get("operation"))
            entity_payload = {
                "entity_id": entity["entity_id"],
                "object_name": entity["name"],
                "class_name": entity["class_name"],
                "query_categories": sorted(
                    {
                        str(entity.get("class_name") or "").strip().lower(),
                        str(document_payload.get("doc_cat") or "").strip().lower(),
                        str(document_payload.get("doc_type") or "").strip().lower(),
                    }
                    - {""}
                ),
                "operation": action,
                "effective_from": entity.get("effective_from") or ingested.get("document_effective_date"),
                "effective_until": entity.get("effective_until"),
                "recorded_on": ingested.get("document_recorded_date"),
                "attributes": dict(entity.get("attributes") or {}),
                "confidence": entity.get("confidence"),
                "doc_id": doc_id,
            }

            attrs_after_rules = business_rules.apply_business_rules_to_attributes(
                dict(entity_payload.get("attributes") or {}),
                context={
                    "country": (entity_payload.get("attributes") or {}).get("country"),
                    "document_type": (ingested.get("document") or {}).get("doc_type") if isinstance(ingested.get("document"), dict) else None,
                    "entity_class": entity_payload.get("class_name"),
                    "operation": entity_payload.get("operation"),
                },
            )
            entity_payload["attributes"] = attrs_after_rules if isinstance(attrs_after_rules, dict) else {}

            validation_result, validation_clause = invoke_solf_entity_validation(
                interpreter=interpreter,
                class_defs=class_defs,
                entity_payload=entity_payload,
            )
            entity_payload["validation_result"] = validation_result
            entity_payload["validation_clause"] = validation_clause
            if not bool(validation_result.get("valid", True)):
                if entity_payload.get("class_name") == "accounting_transaction":
                    LOGGER.warning(
                        "accounting_transaction validation failed object=%s reason=%s details=%s",
                        entity_payload.get("object_name"),
                        validation_result.get("reason"),
                        validation_result,
                    )
                row = {
                    "object_id": None,
                    "object_name": entity_payload["object_name"],
                    "class_name": entity_payload["class_name"],
                    "action": "validation_failed",
                    "action_clause": validation_clause,
                    "validation": validation_result,
                }
                entity_results.append(row)
                if stop_on_error:
                    break
                continue

            entity_payload = resolve_entity_before_action(
                interpreter=interpreter,
                class_defs=class_defs,
                entity_payload=entity_payload,
            )

            resolution_result = entity_payload.get("resolution_result") if isinstance(entity_payload.get("resolution_result"), dict) else {}
            if resolution_result.get("decision") == "ambiguous":
                queued = object_db.enqueue_resolution_ambiguity(
                    connection=connection,
                    entity_payload=entity_payload,
                    resolution_result=resolution_result,
                )
                queued["action"] = "queue_ambiguity"
                queued["action_clause"] = entity_payload.get("resolution_policy_clause")
                queued["resolution"] = resolution_result
                ambiguity_results.append(queued)
                entity_results.append(queued)
                continue

            if action == "ingest" and resolution_result.get("resolved"):
                action = "update"
                entity_payload["operation"] = action

            result, invoked_clause = invoke_solf_entity_action(
                interpreter=interpreter,
                class_defs=class_defs,
                action=action,
                entity_payload=entity_payload,
            )
            fallback_by_action = {
                "ingest": solf_function.db_ingest,
                "update": solf_function.db_update,
                "delete": solf_function.db_delete,
            }
            if not isinstance(result, dict):
                result = fallback_by_action[action](entity_payload)

            # Some SOLF clauses return informational dicts instead of a DB row.
            # For ingest/update we require an object row, so retry via DB fallback.
            if action in {"ingest", "update"} and (not isinstance(result, dict) or "object_id" not in result):
                result = fallback_by_action[action](entity_payload)

            if action == "delete" and isinstance(result, dict) and result.get("deleted") is False:
                row = dict(result)
                row["action"] = action
                row["action_clause"] = invoked_clause
                entity_results.append(row)
                if stop_on_error:
                    break
                continue

            if not isinstance(result, dict) or "object_id" not in result:
                if entity_payload.get("class_name") == "accounting_transaction":
                    LOGGER.warning(
                        "accounting_transaction action failed object=%s action=%s clause=%s result=%s",
                        entity_payload.get("object_name"),
                        action,
                        invoked_clause,
                        result,
                    )
                row = {
                    "object_id": None,
                    "object_name": entity_payload.get("object_name"),
                    "class_name": entity_payload.get("class_name"),
                    "action": "action_failed",
                    "action_clause": invoked_clause,
                    "validation": validation_result,
                    "resolution": resolution_result,
                    "error": (
                        f"SOLF {action} did not return an object row for entity "
                        f"{entity['name']} ({entity['class_name']}). Invoked clause: {invoked_clause or 'none'}"
                    ),
                }
                entity_results.append(row)
                if stop_on_error:
                    break
                continue

            row = dict(result)
            row["entity_id"] = entity.get("entity_id")
            row["entity_payload"] = dict(entity_payload)
            row["action"] = action
            row["action_clause"] = invoked_clause
            row["validation"] = validation_result
            row["resolution"] = resolution_result
            id_to_db_object[entity["entity_id"]] = row
            entity_results.append(row)
    finally:
        solf_function.clear_connection()

    for row in entity_results:
        if not isinstance(row, dict):
            continue
        if int(row.get("object_id") or 0) <= 0:
            continue
        entity_payload = row.get("entity_payload") if isinstance(row.get("entity_payload"), dict) else {}
        if not entity_payload:
            continue
        action = str(row.get("action") or "").strip().lower()
        try:
            sync_result = _sync_domain_record_for_entity(entity_payload, row, action)
        except Exception as exc:
            sync_result = {
                "attempted": True,
                "class_name": str(entity_payload.get("class_name") or "").strip().lower(),
                "ok": False,
                "reason": str(exc),
            }
        if bool(sync_result.get("attempted")):
            domain_sync_results.append(sync_result)
            if stop_on_error and not bool(sync_result.get("ok", True)):
                break

    for rel in ingested["solf_relationships"]:
        src = id_to_db_object.get(rel["source_entity_id"])
        tar = id_to_db_object.get(rel["target_entity_id"])
        if not src or not tar:
            continue

        rel_meta = dict(rel.get("attributes") or {})
        rel_meta["doc_id"] = doc_id
        rel_meta["operation"] = rel.get("operation")

        rel_operation = str(rel.get("operation") or "upsert").lower()
        if rel_operation == "delete":
            row = object_db.deactivate_object_relationship(
                connection=connection,
                relationship_name=rel["relationship_type"],
                relationship_cat=rel["relationship_cat"],
                src_object_id=int(src["object_id"]),
                tar_object_id=int(tar["object_id"]),
                effective_from=rel.get("effective_from") or ingested.get("document_effective_date"),
                metadata={"doc_id": doc_id, "deactivation_reason": "document_delete_operation"},
                commit=True,
            )
        else:
            row = object_db.upsert_temporal_relationship(
                connection=connection,
                relationship_name=rel["relationship_type"],
                relationship_cat=rel["relationship_cat"],
                src_object_id=int(src["object_id"]),
                tar_object_id=int(tar["object_id"]),
                confidence=rel.get("confidence"),
                metadata=rel_meta,
                temporal_attributes=dict(rel.get("attributes") or {}),
                effective_from=rel.get("effective_from") or ingested.get("document_effective_date"),
                effective_until=rel.get("effective_until"),
                exclusivity_scope=rel.get("exclusivity_scope"),
            )

        row["operation"] = rel_operation
        relationship_results.append(row)

    part_summary = _upsert_document_parts(
        connection=connection,
        doc_id=int(doc_id),
        entity_results=entity_results,
        relationship_results=relationship_results,
    )

    semantic_terms: list[dict[str, Any]] = []

    for class_name, class_def in class_defs.items():
        canonical_class = str(class_name or "").strip().lower()
        if canonical_class:
            semantic_terms.append(
                {
                    "kind": "relationship",
                    "canonical_name": canonical_class,
                    "term_text": canonical_class,
                    "language": "und",
                    "source_type": "solf_clause",
                    "metadata": {"doc_id": int(doc_id), "source": "solf_class"},
                }
            )

        for attr_name in sorted(class_def.allowed_attributes):
            canonical_attr = str(attr_name or "").strip().lower()
            if not canonical_attr:
                continue
            semantic_terms.append(
                {
                    "kind": "attribute",
                    "canonical_name": canonical_attr,
                    "term_text": canonical_attr,
                    "language": "und",
                    "source_type": "solf_clause",
                    "metadata": {"doc_id": int(doc_id), "class_name": canonical_class, "source": "solf_allowed_attributes"},
                }
            )

    for entity in ingested.get("solf_entities") or []:
        attrs = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
        class_name = str(entity.get("class_name") or "").strip().lower()
        for attr_name in attrs.keys():
            canonical_attr = str(attr_name or "").strip().lower()
            if not canonical_attr:
                continue
            semantic_terms.append(
                {
                    "kind": "attribute",
                    "canonical_name": canonical_attr,
                    "term_text": canonical_attr,
                    "language": "und",
                    "source_type": "ingest",
                    "metadata": {"doc_id": int(doc_id), "class_name": class_name, "source": "solf_entity_attributes"},
                }
            )

    for rel in ingested.get("solf_relationships") or []:
        rel_name = str(rel.get("relationship_type") or "").strip().lower()
        if not rel_name:
            continue
        semantic_terms.append(
            {
                "kind": "relationship",
                "canonical_name": rel_name,
                "term_text": rel_name,
                "language": "und",
                "source_type": "ingest",
                "metadata": {"doc_id": int(doc_id), "source": "solf_relationships"},
            }
        )

    alias_terms = _expand_semantic_terms_with_llm_aliases(
        ingested=ingested,
        semantic_terms=semantic_terms,
        doc_id=int(doc_id),
    )
    alias_candidates_written = 0
    if alias_terms:
        alias_candidates_written = object_db.upsert_query_alias_candidates(
            connection,
            [
                {
                    "alias_text": item.get("term_text"),
                    "canonical_name": item.get("canonical_name"),
                    "kind": item.get("kind"),
                    "language": item.get("language"),
                    "confidence": (item.get("metadata") or {}).get("confidence", 0.5) if isinstance(item.get("metadata"), dict) else 0.5,
                    "doc_id": int(doc_id),
                    "metadata": {"source": "ingest_llm_alias_expansion"},
                }
                for item in alias_terms
                if isinstance(item, dict)
            ],
        )

    semantic_terms = _annotate_semantic_terms_with_importance(
        ingested=ingested,
        semantic_terms=semantic_terms,
        doc_id=int(doc_id),
    )

    term_role_counts: dict[str, int] = {
        "primary_identifier": 0,
        "primary_attribute": 0,
        "relationship_anchor": 0,
        "supporting_context": 0,
        "noise": 0,
    }
    for row in semantic_terms:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        role = _normalize_term_text(metadata.get("term_role"))
        if role in term_role_counts:
            term_role_counts[role] += 1

    semantic_terms_upserted = 0
    try:
        semantic_terms_upserted = int(object_db.upsert_semantic_terms(connection, semantic_terms)) if semantic_terms else 0
    except Exception as exc:
        LOGGER.warning("Failed to upsert semantic terms for doc_id=%s: %s", doc_id, exc)
        try:
            connection.rollback()
        except Exception:
            pass
        semantic_terms_upserted = 0

    return {
        "doc_id": doc_id,
        "objects_upserted": len([row for row in entity_results if row.get("object_id") is not None]),
        "ambiguities_queued": len(ambiguity_results),
        "relationships_upserted": len(relationship_results),
        "parts_linked": int(part_summary.get("parts_linked") or 0),
        "object_parts_linked": int(part_summary.get("object_parts_linked") or 0),
        "relationship_parts_linked": int(part_summary.get("relationship_parts_linked") or 0),
        "class_parts_linked": int(part_summary.get("class_parts_linked") or 0),
        "semantic_terms_upserted": semantic_terms_upserted,
        "semantic_alias_terms_generated": len(alias_terms),
        "semantic_alias_candidates_written": alias_candidates_written,
        "semantic_alias_terms": [
            {
                "kind": _normalize_term_text(item.get("kind")),
                "canonical_name": _normalize_term_text(item.get("canonical_name")),
                "term_text": _normalize_term_text(item.get("term_text")),
                "language": _normalize_term_text(item.get("language")) or "und",
            }
            for item in alias_terms[:400]
            if isinstance(item, dict)
        ],
        "semantic_term_role_counts": term_role_counts,
        "workflow_processes": workflow_runtime.get("processes") or [],
        "workflow_hooks": workflow_runtime.get("hooks") or [],
        "workflow_stop_on_error": bool(workflow_runtime.get("stop_on_error", False)),
        "domain_sync_results": domain_sync_results,
        "domain_sync_attempted": len(domain_sync_results),
        "domain_sync_failed": len([row for row in domain_sync_results if not bool(row.get("ok", True))]),
        "entity_rows": [
            {
                "entity_id": str(row.get("entity_id") or "").strip() or None,
                "object_id": int(row.get("object_id") or 0) or None,
                "object_name": str(row.get("object_name") or "").strip() or None,
                "class_name": str(row.get("class_name") or "").strip().lower() or None,
                "action": str(row.get("action") or "").strip().lower() or None,
                "resolution_decision": (
                    str((row.get("resolution") or {}).get("decision") or "").strip().lower()
                    if isinstance(row.get("resolution"), dict)
                    else None
                ),
            }
            for row in entity_results
            if isinstance(row, dict)
        ],
    }


def run_ingest(
    source_path_or_uri: str,
    output_json_path: str | None = None,
    user_context: dict[str, Any] | None = None,
    enforce_required_actions: bool | None = None,
    required_action_policy_text: str | None = None,
    reuse_markdown: bool | None = None,
    persist_markdown: bool | None = None,
    skip_markdown_generation: bool = False,
    prefer_markdown_input: bool = False,
    workflow_processes: list[str] | None = None,
    metadata_only: bool = False,
) -> dict[str, Any]:
    _ensure_google_credentials_env_path()

    configure_logging()
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    LOGGER.info("ingest_run_started run_id=%s source=%s", run_id, source_path_or_uri)

    workflow_trace: list[dict[str, Any]] = []

    def _record_step(step: str, status: str = "ok", elapsed_ms: float | None = None, **details: Any) -> None:
        entry = {
            "step": step,
            "status": status,
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "run_id": run_id,
        }
        if ENABLE_TIMING and elapsed_ms is not None and elapsed_ms > 0:
            entry["elapsed_ms"] = round(elapsed_ms, 2)
        entry.update(details)
        workflow_trace.append(entry)
        LOGGER.info("workflow_step step=%s status=%s details=%s", step, status, details)

    from solf_program import read_program

    solf_script = read_program()
    class_defs = parse_solf_classes(solf_script)
    interpreter = build_solf_interpreter(solf_script)
    _record_step("load_solf", class_count=len(class_defs))

    client = get_openrouter_client()
    if client is None:
        raise ValueError("OPENROUTER_API_KEY is required in environment")
    storage_client = storage.Client(project=PROJECT_ID) if storage is not None and PROJECT_ID else None
    normalized_user_context = _normalize_user_context(user_context)
    metadata_context = normalized_user_context.get("metadata") if isinstance(normalized_user_context.get("metadata"), dict) else {}
    metadata_context = dict(metadata_context)
    if workflow_processes and isinstance(workflow_processes, list):
        existing_processes = metadata_context.get("workflow_processes")
        if isinstance(existing_processes, list):
            merged_processes = list(existing_processes)
            for proc in workflow_processes:
                if str(proc).strip().lower() not in {p.lower() for p in merged_processes}:
                    merged_processes.append(proc)
            metadata_context["workflow_processes"] = merged_processes
        else:
            metadata_context["workflow_processes"] = [str(p).strip() for p in workflow_processes if str(p).strip()]
    note_directives = _derive_note_directives(normalized_user_context)
    note_metadata_updates = note_directives.get("metadata_updates") if isinstance(note_directives.get("metadata_updates"), dict) else {}
    if note_metadata_updates:
        metadata_context.update(note_metadata_updates)
    note_workflow_processes = note_directives.get("workflow_processes") if isinstance(note_directives.get("workflow_processes"), list) else []
    if note_workflow_processes:
        existing_processes = metadata_context.get("workflow_processes") if isinstance(metadata_context.get("workflow_processes"), list) else []
        merged_workflow_processes = [str(item).strip() for item in existing_processes if str(item).strip()]
        for process_name in note_workflow_processes:
            normalized_process = str(process_name).strip()
            if normalized_process and normalized_process.lower() not in {item.lower() for item in merged_workflow_processes}:
                merged_workflow_processes.append(normalized_process)
        metadata_context["workflow_processes"] = merged_workflow_processes
    note_additional_rules = [
        _normalize_rule_name_token(name)
        for name in (note_directives.get("additional_rule_names") or [])
        if _normalize_rule_name_token(name)
    ]
    existing_rule_names = metadata_context.get("processing_rule_names")
    if isinstance(existing_rule_names, list):
        merged_rule_names = [str(item).strip() for item in existing_rule_names if str(item).strip()]
    elif isinstance(existing_rule_names, str):
        merged_rule_names = [str(item).strip() for item in re.split(r"[,;|]", existing_rule_names) if str(item).strip()]
    else:
        merged_rule_names = []
    for rule_name in note_additional_rules:
        if rule_name.lower() not in {item.lower() for item in merged_rule_names}:
            merged_rule_names.append(rule_name)
    if merged_rule_names:
        metadata_context["processing_rule_names"] = merged_rule_names

    markdown_rule_override = str(metadata_context.get("ingestion_markdown_rule") or "").strip().lower() or None
    markdown_max_runs_raw = metadata_context.get("ingestion_markdown_max_runs")
    markdown_default_max_runs = int(str(os.getenv("IKOS_MAX_MARKDOWN_GENERATION_RUNS_PER_INGEST", "2")).strip() or "2")
    try:
        markdown_max_runs = int(markdown_max_runs_raw) if markdown_max_runs_raw is not None else markdown_default_max_runs
    except (TypeError, ValueError):
        markdown_max_runs = markdown_default_max_runs
    markdown_max_runs = max(1, markdown_max_runs)
    markdown_runs_used = 0

    def _consume_markdown_generation_budget(context: str) -> None:
        nonlocal markdown_runs_used
        if markdown_runs_used >= markdown_max_runs:
            raise RuntimeError(
                "markdown generation run limit exceeded for this ingestion "
                f"(limit={markdown_max_runs}, context={context})"
            )
        markdown_runs_used += 1
        _record_step(
            "markdown_generation_budget",
            limit=markdown_max_runs,
            used=markdown_runs_used,
            context=context,
        )

    canonical_source_path = str(metadata_context.get("client_file_path") or "").strip()
    reprocess_doc_id_raw = metadata_context.get("reprocess_doc_id")
    try:
        reprocess_doc_id = int(reprocess_doc_id_raw) if reprocess_doc_id_raw is not None else 0
    except (TypeError, ValueError):
        reprocess_doc_id = 0
    allow_existing_doc_markdown_reuse = bool(metadata_context.get("allow_existing_doc_markdown_reuse")) and reprocess_doc_id > 0
    if canonical_source_path and not str(metadata_context.get("stable_source_identity") or "").strip():
        metadata_context["stable_source_identity"] = canonical_source_path
    normalized_user_context["metadata"] = metadata_context

    existing_markdown_path = None
    if allow_existing_doc_markdown_reuse:
        existing_markdown_path = _find_existing_markdown_path_by_doc_path(canonical_source_path)
    if existing_markdown_path and Path(existing_markdown_path).exists():
        prefer_markdown_input = True
        LOGGER.info(
            "existing markdown found for source path during doc_id reprocess; forcing markdown reprocess run_id=%s source=%s markdown=%s doc_id=%s",
            run_id,
            canonical_source_path,
            existing_markdown_path,
            reprocess_doc_id,
        )
    requested_processing_rule_names = _extract_processing_rule_names(normalized_user_context)
    rule_resolution = _resolve_active_processing_rules(requested_processing_rule_names)
    selected_processing_rules = rule_resolution.get("matched_rules") if isinstance(rule_resolution.get("matched_rules"), list) else []
    processing_directives = _derive_processing_directives(selected_processing_rules)

    if requested_processing_rule_names:
        metadata_context = normalized_user_context.get("metadata") if isinstance(normalized_user_context.get("metadata"), dict) else {}
        metadata_context = dict(metadata_context)
        metadata_context["processing_rule_names"] = requested_processing_rule_names
        metadata_context["processing_rule_ids"] = [int(rule.get("rule_id") or 0) for rule in selected_processing_rules]
        metadata_context["processing_rule_missing_names"] = [str(name) for name in (rule_resolution.get("missing_names") or [])]
        metadata_context["processing_directives"] = processing_directives
        normalized_user_context["metadata"] = metadata_context

    LOGGER.info(
        "ingest_user_input_trace run_id=%s payload=%s",
        run_id,
        json.dumps(
            _build_user_input_trace_payload(source_path_or_uri, normalized_user_context),
            ensure_ascii=False,
        ),
    )
    _record_step(
        "resolve_processing_rules",
        requested_count=len(requested_processing_rule_names),
        matched_count=len(selected_processing_rules),
        missing_names=rule_resolution.get("missing_names") or [],
    )

    if requested_processing_rule_names and not selected_processing_rules:
        missing = [str(name) for name in (rule_resolution.get("missing_names") or requested_processing_rule_names)]
        raise RuntimeError(
            "Requested processing rules could not be resolved: " + ", ".join(missing)
        )

    gcs_uri = upload_if_local(storage_client, source_path_or_uri, INGEST_BUCKET)
    _record_step("upload_or_resolve_source", source=source_path_or_uri, gcs_uri=gcs_uri)

    router_prompt = build_router_prompt(class_defs, normalized_user_context)
    routed = generate_json(
        client,
        ROUTER_MODEL,
        source_path_or_uri,
        gcs_uri,
        router_prompt,
        call_name="router",
        run_id=run_id,
        complexity="simple",
    )
    _record_step("genai_router", model=ROUTER_MODEL, document_type=routed.get("document_type"))

    routed_language = _normalize_language_tag(routed.get("language"))
    original_user_description = normalized_user_context.get("description") or ""
    if original_user_description and routed_language:
        localized_description = localize_user_description(
            client=client,
            description=original_user_description,
            target_language=routed_language,
            run_id=run_id,
        )
        normalized_user_context["description"] = localized_description
        metadata_context = normalized_user_context.get("metadata") if isinstance(normalized_user_context.get("metadata"), dict) else {}
        metadata_context = dict(metadata_context)
        metadata_context["user_description_original"] = original_user_description
        metadata_context["user_description_language"] = routed_language
        normalized_user_context["metadata"] = metadata_context
        _record_step("genai_localize_description", target_language=routed_language)

    extraction_prompt = build_extraction_prompt(
        class_defs,
        routed,
        normalized_user_context,
        selected_processing_rules=selected_processing_rules,
    )

    source_ext = _source_extension(source_path_or_uri)
    markdown_first_required = source_ext == ".pdf" or source_ext in IMAGE_EXTENSIONS

    # For PDF/image sources, enforce markdown-first extraction so entity extraction
    # always consumes text/OCR output rather than raw binary placeholders.
    if markdown_first_required and not prefer_markdown_input:
        prefer_markdown_input = True
        LOGGER.info(
            "auto prefer_markdown_input enabled for binary source run_id=%s source=%s ext=%s",
            run_id,
            source_path_or_uri,
            source_ext,
        )

    # When prefer_markdown_input is requested, use the cached .md file as extraction source.
    # Markdown tables have explicit header/row structure which gives the model better
    # column-to-attribute alignment than reading the raw PDF via GCS Part.
    extraction_source = source_path_or_uri
    extraction_source_generated_markdown = False
    preextract_generated_markdown_text: str | None = None
    preextract_generated_markdown_path: Path | None = None
    if prefer_markdown_input:
        existing_markdown_file = Path(existing_markdown_path) if existing_markdown_path else None
        if existing_markdown_file and existing_markdown_file.exists():
            extraction_source = str(existing_markdown_file)
            LOGGER.info(
                "prefer_markdown_input: using existing persisted markdown for extraction run_id=%s path=%s",
                run_id,
                existing_markdown_file,
            )
            _record_step("extraction_source", mode="markdown_existing", path=str(existing_markdown_file))
        else:
            _likely_md_path = _markdown_cache_path(
                source_path_or_uri,
                gcs_uri,
                {"document": {"doc_type": routed.get("document_type", "")}},
            )
            if _likely_md_path.exists():
                extraction_source = str(_likely_md_path)
                LOGGER.info(
                    "prefer_markdown_input: using cached markdown for extraction run_id=%s path=%s",
                    run_id,
                    _likely_md_path,
                )
                _record_step("extraction_source", mode="markdown", path=str(_likely_md_path))
            else:
                # Build markdown now (best-effort) so extraction can use OCR text in the same run.
                generated_md_path = None
                try:
                    generated_md = ""
                    if _md_gen is not None and Path(source_path_or_uri).exists():
                        _consume_markdown_generation_budget("preextract_generation")
                        generated_md = str(
                            _md_gen.file_to_markdown_openrouter(
                                str(source_path_or_uri),
                                parser_rule_override=markdown_rule_override,
                            )
                            or ""
                        ).strip()
                    if generated_md:
                        with tempfile.NamedTemporaryFile(
                            mode="w",
                            suffix=".md",
                            prefix="ikos-preextract-",
                            encoding="utf-8",
                            delete=False,
                        ) as temp_markdown:
                            temp_markdown.write(generated_md)
                            generated_md_path = Path(temp_markdown.name)
                        extraction_source = str(generated_md_path)
                        extraction_source_generated_markdown = True
                        preextract_generated_markdown_text = generated_md
                        preextract_generated_markdown_path = generated_md_path
                        LOGGER.info(
                            "prefer_markdown_input: generated markdown for extraction run_id=%s path=%s",
                            run_id,
                            generated_md_path,
                        )
                        _record_step("extraction_source", mode="markdown_generated", path=str(generated_md_path))
                    else:
                        if markdown_first_required:
                            raise RuntimeError(
                                "markdown-first extraction is required for PDF/image sources, "
                                "but markdown generation returned empty content"
                            )
                        LOGGER.info(
                            "prefer_markdown_input: markdown generation returned empty, falling back to original source run_id=%s",
                            run_id,
                        )
                        _record_step("extraction_source", mode="original", reason="markdown_generation_empty")
                except Exception as exc:
                    if markdown_first_required:
                        raise RuntimeError(
                            "markdown-first extraction is required for PDF/image sources, "
                            f"but markdown generation failed: {exc}"
                        ) from exc
                    LOGGER.warning(
                        "prefer_markdown_input: markdown generation failed, falling back to original source run_id=%s error=%s",
                        run_id,
                        exc,
                    )
                    _record_step("extraction_source", mode="original", reason="markdown_generation_failed")

    try:
        extracted = generate_json(
            client,
            EXTRACT_MODEL,
            extraction_source,
            gcs_uri,
            extraction_prompt,
            call_name="extract",
            run_id=run_id,
        )
    finally:
        if preextract_generated_markdown_path is not None:
            try:
                preextract_generated_markdown_path.unlink(missing_ok=True)
            except OSError:
                LOGGER.warning("Could not remove temporary pre-extraction Markdown file: %s", preextract_generated_markdown_path)
    extracted = _apply_note_directives_to_extracted(extracted, note_directives)
    _record_step(
        "extract_input_source",
        mode="markdown" if str(extraction_source).lower().endswith(".md") else "original",
        path=str(extraction_source),
    )
    _record_step("genai_extract", model=EXTRACT_MODEL, entity_count=len(extracted.get("entities") or []))

    extracted_document = extracted.get("document") if isinstance(extracted.get("document"), dict) else {}
    extracted_metadata = extracted_document.get("metadata") if isinstance(extracted_document.get("metadata"), dict) else {}
    if selected_processing_rules:
        extracted_metadata = dict(extracted_metadata)
        extracted_metadata["processing_rule_names"] = [str(item.get("rule_name") or "") for item in selected_processing_rules if str(item.get("rule_name") or "")]
        extracted_metadata["processing_rule_ids"] = [int(item.get("rule_id") or 0) for item in selected_processing_rules if int(item.get("rule_id") or 0) > 0]
        extracted_document["metadata"] = extracted_metadata
        extracted["document"] = extracted_document

    extracted, processing_directive_stats = _apply_processing_directives(extracted, processing_directives)
    if processing_directives.get("summary_only"):
        _record_step(
            "apply_processing_directives",
            directives=processing_directives,
            removed_detail_item_fields=processing_directive_stats.get("removed_detail_item_fields", 0),
        )

    runtime_rule_trigger = evaluate_runtime_rule_triggers(
        routed=routed,
        extracted=extracted,
        metadata_context=metadata_context,
    )
    triggered_processes = runtime_rule_trigger.get("triggered_processes") if isinstance(runtime_rule_trigger.get("triggered_processes"), list) else []
    if triggered_processes:
        existing_processes = metadata_context.get("workflow_processes") if isinstance(metadata_context.get("workflow_processes"), list) else []
        merged_workflow_processes = [str(item).strip() for item in existing_processes if str(item).strip()]
        for process_name in triggered_processes:
            normalized_process = str(process_name).strip()
            if normalized_process and normalized_process.lower() not in {item.lower() for item in merged_workflow_processes}:
                merged_workflow_processes.append(normalized_process)
        metadata_context["workflow_processes"] = merged_workflow_processes
        normalized_user_context["metadata"] = metadata_context

    _record_step(
        "evaluate_runtime_rule_triggers",
        matched_rules=len(runtime_rule_trigger.get("matched_rules") or []),
        triggered_processes=triggered_processes,
        rule_evaluation_error=bool(runtime_rule_trigger.get("rule_evaluation_error")),
    )

    post_extraction_rule_application = business_rules.apply_post_extraction_business_rules(
        extracted,
        context={
            "country": metadata_context.get("country") or extracted_document.get("country"),
            "document_type": (extracted.get("document") or {}).get("doc_type") if isinstance(extracted.get("document"), dict) else routed.get("document_type"),
            "operation": "ingest",
        },
        selected_rules=selected_processing_rules,
        include_inferred=True,
    )

    def _build_rule_effect_snapshot(rule_application: dict[str, Any]) -> list[dict[str, Any]]:
        snapshot: list[dict[str, Any]] = []
        applied_rules = rule_application.get("applied_rules") if isinstance(rule_application.get("applied_rules"), list) else []
        extracted_payload = rule_application.get("extracted") if isinstance(rule_application.get("extracted"), dict) else {}
        extracted_doc = extracted_payload.get("document") if isinstance(extracted_payload.get("document"), dict) else {}
        extracted_entities = extracted_payload.get("entities") if isinstance(extracted_payload.get("entities"), list) else []
        for app in applied_rules:
            if not isinstance(app, dict):
                continue
            target_scope = str(app.get("target_scope") or "entity").strip().lower() or "entity"
            entity_name = str(app.get("entity_name") or "").strip()
            entity_class = str(app.get("entity_class") or "").strip().lower()
            changed_keys = app.get("changed_keys") if isinstance(app.get("changed_keys"), list) else []
            if target_scope == "document":
                source_attrs = extracted_doc
            else:
                target_entity = next(
                    (
                        entity for entity in extracted_entities
                        if isinstance(entity, dict)
                        and str(entity.get("entity_name") or entity.get("name") or "").strip() == entity_name
                    ),
                    None,
                )
                source_attrs = target_entity.get("attributes") if isinstance((target_entity or {}).get("attributes"), dict) else {}
            for changed_key in changed_keys:
                raw_key = str(changed_key or "").strip()
                key = raw_key
                if key.startswith("document."):
                    key = key[9:]
                if key.startswith("attributes."):
                    key = key[11:]
                if not key:
                    continue
                snapshot.append(
                    {
                        "rule_name": str(app.get("rule_name") or ""),
                        "target_scope": target_scope,
                        "entity_name": entity_name,
                        "entity_class": entity_class,
                        "attribute": key,
                        "value": (source_attrs or {}).get(key),
                    }
                )
        return snapshot

    def _validate_rule_effect_snapshot(snapshot: list[dict[str, Any]], payload_after: dict[str, Any]) -> list[dict[str, Any]]:
        violations: list[dict[str, Any]] = []
        document_after = payload_after.get("document") if isinstance(payload_after.get("document"), dict) else {}
        entities_after = payload_after.get("entities") if isinstance(payload_after.get("entities"), list) else []
        for item in snapshot:
            if not isinstance(item, dict):
                continue
            target_scope = str(item.get("target_scope") or "entity").strip().lower() or "entity"
            key = str(item.get("attribute") or "").strip()
            expected = item.get("value")
            if not key:
                continue
            if target_scope == "document":
                actual = document_after.get(key)
                if actual != expected:
                    violations.append(
                        {
                            "type": "rule_effect_not_persisted",
                            "rule_name": str(item.get("rule_name") or ""),
                            "target_scope": "document",
                            "attribute": key,
                            "expected": expected,
                            "actual": actual,
                        }
                    )
                continue

            entity_name = str(item.get("entity_name") or "").strip()
            entity_class = str(item.get("entity_class") or "").strip().lower()
            target_entity = next(
                (
                    entity for entity in entities_after
                    if isinstance(entity, dict)
                    and str(entity.get("entity_name") or entity.get("name") or "").strip() == entity_name
                ),
                None,
            )
            if target_entity is None and entity_class:
                target_entity = next(
                    (
                        entity for entity in entities_after
                        if isinstance(entity, dict)
                        and str(entity.get("class_name") or entity.get("entity_type") or "").strip().lower() == entity_class
                        and isinstance(entity.get("attributes"), dict)
                        and key in (entity.get("attributes") or {})
                    ),
                    None,
                )
            attrs = target_entity.get("attributes") if isinstance((target_entity or {}).get("attributes"), dict) else {}
            actual = attrs.get(key)
            if actual != expected:
                violations.append(
                    {
                        "type": "rule_effect_not_persisted",
                        "rule_name": str(item.get("rule_name") or ""),
                        "target_scope": "entity",
                        "entity_name": entity_name,
                        "entity_class": entity_class,
                        "attribute": key,
                        "expected": expected,
                        "actual": actual,
                    }
                )
        return violations

    def _collect_rule_reference_defaults(snapshot: list[dict[str, Any]]) -> dict[str, Any]:
        defaults: dict[str, Any] = {}
        for item in snapshot:
            if not isinstance(item, dict):
                continue
            key = str(item.get("attribute") or "").strip()
            value = item.get("value")
            text = str(value or "").strip()
            if not key.endswith("_ref"):
                continue
            if not text or re.fullmatch(r"e\d+", text.lower()):
                continue
            defaults.setdefault(key, value)
        return defaults

    def _collect_selected_rule_reference_defaults(selected_rules: list[dict[str, Any]]) -> dict[str, Any]:
        defaults: dict[str, Any] = {}
        for rule in selected_rules:
            if not isinstance(rule, dict):
                continue
            structured = rule.get("structured_rule") if isinstance(rule.get("structured_rule"), dict) else {}
            directives = structured.get("post_extraction_directives") if isinstance(structured.get("post_extraction_directives"), list) else []
            for directive in directives:
                if not isinstance(directive, dict):
                    continue
                conditions = directive.get("conditions") if isinstance(directive.get("conditions"), list) else []
                if conditions:
                    continue
                set_attrs = directive.get("set_attributes") if isinstance(directive.get("set_attributes"), dict) else {}
                for key, value in set_attrs.items():
                    attr_key = str(key or "").strip()
                    if not attr_key.endswith("_ref"):
                        continue
                    text = str(value or "").strip()
                    if not text or re.fullmatch(r"e\d+", text.lower()):
                        continue
                    defaults.setdefault(attr_key, value)
        return defaults

    def _propagate_rule_reference_defaults(payload_after: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
        if not defaults:
            return payload_after
        payload_out = dict(payload_after or {})
        document_out = payload_out.get("document") if isinstance(payload_out.get("document"), dict) else {}
        entities_out = payload_out.get("entities") if isinstance(payload_out.get("entities"), list) else []
        for key, value in defaults.items():
            document_out[key] = value
        payload_out["document"] = document_out
        patched_entities: list[Any] = []
        for entity in entities_out:
            if not isinstance(entity, dict):
                patched_entities.append(entity)
                continue
            item = dict(entity)
            attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
            attrs = dict(attrs)
            for key, value in defaults.items():
                attrs[key] = value
            item["attributes"] = attrs
            patched_entities.append(item)
        payload_out["entities"] = patched_entities
        return payload_out

    rule_effect_snapshot = _build_rule_effect_snapshot(post_extraction_rule_application)
    extracted = post_extraction_rule_application.get("extracted") if isinstance(post_extraction_rule_application.get("extracted"), dict) else extracted
    rule_integrity = post_extraction_rule_application.get("rule_integrity") if isinstance(post_extraction_rule_application.get("rule_integrity"), dict) else {}
    integrity_violations = rule_integrity.get("violations") if isinstance(rule_integrity.get("violations"), list) else []
    _record_step(
        "apply_post_extraction_business_rules",
        applied_count=int(post_extraction_rule_application.get("applied_count") or 0),
        applied_rule_names=[str(item.get("rule_name") or "") for item in (post_extraction_rule_application.get("applied_rules") or [])],
        rule_integrity_violation_count=len(integrity_violations),
        inference_limits={
            "max_rules": getattr(business_rules, "INFERENCE_MAX_RULES", None),
            "max_directives_per_rule": getattr(business_rules, "INFERENCE_MAX_DIRECTIVES_PER_RULE", None),
            "max_workflow_process_additions": getattr(business_rules, "INFERENCE_MAX_WORKFLOW_PROCESS_ADDITIONS", None),
        },
    )

    if selected_processing_rules and integrity_violations:
        blocking_violation_types = {"unresolved_reference_token", "selected_rule_not_applied"}
        blocking_integrity_violations = [
            item
            for item in integrity_violations
            if isinstance(item, dict)
            and str(item.get("type") or "").strip().lower() in blocking_violation_types
        ]

        if blocking_integrity_violations:
            violation_preview = "; ".join(
                str(item.get("type") or "unknown")
                + ":"
                + str(item.get("rule_name") or item.get("rule_id") or "")
                for item in blocking_integrity_violations[:5]
                if isinstance(item, dict)
            )
            raise RuntimeError(
                "Selected processing rules failed integrity checks: "
                + (violation_preview or "unknown violation")
            )

        _record_step(
            "selected_rule_integrity_non_blocking",
            violation_count=len(integrity_violations),
            violation_types=sorted(
                {
                    str(item.get("type") or "unknown").strip().lower()
                    for item in integrity_violations
                    if isinstance(item, dict)
                }
            ),
        )

    extracted_document = extracted.get("document") if isinstance(extracted.get("document"), dict) else {}
    extracted_metadata = extracted_document.get("metadata") if isinstance(extracted_document.get("metadata"), dict) else {}
    extracted_metadata = dict(extracted_metadata)
    if normalized_user_context["metadata"]:
        existing_user_metadata = extracted_metadata.get("user_metadata") if isinstance(extracted_metadata.get("user_metadata"), dict) else {}
        merged_user_metadata = dict(existing_user_metadata)
        merged_user_metadata.update(normalized_user_context["metadata"])
        if existing_markdown_path:
            merged_source_reference = merged_user_metadata.get("source_reference") if isinstance(merged_user_metadata.get("source_reference"), dict) else {}
            merged_source_reference = dict(merged_source_reference)
            merged_source_reference.setdefault("markdown_cache_path", str(existing_markdown_path))
            merged_user_metadata["source_reference"] = merged_source_reference
        extracted_metadata["user_metadata"] = merged_user_metadata
        extracted_document["metadata"] = extracted_metadata
        extracted["document"] = extracted_document

    extracted = apply_domain_action_policy(routed, extracted, class_defs)
    rule_reference_defaults = _collect_rule_reference_defaults(rule_effect_snapshot)
    selected_rule_reference_defaults = _collect_selected_rule_reference_defaults(selected_processing_rules or [])
    for key, value in selected_rule_reference_defaults.items():
        rule_reference_defaults.setdefault(key, value)

    if selected_rule_reference_defaults:
        _record_step(
            "selected_rule_reference_defaults",
            keys=sorted(selected_rule_reference_defaults.keys()),
            count=len(selected_rule_reference_defaults),
        )

    extracted = _propagate_rule_reference_defaults(extracted, rule_reference_defaults)
    persistence_violations = _validate_rule_effect_snapshot(rule_effect_snapshot, extracted)
    if persistence_violations:
        doc_meta = extracted.get("document") if isinstance(extracted.get("document"), dict) else {}
        doc_meta_obj = doc_meta.get("metadata") if isinstance(doc_meta.get("metadata"), dict) else {}
        doc_meta_obj = dict(doc_meta_obj)
        doc_meta_obj["rule_effect_persistence_violations"] = persistence_violations
        doc_meta["metadata"] = doc_meta_obj
        extracted["document"] = doc_meta
        _record_step("validate_rule_effect_persistence", violation_count=len(persistence_violations))
        if selected_processing_rules:
            violation_preview = "; ".join(
                str(item.get("rule_name") or "") + ":" + str(item.get("attribute") or "")
                for item in persistence_violations[:5]
                if isinstance(item, dict)
            )
            raise RuntimeError(
                "Selected processing rules failed persistence checks: "
                + (violation_preview or "unknown persistence violation")
            )
    else:
        _record_step("validate_rule_effect_persistence", violation_count=0)
    _record_step("apply_domain_action_policy")

    enforce_domain_action_policy(
        routed,
        extracted,
        class_defs,
        enforce_required_actions=enforce_required_actions,
        required_action_policy_text=required_action_policy_text,
    )

    ingested = to_solf_objects(extracted, class_defs)
    ingested_document = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
    ingested["document"] = ingested_document
    existing_metadata = ingested_document.get("metadata") if isinstance(ingested_document.get("metadata"), dict) else {}
    merged_metadata = dict(existing_metadata)
    if normalized_user_context["description"]:
        merged_metadata["user_description"] = normalized_user_context["description"]
    if routed_language:
        merged_metadata["user_description_language"] = routed_language
    if normalized_user_context["tags"]:
        merged_metadata["user_tags"] = normalized_user_context["tags"]
    if normalized_user_context["metadata"]:
        merged_metadata["user_metadata"] = normalized_user_context["metadata"]
    ingested_document["metadata"] = merged_metadata

    if normalized_user_context["tags"]:
        existing_keywords = ingested_document.get("keywords") if isinstance(ingested_document.get("keywords"), list) else []
        merged_keywords = [str(item) for item in existing_keywords if str(item).strip()]
        for tag in normalized_user_context["tags"]:
            if tag not in merged_keywords:
                merged_keywords.append(tag)
        ingested_document["keywords"] = merged_keywords

    note_clause_results = _invoke_note_solf_clauses(
        interpreter=interpreter,
        note_directives=note_directives,
        routed=routed,
        extracted=extracted,
        ingested=ingested,
        run_id=run_id,
    )
    _record_step(
        "invoke_note_solf_clauses",
        requested_clauses=[str(item.get("clause") or "") for item in note_clause_results],
        success_count=len([item for item in note_clause_results if item.get("status") == "ok"]),
        error_count=len([item for item in note_clause_results if item.get("status") == "error"]),
    )

    if reuse_markdown is None:
        reuse_markdown = str(os.getenv("IKOS_REUSE_MARKDOWN", "true")).strip().lower() in {"1", "true", "yes", "on"}
    if persist_markdown is None:
        persist_markdown = str(os.getenv("IKOS_PERSIST_MARKDOWN", "true")).strip().lower() in {"1", "true", "yes", "on"}

    markdown_cache_path = _markdown_cache_path(source_path_or_uri, gcs_uri, ingested)
    markdown_text = ""
    markdown_reused = False
    direct_markdown_text = None
    if not skip_markdown_generation:
        direct_markdown_text = get_direct_markdown_text(source_path_or_uri)

    existing_markdown_file = Path(existing_markdown_path) if existing_markdown_path else None
    if existing_markdown_file and existing_markdown_file.exists():
        markdown_text = existing_markdown_file.read_text(encoding="utf-8")
        markdown_cache_path = existing_markdown_file
        markdown_reused = True
        _record_step("markdown_cache_reuse", cache_path=str(existing_markdown_file), reason="existing_doc_markdown")
    elif direct_markdown_text is not None:
        markdown_text = direct_markdown_text
        markdown_cache_path = None
        _record_step("markdown_generation", status="skipped", reason="auto_skip_plain_text_source")
    elif skip_markdown_generation:
        if reuse_markdown and markdown_cache_path.exists():
            markdown_text = markdown_cache_path.read_text(encoding="utf-8")
            markdown_reused = True
            _record_step("markdown_cache_reuse", cache_path=str(markdown_cache_path), skipped_generation=True)
        else:
            _record_step("markdown_generation", status="skipped", reason="skip_markdown_generation_enabled")
    elif preextract_generated_markdown_text is not None:
        markdown_text = preextract_generated_markdown_text
        markdown_cache_path = None
        markdown_reused = True
        _record_step(
            "markdown_generation",
            status="skipped",
            reason="reuse_preextract_markdown",
            cache_path=str(preextract_generated_markdown_path) if preextract_generated_markdown_path else None,
            canonical_cache_path=None,
        )
    elif str(extraction_source).lower().endswith(".md") and not extraction_source_generated_markdown:
        extraction_md_path = Path(str(extraction_source))
        if extraction_md_path.exists():
            extraction_md_text = extraction_md_path.read_text(encoding="utf-8")
            if str(extraction_md_text or "").strip():
                markdown_text = extraction_md_text
                markdown_cache_path = extraction_md_path
                markdown_reused = True
                _record_step(
                    "markdown_generation",
                    status="skipped",
                    reason="reuse_extraction_markdown",
                    cache_path=str(extraction_md_path),
                    parser_rule_override=markdown_rule_override,
                )
            else:
                markdown_text, markdown_cache_path, markdown_reused = load_or_generate_markdown(
                    client=client,
                    source_path_or_uri=source_path_or_uri,
                    gcs_uri=gcs_uri,
                    extracted=ingested,
                    reuse_markdown=reuse_markdown,
                    persist_markdown=persist_markdown,
                    parser_rule_override=markdown_rule_override,
                    markdown_run_guard=_consume_markdown_generation_budget,
                )
                _record_step(
                    "markdown_generation",
                    cache_path=str(markdown_cache_path) if markdown_cache_path else None,
                    reused=markdown_reused,
                    parser_rule_override=markdown_rule_override,
                )
        else:
            markdown_text, markdown_cache_path, markdown_reused = load_or_generate_markdown(
                client=client,
                source_path_or_uri=source_path_or_uri,
                gcs_uri=gcs_uri,
                extracted=ingested,
                reuse_markdown=reuse_markdown,
                persist_markdown=persist_markdown,
                parser_rule_override=markdown_rule_override,
                markdown_run_guard=_consume_markdown_generation_budget,
            )
            _record_step(
                "markdown_generation",
                cache_path=str(markdown_cache_path) if markdown_cache_path else None,
                reused=markdown_reused,
                parser_rule_override=markdown_rule_override,
            )
    else:
        markdown_text, markdown_cache_path, markdown_reused = load_or_generate_markdown(
            client=client,
            source_path_or_uri=source_path_or_uri,
            gcs_uri=gcs_uri,
            extracted=ingested,
            reuse_markdown=reuse_markdown,
            persist_markdown=persist_markdown,
            parser_rule_override=markdown_rule_override,
            markdown_run_guard=_consume_markdown_generation_budget,
        )
        _record_step(
            "markdown_generation",
            cache_path=str(markdown_cache_path) if markdown_cache_path else None,
            reused=markdown_reused,
            parser_rule_override=markdown_rule_override,
        )

    if not markdown_text.strip() and str(extraction_source).lower().endswith(".md") and not extraction_source_generated_markdown:
        extraction_md_path = Path(str(extraction_source))
        if extraction_md_path.exists():
            fallback_text = extraction_md_path.read_text(encoding="utf-8")
            if str(fallback_text or "").strip():
                markdown_text = fallback_text
                markdown_cache_path = extraction_md_path
                markdown_reused = True
                _record_step(
                    "markdown_generation_fallback",
                    source="extraction_markdown",
                    path=str(extraction_md_path),
                    reason="post_extraction_markdown_empty",
                )

    structured_tables = extract_structured_tables(markdown_text)
    _record_step(
        "extract_structured_tables",
        table_count=len(structured_tables),
        source_kind=(
            "spreadsheet"
            if source_ext in {".xlsx", ".xlsm"}
            else "document"
        ),
        has_tables=bool(structured_tables),
    )

    if structured_tables:
        extracted = _apply_structured_table_enrichment(
            extracted,
            document_type=str(routed.get("document_type") or ""),
            class_defs=class_defs,
        )
        _record_step(
            "apply_structured_table_enrichment",
            table_count=len(structured_tables),
            enriched_entity_count=len(extracted.get("entities") or []),
            enriched_relationship_count=len(extracted.get("relationships") or []),
        )

    spacy_enrichment_summary = {
        "enabled": False,
        "available": False,
        "relationships_detected": 0,
        "relationships_added": 0,
        "entities_added": 0,
        "reason": "spacy_preparser_unavailable",
    }
    if spacy_preparser is not None and markdown_text.strip():
        try:
            spacy_enrichment_summary = spacy_preparser.enrich_ingested_with_spacy_hints(
                ingested,
                markdown_text,
                language_hint=routed_language,
            )
        except Exception as exc:
            LOGGER.warning("spaCy enrichment failed: %s", exc)
            spacy_enrichment_summary = {
                "enabled": False,
                "available": False,
                "relationships_detected": 0,
                "relationships_added": 0,
                "entities_added": 0,
                "reason": "spacy_enrichment_failed",
            }
    elif not markdown_text.strip():
        spacy_enrichment_summary = {
            "enabled": False,
            "available": False,
            "relationships_detected": 0,
            "relationships_added": 0,
            "entities_added": 0,
            "reason": "no_markdown_text",
        }
    _record_step("enrich_spacy_relationship_hints", **spacy_enrichment_summary)

    shareholder_attrs_enriched = 0
    if ENABLE_RELATION_TABLE_ENRICHMENT:
        shareholder_attrs_enriched = enrich_shareholder_relationships_from_markdown(ingested, markdown_text)
        _record_step("enrich_shareholder_relationships", relationships_updated=shareholder_attrs_enriched)
    else:
        _record_step("enrich_shareholder_relationships", status="skipped", reason="feature_disabled")

    person_profile_attrs_enriched = enrich_person_profile_attributes_from_markdown(ingested, markdown_text)
    _record_step("enrich_person_profile_attributes", entities_updated=person_profile_attrs_enriched)

    purpose_attrs_enriched = enrich_company_purpose_from_markdown(ingested, markdown_text)
    _record_step("enrich_company_purpose", entities_updated=purpose_attrs_enriched)

    registration_meta_enriched = enrich_company_registration_metadata_from_markdown(ingested, markdown_text)
    _record_step("enrich_company_registration_metadata", entities_updated=registration_meta_enriched)

    company_eori_enriched = enrich_company_eori_from_markdown(ingested, markdown_text)
    _record_step("enrich_company_eori", entities_updated=company_eori_enriched)

    process_family_enrichment = enrich_process_family_relationships_from_markdown(ingested, markdown_text)
    _record_step("enrich_process_family_relationships", **process_family_enrichment)

    identifier_attrs_pruned = prune_unsupported_identifier_attributes_from_markdown(ingested, markdown_text)
    _record_step("prune_unsupported_identifier_attributes", attributes_removed=identifier_attrs_pruned)

    # Persist source-to-filename mapping and markdown cache reference in document metadata
    # so records can be retrieved by client filename as well as gs:// path.
    metadata_after_markdown = ingested_document.get("metadata") if isinstance(ingested_document.get("metadata"), dict) else {}
    metadata_after_markdown = dict(metadata_after_markdown)
    user_metadata_after_markdown = metadata_after_markdown.get("user_metadata") if isinstance(metadata_after_markdown.get("user_metadata"), dict) else {}
    user_metadata_after_markdown = dict(user_metadata_after_markdown)
    source_reference = user_metadata_after_markdown.get("source_reference") if isinstance(user_metadata_after_markdown.get("source_reference"), dict) else {}
    source_reference = dict(source_reference)
    canonical_source_reference_path = str(user_metadata_after_markdown.get("client_file_path") or "").strip() or source_path_or_uri
    source_reference["source_path_or_uri"] = canonical_source_reference_path
    source_reference["gcs_uri"] = gcs_uri
    stable_source_identity = str(user_metadata_after_markdown.get("stable_source_identity") or "").strip()
    if not stable_source_identity and str(canonical_source_reference_path or "").strip() and not str(canonical_source_reference_path).startswith("gs://"):
        stable_source_identity = str(canonical_source_reference_path).strip()
        user_metadata_after_markdown["stable_source_identity"] = stable_source_identity
    markdown_content_hash = ""
    if markdown_text.strip():
        markdown_content_hash = _normalized_markdown_hash(markdown_text)
        source_reference["markdown_content_hash"] = markdown_content_hash
        if not stable_source_identity:
            doc_type_for_identity = str(ingested_document.get("doc_type") or routed.get("document_type") or "document").strip().lower() or "document"
            stable_source_identity = f"md:{doc_type_for_identity}:{markdown_content_hash}"
            user_metadata_after_markdown["stable_source_identity"] = stable_source_identity
    if stable_source_identity:
        source_reference["stable_source_identity"] = stable_source_identity
    existing_client_file_path = str(user_metadata_after_markdown.get("client_file_path") or "").strip()
    existing_original_local_path = str(source_reference.get("original_local_path") or "").strip()
    source_reference["original_local_path"] = _resolve_original_local_path(
        existing_original_local_path,
        existing_client_file_path,
        source_path_or_uri,
        canonical_source_reference_path,
    )
    source_reference["run_id"] = run_id
    if markdown_cache_path:
        source_reference["markdown_cache_path"] = str(markdown_cache_path)
        source_reference["markdown_reused"] = markdown_reused
    source_reference["structured_table_count"] = len(structured_tables)
    user_metadata_after_markdown["source_reference"] = source_reference
    if structured_tables:
        metadata_after_markdown["structured_tables"] = {
            "table_count": len(structured_tables),
            "source_kind": "spreadsheet" if source_ext in {".xlsx", ".xlsm"} else "document",
            "tables": structured_tables,
        }
    metadata_after_markdown["user_metadata"] = user_metadata_after_markdown
    ingested_document["metadata"] = metadata_after_markdown

    connection = object_db.get_connection()
    try:
        object_db.create_tables(connection, recreate=False)
        ensure_domain_schema(connection)
        ensure_solf_classes_in_db(connection, class_defs)

        lifecycle_policy = str(processing_directives.get("document_lifecycle_policy") or "").strip().lower()
        document_persistence_mode = detect_document_persistence_mode(
            connection,
            gcs_uri,
            ingested.get("document") or {},
            ingested.get("document_effective_date"),
            ingested.get("document_recorded_date"),
        )

        if lifecycle_policy == "replace_existing" and document_persistence_mode.get("persistence_action") == "update_existing":
            deleted_doc_id = int(document_persistence_mode.get("doc_id") or 0)
            replace_result = _delete_existing_document_for_replace(connection, deleted_doc_id)
            _record_step(
                "document_lifecycle_replace",
                matched_doc_id=deleted_doc_id,
                matched_by=document_persistence_mode.get("matched_by"),
                deleted=bool(replace_result.get("deleted")),
                markdown_path=replace_result.get("markdown_path"),
            )
            document_persistence_mode = detect_document_persistence_mode(
                connection,
                gcs_uri,
                ingested.get("document") or {},
                ingested.get("document_effective_date"),
                ingested.get("document_recorded_date"),
            )

        doc_id = insert_document_record(
            connection,
            gcs_uri,
            ingested.get("document") or {},
            ingested.get("document_effective_date"),
            ingested.get("document_recorded_date"),
        )
        category_registry_rows_written = _upsert_document_category_registry(
            connection,
            int(doc_id),
            ingested.get("document") if isinstance(ingested.get("document"), dict) else {},
            ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else [],
        )
        document_geo = (ingested.get("document") or {}).get("geography") if isinstance(ingested.get("document"), dict) else []
        geographic_facets_written = object_db.replace_document_geographic_facets(
            connection,
            int(doc_id),
            document_geo if isinstance(document_geo, list) else [],
        )
        document_actions = (ingested.get("document") or {}).get("action_items") if isinstance(ingested.get("document"), dict) else []
        action_items_written = object_db.replace_document_action_items(
            connection,
            int(doc_id),
            document_actions if isinstance(document_actions, list) else [],
        )
        schema_proposals_written = 0
        if metadata_only:
            db_summary = {
                "doc_id": int(doc_id),
                "objects_upserted": 0,
                "relationships_upserted": 0,
                "metadata_only": True,
            }
        else:
            if domain_db is not None and hasattr(domain_db, "upsert_solf_attribute_proposals"):
                schema_proposals = collect_solf_attribute_proposals(ingested, class_defs, int(doc_id))
                schema_proposals_written = int(domain_db.upsert_solf_attribute_proposals(connection, schema_proposals))
            db_summary = persist_solf_objects(connection, interpreter, class_defs, ingested, doc_id)
        db_summary["schema_attribute_proposals_written"] = schema_proposals_written
        db_summary["geographic_facets_written"] = geographic_facets_written
        db_summary["action_items_written"] = action_items_written
        db_summary["category_registry_rows_written"] = category_registry_rows_written
        db_summary["document_persistence_action"] = document_persistence_mode.get("persistence_action")
        db_summary["document_persistence_match"] = document_persistence_mode
        db_summary["document_lifecycle_policy"] = lifecycle_policy or None
        if metadata_only:
            _record_step(
                "persist_objects",
                status="skipped",
                reason="metadata_only_mode",
                doc_id=doc_id,
                document_persistence_action=db_summary.get("document_persistence_action"),
                document_lifecycle_policy=db_summary.get("document_lifecycle_policy"),
            )
        else:
            _record_step(
                "persist_objects",
                doc_id=doc_id,
                objects_upserted=db_summary.get("objects_upserted"),
                relationships_upserted=db_summary.get("relationships_upserted"),
                schema_attribute_proposals_written=schema_proposals_written,
                document_persistence_action=db_summary.get("document_persistence_action"),
                document_lifecycle_policy=db_summary.get("document_lifecycle_policy"),
            )

        table_cell_rows = 0
        if structured_tables:
            try:
                table_source_kind = "spreadsheet" if source_ext in {".xlsx", ".xlsm"} else "document"
                table_cell_rows = int(
                    object_db.upsert_document_table_cells(
                        connection,
                        int(db_summary["doc_id"]),
                        structured_tables,
                        source_kind=table_source_kind,
                        replace_existing=True,
                    )
                )
                _record_step(
                    "persist_table_cell_provenance",
                    doc_id=db_summary.get("doc_id"),
                    table_count=len(structured_tables),
                    cell_rows_upserted=table_cell_rows,
                    source_kind=table_source_kind,
                )
            except Exception as exc:
                LOGGER.warning("Failed to persist table cell provenance for doc_id=%s: %s", db_summary.get("doc_id"), exc)
                _record_step(
                    "persist_table_cell_provenance",
                    status="error",
                    doc_id=db_summary.get("doc_id"),
                    error=str(exc),
                )

        # Persist extracted Markdown in the tenant-matched document_content table.
        markdown_content_id = None
        if markdown_text.strip():
            try:
                doc_payload = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
                doc_key_for_markdown = str(doc_payload.get("doc_key") or "").strip() or str(Path(source_path_or_uri).name)
                markdown_content_id = markdown_manager.save_markdown_content_to_db(
                    connection,
                    int(db_summary["doc_id"]),
                    markdown_text,
                    source_name=doc_key_for_markdown,
                    metadata={"source_path": str(source_path_or_uri or ""), "storage": "postgresql"},
                )
                connection.commit()
                _record_step(
                    "store_markdown_content",
                    status="ok",
                    doc_id=db_summary["doc_id"],
                    content_id=markdown_content_id,
                )
            except Exception as exc:
                LOGGER.warning("Failed to persist Markdown content for doc_id=%s: %s", db_summary.get("doc_id"), exc)
                _record_step("store_markdown_content", status="error", error=str(exc))
        # Extension triggers enqueue work only; no domain mutation runs inline.
        # Existing persistence helpers commit individually, so this outbox enqueue
        # is a separate transaction. Reprocessing reconciles a crash in that gap.
        from security_context import get_security_context
        ingestion_caller = get_security_context(required=False)
        if ingestion_caller is not None:
            from ingestion_triggers import enqueue_persisted_document
            db_summary["extension_triggers"] = enqueue_persisted_document(connection, int(db_summary["doc_id"]))
            connection.commit()
    finally:
        connection.close()

    source_mime_type = infer_source_mime_type(source_path_or_uri)

    prefer_discovery_for_media = should_use_discovery_for_source(source_path_or_uri, source_mime_type)
    qdrant_enabled_for_doc, discovery_enabled_for_doc, used_indexing_fallback, selected_index_backend = select_indexing_backend(
        prefer_discovery_for_media=prefer_discovery_for_media
    )

    if used_indexing_fallback:
        LOGGER.warning(
            "Preferred indexing backend unavailable for this source; falling back to %s",
            selected_index_backend,
        )

    qdrant_result: dict[str, Any] = {"indexed": False}
    if metadata_only:
        qdrant_result = {
            "indexed": False,
            "skipped": True,
            "reason": "metadata_only_mode",
        }
        _record_step("qdrant_index", status="skipped", reason="metadata_only_mode")
    elif qdrant_enabled_for_doc:
        if QdrantClient is None:
            LOGGER.warning("qdrant-client is not installed; skipping Qdrant indexing")
            _record_step("qdrant_index", status="skipped", reason="client_unavailable")
        else:
            try:
                if not markdown_text.strip():
                    qdrant_result = {
                        "indexed": False,
                        "skipped": True,
                        "reason": "markdown_unavailable",
                    }
                    _record_step("qdrant_index", status="skipped", reason="markdown_unavailable")
                else:
                    base_chunks = chunk_markdown(markdown_text, QDRANT_CHUNK_SIZE, QDRANT_CHUNK_OVERLAP)
                    support_chunks = _build_dense_support_chunks(
                        document_payload=ingested.get("document") if isinstance(ingested.get("document"), dict) else {},
                        ingested=ingested,
                        db_summary=db_summary if isinstance(db_summary, dict) else {},
                    )
                    support_chunk_texts = [str(item.get("text") or "").strip() for item in support_chunks if str(item.get("text") or "").strip()]
                    chunks = list(base_chunks) + support_chunk_texts
                    chunk_feature_types = ["document_chunk" for _ in base_chunks] + [
                        str(item.get("feature_type") or "support_chunk").strip() or "support_chunk"
                        for item in support_chunks
                        if str(item.get("text") or "").strip()
                    ]

                    embeddings = embed_text_chunks(client, chunks)
                    qdrant = QdrantClient(
                        host=QDRANT_HOST,
                        port=QDRANT_PORT,
                        api_key=QDRANT_API_KEY or None,
                    )
                    qdrant_doc = ingested.get("document") if isinstance(ingested.get("document"), dict) else {}
                    qdrant_keywords = qdrant_doc.get("keywords") if isinstance(qdrant_doc.get("keywords"), list) else []
                    qdrant_entity_names = [
                        str(entity.get("entity_name") or "").strip()
                        for entity in (ingested.get("entities") or [])
                        if isinstance(entity, dict) and str(entity.get("entity_name") or "").strip()
                    ]
                    qdrant_result = index_chunks_in_qdrant(
                        qdrant=qdrant,
                        chunks=chunks,
                        embeddings=embeddings,
                        doc_id=db_summary["doc_id"],
                        doc_key=str(qdrant_doc.get("doc_key") or Path(source_path_or_uri).name),
                        doc_cat=str(qdrant_doc.get("doc_cat") or ""),
                        doc_type=str(qdrant_doc.get("doc_type") or routed.get("document_type") or "general_information"),
                        doc_theme=str(qdrant_doc.get("doc_theme") or ""),
                        gcs_uri=gcs_uri,
                        doc_keywords=[str(item).strip() for item in qdrant_keywords if str(item).strip()],
                        semantic_doc_terms=[
                            str(item).strip()
                            for item in (
                                _build_document_semantic_profile(
                                    qdrant_doc.get("doc_cat"),
                                    qdrant_doc.get("doc_type") or routed.get("document_type"),
                                    qdrant_doc.get("doc_theme"),
                                ).get("terms")
                                or []
                            )
                            if str(item).strip()
                        ],
                        entity_names=qdrant_entity_names,
                        chunk_feature_types=chunk_feature_types,
                    )
                    qdrant_result["base_chunk_count"] = len(base_chunks)
                    qdrant_result["support_chunk_count"] = len(support_chunk_texts)
                    _record_step("qdrant_index", indexed=True, chunk_count=qdrant_result.get("chunk_count"))
                    
            except Exception as exc:
                LOGGER.exception("Qdrant indexing failed: %s", exc)
                qdrant_result = {"indexed": False, "error": str(exc)}
                _record_step("qdrant_index", status="error", error=str(exc))

    discovery_result: dict[str, Any] = {"indexed": False}
    if metadata_only:
        discovery_result = {
            "indexed": False,
            "skipped": True,
            "reason": "metadata_only_mode",
        }
        _record_step("discovery_index", status="skipped", reason="metadata_only_mode")
    elif discovery_enabled_for_doc:
        try:
            discovery_struct = build_discovery_struct_data(routed, ingested, db_summary)
            discovery_doc_key = ""
            if isinstance(ingested.get("document"), dict):
                discovery_doc_key = str((ingested.get("document") or {}).get("doc_key") or "").strip()
            discovery_doc_id = _normalize_discovery_document_id(
                _first_non_empty(
                    discovery_doc_key,
                    str(db_summary.get("doc_id") or ""),
                    Path(source_path_or_uri).stem,
                    Path(gcs_uri).name,
                )
            )
            discovery_result = index_document_in_discovery_engine(
                gcs_uri=gcs_uri,
                mime_type=source_mime_type,
                struct_data=discovery_struct,
                document_id=discovery_doc_id,
            )
            _record_step("discovery_index", indexed=True, operation=discovery_result.get("operation"))
        except Exception as exc:
            LOGGER.exception("Discovery indexing failed: %s", exc)
            discovery_result = {
                "indexed": False,
                "error": str(exc),
            }
            _record_step("discovery_index", status="error", error=str(exc))
    elif ENABLE_DISCOVERY_INDEX and not discovery_enabled_for_doc:
        discovery_result = {
            "indexed": False,
            "skipped": True,
            "reason": "discovery_enabled_for_audio_video_only",
        }

    if ENABLE_QDRANT_INDEX and not qdrant_enabled_for_doc:
        qdrant_result = {
            "indexed": False,
            "skipped": True,
            "reason": "qdrant_skipped_for_audio_video",
        }

    tx_match_index_result: dict[str, Any] = {
        "indexed": False,
        "skipped": True,
        "reason": "tx_match_index_disabled",
    }
    if metadata_only:
        tx_match_index_result = {
            "indexed": False,
            "skipped": True,
            "reason": "metadata_only_mode",
        }
        _record_step("tx_match_index", status="skipped", reason="metadata_only_mode")
    elif not ENABLE_TX_MATCH_INDEX:
        _record_step("tx_match_index", status="skipped", reason="tx_match_index_disabled")
    elif tx_match_index is None:
        tx_match_index_result = {
            "indexed": False,
            "skipped": True,
            "reason": "tx_match_index_module_unavailable",
        }
        _record_step("tx_match_index", status="skipped", reason="module_unavailable")
    else:
        try:
            tx_doc_type, tx_rows = tx_match_index.build_tx_match_rows(
                ingested=ingested,
                routed=routed,
                db_summary=db_summary,
            )
            if not tx_rows:
                tx_match_index_result = {
                    "indexed": False,
                    "skipped": True,
                    "eligible": bool(tx_match_index.is_eligible_doc_type(tx_doc_type)),
                    "doc_type": tx_doc_type,
                    "reason": "no_candidate_rows",
                }
                _record_step("tx_match_index", status="skipped", reason="no_candidate_rows", doc_type=tx_doc_type)
            else:
                tx_match_index_result = tx_match_index.index_rows(tx_rows)
                tx_match_index_result["eligible"] = True
                tx_match_index_result["doc_type"] = tx_doc_type
                if tx_match_index_result.get("indexed"):
                    _record_step(
                        "tx_match_index",
                        indexed=True,
                        doc_type=tx_doc_type,
                        row_count=tx_match_index_result.get("row_count"),
                    )
                else:
                    _record_step(
                        "tx_match_index",
                        status="error",
                        doc_type=tx_doc_type,
                        reason=tx_match_index_result.get("reason"),
                    )
                    with object_db.get_connection() as pending_connection:
                        queue_payload = {
                            "doc_id": int(db_summary.get("doc_id") or 0),
                            "doc_type": tx_doc_type,
                            "source_path": source_path_or_uri,
                            "tx_candidate_row_count": len(tx_rows),
                            "tx_rows": tx_rows,
                            "matching_collection": tx_match_index_result.get("collection") or (tx_rows[0].get("matching_collection") if tx_rows else None),
                            "purpose": tx_rows[0].get("purpose") if tx_rows else "transaction_match",
                            "tx_result": tx_match_index_result,
                        }
                        pending_entry = object_db.enqueue_tx_match_index_pending(
                            connection=pending_connection,
                            payload=queue_payload,
                            doc_id=int(db_summary.get("doc_id") or 0) or None,
                            error_message=str(tx_match_index_result.get("reason") or "tx_match_index_failed"),
                        )
                    tx_match_index_result["pending_queue"] = {
                        "queued": True,
                        "pending_id": int(pending_entry.get("pending_id") or 0),
                    }
        except Exception as exc:
            LOGGER.exception("Transaction matching indexing failed: %s", exc)
            tx_match_index_result = {
                "indexed": False,
                "eligible": True,
                "error": str(exc),
            }
            _record_step("tx_match_index", status="error", error=str(exc))
            try:
                with object_db.get_connection() as pending_connection:
                    queue_payload = {
                        "doc_id": int(db_summary.get("doc_id") or 0),
                        "doc_type": str((ingested.get("document") or {}).get("doc_type") or routed.get("document_type") or "").strip().lower(),
                        "source_path": source_path_or_uri,
                        "tx_result": tx_match_index_result,
                    }
                    pending_entry = object_db.enqueue_tx_match_index_pending(
                        connection=pending_connection,
                        payload=queue_payload,
                        doc_id=int(db_summary.get("doc_id") or 0) or None,
                        error_message=str(exc),
                    )
                tx_match_index_result["pending_queue"] = {
                    "queued": True,
                    "pending_id": int(pending_entry.get("pending_id") or 0),
                }
            except Exception as queue_exc:
                LOGGER.warning("Failed to enqueue tx_match_index_pending task: %s", queue_exc)
                tx_match_index_result["pending_queue"] = {
                    "queued": False,
                    "error": str(queue_exc),
                }

    adk_context = build_adk_context_payload(
        source_path_or_uri=source_path_or_uri,
        gcs_uri=gcs_uri,
        routed=routed,
        ingested=ingested,
        db_summary=db_summary,
        discovery_index=discovery_result,
    )

    persistence_action = str((db_summary or {}).get("document_persistence_action") or "").strip().lower()
    persistence_match = (db_summary or {}).get("document_persistence_match") if isinstance((db_summary or {}).get("document_persistence_match"), dict) else {}
    persistence_matched_by = str((persistence_match or {}).get("matched_by") or "").strip().lower()
    skip_person_similarity_review = bool(
        persistence_action == "update_existing"
        and persistence_matched_by in {"source_identity", "doc_path"}
    )

    if skip_person_similarity_review:
        person_similarity_review = {
            "attention_required": False,
            "has_potential_duplicates": False,
            "alert_count": 0,
            "headline": "",
            "alerts": [],
            "recommended_actions": [],
            "skipped": True,
            "skip_reason": "update_existing_same_source_identity_or_path",
            "persistence_action": persistence_action,
            "matched_by": persistence_matched_by,
        }
        _record_step(
            "person_similarity_review",
            status="skipped",
            reason="update_existing_same_source_identity_or_path",
            persistence_action=persistence_action,
            matched_by=persistence_matched_by,
            alert_count=0,
        )
    else:
        person_similarity_review = _build_person_similarity_review(
            ingested=ingested,
            db_summary=db_summary if isinstance(db_summary, dict) else {},
        )
        if bool(person_similarity_review.get("attention_required")):
            _record_step(
                "person_similarity_review",
                status="attention_required",
                alert_count=int(person_similarity_review.get("alert_count") or 0),
            )
        else:
            _record_step("person_similarity_review", status="ok", alert_count=0)

    metadata_for_upsert = ingested_document.get("metadata") if isinstance(ingested_document.get("metadata"), dict) else {}
    note_source = str(metadata_for_upsert.get("note_source") or "").strip().lower()
    is_note_ingest = bool(note_source)
    skip_attribute_upsert = metadata_only or (is_note_ingest and not ENABLE_ATTRIBUTE_EMBEDDING_UPSERT_FOR_NOTES)

    attribute_embedding_index = _build_attribute_embedding_index_if_available(skip_upsert=skip_attribute_upsert)
    _record_step(
        "attribute_embedding_index",
        status="ok" if attribute_embedding_index.get("built") else "skipped",
        **attribute_embedding_index,
    )

    result = {
        "run_id": run_id,
        "source": source_path_or_uri,
        "gcs_uri": gcs_uri,
        "source_mime_type": source_mime_type,
        "routed": routed,
        "extracted": extracted,
        "solf_objects": ingested,
        "db_summary": db_summary,
        "router_schema": build_router_schema(class_defs),
        "inference_limits": {
            "max_rules": getattr(business_rules, "INFERENCE_MAX_RULES", None),
            "max_directives_per_rule": getattr(business_rules, "INFERENCE_MAX_DIRECTIVES_PER_RULE", None),
            "max_workflow_process_additions": getattr(business_rules, "INFERENCE_MAX_WORKFLOW_PROCESS_ADDITIONS", None),
        },
        "discovery_index": discovery_result,
        "qdrant_index": qdrant_result,
        "tx_match_index": tx_match_index_result,
        "indexing_strategy": {
            "prefer_discovery_for_media": prefer_discovery_for_media,
            "qdrant_enabled_for_doc": qdrant_enabled_for_doc,
            "discovery_enabled_for_doc": discovery_enabled_for_doc,
            "selected_index_backend": selected_index_backend,
            "used_fallback": used_indexing_fallback,
            "tx_match_enabled": bool(ENABLE_TX_MATCH_INDEX),
        },
        "markdown": markdown_text,
        "markdown_cache_path": str(markdown_cache_path) if markdown_cache_path else None,
        "markdown_reused": markdown_reused,
        "adk_context": adk_context,
        "attribute_embedding_index": attribute_embedding_index,
        "workflow_trace": workflow_trace,
        "note_directives": note_directives,
        "note_solf_clause_results": note_clause_results,
        "runtime_rule_trigger": runtime_rule_trigger,
        "post_extraction_rule_application": post_extraction_rule_application,
        "person_similarity_review": person_similarity_review,
        "ingestion_attention_required": bool(person_similarity_review.get("attention_required")),
        "ingestion_attention_banner": str(person_similarity_review.get("headline") or ""),
        "output_json_path": None,
    }

    if metadata_only:
        note_action_execution = {
            "execute_actions": False,
            "requested": 0,
            "executed": 0,
            "skipped": 0,
            "reason": "metadata_only_mode",
        }
        result["note_action_execution"] = note_action_execution
        _record_step("execute_note_actions", status="skipped", reason="metadata_only_mode")
    else:
        note_action_execution = _execute_note_action_requests(
            interpreter=interpreter,
            note_directives=note_directives,
            doc_id=int(db_summary.get("doc_id") or 0),
            run_id=run_id,
            source_path_or_uri=source_path_or_uri,
        )
        result["note_action_execution"] = note_action_execution
        _record_step(
            "execute_note_actions",
            requested=int(note_action_execution.get("requested") or 0),
            executed=int(note_action_execution.get("executed") or 0),
            skipped=int(note_action_execution.get("skipped") or 0),
            execute_actions=bool(note_action_execution.get("execute_actions")),
        )

    if output_json_path:
        requested_output_path = Path(output_json_path)
        resolved_output_path = requested_output_path.with_name(
            f"{requested_output_path.stem}-{run_id}{requested_output_path.suffix or '.json'}"
        )
        resolved_output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        result["output_json_path"] = str(resolved_output_path)

    return result


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest a document with GenAI, map to SOLF objects, and persist to object_db schema")
    parser.add_argument("source", help="Local file path or gs:// URI")
    parser.add_argument("--out", default="", help="Optional path to write full ingestion JSON result")
    parser.add_argument("--description", default="", help="Optional user description for the file to improve classification/search")
    parser.add_argument("--tag", action="append", default=[], help="Optional user tag (repeatable) to index with the document")
    parser.add_argument("--metadata-json", default="", help="Optional JSON object string with additional user metadata")
    parser.add_argument("--workflow-process", action="append", default=[], help="Optional workflow process name to trigger (repeatable, e.g., booking_process, vat_classification). Auto-derived from document_type; use to override.")
    parser.add_argument("--no-reuse-markdown", action="store_true", help="Force markdown regeneration instead of reusing a cached .md artifact")
    parser.add_argument("--no-persist-markdown", action="store_true", help="Do not write generated markdown to the cache folder")
    parser.add_argument(
        "--required-domain-action-policy",
        default="",
        help="Override required domain action enforcement policy, e.g. invoice:fail,bill:warn,receipt:ignore",
    )
    parser.add_argument(
        "--disable-required-domain-actions",
        action="store_true",
        help="Disable required domain action enforcement for this run",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main() -> int:
    parser = build_cli()
    args = parser.parse_args()

    configure_logging(getattr(logging, args.log_level))

    user_metadata: dict[str, Any] = {}
    if args.metadata_json:
        try:
            parsed_metadata = json.loads(args.metadata_json)
            if not isinstance(parsed_metadata, dict):
                raise ValueError("--metadata-json must decode to a JSON object")
            user_metadata = parsed_metadata
        except Exception as exc:
            LOGGER.exception("Invalid --metadata-json: %s", exc)
            return 1

    user_context = {
        "description": args.description,
        "tags": args.tag or [],
        "metadata": user_metadata,
    }

    try:
        result = run_ingest(
            args.source,
            args.out or None,
            user_context=user_context,
            enforce_required_actions=not bool(args.disable_required_domain_actions),
            required_action_policy_text=args.required_domain_action_policy or None,
            reuse_markdown=not bool(args.no_reuse_markdown),
            persist_markdown=not bool(args.no_persist_markdown),
            skip_markdown_generation=bool(args.no_reuse_markdown and args.no_persist_markdown),
            workflow_processes=args.workflow_process if args.workflow_process else None,
        )
        print(json.dumps({
            "gcs_uri": result["gcs_uri"],
            "document_type": result["routed"].get("document_type"),
            "objects_upserted": result["db_summary"]["objects_upserted"],
            "relationships_upserted": result["db_summary"]["relationships_upserted"],
            "doc_id": result["db_summary"]["doc_id"],
        }, indent=2, ensure_ascii=False))
        return 0
    except Exception as exc:
        LOGGER.exception("Ingestion failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
