import argparse
import hashlib
import json
import logging
import math
import os
import re
import unicodedata
from pathlib import Path
from types import SimpleNamespace
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, Fusion, FusionQuery, PointStruct, Prefetch, SparseVector, VectorParams
except Exception:  # pragma: no cover
    QdrantClient = None
    Distance = None
    Fusion = None
    FusionQuery = None
    PointStruct = None
    Prefetch = None
    SparseVector = None
    VectorParams = None

# Discovery engine is intentionally disabled in IDMS-Demo.
discoveryengine = None

try:
    import spacy
except Exception:  # pragma: no cover
    spacy = None

try:
    from attribute_embedding_index import AttributeEmbeddingIndex
except Exception:  # pragma: no cover
    AttributeEmbeddingIndex = None


try:
    from pattern_library import PatternLibrary, SemanticPattern
except Exception:  # pragma: no cover
    PatternLibrary = None
    SemanticPattern = None

try:
    from hybrid_search import encode_sparse_vector, merge_hybrid_results, should_use_hybrid_search
except Exception:  # pragma: no cover
    encode_sparse_vector = None
    merge_hybrid_results = None
    should_use_hybrid_search = None

try:
    import business_rules
except Exception:  # pragma: no cover
    business_rules = None


from ikos_config import (
    ENABLE_CONTEXTUAL_LLM_RESOLUTION_FALLBACK,
    ENABLE_DISCOVERY_INDEX,
    ENABLE_LLM_INTENT_PARSER,
    ENABLE_SEMANTIC_LLM_FALLBACK,
    ENABLE_STRICT_QUERY_EVIDENCE_GATE,
    GERMAN_NOUN_GENDER_OVERRIDES,
    STRICT_QUERY_EVIDENCE_REQUIRE_EXPLANATION,
    DISCOVERY_DATA_STORE_ID,
    DISCOVERY_LOCATION,
    DISCOVERY_SERVING_CONFIG,
    EXTRACT_MODEL,
    PROJECT_ID,
    LOCATION,
    EMBEDDING_LOCATION,
    QDRANT_EMBEDDING_MODEL,
    QDRANT_VECTOR_SIZE,
    QDRANT_URL,
    QDRANT_COLLECTION as INGEST_QDRANT_COLLECTION,
    QDRANT_QUERY_COLLECTION as QDRANT_COLLECTION,
    QDRANT_QUERY_API_KEY,
)
from security_context import get_security_context, get_tenant_id
from llm_fallback import generate_content_with_openrouter_fallback, get_openrouter_client
from ikos_query_planner_prompt import build_query_planner_prompt, build_query_planner_request_payload
import object_db
from federated_logical_query import compile_read_only_query


LOGGER = logging.getLogger("idms.query_engine")


@dataclass
class ParsedQuery:
    intent: str
    entity_name: str | None
    attribute_name: str | None
    confidence: float
    language: str = "en"  # Language detected from query ("en", "de", etc.)
    relation_name: str | None = None
    criteria: dict[str, Any] | None = None
    attribute_names: list[str] | None = None
    parse_artifact: dict[str, Any] | None = None


class QueryEngine:
    # Bidirectional attribute name mappings: canonical_name -> {language: [display_names]}
    ATTRIBUTE_DISPLAY_NAMES = {
        "birth_date": {
            "en": ["birth date", "date of birth", "born on"],
            "de": ["geburtsdatum", "geboren am"],
        },
        "birth_place": {
            "en": ["birth place", "place of birth", "born in"],
            "de": ["geburtsort", "geboren in"],
        },
        "registration_no": {
            "en": ["registration number", "registration_no", "registration no", "company number"],
            "de": ["firmennummer", "unternehmensnummer", "registrierungsnummer", "handelsregisternummer"],
        },
        "uid_che": {
            "en": ["uid", "che", "che number", "company uid", "company registration number"],
            "de": ["uid", "che", "che nummer", "unternehmens-uid", "che-nummer"],
        },
        "registered_address": {
            "en": ["registered address", "address"],
            "de": ["adresse", "anschrift"],
        },
        "document_filename": {
            "en": ["file name", "filename", "document filename"],
            "de": ["dateiname", "dokumentname", "datei name"],
        },
        "document_full_path": {
            "en": ["full path", "file path", "document path"],
            "de": ["vollständiger pfad", "vollstaendiger pfad", "pfad"],
        },
        "document_file_info": {
            "en": ["filename with full path", "file info"],
            "de": ["dateiname mit vollständigem pfad", "dateiname mit vollstaendigem pfad"],
        },
        "purpose": {
            "en": ["purpose", "business purpose", "company purpose"],
            "de": ["zweck", "gesellschaftszweck", "unternehmenszweck"],
        },
        "publication_organ": {
            "en": ["publication organ", "publication medium"],
            "de": ["publikationsorgan"],
        },
        "tr_number": {
            "en": ["trade register number", "tr number"],
            "de": ["tr-nr", "handelsregister nummer"],
        },
        "tr_date": {
            "en": [
                "trade register date",
                "tr date",
                "registration date",
                "date of registration",
                "date registered",
                "registration day",
                "entry date",
            ],
            "de": ["tr-datum", "handelsregister datum", "eintragungsdatum", "eintragung"],
        },
        "due_date": {
            "en": [
                "due date",
                "invoice due date",
                "payment due date",
                "payable by",
                "date due",
            ],
            "de": [
                "fälligkeitsdatum",
                "faelligkeitsdatum",
                "fällig am",
                "faellig am",
                "zahlungsziel",
            ],
        },
        "statute_date": {
            "en": ["statute date", "articles of association date", "statutes date"],
            "de": ["statutendatum", "datum der statuten", "urkundendatum"],
        },
        "shareholders": {
            "en": ["shareholders", "share holders", "shareholders list", "shares and amounts"],
            "de": ["aktionäre", "gesellschafter", "eigentümer", "anteilseigner", "anteile", "beträge"],
        },
        "total_capital": {
            "en": ["total capital", "share capital", "nominal capital"],
            "de": ["gesamtkapital", "stammkapital", "kapital"],
        },
        "eori_contact_person": {
            "en": ["eori contact person", "eori contact", "contact person for eori"],
            "de": ["eori-ansprechpartner", "eori ansprechpartner", "ansprechpartner für eori"],
        },
        "eori_contact_email": {
            "en": ["email of eori contact person", "eori contact email"],
            "de": ["email für eori-ansprechpartner", "e-mail für eori ansprechpartner"],
        },
        "process_contact_person": {
            "en": ["contact person", "process contact", "responsible person"],
            "de": ["ansprechpartner", "kontaktperson", "verantwortliche person"],
        },
        "process_contact_email": {
            "en": ["contact email", "email", "e-mail"],
            "de": ["kontakt-email", "e-mail", "email"],
        },
        "responsible_company": {
            "en": ["responsible company", "applicant company", "handling company"],
            "de": ["verantwortliches unternehmen", "antragstellendes unternehmen", "zuständige firma"],
        },
    }

    PLAN_OUTPUT_SHAPES = {"table", "list", "scalar", "flat", "document"}

    # Generic alias map for planner outputs that may vary by model.
    PLAN_ATTRIBUTE_ALIASES = {
        "email": "process_contact_email",
        "email_address": "process_contact_email",
        "contact_email": "process_contact_email",
        "contact_mail": "process_contact_email",
        "person_name": "process_contact_person",
        "contact_name": "process_contact_person",
        "name": "process_contact_person",
        "birthday": "birth_date",
        "age_or_birth_date": "birth_date",
        "document_theme": "theme",
    }
    
    # Language detection patterns: tuple of (regex_patterns, language_code)
    LANGUAGE_PATTERNS = [
        ([r"\b(?:was\s+ist|wie\s+lautet|zeige|gib\s+mir|finde)\b", r"[äöüß]"], "de"),
        ([r"\b(?:what\s+is|what's|show|get|find)\b", r"\b(?:of|for|the)\b"], "en"),
    ]

    SEMANTIC_STOPWORDS = {
        "zeigen", "zeige", "mir", "den", "die", "das", "und", "mit", "vollständigen", "vollständigem",
        "vollstaendigen", "vollstaendigem", "pfad", "dateinamen", "dateiname", "dokuments", "dokument",
        "das", "die", "der", "für", "fuer", "enthält", "enthaelt", "an", "sie", "mir", "bitte",
        "was", "wie", "lautet",
    }

    # Maps English and German number words to integers for temporal parsing.
    _WORD_TO_INT: dict[str, int] = {
        "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
        "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
        "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
        "fifteen": 15, "twenty": 20, "thirty": 30,
        # German
        "ein": 1, "eine": 1, "zwei": 2, "drei": 3, "vier": 4, "fünf": 5, "fuenf": 5,
        "sechs": 6, "sieben": 7, "acht": 8, "neun": 9, "zehn": 10,
        "elf": 11, "zwölf": 12, "zwoelf": 12,
    }

    # Converts time-unit words to approximate day counts.
    _UNIT_DAYS: dict[str, int] = {
        "day": 1, "days": 1, "tag": 1, "tage": 1,
        "week": 7, "weeks": 7, "woche": 7, "wochen": 7,
        "month": 30, "months": 30, "monat": 30, "monate": 30,
        "year": 365, "years": 365, "jahr": 365, "jahre": 365,
    }

    _MONTH_NAME_TO_NUM: dict[str, int] = {
        "january": 1, "jan": 1,
        "february": 2, "feb": 2,
        "march": 3, "mar": 3,
        "april": 4, "apr": 4,
        "may": 5,
        "june": 6, "jun": 6,
        "july": 7, "jul": 7,
        "august": 8, "aug": 8,
        "september": 9, "sep": 9, "sept": 9,
        "october": 10, "oct": 10,
        "november": 11, "nov": 11,
        "december": 12, "dec": 12,
        "januar": 1,
        "februar": 2,
        "marz": 3, "maerz": 3,
        "mai": 5,
        "juni": 6,
        "juli": 7,
        "aug": 8,
        "oktober": 10, "okt": 10,
        "dezember": 12, "dez": 12,
    }

    # Maps plural/singular entity-type words to canonical type + retrieval hints.
    _ENTITY_TYPE_MAP: dict[str, tuple[str, list[str]]] = {
        "expense":      ("expense",     ["amount", "cost", "expense", "receipt", "date"]),
        "expenses":     ("expense",     ["amount", "cost", "expense", "receipt", "date"]),
        "customer":     ("customer",    ["billing_address", "shipping_address", "address", "contact_person_ref", "account_manager_ref", "country"]),
        "customers":    ("customer",    ["billing_address", "shipping_address", "address", "contact_person_ref", "account_manager_ref", "country"]),
        "ausgaben":     ("expense",     ["amount", "cost", "expense", "date"]),
        "kosten":       ("expense",     ["amount", "cost", "expense", "date"]),
        "invoice":      ("invoice",     ["invoice_no", "amount", "date", "billing"]),
        "invoices":     ("invoice",     ["invoice_no", "amount", "date", "billing"]),
        "rechnung":     ("invoice",     ["invoice_no", "amount", "date"]),
        "rechnungen":   ("invoice",     ["invoice_no", "amount", "date"]),
        "transaction":  ("transaction", ["amount", "date", "debit", "credit", "account"]),
        "transactions": ("transaction", ["amount", "date", "debit", "credit", "account"]),
        "payment":      ("payment",     ["amount", "date", "payment_method", "reference"]),
        "payments":     ("payment",     ["amount", "date", "payment_method", "reference"]),
        "zahlungen":    ("payment",     ["amount", "date", "payment_method"]),
        "zahlung":      ("payment",     ["amount", "date", "payment_method"]),
        "receipt":      ("receipt",     ["amount", "date", "receipt_no"]),
        "receipts":     ("receipt",     ["amount", "date", "receipt_no"]),
        "beleg":        ("receipt",     ["amount", "date"]),
        "belege":       ("receipt",     ["amount", "date"]),
        "report":       ("report",      ["report_type", "date", "summary"]),
        "reports":      ("report",      ["report_type", "date", "summary"]),
        "contract":     ("contract",    ["contract_no", "date", "parties"]),
        "contracts":    ("contract",    ["contract_no", "date", "parties"]),
        "vertrag":      ("contract",    ["contract_no", "date"]),
        "verträge":     ("contract",    ["contract_no", "date"]),
        "document":     ("document",    ["document_type", "date", "filename"]),
        "documents":    ("document",    ["document_type", "date", "filename"]),
        "dokument":     ("document",    ["document_type", "date", "filename"]),
        "dokumente":    ("document",    ["document_type", "date", "filename"]),
    }

    _VIRTUAL_SEMANTIC_PATTERNS: list[dict[str, Any]] = [
        {"pattern_text": "how many shares does * hold in *", "semantic_concept": "share_count", "mapped_attributes": {"shareholders": "shareholders"}, "pattern_language": "en", "confidence": 0.86},
        {"pattern_text": "what is the share count of * in *", "semantic_concept": "share_count", "mapped_attributes": {"shareholders": "shareholders"}, "pattern_language": "en", "confidence": 0.84},
        {"pattern_text": "who are the shareholders of *", "semantic_concept": "shareholders", "mapped_attributes": {"shareholders": "shareholders"}, "pattern_language": "en", "confidence": 0.88},
        {"pattern_text": "which companies does * hold shares in", "semantic_concept": "shareholders_person", "mapped_attributes": {"shareholders": "shareholders"}, "pattern_language": "en", "confidence": 0.84},
        {"pattern_text": "wie viele anteile haelt * an *", "semantic_concept": "share_count", "mapped_attributes": {"shareholders": "shareholders"}, "pattern_language": "de", "confidence": 0.86},
        {"pattern_text": "wer sind die aktionaere von *", "semantic_concept": "shareholders", "mapped_attributes": {"shareholders": "shareholders"}, "pattern_language": "de", "confidence": 0.88},
        {"pattern_text": "in welchen unternehmen haelt * anteile", "semantic_concept": "shareholders_person", "mapped_attributes": {"shareholders": "shareholders"}, "pattern_language": "de", "confidence": 0.84},
    ]

    # Generic semantic intent-frame registry. Each handler returns ParsedQuery | None.
    SEMANTIC_INTENT_FRAME_REGISTRY: list[dict[str, Any]] = [
        {
            "name": "document_about_entity",
            "intent": "criteria_lookup",
            "handler": "_semantic_frame_document_about_entity",
            "priority": 10,
        },
        {
            "name": "shareholding_queries",
            "intent": "attribute_lookup",
            "handler": "_semantic_frame_shareholding",
            "priority": 20,
        },
    ]

    def __init__(self) -> None:
        self.qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_QUERY_API_KEY) if QdrantClient is not None else None
        self._qdrant_collection_name = QDRANT_COLLECTION
        self._last_qdrant_search_mode = "uninitialized"
        self._last_qdrant_search_collection = str(QDRANT_COLLECTION or "").strip()
        self.genai_client = self._make_genai_client()
        self.embedding_client = self._make_embedding_client()
        self._spacy_nlp = None
        self._spacy_nlp_initialized = False
        if AttributeEmbeddingIndex is not None:
            self.attr_embedding_index = AttributeEmbeddingIndex.get_shared_instance()
        else:
            self.attr_embedding_index = None
        # Initialize pattern library for learned pattern matching
        if PatternLibrary is not None:
            try:
                db_conn = self._get_connection()
                self.pattern_library = PatternLibrary(db_conn)
            except Exception as e:
                import logging
                logging.warning(f"Failed to initialize pattern library: {e}")
                self.pattern_library = None
        else:
            self.pattern_library = None
        self._pattern_vector_collection = f"{QDRANT_COLLECTION}_semantic_patterns"
        self._pattern_vectors_ready = False
        self._pattern_vectors_last_sync: datetime | None = None
        self._pattern_vectors_tenant: str | None = None
        self._schema_alias_cache: dict[str, str] = {}
        self._schema_alias_cache_tenant: str | None = None
        self._schema_alias_caches: dict[str, dict[str, str]] = {}
        self._attribute_token_lexicon_cache: set[str] | None = None
        self.solf_category_inference = None
        self._refresh_schema_alias_cache()
        self._qdrant_collection_name = self._resolve_qdrant_collection_name()

    def _resolve_qdrant_collection_name(self, force_refresh: bool = False) -> str:
        """Resolve Qdrant collection name with unified standardization on idms_demo_documents.
        Preference order: configured → ingest override → idms_demo_documents → other doc collections.
        This ensures no collection drift across environments.
        """
        # Default to standardized collection name
        configured = str(self._qdrant_collection_name or QDRANT_COLLECTION or "").strip() or "idms_demo_documents"
        qdrant_client = getattr(self, "qdrant", None)
        if qdrant_client is None:
            return configured

        try:
            collections = qdrant_client.get_collections()
            names = [str(c.name).strip() for c in (collections.collections or []) if str(getattr(c, "name", "")).strip()]
            existing = {name for name in names if name}
            if not existing:
                return configured

            # Prefer configured name if it exists
            if configured in existing:
                return configured

            # Check ingest override
            ingest_collection = str(INGEST_QDRANT_COLLECTION or "").strip()
            if ingest_collection and ingest_collection in existing:
                return ingest_collection

            # Standard fallback cascade: idms_demo_documents → legacy names → any doc collection
            fallback_candidates = [
                "idms_demo_documents",    # Primary standard
                "idms_proto_documents",   # Legacy (phase-out candidate)
                "idms_documents",         # Alternative standard
                "idms_demo",              # Alternative
            ]
            for candidate in fallback_candidates:
                if candidate in existing:
                    return candidate

            doc_like = [
                name
                for name in names
                if name.endswith("_documents")
                and not name.endswith("_semantic_patterns")
                and name != "attribute_embeddings"
            ]
            if doc_like:
                return doc_like[0]

            return configured
        except Exception:
            if force_refresh:
                return configured
            return configured

    @staticmethod
    def _is_qdrant_bad_request(exc: Exception) -> bool:
        text = str(exc or "").lower()
        return "400" in text or "bad request" in text

    def _qdrant_dense_search_points(
        self,
        collection_name: str,
        dense_vector: list[float] | None,
        limit: int,
        with_payload: bool = True,
        vector_name: str | None = None,
    ) -> list[Any]:
        if not dense_vector or requests is None:
            self._last_qdrant_search_mode = "skipped"
            self._last_qdrant_search_collection = collection_name
            return []

        base = str(QDRANT_URL or "").rstrip("/")
        if not base:
            self._last_qdrant_search_mode = "missing_base_url"
            self._last_qdrant_search_collection = collection_name
            return []

        body: dict[str, Any] = {
            "limit": int(limit),
            "with_payload": bool(with_payload),
        }
        security_context = get_security_context(required=False)
        tenant_id = str(security_context.tenant_id) if security_context is not None else "00000000-0000-0000-0000-000000000001"
        vector_filter: dict[str, Any] = {"must": [{"key": "tenant_id", "match": {"value": tenant_id}}]}
        excluded_levels: list[dict[str, Any]] = []
        if security_context is None or not security_context.allows("finance.read"):
            excluded_levels.append({"key": "access_level", "match": {"value": "finance"}})
        if security_context is None or not security_context.allows("documents.restricted.read"):
            excluded_levels.append({"key": "access_level", "match": {"value": "restricted"}})
        if excluded_levels:
            vector_filter["must_not"] = excluded_levels
        body["filter"] = vector_filter
        if vector_name:
            body["vector"] = {"name": vector_name, "vector": dense_vector}
        else:
            body["vector"] = dense_vector

        headers = {"Content-Type": "application/json"}
        api_key = str(QDRANT_QUERY_API_KEY or "").strip()
        if api_key:
            headers["api-key"] = api_key

        try:
            response = requests.post(
                f"{base}/collections/{collection_name}/points/search",
                headers=headers,
                data=json.dumps(body),
                timeout=12,
            )
            if response.status_code != 200:
                self._last_qdrant_search_mode = f"rest_points_search_http_{response.status_code}"
                self._last_qdrant_search_collection = collection_name
                return []
            data = response.json() if response.content else {}
            points = data.get("result") if isinstance(data, dict) else None
            out: list[Any] = []
            for point in points or []:
                if not isinstance(point, dict):
                    continue
                out.append(
                    SimpleNamespace(
                        payload=point.get("payload") or {},
                        score=float(point.get("score") or 0.0),
                    )
                )
            self._last_qdrant_search_mode = "rest_points_search"
            self._last_qdrant_search_collection = collection_name
            return out
        except Exception:
            self._last_qdrant_search_mode = "rest_points_search_exception"
            self._last_qdrant_search_collection = collection_name
            return []

    def _qdrant_hybrid_search_points(
        self,
        collection_name: str,
        query_text: str,
        dense_vector: list[float] | None,
        limit: int,
        sparse_weight: float = 0.3,
        dense_weight: float = 0.7,
        with_payload: bool = True,
        dense_vector_name: str | None = "dense",
        sparse_vector_name: str = "sparse",
    ) -> list[Any]:
        """Perform hybrid search combining sparse (BM25) and dense (semantic) vectors.
        
        Args:
            collection_name: Qdrant collection to search
            query_text: Original query text for sparse encoding
            dense_vector: Pre-computed dense embedding
            limit: Maximum results to return
            sparse_weight: Weight for sparse/keyword search (0-1)
            dense_weight: Weight for dense/semantic search (0-1)
            with_payload: Whether to include payload in results
        
        Returns:
            List of merged results sorted by combined score
        """
        if not dense_vector or not query_text or requests is None:
            self._last_qdrant_search_mode = "hybrid_skipped"
            self._last_qdrant_search_collection = collection_name
            return []
        
        base = str(QDRANT_URL or "").rstrip("/")
        if not base:
            self._last_qdrant_search_mode = "missing_base_url"
            return []
        
        headers = {"Content-Type": "application/json"}
        api_key = str(QDRANT_QUERY_API_KEY or "").strip()
        if api_key:
            headers["api-key"] = api_key
        
        try:
            security_context = get_security_context(required=False)
            tenant_id = str(security_context.tenant_id) if security_context is not None else "00000000-0000-0000-0000-000000000001"
            tenant_filter: dict[str, Any] = {"must": [{"key": "tenant_id", "match": {"value": tenant_id}}]}
            excluded_levels: list[dict[str, Any]] = []
            if security_context is None or not security_context.allows("finance.read"):
                excluded_levels.append({"key": "access_level", "match": {"value": "finance"}})
            if security_context is None or not security_context.allows("documents.restricted.read"):
                excluded_levels.append({"key": "access_level", "match": {"value": "restricted"}})
            if excluded_levels:
                tenant_filter["must_not"] = excluded_levels
            # Encode query as sparse vector
            sparse_indices, sparse_values = (
                encode_sparse_vector(query_text) if encode_sparse_vector else ([], [])
            )
            
            sparse_results = []
            dense_results = []
            
            # Run sparse search if available
            if sparse_indices and sparse_values:
                try:
                    sparse_body = {
                        "limit": int(limit * 2),  # Get more results for merging
                        "with_payload": bool(with_payload),
                        "filter": tenant_filter,
                        "vector": {
                            "name": sparse_vector_name,
                            "vector": {"indices": sparse_indices, "values": sparse_values},
                        },
                    }
                    response = requests.post(
                        f"{base}/collections/{collection_name}/points/search",
                        headers=headers,
                        data=json.dumps(sparse_body),
                        timeout=12,
                    )
                    if response.status_code == 200:
                        data = response.json() or {}
                        for point in data.get("result", []) or []:
                            if isinstance(point, dict):
                                sparse_results.append(
                                    SimpleNamespace(
                                        id=str(point.get("id", "")),
                                        payload=point.get("payload", {}),
                                        score=float(point.get("score", 0.0)),
                                    )
                                )
                except Exception as e:
                    LOGGER.debug("Sparse search failed: %s", e)
            
            # Run dense search
            dense_body = {
                "limit": int(limit * 2),
                "with_payload": bool(with_payload),
                "filter": tenant_filter,
            }
            if dense_vector_name:
                dense_body["vector"] = {"name": dense_vector_name, "vector": dense_vector}
            else:
                dense_body["vector"] = dense_vector
            response = requests.post(
                f"{base}/collections/{collection_name}/points/search",
                headers=headers,
                data=json.dumps(dense_body),
                timeout=12,
            )
            
            if response.status_code == 200:
                data = response.json() or {}
                for point in data.get("result", []) or []:
                    if isinstance(point, dict):
                        dense_results.append(
                            SimpleNamespace(
                                id=str(point.get("id", "")),
                                payload=point.get("payload", {}),
                                score=float(point.get("score", 0.0)),
                            )
                        )
            
            # Merge results using weighted scoring
            if merge_hybrid_results:
                merged = merge_hybrid_results(
                    sparse_results=[
                        {"id": r.id, "score": r.score, "payload": r.payload}
                        for r in sparse_results
                    ],
                    dense_results=[
                        {"id": r.id, "score": r.score, "payload": r.payload}
                        for r in dense_results
                    ],
                    sparse_weight=sparse_weight,
                    dense_weight=dense_weight,
                    limit=limit,
                )
                self._last_qdrant_search_mode = f"hybrid_search (sparse:{len(sparse_results)} dense:{len(dense_results)})"
                self._last_qdrant_search_collection = collection_name
                return [
                    SimpleNamespace(payload=m["payload"], score=m["score"])
                    for m in merged
                ]
            else:
                # Fallback: just use dense results
                self._last_qdrant_search_mode = "hybrid_fallback_to_dense"
                return dense_results[:limit]
                
        except Exception as e:
            LOGGER.debug("Hybrid search exception: %s", e)
            self._last_qdrant_search_mode = "hybrid_search_exception"
            self._last_qdrant_search_collection = collection_name
            return []

    def _qdrant_collection_vector_profile(self, collection_name: str) -> dict[str, Any]:
        """Inspect collection vector layout so queries can match named vs unnamed schemas."""
        profile = {
            "dense_vector_name": None,
            "supports_sparse": False,
            "sparse_vector_name": "sparse",
        }
        qdrant_client = getattr(self, "qdrant", None)
        if qdrant_client is None:
            return profile

        try:
            info = qdrant_client.get_collection(collection_name)
            params = getattr(getattr(info, "config", None), "params", None)

            vectors = getattr(params, "vectors", None)
            # Named dense vectors: select "dense" if present, otherwise first available.
            if isinstance(vectors, dict):
                keys = [str(k).strip() for k in vectors.keys() if str(k).strip()]
                if "dense" in keys:
                    profile["dense_vector_name"] = "dense"
                elif keys:
                    profile["dense_vector_name"] = keys[0]
            else:
                # Unnamed single-vector collections require plain "vector": [..] requests.
                profile["dense_vector_name"] = None

            sparse_vectors = getattr(params, "sparse_vectors", None)
            if isinstance(sparse_vectors, dict) and sparse_vectors:
                sparse_keys = [str(k).strip() for k in sparse_vectors.keys() if str(k).strip()]
                if sparse_keys:
                    profile["supports_sparse"] = True
                    profile["sparse_vector_name"] = "sparse" if "sparse" in sparse_keys else sparse_keys[0]
        except Exception:
            return profile

        return profile

    def _make_genai_client(self):
        return get_openrouter_client()

    def _make_embedding_client(self):
        return get_openrouter_client()

    def _get_spacy_nlp(self):
        """Lazily load a spaCy NER model if available in environment."""
        if getattr(self, "_spacy_nlp_initialized", False):
            return getattr(self, "_spacy_nlp", None)

        self._spacy_nlp_initialized = True
        if spacy is None:
            self._spacy_nlp = None
            return None

        for model_name in ("en_core_web_sm", "xx_ent_wiki_sm", "de_core_news_sm"):
            try:
                self._spacy_nlp = spacy.load(model_name)
                return self._spacy_nlp
            except Exception:
                continue

        self._spacy_nlp = None
        return None

    def _extract_entity_for_file_info_intent(self, question: str) -> str | None:
        """Extract subject entity from file-info relation phrasing.
        Example: "Which document mentioned Alex Example? Please return ..."
        should return "Alex Example" instead of a weaker surname-only hint.
        """
        text = str(question or "").strip()
        if not text:
            return None

        relation_patterns = [
            r"\b(?:mentioned|mentions|mention|containing|contains|contain|about|concerning|regarding|describes|describing|on)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\s+please\b|\s+return\b|\s+with\b|\?|$)",
            r"\b(?:betreffend|ueber|über|bezogen\s+auf|im\s+zusammenhang\s+mit|zu)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\s+bitte\b|\s+mit\b|\?|$)",
        ]
        for pattern in relation_patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if not m:
                continue
            candidate = self._normalize_entity_name_hint((m.group(1) or "").strip(" ."))
            if candidate and not self._is_suspicious_entity_hint(candidate):
                return candidate
        return None

    def _canonical_attribute_names(self) -> set[str]:
        names = set(self.ATTRIBUTE_DISPLAY_NAMES.keys())
        if AttributeEmbeddingIndex is not None:
            try:
                names.update(getattr(AttributeEmbeddingIndex, "CANONICAL_ATTRIBUTES", {}).keys())
            except Exception:
                pass
        return names

    def _make_discovery_search_client(self):
        return None

    def _get_connection(self):
        try:
            from sql_db import get_connection
        except Exception as exc:
            raise RuntimeError("Database dependencies are unavailable") from exc

        return get_connection()

    @staticmethod
    def _quote_sql_identifier(identifier: str) -> str:
        text = str(identifier or "").strip()
        if not text:
            raise ValueError("SQL identifier must not be empty")
        return '"' + text.replace('"', '""') + '"'

    def _extract_federated_list_query(self, question: str) -> dict[str, Any] | None:
        text = str(question or "").strip()
        if not text:
            return None

        filter_field_hint = ""
        filter_value_hint = ""
        where_match = re.search(
            r"\bwhere\s+(?P<field>[a-zA-Z0-9_\- ]{2,64}?)\s*(?:=|is|equals|like|contains)\s*(?P<value>[^\?]+)",
            text,
            flags=re.IGNORECASE,
        )
        if where_match:
            filter_field_hint = str(where_match.group("field") or "").strip().lower()
            filter_value_hint = str(where_match.group("value") or "").strip().strip("\"'").strip()
            filter_value_hint = re.sub(r"\s+source\s+[a-zA-Z0-9_\-]+\s*$", "", filter_value_hint, flags=re.IGNORECASE).strip()

        query_specs: list[tuple[str, list[re.Pattern[str]]]] = [
            (
                "list",
                [
                    re.compile(
                        r"\b(?:list|show|find|get|give|display|retrieve)\b(?:\s+me)?(?:\s+all)?(?:\s+the)?\s+"
                        r"(?:(?:new|recent|latest)\s+)?"
                        r"(?P<entity>[a-zA-Z_][a-zA-Z0-9_\- ]{1,80}?)\s+"
                        r"(?:from|in|since|during)\s+(?P<year>20\d{2}|19\d{2})\b",
                        flags=re.IGNORECASE,
                    ),
                    re.compile(
                        r"\b(?:new|recent|latest)\s+(?P<entity>[a-zA-Z_][a-zA-Z0-9_\- ]{1,80}?)\s+"
                        r"(?:from|in|since|during)\s+(?P<year>20\d{2}|19\d{2})\b",
                        flags=re.IGNORECASE,
                    ),
                ],
            ),
            (
                "count",
                [
                    re.compile(
                        r"\b(?:how\s+many|count|number\s+of)\s+"
                        r"(?P<entity>[a-zA-Z_][a-zA-Z0-9_\- ]{1,80}?)\s+"
                        r"(?:were\s+)?(?:created|registered|added|entered)?\s*"
                        r"(?:from|in|since|during)\s+(?P<year>20\d{2}|19\d{2})\b",
                        flags=re.IGNORECASE,
                    ),
                ],
            ),
            (
                "summary",
                [
                    re.compile(
                        r"\b(?:summari(?:s|z)e|summary\s+of|overview\s+of)\s+"
                        r"(?P<entity>[a-zA-Z_][a-zA-Z0-9_\- ]{1,80}?)\s+"
                        r"(?:from|in|since|during)\s+(?P<year>20\d{2}|19\d{2})\b",
                        flags=re.IGNORECASE,
                    ),
                ],
            ),
        ]

        match = None
        operation = "list"
        for op_name, patterns in query_specs:
            for pattern in patterns:
                match = pattern.search(text)
                if match:
                    operation = op_name
                    break
            if match:
                break
        if not match:
            return None

        entity_raw = str(match.group("entity") or "").strip().lower()
        year = int(match.group("year"))
        if year < 1900 or year > 2100:
            return None

        source_key_hint = ""
        source_match = re.search(r"\bsource\s+([a-zA-Z0-9_\-]+)\b", text, flags=re.IGNORECASE)
        if source_match:
            source_key_hint = str(source_match.group(1) or "").strip().lower()

        return {
            "operation": operation,
            "entity_hint": entity_raw,
            "year": year,
            "source_key_hint": source_key_hint,
            "filter_field_hint": filter_field_hint,
            "filter_value_hint": filter_value_hint,
        }

    @staticmethod
    def _resolve_inventory_column_hint(field_hint: str, columns: list[dict[str, Any]]) -> str | None:
        hint = str(field_hint or "").strip().lower()
        if not hint:
            return None

        hint_tokens = {tok for tok in re.findall(r"[a-zA-Z0-9]{2,}", hint) if tok}
        if not hint_tokens:
            return None

        best_name = None
        best_score = 0.0
        for item in columns:
            if not isinstance(item, dict):
                continue
            col_name = str(item.get("column_name") or "").strip()
            if not col_name:
                continue
            col_norm = col_name.lower()
            col_tokens = {tok for tok in re.findall(r"[a-zA-Z0-9]{2,}", col_norm) if tok}
            if not col_tokens:
                continue

            score = 0.0
            if hint == col_norm:
                score += 3.0
            if hint in col_norm or col_norm in hint:
                score += 1.5
            overlap = len(hint_tokens & col_tokens)
            if overlap > 0:
                score += float(overlap) / float(max(1, len(hint_tokens)))

            if score > best_score:
                best_score = score
                best_name = col_name

        if best_score <= 0.2:
            return None
        return best_name

    @staticmethod
    def _semantic_query_tokens(text: str) -> set[str]:
        stopwords = {
            "list", "show", "find", "get", "give", "display", "retrieve", "all", "the", "new", "recent", "latest",
            "from", "in", "since", "during", "source", "database", "records", "record", "entries", "entry", "rows", "row",
            "and", "for", "with", "that", "this", "those", "these",
        }
        normalized = str(text or "").strip().lower()
        normalized = normalized.replace("_", " ").replace("-", " ")
        tokens = re.findall(r"[a-zA-Z0-9]{3,}", normalized)
        return {tok for tok in tokens if tok not in stopwords}

    @staticmethod
    def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
        if not vec_a or not vec_b or len(vec_a) != len(vec_b):
            return 0.0
        dot = 0.0
        norm_a = 0.0
        norm_b = 0.0
        for a, b in zip(vec_a, vec_b):
            dot += float(a) * float(b)
            norm_a += float(a) * float(a)
            norm_b += float(b) * float(b)
        if norm_a <= 0.0 or norm_b <= 0.0:
            return 0.0
        return dot / (math.sqrt(norm_a) * math.sqrt(norm_b))

    def _semantic_inventory_candidate_score(
        self,
        *,
        question: str,
        entity_hint: str,
        table_name: str,
        columns: list[dict[str, Any]],
    ) -> float:
        col_names = [str(item.get("column_name") or "").strip().lower() for item in columns if isinstance(item, dict)]
        col_names = [name for name in col_names if name]
        if not table_name and not col_names:
            return 0.0

        candidate_text = " ".join([str(table_name or "").strip().lower(), " ".join(col_names)]).strip()
        if not candidate_text:
            return 0.0

        semantic = 0.0
        embed_text = getattr(self, "embed_text", None)
        if callable(embed_text):
            try:
                q_vec = embed_text(question)
                cand_vec = embed_text(candidate_text)
                if isinstance(q_vec, list) and isinstance(cand_vec, list):
                    semantic = max(0.0, self._cosine_similarity(q_vec, cand_vec))
            except Exception:
                semantic = 0.0

        q_tokens = self._semantic_query_tokens(question)
        c_tokens = self._semantic_query_tokens(candidate_text)
        overlap = 0.0
        if q_tokens and c_tokens:
            overlap = float(len(q_tokens & c_tokens)) / float(max(1, min(len(q_tokens), 8)))

        entity_bonus = 0.0
        entity_norm = str(entity_hint or "").strip().lower()
        if entity_norm:
            table_norm = str(table_name or "").strip().lower()
            if entity_norm in table_norm:
                entity_bonus += 0.35
            if any(entity_norm in col for col in col_names):
                entity_bonus += 0.35

        return (semantic * 0.75) + (overlap * 0.65) + entity_bonus

    @staticmethod
    def _infer_candidate_created_column(columns: list[dict[str, Any]]) -> str | None:
        candidates = [
            "created_at",
            "created_on",
            "creation_date",
            "signup_date",
            "registered_at",
            "inserted_at",
            "entry_date",
            "last_update",
            "updated_at",
            "updated_on",
            "modified_at",
            "invoice_date",
            "document_date",
            "transaction_date",
            "posting_date",
            "date",
        ]
        available = {str(item.get("column_name") or "").strip().lower() for item in columns if isinstance(item, dict)}
        for name in candidates:
            if name in available:
                return name
        return None

    @staticmethod
    def _pick_projection_columns(columns: list[dict[str, Any]]) -> list[str]:
        preferred = [
            "id",
            "customer_id",
            "client_id",
            "account_id",
            "name",
            "customer_name",
            "client_name",
            "company_name",
            "email",
            "created_at",
            "created_on",
            "signup_date",
            "entry_date",
        ]
        available = [str(item.get("column_name") or "").strip() for item in columns if isinstance(item, dict)]
        available_lut = {name.lower(): name for name in available if name}
        selected: list[str] = []
        for p in preferred:
            col = available_lut.get(p)
            if col and col not in selected:
                selected.append(col)
        if not selected:
            selected = available[:6]
        return selected[:8]

    def _resolve_source_password(self, source_key: str) -> str:
        sanitized = re.sub(r"[^A-Za-z0-9]", "_", str(source_key or "").upper())
        per_source_key = f"IDMS_SOURCE_DB_PASSWORD_{sanitized}" if sanitized else ""
        return (
            (os.getenv(per_source_key, "") if per_source_key else "")
            or os.getenv("IDMS_SOURCE_DB_PASSWORD", "")
            or os.getenv("IDMS_DB_PASSWORD", "")
        )

    @staticmethod
    def _extract_generic_entity_hint(question: str) -> str:
        text = str(question or "").strip()
        if not text:
            return ""

        patterns = [
            r"\b(?:list|show|get|find|give|display|retrieve|fetch|search)\b(?:\s+me)?(?:\s+all)?(?:\s+the)?\s+(?P<entity>[a-zA-Z0-9_][a-zA-Z0-9_\- ]{1,80}?)\s+(?:records?|rows?|entries?|details?|data|info|overview)\b",
            r"\b(?:list|show|get|find|give|display|retrieve|fetch|search)\b(?:\s+me)?(?:\s+all)?(?:\s+the)?\s+(?P<entity>[a-zA-Z0-9_][a-zA-Z0-9_\- ]{1,80})\b",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                entity = str(match.group("entity") or "").strip().lower()
                if entity:
                    entity = re.sub(r"\s+from\s+.*$", "", entity, flags=re.IGNORECASE).strip()
                    entity = re.sub(r"\s+(?:with|where|for|in|from|and|or)\s+.*$", "", entity, flags=re.IGNORECASE).strip()
                    if entity:
                        return entity

        lowered = text.lower()
        for stopword in ("show", "list", "find", "give", "display", "retrieve", "fetch", "search", "records", "record", "rows", "row", "entries", "entry", "details", "data", "info"):
            lowered = re.sub(rf"\b{re.escape(stopword)}\b", " ", lowered)
        tokens = re.findall(r"[a-zA-Z0-9][a-zA-Z0-9_\-]{1,}", lowered)
        cleaned = []
        for token in tokens:
            if token in {"me", "all", "the", "a", "an", "of", "for", "in", "from", "with", "and", "or", "source"}:
                continue
            cleaned.append(token)
        if cleaned:
            return " ".join(cleaned[:5])
        return ""

    def _build_generic_federated_entity_plan(self, question: str) -> dict[str, Any] | None:
        text = str(question or "").strip()
        if not text:
            return None

        if re.search(
            r"\b(?:with|their|full|address|addresses|contact|persons?|email|phone|telephone|country|city|state|region|details|where|which|who|what|when|how)\b",
            text,
            flags=re.IGNORECASE,
        ):
            return None

        entity_hint = self._extract_generic_entity_hint(question)
        if not entity_hint:
            return None

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT source_id, source_key, source_name, db_host, db_port, db_name, db_user, db_schema
                        FROM source_database_registry
                        WHERE status = 'active'
                        ORDER BY updated_at DESC, source_id DESC
                        LIMIT 50
                        """
                    )
                    sources = cur.fetchall() or []
                if not sources:
                    return None

                for src in sources:
                    source_id = int(src[0])
                    source_key = str(src[1] or "").strip()

                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT schema_name, table_name, metadata
                            FROM source_schema_inventory
                            WHERE source_id = %s
                            ORDER BY table_name
                            """,
                            (source_id,),
                        )
                        inventory = cur.fetchall() or []

                    best_plan: dict[str, Any] | None = None
                    best_score = 0.0
                    for row in inventory:
                        schema_name = str(row[0] or "").strip()
                        table_name = str(row[1] or "").strip()
                        metadata = row[2] if isinstance(row[2], dict) else {}
                        columns = metadata.get("columns") if isinstance(metadata.get("columns"), list) else []
                        if not schema_name or not table_name or not columns:
                            continue

                        table_lower = table_name.lower()
                        score = 0.0
                        singular_hint = entity_hint[:-1] if entity_hint.endswith("s") else entity_hint
                        if entity_hint and entity_hint in table_lower:
                            score += 5.0
                        if singular_hint and singular_hint in table_lower:
                            score += 3.5
                        if any(token in table_lower for token in ("customer", "client", "supplier", "vendor", "account", "invoice", "document", "contact")):
                            score += 1.5

                        alias_map = self._schema_alias_fallback_map()
                        alias_match = 0.0
                        normalized_hint = self._normalize_alias_key(entity_hint)
                        if normalized_hint:
                            canonical_hint = alias_map.get(normalized_hint)
                            if canonical_hint:
                                alias_match += 3.0
                                for alias_key, alias_value in alias_map.items():
                                    if alias_value == canonical_hint and (alias_key in table_lower or normalized_hint in alias_key):
                                        alias_match += 1.5
                        if alias_match:
                            score += alias_match

                        semantic_score = self._semantic_inventory_candidate_score(
                            question=question,
                            entity_hint=entity_hint,
                            table_name=table_name,
                            columns=columns,
                        )
                        score += max(0.0, semantic_score) * 7.0

                        created_col = self._infer_candidate_created_column(columns)
                        if created_col:
                            score += 2.5

                        col_names = [str(item.get("column_name") or "").strip().lower() for item in columns if isinstance(item, dict)]
                        if entity_hint:
                            for token in re.findall(r"[a-zA-Z0-9]{2,}", entity_hint):
                                if token and any(token in col_name for col_name in col_names):
                                    score += 2.0

                        if score <= 0.0:
                            continue

                        projection_cols = self._pick_projection_columns(columns)
                        if created_col and created_col not in [c.lower() for c in projection_cols]:
                            projection_cols.append(created_col)
                        if not projection_cols:
                            continue

                        candidate = {
                            "source": {
                                "source_id": source_id,
                                "source_key": source_key,
                                "source_name": str(src[2] or "").strip(),
                                "db_host": str(src[3] or "").strip(),
                                "db_port": int(src[4] or 5432),
                                "db_name": str(src[5] or "").strip(),
                                "db_user": str(src[6] or "").strip(),
                                "db_schema": str(src[7] or "").strip() or schema_name,
                            },
                            "query": {
                                "schema_name": schema_name,
                                "table_name": table_name,
                                "created_column": created_col or "created_at",
                                "select_columns": projection_cols[:8],
                                "year": 0,
                                "limit": 200,
                                "operation": "list",
                                "filter_column": None,
                                "filter_value": None,
                            },
                            "routing": {
                                "mode": "federated_read",
                                "reason": "generic_entity_source_match",
                            },
                        }

                        if score > best_score:
                            best_score = score
                            best_plan = candidate

                    if best_plan:
                        return best_plan
        except Exception:
            return None

        return None

    def _build_federated_read_plan(self, question: str) -> dict[str, Any] | None:
        parsed = self._extract_federated_list_query(question)
        if not parsed:
            return None

        operation = str(parsed.get("operation") or "list").strip().lower() or "list"
        entity_hint = str(parsed.get("entity_hint") or "").strip().lower()
        year = int(parsed.get("year") or 0)
        source_key_hint = str(parsed.get("source_key_hint") or "").strip().lower()
        filter_field_hint = str(parsed.get("filter_field_hint") or "").strip().lower()
        filter_value_hint = str(parsed.get("filter_value_hint") or "").strip()
        if year <= 0:
            return None

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT source_id, source_key, source_name, db_host, db_port, db_name, db_user, db_schema
                        FROM source_database_registry
                        WHERE status = 'active'
                        ORDER BY updated_at DESC, source_id DESC
                        LIMIT 50
                        """
                    )
                    sources = cur.fetchall() or []
                if not sources:
                    return None

                for src in sources:
                    source_id = int(src[0])
                    source_key = str(src[1] or "").strip()
                    if source_key_hint and source_key.lower() != source_key_hint:
                        continue

                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT schema_name, table_name, metadata
                            FROM source_schema_inventory
                            WHERE source_id = %s
                            ORDER BY table_name
                            """,
                            (source_id,),
                        )
                        inventory = cur.fetchall() or []

                    best_plan: dict[str, Any] | None = None
                    best_score = 0
                    for row in inventory:
                        schema_name = str(row[0] or "").strip()
                        table_name = str(row[1] or "").strip()
                        metadata = row[2] if isinstance(row[2], dict) else {}
                        columns = metadata.get("columns") if isinstance(metadata.get("columns"), list) else []
                        if not schema_name or not table_name or not columns:
                            continue

                        table_lower = table_name.lower()
                        score = 0.0
                        singular_hint = entity_hint[:-1] if entity_hint.endswith("s") else entity_hint
                        if entity_hint and entity_hint in table_lower:
                            score += 4.0
                        if singular_hint and singular_hint in table_lower:
                            score += 3.0
                        if any(token in table_lower for token in ("customer", "client", "account", "buyer")):
                            score += 2.0

                        semantic_score = self._semantic_inventory_candidate_score(
                            question=question,
                            entity_hint=entity_hint,
                            table_name=table_name,
                            columns=columns,
                        )
                        score += max(0.0, semantic_score) * 6.0

                        created_col = self._infer_candidate_created_column(columns)
                        if created_col:
                            score += 3.0
                        if score <= 0 or not created_col:
                            continue

                        filter_column = None
                        if filter_field_hint and filter_value_hint:
                            filter_column = self._resolve_inventory_column_hint(filter_field_hint, columns)
                            if not filter_column:
                                continue
                            score += 1.0

                        projection_cols = self._pick_projection_columns(columns)
                        if created_col not in [c.lower() for c in projection_cols]:
                            projection_cols.append(created_col)

                        candidate = {
                            "source": {
                                "source_id": source_id,
                                "source_key": source_key,
                                "source_name": str(src[2] or "").strip(),
                                "db_host": str(src[3] or "").strip(),
                                "db_port": int(src[4] or 5432),
                                "db_name": str(src[5] or "").strip(),
                                "db_user": str(src[6] or "").strip(),
                                "db_schema": str(src[7] or "").strip() or schema_name,
                            },
                            "query": {
                                "schema_name": schema_name,
                                "table_name": table_name,
                                "created_column": created_col,
                                "select_columns": projection_cols[:8],
                                "year": year,
                                "limit": 200,
                                "operation": operation,
                                "filter_column": filter_column,
                                "filter_value": filter_value_hint if filter_column else None,
                            },
                            "routing": {
                                "mode": "federated_read",
                                "reason": "direct_source_list_new_entities",
                            },
                        }

                        if score > best_score:
                            best_score = score
                            best_plan = candidate

                    if best_plan:
                        return best_plan
        except Exception:
            return None

        return None

    def _execute_federated_read_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is required for federated source execution")

        source = plan.get("source") if isinstance(plan.get("source"), dict) else {}
        query = plan.get("query") if isinstance(plan.get("query"), dict) else {}

        source_key = str(source.get("source_key") or "").strip()
        password = self._resolve_source_password(source_key)
        if not password:
            raise RuntimeError(
                f"Missing source DB password. Set IDMS_SOURCE_DB_PASSWORD_{re.sub(r'[^A-Za-z0-9]', '_', source_key.upper())} or IDMS_SOURCE_DB_PASSWORD"
            )

        schema_name = str(query.get("schema_name") or "").strip()
        table_name = str(query.get("table_name") or "").strip()
        created_column = str(query.get("created_column") or "").strip()
        select_columns = [str(item).strip() for item in list(query.get("select_columns") or []) if str(item).strip()]
        year = int(query.get("year") or 0)
        limit = max(1, min(int(query.get("limit") or 200), 500))
        operation = str(query.get("operation") or "list").strip().lower() or "list"
        filter_column = str(query.get("filter_column") or "").strip()
        filter_value = str(query.get("filter_value") or "").strip()

        if not schema_name or not table_name or not select_columns:
            raise ValueError("Invalid federated read plan")

        where_clauses: list[str] = []
        params: list[Any] = []
        if year > 0:
            if not created_column:
                raise ValueError("Invalid federated read plan")
            start = date(year, 1, 1)
            end = date(year + 1, 1, 1)
            where_clauses.extend([
                f"{self._quote_sql_identifier(created_column)} >= %s",
                f"{self._quote_sql_identifier(created_column)} < %s",
            ])
            params.extend([start, end])
        if filter_column and filter_value:
            where_clauses.append(f"CAST({self._quote_sql_identifier(filter_column)} AS TEXT) ILIKE %s")
            params.append(f"%{filter_value}%")

        where_sql = " AND ".join(where_clauses)
        table_sql = f"{self._quote_sql_identifier(schema_name)}.{self._quote_sql_identifier(table_name)}"
        select_sql = ", ".join(self._quote_sql_identifier(col) for col in select_columns)
        order_sql = (
            f"ORDER BY {self._quote_sql_identifier(created_column)} DESC " if created_column and year > 0 else ""
        ).strip()

        count_sql = f"SELECT COUNT(*)::bigint AS matched_count FROM {table_sql}"
        if where_sql:
            count_sql = f"{count_sql} WHERE {where_sql}"
        list_sql = f"SELECT {select_sql} FROM {table_sql}"
        if where_sql:
            list_sql = f"{list_sql} WHERE {where_sql}"
        if order_sql:
            list_sql = f"{list_sql} {order_sql}"
        list_sql = f"{list_sql} LIMIT %s"

        conn = psycopg2.connect(
            host=str(source.get("db_host") or "localhost"),
            port=int(source.get("db_port") or 5432),
            database=str(source.get("db_name") or ""),
            user=str(source.get("db_user") or "postgres"),
            password=password,
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(count_sql, tuple(params))
                count_row = cur.fetchone()
                matched_count = int((count_row[0] if isinstance(count_row, (list, tuple)) else count_row) or 0)

                if operation == "count":
                    return {
                        "sql": count_sql,
                        "params": [str(p) for p in params],
                        "rows": [],
                        "count": matched_count,
                        "matched_count": matched_count,
                        "operation": "count",
                    }

                cur.execute(list_sql, tuple(params + [limit]))
                rows = cur.fetchall() or []
                column_names = [desc[0] for desc in list(cur.description or [])]

            output_rows: list[dict[str, Any]] = []
            for row in rows:
                payload: dict[str, Any] = {}
                for idx, col in enumerate(column_names):
                    value = row[idx] if idx < len(row) else None
                    if isinstance(value, (datetime, date)):
                        payload[col] = value.isoformat()
                    else:
                        payload[col] = value
                output_rows.append(payload)

            return {
                "sql": list_sql,
                "params": [str(p) for p in (params + [limit])],
                "rows": output_rows,
                "count": len(output_rows),
                "matched_count": matched_count,
                "operation": operation,
                "count_sql": count_sql,
            }
        finally:
            conn.close()

    @staticmethod
    def _inventory_columns_by_name(columns: list[dict[str, Any]]) -> dict[str, str]:
        return {
            str(item.get("column_name") or "").strip().lower(): str(item.get("column_name") or "").strip()
            for item in columns
            if isinstance(item, dict) and str(item.get("column_name") or "").strip()
        }

    def _try_federated_customer_country_read(self, question: str) -> dict[str, Any] | None:
        match = re.search(
            r"\b(?:list|show|find|get|give|display|retrieve)\b(?:\s+me)?(?:\s+all)?(?:\s+the)?\s+"
            r"(?:customers?|clients?)\s+(?:from|in)\s+(?P<country>[A-Za-z][A-Za-z .-]{1,48})\??$",
            str(question or "").strip(),
            flags=re.IGNORECASE,
        )
        if not match:
            return None

        country = str(match.group("country") or "").strip(" .?")
        if not country:
            return None

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT r.source_key, r.source_name, r.db_host, r.db_port, r.db_name, r.db_user,
                               i.schema_name, i.table_name, i.metadata
                        FROM source_database_registry r
                        JOIN source_schema_inventory i ON i.source_id = r.source_id
                        WHERE r.status = 'active'
                        ORDER BY r.updated_at DESC, i.table_name
                        """
                    )
                    inventory = cur.fetchall() or []
        except Exception:
            return None

        candidate = None
        for row in inventory:
            metadata = row[8] if isinstance(row[8], dict) else {}
            categories = {str(item).strip().lower() for item in metadata.get("query_categories") or []}
            columns = metadata.get("columns") if isinstance(metadata.get("columns"), list) else []
            column_names = self._inventory_columns_by_name(columns)
            if "customer" not in categories or "country" not in column_names:
                continue
            name_column = next(
                (column_names[key] for key in ("customer_name", "client_name", "company_name", "name") if key in column_names),
                None,
            )
            if name_column:
                candidate = (row, name_column, column_names["country"])
                break
        if candidate is None:
            return None

        row, name_column, country_column = candidate
        source_key = str(row[0] or "").strip()
        password = self._resolve_source_password(source_key)
        if not password:
            return None

        table_sql = f"{self._quote_sql_identifier(str(row[6]))}.{self._quote_sql_identifier(str(row[7]))}"
        sql = (
            f"SELECT {self._quote_sql_identifier(name_column)}, {self._quote_sql_identifier(country_column)} "
            f"FROM {table_sql} WHERE CAST({self._quote_sql_identifier(country_column)} AS TEXT) ILIKE %s "
            f"ORDER BY {self._quote_sql_identifier(name_column)} LIMIT %s"
        )
        try:
            with psycopg2.connect(
                host=str(row[2] or "localhost"), port=int(row[3] or 5432), database=str(row[4] or ""),
                user=str(row[5] or "postgres"), password=password, connect_timeout=5,
            ) as source_conn:
                with source_conn.cursor() as cur:
                    cur.execute(sql, (f"%{country}%", 200))
                    rows = cur.fetchall() or []
                    result_columns = [desc[0] for desc in cur.description or []]
        except Exception as exc:
            return {"question": question, "intent": "federated_read", "answer": self._prefix_source(str(exc), "federated_sql_error"), "data": {"sql": sql}, "source": "federated_sql_error"}

        data_rows = [dict(zip(result_columns, value)) for value in rows]
        return {
            "question": question,
            "intent": "federated_read",
            "answer": self._prefix_source(f"Found {len(data_rows)} customers from {country} in source '{source_key}'.", "federated_sql"),
            "data": {"rows": data_rows, "execution": {"sql": sql, "params": [country, 200]}},
            "source": "federated_sql",
            "candidate_count": len(data_rows),
        }

    def _try_federated_customer_order_total_read(self, question: str) -> dict[str, Any] | None:
        match = re.search(
            r"\b(?:show|list|find|get)\b.*\b(?:customers?|clients?)\b.*\b(?:ordered|orders?)\b.*?"
            r"(?:more than|over|greater than)\s*\$?(?P<amount>[0-9][0-9,]*(?:\.\d{1,2})?).*\b(?:last year)\b",
            str(question or ""),
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        amount = float(str(match.group("amount") or "0").replace(",", ""))
        target_year = date.today().year - 1

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT r.source_key, r.db_host, r.db_port, r.db_name, r.db_user,
                               i.schema_name, i.table_name, i.metadata
                        FROM source_database_registry r
                        JOIN source_schema_inventory i ON i.source_id = r.source_id
                        WHERE r.status = 'active'
                        ORDER BY r.updated_at DESC, i.table_name
                        """
                    )
                    inventory = cur.fetchall() or []
        except Exception:
            return None

        by_source: dict[str, list[Any]] = {}
        for row in inventory:
            by_source.setdefault(str(row[0] or ""), []).append(row)

        for source_key, source_rows in by_source.items():
            customer_row = next((row for row in source_rows if "customer" in {str(item).lower() for item in ((row[7] or {}).get("query_categories") or [])}), None)
            order_row = next((row for row in source_rows if "purchase_order" in {str(item).lower() for item in ((row[7] or {}).get("query_categories") or [])}), None)
            if customer_row is None or order_row is None:
                continue

            customer_columns = self._inventory_columns_by_name((customer_row[7] or {}).get("columns") or [])
            order_columns = self._inventory_columns_by_name((order_row[7] or {}).get("columns") or [])
            customer_id = next((customer_columns[key] for key in ("customer_id", "client_id", "id") if key in customer_columns), None)
            customer_name = next((customer_columns[key] for key in ("customer_name", "client_name", "company_name", "name") if key in customer_columns), None)
            amount_column = next((order_columns[key] for key in ("total_amount", "order_total", "amount", "total") if key in order_columns), None)
            date_column = next((order_columns[key] for key in ("order_date", "created_at", "transaction_date", "date") if key in order_columns), None)
            foreign_keys = (order_row[7] or {}).get("foreign_keys") or []
            join_column = next(
                (
                    str(fk.get("column_name") or "") for fk in foreign_keys
                    if str(fk.get("foreign_table_name") or "").lower() == str(customer_row[6] or "").lower()
                    and str(fk.get("foreign_column_name") or "").lower() == str(customer_id or "").lower()
                ),
                None,
            )
            if not all((customer_id, customer_name, amount_column, date_column, join_column)):
                continue

            password = self._resolve_source_password(source_key)
            if not password:
                continue
            customer_sql = f"{self._quote_sql_identifier(str(customer_row[5]))}.{self._quote_sql_identifier(str(customer_row[6]))}"
            order_sql = f"{self._quote_sql_identifier(str(order_row[5]))}.{self._quote_sql_identifier(str(order_row[6]))}"
            sql = (
                f"SELECT c.{self._quote_sql_identifier(customer_name)}, SUM(o.{self._quote_sql_identifier(amount_column)}) AS order_total "
                f"FROM {customer_sql} c JOIN {order_sql} o ON o.{self._quote_sql_identifier(join_column)} = c.{self._quote_sql_identifier(customer_id)} "
                f"WHERE o.{self._quote_sql_identifier(date_column)} >= %s AND o.{self._quote_sql_identifier(date_column)} < %s "
                f"GROUP BY c.{self._quote_sql_identifier(customer_id)}, c.{self._quote_sql_identifier(customer_name)} "
                f"HAVING SUM(o.{self._quote_sql_identifier(amount_column)}) > %s ORDER BY order_total DESC LIMIT %s"
            )
            try:
                with psycopg2.connect(host=str(customer_row[1] or "localhost"), port=int(customer_row[2] or 5432), database=str(customer_row[3] or ""), user=str(customer_row[4] or "postgres"), password=password, connect_timeout=5) as source_conn:
                    with source_conn.cursor() as cur:
                        cur.execute(sql, (date(target_year, 1, 1), date(target_year + 1, 1, 1), amount, 200))
                        values = cur.fetchall() or []
                        columns = [desc[0] for desc in cur.description or []]
            except Exception as exc:
                return {"question": question, "intent": "federated_read", "answer": self._prefix_source(str(exc), "federated_sql_error"), "data": {"sql": sql}, "source": "federated_sql_error"}

            rows = [dict(zip(columns, value)) for value in values]
            return {
                "question": question,
                "intent": "federated_read",
                "answer": self._prefix_source(f"Found {len(rows)} customers with orders over {amount:g} in {target_year} from source '{source_key}'.", "federated_sql"),
                "data": {"rows": rows, "execution": {"sql": sql, "params": [target_year, amount]}},
                "source": "federated_sql",
                "candidate_count": len(rows),
            }
        return None

    def _try_catalog_federated_read(
        self,
        question: str,
        parsed: ParsedQuery | None = None,
    ) -> dict[str, Any] | None:
        parsed = parsed or self.parse_query(question)
        criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        llm_plan = criteria.get("llm_query_plan") if isinstance(criteria.get("llm_query_plan"), dict) else {}
        source_plan = llm_plan.get("source") if isinstance(llm_plan.get("source"), dict) else {}
        source_resolution = criteria.get("source_resolution") if isinstance(criteria.get("source_resolution"), dict) else {}
        resolved_external = bool(source_resolution.get("preferred_scope") == "external")
        if str(source_plan.get("preference") or "auto").strip().lower() != "external" and not resolved_external:
            return None

        root = str(source_plan.get("root_entity") or "").strip()
        targets = llm_plan.get("targets") if isinstance(llm_plan.get("targets"), dict) else {}
        if not root:
            entity_types = [str(item).strip() for item in targets.get("entity_types") or [] if str(item).strip()]
            root = entity_types[0] if entity_types else ""
        if not root:
            root = str(source_resolution.get("root_entity") or "").strip()
        if not root:
            return None

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT r.source_key, r.db_host, r.db_port, r.db_name, r.db_user,
                               i.schema_name, i.table_name, i.metadata
                        FROM source_database_registry r
                        JOIN source_schema_inventory i ON i.source_id = r.source_id
                        WHERE r.status = 'active'
                        ORDER BY r.updated_at DESC, i.table_name
                        """
                    )
                    inventory = cur.fetchall() or []
        except Exception:
            return None

        catalog = [
            {
                "source_key": row[0],
                "db_host": row[1],
                "db_port": row[2],
                "db_name": row[3],
                "db_user": row[4],
                "schema_name": row[5],
                "table_name": row[6],
                "metadata": row[7] if isinstance(row[7], dict) else {},
            }
            for row in inventory
        ]
        attributes = [str(item).strip() for item in targets.get("attributes") or [] if str(item).strip()]
        if not attributes:
            return None
        filters = llm_plan.get("filters") if isinstance(llm_plan.get("filters"), dict) else {}
        logical_filters = filters.get("attribute_filters") if isinstance(filters.get("attribute_filters"), list) else []
        if not logical_filters:
            logical_filters = list(source_resolution.get("filters") or [])
        aggregation = llm_plan.get("aggregation") if isinstance(llm_plan.get("aggregation"), dict) else {}
        logical_plan = {
            "root": root,
            "joins": (
                source_plan.get("join_entities")
                if isinstance(source_plan.get("join_entities"), list)
                else list(source_resolution.get("join_entities") or [])
            ),
            "select": attributes,
            "filters": logical_filters,
            "aggregation": aggregation,
            "limit": aggregation.get("limit") or 200,
        }
        try:
            compiled = compile_read_only_query(logical_plan, catalog)
        except ValueError:
            return None

        source = next((item for item in catalog if str(item.get("source_key") or "") == compiled.source_key), None)
        if source is None:
            return None
        password = self._resolve_source_password(compiled.source_key)
        if not password:
            return None
        try:
            with psycopg2.connect(
                host=str(source.get("db_host") or "localhost"), port=int(source.get("db_port") or 5432),
                database=str(source.get("db_name") or ""), user=str(source.get("db_user") or "postgres"),
                password=password, connect_timeout=5,
            ) as source_conn:
                with source_conn.cursor() as cur:
                    cur.execute(compiled.sql, tuple(compiled.params))
                    rows = cur.fetchall() or []
                    columns = [desc[0] for desc in cur.description or []]
        except Exception as exc:
            return {
                "question": question,
                "intent": "federated_read",
                "answer": self._prefix_source(f"Federated source query failed: {exc}", "federated_sql_error"),
                "data": {"logical_plan": logical_plan},
                "source": "federated_sql_error",
            }

        result_rows = [dict(zip(columns, row)) for row in rows]
        return {
            "question": question,
            "intent": "federated_read",
            "answer": self._prefix_source(f"Found {len(result_rows)} matching records in source '{compiled.source_key}'.", "federated_sql"),
            "data": {"rows": result_rows, "logical_plan": logical_plan, "execution": {"sql": compiled.sql, "params": [str(item) for item in compiled.params]}},
            "source": "federated_sql",
            "candidate_count": len(result_rows),
        }

    def _classify_query_pattern(
        self,
        parsed: ParsedQuery,
        extraction: dict[str, Any],
    ) -> dict[str, Any]:
        """Map a parsed query to a universal pattern class before source resolution."""
        criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        llm_plan = criteria.get("llm_query_plan") if isinstance(criteria.get("llm_query_plan"), dict) else {}
        aggregation = llm_plan.get("aggregation") if isinstance(llm_plan.get("aggregation"), dict) else {}
        output = llm_plan.get("output") if isinstance(llm_plan.get("output"), dict) else {}
        document_hints = extraction.get("document_hints") if isinstance(extraction.get("document_hints"), list) else []
        temporal_hints = extraction.get("temporal_hints") if isinstance(extraction.get("temporal_hints"), list) else []
        intent = str(parsed.intent or "").strip().lower()
        attribute = str(parsed.attribute_name or "").strip().lower()
        relationship = str(parsed.relation_name or "").strip().lower()
        output_shape = str(output.get("shape") or "").strip().lower()
        text_tokens = set(str(value).strip().lower() for value in extraction.get("nouns") or [])
        text_tokens.update(str(value).strip().lower() for value in extraction.get("verbs") or [])
        question_text = " ".join(
            str(value).strip().lower()
            for value in (
                extraction.get("question_text"),
                extraction.get("raw_text"),
                criteria.get("question"),
            )
            if str(value or "").strip()
        )
        categories = {str(value).strip().lower() for value in extraction.get("categories") or []}
        action_items = llm_plan.get("action_items") if isinstance(llm_plan.get("action_items"), dict) else {}
        derived = llm_plan.get("derived") if isinstance(llm_plan.get("derived"), dict) else {}
        source_plan = llm_plan.get("source") if isinstance(llm_plan.get("source"), dict) else {}
        has_comparison = bool(
            intent == "compare"
            or any(token in question_text for token in {"compare", "comparison", "versus", " vs ", "difference", "higher", "lower", "better", "worse"})
        )
        has_diagnostic = bool(
            any(token in question_text for token in {"why", "root cause", "diagnos", "failure", "failed", "error", "incident", "issue"})
            or "diagnostic" in categories
        )
        has_procedure = bool(
            any(token in question_text for token in {"how do i", "how to", "steps", "procedure", "sop", "standard operating", "instructions"})
            or "procedural" in categories
        )
        has_capability = bool(
            any(token in question_text for token in {"can ikos", "can the system", "is it possible", "does ikos support", "capability", "feasible", "architecture"})
            or "capability" in categories
        )
        has_clarification = bool(
            criteria.get("clarification_requested")
            or criteria.get("ambiguous")
            or criteria.get("resolution_status") in {"ambiguous_entity", "ambiguous_value"}
            or any(token in question_text for token in {"which one", "what do you mean", "clarify", "ambiguous", "unclear"})
        )
        has_existence = bool(
            any(token in question_text for token in {"does", "do we have", "is there", "are there", "exists", "exist", "whether"})
            or attribute in {"exists", "existence", "is_active"}
        )
        has_latest_document = bool(
            document_hints
            and any(token in question_text for token in {"latest", "most recent", "current", "newest", "last", "previous"})
        )
        has_document_synthesis = bool(
            document_hints
            and any(token in question_text for token in {"summarize", "summary", "overview", "explain the document", "synthesize", "full content"})
        )
        has_clause_extraction = bool(
            document_hints
            and (criteria.get("geography") or any(token in question_text for token in {"clause", "section", "paragraph", "page", "where in the document"}))
        )
        has_federated_join = bool(
            source_plan.get("preference") == "external"
            and (source_plan.get("join_entities") or any(token in question_text for token in {"across", "between", "join", "from .* and", "source systems"}))
        )

        if has_clarification:
            pattern_key = "clarification_ambiguity"
        elif has_capability:
            pattern_key = "capability_feasibility"
        elif has_procedure:
            pattern_key = "procedural_sop"
        elif has_diagnostic:
            pattern_key = "root_cause_diagnostic"
        elif has_comparison:
            pattern_key = "comparative_analysis"
        elif has_federated_join:
            pattern_key = "federated_join"
        elif bool(action_items.get("requested")) or intent in {"workflow_action", "action"}:
            pattern_key = "workflow_action"
        elif has_latest_document:
            pattern_key = "latest_document_state"
        elif has_document_synthesis:
            pattern_key = "document_synthesis"
        elif has_clause_extraction:
            pattern_key = "spatial_clause_extraction"
        elif has_existence:
            pattern_key = "existence_boolean"
        elif intent in {"aggregate", "compute"} or aggregation.get("function"):
            pattern_key = "metric_aggregation"
        elif document_hints and attribute:
            pattern_key = "document_attribute"
        elif document_hints:
            pattern_key = "document_reference"
        elif criteria.get("geography") or attribute in {"address", "location", "birth_place", "venue"}:
            pattern_key = "spatial_location"
        elif intent in {"criteria_lookup", "filter"} or output_shape in {"list", "table"}:
            pattern_key = "conditional_collection" if criteria.get("attribute_filters") or temporal_hints else "collection"
        elif relationship:
            pattern_key = "relational_graph"
        elif attribute in {"status", "state"}:
            pattern_key = "status"
        elif temporal_hints or any(token in question_text for token in {"schedule", "calendar", "deadline", "date", "when", "next", "upcoming"}):
            pattern_key = "temporal_schedule"
        elif any(token in text_tokens for token in {"duration", "long", "time"}) and not temporal_hints:
            pattern_key = "duration"
        elif any(token in text_tokens for token in {"send", "receive", "responsible", "recipient"}):
            pattern_key = "responsibility"
        elif any(token in text_tokens for token in {"start", "trigger", "calculate", "run"}):
            pattern_key = "workflow_action"
        elif parsed.entity_name and not attribute:
            pattern_key = "identity"
        else:
            pattern_key = "entity_attribute"

        try:
            with self._get_connection() as connection:
                definition = object_db.get_query_pattern_definition(connection, pattern_key)
        except Exception:
            definition = None
        return definition or {
            "pattern_key": pattern_key,
            "intent_key": intent or "attribute_lookup",
            "allowed_source_scopes": ["internal", "external", "document_kb"],
            "allowed_access_methods": ["semantic_lookup", "sql", "rag", "document_facet"],
            "reason": "pattern_registry_unavailable",
        }

    def _resolve_query_sources(self, parsed: ParsedQuery, question: str = "") -> ParsedQuery:
        """Resolve logical query concepts through the category registry before retrieval."""
        criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        extraction = parsed.parse_artifact if isinstance(parsed.parse_artifact, dict) else self._build_parse_artifact(
            str(question or parsed.entity_name or ""),
            default_entity_hint=parsed.entity_name,
            detected_language=parsed.language,
            intent=parsed.intent,
            criteria=criteria,
        )
        pattern = self._classify_query_pattern(parsed, extraction)
        criteria["query_pattern"] = pattern
        llm_plan = criteria.get("llm_query_plan") if isinstance(criteria.get("llm_query_plan"), dict) else {}
        source_plan = llm_plan.get("source") if isinstance(llm_plan.get("source"), dict) else {}
        targets = llm_plan.get("targets") if isinstance(llm_plan.get("targets"), dict) else {}
        category_names: list[str] = []
        for value in (
            criteria.get("entity_type"),
            parsed.entity_name,
            parsed.relation_name,
            source_plan.get("root_entity"),
        ):
            normalized = str(value or "").strip().lower()
            if normalized and normalized not in category_names:
                category_names.append(normalized)
        for value in targets.get("entity_types") or []:
            normalized = str(value or "").strip().lower()
            if normalized and normalized not in category_names:
                category_names.append(normalized)

        attributes = [str(parsed.attribute_name or "").strip().lower()] if parsed.attribute_name else []
        attributes.extend(
            str(value or "").strip().lower()
            for value in targets.get("attributes") or []
            if str(value or "").strip()
        )
        attributes.extend(
            str(value or "").strip().lower()
            for value in extraction.get("attributes") or []
            if str(value or "").strip()
        )
        attributes = sorted(set(attributes))
        document_type = str(
            criteria.get("document_type")
            or ((llm_plan.get("geography") or {}).get("document_type") if isinstance(llm_plan.get("geography"), dict) else "")
            or ""
        ).strip().lower()

        solf_inference: dict[str, Any] = {}
        solf_category_inference = getattr(self, "solf_category_inference", None)
        if callable(solf_category_inference):
            try:
                inferred = solf_category_inference(
                    {
                        "source_hint": (
                            "document"
                            if bool(criteria.get("geography") or criteria.get("action_items") or document_type)
                            else "external"
                            if bool(source_plan.get("preference") == "external" or criteria.get("external_query"))
                            else str(source_plan.get("preference") or "auto").strip().lower()
                        ),
                        "entities": extraction.get("entities") if isinstance(extraction.get("entities"), list) else [],
                        "entity_types": sorted(set(category_names)),
                        "attributes": attributes,
                        "constraints": extraction.get("constraints") if isinstance(extraction.get("constraints"), list) else [],
                        "document_hints": extraction.get("document_hints") if isinstance(extraction.get("document_hints"), list) else [],
                        "temporal_hints": extraction.get("temporal_hints") if isinstance(extraction.get("temporal_hints"), list) else [],
                        "intent": str(parsed.intent or "semantic_lookup").strip().lower(),
                    }
                )
                if isinstance(inferred, dict):
                    solf_inference = inferred
            except Exception:
                LOGGER.exception("SOLF category inference failed")

        try:
            with self._get_connection() as connection:
                candidates = object_db.resolve_category_registry_for_query(
                    connection,
                    entity_type=str(criteria.get("entity_type") or "").strip() or None,
                    attribute_names=attributes,
                    document_type=document_type or None,
                    category_names=category_names,
                    limit=100,
                )
        except Exception:
            candidates = []

        allowed_scopes = {str(value).strip().lower() for value in pattern.get("allowed_source_scopes") or []}
        allowed_methods = {str(value).strip().lower() for value in pattern.get("allowed_access_methods") or []}
        candidates = [
            item for item in candidates
            if (
                not allowed_scopes
                or not str(item.get("source_scope") or "").strip()
                or str(item.get("source_scope") or "").strip().lower() in allowed_scopes
            )
            and (
                not allowed_methods
                or not str(item.get("access_method") or "").strip()
                or str(item.get("access_method") or "").strip().lower() in allowed_methods
            )
        ]
        external_candidates = [item for item in candidates if item.get("source_scope") == "external"]
        internal_candidates = [item for item in candidates if item.get("source_scope") == "internal"]
        document_requested = bool(criteria.get("geography") or criteria.get("action_items") or extraction.get("document_hints"))
        aggregation = llm_plan.get("aggregation") if isinstance(llm_plan.get("aggregation"), dict) else {}
        output = llm_plan.get("output") if isinstance(llm_plan.get("output"), dict) else {}
        collection_requested = (
            str(parsed.intent or "").strip().lower() in {"criteria_lookup", "aggregate", "filter"}
            or bool(aggregation.get("function"))
            or str(output.get("shape") or "").strip().lower() in {"list", "table"}
        )
        named_scalar_fact = bool(parsed.entity_name and attributes and not collection_requested)
        scope_priority = {"external": 3, "document_kb": 2, "internal": 1}
        if document_requested:
            preferred_scope = "document_kb"
        elif named_scalar_fact and internal_candidates:
            preferred_scope = "internal"
        else:
            preferred_scope = max(
                (str(item.get("source_scope") or "") for item in candidates),
                key=lambda value: scope_priority.get(value, 0),
                default=None,
            )
        if solf_inference.get("source_scope") in {"external", "document_kb", "internal"}:
            inferred_scope = str(solf_inference.get("source_scope") or "")
            if inferred_scope == "document_kb" and document_requested:
                preferred_scope = inferred_scope
            elif inferred_scope == "external" and external_candidates and not document_requested and not named_scalar_fact:
                preferred_scope = inferred_scope

        root_entity = str(source_plan.get("root_entity") or "").strip()
        if not root_entity:
            root_entity = next(
                (
                    str(item.get("category_name") or "").strip()
                    for item in external_candidates
                    if not item.get("column_name")
                ),
                "",
            )
        join_entities = [
            str(item.get("category_name") or "").strip()
            for item in external_candidates
            if not item.get("column_name") and str(item.get("category_name") or "").strip() != root_entity
        ]
        filters = [
            {
                "field": str(item.get("attribute") or item.get("column_name") or "").strip(),
                "operator": "eq",
                "value": item.get("value"),
            }
            for item in (criteria.get("attribute_filters") or [])
            if isinstance(item, dict) and item.get("value") is not None
        ]
        selected_candidates = [
            item for item in candidates
            if str(item.get("source_scope") or "").strip().lower() == str(preferred_scope or "").strip().lower()
        ]
        selected_access_method = next(
            (str(item.get("access_method") or "").strip().lower() for item in selected_candidates if str(item.get("access_method") or "").strip()),
            None,
        )
        execution_plan = {
            "pattern_key": pattern.get("pattern_key"),
            "intent_key": pattern.get("intent_key"),
            "source_scope": preferred_scope,
            "access_method": selected_access_method,
            "authorized": bool(selected_candidates),
            "candidate_count": len(selected_candidates),
            "reason": "category_registry_validated" if selected_candidates else "no_validated_category_source",
        }
        criteria["source_resolution"] = {
            "resolved": bool(candidates),
            "preferred_scope": preferred_scope,
            "candidates": candidates,
            "root_entity": root_entity or None,
            "join_entities": sorted(set(join_entities)),
            "filters": filters,
            "reason": (
                "named_scalar_internal_precedence"
                if named_scalar_fact and internal_candidates
                else "category_registry_resolution"
                if candidates
                else "no_category_registry_match"
            ),
            "solf_inference": solf_inference,
            "extraction_artifact": extraction,
            "query_pattern": pattern,
            "execution_plan": execution_plan,
        }
        parsed.criteria = criteria
        return parsed

    def _try_federated_sql_read(
        self,
        question: str,
        parsed: ParsedQuery | None = None,
    ) -> dict[str, Any] | None:
        if parsed is not None:
            criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
            source_resolution = criteria.get("source_resolution") if isinstance(criteria.get("source_resolution"), dict) else {}
            execution_plan = source_resolution.get("execution_plan") if isinstance(source_resolution.get("execution_plan"), dict) else {}
            if not (
                execution_plan.get("authorized")
                and execution_plan.get("source_scope") == "external"
                and execution_plan.get("access_method") == "sql"
            ):
                return None
        catalog_result = self._try_catalog_federated_read(question, parsed=parsed)
        if catalog_result is not None:
            return catalog_result
        plan = self._build_federated_read_plan(question) or self._build_generic_federated_entity_plan(question)
        if not isinstance(plan, dict):
            return None
        try:
            execution = self._execute_federated_read_plan(plan)
        except Exception as exc:
            return {
                "question": question,
                "intent": "federated_read",
                "answer": self._prefix_source(
                    f"Federated source query was planned but failed: {str(exc)}",
                    "federated_sql_error",
                ),
                "data": {
                    "federated_plan": plan,
                    "error": str(exc),
                },
                "source": "federated_sql_error",
                "candidate_count": 0,
            }

        rows = execution.get("rows") if isinstance(execution.get("rows"), list) else []
        source = plan.get("source") if isinstance(plan.get("source"), dict) else {}
        source_key = str(source.get("source_key") or "source").strip() or "source"
        year = int(((plan.get("query") or {}).get("year") if isinstance(plan.get("query"), dict) else 0) or 0)
        operation = str(((plan.get("query") or {}).get("operation") if isinstance(plan.get("query"), dict) else "list") or "list").strip().lower() or "list"

        def _preview_row_text(row: dict[str, Any]) -> str:
            if not isinstance(row, dict):
                return ""
            name = ""
            for key in ("customer_name", "entity_name", "company_name", "name", "title"):
                value = str(row.get(key) or "").strip()
                if value:
                    name = value
                    break
            if not name:
                values = [str(value or "").strip() for value in row.values() if str(value or "").strip()]
                name = values[0] if values else ""
            if not name:
                return ""
            country = str(row.get("country") or row.get("country_name") or "").strip()
            if country:
                return f"{name} ({country})"
            return name

        def _preview_rows_text(rows_value: list[dict[str, Any]], limit: int = 10) -> str:
            preview_items = [text for text in (_preview_row_text(row) for row in rows_value[:limit]) if text]
            return "; ".join(preview_items)

        if operation == "count":
            count_value = int(execution.get("matched_count") or execution.get("count") or 0)
            if year > 0:
                answer = f"Found {count_value} matching records in source '{source_key}' for {year}."
            else:
                answer = f"Found {count_value} matching records in source '{source_key}'."
            return {
                "question": question,
                "intent": "federated_read",
                "answer": self._prefix_source(answer, "federated_sql"),
                "data": {
                    "rows": [],
                    "count": count_value,
                    "federated_plan": plan,
                    "execution": {
                        "sql": execution.get("sql"),
                        "params": execution.get("params"),
                    },
                },
                "source": "federated_sql",
                "candidate_count": count_value,
            }

        if rows:
            preview = rows[:5]
            matched_count = int(execution.get("matched_count") or len(rows))
            preview_text = _preview_rows_text(rows, limit=10)
            if year > 0:
                if operation == "summary":
                    answer = f"Summary for source '{source_key}' in {year}: {matched_count} matching records (showing {len(preview)} preview rows)."
                else:
                    answer = f"Found {len(rows)} records in source '{source_key}' for {year}."
            elif operation == "summary":
                answer = f"Summary for source '{source_key}': {matched_count} matching records (showing {len(preview)} preview rows)."
            else:
                answer = f"Found {len(rows)} records in source '{source_key}'."
            if preview_text:
                answer = f"{answer} {preview_text}."
            if len(rows) > len(preview):
                answer = f"{answer} Showing first {len(preview)} of {len(rows)} records."
            return {
                "question": question,
                "intent": "federated_read",
                "answer": self._prefix_source(answer, "federated_sql"),
                "data": {
                    "rows": rows,
                    "preview": preview,
                    "matched_count": matched_count,
                    "federated_plan": plan,
                    "execution": {
                        "sql": execution.get("sql"),
                        "params": execution.get("params"),
                        "count_sql": execution.get("count_sql"),
                    },
                },
                "source": "federated_sql",
                "candidate_count": len(rows),
            }

        if year > 0:
            answer = f"No records found in source '{source_key}' for {year}."
        else:
            answer = f"No records found in source '{source_key}'."
        return {
            "question": question,
            "intent": "federated_read",
            "answer": self._prefix_source(answer, "federated_sql"),
            "data": {
                "rows": [],
                "federated_plan": plan,
                "execution": {
                    "sql": execution.get("sql"),
                    "params": execution.get("params"),
                },
            },
            "source": "federated_sql",
            "candidate_count": 0,
        }

    def _detect_query_language(self, question: str) -> str:
        """Detect the language of a query without hardcoding language names.
        Returns language code ('en', 'de', etc.). Defaults to 'en'.
        """
        lowered = question.lower()
        german_markers = [
            r"\b(?:wie\s+lautet|was\s+ist|wer\s+ist|wo\s+ist|wann\s+ist|wie\s+viel|zeige|gib\s+mir|finde|nenn(?:e|t)?|erkl[aä]re)\b",
            r"\b(?:der|die|das|den|dem|des|im|im\s+handelsregisterauszug|statutendatum|handelsregisterauszug|registerauszug|anteile|kontaktperson|adresse)\b",
        ]
        if any(re.search(pattern, lowered) for pattern in german_markers):
            return "de"

        # Check for English keywords
        if re.search(r"\b(?:what\s+is|what's|show|get|find|who\s+are|shareholders|of|for|when\s+was)\b", lowered):
            return "en"
        return "en"

    def _extract_degree_institution_query(self, text: str, detected_language: str) -> ParsedQuery | None:
        """Detect generic education/degree/qualification questions without degree-specific hardcoding."""
        lowered = text.lower().strip()
        stripped = lowered
        default_entity_hint = None
        context_entity_hint = None
        has_contact_cue = False
        has_email_cue = False
        has_process_cue = False

        institution_patterns = [
            r"^\s*where\s+did\s+(.+?)\s+(?:get|earn|obtain|receive|complete|finish|do|take)\s+(?:his|her|their)?\s*(?:degree|qualification|qualifications|diploma|certificate|certification|doctorate|ph\.?\s*d\.?|phd|b\.?\s*sc\.?|m\.?\s*sc\.?|b\.?\s*a\.?|m\.?\s*a\.?|sc\.?\s*d\.?|d\.?\s*sc\.?)\s*\??\s*$",
            r"^\s*from\s+where\s+did\s+(.+?)\s+(?:get|earn|obtain|receive|complete|finish)\s+(?:his|her|their)?\s*(?:degree|qualification|qualifications|diploma|certificate|certification|doctorate|ph\.?\s*d\.?|phd|b\.?\s*sc\.?|m\.?\s*sc\.?|b\.?\s*a\.?|m\.?\s*a\.?|sc\.?\s*d\.?|d\.?\s*sc\.?)\s*\??\s*$",
            r"^\s*wo\s+hat\s+(.+?)\s+(?:sein|seine|seinen|ihr|ihre|ihren|den|die|das)?\s*(?:abschluss|qualifikation|diplom|zertifikat|doktorgrad|doktorat|promotion|b\.?\s*sc\.?|m\.?\s*sc\.?|b\.?\s*a\.?|m\.?\s*a\.?|ph\.?\s*d\.?|phd)\s+(?:erhalten|gemacht|abgeschlossen|erworben)\s*\??\s*$",
            r"^\s*von\s+wo\s+hat\s+(.+?)\s+(?:sein|seine|seinen|ihr|ihre|ihren|den|die|das)?\s*(?:abschluss|qualifikation|diplom|zertifikat|doktorgrad|doktorat|promotion|b\.?\s*sc\.?|m\.?\s*sc\.?|b\.?\s*a\.?|m\.?\s*a\.?|ph\.?\s*d\.?|phd)\s+(?:erhalten|gemacht|abgeschlossen|erworben)\s*\??\s*$",
        ]
        for pattern in institution_patterns:
            match = re.match(pattern, lowered, flags=re.IGNORECASE)
            if not match:
                continue
            candidate = (match.group(1) or "").strip(" .?")
            candidate = re.sub(r"^(?:dr\.?|prof\.?|mr\.?|mrs\.?|ms\.)\s+", "", candidate, flags=re.IGNORECASE)
            entity_name = self._normalize_entity_name_hint(candidate)
            if not entity_name:
                return None
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=entity_name,
            )
        if has_contact_cue and (has_email_cue or has_process_cue):
            return None

        # Identifier lookup questions (for example EORI/VAT) are scalar
        # attribute intents and should not be treated as table cell requests.
        if self._looks_like_identifier_request(stripped):
            return None

        # Time-windowed list-style requests (expenses/payments/invoices, etc.)
        # belong to criteria parsing rather than row/column extraction.
        if self._extract_time_window(stripped) and re.search(
            r"\b(?:expense|expenses|payment|payments|invoice|invoices|receipt|receipts|transaction|transactions|travel|travelling)\b",
            lowered,
        ):
            return None

        # Guardrail: relation-style finance document queries (for example
        # "show me bill related to mobile service for Aphotonix GmbH") should
        # go through criteria/document retrieval, not strict table grounding.
        if re.search(r"\b(?:bill|bills|invoice|invoices|receipt|receipts|document|documents)\b", lowered):
            relation_cue = re.search(
                r"\b(?:related\s+to|concerning|about|regarding|containing|contains|mentions?|describes?|for)\b",
                lowered,
            )
            if relation_cue:
                return None

        # Guardrail: value-style amount/cost questions with contextual phrases
        # (for example "amount for mobile services for Aphotonix GmbH") are
        # semantic value lookups, not table row/column requests.
        asks_value_amount = bool(
            re.search(
                r"\b(?:amount|total|cost|price|value|due|sum|charge|fee|betrag|gesamt|kosten|preis|wert|summe)\b",
                lowered,
            )
        )
        has_context_preposition_chain = bool(
            re.search(r"\b(?:for|of|in|von|fuer|für)\b.*\b(?:for|of|in|von|fuer|für)\b", lowered)
        )
        explicit_table_cue = bool(
            re.search(r"\b(?:row|column|cell|table|line\s*item|zeile|spalte|tabelle)\b", lowered)
        )
        generic_relation_list_style = bool(
            re.search(
                r"\b(?:show|list|get|find|give\s+me|zeige|zeig|gib\s+mir|finde)\b.*\b(?:of|for|von|fuer|für)\b",
                lowered,
                flags=re.IGNORECASE,
            )
        )
        if generic_relation_list_style and not explicit_table_cue and not asks_value_amount:
            return None
        if asks_value_amount and has_context_preposition_chain and not explicit_table_cue:
            return None

        context_entity_hint = default_entity_hint
        context_match = re.match(r"^\s*in\s+the\s+(.+?),\s*(.+)$", stripped, flags=re.IGNORECASE)
        if context_match:
            context_entity_hint = self._normalize_entity_name_hint(str(context_match.group(1) or "").strip()) or context_entity_hint
            stripped = str(context_match.group(2) or "").strip()

        patterns = [
            r"^\s*(?:what(?:'s| is)|show me|show|get me|get|find me|find|give me|how much is)\s+(?:the\s+)?(.+?)\s+(?:in|for)\s+(.+?)(?:\?|$)",
            r"^\s*(?:what(?:'s| is| was)|show me|show|get me|get|find me|find|give me|how much is|how much was)\s+(?:the\s+)?(?:budget|amount|expenditure|cost)?\s*(?:for|of)\s+(.+?)\s+(?:in|for)\s+(.+?)(?:\?|$)",
            r"^\s*(?:what(?:'s| is| was)|show me|show|get me|get|find me|find|give me|how much is|how much was)\s+(?:the\s+)?(.+?)\s+of\s+(.+?)(?:\?|$)",
            r"^\s*(?:wie\s+hoch\s+ist|was\s+ist|zeige|gib\s+mir|finde)\s+(?:der|die|das|den|dem|des)?\s*(.+?)\s+(?:in|im|für|fuer)\s+(.+?)(?:\?|$)",
        ]

        for pattern in patterns:
            match = re.match(pattern, stripped, flags=re.IGNORECASE)
            if not match:
                continue

            row_raw = str(match.group(1) or "").strip(" .,:;\"'[]()")
            column_raw = str(match.group(2) or "").strip(" .,:;\"'[]()")
            row_raw = re.sub(r"^(?:me|us)\s+", "", row_raw, flags=re.IGNORECASE)
            # Pattern-specific swap for "<column> of <row>" shape.
            if "\\s+of\\s+" in pattern:
                row_raw, column_raw = column_raw, row_raw
            if not row_raw or not column_raw:
                continue

            row_tokens = re.findall(r"[A-Za-z0-9äöüÄÖÜß]+", row_raw)
            column_tokens = re.findall(r"[A-Za-z0-9äöüÄÖÜß]+", column_raw)
            if len(row_tokens) < 2 and not default_entity_hint:
                continue
            if len(column_tokens) > 8:
                continue

            column_lower = column_raw.lower()
            if re.search(r"\b(?:last|past|previous|prior|next|current|recent)\b", column_lower) and re.search(
                r"\b(?:day|days|week|weeks|month|months|year|years|quarter|quarters|month-to-date|year-to-date)\b",
                column_lower,
            ):
                continue

            entity_name = context_entity_hint or default_entity_hint or self._normalize_entity_name_hint(row_raw)
            if not entity_name:
                continue

            resolved_attrs = self._expand_query_attribute_candidates(column_raw, entity_name)
            if not resolved_attrs:
                resolved_attr, _ = self._resolve_schema_term(column_raw, entity_name)
                resolved_attr = str(resolved_attr or "").strip()
                if resolved_attr:
                    resolved_attrs = [resolved_attr]

            if not resolved_attrs:
                normalized_column = self._normalize_attribute_name(column_raw)
                if normalized_column:
                    resolved_attrs = [normalized_column]

            if not resolved_attrs:
                continue

            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=entity_name,
                attribute_name=resolved_attrs[0],
                confidence=0.87,
                language=detected_language,
                attribute_names=resolved_attrs,
                criteria={
                    "parse_method": "table_cell_pattern",
                    "table_request": True,
                    "table_parent_entity": context_entity_hint,
                    "table_row_label": row_raw,
                    "table_column": column_raw,
                },
            )

        return None

    def _normalize_for_matching(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKC", str(text or "").lower().strip())
        normalized = (
            normalized
            .replace("ä", "a")
            .replace("ö", "o")
            .replace("ü", "u")
            .replace("ß", "ss")
        )
        return " ".join(normalized.split())

    def _normalize_table_text(self, text: str) -> str:
        cleaned = str(text or "")
        cleaned = cleaned.replace("<br/>", " ").replace("<br>", " ").replace("&nbsp;", " ")
        cleaned = re.sub(r"\*\*(.*?)\*\*", r"\1", cleaned)
        cleaned = self._normalize_for_matching(cleaned)
        cleaned = re.sub(r"[^a-z0-9 ]+", " ", cleaned)
        return " ".join(cleaned.split())

    def _parse_markdown_tables(self, markdown_text: str) -> list[dict[str, Any]]:
        lines = [str(line).rstrip() for line in str(markdown_text or "").splitlines()]
        tables: list[dict[str, Any]] = []

        def _split_cells(line: str) -> list[str]:
            row = line.strip()
            if row.startswith("|"):
                row = row[1:]
            if row.endswith("|"):
                row = row[:-1]
            return [cell.strip() for cell in row.split("|")]

        def _is_separator_line(line: str) -> bool:
            cells = _split_cells(line)
            if not cells:
                return False
            non_empty = [cell for cell in cells if cell.strip()]
            if not non_empty:
                return False
            return all(re.match(r"^:?-{3,}:?$", cell.strip()) for cell in non_empty)

        i = 0
        while i + 1 < len(lines):
            line = lines[i]
            next_line = lines[i + 1]
            if "|" not in line or "|" not in next_line or not _is_separator_line(next_line):
                i += 1
                continue

            headers = _split_cells(line)
            rows: list[list[str]] = []
            j = i + 2
            while j < len(lines):
                row_line = lines[j]
                if "|" not in row_line or not row_line.strip():
                    break
                if _is_separator_line(row_line):
                    j += 1
                    continue
                rows.append(_split_cells(row_line))
                j += 1

            if headers and rows:
                tables.append({"headers": headers, "rows": rows})
            i = j

        return tables

    def _lookup_grounded_table_cell(
        self,
        conn,
        *,
        parent_entity: str | None,
        row_label: str,
        column_label: str,
        scope_doc_ids: list[int] | None = None,
    ) -> dict[str, Any] | None:
        row_norm = self._normalize_table_text(row_label)
        col_norm = self._normalize_table_text(column_label)
        parent_norm = self._normalize_table_text(parent_entity or "")
        if not row_norm or not col_norm:
            return None

        row_tokens = [tok for tok in row_norm.split() if tok]
        col_tokens = [tok for tok in col_norm.split() if tok]

        scope_doc_ids = [int(x) for x in (scope_doc_ids or []) if isinstance(x, int)]

        def _load_docs(doc_scope: list[int]) -> list[tuple[Any, ...]]:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT d.doc_id, d.doc_name, d.doc_path, d.markdown_path, d.doc_key, d.doc_theme
                    FROM document d
                    WHERE COALESCE(d.status, 'active') = 'active'
                      AND COALESCE(d.markdown_path, '') <> ''
                      AND (
                        COALESCE(array_length(%s::int[], 1), 0) = 0
                        OR d.doc_id = ANY(%s)
                      )
                    ORDER BY d.entry_date DESC NULLS LAST
                    LIMIT 120
                    """,
                    (doc_scope, doc_scope),
                )
                return list(cur.fetchall() or [])

        def _scan_docs(docs: list[tuple[Any, ...]], current_best: dict[str, Any] | None) -> dict[str, Any] | None:
            best_local = current_best
            for doc_id, doc_name, doc_path, markdown_path, doc_key, doc_theme in docs:
                md_path = str(markdown_path or "").strip()
                if not md_path:
                    continue
                try:
                    md_text = Path(md_path).read_text(encoding="utf-8")
                except Exception:
                    continue

                tables = self._parse_markdown_tables(md_text)
                for table in tables:
                    headers = [str(h or "") for h in list(table.get("headers") or [])]
                    rows = [list(r or []) for r in list(table.get("rows") or [])]
                    if not headers or not rows:
                        continue

                    table_blob = self._normalize_table_text(
                        " ".join(headers + [" ".join(str(c or "") for c in row) for row in rows[:120]])
                    )
                    parent_bonus = 4 if (parent_norm and parent_norm in table_blob) else 0

                    best_col_idx = None
                    best_col_score = 0
                    for idx, header in enumerate(headers):
                        hn = self._normalize_table_text(header)
                        if not hn:
                            continue
                        score = sum(1 for tok in col_tokens if tok in hn)
                        if col_norm in hn:
                            score += 6
                        if score > best_col_score:
                            best_col_score = score
                            best_col_idx = idx
                    if best_col_idx is None or best_col_score <= 0:
                        continue

                    for row in rows:
                        if not row:
                            continue
                        row_head = self._normalize_table_text(row[0])
                        if not row_head:
                            continue
                        row_score = sum(1 for tok in row_tokens if tok in row_head)
                        if row_norm in row_head:
                            row_score += 8
                        if row_score <= 0 or best_col_idx >= len(row):
                            continue
                        value = str(row[best_col_idx] or "").strip()
                        if not value:
                            continue

                        total_score = row_score * 10 + best_col_score + parent_bonus
                        candidate = {
                            "doc_id": int(doc_id),
                            "doc_name": str(doc_name or ""),
                            "doc_path": str(doc_path or ""),
                            "doc_key": str(doc_key or ""),
                            "doc_theme": str(doc_theme or ""),
                            "markdown_path": md_path,
                            "row_label": str(row[0] or "").strip(),
                            "column_label": str(headers[best_col_idx] or "").strip(),
                            "attribute_value": value,
                            "score": total_score,
                        }
                        if best_local is None or int(candidate["score"]) > int(best_local.get("score") or 0):
                            best_local = candidate
            return best_local

        best = _scan_docs(_load_docs(scope_doc_ids), None)
        if best is None and scope_doc_ids:
            best = _scan_docs(_load_docs([]), best)

        return best

    def _looks_like_fuel_station_invoice_question(self, question: str) -> bool:
        text = str(question or "").strip().lower()
        asks_station = bool(re.search(r"\b(?:tankstelle|tankstellen|gas\s+station|gas\s+stations|fuel\s+station|fuel\s+stations|petrol\s+station|petrol\s+stations)\b", text))
        mentions_invoice = bool(re.search(r"\b(?:tankrechnung|tankrechnungen|fuel\s+invoice|fuel\s+receipt|petrol\s+invoice|gas\s+invoice|rechnung)\b", text))
        return asks_station and mentions_invoice

    def _extract_recipient_hint_for_fuel_station_question(self, question: str) -> str:
        text = str(question or "").strip()
        patterns = [
            r"\ban\s+([A-Za-z0-9äöüÄÖÜß .&_\-/]+?)(?=\s+(?:vom|im|in|aus|from|for|during)\b|[\?\.!]|$)",
            r"\bto\s+([A-Za-z0-9äöüÄÖÜß .&_\-/]+?)(?=\s+(?:from|in|during)\b|[\?\.!]|$)",
            r"\bfor\s+([A-Za-z0-9äöüÄÖÜß .&_\-/]+?)(?=\s+(?:from|in|during)\b|[\?\.!]|$)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            candidate = self._normalize_entity_name_hint(str(match.group(1) or "").strip(" ."))
            if candidate:
                return candidate
        return ""

    def _extract_month_year_hint(self, question: str) -> tuple[int, int] | None:
        text = str(question or "").strip().lower()
        month_match = re.search(
            r"(?:\bim\s+|\bvom\s+|\bin\s+|\bfrom\s+)([a-zA-ZäöüÄÖÜß]+)(?:\s+(\d{4}|\d{2}))?",
            text,
            flags=re.IGNORECASE,
        )
        if not month_match:
            return None
        month_num = self._parse_month_name(month_match.group(1))
        if not month_num:
            return None
        year_raw = str(month_match.group(2) or "").strip()
        if year_raw:
            year = int(year_raw)
            if year < 100:
                year += 2000
        else:
            year = date.today().year
        return (month_num, year)

    def _parse_compact_doc_date(self, raw: str) -> date | None:
        text = str(raw or "").strip()
        if not text:
            return None
        for fmt in ("%d.%m.%y", "%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        return None

    def _try_fuel_station_invoice_answer(self, question: str) -> dict[str, Any] | None:
        if not self._looks_like_fuel_station_invoice_question(question):
            return None

        recipient_hint = self._extract_recipient_hint_for_fuel_station_question(question)
        month_year = self._extract_month_year_hint(question)
        if not recipient_hint:
            return None

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT d.doc_id, d.doc_key, d.doc_path, d.markdown_path, d.doc_date, d.doc_theme
                    FROM document d
                    WHERE COALESCE(d.status, 'active') = 'active'
                      AND COALESCE(d.markdown_path, '') <> ''
                    ORDER BY d.entry_date DESC NULLS LAST, d.doc_id DESC
                    LIMIT 250
                    """
                )
                docs = list(cur.fetchall() or [])

        station_hits: list[dict[str, Any]] = []
        recipient_norm = self._normalize_table_text(recipient_hint)

        for doc_id, doc_key, doc_path, markdown_path, doc_date, doc_theme in docs:
            md_path = str(markdown_path or "").strip()
            if not md_path:
                continue
            try:
                md_text = Path(md_path).read_text(encoding="utf-8")
            except Exception:
                continue

            md_norm = self._normalize_table_text(md_text)
            if recipient_norm and recipient_norm not in md_norm:
                continue
            if not re.search(r"\b(?:tankstelle|tankstellen|diesel|fuel|tankkarte|benzin|petrol)\b", md_text, flags=re.IGNORECASE):
                continue

            tables = self._parse_markdown_tables(md_text)
            for table in tables:
                headers = [str(h or "").strip() for h in list(table.get("headers") or [])]
                rows = [list(r or []) for r in list(table.get("rows") or [])]
                if not headers or not rows:
                    continue

                header_norms = [self._normalize_table_text(item) for item in headers]
                station_idx = next((idx for idx, item in enumerate(header_norms) if item in {"tankstelle", "gas station", "fuel station", "petrol station", "station"}), None)
                date_idx = next((idx for idx, item in enumerate(header_norms) if item in {"datum", "date"}), None)
                if station_idx is None:
                    continue

                for row in rows:
                    if station_idx >= len(row):
                        continue
                    station_value = str(row[station_idx] or "").strip()
                    if not station_value:
                        continue

                    row_date = None
                    if date_idx is not None and date_idx < len(row):
                        row_date = self._parse_compact_doc_date(str(row[date_idx] or "").strip())
                    if month_year and row_date is not None:
                        if row_date.month != month_year[0] or row_date.year != month_year[1]:
                            continue

                    station_hits.append(
                        {
                            "doc_id": int(doc_id),
                            "doc_key": str(doc_key or ""),
                            "doc_path": str(doc_path or ""),
                            "doc_theme": str(doc_theme or ""),
                            "station": station_value,
                            "row_date": row_date.isoformat() if row_date else None,
                        }
                    )

        unique_hits: list[dict[str, Any]] = []
        seen_station_keys: set[str] = set()
        for item in station_hits:
            key = self._normalize_table_text(str(item.get("station") or ""))
            if not key or key in seen_station_keys:
                continue
            seen_station_keys.add(key)
            unique_hits.append(item)

        language = "de" if str(self._detect_query_language(question) or "en").lower().startswith("de") else "en"
        if unique_hits:
            station_names = [str(item.get("station") or "").strip() for item in unique_hits if str(item.get("station") or "").strip()]
            if language == "de":
                answer = f"Auf der Tankrechnung an {recipient_hint} sind folgende Tankstellen aufgeführt: {', '.join(station_names)}."
            else:
                answer = f"The fuel invoice for {recipient_hint} lists these stations: {', '.join(station_names)}."
            return {
                "question": question,
                "intent": "document_station_lookup",
                "answer": self._prefix_source(answer, "markdown_table"),
                "data": {
                    "recipient_name": recipient_hint,
                    "stations": unique_hits,
                    "month": month_year[0] if month_year else None,
                    "year": month_year[1] if month_year else None,
                },
                "source": "markdown_table",
            }

        if language == "de":
            month_desc = " im angefragten Zeitraum" if month_year else ""
            answer = f"Es gibt keine Tankstellen, die auf der Tankrechnung an {recipient_hint}{month_desc} aufgeführt sind."
        else:
            month_desc = " in the requested period" if month_year else ""
            answer = f"There are no gas stations listed on the fuel invoice for {recipient_hint}{month_desc}."
        return {
            "question": question,
            "intent": "document_station_lookup",
            "answer": self._prefix_source(answer, "not_found"),
            "data": {
                "recipient_name": recipient_hint,
                "stations": [],
                "month": month_year[0] if month_year else None,
                "year": month_year[1] if month_year else None,
            },
            "source": "not_found",
        }

    def _extract_entity_from_pattern_template(self, pattern_text: str, user_text: str) -> str:
        """Extract wildcard entity with tolerant fallback for umlaut/ascii variants."""
        template_regex = "^" + re.escape(pattern_text).replace(r"\*", r"(.+?)") + "$"
        template_hit = re.match(template_regex, user_text, flags=re.IGNORECASE)
        if template_hit:
            return " ".join([g.strip() for g in template_hit.groups() if str(g).strip()])

        normalized_pattern = self._normalize_for_matching(pattern_text)
        normalized_text = self._normalize_for_matching(user_text)
        normalized_regex = "^" + re.escape(normalized_pattern).replace(r"\*", r"(.+?)") + "$"
        normalized_hit = re.match(normalized_regex, normalized_text, flags=re.IGNORECASE)
        if normalized_hit:
            return " ".join([g.strip() for g in normalized_hit.groups() if str(g).strip()])
        return ""

    def _extract_pattern_wildcard_groups(self, pattern_text: str, user_text: str) -> list[str]:
        """Extract ordered wildcard groups from a template pattern against user text."""
        template_regex = "^" + re.escape(pattern_text).replace(r"\*", r"(.+?)") + "$"
        template_hit = re.match(template_regex, user_text, flags=re.IGNORECASE)
        if template_hit:
            return [str(g or "").strip() for g in template_hit.groups()]

        normalized_pattern = self._normalize_for_matching(pattern_text)
        normalized_text = self._normalize_for_matching(user_text)
        normalized_regex = "^" + re.escape(normalized_pattern).replace(r"\*", r"(.+?)") + "$"
        normalized_hit = re.match(normalized_regex, normalized_text, flags=re.IGNORECASE)
        if normalized_hit:
            return [str(g or "").strip() for g in normalized_hit.groups()]
        return []

    def _substitute_wildcard_refs(self, value: Any, groups: list[str]) -> Any:
        """Replace placeholders like $1, $2 in nested structures with wildcard group captures."""
        if isinstance(value, str):
            out = value
            for idx, group in enumerate(groups, start=1):
                out = out.replace(f"${idx}", str(group or ""))
            return out
        if isinstance(value, list):
            return [self._substitute_wildcard_refs(item, groups) for item in value]
        if isinstance(value, dict):
            return {k: self._substitute_wildcard_refs(v, groups) for k, v in value.items()}
        return value

    def _get_attribute_display_name(self, canonical_attr: str, language: str = "en") -> str:
        """Get the human-friendly display name for an attribute in specified language.
        Without hardcoding English/German, uses the mapping dictionary.
        """
        if canonical_attr not in self.ATTRIBUTE_DISPLAY_NAMES:
            # Fallback for ingestion-driven attributes not yet mirrored in display map.
            return canonical_attr.replace("_", " ")
        
        names = self.ATTRIBUTE_DISPLAY_NAMES[canonical_attr].get(language, [])
        if names:
            return names[0]  # Return first (primary) display name for this language
        
        # Fallback to English if language not available
        fallback_names = self.ATTRIBUTE_DISPLAY_NAMES[canonical_attr].get("en", [])
        return fallback_names[0] if fallback_names else canonical_attr.replace("_", " ")
    def _get_entity_attribute_names(self, entity_name: str) -> list[str]:
        """Fetch attribute type names actually stored in the DB for entity_name."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT a.attr_type
                        FROM object_instance oi
                        JOIN attribute a
                          ON a.src_type = 'object' AND a.src_id = oi.object_id
                        WHERE LOWER(oi.object_name) = LOWER(%s)
                                                    AND COALESCE(oi.status, 'active') = 'active'
                                                    AND oi.valid_from <= CURRENT_DATE
                                                    AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                                                    AND a.valid_from <= CURRENT_DATE
                                                    AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
                          AND a.attr_type IS NOT NULL
                        ORDER BY a.attr_type
                        """,
                        (entity_name,),
                    )
                    return [row[0] for row in cur.fetchall()]
        except Exception:
            return []

    def _get_entity_schema_terms(self, entity_name: str) -> dict[str, list[str]]:
        """Fetch both attribute and relationship names stored for entity_name."""
        return {
            "attributes": self._get_entity_attribute_names(entity_name),
            "relationships": self._get_entity_relationship_names(entity_name),
        }

    def _get_entity_relationship_names(self, entity_name: str) -> list[str]:
        """Fetch relationship names actually stored for entity_name."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT rel.relationship_name
                        FROM object_instance oi
                        JOIN object_relationship rel
                          ON rel.src_object_id = oi.object_id
                          OR rel.tar_object_id = oi.object_id
                        WHERE LOWER(oi.object_name) = LOWER(%s)
                                                    AND COALESCE(oi.status, 'active') = 'active'
                                                    AND oi.valid_from <= CURRENT_DATE
                                                    AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                                                    AND rel.valid_from <= CURRENT_DATE
                                                    AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                          AND rel.relationship_name IS NOT NULL
                        ORDER BY rel.relationship_name
                        """,
                        (entity_name,),
                    )
                    return [row[0] for row in cur.fetchall()]
        except Exception:
            return []

    @staticmethod
    def _relationship_name_alias_variants(raw_name: str | None) -> set[str]:
        text = str(raw_name or "").strip().lower()
        if not text:
            return set()

        cleaned = re.sub(r"[^a-z0-9]+", "_", text)
        cleaned = re.sub(r"_+", "_", cleaned).strip("_")
        if not cleaned:
            return set()

        variants: set[str] = {cleaned}
        for candidate in [cleaned, cleaned.rstrip("s"), cleaned.removesuffix("_of"), cleaned.removesuffix("_to")]:
            candidate = re.sub(r"_+", "_", candidate).strip("_")
            if candidate:
                variants.add(candidate)
                if candidate.endswith("_persons"):
                    variants.add(candidate[:-1])
                if candidate.endswith("_people"):
                    variants.add(candidate[:-1] + "_person")
                if candidate.endswith("_contacts"):
                    variants.add(candidate[:-1] + "_contact")
                if candidate.endswith("_person"):
                    variants.add(candidate)
                    variants.add(candidate + "s")
        if "related" in cleaned or "contact" in cleaned or "person" in cleaned or "people" in cleaned:
            variants.update({"contact_person", "related_person", "person", "people", "contact", "contacts"})
        if "contact_person" in cleaned or "related_person" in cleaned:
            variants.add("contact_person")
        if "person" in cleaned and "contact" not in cleaned and "related" not in cleaned:
            variants.add("person")
        return {v for v in variants if v}

    def _match_entity_relationship_name(self, entity_name: str, requested_name: str) -> str | None:
        """Return the stored relationship name if the entity actually has a matching relationship."""
        normalized_requested = self._normalize_alias_key(requested_name)
        if not normalized_requested:
            return None

        alias_candidates = self._relationship_name_alias_variants(requested_name)
        if not alias_candidates:
            alias_candidates = {normalized_requested}

        for relationship_name in self._get_entity_relationship_names(entity_name):
            relationship_variants = self._relationship_name_alias_variants(relationship_name)
            if not relationship_variants:
                relationship_variants = {self._normalize_alias_key(relationship_name)}

            # Direct match plus generic alias compatibility.
            if (
                self._normalize_alias_key(relationship_name) == normalized_requested
                or normalized_requested in self._normalize_alias_key(relationship_name)
                or self._normalize_alias_key(relationship_name) in normalized_requested
                or bool(alias_candidates & relationship_variants)
            ):
                return relationship_name
        return None

    def _sql_related_object_lookup(
        self,
        conn,
        entity_name: str,
        relationship_name: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                                SELECT
                                        base.object_name AS base_name,
                                        related.object_name AS related_name,
                                        rel.relationship_name,
                                        COALESCE(MAX(doc.doc_id), 0) AS doc_id,
                                        COALESCE(MAX(doc.doc_key), '') AS doc_key,
                                        COALESCE(MAX(doc.doc_path), '') AS doc_path
                                FROM object_instance base
                                JOIN object_relationship rel
                                    ON rel.src_object_id = base.object_id OR rel.tar_object_id = base.object_id
                                JOIN object_instance related
                                    ON related.object_id = CASE
                                                WHEN rel.src_object_id = base.object_id THEN rel.tar_object_id
                                                ELSE rel.src_object_id
                                         END
                                LEFT JOIN part part_rel
                                    ON part_rel.relationship_id = rel.relationship_id
                                LEFT JOIN document doc
                                    ON doc.doc_id = part_rel.doc_id
                                WHERE LOWER(base.object_name) = LOWER(%s)
                                                                        AND COALESCE(base.status, 'active') = 'active'
                                                                        AND base.valid_from <= CURRENT_DATE
                                                                        AND (base.valid_until IS NULL OR base.valid_until > CURRENT_DATE)
                                                                        AND COALESCE(related.status, 'active') = 'active'
                                                                        AND related.valid_from <= CURRENT_DATE
                                                                        AND (related.valid_until IS NULL OR related.valid_until > CURRENT_DATE)
                                                                        AND rel.valid_from <= CURRENT_DATE
                                                                        AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                                    AND LOWER(rel.relationship_name) = LOWER(%s)
                                GROUP BY base.object_name, related.object_name, rel.relationship_name
                                ORDER BY related.object_name
                LIMIT 20
                """,
                (entity_name, relationship_name),
            )
            rows = cur.fetchall()

        if not rows:
            return None

        related_names = [str(row[1] or "").strip() for row in rows if str(row[1] or "").strip()]
        if not related_names:
            return None

        value: Any = related_names[0] if len(related_names) == 1 else related_names
        return {
            "entity_name": rows[0][0],
            "attribute_name": self._normalize_attribute_name(relationship_name) or relationship_name,
            "attribute_value": value,
            "relationship_name": rows[0][2],
            "doc_id": rows[0][3] or None,
            "doc_name": rows[0][4] or None,
            "doc_path": rows[0][5] or None,
        }

    def _resolve_entity_name_for_lookup(self, conn, entity_name: str | None) -> str | None:
        """Resolve short/alias entity hints to canonical object_name when unique."""
        details = self._resolve_entity_name_for_lookup_details(conn, entity_name)
        if not details:
            return None
        return str(details.get("resolved_name") or details.get("entity_hint") or "").strip() or None

    def _resolve_entity_object_scope(self, conn, entity_name: str | None) -> dict[str, Any]:
        """Resolve an entity name into object-id scope, including alias-linked instances."""
        hint = self._normalize_entity_name_hint(entity_name)
        if not hint or self._is_suspicious_entity_hint(hint):
            return {"entity_hint": "", "resolved_name": None, "object_ids": [], "object_names": []}

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT object_id, object_name, class_name
                FROM object_instance
                WHERE LOWER(object_name) = LOWER(%s)
                  AND COALESCE(status, 'active') = 'active'
                  AND valid_from <= CURRENT_DATE
                  AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                ORDER BY entry_date DESC
                LIMIT 1
                """,
                (hint,),
            )
            seed = cur.fetchone()

        if seed is None:
            details = self._resolve_entity_name_for_lookup_details(conn, hint)
            resolved_name = str((details or {}).get("resolved_name") or "").strip()
            if not resolved_name:
                return {
                    "entity_hint": hint,
                    "resolved_name": None,
                    "object_ids": [],
                    "object_names": [],
                    "similar_candidates": self._find_similar_person_candidates(conn, hint, limit=5),
                }
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT object_id, object_name, class_name
                    FROM object_instance
                    WHERE LOWER(object_name) = LOWER(%s)
                      AND COALESCE(status, 'active') = 'active'
                      AND valid_from <= CURRENT_DATE
                      AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                    ORDER BY entry_date DESC
                    LIMIT 1
                    """,
                    (resolved_name,),
                )
                seed = cur.fetchone()

        if seed is None:
            return {"entity_hint": hint, "resolved_name": None, "object_ids": [], "object_names": []}

        seed_object_id = int(seed[0])
        seed_name = str(seed[1] or "").strip()

        rows: list[tuple[Any, Any]] = []
        alias_table_exists = False
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.object_alias')")
            alias_table_exists = cur.fetchone()[0] is not None

        if alias_table_exists:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
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
                        SELECT oi.object_id, oi.object_name
                        FROM object_instance oi
                        WHERE oi.object_id IN (SELECT DISTINCT object_id FROM alias_graph)
                          AND COALESCE(oi.status, 'active') = 'active'
                          AND oi.valid_from <= CURRENT_DATE
                          AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                        ORDER BY oi.object_name, oi.object_id
                        """,
                        (seed_object_id,),
                    )
                    rows = cur.fetchall() or []
            except Exception:
                rows = []

        if not rows:
            return {
                "entity_hint": hint,
                "resolved_name": seed_name,
                "object_ids": [seed_object_id],
                "object_names": [seed_name],
                "seed_object_id": seed_object_id,
            }

        object_ids = [int(row[0]) for row in rows]
        object_names = [str(row[1] or "").strip() for row in rows if str(row[1] or "").strip()]
        return {
            "entity_hint": hint,
            "resolved_name": seed_name,
            "seed_object_id": seed_object_id,
            "object_ids": object_ids,
            "object_names": object_names,
        }

    def _find_similar_person_candidates(self, conn, name_hint: str, limit: int = 5) -> list[str]:
        hint = str(name_hint or "").strip()
        if not hint:
            return []

        # Use search_objects with merged variant resolution so merged names
        # (e.g. "Alex Example") are followed to their canonical active objects.
        candidates = object_db.search_objects(
            connection=conn,
            name_query=hint,
            class_name="person",
            limit=max(1, int(limit)),
            include_merged_variants=True,
        )
        return [str(c["object_name"]).strip() for c in candidates if str(c.get("object_name") or "").strip()]

    def _resolve_entity_name_for_lookup_details(self, conn, entity_name: str | None) -> dict[str, Any] | None:
        """Resolve entity hints and include ambiguity metadata for generic disambiguation."""
        title_hint = self._extract_title_prefix(entity_name)
        hint = self._normalize_entity_name_hint(entity_name)
        if not hint or self._is_suspicious_entity_hint(hint):
            return None

        details: dict[str, Any] = {
            "entity_hint": hint,
            "resolved_name": None,
            "ambiguous": False,
            "candidates": [],
            "top_score": None,
            "second_score": None,
            "score_gap": None,
        }

        # Tier 1: Exact match
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT object_name, class_name
                FROM object_instance
                WHERE LOWER(object_name) = LOWER(%s)
                                    AND COALESCE(status, 'active') = 'active'
                                    AND valid_from <= CURRENT_DATE
                                    AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                LIMIT 1;
                """,
                (hint,),
            )
            row = cur.fetchone()
            if row and str(row[0] or "").strip():
                resolved = str(row[0]).strip()
                details["resolved_name"] = resolved

                # Exact active-name hits should resolve directly. Re-asking for
                # clarification here caused loops like "Alex Example" -> choose
                # "Alex Example" -> ambiguous again, even though the user already
                # selected an exact candidate name.
                with conn.cursor() as dup_cur:
                    dup_cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM object_instance
                        WHERE LOWER(object_name) = LOWER(%s)
                          AND COALESCE(status, 'active') = 'active'
                          AND valid_from <= CURRENT_DATE
                          AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                        """,
                        (hint,),
                    )
                    exact_count = int((dup_cur.fetchone() or [0])[0] or 0)

                if exact_count > 1:
                    details["ambiguous"] = True
                    details["resolved_name"] = None
                    details["candidates"] = [resolved]
                    details["top_score"] = None
                    details["second_score"] = None
                    details["score_gap"] = None
                    details["reason"] = "duplicate_exact_name"
                    return details

                return details

        # Tier 1.5: Merged-object exact match (Option A)
        # If the hint exactly matches a merged object, resolve it to its canonical
        # active object via the merged_record alias link.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT oi.object_id
                FROM object_instance oi
                WHERE LOWER(oi.object_name) = LOWER(%s)
                  AND COALESCE(oi.status, 'active') = 'merged'
                LIMIT 1
                """,
                (hint,),
            )
            merged_row = cur.fetchone()

        if merged_row:
            canonical_id = object_db.resolve_merged_object_to_canonical(conn, int(merged_row[0]))
            canonical_obj = object_db.get_object_instance_by_id(conn, canonical_id)
            if canonical_obj and canonical_obj.get("status") == "active":
                details["resolved_name"] = str(canonical_obj["object_name"]).strip()
                details["resolved_from_merged"] = int(merged_row[0])
                return details

        # Tier 2: Initial expansion (e.g., "A.E." → "Alex Example", "J. Doe" → "Jane Doe")
        initial_expansion = self._try_resolve_initials(conn, hint)
        if initial_expansion:
            details["resolved_name"] = initial_expansion
            return details

        # Tier 2.5: Deterministic person token alignment.
        # For hints like "Alex Example", prefer person candidates with matching
        # given-name and surname tokens before broader fuzzy/search ranking.
        person_token_match = self._resolve_person_name_by_tokens_db(conn, hint)
        if person_token_match:
            details["resolved_name"] = person_token_match
            details["resolved_from_person_token_match"] = True
            return details

        # Tier 3: Similarity-based search via search_objects (Option C)
        # Replaces raw LIKE token query; handles merged variants and uses proper
        # similarity scoring rather than the last-token heuristic.
        search_results = object_db.search_objects(
            connection=conn,
            name_query=hint,
            class_name=None,
            limit=5,
            include_merged_variants=True,
        )
        rows = [str(r["object_name"]).strip() for r in search_results if str(r.get("object_name") or "").strip()]

        unique_rows = sorted(set(rows))
        if len(unique_rows) == 1:
            details["resolved_name"] = unique_rows[0]
            return details

        if not unique_rows:
            fuzzy_legal_entity = self._fuzzy_resolve_legal_entity_name(conn, hint)
            if fuzzy_legal_entity:
                details["resolved_name"] = fuzzy_legal_entity
                details["resolved_from_fuzzy_legal_entity"] = True
                return details
            details["resolved_name"] = hint
            return details

        ranked = self._rank_entity_candidates(conn, hint, unique_rows, title_hint)
        if not ranked:
            details["resolved_name"] = hint
            return details

        details["candidates"] = [item[3] for item in ranked[:5]]

        top_score = int(ranked[0][0])
        details["top_score"] = top_score

        if len(ranked) == 1:
            details["resolved_name"] = ranked[0][3]
            return details

        second_score = int(ranked[1][0])
        score_gap = top_score - second_score
        details["second_score"] = second_score
        details["score_gap"] = score_gap

        hint_tokens = [t for t in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]+", hint) if t]
        min_gap = 2 if len(hint_tokens) > 1 else 3
        if score_gap >= min_gap:
            details["resolved_name"] = ranked[0][3]
            return details

        details["ambiguous"] = True
        details["resolved_name"] = None
        return details

    def _extract_title_prefix(self, raw: str | None) -> str | None:
        text = str(raw or "").strip()
        if not text:
            return None

        m = re.match(r"^(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|herr|frau)\b", text, flags=re.IGNORECASE)
        if not m:
            return None
        return m.group(1).lower().rstrip(".")

    def _fuzzy_resolve_legal_entity_name(self, conn, hint: str | None) -> str | None:
        """Resolve minor typos for legal-entity names sharing a company suffix."""
        import difflib

        raw_hint = str(hint or "").strip()
        if not raw_hint:
            return None

        match = re.match(
            r"^(.+?)\s+(GmbH|AG|SA|Sarl|Ltd\.?|Inc\.?|LLC)$",
            raw_hint,
            flags=re.IGNORECASE,
        )
        if not match:
            return None

        hint_core = str(match.group(1) or "").strip()
        suffix = str(match.group(2) or "").strip().lower()
        if len(hint_core) < 3:
            return None

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT object_name
                FROM object_instance
                WHERE COALESCE(status, 'active') = 'active'
                  AND valid_from <= CURRENT_DATE
                  AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                  AND LOWER(object_name) LIKE %s
                ORDER BY object_name
                LIMIT 200
                """,
                (f"% {suffix}",),
            )
            candidates = [str(row[0] or "").strip() for row in cur.fetchall() if str(row[0] or "").strip()]

        if not candidates:
            return None

        def _norm_core(value: str) -> str:
            return re.sub(r"[^a-z0-9]", "", str(value or "").lower())

        hint_norm = _norm_core(hint_core)
        if not hint_norm:
            return None

        scored: list[tuple[float, str]] = []
        for name in candidates:
            cm = re.match(
                r"^(.+?)\s+(GmbH|AG|SA|Sarl|Ltd\.?|Inc\.?|LLC)$",
                name,
                flags=re.IGNORECASE,
            )
            if not cm:
                continue
            core = _norm_core(cm.group(1) or "")
            if not core:
                continue
            ratio = difflib.SequenceMatcher(None, hint_norm, core).ratio()
            scored.append((ratio, name))

        if not scored:
            return None

        scored.sort(key=lambda item: item[0], reverse=True)
        top_ratio, top_name = scored[0]
        second_ratio = scored[1][0] if len(scored) > 1 else 0.0

        # Conservative acceptance to avoid over-correcting unrelated companies.
        if top_ratio >= 0.84 and (top_ratio - second_ratio) >= 0.05:
            return top_name
        return None

    def _select_person_candidate_by_token_alignment(self, hint: str, candidates: list[str]) -> str | None:
        """Return best person candidate by first-name/surname token alignment."""
        hint_tokens = [t.lower() for t in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]+", str(hint or "")) if t]
        if len(hint_tokens) < 2:
            return None

        hint_surname = hint_tokens[-1]
        hint_given = hint_tokens[:-1]

        best_name: str | None = None
        best_score: int | None = None
        tie = False

        for raw_name in candidates:
            name = str(raw_name or "").strip()
            if not name:
                continue

            tokens = [t.lower() for t in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]+", name) if t]
            if len(tokens) < 2:
                continue

            has_comma = "," in name
            surname = tokens[0] if has_comma else tokens[-1]
            given = tokens[1:] if has_comma else tokens[:-1]
            if surname != hint_surname or not given:
                continue

            given_matches = 0
            for tok in hint_given:
                if len(tok) == 1:
                    if any(g.startswith(tok) for g in given):
                        given_matches += 1
                else:
                    if tok in given or any(g.startswith(tok) for g in given):
                        given_matches += 1
            if given_matches == 0:
                continue

            # First-given-name alignment gets extra weight for person disambiguation.
            first_bonus = 0
            first_hint = hint_given[0] if hint_given else ""
            first_given = given[0] if given else ""
            if first_hint and first_given:
                if first_given == first_hint:
                    first_bonus = 3
                elif first_given.startswith(first_hint) or first_hint.startswith(first_given):
                    first_bonus = 2

            score = (given_matches * 10) + first_bonus
            if best_score is None or score > best_score:
                best_name = name
                best_score = score
                tie = False
            elif score == best_score:
                tie = True

        if tie:
            return None
        return best_name

    def _resolve_person_name_by_tokens_db(self, conn, hint: str) -> str | None:
        """Resolve person hints by deterministic token matching against active people."""
        hint_tokens = [t.lower() for t in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]+", str(hint or "")) if t]
        if len(hint_tokens) < 2:
            return None

        surname = hint_tokens[-1]
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT object_name
                FROM object_instance
                WHERE LOWER(class_name) = 'person'
                  AND COALESCE(status, 'active') = 'active'
                  AND valid_from <= CURRENT_DATE
                  AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                  AND LOWER(object_name) LIKE LOWER(%s)
                ORDER BY entry_date DESC
                LIMIT 300
                """,
                (f"%{surname}%",),
            )
            rows = [str(row[0] or "").strip() for row in cur.fetchall() if str(row[0] or "").strip()]

        if not rows:
            return None

        return self._select_person_candidate_by_token_alignment(hint, rows)

    def _candidate_profile_richness(self, conn, candidate_name: str) -> int:
        """Return number of active attributes for exact object_name candidate."""
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM attribute a
                    JOIN object_instance oi
                      ON a.src_type = 'object' AND a.src_id = oi.object_id
                    WHERE LOWER(oi.object_name) = LOWER(%s)
                      AND COALESCE(oi.status, 'active') = 'active'
                      AND oi.valid_from <= CURRENT_DATE
                      AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                      AND a.valid_from <= CURRENT_DATE
                      AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
                    """,
                    (candidate_name,),
                )
                row = cur.fetchone()
                return int(row[0] or 0) if row else 0
        except Exception:
            return 0

    def _candidate_has_doctoral_signal(self, conn, candidate_name: str) -> bool:
        """Check if candidate has doctoral signal in attributes or relationship metadata."""
        degree_patterns = ["%phd%", "%ph.d%", "%doctor%", "%doktor%"]
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM attribute a
                    JOIN object_instance oi
                      ON a.src_type = 'object' AND a.src_id = oi.object_id
                    WHERE LOWER(oi.object_name) = LOWER(%s)
                      AND COALESCE(oi.status, 'active') = 'active'
                      AND oi.valid_from <= CURRENT_DATE
                      AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                      AND a.valid_from <= CURRENT_DATE
                      AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
                      AND (
                            LOWER(COALESCE(a.attr_type, '')) LIKE ANY(%s)
                         OR LOWER(COALESCE(a.attr_json->>'value', '')) LIKE ANY(%s)
                      )
                    LIMIT 1
                    """,
                    (candidate_name, degree_patterns, degree_patterns),
                )
                if cur.fetchone():
                    return True

                cur.execute(
                    """
                    SELECT 1
                    FROM object_instance oi
                    JOIN object_relationship rel
                      ON rel.src_object_id = oi.object_id OR rel.tar_object_id = oi.object_id
                    WHERE LOWER(oi.object_name) = LOWER(%s)
                      AND COALESCE(oi.status, 'active') = 'active'
                      AND oi.valid_from <= CURRENT_DATE
                      AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                      AND rel.valid_from <= CURRENT_DATE
                      AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                      AND LOWER(COALESCE(rel.metadata->>'degree', '')) LIKE ANY(%s)
                    LIMIT 1
                    """,
                    (candidate_name, degree_patterns),
                )
                return bool(cur.fetchone())
        except Exception:
            return False

    def _rank_entity_candidates(self, conn, hint: str, candidates: list[str], title_hint: str | None = None) -> list[tuple[int, int, int, str]]:
        """Rank candidate entities by generic lexical and profile signals."""
        hint_tokens = [t.lower() for t in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]+", str(hint or "")) if t]
        if not hint_tokens:
            return []
        hint_has_comma = "," in str(hint or "")

        hint_surname = hint_tokens[-1]
        hint_given = hint_tokens[:-1] if len(hint_tokens) > 1 else []
        needs_doctor_signal = title_hint in {"dr", "prof"}

        scored: list[tuple[int, int, int, str]] = []
        for raw_name in candidates:
            name = str(raw_name or "").strip()
            if not name:
                continue

            tokens = [t.lower() for t in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]+", name) if t]
            if not tokens:
                continue

            has_comma = "," in name
            surname = tokens[0] if has_comma else tokens[-1]
            given = tokens[1:] if has_comma else tokens[:-1]

            score = 0
            if surname == hint_surname:
                score += 5

            for idx, token in enumerate(hint_given):
                if not token:
                    continue

                if len(token) == 1:
                    if any(g.startswith(token) for g in given):
                        score += 2
                else:
                    if token in given:
                        score += 8
                    elif any(g.startswith(token) for g in given):
                        score += 4

                if idx == 0 and given:
                    first_given = given[0]
                    if first_given == token or first_given.startswith(token) or (len(token) == 1 and first_given.startswith(token)):
                        score += 2

            if not has_comma:
                score += 1
            elif not hint_has_comma:
                score -= 3
            else:
                score += 1

            if needs_doctor_signal and self._candidate_has_doctoral_signal(conn, name):
                score += 4

            richness = self._candidate_profile_richness(conn, name)
            score += min(richness, 2)

            if score > 0 and surname == hint_surname:
                scored.append((score, len(tokens), richness, name))

        scored.sort(key=lambda item: (-item[0], -item[1], -item[2], item[3]))
        return scored

    def _choose_best_entity_candidate(self, conn, hint: str, candidates: list[str], title_hint: str | None = None) -> str | None:
        """Choose best entity candidate when multiple partial matches exist."""
        scored = self._rank_entity_candidates(conn, hint, candidates, title_hint)

        if not scored:
            return None

        scored.sort(key=lambda item: (-item[0], -item[1], -item[2], item[3]))
        if len(scored) == 1 or scored[0][0] > scored[1][0]:
            return scored[0][3]
        return None

    def _try_resolve_initials(self, conn, hint: str) -> str | None:
        """Match initials like 'A.E.' to full names like 'Alex Example'."""
        # Pattern: one or more letters with dots, followed by space and a full name token
        # E.g., "C.Y. Pang", "B. Pang", "J.R.R. Tolkien"
        initial_pattern = r"^((?:[A-Z]\.\s*)+)\s+(.+)$"
        m = re.match(initial_pattern, hint.strip(), flags=re.IGNORECASE)
        if not m:
            return None
        
        initials_str = m.group(1)  # "C.Y." or "B."
        surname = m.group(2).strip()  # "Pang"
        
        # Parse initials from string like "C.Y." or "B."
        initials = [c.upper() for c in re.findall(r"[A-Za-z]", initials_str)]
        if not initials:
            return None
        
        # Query for people with matching surname candidates.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT object_name
                FROM object_instance
                WHERE COALESCE(status, 'active') = 'active'
                  AND valid_from <= CURRENT_DATE
                  AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                  AND (LOWER(object_name) LIKE LOWER(%s) 
                       OR LOWER(object_name) LIKE LOWER(%s))
                ORDER BY object_name
                LIMIT 10
                """,
                (f"%{surname}%", f"%{surname}"),
            )
            rows = [str(item[0]).strip() for item in cur.fetchall() if str(item[0] or "").strip()]
        
        if not rows:
            return None
        
        # Filter to names where initials match.
        # Supports both natural order ("Benjamin Kin Sing Pang") and comma order
        # ("Pang, Benjamin Kin Sing").
        candidates = []
        for name in rows:
            raw_name = str(name or "").strip()
            if not raw_name:
                continue

            name_tokens = [tok for tok in re.findall(r"[A-Za-z0-9äöüÄÖÜß]+", raw_name) if tok]
            if len(name_tokens) < 2:
                continue

            has_comma = "," in raw_name
            if has_comma:
                name_surname = name_tokens[0]
                given_tokens = name_tokens[1:]
            else:
                name_surname = name_tokens[-1]
                given_tokens = name_tokens[:-1]

            if name_surname.lower() != surname.lower() or not given_tokens:
                continue

            given_initials = [token[0].upper() for token in given_tokens if token]
            if len(given_initials) < len(initials):
                continue
            if all(given_initials[i] == initials[i] for i in range(len(initials))):
                candidates.append(raw_name)

        unique_candidates = sorted(set(candidates), key=lambda text: text.lower())
        if len(unique_candidates) == 1:
            return unique_candidates[0]

        # Conservative tie-break: only accept top candidate if the lexical score gap is clear.
        if len(unique_candidates) > 1:
            ranked = self._rank_entity_candidates(conn, hint, unique_candidates)
            if len(ranked) == 1:
                return ranked[0][3]
            if len(ranked) > 1 and (int(ranked[0][0]) - int(ranked[1][0])) >= 3:
                return ranked[0][3]

        return None

    def _lookup_person_education_institution(self, conn, entity_name: str, attribute_name: str) -> dict[str, Any] | None:
        """Resolve generic education/qualification information via person<->institution links."""
        token_candidates = [tok for tok in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]{3,}", str(entity_name or "")) if tok]
        person_token = token_candidates[-1] if token_candidates else str(entity_name or "")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    person.object_name AS person_name,
                    inst.object_name AS institution_name,
                    rel.relationship_name,
                    rel.metadata,
                    (
                        SELECT a.attr_json->>'value'
                        FROM attribute a
                        WHERE a.src_type = 'object'
                          AND a.src_id = inst.object_id
                          AND a.attr_type IN ('awarding_body', 'institution', 'name')
                          AND a.valid_from <= CURRENT_DATE
                          AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
                        ORDER BY CASE WHEN a.attr_type = 'awarding_body' THEN 0 ELSE 1 END, a.entry_date DESC
                        LIMIT 1
                    ) AS institution_value,
                    COALESCE(MAX(doc.doc_id), 0) AS doc_id,
                    COALESCE(MAX(doc.doc_key), '') AS doc_key,
                    COALESCE(MAX(doc.doc_path), '') AS doc_path
                FROM object_instance person
                JOIN object_relationship rel
                  ON rel.src_object_id = person.object_id OR rel.tar_object_id = person.object_id
                JOIN object_instance inst
                  ON inst.object_id = CASE
                        WHEN rel.src_object_id = person.object_id THEN rel.tar_object_id
                        ELSE rel.src_object_id
                     END
                                LEFT JOIN part part_rel
                                    ON part_rel.relationship_id = rel.relationship_id
                LEFT JOIN document doc
                  ON doc.doc_id = part_rel.doc_id
                WHERE (
                        LOWER(person.object_name) = LOWER(%s)
                     OR LOWER(person.object_name) LIKE LOWER(%s)
                      )
                  AND LOWER(inst.class_name) IN ('education_institution', 'institution', 'university', 'school')
                  AND LOWER(rel.relationship_name) IN ('alumnus', 'alumnus_of', 'educated_at')
                  AND COALESCE(person.status, 'active') = 'active'
                  AND person.valid_from <= CURRENT_DATE
                  AND (person.valid_until IS NULL OR person.valid_until > CURRENT_DATE)
                  AND COALESCE(inst.status, 'active') = 'active'
                  AND inst.valid_from <= CURRENT_DATE
                  AND (inst.valid_until IS NULL OR inst.valid_until > CURRENT_DATE)
                  AND rel.valid_from <= CURRENT_DATE
                  AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                GROUP BY person.object_name, inst.object_name, rel.relationship_name, rel.metadata, inst.object_id
                ORDER BY
                    CASE WHEN LOWER(person.object_name) = LOWER(%s) THEN 0 ELSE 1 END,
                    CASE WHEN COALESCE(rel.metadata->>'degree', '') <> '' THEN 0 ELSE 1 END,
                    COALESCE(MAX(doc.doc_id), 0) DESC,
                    inst.object_name
                LIMIT 25
                """,
                (entity_name, f"%{person_token}%", entity_name),
            )
            rows = cur.fetchall()

        if not rows:
            return None

        entries: list[dict[str, Any]] = []
        for row in rows:
            metadata = row[3] if isinstance(row[3], dict) else {}
            institution = str(row[4] or row[1] or "").strip()
            degree = str(metadata.get("degree") or metadata.get("qualification") or metadata.get("title") or "").strip()
            if not institution and not degree:
                continue
            entries.append(
                {
                    "person_name": str(row[0] or "").strip(),
                    "institution": institution,
                    "degree": degree,
                    "relationship_name": str(row[2] or "").strip(),
                    "doc_id": row[5] or None,
                    "doc_name": row[6] or None,
                    "doc_path": row[7] or None,
                }
            )

        if not entries:
            return None

        requested = str(attribute_name or "").strip().lower()
        institution_attrs = {"institution", "university", "phd_institution"}
        qualification_attrs = {
            "qualification",
            "qualifications",
            "degree",
            "degrees",
            "education",
            "diploma",
            "diplomas",
            "certificate",
            "certificates",
            "certification",
        }

        if requested in institution_attrs:
            values = [item["institution"] for item in entries if item["institution"]]
        elif requested in qualification_attrs:
            values = []
            for item in entries:
                deg = item.get("degree") or ""
                inst = item.get("institution") or ""
                if deg and inst:
                    values.append(f"{deg} ({inst})")
                elif deg:
                    values.append(deg)
                elif inst:
                    values.append(f"educated at {inst}")
        else:
            values = []
            for item in entries:
                deg = item.get("degree") or ""
                inst = item.get("institution") or ""
                if deg and inst:
                    values.append(f"{deg} ({inst})")
                elif inst:
                    values.append(inst)
                elif deg:
                    values.append(deg)

        deduped: list[str] = []
        for value in values:
            cleaned = str(value or "").strip()
            if cleaned and cleaned not in deduped:
                deduped.append(cleaned)

        if not deduped:
            return None

        return {
            "entity_name": entries[0]["person_name"] or entity_name,
            "attribute_name": attribute_name,
            "attribute_value": deduped[0] if len(deduped) == 1 else deduped,
            "relationship_name": entries[0]["relationship_name"] or None,
            "related_entity_name": entries[0]["institution"] or None,
            "doc_id": entries[0]["doc_id"],
            "doc_name": entries[0]["doc_name"],
            "doc_path": entries[0]["doc_path"],
            "education_records": entries,
        }

    def _resolve_relationship_name(self, raw: str | None, entity_name: str | None = None) -> str | None:
        if not raw:
            return None

        if entity_name:
            entity_match = self._match_entity_relationship_name(entity_name, raw)
            if entity_match:
                return entity_match

        if self.attr_embedding_index:
            try:
                schema_match = self.attr_embedding_index.find_schema_term(
                    raw,
                    confidence_threshold=0.65,
                    kinds={"relationship"},
                )
                canonical = str((schema_match or {}).get("canonical_name") or "").strip()
                if canonical:
                    return canonical
            except Exception:
                pass

        normalized = self._normalize_alias_key(raw)
        return normalized or None

    def _lookup_related_attribute_via_relationship(
        self,
        conn,
        entity_name: str,
        relationship_name: str,
        attribute_name: str | None,
    ) -> dict[str, Any] | None:
        related = self._sql_related_object_lookup(conn, entity_name, relationship_name)
        if not related:
            return None

        related_result = dict(related)
        related_result.setdefault("source_entity_name", entity_name)
        related_result.setdefault("relationship_name", relationship_name)

        if not attribute_name:
            return related_result

        normalized_relation = self._normalize_attribute_name(relationship_name)
        normalized_attribute = self._normalize_attribute_name(attribute_name)
        if normalized_attribute and normalized_attribute == normalized_relation:
            return related_result

        related_names = related_result.get("attribute_value")
        candidate_names = related_names if isinstance(related_names, list) else [related_names]
        for candidate_name in candidate_names:
            related_entity = self._normalize_entity_name_hint(str(candidate_name or "").strip())
            if not related_entity:
                continue
            related_attribute = self.sql_exact_attribute_lookup(conn, related_entity, attribute_name)
            if related_attribute:
                merged = dict(related_attribute)
                merged["source_entity_name"] = entity_name
                merged["relationship_name"] = related_result.get("relationship_name") or relationship_name
                merged["related_entity_name"] = related_attribute.get("entity_name") or related_entity
                merged["related_entities"] = candidate_names if len(candidate_names) > 1 else related_entity
                return merged

        # If the relationship itself is the object and the requested attribute is
        # a field of that related object (for example email/telephone of a contact
        # person), do not stop here with the related person's name. Let the later
        # generic relationship-payload extraction path resolve the actual field.
        return None

    def _llm_extract_intent(self, question: str) -> "ParsedQuery | None":
        """Last-resort fallback: ask the LLM to extract entity + attribute as JSON.
        Feeds the LLM with the entity's actual DB attribute names when available,
        so it picks from real stored attributes rather than a hardcoded canonical list.
        Only called when all regex/alias/embedding/SQL tiers have failed.
        """
        if not self.genai_client:
            return None

        # Extract entity first (cheap, no LLM) so we can ground the prompt with real DB attributes
        entity_hint = self._best_entity_hint(question)
        db_attrs: list[str] = []
        if entity_hint:
            db_attrs = self._get_entity_attribute_names(entity_hint)

        if db_attrs:
            attr_context = (
                f"Available attributes for '{entity_hint}' in the database: {', '.join(db_attrs)}. "
                "Choose the closest match from this list."
            )
        else:
            known_attrs = sorted(self._canonical_attribute_names())
            attr_context = f"Known canonical attributes: {', '.join(known_attrs)}"

        prompt = (
            "Extract structured information from this natural language query.\n"
            "Infer intent from linguistic structure, not keyword memorization.\n"
            "Use verb/adjective/adverb cues and noun-phrase combinations to infer the requested domain concept.\n"
            "Then map that concept to the closest database attribute/relationship noun in snake_case.\n"
            "Examples of linguistic normalization:\n"
            "- verbal/event cues (registered, approved, born, expired, renewed) -> date/time/status style attributes\n"
            "- adjectival cues (active, valid, expired, responsible) -> status/owner/contact style attributes\n"
            "- noun compounds (trade register date, contact email, filing number) -> canonical attribute nouns\n"
            "If the query asks for an attribute of a related entity, set relation to the linking relationship "
            "between the base entity and that related entity, and set attribute to the final requested field.\n"
            "If the query asks for multiple fields (for example 'when and where'), include all fields in attributes.\n"
            "If uncertain between close candidates, choose the most semantically precise noun in available attributes.\n"
            "Return ONLY valid JSON with no extra text:\n"
            '{"entity": <company or person name, or null>, '
            '"attribute": <attribute being asked in snake_case, or null>, '
            '"attributes": <array of attribute names in snake_case, optional>, '
            '"relation": <relationship or linking phrase, or null>, '
            '"language": <"en"|"de"|"fr"|"es"|"it">}\n'
            f"{attr_context}\n"
            f"Query: {json.dumps(question)}"
        )
        try:
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: self.genai_client.models.generate_content(
                    model=EXTRACT_MODEL,
                    contents=prompt,
                ),
                model=EXTRACT_MODEL,
                contents=prompt,
                temperature=0.0,
                call_name="query_intent_extract",
                complexity="simple",
            )
            text = (response.text or "").strip()
            # Strip markdown code fences if present
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
            text = re.sub(r"\s*```\s*$", "", text, flags=re.MULTILINE)
            data = json.loads(text)
            entity = (data.get("entity") or "").strip() or None
            attribute_raw = (data.get("attribute") or "").strip() or None
            raw_attributes = data.get("attributes") if isinstance(data.get("attributes"), list) else []
            relation_raw = (data.get("relation") or "").strip() or None
            language = data.get("language", "en")
            if not entity and not attribute_raw and not raw_attributes and not relation_raw:
                return None

            attribute = self._resolve_contextual_attribute_name(attribute_raw, relation_raw)
            attributes: list[str] = []
            for item in raw_attributes:
                resolved = self._resolve_contextual_attribute_name(str(item or "").strip(), relation_raw)
                if resolved and resolved not in attributes:
                    attributes.append(resolved)
            if attribute and attribute not in attributes:
                attributes.insert(0, attribute)

            return ParsedQuery(
                intent="attribute_lookup" if (entity and (attribute or relation_raw)) else "semantic_lookup",
                entity_name=entity,
                attribute_name=attribute or (attributes[0] if attributes else None),
                confidence=0.7,
                language=language,
                relation_name=relation_raw,
                attribute_names=attributes or None,
                criteria={"parse_method": "llm_intent_parser"},
            )
        except Exception:
            return None

    def _planner_attribute_from_context(self, attr_raw: str | None, relation_raw: str | None = None) -> str | None:
        attr = str(attr_raw or "").strip().lower()
        if not attr:
            return None

        relation = str(relation_raw or "").strip().lower()
        alias = self.PLAN_ATTRIBUTE_ALIASES.get(attr)
        if alias:
            # Context-aware adjustment for EORI contact intents.
            if "eori" in relation and alias == "process_contact_person":
                return "eori_contact_person"
            if "eori" in relation and alias == "process_contact_email":
                return "eori_contact_email"
            return alias

        resolved = self._resolve_contextual_attribute_name(attr, relation_raw)
        if resolved:
            return resolved
        return attr

    def _repair_structured_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        repaired = dict(plan or {})

        # Ensure output shape is always one of supported shapes.
        output = repaired.get("output") if isinstance(repaired.get("output"), dict) else {}
        shape = str(output.get("shape") or "").strip().lower()
        if shape not in self.PLAN_OUTPUT_SHAPES:
            fields = output.get("fields") if isinstance(output.get("fields"), list) else []
            if len(fields) <= 1:
                shape = "scalar"
            elif len(fields) >= 3:
                shape = "table"
            else:
                shape = "list"
        output["shape"] = shape

        targets = repaired.get("targets") if isinstance(repaired.get("targets"), dict) else {}
        attrs = targets.get("attributes") if isinstance(targets.get("attributes"), list) else []

        # Normalize target attributes through canonical aliasing.
        relation_name = None
        rel_filters = ((repaired.get("filters") or {}).get("relationship_filters") if isinstance(repaired.get("filters"), dict) else None)
        if isinstance(rel_filters, list) and rel_filters:
            relation_name = str((rel_filters[0] or {}).get("relationship_type") or "").strip()

        normalized_attrs: list[str] = []
        for raw in attrs:
            canonical = self._planner_attribute_from_context(str(raw or ""), relation_name)
            if canonical and canonical not in normalized_attrs:
                normalized_attrs.append(canonical)
        targets["attributes"] = normalized_attrs

        # Keep output fields aligned with normalized targets and derived outputs.
        derived = repaired.get("derived") if isinstance(repaired.get("derived"), dict) else {}
        derived_metrics = derived.get("metrics") if isinstance(derived.get("metrics"), list) else []
        derived_inferences = derived.get("inferences") if isinstance(derived.get("inferences"), list) else []
        merged_fields: list[str] = []
        for item in list(normalized_attrs) + list(derived_metrics) + list(derived_inferences):
            text = str(item or "").strip()
            if text and text not in merged_fields:
                merged_fields.append(text)
        output["fields"] = merged_fields

        # Normalize temporal object shape.
        temporal = repaired.get("temporal") if isinstance(repaired.get("temporal"), dict) else {}
        date_range = temporal.get("date_range") if isinstance(temporal.get("date_range"), dict) else {}
        temporal["date_range"] = {
            "start": date_range.get("start"),
            "end": date_range.get("end"),
            "window": date_range.get("window"),
        }

        repaired["targets"] = targets
        repaired["output"] = output
        repaired["derived"] = derived
        repaired["temporal"] = temporal
        return repaired

    def _plan_to_parsed_query(self, plan: dict[str, Any], detected_language: str) -> ParsedQuery | None:
        if not isinstance(plan, dict):
            return None

        anchors = plan.get("anchors") if isinstance(plan.get("anchors"), dict) else {}
        primary = anchors.get("primary") if isinstance(anchors.get("primary"), dict) else {}
        entity_name = str(primary.get("entity_name") or "").strip() or None

        targets = plan.get("targets") if isinstance(plan.get("targets"), dict) else {}
        attributes = targets.get("attributes") if isinstance(targets.get("attributes"), list) else []
        attribute_names = [str(x).strip() for x in attributes if str(x or "").strip()]
        attribute_name = attribute_names[0] if attribute_names else None

        relation_name = None
        filters = plan.get("filters") if isinstance(plan.get("filters"), dict) else {}
        rel_filters = filters.get("relationship_filters") if isinstance(filters.get("relationship_filters"), list) else []
        if rel_filters:
            first_rel = rel_filters[0] if isinstance(rel_filters[0], dict) else {}
            relation_name = str(first_rel.get("relationship_type") or "").strip() or None

        intent = str(plan.get("intent") or "").strip().lower()
        if intent not in {"lookup", "retrieve", "filter", "aggregate", "compare", "explain", "classify", "resolve", "compute"}:
            intent = "attribute_lookup" if (entity_name and (attribute_name or relation_name)) else "semantic_lookup"
        elif intent in {"lookup", "retrieve", "compute"}:
            intent = "attribute_lookup" if (attribute_name or relation_name) else "semantic_lookup"

        confidence = 0.78
        conf = plan.get("confidence") if isinstance(plan.get("confidence"), dict) else {}
        try:
            overall = float(conf.get("overall") or 0.0)
            if overall > 0:
                confidence = max(0.65, min(0.98, overall))
        except Exception:
            pass

        relation_filters_for_lookup: list[dict[str, Any]] = []
        for raw in rel_filters:
            if not isinstance(raw, dict):
                continue
            relationship_type = str(raw.get("relationship_type") or "").strip().lower()
            target_name = str(
                raw.get("target_name")
                or raw.get("target_entity_name")
                or raw.get("target")
                or ""
            ).strip()
            if relationship_type and target_name:
                relation_filters_for_lookup.append(
                    {
                        "relationship_names": [relationship_type],
                        "target_name": target_name,
                    }
                )

        criteria = {
            "parse_method": "llm_structured_plan",
            "llm_query_plan": plan,
        }
        if relation_filters_for_lookup:
            criteria["relation_filters"] = relation_filters_for_lookup
        output = plan.get("output") if isinstance(plan.get("output"), dict) else {}
        shape = str(output.get("shape") or "").strip().lower()
        if shape:
            criteria["response_shape"] = shape
        temporal = plan.get("temporal") if isinstance(plan.get("temporal"), dict) else {}
        date_range = temporal.get("date_range") if isinstance(temporal.get("date_range"), dict) else {}
        if date_range.get("window"):
            criteria["time_window"] = str(date_range.get("window") or "")
        geography = plan.get("geography") if isinstance(plan.get("geography"), dict) else {}
        location = str(geography.get("location") or "").strip()
        country_code = str(geography.get("country_code") or "").strip().upper()
        role_name = str(geography.get("role_name") or "").strip().lower()
        document_type = str(geography.get("document_type") or "").strip().lower()
        if location or country_code:
            criteria["geography"] = {
                "location": location or None,
                "country_code": country_code or None,
                "role_name": role_name or None,
                "document_type": document_type or None,
            }
        action_items = plan.get("action_items") if isinstance(plan.get("action_items"), dict) else {}
        if bool(action_items.get("requested")):
            criteria["action_items"] = {
                "requested": True,
                "subject_name": str(action_items.get("subject_name") or entity_name or "").strip() or None,
                "document_query": str(action_items.get("document_query") or "").strip() or None,
                "status": str(action_items.get("status") or "open").strip().lower() or "open",
            }

        if not entity_name and not attribute_name and not relation_name:
            return None

        return ParsedQuery(
            intent=intent,
            entity_name=entity_name,
            attribute_name=attribute_name,
            confidence=confidence,
            language=detected_language,
            relation_name=relation_name,
            attribute_names=attribute_names or None,
            criteria=criteria,
        )

    def _normalize_parsed_for_execution(self, parsed: ParsedQuery, question: str) -> ParsedQuery:
        """Sanitize planner-driven ParsedQuery before execution.

        This keeps execution resilient when LLM output varies slightly across runs.
        """
        if not isinstance(parsed, ParsedQuery):
            return parsed

        criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        if str(criteria.get("parse_method") or "").strip().lower() != "llm_structured_plan":
            return parsed

        plan = criteria.get("llm_query_plan") if isinstance(criteria.get("llm_query_plan"), dict) else {}
        if not isinstance(plan, dict):
            return parsed

        repaired = self._repair_structured_plan(plan)

        anchors = repaired.get("anchors") if isinstance(repaired.get("anchors"), dict) else {}
        primary = anchors.get("primary") if isinstance(anchors.get("primary"), dict) else {}
        primary_name = str(primary.get("entity_name") or "").strip()
        if not parsed.entity_name and primary_name:
            parsed.entity_name = primary_name

        filters = repaired.get("filters") if isinstance(repaired.get("filters"), dict) else {}
        rel_filters = filters.get("relationship_filters") if isinstance(filters.get("relationship_filters"), list) else []
        if not parsed.relation_name and rel_filters:
            parsed.relation_name = str((rel_filters[0] or {}).get("relationship_type") or "").strip() or None

        targets = repaired.get("targets") if isinstance(repaired.get("targets"), dict) else {}
        attrs_raw = targets.get("attributes") if isinstance(targets.get("attributes"), list) else []
        relation_hint = parsed.relation_name
        normalized_attrs: list[str] = []
        for item in attrs_raw:
            canonical = self._planner_attribute_from_context(str(item or ""), relation_hint)
            if canonical and canonical not in normalized_attrs:
                normalized_attrs.append(canonical)

        # Fallback to parsed fields if plan attrs are empty.
        if not normalized_attrs:
            if parsed.attribute_names:
                for item in parsed.attribute_names:
                    canonical = self._planner_attribute_from_context(str(item or ""), relation_hint)
                    if canonical and canonical not in normalized_attrs:
                        normalized_attrs.append(canonical)
            elif parsed.attribute_name:
                canonical = self._planner_attribute_from_context(str(parsed.attribute_name or ""), relation_hint)
                if canonical:
                    normalized_attrs.append(canonical)

        parsed.attribute_names = normalized_attrs or parsed.attribute_names
        if normalized_attrs:
            parsed.attribute_name = normalized_attrs[0]

        # Build relation_filters format expected by SQL relation lookup.
        relation_filters_for_lookup: list[dict[str, Any]] = []
        for raw in rel_filters:
            if not isinstance(raw, dict):
                continue
            relationship_type = str(raw.get("relationship_type") or "").strip().lower()
            target_name = str(
                raw.get("target_name")
                or raw.get("target_entity_name")
                or raw.get("target")
                or ""
            ).strip()
            if relationship_type and target_name:
                relation_filters_for_lookup.append(
                    {
                        "relationship_names": [relationship_type],
                        "target_name": target_name,
                    }
                )

        output = repaired.get("output") if isinstance(repaired.get("output"), dict) else {}
        shape = str(output.get("shape") or "").strip().lower()

        criteria["llm_query_plan"] = repaired
        if shape:
            criteria["response_shape"] = shape
        if relation_filters_for_lookup:
            criteria["relation_filters"] = relation_filters_for_lookup
        parsed.criteria = criteria

        # If entity is still missing, attempt lexical recovery from question.
        if not parsed.entity_name:
            recovered_entity = self._best_entity_hint(question)
            if recovered_entity:
                parsed.entity_name = recovered_entity

        return parsed

    def _llm_extract_structured_plan(self, question: str, detected_language: str) -> "ParsedQuery | None":
        if not self.genai_client:
            return None

        prompt = build_query_planner_prompt(question)
        planner_payload = build_query_planner_request_payload(question, EXTRACT_MODEL)

        try:
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: self.genai_client.models.generate_content(
                    model=EXTRACT_MODEL,
                    contents=prompt,
                ),
                model=EXTRACT_MODEL,
                contents=prompt,
                temperature=float(planner_payload.get("temperature", 0.0) or 0.0),
                response_format=planner_payload.get("response_format") if isinstance(planner_payload, dict) else None,
                call_name="query_structured_plan",
                complexity="simple",
            )
            text = (response.text or "").strip()
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
            text = re.sub(r"\s*```\s*$", "", text, flags=re.MULTILINE)
            json_start = text.find("{")
            if json_start > 0:
                text = text[json_start:]
            data = json.loads(text)
            repaired = self._repair_structured_plan(data if isinstance(data, dict) else {})
            return self._plan_to_parsed_query(repaired, detected_language)
        except Exception:
            return None

    def _resolve_contextual_attribute_name(self, attribute_raw: str | None, relation_raw: str | None = None) -> str | None:
        attribute = self._resolve_attribute_name_tiered(attribute_raw) if attribute_raw else None
        if attribute:
            return attribute

        return attribute

    def _decompose_multi_question(self, question: str) -> list[str] | None:
        text = str(question or "").strip()
        if not text:
            return None

        match = re.match(
            r"^\s*(?P<prefix>.+?)\s+(?P<copula>is|was|are|were|ist|war)\s+(?P<tail>.+?)\s*\??$",
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None

        prefix = str(match.group("prefix") or "").strip()
        copula = str(match.group("copula") or "").strip().lower()
        tail = str(match.group("tail") or "").strip(" ?")
        if not prefix or not tail:
            return None

        wh_pattern = r"how\s+long|how\s+much|what|when|where|who|which|how|wie\s+lange|wie\s+viel|was|wann|wo|wer|welche(?:n|r|s)?|wie"
        wh_tokens = re.findall(rf"\b(?:{wh_pattern})\b", prefix, flags=re.IGNORECASE)
        if len(wh_tokens) < 2:
            return None

        reduced = re.sub(rf"\b(?:{wh_pattern})\b", " ", prefix, flags=re.IGNORECASE)
        reduced = re.sub(r"\b(?:and|und|or|oder)\b", " ", reduced, flags=re.IGNORECASE)
        reduced = re.sub(r"[,/&]+", " ", reduced)
        reduced = re.sub(r"\s+", " ", reduced).strip()
        if reduced:
            return None

        sub_questions: list[str] = []
        seen: set[str] = set()
        for token in wh_tokens:
            tok = str(token or "").strip().lower()
            if not tok or tok in seen:
                continue
            seen.add(tok)
            sub_questions.append(f"{tok} {copula} {tail}?")

        return sub_questions if len(sub_questions) >= 2 else None

    def _extract_interrogative_event_query(self, text: str, detected_language: str) -> ParsedQuery | None:
        match = re.match(
            r"^\s*(?P<wh>how\s+long|how\s+much|what\s+day|what\s+date|what|when|where|who|which|how|an\s+welchem\s+tag|wie\s+lange|wie\s+viel|was|wann|wo|wer|welche(?:n|r|s)?|wie)\s+"
            r"(?P<copula>is|was|are|were|ist|war|wurde|wurden)\s+"
            r"(?P<body>[a-zA-Z0-9äöüÄÖÜß .:_&\-/]+?)\s*\??$",
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None

        wh = str(match.group("wh") or "").strip().lower()
        body = str(match.group("body") or "").strip(" .")
        if not body:
            return None

        question_hint_map: dict[str, list[str]] = {
            "when": ["date", "time", "start date", "end date"],
            "what day": ["date", "day", "entry date"],
            "what date": ["date", "entry date"],
            "wann": ["datum", "zeit", "startdatum", "endedatum"],
            "an welchem tag": ["datum", "tag", "eintragungsdatum"],
            "where": ["place", "location"],
            "wo": ["ort", "standort", "location"],
            "how long": ["duration", "term"],
            "wie lange": ["dauer", "laufzeit"],
            "how much": ["amount", "value", "price", "cost"],
            "wie viel": ["betrag", "wert", "preis", "kosten"],
            "who": ["person", "owner", "contact"],
            "wer": ["person", "inhaber", "kontakt"],
            "what": ["type", "status", "description"],
            "was": ["typ", "status", "beschreibung"],
            "how": ["method", "process", "status"],
            "wie": ["methode", "prozess", "status"],
        }

        irregular_event_roots = {
            "born": "birth",
            "geboren": "geburt",
        }

        body_tokens = [tok for tok in re.split(r"\s+", body) if tok]
        if len(body_tokens) < 2:
            return None

        def _candidate_terms(predicate_value: str) -> list[str]:
            predicate_norm = re.sub(r"\s+", " ", predicate_value.strip().lower())
            roots: list[str] = [predicate_norm]
            if predicate_norm in irregular_event_roots:
                roots.insert(0, irregular_event_roots[predicate_norm])
            if len(predicate_norm) > 4 and predicate_norm.endswith("ed"):
                roots.append(predicate_norm[:-2])
            if len(predicate_norm) > 5 and predicate_norm.endswith("ing"):
                roots.append(predicate_norm[:-3])

            hints = question_hint_map.get(wh, [])
            out: list[str] = []
            seen: set[str] = set()
            for root in roots:
                variants: list[str] = []
                for hint in hints:
                    variants.extend([
                        f"{root} {hint}",
                        f"{hint} of {root}",
                        f"{root}_{hint}",
                    ])
                variants.extend([root, root.replace("_", " "), root.replace(" ", "_")])
                for candidate in variants:
                    key = candidate.strip().lower()
                    if key and key not in seen:
                        seen.add(key)
                        out.append(candidate)
            return out

        # Prefer the longest entity span and shortest predicate span.
        for pred_len in range(1, min(4, len(body_tokens)) + 1):
            entity_tokens = body_tokens[:-pred_len]
            predicate_tokens = body_tokens[-pred_len:]
            if not entity_tokens:
                continue

            entity_name = self._normalize_entity_name_hint(" ".join(entity_tokens).strip(" ."))
            predicate = " ".join(predicate_tokens).strip(" .")
            if not entity_name or not predicate:
                continue

            resolved_attrs: list[str] = []
            for candidate in _candidate_terms(predicate):
                attribute_name, _schema_kind = self._resolve_schema_term(candidate, entity_name)
                if attribute_name and attribute_name in self._canonical_attribute_names():
                    if attribute_name not in resolved_attrs:
                        resolved_attrs.append(attribute_name)

            if resolved_attrs:
                return ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=entity_name,
                    attribute_name=resolved_attrs[0],
                    confidence=0.86,
                    language=detected_language,
                    attribute_names=resolved_attrs,
                )

        return None

    def _resolve_schema_term(self, raw: str | None, entity_name: str | None = None) -> tuple[str | None, str | None]:
        """Resolve a query term to an attribute or relationship, if possible."""
        if not raw:
            return None, None

        raw_text = str(raw or "").strip().lower()

        # Prefer the plain current address field for generic address questions.
        # Explicit registered-address wording still resolves to registered_address.
        if (
            raw_text in {"address", "full address", "current address", "postal address", "adresse", "anschrift", "vollständige adresse", "vollstaendige adresse"}
            or ("address" in raw_text and "registered" not in raw_text and "registered_address" not in raw_text)
        ):
            return "address", "attribute"

        normalized = self._normalize_attribute_name(raw)
        if normalized and normalized in self._canonical_attribute_names():
            return normalized, "attribute"

        # Guardrail for unforeseen identifier labels (e.g., VAT-number, MWST-Nummer):
        # do not semantically coerce them into known IDs unless there is lexical evidence.
        if self._looks_like_identifier_request(raw):
            raw_key = self._normalize_alias_key(raw)
            schema_aliases = self._schema_alias_fallback_map()
            if raw_key not in schema_aliases:
                return normalized or raw_key or None, "attribute"

        if self.attr_embedding_index and self._semantic_attribute_search_allowed(raw, entity_name):
            allowed_kinds = {"attribute", "relationship", "entity_type", "category"}
            if entity_name:
                schema_terms = self._get_entity_schema_terms(entity_name)
                if schema_terms.get("relationships"):
                    allowed_kinds = {"attribute", "relationship", "entity_type", "category"}
            schema_match = self.attr_embedding_index.find_schema_term(raw, confidence_threshold=0.65, kinds=allowed_kinds)
            if schema_match:
                kind = str(schema_match.get("kind") or "").strip().lower() or None
                canonical_name = str(schema_match.get("canonical_name") or "").strip() or None
                if canonical_name:
                    return canonical_name, kind

        if business_rules is not None:
            try:
                lower_raw = raw_text
                country_hint = None
                if "switzerland" in lower_raw or "schweiz" in lower_raw:
                    country_hint = "switzerland"
                elif "germany" in lower_raw or "deutschland" in lower_raw:
                    country_hint = "germany"

                rule_match = business_rules.resolve_attribute_via_business_rules(
                    requested_attribute=str(raw),
                    context={
                        "country": country_hint,
                        "entity_class": None,
                    },
                )
                if isinstance(rule_match, dict):
                    canonical = str(rule_match.get("canonical_attribute") or "").strip()
                    if canonical:
                        return canonical, "attribute"
            except Exception:
                pass

        if normalized:
            return normalized, "attribute"

        return None, None

    def _attribute_rescue_candidates_from_text(self, text: str) -> list[str]:
        """Collect explicit attribute mentions in query text, ordered by specificity."""
        normalized_text = re.sub(r"\s+", " ", str(text or "").strip().lower())
        if not normalized_text:
            return []

        ranked: list[tuple[int, str]] = []
        canonical_attrs = self._canonical_attribute_names()
        for canonical_name, translations in self.ATTRIBUTE_DISPLAY_NAMES.items():
            candidates: list[str] = [canonical_name, canonical_name.replace("_", " ")]
            if isinstance(translations, dict):
                for names in translations.values():
                    if isinstance(names, list):
                        candidates.extend(str(n) for n in names)

            for candidate in candidates:
                phrase = re.sub(r"\s+", " ", str(candidate or "").strip().lower())
                if not phrase:
                    continue
                pattern = r"(?<![a-z0-9_])" + re.escape(phrase) + r"(?![a-z0-9_])"
                if re.search(pattern, normalized_text):
                    ranked.append((len(phrase), canonical_name))

        # Alias/normalization fallback: if display labels do not contain the exact phrase,
        # map short n-grams through attribute normalization (e.g. "eori number" -> "eori_no").
        tokens = re.findall(r"[a-z0-9_\-]+", normalized_text)
        for n in range(1, 5):
            for i in range(0, max(0, len(tokens) - n + 1)):
                phrase = " ".join(tokens[i:i + n]).strip()
                if not phrase:
                    continue
                normalized = self._normalize_attribute_name(phrase)
                if normalized and normalized in canonical_attrs:
                    ranked.append((len(phrase), normalized))

        if not ranked:
            return []

        ranked.sort(key=lambda item: (-item[0], item[1]))
        ordered: list[str] = []
        for _, name in ranked:
            if name not in ordered:
                ordered.append(name)
        return ordered

    def _explicit_attribute_from_text(self, text: str) -> str | None:
        """Find the strongest explicit attribute mention in query text."""
        candidates = self._attribute_rescue_candidates_from_text(text)
        return candidates[0] if candidates else None

    @staticmethod
    def _is_attribute_candidate_token(token: str) -> bool:
        value = str(token or "").strip().lower()
        if len(value) < 3:
            return False
        if not re.search(r"[a-zA-ZäöüÄÖÜß]", value):
            return False
        if value.isdigit():
            return False
        return True

    def _attribute_token_lexicon(self) -> set[str]:
        cache = getattr(self, "_attribute_token_lexicon_cache", None)
        if cache is not None:
            return set(cache)

        lexicon: set[str] = set()

        for alias_key in self._schema_alias_fallback_map().keys():
            for tok in re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", str(alias_key or "")):
                if self._is_attribute_candidate_token(tok):
                    lexicon.add(tok)

        for canonical_name, translations in self.ATTRIBUTE_DISPLAY_NAMES.items():
            for candidate in [canonical_name, canonical_name.replace("_", " ")]:
                for tok in re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", str(candidate or "")):
                    if self._is_attribute_candidate_token(tok):
                        lexicon.add(tok)
            if isinstance(translations, dict):
                for names in translations.values():
                    if not isinstance(names, list):
                        continue
                    for name in names:
                        for tok in re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", str(name or "")):
                            if self._is_attribute_candidate_token(tok):
                                lexicon.add(tok)

        self._attribute_token_lexicon_cache = set(lexicon)
        return set(lexicon)

    def _extract_attribute_phrases_via_spacy(self, text: str, entity_name: str | None = None) -> list[str]:
        """Use spaCy syntax cues (noun/adjective phrases) to propose generic attribute phrases.

        This avoids hardcoded legal-entity tokens by relying on grammatical structure
        and later schema-aware gating.
        """
        nlp = self._get_spacy_nlp()
        if nlp is None:
            return []

        raw_text = str(text or "").strip()
        if not raw_text:
            return []

        try:
            doc = nlp(raw_text)
        except Exception:
            return []

        entity_tokens: set[str] = set()
        if entity_name:
            entity_tokens = {
                tok
                for tok in re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", str(entity_name or "").lower())
                if self._is_attribute_candidate_token(tok)
            }

        phrases: list[str] = []

        # Noun chunks capture core attribute phrases like "invoice date" or "contact email".
        if hasattr(doc, "noun_chunks"):
            try:
                for chunk in doc.noun_chunks:
                    chunk_tokens = []
                    for token in chunk:
                        pos = str(getattr(token, "pos_", "") or "").upper()
                        if pos in {"DET", "PRON", "ADP", "CCONJ", "SCONJ", "PART", "AUX", "PUNCT"}:
                            continue
                        token_text = str(getattr(token, "text", "") or "").strip().lower()
                        if self._is_attribute_candidate_token(token_text):
                            chunk_tokens.append(token_text)

                    if not chunk_tokens:
                        continue

                    if entity_tokens and all(tok in entity_tokens for tok in chunk_tokens):
                        continue

                    phrase = " ".join(chunk_tokens).strip()
                    if phrase:
                        phrases.append(phrase)
            except Exception:
                pass

        # Token-level fallback for models without parser chunks.
        for token in doc:
            pos = str(getattr(token, "pos_", "") or "").upper()
            if pos not in {"NOUN", "PROPN", "ADJ"}:
                continue
            token_text = str(getattr(token, "text", "") or "").strip().lower()
            if not self._is_attribute_candidate_token(token_text):
                continue
            if entity_tokens and token_text in entity_tokens:
                continue
            phrases.append(token_text)

        ordered: list[str] = []
        for phrase in phrases:
            normalized = re.sub(r"\s+", " ", str(phrase or "").strip().lower())
            if normalized and normalized not in ordered:
                ordered.append(normalized)
        return ordered

    def _semantic_attribute_search_allowed(
        self,
        phrase: str | None,
        entity_name: str | None = None,
        relation_name: str | None = None,
    ) -> bool:
        text = re.sub(r"\s+", " ", str(phrase or "").strip().lower())
        if not text:
            return False

        tokens = [
            tok
            for tok in re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", text)
            if tok
        ]
        if not tokens:
            return False

        if len(tokens) > 8:
            return False

        content_tokens = [tok for tok in tokens if self._is_attribute_candidate_token(tok)]
        if not content_tokens:
            return False

        entity_tokens: set[str] = set()
        if entity_name:
            entity_tokens = {
                tok
                for tok in re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", str(entity_name or "").lower())
                if tok and self._is_attribute_candidate_token(tok)
            }
        if entity_tokens and all(tok in entity_tokens for tok in content_tokens):
            return False

        # Reject likely entity/name fragments when content tokens mostly mirror entity terms.
        if entity_tokens and len(content_tokens) <= 2:
            overlap = sum(1 for tok in content_tokens if tok in entity_tokens)
            if overlap >= len(content_tokens):
                return False

        normalized = self._normalize_alias_key(text)
        schema_aliases = self._schema_alias_fallback_map()
        if normalized in schema_aliases:
            return True

        relation_text = str(relation_name or "").strip().lower()
        if len(content_tokens) == 1:
            if relation_text and text == relation_text:
                return True
            # Single-token phrases are too noisy for semantic probing unless
            # they are explicit schema aliases handled above.
            return False

        for canonical_name, translations in self.ATTRIBUTE_DISPLAY_NAMES.items():
            candidates: list[str] = [canonical_name, canonical_name.replace("_", " ")]
            if isinstance(translations, dict):
                for names in translations.values():
                    if isinstance(names, list):
                        candidates.extend(str(name or "") for name in names)
            for candidate in candidates:
                candidate_text = re.sub(r"\s+", " ", str(candidate or "").strip().lower())
                if candidate_text and candidate_text == text:
                    return True

        # Generic lexical signal: allow semantic search when phrase tokens overlap
        # known schema aliases, even if wording is not an exact match.
        alias_overlap = 0
        for alias_key in schema_aliases.keys():
            alias_tokens = [
                tok
                for tok in re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", str(alias_key or ""))
                if self._is_attribute_candidate_token(tok)
            ]
            if not alias_tokens:
                continue
            if any(tok in alias_tokens for tok in content_tokens):
                alias_overlap += 1
                if alias_overlap >= 1:
                    return True

        if any(tok in self._attribute_token_lexicon() for tok in content_tokens):
            return True

        if relation_text and text == relation_text:
            return True

        return False

    def _expand_query_attribute_candidates(
        self,
        question: str,
        entity_name: str | None,
        relation_name: str | None = None,
    ) -> list[str]:
        """Expand plausible attribute candidates from query wording.

        Combines explicit phrase hits with semantic schema-term resolution so
        semantically similar but non-exact wording is still searched.
        """
        text = re.sub(r"\s+", " ", str(question or "").strip())
        if not text:
            return []

        known_schema = self._get_entity_schema_terms(entity_name) if entity_name else {"attributes": [], "relationships": []}
        known_attrs = {str(item).strip() for item in list(known_schema.get("attributes") or []) if str(item).strip()}
        known_rels = {str(item).strip() for item in list(known_schema.get("relationships") or []) if str(item).strip()}
        canonical_attrs = self._canonical_attribute_names()

        seed_phrases: list[str] = []
        seed_phrases.extend(self._attribute_rescue_candidates_from_text(text))
        explicit_candidates_present = bool(seed_phrases)

        # Add grammar-aware candidates first (noun/adjective phrases), then
        # schema gating decides which ones can trigger semantic probing.
        for phrase in self._extract_attribute_phrases_via_spacy(text, entity_name):
            if self._semantic_attribute_search_allowed(phrase, entity_name, relation_name):
                seed_phrases.append(phrase)

        fragments = [text]
        fragments.extend(
            frag.strip()
            for frag in re.split(r"\b(?:and|und|or|oder|sowie|plus|with|mit|samt)\b|[,;/]", text, flags=re.IGNORECASE)
            if str(frag or "").strip()
        )

        for frag in fragments[:24]:
            normalized = re.sub(r"\s+", " ", str(frag or "").strip())
            if not normalized:
                continue
            if not explicit_candidates_present and self._semantic_attribute_search_allowed(normalized, entity_name, relation_name):
                seed_phrases.append(normalized)

            tokens = [
                tok
                for tok in re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]{3,}", normalized.lower())
                if self._is_attribute_candidate_token(tok)
            ]
            if not tokens:
                continue

            max_n = min(3, len(tokens))
            for n in range(max_n, 0, -1):
                for i in range(0, len(tokens) - n + 1):
                    candidate_phrase = " ".join(tokens[i : i + n])
                    if self._semantic_attribute_search_allowed(candidate_phrase, entity_name, relation_name):
                        seed_phrases.append(candidate_phrase)

        ordered_phrases: list[str] = []
        for phrase in seed_phrases:
            p = str(phrase or "").strip()
            if p and p not in ordered_phrases:
                ordered_phrases.append(p)

        # Cap to a sensible limit before any embedding calls.
        work_phrases = ordered_phrases[:24]

        # Batch-embed all uncached phrases in ONE API call when the index is ready.
        if self.attr_embedding_index and getattr(self.attr_embedding_index, "_index_built", False):
            batch_results = self.attr_embedding_index.batch_find_schema_terms(
                work_phrases,
                confidence_threshold=0.65,
                kinds={"attribute", "relationship", "entity_type"},
            )
            results: list[str] = []
            for phrase, schema_match in zip(work_phrases, batch_results):
                if schema_match:
                    kind = str(schema_match.get("kind") or "attribute").lower()
                    candidate = str(schema_match.get("canonical_name") or "").strip()
                    if candidate:
                        if kind == "relationship":
                            if candidate in known_rels or candidate in canonical_attrs or candidate == str(relation_name or "").strip():
                                if candidate not in results:
                                    results.append(candidate)
                        elif candidate in known_attrs or candidate in canonical_attrs:
                            if candidate not in results:
                                results.append(candidate)
                        continue
                # Fallback: tiered normalisation (no additional API call)
                norm = str(self._normalize_attribute_name(phrase) or "").strip()
                if norm and (norm in known_attrs or norm in canonical_attrs):
                    if norm not in results:
                        results.append(norm)
            return results

        # No embedding index available – resolve purely lexically.
        results = []
        for phrase in work_phrases:
            resolved, kind = self._resolve_schema_term(phrase, entity_name)
            candidate = str(resolved or "").strip()
            if candidate:
                if kind == "relationship":
                    if candidate in known_rels or candidate in canonical_attrs or candidate == str(relation_name or "").strip():
                        if candidate not in results:
                            results.append(candidate)
                    continue
                if candidate in known_attrs or candidate in canonical_attrs:
                    if candidate not in results:
                        results.append(candidate)
                    continue
            semantic_attr = str(self._resolve_attribute_name_tiered(phrase) or "").strip()
            if semantic_attr and (semantic_attr in known_attrs or semantic_attr in canonical_attrs):
                if semantic_attr not in results:
                    results.append(semantic_attr)
        return results

    def _has_broad_attribute_scope_cue(self, question: str) -> bool:
        text = str(question or "").strip().lower()
        if not text:
            return False
        patterns = [
            r"\b(?:all|full|complete|entire|everything|overall|comprehensive)\b",
            r"\b(?:all\s+(?:data|information|details|fields?|columns?|attributes?))\b",
            r"\b(?:show|give|list|return|display)\s+(?:me\s+)?(?:all|full|complete)\b",
            r"\b(?:personal\s+information|personal\s+details|profile|full\s+profile|identity\s+details)\b",
            r"\b(?:alle|vollst[aä]ndig(?:e|es|en)?|komplett(?:e|es|en)?|gesamte[nrms]?)\b",
            r"\b(?:alle\s+(?:daten|informationen|angaben|details|felder|spalten|attribute))\b",
            r"\b(?:informationen|angaben|profil|details|daten|felder|spalten|attribute)\b",
        ]
        return any(re.search(p, text, flags=re.IGNORECASE) for p in patterns)

    def _has_file_info_cue(self, question: str) -> bool:
        text = str(question or "").strip().lower()
        if not text:
            return False
        patterns = [
            r"\b(?:file|filename|path|full path|document path|document filename)\b",
            r"\b(?:dateiname|pfad|vollst[aä]ndig(?:e|en|em)?\s+pfad|dokumentname)\b",
        ]
        return any(re.search(p, text, flags=re.IGNORECASE) for p in patterns)

    def _related_attribute_hints_from_text(self, question: str, relation_name: str | None) -> list[str]:
        """Infer extra related-object attributes from free-form multi-attribute wording.

        This is relationship-generic (not EORI-specific): for prompts like
        "name and email of the contact person", include both the relationship
        identity and concrete related-object attributes.
        """
        text = str(question or "").strip().lower()
        relation = str(relation_name or "").strip()
        if not text or not relation:
            return []

        has_multi_attr_connector = bool(re.search(r"\b(?:and|und|sowie|plus|with|mit|samt)\b", text, flags=re.IGNORECASE))
        has_details_cue = bool(re.search(r"\b(?:details?|information|info|daten|data|contact\s+details|contact\s+information|kontaktdaten|kontaktinformationen)\b", text, flags=re.IGNORECASE))
        has_name_cue = bool(
            re.search(
                r"\b(?:name|namen|person\s*name|kontaktname|kontakt\s*name|contact\s*name|ansprechpartner(?:in)?|contact\s*person|kontaktperson)\b",
                text,
                flags=re.IGNORECASE,
            )
        )
        has_email_cue = self._has_eori_contact_email_intent(text) or bool(
            re.search(r"\b(?:email|e-mail|mail|mailadresse|emailadresse)\b", text, flags=re.IGNORECASE)
        )
        has_phone_cue = bool(
            re.search(
                r"\b(?:phone(?:\s*number)?|telephone|telefon(?:nummer)?|tel\.?|mobile|handy|mobil(?:nummer)?)\b",
                text,
                flags=re.IGNORECASE,
            )
        )
        has_role_cue = bool(re.search(r"\b(?:role|funktion|position|title|titel|rolle)\b", text, flags=re.IGNORECASE))
        has_contact_details_scope = bool(
            re.search(
                r"\b(?:contact\s+details|contact\s+information|contact\s+info|kontaktdaten|kontaktinformationen)\b",
                text,
                flags=re.IGNORECASE,
            )
        )

        hints: list[str] = []
        if has_name_cue:
            hints.append(relation)

        if has_contact_details_scope:
            hints.append(relation)
            hints.append("email")
            hints.extend(["phone", "telephone"])

        if has_details_cue or has_multi_attr_connector or has_email_cue or has_phone_cue or has_role_cue:
            if has_email_cue or has_details_cue:
                hints.append("email")
            if has_phone_cue or has_details_cue:
                hints.extend(["phone", "telephone"])
            if has_role_cue:
                hints.append("role")

        ordered: list[str] = []
        for item in hints:
            name = str(item or "").strip()
            if name and name not in ordered:
                ordered.append(name)
        return ordered

    def _has_email_intent(self, text: str) -> bool:
        """Generic: detect any email/e-mail request in query text."""
        normalized_text = str(text or "").strip().lower()
        if not normalized_text:
            return False
        patterns = [
            r"\b(?:email|e-mail|e_mail|mail|mailadresse|emailadresse)\b",
            r"\be[\-\s]?mail(?:[\-\s]?adresse)?\b",
        ]
        return any(re.search(p, normalized_text, flags=re.IGNORECASE) for p in patterns)

    def _infer_temporal_scope_from_question(self, question: str | None) -> str:
        """Infer whether the user wants current, historical, or any-time data.

        Default is ``current`` because most present-tense queries should prefer
        the currently valid state. Questions that explicitly ask for history,
        previous values, or changes over time return ``historical``.
        """
        text = str(question or "").strip().lower()
        if not text:
            return "current"

        historical_patterns = [
            r"\b(?:historical|historisch(?:e|en|er|es)?|history|timeline|timeline\s+of)\b",
            r"\b(?:former|previous|prior|earlier|old|past|back\s+then|used\s+to|used\s+to\s+be|was\s+formerly)\b",
            r"\b(?:damals|früher|frueher|ehemalig(?:e|en|er|es)?|vorher|zuvor|bisher)\b",
            r"\b(?:as\s+of|on\s+date|valid\s+on|at\s+that\s+time|then)\b",
            r"\b(?:verlauf|änderung(?:en)?|aenderung(?:en)?|entwicklung)\b",
        ]
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in historical_patterns):
            return "historical"

        current_patterns = [
            r"\b(?:current|currently|present|now|today|latest|active|valid\s+now)\b",
            r"\b(?:heute|aktuell|derzeit|jetzt|gegenwärtig|gueltig|gültig)\b",
        ]
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in current_patterns):
            return "current"

        return "current"

    def _temporal_scope_from_criteria(self, criteria: dict[str, Any] | None) -> str:
        scope = str(criteria.get("temporal_scope") or "").strip().lower() if isinstance(criteria, dict) else ""
        if scope in {"current", "historical", "any"}:
            return scope
        return "current"

    def _normalize_temporal_lookup_result(
        self,
        result: dict[str, Any] | None,
        attribute_name: str | None,
        temporal_scope: str = "current",
    ) -> dict[str, Any] | None:
        """Normalize a lookup result according to the temporal policy.

        For current-time questions, collapse list-valued results for attributes
        that are not naturally multi-valued so the answer stays singular.
        """
        if not result or not isinstance(result, dict):
            return result

        normalized = dict(result)
        normalized["temporal_scope"] = temporal_scope
        normalized["attribute_value"] = self._repair_mojibake_value(normalized.get("attribute_value"))

        value = normalized.get("attribute_value")
        if (
            temporal_scope != "historical"
            and isinstance(value, list)
            and not self._is_naturally_multi_value_attribute(attribute_name)
        ):
            collapsed = [item for item in value if str(item or "").strip()]
            if collapsed:
                normalized["attribute_value"] = collapsed[0]
                normalized["temporal_value_candidates"] = collapsed
        return normalized

    @staticmethod
    def _repair_mojibake_text(text: str) -> str:
        raw = str(text or "")
        if not raw:
            return raw

        suspicious_markers = ("Ã", "Â", "â", "ð", "œ", "ž")
        if not any(marker in raw for marker in suspicious_markers):
            return raw

        try:
            repaired = raw.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
            if repaired and repaired != raw:
                return repaired
        except Exception:
            pass
        return raw

    def _repair_mojibake_value(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._repair_mojibake_text(value)
        if isinstance(value, list):
            return [self._repair_mojibake_value(item) for item in value]
        if isinstance(value, dict):
            return {key: self._repair_mojibake_value(item) for key, item in value.items()}
        return value

    def _format_mapping_value_for_answer(self, mapping: dict[str, Any]) -> str:
        if not isinstance(mapping, dict):
            return str(mapping)

        def _clean(value: Any) -> str:
            text = str(value or "").strip()
            return text

        name_value = _clean(mapping.get("name"))
        preferred_keys = ["email", "phone", "telephone", "mobile", "role", "title", "id", "value"]

        details: list[str] = []
        used: set[str] = set()
        for key in preferred_keys:
            if key not in mapping:
                continue
            text = _clean(mapping.get(key))
            if not text:
                continue
            details.append(f"{key}: {text}")
            used.add(key)

        for key, raw_value in mapping.items():
            if key in used or key == "name":
                continue
            text = _clean(raw_value)
            if not text:
                continue
            details.append(f"{key}: {text}")

        if name_value and details:
            return f"{name_value} ({', '.join(details)})"
        if name_value:
            return name_value
        return ", ".join(details) if details else ""

    def _related_object_attribute_map(
        self,
        conn,
        related_object_ids: list[int],
        max_attrs_per_object: int = 12,
    ) -> dict[int, dict[str, Any]]:
        """Fetch a compact latest attribute map for related objects.

        This is intentionally generic and schema-agnostic so broad-scope
        relationship requests can surface rich related-object details.
        """
        ids = [int(item) for item in related_object_ids if isinstance(item, int)]
        if not ids:
            return {}

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT src_id, attr_type, attr_json->>'value' AS attr_value
                FROM (
                    SELECT
                        a.src_id,
                        a.attr_type,
                        a.attr_json,
                        ROW_NUMBER() OVER (
                            PARTITION BY a.src_id, LOWER(a.attr_type)
                            ORDER BY a.entry_date DESC NULLS LAST, a.attr_id DESC
                        ) AS rn
                    FROM attribute a
                    WHERE a.src_type = 'object'
                      AND a.src_id = ANY(%s)
                      AND a.valid_from <= CURRENT_DATE
                      AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
                ) ranked
                WHERE rn = 1
                ORDER BY src_id, attr_type;
                """,
                (ids,),
            )
            rows = cur.fetchall() or []

        per_object: dict[int, dict[str, Any]] = {}
        per_object_count: dict[int, int] = {}
        for row in rows:
            obj_id = int(row[0]) if isinstance(row[0], int) else None
            if obj_id is None:
                continue
            attr_type = str(row[1] or "").strip()
            attr_value = str(row[2] or "").strip()
            if not attr_type or not attr_value:
                continue

            used = per_object_count.get(obj_id, 0)
            if used >= max_attrs_per_object:
                continue

            object_map = per_object.setdefault(obj_id, {})
            if attr_type in object_map:
                continue
            object_map[attr_type] = self._repair_mojibake_text(attr_value)
            per_object_count[obj_id] = used + 1

        return per_object

    def _format_attribute_value_for_answer(self, value: Any) -> str:
        if isinstance(value, dict):
            rendered = self._format_mapping_value_for_answer(value)
            return rendered or "None found"
        if isinstance(value, list):
            parts: list[str] = []
            for item in value:
                if isinstance(item, dict):
                    rendered = self._format_mapping_value_for_answer(item)
                else:
                    rendered = str(item or "").strip()
                if rendered:
                    parts.append(rendered)
            return "; ".join(parts) if parts else "None found"
        rendered = str(value or "").strip()
        return rendered or "None found"

    # Keep old name as alias for backward compatibility with any callers
    def _has_eori_contact_email_intent(self, text: str) -> bool:
        return self._has_email_intent(text)

    def _extract_process_actor_intent(
        self,
        query_text: str,
        entity_hint: str | None,
    ) -> dict[str, Any] | None:
        """Generic: detect queries asking for person/company responsible for any process/number.

        Works for EORI, customs, VAT, trade register, or any other registration type.
        Returns None or a dict with entity_name, process_concept, requested_attrs, wants_company.
        """
        lowered_text = str(query_text or "").strip().lower()
        suspicious_entity_hint = bool(
            entity_hint
            and re.search(
                r"\b(?:contact|ansprechpartner|kontakt|person|responsible|customs|vat|eori|registration|registrierung|application|antrag)\b",
                str(entity_hint),
                flags=re.IGNORECASE,
            )
        )
        if suspicious_entity_hint:
            legal_entity_hint = self._extract_legal_entity_mention(query_text)
            if legal_entity_hint:
                entity_hint = legal_entity_hint
        if not entity_hint:
            entity_hint = self._extract_entity_name_loose(query_text)
        if not entity_hint:
            return None

        # Must have actor or company focus cue
        has_actor_focus = bool(re.search(
            r"\b(?:wer|who|details?|angaben|buchf[üu]hrung|buchhaltung|accounting|"
            r"ansprechpartner(?:in)?(?:s)?|kontakt(?:daten)?|responsible|zust[aä]ndig|verantwortlich)\b",
            lowered_text, flags=re.IGNORECASE
        ))
        has_company_focus = bool(re.search(
            r"\b(?:company|firm|organization|organisation|unternehmen|firma|gesellschaft)\b",
            lowered_text, flags=re.IGNORECASE
        ))
        if not has_actor_focus and not has_company_focus:
            return None

        if has_company_focus:
            legal_entity_hint = self._extract_legal_entity_mention(query_text)
            if legal_entity_hint:
                entity_hint = legal_entity_hint

        # Must have application/submission/responsible verb or noun
        has_application = bool(re.search(
            r"\b(?:beantragt(?:e|en|er|es)?|beantragen|antrag(?:te|en)?|antragstellung|"
            r"applied|apply|requested|request|application|submitted|filed|eingereicht|"
            r"angemeldet|registered|registriert|handled|bearbeit(?:et|en)?|responsible|verantwortlich)\b",
            lowered_text, flags=re.IGNORECASE
        ))
        if not has_application:
            return None

        # Extract process concept: the noun before "-number", "-Nummer", "application", etc.
        process_concept: str | None = None
        process_patterns = [
            # "the EORI-number", "die EORI-Nummer", "the customs number"
            r"\b(?:die|den|der|das|the|a|an)?\s*([a-zA-Z\xc4\xd6\xdc\xe4\xf6\xfc\xdf0-9_\-]{2,25})"
            r"(?:[\-\s](?:number|nummer|no\b|nr\.?\b|registration|registrierung|application|antrag"
            r"|certificate|zertifikat|permit|erlaubnis|license|lizenz|clearance))\b",
        ]
        for pat in process_patterns:
            m = re.search(pat, lowered_text, flags=re.IGNORECASE)
            if m:
                raw = (m.group(1) or "").strip().strip("-")
                _skip = {"the", "a", "an", "die", "der", "das", "den", "for", "fur", "fuer"}
                if raw and raw.lower() not in _skip and len(raw) >= 2:
                    process_concept = raw.lower()
                    break

        if not process_concept:
            # Fallback: use significant noun before entity as process concept
            entity_lower = entity_hint.lower()
            pos = lowered_text.find(entity_lower)
            if pos > 5:
                before = lowered_text[:pos].strip()
                _skip_tokens = {
                    "show", "me", "the", "a", "an", "die", "den", "der", "das", "bitte",
                    "for", "of", "by", "von", "fur", "fuer", "bei", "to", "who", "wer",
                    "what", "was", "company", "firm", "person", "details", "information",
                    "info", "contact", "responsible", "in", "is", "hat", "have", "has",
                    "and", "oder", "mit", "und", "mir", "zeigen", "zeige", "gib", "finde",
                }
                tokens = [t for t in re.findall(r"[a-zA-Z\xc4\xd6\xdc\xe4\xf6\xfc\xdf0-9_\-]{3,}", before)
                          if t.lower() not in _skip_tokens]
                if tokens:
                    process_concept = tokens[-1].lower()

        if not process_concept:
            return None

        has_email = self._has_email_intent(lowered_text)
        has_phone = bool(re.search(
            r"\b(?:phone(?:\s*number)?|telephone|telefon(?:nummer)?|tel\.?|mobile|handy|mobil(?:nummer)?)\b",
            lowered_text, flags=re.IGNORECASE
        ))

        requested_attrs: list[str] = []
        if has_company_focus:
            requested_attrs.append("responsible_company")
        requested_attrs.append("process_contact_person")
        if has_email or has_company_focus:
            requested_attrs.append("process_contact_email")
        if has_phone:
            requested_attrs.append("phone")

        return {
            "entity_name": entity_hint,
            "process_concept": process_concept,
            "requested_attrs": requested_attrs,
            "wants_company": has_company_focus,
        }

    def _extract_process_contact_intent(
        self,
        query_text: str,
        entity_hint: str | None,
    ) -> dict[str, Any] | None:
        """Generic: detect queries asking for contact person/email for any named process.

        Catches simpler phrasings without application verbs, e.g.:
        'What is the EORI contact email for Company X?'
        'Who is the customs contact person for Company X?'
        """
        lowered_text = str(query_text or "").strip().lower()
        suspicious_entity_hint = bool(
            entity_hint
            and re.search(
                r"\b(?:contact|ansprechpartner|kontakt|person|responsible|customs|vat|eori|registration|registrierung|application|antrag)\b",
                str(entity_hint),
                flags=re.IGNORECASE,
            )
        )
        if suspicious_entity_hint:
            legal_entity_hint = self._extract_legal_entity_mention(query_text)
            if legal_entity_hint:
                entity_hint = legal_entity_hint
        if not entity_hint:
            entity_hint = self._extract_entity_name_loose(query_text)
        if not entity_hint:
            return None

        has_contact_cue = bool(re.search(
            r"\b(?:ansprechpartner(?:in)?(?:s)?|kontaktperson(?:en)?|kontakt(?:daten)?|contact(?:[_\s]?persons?)?)\b",
            lowered_text, flags=re.IGNORECASE
        ))
        if not has_contact_cue:
            return None

        # Keep this gate process-specific. Generic relationship contact queries
        # (e.g. "contact persons of <entity>") should route through the
        # relationship parser instead of process_contact_* attributes.
        has_process_anchor = bool(re.search(
            r"\b(?:process|eori|customs|zoll|vat|mwst|registration|registrierung|application|antrag|clearance|permit|license|lizenz)\b",
            lowered_text,
            flags=re.IGNORECASE,
        ))
        if not has_process_anchor:
            return None

        has_email = self._has_email_intent(lowered_text)

        # Extract process concept: word BEFORE or AFTER the contact cue.
        _contact_cue_pat = (
            r"\b([a-zA-Z\xc4\xd6\xdc\xe4\xf6\xfc\xdf0-9_\-]{2,25})"
            r"[\s\-](?:ansprechpartner(?:in)?(?:s)?|kontaktperson(?:en)?|kontakt(?:daten)?|contact(?:[_\s]?persons?)?)"
        )
        _after_contact_pat = (
            r"(?:ansprechpartner(?:in)?(?:s)?|kontaktperson(?:en)?|kontakt(?:daten)?|contact(?:[_\s]?persons?)?)"
            r"[\s\-]+(?:der|die|des|the|of|for|von|f[\xfc]r)?\s*"
            r"([a-zA-Z\xc4\xd6\xdc\xe4\xf6\xfc\xdf0-9_\-]{2,25})"
        )
        _skip = {"the", "a", "an", "die", "der", "das", "den", "des", "eins", "ein",
             "mein", "kein", "fur", "fuer", "für", "for", "von"}
        process_concept: str | None = None

        # Prefer word immediately BEFORE the contact cue (iterate past stop words)
        for m in re.finditer(_contact_cue_pat, lowered_text, flags=re.IGNORECASE):
            raw = (m.group(1) or "").strip().strip("-")
            if raw and raw.lower() not in _skip and len(raw) >= 2:
                process_concept = raw.lower()
                break

        # Fallback: word immediately AFTER the contact cue (e.g. "Kontaktdaten der EORI-Registrierung")
        if not process_concept:
            for m in re.finditer(_after_contact_pat, lowered_text, flags=re.IGNORECASE):
                raw = (m.group(1) or "").strip().strip("-")
                if raw and raw.lower() not in _skip and len(raw) >= 2:
                    process_concept = raw.lower()
                    break

        # Additional fallback: infer process concept from "<concept>-number/Nummer/application" phrase
        # to support wording like "E-Mail-Adresse der Kontaktperson fuer die Beantragung der EORI-Nummer ...".
        if not process_concept:
            process_patterns = [
                r"\b(?:die|den|der|das|the|a|an)?\s*([a-zA-Z\xc4\xd6\xdc\xe4\xf6\xfc\xdf0-9_\-]{2,25})"
                r"(?:[\-\s](?:number|nummer|no\b|nr\.?\b|registration|registrierung|application|antrag"
                r"|certificate|zertifikat|permit|erlaubnis|license|lizenz|clearance))\b",
            ]
            for pat in process_patterns:
                m = re.search(pat, lowered_text, flags=re.IGNORECASE)
                if not m:
                    continue
                raw = (m.group(1) or "").strip().strip("-")
                if raw and raw.lower() not in _skip and len(raw) >= 2:
                    process_concept = raw.lower()
                    break

        if not process_concept:
            return None

        requested_attrs = []
        if has_email:
            requested_attrs.append("process_contact_email")
            requested_attrs.append("process_contact_person")
        else:
            requested_attrs.append("process_contact_person")

        return {
            "entity_name": entity_hint,
            "process_concept": process_concept,
            "requested_attrs": requested_attrs,
            "wants_company": False,
        }

    def _llm_advise_attributes_for_query(
        self,
        question: str,
        entity_name: str,
        current_attribute: str | None = None,
    ) -> list[str]:
        """Ask LLM for attribute scope guidance using only DB-known attributes for the entity."""
        if not self.genai_client:
            return []

        available_attrs = [
            str(item).strip()
            for item in self._get_entity_attribute_names(entity_name)
            if str(item).strip()
        ]
        if not available_attrs:
            return []

        allowed = sorted(set(available_attrs))
        broad_scope = self._has_broad_attribute_scope_cue(question)
        prompt = (
            "Determine which attributes are required to answer the user query for the given entity. "
            "Use ONLY attributes from available_attributes. "
            "If query asks for full/complete/all details, set wants_all_attributes=true. "
            "Return ONLY valid JSON with no extra text:\n"
            '{"wants_all_attributes": <true|false>, "attributes": [<attribute names from available_attributes>]}\n'
            f"Entity: {json.dumps(entity_name)}\n"
            f"Current interpreted attribute: {json.dumps(str(current_attribute or ''))}\n"
            f"available_attributes: {json.dumps(allowed, ensure_ascii=False)}\n"
            f"broad_scope_detected: {str(broad_scope).lower()}\n"
            f"Query: {json.dumps(question, ensure_ascii=False)}"
        )

        try:
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: self.genai_client.models.generate_content(
                    model=EXTRACT_MODEL,
                    contents=prompt,
                ),
                model=EXTRACT_MODEL,
                contents=prompt,
                temperature=0.0,
                call_name="attribute_scope_advisor",
                complexity="simple",
            )
            text = (response.text or "").strip()
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
            text = re.sub(r"\s*```\s*$", "", text, flags=re.MULTILINE)
            payload = json.loads(text)
            wants_all = bool(payload.get("wants_all_attributes"))
            raw_attrs = payload.get("attributes") if isinstance(payload.get("attributes"), list) else []

            advised = [str(item).strip() for item in raw_attrs if str(item).strip()]
            advised = [item for item in advised if item in allowed]

            if wants_all:
                # Keep practical limit to avoid huge responses for very wide schemas.
                return allowed[:16]

            dedup: list[str] = []
            for item in advised:
                if item not in dedup:
                    dedup.append(item)
            return dedup[:16]
        except Exception:
            return []

    def _prefix_source(self, answer_text: str, source: str) -> str:
        return f"[source: {source}] {str(answer_text or '').strip()}".strip()

    @staticmethod
    def _leading_question_head(question: str) -> str | None:
        text = str(question or "").strip().lower()
        if not text:
            return None

        heads = [
            "how long", "how much", "wie lange", "wie viel",
            "when", "where", "what", "how", "who", "which",
            "wann", "wo", "was", "wie", "wer", "welche",
        ]
        for head in heads:
            if text.startswith(head + " ") or text == head:
                return head
        return None

    def _is_full_profile_request(self, question: str) -> bool:
        text = str(question or "").strip().lower()
        if not text:
            return False
        patterns = [
            r"\b(?:full|complete|all)\s+(?:personal\s+)?(?:information|info|details|profile)\b",
            r"\bpersonal\s+information\b",
            r"\b(?:vollst[aä]ndige|komplette|alle)\s+(?:personenbezogenen\s+)?(?:informationen|angaben|details|profil)\b",
            r"\bpersonenbezogene\s+informationen\b",
        ]
        return any(re.search(p, text, flags=re.IGNORECASE) for p in patterns)

    def _sql_entity_profile_lookup(self, conn, entity_name: str, temporal_scope: str = "current") -> dict[str, Any] | None:
        """Return a profile bundle for an entity, preferring current-valid data by default."""
        resolved = self._resolve_entity_name_for_lookup(conn, entity_name) or entity_name
        prefer_historical = str(temporal_scope or "").strip().lower() == "historical"
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    oi.object_name,
                    a.attr_type,
                    a.attr_json->>'value' AS attr_value,
                    a.valid_from,
                    a.valid_until,
                    a.entry_date,
                    a.attr_id
                FROM object_instance oi
                JOIN attribute a
                  ON a.src_type = 'object' AND a.src_id = oi.object_id
                WHERE LOWER(oi.object_name) = LOWER(%s)
                  AND COALESCE(oi.status, 'active') = 'active'
                  AND oi.valid_from <= CURRENT_DATE
                  AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                  AND (
                        %s
                        OR (
                            a.valid_from <= CURRENT_DATE
                            AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
                        )
                  )
                ORDER BY a.attr_type, a.valid_from DESC NULLS LAST, a.valid_until DESC NULLS LAST, a.entry_date DESC NULLS LAST, a.attr_id DESC
                """,
                (resolved, prefer_historical),
            )
            rows = cur.fetchall() or []

        if not rows:
            return None

        skip_attrs = {"generic_doc3", "description"}
        grouped: dict[str, list[tuple[Any, ...]]] = {}
        for row in rows:
            attr_type = str(row[1] or "").strip()
            if not attr_type:
                continue
            if attr_type.lower() in skip_attrs:
                continue
            grouped.setdefault(attr_type, []).append(row)

        def _is_current_row(row: tuple[Any, ...]) -> bool:
            valid_from = row[3]
            valid_until = row[4]
            return bool(valid_from <= date.today() and (valid_until is None or valid_until > date.today()))

        def _pick_row(candidates: list[tuple[Any, ...]]) -> tuple[Any, ...] | None:
            if not candidates:
                return None
            if prefer_historical:
                expired = [row for row in candidates if not _is_current_row(row)]
                if expired:
                    return expired[0]
            current_rows = [row for row in candidates if _is_current_row(row)]
            if current_rows:
                return current_rows[0]
            return candidates[0]

        dedup: dict[str, Any] = {}
        for attr_type, candidates in grouped.items():
            picked = _pick_row(candidates)
            if not picked:
                continue
            value = picked[2]
            if value is None or str(value).strip() == "":
                continue
            dedup[attr_type] = value

        if not dedup:
            return None

        preferred_order = [
            "birth_date",
            "birth_place",
            "nationality",
            "address",
            "registered_address",
            "eori_no",
            "eori_contact_email",
            "email",
            "phone",
        ]
        ordered_attrs = sorted(
            dedup.items(),
            key=lambda item: (
                preferred_order.index(item[0]) if item[0] in preferred_order else 999,
                item[0],
            ),
        )

        attributes = [
            {"attribute_name": name, "attribute_value": value}
            for name, value in ordered_attrs[:16]
        ]
        return {
            "entity_name": str(rows[0][0] or resolved),
            "attributes": attributes,
        }

    def stable_token_index(self, token: str) -> int:
        digest = hashlib.md5(token.encode("utf-8")).hexdigest()
        return int(digest[:8], 16)

    def encode_sparse(self, text: str) -> tuple[list[int], list[float]]:
        tokens = re.findall(r"[\w\-/.:]+", text.lower())
        freq: dict[int, float] = {}
        for token in tokens:
            idx = self.stable_token_index(token)
            freq[idx] = freq.get(idx, 0.0) + 1.0
        return list(freq.keys()), list(freq.values())

    # Module-level embedding cache shared across all QueryEngine instances so that
    # repeated calls with the same text (same or different instances) are free.
    _EMBED_CACHE: dict[str, list[float]] = {}
    _EMBED_CACHE_MAX = 512

    def embed_text(self, text: str) -> list[float] | None:
        if not self.embedding_client:
            return None
        key = str(text or "").strip()
        if not key:
            return None
        cached = self.__class__._EMBED_CACHE.get(key)
        if cached is not None:
            LOGGER.info("embedding_cache_hit model=%s text_len=%s", QDRANT_EMBEDDING_MODEL, len(key))
            return cached
        LOGGER.info("embedding_cache_miss model=%s text_len=%s", QDRANT_EMBEDDING_MODEL, len(key))
        try:
            response = self.embedding_client.models.embed_content(
                model=QDRANT_EMBEDDING_MODEL,
                contents=text,
            )
            if not getattr(response, "embeddings", None):
                return None
            result = list(response.embeddings[0].values)
            cache = self.__class__._EMBED_CACHE
            if len(cache) >= self.__class__._EMBED_CACHE_MAX:
                # Evict oldest quarter to bound memory
                drop = list(cache.keys())[:self.__class__._EMBED_CACHE_MAX // 4]
                for k in drop:
                    cache.pop(k, None)
            cache[key] = result
            return result
        except Exception:
            return None

    def _pattern_point_id(self, pattern_id: int, language: str, tenant_id: str | None = None) -> int:
        digest = hashlib.md5(f"pattern:{tenant_id or get_tenant_id()}:{pattern_id}:{language}".encode("utf-8")).hexdigest()
        return int(digest[:15], 16)

    def _prepare_pattern_embedding_text(self, pattern_text: str, semantic_concept: str) -> str:
        normalized_pattern = re.sub(r"\*", " entity ", str(pattern_text or "").strip().lower())
        normalized_pattern = re.sub(r"\s+", " ", normalized_pattern).strip()
        concept_text = str(semantic_concept or "").strip().lower().replace("_", " ")
        if concept_text:
            return f"{normalized_pattern} semantic concept {concept_text}".strip()
        return normalized_pattern

    def _normalize_question_for_pattern_semantics(self, question: str, entity_hint: str | None = None) -> str:
        text = str(question or "").strip()
        if not text:
            return ""

        candidate_entity = entity_hint or self._best_entity_hint(text)
        if candidate_entity:
            text = re.sub(re.escape(str(candidate_entity)), " entity ", text, flags=re.IGNORECASE)

        text = re.sub(r"\b(?:could\s+you|can\s+you|would\s+you|please|bitte|kannst\s+du|koenntest\s+du)\b", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\b(?:tell\s+me|show\s+me|gib\s+mir|zeige\s+mir)\b", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"[\?\!\.,;:]", " ", text)
        text = re.sub(r"\s+", " ", text).strip().lower()
        return text

    def _list_patterns_for_vector_sync(self) -> list[Any]:
        if not self.pattern_library:
            return []
        out: list[Any] = []
        seen: set[int] = set()
        for lang in ("en", "de"):
            try:
                items = self.pattern_library.list_patterns(language=lang)
            except Exception:
                items = []
            for item in items:
                pattern_id = getattr(item, "pattern_id", None)
                if not isinstance(pattern_id, int):
                    continue
                if pattern_id in seen:
                    continue
                seen.add(pattern_id)
                out.append(item)
        return out

    def _ensure_semantic_pattern_vectors(self, force: bool = False) -> bool:
        if (
            self.qdrant is None
            or PointStruct is None
            or VectorParams is None
            or Distance is None
            or not self.pattern_library
        ):
            return False

        now = datetime.utcnow()
        tenant_id = get_tenant_id()
        if (
            not force
            and self._pattern_vectors_ready
            and self._pattern_vectors_last_sync is not None
            and self._pattern_vectors_tenant == tenant_id
            and (now - self._pattern_vectors_last_sync).total_seconds() < 600
        ):
            return True

        try:
            try:
                self.qdrant.get_collection(collection_name=self._pattern_vector_collection)
            except Exception:
                self.qdrant.create_collection(
                    collection_name=self._pattern_vector_collection,
                    vectors_config={
                        "dense": VectorParams(size=QDRANT_VECTOR_SIZE, distance=Distance.COSINE),
                    },
                )

            patterns = self._list_patterns_for_vector_sync()
            points: list[Any] = []
            for pattern in patterns:
                pattern_id = getattr(pattern, "pattern_id", None)
                pattern_text = str(getattr(pattern, "pattern_text", "") or "").strip()
                pattern_language = str(getattr(pattern, "pattern_language", "en") or "en").strip().lower()
                semantic_concept = str(getattr(pattern, "semantic_concept", "") or "").strip().lower()
                mapped_attributes = getattr(pattern, "mapped_attributes", {}) or {}
                confidence = float(getattr(pattern, "confidence", 0.0) or 0.0)
                if not isinstance(pattern_id, int) or not pattern_text or not semantic_concept:
                    continue

                embedding_text = self._prepare_pattern_embedding_text(pattern_text, semantic_concept)
                dense = self.embed_text(embedding_text)
                if not dense:
                    continue

                payload = {
                    "tenant_id": tenant_id,
                    "pattern_id": pattern_id,
                    "pattern_text": pattern_text,
                    "semantic_concept": semantic_concept,
                    "mapped_attributes": mapped_attributes,
                    "pattern_language": pattern_language,
                    "confidence": confidence,
                }
                points.append(
                    PointStruct(
                        id=self._pattern_point_id(pattern_id, pattern_language, tenant_id),
                        vector={"dense": dense},
                        payload=payload,
                    )
                )

            for virtual in list(self._VIRTUAL_SEMANTIC_PATTERNS):
                pattern_text = str(virtual.get("pattern_text") or "").strip()
                semantic_concept = str(virtual.get("semantic_concept") or "").strip().lower()
                pattern_language = str(virtual.get("pattern_language") or "en").strip().lower()
                mapped_attributes = virtual.get("mapped_attributes") if isinstance(virtual.get("mapped_attributes"), dict) else {}
                confidence = float(virtual.get("confidence") or 0.80)
                if not pattern_text or not semantic_concept:
                    continue
                embedding_text = self._prepare_pattern_embedding_text(pattern_text, semantic_concept)
                dense = self.embed_text(embedding_text)
                if not dense:
                    continue
                virtual_id = self._pattern_point_id(abs(hash(f"virtual:{pattern_language}:{semantic_concept}:{pattern_text}")), pattern_language, tenant_id)
                payload = {
                    "tenant_id": tenant_id,
                    "pattern_id": None,
                    "pattern_text": pattern_text,
                    "semantic_concept": semantic_concept,
                    "mapped_attributes": mapped_attributes,
                    "pattern_language": pattern_language,
                    "confidence": confidence,
                    "source_type": "virtual_seed",
                }
                points.append(
                    PointStruct(
                        id=virtual_id,
                        vector={"dense": dense},
                        payload=payload,
                    )
                )

            if points:
                self.qdrant.upsert(collection_name=self._pattern_vector_collection, points=points, wait=True)

            self._pattern_vectors_ready = True
            self._pattern_vectors_last_sync = now
            self._pattern_vectors_tenant = tenant_id
            return True
        except Exception:
            return False

    def _semantic_vector_pattern_match(self, question: str, language: str = "en", entity_hint: str | None = None) -> Any | None:
        if not self._ensure_semantic_pattern_vectors(force=False):
            return None
        normalized_question = self._normalize_question_for_pattern_semantics(question, entity_hint=entity_hint)
        query_texts = [normalized_question, str(question or "").strip()]
        concept_bucket: dict[str, dict[str, Any]] = {}

        for idx, query_text in enumerate(query_texts):
            if not query_text:
                continue
            dense = self.embed_text(query_text)
            if not dense:
                continue

            points = self._qdrant_dense_search_points(
                collection_name=self._pattern_vector_collection,
                dense_vector=dense,
                limit=14,
                with_payload=True,
                vector_name="dense",
            )
            if not points:
                continue

            bonus = 0.02 if idx == 0 else 0.0
            for point in points:
                payload = getattr(point, "payload", None) or {}
                point_lang = str(payload.get("pattern_language") or "en").lower()
                if point_lang != str(language or "en").lower() and point_lang not in {"en", "de"}:
                    continue
                concept = str(payload.get("semantic_concept") or "").strip().lower()
                if not concept:
                    continue

                score = float(getattr(point, "score", 0.0) or 0.0) + bonus
                state = concept_bucket.get(concept)
                if state is None:
                    concept_bucket[concept] = {
                        "best_score": score,
                        "payload": payload,
                        "hits": 1,
                    }
                else:
                    state["hits"] = int(state.get("hits") or 0) + 1
                    if score > float(state.get("best_score") or 0.0):
                        state["best_score"] = score
                        state["payload"] = payload

        if not concept_bucket:
            return None

        ranked_concepts: list[tuple[float, dict[str, Any]]] = []
        for _concept, state in concept_bucket.items():
            best_score = float(state.get("best_score") or 0.0)
            hits = int(state.get("hits") or 0)
            concept_score = best_score + min(0.05, max(0, hits - 1) * 0.015)
            ranked_concepts.append((concept_score, state))

        ranked_concepts.sort(key=lambda x: x[0], reverse=True)
        best_score, best_state = ranked_concepts[0]
        second_score = ranked_concepts[1][0] if len(ranked_concepts) > 1 else 0.0

        if best_score < 0.58:
            return None
        if (best_score - second_score) < 0.008 and best_score < 0.67:
            return None

        best_payload = best_state.get("payload") if isinstance(best_state, dict) else None
        if not isinstance(best_payload, dict):
            return None

        if SemanticPattern is None:
            return None

        return SemanticPattern(
            pattern_id=int(best_payload.get("pattern_id")) if str(best_payload.get("pattern_id") or "").isdigit() else None,
            pattern_text=str(best_payload.get("pattern_text") or "").strip(),
            semantic_concept=str(best_payload.get("semantic_concept") or "").strip(),
            mapped_attributes=best_payload.get("mapped_attributes") if isinstance(best_payload.get("mapped_attributes"), dict) else {},
            computation_rule=None,
            entity_class=None,
            source_type=str(best_payload.get("source_type") or "vector_semantic"),
            confidence=float(min(0.99, max(0.0, best_score))),
            pattern_language=str(best_payload.get("pattern_language") or language or "en").strip() or "en",
            metadata={"vector_score": best_score},
        )

    def _extract_semantic_shareholding_query(self, text: str, language: str) -> ParsedQuery | None:
        """Lightweight dispatcher — extracts entity and delegates to generic relationship gate."""
        raw_text = str(text or "").strip()
        if not raw_text:
            return None

        normalized = self._normalize_semantic_text(raw_text)
        tokens = set(re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", normalized))
        share_tokens = {
            "share", "shares", "shareholder", "shareholders", "stock", "stocks",
            "aktionar", "aktionare", "gesellschafter", "anteil", "anteile", "anteilseigner", "eigentumer",
        }
        if not any(tok in tokens for tok in share_tokens):
            return None

        def _clean_entity(value: str | None) -> str | None:
            if not value:
                return None
            cleaned = re.sub(r"\s+(?:and|und)\s+.*$", "", str(value).strip(" ."), flags=re.IGNORECASE)
            cleaned = re.sub(r"\s+'?s$", "", cleaned, flags=re.IGNORECASE)
            return self._normalize_entity_name_hint(cleaned)

        # Person-specific share-count query: "how many shares does <person> hold in <company>?"
        count_patterns = [
            r"(?:how\s+many|number\s+of|count\s+of)\s+shares?\s+(?:does|do)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)\s+(?:hold|own|have)\s+(?:in|of)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
            r"wie\s+viele\s+anteile?\s+(?:hat|h[aä]lt|haelt)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)\s+(?:an|bei|in|von)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
        ]
        for pattern in count_patterns:
            m = re.search(pattern, raw_text, flags=re.IGNORECASE)
            if not m:
                continue
            person_name = _clean_entity(m.group(1))
            company_name = _clean_entity(m.group(2))
            if person_name and company_name:
                return ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=person_name,
                    attribute_name="shareholders",
                    confidence=0.9,
                    language=language,
                    criteria={
                        "target_company": company_name,
                        "question_type": "share_count",
                        "semantic_frame": "share_count",
                        "relationship_concept": "shareholder",
                    },
                )

        # Company-centric: who are the shareholders of <company>?
        asks_counts = bool(
            re.search(
                r"\b(?:how\s+many\s+shares?\s+(?:each|each\s+one|per\s+shareholder)|shares?\s+each\s+(?:holds?|owns?)|each\s+holdings?|holdings?\s+each|wie\s+viele\s+anteile\s+(?:je|pro))\b",
                raw_text,
                flags=re.IGNORECASE,
            )
        )
        company_patterns = [
            r"(?:who\s+are|list|show|get|find).+?(?:shareholders?|stockholders?)\s+(?:of|in|for)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
            r"(?:wer\s+sind|zeige|liste|gib\s+mir).+?(?:aktion[aä]re|gesellschafter|anteilseigner)\s+(?:von|bei|der|des)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
            r"(?:shares?|anteile?).+?(?:in|of|an|bei|von)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
        ]
        for pattern in company_patterns:
            m = re.search(pattern, raw_text, flags=re.IGNORECASE)
            if not m:
                continue
            company_name = _clean_entity(m.group(1))
            if company_name:
                return ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=company_name,
                    attribute_name="shareholders",
                    confidence=0.86,
                    language=language,
                    criteria={
                        "question_type": "shareholders_with_counts" if asks_counts else "shareholders_list",
                        "semantic_frame": "shareholders",
                        "relationship_concept": "shareholder",
                    },
                )

        # Person-centric: what companies does <person> hold shares in?
        person_patterns = [
            r"(?:what|which|in\s+which)\s+companies\s+(?:does|do)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)\s+(?:hold|own|have)\s+shares?(?:\s+in)?(?:[\?\.!]|$)",
            r"(?:where)\s+(?:does|do)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)\s+(?:hold|own|have)\s+shares?(?:[\?\.!]|$)",
            r"(?:in\s+welchen\s+unternehmen|bei\s+welchen\s+unternehmen)\s+(?:hat|h[aä]lt|haelt)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)\s+anteile?(?:[\?\.!]|$)",
        ]
        for pattern in person_patterns:
            m = re.search(pattern, raw_text, flags=re.IGNORECASE)
            if not m:
                continue
            person_name = _clean_entity(m.group(1))
            if person_name:
                return ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=person_name,
                    attribute_name="shareholders",
                    confidence=0.85,
                    language=language,
                    criteria={"question_type": "companies_held", "semantic_frame": "shareholders_person",
                               "relationship_concept": "shareholder"},
                )

        return None

    def _semantic_frame_document_about_entity(
        self,
        text: str,
        language: str,
        default_entity_hint: str | None,
    ) -> ParsedQuery | None:
        return self._extract_semantic_doc_note_entity_criteria(
            text=text,
            language=language,
            time_window=self._extract_time_window(text),
            default_entity_hint=default_entity_hint,
        )

    def _semantic_frame_shareholding(
        self,
        text: str,
        language: str,
        default_entity_hint: str | None,
    ) -> ParsedQuery | None:
        _ = default_entity_hint
        return self._extract_semantic_shareholding_query(text=text, language=language)

    def _route_semantic_intent_frame(
        self,
        text: str,
        language: str,
        default_entity_hint: str | None = None,
    ) -> ParsedQuery | None:
        frames = sorted(self.SEMANTIC_INTENT_FRAME_REGISTRY, key=lambda x: int(x.get("priority") or 999))
        for frame in frames:
            handler_name = str(frame.get("handler") or "").strip()
            if not handler_name:
                continue
            handler = getattr(self, handler_name, None)
            if not callable(handler):
                continue
            try:
                matched = handler(text, language, default_entity_hint)
            except Exception:
                continue
            if not isinstance(matched, ParsedQuery):
                continue
            criteria = matched.criteria if isinstance(matched.criteria, dict) else {}
            criteria.setdefault("semantic_frame", str(frame.get("name") or "semantic_frame"))
            criteria.setdefault("parse_method", "semantic_frame_registry")
            matched.criteria = criteria
            return matched
        return None

    def _build_match_telemetry(self, parsed: ParsedQuery) -> dict[str, Any]:
        criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        pattern_source = str(criteria.get("pattern_source") or "").strip()
        parse_method = str(criteria.get("parse_method") or "").strip()
        semantic_frame = str(criteria.get("semantic_frame") or "").strip() or None
        matched_pattern = str(criteria.get("matched_pattern") or "").strip() or None

        if not parse_method:
            if pattern_source in {"vector_semantic", "virtual_seed"}:
                parse_method = "vector_semantic_pattern"
            elif matched_pattern or criteria.get("pattern_id") is not None:
                parse_method = "deterministic_pattern"
            elif parsed.intent == "criteria_lookup" and semantic_frame:
                parse_method = "semantic_frame_registry"
            elif parsed.intent == "semantic_lookup":
                parse_method = "semantic_fallback"
            else:
                parse_method = "rule_based"

        return {
            "parse_method": parse_method,
            "pattern_source": pattern_source or None,
            "semantic_frame": semantic_frame,
            "matched_pattern": matched_pattern,
            "confidence": float(parsed.confidence or 0.0),
            "intent": parsed.intent,
        }

    def _attach_match_telemetry(self, data: Any, telemetry: dict[str, Any]) -> Any:
        if isinstance(data, dict):
            enriched = dict(data)
            enriched["match_telemetry"] = dict(telemetry or {})
            return enriched
        return {
            "value": data,
            "match_telemetry": dict(telemetry or {}),
        }

    def _parse_method_of(self, parsed: ParsedQuery) -> str:
        criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        return str(criteria.get("parse_method") or "").strip()

    def _parse_candidate_score(self, parsed: ParsedQuery) -> float:
        score = float(parsed.confidence or 0.0)
        method = self._parse_method_of(parsed)

        if method == "deterministic_pattern":
            score += 0.15
        elif method == "semantic_frame_registry":
            score += 0.10
        elif method == "criteria_rule":
            score += 0.06
        elif method == "explicit_attribute_lexical":
            score += 0.04
        elif method == "llm_structured_plan":
            score += 0.08
        elif method == "identifier_multilang_gate":
            score -= 0.08
        elif method == "llm_intent_parser":
            score -= 0.02

        if parsed.intent == "semantic_lookup":
            score -= 0.05
        return score

    def _build_parse_artifact(
        self,
        question: str,
        *,
        default_entity_hint: str | None = None,
        detected_language: str | None = None,
        intent: str | None = None,
        criteria: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        text = str(question or "").strip()
        lowered = text.lower()
        words = re.findall(r"[a-zA-Z0-9][a-zA-Z0-9_\-/]{1,}", lowered)
        verb_words = {
            "list", "show", "find", "give", "display", "retrieve", "fetch", "search", "generate",
            "showing", "listing", "displaying", "looking", "give", "finds", "showed",
        }
        adverb_words = {"all", "recent", "latest", "new", "full", "directly", "briefly", "only", "exactly"}

        nouns: list[str] = []
        verbs: list[str] = []
        adverbs: list[str] = []
        entities: list[str] = []
        categories: list[str] = []
        seen: set[str] = set()
        spacy_entities: list[dict[str, str]] = []
        noun_chunks: list[str] = []
        dependency_relations: list[dict[str, str]] = []
        prepositional_constraints: list[str] = []

        nlp = self._get_spacy_nlp()
        if nlp is not None and text:
            try:
                doc = nlp(text)
                for entity in getattr(doc, "ents", []):
                    entity_text = str(getattr(entity, "text", "") or "").strip()
                    entity_label = str(getattr(entity, "label_", "") or "").strip().upper()
                    if entity_text and entity_label:
                        spacy_entities.append({"text": entity_text, "label": entity_label})
                for chunk in getattr(doc, "noun_chunks", []):
                    chunk_text = str(getattr(chunk, "text", "") or "").strip()
                    if chunk_text:
                        noun_chunks.append(chunk_text)
                for token in doc:
                    dep = str(getattr(token, "dep_", "") or "")
                    if dep in {"prep", "pobj", "nmod", "obl"}:
                        token_text = str(getattr(token, "text", "") or "").strip()
                        head_text = str(getattr(getattr(token, "head", None), "text", "") or "").strip()
                        if token_text:
                            dependency_relations.append({"token": token_text, "dependency": dep, "head": head_text})
                    if dep in {"pobj", "obl"}:
                        head = getattr(token, "head", None)
                        head_dep = str(getattr(head, "dep_", "") or "")
                        if head_dep == "prep":
                            phrase = f"{getattr(head, 'text', '')} {getattr(token, 'text', '')}".strip()
                            if phrase:
                                prepositional_constraints.append(phrase)
            except Exception:
                pass

        for word in words:
            clean = re.sub(r"[^a-z0-9]", "", word.lower())
            if not clean:
                continue
            if clean in verb_words:
                if clean not in seen:
                    verbs.append(clean)
                    seen.add(clean)
            if clean in adverb_words:
                if clean not in seen:
                    adverbs.append(clean)
                    seen.add(clean)
            if len(clean) >= 3 and clean not in {"show", "list", "find", "give", "display", "retrieve", "fetch", "search", "generate"}:
                nouns.append(clean)

        if default_entity_hint:
            entity_hint = str(default_entity_hint).strip().lower()
            for token in re.findall(r"[a-zA-Z0-9]{2,}", entity_hint):
                if token and token not in {"with", "their", "from", "for", "in", "and", "the", "all"}:
                    entities.append(token)
        if isinstance(criteria, dict):
            for key in ("entity_type", "relationship_concept", "document_type"):
                value = str(criteria.get(key) or "").strip()
                if value:
                    for token in re.findall(r"[a-zA-Z0-9]{2,}", value.lower()):
                        if token not in {"with", "their", "from", "for", "in", "and", "the", "all"}:
                            entities.append(token)

        for entity in spacy_entities:
            if entity.get("label") in {"PERSON", "ORG", "GPE", "LOC", "PRODUCT"}:
                normalized_entity = self._normalize_entity_name_hint(entity.get("text")) or entity.get("text", "").strip()
                if normalized_entity:
                    entities.append(normalized_entity)

        if re.search(r"\b(?:list|show|get|find|generate|display|retrieve|search)\b", lowered, flags=re.IGNORECASE):
            categories.append("entity_listing")
        if re.search(r"\b(?:with|where|from|for|in)\b", lowered, flags=re.IGNORECASE):
            categories.append("filtered_lookup")
        if re.search(r"\b(?:address|email|phone|contact|telephone|details|profile)\b", lowered, flags=re.IGNORECASE):
            categories.append("attribute_access")
        if re.search(r"\b(?:supplier|customer|employee|invoice|document|record|account)\b", lowered, flags=re.IGNORECASE):
            categories.append("entity_type")
        if str(intent or "").strip().lower() in {"criteria_lookup", "attribute_lookup", "semantic_lookup"}:
            categories.append(str(intent).strip().lower())

        normalized = {
            "artifact_type": "query_extraction",
            "version": 2,
            "question_text": lowered,
            "raw_text": lowered,
            "nouns": sorted(dict.fromkeys(nouns))[:25],
            "verbs": sorted(dict.fromkeys(verbs))[:25],
            "adverbs": sorted(dict.fromkeys(adverbs))[:25],
            "entities": sorted(dict.fromkeys([value for value in entities if value]))[:25],
            "categories": sorted(dict.fromkeys(categories))[:25],
            "language": str(detected_language or "en").strip().lower() or "en",
            "spacy": {
                "entities": spacy_entities[:25],
                "noun_chunks": sorted(dict.fromkeys(noun_chunks))[:25],
                "dependency_relations": dependency_relations[:40],
                "prepositional_constraints": sorted(dict.fromkeys(prepositional_constraints))[:20],
            },
            "attributes": [],
            "constraints": list(criteria.get("attribute_filters") or []) if isinstance(criteria, dict) else [],
            "document_hints": [],
            "temporal_hints": [],
            "intent": str(intent or "").strip().lower() or "semantic_lookup",
        }
        if isinstance(criteria, dict):
            normalized["attributes"] = [
                str(value).strip()
                for value in list(criteria.get("attribute_hints") or [])
                if str(value).strip()
            ]
            time_window = criteria.get("time_window")
            if isinstance(time_window, dict):
                normalized["temporal_hints"].append(time_window)
            elif time_window:
                normalized["temporal_hints"].append(str(time_window))
            geography = criteria.get("geography") if isinstance(criteria.get("geography"), dict) else {}
            if geography:
                normalized["constraints"].append({"kind": "geography", **geography})
        document_hint_terms = {
            "letter", "notice", "invoice", "contract", "email", "mail", "registration", "document",
            "brief", "schreiben", "rechnung", "vertrag", "e-mail",
        }
        normalized["document_hints"] = sorted(
            {token for token in re.findall(r"[a-zA-Z0-9-]{3,}", lowered) if token in document_hint_terms}
        )
        if re.search(r"\b(?:last|latest|recent|newest|previous|letzte|aktuell)\b", lowered, flags=re.IGNORECASE):
            normalized["temporal_hints"].append("recency")
        if not normalized["entities"]:
            for token in re.findall(r"[a-zA-Z0-9]{2,}", lowered):
                if token.lower() not in {"list", "show", "find", "give", "display", "retrieve", "fetch", "search", "generate", "their", "full", "addresses", "contact", "persons", "from", "italy", "with", "all", "the", "and", "for", "in", "me"}:
                    normalized["entities"].append(token.lower())
            normalized["entities"] = sorted(dict.fromkeys(normalized["entities"]))[:25]
        return normalized

    def _attach_parse_artifact(
        self,
        parsed: ParsedQuery | None,
        question: str,
        *,
        default_entity_hint: str | None = None,
        detected_language: str | None = None,
    ) -> ParsedQuery | None:
        if not isinstance(parsed, ParsedQuery):
            return parsed
        criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        parsed.parse_artifact = self._build_parse_artifact(
            question,
            default_entity_hint=default_entity_hint,
            detected_language=detected_language or parsed.language,
            intent=parsed.intent,
            criteria=criteria,
        )
        return parsed

    def _parse_artifact_routing_score(self, parsed: ParsedQuery) -> float:
        artifact = parsed.parse_artifact if isinstance(parsed.parse_artifact, dict) else {}
        categories = [str(item).strip().lower() for item in artifact.get("categories") or []]
        nouns = [str(item).strip().lower() for item in artifact.get("nouns") or []]
        score = 0.0

        if parsed.intent == "criteria_lookup" and "entity_listing" in categories:
            score += 0.22
        if parsed.intent == "attribute_lookup" and "attribute_access" in categories:
            score += 0.12
        if parsed.intent == "attribute_lookup" and any(token in {"email", "address", "contact", "telephone", "phone"} for token in nouns):
            score += 0.10
        if parsed.intent == "criteria_lookup" and "filtered_lookup" in categories:
            score += 0.06
        if parsed.intent == "criteria_lookup" and parsed.entity_name:
            score += 0.04
        return score

    def _select_best_parse_candidate(self, candidates: list[ParsedQuery]) -> ParsedQuery | None:
        usable = [item for item in (candidates or []) if isinstance(item, ParsedQuery)]
        if not usable:
            return None

        for item in usable:
            if not isinstance(item.parse_artifact, dict):
                item.parse_artifact = self._build_parse_artifact(
                    str(getattr(item, "entity_name", "") or "") + " " + str(getattr(item, "attribute_name", "") or ""),
                    default_entity_hint=item.entity_name,
                    detected_language=item.language,
                    intent=item.intent,
                    criteria=item.criteria if isinstance(item.criteria, dict) else {},
                )

        scored = []
        for item in usable:
            base_score = float(self._parse_candidate_score(item)) + self._parse_artifact_routing_score(item)
            scored.append((base_score, item))

        ranked = sorted(scored, key=lambda pair: pair[0], reverse=True)
        best_score, best = ranked[0]
        best_method = self._parse_method_of(best)

        # Generic anti-hijack arbitration: if lexical identifier parsing narrowly wins,
        # prefer a near-tie semantic/criteria candidate.
        if best_method == "identifier_multilang_gate":
            for score, item in ranked[1:]:
                method = self._parse_method_of(item)
                if method != "identifier_multilang_gate" and score + 0.02 >= best_score:
                    best = item
                    best_score = score
                    break

        criteria = best.criteria if isinstance(best.criteria, dict) else {}
        criteria.setdefault("arbitration", "candidate_scoring")
        criteria.setdefault("arbitration_score", round(best_score, 6))
        criteria.setdefault("arbitration_candidates", len(ranked))
        best.criteria = criteria
        return best

    def _pattern_match_to_parsed_query(
        self,
        pattern_match: Any,
        text: str,
        entity_hint: str | None,
        matched_language: str,
    ) -> ParsedQuery:
        template_text = str(getattr(pattern_match, "pattern_text", "") or "").strip()
        wildcard_groups: list[str] = []
        if "*" in template_text:
            wildcard_groups = self._extract_pattern_wildcard_groups(template_text, text)
            if wildcard_groups and not entity_hint:
                captured_entity = " ".join([part for part in wildcard_groups if str(part).strip()])
                normalized_captured = self._normalize_entity_name_hint(captured_entity)
                if normalized_captured:
                    entity_hint = normalized_captured

        mapped = getattr(pattern_match, "mapped_attributes", {}) or {}
        raw_attrs = list(mapped.values()) if isinstance(mapped, dict) else []
        scalar_attrs: list[str] = []
        for item in raw_attrs:
            if isinstance(item, str):
                if item.strip():
                    scalar_attrs.append(item.strip())
            elif isinstance(item, (list, tuple)):
                for sub in item:
                    if isinstance(sub, str) and sub.strip():
                        scalar_attrs.append(sub.strip())

        # Optional pattern metadata for generic relation-filtered attribute lookup.
        relation_filters_raw = mapped.get("relation_filters") if isinstance(mapped, dict) else None
        relation_filters_sub = self._substitute_wildcard_refs(relation_filters_raw, wildcard_groups)
        relation_filters: list[dict[str, Any]] = []
        if isinstance(relation_filters_sub, list):
            for item in relation_filters_sub:
                if not isinstance(item, dict):
                    continue
                rel_names = item.get("relationship_names")
                target_name = str(item.get("target") or item.get("target_name") or "").strip()
                if isinstance(rel_names, str):
                    rel_names = [rel_names]
                rel_list = [str(r).strip() for r in list(rel_names or []) if str(r).strip()]
                if rel_list and target_name:
                    relation_filters.append(
                        {
                            "relationship_names": rel_list,
                            "target_name": target_name,
                        }
                    )
        target_entity_class = (
            str(self._substitute_wildcard_refs(mapped.get("target_entity_class"), wildcard_groups) or "").strip()
            if isinstance(mapped, dict)
            else ""
        )

        dedup_attrs: list[str] = []
        for attr in scalar_attrs:
            if attr not in dedup_attrs:
                dedup_attrs.append(attr)
        scalar_attrs = dedup_attrs

        attr_name = scalar_attrs[0] if scalar_attrs else None
        if not attr_name and not scalar_attrs:
            raise ValueError("Pattern matched but produced no scalar attribute; deferring to LLM parser")

        concept_name = str(getattr(pattern_match, "semantic_concept", "") or "").strip().lower()
        entity_optional_concepts = {
            "duration",
            "travel_duration",
            "duration_years",
            "relationship_duration",
            "tenure",
        }
        has_relation_filters = bool(relation_filters)
        if not entity_hint and not has_relation_filters and (attr_name or scalar_attrs) and concept_name not in entity_optional_concepts:
            raise ValueError("Pattern matched but no entity extracted; deferring to LLM parser")

        criteria_payload: dict[str, Any] = {
            "pattern_id": getattr(pattern_match, "pattern_id", None),
            "matched_pattern": getattr(pattern_match, "pattern_text", None),
            "pattern_source": str(getattr(pattern_match, "source_type", "") or "library"),
        }
        if relation_filters:
            criteria_payload["relation_filters"] = relation_filters
        if target_entity_class:
            criteria_payload["target_entity_class"] = target_entity_class

        return ParsedQuery(
            intent="attribute_lookup",
            entity_name=entity_hint,
            attribute_name=attr_name,
            confidence=float(getattr(pattern_match, "confidence", 0.0) or 0.0),
            language=matched_language,
            attribute_names=scalar_attrs or None,
            criteria=criteria_payload,
        )

    def _match_deterministic_pattern_query(
        self,
        text: str,
        detected_language: str,
        default_entity_hint: str | None,
    ) -> ParsedQuery | None:
        """Run deterministic DB-backed pattern matching without vector fallback.

        This provides a generic anti-hijack path for high-confidence semantic patterns
        and avoids query-specific hardcoded parser bypasses.
        """
        pattern_library = getattr(self, "pattern_library", None)
        if not pattern_library:
            return None

        try:
            entity_hint = default_entity_hint
            if not entity_hint:
                tail_match = re.search(r"(?:is|ist|of|for|von|für)\s+(.+?)(?:\?|$)", text, flags=re.IGNORECASE)
                if tail_match:
                    entity_hint = self._normalize_entity_name_hint((tail_match.group(1) or "").strip(" ."))

            candidate_languages = [detected_language, "en", "de"]
            seen_languages = set()
            for lang in candidate_languages:
                if not lang or lang in seen_languages:
                    continue
                seen_languages.add(lang)
                current_match = pattern_library.match_pattern(text, None, lang)
                if not current_match or current_match.confidence < 0.75:
                    continue

                source_type = str(getattr(current_match, "source_type", "") or "")
                # Keep this helper deterministic-only.
                if source_type == "vector_semantic":
                    continue

                parsed = self._pattern_match_to_parsed_query(
                    pattern_match=current_match,
                    text=text,
                    entity_hint=entity_hint,
                    matched_language=lang,
                )
                if isinstance(parsed.criteria, dict):
                    parsed.criteria.setdefault("parse_method", "deterministic_pattern")
                    parsed.criteria.setdefault("semantic_concept", str(getattr(current_match, "semantic_concept", "") or ""))
                return parsed
        except Exception:
            return None

        return None

    def _schema_attribute_match_for_text(self, text: str, entity_name: str | None) -> str | None:
        candidate_text = str(text or "").strip()
        if not candidate_text or not entity_name:
            return None

        schema_names = [
            str(item).strip()
            for item in self._get_entity_attribute_names(entity_name)
            if str(item).strip()
        ]
        if not schema_names:
            return None

        normalized_schema = {
            self._normalize_alias_key(item): item for item in schema_names if self._normalize_alias_key(item)
        }
        if not normalized_schema:
            return None

        candidates: list[str] = []
        candidates.extend(self._attribute_rescue_candidates_from_text(candidate_text))
        for token in re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", candidate_text.lower()):
            if len(token) >= 3:
                candidates.append(token)

        # First try exact/normalised literal schema matches.
        for candidate in candidates:
            normalized = self._normalize_alias_key(candidate)
            if not normalized:
                continue
            if normalized in normalized_schema:
                return normalized_schema[normalized]

        # Then allow semantic schema resolution (alias/embedding-backed) when the
        # wording is a paraphrase or near-synonym rather than an exact column name.
        for candidate in candidates:
            resolved_attr, kind = self._resolve_schema_term(candidate, entity_name)
            if kind in {"attribute", "relationship", "entity_type", "category"}:
                attr_name = str(resolved_attr or "").strip()
                if attr_name and attr_name in schema_names:
                    return attr_name

        resolved_attr, kind = self._resolve_schema_term(candidate_text, entity_name)
        if kind in {"attribute", "relationship", "entity_type", "category"}:
            attr_name = str(resolved_attr or "").strip()
            if attr_name and attr_name in schema_names:
                return attr_name

        return None

    def _extract_contact_person_details_intent(
        self,
        text: str,
        detected_language: str,
        default_entity_hint: str | None = None,
    ) -> ParsedQuery | None:
        lowered = str(text or "").strip().lower()
        if not lowered:
            return None

        broad_scope = self._has_broad_attribute_scope_cue(text)
        has_process_anchor = bool(re.search(
            r"\b(?:process|eori|customs|zoll|vat|mwst|registration|registrierung|application|antrag|clearance|permit|license|lizenz)\b",
            lowered,
            flags=re.IGNORECASE,
        ))
        if has_process_anchor:
            return None

        has_contact_person_cue = bool(
            re.search(
                r"\b(?:contact\s+persons?|contact\s+person|contacts?|kontaktpersonen?|kontaktperson|ansprechpartner(?:in)?(?:s)?)\b",
                lowered,
                flags=re.IGNORECASE,
            )
        )
        if not has_contact_person_cue:
            return None

        requested_attrs: list[str] = []
        if self._has_email_intent(text):
            requested_attrs.append("email")
        if re.search(r"\b(?:telephone|phone|mobile|tel|telefon|phone\s*number|telephone\s*number)\b", lowered, flags=re.IGNORECASE):
            requested_attrs.append("telephone")
        if re.search(r"\b(?:name|contact\s*name|person\s*name|kontaktname)\b", lowered, flags=re.IGNORECASE):
            requested_attrs.append("name")
        if re.search(r"\b(?:role|position|function|title|rolle|funktion|titel)\b", lowered, flags=re.IGNORECASE):
            requested_attrs.append("role")

        if not broad_scope and not requested_attrs:
            return None

        entity_match = re.search(
            r"(?:contact\s+persons?|contact\s+person|contacts?|kontaktpersonen?|kontaktperson|ansprechpartner(?:in)?(?:s)?)\s+(?:of|for|von|f[üu]r)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
            text,
            flags=re.IGNORECASE,
        )
        if not entity_match:
            return None

        entity_hint = self._normalize_entity_name_hint((entity_match.group(1) or "").strip(" ."))
        if not entity_hint:
            entity_hint = default_entity_hint
        if not entity_hint:
            return None

        if requested_attrs:
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=entity_hint,
                attribute_name="contact_person",
                confidence=0.9,
                language=detected_language,
                relation_name="contact_person",
                attribute_names=requested_attrs,
                criteria={
                    "parse_method": "contact_person_details_gate",
                    "relationship_concept": "contact_person",
                    "broad_scope": True,
                },
            )

        schema_attr = self._schema_attribute_match_for_text(text, entity_hint)
        if schema_attr:
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=entity_hint,
                attribute_name=schema_attr,
                confidence=0.89,
                language=detected_language,
                criteria={
                    "parse_method": "schema_attribute_gate",
                    "schema_attribute": schema_attr,
                    "broad_scope": True,
                },
            )

        return ParsedQuery(
            intent="attribute_lookup",
            entity_name=entity_hint,
            attribute_name="contact_person",
            confidence=0.9,
            language=detected_language,
            relation_name="contact_person",
            criteria={
                "parse_method": "contact_person_details_gate",
                "relationship_concept": "contact_person",
                "broad_scope": True,
            },
        )

    def parse_query(self, question: str) -> ParsedQuery:
        text = question.strip()
        lowered = text.lower()
        detected_language = self._detect_query_language(question)
        default_entity_hint = self._best_entity_hint(text)

        contact_person_details_query = self._extract_contact_person_details_intent(text, detected_language, default_entity_hint)
        if contact_person_details_query:
            return contact_person_details_query

        # Generic relation-details gate: catches forms like
        # "<relationship> details for <entity>" and validates the relationship
        # against the entity schema before routing to attribute lookup.
        relation_detail_match = re.search(
            r"^\s*([A-Za-z0-9äöüÄÖÜß_\- ]+?)\s+details?\s+(?:of|for|von|fuer|für)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
            text,
            flags=re.IGNORECASE,
        )
        if relation_detail_match:
            relation_raw = str(relation_detail_match.group(1) or "").strip()
            entity_raw = str(relation_detail_match.group(2) or "").strip(" .")
            entity_hint = self._normalize_entity_name_hint(entity_raw)
            if relation_raw and entity_hint:
                matched_rel = self._match_entity_relationship_name(entity_hint, relation_raw)
                if not matched_rel:
                    try:
                        with self._get_connection() as _conn:
                            resolved_hint = self._resolve_entity_name_for_lookup(_conn, entity_hint) or entity_hint
                        matched_rel = self._match_entity_relationship_name(resolved_hint, relation_raw)
                        if matched_rel:
                            entity_hint = resolved_hint
                    except Exception:
                        pass
                if matched_rel:
                    return ParsedQuery(
                        intent="attribute_lookup",
                        entity_name=entity_hint,
                        attribute_name=matched_rel,
                        confidence=0.9,
                        language=detected_language,
                        criteria={"parse_method": "generic_relation_detail_gate", "relationship_concept": relation_raw},
                    )

        # ── Generic relationship query gate ──────────────────────────────────
        # "who/what are the <rel> of <entity>?" maps to entity+relationship lookup
        # without needing any hardcoded attribute path. Works for shareholders,
        # directors, eori contact, employees, subsidiaries, etc.
        skip_generic_relationship_gate = bool(
            self._looks_like_identifier_request(text)
            or (
                self._has_email_intent(text)
                and bool(re.search(r"\b(?:contact|kontakt|ansprechpartner|eori)\b", text, flags=re.IGNORECASE))
            )
        )
        _generic_rel_patterns = [
            r"(?:who|what|which)\s+(?:are|is)\s+(?:the\s+)?([a-zA-ZäöüÄÖÜß_\- ]+?)\s+of\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
            r"(?:list|show|get|find|give\s+me)\s+(?:the\s+|all\s+)?([a-zA-ZäöüÄÖÜß_\- ]+?)\s+of\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
            r"(?:wer|was|welche[rns]?)\s+(?:sind|ist)\s+(?:die|der|das\s+)?([a-zA-ZäöüÄÖÜß_\- ]+?)\s+(?:von|der|des|bei)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
        ]
        _skip_rel_terms = {
            "name", "names", "file", "files", "document", "documents", "note", "notes",
            "title", "titles", "path", "address", "email", "date", "value", "attribute",
            "answer",
        }
        for _rp in _generic_rel_patterns:
            if skip_generic_relationship_gate:
                break
            _m = re.search(_rp, text, flags=re.IGNORECASE)
            if not _m:
                continue
            _rel_raw = (_m.group(1) or "").strip().rstrip("s")
            _ent_raw = (_m.group(2) or "").strip(" .")
            _ent_raw = re.sub(r"\s+(?:and|und)\s+.*$", "", _ent_raw, flags=re.IGNORECASE).strip(" .")
            _rel_concept = re.sub(r"[^a-zA-ZäöüÄÖÜß0-9 _\-]", "", _rel_raw).strip()
            _ent_hint = self._normalize_entity_name_hint(_ent_raw)
            if not _rel_concept or not _ent_hint:
                continue
            _rel_words = set(re.findall(r"[a-zA-Z]{3,}", _rel_concept.lower()))
            if _rel_words <= _skip_rel_terms:
                continue
            # Verify this relationship concept actually exists for this entity in the DB
            _matched_rel = self._match_entity_relationship_name(_ent_hint, _rel_concept)
            if not _matched_rel:
                try:
                    with self._get_connection() as _conn:
                        _resolved = self._resolve_entity_name_for_lookup(_conn, _ent_hint) or _ent_hint
                    _matched_rel = self._match_entity_relationship_name(_resolved, _rel_concept)
                    if _matched_rel:
                        _ent_hint = _resolved
                except Exception:
                    pass
            if not _matched_rel:
                continue  # not a known relationship — let other parsers handle it
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=_ent_hint,
                attribute_name=_matched_rel,
                confidence=0.90,
                language=detected_language,
                criteria={"parse_method": "generic_relationship_gate", "relationship_concept": _rel_concept},
            )

        # Explicit ownership gate: keep "who own(s) <company>" deterministic and
        # route through shareholder SQL instead of semantic fallback.
        ownership_gate_match = re.search(
            r"\b(?:who\s+(?:own|owns)\s+)(?:the\s+)?([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:[\?\.!]|$)",
            text,
            flags=re.IGNORECASE,
        )
        if ownership_gate_match:
            ownership_entity = self._normalize_entity_name_hint((ownership_gate_match.group(1) or "").strip(" ."))
            if ownership_entity:
                return ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=ownership_entity,
                    attribute_name="shareholders",
                    confidence=0.92,
                    language=detected_language,
                    criteria={"parse_method": "explicit_ownership_gate", "question_type": "shareholders_list"},
                )

        # High-priority intent gate: file name + full path requests should resolve
        # through document_file_info lookup (which prefers original_local_path).
        asks_file_name = bool(
            re.search(
                r"\b(?:file\s*name|filename|document\s*name|dateiname|dokumentname)\b",
                text,
                flags=re.IGNORECASE,
            )
        )
        asks_full_path = bool(
            re.search(
                r"\b(?:full\s*path|file\s*path|document\s*path|vollst[aä]ndig(?:e|en|em)?\s+pfad|vollstaendig(?:e|en|em)?\s+pfad|pfad)\b",
                text,
                flags=re.IGNORECASE,
            )
        )
        if asks_file_name and asks_full_path:
            file_info_entity = self._extract_entity_for_file_info_intent(text)
            entity_name = file_info_entity or default_entity_hint or self._extract_entity_name_loose(text)
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=entity_name or None,
                attribute_name="document_file_info",
                confidence=0.91,
                language=detected_language,
                criteria={
                    "parse_method": "file_info_intent_gate",
                },
            )

        degree_query = self._extract_degree_institution_query(text, detected_language)
        if degree_query:
            return degree_query

        table_cell_query = None

        # High-priority deterministic gates for ambiguous phrasing that often drifts
        # to neighboring attributes in semantic/pattern matching.
        if default_entity_hint:
            asks_registration_day = bool(
                re.search(
                    r"\b(?:what\s+day|which\s+day|when|welcher\s+tag|wann)\b.*\b(?:registered|registration|registriert|eintragung|register)\b",
                    text,
                    flags=re.IGNORECASE,
                )
            )
            if asks_registration_day:
                return ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=default_entity_hint,
                    attribute_name="tr_date",
                    confidence=0.9,
                    language=detected_language,
                    criteria={"parse_method": "explicit_attribute_lexical"},
                )

            asks_total_capital = bool(
                re.search(
                    r"\b(?:total\s+capital|share\s+capital|nominal\s+capital|gesamtkapital|stammkapital)\b",
                    text,
                    flags=re.IGNORECASE,
                )
            )
            if asks_total_capital:
                return ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=default_entity_hint,
                    attribute_name="total_capital",
                    confidence=0.9,
                    language=detected_language,
                    criteria={"parse_method": "explicit_attribute_lexical"},
                )

        # Generic anti-hijack gate: prefer strong deterministic semantic patterns
        # before lexical identifier routing.
        deterministic_pattern_query = self._match_deterministic_pattern_query(
            text=text,
            detected_language=detected_language,
            default_entity_hint=default_entity_hint,
        )
        arbitration_candidates: list[ParsedQuery] = []
        if deterministic_pattern_query:
            arbitration_candidates.append(deterministic_pattern_query)

        provisional_process_actor = self._extract_process_actor_intent(text, default_entity_hint)
        provisional_process_contact = self._extract_process_contact_intent(text, default_entity_hint)

        if provisional_process_actor:
            arbitration_candidates.append(
                ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=provisional_process_actor["entity_name"],
                    attribute_name=provisional_process_actor["requested_attrs"][0],
                    confidence=0.87,
                    language=detected_language,
                    relation_name=provisional_process_actor["process_concept"],
                    attribute_names=provisional_process_actor["requested_attrs"],
                    criteria={
                        "parse_method": "process_actor_gate",
                        "process_concept": provisional_process_actor["process_concept"],
                    },
                )
            )
        elif provisional_process_contact:
            arbitration_candidates.append(
                ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=provisional_process_contact["entity_name"],
                    attribute_name=provisional_process_contact["requested_attrs"][0],
                    confidence=0.85,
                    language=detected_language,
                    relation_name=provisional_process_contact["process_concept"],
                    attribute_names=provisional_process_contact["requested_attrs"],
                    criteria={
                        "parse_method": "process_contact_gate",
                        "process_concept": provisional_process_contact["process_concept"],
                    },
                )
            )

        # Identifier-first multilingual parser before criteria routing to avoid
        # semantic hijacking (for example EORI-number -> shareholders patterns).
        if ENABLE_LLM_INTENT_PARSER and self.genai_client and not self._looks_like_identifier_request(text) and not provisional_process_actor and not provisional_process_contact:
            llm_structured = self._llm_extract_structured_plan(question, detected_language)
            if llm_structured:
                arbitration_candidates.append(llm_structured)

        # Identifier-first multilingual parser before criteria routing to avoid
        # semantic hijacking (for example EORI-number -> shareholders patterns).
        if self._looks_like_identifier_request(text) and not provisional_process_actor and not provisional_process_contact:
            entity_name = self._normalize_entity_name_hint(default_entity_hint or self._extract_entity_name_loose(text) or "")
            left_text = text
            if entity_name:
                ent_pos = re.search(re.escape(entity_name), text, flags=re.IGNORECASE)
                if ent_pos:
                    left_text = text[: ent_pos.start()].strip(" ?.!,:;")
            if not entity_name:
                ent_match = re.search(
                    r"([A-Za-z0-9äöüÄÖÜßà-ÿ .&_\-]{2,}?\s+(?:GmbH|AG|SA|Sarl|Ltd\.?|Inc\.?|LLC))\b",
                    text,
                    flags=re.IGNORECASE,
                )
                if ent_match:
                    entity_name = self._normalize_entity_name_hint((ent_match.group(1) or "").strip(" ."))
                    left_text = text[: ent_match.start()].strip(" ?.!,:;")
            if entity_name:
                left_text = re.sub(
                    r"^(?:what\s+is|what's|wie\s+lautet|was\s+ist|quel(?:le)?\s+est|"
                    r"cual\s+es|cu[aá]l\s+es|que\s+es|qu[eé]\s+es|dame|muestra|donne(?:z)?|montre(?:z)?)\s+",
                    "",
                    left_text,
                    flags=re.IGNORECASE,
                )
                left_text = re.sub(r"^(?:the|le|la|les|el|los|las|die|der|das)\s+", "", left_text, flags=re.IGNORECASE)
                left_text = re.sub(r"(?:de|du|des|del|von|für|for|of)\s*$", "", left_text, flags=re.IGNORECASE).strip(" -")
                if entity_name and left_text:
                    attr_name, _ = self._resolve_schema_term(left_text, entity_name)
                    if attr_name and not self._query_has_identifier_anchor_for_attr(left_text, attr_name):
                        attr_name = None
                    if not attr_name:
                        attr_name = self._normalize_attribute_name(left_text)
                    if attr_name:
                        arbitration_candidates.append(ParsedQuery(
                            intent="attribute_lookup",
                            entity_name=entity_name,
                            attribute_name=attr_name,
                            confidence=0.9,
                            language=detected_language,
                            criteria={"parse_method": "identifier_multilang_gate"},
                        ))

        # Resolve explicit criteria intents before semantic frames so entity-of-person
        # questions like "invoices of Alice Example" are not hijacked by document-note
        # semantic fallback.
        criteria_query = self._extract_criteria_query(
            text,
            detected_language,
            default_entity_hint=default_entity_hint,
        )
        if not criteria_query:
            criteria_query = self._route_semantic_intent_frame(
                text=text,
                language=detected_language,
                default_entity_hint=default_entity_hint,
            )
        if criteria_query:
            if isinstance(criteria_query.criteria, dict):
                parse_method = str(criteria_query.criteria.get("parse_method") or "").strip()
                if not parse_method and criteria_query.intent == "criteria_lookup":
                    criteria_query.criteria.setdefault("parse_method", "criteria_rule")
                elif not parse_method and criteria_query.intent == "semantic_lookup":
                    criteria_query.criteria.setdefault("parse_method", "semantic_frame_registry")
            arbitration_candidates.append(criteria_query)

        # Identifier-first multilingual parser: extract attribute phrase before a legal entity
        # and resolve it through schema normalization (works for EN/DE/FR/ES variants).
        if self._looks_like_identifier_request(text) and not provisional_process_actor and not provisional_process_contact:
            entity_name = self._normalize_entity_name_hint(default_entity_hint or self._extract_entity_name_loose(text) or "")
            left_text = text
            if entity_name:
                ent_pos = re.search(re.escape(entity_name), text, flags=re.IGNORECASE)
                if ent_pos:
                    left_text = text[: ent_pos.start()].strip(" ?.!,:;")
            if not entity_name:
                ent_match = re.search(
                    r"([A-Za-z0-9äöüÄÖÜßà-ÿ .&_-]{2,}?\s+(?:GmbH|AG|SA|Sarl|Ltd\.?|Inc\.?|LLC))\b",
                    text,
                    flags=re.IGNORECASE,
                )
                if ent_match:
                    entity_name = self._normalize_entity_name_hint((ent_match.group(1) or "").strip(" ."))
                    left_text = text[: ent_match.start()].strip(" ?.!,:;")
            if entity_name:
                left_text = re.sub(
                    r"^(?:what\s+is|what's|wie\s+lautet|was\s+ist|quel(?:le)?\s+est|"
                    r"cual\s+es|cu[aá]l\s+es|que\s+es|qu[eé]\s+es|dame|muestra|donne(?:z)?|montre(?:z)?)\s+",
                    "",
                    left_text,
                    flags=re.IGNORECASE,
                )
                left_text = re.sub(r"^(?:the|le|la|les|el|los|las|die|der|das)\s+", "", left_text, flags=re.IGNORECASE)
                left_text = re.sub(r"(?:de|du|des|del|von|für|for|of)\s*$", "", left_text, flags=re.IGNORECASE).strip(" -")
                if entity_name and left_text:
                    attr_name, _ = self._resolve_schema_term(left_text, entity_name)
                    if attr_name and not self._query_has_identifier_anchor_for_attr(left_text, attr_name):
                        attr_name = None
                    if not attr_name:
                        attr_name = self._normalize_attribute_name(left_text)
                    if attr_name:
                        arbitration_candidates.append(ParsedQuery(
                            intent="attribute_lookup",
                            entity_name=entity_name,
                            attribute_name=attr_name,
                            confidence=0.9,
                            language=detected_language,
                            criteria={"parse_method": "identifier_multilang_gate"},
                        ))

        if self._question_requests_document_listing(text):
            criteria_candidates = [
                cand
                for cand in arbitration_candidates
                if isinstance(cand, ParsedQuery) and cand.intent == "criteria_lookup"
            ]
            if criteria_candidates:
                preferred_criteria = max(criteria_candidates, key=self._parse_candidate_score)
                preferred_meta = preferred_criteria.criteria if isinstance(preferred_criteria.criteria, dict) else {}
                preferred_meta.setdefault("arbitration", "document_listing_preference")
                preferred_criteria.criteria = preferred_meta
                if detected_language and str(preferred_criteria.language or "").strip().lower() != detected_language:
                    preferred_criteria.language = detected_language
                return preferred_criteria

        selected_candidate = self._select_best_parse_candidate(arbitration_candidates)
        if selected_candidate:
            selected_candidate = self._attach_parse_artifact(
                selected_candidate,
                text,
                default_entity_hint=default_entity_hint,
                detected_language=detected_language,
            )
            if detected_language and str(selected_candidate.language or "").strip().lower() != detected_language:
                selected_candidate.language = detected_language
            return selected_candidate

        # High-priority address intent gates to avoid semantic drift to unrelated attributes.
        where_live_patterns = [
            r"(?:where\s+does|where\s+is)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)\s+(?:live|living)(?:\?|$)",
            r"wo\s+wohnt\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|$)",
        ]
        for pattern in where_live_patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                entity_name = self._normalize_entity_name_hint((match.group(1) or "").strip(" ."))
                if entity_name:
                    return ParsedQuery(
                        intent="attribute_lookup",
                        entity_name=entity_name,
                        attribute_name="address",
                        confidence=0.9,
                        language=detected_language,
                    )

        # Generic compact forms for unforeseen attributes, but only for terse noun phrases.
        # Examples: "Alex Example birth place", "ExampleCo GmbH purpose".
        compact_text = re.sub(r"\s+", " ", text.strip(" ?.!"))
        lower_compact = compact_text.lower()
        if compact_text and not self._looks_like_identifier_request(text) and not re.search(
            r"\b(?:what|which|who|when|where|how|does|do|did|is|are|was|were|show|tell|give|can|could|would|please|zeige|gib|finde|wie|was|wer|of|for|von|für|bei|über)\b",
            lower_compact,
        ):
            tokens = compact_text.split(" ")
            if 2 <= len(tokens) <= 9:
                # Form A: <entity> <attribute phrase>
                for attr_len in range(1, min(4, len(tokens) - 1) + 1):
                    attr_raw = " ".join(tokens[-attr_len:]).strip()
                    entity_raw = " ".join(tokens[:-attr_len]).strip()
                    if not entity_raw or not attr_raw:
                        continue
                    if (
                        attr_len == 1
                        and len(tokens) >= 4
                        and "_" not in attr_raw
                        and "-" not in attr_raw
                    ):
                        continue
                    entity_token_count = len(re.findall(r"[A-Za-z0-9äöüÄÖÜß]+", entity_raw))
                    # Prevent pathological splits like "Alex | Example birth place".
                    if entity_token_count < 2:
                        continue
                    entity_name = self._normalize_entity_name_hint(entity_raw)
                    if not entity_name:
                        continue
                    attr_name, _ = self._resolve_schema_term(attr_raw, entity_name)
                    if not attr_name:
                        attr_name = self._normalize_attribute_name(attr_raw)
                    if not attr_name:
                        continue
                    return ParsedQuery(
                        intent="attribute_lookup",
                        entity_name=entity_name,
                        attribute_name=attr_name,
                        confidence=0.88,
                        language=detected_language,
                    )

                # Form B: <attribute phrase> <entity>
                for attr_len in range(1, min(4, len(tokens) - 1) + 1):
                    attr_raw = " ".join(tokens[:attr_len]).strip()
                    entity_raw = " ".join(tokens[attr_len:]).strip()
                    if not entity_raw or not attr_raw:
                        continue
                    if (
                        attr_len == 1
                        and len(tokens) >= 4
                        and "_" not in attr_raw
                        and "-" not in attr_raw
                    ):
                        continue
                    entity_token_count = len(re.findall(r"[A-Za-z0-9äöüÄÖÜß]+", entity_raw))
                    if entity_token_count < 2:
                        continue
                    entity_name = self._normalize_entity_name_hint(entity_raw)
                    if not entity_name:
                        continue
                    attr_name, _ = self._resolve_schema_term(attr_raw, entity_name)
                    if not attr_name:
                        attr_name = self._normalize_attribute_name(attr_raw)
                    if not attr_name:
                        continue
                    return ParsedQuery(
                        intent="attribute_lookup",
                        entity_name=entity_name,
                        attribute_name=attr_name,
                        confidence=0.88,
                        language=detected_language,
                    )

        # High-priority generic gate: process-actor intent (who applied/handled X for entity)
        # Works for any process type: EORI, customs, VAT, trade register, etc.
        process_actor = self._extract_process_actor_intent(text, default_entity_hint)
        if process_actor:
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=process_actor["entity_name"],
                attribute_name=process_actor["requested_attrs"][0],
                confidence=0.87,
                language=detected_language,
                relation_name=process_actor["process_concept"],
                attribute_names=process_actor["requested_attrs"],
                criteria={"parse_method": "process_actor_gate", "process_concept": process_actor["process_concept"]},
            )

        # Generic gate: who is the contact person/email for a named process (no application verb needed)
        process_contact = self._extract_process_contact_intent(text, default_entity_hint)
        if process_contact:
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=process_contact["entity_name"],
                attribute_name=process_contact["requested_attrs"][0],
                confidence=0.85,
                language=detected_language,
                relation_name=process_contact["process_concept"],
                attribute_names=process_contact["requested_attrs"],
                criteria={"parse_method": "process_contact_gate", "process_concept": process_contact["process_concept"]},
            )

        # Prefer LLM intent extraction before semantic pattern matching when available.
        # This reduces pattern-library hijacking on verb-driven event questions.
        if (
            ENABLE_LLM_INTENT_PARSER
            and self.genai_client
            and not self._looks_like_identifier_request(text)
        ):
            llm_parsed = self._llm_extract_intent(question)
            if (
                llm_parsed
                and llm_parsed.intent == "attribute_lookup"
                and llm_parsed.entity_name
                and (llm_parsed.attribute_name or llm_parsed.relation_name)
            ):
                llm_attr = str(llm_parsed.attribute_name or "").strip()
                llm_identifier_attr = bool(self._identifier_anchor_tokens_for_canonical(llm_attr))
                if (
                    self._looks_like_identifier_request(text)
                    and (
                        (not llm_identifier_attr)
                        or (llm_attr and not self._query_has_identifier_anchor_for_attr(text, llm_attr))
                    )
                ):
                    llm_parsed = None
                else:
                    llm_parsed.language = detected_language
                    llm_parsed.confidence = max(float(llm_parsed.confidence or 0.0), 0.8)
                    return llm_parsed

        # Lexical-priority guard: if an explicit attribute phrase is present,
        # prefer it over weaker vector-semantic pattern matches.
        explicit_attr = self._explicit_attribute_from_text(text)
        if explicit_attr and default_entity_hint and not (ENABLE_LLM_INTENT_PARSER and self.genai_client):
            explicit_entity = self._normalize_entity_name_hint(self._extract_entity_name_loose(text)) or default_entity_hint
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=explicit_entity,
                attribute_name=explicit_attr,
                confidence=0.9,
                language=detected_language,
                criteria={"parse_method": "explicit_attribute_lexical"},
            )

        # Stage 0: Pattern library matching (learned and seeded patterns)
        pattern_library = getattr(self, "pattern_library", None)
        if pattern_library:
            try:
                # Try to match against learned/seeded patterns
                entity_hint = default_entity_hint
                if not entity_hint:
                    tail_match = re.search(r"(?:is|ist|of|for|von|für)\s+(.+?)(?:\?|$)", text, flags=re.IGNORECASE)
                    if tail_match:
                        entity_hint = self._normalize_entity_name_hint((tail_match.group(1) or "").strip(" ."))
                entity_class = None  # Could be inferred from entity_hint if needed
                candidate_languages = [detected_language, "en", "de"]
                seen_languages = set()
                pattern_match = None
                matched_language = detected_language
                for lang in candidate_languages:
                    if not lang or lang in seen_languages:
                        continue
                    seen_languages.add(lang)
                    current_match = pattern_library.match_pattern(text, entity_class, lang)
                    if current_match and current_match.confidence >= 0.75:
                        pattern_match = current_match
                        matched_language = lang
                        break

                if not pattern_match:
                    share_semantic_query = self._semantic_frame_shareholding(text, detected_language, entity_hint)
                    if share_semantic_query:
                        if isinstance(share_semantic_query.criteria, dict):
                            share_semantic_query.criteria.setdefault("parse_method", "semantic_frame_registry")
                        return share_semantic_query

                    semantic_pattern = self._semantic_vector_pattern_match(
                        text,
                        language=detected_language,
                        entity_hint=entity_hint,
                    )
                    if semantic_pattern and float(getattr(semantic_pattern, "confidence", 0.0) or 0.0) >= 0.64:
                        pattern_match = semantic_pattern
                        matched_language = str(getattr(semantic_pattern, "pattern_language", detected_language) or detected_language)

                if pattern_match:
                    source_type = str(getattr(pattern_match, "source_type", "") or "")
                    min_conf = 0.64 if source_type == "vector_semantic" else 0.74
                    if float(getattr(pattern_match, "confidence", 0.0) or 0.0) < min_conf:
                        raise ValueError("Pattern confidence below acceptance threshold")
                    if source_type == "vector_semantic":
                        # Generic semantic safety: require at least one meaningful lexical
                        # anchor overlap so unrelated event patterns do not hijack queries.
                        question_tokens = {
                            tok
                            for tok in re.findall(r"[a-z0-9_]+", str(text or "").lower())
                            if len(tok) >= 3
                        }
                        lexical_stop = {
                            "what", "when", "where", "who", "which", "how", "is", "was", "were", "are",
                            "the", "for", "of", "and", "with", "show", "get", "find", "tell", "give", "me",
                            "was", "ist", "war", "sind", "wer", "was", "wann", "wo", "wie", "von", "fur",
                            "fuer", "der", "die", "das", "den", "dem", "des",
                        }
                        pattern_tokens = [
                            tok
                            for tok in re.findall(r"[a-z0-9_]+", str(getattr(pattern_match, "pattern_text", "") or "").lower())
                            if len(tok) >= 3 and tok not in lexical_stop
                        ]
                        concept_tokens = [
                            tok
                            for tok in re.findall(r"[a-z0-9_]+", str(getattr(pattern_match, "semantic_concept", "") or "").lower())
                            if len(tok) >= 3 and tok not in lexical_stop
                        ]
                        anchors = set(pattern_tokens + concept_tokens)
                        if anchors and not (anchors & question_tokens):
                            raise ValueError("Vector semantic pattern failed lexical compatibility check")
                    semantic_concept = str(getattr(pattern_match, "semantic_concept", "") or "").strip().lower()
                    if semantic_concept in {"share_count", "shareholders", "shareholders_person"}:
                        share_semantic_query = self._semantic_frame_shareholding(text, detected_language, entity_hint)
                        if share_semantic_query:
                            if isinstance(share_semantic_query.criteria, dict):
                                share_semantic_query.criteria.setdefault("parse_method", "semantic_frame_registry")
                            return share_semantic_query
                    parsed_from_pattern = self._pattern_match_to_parsed_query(
                        pattern_match=pattern_match,
                        text=text,
                        entity_hint=entity_hint,
                        matched_language=matched_language,
                    )
                    parsed_attr = str(parsed_from_pattern.attribute_name or "").strip()
                    if (
                        source_type == "vector_semantic"
                        and parsed_attr
                        and self._looks_like_identifier_request(text)
                        and self._identifier_anchor_tokens_for_canonical(parsed_attr)
                        and not self._query_has_identifier_anchor_for_attr(text, parsed_attr)
                    ):
                        raise ValueError("Vector semantic pattern failed identifier anchor compatibility")
                    return parsed_from_pattern
            except Exception as e:
                import logging
                logging.debug(f"Pattern library matching failed: {e}")

        if not self._looks_like_identifier_request(text):
            event_query = self._extract_interrogative_event_query(text, detected_language)
            if event_query:
                return event_query

        # High-priority intent gate: file name + full path requests should resolve
        # through document_file_info lookup (which prefers original_local_path).
        asks_file_name = bool(
            re.search(
                r"\b(?:file\s*name|filename|document\s*name|dateiname|dokumentname)\b",
                text,
                flags=re.IGNORECASE,
            )
        )
        asks_full_path = bool(
            re.search(
                r"\b(?:full\s*path|file\s*path|document\s*path|vollst[aä]ndig(?:e|en|em)?\s+pfad|vollstaendig(?:e|en|em)?\s+pfad|pfad)\b",
                text,
                flags=re.IGNORECASE,
            )
        )
        if asks_file_name and asks_full_path:
            file_info_entity = self._extract_entity_for_file_info_intent(text)
            entity_name = file_info_entity or self._best_entity_hint(text) or self._extract_entity_name_loose(text)
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=entity_name or None,
                attribute_name="document_file_info",
                confidence=0.91,
                language=detected_language,
                criteria={
                    "parse_method": "file_info_intent_gate",
                },
            )

        # High-priority pattern: filename + full path request (German variants)
        file_info_patterns = [
            r"(?:zeige|zeigen|gib\s+mir|wie\s+lautet|was\s+ist)\s+.*?(?:dateiname|dokumentname).*?(?:vollständige(?:m|n)?\s+pfad|vollstaendige(?:m|n)?\s+pfad|pfad).*?(?:für|von)\s+(?:die\s+registrierung\s+der\s+)?([a-zA-Z0-9 .&äöüÄÖÜß_\-/]+?)(?:\s+an)?(?:\?|$)",
            r"(?:dateiname|dokumentname)\s+.*?(?:mit\s+)?(?:vollständige(?:m|n)?\s+pfad|vollstaendige(?:m|n)?\s+pfad).*?(?:für|von)\s+(?:die\s+registrierung\s+der\s+)?([a-zA-Z0-9 .&äöüÄÖÜß_\-/]+?)(?:\s+an)?(?:\?|$)",
        ]
        for pattern in file_info_patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                entity_name = self._normalize_entity_name_hint((match.group(1) or "").strip(" ."))
                return ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=entity_name or None,
                    attribute_name="document_file_info",
                    confidence=0.9,
                    language=detected_language,
                )

        share_amount_patterns = [
            r"(?:how\s+many\s+shares?\s+of\s+)([a-zA-Z0-9 .&_\-/]+?)\s+(?:does|do)\s+([a-zA-Z0-9 .&_\-/]+?)\s+(?:hold|owns?|have)(?:\?|$)",
            r"(?:how\s+many\s+shares?\s+(?:does|do)\s+)([a-zA-Z0-9 .&_\-/]+?)\s+(?:hold|owns?|have)\s+(?:in|of)\s+([a-zA-Z0-9 .&_\-/]+?)(?:\?|$)",
        ]
        for idx, pattern in enumerate(share_amount_patterns):
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue

            if idx == 0:
                company_name = self._normalize_entity_name_hint((match.group(1) or "").strip(" ."))
                person_name = self._normalize_entity_name_hint((match.group(2) or "").strip(" ."))
            else:
                person_name = self._normalize_entity_name_hint((match.group(1) or "").strip(" ."))
                company_name = self._normalize_entity_name_hint((match.group(2) or "").strip(" ."))

            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=person_name or None,
                attribute_name="shareholders",
                confidence=0.93,
                language=detected_language,
                criteria={
                    "target_company": company_name,
                    "question_type": "share_count",
                },
            )

        shareholder_patterns = [
            # English patterns
            r"(?:who\s+are|who\s+is)\s+(?:the\s+)?share\s*holders\s+of\s+([a-zA-Z0-9 .&_\-/]+?)(?:\?|$)",
            r"(?:who\s+are|who\s+is)\s+(?:the\s+)?shareholders\s+of\s+([a-zA-Z0-9 .&_\-/]+?)(?:\?|$)",
            r"(?:list|show|get|find)\s+(?:the\s+)?share\s*holders\s+of\s+([a-zA-Z0-9 .&_\-/]+?)(?:\?|$)",
            r"(?:list|show|get|find)\s+(?:the\s+)?shareholders\s+of\s+([a-zA-Z0-9 .&_\-/]+?)(?:\?|$)",
            # Person-centric English patterns: "What companies does X hold shares (in)?"
            r"(?:what\s+companies\s+does\s+)([a-zA-Z0-9 .&_\-/]+?)\s+(?:hold|owns?|have)\s+shares?(?:\s+in)?(?:\?|$)",
            r"(?:which\s+companies\s+does\s+)([a-zA-Z0-9 .&_\-/]+?)\s+(?:hold|owns?|have)\s+shares?(?:\s+in)?(?:\?|$)",
            r"(?:in\s+which\s+companies\s+does\s+)([a-zA-Z0-9 .&_\-/]+?)\s+(?:hold|owns?|have)\s+shares?(?:\?|$)",
            r"(?:where\s+does\s+)([a-zA-Z0-9 .&_\-/]+?)\s+(?:hold|owns?|have)\s+shares?(?:\?|$)",
            # German patterns: flexible matching for Aktionäre, Gesellschafter, with Anteile/Beträge
            r"(?:zeigen?|gib|suche?|finde?\s+(?:mir|alle)?)\s+.+?(?:aktionäre|gesellschafter|anteils?eigner)\s+(?:von|der|des)\s+([a-zA-Z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|\.?\s*$)",
            r"(?:liste|zeige?|zeigen|gib|suche?)\s+(?:alle\s+|mir\s+)?(?:aktionäre|gesellschafter)\s+(?:von|der|des)\s+([a-zA-Z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|\.?\s*$)",
            r"wer\s+(?:sind|ist)\s+(?:die\s+)?(?:aktionäre|gesellschafter)\s+(?:von|der|des)\s+([a-zA-Z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|\.?\s*$)",
        ]

        for pattern in shareholder_patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                entity_name = (match.group(1) or "").strip(" .")
                return ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=entity_name or None,
                    attribute_name="shareholders",
                    confidence=0.9,
                    language=detected_language,
                )

        # Generic booster: process-actor intent (who applied/handled X for entity)
        process_actor = self._extract_process_actor_intent(text, default_entity_hint)
        if process_actor:
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=process_actor["entity_name"],
                attribute_name=process_actor["requested_attrs"][0],
                confidence=0.87,
                language=detected_language,
                relation_name=process_actor["process_concept"],
                attribute_names=process_actor["requested_attrs"],
                criteria={"parse_method": "process_actor_gate", "process_concept": process_actor["process_concept"]},
            )

        # Generic booster: contact person/email for any named process
        process_contact = self._extract_process_contact_intent(text, default_entity_hint)
        if process_contact:
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=process_contact["entity_name"],
                attribute_name=process_contact["requested_attrs"][0],
                confidence=0.85,
                language=detected_language,
                relation_name=process_contact["process_concept"],
                attribute_names=process_contact["requested_attrs"],
                criteria={"parse_method": "process_contact_gate", "process_concept": process_contact["process_concept"]},
            )

        patterns: list[tuple[str, str]] = [
            (r"(?:what is|what's|show|get|find)\s+(?:the\s+)?([a-zA-Z0-9_\- ]+?)\s+(?:of|for)\s+([a-zA-Z0-9 .&_\-/]+?)(?:\?|$)", "attribute_lookup"),
            (r"(?:give me|tell me)\s+(?:the\s+)?([a-zA-Z0-9_\- ]+?)\s+(?:of|for)\s+([a-zA-Z0-9 .&_\-/]+?)(?:\?|$)", "attribute_lookup"),
            (r"(?:when is|when was)\s+([a-zA-Z0-9_\- ]+?)\s+(?:for|of)\s+([a-zA-Z0-9 .&_\-/]+?)(?:\?|$)", "attribute_lookup"),
            # German: attribute ... von/für <entity>
            (r"(?:was\s+ist|wie\s+lautet|zeige|gib\s+mir|finde)\s+(?:der|die|das)?\s*([a-zA-Z0-9äöüÄÖÜß_\-. ]+?)(?:[.:])?\s*(?:von|für)\s+([a-zA-Z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|$)", "attribute_lookup"),
            # German: attribute ... der/des <entity> (e.g. 'Wie lautet die Adresse der ExampleCo GmbH?')
            (r"(?:was\s+ist|wie\s+lautet|zeige|gib\s+mir|finde)\s+(?:der|die|das)?\s*([a-zA-Z0-9äöüÄÖÜß_\-. ]+?)(?:[.:])?\s*(?:der|des)\s+([a-zA-Z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|$)", "attribute_lookup"),
            # French: attribut ... de/du/des <entité>
            (r"(?:quel(?:le)?\s+est|donne(?:z)?|montre(?:z)?|trouve(?:z)?)\s+(?:le|la|les|l')?\s*([a-zA-Z0-9àâæéèêëïîôùûüœç_\-. ']+?)(?:[.:])?\s*(?:de|du|des)\s+([a-zA-Z0-9 .&àâæéèêëïîôùûüœç_\-/']+?)(?:\?|$)", "attribute_lookup"),
            # Spanish: atributo ... de/del <entidad>
            (r"(?:cual\s+es|cu[aá]l\s+es|que\s+es|qu[eé]\s+es|dame|muestra|encuentra)\s+(?:el|la|los|las)?\s*([a-zA-Z0-9áéíóúñü_\-. ]+?)(?:[.:])?\s*(?:de|del)\s+([a-zA-Z0-9 .&áéíóúñü_\-/]+?)(?:\?|$)", "attribute_lookup"),
        ]

        for pattern, intent in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            attribute_name, schema_kind = self._resolve_schema_term(match.group(1), self._normalize_entity_name_hint((match.group(2) or "").strip(" .")))
            entity_name = self._normalize_entity_name_hint((match.group(2) or "").strip(" ."))
            if schema_kind == "relationship" and attribute_name:
                return ParsedQuery(
                    intent=intent,
                    entity_name=entity_name or None,
                    attribute_name=attribute_name,
                    confidence=0.9,
                    language=detected_language,
                )
            return ParsedQuery(
                intent=intent,
                entity_name=entity_name or None,
                attribute_name=attribute_name,
                confidence=0.85,
                language=detected_language,
            )

        if "metadata" in lowered or "attributes" in lowered or "identifier" in lowered:
            return ParsedQuery(
                intent="attribute_lookup",
                entity_name=default_entity_hint,
                attribute_name=None,
                confidence=0.5,
                language=detected_language,
            )

        return ParsedQuery(
            intent="semantic_lookup",
            entity_name=default_entity_hint,
            attribute_name=None,
            confidence=0.4,
            language=detected_language,
        )

    def _resolve_entity_mention_from_db(self, question: str) -> str | None:
        """Resolve entity mention by matching known object names directly in the question text."""
        q = re.sub(r"\s+", " ", str(question or "").strip().lower())
        if not q or len(q) < 3:
            return None

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT object_name
                        FROM object_instance
                        WHERE COALESCE(status, 'active') = 'active'
                          AND valid_from <= CURRENT_DATE
                          AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                          AND LENGTH(TRIM(object_name)) >= 3
                          AND POSITION(LOWER(object_name) IN %s) > 0
                        ORDER BY LENGTH(object_name) DESC, object_name ASC
                        LIMIT 10;
                        """,
                        (q,),
                    )
                    rows = [str(r[0]).strip() for r in cur.fetchall() if str(r[0] or "").strip()]
            if not rows:
                return None

            # Prefer longest exact mention found in question; normalize afterward.
            return self._normalize_entity_name_hint(rows[0])
        except Exception:
            return None

    def _extract_entity_via_llm(self, question: str) -> str | None:
        """Final fallback: LLM-based entity extraction constrained to query text only (no hallucination).
        Asks LLM to identify person/organization names already mentioned in the question.
        Only called when regex + DB mention resolution both fail.
        """
        if not self.genai_client:
            return None

        prompt = (
            "Extract person or organization names that are EXPLICITLY MENTIONED in this question text. "
            "Return ONLY names that actually appear in the question—do NOT infer, guess, or hallucinate names. "
            "If no clear person or organization name is found, return null.\n"
            "Return ONLY valid JSON with no extra text:\n"
            '{"entity_name": <person or organization name found in question text, or null>}\n'
            f"Question: {json.dumps(question)}"
        )
        try:
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: self.genai_client.models.generate_content(
                    model=EXTRACT_MODEL,
                    contents=prompt,
                ),
                model=EXTRACT_MODEL,
                contents=prompt,
                temperature=0.0,
                call_name="entity_extract_llm",
                complexity="simple",
            )
            text = (response.text or "").strip()
            # Strip markdown code fences if present
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
            text = re.sub(r"\s*```\s*$", "", text, flags=re.MULTILINE)
            data = json.loads(text)
            entity = (data.get("entity_name") or "").strip() or None
            if entity:
                return self._normalize_entity_name_hint(entity)
            return None
        except Exception:
            return None

    def _extract_entity_via_spacy(self, question: str) -> str | None:
        """Extract a person/org entity from question text using spaCy NER."""
        nlp = self._get_spacy_nlp()
        if nlp is None:
            return None

        text = str(question or "").strip()
        if not text:
            return None

        try:
            doc = nlp(text)
        except Exception:
            return None

        preferred_labels = {"PERSON", "ORG", "PER"}
        candidates: list[tuple[int, str]] = []

        for ent in getattr(doc, "ents", []):
            label = str(getattr(ent, "label_", "") or "").upper()
            if label not in preferred_labels:
                continue
            value = self._normalize_entity_name_hint(getattr(ent, "text", ""))
            if not value:
                continue
            token_count = len(re.findall(r"[A-Za-z0-9äöüÄÖÜß]+", value))
            candidates.append((token_count, value))

        if not candidates:
            return None

        # Prefer the most specific mention (more tokens), then longer text.
        candidates.sort(key=lambda item: (-item[0], -len(item[1]), item[1]))
        return candidates[0][1]

    def _is_suspicious_entity_hint(self, hint: str | None) -> bool:
        """Detect over-captured entity hints that include question clause text."""
        text = str(hint or "").strip().lower()
        if not text:
            return True

        if re.fullmatch(
            r"(?:most\s+recent|current|latest|recent|last|previous|prior|next|new|newest|previously|letzt(?:e|er|en|es)?|aktuell(?:e|er|en|es)?|neu(?:e|er|en|es)?)",
            text,
        ):
            return True

        if re.search(r"^(?:of|for|von|für)\s+", text):
            return True

        if re.search(
            r"^(?:what\s+is|what's|which|who|where|how|can\s+you|could\s+you|would\s+you|wie\s+lautet|was\s+ist|wer\s+ist|wo\s+wohnt|zeige|gib\s+mir|tell\s+me|give\s+me)\b",
            text,
        ):
            return True

        if re.search(r"\b(?:does|do|did|hold|holds|have|has|is|are|was|were)\b", text):
            return True

        if re.search(r"\b(?:please|pls|bitte|show|tell|give|can|you|wo|wohnt|lebt)\b", text):
            return True

        tokens = re.findall(r"[a-z0-9äöüß]+", text)
        return len(tokens) > 6

    def _best_entity_hint(self, question: str) -> str | None:
        """Best-effort entity extraction: DB mention → spaCy → regex → LLM."""
        explicit_legal_entity = self._extract_legal_entity_mention(question)

        hint = self._resolve_entity_mention_from_db(question)
        if hint and not self._is_suspicious_entity_hint(hint):
            if explicit_legal_entity:
                normalized_hint = self._normalize_entity_name_hint(hint) or ""
                normalized_explicit = self._normalize_entity_name_hint(explicit_legal_entity) or ""
                if normalized_hint and normalized_explicit and normalized_hint.casefold() == normalized_explicit.casefold():
                    return explicit_legal_entity
            return hint

        hint = self._extract_entity_via_spacy(question)
        if hint and not self._is_suspicious_entity_hint(hint):
            return hint

        hint = self._extract_entity_name_loose(question)
        if hint and not self._is_suspicious_entity_hint(hint):
            return hint

        # Last non-LLM fallback: keep regex hint only if it's all we have.
        if hint:
            return hint

        # Final fallback: LLM with strict constraint to extract only from query text (no hallucination)
        return self._extract_entity_via_llm(question)

    def _extract_entity_name_loose(self, question: str) -> str | None:
        patterns = [
            r"\bof\s+([A-Za-z0-9 .&_\-/]+?)(?:\?|$)",
            r"\bfor\s+([A-Za-z0-9 .&_\-/]+?)(?:\?|$)",
            r"\bat\s+([A-Za-z0-9 .&_\-/]+?)(?:\?|$)",
            r"\babout\s+([A-Za-z0-9 .&_\-/]+?)(?:\?|$)",
            r"\bvon\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|$)",
            r"\bfür\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|$)",
            r"\bbei\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|$)",
            r"\büber\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|$)",
            # German education/qualification shape: "welches diplom hat Alex Example?"
            r"\b(?:welche[rsn]?|was)\s+[A-Za-z0-9 .&äöüÄÖÜß_\-/]+\s+hat\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|$)",
            # English education/qualification shape: "which degree does Alex Example have?"
            r"\b(?:what|which)\s+[A-Za-z0-9 .&_\-/]+\s+does\s+([A-Za-z0-9 .&_\-/]+?)\s+have(?:\?|$)",
            # English person-centric shareholding shape: "what companies does Alex Example hold shares"
            r"\b(?:what|which)\s+companies\s+does\s+([A-Za-z0-9 .&_\-/]+?)\s+(?:hold|owns?|have)\s+shares?(?:\s+in)?(?:\?|$)",
            # Title + name patterns: "Dr. Example", "Mr. Alex Example", "Mrs. Schmidt"
            r"\b(?:Dr|Mr|Mrs|Ms|Prof|Sir|Frau|Herr|Herr Dr|Dr\.\s?(?:rer|phil|med|ing|jur))\.?\s+([A-Za-z0-9 .äöüÄÖÜß_\-/]+?)(?:\?|$)",
            # Standalone initials + surname shape: "C.Y. Pang", "J. R. Tolkien"
            r"^\s*((?:[A-Z]\.\s*){1,4}[A-Za-z0-9 .äöüÄÖÜß_\-/]+?)\s*(?:\?|$)",
        ]
        for pattern in patterns:
            m = re.search(pattern, question, flags=re.IGNORECASE)
            if m:
                candidate = m.group(1).strip(" .")
                if candidate:
                    return self._normalize_entity_name_hint(candidate)

        # Standalone name phrase fallback (e.g., "Alex Example").
        standalone = str(question or "").strip().strip("?").strip()
        if standalone and re.fullmatch(r"[A-Za-zÄÖÜäöüß][A-Za-z0-9ÄÖÜäöüß.,'\- ]{2,120}", standalone):
            words = re.findall(r"[A-Za-zÄÖÜäöüß]{2,}", standalone)
            stop_tokens = {
                "what", "which", "who", "when", "where", "why", "how", "is", "are", "was", "were",
                "does", "do", "did", "the", "a", "an", "of", "for", "about", "von", "fur", "bei", "uber",
                "can", "you", "please", "tell", "show", "give", "me", "wo", "wohnt", "lebt", "ist",
            }
            lowered_words = {w.lower() for w in re.findall(r"[A-Za-zÄÖÜäöüß]+", standalone)}
            if 1 < len(words) <= 6 and not (lowered_words & stop_tokens):
                candidate = self._normalize_entity_name_hint(standalone)
                if candidate:
                    return candidate

        # Generic legal-entity suffix fallback when no clear preposition match exists.
        suffix_match = re.search(
            r"([A-Za-z0-9äöüÄÖÜß .&_-]{2,}\s+(?:GmbH|AG|SA|Sarl|Ltd\.?|Inc\.?|LLC))\b",
            question,
            flags=re.IGNORECASE,
        )
        if suffix_match:
            candidate = (suffix_match.group(1) or "").strip(" .")
            if candidate:
                return self._normalize_entity_name_hint(candidate)

        return None

    def _extract_legal_entity_mention(self, text: str | None) -> str | None:
        raw_text = str(text or "").strip()
        if not raw_text:
            return None

        pattern = re.compile(
            r"((?:[A-ZÄÖÜ0-9][A-Za-z0-9äöüÄÖÜß.&_-]*\s+){1,6}(?:GmbH|AG|SA|Sarl|Ltd\.?|Inc\.?|LLC))\b"
        )
        matches = [m.group(1).strip(" .") for m in pattern.finditer(raw_text) if str(m.group(1) or "").strip()]
        if not matches:
            return None
        return self._normalize_entity_name_hint(matches[-1])

    def _normalize_entity_name_hint(self, raw: str | None) -> str | None:
        if not raw:
            return None
        text = str(raw).strip(" .")
        
        # Remove title prefixes (Dr., Mr., Mrs., Ms., Prof., etc.)
        # Handles both short (Dr.) and long forms (Dr. phil, Dr. med, Dr. rer)
        text = re.sub(
            r"^(?:Dr\.?\s+(?:phil|med|ing|jur)?|Mr\.?|Mrs\.?|Ms\.?|Prof\.?|Sir\s+|Frau|Herr\s+(?:Dr\.?)?)\.?\s+",
            "",
            text,
            flags=re.IGNORECASE
        ).strip()
        
        # Remove leading determiners frequently captured by German/English patterns.
        text = re.sub(r"^(?:der|die|das|den|dem|des|the)\s+", "", text, flags=re.IGNORECASE)
        # Remove leading prepositions and question prefixes accidentally captured as part of entity spans.
        text = re.sub(r"^(?:of|for|von|für)\s+", "", text, flags=re.IGNORECASE)
        text = re.sub(
            r"^(?:what\s+is|what's|wie\s+lautet|was\s+ist|zeige|gib\s+mir|tell\s+me|give\s+me|can\s+you\s+give\s+me)\s+",
            "",
            text,
            flags=re.IGNORECASE,
        )

        # Drop leading temporal modifiers so list/time phrases stay generic.
        text = re.sub(
            r"^(?:(?:the|der|die|das|den|dem|des)\s+)?(?:most\s+recent|current|latest|recent|last|previous|prior|next|new|newest|previously|letzt(?:e|er|en|es)?|aktuell(?:e|er|en|es)?|neu(?:e|er|en|es)?)\s+",
            "",
            text,
            flags=re.IGNORECASE,
        )
        
        # Remove trailing explanatory tails that belong to question intent, not entity name.
        text = re.sub(
            r"\s+(?:and|und)\s+(?:his|her|their|its|sein(?:e|en|em|er)?|ihr(?:e|en|em|er)?)\s+"
            r"(?:qualification(?:s)?|qualifikation(?:en)?|degree(?:s)?|abschluss(?:e)?|diplom(?:e)?|certificate(?:s)?|zertifikat(?:e)?)$",
            "",
            text,
            flags=re.IGNORECASE,
        )
        # Remove common document-context suffixes users append to company names.
        text = re.sub(r"[-\s]+(?:registrierung|register|registration)$", "", text, flags=re.IGNORECASE)
        # Remove conversational trailing particles occasionally captured by regex.
        text = re.sub(r"\s+(?:an|bitte|please|pls|erreiche|erreichen|erreicht)$", "", text, flags=re.IGNORECASE)
        text = text.strip(" .-")
        return text or None

    def _parse_date_str(self, raw: str) -> str | None:
        """Parse common date string formats to ISO YYYY-MM-DD. Returns None if unparseable."""
        import datetime as _dt
        raw = str(raw or "").strip()
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return _dt.datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
        return None

    def _parse_month_name(self, raw: str) -> int | None:
        token = str(raw or "").strip().lower()
        token = token.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
        token = re.sub(r"[^a-z]", "", token)
        if not token:
            return None
        return self._MONTH_NAME_TO_NUM.get(token)

    def _extract_time_window(self, text: str) -> dict[str, Any] | None:
        """Extract a date-range constraint from natural language text.

        Supports:
        - "last/past N days/weeks/months/years" (and German equivalents)
        - "between YYYY-MM-DD and YYYY-MM-DD"
        - "after/since DATE" and "before/until DATE"

        Returns a dict with keys date_from, date_to (ISO strings or None), description.
        """
        lowered = str(text or "").strip().lower()
        today = date.today()

        # Relative: "last N months" / "letzten 3 Monate"
        relative = re.search(
            r"(?:last|past|letzten?|vergangenen?)\s+([a-zA-ZäöüÄÖÜß0-9]+)\s+(days?|weeks?|months?|years?|tage?|wochen?|monate?|jahre?)",
            lowered,
        )
        if relative:
            raw_count = relative.group(1).strip()
            unit = relative.group(2).strip().lower()
            count: int | None = self._WORD_TO_INT.get(raw_count)
            if count is None:
                try:
                    count = int(raw_count)
                except ValueError:
                    count = None
            if count is not None:
                unit_days = self._UNIT_DAYS.get(unit, 30)
                date_from = today - timedelta(days=count * unit_days)
                return {
                    "date_from": date_from.isoformat(),
                    "date_to": today.isoformat(),
                    "description": f"last {count} {unit}",
                }

        # Absolute range: "between 2026-01-01 and 2026-03-31"
        abs_range = re.search(
            r"between\s+(\d{4}-\d{2}-\d{2}|\d{1,2}[./]\d{1,2}[./]\d{4})\s+and\s+(\d{4}-\d{2}-\d{2}|\d{1,2}[./]\d{1,2}[./]\d{4})",
            lowered,
        )
        if abs_range:
            d_from = self._parse_date_str(abs_range.group(1))
            d_to = self._parse_date_str(abs_range.group(2))
            if d_from and d_to:
                return {"date_from": d_from, "date_to": d_to, "description": f"between {d_from} and {d_to}"}

        month_range = re.search(
            r"(?:from|between|vom?|zwischen)\s+([a-zA-ZäöüÄÖÜß]+)(?:\s+(\d{4}))?\s+(?:to|and|bis|und)\s+([a-zA-ZäöüÄÖÜß]+)(?:\s+(\d{4}))?",
            lowered,
            flags=re.IGNORECASE,
        )
        if month_range:
            m1 = self._parse_month_name(month_range.group(1))
            m2 = self._parse_month_name(month_range.group(3))
            y1_raw = month_range.group(2)
            y2_raw = month_range.group(4)
            if m1 and m2:
                y1 = int(y1_raw) if y1_raw else (int(y2_raw) if y2_raw else today.year)
                y2 = int(y2_raw) if y2_raw else y1
                if (y2, m2) < (y1, m1):
                    y2 = y1 + 1
                d_from = date(y1, m1, 1)
                if m2 == 12:
                    d_to = date(y2 + 1, 1, 1) - timedelta(days=1)
                else:
                    d_to = date(y2, m2 + 1, 1) - timedelta(days=1)
                return {
                    "date_from": d_from.isoformat(),
                    "date_to": d_to.isoformat(),
                    "description": f"from {month_range.group(1)} to {month_range.group(3)}",
                }

        # Lower-bound: "after 2026-01-01" / "since 2026-01-01" / "ab 2026-01-01"
        after = re.search(
            r"(?:after|since|ab)\s+(\d{4}-\d{2}-\d{2}|\d{1,2}[./]\d{1,2}[./]\d{4})",
            lowered,
        )
        if after:
            d_from = self._parse_date_str(after.group(1))
            if d_from:
                return {"date_from": d_from, "date_to": None, "description": f"after {d_from}"}

        # Upper-bound: "before 2026-03-31" / "until 2026-03-31" / "bis 2026-03-31"
        before = re.search(
            r"(?:before|until|bis)\s+(\d{4}-\d{2}-\d{2}|\d{1,2}[./]\d{1,2}[./]\d{4})",
            lowered,
        )
        if before:
            d_to = self._parse_date_str(before.group(1))
            if d_to:
                return {"date_from": None, "date_to": d_to, "description": f"before {d_to}"}

        return None

    def _looks_like_travel_expense_total_question(self, question: str) -> bool:
        text = str(question or "").strip().lower()
        asks_total = bool(re.search(r"\b(total|sum|gesamt|summe|how\s+much)\b", text))
        mentions_expense = bool(re.search(r"\b(expense|expenses|ausgaben|kosten)\b", text))
        mentions_travel = bool(re.search(r"\b(travel|travelling|travelling\s+expenses|reisen|reise)\b", text))
        return asks_total and mentions_expense and mentions_travel

    def _extract_company_hint_for_expense_question(self, question: str) -> str:
        text = str(question or "").strip()
        m = re.search(
            r"(?:expenses?|ausgaben|kosten)\s+(?:of|for|von|fuer|für)\s+([A-Za-z0-9äöüÄÖÜß .&_\-/]+?)(?=\s+(?:from|between|to|in|during|ab|vom|bis|zwischen)\b|[\?\.!]|$)",
            text,
            flags=re.IGNORECASE,
        )
        if not m:
            return ""
        return self._normalize_entity_name_hint(str(m.group(1) or "").strip(" .")) or ""

    def _try_accounting_travel_expense_total_answer(self, question: str, parsed: ParsedQuery) -> dict[str, Any] | None:
        if not self._looks_like_travel_expense_total_question(question):
            return None

        lowered_question = str(question or "").strip().lower()
        include_vat = bool(
            re.search(
                r"\b(?:including|include|incl\.?|inkl\.?|inklusive|mit)\s+(?:vat|mwst|tax)\b|\b(?:vat|mwst|tax)\s+(?:included|inklusive|included)\b",
                lowered_question,
                flags=re.IGNORECASE,
            )
        )

        criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        tw = criteria.get("time_window") if isinstance(criteria.get("time_window"), dict) else (self._extract_time_window(question) or {})
        date_from = str(tw.get("date_from") or "").strip() or None
        date_to = str(tw.get("date_to") or "").strip() or None
        company_hint = self._extract_company_hint_for_expense_question(question)

        if not date_from and not date_to and not company_hint:
            return None

        def _unwrap(value: Any) -> Any:
            if isinstance(value, dict) and "value" in value:
                return value.get("value")
            return value

        def _to_amount(value: Any) -> float:
            raw = _unwrap(value)
            if raw is None:
                return 0.0
            if isinstance(raw, (int, float)):
                return float(raw)
            text = str(raw).strip()
            if not text:
                return 0.0
            text = text.replace("'", "").replace(" ", "")
            text = text.replace(",", ".") if text.count(",") and not text.count(".") else text.replace(",", "")
            text = re.sub(r"[^0-9.\-]", "", text)
            try:
                return float(text)
            except Exception:
                return 0.0

        def _contains_company(attrs: dict[str, Any], needle: str) -> bool:
            if not needle:
                return True
            probe = needle.lower()
            for key in ("legal_entity_ref", "company_ref", "employer_ref", "current_employer_ref"):
                candidate = str(_unwrap(attrs.get(key)) or "").strip().lower()
                if candidate and (probe in candidate or candidate in probe):
                    return True
            return False

        total = 0.0
        contributing: list[dict[str, Any]] = []
        invoice_totals_cache: dict[int, tuple[float, float]] = {}

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            oi.object_id,
                            oi.object_name,
                            d.doc_id,
                            d.doc_name,
                            d.doc_date::text
                        FROM object_instance oi
                        LEFT JOIN part p ON p.object_id = oi.object_id
                        LEFT JOIN document d ON d.doc_id = p.doc_id
                        WHERE LOWER(COALESCE(oi.class_name, '')) = 'accounting_transaction'
                          AND (%s IS NULL OR (d.doc_date IS NOT NULL AND d.doc_date >= %s::date))
                          AND (%s IS NULL OR (d.doc_date IS NOT NULL AND d.doc_date <= %s::date))
                        ORDER BY oi.object_id DESC
                        LIMIT 2000
                        """,
                        (date_from, date_from, date_to, date_to),
                    )
                    rows = cur.fetchall()

                seen_object_ids: set[int] = set()
                for row in rows:
                    object_id = int(row[0]) if row and row[0] is not None else 0
                    if object_id <= 0:
                        continue
                    if object_id in seen_object_ids:
                        continue
                    seen_object_ids.add(object_id)
                    attrs = object_db.get_current_temporal_attributes(conn, object_id, "object")
                    if not isinstance(attrs, dict):
                        continue
                    if not _contains_company(attrs, company_hint):
                        continue

                    booking_name = str(_unwrap(attrs.get("booking_debit_account_name")) or "").strip().lower()
                    lines_raw = _unwrap(attrs.get("ledger_lines"))
                    lines: list[dict[str, Any]] = []
                    if isinstance(lines_raw, dict):
                        lines = [lines_raw]
                    elif isinstance(lines_raw, list):
                        lines = [item for item in lines_raw if isinstance(item, dict)]

                    object_sum_excl_vat = 0.0
                    vat_component = 0.0
                    for line in lines:
                        direction = str(_unwrap(line.get("direction")) or "").strip().lower()
                        account_number = int(_to_amount(line.get("account_number")) or 0)
                        line_desc = str(_unwrap(line.get("line_description")) or "").strip().lower()
                        swiss_vat_code = str(_unwrap(line.get("swiss_vat_code")) or "").strip().lower()
                        travel_like = (
                            account_number in {6400, 6500}
                            or "travel" in line_desc
                            or "travelling" in line_desc
                            or "reise" in line_desc
                        )
                        if direction == "debit" and travel_like:
                            amount = _to_amount(line.get("amount_chf") or line.get("amount_source_currency"))
                            if amount > 0:
                                object_sum_excl_vat += amount
                        if direction == "debit" and (
                            account_number == 1060
                            or swiss_vat_code.startswith("ipb")
                            or "vat" in line_desc
                            or "mwst" in line_desc
                            or "tax" in line_desc
                        ):
                            amount = _to_amount(line.get("amount_chf") or line.get("amount_source_currency"))
                            if amount > 0:
                                vat_component += amount

                    travel_by_booking = ("travel" in booking_name or "travelling" in booking_name or "reise" in booking_name)
                    if object_sum_excl_vat <= 0 and travel_by_booking:
                        object_sum_excl_vat = _to_amount(attrs.get("net_amount") or attrs.get("total_amount") or attrs.get("gross_amount"))

                    object_sum = object_sum_excl_vat
                    if include_vat:
                        gross_amount = _to_amount(attrs.get("gross_amount") or attrs.get("total_amount"))
                        if gross_amount > 0 and (object_sum_excl_vat > 0 or travel_by_booking):
                            object_sum = gross_amount
                        else:
                            if vat_component <= 0:
                                vat_component = _to_amount(attrs.get("tax_amount") or attrs.get("vat_amount"))
                            object_sum = object_sum_excl_vat + max(0.0, vat_component)

                        if object_sum <= object_sum_excl_vat and int(row[2] or 0) > 0:
                            doc_id = int(row[2])
                            cached = invoice_totals_cache.get(doc_id)
                            if cached is None:
                                with conn.cursor() as invoice_cur:
                                    invoice_cur.execute(
                                        """
                                        SELECT oi.object_id
                                        FROM part p
                                        JOIN object_instance oi ON oi.object_id = p.object_id
                                        WHERE p.doc_id = %s
                                          AND LOWER(COALESCE(oi.class_name, '')) IN ('invoice', 'bill', 'receipt')
                                        ORDER BY oi.object_id DESC
                                        LIMIT 1
                                        """,
                                        (doc_id,),
                                    )
                                    invoice_row = invoice_cur.fetchone()
                                invoice_gross = 0.0
                                invoice_tax = 0.0
                                if invoice_row and int(invoice_row[0] or 0) > 0:
                                    invoice_attrs = object_db.get_current_temporal_attributes(conn, int(invoice_row[0]), "object")
                                    if isinstance(invoice_attrs, dict):
                                        invoice_gross = _to_amount(
                                            invoice_attrs.get("gross_amount")
                                            or invoice_attrs.get("total_amount")
                                            or invoice_attrs.get("total_gross_amount")
                                        )
                                        invoice_tax = _to_amount(invoice_attrs.get("tax_amount") or invoice_attrs.get("vat_amount"))
                                cached = (invoice_gross, invoice_tax)
                                invoice_totals_cache[doc_id] = cached

                            invoice_gross, invoice_tax = cached
                            if invoice_gross > 0:
                                object_sum = invoice_gross
                            elif invoice_tax > 0 and object_sum_excl_vat > 0:
                                object_sum = object_sum_excl_vat + invoice_tax
                            if invoice_tax > vat_component:
                                vat_component = invoice_tax

                    if object_sum > 0:
                        total += object_sum
                        contributing.append(
                            {
                                "object_id": object_id,
                                "object_name": str(row[1] or ""),
                                "doc_id": row[2],
                                "doc_name": row[3],
                                "doc_date": row[4],
                                "amount_chf": round(object_sum, 2),
                                "amount_excl_vat_chf": round(object_sum_excl_vat, 2),
                                "vat_component_chf": round(vat_component, 2),
                            }
                        )
        except Exception:
            return None

        if total <= 0 or not contributing:
            return None

        company_label = company_hint or "the selected entity"
        from_label = date_from or "(open start)"
        to_label = date_to or "(open end)"
        amount_txt = f"{round(total, 2):,.2f}".replace(",", "")
        vat_phrase = " including VAT" if include_vat else ""
        answer = f"Total travelling expenses{vat_phrase} for {company_label} from {from_label} to {to_label} is {amount_txt} CHF."
        return {
            "question": question,
            "intent": "aggregate_lookup",
            "answer": self._prefix_source(answer, "sql_aggregate"),
            "data": {
                "company_filter": company_hint,
                "date_from": date_from,
                "date_to": date_to,
                "currency": "CHF",
                "vat_included": include_vat,
                "total_amount": round(total, 2),
                "contributing_transactions": contributing,
                "count": len(contributing),
            },
            "source": "sql_aggregate",
            "candidate_count": len(contributing),
        }

    def _extract_criteria_query(
        self,
        text: str,
        language: str,
        default_entity_hint: str | None = None,
    ) -> ParsedQuery | None:
        lowered = str(text or "").strip().lower()

        # Extract time window once — applied to any criteria pattern that matches.
        time_window = self._extract_time_window(text)

        # --- Pattern: "<entity_type> of <person>" with optional time constraint ---
        # e.g. "Show me the expenses of Alex Example in the last three months"
        _etype_pattern = "|".join(re.escape(k) for k in self._ENTITY_TYPE_MAP)
        entity_of_person = re.search(
            rf"(?:show\s+(?:me\s+)?(?:the\s+)?|list\s+(?:all\s+)?|get\s+|find\s+|zeige?\s+(?:mir\s+)?(?:die\s+|den\s+|alle?\s+)?)?({_etype_pattern})\s+(?:of|von|f\u00fcr)\s+([A-Z][a-zA-Z\u00e4\u00f6\u00fc\u00c4\u00d6\u00dc\u00df0-9 .]+?)(?=\s+(?:in|from|over|during|between|before|after|since|ab|bis|zwischen|vom?|in\s+the)|\s*[,;:\.!\?]|$)",
            text,
            flags=re.IGNORECASE,
        )
        if entity_of_person:
            entity_word = (entity_of_person.group(1) or "").strip().lower()
            actor_raw = (entity_of_person.group(2) or "").strip(" .")
            canonical_type, attr_hints = self._ENTITY_TYPE_MAP.get(entity_word, (entity_word, ["date", "amount"]))
            actor = self._normalize_entity_name_hint(actor_raw)
            criteria: dict[str, Any] = {
                "entity_type": canonical_type,
                "must_contain": [actor] if actor else [],
                "attribute_hints": attr_hints,
            }
            if time_window:
                criteria["time_window"] = time_window
            return ParsedQuery(
                intent="criteria_lookup",
                entity_name=actor,
                attribute_name=None,
                confidence=0.88,
                language=language,
                criteria=criteria,
            )

        semantic_doc_query = self._extract_semantic_doc_note_entity_criteria(
            text=text,
            language=language,
            time_window=time_window,
            default_entity_hint=default_entity_hint,
        )
        if semantic_doc_query:
            return semantic_doc_query

        entity_directory = re.search(
            r"(?:show|list|get|find|generate|give\s+me)\s+(?:me\s+)?(?:a\s+)?(?:list\s+of\s+)?([A-Za-z][A-Za-z0-9 _\-/&äöüÄÖÜß]+?)(?:\s+(?:records?|entries|data))?\s+(?:with\s+their\s+)?(.+?)\s+(?:from|in)\s+([A-Za-z0-9 .,&'\-/äöüÄÖÜß]+?)(?:[\?\.!]|$)",
            text,
            flags=re.IGNORECASE,
        )
        if entity_directory:
            entity_phrase = (entity_directory.group(1) or "").strip(" .")
            attributes_phrase = (entity_directory.group(2) or "").strip(" .")
            qualifier = self._normalize_entity_name_hint((entity_directory.group(3) or "").strip(" ."))
            entity_type = self._normalize_attribute_name(entity_phrase) or entity_phrase.strip().lower()
            if entity_type.endswith("ies") and len(entity_type) > 3:
                entity_type = entity_type[:-3] + "y"
            elif entity_type.endswith("ses") and len(entity_type) > 3:
                entity_type = entity_type[:-2]
            elif entity_type.endswith("s") and len(entity_type) > 3:
                entity_type = entity_type[:-1]

            def _directory_attribute_candidates(attribute_text: str) -> list[str]:
                normalized = self._normalize_semantic_text(attribute_text)
                candidates: list[str] = []
                if any(term in normalized for term in ["address", "addresses", "full address", "full addresses"]):
                    candidates.extend(["billing_address", "shipping_address", "address", "registered_address", "address_full"])
                if any(term in normalized for term in ["contact person", "contact persons", "contact", "contacts", "responsible person"]):
                    candidates.extend(["contact_person_ref", "account_manager_ref", "process_contact_person", "responsible_company"])

                fragments = [
                    part.strip()
                    for part in re.split(r"\s+(?:and|with|,|/|plus)\s+", attribute_text, flags=re.IGNORECASE)
                    if part.strip()
                ]
                for fragment in fragments:
                    canonical = self._normalize_attribute_name(fragment)
                    if canonical:
                        candidates.append(canonical)
                    elif fragment not in candidates:
                        candidates.append(fragment)

                deduped: list[str] = []
                seen: set[str] = set()
                for candidate in candidates:
                    key = str(candidate or "").strip().lower()
                    if key and key not in seen:
                        seen.add(key)
                        deduped.append(candidate)
                return deduped

            requested_attributes = _directory_attribute_candidates(attributes_phrase)
            criteria: dict[str, Any] = {
                "entity_type": entity_type,
                "must_contain": [qualifier] if qualifier else [],
                "attribute_hints": requested_attributes or ["address"],
                "response_format": "entity_directory",
                "filter_value": qualifier,
                "semantic_only": True,
            }
            if time_window:
                criteria["time_window"] = time_window
            return ParsedQuery(
                intent="criteria_lookup",
                entity_name=None,
                attribute_name=None,
                confidence=0.9,
                language=language,
                criteria=criteria,
            )

        # --- Pattern: employees with a skill ---
        employee_with_skill = re.search(
            r"(?:list|show|get|find)\s+(?:all\s+)?employees?\s+with\s+([a-zA-Z0-9_\- ]+?)\s+skills?(?:[\?\.!]|$)",
            lowered,
            flags=re.IGNORECASE,
        )
        if employee_with_skill:
            skill = (employee_with_skill.group(1) or "").strip(" .")
            criteria = {
                "entity_type": "employee",
                "must_contain": [skill],
                "attribute_hints": ["skills", "skill", "competencies", "expertise", "description"],
            }
            if time_window:
                criteria["time_window"] = time_window
            return ParsedQuery(
                intent="criteria_lookup",
                entity_name=None,
                attribute_name=None,
                confidence=0.86,
                language=language,
                criteria=criteria,
            )

        # --- Pattern: descriptive company match ---
        descriptive_company = re.search(
            r"(?:which|what)\s+company\s+(?:provides|offers|delivers)\s+(.+?)(?:[\?\.!]|$)",
            lowered,
            flags=re.IGNORECASE,
        )
        if descriptive_company:
            phrase = (descriptive_company.group(1) or "").strip(" .")
            must_terms = [term.strip() for term in re.split(r"[,/]", phrase) if term.strip()]
            if len(must_terms) <= 1:
                must_terms = [part.strip() for part in re.split(r"\band\b", phrase) if part.strip()]
            must_terms = [term for term in must_terms if term and len(term) > 2]
            if must_terms:
                criteria = {
                    "entity_type": "company",
                    "must_contain": must_terms,
                    "attribute_hints": ["description", "services", "activity"],
                    "semantic_only": True,
                }
                if time_window:
                    criteria["time_window"] = time_window
                return ParsedQuery(
                    intent="criteria_lookup",
                    entity_name=None,
                    attribute_name=None,
                    confidence=0.76,
                    language=language,
                    criteria=criteria,
                )

        # --- Pattern: descriptive company match (German) ---
        # e.g. "Welches Unternehmen bietet Lasersysteme und Engineering-basierte Lösungen an?"
        descriptive_company_de = re.search(
            r"(?:welches|welche|was\s+f[üu]r\s+ein)\s+unternehmen\s+(?:bietet|liefert|offeriert)\s+(.+?)(?:\s+an)?(?:[\?\.!]|$)",
            lowered,
            flags=re.IGNORECASE,
        )
        if descriptive_company_de:
            phrase = (descriptive_company_de.group(1) or "").strip(" .")
            must_terms = [term.strip() for term in re.split(r"[,/]", phrase) if term.strip()]
            if len(must_terms) <= 1:
                must_terms = [part.strip() for part in re.split(r"\bund\b", phrase) if part.strip()]
            must_terms = [term for term in must_terms if term and len(term) > 2]
            if must_terms:
                criteria = {
                    "entity_type": "company",
                    "must_contain": must_terms,
                    "attribute_hints": ["description", "services", "activity", "zweck"],
                    "semantic_only": True,
                }
                if time_window:
                    criteria["time_window"] = time_window
                return ParsedQuery(
                    intent="criteria_lookup",
                    entity_name=None,
                    attribute_name=None,
                    confidence=0.78,
                    language=language,
                    criteria=criteria,
                )

        # --- Pattern: document/note concerning/about <entity> (English + German) ---
        # e.g. "Show me the document concerning Alex Example"
        #      "Zeig mir die Notiz betreffend Alex Example"
        doc_note_entity = re.search(
            r"(?:show|list|get|find|give\s+me|zeige?|zeig|liste|finde)\s+(?:me\s+|mir\s+)?(?:the\s+|das\s+|die\s+|den\s+)?(document|documents|note|notes|dokument|dokumente|notiz|notizen)\s+(?:concerning|about|on|regarding|betreffend|ueber|über|zu)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|$)",
            str(text or ""),
            flags=re.IGNORECASE,
        )
        if doc_note_entity:
            kind = str(doc_note_entity.group(1) or "").strip().lower()
            entity_raw = (doc_note_entity.group(2) or "").strip(" .")
            entity_name = self._normalize_entity_name_hint(entity_raw)
            if entity_name:
                # Use lexical criteria over document metadata/text fields plus object linkage.
                # Keep entity_type broad so we do not filter out person-linked docs.
                criteria = {
                    "entity_type": "",
                    "must_contain": [entity_name],
                    "attribute_hints": ["description", "document", "note", "metadata"],
                    "semantic_only": True,
                    "document_hint": (
                        "note"
                        if kind.startswith("note") or kind.startswith("notiz")
                        else "document"
                    ),
                }
                if time_window:
                    criteria["time_window"] = time_window
                return ParsedQuery(
                    intent="criteria_lookup",
                    entity_name=entity_name,
                    attribute_name=None,
                    confidence=0.9,
                    language=language,
                    criteria=criteria,
                )

        # --- Pattern: document/note for <entity> without explicit relation term ---
        # e.g. "Show documents for Alex Example" should return a list, not a single attribute answer.
        doc_note_for_entity = re.search(
            r"(?:show|list|get|find|give\s+me|zeige?|zeig|liste|finde)\s+(?:me\s+|mir\s+)?(?:the\s+|das\s+|die\s+|den\s+)?(document|documents|note|notes|dokument|dokumente|notiz|notizen)\s+(?:for|of|von|für|zu)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\?|$)",
            str(text or ""),
            flags=re.IGNORECASE,
        )
        if doc_note_for_entity:
            kind = str(doc_note_for_entity.group(1) or "").strip().lower()
            entity_raw = (doc_note_for_entity.group(2) or "").strip(" .")
            entity_name = self._normalize_entity_name_hint(entity_raw)
            if entity_name:
                is_note_kind = kind.startswith("note") or kind.startswith("notiz")
                criteria = {
                    "entity_type": "",
                    "must_contain": [entity_name],
                    "attribute_hints": ["description", "document", "note", "metadata"],
                    "semantic_only": True,
                    "document_hint": "note" if is_note_kind else "all",
                    "semantic_frame": "document_about_entity",
                    "semantic_relation": "implicit",
                    "semantic_retrieval_mode": "direct",
                }
                if time_window:
                    criteria["time_window"] = time_window
                return ParsedQuery(
                    intent="criteria_lookup",
                    entity_name=entity_name,
                    attribute_name=None,
                    confidence=0.88,
                    language=language,
                    criteria=criteria,
                )

        # Generic list-style document queries such as "last invoices" should
        # stay on the criteria/document path, not drift into attribute lookup.
        list_document_match = re.search(
            rf"\b(?:last|latest|recent|new|newest|current|previous|prior|oldest|earliest)\s+(?:the\s+)?(?P<etype>{_etype_pattern})\b|\b(?P<etype2>{_etype_pattern})\s+(?:last|latest|recent|new|newest|current|previous|prior|oldest|earliest)\b",
            text,
            flags=re.IGNORECASE,
        )
        if list_document_match:
            entity_word = (list_document_match.group("etype") or list_document_match.group("etype2") or "").strip().lower()
            canonical_type, attr_hints = self._ENTITY_TYPE_MAP.get(entity_word, (entity_word, ["date", "amount"]))
            criteria: dict[str, Any] = {
                "entity_type": canonical_type,
                "must_contain": [entity_word],
                "attribute_hints": attr_hints,
                "semantic_only": True,
                "document_hint": "document" if canonical_type in {"invoice", "receipt", "document", "contract", "report"} else "all",
            }
            if time_window:
                criteria["time_window"] = time_window
            return ParsedQuery(
                intent="criteria_lookup",
                entity_name=None,
                attribute_name=None,
                confidence=0.84,
                language=language,
                criteria=criteria,
            )

        return None

    def _extract_semantic_doc_note_entity_criteria(
        self,
        text: str,
        language: str,
        time_window: dict[str, Any] | None,
        default_entity_hint: str | None = None,
    ) -> ParsedQuery | None:
        """Semantic frame match for multilingual document/note-about-entity queries.

        Frame: action? + (document|note) + relation/aboutness + entity_name
        """
        raw_text = str(text or "").strip()
        if not raw_text:
            return None

        normalized_text = self._normalize_semantic_text(raw_text)
        raw_lower_text = raw_text.lower()
        lexical_tokens = set(re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", normalized_text))
        if not lexical_tokens:
            return None

        document_tokens = {
            "document", "documents", "dokument", "dokumente", "unterlage", "unterlagen", "akte", "akten", "file", "files",
            # Extended: graph/profile/record phrasing users naturally write
            "node", "nodes", "knoten", "profile", "profiles", "profil", "profile",
            "record", "records", "eintrag", "eintraege", "entry", "entries",
            # Finance-document phrasing users often use instead of the generic word "document"
            "bill", "bills", "invoice", "invoices", "receipt", "receipts", "rechnung", "rechnungen", "beleg", "belege",
        }
        _etype_pattern = "|".join(re.escape(k) for k in self._ENTITY_TYPE_MAP)
        note_tokens = {
            "note", "notes", "notiz", "notizen", "memo", "memos",
        }
        action_tokens = {
            "show", "list", "get", "find", "give", "zeige", "zeig", "liste", "list", "finde", "suche", "gib",
        }

        has_document_token = any(tok in lexical_tokens for tok in document_tokens)
        has_note_token = any(tok in lexical_tokens for tok in note_tokens)
        if not has_document_token and not has_note_token:
            return None

        # Do not let document semantic-frame parsing hijack value-style questions
        # (e.g. "How much was the telephone bill for ..."). These should prefer
        # attribute/value resolution paths, which can still bridge to documents later.
        if self._is_value_document_context_question(raw_text):
            has_listing_action = any(tok in lexical_tokens for tok in action_tokens)
            if not has_listing_action:
                return None

        # Relation phrases intentionally include multilingual paraphrases.
        # We split them into direct-content vs graph-relation intent to control
        # whether indirect expansion should be used during retrieval.
        direct_relation_phrases = [
            "concerning", "about", "regarding", "contains", "contain", "contained", "containing", "mentions", "mentioned", "mention", "describes", "describing", "on",
            "betreffend", "beschreibt", "enthaelt", "enthalt",
        ]
        related_relation_phrases = [
            "related to", "related with", "connected to", "linked to",
            "bezogen auf", "bezug auf", "im zusammenhang mit", "im kontext von", "verbunden mit", "verknupft mit", "verknupft", "verknüpft mit", "verknüpft", "zu",
        ]
        relation_phrases = related_relation_phrases + direct_relation_phrases
        matched_relation = next(
            (
                phrase
                for phrase in relation_phrases
                if phrase in normalized_text or phrase in raw_lower_text
            ),
            None,
        )

        retrieval_mode = "direct"
        if any((phrase in normalized_text) or (phrase in raw_lower_text) for phrase in related_relation_phrases):
            retrieval_mode = "related"

        entity_raw = None
        entity_after_relation_patterns = [
            r"(?:concerning|about|regarding|related\s+to|related\s+with|connected\s+to|linked\s+to|describes?|describing|on)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\s+with\b|\s+that\b|\s+which\b|\s+where\b|\s+for\b|\s+having\b|\s+showing\b|\s+including\b|[\?\.!]|$)",
            r"(?:betreffend|bezogen\s+auf|bezug\s+auf|im\s+zusammenhang\s+mit|verbunden\s+mit|verkn[üu]pft\s+mit|beschreibt|ueber|über|zu)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\s+mit\b|\s+als\b|\s+und\b|\s+oder\b|[\?\.!]|$)",
            r"(?:that|which|das|der|die)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)\s+(?:describes|beschreibt)(?:[\?\.!]|$)",
        ]
        for pattern in entity_after_relation_patterns:
            m = re.search(pattern, raw_text, flags=re.IGNORECASE)
            if m:
                entity_raw = (m.group(1) or "").strip(" .")
                if entity_raw:
                    break

        # Handle phrasing like "related to travelling for Chung Yeung Pang":
        # the relation object (travelling) is a topic, while the for-clause carries
        # the primary entity anchor.
        topic_term = self._normalize_entity_name_hint(entity_raw) if entity_raw else None
        anchor_for_entity = None
        for_clause_match = re.search(
            r"\b(?:for|f[üu]r)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\s+with\b|\s+that\b|\s+which\b|\s+having\b|\s+showing\b|\s+including\b|[\?\.!]|$)",
            raw_text,
            flags=re.IGNORECASE,
        )
        if for_clause_match:
            anchor_for_entity = self._normalize_entity_name_hint((for_clause_match.group(1) or "").strip(" ."))
        if anchor_for_entity:
            entity_raw = anchor_for_entity

        # If spaCy already identified the entity (passed in as default_entity_hint), trust it
        # over the regex result. The regex captures trailing words like "with titles and file names"
        # even when our terminators miss edge cases. spaCy's NER knows exactly where the name ends.
        # Use the hint when: it is set, and the regex either over-captured (hint is a prefix/substring
        # of entity_raw) or we got nothing from regex.
        if default_entity_hint:
            hint_lower = default_entity_hint.lower()
            raw_lower = (entity_raw or "").lower()
            if not entity_raw or hint_lower in raw_lower:
                entity_raw = default_entity_hint
        # Generic list-style document queries such as "last invoices" should
        # Generic list-style document queries such as "last invoices" should
        # stay on the criteria/document path, not drift into attribute lookup.
        list_document_match = re.search(
            rf"\b(?:last|latest|recent|new|newest|current|previous|prior|oldest|earliest)\s+(?:the\s+)?(?P<etype>{_etype_pattern})\b|\b(?P<etype2>{_etype_pattern})\s+(?:last|latest|recent|new|newest|current|previous|prior|oldest|earliest)\b",
            text,
            flags=re.IGNORECASE,
        )
        if list_document_match:
            entity_word = (list_document_match.group("etype") or list_document_match.group("etype2") or "").strip().lower()
            canonical_type, attr_hints = self._ENTITY_TYPE_MAP.get(entity_word, (entity_word, ["date", "amount"]))
            criteria: dict[str, Any] = {
                "entity_type": canonical_type,
                "must_contain": [entity_word],
                "attribute_hints": attr_hints,
                "semantic_only": True,
                "document_hint": "document" if canonical_type in {"invoice", "receipt", "document", "contract", "report"} else "all",
            }
            if time_window:
                criteria["time_window"] = time_window
            return ParsedQuery(
                intent="criteria_lookup",
                entity_name=None,
                attribute_name=None,
                confidence=0.84,
                language=language,
                criteria=criteria,
            )
        # stay on the criteria/document path, not drift into attribute lookup.
        list_document_match = re.search(
            rf"\b(?:last|latest|recent|new|newest|current|previous|prior|oldest|earliest)\s+(?:the\s+)?({_etype_pattern})\b|\b({_etype_pattern})\s+(?:last|latest|recent|new|newest|current|previous|prior|oldest|earliest)\b",
            text,
            flags=re.IGNORECASE,
        )
        if list_document_match:
            entity_word = (list_document_match.group(1) or list_document_match.group(2) or "").strip().lower()
            canonical_type, attr_hints = self._ENTITY_TYPE_MAP.get(entity_word, (entity_word, ["date", "amount"]))
            criteria: dict[str, Any] = {
                "entity_type": canonical_type,
                "must_contain": [entity_word],
                "attribute_hints": attr_hints,
                "semantic_only": True,
                "document_hint": "document" if canonical_type in {"invoice", "receipt", "document", "contract", "report"} else "all",
            }
            if time_window:
                criteria["time_window"] = time_window
            return ParsedQuery(
                intent="criteria_lookup",
                entity_name=None,
                attribute_name=None,
                confidence=0.84,
                language=language,
                criteria=criteria,
            )

        entity_name = self._normalize_entity_name_hint(entity_raw) if entity_raw else None
        if not entity_name:
            entity_name = default_entity_hint or self._best_entity_hint(raw_text)
        if not entity_name:
            return None

        must_entities: list[str] = []
        if entity_raw:
            # Support phrases like "X and Y" / "X und Y" by splitting into entity hints.
            for part in re.split(r"\b(?:and|und|&|,)\b", entity_raw, flags=re.IGNORECASE):
                normalized_part = self._normalize_entity_name_hint(str(part or "").strip(" ."))
                if normalized_part and normalized_part not in must_entities:
                    must_entities.append(normalized_part)
        if topic_term and topic_term not in must_entities:
            must_entities.append(topic_term)
        if not must_entities:
            must_entities = [entity_name]
        entity_name = must_entities[0]

        semantic_score = 0.0
        semantic_score += 0.35 if (has_document_token or has_note_token) else 0.0
        semantic_score += 0.30 if matched_relation else 0.0
        semantic_score += 0.10 if any(tok in lexical_tokens for tok in action_tokens) else 0.0
        semantic_score += 0.10 if entity_raw else 0.0
        semantic_score += 0.10 if re.search(re.escape(str(entity_name)), raw_text, flags=re.IGNORECASE) else 0.0

        if semantic_score < 0.72:
            return None

        title_tokens = {"title", "titles", "titel", "titeln", "tile", "tiles"}
        file_tokens = {"file", "files", "filename", "filenames", "name", "names", "dateiname", "dateinamen", "pfad", "path"}
        wants_title_and_files = bool(any(tok in lexical_tokens for tok in title_tokens) and any(tok in lexical_tokens for tok in file_tokens))

        explicit_note_request = bool(
            re.search(
                r"\bnote(?:s)?\s+(?:title|titles|tile|tiles|file|files|filename|filenames|name|names)\b",
                normalized_text,
                flags=re.IGNORECASE,
            )
        )
        explicit_all_documents = bool(
            re.search(
                r"\ball\s+documents\b|\balle\s+dokumente\b",
                normalized_text,
                flags=re.IGNORECASE,
            )
        )
        explicit_notes_only = bool(
            re.search(
                r"\bonly\s+notes?\b|\bnur\s+notizen?\b",
                normalized_text,
                flags=re.IGNORECASE,
            )
        )
        explicit_documents_only = bool(
            re.search(
                r"\bonly\s+documents?\b|\bnur\s+dokumente?\b|\bexclude\s+notes?\b|\bohne\s+notizen?\b",
                normalized_text,
                flags=re.IGNORECASE,
            )
        )

        if explicit_notes_only:
            document_hint = "note"
        elif explicit_documents_only:
            document_hint = "document"
        elif has_note_token and has_document_token and explicit_all_documents:
            document_hint = "all"
        elif has_note_token and has_document_token:
            document_hint = "all"
        elif has_document_token and (wants_title_and_files or matched_relation):
            # Default to combined scope for document-related discovery queries.
            document_hint = "all"
        elif has_note_token and (not has_document_token or explicit_note_request):
            document_hint = "note"
        else:
            document_hint = "document"

        criteria: dict[str, Any] = {
            "entity_type": "",
            "must_contain": must_entities,
            "attribute_hints": ["description", "document", "note", "metadata"],
            "semantic_only": True,
            "document_hint": document_hint,
            "semantic_frame": "document_about_entity",
            "semantic_relation": matched_relation or "implicit",
            "semantic_retrieval_mode": retrieval_mode,
        }
        if anchor_for_entity and topic_term and topic_term.lower() != str(entity_name or "").lower():
            criteria["require_all_terms"] = True
        if wants_title_and_files:
            criteria["response_format"] = "doc_titles_and_files"
        if time_window:
            criteria["time_window"] = time_window

        return ParsedQuery(
            intent="criteria_lookup",
            entity_name=entity_name,
            attribute_name=None,
            confidence=min(0.94, max(0.72, semantic_score)),
            language=language,
            criteria=criteria,
        )

    def search_by_criteria(
        self,
        question: str,
        parsed: ParsedQuery | None = None,
        scope: dict[str, Any] | None = None,
        limit: int = 20,
    ) -> dict[str, Any] | None:
        parsed_query = parsed or self.parse_query(question)
        criteria = parsed_query.criteria if isinstance(parsed_query.criteria, dict) else {}
        if not criteria:
            return None

        must_contain = [str(item).strip() for item in list(criteria.get("must_contain") or []) if str(item).strip()]
        if not must_contain:
            return None

        entity_type = str(criteria.get("entity_type") or "").strip().lower()
        attr_hints = [str(item).strip().lower() for item in list(criteria.get("attribute_hints") or []) if str(item).strip()]
        if not attr_hints:
            attr_hints = ["description"]
        semantic_only = bool(criteria.get("semantic_only"))
        require_all_terms = bool(criteria.get("require_all_terms"))
        keyword_patterns = self._criteria_keyword_patterns(must_contain)

        scope_doc_ids = self._unique_ints(list((scope or {}).get("doc_ids") or []))

        # Extract optional time window from criteria.
        tw = criteria.get("time_window") if isinstance(criteria.get("time_window"), dict) else {}
        date_from: str | None = str(tw["date_from"]).strip() if tw.get("date_from") else None
        date_to: str | None = str(tw["date_to"]).strip() if tw.get("date_to") else None

        # For document/note phrasing, prioritize doc-level matches first.
        doc_hint_result = self._search_documents_by_hint(
            criteria=criteria,
            must_contain=must_contain,
            question=question,
            scope_doc_ids=scope_doc_ids,
            limit=limit,
            date_from=date_from,
            date_to=date_to,
        )
        if doc_hint_result:
            return doc_hint_result

        if semantic_only:
            semantic = self._search_by_criteria_semantic_fallback(
                question=question,
                criteria=criteria,
                scope_doc_ids=scope_doc_ids,
                limit=limit,
            )
            if semantic:
                return semantic

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            oi.object_id,
                            oi.object_name,
                            oi.class_name,
                            oi.metadata,
                            a.attr_type,
                            a.attr_json,
                            d.doc_id,
                            d.doc_name,
                            d.doc_path,
                            d.doc_date::text
                        FROM object_instance oi
                        LEFT JOIN attribute a
                          ON a.src_type='object' AND a.src_id=oi.object_id
                        LEFT JOIN part p ON p.object_id=oi.object_id
                        LEFT JOIN document d ON d.doc_id=p.doc_id
                        WHERE (
                            %s = ''
                            OR LOWER(COALESCE(oi.class_name, '')) = %s
                            OR LOWER(COALESCE(oi.object_name, '')) LIKE %s
                        )
                          AND (
                                                        COALESCE(concat_ws(' ', oi.object_name, oi.class_name, oi.metadata::text, a.attr_type, a.attr_json::text, d.doc_name, d.doc_desc, d.keyword_text), '') ILIKE ANY(%s)
                          )
                          AND (
                            COALESCE(array_length(%s::int[], 1), 0) = 0
                            OR d.doc_id = ANY(%s)
                          )
                          AND (
                            %s IS NULL
                            OR (d.doc_date IS NOT NULL AND d.doc_date >= %s::date)
                          )
                          AND (
                            %s IS NULL
                            OR (d.doc_date IS NOT NULL AND d.doc_date <= %s::date)
                          )
                        ORDER BY d.doc_date DESC NULLS LAST, d.entry_date DESC NULLS LAST
                        LIMIT 800;
                        """,
                        (
                            entity_type,
                            entity_type,
                            f"%{entity_type}%",
                            keyword_patterns,
                            scope_doc_ids,
                            scope_doc_ids,
                            date_from, date_from,
                            date_to, date_to,
                        ),
                    )
                    rows = cur.fetchall()
        except Exception:
            return None

        scored: dict[int | str, dict[str, Any]] = {}
        for row in rows:
            object_name = str(row[1] or "").strip()
            if not object_name:
                continue
            blob = " ".join(
                [
                    object_name,
                    str(row[2] or ""),
                    str(row[3] or ""),
                    str(row[4] or ""),
                    str(row[5] or ""),
                    str(row[7] or ""),
                ]
            )
            blob_norm = self._normalize_semantic_text(blob)
            matched_terms = [token for token in must_contain if self._criteria_term_matches_haystack(token, blob_norm)]
            hit_count = len(matched_terms)
            if hit_count <= 0:
                continue
            if require_all_terms and hit_count < len(must_contain):
                continue

            doc_id = row[6]
            key: int | str = int(doc_id) if isinstance(doc_id, int) else f"obj:{row[0]}:{object_name.lower()}"
            prev = scored.get(key)
            score = float(hit_count)
            item = {
                "object_id": row[0],
                "entity_name": object_name,
                "entity_type": row[2],
                "doc_id": doc_id,
                "doc_name": row[7],
                "doc_path": row[8],
                "doc_date": row[9],
                "matched_terms": matched_terms,
                "score": score,
            }
            if prev is None or score > float(prev.get("score") or 0.0):
                scored[key] = item

        ranked = sorted(scored.values(), key=lambda x: (-float(x.get("score") or 0.0), str(x.get("entity_name") or "")))
        matches = ranked[: max(1, int(limit))]
        if not matches:
            semantic = self._search_by_criteria_semantic_fallback(
                question=question,
                criteria=criteria,
                scope_doc_ids=scope_doc_ids,
                limit=limit,
            )
            if semantic:
                return semantic
            return None

        return {
            "criteria": criteria,
            "time_window": tw or None,
            "count": len(matches),
            "matches": matches,
            "source": "criteria_sql",
        }

    def _search_documents_by_hint(
        self,
        criteria: dict[str, Any],
        must_contain: list[str],
        question: str,
        scope_doc_ids: list[int],
        limit: int,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict[str, Any] | None:
        doc_hint = str(criteria.get("document_hint") or "").strip().lower()
        if doc_hint not in {"document", "note", "all"}:
            return None

        retrieval_mode = str(criteria.get("semantic_retrieval_mode") or "direct").strip().lower()
        allow_indirect_expansion = retrieval_mode in {"related", "expanded"} or bool(
            criteria.get("allow_indirect_relationships")
        )
        scope_doc_id_set = set(scope_doc_ids or [])

        sql_doc_filter = "any" if doc_hint == "all" else doc_hint

        keyword_patterns = self._criteria_keyword_patterns(must_contain)
        if not keyword_patterns:
            return None

        require_all_terms = bool(criteria.get("require_all_terms"))
        required_terms_norm = {
            self._normalize_semantic_text(term)
            for term in must_contain
            if self._normalize_semantic_text(term)
        }
        semantic_frame = str(criteria.get("semantic_frame") or "").strip().lower()
        entity_anchor_term = str((must_contain[0] if must_contain else "") or "").strip()
        entity_anchor_doc_ids: set[int] = set()

        if require_all_terms and semantic_frame == "document_about_entity" and entity_anchor_term:
            entity_patterns = self._criteria_keyword_patterns([entity_anchor_term])
            if entity_patterns:
                try:
                    with self._get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT DISTINCT p.doc_id
                                FROM part p
                                                                LEFT JOIN object_instance oi ON oi.object_id = p.object_id
                                                                LEFT JOIN object_relationship rel ON rel.relationship_id = p.relationship_id
                                                                LEFT JOIN object_instance src ON src.object_id = rel.src_object_id
                                                                LEFT JOIN object_instance tar ON tar.object_id = rel.tar_object_id
                                                                WHERE 1=1
                                  AND (
                                    COALESCE(array_length(%s::int[], 1), 0) = 0
                                    OR p.doc_id = ANY(%s)
                                  )
                                                                    AND COALESCE(
                                                                                concat_ws(
                                                                                        ' ',
                                                                                        oi.object_name,
                                                                                        oi.class_name,
                                                                                        oi.metadata::text,
                                                                                        src.object_name,
                                                                                        src.class_name,
                                                                                        src.metadata::text,
                                                                                        tar.object_name,
                                                                                        tar.class_name,
                                                                                        tar.metadata::text
                                                                                ),
                                                                                ''
                                                                            ) ILIKE ANY(%s)
                                """,
                                (scope_doc_ids, scope_doc_ids, entity_patterns),
                            )
                            entity_anchor_doc_ids = {int(row[0]) for row in (cur.fetchall() or []) if isinstance(row[0], int)}
                except Exception:
                    entity_anchor_doc_ids = set()

        def _has_full_term_coverage(matched_terms: list[str]) -> bool:
            if not require_all_terms:
                return True
            matched_norm = {
                self._normalize_semantic_text(term)
                for term in (matched_terms or [])
                if self._normalize_semantic_text(term)
            }
            return required_terms_norm.issubset(matched_norm)

        def _upsert_scored(item: dict[str, Any]) -> None:
            doc_id = item.get("doc_id")
            if not isinstance(doc_id, int):
                return
            prev = scored.get(doc_id)
            if prev is None:
                scored[doc_id] = item
                return
            merged_terms: list[str] = []
            seen: set[str] = set()
            for token in list(prev.get("matched_terms") or []) + list(item.get("matched_terms") or []):
                key = str(token or "").strip().lower()
                if not key or key in seen:
                    continue
                seen.add(key)
                merged_terms.append(str(token))
            winner = dict(prev if float(prev.get("score") or 0.0) >= float(item.get("score") or 0.0) else item)
            winner["matched_terms"] = merged_terms
            winner["score"] = max(float(prev.get("score") or 0.0), float(item.get("score") or 0.0))
            scored[doc_id] = winner

        def _needs_enrichment(doc_id: int) -> bool:
            existing = scored.get(doc_id)
            if existing is None:
                return True
            if not require_all_terms:
                return False
            return not _has_full_term_coverage(list(existing.get("matched_terms") or []))

        q_norm = self._normalize_semantic_text(question)
        q_asks_money = bool(
            re.search(
                r"\b(total|amount|cost|price|due|sum|charge|fee|bill|invoice|rechnung|betrag|gesamt|kosten|preis)\b",
                q_norm,
                flags=re.IGNORECASE,
            )
        )
        telecom_terms = {
            "telecom", "telecommunication", "telecommunications", "telephone", "phone", "mobile", "cell", "cellular", "gsm",
            "telefon", "telefonie", "mobil", "mobilfunk", "telekommunikation",
        }
        billing_terms = {
            "bill", "bills", "invoice", "invoices", "receipt", "receipts", "statement",
            "rechnung", "rechnungen", "beleg", "belege", "quittung", "payment", "payments", "charge", "charges",
            "amount", "cost", "price", "due", "total", "betrag", "kosten", "preis", "gesamt", "sum",
        }
        total_terms = {
            "amount due", "total due", "total amount", "grand total", "payable", "zu bezahlen", "zahlbar", "gesamtbetrag",
        }
        q_wants_telecom = any(term in q_norm for term in telecom_terms)
        q_wants_billing = any(term in q_norm for term in billing_terms)

        def _semantic_doc_boost(haystack_text: str) -> float:
            hay = self._normalize_semantic_text(haystack_text)
            if not hay:
                return 0.0

            boost = 0.0
            has_telecom = any(term in hay for term in telecom_terms)
            has_billing = any(term in hay for term in billing_terms)
            has_total = any(term in hay for term in total_terms)

            if q_wants_telecom and has_telecom:
                boost += 1.20
            if q_wants_billing and has_billing:
                boost += 1.20
            if q_wants_telecom and q_wants_billing and has_telecom and has_billing:
                boost += 0.90
            if q_asks_money and has_total:
                boost += 0.60
            return boost

        query_sql = """
                        SELECT
                            d.doc_id,
                            d.doc_key,
                            d.doc_path,
                            d.doc_cat,
                            d.doc_type,
                            d.doc_date::text,
                            d.doc_theme,
                            d.keyword_text,
                            d.metadata
                        FROM document d
                        WHERE COALESCE(d.status, 'active') = 'active'
                          AND d.valid_from <= CURRENT_DATE
                          AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                          AND (
                            COALESCE(array_length(%s::int[], 1), 0) = 0
                            OR d.doc_id = ANY(%s)
                          )
                          AND (
                            %s IS NULL
                            OR (d.doc_date IS NOT NULL AND d.doc_date >= %s::date)
                          )
                          AND (
                            %s IS NULL
                            OR (d.doc_date IS NOT NULL AND d.doc_date <= %s::date)
                          )
                          AND COALESCE(concat_ws(' ', d.doc_key, d.doc_path, d.doc_theme, d.keyword_text, d.doc_cat, d.doc_type, d.metadata::text), '') ILIKE ANY(%s)
                          AND (
                            (%s = 'note' AND (
                                LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                            ))
                            OR (%s = 'document' AND NOT (
                                LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                            ))
                            OR (%s = 'any')
                          )
                        ORDER BY d.doc_date DESC NULLS LAST, d.entry_date DESC NULLS LAST
                        LIMIT 500;
                        """

        def _fetch_rows(doc_filter: str) -> list[tuple[Any, ...]]:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        query_sql,
                        (
                            scope_doc_ids,
                            scope_doc_ids,
                            date_from,
                            date_from,
                            date_to,
                            date_to,
                            keyword_patterns,
                            doc_filter,
                            doc_filter,
                            doc_filter,
                        ),
                    )
                    return list(cur.fetchall() or [])

        try:
            rows = _fetch_rows(sql_doc_filter)
            if not rows and doc_hint == "document":
                # Relax document mode if everything is tagged as note-like in metadata.
                rows = _fetch_rows("any")
        except Exception:
            return None

        scored: dict[int, dict[str, Any]] = {}

        def _term_matches_haystack(term: str, haystack: str) -> bool:
            return self._criteria_term_matches_haystack(term, haystack)

        for row in rows:
            doc_id = row[0]
            if not isinstance(doc_id, int):
                continue

            metadata = row[8] if isinstance(row[8], dict) else {}
            metadata_desc = str(metadata.get("description") or "")
            metadata_user_desc = str(metadata.get("user_description") or "")
            user_meta = metadata.get("user_metadata") if isinstance(metadata.get("user_metadata"), dict) else {}
            user_subject = str(user_meta.get("email_subject") or user_meta.get("subject") or "")
            user_summary = str(user_meta.get("summary") or "")

            blob = self._normalize_semantic_text(
                " ".join(
                    [
                        str(row[1] or ""),
                        str(row[3] or ""),
                        str(row[4] or ""),
                        str(row[6] or ""),
                        str(row[7] or ""),
                        metadata_desc,
                        metadata_user_desc,
                        user_subject,
                        user_summary,
                    ]
                )
            )

            matched_terms: list[str] = []
            for term in must_contain:
                matched_from_text = _term_matches_haystack(term, blob)
                matched_from_entity_link = (
                    bool(entity_anchor_doc_ids)
                    and term == entity_anchor_term
                    and isinstance(doc_id, int)
                    and doc_id in entity_anchor_doc_ids
                )
                matched_from_scope_anchor = (
                    require_all_terms
                    and semantic_frame == "document_about_entity"
                    and retrieval_mode in {"related", "expanded"}
                    and term == entity_anchor_term
                    and isinstance(doc_id, int)
                    and doc_id in scope_doc_id_set
                )
                if matched_from_text or matched_from_entity_link or matched_from_scope_anchor:
                    matched_terms.append(term)
            if not matched_terms:
                continue

            score = float(len(matched_terms)) + _semantic_doc_boost(blob)
            _d_meta = row[8] if isinstance(row[8], dict) else {}
            item = {
                "object_id": None,
                "entity_name": str(row[1] or row[2] or f"document_{doc_id}"),
                "entity_type": "document",
                "doc_id": doc_id,
                "doc_name": row[1],
                "doc_path": row[2],
                "doc_date": row[5],
                "title": self._resolve_doc_title(row[1], _d_meta),
                "file_name": self._resolve_file_name(row[1], row[2], _d_meta),
                "file_path": row[2],
                "doc_cat": str(row[3] or ""),
                "doc_type": str(row[4] or ""),
                "doc_theme": str(row[6] or ""),
                "keyword_text": str(row[7] or ""),
                "matched_terms": matched_terms,
                "score": score,
            }
            _upsert_scored(item)

        # In direct mode, add lexical evidence from indexed chunk text as well.
        # This keeps retrieval semantically strict (explicit mention) while avoiding
        # false negatives when names appear in body text but not in document metadata.
        if retrieval_mode == "direct":
            # Fast structural path: direct object links via part.object_id.
            # If an entity object is explicitly linked to the document, include it
            # without requiring full-text hits in document metadata.
            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT
                                d.doc_id,
                                d.doc_key,
                                d.doc_path,
                                d.doc_date::text,
                                d.metadata,
                                oi.object_id,
                                oi.object_name,
                                oi.class_name,
                                oi.metadata
                            FROM document d
                            JOIN part p
                              ON p.doc_id = d.doc_id
                             AND p.object_id IS NOT NULL
                            JOIN object_instance oi
                              ON oi.object_id = p.object_id
                            WHERE COALESCE(d.status, 'active') = 'active'
                              AND d.valid_from <= CURRENT_DATE
                              AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                              AND (
                                COALESCE(array_length(%s::int[], 1), 0) = 0
                                OR d.doc_id = ANY(%s)
                              )
                              AND (
                                %s IS NULL
                                OR (d.doc_date IS NOT NULL AND d.doc_date >= %s::date)
                              )
                              AND (
                                %s IS NULL
                                OR (d.doc_date IS NOT NULL AND d.doc_date <= %s::date)
                              )
                                                            AND COALESCE(concat_ws(' ', oi.object_name, oi.class_name, oi.metadata::text), '') ILIKE ANY(%s)
                              AND (
                                (%s = 'note' AND (
                                    LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                                ))
                                OR (%s = 'document' AND NOT (
                                    LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                                ))
                                OR (%s = 'any')
                              )
                            ORDER BY d.doc_date DESC NULLS LAST, d.entry_date DESC NULLS LAST
                            LIMIT 1200;
                            """,
                            (
                                scope_doc_ids,
                                scope_doc_ids,
                                date_from,
                                date_from,
                                date_to,
                                date_to,
                                keyword_patterns,
                                sql_doc_filter,
                                sql_doc_filter,
                                sql_doc_filter,
                            ),
                        )
                        direct_link_rows = cur.fetchall()

                for row in direct_link_rows:
                    doc_id = row[0]
                    if not isinstance(doc_id, int):
                        continue

                    haystack = self._normalize_semantic_text(
                        " ".join(
                            [
                                str(row[6] or ""),
                                str(row[7] or ""),
                                str(row[8] or ""),
                            ]
                        )
                    )

                    matched_terms: list[str] = []
                    for term in must_contain:
                        if _term_matches_haystack(term, haystack):
                            matched_terms.append(term)
                    if not matched_terms:
                        continue

                    metadata = row[4] if isinstance(row[4], dict) else {}
                    doc_semantic_text = " ".join(
                        [
                            str(row[1] or ""),
                            str(row[2] or ""),
                            str(row[6] or ""),
                            str(row[7] or ""),
                            str(row[8] or ""),
                        ]
                    )
                    item = {
                        "object_id": row[5] if isinstance(row[5], int) else None,
                        "entity_name": str(row[1] or row[2] or f"document_{doc_id}"),
                        "entity_type": "document",
                        "doc_id": doc_id,
                        "doc_name": row[1],
                        "doc_path": row[2],
                        "doc_date": row[3],
                        "title": self._resolve_doc_title(row[1], metadata),
                        "file_name": self._resolve_file_name(row[1], row[2], metadata),
                        "file_path": row[2],
                        "matched_terms": matched_terms,
                        # Strong direct evidence: explicit object link in part table.
                        "score": float(len(matched_terms)) + 2.25 + _semantic_doc_boost(doc_semantic_text),
                    }
                    _upsert_scored(item)
            except Exception:
                pass

            q_text = str(question or "").strip() or " ".join(must_contain)
            candidates = self.qdrant_candidates(q_text, limit=max(24, int(limit) * 8))
            scope_set = set(scope_doc_ids or [])
            candidate_hits: dict[int, dict[str, Any]] = {}

            for cand in candidates:
                cand_doc_id = cand.get("doc_id")
                if not isinstance(cand_doc_id, int):
                    continue
                if scope_set and cand_doc_id not in scope_set:
                    continue

                haystack = self._normalize_semantic_text(
                    " ".join(
                        [
                            str(cand.get("text") or ""),
                            str(cand.get("chunk_text") or ""),
                            str(cand.get("doc_key") or ""),
                            str(cand.get("document_type") or cand.get("doc_type") or ""),
                        ]
                    )
                )

                matched_terms: list[str] = []
                for term in must_contain:
                    if _term_matches_haystack(term, haystack):
                        matched_terms.append(term)
                if not matched_terms:
                    continue

                score = float(len(matched_terms)) + 0.75 + _semantic_doc_boost(haystack)
                prev_hit = candidate_hits.get(cand_doc_id)
                if prev_hit is None or score > float(prev_hit.get("score") or 0.0):
                    candidate_hits[cand_doc_id] = {
                        "matched_terms": matched_terms,
                        "score": score,
                    }

            q_doc_ids = [doc_id for doc_id in candidate_hits.keys() if _needs_enrichment(doc_id)]
            if q_doc_ids:
                try:
                    with self._get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT
                                    d.doc_id,
                                    d.doc_key,
                                    d.doc_path,
                                    d.doc_date::text,
                                    d.metadata
                                FROM document d
                                WHERE d.doc_id = ANY(%s)
                                  AND COALESCE(d.status, 'active') = 'active'
                                  AND d.valid_from <= CURRENT_DATE
                                  AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                                  AND (
                                    %s IS NULL
                                    OR (d.doc_date IS NOT NULL AND d.doc_date >= %s::date)
                                  )
                                  AND (
                                    %s IS NULL
                                    OR (d.doc_date IS NOT NULL AND d.doc_date <= %s::date)
                                  )
                                  AND (
                                    (%s = 'note' AND (
                                        LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                                    ))
                                    OR (%s = 'document' AND NOT (
                                        LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                                    ))
                                    OR (%s = 'any')
                                  );
                                """,
                                (
                                    q_doc_ids,
                                    date_from,
                                    date_from,
                                    date_to,
                                    date_to,
                                    sql_doc_filter,
                                    sql_doc_filter,
                                    sql_doc_filter,
                                ),
                            )
                            q_rows = cur.fetchall()

                    for row in q_rows:
                        doc_id = row[0]
                        if not isinstance(doc_id, int):
                            continue
                        hit = candidate_hits.get(doc_id)
                        if not hit:
                            continue

                        metadata = row[4] if isinstance(row[4], dict) else {}
                        item = {
                            "object_id": None,
                            "entity_name": str(row[1] or row[2] or f"document_{doc_id}"),
                            "entity_type": "document",
                            "doc_id": doc_id,
                            "doc_name": row[1],
                            "doc_path": row[2],
                            "doc_date": row[3],
                            "title": self._resolve_doc_title(row[1], metadata),
                            "file_name": self._resolve_file_name(row[1], row[2], metadata),
                            "file_path": row[2],
                            "doc_theme": str(metadata.get("doc_theme") or ""),
                            "matched_terms": list(hit.get("matched_terms") or []),
                            "score": float(hit.get("score") or 0.0),
                        }
                        _upsert_scored(item)
                except Exception:
                    pass

            # Final direct-evidence fallback: check cached markdown text if available.
            # This ensures explicit mentions in OCR/markdown are considered even when
            # document metadata and vector index are sparse.
            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT
                                d.doc_id,
                                d.doc_key,
                                d.doc_path,
                                d.doc_date::text,
                                d.metadata
                            FROM document d
                            WHERE COALESCE(d.status, 'active') = 'active'
                              AND d.valid_from <= CURRENT_DATE
                              AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                              AND (
                                COALESCE(array_length(%s::int[], 1), 0) = 0
                                OR d.doc_id = ANY(%s)
                              )
                              AND (
                                %s IS NULL
                                OR (d.doc_date IS NOT NULL AND d.doc_date >= %s::date)
                              )
                              AND (
                                %s IS NULL
                                OR (d.doc_date IS NOT NULL AND d.doc_date <= %s::date)
                              )
                              AND (
                                (%s = 'note' AND (
                                    LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                                ))
                                OR (%s = 'document' AND NOT (
                                    LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                                ))
                                OR (%s = 'any')
                              );
                            """,
                            (
                                scope_doc_ids,
                                scope_doc_ids,
                                date_from,
                                date_from,
                                date_to,
                                date_to,
                                sql_doc_filter,
                                sql_doc_filter,
                                sql_doc_filter,
                            ),
                        )
                        md_rows = cur.fetchall()

                base_dir = os.path.dirname(__file__)
                for row in md_rows:
                    doc_id = row[0]
                    if not isinstance(doc_id, int) or not _needs_enrichment(doc_id):
                        continue

                    metadata = row[4] if isinstance(row[4], dict) else {}
                    user_md = metadata.get("user_metadata") if isinstance(metadata, dict) else {}
                    source_ref = user_md.get("source_reference") if isinstance(user_md, dict) else {}
                    markdown_rel = source_ref.get("markdown_cache_path") if isinstance(source_ref, dict) else None
                    if not markdown_rel:
                        continue

                    md_path = str(markdown_rel).strip()
                    if not os.path.isabs(md_path):
                        md_path = os.path.join(base_dir, md_path)
                    md_path = os.path.normpath(md_path)
                    if not os.path.exists(md_path):
                        continue

                    try:
                        with open(md_path, "r", encoding="utf-8", errors="ignore") as fh:
                            md_text = fh.read()
                    except Exception:
                        continue

                    haystack = self._normalize_semantic_text(md_text)
                    matched_terms: list[str] = []
                    for term in must_contain:
                        if _term_matches_haystack(term, haystack):
                            matched_terms.append(term)
                    if not matched_terms:
                        continue

                    md_semantic_text = " ".join(
                        [
                            str(row[1] or ""),
                            str(row[2] or ""),
                            str(metadata.get("doc_theme") or ""),
                            str(metadata.get("keyword_text") or ""),
                            md_text[:1400],
                        ]
                    )
                    item = {
                        "object_id": None,
                        "entity_name": str(row[1] or row[2] or f"document_{doc_id}"),
                        "entity_type": "document",
                        "doc_id": doc_id,
                        "doc_name": row[1],
                        "doc_path": row[2],
                        "doc_date": row[3],
                        "title": self._resolve_doc_title(row[1], metadata),
                        "file_name": self._resolve_file_name(row[1], row[2], metadata),
                        "file_path": row[2],
                        "matched_terms": matched_terms,
                        "score": float(len(matched_terms)) + 0.65 + _semantic_doc_boost(md_semantic_text),
                    }
                    _upsert_scored(item)
            except Exception:
                pass

        ranked = sorted(scored.values(), key=lambda x: (-float(x.get("score") or 0.0), str(x.get("entity_name") or "")))

        # Augment with docs structurally linked to matched entities only when the
        # query intent is explicitly relation-oriented (for example "related to").
        if allow_indirect_expansion:
            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                        """
                        SELECT
                            d.doc_id,
                            d.doc_key,
                            d.doc_path,
                            d.doc_date::text,
                            d.metadata,
                            COALESCE(oi.object_name, src.object_name, tar.object_name, '') AS linked_name,
                            COALESCE(oi.class_name, src.class_name, tar.class_name, '') AS linked_class
                        FROM document d
                        LEFT JOIN part p_obj ON p_obj.doc_id = d.doc_id AND p_obj.object_id IS NOT NULL
                        LEFT JOIN object_instance oi ON oi.object_id = p_obj.object_id
                        LEFT JOIN part p_rel ON p_rel.doc_id = d.doc_id AND p_rel.relationship_id IS NOT NULL
                        LEFT JOIN object_relationship rel ON rel.relationship_id = p_rel.relationship_id
                        LEFT JOIN object_instance src ON src.object_id = rel.src_object_id
                        LEFT JOIN object_instance tar ON tar.object_id = rel.tar_object_id
                        WHERE COALESCE(d.status, 'active') = 'active'
                          AND d.valid_from <= CURRENT_DATE
                          AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                          AND (
                            COALESCE(array_length(%s::int[], 1), 0) = 0
                            OR d.doc_id = ANY(%s)
                          )
                          AND (
                            %s IS NULL
                            OR (d.doc_date IS NOT NULL AND d.doc_date >= %s::date)
                          )
                          AND (
                            %s IS NULL
                            OR (d.doc_date IS NOT NULL AND d.doc_date <= %s::date)
                          )
                          AND (
                            COALESCE(oi.object_name, '') ILIKE ANY(%s)
                            OR COALESCE(src.object_name, '') ILIKE ANY(%s)
                            OR COALESCE(tar.object_name, '') ILIKE ANY(%s)
                          )
                          AND (
                            (%s = 'note' AND (
                                LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                            ))
                            OR (%s = 'document' AND NOT (
                                LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                            ))
                            OR (%s = 'any')
                          )
                        ORDER BY d.doc_date DESC NULLS LAST, d.entry_date DESC NULLS LAST
                        LIMIT 1200;
                        """,
                        (
                            scope_doc_ids,
                            scope_doc_ids,
                            date_from,
                            date_from,
                            date_to,
                            date_to,
                            keyword_patterns,
                            keyword_patterns,
                            keyword_patterns,
                            sql_doc_filter,
                            sql_doc_filter,
                            sql_doc_filter,
                        ),
                    )
                    linked_rows = cur.fetchall()

                    # Second-hop structural evidence: if an entity is linked to another object via
                    # relationship edges, include documents attached to that connected object.
                    # This helps surface official company docs for person-centric queries.
                    cur.execute(
                        """
                        WITH matched_entities AS (
                            SELECT oi.object_id
                            FROM object_instance oi
                            WHERE COALESCE(oi.object_name, '') ILIKE ANY(%s)
                        ),
                        connected_objects AS (
                            SELECT DISTINCT
                                CASE
                                    WHEN rel.src_object_id = me.object_id THEN rel.tar_object_id
                                    WHEN rel.tar_object_id = me.object_id THEN rel.src_object_id
                                    ELSE NULL
                                END AS connected_object_id,
                                rel.relationship_id,
                                me.object_id AS anchor_object_id
                            FROM matched_entities me
                            JOIN object_relationship rel
                                ON rel.src_object_id = me.object_id
                                OR rel.tar_object_id = me.object_id
                        )
                        SELECT
                            d.doc_id,
                            d.doc_key,
                            d.doc_path,
                            d.doc_date::text,
                            d.metadata,
                            COALESCE(anchor.object_name, '') AS anchor_name,
                            COALESCE(conn_obj.object_name, '') AS connected_name,
                            COALESCE(conn_obj.class_name, '') AS connected_class
                        FROM connected_objects co
                        JOIN object_instance conn_obj
                            ON conn_obj.object_id = co.connected_object_id
                        LEFT JOIN object_instance anchor
                            ON anchor.object_id = co.anchor_object_id
                        JOIN part p
                            ON (
                                p.object_id = co.connected_object_id
                                OR p.relationship_id = co.relationship_id
                            )
                        JOIN document d
                            ON d.doc_id = p.doc_id
                        WHERE COALESCE(d.status, 'active') = 'active'
                            AND d.valid_from <= CURRENT_DATE
                            AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                            AND (
                                COALESCE(array_length(%s::int[], 1), 0) = 0
                                OR d.doc_id = ANY(%s)
                            )
                            AND (
                                %s IS NULL
                                OR (d.doc_date IS NOT NULL AND d.doc_date >= %s::date)
                            )
                            AND (
                                %s IS NULL
                                OR (d.doc_date IS NOT NULL AND d.doc_date <= %s::date)
                            )
                            AND (
                                (%s = 'note' AND (
                                    LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                                ))
                                OR (%s = 'document' AND NOT (
                                    LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                    OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                                ))
                                OR (%s = 'any')
                            )
                        ORDER BY d.doc_date DESC NULLS LAST, d.entry_date DESC NULLS LAST
                        LIMIT 1200;
                        """,
                        (
                            keyword_patterns,
                            scope_doc_ids,
                            scope_doc_ids,
                            date_from,
                            date_from,
                            date_to,
                            date_to,
                            sql_doc_filter,
                            sql_doc_filter,
                            sql_doc_filter,
                        ),
                    )
                    related_rows = cur.fetchall()

                    # If target documents are not yet materialized in part rows, derive connected
                    # entity names via relationships and use those names as document lexical hints.
                    cur.execute(
                        """
                        WITH matched_entities AS (
                            SELECT oi.object_id, oi.object_name
                            FROM object_instance oi
                            WHERE COALESCE(oi.object_name, '') ILIKE ANY(%s)
                        )
                        SELECT DISTINCT
                            me.object_name AS anchor_name,
                            CASE
                                WHEN rel.src_object_id = me.object_id THEN tar.object_name
                                ELSE src.object_name
                            END AS connected_name,
                            CASE
                                WHEN rel.src_object_id = me.object_id THEN tar.class_name
                                ELSE src.class_name
                            END AS connected_class
                        FROM matched_entities me
                        JOIN object_relationship rel
                          ON rel.src_object_id = me.object_id
                          OR rel.tar_object_id = me.object_id
                        LEFT JOIN object_instance src ON src.object_id = rel.src_object_id
                        LEFT JOIN object_instance tar ON tar.object_id = rel.tar_object_id;
                        """,
                        (keyword_patterns,),
                    )
                    related_entities = cur.fetchall()

                    connected_names: list[str] = []
                    preferred_classes = {"company", "legal_entity", "entity", "organization", "organisation"}
                    for rel_row in related_entities:
                        connected_name = str(rel_row[1] or "").strip()
                        connected_class = str(rel_row[2] or "").strip().lower()
                        if not connected_name:
                            continue
                        if connected_class and connected_class not in preferred_classes:
                            continue
                        if connected_name not in connected_names:
                            connected_names.append(connected_name)

                    related_doc_rows: list[tuple[Any, ...]] = []
                    if connected_names:
                        connected_patterns: list[str] = []
                        for name in connected_names:
                            lowered_name = str(name or "").strip().lower()
                            if not lowered_name:
                                continue
                            connected_patterns.append(f"%{lowered_name}%")
                            for token in re.findall(r"[a-zA-ZäöüÄÖÜß0-9]{4,}", lowered_name):
                                connected_patterns.append(f"%{token}%")
                        connected_patterns = sorted(set(connected_patterns))

                        if connected_patterns:
                            cur.execute(
                                """
                                SELECT
                                    d.doc_id,
                                    d.doc_key,
                                    d.doc_path,
                                    d.doc_date::text,
                                    d.metadata,
                                    d.doc_theme,
                                    d.keyword_text,
                                    d.doc_cat,
                                    d.doc_type
                                FROM document d
                                WHERE COALESCE(d.status, 'active') = 'active'
                                  AND d.valid_from <= CURRENT_DATE
                                  AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                                  AND (
                                    COALESCE(array_length(%s::int[], 1), 0) = 0
                                    OR d.doc_id = ANY(%s)
                                  )
                                  AND (
                                    %s IS NULL
                                    OR (d.doc_date IS NOT NULL AND d.doc_date >= %s::date)
                                  )
                                  AND (
                                    %s IS NULL
                                    OR (d.doc_date IS NOT NULL AND d.doc_date <= %s::date)
                                  )
                                  AND LOWER(COALESCE(concat_ws(' ', d.doc_key, d.doc_path, d.doc_theme, d.keyword_text, d.doc_cat, d.doc_type, d.metadata::text), '')) LIKE ANY(%s)
                                  AND (
                                    (%s = 'note' AND (
                                        LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                                    ))
                                    OR (%s = 'document' AND NOT (
                                        LOWER(COALESCE(d.doc_type, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_cat, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.metadata->>'user_description', '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_key, '')) LIKE '%%note%%'
                                        OR LOWER(COALESCE(d.doc_path, '')) LIKE '%%note%%'
                                    ))
                                    OR (%s = 'any')
                                  )
                                ORDER BY d.doc_date DESC NULLS LAST, d.entry_date DESC NULLS LAST
                                LIMIT 1200;
                                """,
                                (
                                    scope_doc_ids,
                                    scope_doc_ids,
                                    date_from,
                                    date_from,
                                    date_to,
                                    date_to,
                                    connected_patterns,
                                    sql_doc_filter,
                                    sql_doc_filter,
                                    sql_doc_filter,
                                ),
                            )
                            related_doc_rows = cur.fetchall()

                for row in linked_rows:
                    doc_id = row[0]
                    if not isinstance(doc_id, int):
                        continue
                    linked_name = str(row[5] or "").strip()
                    linked_class = str(row[6] or "").strip()
                    haystack = self._normalize_semantic_text(f"{linked_name} {linked_class}")

                    matched_terms: list[str] = []
                    for term in must_contain:
                        normalized_term = self._normalize_semantic_text(term)
                        if normalized_term and any(v in haystack for v in self._semantic_token_variants(normalized_term)):
                            matched_terms.append(term)
                    if not matched_terms:
                        continue

                    metadata = row[4] if isinstance(row[4], dict) else {}
                    item = {
                        "object_id": None,
                        "entity_name": str(row[1] or row[2] or f"document_{doc_id}"),
                        "entity_type": "document",
                        "doc_id": doc_id,
                        "doc_name": row[1],
                        "doc_path": row[2],
                        "doc_date": row[3],
                        "title": self._resolve_doc_title(row[1], metadata),
                        "file_name": self._resolve_file_name(row[1], row[2], metadata),
                        "file_path": row[2],
                        "matched_terms": matched_terms,
                        # Linked-entity evidence is strong; boost to ensure inclusion.
                        "score": float(len(matched_terms) + 2.0) + _semantic_doc_boost(haystack),
                    }
                    _upsert_scored(item)

                for row in related_rows:
                    doc_id = row[0]
                    if not isinstance(doc_id, int):
                        continue

                    anchor_name = str(row[5] or "").strip()
                    connected_name = str(row[6] or "").strip()
                    connected_class = str(row[7] or "").strip()
                    haystack = self._normalize_semantic_text(f"{anchor_name} {connected_name} {connected_class}")

                    matched_terms: list[str] = []
                    for term in must_contain:
                        normalized_term = self._normalize_semantic_text(term)
                        if normalized_term and any(v in haystack for v in self._semantic_token_variants(normalized_term)):
                            matched_terms.append(term)
                    if not matched_terms:
                        continue

                    metadata = row[4] if isinstance(row[4], dict) else {}
                    item = {
                        "object_id": None,
                        "entity_name": str(row[1] or row[2] or f"document_{doc_id}"),
                        "entity_type": "document",
                        "doc_id": doc_id,
                        "doc_name": row[1],
                        "doc_path": row[2],
                        "doc_date": row[3],
                        "title": self._resolve_doc_title(row[1], metadata),
                        "file_name": self._resolve_file_name(row[1], row[2], metadata),
                        "file_path": row[2],
                        "matched_terms": matched_terms,
                        # Second-hop evidence is slightly weaker than direct linked_name evidence.
                        "score": float(len(matched_terms) + 1.5) + _semantic_doc_boost(haystack),
                    }
                    _upsert_scored(item)

                for row in related_doc_rows:
                    doc_id = row[0]
                    if not isinstance(doc_id, int):
                        continue

                    metadata = row[4] if isinstance(row[4], dict) else {}
                    item = {
                        "object_id": None,
                        "entity_name": str(row[1] or row[2] or f"document_{doc_id}"),
                        "entity_type": "document",
                        "doc_id": doc_id,
                        "doc_name": row[1],
                        "doc_path": row[2],
                        "doc_date": row[3],
                        "title": self._resolve_doc_title(row[1], metadata),
                        "file_name": self._resolve_file_name(row[1], row[2], metadata),
                        "file_path": row[2],
                        "doc_theme": str(row[5] or ""),
                        "keyword_text": str(row[6] or ""),
                        "doc_cat": str(row[7] or ""),
                        "doc_type": str(row[8] or ""),
                        # Keep the original queried entity as the matched term, because this match
                        # is supported via relationship-connected entity names.
                        "matched_terms": list(must_contain),
                        "score": float(len(must_contain) + 1.25)
                        + _semantic_doc_boost(
                            " ".join(
                                [
                                    str(row[1] or ""),
                                    str(row[2] or ""),
                                    str(row[5] or ""),
                                    str(row[6] or ""),
                                    str(row[7] or ""),
                                    str(row[8] or ""),
                                ]
                            )
                        ),
                    }
                    _upsert_scored(item)
            except Exception:
                pass

        if require_all_terms and scored:
            scored = {
                doc_id: item
                for doc_id, item in scored.items()
                if _has_full_term_coverage(list(item.get("matched_terms") or []))
            }

        ranked = sorted(scored.values(), key=lambda x: (-float(x.get("score") or 0.0), str(x.get("entity_name") or "")))
        matches = ranked[: max(1, int(limit))]
        if not matches:
            return None

        return {
            "criteria": criteria,
            "time_window": {
                "date_from": date_from,
                "date_to": date_to,
            }
            if (date_from or date_to)
            else None,
            "count": len(matches),
            "matches": matches,
            "source": "criteria_docs_sql",
        }

    def _criteria_terms(self, must_contain: list[str]) -> tuple[list[str], list[str]]:
        phrases: list[str] = []
        tokens: list[str] = []
        seen_phrases: set[str] = set()
        seen_tokens: set[str] = set()

        for phrase in must_contain:
            normalized_phrase = self._normalize_semantic_text(phrase)
            if normalized_phrase and normalized_phrase not in seen_phrases:
                seen_phrases.add(normalized_phrase)
                phrases.append(normalized_phrase)

            phrase_tokens = re.findall(r"[a-zA-ZäöüÄÖÜß0-9]{4,}", str(phrase or "").lower())
            for token in phrase_tokens:
                normalized_token = self._normalize_semantic_text(token)
                if normalized_token in self.SEMANTIC_STOPWORDS:
                    continue
                if normalized_token and normalized_token not in seen_tokens:
                    seen_tokens.add(normalized_token)
                    tokens.append(normalized_token)

            # Also split compound forms like "engineering-basierten" into searchable parts.
            for part in re.split(r"[-_/]+", str(phrase or "").lower()):
                cleaned_part = part.strip()
                if not cleaned_part:
                    continue
                for token in re.findall(r"[a-zA-ZäöüÄÖÜß0-9]{4,}", cleaned_part):
                    normalized_token = self._normalize_semantic_text(token)
                    if normalized_token in self.SEMANTIC_STOPWORDS:
                        continue
                    if normalized_token and normalized_token not in seen_tokens:
                        seen_tokens.add(normalized_token)
                        tokens.append(normalized_token)

        return phrases, tokens

    def _criteria_keyword_patterns(self, must_contain: list[str]) -> list[str]:
        patterns: list[str] = []
        seen: set[str] = set()
        phrases, tokens = self._criteria_terms(must_contain)
        for term in list(phrases) + list(tokens):
            normalized = self._normalize_semantic_text(term)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            patterns.append(f"%{normalized}%")
        return patterns

    def _search_by_criteria_semantic_fallback(
        self,
        question: str,
        criteria: dict[str, Any],
        scope_doc_ids: list[int],
        limit: int,
    ) -> dict[str, Any] | None:
        must_contain = [str(item).strip() for item in list(criteria.get("must_contain") or []) if str(item).strip()]
        if not must_contain:
            return None

        require_all_terms = bool(criteria.get("require_all_terms"))

        def _primary_term_matches(haystack: str, term: str) -> bool:
            return self._criteria_term_matches_haystack(term, haystack)

        def _all_primary_terms_match(haystack: str) -> bool:
            if not require_all_terms:
                return True
            return all(_primary_term_matches(haystack, term) for term in must_contain)

        phrases, tokens = self._criteria_terms(must_contain)
        entity_type = str(criteria.get("entity_type") or "").strip().lower()

        candidates = self.qdrant_candidates(question, limit=max(12, int(limit) * 6))
        if not candidates:
            return self._search_by_criteria_text_semantic_fallback(criteria, phrases, tokens, limit)

        doc_scores: dict[int, float] = {}
        doc_terms: dict[int, set[str]] = {}
        scope_set = set(scope_doc_ids or [])

        for item in candidates:
            doc_id = item.get("doc_id")
            if not isinstance(doc_id, int):
                continue
            if scope_set and doc_id not in scope_set:
                continue

            chunk_text = " ".join(
                [
                    str(item.get("text") or ""),
                    str(item.get("chunk_text") or ""),
                    str(item.get("doc_key") or ""),
                    str(item.get("document_type") or item.get("doc_type") or ""),
                ]
            )
            haystack = self._normalize_semantic_text(chunk_text)
            score = 0.0
            matched: set[str] = set()

            for phrase in phrases:
                if phrase and any(variant in haystack for variant in self._semantic_token_variants(phrase)):
                    score += 2.0
                    matched.add(phrase)

            for token in tokens:
                if token and any(variant in haystack for variant in self._semantic_token_variants(token)):
                    score += 1.0
                    matched.add(token)

            if score <= 0:
                continue
            if not _all_primary_terms_match(haystack):
                continue

            doc_scores[doc_id] = max(float(doc_scores.get(doc_id) or 0.0), score)
            existing_terms = doc_terms.get(doc_id) or set()
            existing_terms.update(matched)
            doc_terms[doc_id] = existing_terms

        if not doc_scores:
            return self._search_by_criteria_text_semantic_fallback(criteria, phrases, tokens, limit)

        ranked_doc_ids = [doc_id for doc_id, _ in sorted(doc_scores.items(), key=lambda x: -float(x[1]))][:100]
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            oi.object_id,
                            oi.object_name,
                            oi.class_name,
                            d.doc_id,
                            d.doc_name,
                            d.doc_path,
                            d.doc_date::text
                        FROM document d
                        JOIN part p ON p.doc_id = d.doc_id
                        JOIN object_instance oi ON oi.object_id = p.object_id
                        WHERE d.doc_id = ANY(%s)
                          AND COALESCE(d.status, 'active') = 'active'
                          AND d.valid_from <= CURRENT_DATE
                          AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                          AND COALESCE(oi.status, 'active') = 'active'
                          AND oi.valid_from <= CURRENT_DATE
                          AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                          AND (
                                %s = ''
                              OR LOWER(COALESCE(oi.class_name, '')) = %s
                                OR LOWER(COALESCE(oi.object_name, '')) LIKE %s
                          )
                        LIMIT 1000;
                        """,
                        (ranked_doc_ids, entity_type, entity_type, f"%{entity_type}%"),
                    )
                    rows = cur.fetchall()
        except Exception:
            return None

        if not rows:
            return None

        document_hint = str(criteria.get("document_hint") or "").strip().lower()
        is_document_oriented = document_hint in {"document", "note", "all"} or str(criteria.get("semantic_frame") or "").strip().lower() == "document_about_entity"

        by_bucket: dict[str, dict[str, Any]] = {}
        for row in rows:
            object_name = str(row[1] or "").strip()
            if not object_name:
                continue

            doc_id = int(row[3]) if isinstance(row[3], int) else None
            base_score = float(doc_scores.get(doc_id or -1) or 0.0)
            name_haystack = self._normalize_semantic_text(object_name)
            name_boost = 0.0
            for token in tokens:
                if token and any(variant in name_haystack for variant in self._semantic_token_variants(token)):
                    name_boost += 0.5

            total_score = base_score + name_boost
            if is_document_oriented and doc_id is not None:
                bucket_key = f"doc:{doc_id}"
            else:
                bucket_key = f"obj:{object_name}"

            display_name = str(row[4] or object_name or "").strip() if is_document_oriented else object_name
            current = by_bucket.get(bucket_key)
            candidate = {
                "object_id": row[0],
                "entity_name": display_name,
                "entity_type": "document" if is_document_oriented else row[2],
                "doc_id": row[3],
                "doc_name": row[4],
                "doc_path": row[5],
                "doc_date": row[6],
                "matched_terms": sorted(list(doc_terms.get(doc_id or -1) or set())),
                "score": total_score,
            }
            if current is None or float(candidate["score"]) > float(current.get("score") or 0.0):
                by_bucket[bucket_key] = candidate

        ranked = sorted(by_bucket.values(), key=lambda x: (-float(x.get("score") or 0.0), str(x.get("entity_name") or "")))
        matches = ranked[: max(1, int(limit))]
        if not matches:
            return None

        return {
            "criteria": criteria,
            "time_window": None,
            "count": len(matches),
            "matches": matches,
            "source": "criteria_semantic_qdrant",
        }

    def _search_by_criteria_text_semantic_fallback(
        self,
        criteria: dict[str, Any],
        phrases: list[str],
        tokens: list[str],
        limit: int,
    ) -> dict[str, Any] | None:
        must_contain = [str(item).strip() for item in list(criteria.get("must_contain") or []) if str(item).strip()]
        require_all_terms = bool(criteria.get("require_all_terms"))

        def _primary_term_matches(haystack: str, term: str) -> bool:
            return self._criteria_term_matches_haystack(term, haystack)

        def _all_primary_terms_match(haystack: str) -> bool:
            if not require_all_terms:
                return True
            return all(_primary_term_matches(haystack, term) for term in must_contain)

        entity_type = str(criteria.get("entity_type") or "").strip().lower()
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            oi.object_id,
                            oi.object_name,
                            oi.class_name,
                            oi.metadata,
                            a.attr_type,
                            a.attr_json,
                            d.doc_id,
                            d.doc_name,
                            d.doc_path,
                            d.doc_date::text
                        FROM object_instance oi
                        LEFT JOIN attribute a
                          ON a.src_type='object' AND a.src_id=oi.object_id
                        LEFT JOIN part p ON p.object_id=oi.object_id
                        LEFT JOIN document d ON d.doc_id=p.doc_id
                        WHERE (
                            %s = ''
                            OR LOWER(COALESCE(oi.class_name, '')) = %s
                            OR LOWER(COALESCE(oi.object_name, '')) LIKE %s
                        )
                          AND COALESCE(oi.status, 'active') = 'active'
                          AND oi.valid_from <= CURRENT_DATE
                          AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                        LIMIT 2500;
                        """,
                        (entity_type, entity_type, f"%{entity_type}%"),
                    )
                    rows = cur.fetchall()
        except Exception:
            return None

        scored: dict[str, dict[str, Any]] = {}
        for row in rows:
            object_name = str(row[1] or "").strip()
            if not object_name:
                continue

            blob = self._normalize_semantic_text(
                " ".join(
                    [
                        object_name,
                        str(row[3] or ""),
                        str(row[4] or ""),
                        str(row[5] or ""),
                    ]
                )
            )

            score = 0.0
            matched: set[str] = set()
            for phrase in phrases:
                if phrase and any(variant in blob for variant in self._semantic_token_variants(phrase)):
                    score += 2.0
                    matched.add(phrase)
            for token in tokens:
                if token and any(variant in blob for variant in self._semantic_token_variants(token)):
                    score += 1.0
                    matched.add(token)

            if score <= 0:
                continue
            if not _all_primary_terms_match(blob):
                continue

            item = {
                "object_id": row[0],
                "entity_name": object_name,
                "entity_type": row[2],
                "doc_id": row[6],
                "doc_name": row[7],
                "doc_path": row[8],
                "doc_date": row[9],
                "matched_terms": sorted(list(matched)),
                "score": score,
            }
            prev = scored.get(object_name)
            if prev is None or float(item["score"]) > float(prev.get("score") or 0.0):
                scored[object_name] = item

        ranked = sorted(scored.values(), key=lambda x: (-float(x.get("score") or 0.0), str(x.get("entity_name") or "")))
        matches = ranked[: max(1, int(limit))]
        if not matches:
            return None

        return {
            "criteria": criteria,
            "time_window": None,
            "count": len(matches),
            "matches": matches,
            "source": "criteria_semantic_text",
        }

    def _resolve_attribute_name_tiered(self, raw: str | None) -> str | None:
        """
        Tiered attribute name resolution for multi-language support.
        Tier 1: Dictionary alias (fast, deterministic)
        Tier 2: Vector embedding lookup (language-agnostic)
        Tier 3: Fallback to normalized form
        """
        if not raw:
            return None

        # Tier 1: Dictionary alias (current behavior)
        tier1_result = self._normalize_attribute_name(raw)
        if tier1_result and tier1_result in self._canonical_attribute_names():
            return tier1_result

        # Tier 2: Vector embedding lookup (multi-language support)
        if self.attr_embedding_index:
            embedding_result = self.attr_embedding_index.find_attribute(
                raw, confidence_threshold=0.65
            )
            if embedding_result:
                canonical_attr, _confidence = embedding_result
                return canonical_attr

        # Tier 3: Fallback to dictionary with any normalized match
        if tier1_result:
            return tier1_result

        return None

    @staticmethod
    def _normalize_alias_key(raw: str | None) -> str:
        normalized = re.sub(r"\s+", "_", str(raw or "").strip().lower())
        normalized = re.sub(r"[^a-z0-9_\-]", "", normalized)
        return normalized

    def _schema_alias_fallback_map(self) -> dict[str, str]:
        active_tenant_id = get_tenant_id()
        cache = self._schema_alias_caches.get(active_tenant_id, {})
        if cache:
            return cache

        self._refresh_schema_alias_cache()
        return self._schema_alias_caches.get(active_tenant_id, {})

    def _identifier_anchor_tokens_for_canonical(self, canonical_name: str) -> set[str]:
        """Return anchor tokens that make identifier mappings semantically safe."""
        name = str(canonical_name or "").strip().lower()
        if name == "eori_no":
            return {"eori"}
        if name == "registration_no":
            return {
                "registration", "register", "reg", "company", "firm", "firma",
                "unternehmens", "uid", "che", "tax",
                "vat", "mwst", "must", "ust", "iva", "tva",
            }
        if name == "uid_che":
            return {
                "registration", "register", "reg", "company", "firm", "firma",
                "unternehmens", "uid", "che", "tax",
                "vat", "mwst", "must", "ust", "iva", "tva",
            }
        if name == "tr_number":
            return {"trade", "register", "tr", "handelsregister"}
        return set()

    def _looks_like_identifier_request(self, raw: str | None) -> bool:
        text = str(raw or "").strip().lower()
        if not text:
            return False
        return bool(
            re.search(
                r"\b(?:number|nummer|no\.?|nr\.?|id|identifier|uid|vat|mwst|must|ust|tax|customer|iva|tva)\b",
                text,
                flags=re.IGNORECASE,
            )
        )

    def _raw_identifier_tokens(self, raw: str | None) -> set[str]:
        text = str(raw or "").strip().lower()
        tokens = {
            tok
            for tok in re.findall(r"[a-z0-9äöüß]{2,}", text)
            if tok not in {"number", "nummer", "no", "nr", "id", "identifier"}
        }
        return tokens

    def _is_unsafe_identifier_projection(self, raw: str | None, canonical_name: str, score: float | None) -> bool:
        """Block weak semantic jumps for unknown identifier terms.

        Example: "VAT-Number" should not silently map to "registration_no".
        """
        if not self._looks_like_identifier_request(raw):
            return False

        anchors = self._identifier_anchor_tokens_for_canonical(canonical_name)
        if not anchors:
            return False

        raw_tokens = self._raw_identifier_tokens(raw)
        if raw_tokens.intersection(anchors):
            return False

        # Accept only very high-confidence semantic mappings when no anchor overlaps.
        return float(score or 0.0) < 0.95

    def _query_has_identifier_anchor_for_attr(self, raw: str | None, canonical_name: str) -> bool:
        anchors = self._identifier_anchor_tokens_for_canonical(canonical_name)
        if not anchors:
            return True
        raw_tokens = self._raw_identifier_tokens(raw)
        return bool(raw_tokens.intersection(anchors))

    def _suggest_identifier_attribute_candidates(self, question: str, parsed: ParsedQuery) -> list[str]:
        """Suggest likely canonical identifier attributes for unresolved ID-like queries."""
        text = str(question or "").lower()
        tokens = self._raw_identifier_tokens(text)

        suggestions: list[str] = []
        if any(tok in tokens for tok in {"eori"}):
            suggestions.append("eori_no")
        if any(tok in tokens for tok in {"tax", "vat", "mwst", "must", "ust", "uid", "registration", "register", "reg", "che", "iva", "tva"}):
            suggestions.append("uid_che")
        if any(tok in tokens for tok in {"trade", "tr", "handelsregister"}):
            suggestions.append("tr_number")

        # Stable fallback shortlist for identifier requests.
        for item in ("eori_no", "uid_che", "tr_number"):
            if item not in suggestions:
                suggestions.append(item)
        return suggestions[:4]

    def _identifier_not_found_message(self, question: str, parsed: ParsedQuery) -> str:
        suggestions = self._suggest_identifier_attribute_candidates(question, parsed)
        suggestions_text = ", ".join(suggestions)
        attribute_label = str(parsed.attribute_name or "identifier").strip() or "identifier"
        entity_label = str(parsed.entity_name or "the entity").strip() or "the entity"
        if str(parsed.language or "en").lower() == "de":
            return (
                f"Ich konnte {attribute_label} für {entity_label} nicht finden. "
                f"Versuchen Sie bekannte Identifier-Felder wie: {suggestions_text}."
            )
        return (
            f"I could not find {attribute_label} for {entity_label}. "
            f"Try known identifier fields such as: {suggestions_text}."
        )

    def _refresh_schema_alias_cache(self) -> None:
        active_tenant_id = get_tenant_id()
        aliases: dict[str, str] = {}
        # Tier 1: static canonical alias vocabulary.
        if AttributeEmbeddingIndex is not None:
            try:
                canonical_terms = getattr(AttributeEmbeddingIndex, "CANONICAL_ATTRIBUTES", {})
                for canonical_name, terms in canonical_terms.items():
                    canonical_key = self._normalize_alias_key(canonical_name)
                    if canonical_key:
                        aliases[canonical_key] = canonical_name
                    for term in terms:
                        key = self._normalize_alias_key(term)
                        if key:
                            aliases[key] = canonical_name
            except Exception:
                pass

        # Tier 2: DB-driven alias overrides (preferred over static mapping).
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT alias_text, canonical_name
                        FROM query_term_alias
                                                WHERE tenant_id = %s::uuid AND is_active = true
                          AND kind IN ('attribute', 'relationship', 'entity_type', 'category')
                        ORDER BY priority ASC, alias_id ASC;
                                                """,
                                                (active_tenant_id,),
                    )
                    for row in cur.fetchall() or []:
                        alias_text = str(row[0] or "").strip()
                        canonical_name = str(row[1] or "").strip()
                        if not alias_text or not canonical_name:
                            continue
                        key = self._normalize_alias_key(alias_text)
                        if key:
                            aliases[key] = canonical_name
                        canonical_key = self._normalize_alias_key(canonical_name)
                        if canonical_key and canonical_key not in aliases:
                            aliases[canonical_key] = canonical_name
        except Exception:
            # Table may not exist yet in older deployments; keep static aliases.
            pass

        self._schema_alias_caches[active_tenant_id] = aliases
        # Retain compatibility for callers that inspect the old cache attribute.
        self._schema_alias_cache = aliases
        self._schema_alias_cache_tenant = active_tenant_id

    def _normalize_attribute_name(self, raw: str | None) -> str | None:
        if not raw:
            return None

        active_tenant_id = get_tenant_id()
        if active_tenant_id not in self._schema_alias_caches:
            self._refresh_schema_alias_cache()

        raw_text = str(raw or "").strip().lower()
        if (
            re.search(r"\b(?:phone|telephone|telefon|mobile|mobil|telecom|telecommunication)\b", raw_text)
            and re.search(r"\b(?:bill|invoice|rechnung|charge|charges|cost|amount|fee|total|price|payment|services?)\b", raw_text)
        ):
            # Billing phrasing should resolve to monetary value, not contact number.
            return "net_amount"

        normalized = self._normalize_alias_key(raw)
        schema_aliases = self._schema_alias_fallback_map()

        minimal_aliases = {
            "email": "email",
            "e_mail": "email",
            "e-mail": "email",
            "mail": "email",
            "email_adresse": "email",
            "e_mail_adresse": "email",
            "e-mail-adresse": "email",
            "eori": "eori_no",
            "eori_no": "eori_no",
            "eori_number": "eori_no",
            "eorinumber": "eori_no",
            "eori_nr": "eori_no",
            "eorinr": "eori_no",
            "eori_nummer": "eori_no",
            "uid": "uid_che",
            "uid_che": "uid_che",
            "che": "uid_che",
            "che_number": "uid_che",
            "company_uid": "uid_che",
            "company_registration_number": "uid_che",
            "registration_number": "uid_che",
            "registration_no": "uid_che",
            "company_number": "uid_che",
            "total_capital": "total_capital",
            "totalcapital": "total_capital",
            "share_capital": "total_capital",
            "stammkapital": "total_capital",
            "gesamtkapital": "total_capital",
        }

        # Guardrail: for identifier-like terms (X-number, X-no, X-id), avoid semantic
        # coercion unless there is an explicit lexical alias in the known schema.
        if (
            self._looks_like_identifier_request(raw)
            and normalized not in schema_aliases
            and normalized not in minimal_aliases
        ):
            return normalized or None

        # Prefer ingestion-backed schema index (lexical + vector) before hardcoded aliases.
        if self.attr_embedding_index and self._semantic_attribute_search_allowed(raw):
            try:
                schema_match = self.attr_embedding_index.find_schema_term(
                    raw,
                    confidence_threshold=0.65,
                    kinds={"attribute"},
                )
                canonical = str((schema_match or {}).get("canonical_name") or "").strip()
                score = float((schema_match or {}).get("score") or 0.0)
                if canonical and not self._is_unsafe_identifier_projection(raw, canonical, score):
                    return canonical
            except Exception:
                pass

        # Tiered static fallback: schema index vocabulary first, then minimal safety aliases.
        if normalized in schema_aliases:
            return schema_aliases[normalized]
        return minimal_aliases.get(normalized, normalized or None)

    def _key_variants(self, key: str) -> list[str]:
        spaced = key.replace("_", " ")
        return [
            key,
            key.lower(),
            key.upper(),
            key.title(),
            spaced,
            spaced.title(),
        ]

    def _extract_attr_value(self, payload: Any, attribute_name: str) -> Any:
        if not isinstance(payload, dict):
            return None
        for variant in self._key_variants(attribute_name):
            if variant in payload and payload[variant] not in (None, ""):
                return payload[variant]
        return None

    def _extract_attribute_value_from_related_payload(
        self,
        payload: Any,
        attribute_name: str,
        relation_name: str | None = None,
    ) -> Any:
        attr = str(attribute_name or "").strip()
        if not attr:
            return None

        variants = []
        direct_variants = [attr, self._normalize_attribute_name(attr)]
        for item in direct_variants:
            if item and item not in variants:
                variants.append(item)

        relation_like_attrs = {"contact_person", "contact_person_name", "person_name", "contact_name", "name"}
        if attr in relation_like_attrs or str(relation_name or "").strip().lower() in relation_like_attrs:
            for item in self._candidate_source_row_keys(attr, relation_name):
                if item and item not in variants:
                    variants.append(item)
        else:
            for item in self._candidate_source_row_keys(attr):
                if item and item not in variants:
                    variants.append(item)

        if attr in relation_like_attrs:
            for item in ["name", "contact_person", "contact_name", "person_name"]:
                if item and item not in variants:
                    variants.append(item)

        if isinstance(payload, dict):
            for variant in variants:
                if variant in payload and payload[variant] not in (None, ""):
                    return payload[variant]
            if "attribute_value" in payload:
                value = self._extract_attribute_value_from_related_payload(
                    payload.get("attribute_value"),
                    attr,
                    relation_name,
                )
                if value not in (None, "", [], {}):
                    return value
            if "name" in payload and payload["name"] not in (None, ""):
                return payload["name"]
            return None

        if isinstance(payload, list):
            extracted_values: list[Any] = []
            for item in payload:
                if isinstance(item, dict):
                    value = self._extract_attribute_value_from_related_payload(item, attr, relation_name)
                    if value is None:
                        continue
                    if isinstance(value, list):
                        extracted_values.extend(value)
                    else:
                        extracted_values.append(value)
            if extracted_values:
                deduped: list[Any] = []
                for value in extracted_values:
                    text = str(value or "").strip()
                    if text and text not in {str(item or "").strip() for item in deduped if isinstance(item, str)}:
                        deduped.append(value)
                return deduped if len(deduped) > 1 else deduped[0] if deduped else None
            return None

        return None

    @staticmethod
    def _is_placeholder_identifier_value(value: Any) -> bool:
        text = str(value or "").strip().lower()
        if not text:
            return True
        if text in {"none", "null", "n/a", "na", "-", "--"}:
            return True
        return bool(re.fullmatch(r"0+(?:[.]0+)?", text))

    def _structured_table_term_forms(self, value: Any) -> list[str]:
        text = str(value or "").strip()
        if not text:
            return []

        candidates = [
            text,
            text.replace("_", " "),
            self._normalize_semantic_text(text),
            self._normalize_semantic_text(text.replace("_", " ")),
            self._normalize_attribute_name(text) or "",
        ]
        out: list[str] = []
        for candidate in candidates:
            normalized = str(candidate or "").strip().lower()
            if normalized and normalized not in out:
                out.append(normalized)
        return out

    def _structured_table_text(self, payload: Any) -> str:
        try:
            return self._normalize_semantic_text(json.dumps(payload or {}, ensure_ascii=False))
        except Exception:
            return self._normalize_semantic_text(str(payload or ""))

    def _record_matches_structured_table_entity(self, record: dict[str, Any], entity_name: str) -> bool:
        if not entity_name:
            return True

        row_text = self._structured_table_text(record)
        for term in self._structured_table_term_forms(entity_name):
            if term and term in row_text:
                return True
        return False

    def _record_matches_structured_table_attribute(
        self,
        record: dict[str, Any],
        attribute_candidates: list[str],
    ) -> tuple[str | None, Any | None]:
        if not isinstance(record, dict):
            return None, None

        monetary_request = any(
            str(self._normalize_attribute_name(candidate) or "").strip().lower()
            in {"amount", "net_amount", "gross_amount", "tax_amount", "total_amount", "price", "cost", "value"}
            for candidate in attribute_candidates
        )

        if monetary_request:
            preferred_monetary_keys = {
                "amount",
                "net_amount",
                "gross_amount",
                "tax_amount",
                "total_amount",
                "total_amount_chf",
                "amount_chf",
                "amount_source_currency",
                "unit_price",
                "unit_price_chf",
                "unit_price_cny",
                "price",
                "cost",
                "value",
            }
            normalized_record = {
                self._normalize_attribute_name(key): (key, value)
                for key, value in record.items()
                if str(key).strip()
            }
            for preferred_key in preferred_monetary_keys:
                if preferred_key in normalized_record:
                    original_key, value = normalized_record[preferred_key]
                    if value not in (None, ""):
                        return original_key, value

        for candidate in attribute_candidates:
            for variant in self._key_variants(candidate):
                if variant in record and record[variant] not in (None, ""):
                    return variant, record[variant]

        normalized_record = {self._normalize_attribute_name(key): value for key, value in record.items() if str(key).strip()}
        for candidate in attribute_candidates:
            normalized_candidate = self._normalize_attribute_name(candidate)
            if normalized_candidate and normalized_candidate in normalized_record and normalized_record[normalized_candidate] not in (None, ""):
                return normalized_candidate, normalized_record[normalized_candidate]

        return None, None

    def _attribute_filters_from_criteria(self, criteria: dict[str, Any] | None) -> list[dict[str, Any]]:
        if not isinstance(criteria, dict):
            return []
        llm_plan = criteria.get("llm_query_plan") if isinstance(criteria.get("llm_query_plan"), dict) else {}
        filters = llm_plan.get("filters") if isinstance(llm_plan.get("filters"), dict) else {}
        raw_filters = filters.get("attribute_filters") if isinstance(filters.get("attribute_filters"), list) else []

        parsed_filters: list[dict[str, Any]] = []
        for item in raw_filters:
            if not isinstance(item, dict):
                continue
            attr_name = str(item.get("attribute") or "").strip()
            operator = str(item.get("operator") or "eq").strip().lower() or "eq"
            value = item.get("value")
            if not attr_name or value in (None, ""):
                continue
            parsed_filters.append(
                {
                    "attribute": attr_name,
                    "operator": operator,
                    "value": value,
                }
            )
        return parsed_filters

    def _structured_table_record_matches_attribute_filters(
        self,
        record: dict[str, Any],
        metadata: dict[str, Any],
        attribute_filters: list[dict[str, Any]],
    ) -> bool:
        if not attribute_filters:
            return True

        record_text = self._structured_table_text(record)
        metadata_text = self._structured_table_text(metadata)

        for item in attribute_filters:
            attr_name = str(item.get("attribute") or "").strip()
            operator = str(item.get("operator") or "eq").strip().lower() or "eq"
            expected_value = item.get("value")
            if not attr_name or expected_value in (None, ""):
                continue

            expected_text = str(expected_value).strip()
            expected_forms = self._structured_table_term_forms(expected_text)
            filter_candidates = self._expand_query_attribute_candidates(attr_name, None)
            if attr_name not in filter_candidates:
                filter_candidates.insert(0, attr_name)

            matched_key, matched_value = self._record_matches_structured_table_attribute(record, filter_candidates)
            if matched_value not in (None, ""):
                actual_text = str(matched_value).strip().lower()
                if operator == "eq":
                    if not any(form == actual_text for form in expected_forms):
                        return False
                else:
                    if not any(form in actual_text for form in expected_forms):
                        return False
                continue

            if operator == "eq":
                if not any(form and (form in record_text or form in metadata_text) for form in expected_forms):
                    return False
            else:
                if not any(form and (form in record_text or form in metadata_text) for form in expected_forms):
                    return False

        return True

    def _lookup_attribute_in_structured_tables(
        self,
        conn,
        entity_name: str,
        attribute_name: str,
        relation_name: str | None = None,
        scope_doc_ids: list[int] | None = None,
        attribute_filters: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        if not entity_name or not attribute_name:
            return None

        attribute_candidates = self._expand_query_attribute_candidates(attribute_name, entity_name, relation_name)
        if attribute_name not in attribute_candidates:
            attribute_candidates.insert(0, attribute_name)

        doc_rows: list[tuple[Any, ...]] = []
        try:
            with conn.cursor() as cur:
                params: list[Any] = []
                sql = """
                SELECT doc_id, doc_name, doc_path, metadata
                FROM document
                WHERE COALESCE(status, 'active') = 'active'
                  AND metadata ? 'structured_tables'
                """
                if scope_doc_ids:
                    sql += " AND doc_id = ANY(%s)"
                    params.append(scope_doc_ids)
                sql += " ORDER BY entry_date DESC LIMIT 200"
                cur.execute(sql, tuple(params))
                doc_rows = cur.fetchall()
        except Exception:
            return None

        entity_forms = self._structured_table_term_forms(entity_name)
        relation_forms = self._structured_table_term_forms(relation_name) if relation_name else []
        attribute_forms = self._structured_table_term_forms(attribute_name)
        active_attribute_filters = list(attribute_filters or [])

        for row in doc_rows:
            metadata = row[3] if len(row) > 3 and isinstance(row[3], dict) else {}
            structured = metadata.get("structured_tables") if isinstance(metadata.get("structured_tables"), dict) else {}
            tables = structured.get("tables") if isinstance(structured.get("tables"), list) else []
            doc_text = self._structured_table_text(metadata)
            doc_text_matches = any(term in doc_text for term in entity_forms + relation_forms + attribute_forms)

            for table in tables:
                if not isinstance(table, dict):
                    continue
                records = table.get("records") if isinstance(table.get("records"), list) else []
                if not records:
                    continue

                for record in records:
                    if not isinstance(record, dict):
                        continue
                    if not self._record_matches_structured_table_entity(record, entity_name):
                        if not doc_text_matches:
                            continue
                    if not self._structured_table_record_matches_attribute_filters(record, metadata, active_attribute_filters):
                        continue

                    matched_key, matched_value = self._record_matches_structured_table_attribute(record, attribute_candidates)
                    if matched_value in (None, ""):
                        continue

                    result = {
                        "object_id": None,
                        "entity_name": entity_name,
                        "entity_type": str(table.get("table_kind") or table.get("source_kind") or "table").strip(),
                        "attribute_name": matched_key or attribute_name,
                        "attribute_value": matched_value,
                        "doc_id": row[0] if len(row) > 0 else None,
                        "doc_name": row[1] if len(row) > 1 else None,
                        "doc_path": row[2] if len(row) > 2 else None,
                        "doc_date": None,
                    }
                    if isinstance(table.get("mapped_to"), str):
                        result["table_mapped_to"] = table.get("mapped_to")
                    if relation_name:
                        result["relationship_name"] = relation_name
                    return result

        return None

    def _result_satisfies_attribute_filters(
        self,
        conn,
        result: dict[str, Any] | None,
        attribute_filters: list[dict[str, Any]] | None,
    ) -> bool:
        """Validate that a resolved attribute result satisfies all planner attribute filters.

        Evidence sources (in order):
        1) direct result attribute/value when filter targets same attribute,
        2) document structured table records for result.doc_id,
        3) relaxed textual evidence over result + metadata.
        """
        active_filters = list(attribute_filters or [])
        if not active_filters:
            return True
        if not isinstance(result, dict) or not result:
            return False

        # Fast-path: explicit result attribute equals filter attribute and value satisfies filter.
        result_attr = str(result.get("attribute_name") or "").strip()
        result_val = result.get("attribute_value")
        if result_attr and result_val not in (None, ""):
            record = {result_attr: result_val}
            if self._structured_table_record_matches_attribute_filters(record, result, active_filters):
                return True

        # Try document-level structured evidence when provenance is available.
        metadata: dict[str, Any] = {}
        doc_id = result.get("doc_id")
        if isinstance(doc_id, int):
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT metadata
                        FROM document
                        WHERE doc_id = %s
                          AND COALESCE(status, 'active') = 'active'
                          AND valid_from <= CURRENT_DATE
                          AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                        LIMIT 1
                        """,
                        (doc_id,),
                    )
                    row = cur.fetchone()
                if row and isinstance(row[0], dict):
                    metadata = row[0]
            except Exception:
                metadata = {}

        structured = metadata.get("structured_tables") if isinstance(metadata.get("structured_tables"), dict) else {}
        tables = structured.get("tables") if isinstance(structured.get("tables"), list) else []
        for table in tables:
            if not isinstance(table, dict):
                continue
            records = table.get("records") if isinstance(table.get("records"), list) else []
            for record in records:
                if not isinstance(record, dict):
                    continue
                if self._structured_table_record_matches_attribute_filters(record, metadata, active_filters):
                    return True

        # Last resort: textual evidence over result + metadata.
        haystack = self._structured_table_text({"result": result, "metadata": metadata})
        if not haystack:
            return False
        for item in active_filters:
            expected_forms = self._structured_table_term_forms(item.get("value"))
            if not any(form and form in haystack for form in expected_forms):
                return False
        return True

    def _sql_shareholder_lookup(self, conn, entity_name: str) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    company.object_name AS company_name,
                    person.object_name AS shareholder_name,
                    rel.relationship_name,
                    COALESCE(MAX(CASE WHEN attr.attr_type = 'share_count' THEN attr.attr_json->>'value' END), '') AS share_count,
                    COALESCE(MAX(CASE WHEN attr.attr_type = 'share_total_nominal_value_chf' THEN attr.attr_json->>'value' END), '') AS share_total_nominal_value_chf,
                    COALESCE(MAX(CASE WHEN attr.attr_type = 'role' THEN attr.attr_json->>'value' END), '') AS role,
                    COALESCE(MAX(doc.doc_id), 0) AS doc_id,
                    COALESCE(MAX(doc.doc_key), '') AS doc_key,
                    COALESCE(MAX(doc.doc_path), '') AS doc_path
                FROM object_instance company
                JOIN object_relationship rel
                  ON rel.tar_object_id = company.object_id
                JOIN object_instance person
                  ON person.object_id = rel.src_object_id
                LEFT JOIN attribute attr
                  ON attr.src_type = 'relationship' AND attr.src_id = rel.relationship_id
                                LEFT JOIN part part_rel
                                    ON part_rel.relationship_id = rel.relationship_id
                LEFT JOIN document doc
                  ON doc.doc_id = part_rel.doc_id
                WHERE LOWER(company.object_name) = LOWER(%s)
                                    AND COALESCE(company.status, 'active') = 'active'
                                    AND company.valid_from <= CURRENT_DATE
                                    AND (company.valid_until IS NULL OR company.valid_until > CURRENT_DATE)
                                    AND COALESCE(person.status, 'active') = 'active'
                                    AND person.valid_from <= CURRENT_DATE
                                    AND (person.valid_until IS NULL OR person.valid_until > CURRENT_DATE)
                  AND LOWER(rel.relationship_name) LIKE '%%shareholder%%'
                                    AND rel.valid_from <= CURRENT_DATE
                                    AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                GROUP BY company.object_name, person.object_name, rel.relationship_name
                ORDER BY person.object_name
                """,
                (entity_name,),
            )
            rows = cur.fetchall()

        if not rows:
            return None

        shareholders: list[dict[str, Any]] = []
        company_name = rows[0][0]
        doc_id = rows[0][6] or None
        doc_key = rows[0][7] or None
        doc_path = rows[0][8] or None

        for row in rows:
            share_count = row[3]
            share_total = row[4]
            role = row[5]
            shareholders.append(
                {
                    "name": row[1],
                    "relationship_name": row[2],
                    "share_count": int(share_count) if str(share_count).strip() else None,
                    "share_total_nominal_value_chf": float(share_total) if str(share_total).strip() else None,
                    "role": role or None,
                }
            )

        return {
            "entity_name": company_name,
            "attribute_name": "shareholders",
            "attribute_value": shareholders,
            "doc_id": doc_id,
            "doc_name": doc_key,
            "doc_path": doc_path,
        }

    def _sql_companies_held_by_person_lookup(self, conn, person_name: str) -> dict[str, Any] | None:
        """Return companies where a person is shareholder/owner by relationship evidence."""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    person.object_name AS person_name,
                    company.object_name AS company_name,
                    rel.relationship_name,
                    COALESCE(MAX(CASE WHEN attr.attr_type = 'share_count' THEN attr.attr_json->>'value' END), '') AS share_count,
                    COALESCE(MAX(CASE WHEN attr.attr_type = 'share_total_nominal_value_chf' THEN attr.attr_json->>'value' END), '') AS share_total_nominal_value_chf,
                    COALESCE(MAX(CASE WHEN attr.attr_type = 'role' THEN attr.attr_json->>'value' END), '') AS role,
                    COALESCE(MAX(doc.doc_id), 0) AS doc_id,
                    COALESCE(MAX(doc.doc_key), '') AS doc_key,
                    COALESCE(MAX(doc.doc_path), '') AS doc_path
                FROM object_instance person
                JOIN object_relationship rel
                  ON rel.src_object_id = person.object_id
                JOIN object_instance company
                  ON company.object_id = rel.tar_object_id
                LEFT JOIN attribute attr
                  ON attr.src_type = 'relationship' AND attr.src_id = rel.relationship_id
                                LEFT JOIN part part_rel
                                    ON part_rel.relationship_id = rel.relationship_id
                LEFT JOIN document doc
                  ON doc.doc_id = part_rel.doc_id
                WHERE LOWER(person.object_name) = LOWER(%s)
                  AND COALESCE(person.status, 'active') = 'active'
                  AND person.valid_from <= CURRENT_DATE
                  AND (person.valid_until IS NULL OR person.valid_until > CURRENT_DATE)
                  AND COALESCE(company.status, 'active') = 'active'
                  AND company.valid_from <= CURRENT_DATE
                  AND (company.valid_until IS NULL OR company.valid_until > CURRENT_DATE)
                  AND (
                        LOWER(rel.relationship_name) LIKE '%%shareholder%%'
                     OR LOWER(rel.relationship_name) LIKE '%%owner%%'
                     OR LOWER(rel.relationship_name) LIKE '%%shareholder_and_president%%'
                  )
                  AND rel.valid_from <= CURRENT_DATE
                  AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                GROUP BY person.object_name, company.object_name, rel.relationship_name
                ORDER BY company.object_name
                """,
                (person_name,),
            )
            rows = cur.fetchall()

        if not rows:
            return None

        person_label = rows[0][0]
        doc_id = rows[0][6] or None
        doc_key = rows[0][7] or None
        doc_path = rows[0][8] or None
        companies: list[dict[str, Any]] = []
        for row in rows:
            share_count = row[3]
            share_total = row[4]
            role = row[5]
            companies.append(
                {
                    "name": row[1],
                    "relationship_name": row[2],
                    "share_count": int(share_count) if str(share_count).strip() else None,
                    "share_total_nominal_value_chf": float(share_total) if str(share_total).strip() else None,
                    "role": role or None,
                }
            )

        return {
            "entity_name": person_label,
            "attribute_name": "shareholders",
            "attribute_value": companies,
            "doc_id": doc_id,
            "doc_name": doc_key,
            "doc_path": doc_path,
        }

    def _sql_share_count_for_person_company(self, conn, person_name: str, company_name: str) -> dict[str, Any] | None:
        """Resolve share_count for a specific person/company pair."""
        resolved_person = self._resolve_entity_name_for_lookup(conn, person_name)
        resolved_company = self._resolve_entity_name_for_lookup(conn, company_name)
        if not resolved_person or not resolved_company:
            return None

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    person.object_name AS person_name,
                    company.object_name AS company_name,
                    rel.relationship_name,
                    COALESCE(MAX(CASE WHEN attr.attr_type = 'share_count' THEN attr.attr_json->>'value' END), '') AS share_count,
                    rel.metadata,
                    COALESCE(MAX(doc.doc_id), 0) AS doc_id,
                    COALESCE(MAX(doc.doc_key), '') AS doc_key,
                    COALESCE(MAX(doc.doc_path), '') AS doc_path
                FROM object_relationship rel
                JOIN object_instance person ON person.object_id = rel.src_object_id
                JOIN object_instance company ON company.object_id = rel.tar_object_id
                LEFT JOIN attribute attr ON attr.src_type = 'relationship' AND attr.src_id = rel.relationship_id
                  AND attr.valid_from <= CURRENT_DATE
                  AND (attr.valid_until IS NULL OR attr.valid_until > CURRENT_DATE)
                LEFT JOIN part part_rel ON part_rel.relationship_id = rel.relationship_id
                LEFT JOIN document doc ON doc.doc_id = part_rel.doc_id
                WHERE LOWER(person.object_name) = LOWER(%s)
                  AND LOWER(company.object_name) = LOWER(%s)
                  AND LOWER(rel.relationship_name) LIKE '%%shareholder%%'
                  AND COALESCE(person.status, 'active') = 'active'
                  AND person.valid_from <= CURRENT_DATE
                  AND (person.valid_until IS NULL OR person.valid_until > CURRENT_DATE)
                  AND COALESCE(company.status, 'active') = 'active'
                  AND company.valid_from <= CURRENT_DATE
                  AND (company.valid_until IS NULL OR company.valid_until > CURRENT_DATE)
                  AND rel.valid_from <= CURRENT_DATE
                  AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                GROUP BY person.object_name, company.object_name, rel.relationship_name, rel.metadata, rel.relationship_id
                ORDER BY COALESCE(MAX(doc.doc_id), 0) DESC, rel.relationship_id DESC
                LIMIT 5
                """,
                (resolved_person, resolved_company),
            )
            rows = cur.fetchall()

        if not rows:
            return None

        chosen = rows[0]
        metadata = chosen[4] if isinstance(chosen[4], dict) else {}
        share_count_raw = str(chosen[3] or "").strip() or str(metadata.get("share_count") or "").strip()
        share_count: int | None = None
        if share_count_raw:
            try:
                share_count = int(float(share_count_raw))
            except Exception:
                share_count = None

        return {
            "entity_name": chosen[0],
            "related_entity_name": chosen[1],
            "relationship_name": chosen[2],
            "attribute_name": "share_count",
            "attribute_value": share_count,
            "doc_id": chosen[5] or None,
            "doc_name": chosen[6] or None,
            "doc_path": chosen[7] or None,
        }

    @staticmethod
    def _is_ownership_lookup_intent(
        raw_attr_key: str,
        normalized_attr: str | None,
        relation_name: str | None,
        resolved_relationship: str | None,
    ) -> bool:
        signal_text = " ".join(
            [
                str(raw_attr_key or "").strip().lower(),
                str(normalized_attr or "").strip().lower(),
                str(relation_name or "").strip().lower(),
                str(resolved_relationship or "").strip().lower(),
            ]
        )
        if not signal_text.strip():
            return False
        return bool(
            re.search(
                r"\b(?:own|owns|owned|owner|owners|ownership|shareholder|shareholders|beneficial[_\s-]?owner|aktion[äa]r|gesellschafter|anteil(?:seigner|inhaber)?)\b",
                signal_text,
                flags=re.IGNORECASE,
            )
        )

    def _sql_governance_roles_lookup(self, conn, entity_name: str) -> dict[str, Any] | None:
        """Fallback for ownership-style asks when explicit ownership is absent.

        Returns only governance-level person roles (director/member/board/executive/signatory)
        and intentionally excludes generic staff/customer/vendor style relations.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    anchor.object_name AS company_name,
                    related.object_name AS person_name,
                    rel.relationship_name,
                    rel.metadata,
                    COALESCE(MAX(doc.doc_id), 0) AS doc_id,
                    COALESCE(MAX(doc.doc_key), '') AS doc_key,
                    COALESCE(MAX(doc.doc_path), '') AS doc_path
                FROM object_instance anchor
                JOIN object_relationship rel
                  ON rel.src_object_id = anchor.object_id
                  OR rel.tar_object_id = anchor.object_id
                JOIN object_instance related
                  ON related.object_id = CASE
                        WHEN rel.src_object_id = anchor.object_id THEN rel.tar_object_id
                        ELSE rel.src_object_id
                     END
                LEFT JOIN part part_rel ON part_rel.relationship_id = rel.relationship_id
                LEFT JOIN document doc ON doc.doc_id = part_rel.doc_id
                WHERE LOWER(anchor.object_name) = LOWER(%s)
                  AND LOWER(COALESCE(related.class_name, '')) = 'person'
                  AND COALESCE(anchor.status, 'active') = 'active'
                  AND anchor.valid_from <= CURRENT_DATE
                  AND (anchor.valid_until IS NULL OR anchor.valid_until > CURRENT_DATE)
                  AND COALESCE(related.status, 'active') = 'active'
                  AND related.valid_from <= CURRENT_DATE
                  AND (related.valid_until IS NULL OR related.valid_until > CURRENT_DATE)
                  AND rel.valid_from <= CURRENT_DATE
                  AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                GROUP BY anchor.object_name, related.object_name, rel.relationship_name, rel.metadata
                ORDER BY related.object_name
                """,
                (entity_name,),
            )
            rows = cur.fetchall()

        if not rows:
            return None

        strict_rel_allow = {
            "is_director_of",
            "is_member_of",
            "board_member_of",
            "is_board_member_of",
            "is_chair_of",
            "is_president_of",
            "is_ceo_of",
            "is_cfo_of",
            "is_coo_of",
            "managing_director_of",
            "authorized_signatory_of",
            "holds_position_in",
        }
        governance_role_markers = {
            "mitglied",
            "direktor",
            "direktorin",
            "geschäftsführer",
            "geschaeftsfuehrer",
            "chair",
            "president",
            "vorstand",
            "board",
            "executive",
            "ceo",
            "cfo",
            "coo",
            "einzelunterschrift",
            "kollektivunterschrift",
            "signatory",
        }

        deduped_entries: dict[str, dict[str, Any]] = {}
        for row in rows:
            rel_name = str(row[2] or "").strip()
            rel_norm = rel_name.lower()
            metadata = row[3] if isinstance(row[3], dict) else {}
            role_text = str(
                metadata.get("role")
                or metadata.get("position")
                or ""
            ).strip()
            signing_text = str(metadata.get("signing_authority") or "").strip()
            role_haystack = f"{role_text} {signing_text}".lower()

            rel_allowed = rel_norm in strict_rel_allow
            role_allowed = any(marker in role_haystack for marker in governance_role_markers)
            if not rel_allowed and not role_allowed:
                continue

            person_name = str(row[1] or "").strip()
            if not person_name:
                continue

            entry = {
                "name": person_name,
                "role": role_text or None,
                "signing_authority": signing_text or None,
                "relationship_name": rel_name,
            }
            dedupe_key = "|".join(
                [
                    person_name.lower(),
                    str(role_text or "").strip().lower(),
                    str(signing_text or "").strip().lower(),
                ]
            )
            existing = deduped_entries.get(dedupe_key)
            if not existing:
                deduped_entries[dedupe_key] = entry
                continue
            existing_rel = str(existing.get("relationship_name") or "").strip().lower()
            if existing_rel == "holds_position_in" and rel_norm != "holds_position_in":
                deduped_entries[dedupe_key] = entry

        entries = sorted(deduped_entries.values(), key=lambda item: str(item.get("name") or "").lower())
        if not entries:
            return None

        company_name = str(rows[0][0] or entity_name).strip() or entity_name
        return {
            "entity_name": company_name,
            "attribute_name": "governance_roles",
            "attribute_value": entries,
            "explicit_ownership_found": False,
            "doc_id": rows[0][4] or None,
            "doc_name": rows[0][5] or None,
            "doc_path": rows[0][6] or None,
        }


    def _sql_generic_relationship_lookup(
        self,
        conn,
        entity_name: str,
        relationship_concept: str,
        broad_scope: bool = False,
    ) -> dict[str, Any] | None:
        """Generic relationship lookup: find all entities related to entity_name through
        any relationship whose name contains the concept keyword (e.g. 'shareholder',
        'director', 'eori', 'employee', 'subsidiary').

        This is the scalable replacement for hard-coded relationship lookups.  Any
        relationship stored in object_relationship is automatically searchable.
        """
        concept = str(relationship_concept or "").strip().lower()
        if not concept:
            return None

        # Try exact concept and a stripped-trailing-s form so "shareholders" also matches
        # relationships named "shareholder" or "shareholder_and_president".
        concepts = [concept]
        if concept.endswith("s") and len(concept) > 3:
            concepts.append(concept[:-1])
        # Also try underscore-normalised form
        concepts.append(concept.replace(" ", "_").replace("-", "_"))

        seen_patterns: set[str] = set()
        patterns = []
        for c in concepts:
            p = f"%{c}%"
            if p not in seen_patterns:
                seen_patterns.add(p)
                patterns.append(p)

        rows: list[tuple] = []
        for concept_pattern in patterns:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        anchor.object_name            AS anchor_name,
                        related.object_id             AS related_object_id,
                        related.object_name           AS related_name,
                        related.class_name            AS related_class,
                        rel.relationship_name,
                        COALESCE(MAX(CASE WHEN attr.attr_type = 'share_count'
                                         THEN attr.attr_json->>'value' END), '') AS share_count,
                        COALESCE(MAX(CASE WHEN attr.attr_type = 'share_total_nominal_value_chf'
                                         THEN attr.attr_json->>'value' END), '') AS share_nominal_chf,
                        COALESCE(MAX(CASE WHEN attr.attr_type = 'role'
                                         THEN attr.attr_json->>'value' END), '') AS role,
                        COALESCE(MAX(CASE WHEN LOWER(obj_attr.attr_type) = 'email'
                                         THEN obj_attr.attr_json->>'value' END), '') AS contact_email,
                        COALESCE(MAX(CASE WHEN LOWER(obj_attr.attr_type) IN ('phone', 'telephone', 'tel')
                                         THEN obj_attr.attr_json->>'value' END), '') AS contact_phone,
                        COALESCE(MAX(CASE WHEN LOWER(obj_attr.attr_type) IN ('mobile', 'mobile_phone', 'cell', 'handy')
                                         THEN obj_attr.attr_json->>'value' END), '') AS contact_mobile,
                        COALESCE(MAX(doc.doc_id), 0)  AS doc_id,
                        COALESCE(MAX(doc.doc_key), '') AS doc_key,
                        COALESCE(MAX(doc.doc_path), '') AS doc_path
                    FROM object_instance anchor
                    JOIN object_relationship rel
                      ON rel.src_object_id = anchor.object_id
                      OR rel.tar_object_id = anchor.object_id
                    JOIN object_instance related
                      ON related.object_id = CASE
                            WHEN rel.src_object_id = anchor.object_id THEN rel.tar_object_id
                            ELSE rel.src_object_id
                         END
                    LEFT JOIN attribute attr
                      ON attr.src_type = 'relationship' AND attr.src_id = rel.relationship_id
                                        LEFT JOIN attribute obj_attr
                                            ON obj_attr.src_type = 'object'
                                         AND obj_attr.src_id = related.object_id
                                         AND obj_attr.valid_from <= CURRENT_DATE
                                         AND (obj_attr.valid_until IS NULL OR obj_attr.valid_until > CURRENT_DATE)
                    LEFT JOIN part part_rel ON part_rel.relationship_id = rel.relationship_id
                    LEFT JOIN document doc   ON doc.doc_id = part_rel.doc_id
                    WHERE LOWER(anchor.object_name) = LOWER(%s)
                      AND COALESCE(anchor.status, 'active') = 'active'
                      AND anchor.valid_from <= CURRENT_DATE
                      AND (anchor.valid_until IS NULL OR anchor.valid_until > CURRENT_DATE)
                      AND COALESCE(related.status, 'active') = 'active'
                      AND related.valid_from <= CURRENT_DATE
                      AND (related.valid_until IS NULL OR related.valid_until > CURRENT_DATE)
                      AND rel.valid_from <= CURRENT_DATE
                      AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                      AND LOWER(rel.relationship_name) LIKE %s
                    GROUP BY anchor.object_name, related.object_id, related.object_name, related.class_name, rel.relationship_name
                    ORDER BY related.object_name;
                    """,
                    (entity_name, concept_pattern),
                )
                rows.extend(cur.fetchall())
            if rows:
                break  # found results with this pattern; no need to try weaker forms

        if not rows:
            return None

        anchor_name = rows[0][0]
        doc_id = rows[0][11] or None
        doc_key = rows[0][12] or None
        doc_path = rows[0][13] or None
        related_attr_map: dict[int, dict[str, Any]] = {}
        if broad_scope:
            related_ids = [int(row[1]) for row in rows if isinstance(row[1], int)]
            related_attr_map = self._related_object_attribute_map(conn, related_ids)

        # Deduplicate by normalised name tokens; keep max share_count per person.
        seen: dict[str, dict[str, Any]] = {}
        for row in rows:
            name = str(row[2] or "").strip()
            if not name:
                continue
            norm = " ".join(sorted(re.findall(r"[a-zA-ZäöüÄÖÜß0-9]+", name.lower())))
            sc_raw = str(row[5] or "").strip()
            try:
                sc = int(sc_raw) if sc_raw else None
            except Exception:
                sc = None
            nominal_raw = str(row[6] or "").strip()
            try:
                nominal = float(nominal_raw) if nominal_raw else None
            except Exception:
                nominal = None
            role = str(row[7] or "").strip() or None
            contact_email = str(row[8] or "").strip() or None
            contact_phone = str(row[9] or "").strip() or None
            contact_mobile = str(row[10] or "").strip() or None

            entry = seen.get(norm)
            if entry is None:
                candidate_entry: dict[str, Any] = {
                    "name": name,
                    "relationship_name": row[4],
                    "class_name": row[3] or None,
                    "share_count": sc,
                    "share_nominal_chf": nominal,
                    "role": role,
                }
                if contact_email:
                    candidate_entry["email"] = contact_email
                if contact_phone:
                    candidate_entry["phone"] = contact_phone
                if contact_mobile:
                    candidate_entry["mobile"] = contact_mobile
                if broad_scope:
                    extra_attrs = related_attr_map.get(int(row[1])) if isinstance(row[1], int) else None
                    if isinstance(extra_attrs, dict):
                        for attr_key, attr_value in extra_attrs.items():
                            key = str(attr_key or "").strip()
                            if not key or key in candidate_entry:
                                continue
                            value_text = str(attr_value or "").strip()
                            if not value_text:
                                continue
                            candidate_entry[key] = value_text
                seen[norm] = candidate_entry
            else:
                if sc is not None and (entry["share_count"] is None or sc > entry["share_count"]):
                    entry["share_count"] = sc
                if nominal is not None and (entry["share_nominal_chf"] is None or nominal > entry["share_nominal_chf"]):
                    entry["share_nominal_chf"] = nominal
                if role and not entry["role"]:
                    entry["role"] = role
                if contact_email and not entry.get("email"):
                    entry["email"] = contact_email
                if contact_phone and not entry.get("phone"):
                    entry["phone"] = contact_phone
                if contact_mobile and not entry.get("mobile"):
                    entry["mobile"] = contact_mobile
                if broad_scope:
                    extra_attrs = related_attr_map.get(int(row[1])) if isinstance(row[1], int) else None
                    if isinstance(extra_attrs, dict):
                        for attr_key, attr_value in extra_attrs.items():
                            key = str(attr_key or "").strip()
                            if not key or key in entry:
                                continue
                            value_text = str(attr_value or "").strip()
                            if not value_text:
                                continue
                            entry[key] = value_text

        related_entries = sorted(seen.values(), key=lambda x: str(x.get("name") or "").lower())

        has_rich = any(
            e.get("share_count") is not None or e.get("share_nominal_chf") is not None
            for e in related_entries
        )
        if has_rich or broad_scope:
            attribute_value: Any = related_entries
        else:
            attribute_value = [
                {"name": e["name"], "role": e.get("role"), "relationship_name": e.get("relationship_name")}
                for e in related_entries
            ]

        return {
            "entity_name": anchor_name,
            "attribute_name": self._normalize_attribute_name(relationship_concept) or relationship_concept,
            "attribute_value": attribute_value,
            "relationship_concept": relationship_concept,
            "doc_id": doc_id,
            "doc_name": doc_key,
            "doc_path": doc_path,
        }

    def _sql_eori_contact_person_lookup(self, conn, entity_name: str) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    company.object_name AS company_name,
                    person.object_name AS contact_name,
                    rel.relationship_name,
                    COALESCE(MAX(doc.doc_id), 0) AS doc_id,
                    COALESCE(MAX(doc.doc_key), '') AS doc_key,
                    COALESCE(MAX(doc.doc_path), '') AS doc_path
                FROM object_instance company
                JOIN object_relationship rel
                  ON rel.tar_object_id = company.object_id
                JOIN object_instance person
                  ON person.object_id = rel.src_object_id
                                LEFT JOIN part part_rel
                                    ON part_rel.relationship_id = rel.relationship_id
                LEFT JOIN document doc
                  ON doc.doc_id = part_rel.doc_id
                WHERE LOWER(company.object_name) = LOWER(%s)
                                    AND COALESCE(company.status, 'active') = 'active'
                                    AND company.valid_from <= CURRENT_DATE
                                    AND (company.valid_until IS NULL OR company.valid_until > CURRENT_DATE)
                                    AND COALESCE(person.status, 'active') = 'active'
                                    AND person.valid_from <= CURRENT_DATE
                                    AND (person.valid_until IS NULL OR person.valid_until > CURRENT_DATE)
                                    AND rel.valid_from <= CURRENT_DATE
                                    AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                  AND (
                    LOWER(rel.relationship_name) = 'eori_contact_person'
                    OR LOWER(rel.relationship_name) = 'eori-ansprechpartner'
                    OR LOWER(rel.relationship_name) LIKE '%%eori%%ansprech%%'
                  )
                GROUP BY company.object_name, person.object_name, rel.relationship_name
                ORDER BY person.object_name
                LIMIT 1
                """,
                (entity_name,),
            )
            row = cur.fetchone()

        if not row:
            return None

        return {
            "entity_name": row[0],
            "attribute_name": "eori_contact_person",
            "attribute_value": row[1],
            "relationship_name": row[2],
            "doc_id": row[3] or None,
            "doc_name": row[4] or None,
            "doc_path": row[5] or None,
        }

    def _recover_shareholder_lookup_from_question(
        self,
        conn,
        parsed: ParsedQuery,
        question: str,
    ) -> dict[str, Any] | None:
        """Best-effort generic recovery for shareholder queries before not_found.

        Uses entity candidates from parsed payload, DB mention resolution, and NER hints,
        then retries company-centric and person-centric shareholder SQL lookups.
        """
        raw_question = str(question or "").strip()
        if not raw_question:
            return None

        candidate_hints: list[str] = []

        parsed_entity = str(parsed.entity_name or "").strip()
        if parsed_entity:
            candidate_hints.append(parsed_entity)

        db_mention = self._resolve_entity_mention_from_db(raw_question)
        if db_mention:
            candidate_hints.append(db_mention)

        best_hint = self._best_entity_hint(raw_question)
        if best_hint:
            candidate_hints.append(best_hint)

        spacy_hint = self._extract_entity_via_spacy(raw_question)
        if spacy_hint:
            candidate_hints.append(spacy_hint)

        for m in re.finditer(
            r"\b(?:of|for|von|fuer|für|der|des|bei|in)\s+([A-Za-z0-9 .&äöüÄÖÜß_\-/]+?)(?:\s+(?:and|und)\s+how\s+many\s+shares?.*|[\?\.!]|$)",
            raw_question,
            flags=re.IGNORECASE,
        ):
            phrase = self._normalize_entity_name_hint((m.group(1) or "").strip(" ."))
            if phrase:
                candidate_hints.append(phrase)

        seen: set[str] = set()
        ordered_hints: list[str] = []
        for hint in candidate_hints:
            h = self._normalize_entity_name_hint(hint)
            if not h:
                continue
            key = h.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered_hints.append(h)

        asks_company_shareholders = bool(
            re.search(
                r"\b(?:who\s+are|list|show|get|find|wer\s+sind|zeige|liste|gib\s+mir).*(?:shareholders?|stockholders?|aktion[aä]re|gesellschafter)\b",
                raw_question,
                flags=re.IGNORECASE,
            )
        )

        for hint in ordered_hints:
            resolved = self._resolve_entity_name_for_lookup(conn, hint) or hint

            company_first = asks_company_shareholders
            if company_first:
                result = self._sql_shareholder_lookup(conn, resolved)
                if result:
                    return result
                result = self._sql_companies_held_by_person_lookup(conn, resolved)
                if result:
                    return result
            else:
                result = self._sql_companies_held_by_person_lookup(conn, resolved)
                if result:
                    return result
                result = self._sql_shareholder_lookup(conn, resolved)
                if result:
                    return result

        return None

    def _sql_eori_contact_email_lookup(self, conn, entity_name: str) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    company.object_name AS company_name,
                    person.object_name AS contact_name,
                    email_attr.attr_json->>'value' AS contact_email,
                    rel.relationship_name,
                    COALESCE(MAX(doc.doc_id), 0) AS doc_id,
                    COALESCE(MAX(doc.doc_key), '') AS doc_key,
                    COALESCE(MAX(doc.doc_path), '') AS doc_path
                FROM object_instance company
                JOIN object_relationship rel
                  ON rel.tar_object_id = company.object_id
                JOIN object_instance person
                  ON person.object_id = rel.src_object_id
                JOIN attribute email_attr
                  ON email_attr.src_type = 'object'
                 AND email_attr.src_id = person.object_id
                 AND LOWER(email_attr.attr_type) = 'email'
                                LEFT JOIN part part_rel
                                    ON part_rel.relationship_id = rel.relationship_id
                LEFT JOIN document doc
                  ON doc.doc_id = part_rel.doc_id
                WHERE LOWER(company.object_name) = LOWER(%s)
                                    AND COALESCE(company.status, 'active') = 'active'
                                    AND COALESCE(company.valid_from, CURRENT_DATE) <= CURRENT_DATE
                                    AND (company.valid_until IS NULL OR company.valid_until > CURRENT_DATE)
                                    AND COALESCE(person.status, 'active') = 'active'
                                    AND COALESCE(person.valid_from, CURRENT_DATE) <= CURRENT_DATE
                                    AND (person.valid_until IS NULL OR person.valid_until > CURRENT_DATE)
                                    AND COALESCE(rel.valid_from, CURRENT_DATE) <= CURRENT_DATE
                                    AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                                    AND COALESCE(email_attr.valid_from, CURRENT_DATE) <= CURRENT_DATE
                                    AND (email_attr.valid_until IS NULL OR email_attr.valid_until > CURRENT_DATE)
                  AND (
                    LOWER(rel.relationship_name) = 'eori_contact_person'
                    OR LOWER(rel.relationship_name) = 'eori-ansprechpartner'
                    OR LOWER(rel.relationship_name) LIKE '%%eori%%ansprech%%'
                  )
                GROUP BY company.object_name, person.object_name, email_attr.attr_json->>'value', rel.relationship_name
                ORDER BY person.object_name
                LIMIT 1
                """,
                (entity_name,),
            )
            row = cur.fetchone()

        if not row:
            return None

        return {
            "entity_name": row[0],
            "attribute_name": "eori_contact_email",
            "attribute_value": row[2],
            "contact_person": row[1],
            "relationship_name": row[3],
            "doc_id": row[4] or None,
            "doc_name": row[5] or None,
            "doc_path": row[6] or None,
        }

    def _sql_process_contact_lookup(
        self, conn, entity_name: str, process_concept: str
    ) -> dict[str, Any] | None:
        """Generic: find the person related to entity_name via any relationship matching process_concept.

        Fetches email and phone of the related person. Works for EORI, customs, VAT, or any process.
        Automatically tries a normalized concept (strips '-number'/'-nummer' suffixes) as fallback.
        """
        # Normalize: strip trailing '-number', '-nummer', '-no', '-nr' so 'eori-number' → 'eori'
        normalized_concept = re.sub(
            r"[\-_](?:number|nummer|no|nr|registration|registrierung)\.?$",
            "", str(process_concept or "").strip(), flags=re.IGNORECASE
        ).strip("-_ ")
        concepts = [process_concept]
        if normalized_concept and normalized_concept.lower() != process_concept.lower():
            concepts.append(normalized_concept)

        for concept in concepts:
            concept_pattern = f"%{concept}%"
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        anchor.object_name              AS entity_name,
                        related.object_name             AS contact_name,
                        rel.relationship_name,
                        MAX(CASE WHEN LOWER(attr.attr_type) = 'email'
                                 THEN attr.attr_json->>'value' END) AS contact_email,
                        MAX(CASE WHEN LOWER(attr.attr_type) IN ('phone','telephone','tel')
                                 THEN attr.attr_json->>'value' END) AS contact_phone,
                        COALESCE(MAX(doc.doc_id), 0)    AS doc_id,
                        COALESCE(MAX(doc.doc_key), '')  AS doc_key,
                        COALESCE(MAX(doc.doc_path), '') AS doc_path
                    FROM object_instance anchor
                    JOIN object_relationship rel
                      ON rel.tar_object_id = anchor.object_id
                      OR rel.src_object_id = anchor.object_id
                    JOIN object_instance related
                      ON related.object_id = CASE
                            WHEN rel.src_object_id = anchor.object_id THEN rel.tar_object_id
                            ELSE rel.src_object_id
                         END
                    LEFT JOIN attribute attr
                      ON attr.src_type = 'object' AND attr.src_id = related.object_id
                      AND COALESCE(attr.valid_from, CURRENT_DATE) <= CURRENT_DATE
                      AND (attr.valid_until IS NULL OR attr.valid_until > CURRENT_DATE)
                    LEFT JOIN part part_rel ON part_rel.relationship_id = rel.relationship_id
                    LEFT JOIN document doc   ON doc.doc_id = part_rel.doc_id
                    WHERE LOWER(anchor.object_name) = LOWER(%s)
                      AND LOWER(rel.relationship_name) LIKE %s
                      AND COALESCE(anchor.status, 'active') = 'active'
                      AND COALESCE(anchor.valid_from, CURRENT_DATE) <= CURRENT_DATE
                      AND (anchor.valid_until IS NULL OR anchor.valid_until > CURRENT_DATE)
                      AND COALESCE(related.status, 'active') = 'active'
                      AND COALESCE(related.valid_from, CURRENT_DATE) <= CURRENT_DATE
                      AND (related.valid_until IS NULL OR related.valid_until > CURRENT_DATE)
                      AND COALESCE(rel.valid_from, CURRENT_DATE) <= CURRENT_DATE
                      AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                    GROUP BY anchor.object_name, related.object_name, rel.relationship_name
                    ORDER BY related.object_name
                    LIMIT 1
                    """,
                    (entity_name, concept_pattern),
                )
                row = cur.fetchone()

            if row:
                return {
                    "entity_name": row[0],
                    "attribute_name": "process_contact_person",
                    "attribute_value": row[1],
                    "relationship_name": row[2],
                    "contact_email": row[3] or None,
                    "contact_phone": row[4] or None,
                    "doc_id": row[5] or None,
                    "doc_name": row[6] or None,
                    "doc_path": row[7] or None,
                }

        return None

    def _sql_infer_employer_company(
        self, conn, contact_name: str, email_hint: str | None = None,
        exclude_name: str | None = None,
    ) -> str | None:
        """Generic: infer employer company for a contact person.

        1. Direct company relationship.
        2. Fallback: email domain matching.
        exclude_name prevents returning the anchor entity itself.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT related.object_name
                FROM object_instance contact
                JOIN object_relationship rel ON rel.src_object_id = contact.object_id
                JOIN object_instance related ON related.object_id = rel.tar_object_id
                WHERE LOWER(contact.object_name) = LOWER(%s)
                  AND LOWER(related.class_name) = 'company'
                  AND COALESCE(contact.status, 'active') = 'active'
                  AND COALESCE(contact.valid_from, CURRENT_DATE) <= CURRENT_DATE
                  AND (contact.valid_until IS NULL OR contact.valid_until > CURRENT_DATE)
                  AND COALESCE(related.status, 'active') = 'active'
                  AND COALESCE(related.valid_from, CURRENT_DATE) <= CURRENT_DATE
                  AND (related.valid_until IS NULL OR related.valid_until > CURRENT_DATE)
                  AND COALESCE(rel.valid_from, CURRENT_DATE) <= CURRENT_DATE
                  AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                ORDER BY related.object_name
                LIMIT 10
                """,
                (contact_name,),
            )
            rows = cur.fetchall()
        for row in rows:
            candidate = row[0]
            if exclude_name and candidate.lower() == str(exclude_name).lower():
                continue
            return candidate

        if email_hint and "@" in str(email_hint):
            domain = str(email_hint).split("@", 1)[1]
            domain_root = domain.split(".", 1)[0].strip()
            if domain_root:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT object_name
                        FROM object_instance
                        WHERE LOWER(class_name) = 'company'
                          AND COALESCE(status, 'active') = 'active'
                          AND valid_from <= CURRENT_DATE
                          AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                          AND LOWER(object_name) LIKE LOWER(%s)
                        ORDER BY CASE WHEN LOWER(object_name) = LOWER(%s) THEN 0 ELSE 1 END, object_name
                        LIMIT 1
                        """,
                        (f"%{domain_root}%", domain_root),
                    )
                    row = cur.fetchone()
                if row:
                    candidate = row[0]
                    if not exclude_name or candidate.lower() != str(exclude_name).lower():
                        return candidate

        return None

    # ---------------------------------------------------------------------------
    # Legacy EORI-specific helpers kept for backward compatibility.
    # They now delegate to the generic functions above.
    # ---------------------------------------------------------------------------

    def _sql_eori_responsible_company_lookup(self, conn, entity_name: str) -> dict[str, Any] | None:
        contact = self._sql_process_contact_lookup(conn, entity_name, "eori")
        if not contact:
            return None
        employer = self._sql_infer_employer_company(
            conn,
            contact["attribute_value"],
            contact.get("contact_email"),
            exclude_name=entity_name,
        )
        if not employer:
            return None
        return {
            "entity_name": entity_name,
            "attribute_name": "responsible_company",
            "attribute_value": employer,
            "contact_person": contact["attribute_value"],
            "contact_email": contact.get("contact_email"),
            "relationship_name": contact["relationship_name"],
            "doc_id": contact.get("doc_id"),
            "doc_name": contact.get("doc_name"),
            "doc_path": contact.get("doc_path"),
        }

    def _discovery_serving_config_path(self) -> str:
        return ""

    def discovery_candidates(self, question: str, limit: int = 8) -> list[dict[str, Any]]:
        del question
        del limit
        return []

    def search_discovery(self, question: str, limit: int = 8) -> dict[str, Any]:
        results = self.discovery_candidates(question, limit=limit)
        return {
            "question": question,
            "candidate_count": len(results),
            "results": results,
            "summary": self._semantic_summary_from_discovery(results),
        }

    def _lookup_attribute_in_discovery_results(
        self,
        attribute_name: str,
        entity_name: str | None,
        results: list[dict[str, Any]],
        attribute_filters: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        for item in results:
            struct_data = item.get("struct_data") if isinstance(item.get("struct_data"), dict) else {}
            derived_struct_data = item.get("derived_struct_data") if isinstance(item.get("derived_struct_data"), dict) else {}

            if entity_name:
                haystack = json.dumps(struct_data, ensure_ascii=False).lower() + " " + json.dumps(derived_struct_data, ensure_ascii=False).lower()
                if entity_name.lower() not in haystack:
                    continue

            if not self._structured_table_record_matches_attribute_filters(
                struct_data,
                {"struct_data": struct_data, "derived_struct_data": derived_struct_data},
                list(attribute_filters or []),
            ):
                continue

            attr_value = self._extract_attr_value(struct_data, attribute_name)
            if attr_value is None:
                attr_value = self._extract_attr_value(derived_struct_data, attribute_name)
            if attr_value is None:
                continue

            return {
                "attribute_name": attribute_name,
                "attribute_value": attr_value,
                "document_id": item.get("document_id"),
                "doc_path": item.get("content_uri"),
                "mime_type": item.get("mime_type"),
                "source": "discovery",
            }
        return None

    @staticmethod
    def _is_absolute_or_uri_path(path_text: str | None) -> bool:
        text = str(path_text or "").strip()
        if not text:
            return False
        if re.match(r"^[A-Za-z]:[\\/]", text):
            return True
        if text.startswith("\\\\") or text.startswith("//"):
            return True
        if text.startswith("/"):
            return True
        if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", text):
            return True
        return False

    @staticmethod
    def _looks_like_temp_upload_path(path_text: str | None) -> bool:
        text = str(path_text or "").strip().lower()
        if not text:
            return False
        return bool(re.search(r"(?:^|[\\/])idms-upload-[a-z0-9]+\.[a-z0-9]+$", text))

    @staticmethod
    def _is_useful_document_full_path(path_text: str | None) -> bool:
        text = str(path_text or "").strip()
        if not text:
            return False
        if QueryEngine._is_absolute_or_uri_path(text):
            return True
        return False

    def _resolve_document_full_path(self, doc_path: str | None, metadata: dict | None) -> str:
        meta = metadata if isinstance(metadata, dict) else {}
        user_metadata = meta.get("user_metadata") if isinstance(meta.get("user_metadata"), dict) else {}
        source_ref = user_metadata.get("source_reference") if isinstance(user_metadata.get("source_reference"), dict) else {}

        candidates = [
            str(source_ref.get("original_local_path") or "").strip(),
            str(user_metadata.get("client_file_path") or "").strip(),
            str(source_ref.get("source_path_or_uri") or "").strip(),
            str(source_ref.get("gcs_uri") or "").strip(),
            str(doc_path or "").strip(),
        ]

        # Prefer only usable source paths/URIs.
        for cand in candidates:
            if cand and self._is_useful_document_full_path(cand):
                return cand

        return ""

    def _compose_document_file_identity(
        self,
        doc_name: str | None,
        doc_key: str | None,
        doc_path: str | None,
        metadata: dict | None,
    ) -> tuple[str, str]:
        meta = metadata if isinstance(metadata, dict) else {}
        user_metadata = meta.get("user_metadata") if isinstance(meta.get("user_metadata"), dict) else {}

        doc_name_value = str(doc_name or "").strip()
        client_file_name = str(user_metadata.get("client_file_name") or "").strip()
        doc_path_value = str(doc_path or "").strip()
        path_file_name = os.path.basename(doc_path_value)
        if re.match(r"^\d{8}_\d{6}_idms-upload-[a-z0-9]+\.[A-Za-z0-9]+$", path_file_name):
            path_file_name = ""

        doc_key_value = str(doc_key or "").strip()
        key_as_filename = ""
        if doc_key_value:
            normalized_key = re.sub(r"[^A-Za-z0-9._-]+", "_", doc_key_value).strip("_")
            if normalized_key:
                key_as_filename = f"{normalized_key}.pdf"

        file_name = doc_name_value or client_file_name or key_as_filename or path_file_name or doc_key_value
        full_path = self._resolve_document_full_path(doc_path_value, meta)
        return file_name, full_path

    def sql_exact_attribute_lookup(
        self,
        conn,
        entity_name: str,
        attribute_name: str,
    ) -> dict[str, Any] | None:
        scope = self._resolve_entity_object_scope(conn, entity_name)
        scoped_object_ids = [int(item) for item in list(scope.get("object_ids") or []) if isinstance(item, int) or str(item).isdigit()]
        if not scoped_object_ids:
            resolved_name = str(scope.get("resolved_name") or "").strip()
            if resolved_name:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT object_id
                        FROM object_instance
                        WHERE LOWER(object_name) = LOWER(%s)
                          AND COALESCE(status, 'active') = 'active'
                          AND valid_from <= CURRENT_DATE
                          AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                        LIMIT 1
                        """,
                        (resolved_name,),
                    )
                    row = cur.fetchone()
                if row:
                    scoped_object_ids = [int(row[0])]

        # Document-level attributes: filename/path/file-info from document + metadata.
        if attribute_name in {"document_filename", "document_full_path", "document_file_info"}:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        d.doc_id,
                        d.doc_name,
                        d.doc_key,
                        d.doc_path,
                        d.metadata,
                        oi.object_name
                    FROM document d
                    LEFT JOIN part p ON p.doc_id = d.doc_id AND p.object_id IS NOT NULL
                    LEFT JOIN object_instance oi ON oi.object_id = p.object_id
                    WHERE (
                        LOWER(COALESCE(oi.object_name, '')) LIKE LOWER(%s)
                        OR LOWER(COALESCE(d.doc_key, '')) LIKE LOWER(%s)
                        OR LOWER(COALESCE(d.doc_path, '')) LIKE LOWER(%s)
                        OR LOWER(COALESCE(d.metadata::text, '')) LIKE LOWER(%s)
                    )
                                            AND COALESCE(d.status, 'active') = 'active'
                                            AND d.valid_from <= CURRENT_DATE
                                            AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                    ORDER BY d.entry_date DESC
                    LIMIT 1;
                    """,
                    (
                        f"%{entity_name}%",
                        f"%{entity_name}%",
                        f"%{entity_name}%",
                        f"%{entity_name}%",
                    ),
                )
                doc_row = cur.fetchone()

            if doc_row:
                metadata = doc_row[4] if isinstance(doc_row[4], dict) else {}
                file_name, full_path = self._compose_document_file_identity(
                    doc_name=doc_row[1],
                    doc_key=doc_row[2],
                    doc_path=doc_row[3],
                    metadata=metadata,
                )

                if attribute_name == "document_full_path" and full_path:
                    return {
                        "object_id": None,
                        "entity_name": doc_row[5] or entity_name,
                        "entity_type": "document",
                        "attribute_name": "document_full_path",
                        "attribute_value": full_path,
                        "doc_id": doc_row[0],
                        "doc_name": doc_row[1],
                        "doc_path": doc_row[3],
                        "doc_date": None,
                    }

                if attribute_name == "document_file_info" and (file_name or full_path):
                    return {
                        "object_id": None,
                        "entity_name": doc_row[5] or entity_name,
                        "entity_type": "document",
                        "attribute_name": "document_file_info",
                        "attribute_value": {
                            "file_name": file_name,
                            "full_path": full_path,
                        },
                        "doc_id": doc_row[0],
                        "doc_name": doc_row[1],
                        "doc_path": doc_row[3],
                        "doc_date": None,
                    }

                if attribute_name == "document_filename" and file_name:
                    return {
                        "object_id": None,
                        "entity_name": doc_row[5] or entity_name,
                        "entity_type": "document",
                        "attribute_name": "document_filename",
                        "attribute_value": file_name,
                        "doc_id": doc_row[0],
                        "doc_name": doc_row[1],
                        "doc_path": doc_row[3],
                        "doc_date": None,
                    }

        # Tier A: targeted lookup by attr_type on current object attributes.
        attribute_candidates = [attribute_name]
        if attribute_name in {"address", "registered_address", "address_full", "anschrift", "adresse"}:
            if attribute_name == "address":
                attribute_candidates = ["address", "address_full", "registered_address"]
            elif attribute_name == "registered_address":
                attribute_candidates = ["registered_address", "address_full", "address"]
            else:
                attribute_candidates = ["address_full", "address", "registered_address"]
        elif attribute_name in {"registration_no", "uid_che", "tr_number"}:
            if attribute_name in {"registration_no", "uid_che"}:
                attribute_candidates = ["uid_che", "registration_no", "tr_number"]
            else:
                attribute_candidates = ["tr_number", "uid_che", "registration_no"]

        prefer_identifier_quality = str(attribute_name or "").strip().lower() in {
            "eori_no", "eori_number", "eori", "eori_nr", "eori_nummer",
            "registration_no", "uid_che", "tr_number",
        }

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    oi.object_id,
                    oi.object_name,
                    oi.class_name,
                    a.attr_type,
                    a.attr_json->>'value' AS attr_value
                FROM object_instance oi
                JOIN attribute a
                  ON a.src_type = 'object'
                 AND a.src_id = oi.object_id
                 AND LOWER(a.attr_type) = ANY(%s)
                                WHERE oi.object_id = ANY(%s)
                                    AND COALESCE(oi.status, 'active') = 'active'
                                    AND oi.valid_from <= CURRENT_DATE
                                    AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                                    AND (
                                        (
                                            NOT %s
                                            AND a.valid_from <= CURRENT_DATE
                                            AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
                                        )
                                        OR %s
                                    )
                ORDER BY
                    CASE
                        WHEN LOWER(a.attr_type) = LOWER(%s) THEN 0
                        ELSE 1
                    END,
                    CASE
                        WHEN %s AND LOWER(a.attr_type) = 'eori_no'
                             AND COALESCE(a.attr_json->>'value', '') ~ '[0-9]'
                             AND LENGTH(COALESCE(a.attr_json->>'value', '')) >= 8
                        THEN 0
                        WHEN %s AND LOWER(a.attr_type) = 'eori_no' THEN 1
                        ELSE 0
                    END,
                    CASE
                        WHEN %s
                             AND a.valid_from <= CURRENT_DATE
                             AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
                        THEN 0
                        WHEN %s THEN 1
                        ELSE 0
                    END,
                    a.entry_date DESC NULLS LAST,
                    a.attr_id DESC
                LIMIT 1;
                """,
                (
                    [name.lower() for name in attribute_candidates],
                    scoped_object_ids,
                    prefer_identifier_quality,
                    prefer_identifier_quality,
                    str(attribute_name or "").strip().lower(),
                    prefer_identifier_quality,
                    prefer_identifier_quality,
                    prefer_identifier_quality,
                    prefer_identifier_quality,
                ),
            )
            targeted = cur.fetchone()
        if (
            targeted
            and targeted[4] is not None
            and not (
                prefer_identifier_quality
                and self._is_placeholder_identifier_value(targeted[4])
            )
        ):
            return {
                "object_id": targeted[0],
                "entity_name": targeted[1],
                "entity_type": targeted[2],
                "attribute_name": targeted[3] or attribute_name,
                "attribute_value": targeted[4],
                "doc_id": None,
                "doc_name": None,
                "doc_path": None,
                "doc_date": None,
            }

        # Compatibility fallback for historical purpose values.
        if attribute_name == "purpose":
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        oi.object_id,
                        oi.object_name,
                        oi.class_name,
                        a.attr_json->>'value' AS attr_value
                    FROM object_instance oi
                    JOIN attribute a
                      ON a.src_type = 'object'
                     AND a.src_id = oi.object_id
                     AND a.attr_type = 'description'
                                        WHERE oi.object_id = ANY(%s)
                                        AND COALESCE(oi.status, 'active') = 'active'
                                        AND oi.valid_from <= CURRENT_DATE
                                        AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                                        AND a.valid_from <= CURRENT_DATE
                                        AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
                    ORDER BY a.entry_date DESC NULLS LAST
                    LIMIT 1;
                    """,
                    (scoped_object_ids,),
                )
                legacy = cur.fetchone()
            if legacy and legacy[3] is not None:
                return {
                    "object_id": legacy[0],
                    "entity_name": legacy[1],
                    "entity_type": legacy[2],
                    "attribute_name": "purpose",
                    "attribute_value": legacy[3],
                    "doc_id": None,
                    "doc_name": None,
                    "doc_path": None,
                    "doc_date": None,
                }

        # Tier B: broad scan in attr_json and object metadata.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    oi.object_id,
                    oi.object_name,
                    oi.class_name,
                    a.attr_json,
                    oi.metadata
                FROM object_instance oi
                LEFT JOIN attribute a
                  ON a.src_type = 'object'
                 AND a.src_id = oi.object_id
                                WHERE oi.object_id = ANY(%s)
                                    AND COALESCE(oi.status, 'active') = 'active'
                                    AND oi.valid_from <= CURRENT_DATE
                                    AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                ORDER BY a.entry_date DESC NULLS LAST, oi.entry_date DESC
                LIMIT 50;
                """,
                (scoped_object_ids,),
            )
            rows = cur.fetchall()

        for row in rows:
            attr_value = self._extract_attr_value(row[3], attribute_name)
            if attr_value is None:
                attr_value = self._extract_attr_value(row[4], attribute_name)
            if attr_value is None:
                continue

            return {
                "object_id": row[0],
                "entity_name": row[1],
                "entity_type": row[2],
                "attribute_name": attribute_name,
                "attribute_value": attr_value,
                "doc_id": None,
                "doc_name": None,
                "doc_path": None,
                "doc_date": None,
            }

        return None

    def _document_file_info_from_doc_id(
        self,
        conn,
        doc_id: int,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT doc_id, doc_name, doc_key, doc_path, metadata
                FROM document
                WHERE doc_id = %s
                LIMIT 1;
                """,
                (int(doc_id),),
            )
            row = cur.fetchone()

        if not row:
            return None

        metadata = row[4] if isinstance(row[4], dict) else {}
        file_name, full_path = self._compose_document_file_identity(
            doc_name=row[1],
            doc_key=row[2],
            doc_path=row[3],
            metadata=metadata,
        )
        if not file_name and not full_path:
            return None

        return {
            "object_id": None,
            "entity_name": None,
            "entity_type": "document",
            "attribute_name": "document_file_info",
            "attribute_value": {
                "file_name": file_name,
                "full_path": full_path,
            },
            "doc_id": row[0],
            "doc_name": row[1],
            "doc_path": row[3],
            "doc_date": None,
        }

    def _document_file_info_from_semantic_candidates(
        self,
        conn,
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        for item in candidates or []:
            doc_id = item.get("doc_id")
            if not isinstance(doc_id, int):
                continue
            info = self._document_file_info_from_doc_id(conn, doc_id)
            if info:
                return info
        return None

    def _semantic_tokens(self, text: str) -> list[str]:
        raw_tokens = re.findall(r"[a-zA-ZäöüÄÖÜß0-9]{4,}", str(text or "").lower())
        tokens: list[str] = []
        seen: set[str] = set()
        for tok in raw_tokens:
            if tok in self.SEMANTIC_STOPWORDS:
                continue
            if tok in seen:
                continue
            seen.add(tok)
            tokens.append(tok)
        return tokens[:12]

    @staticmethod
    def _resolve_doc_title(doc_key: str | None, metadata: dict) -> str | None:
        """Return a human-readable title for a document result item.

        Priority:
          1. metadata.note_title (explicitly set on notes)
          2. metadata.user_description stripped of the 'Note/memo:' prefix
          3. doc_key humanised (underscores/dashes → spaces, title-cased)
        """
        note_title = str(metadata.get("note_title") or "").strip()
        if note_title:
            return note_title

        user_desc = str(metadata.get("user_description") or "").strip()
        if user_desc:
            # Strip common prefixes like "Note/memo: " so only the real title remains.
            cleaned = re.sub(r"^(?:note/memo|note|memo|document)\s*:\s*", "", user_desc, flags=re.IGNORECASE).strip()
            if cleaned:
                return cleaned

        key = str(doc_key or "").strip()
        if key:
            return re.sub(r"[-_]+", " ", key).title()

        return None

    @staticmethod
    def _resolve_file_name(doc_key: str | None, doc_path: str | None, metadata: dict) -> str | None:
        """Return the best file name for a document result item.

        Priority:
          1. metadata.user_metadata.client_file_name  (original upload name)
          2. metadata.user_metadata.source_reference.original_local_path  basename
          3. basename of doc_path (GCS URI)
          4. doc_key as a last resort
        """
        user_meta = metadata.get("user_metadata") if isinstance(metadata.get("user_metadata"), dict) else {}
        client_fn = str(user_meta.get("client_file_name") or "").strip()
        if client_fn:
            return client_fn

        sr = user_meta.get("source_reference") if isinstance(user_meta.get("source_reference"), dict) else {}
        orig_path = str(sr.get("original_local_path") or user_meta.get("client_file_path") or "").strip()
        if orig_path:
            bn = os.path.basename(orig_path)
            if bn:
                return bn

        path_bn = os.path.basename(str(doc_path or "").strip())
        if path_bn:
            return path_bn

        return str(doc_key or "").strip() or None

    def _normalize_semantic_text(self, text: str) -> str:
        lowered = str(text or "").lower()
        # Normalize common umlaut variants and observed mojibake forms.
        lowered = (
            lowered.replace("ä", "ae")
            .replace("ö", "oe")
            .replace("ü", "ue")
            .replace("ß", "ss")
            .replace("÷", "oe")
            .replace("õ", "ae")
            .replace("³", "ue")
        )
        return lowered

    def _semantic_token_variants(self, token: str) -> list[str]:
        t = self._normalize_semantic_text(token)
        variants = {t}
        variants.add(t.replace("ae", "a"))
        variants.add(t.replace("oe", "o"))
        variants.add(t.replace("ue", "u"))
        return [v for v in variants if v]

    def _criteria_term_matches_haystack(self, term: str, haystack: str) -> bool:
        normalized_term = self._normalize_semantic_text(term)
        if not normalized_term:
            return False

        variant_set: set[str] = set(self._semantic_token_variants(normalized_term))
        pieces = [
            tok
            for tok in re.findall(r"[a-zA-ZäöüÄÖÜß0-9]{2,}", normalized_term)
            if tok not in self.SEMANTIC_STOPWORDS
        ]
        for token in pieces:
            variant_set.update(self._semantic_token_variants(token))

        # Domain aliasing for travel intent so queries with "travelling" match
        # ticket/reise/fahrt style document language.
        travel_aliases = {
            "travel", "travelling", "traveling", "trip", "journey", "reisen", "reise", "fahrt", "ticket", "tickets",
            "booking", "booked", "reservation", "itinerary",
        }
        if normalized_term in travel_aliases or any(tok in travel_aliases for tok in pieces):
            variant_set.update(travel_aliases)

        # Domain aliasing for telecom/billing intent so wording variants like
        # telephone/mobile/telecom and bill/invoice/rechnung converge.
        telecom_aliases = {
            "telephone", "phone", "mobile", "telecom", "telecommunications", "cell", "cellular", "gsm",
            "telefon", "telefonie", "mobil", "mobilfunk", "telekommunikation",
            "service", "services", "provider", "carrier",
        }
        billing_aliases = {
            "bill", "bills", "invoice", "invoices", "receipt", "receipts", "statement",
            "rechnung", "rechnungen", "beleg", "belege", "quittung", "zahlungen", "payment", "payments",
            "amount", "cost", "price", "due", "total", "betrag", "kosten", "preis", "gesamt",
        }
        if normalized_term in telecom_aliases or any(tok in telecom_aliases for tok in pieces):
            variant_set.update(telecom_aliases)
        if normalized_term in billing_aliases or any(tok in billing_aliases for tok in pieces):
            variant_set.update(billing_aliases)

        if any(v in haystack for v in variant_set if v):
            return True

        if len(pieces) < 2:
            return False
        return all(any(v in haystack for v in self._semantic_token_variants(tok)) for tok in pieces)

    def _document_file_info_from_semantic_text(self, conn, hint_text: str) -> dict[str, Any] | None:
        tokens = self._semantic_tokens(hint_text)
        if not tokens:
            return None

        raw_words = re.findall(r"[a-zA-ZäöüÄÖÜß0-9_\-]+", str(hint_text or "").lower())
        strict_entity_phrase = len(raw_words) <= 4 and len(tokens) >= 2

        score_by_doc: dict[int, int] = {}

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    d.doc_id,
                    COALESCE(d.doc_key, '') AS doc_key,
                    COALESCE(d.doc_name, '') AS doc_name,
                    COALESCE(d.doc_desc, '') AS doc_desc,
                    COALESCE(d.keyword_text, '') AS keyword_text,
                    COALESCE(d.metadata::text, '') AS metadata_text
                FROM document d
                                WHERE COALESCE(d.status, 'active') = 'active'
                                    AND d.valid_from <= CURRENT_DATE
                                    AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                ORDER BY d.entry_date DESC
                LIMIT 300;
                """
            )
            doc_rows = cur.fetchall()

        for row in doc_rows:
            doc_id = int(row[0])
            haystack = self._normalize_semantic_text(f"{row[1]} {row[2]} {row[3]} {row[4]} {row[5]}")
            score = 0
            for tok in tokens:
                if any(variant in haystack for variant in self._semantic_token_variants(tok)):
                    score += 1
            if strict_entity_phrase and score < len(tokens):
                continue
            if score > 0:
                score_by_doc[doc_id] = score_by_doc.get(doc_id, 0) + score

        # Boost scores from object-level metadata where long business descriptions typically live.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT object_id, metadata
                FROM object_instance
                ORDER BY entry_date DESC
                LIMIT 1000;
                """
            )
            obj_rows = cur.fetchall()

        for _, metadata in obj_rows:
            obj_meta = metadata if isinstance(metadata, dict) else {}
            doc_id_val = obj_meta.get("doc_id")
            if not isinstance(doc_id_val, int):
                continue
            haystack = self._normalize_semantic_text(json.dumps(obj_meta, ensure_ascii=False))
            score = 0
            for tok in tokens:
                if any(variant in haystack for variant in self._semantic_token_variants(tok)):
                    score += 1
            if score > 0:
                # Object metadata is strong signal for descriptive semantic match.
                score_by_doc[doc_id_val] = score_by_doc.get(doc_id_val, 0) + (score * 2)

        best_doc_id: int | None = None
        best_score = 0
        for doc_id, score in score_by_doc.items():
            if score > best_score:
                best_doc_id = doc_id
                best_score = score

        if best_doc_id is None or best_score < 1:
            return None
        return self._document_file_info_from_doc_id(conn, best_doc_id)

        # Tier A: targeted lookup by attr_type — covers attributes stored as
        # attr_type='statute_date', attr_json={'value': '...'} (the canonical storage pattern).
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    oi.object_id,
                    oi.object_name,
                    oi.class_name,
                    a.attr_json->>'value' AS attr_value
                FROM object_instance oi
                JOIN attribute a
                  ON a.src_type = 'object'
                 AND a.src_id = oi.object_id
                 AND a.attr_type = %s
                WHERE LOWER(oi.object_name) = LOWER(%s)
                                    AND COALESCE(oi.status, 'active') = 'active'
                                    AND oi.valid_from <= CURRENT_DATE
                                    AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                                    AND a.valid_from <= CURRENT_DATE
                                    AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
                ORDER BY a.entry_date DESC NULLS LAST
                LIMIT 1;
                """,
                (attribute_name, entity_name),
            )
            targeted = cur.fetchone()
        if targeted and targeted[3] is not None:
            return {
                "object_id": targeted[0],
                "entity_name": targeted[1],
                "entity_type": targeted[2],
                "attribute_name": attribute_name,
                "attribute_value": targeted[3],
                "doc_id": None,
                "doc_name": None,
                "doc_path": None,
                "doc_date": None,
            }

        # Compatibility fallback: many historical ingests stored company purpose
        # in "description". Serve it for purpose queries until reprocessing converges.
        if attribute_name == "purpose":
            legacy = self.sql_exact_attribute_lookup(conn, entity_name, "description")
            if legacy:
                adapted = dict(legacy)
                adapted["attribute_name"] = "purpose"
                return adapted

        # Tier B: broad scan — covers attributes embedded as keys in attr_json or oi.metadata.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    oi.object_id,
                    oi.object_name,
                    oi.class_name,
                    a.attr_json,
                    oi.metadata,
                    NULL::BIGINT AS doc_id,
                    NULL::VARCHAR AS doc_name,
                    NULL::VARCHAR AS doc_path,
                    NULL::TEXT AS doc_date
                FROM object_instance oi
                LEFT JOIN attribute a
                  ON a.src_type = 'object'
                 AND a.src_id = oi.object_id
                WHERE LOWER(oi.object_name) = LOWER(%s)
                                    AND COALESCE(oi.status, 'active') = 'active'
                                    AND oi.valid_from <= CURRENT_DATE
                                    AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
                ORDER BY a.entry_date DESC NULLS LAST, oi.entry_date DESC
                LIMIT 50;
                """,
                (entity_name,),
            )
            rows = cur.fetchall()

        for row in rows:
            attr_value = self._extract_attr_value(row[3], attribute_name)
            if attr_value is None:
                attr_value = self._extract_attr_value(row[4], attribute_name)
            if attr_value is None:
                continue

            return {
                "object_id": row[0],
                "entity_name": row[1],
                "entity_type": row[2],
                "attribute_name": attribute_name,
                "attribute_value": attr_value,
                "doc_id": row[5],
                "doc_name": row[6],
                "doc_path": row[7],
                "doc_date": row[8],
            }

        with conn.cursor() as cur:
            cur.execute(
                """
                WITH target_objects AS (
                    SELECT object_id, object_name, class_name
                    FROM object_instance
                    WHERE LOWER(object_name) = LOWER(%s)
                )
                SELECT
                    n.object_id,
                    n.object_name,
                    n.class_name,
                    a.attr_json,
                    d.identifiers_kv,
                    d.metadata,
                    d.doc_id,
                    d.doc_key,
                    d.doc_path,
                    d.doc_date::text
                FROM target_objects n
                LEFT JOIN attribute a
                    ON a.src_type = 'object'
                AND a.src_id = n.object_id
                LEFT JOIN part p
                    ON p.object_id = n.object_id
                LEFT JOIN document d
                    ON d.doc_id = p.doc_id
                ORDER BY d.entry_date DESC
                LIMIT 50;
                """,
                (entity_name,),
            )
            rows = cur.fetchall()

        for row in rows:
            attr_value = self._extract_attr_value(row[3], attribute_name)
            if attr_value is None:
                attr_value = self._extract_attr_value(row[4], attribute_name)
            if attr_value is None:
                attr_value = self._extract_attr_value(row[5], attribute_name)
            if attr_value is None:
                continue

            return {
                "object_id": row[0],
                "entity_name": row[1],
                "entity_type": row[2],
                "attribute_name": attribute_name,
                "attribute_value": attr_value,
                "doc_id": row[6],
                "doc_name": row[7],
                "doc_path": row[8],
                "doc_date": row[9],
            }

        return None

    def qdrant_candidates(self, question: str, limit: int = 8) -> list[dict[str, Any]]:
        if getattr(self, "qdrant", None) is None:
            return []
        
        dense = self.embed_text(question)
        collection_name = self._resolve_qdrant_collection_name()
        vector_profile = self._qdrant_collection_vector_profile(collection_name)
        
        # Decide whether to use hybrid search (sparse + dense)
        use_hybrid = (
            should_use_hybrid_search and 
            should_use_hybrid_search(question) and
            encode_sparse_vector and
            merge_hybrid_results and
            bool(vector_profile.get("supports_sparse"))
        )
        
        if use_hybrid:
            # Use hybrid search combining sparse and dense vectors
            search_points = self._qdrant_hybrid_search_points(
                collection_name=collection_name,
                query_text=question,
                dense_vector=dense,
                limit=limit,
                sparse_weight=0.3,  # 30% weight to keyword matching
                dense_weight=0.7,   # 70% weight to semantic matching
                with_payload=True,
                dense_vector_name=vector_profile.get("dense_vector_name"),
                sparse_vector_name=str(vector_profile.get("sparse_vector_name") or "sparse"),
            )
            LOGGER.info("qdrant_search mode=hybrid question_len=%d results=%d", len(question), len(search_points))
        else:
            # Fall back to dense-only search
            search_points = self._qdrant_dense_search_points(
                collection_name=collection_name,
                dense_vector=dense,
                limit=limit,
                with_payload=True,
                vector_name=vector_profile.get("dense_vector_name"),
            )
            LOGGER.info("qdrant_search mode=dense question_len=%d results=%d", len(question), len(search_points))
        
        if search_points:
            self._qdrant_collection_name = collection_name
            return [p.payload or {} for p in search_points]

        # Retry with refreshed collection name if initial search failed
        refreshed = self._resolve_qdrant_collection_name(force_refresh=True)
        if refreshed and refreshed != collection_name:
            refreshed_profile = self._qdrant_collection_vector_profile(refreshed)
            if use_hybrid:
                refreshed_points = self._qdrant_hybrid_search_points(
                    collection_name=refreshed,
                    query_text=question,
                    dense_vector=dense,
                    limit=limit,
                    sparse_weight=0.3,
                    dense_weight=0.7,
                    with_payload=True,
                    dense_vector_name=refreshed_profile.get("dense_vector_name"),
                    sparse_vector_name=str(refreshed_profile.get("sparse_vector_name") or "sparse"),
                )
            else:
                refreshed_points = self._qdrant_dense_search_points(
                    collection_name=refreshed,
                    dense_vector=dense,
                    limit=limit,
                    with_payload=True,
                    vector_name=refreshed_profile.get("dense_vector_name"),
                )
            
            if refreshed_points:
                self._qdrant_collection_name = refreshed
                return [p.payload or {} for p in refreshed_points]

        keyword_fallback = self._qdrant_keyword_candidates(
            collection_name=refreshed if refreshed else collection_name,
            question=question,
            limit=limit,
        )
        if keyword_fallback:
            self._last_qdrant_search_mode = "keyword_payload_fallback"
            self._last_qdrant_search_collection = refreshed if refreshed else collection_name
            return keyword_fallback

        return []

    def _qdrant_keyword_candidates(self, collection_name: str, question: str, limit: int = 8) -> list[dict[str, Any]]:
        """Fallback lexical search over stored Qdrant payload text when vector search fails."""
        qdrant_client = getattr(self, "qdrant", None)
        if qdrant_client is None:
            return []

        # Keep only content-bearing tokens.
        tokens = [
            tok for tok in re.findall(r"[a-z0-9]{3,}", str(question or "").lower())
            if tok not in {
                "what", "is", "the", "for", "with", "and", "der", "die", "das", "von", "fur", "fuer",
                "number", "nummer", "identifier", "company", "firma", "gmbh",
            }
        ]
        if not tokens:
            return []

        low_signal_tokens = {"id", "no", "nr", "register", "registration"}
        strong_tokens = [tok for tok in tokens if tok not in low_signal_tokens]

        ranked: list[tuple[int, dict[str, Any]]] = []
        offset = None
        fetched = 0
        max_scan = 2000
        batch = 250

        while fetched < max_scan:
            try:
                points, next_offset = qdrant_client.scroll(
                    collection_name=collection_name,
                    with_payload=True,
                    with_vectors=False,
                    limit=batch,
                    offset=offset,
                )
            except Exception:
                break

            if not points:
                break

            for point in points:
                payload = point.payload if isinstance(point.payload, dict) else {}
                payload_keywords = payload.get("doc_keywords") if isinstance(payload.get("doc_keywords"), list) else []
                payload_entities = payload.get("entity_names") if isinstance(payload.get("entity_names"), list) else []
                haystack = " ".join(
                    [
                        str(payload.get("chunk_text") or ""),
                        str(payload.get("doc_key") or ""),
                        str(payload.get("doc_type") or ""),
                        str(payload.get("gcs_uri") or ""),
                        " ".join(str(item) for item in payload_keywords),
                        " ".join(str(item) for item in payload_entities),
                    ]
                ).lower()
                if not haystack:
                    continue

                score = 0
                strong_hit = False
                for tok in tokens:
                    if tok in haystack:
                        score += 1
                        if tok in strong_tokens:
                            strong_hit = True
                if score > 0 and (strong_hit or not strong_tokens):
                    ranked.append((score, payload))

            fetched += len(points)
            offset = next_offset
            if offset is None:
                break

        if not ranked:
            return []

        ranked.sort(key=lambda item: item[0], reverse=True)
        out: list[dict[str, Any]] = []
        seen: set[tuple[Any, Any]] = set()
        for _score, payload in ranked:
            key = (payload.get("doc_id"), payload.get("chunk_index"))
            if key in seen:
                continue
            seen.add(key)
            out.append(payload)
            if len(out) >= max(1, int(limit)):
                break
        return out

    def _infer_identifier_attribute_for_entity(self, entity_name: str, question: str) -> str | None:
        """Best-effort recovery for unforeseen identifier attributes stored in DB.

        Example: if a new attr like customs_operator_id exists, identifier queries can still
        resolve it even when it is not in canonical aliases yet.
        """
        attrs = [str(item).strip() for item in self._get_entity_attribute_names(entity_name) if str(item).strip()]
        if not attrs:
            return None

        normalized_q = self._normalize_alias_key(question)
        q_tokens = {
            tok
            for tok in re.findall(r"[a-z0-9]{2,}", normalized_q)
            if tok not in {"what", "is", "the", "of", "for", "number", "nummer", "no", "nr", "id"}
        }
        if not q_tokens:
            return None

        best_attr = None
        best_score = 0
        for attr in attrs:
            key = self._normalize_alias_key(attr)
            attr_tokens = set(re.findall(r"[a-z0-9]{2,}", key))
            overlap = len(q_tokens.intersection(attr_tokens))
            contains = 1 if any(tok in key for tok in q_tokens) else 0
            score = overlap * 3 + contains
            if score > best_score:
                best_score = score
                best_attr = attr

        return best_attr if best_score >= 2 else None

    def _unique_ints(self, values: list[Any]) -> list[int]:
        out: list[int] = []
        seen: set[int] = set()
        for v in values:
            if isinstance(v, int) and v not in seen:
                seen.add(v)
                out.append(v)
        return out

    def _fetch_nodes_by_ids(self, conn, node_ids: list[int], limit: int = 50) -> list[dict[str, Any]]:
        if not node_ids:
            return []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT object_id, object_name, class_name, metadata
                FROM object_instance
                WHERE object_id = ANY(%s)
                LIMIT %s;
                """,
                (node_ids, limit),
            )
            rows = cur.fetchall()
        return [
            {
                "object_id": row[0],
                "object_name": row[1],
                "entity_type": row[2],
                "metadata": row[3],
            }
            for row in rows
        ]

    def _fetch_edges_by_ids(self, conn, edge_ids: list[int], limit: int = 50) -> list[dict[str, Any]]:
        if not edge_ids:
            return []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT relationship_id, relationship_name, relationship_cat, src_object_id, tar_object_id, metadata
                FROM object_relationship
                WHERE relationship_id = ANY(%s)
                LIMIT %s;
                """,
                (edge_ids, limit),
            )
            rows = cur.fetchall()
        return [
            {
                "relationship_id": row[0],
                "relationship_name": row[1],
                "relationship_cat": row[2],
                "src_object_id": row[3],
                "tar_object_id": row[4],
                "metadata": row[5],
            }
            for row in rows
        ]

    def _fetch_docs_by_ids(self, conn, doc_ids: list[int], limit: int = 50) -> list[dict[str, Any]]:
        if not doc_ids:
            return []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT doc_id, doc_key, doc_type, doc_cat, doc_path, doc_date::text, metadata, identifiers_kv
                FROM document
                WHERE doc_id = ANY(%s)
                LIMIT %s;
                """,
                (doc_ids, limit),
            )
            rows = cur.fetchall()
        return [
            {
                "doc_id": row[0],
                "doc_key": row[1],
                "doc_type": row[2],
                "doc_cat": row[3],
                "doc_path": row[4],
                "doc_date": row[5],
                "metadata": row[6],
                "identifiers": row[7],
            }
            for row in rows
        ]

    @staticmethod
    def _normalize_source_row_key(value: Any) -> str:
        text = re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower())
        return re.sub(r"\s+", "_", text).strip("_")

    def _relation_route_candidates(self, relation_name: str | None, attribute_name: str | None) -> list[str]:
        """Return ordered relation concepts for route exploration and backtracking.

        The engine first tries the explicit relation context, then a small set of
        semantically related variants (for example contact_person -> contact), and
        finally the requested attribute name as a last-resort concept. This lets a
        broad-scope lookup recover from a near-match relation name without falling
        back to a generic semantic lookup.
        """
        candidates: list[str] = []
        seen: set[str] = set()

        def _add(value: Any) -> None:
            text = str(value or "").strip()
            if not text:
                return
            normalized = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
            variants = []
            if text:
                variants.append(text)
            if normalized:
                variants.append(normalized)
                if "_" not in normalized and "-" not in normalized:
                    variants.append(normalized.replace("_", " "))
            if normalized and normalized.endswith("s") and len(normalized) > 3:
                variants.append(normalized[:-1])
            for variant in variants:
                if not variant:
                    continue
                key = str(variant).strip().lower()
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(str(variant).strip())

        def _add_route_variants(value: Any) -> None:
            text = str(value or "").strip()
            if not text:
                return
            _add(text)
            normalized = self._normalize_source_row_key(text)
            relation_key = normalized
            if relation_key in {"contact_person", "contact_person_name", "contact_name", "person_name"}:
                _add("contact_person")
                _add("contact")
            elif relation_key in {"shareholder", "shareholders", "owner", "owners"}:
                _add("shareholder")
                _add("owner")
            elif relation_key in {"director", "directors", "board_member", "board_members"}:
                _add("director")
                _add("board_member")
            elif relation_key in {"employee", "employees", "staff"}:
                _add("employee")
                _add("staff")

        relation_hint = str(relation_name or "").strip()
        attribute_hint = str(attribute_name or "").strip()

        if relation_hint:
            _add_route_variants(relation_hint)

        if attribute_hint:
            _add_route_variants(attribute_hint)

        return candidates

    def _score_relation_route_result(
        self,
        route_result: dict[str, Any] | None,
        requested_attribute: str | None,
        route_concept: str | None,
        relation_context: str | None,
    ) -> int:
        if not isinstance(route_result, dict):
            return -1000

        score = 0
        extracted = self._extract_attribute_value_from_related_payload(
            route_result.get("attribute_value"),
            requested_attribute,
            relation_context,
        )
        if extracted not in (None, "", [], {}):
            score += 120
        elif route_result.get("attribute_value") not in (None, "", [], {}):
            score += 30

        if str(route_concept or "").strip().lower() == str(relation_context or "").strip().lower():
            score += 20
        if str(route_concept or "").strip().lower() == str(requested_attribute or "").strip().lower():
            score += 10
        if str(route_result.get("relationship_concept") or "").strip().lower() == str(route_concept or "").strip().lower():
            score += 5
        return score

    def _explore_relation_lookup_paths(
        self,
        conn,
        entity_name: str,
        requested_attribute: str | None,
        relation_context: str | None,
        broad_scope: bool = False,
        max_depth: int = 2,
    ) -> tuple[dict[str, Any] | None, Any]:
        """Try a small search tree of relation concepts and keep the highest-scoring branch.

        This is a lightweight planner for chained relation phrasing such as
        "Z of Y of X": it explores alternate relation concepts, avoids loops via a
        visited set, and prefers branches that yield a non-empty extracted value.
        """
        if not entity_name:
            return None, None

        requested_attr = str(requested_attribute or "").strip()
        relation_hint = str(relation_context or "").strip()
        best_result: dict[str, Any] | None = None
        best_extracted: Any = None
        best_score = -10**9
        visited: set[str] = set()

        def _recurse(current_concept: str | None, depth: int, path: tuple[str, ...]) -> None:
            nonlocal best_result, best_extracted, best_score
            if depth > max_depth:
                return
            concept_text = str(current_concept or "").strip()
            if not concept_text:
                return
            concept_key = self._normalize_source_row_key(concept_text)
            if concept_key in visited:
                return
            visited.add(concept_key)

            candidate_concepts = self._relation_route_candidates(concept_text, requested_attr)
            for concept in candidate_concepts:
                normalized = self._normalize_source_row_key(concept)
                if normalized in visited and normalized != concept_key:
                    continue
                route_result = self._sql_generic_relationship_lookup(
                    conn,
                    entity_name,
                    concept,
                    broad_scope=broad_scope,
                )
                if not isinstance(route_result, dict):
                    continue

                extracted = self._extract_attribute_value_from_related_payload(
                    route_result.get("attribute_value"),
                    requested_attr,
                    relation_hint or concept_text,
                )
                score = self._score_relation_route_result(route_result, requested_attr, concept, relation_hint or concept_text)
                if score > best_score:
                    best_score = score
                    best_result = dict(route_result)
                    best_extracted = extracted

                if extracted in (None, "", [], {}) and depth < max_depth:
                    next_path = path + (concept,)
                    _recurse(concept, depth + 1, next_path)

            if depth == 0:
                for concept in [requested_attr, relation_hint]:
                    if not concept:
                        continue
                    candidate_key = self._normalize_source_row_key(concept)
                    if candidate_key in visited:
                        continue
                    route_result = self._sql_generic_relationship_lookup(
                        conn,
                        entity_name,
                        concept,
                        broad_scope=broad_scope,
                    )
                    if not isinstance(route_result, dict):
                        continue
                    extracted = self._extract_attribute_value_from_related_payload(
                        route_result.get("attribute_value"),
                        requested_attr,
                        relation_hint or concept,
                    )
                    score = self._score_relation_route_result(route_result, requested_attr, concept, relation_hint)
                    if score > best_score:
                        best_score = score
                        best_result = dict(route_result)
                        best_extracted = extracted

        _recurse(relation_hint or requested_attr or None, 0, ())
        if best_result is None:
            return None, None
        return best_result, best_extracted

    def _candidate_source_row_keys(self, attribute_name: str | None, relation_name: str | None = None) -> list[str]:
        raw_candidates = [str(attribute_name or "").strip(), str(relation_name or "").strip()]
        names: list[str] = []
        for item in raw_candidates:
            if not item:
                continue
            lower = item.lower()
            names.append(lower)
            names.extend(re.split(r"[^a-z0-9]+", lower))

        alias_map = {
            "telephone": ["telephone", "telephone_no", "telephone_number", "phone", "phone_number", "tel", "mobile", "mobile_no"],
            "phone": ["telephone", "telephone_no", "telephone_number", "phone", "phone_number", "tel", "mobile", "mobile_no"],
            "email": ["email", "email_address", "e_mail", "emailaddress"],
            "contact_person": ["contact_person", "contact_person_name", "contact_name", "person_name", "name"],
            "contact_person_name": ["contact_person", "contact_person_name", "contact_name", "person_name", "name"],
            "name": ["name", "contact_name", "person_name", "contact_person_name"],
            "company": ["company", "company_name", "organisation", "organization", "customer_name", "name"],
        }
        normalized_names = []
        for item in names:
            if not item:
                continue
            normalized_names.append(self._normalize_source_row_key(item))
            aliases = alias_map.get(item, [])
            for alias in aliases:
                normalized_names.append(self._normalize_source_row_key(alias))

        # Keep a stable, de-duplicated order while preserving likely human-readable aliases.
        seen: set[str] = set()
        ordered: list[str] = []
        for item in normalized_names:
            if not item or item in seen:
                continue
            seen.add(item)
            ordered.append(item)
        return ordered

    def _lookup_attribute_from_synchronized_source(
        self,
        conn,
        entity_name: str | None,
        attribute_name: str | None,
        relation_name: str | None = None,
    ) -> dict[str, Any] | None:
        entity_hint = str(entity_name or "").strip()
        attr_hint = str(attribute_name or "").strip()
        if not entity_hint or not attr_hint:
            return None

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT object_id
                FROM object_instance
                WHERE LOWER(COALESCE(object_name, '')) = LOWER(%s)
                  AND COALESCE(status, 'active') = 'active'
                  AND valid_from <= CURRENT_DATE
                  AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                ORDER BY entry_date DESC NULLS LAST, object_id DESC
                LIMIT 1;
                """,
                (entity_hint,),
            )
            entity_row = None
            if hasattr(cur, "fetchone"):
                entity_row = cur.fetchone()
            elif hasattr(cur, "fetchall"):
                rows = cur.fetchall() or []
                entity_row = rows[0] if rows else None

        if not entity_row or not entity_row[0]:
            return None

        object_id = int(entity_row[0])
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    sem.source_id,
                    sem.schema_name,
                    sem.table_name,
                    sem.source_pk_value,
                    sem.target_class_name,
                    sem.target_metadata,
                    sdr.source_key,
                    sdr.source_name
                FROM source_entity_mapping sem
                LEFT JOIN source_database_registry sdr
                  ON sdr.source_id = sem.source_id
                WHERE sem.target_object_id = %s
                  AND COALESCE(sem.target_metadata, '{}'::jsonb) IS NOT NULL
                ORDER BY sem.updated_at DESC NULLS LAST, sem.mapping_id DESC
                LIMIT 20;
                """,
                (object_id,),
            )
            mappings = cur.fetchall() or []

        if not mappings:
            return None

        raw_candidates = self._candidate_source_row_keys(attr_hint, relation_name)
        if not raw_candidates:
            return None

        for row in mappings:
            if not isinstance(row, (tuple, list)) or len(row) <= 5:
                continue
            target_metadata = row[5] if isinstance(row[5], dict) else {}
            source_row = target_metadata.get("source_row") if isinstance(target_metadata.get("source_row"), dict) else {}
            if not source_row:
                continue

            best_match = None
            best_score = -1
            for source_key, source_value in source_row.items():
                if source_value in (None, ""):
                    continue
                source_norm = self._normalize_source_row_key(source_key)
                for candidate in raw_candidates:
                    score = 0
                    if source_norm == candidate:
                        score = 100
                    elif candidate in source_norm or source_norm in candidate:
                        score = 60
                    else:
                        candidate_tokens = set(re.findall(r"[a-z0-9]+", candidate))
                        source_tokens = set(re.findall(r"[a-z0-9]+", source_norm))
                        overlap = len(candidate_tokens & source_tokens)
                        if overlap > 0:
                            score = overlap * 20
                    if score > best_score:
                        best_score = score
                        best_match = (source_key, source_value)

            if best_match is None:
                continue

            source_key = str(row[6] or "").strip() or "synchronized_source"
            source_name = str(row[7] or "").strip() or source_key
            source_row_key, source_value = best_match
            return {
                "entity_name": entity_hint,
                "attribute_name": attr_hint,
                "attribute_value": self._repair_mojibake_text(source_value),
                "source": "synchronized_source",
                "source_key": source_key,
                "source_name": source_name,
                "source_table": str(row[2] or "").strip() or None,
                "source_schema": str(row[1] or "").strip() or None,
                "source_pk_value": str(row[3] or "").strip() or None,
                "source_row_key": source_row_key,
            }

        return None

    def resolve_search_scope(self, question: str, limit: int = 12) -> dict[str, Any]:
        candidates = self.qdrant_candidates(question, limit=limit)
        discovery_results = self.discovery_candidates(question, limit=limit)
        object_ids = self._unique_ints([c.get("object_id") for c in candidates])
        relationship_ids = self._unique_ints([c.get("relationship_id") for c in candidates])
        doc_ids = self._unique_ints([c.get("doc_id") for c in candidates])

        objects: list[dict[str, Any]] = []
        relationships: list[dict[str, Any]] = []
        documents: list[dict[str, Any]] = []
        try:
            with self._get_connection() as conn:
                objects = self._fetch_nodes_by_ids(conn, object_ids, limit=limit)
                relationships = self._fetch_edges_by_ids(conn, relationship_ids, limit=limit)
                documents = self._fetch_docs_by_ids(conn, doc_ids, limit=limit)
        except Exception:
            # Degrade gracefully when database dependencies are unavailable.
            objects, relationships, documents = [], [], []

        return {
            "candidate_count": len(candidates),
            "discovery_candidate_count": len(discovery_results),
            "qdrant_search_mode": self._last_qdrant_search_mode,
            "qdrant_collection": self._last_qdrant_search_collection or self._qdrant_collection_name,
            "object_ids": object_ids,
            "relationship_ids": relationship_ids,
            "doc_ids": doc_ids,
            "objects": objects,
            "relationships": relationships,
            "documents": documents,
            "candidates": candidates,
            "discovery_results": discovery_results,
        }

    def lookup_attribute(
        self,
        attribute_name: str | None,
        entity_name: str | None = None,
        relation_name: str | None = None,
        scope: dict[str, Any] | None = None,
        criteria: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        raw_attr_key = str(attribute_name or "").strip().lower()
        # These virtual process-actor attributes must bypass schema resolution (which would remap them).
        process_actor_attrs = {"process_contact_person", "process_contact_email",
                               "responsible_company", "applicant_company", "responsible_firm"}
        if raw_attr_key in process_actor_attrs:
            normalized_attr, schema_kind = raw_attr_key, "attribute"
        elif raw_attr_key in {"registration_no", "uid", "uid_che", "che", "company_number", "company_registration_number"}:
            normalized_attr, schema_kind = "uid_che", "attribute"
        elif raw_attr_key == "total_capital":
            normalized_attr, schema_kind = "total_capital", "attribute"
        else:
            normalized_attr, schema_kind = self._resolve_schema_term(attribute_name, entity_name) if attribute_name else (None, None)
            if not normalized_attr:
                normalized_attr = self._normalize_attribute_name(attribute_name) if attribute_name else None
        resolved_relationship = self._resolve_relationship_name(
            relation_name or (attribute_name if schema_kind == "relationship" else None),
            entity_name,
        )
        # Use raw relation_name as process concept when it exists, since resolved form may differ
        raw_relation = str(relation_name or "").strip()

        scope_data = scope or {}
        node_ids = self._unique_ints(list(scope_data.get("node_ids") or []))
        edge_ids = self._unique_ints(list(scope_data.get("edge_ids") or []))
        doc_ids = self._unique_ints(list(scope_data.get("doc_ids") or []))
        discovery_results = list(scope_data.get("discovery_results") or [])
        strict_relation_lookup = bool(
            isinstance(criteria, dict)
            and criteria.get("strict_relation_lookup")
        )
        attribute_filters = self._attribute_filters_from_criteria(criteria)
        if strict_relation_lookup and normalized_attr in {"statute_date", "tr_date"}:
            # Statute/trade-register dates are often represented as object attributes
            # even when the planner emits relation filters from textual context.
            strict_relation_lookup = False
        temporal_scope = self._temporal_scope_from_criteria(criteria)
        address_semantic_attrs = {"address", "registered_address", "address_full", "anschrift", "adresse"}

        try:
            with self._get_connection() as conn:
                resolution_details = self._resolve_entity_name_for_lookup_details(conn, entity_name) if entity_name else None
                if resolution_details and resolution_details.get("ambiguous"):
                    return {
                        "resolution_status": "ambiguous_entity",
                        "entity_hint": resolution_details.get("entity_hint") or entity_name,
                        "entity_candidates": list(resolution_details.get("candidates") or []),
                        "top_score": resolution_details.get("top_score"),
                        "second_score": resolution_details.get("second_score"),
                        "score_gap": resolution_details.get("score_gap"),
                    }

                resolved_entity_name = (
                    str(resolution_details.get("resolved_name") or "").strip() if resolution_details else entity_name
                ) or entity_name

                # If entity_name was a near-match/typo and got canonicalized, retry
                # schema-term + relationship resolution against the canonical entity.
                if (
                    attribute_name
                    and resolved_entity_name
                    and str(resolved_entity_name).strip().lower() != str(entity_name or "").strip().lower()
                ):
                    retry_attr, retry_kind = self._resolve_schema_term(attribute_name, resolved_entity_name)
                    if retry_attr:
                        normalized_attr, schema_kind = retry_attr, retry_kind
                        if schema_kind == "relationship" and not relation_name:
                            resolved_relationship = self._resolve_relationship_name(retry_attr, resolved_entity_name)

                    # Keep relationship intent stable when relation-name matching depended on canonical entity.
                    retry_relation = self._resolve_relationship_name(
                        relation_name or (attribute_name if schema_kind == "relationship" else None),
                        resolved_entity_name,
                    )
                    if retry_relation:
                        resolved_relationship = retry_relation

                ownership_intent = self._is_ownership_lookup_intent(
                    raw_attr_key=raw_attr_key,
                    normalized_attr=normalized_attr,
                    relation_name=relation_name,
                    resolved_relationship=resolved_relationship,
                )

                relation_filters = []
                target_entity_class = ""
                if isinstance(criteria, dict):
                    relation_filters = list(criteria.get("relation_filters") or [])
                    target_entity_class = str(criteria.get("target_entity_class") or "").strip().lower()
                if normalized_attr and relation_filters:
                    relation_match = self._sql_attribute_lookup_with_relation_filters(
                        conn,
                        attribute_name=normalized_attr,
                        relation_filters=relation_filters,
                        target_entity_class=target_entity_class,
                    )
                    if relation_match:
                        return relation_match

                if resolved_entity_name and normalized_attr == "total_capital":
                    share_data = self._sql_shareholder_lookup(conn, resolved_entity_name)
                    if share_data and isinstance(share_data.get("attribute_value"), list):
                        total_capital = 0.0
                        has_value = False
                        for holder in list(share_data.get("attribute_value") or []):
                            if not isinstance(holder, dict):
                                continue
                            value = holder.get("share_total_nominal_value_chf")
                            if value in (None, ""):
                                value = holder.get("share_nominal_chf")
                            try:
                                numeric = float(value)
                            except (TypeError, ValueError):
                                continue
                            total_capital += numeric
                            has_value = True
                        if has_value:
                            return {
                                "entity_name": str(share_data.get("entity_name") or resolved_entity_name),
                                "attribute_name": "total_capital",
                                "attribute_value": round(total_capital, 2),
                                "currency": "CHF",
                                "doc_id": share_data.get("doc_id"),
                                "doc_name": share_data.get("doc_name"),
                                "doc_path": share_data.get("doc_path"),
                            }

                # ── Generic process-actor lookup ────────────────────────────────────────
                # Handles process_contact_person, process_contact_email, responsible_company,
                # and the legacy eori_contact_email — for any process concept (EORI, customs, VAT, …)
                process_concept = raw_relation or str(resolved_relationship or "").strip()

                if resolved_entity_name and normalized_attr in {
                    "process_contact_person", "process_contact_email",
                } and process_concept:
                    contact = self._sql_process_contact_lookup(conn, resolved_entity_name, process_concept)
                    if contact:
                        if not contact.get("contact_email") and str(contact.get("attribute_value") or "").strip():
                            email_match = self.sql_exact_attribute_lookup(
                                conn,
                                str(contact.get("attribute_value") or "").strip(),
                                "email",
                            )
                            if email_match and email_match.get("attribute_value") not in (None, ""):
                                contact["contact_email"] = email_match.get("attribute_value")
                        if normalized_attr == "process_contact_email":
                            return {
                                "entity_name": contact["entity_name"],
                                "attribute_name": "process_contact_email",
                                "attribute_value": contact.get("contact_email"),
                                "contact_person": contact["attribute_value"],
                                "relationship_name": contact["relationship_name"],
                                "doc_id": contact.get("doc_id"),
                                "doc_name": contact.get("doc_name"),
                                "doc_path": contact.get("doc_path"),
                            }
                        return contact

                if resolved_entity_name and normalized_attr in {
                    "responsible_company", "applicant_company", "responsible_firm",
                } and process_concept:
                    contact = self._sql_process_contact_lookup(conn, resolved_entity_name, process_concept)
                    if contact:
                        employer = self._sql_infer_employer_company(
                            conn,
                            contact["attribute_value"],
                            contact.get("contact_email"),
                            exclude_name=resolved_entity_name,
                        )
                        if employer:
                            return {
                                "entity_name": contact["entity_name"],
                                "attribute_name": "responsible_company",
                                "attribute_value": employer,
                                "contact_person": contact["attribute_value"],
                                "relationship_name": contact["relationship_name"],
                                "doc_id": contact.get("doc_id"),
                                "doc_name": contact.get("doc_name"),
                                "doc_path": contact.get("doc_path"),
                            }

                # Legacy eori_contact_email: now routed through generic process contact lookup
                if resolved_entity_name and normalized_attr == "eori_contact_email":
                    contact = self._sql_process_contact_lookup(conn, resolved_entity_name, "eori")
                    if contact and contact.get("contact_email"):
                        return {
                            "entity_name": contact["entity_name"],
                            "attribute_name": "eori_contact_email",
                            "attribute_value": contact["contact_email"],
                            "contact_person": contact["attribute_value"],
                            "relationship_name": contact["relationship_name"],
                            "doc_id": contact.get("doc_id"),
                            "doc_name": contact.get("doc_name"),
                            "doc_path": contact.get("doc_path"),
                        }

                # Legacy eori_contact_person: route through the same generic process
                # contact lookup used for process_contact_person so multi-attribute
                # contact queries can resolve name + email consistently.
                if resolved_entity_name and normalized_attr == "eori_contact_person":
                    contact = self._sql_process_contact_lookup(conn, resolved_entity_name, "eori")
                    if contact and contact.get("attribute_value"):
                        return {
                            "entity_name": contact["entity_name"],
                            "attribute_name": "eori_contact_person",
                            "attribute_value": contact["attribute_value"],
                            "contact_email": contact.get("contact_email"),
                            "relationship_name": contact["relationship_name"],
                            "doc_id": contact.get("doc_id"),
                            "doc_name": contact.get("doc_name"),
                            "doc_path": contact.get("doc_path"),
                        }
                # ── End generic process-actor lookup ────────────────────────────────────

                if entity_name and resolved_relationship:
                    related = self._lookup_related_attribute_via_relationship(conn, resolved_entity_name or entity_name, resolved_relationship, normalized_attr)
                    if related:
                        return related

                if resolved_entity_name and ownership_intent:
                    explicit_owners = self._sql_shareholder_lookup(conn, resolved_entity_name)
                    if explicit_owners:
                        return self._normalize_temporal_lookup_result(explicit_owners, normalized_attr, temporal_scope)
                    governance_roles = self._sql_governance_roles_lookup(conn, resolved_entity_name)
                    if governance_roles:
                        return self._normalize_temporal_lookup_result(governance_roles, normalized_attr, temporal_scope)

                # For strict relationship-driven queries, keep the lookup relation-aware.
                # If a relation context is present, allow synchronized-source and
                # relationship-payload fallback to satisfy the requested attribute.
                if strict_relation_lookup and not relation_name:
                    return None

                if resolved_entity_name and normalized_attr in {
                    "phd_institution",
                    "education",
                    "university",
                    "institution",
                    "degree",
                    "degrees",
                    "qualification",
                    "qualifications",
                    "qualitification",
                    "diploma",
                    "diplomas",
                    "certificate",
                    "certificates",
                    "certification",
                }:
                    edu_result = self._lookup_person_education_institution(conn, resolved_entity_name, normalized_attr)
                    if edu_result:
                        return edu_result

                broad_scope = bool(isinstance(criteria, dict) and criteria.get("broad_scope"))
                if resolved_entity_name and normalized_attr and normalized_attr not in address_semantic_attrs:
                    if not attribute_filters:
                        relation_context = str(relation_name or resolved_relationship or "").strip()
                        if relation_context and (strict_relation_lookup or broad_scope or normalized_attr in {"email", "telephone", "phone", "mobile", "contact_person", "name"}):
                            relation_like_attrs = {"contact_person", "contact_person_name", "person_name", "contact_name", "name"}

                            if normalized_attr in relation_like_attrs:
                                explored_route, extracted = self._explore_relation_lookup_paths(
                                    conn,
                                    resolved_entity_name,
                                    normalized_attr,
                                    relation_context,
                                    broad_scope=broad_scope,
                                )
                                if explored_route is not None and extracted not in (None, "", [], {}):
                                    result = dict(explored_route)
                                    result["attribute_name"] = normalized_attr
                                    result["attribute_value"] = extracted
                                    return self._normalize_temporal_lookup_result(result, normalized_attr, temporal_scope)

                            synchronized_result = self._lookup_attribute_from_synchronized_source(
                                conn,
                                resolved_entity_name,
                                normalized_attr,
                                relation_name=relation_context,
                            )
                            if synchronized_result:
                                return self._normalize_temporal_lookup_result(synchronized_result, normalized_attr, temporal_scope)

                            explored_route, extracted = self._explore_relation_lookup_paths(
                                conn,
                                resolved_entity_name,
                                normalized_attr,
                                relation_context,
                                broad_scope=broad_scope,
                            )
                            if explored_route is not None and extracted not in (None, "", [], {}):
                                result = dict(explored_route)
                                result["attribute_name"] = normalized_attr
                                result["attribute_value"] = extracted
                                return self._normalize_temporal_lookup_result(result, normalized_attr, temporal_scope)

                # Generic relationship-concept lookup: works for ANY relationship stored
                # in object_relationship, not just hard-coded ones like shareholders.
                if resolved_entity_name and normalized_attr and normalized_attr not in address_semantic_attrs:
                    if not attribute_filters:
                        broad_scope = bool(isinstance(criteria, dict) and criteria.get("broad_scope"))
                        relation_context = str(relation_name or resolved_relationship or "").strip()
                        explored_route, extracted = self._explore_relation_lookup_paths(
                            conn,
                            resolved_entity_name,
                            normalized_attr,
                            relation_context,
                            broad_scope=broad_scope,
                        )
                        if explored_route is not None:
                            if extracted not in (None, "", [], {}):
                                result = dict(explored_route)
                                result["attribute_name"] = normalized_attr
                                result["attribute_value"] = extracted
                                return self._normalize_temporal_lookup_result(result, normalized_attr, temporal_scope)
                            if str(normalized_attr or "").strip().lower() in {
                                str(explored_route.get("attribute_name") or "").strip().lower(),
                                str(explored_route.get("relationship_concept") or "").strip().lower(),
                            }:
                                return self._normalize_temporal_lookup_result(explored_route, normalized_attr, temporal_scope)

                if resolved_entity_name:
                    synchronized_result = self._lookup_attribute_from_synchronized_source(
                        conn,
                        resolved_entity_name,
                        normalized_attr,
                        relation_name=resolved_relationship or None,
                    )
                    if synchronized_result:
                        return self._normalize_temporal_lookup_result(synchronized_result, normalized_attr, temporal_scope)

                    if not attribute_filters:
                        direct = self.sql_exact_attribute_lookup(
                            conn,
                            resolved_entity_name,
                            normalized_attr,
                        )
                        if direct:
                            return self._normalize_temporal_lookup_result(direct, normalized_attr, temporal_scope)

                    # Attribute storage compatibility fallback: some datasets store
                    # statute-style dates under trade-register date (`tr_date`).
                    if normalized_attr == "statute_date" and not attribute_filters:
                        fallback_direct = self.sql_exact_attribute_lookup(
                            conn,
                            resolved_entity_name,
                            "tr_date",
                        )
                        if fallback_direct:
                            fallback_direct = dict(fallback_direct)
                            fallback_direct["attribute_name"] = "statute_date"
                            return self._normalize_temporal_lookup_result(fallback_direct, normalized_attr, temporal_scope)

                    if normalized_attr not in address_semantic_attrs:
                        relationship_name = self._match_entity_relationship_name(resolved_entity_name, normalized_attr)
                        if relationship_name:
                            related = self._sql_related_object_lookup(conn, resolved_entity_name, relationship_name)
                            if related:
                                return self._normalize_temporal_lookup_result(related, normalized_attr, temporal_scope)

                    structured = self._lookup_attribute_in_structured_tables(
                        conn,
                        resolved_entity_name,
                        normalized_attr,
                        relation_name=resolved_relationship or None,
                        scope_doc_ids=doc_ids,
                        attribute_filters=attribute_filters,
                    )
                    if structured:
                        return self._normalize_temporal_lookup_result(structured, normalized_attr, temporal_scope)

                    if normalized_attr == "statute_date":
                        structured_fallback = self._lookup_attribute_in_structured_tables(
                            conn,
                            resolved_entity_name,
                            "tr_date",
                            relation_name=resolved_relationship or None,
                            scope_doc_ids=doc_ids,
                            attribute_filters=attribute_filters,
                        )
                        if structured_fallback:
                            structured_fallback = dict(structured_fallback)
                            structured_fallback["attribute_name"] = "statute_date"
                            return self._normalize_temporal_lookup_result(structured_fallback, normalized_attr, temporal_scope)

                parsed = ParsedQuery(
                    intent="attribute_lookup",
                    entity_name=resolved_entity_name or entity_name,
                    attribute_name=normalized_attr,
                    confidence=0.8,
                )
                if not attribute_filters:
                    return self.sql_fallback_from_ids(
                        conn=conn,
                        parsed=parsed,
                        node_ids=node_ids,
                        edge_ids=edge_ids,
                        doc_ids=doc_ids,
                    )
        except Exception as exc:
            import traceback
            print('LOOKUP_EXCEPTION', repr(exc), flush=True)
            print(traceback.format_exc(), flush=True)
            raise

        discovery_match = self._lookup_attribute_in_discovery_results(
            attribute_name=normalized_attr,
            entity_name=entity_name,
            results=discovery_results,
        )
        if discovery_match:
            return discovery_match

        return None

    def _sql_attribute_lookup_with_relation_filters(
        self,
        conn,
        attribute_name: str,
        relation_filters: list[dict[str, Any]],
        target_entity_class: str | None = None,
    ) -> dict[str, Any] | None:
        valid_filters: list[tuple[list[str], str]] = []
        for item in relation_filters:
            if not isinstance(item, dict):
                continue
            rel_names = item.get("relationship_names")
            if isinstance(rel_names, str):
                rel_names = [rel_names]
            rel_list = [str(name).strip().lower() for name in list(rel_names or []) if str(name).strip()]
            target_name = str(item.get("target_name") or item.get("target") or "").strip()
            if rel_list and target_name:
                valid_filters.append((rel_list, target_name))

        if not valid_filters or not attribute_name:
            return None

        attr_candidates = [str(attribute_name).strip().lower()]
        if attr_candidates[0] == "due_date":
            for alias in ["payment_due_date", "faelligkeitsdatum", "fälligkeitsdatum"]:
                if alias not in attr_candidates:
                    attr_candidates.append(alias)

        where_parts = []
        params: list[Any] = [attr_candidates]
        if target_entity_class:
            where_parts.append("LOWER(COALESCE(oi.class_name, '')) = %s")
            params.append(target_entity_class)

        for idx, (rel_names, target_name) in enumerate(valid_filters):
            where_parts.append(
                f"""
                EXISTS (
                    SELECT 1
                    FROM object_relationship r{idx}
                    JOIN object_instance t{idx}
                      ON t{idx}.object_id = r{idx}.tar_object_id
                    WHERE r{idx}.src_object_id = oi.object_id
                      AND LOWER(COALESCE(r{idx}.relationship_name, '')) = ANY(%s)
                      AND LOWER(COALESCE(t{idx}.object_name, '')) LIKE LOWER(%s)
                )
                """
            )
            params.extend([rel_names, f"%{target_name}%"])

        where_clause = " AND ".join(where_parts) if where_parts else "TRUE"
        sql = f"""
            SELECT
                oi.object_id,
                oi.object_name,
                oi.class_name,
                a.attr_type,
                a.attr_json->>'value' AS attr_value,
                d.doc_id,
                d.doc_key,
                d.doc_path,
                d.doc_date::text
            FROM object_instance oi
            JOIN attribute a
              ON a.src_type = 'object'
             AND a.src_id = oi.object_id
             AND LOWER(COALESCE(a.attr_type, '')) = ANY(%s)
            LEFT JOIN part p
              ON p.object_id = oi.object_id
            LEFT JOIN document d
              ON d.doc_id = p.doc_id
            WHERE {where_clause}
              AND COALESCE(oi.status, 'active') = 'active'
              AND oi.valid_from <= CURRENT_DATE
              AND (oi.valid_until IS NULL OR oi.valid_until > CURRENT_DATE)
              AND a.valid_from <= CURRENT_DATE
              AND (a.valid_until IS NULL OR a.valid_until > CURRENT_DATE)
            ORDER BY a.entry_date DESC NULLS LAST, oi.entry_date DESC
            LIMIT 1;
        """
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
        if not row or row[4] in (None, ""):
            return None

        return {
            "object_id": row[0],
            "entity_name": row[1],
            "entity_type": row[2],
            "attribute_name": row[3] or attribute_name,
            "attribute_value": row[4],
            "doc_id": row[5],
            "doc_name": row[6],
            "doc_path": row[7],
            "doc_date": row[8],
        }

    def _build_ambiguous_entity_response(
        self,
        question: str,
        intent: str,
        language: str,
        ambiguity_data: dict[str, Any],
    ) -> dict[str, Any]:
        entity_hint = str(ambiguity_data.get("entity_hint") or "").strip() or "the requested entity"
        raw_candidates = list(ambiguity_data.get("entity_candidates") or [])
        candidate_names = [str(item).strip() for item in raw_candidates if str(item).strip()][:5]
        listed = ", ".join(candidate_names) if candidate_names else "no candidates"
        reason = str(ambiguity_data.get("reason") or "").strip().lower()

        advisory = (
            "If these refer to the same person, you can link aliases first and merge records only after confirmation."
        )

        if str(language or "en").lower() == "de":
            answer_text = (
                f"Der Name '{entity_hint}' ist mehrdeutig. "
                f"Bitte präzisieren Sie die Entität. Kandidaten: {listed}. {advisory}"
            )
        else:
            answer_text = (
                f"The name '{entity_hint}' is ambiguous. "
                f"Please specify which entity you mean. Candidates: {listed}. {advisory}"
            )

        return {
            "question": question,
            "intent": intent,
            "answer": self._prefix_source(answer_text, "entity_ambiguous"),
            "data": {
                "resolution_status": "ambiguous_entity",
                "entity_hint": entity_hint,
                "entity_candidates": candidate_names,
                "top_score": ambiguity_data.get("top_score"),
                "second_score": ambiguity_data.get("second_score"),
                "score_gap": ambiguity_data.get("score_gap"),
                "reason": reason or "ambiguous_entity",
                "advisory": advisory,
            },
            "source": "entity_ambiguous",
            "candidate_count": len(candidate_names),
        }

    @staticmethod
    def _contains_singular_resolution_intent(question: str) -> bool:
        text = str(question or "").strip().lower()
        if not text:
            return False
        markers = (
            " current ", " latest ", " exact ", " precise ", " specific ", " full ",
            " aktuell ", " letzte ", " letzter ", " genau ", " präzise ", " praezise ", " vollständig ", " vollstaendig ",
        )
        wrapped = f" {text} "
        return any(marker in wrapped for marker in markers)

    @staticmethod
    def _contains_plural_resolution_intent(question: str) -> bool:
        text = str(question or "").strip().lower()
        if not text:
            return False
        markers = (
            " all ", " each ", " every ", " list ", " enumerate ", " candidates ", " options ",
            " alle ", " jeweils ", " liste ", " auflisten ", " mehrere ",
        )
        wrapped = f" {text} "
        return any(marker in wrapped for marker in markers)

    @staticmethod
    def _criteria_question_prefers_listing(question: str, response_format: str = "") -> bool:
        """Detect list-style document queries that should stay deterministic."""
        fmt = str(response_format or "").strip().lower()
        if fmt in {"doc_titles_and_files", "entity_directory"}:
            return True

        text = str(question or "").strip().lower()
        if not text:
            return False

        listing_markers = (
            " list ", " show ", " give me ", " enumerate ", " titles ", " title ",
            " file names ", " filename ", " filenames ", " paths ", " path ",
            " liste ", " zeige ", " dateinamen ", " dateiname ", " pfad ", " pfade ",
        )
        value_markers = (
            " how much ", " what is ", " what was ", " what are ", " which is ",
            " wie viel ", " wie hoch ", " was ist ", " was war ",
            " amount ", " total ", " cost ", " price ", " value ", " due ", " balance ", " sum ",
            " betrag ", " gesamt ", " kosten ", " preis ",
        )

        wrapped = f" {text} "
        has_listing = any(marker in wrapped for marker in listing_markers)
        has_value = any(marker in wrapped for marker in value_markers)
        return bool(has_listing and not has_value)

    def _query_document_type_anchors(self, question: str, parsed: ParsedQuery | None = None) -> set[str]:
        """Extract canonical document/business object anchors from query text and parsed criteria."""
        text = str(question or "").strip().lower()
        anchors: set[str] = set()
        if not text:
            return anchors

        for raw_token, mapped in self._ENTITY_TYPE_MAP.items():
            canonical = str((mapped[0] if isinstance(mapped, tuple) and mapped else "") or "").strip().lower()
            if not canonical:
                continue
            if re.search(rf"\b{re.escape(str(raw_token).lower())}\b", text):
                anchors.add(canonical)

        if isinstance(parsed, ParsedQuery) and isinstance(parsed.criteria, dict):
            criteria_entity_type = str(parsed.criteria.get("entity_type") or "").strip().lower()
            if criteria_entity_type:
                anchors.add(criteria_entity_type)

        return anchors

    def _question_requests_document_listing(self, question: str, parsed: ParsedQuery | None = None) -> bool:
        """Detect list/temporal requests over document-like nouns (e.g. 'last invoices')."""
        text = str(question or "").strip().lower()
        if not text:
            return False
        if self._is_value_document_context_question(question):
            return False

        anchors = self._query_document_type_anchors(question, parsed)
        document_like = {"invoice", "receipt", "document", "contract", "report", "payment", "transaction", "expense"}
        if not (anchors & document_like):
            return False

        has_temporal = bool(
            re.search(
                r"\b(?:last|latest|recent|new|newest|current|previous|prior|oldest|earliest|letzte[nmrs]?|aktuell(?:e|en|er|es)?)\b",
                text,
            )
        )
        has_listing = bool(
            re.search(
                r"\b(?:list|show|display|enumerate|get|find|what\s+are|which\s+are|give\s+me|liste|zeige|auflisten|welche\s+sind|was\s+sind)\b",
                text,
            )
        )
        return bool(has_temporal or has_listing)

    @staticmethod
    def _attribute_relevance_bucket(attribute_name: str | None) -> str:
        attr = str(attribute_name or "").strip().lower()
        if not attr:
            return "unknown"

        profile_attrs = {
            "purpose", "publication_organ", "tr_number", "tr_date", "statute_date",
            "registration_no", "uid_che", "total_capital", "registered_address",
        }
        if attr in profile_attrs:
            return "entity_profile"
        if attr.startswith("document_"):
            return "document_metadata"
        if attr in {"description", "generic_doc3"}:
            return "document_content"
        if any(token in attr for token in ("invoice", "bill", "receipt", "payment", "due_date", "vat", "tax", "gross", "net", "amount", "total")):
            return "financial_document"
        return "generic"

    def _result_is_relevance_compatible(
        self,
        question: str,
        parsed: ParsedQuery,
        result: dict[str, Any] | None,
    ) -> tuple[bool, str]:
        """Return (is_compatible, reason) for high-confidence scalar result acceptance."""
        if not isinstance(result, dict):
            return True, "no_result"
        if parsed.intent != "attribute_lookup":
            return True, "non_attribute_intent"
        if not self._question_requests_document_listing(question, parsed):
            return True, "no_document_listing_anchor"

        attr_name = str(result.get("attribute_name") or parsed.attribute_name or parsed.relation_name or "").strip().lower()
        if not attr_name:
            return True, "no_attribute_name"

        # Explicitly requested attributes stay deterministic.
        explicit_attr = self._explicit_attribute_from_text(question)
        if explicit_attr and explicit_attr == attr_name:
            return True, "explicit_attribute_requested"

        bucket = self._attribute_relevance_bucket(attr_name)
        if bucket == "entity_profile":
            return False, f"document_listing_query_incompatible_with_{attr_name}"

        return True, "compatible"

    def _collect_query_evidence(self, question: str, parsed: ParsedQuery) -> dict[str, Any]:
        """Collect deterministic evidence anchors used to permit weak fallback routing."""
        text = str(question or "").strip()
        lowered = text.lower()

        entity_hits: list[str] = []
        schema_hits: list[str] = []
        document_type_anchors = sorted(self._query_document_type_anchors(question, parsed))
        relation_anchors: list[str] = []
        weak_hints: dict[str, Any] = {}

        canonical_attrs = {str(item).strip().lower() for item in self._canonical_attribute_names() if str(item).strip()}
        alias_values = {
            str(item).strip().lower()
            for item in list(self._schema_alias_fallback_map().values())
            if str(item).strip()
        }

        def _is_grounded_schema_term(name: str | None, kind: str | None, candidate_text: str | None = None) -> bool:
            canonical = str(name or "").strip().lower()
            k = str(kind or "").strip().lower()
            candidate_key = self._normalize_alias_key(str(candidate_text or "").strip())
            if not canonical or not k:
                return False
            if k == "attribute":
                return bool(canonical in canonical_attrs or canonical in alias_values or (candidate_key and candidate_key in self._schema_alias_fallback_map()))
            if k == "relationship":
                if parsed.entity_name:
                    try:
                        if self._match_entity_relationship_name(parsed.entity_name, canonical):
                            return True
                    except Exception:
                        pass
                return bool(canonical in alias_values or (candidate_key and candidate_key in self._schema_alias_fallback_map()))
            return False

        resolved_db_entity = self._resolve_entity_mention_from_db(question)
        if resolved_db_entity:
            entity_hits.append(str(resolved_db_entity).strip())

        spacy_entity = self._extract_entity_via_spacy(question)
        if spacy_entity and spacy_entity not in entity_hits:
            entity_hits.append(str(spacy_entity).strip())

        parsed_entity = str(parsed.entity_name or "").strip()
        if parsed_entity and parsed_entity.lower() in lowered:
            weak_hints["parsed_entity_mention"] = parsed_entity

        attr_candidates: list[str] = []
        explicit_attr = self._explicit_attribute_from_text(text)
        if explicit_attr:
            attr_candidates.append(str(explicit_attr).strip())
        attr_candidates.extend(self._attribute_rescue_candidates_from_text(text))
        attr_candidates.extend(self._extract_attribute_phrases_via_spacy(text, parsed.entity_name))

        seen_candidates: set[str] = set()
        normalized_attr = str(parsed.attribute_name or "").strip()
        if normalized_attr:
            weak_hints["parsed_attribute"] = normalized_attr

        for candidate in attr_candidates[:30]:
            phrase = str(candidate or "").strip()
            if not phrase:
                continue
            key = phrase.lower()
            if key in seen_candidates:
                continue
            seen_candidates.add(key)
            resolved_name, resolved_kind = self._resolve_schema_term(phrase, parsed.entity_name)
            if resolved_name and resolved_kind and _is_grounded_schema_term(resolved_name, resolved_kind, phrase):
                schema_hit = f"{resolved_kind}:{resolved_name}"
                if schema_hit not in schema_hits:
                    schema_hits.append(schema_hit)

        if parsed.relation_name and _is_grounded_schema_term(parsed.relation_name, "relationship", parsed.relation_name):
            relation_anchors.append(str(parsed.relation_name).strip())
        elif parsed.relation_name:
            weak_hints["parsed_relationship"] = str(parsed.relation_name).strip()

        parsed_criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        for key in ("relation_filters", "relationship_filters"):
            rel_items = parsed_criteria.get(key)
            if not isinstance(rel_items, list):
                continue
            for item in rel_items:
                if not isinstance(item, dict):
                    continue
                rel_name = str(item.get("relationship_name") or "").strip()
                if rel_name and _is_grounded_schema_term(rel_name, "relationship", rel_name) and rel_name not in relation_anchors:
                    relation_anchors.append(rel_name)

        external_source_pattern_hit = bool(self._extract_federated_list_query(question) or self._build_generic_federated_entity_plan(question))
        criteria_anchor = bool(
            parsed.intent == "criteria_lookup"
            and (
                str(parsed_criteria.get("entity_type") or "").strip()
                or bool(parsed_criteria.get("must_contain"))
            )
        )

        categories = {
            "entity": bool(entity_hits),
            "schema": bool(schema_hits),
            "document_type": bool(document_type_anchors),
            "relationship": bool(relation_anchors),
            "criteria": bool(criteria_anchor),
            "external_pattern": bool(external_source_pattern_hit),
        }
        matched_categories = [name for name, ok in categories.items() if ok]

        allow_semantic_matching = bool(matched_categories or weak_hints)

        return {
            "entity_hits": entity_hits[:6],
            "schema_hits": schema_hits[:12],
            "document_type_anchors": document_type_anchors[:8],
            "relationship_anchors": relation_anchors[:8],
            "criteria_anchor": criteria_anchor,
            "external_source_pattern_hit": external_source_pattern_hit,
            "categories": categories,
            "matched_categories": matched_categories,
            "matched_category_count": len(matched_categories),
            "has_any_anchor": bool(matched_categories),
            "allow_semantic_matching": allow_semantic_matching,
            "weak_hints": weak_hints,
        }

    def _build_no_evidence_not_found_message(self, question: str) -> str:
        if not STRICT_QUERY_EVIDENCE_REQUIRE_EXPLANATION:
            return "I could not find a confident answer in IDMS_demo."

        return (
            "I could not find a grounded match for this query in IDMS_demo. "
            "No reliable anchors were found for entity names, schema attributes/relationships, "
            "document categories/types, or external source patterns. "
            "Please include at least one concrete entity, document type/category, relationship, or field name."
        )

    @staticmethod
    def _is_value_document_context_question(question: str) -> bool:
        """Detect value-style questions that should use document-context resolution."""
        text = str(question or "").strip().lower()
        if not text:
            return False

        value_markers = (
            " how much ", " what is ", " what was ", " amount ", " total ", " cost ", " price ", " value ",
            " due ", " balance ", " sum ", " charge ", " fee ", " quotation ", " quote ", " offer ",
            " wie viel ", " wie hoch ", " betrag ", " gesamt ", " kosten ", " preis ",
        )
        context_markers = (
            " document ", " documents ", " note ", " notes ", " invoice ", " bill ", " receipt ",
            " registration ", " registered ", " contract ", " order ", " payment ", " statement ",
            " related to ", " connected to ", " linked to ", " about ", " regarding ",
            " service ", " services ", " mobile ", " phone ", " telephone ", " telecom ", " telecommunication ",
            " dokument ", " dokumente ", " notiz ", " notizen ", " rechnung ", " registrierung ",
            " dienst ", " dienste ", " mobil ", " telefon ", " telekom ",
            " bezogen auf ", " im zusammenhang mit ", " verbunden mit ",
        )
        table_only_markers = (
            " in expenditure ", " in cumulative ", " in table ", " row ", " column ", " line item ",
            " in der tabelle ", " zeile ", " spalte ",
        )

        wrapped = f" {text} "
        has_value = any(marker in wrapped for marker in value_markers)
        has_context = any(marker in wrapped for marker in context_markers)
        table_only = any(marker in wrapped for marker in table_only_markers)
        return bool(has_value and has_context and not table_only)

    @staticmethod
    def _has_multi_context_preposition_chain(question: str) -> bool:
        """Detect chained contextual phrases like '<value> for <topic> for <entity>'."""
        text = str(question or "").strip().lower()
        if not text:
            return False

        parts = re.split(r"\b(?:for|of|in|von|fuer|für|about|regarding|concerning)\b", text, flags=re.IGNORECASE)
        meaningful = [
            re.sub(r"\s+", " ", str(part or "").strip())
            for part in parts
            if re.sub(r"\s+", " ", str(part or "").strip())
        ]
        if len(meaningful) < 3:
            return False

        # Require that at least two trailing contextual chunks contain noun-like content.
        trailing = meaningful[1:]
        content_chunks = 0
        for chunk in trailing:
            tokens = re.findall(r"[a-zA-ZäöüÄÖÜß0-9]{3,}", chunk)
            if len(tokens) >= 1:
                content_chunks += 1
        return content_chunks >= 2

    def _requires_contextual_value_resolution(
        self,
        question: str,
        parsed: ParsedQuery,
        criteria: dict[str, Any] | None,
        direct_result: dict[str, Any] | None,
    ) -> bool:
        """Generic guard to avoid premature scalar answers for contextual value queries."""
        if parsed.intent not in {"attribute_lookup", "criteria_lookup"}:
            return False
        if not isinstance(direct_result, dict):
            return False

        text = str(question or "").strip().lower()
        if not text:
            return False

        # Keep explicit table/cell requests deterministic.
        if re.search(r"\b(?:row|column|cell|table|line\s*item|zeile|spalte|tabelle)\b", text):
            return False

        value_markers = {
            "amount", "total", "cost", "price", "value", "due", "sum", "charge", "fee",
            "betrag", "gesamt", "kosten", "preis", "wert", "summe",
        }
        telecom_markers = {
            "phone", "telephone", "mobile", "telecom", "telecommunication",
            "telefon", "mobil", "telekommunikation",
        }
        billing_markers = {
            "bill", "invoice", "receipt", "statement", "payment", "charge", "fee",
            "rechnung", "beleg", "zahlung", "quittung",
        }
        attr_name = str(parsed.attribute_name or direct_result.get("attribute_name") or "").strip().lower()
        asks_value = bool(any(marker in text for marker in value_markers))
        attr_is_value = bool(
            attr_name in {
                "amount", "net_amount", "gross_amount", "tax_amount", "total_amount",
                "amount_chf", "amount_total_due", "unit_price", "unit_price_cny", "unit_price_chf",
                "cost", "price", "value",
            }
        )

        asks_how_much = " how much " in f" {text} "
        has_telecom_context = any(marker in text for marker in telecom_markers)
        has_billing_context = any(marker in text for marker in billing_markers)
        if asks_how_much and has_telecom_context and has_billing_context:
            asks_value = True

        if not asks_value and not attr_is_value:
            return False

        # If explicit filters exist, allow direct answers only when evidence satisfies them.
        # Otherwise keep routing on contextual resolution to avoid premature scalar matches.
        if self._attribute_filters_from_criteria(criteria):
            try:
                if self._result_satisfies_attribute_filters(direct_result, criteria):
                    return False
            except Exception:
                pass

        entity_hint = parsed.entity_name or self._best_entity_hint(question)
        if not str(entity_hint or "").strip():
            return False

        # Strong signal: value question with explicit document/service context
        # should avoid early scalar shortcuts and use contextual grounding.
        if self._is_value_document_context_question(question):
            return True

        # Generic contextual chain signal: multiple context-bearing preposition phrases.
        if not self._has_multi_context_preposition_chain(question):
            return False

        return True

    def _collect_criteria_doc_evidence(
        self,
        criteria_result: dict[str, Any],
        limit_docs: int = 8,
        question: str | None = None,
    ) -> list[dict[str, Any]]:
        """Collect compact document evidence snippets from criteria matches."""
        matches = list(criteria_result.get("matches") or []) if isinstance(criteria_result, dict) else []
        if not matches:
            return []

        q_norm = str(question or "").strip().lower()
        wants_telecom = bool(re.search(r"\b(?:telephone|phone|mobile|telecom|telefon|mobil|telekommunikation)\b", q_norm, flags=re.IGNORECASE))
        wants_billing = bool(re.search(r"\b(?:bill|invoice|receipt|payment|charge|fee|rechnung|beleg|zahlung|quittung)\b", q_norm, flags=re.IGNORECASE))

        finance_tokens = {
            "invoice", "bill", "receipt", "payment", "quotation", "quote", "offer",
            "rechnung", "beleg", "zahlung", "offerte",
            "amount", "total", "due", "cost", "price", "value", "sum", "fee", "charge",
            "betrag", "gesamt", "kosten", "preis", "wert", "summe", "gebuhr", "gebuehr",
        }

        def _match_rank(item: dict[str, Any]) -> float:
            base = float(item.get("score") or 0.0)
            text_blob = " ".join(
                [
                    str(item.get("title") or ""),
                    str(item.get("doc_name") or ""),
                    str(item.get("doc_path") or ""),
                    str(item.get("doc_theme") or ""),
                    str(item.get("doc_cat") or ""),
                    str(item.get("doc_type") or ""),
                    str(item.get("keyword_text") or ""),
                    " ".join(str(t) for t in (item.get("matched_terms") or [])),
                ]
            ).lower()
            bonus = 0.0
            for tok in finance_tokens:
                if tok in text_blob:
                    bonus += 0.75

            has_telecom = bool(re.search(r"\b(?:telephone|phone|mobile|telecom|telefon|mobil|yallo|sunrise)\b", text_blob, flags=re.IGNORECASE))
            has_billing = bool(re.search(r"\b(?:bill|invoice|receipt|payment|charge|fee|rechnung|beleg|zahlung|quittung)\b", text_blob, flags=re.IGNORECASE))
            has_transport = bool(re.search(r"\b(?:ticket|train|rail|sbb|booking|fahrt|journey)\b", text_blob, flags=re.IGNORECASE))
            has_purchase_order = bool(re.search(r"\b(?:purchase\s*order|order\s*no\.|teyu|chiller)\b", text_blob, flags=re.IGNORECASE))

            topical = 0.0
            if wants_telecom:
                topical += 2.0 if has_telecom else -1.4
            if wants_billing:
                topical += 2.0 if has_billing else -1.4
            if wants_billing and has_transport:
                topical -= 2.5
            if wants_billing and has_purchase_order:
                topical -= 2.0

            return base + bonus + topical

        ranked_matches = sorted(
            [item for item in matches if isinstance(item, dict)],
            key=_match_rank,
            reverse=True,
        )

        seed_docs: list[dict[str, Any]] = []
        seen_doc_ids: set[int] = set()
        for item in ranked_matches:
            doc_id = item.get("doc_id")
            if not isinstance(doc_id, int) or doc_id in seen_doc_ids:
                continue
            seen_doc_ids.add(doc_id)
            seed_docs.append(
                {
                    "doc_id": doc_id,
                    "doc_name": item.get("doc_name"),
                    "doc_path": item.get("doc_path"),
                    "title": item.get("title"),
                    "matched_terms": list(item.get("matched_terms") or []),
                }
            )
            if len(seed_docs) >= max(1, int(limit_docs)):
                break

        if not seed_docs:
            return []

        doc_ids = [int(item["doc_id"]) for item in seed_docs]
        db_meta: dict[int, dict[str, Any]] = {}
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT doc_id, doc_key, doc_path, metadata
                        FROM document
                        WHERE doc_id = ANY(%s)
                        LIMIT 50;
                        """,
                        (doc_ids,),
                    )
                    for row in cur.fetchall() or []:
                        if isinstance(row[0], int):
                            db_meta[int(row[0])] = {
                                "doc_key": row[1],
                                "doc_path": row[2],
                                "metadata": row[3] if isinstance(row[3], dict) else {},
                            }
        except Exception:
            db_meta = {}

        out: list[dict[str, Any]] = []
        base_dir = os.path.dirname(__file__)

        def _financial_focus_from_text(full_text: str) -> str:
            raw = str(full_text or "")
            if not raw:
                return ""
            lines = [ln.strip() for ln in raw.splitlines() if str(ln or "").strip()]
            if not lines:
                return ""

            focus_markers = (
                "total", "amount due", "total due", "payable", "grand total", "invoice", "rechnung", "zahlbar",
                "zu bezahlen", "gesamt", "summe", "mwst", "vat", "tax", "subtotal", "zwischen", "totalbetrag",
            )
            currency_re = re.compile(r"\b(?:CHF|EUR|USD)\b|\b[0-9]{1,3}(?:[\'\s,.][0-9]{3})*(?:[.,][0-9]{1,2})\b", re.IGNORECASE)

            selected: list[str] = []
            seen: set[str] = set()
            for idx, line in enumerate(lines):
                lower = line.lower()
                marker_hit = any(marker in lower for marker in focus_markers)
                currency_hit = bool(currency_re.search(line))
                if not (marker_hit or currency_hit):
                    continue

                for j in (idx - 1, idx, idx + 1):
                    if j < 0 or j >= len(lines):
                        continue
                    candidate = lines[j]
                    if candidate in seen:
                        continue
                    seen.add(candidate)
                    selected.append(candidate)
                if len(selected) >= 90:
                    break

            if not selected:
                return ""
            return "\n".join(selected)[:5000]

        for item in seed_docs:
            doc_id = int(item["doc_id"])
            meta_info = db_meta.get(doc_id, {})
            metadata = meta_info.get("metadata") if isinstance(meta_info.get("metadata"), dict) else {}
            user_meta = metadata.get("user_metadata") if isinstance(metadata, dict) else {}
            source_ref = user_meta.get("source_reference") if isinstance(user_meta, dict) else {}
            markdown_rel = source_ref.get("markdown_cache_path") if isinstance(source_ref, dict) else None

            snippet = ""
            financial_focus = ""
            if markdown_rel:
                md_path = str(markdown_rel).strip()
                if md_path:
                    if not os.path.isabs(md_path):
                        md_path = os.path.join(base_dir, md_path)
                    md_path = os.path.normpath(md_path)
                    if os.path.exists(md_path):
                        try:
                            with open(md_path, "r", encoding="utf-8", errors="ignore") as fh:
                                full_text = str(fh.read() or "")
                                snippet = full_text[:2600]
                                financial_focus = _financial_focus_from_text(full_text)
                        except Exception:
                            snippet = ""
                            financial_focus = ""

            if not snippet and isinstance(metadata, dict):
                # Keep metadata fallback compact and safe when markdown is unavailable.
                metadata_text = json.dumps(metadata, ensure_ascii=False)
                snippet = metadata_text[:1200] if metadata_text else ""

            if not financial_focus and isinstance(metadata, dict):
                metadata_text = json.dumps(metadata, ensure_ascii=False)
                financial_focus = metadata_text[:1500] if metadata_text else ""

            out.append(
                {
                    "doc_id": doc_id,
                    "doc_name": item.get("doc_name") or meta_info.get("doc_key"),
                    "doc_path": item.get("doc_path") or meta_info.get("doc_path"),
                    "title": item.get("title"),
                    "doc_theme": item.get("doc_theme"),
                    "doc_cat": item.get("doc_cat"),
                    "doc_type": item.get("doc_type"),
                    "keyword_text": item.get("keyword_text"),
                    "matched_terms": list(item.get("matched_terms") or []),
                    "evidence_snippet": snippet,
                    "financial_focus": financial_focus,
                }
            )

        return out

    def _criteria_contextual_llm_resolution_fallback(
        self,
        question: str,
        parsed: ParsedQuery,
        criteria_result: dict[str, Any],
        candidates: list[dict[str, Any]],
        discovery_results: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Resolve value-style questions from top criteria documents using grounded context only."""
        if not self.genai_client:
            return None

        evidence_docs = self._collect_criteria_doc_evidence(criteria_result, limit_docs=5, question=question)

        def _extract_money_candidates(text: str) -> list[tuple[float, str, int, bool, bool, bool, bool, bool, bool]]:
            # Returns tuples:
            # (amount, currency, index, is_total_like, is_fee_like, is_capital_like, is_strict_total_like, is_subtotal_like, is_tax_like)
            raw = str(text or "")
            if not raw:
                return []

            patterns = [
                r"\b(CHF|EUR|USD)\s*([0-9]{1,3}(?:[\'\s,.][0-9]{3})*(?:[.,][0-9]{1,2})?)",
                r"\b([0-9]{1,3}(?:[\'\s,.][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(CHF|EUR|USD)\b",
            ]
            total_markers = (
                "total amount due", "amount due", "payable", "total due", "grand total", "invoice total",
                "zu bezahlen", "zahlbar", "gesamtbetrag", "gesamt", "summe", "rechnungstotal",
            )
            strict_total_markers = (
                "total amount due", "amount due", "payable", "total due", "grand total",
                "zu bezahlen", "zahlbar", "gesamtbetrag", "rechnungstotal", "rechnung total",
            )
            subtotal_markers = (
                "subtotal", "without vat", "ohne mwst", "mwst", "vat", "tax", "line item", "mobile services",
                "rundungsdifferenz", "rounding", "anruf", "per call", "pro anruf", "call rate",
            )
            fee_markers = (
                "basic fee", "fee", "grundgeb", "grundgebuhr", "grundgebühr", "eintrag", "entry",
                "postage", "small letter", "e-mail fee", "line item", "funktion", "zeichnungsberechtigung", "mobile services",
                "anruf", "per call", "pro anruf", "call rate", "chf 1.50 / anruf",
            )
            capital_markers = (
                "stammkapital", "share capital", "capital", "stammanteile", "nominal capital", "kapital",
                "gesellschafter", "shares", "shareholders",
            )
            tax_markers = (
                "mwst", "vat", "tax", "ust", "tva", "iva", "vat amount", "mwst.-betrag", "steuer",
            )

            out: list[tuple[float, str, int, bool, bool, bool, bool, bool, bool]] = []
            for pattern in patterns:
                for match in re.finditer(pattern, raw, flags=re.IGNORECASE):
                    groups = match.groups()
                    if len(groups) != 2:
                        continue
                    left, right = groups
                    if re.fullmatch(r"[A-Za-z]{3}", str(left or ""), flags=re.IGNORECASE):
                        currency = str(left).upper()
                        amount_raw = str(right or "")
                    else:
                        amount_raw = str(left or "")
                        currency = str(right).upper()

                    normalized = amount_raw.replace("'", "").replace(" ", "")
                    # If both separators exist, assume last one is decimal.
                    if "," in normalized and "." in normalized:
                        if normalized.rfind(",") > normalized.rfind("."):
                            normalized = normalized.replace(".", "").replace(",", ".")
                        else:
                            normalized = normalized.replace(",", "")
                    else:
                        if normalized.count(",") == 1 and normalized.count(".") == 0:
                            normalized = normalized.replace(",", ".")
                        elif normalized.count(",") > 1 and normalized.count(".") == 0:
                            normalized = normalized.replace(",", "")

                    try:
                        amount = float(normalized)
                    except Exception:
                        continue

                    ctx_start = max(0, match.start() - 90)
                    ctx_end = min(len(raw), match.end() + 90)
                    context = raw[ctx_start:ctx_end].lower()
                    is_total_like = any(marker in context for marker in total_markers)
                    is_strict_total_like = any(marker in context for marker in strict_total_markers)
                    is_fee_like = any(marker in context for marker in fee_markers)
                    is_capital_like = any(marker in context for marker in capital_markers)
                    is_subtotal_like = any(marker in context for marker in subtotal_markers)
                    is_tax_like = any(marker in context for marker in tax_markers)
                    out.append((amount, currency, match.start(), is_total_like, is_fee_like, is_capital_like, is_strict_total_like, is_subtotal_like, is_tax_like))
            return out

        def _best_total_from_evidence(docs: list[dict[str, Any]]) -> tuple[float, str] | None:
            question_tokens = [
                tok
                for tok in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]{4,}", str(question or "").lower())
                if tok
                not in {
                    "what", "which", "where", "when", "with", "from", "into", "about", "much",
                    "cost", "price", "value", "amount", "total", "tell", "find", "related", "documents",
                    "show", "give", "query", "that", "this", "have", "does", "did", "were", "been",
                    "was", "sind", "ist", "eine", "einer", "eines", "dieser", "mobile", "services",
                }
            ]
            if not question_tokens:
                question_tokens = [
                    tok
                    for tok in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]{4,}", str(question or "").lower())
                    if tok
                    not in {
                        "what", "which", "where", "when", "with", "from", "into", "about", "much",
                        "cost", "price", "value", "amount", "total", "tell", "find", "related", "documents",
                        "show", "give", "query", "that", "this", "have", "does", "did", "were", "been",
                        "was", "sind", "ist", "eine", "einer", "eines", "dieser",
                    }
                ]

            q_norm = self._normalize_semantic_text(question)
            telecom_terms = {
                "telecom", "telecommunication", "telecommunications", "telephone", "phone", "mobile", "cell", "cellular", "gsm",
                "telefon", "telefonie", "mobil", "mobilfunk", "telekommunikation",
            }
            billing_terms = {
                "bill", "bills", "invoice", "invoices", "receipt", "receipts", "statement",
                "rechnung", "rechnungen", "beleg", "belege", "quittung", "payment", "payments", "charge", "charges",
                "amount", "cost", "price", "due", "total", "betrag", "kosten", "preis", "gesamt", "sum",
            }
            q_wants_telecom = any(tok in q_norm for tok in telecom_terms)
            q_wants_billing = any(tok in q_norm for tok in billing_terms)

            def _extract_labeled_total(text: str) -> tuple[float, str] | None:
                raw = str(text or "")
                if not raw:
                    return None

                label_patterns = [
                    r"rechnungstotal",
                    r"total\s+amount\s+due",
                    r"amount\s+due",
                    r"total\s+due",
                    r"grand\s+total",
                    r"invoice\s+total",
                    r"zu\s+bezahlen",
                    r"zahlbar",
                    r"gesamtbetrag",
                ]
                money_patterns = [
                    r"\b(CHF|EUR|USD)\s*([0-9]{1,3}(?:[\'\s,.][0-9]{3})*(?:[.,][0-9]{1,2})?)",
                    r"\b([0-9]{1,3}(?:[\'\s,.][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(CHF|EUR|USD)\b",
                ]

                def _parse_money(candidate: str) -> tuple[float, str] | None:
                    for money_pattern in money_patterns:
                        match = re.search(money_pattern, candidate, flags=re.IGNORECASE)
                        if not match:
                            continue
                        left, right = match.groups()
                        if re.fullmatch(r"[A-Za-z]{3}", str(left or ""), flags=re.IGNORECASE):
                            currency = str(left).upper()
                            amount_raw = str(right or "")
                        else:
                            amount_raw = str(left or "")
                            currency = str(right).upper()

                        normalized = amount_raw.replace("'", "").replace(" ", "")
                        if "," in normalized and "." in normalized:
                            if normalized.rfind(",") > normalized.rfind("."):
                                normalized = normalized.replace(".", "").replace(",", ".")
                            else:
                                normalized = normalized.replace(",", "")
                        else:
                            if normalized.count(",") == 1 and normalized.count(".") == 0:
                                normalized = normalized.replace(",", ".")
                            elif normalized.count(",") > 1 and normalized.count(".") == 0:
                                normalized = normalized.replace(",", "")
                        try:
                            value = float(normalized)
                        except Exception:
                            continue
                        if value > 0:
                            return (value, currency)
                    return None

                lowered = raw.lower()
                for label_pattern in label_patterns:
                    for marker in re.finditer(label_pattern, lowered, flags=re.IGNORECASE):
                        window = raw[marker.start() : min(len(raw), marker.end() + 160)]
                        parsed_money = _parse_money(window)
                        if parsed_money is not None:
                            return parsed_money
                return None

            # Prefer labeled totals from the most relevant semantically ranked docs.
            for doc in docs[:5]:
                if not isinstance(doc, dict):
                    continue
                doc_blob = " ".join(
                    [
                        str(doc.get("doc_name") or ""),
                        str(doc.get("doc_path") or ""),
                        str(doc.get("title") or ""),
                        str(doc.get("doc_theme") or ""),
                        str(doc.get("doc_cat") or ""),
                        str(doc.get("doc_type") or ""),
                        str(doc.get("keyword_text") or ""),
                        " ".join(str(t) for t in (doc.get("matched_terms") or [])),
                    ]
                ).lower()
                has_telecom = any(tok in doc_blob for tok in telecom_terms)
                has_billing = any(tok in doc_blob for tok in billing_terms)
                if q_wants_telecom and not has_telecom:
                    continue
                if q_wants_billing and not has_billing:
                    continue

                prioritized_text = "\n".join(
                    [
                        str(doc.get("financial_focus") or ""),
                        str(doc.get("evidence_snippet") or ""),
                    ]
                )
                labeled_total = _extract_labeled_total(prioritized_text)
                if labeled_total is not None:
                    return labeled_total

            scored_totals: list[tuple[float, float, str, bool, bool, bool, bool]] = []
            for doc in docs:
                if not isinstance(doc, dict):
                    continue
                snippet = str(doc.get("evidence_snippet") or "")
                focus = str(doc.get("financial_focus") or "")
                joined = "\n".join(part for part in (focus, snippet) if part)
                if not joined:
                    continue

                haystack = " ".join(
                    [
                        str(doc.get("doc_name") or ""),
                        str(doc.get("doc_path") or ""),
                        str(doc.get("title") or ""),
                        str(doc.get("doc_theme") or ""),
                        str(doc.get("doc_cat") or ""),
                        str(doc.get("doc_type") or ""),
                        str(doc.get("keyword_text") or ""),
                        " ".join(str(t) for t in (doc.get("matched_terms") or [])),
                        joined,
                    ]
                ).lower()
                relevance = sum(1 for tok in question_tokens if tok and tok in haystack)

                has_telecom = any(tok in haystack for tok in telecom_terms)
                has_billing = any(tok in haystack for tok in billing_terms)
                doc_affinity = 0.0
                if q_wants_telecom and has_telecom:
                    doc_affinity += 1.8
                if q_wants_billing and has_billing:
                    doc_affinity += 1.8
                if q_wants_telecom and q_wants_billing and has_telecom and has_billing:
                    doc_affinity += 1.2

                money_candidates = _extract_money_candidates(joined)
                amount_frequency: dict[tuple[str, float], int] = {}
                for amount, currency, _idx, *_rest in money_candidates:
                    key = (str(currency), round(float(amount), 2))
                    amount_frequency[key] = int(amount_frequency.get(key, 0)) + 1

                for amount, currency, _idx, is_total_like, is_fee_like, is_capital_like, is_strict_total_like, is_subtotal_like, is_tax_like in money_candidates:
                    if is_capital_like:
                        continue
                    candidate_score = float(relevance) + doc_affinity
                    if is_strict_total_like:
                        candidate_score += 3.4
                    if is_total_like:
                        candidate_score += 2.8
                    if is_fee_like:
                        candidate_score -= 1.9
                    if is_subtotal_like:
                        candidate_score -= 2.1
                    if is_tax_like:
                        candidate_score -= 3.2
                    repeats = int(amount_frequency.get((str(currency), round(float(amount), 2)), 0))
                    if repeats > 1:
                        candidate_score += min(1.2, 0.5 * float(repeats - 1))
                    if amount <= 0:
                        continue
                    scored_totals.append((candidate_score, amount, currency, is_fee_like, is_strict_total_like, is_subtotal_like, is_tax_like))

            if not scored_totals:
                return None

            non_fee_non_tax = [item for item in scored_totals if (not item[3]) and (not item[6])]
            non_fee = [item for item in scored_totals if not item[3]]
            pool = non_fee_non_tax if non_fee_non_tax else (non_fee if non_fee else scored_totals)

            best_score = max(item[0] for item in pool)
            if best_score < 1.0:
                return None

            best_candidates = [item for item in pool if item[0] >= (best_score - 0.20)]
            # Prefer strongest total cues first, then the highest score. As a final tie-break,
            # avoid drifting to larger unrelated amounts from lower-affinity invoices.
            chosen = max(
                best_candidates,
                key=lambda item: (
                    1 if item[4] else 0,
                    0 if item[5] else 1,
                    0 if item[6] else 1,
                    item[0],
                    -abs(item[1]),
                ),
            )
            return (chosen[1], chosen[2])

        def _extract_first_money_from_text(text: str) -> tuple[float, str] | None:
            raw = str(text or "")
            if not raw:
                return None
            patterns = [
                r"\b(CHF|EUR|USD)\s*([0-9]{1,3}(?:[\'\s,.][0-9]{3})*(?:[.,][0-9]{1,2})?)",
                r"\b([0-9]{1,3}(?:[\'\s,.][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(CHF|EUR|USD)\b",
            ]
            for pattern in patterns:
                match = re.search(pattern, raw, flags=re.IGNORECASE)
                if not match:
                    continue
                left, right = match.groups()
                if re.fullmatch(r"[A-Za-z]{3}", str(left or ""), flags=re.IGNORECASE):
                    currency = str(left).upper()
                    amount_raw = str(right or "")
                else:
                    amount_raw = str(left or "")
                    currency = str(right).upper()
                normalized = amount_raw.replace("'", "").replace(" ", "")
                if "," in normalized and "." in normalized:
                    if normalized.rfind(",") > normalized.rfind("."):
                        normalized = normalized.replace(".", "").replace(",", ".")
                    else:
                        normalized = normalized.replace(",", "")
                else:
                    if normalized.count(",") == 1 and normalized.count(".") == 0:
                        normalized = normalized.replace(",", ".")
                    elif normalized.count(",") > 1 and normalized.count(".") == 0:
                        normalized = normalized.replace(",", "")
                try:
                    return (float(normalized), currency)
                except Exception:
                    continue
            return None

        qdrant_context: list[dict[str, Any]] = []
        for item in list(candidates or [])[:4]:
            if not isinstance(item, dict):
                continue
            qdrant_context.append(
                {
                    "doc_id": item.get("doc_id"),
                    "doc_key": item.get("doc_key"),
                    "text": str(item.get("text") or item.get("chunk_text") or "")[:900],
                }
            )

        discovery_context: list[dict[str, Any]] = []
        for item in list(discovery_results or [])[:3]:
            if not isinstance(item, dict):
                continue
            discovery_context.append(
                {
                    "document_id": item.get("document_id"),
                    "doc_path": item.get("content_uri"),
                    "struct_data": item.get("struct_data") if isinstance(item.get("struct_data"), dict) else {},
                    "derived_struct_data": item.get("derived_struct_data") if isinstance(item.get("derived_struct_data"), dict) else {},
                }
            )

        if not evidence_docs and not qdrant_context and not discovery_context:
            return None

        prompt = (
            "You are resolving a user question using grounded document evidence only. "
            "Use only the provided evidence snippets and contexts. "
            "If there is no explicit answer, return null.\n"
            "Return ONLY valid JSON in this shape:\n"
            '{"answer": <string|null>, "confidence": <0..1>, "evidence": <short quote|null>, '
            '"doc_id": <integer|null>, "resolved_attribute": <string|null>}\n'
            f"Question: {json.dumps(question, ensure_ascii=False)}\n"
            f"Parsed intent: {json.dumps(parsed.intent, ensure_ascii=False)}\n"
            f"Criteria source: {json.dumps(str(criteria_result.get('source') or ''), ensure_ascii=False)}\n"
            f"Top criteria documents: {json.dumps(evidence_docs, ensure_ascii=False)}\n"
            f"Qdrant context: {json.dumps(qdrant_context, ensure_ascii=False)}\n"
            f"Discovery context: {json.dumps(discovery_context, ensure_ascii=False)}"
        )

        asks_money = bool(
            re.search(
                r"\b(total|amount|cost|price|due|sum|wert|betrag|gesamt|kosten|preis|zahlbar)\b",
                str(question or "").lower(),
                flags=re.IGNORECASE,
            )
        )
        deterministic_total = _best_total_from_evidence(evidence_docs)

        def _deterministic_total_result(min_confidence: float = 0.82) -> dict[str, Any] | None:
            if not asks_money or deterministic_total is None:
                return None
            total_amount, total_currency = deterministic_total
            primary_doc_id = None
            primary_evidence = None
            if evidence_docs and isinstance(evidence_docs[0], dict):
                raw_doc_id = evidence_docs[0].get("doc_id")
                primary_doc_id = int(raw_doc_id) if isinstance(raw_doc_id, int) else None
                primary_evidence = evidence_docs[0].get("financial_focus") or evidence_docs[0].get("evidence_snippet")

            return {
                "answer": f"The total amount due is {total_amount:,.2f} {total_currency}.".replace(",", ""),
                "confidence": float(min_confidence),
                "evidence": str(primary_evidence or "")[:280] if primary_evidence else None,
                "doc_id": primary_doc_id,
                "resolved_attribute": "amount_total_due",
                "criteria_documents": evidence_docs,
                "source": "criteria_contextual_llm",
            }

        try:
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: self.genai_client.models.generate_content(
                    model=EXTRACT_MODEL,
                    contents=prompt,
                ),
                model=EXTRACT_MODEL,
                contents=prompt,
                temperature=0.0,
                call_name="criteria_contextual_llm_resolution_fallback",
                complexity="simple",
            )
            text = (response.text or "").strip()
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
            text = re.sub(r"\s*```\s*$", "", text, flags=re.MULTILINE)
            data = json.loads(text)

            answer = str(data.get("answer") or "").strip()
            if not answer:
                return _deterministic_total_result()

            try:
                confidence = float(data.get("confidence", 0.0))
            except Exception:
                confidence = 0.0
            if confidence < 0.70:
                deterministic = _deterministic_total_result(min_confidence=0.80)
                return deterministic

            raw_doc_id = data.get("doc_id")
            resolved_doc_id = int(raw_doc_id) if isinstance(raw_doc_id, int) else None

            # Generic monetary safety rule: if evidence contains an explicit payable/total
            # amount, prefer that over partial fee-like subtotals.
            total_override = _best_total_from_evidence(evidence_docs)
            if total_override is not None:
                total_amount, total_currency = total_override
                answer_blob = str(answer or "").lower()
                asks_money = bool(
                    re.search(
                        r"\b(total|amount|cost|price|due|sum|wert|betrag|gesamt|kosten|preis|zahlbar)\b",
                        str(question or "").lower(),
                        flags=re.IGNORECASE,
                    )
                )
                mentions_total = bool(
                    re.search(
                        r"\b(total|amount due|total due|payable|grand total|zu bezahlen|gesamtbetrag|zahlbar)\b",
                        answer_blob,
                        flags=re.IGNORECASE,
                    )
                )
                fee_like_answer = bool(
                    re.search(
                        r"\b(fee|basic fee|anruf|line item|rounding|rundungsdifferenz|mwst|vat|tax)\b",
                        answer_blob,
                        flags=re.IGNORECASE,
                    )
                )
                extracted_answer_money = _extract_first_money_from_text(answer)
                mismatch_with_deterministic = False
                if extracted_answer_money is not None:
                    answer_amount, answer_currency = extracted_answer_money
                    if answer_currency == total_currency:
                        mismatch_with_deterministic = abs(answer_amount - total_amount) > max(0.05, 0.25 * max(total_amount, 1.0))
                    elif answer_amount > 0:
                        mismatch_with_deterministic = True

                if asks_money and (not mentions_total or fee_like_answer or mismatch_with_deterministic):
                    answer = f"The total amount due is {total_amount:,.2f} {total_currency}.".replace(",", "")
                    resolved_attr = "amount_total_due"
                    return {
                        "answer": answer,
                        "confidence": max(confidence, 0.82),
                        "evidence": data.get("evidence"),
                        "doc_id": resolved_doc_id,
                        "resolved_attribute": resolved_attr,
                        "criteria_documents": evidence_docs,
                        "source": "criteria_contextual_llm",
                    }

            return {
                "answer": answer,
                "confidence": confidence,
                "evidence": data.get("evidence"),
                "doc_id": resolved_doc_id,
                "resolved_attribute": data.get("resolved_attribute"),
                "criteria_documents": evidence_docs,
                "source": "criteria_contextual_llm",
            }
        except Exception:
            return _deterministic_total_result(min_confidence=0.80)

    @staticmethod
    def _is_naturally_multi_value_attribute(attribute_name: str | None) -> bool:
        normalized = str(attribute_name or "").strip().lower()
        return normalized in {
            "shareholders",
            "documents",
            "document_list",
            "related_entities",
            "tags",
            "parties",
        }

    @staticmethod
    def _normalize_ambiguous_value_candidates(values: list[Any], limit: int = 8) -> list[Any]:
        out: list[Any] = []
        seen: set[str] = set()
        for item in list(values or []):
            normalized: Any
            if isinstance(item, dict):
                normalized = {
                    "name": item.get("name") or item.get("entity_name") or item.get("value"),
                    "value": item.get("attribute_value") if "attribute_value" in item else item.get("value"),
                    "score": item.get("score"),
                    "doc_id": item.get("doc_id"),
                    "doc_name": item.get("doc_name"),
                    "doc_path": item.get("doc_path"),
                }
                normalized = {k: v for k, v in normalized.items() if v not in (None, "")}
            else:
                text = str(item or "").strip()
                if not text:
                    continue
                normalized = text
            dedupe_key = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            out.append(normalized)
            if len(out) >= max(1, int(limit)):
                break
        return out

    def _build_ambiguous_value_response(
        self,
        *,
        question: str,
        intent: str,
        language: str,
        entity_name: str,
        attribute_name: str,
        values: list[Any],
        source_match: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        candidate_values = self._normalize_ambiguous_value_candidates(values, limit=8)
        display_attr = self._get_attribute_display_name(attribute_name, language)
        listed_items: list[str] = []
        for item in candidate_values[:5]:
            if isinstance(item, dict):
                listed_items.append(str(item.get("name") or item.get("value") or item))
            else:
                listed_items.append(str(item))
        listed = ", ".join(listed_items)
        if str(language or "en").lower() == "de":
            answer_text = (
                f"Mehrdeutiger Wert für {display_attr} von {entity_name}. "
                f"Bitte präzisieren Sie den gewünschten Kandidaten. Kandidaten: {listed}."
            )
        else:
            answer_text = (
                f"Ambiguous value for {display_attr} of {entity_name}. "
                f"Please specify which candidate should be used. Candidates: {listed}."
            )

        provenance: list[dict[str, Any]] = []
        if isinstance(source_match, dict):
            prov_item = {
                "doc_id": source_match.get("doc_id"),
                "doc_name": source_match.get("doc_name"),
                "doc_path": source_match.get("doc_path"),
                "relationship_name": source_match.get("relationship_name"),
                "attribute_name": source_match.get("attribute_name") or attribute_name,
                "entity_name": source_match.get("entity_name") or entity_name,
            }
            prov_item = {k: v for k, v in prov_item.items() if v not in (None, "")}
            if prov_item:
                provenance.append(prov_item)

        return {
            "question": question,
            "intent": intent,
            "answer": self._prefix_source(answer_text, "value_ambiguous"),
            "data": {
                "resolution_status": "ambiguous_value",
                "entity_name": entity_name,
                "attribute_name": attribute_name,
                "candidate_values": candidate_values,
                "candidate_count": len(candidate_values),
                "provenance": provenance,
            },
            "source": "value_ambiguous",
            "candidate_count": len(candidate_values),
        }

    def sql_fallback_from_ids(
        self,
        conn,
        parsed: ParsedQuery,
        node_ids: list[int],
        edge_ids: list[int],
        doc_ids: list[int],
    ) -> dict[str, Any] | None:
        if parsed.intent != "attribute_lookup" or not parsed.attribute_name:
            return None

        if not node_ids and not edge_ids and not doc_ids:
            return None

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    n.object_id,
                    n.object_name,
                    n.class_name,
                    a.attr_json,
                    d.identifiers_kv,
                    d.metadata,
                    d.doc_id,
                    d.doc_key,
                    d.doc_path,
                    d.doc_date::text
                FROM document d
                LEFT JOIN part p ON p.doc_id = d.doc_id
                LEFT JOIN object_instance n ON p.object_id = n.object_id
                LEFT JOIN attribute a
                  ON a.src_type='object' AND a.src_id=n.object_id
                WHERE (
                    d.doc_id = ANY(%s)
                                        OR n.object_id = ANY(%s)
                )
                  AND COALESCE(d.status, 'active') = 'active'
                  AND d.valid_from <= CURRENT_DATE
                  AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                ORDER BY d.entry_date DESC
                LIMIT 150;
                """,
                (doc_ids, node_ids),
            )
            rows = cur.fetchall()

        for row in rows:
            if parsed.entity_name and isinstance(row[1], str):
                if parsed.entity_name.lower() not in row[1].lower():
                    continue

            attr_value = self._extract_attr_value(row[3], parsed.attribute_name)
            if attr_value is None:
                attr_value = self._extract_attr_value(row[4], parsed.attribute_name)
            if attr_value is None:
                attr_value = self._extract_attr_value(row[5], parsed.attribute_name)
            if attr_value is None:
                continue

            return {
                "object_id": row[0],
                "entity_name": row[1],
                "entity_type": row[2],
                "attribute_name": parsed.attribute_name,
                "attribute_value": attr_value,
                "doc_id": row[6],
                "doc_name": row[7],
                "doc_path": row[8],
                "doc_date": row[9],
            }

        return None

    def _semantic_summary_from_candidates(self, candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not candidates:
            return None
        top = candidates[0]
        return {
            "doc_id": top.get("doc_id"),
            "doc_key": top.get("doc_key"),
            "doc_type": top.get("document_type") or top.get("doc_type"),
            "chunk_text": top.get("text"),
            "gcs_uri": top.get("gcs_uri"),
        }

    def _semantic_summary_from_discovery(self, results: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not results:
            return None
        top = results[0]
        struct_data = top.get("struct_data") if isinstance(top.get("struct_data"), dict) else {}
        return {
            "document_id": top.get("document_id"),
            "doc_type": struct_data.get("doc_type") or struct_data.get("doc_cat"),
            "doc_path": top.get("content_uri"),
            "mime_type": top.get("mime_type"),
            "struct_data": struct_data,
        }

    def _semantic_llm_attribute_fallback(
        self,
        question: str,
        parsed: ParsedQuery,
        candidates: list[dict[str, Any]],
        discovery_results: list[dict[str, Any]],
        attribute_filters: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        """Use semantic context + structured DB entity facts as evidence when SQL tiers fail."""
        if not self.genai_client:
            return None
        if parsed.intent != "attribute_lookup" or not parsed.attribute_name:
            return None

        evidence_chunks: list[dict[str, Any]] = []
        active_attribute_filters = list(attribute_filters or [])

        def _text_matches_filters(text: str) -> bool:
            if not active_attribute_filters:
                return True
            haystack = self._normalize_semantic_text(text)
            for item in active_attribute_filters:
                expected_forms = self._structured_table_term_forms(item.get("value"))
                if not any(form and form in haystack for form in expected_forms):
                    return False
            return True

        # Gather structured entity context from DB (attributes, relationships, linked documents)
        entity_context: dict[str, Any] | None = None
        if parsed.entity_name:
            try:
                with self._get_connection() as conn:
                    entity_context = self._gather_entity_resolution_context(conn, parsed.entity_name)
            except Exception:
                entity_context = None

        for item in candidates[:6]:
            if not isinstance(item, dict):
                continue
            chunk_text = str(item.get("text") or item.get("chunk_text") or "").strip()
            if not chunk_text:
                continue
            if not _text_matches_filters(chunk_text):
                continue
            evidence_chunks.append(
                {
                    "source": "qdrant",
                    "doc_id": item.get("doc_id"),
                    "doc_key": item.get("doc_key"),
                    "doc_path": item.get("gcs_uri") or item.get("doc_path"),
                    "text": chunk_text[:1200],
                }
            )

        for item in discovery_results[:4]:
            if not isinstance(item, dict):
                continue
            struct_data = item.get("struct_data") if isinstance(item.get("struct_data"), dict) else {}
            derived_struct_data = item.get("derived_struct_data") if isinstance(item.get("derived_struct_data"), dict) else {}
            if not self._structured_table_record_matches_attribute_filters(
                struct_data,
                {"struct_data": struct_data, "derived_struct_data": derived_struct_data},
                active_attribute_filters,
            ):
                continue
            packed = json.dumps(
                {
                    "struct_data": struct_data,
                    "derived_struct_data": derived_struct_data,
                },
                ensure_ascii=False,
            )
            if not packed or packed == '{"struct_data": {}, "derived_struct_data": {}}':
                continue
            evidence_chunks.append(
                {
                    "source": "discovery",
                    "document_id": item.get("document_id"),
                    "doc_path": item.get("content_uri"),
                    "text": packed[:1200],
                }
            )

        # Proceed if we have entity context OR Qdrant/Discovery chunks — not strictly both
        if not evidence_chunks and not entity_context:
            return None

        prompt_parts: list[str] = [
            "You are extracting a factual attribute value from provided context only. ",
            "Use ONLY the facts listed below. Do not invent or guess. ",
            "If the value is not explicitly present in any section, return null.\n",
            "Return ONLY valid JSON:\n",
            '{"value": <string|number|array|null>, "confidence": <0..1>, "evidence": <short quote or null>, '
            '"lexical_signals": {'
            '"en": {"nouns": <array of strings>, "verbs": <array of strings>, "adjectives": <array of strings>, '
            '"adverbs": <array of strings>, "trigger_phrases": <array of strings>, "attribute_clues": <array of strings>, '
            '"relationship_clues": <array of strings>, "metadata_clues": <array of strings>}, '
            '"de": {"nouns": <array of strings>, "verbs": <array of strings>, "adjectives": <array of strings>, '
            '"adverbs": <array of strings>, "trigger_phrases": <array of strings>, "attribute_clues": <array of strings>, '
            '"relationship_clues": <array of strings>, "metadata_clues": <array of strings>}}}\n',
            f"Question: {json.dumps(question, ensure_ascii=False)}\n",
            f"Entity: {json.dumps(parsed.entity_name, ensure_ascii=False)}\n",
            f"Requested attribute: {json.dumps(parsed.attribute_name, ensure_ascii=False)}\n",
        ]
        if active_attribute_filters:
            prompt_parts.append(
                "Required filters that MUST be satisfied by the evidence for any answer:\n"
                f"{json.dumps(active_attribute_filters, ensure_ascii=False)}\n"
            )
        if entity_context:
            # Summarise attributes and relationships concisely to keep token count reasonable
            ec_summary: dict[str, Any] = {
                "canonical_name": entity_context.get("object", {}).get("object_name") if entity_context.get("object") else entity_context.get("entity_name"),
                "class": entity_context.get("object", {}).get("class_name") if entity_context.get("object") else None,
                "attributes": entity_context.get("attributes", [])[:30],
                "relationships": entity_context.get("relationships", [])[:20],
                "linked_documents": [
                    {"doc_name": d.get("doc_name"), "doc_date": d.get("doc_date"), "description": d.get("description")}
                    for d in entity_context.get("documents", [])[:8]
                ],
            }
            prompt_parts.append(f"Structured knowledge-base facts for entity:\n{json.dumps(ec_summary, ensure_ascii=False)}\n")
        if evidence_chunks:
            prompt_parts.append(f"Document/semantic context snippets:\n{json.dumps(evidence_chunks, ensure_ascii=False)}")
        prompt = "".join(prompt_parts)

        try:
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: self.genai_client.models.generate_content(
                    model=EXTRACT_MODEL,
                    contents=prompt,
                ),
                model=EXTRACT_MODEL,
                contents=prompt,
                temperature=0.0,
                call_name="semantic_llm_attribute_fallback",
                complexity="simple",
            )
            text = (response.text or "").strip()
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
            text = re.sub(r"\s*```\s*$", "", text, flags=re.MULTILINE)
            data = json.loads(text)

            value = data.get("value")
            if value in (None, "", []):
                return None

            try:
                confidence = float(data.get("confidence", 0.0))
            except Exception:
                confidence = 0.0
            if confidence < 0.7:
                return None

            return {
                "entity_name": parsed.entity_name,
                "attribute_name": parsed.attribute_name,
                "attribute_value": value,
                "confidence": confidence,
                "evidence": data.get("evidence"),
                "lexical_signals": self._normalize_bilingual_lexical_signals(
                    data.get("lexical_signals"),
                    default_language=parsed.language,
                ),
                "source": "semantic_llm_fallback",
            }
        except Exception:
            return None

    def _gather_entity_resolution_context(self, conn, entity_name: str) -> dict[str, Any] | None:
        normalized_entity = str(entity_name or "").strip()
        if not normalized_entity:
            return None

        scope = self._resolve_entity_object_scope(conn, normalized_entity)
        resolved = str(scope.get("resolved_name") or "").strip()
        if resolved:
            normalized_entity = resolved
        scoped_object_ids = [int(item) for item in list(scope.get("object_ids") or []) if str(item).isdigit()]

        context: dict[str, Any] = {
            "entity_name": normalized_entity,
            "object": None,
            "attributes": [],
            "relationships": [],
            "documents": [],
            "alias_group": list(scope.get("object_names") or []),
        }

        row = None
        if scoped_object_ids:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT object_id, object_name, class_name, metadata
                    FROM object_instance
                    WHERE object_id = ANY(%s)
                      AND COALESCE(status, 'active') = 'active'
                      AND valid_from <= CURRENT_DATE
                      AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                    ORDER BY CASE WHEN LOWER(object_name) = LOWER(%s) THEN 0 ELSE 1 END,
                             entry_date DESC
                    LIMIT 1;
                    """,
                    (scoped_object_ids, normalized_entity),
                )
                row = cur.fetchone()

        if row is None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT object_id, object_name, class_name, metadata
                    FROM object_instance
                    WHERE LOWER(object_name) = LOWER(%s)
                      AND COALESCE(status, 'active') = 'active'
                      AND valid_from <= CURRENT_DATE
                      AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                    ORDER BY entry_date DESC
                    LIMIT 1;
                    """,
                    (normalized_entity,),
                )
                row = cur.fetchone()

        if not row:
            # Exact match failed — try LIKE search on the last significant token.
            # Collect up to 3 candidates so the LLM can infer which one is relevant.
            token_candidates = [tok for tok in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]{3,}", normalized_entity) if tok]
            token = token_candidates[-1] if token_candidates else normalized_entity
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT object_id, object_name, class_name, metadata
                    FROM object_instance
                    WHERE LOWER(object_name) LIKE LOWER(%s)
                      AND COALESCE(status, 'active') = 'active'
                      AND valid_from <= CURRENT_DATE
                      AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                    ORDER BY LENGTH(object_name) ASC
                    LIMIT 3;
                    """,
                    (f"%{token}%",),
                )
                like_rows = cur.fetchall()
            if not like_rows:
                return None
            # Use the shortest (most specific) match as primary; note ambiguity if multiple found
            row = like_rows[0]
            if len(like_rows) > 1:
                context["ambiguous_candidates"] = [
                    {"object_id": int(r[0]), "object_name": r[1], "class_name": r[2]}
                    for r in like_rows
                ]

        object_id = int(row[0])
        context["object"] = {
            "object_id": object_id,
            "object_name": row[1],
            "class_name": row[2],
            "metadata": row[3] if isinstance(row[3], dict) else {},
        }

        scoped_ids = scoped_object_ids or [object_id]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT attr_type, attr_json
                FROM attribute
                WHERE src_type = 'object'
                  AND src_id = ANY(%s)
                  AND valid_from <= CURRENT_DATE
                  AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                ORDER BY attr_type, entry_date DESC NULLS LAST, attr_id DESC
                LIMIT 120;
                """,
                (scoped_ids,),
            )
            attr_rows = cur.fetchall()

        seen_attrs: set[str] = set()
        for attr_type, attr_json in attr_rows:
            attr_name = str(attr_type or "").strip()
            if not attr_name or attr_name in seen_attrs:
                continue
            seen_attrs.add(attr_name)
            payload = attr_json if isinstance(attr_json, dict) else {}
            value = payload.get("value") if isinstance(payload, dict) else None
            if value in (None, "", [], {}):
                continue
            context["attributes"].append({"attribute_name": attr_name, "attribute_value": value})

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    rel.relationship_name,
                    src.object_name AS source_name,
                    src.class_name AS source_class,
                    tar.object_name AS target_name,
                    tar.class_name AS target_class,
                    CASE WHEN rel.src_object_id = %s THEN 'outgoing' ELSE 'incoming' END AS direction,
                    rel.metadata
                FROM object_relationship rel
                JOIN object_instance src ON src.object_id = rel.src_object_id
                JOIN object_instance tar ON tar.object_id = rel.tar_object_id
                                WHERE (rel.src_object_id = ANY(%s) OR rel.tar_object_id = ANY(%s))
                  AND rel.valid_from <= CURRENT_DATE
                  AND (rel.valid_until IS NULL OR rel.valid_until > CURRENT_DATE)
                ORDER BY rel.entry_date DESC NULLS LAST, rel.relationship_id DESC
                LIMIT 40;
                """,
                                (object_id, scoped_ids, scoped_ids),
            )
            rel_rows = cur.fetchall()

        for rel_name, source_name, source_class, target_name, target_class, direction, metadata in rel_rows:
            related_name = target_name if str(direction) == "outgoing" else source_name
            related_class = target_class if str(direction) == "outgoing" else source_class
            context["relationships"].append(
                {
                    "relationship_name": rel_name,
                    "direction": direction,
                    "related_entity_name": related_name,
                    "related_class_name": related_class,
                    "metadata": metadata if isinstance(metadata, dict) else {},
                }
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT d.doc_id, d.doc_name, d.doc_path, d.doc_date, d.metadata
                FROM document d
                JOIN part p ON p.doc_id = d.doc_id
                                WHERE p.object_id = ANY(%s)
                  AND COALESCE(d.status, 'active') = 'active'
                  AND d.valid_from <= CURRENT_DATE
                  AND (d.valid_until IS NULL OR d.valid_until > CURRENT_DATE)
                ORDER BY d.doc_date DESC NULLS LAST
                LIMIT 12;
                """,
                                (scoped_ids,),
            )
            doc_rows = cur.fetchall()

        for doc_id, doc_name, doc_path, doc_date, metadata in doc_rows:
            meta = metadata if isinstance(metadata, dict) else {}
            context["documents"].append(
                {
                    "doc_id": doc_id,
                    "doc_name": doc_name,
                    "doc_path": doc_path,
                    "doc_date": str(doc_date) if doc_date is not None else None,
                    "description": ((meta.get("description") if isinstance(meta, dict) else None) or ""),
                    "metadata": meta,
                }
            )

        return context

    def _contextual_llm_resolution_fallback(
        self,
        question: str,
        parsed: ParsedQuery,
        candidates: list[dict[str, Any]],
        discovery_results: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if not ENABLE_CONTEXTUAL_LLM_RESOLUTION_FALLBACK or not self.genai_client:
            return None

        entity_name = parsed.entity_name or self._best_entity_hint(question)
        entity_context: dict[str, Any] | None = None
        if entity_name:
            try:
                with self._get_connection() as conn:
                    entity_context = self._gather_entity_resolution_context(conn, entity_name)
            except Exception:
                entity_context = None

        qdrant_context = []
        for item in candidates[:5]:
            if not isinstance(item, dict):
                continue
            qdrant_context.append(
                {
                    "doc_id": item.get("doc_id"),
                    "doc_key": item.get("doc_key"),
                    "node_name": item.get("node_name") or item.get("entity_name"),
                    "text": str(item.get("text") or item.get("chunk_text") or "")[:900],
                }
            )

        discovery_context = []
        for item in discovery_results[:4]:
            if not isinstance(item, dict):
                continue
            struct_data = item.get("struct_data") if isinstance(item.get("struct_data"), dict) else {}
            derived_struct_data = item.get("derived_struct_data") if isinstance(item.get("derived_struct_data"), dict) else {}
            discovery_context.append(
                {
                    "document_id": item.get("document_id"),
                    "doc_path": item.get("content_uri"),
                    "mime_type": item.get("mime_type"),
                    "struct_data": struct_data,
                    "derived_struct_data": derived_struct_data,
                }
            )

        if not entity_context and not qdrant_context and not discovery_context:
            return None

        prompt = (
            "You are resolving a user question against grounded IDMS context only. "
            "Use ONLY the provided entity, relationship, document, and semantic context. "
            "Do not invent facts. If the answer is not supported, return null.\n"
            "Return ONLY valid JSON with this exact shape:\n"
            '{"answer": <string|null>, "confidence": <0..1>, "matched_entity": <string|null>, '
            '"resolved_attributes": <array of strings>, "supporting_facts": <array of short strings>, '
            '"lexical_signals": {'
            '"en": {"nouns": <array of strings>, "verbs": <array of strings>, "adjectives": <array of strings>, '
            '"adverbs": <array of strings>, "trigger_phrases": <array of strings>, "attribute_clues": <array of strings>, '
            '"relationship_clues": <array of strings>, "metadata_clues": <array of strings>}, '
            '"de": {"nouns": <array of strings>, "verbs": <array of strings>, "adjectives": <array of strings>, '
            '"adverbs": <array of strings>, "trigger_phrases": <array of strings>, "attribute_clues": <array of strings>, '
            '"relationship_clues": <array of strings>, "metadata_clues": <array of strings>}}}\n'
            f"Question: {json.dumps(question, ensure_ascii=False)}\n"
            f"Parsed intent: {json.dumps(parsed.intent, ensure_ascii=False)}\n"
            f"Requested attribute: {json.dumps(parsed.attribute_name, ensure_ascii=False)}\n"
            f"Requested attributes: {json.dumps(parsed.attribute_names or [], ensure_ascii=False)}\n"
            f"Entity context: {json.dumps(entity_context or {}, ensure_ascii=False)}\n"
            f"Qdrant context: {json.dumps(qdrant_context, ensure_ascii=False)}\n"
            f"Discovery context: {json.dumps(discovery_context, ensure_ascii=False)}"
        )

        try:
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: self.genai_client.models.generate_content(
                    model=EXTRACT_MODEL,
                    contents=prompt,
                ),
                model=EXTRACT_MODEL,
                contents=prompt,
                temperature=0.0,
                call_name="contextual_llm_resolution_fallback",
                complexity="simple",
            )
            text = (response.text or "").strip()
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
            text = re.sub(r"\s*```\s*$", "", text, flags=re.MULTILINE)
            data = json.loads(text)
            answer = str(data.get("answer") or "").strip()
            if not answer:
                return None
            try:
                confidence = float(data.get("confidence", 0.0))
            except Exception:
                confidence = 0.0
            if confidence < 0.7:
                return None

            return {
                "answer": answer,
                "confidence": confidence,
                "matched_entity": data.get("matched_entity") or entity_name,
                "resolved_attributes": list(data.get("resolved_attributes") or []),
                "supporting_facts": list(data.get("supporting_facts") or []),
                "lexical_signals": self._normalize_bilingual_lexical_signals(
                    data.get("lexical_signals"),
                    default_language=parsed.language,
                ),
                "entity_context": entity_context or {},
                "source": "contextual_llm_resolution_fallback",
            }
        except Exception:
            return None

    def _normalize_bilingual_lexical_signals(
        self,
        raw: Any,
        default_language: str = "en",
    ) -> dict[str, dict[str, list[str]]]:
        """Normalize lexical-signal payload into stable bilingual shape for downstream learning."""
        categories = (
            "nouns",
            "verbs",
            "adjectives",
            "adverbs",
            "trigger_phrases",
            "attribute_clues",
            "relationship_clues",
            "metadata_clues",
        )
        normalized: dict[str, dict[str, list[str]]] = {
            "en": {k: [] for k in categories},
            "de": {k: [] for k in categories},
        }

        def _listify(value: Any) -> list[str]:
            items: list[str] = []
            if isinstance(value, list):
                source = value
            elif isinstance(value, str):
                source = [value]
            else:
                source = []
            seen: set[str] = set()
            for item in source:
                text = str(item or "").strip().lower()
                text = re.sub(r"\s+", " ", text)
                if not text or len(text) < 2 or len(text) > 140:
                    continue
                if text in seen:
                    continue
                seen.add(text)
                items.append(text)
            return items

        default_lang = "de" if str(default_language or "").lower().startswith("de") else "en"
        data = raw if isinstance(raw, dict) else {}

        # Preferred shape: lexical_signals.en / lexical_signals.de.
        if any(lang in data for lang in ("en", "de")):
            for lang in ("en", "de"):
                bucket = data.get(lang)
                if not isinstance(bucket, dict):
                    continue
                for key in categories:
                    normalized[lang][key] = _listify(bucket.get(key))
            return normalized

        # Backward compatibility: flat lexical_signals object, map to default language.
        if isinstance(data, dict):
            for key in categories:
                normalized[default_lang][key] = _listify(data.get(key))

        return normalized

    def _generate_bilingual_pattern_candidates(
        self,
        parsed: ParsedQuery,
        lexical_signals: dict[str, dict[str, list[str]]],
    ) -> dict[str, list[str]]:
        """Generate practical EN/DE wildcard pattern candidates from lexical clues."""
        semantic_hint = str(parsed.attribute_name or parsed.relation_name or parsed.intent or "attribute").replace("_", " ")
        generated: dict[str, list[str]] = {"en": [], "de": []}

        for lang in ("en", "de"):
            clues: list[str] = []
            bucket = lexical_signals.get(lang) if isinstance(lexical_signals, dict) else {}
            if isinstance(bucket, dict):
                # Use compact semantic clues for template generation; trigger phrases are better as synonyms.
                for key in ("attribute_clues", "nouns"):
                    value = bucket.get(key)
                    if isinstance(value, list):
                        clues.extend(str(item or "").strip().lower() for item in value)

            if not clues:
                clues = [semantic_hint.lower()]

            seen: set[str] = set()
            for clue in clues[:8]:
                if not clue:
                    continue
                # Skip overly long phrase-like clues when building wildcard templates.
                if len(clue.split()) > 3:
                    continue
                if lang == "de":
                    article_nom, article_acc = self._german_articles_for_clue(clue)
                    wh_word_acc = self._german_which_word_accusative(clue)
                    candidates = [
                        f"was ist {article_nom} {clue} von *",
                        f"{wh_word_acc} {clue} hat *",
                        f"wo hat * {article_acc} {clue} erhalten",
                    ]
                else:
                    candidates = [
                        f"what is the {clue} of *",
                        f"what {clue} does * have",
                        f"where did * get the {clue}",
                    ]

                for item in candidates:
                    normalized_item = re.sub(r"\s+", " ", item.strip().lower()).strip(" ?.!")
                    if "*" not in normalized_item or len(normalized_item) < 10 or normalized_item in seen:
                        continue
                    seen.add(normalized_item)
                    generated[lang].append(normalized_item)
                    if len(generated[lang]) >= 16:
                        break
                if len(generated[lang]) >= 16:
                    break

        return generated

    def _german_articles_for_clue(self, clue: str) -> tuple[str, str]:
        """Infer likely German nominative/accusative article for a noun-like clue."""
        gender = self._german_gender_for_clue(clue)
        if gender == "f":
            return ("die", "die")
        if gender == "n":
            return ("das", "das")
        # Default masculine: nominative der, accusative den.
        return ("der", "den")

    def _german_which_word_accusative(self, clue: str) -> str:
        """Infer accusative form of 'which' for a noun-like German clue."""
        gender = self._german_gender_for_clue(clue)
        if gender == "f":
            return "welche"
        if gender == "n":
            return "welches"
        # Default masculine accusative.
        return "welchen"

    def _german_gender_for_clue(self, clue: str) -> str:
        """Resolve likely grammatical gender for a German noun clue: m/f/n."""
        noun = str(clue or "").strip().lower()
        if not noun:
            return "f"

        # Prefer explicit overrides from config for common business/HR/education terms.
        # If the clue has multiple words, use the head noun (last token) first.
        gender_overrides: dict[str, str] = dict(GERMAN_NOUN_GENDER_OVERRIDES or {})

        tokens = re.findall(r"[a-zA-Z0-9äöüß]+", noun)
        if tokens:
            head = tokens[-1]
            if head in gender_overrides:
                return gender_overrides[head]
            # Also try whole clue for single-token and phrase overrides.
            joined = " ".join(tokens)
            if joined in gender_overrides:
                return gender_overrides[joined]

        # Heuristic suffix rules for fallback.
        feminine_suffixes = (
            "ung", "keit", "heit", "ion", "tät", "tat", "schaft", "ik", "ur", "enz", "anz",
        )
        neuter_suffixes = ("chen", "lein", "ment", "um", "ma")

        if noun.endswith(feminine_suffixes):
            return "f"
        if noun.endswith(neuter_suffixes):
            return "n"
        return "m"

    def _try_learn_patterns_from_lexical_signals(
        self,
        question: str,
        parsed: ParsedQuery,
        answer_source: str,
        lexical_signals: dict[str, dict[str, list[str]]] | None,
    ) -> None:
        """Persist bilingual lexical-signal pattern candidates into semantic pattern library."""
        if (
            not self.pattern_library
            or not lexical_signals
            or answer_source == "not_found"
            or parsed.confidence < 0.70
        ):
            return

        semantic_concept = str(parsed.attribute_name or parsed.relation_name or parsed.intent or "semantic_lookup").strip().lower()
        if not semantic_concept:
            return

        try:
            generic_question = str(question or "").strip().lower()
            if parsed.entity_name:
                entity_rx = re.compile(re.escape(str(parsed.entity_name)), flags=re.IGNORECASE)
                generic_question = entity_rx.sub("*", generic_question, count=1)
            generic_question = re.sub(r"\s+", " ", generic_question).strip(" ?.!")

            mapped_attributes = {semantic_concept: semantic_concept}
            confidence = float(max(0.80, min(0.92, parsed.confidence * 0.85)))

            bilingual_candidates = self._generate_bilingual_pattern_candidates(parsed, lexical_signals)
            default_lang = "de" if str(parsed.language or "").lower().startswith("de") else "en"
            if generic_question and "*" in generic_question:
                bilingual_candidates.setdefault(default_lang, [])
                bilingual_candidates[default_lang] = [generic_question] + list(bilingual_candidates[default_lang])

            for lang in ("en", "de"):
                seed_patterns = bilingual_candidates.get(lang) or []
                if not seed_patterns:
                    continue

                seed_pattern = seed_patterns[0]
                pattern_id = self.pattern_library.add_pattern(
                    pattern_text=seed_pattern,
                    semantic_concept=semantic_concept,
                    mapped_attributes=mapped_attributes,
                    computation_rule=None,
                    entity_class=None,
                    source_type="learned",
                    confidence=confidence,
                    pattern_language=lang,
                    metadata={
                        "origin": "lexical_signals",
                        "answer_source": answer_source,
                        "question": str(question or "")[:240],
                    },
                    created_by="system:llm_lexical_signals",
                )
                if not pattern_id:
                    continue

                # Add additional wildcard templates and lexical clue tokens as synonyms.
                for synonym in seed_patterns[1:12]:
                    self.pattern_library.add_synonym(
                        pattern_id=pattern_id,
                        synonym_text=synonym,
                        language=lang,
                        semantic_distance=0.15,
                        match_type="template" if "*" in synonym else "exact",
                    )

                bucket = lexical_signals.get(lang) if isinstance(lexical_signals, dict) else {}
                if isinstance(bucket, dict):
                    for key in ("trigger_phrases", "attribute_clues", "relationship_clues"):
                        raw_items = bucket.get(key)
                        if not isinstance(raw_items, list):
                            continue
                        for token in raw_items[:12]:
                            clean_token = re.sub(r"\s+", " ", str(token or "").strip().lower()).strip(" ?.!")
                            if len(clean_token) < 2:
                                continue
                            self.pattern_library.add_synonym(
                                pattern_id=pattern_id,
                                synonym_text=clean_token,
                                language=lang,
                                semantic_distance=0.20,
                                match_type="exact",
                            )
        except Exception:
            return

    def answer(self, question: str, _allow_multi_split: bool = True) -> dict[str, Any]:
        if _allow_multi_split:
            split_questions = self._decompose_multi_question(question)
            if split_questions:
                sub_results = [self.answer(sub_q, _allow_multi_split=False) for sub_q in split_questions[:4]]
                successful = [item for item in sub_results if str(item.get("source") or "") != "not_found"]
                if successful:
                    merged_lines: list[str] = []
                    for item in successful:
                        answer_text = str(item.get("answer") or "").strip()
                        answer_text = re.sub(r"^\[source:\s*[^\]]+\]\s*", "", answer_text, flags=re.IGNORECASE)
                        answer_text = answer_text.rstrip(" .")
                        head = self._leading_question_head(str(item.get("question") or ""))
                        if head and answer_text:
                            answer_text = f"{head}: {answer_text}"
                        if answer_text and answer_text not in merged_lines:
                            merged_lines.append(answer_text)

                    if merged_lines:
                        return {
                            "question": question,
                            "intent": "multi_attribute_lookup",
                            "answer": self._prefix_source("; ".join(merged_lines) + ".", "composite"),
                            "data": {"sub_results": sub_results},
                            "source": "composite",
                            "candidate_count": len(successful),
                        }

                return {
                    "question": question,
                    "intent": "multi_attribute_lookup",
                    "answer": self._prefix_source("I could not find a confident answer in IDMS_demo.", "not_found"),
                    "data": {"sub_results": sub_results},
                    "source": "not_found",
                }

        parsed = self.parse_query(question)
        parsed = self._normalize_parsed_for_execution(parsed, question)
        parsed = self._resolve_query_sources(parsed, question=question)

        try:
            federated_result = self._try_federated_sql_read(question, parsed=parsed)
        except TypeError as exc:
            if "unexpected keyword argument 'parsed'" not in str(exc):
                raise
            federated_result = self._try_federated_sql_read(question)
        if isinstance(federated_result, dict) and str(federated_result.get("source") or "").strip().lower() in {
            "federated_sql",
            "federated_sql_error",
        }:
            return federated_result

        parsed_criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        action_item_criteria = parsed_criteria.get("action_items") if isinstance(parsed_criteria.get("action_items"), dict) else {}
        if action_item_criteria.get("requested"):
            try:
                with self._get_connection() as connection:
                    action_rows = object_db.search_document_action_items(
                        connection,
                        subject_query=action_item_criteria.get("subject_name"),
                        document_query=action_item_criteria.get("document_query"),
                        status=action_item_criteria.get("status") or "open",
                    )
                if action_rows:
                    action_text = "; ".join(
                        f"{row['instruction']}" + (f" (due {row['due_date']})" if row.get("due_date") else "")
                        for row in action_rows[:10]
                    )
                    return {
                        "question": question,
                        "intent": "document_action_item_lookup",
                        "answer": self._prefix_source(f"Required actions: {action_text}.", "document_action_item"),
                        "data": {"rows": action_rows, "criteria": action_item_criteria},
                        "source": "document_action_item",
                        "candidate_count": len(action_rows),
                    }
            except Exception:
                LOGGER.exception("Document action-item lookup failed")
        geography = parsed_criteria.get("geography") if isinstance(parsed_criteria.get("geography"), dict) else {}
        if geography:
            try:
                with self._get_connection() as connection:
                    geographic_rows = object_db.search_documents_by_geography(
                        connection,
                        location=str(geography.get("location") or ""),
                        country_code=str(geography.get("country_code") or "") or None,
                        role_name=str(geography.get("role_name") or "") or None,
                        doc_type=str(geography.get("document_type") or "") or None,
                    )
                if geographic_rows:
                    return {
                        "question": question,
                        "intent": "document_geography_lookup",
                        "answer": self._prefix_source(f"Found {len(geographic_rows)} documents matching the geographic criteria.", "document_geographic_facet"),
                        "data": {"rows": geographic_rows, "geography": geography},
                        "source": "document_geographic_facet",
                        "candidate_count": len(geographic_rows),
                    }
            except Exception:
                LOGGER.exception("Document geographic facet lookup failed")
        prefer_doc_value_bridge = self._is_value_document_context_question(question)

        # Force explicit table-cell parsing when the question clearly asks for
        # row/column lookup semantics, to avoid drifting into generic attribute answers.
        table_cell_candidate = self._extract_degree_institution_query(question, str(parsed.language or "en"))
        if table_cell_candidate and isinstance(table_cell_candidate.criteria, dict) and not prefer_doc_value_bridge:
            parsed = table_cell_candidate

        match_telemetry = self._build_match_telemetry(parsed)
        fallback_diagnostics: dict[str, Any] = {
            "attempted_flows": ["parse_query"],
            "candidate_attributes": [],
            "candidate_entities": [],
        }
        query_evidence = self._collect_query_evidence(question, parsed)
        fallback_diagnostics["query_evidence"] = query_evidence

        allow_semantic_fallback = bool(query_evidence.get("allow_semantic_matching"))
        if ENABLE_STRICT_QUERY_EVIDENCE_GATE and not bool(query_evidence.get("has_any_anchor")) and not allow_semantic_fallback:
            fallback_diagnostics["evidence_guard"] = {
                "applied": True,
                "reason": "no_grounded_anchor",
            }
            not_found_data = self._attach_match_telemetry(None, match_telemetry)
            if isinstance(not_found_data, dict):
                not_found_data["fallback_diagnostics"] = fallback_diagnostics
            return {
                "question": question,
                "intent": parsed.intent,
                "answer": self._prefix_source(self._build_no_evidence_not_found_message(question), "not_found"),
                "data": not_found_data,
                "source": "not_found",
            }

        parsed_criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        strict_table_request = bool(parsed_criteria.get("table_request")) and not prefer_doc_value_bridge
        table_row_label = str(parsed_criteria.get("table_row_label") or "").strip()
        table_column = str(parsed_criteria.get("table_column") or "").strip()
        if strict_table_request:
            fallback_diagnostics["table_request"] = {
                "enabled": True,
                "row_label": table_row_label,
                "column": table_column,
            }

        def _diag_flow(name: str) -> None:
            flows = fallback_diagnostics.get("attempted_flows")
            if isinstance(flows, list) and name not in flows:
                flows.append(name)

        candidates: list[dict[str, Any]] = []
        discovery_results: list[dict[str, Any]] = []
        node_ids: list[int] = []
        edge_ids: list[int] = []
        doc_ids: list[int] = []
        semantic_context_loaded = False

        def _ensure_semantic_context() -> None:
            nonlocal semantic_context_loaded, candidates, discovery_results, node_ids, edge_ids, doc_ids
            if semantic_context_loaded:
                return
            candidates = self.qdrant_candidates(question)
            discovery_results = self.discovery_candidates(question)
            node_ids = self._unique_ints([c.get("node_id") for c in candidates])
            edge_ids = self._unique_ints([c.get("edge_id") for c in candidates])
            doc_ids = self._unique_ints([c.get("doc_id") for c in candidates])
            semantic_context_loaded = True

        if parsed.intent == "criteria_lookup":
            criteria_result = self.search_by_criteria(
                question=question,
                parsed=parsed,
                scope={"doc_ids": doc_ids},
                limit=20,
            )
            if criteria_result:
                top_names = [str(item.get("entity_name") or "") for item in list(criteria_result.get("matches") or [])[:5]]
                criteria_source = str(criteria_result.get("source") or "criteria_sql")
                response_format = str(((parsed.criteria or {}).get("response_format") if isinstance(parsed.criteria, dict) else "") or "").strip().lower()

                # Generic bridge: for value-style questions, use top criteria documents
                # as grounded evidence for contextual LLM extraction.
                if not self._criteria_question_prefers_listing(question, response_format):
                    _ensure_semantic_context()
                    _diag_flow("criteria_contextual_llm_resolution")
                    criteria_contextual = self._criteria_contextual_llm_resolution_fallback(
                        question=question,
                        parsed=parsed,
                        criteria_result=criteria_result,
                        candidates=candidates,
                        discovery_results=discovery_results,
                    )
                    if criteria_contextual:
                        answer_text = str(criteria_contextual.get("answer") or "").strip()
                        if answer_text:
                            return {
                                "question": question,
                                "intent": parsed.intent,
                                "answer": self._prefix_source(answer_text, "criteria_contextual_llm"),
                                "data": self._attach_match_telemetry(
                                    {
                                        "criteria_result": criteria_result,
                                        "criteria_contextual": criteria_contextual,
                                    },
                                    match_telemetry,
                                ),
                                "matches": criteria_result.get("matches"),
                                "source": "criteria_contextual_llm",
                                "candidate_count": len(criteria_result.get("matches") or []),
                            }

                if response_format == "entity_directory":
                    rows: list[dict[str, Any]] = []
                    requested_filter = str((parsed.criteria or {}).get("filter_value") or "").strip()
                    requested_attributes = [
                        str(item).strip()
                        for item in list((parsed.criteria or {}).get("attribute_hints") or [])
                        if str(item).strip()
                    ]
                    for item in list(criteria_result.get("matches") or [])[:50]:
                        entity_name = str(item.get("entity_name") or "").strip()
                        if not entity_name:
                            continue

                        def _lookup_value(attr_name: str) -> Any:
                            found = self.lookup_attribute(attr_name, entity_name)
                            if not isinstance(found, dict):
                                return None
                            if str(found.get("resolution_status") or "").strip().lower() == "ambiguous_entity":
                                return None
                            return found.get("attribute_value")

                        row_attributes: dict[str, Any] = {}
                        for attr_name in requested_attributes:
                            value = _lookup_value(attr_name)
                            if value is None:
                                continue
                            row_attributes[attr_name] = value

                        row = {
                            "entity_name": entity_name,
                            "filter_value": requested_filter,
                            "attributes": row_attributes,
                        }
                        rows.append(row)

                    answer_lines: list[str] = []
                    for row in rows[:20]:
                        entity_name = str(row.get("entity_name") or "").strip()
                        filter_value = str(row.get("filter_value") or "").strip()
                        attributes = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
                        parts = [entity_name]
                        if filter_value:
                            parts.append(f"filter={filter_value}")
                        if isinstance(attributes, dict) and attributes:
                            attr_text = ", ".join(f"{key}={value}" for key, value in attributes.items())
                            if attr_text:
                                parts.append(attr_text)
                        answer_lines.append(" | ".join(parts))

                    answer_text = "Entity directory: " + "; ".join(answer_lines) + "." if answer_lines else "No matching records found."
                    return {
                        "question": question,
                        "intent": parsed.intent,
                        "answer": self._prefix_source(answer_text, criteria_source),
                        "data": {
                            "rows": rows,
                            "criteria_result": self._attach_match_telemetry(criteria_result, match_telemetry),
                        },
                        "matches": criteria_result.get("matches"),
                        "source": criteria_source,
                        "candidate_count": len(rows),
                    }
                answer_text = "Matches: " + ", ".join([name for name in top_names if name]) + "."
                if response_format == "doc_titles_and_files":
                    lines: list[str] = []
                    for item in list(criteria_result.get("matches") or [])[:20]:
                        title = str(item.get("title") or item.get("doc_name") or item.get("entity_name") or "").strip()
                        file_name = str(item.get("file_name") or os.path.basename(str(item.get("doc_path") or "").strip()) or "").strip()
                        file_path = str(item.get("file_path") or item.get("doc_path") or "").strip()
                        if title and file_name:
                            lines.append(f"{title} | {file_name} | {file_path}")
                        elif title:
                            lines.append(f"{title} | {file_path}")
                    if lines:
                        answer_text = "Document titles and file names: " + "; ".join(lines) + "."
                return {
                    "question": question,
                    "intent": parsed.intent,
                    "answer": self._prefix_source(
                        answer_text,
                        criteria_source,
                    ),
                    "data": self._attach_match_telemetry(criteria_result, match_telemetry),
                    "matches": criteria_result.get("matches"),
                    "source": criteria_source,
                    "candidate_count": len(criteria_result.get("matches") or []),
                }

        if parsed.intent == "attribute_lookup" and not parsed.entity_name and isinstance(parsed.criteria, dict) and parsed.criteria.get("pattern_id"):
            pattern, concept, rule = self._resolve_pattern_and_rule(parsed)
            concept_text = str(concept or "derived_attribute").strip() or "derived_attribute"
            rule_text = str(rule or "").strip() or "n/a"
            if str(parsed.language or "en").lower() == "de":
                missing_entity_msg = (
                    f"Die Pattern-Regel '{concept_text}' benötigt eine Entität im Query. "
                    f"Bitte frage zum Beispiel: 'relationship duration of <entity>' (Regel: {rule_text})."
                )
            else:
                missing_entity_msg = (
                    f"Pattern rule '{concept_text}' requires an entity in the query. "
                    f"Try for example: 'relationship duration of <entity>' (rule: {rule_text})."
                )
            return {
                "question": question,
                "intent": parsed.intent,
                "answer": self._prefix_source(missing_entity_msg, "derived_rule_input_missing"),
                "data": {
                    "derived_error": {
                        "code": "entity_required_for_pattern",
                        "semantic_concept": concept_text,
                        "rule": rule_text,
                        "matched_pattern": str((parsed.criteria or {}).get("matched_pattern") or ""),
                    }
                },
                "source": "derived_rule_input_missing",
            }

        try:
            _diag_flow("sql_exact_lookup")
            with self._get_connection() as conn:
                if strict_table_request:
                    _ensure_semantic_context()
                    _diag_flow("table_cell_grounding")
                    grounded_cell = self._lookup_grounded_table_cell(
                        conn,
                        parent_entity=parsed.entity_name,
                        row_label=table_row_label,
                        column_label=table_column,
                        scope_doc_ids=doc_ids,
                    )
                    if grounded_cell:
                        grounded_value = grounded_cell.get("attribute_value")
                        grounded_row = grounded_cell.get("row_label")
                        grounded_col = grounded_cell.get("column_label")
                        grounded_doc = grounded_cell.get("doc_name") or grounded_cell.get("doc_path") or grounded_cell.get("doc_id")
                        return {
                            "question": question,
                            "intent": parsed.intent,
                            "answer": self._prefix_source(
                                f"{grounded_row} in {grounded_col}: {grounded_value} (document: {grounded_doc}).",
                                "table_cell_grounded",
                            ),
                            "data": self._attach_match_telemetry(grounded_cell, match_telemetry),
                            "source": "table_cell_grounded",
                            "candidate_count": len(candidates),
                        }

                if parsed.intent == "attribute_lookup" and parsed.entity_name and self._is_full_profile_request(question):
                    profile = self._sql_entity_profile_lookup(
                        conn,
                        parsed.entity_name,
                        temporal_scope=self._infer_temporal_scope_from_question(question),
                    )
                    if profile and isinstance(profile.get("attributes"), list):
                        parts: list[str] = []
                        for item in profile["attributes"][:12]:
                            attr_name = str(item.get("attribute_name") or "attribute").strip() or "attribute"
                            display_attr = self._get_attribute_display_name(attr_name, parsed.language)
                            parts.append(f"{display_attr}: {item.get('attribute_value')}")
                        if parts:
                            entity_label = str(profile.get("entity_name") or parsed.entity_name)
                            return {
                                "question": question,
                                "intent": parsed.intent,
                                "answer": self._prefix_source(f"{entity_label} -> " + "; ".join(parts) + ".", "sql_profile_composite"),
                                "data": self._attach_match_telemetry(profile, match_telemetry),
                                "source": "sql_profile_composite",
                            }

                if (
                    parsed.intent == "attribute_lookup"
                    and str(parsed.attribute_name or "").strip().lower() == "shareholders"
                    and isinstance(parsed.criteria, dict)
                    and str(parsed.criteria.get("question_type") or "") == "share_count"
                    and parsed.entity_name
                    and str(parsed.criteria.get("target_company") or "").strip()
                ):
                    share_count_result = self._sql_share_count_for_person_company(
                        conn,
                        parsed.entity_name,
                        str(parsed.criteria.get("target_company") or ""),
                    )
                    if share_count_result:
                        person_name = str(share_count_result.get("entity_name") or parsed.entity_name)
                        company_name = str(share_count_result.get("related_entity_name") or parsed.criteria.get("target_company") or "")
                        share_count_value = share_count_result.get("attribute_value")
                        if share_count_value is None:
                            answer_text = f"Share count for {person_name} in {company_name} is not available."
                        else:
                            answer_text = f"{person_name} holds {share_count_value} shares of {company_name}."
                        return {
                            "question": question,
                            "intent": parsed.intent,
                            "answer": self._prefix_source(answer_text, "sql_exact"),
                            "data": self._attach_match_telemetry(share_count_result, match_telemetry),
                            "source": "sql_exact",
                        }

                has_relation_filters = bool(
                    isinstance(parsed.criteria, dict)
                    and (
                        list(parsed.criteria.get("relation_filters") or [])
                        or list(parsed.criteria.get("relationship_filters") or [])
                    )
                )
                strict_relation_lookup = bool(
                    parsed.intent == "attribute_lookup"
                    and (
                        str(parsed.relation_name or "").strip()
                        or has_relation_filters
                    )
                )
                # Safety gate: explicit relationship phrasing (for example
                # "grandchild of <person>") must not fall back to the base
                # entity's own attributes if relationship traversal is unresolved.
                relation_phrase_hint = bool(
                    re.search(
                        r"\b(?:grandchild|child|son|daughter|parent|spouse|husband|wife|partner|ansprechpartner|contact\s+person)\b\s+\b(?:of|von|der|des)\b",
                        question,
                        flags=re.IGNORECASE,
                    )
                )
                if relation_phrase_hint:
                    strict_relation_lookup = True

                criteria_for_lookup: dict[str, Any] = (
                    dict(parsed.criteria) if isinstance(parsed.criteria, dict) else {}
                )
                answer_attribute_filters = self._attribute_filters_from_criteria(criteria_for_lookup)
                criteria_for_lookup["temporal_scope"] = self._infer_temporal_scope_from_question(question)
                if strict_relation_lookup:
                    criteria_for_lookup["strict_relation_lookup"] = True
                    parsed.criteria = dict(criteria_for_lookup)
                if (
                    parsed.intent == "attribute_lookup"
                    and (parsed.entity_name or has_relation_filters)
                    and (parsed.attribute_name or parsed.relation_name)
                ):
                    lookup_entity_name = parsed.entity_name
                    is_identifier_query = self._looks_like_identifier_request(question)
                    parsed_attr_name = str(parsed.attribute_name or "").strip()
                    strict_identifier_mode = bool(
                        is_identifier_query
                        and parsed_attr_name
                        and not parsed.relation_name
                    )
                    if self._is_suspicious_entity_hint(lookup_entity_name):
                        recovered_entity = self._best_entity_hint(question)
                        if recovered_entity and not self._is_suspicious_entity_hint(recovered_entity):
                            lookup_entity_name = recovered_entity

                    # Canonicalize the anchor entity early so strict relation filters
                    # do not miss because of minor spelling variants (for example
                    # "ExampelCo" vs "ExampleCo") in target_name.
                    if lookup_entity_name:
                        try:
                            resolved_lookup = self._resolve_entity_name_for_lookup(conn, lookup_entity_name)
                            if resolved_lookup:
                                lookup_entity_name = resolved_lookup
                        except Exception:
                            pass

                    # If planner relation filters target the same entity anchor as parsed.entity_name,
                    # rewrite target_name/target to the resolved canonical name.
                    if lookup_entity_name and isinstance(criteria_for_lookup, dict):
                        relation_filters_raw = criteria_for_lookup.get("relation_filters")
                        if isinstance(relation_filters_raw, list):
                            parsed_entity_key = self._normalize_alias_key(parsed.entity_name or "")
                            canonical_entity_name = str(lookup_entity_name).strip()
                            patched_filters: list[dict[str, Any]] = []
                            for item in relation_filters_raw:
                                if not isinstance(item, dict):
                                    patched_filters.append(item)
                                    continue
                                patched = dict(item)
                                raw_target_name = str(item.get("target_name") or "").strip()
                                raw_target = str(item.get("target") or "").strip()
                                target_text = raw_target_name or raw_target
                                target_key = self._normalize_alias_key(target_text)
                                if (
                                    parsed_entity_key
                                    and target_key
                                    and target_key == parsed_entity_key
                                ):
                                    patched["target_name"] = canonical_entity_name
                                    if "target" in patched:
                                        patched["target"] = canonical_entity_name
                                patched_filters.append(patched)
                            criteria_for_lookup["relation_filters"] = patched_filters

                    requested_attrs: list[str] = []
                    broad_scope = self._has_broad_attribute_scope_cue(question)
                    file_scope = self._has_file_info_cue(question)
                    if broad_scope:
                        criteria_for_lookup["broad_scope"] = True
                    file_info_attrs = {"document_file_info", "document_filename", "document_full_path"}
                    criteria_dict = parsed.criteria if isinstance(parsed.criteria, dict) else {}
                    parse_method = str(criteria_dict.get("parse_method") or "").strip().lower()
                    explicit_multi_attribute_request = bool(parsed.attribute_names and len(parsed.attribute_names) > 1)
                    is_process_contact_email_focus = (
                        parse_method == "process_contact_gate"
                        and str(parsed.attribute_name or "").strip().lower() == "process_contact_email"
                        and self._has_email_intent(question)
                        and not explicit_multi_attribute_request
                    )
                    if parsed.attribute_names:
                        for item in parsed.attribute_names:
                            name = str(item or "").strip()
                            if name and name not in requested_attrs:
                                requested_attrs.append(name)
                    elif parsed.attribute_name:
                        initial_attr = str(parsed.attribute_name).strip()
                        if not (
                            broad_scope
                            and not file_scope
                            and initial_attr in file_info_attrs
                        ):
                            requested_attrs.append(initial_attr)

                    if is_process_contact_email_focus:
                        requested_attrs = ["process_contact_email"]

                    if parsed.relation_name and not is_process_contact_email_focus:
                        relation_hints = self._related_attribute_hints_from_text(question, parsed.relation_name)
                        has_email_seed = any("email" in str(item or "").lower() for item in requested_attrs)
                        for item in relation_hints:
                            hint = str(item or "").strip()
                            if not hint:
                                continue
                            if hint == "email" and has_email_seed:
                                continue
                            if hint not in requested_attrs:
                                requested_attrs.append(hint)

                    if explicit_multi_attribute_request and parsed.relation_name and parsed.attribute_name == "contact_person":
                        for item in parsed.attribute_names:
                            hint = str(item or "").strip()
                            if hint and hint not in requested_attrs:
                                requested_attrs.append(hint)

                    has_multi_attr_cue = bool(
                        re.search(r"\b(?:and|und|or|oder|with|mit|sowie|plus|including|inkl\.?|samt)\b", question, flags=re.IGNORECASE)
                    )
                    # Keep explicit single-attribute asks deterministic and avoid semantic over-expansion.
                    strict_single_attr = (
                        parse_method == "explicit_attribute_lexical"
                        and not parsed.relation_name
                        and len(requested_attrs) <= 1
                        and not has_multi_attr_cue
                    )

                    if not strict_single_attr and not strict_identifier_mode:
                        semantic_candidates = self._expand_query_attribute_candidates(
                            question=question,
                            entity_name=lookup_entity_name,
                            relation_name=parsed.relation_name,
                        )
                        for item in semantic_candidates:
                            name = str(item or "").strip()
                            if name and name not in requested_attrs:
                                requested_attrs.append(name)

                    should_advise_scope = (
                        (broad_scope and not file_scope)
                        or float(parsed.confidence or 0.0) < 0.80
                    )
                    if (
                        should_advise_scope
                        and not strict_table_request
                        and lookup_entity_name
                        and not parsed.relation_name
                        and not strict_single_attr
                        and not strict_identifier_mode
                    ):
                        advised_attrs = self._llm_advise_attributes_for_query(
                            question=question,
                            entity_name=lookup_entity_name,
                            current_attribute=parsed.attribute_name,
                        )
                        for item in advised_attrs:
                            name = str(item or "").strip()
                            if name and name not in requested_attrs:
                                requested_attrs.append(name)

                    # Deterministic fallback for broad-scope requests: include known entity attrs.
                    if broad_scope and not file_scope and lookup_entity_name and not parsed.relation_name and not strict_identifier_mode:
                        known_attrs = [
                            str(item).strip()
                            for item in self._get_entity_attribute_names(lookup_entity_name)
                            if str(item).strip()
                        ]
                        skip_attrs = {"generic_doc3", "description"}
                        for item in known_attrs[:20]:
                            if item.lower() in skip_attrs:
                                continue
                            if item not in requested_attrs:
                                requested_attrs.append(item)

                    if broad_scope and not file_scope:
                        requested_attrs = [
                            item
                            for item in requested_attrs
                            if item not in file_info_attrs
                        ]

                    if parsed.relation_name and not file_scope:
                        requested_attrs = [
                            item
                            for item in requested_attrs
                            if item not in file_info_attrs
                        ]

                    if len(requested_attrs) > 1:
                        if strict_table_request:
                            requested_attrs = requested_attrs[:1]
                        if explicit_multi_attribute_request and parsed.relation_name and parsed.attribute_name in parsed.attribute_names:
                            requested_attrs = [str(item).strip() for item in parsed.attribute_names if str(item).strip()]
                        multi_matches: list[dict[str, Any]] = []
                        for attr in requested_attrs[:12]:
                            match = self.lookup_attribute(
                                attr,
                                lookup_entity_name,
                                relation_name=parsed.relation_name,
                                criteria=criteria_for_lookup,
                            )
                            if match and str(match.get("resolution_status") or "") == "ambiguous_entity":
                                return self._build_ambiguous_entity_response(
                                    question=question,
                                    intent=parsed.intent,
                                    language=parsed.language,
                                    ambiguity_data=match,
                                )
                            if match:
                                if answer_attribute_filters and not self._result_satisfies_attribute_filters(conn, match, answer_attribute_filters):
                                    fallback_diagnostics.setdefault("filtered_out_matches", [])
                                    if isinstance(fallback_diagnostics.get("filtered_out_matches"), list):
                                        fallback_diagnostics["filtered_out_matches"].append(
                                            {
                                                "attribute_name": str(match.get("attribute_name") or attr),
                                                "entity_name": str(match.get("entity_name") or lookup_entity_name or ""),
                                                "reason": "attribute_filters_not_satisfied",
                                            }
                                        )
                                    continue
                                multi_matches.append(match)

                        if multi_matches:
                            unique_multi_matches: list[dict[str, Any]] = []
                            seen_attr_names: set[str] = set()
                            for item in multi_matches:
                                key = str(item.get("attribute_name") or "").strip().lower()
                                if key and key in seen_attr_names:
                                    continue
                                if key:
                                    seen_attr_names.add(key)
                                unique_multi_matches.append(item)
                            multi_matches = unique_multi_matches

                        if broad_scope and not file_scope:
                            multi_matches = [
                                item
                                for item in multi_matches
                                if str(item.get("attribute_name") or "").strip() not in file_info_attrs
                            ]

                        if multi_matches:
                            derived_multi_answer = self._derived_answer_from_pattern_matches(parsed, multi_matches)
                            if derived_multi_answer:
                                single_attr_name = str(multi_matches[0].get("attribute_name") or "").strip()
                                self._try_learn_pattern(
                                    question,
                                    parsed,
                                    "sql_exact_derived",
                                    [single_attr_name] if single_attr_name else None,
                                )
                                return {
                                    "question": question,
                                    "intent": parsed.intent,
                                    "answer": self._prefix_source(derived_multi_answer, "sql_exact_derived"),
                                    "data": self._attach_match_telemetry({
                                        "entity_name": parsed.entity_name,
                                        "attributes": [
                                            {
                                                "attribute_name": str(item.get("attribute_name") or ""),
                                                "attribute_value": item.get("attribute_value"),
                                            }
                                            for item in multi_matches
                                        ],
                                    }, match_telemetry),
                                    "source": "sql_exact_derived",
                                }

                            derived_diag = self._derived_clause_diagnostics(parsed, multi_matches)
                            if derived_diag:
                                return {
                                    "question": question,
                                    "intent": parsed.intent,
                                    "answer": self._prefix_source(str(derived_diag.get("message") or "Derived rule inputs are incomplete."), "derived_rule_input_missing"),
                                    "data": {
                                        "entity_name": parsed.entity_name,
                                        "attributes": [
                                            {
                                                "attribute_name": str(item.get("attribute_name") or ""),
                                                "attribute_value": item.get("attribute_value"),
                                            }
                                            for item in multi_matches
                                        ],
                                        "derived_error": derived_diag,
                                    },
                                    "source": "derived_rule_input_missing",
                                }

                            # Keep shareholder answers consistent with sql_exact formatting.
                            shareholder_match = None
                            for item in multi_matches:
                                attr_name = str(item.get("attribute_name") or "").strip().lower()
                                attr_value = item.get("attribute_value")
                                if attr_name in {"shareholders", "shareholder_of"} and isinstance(attr_value, list):
                                    shareholder_match = item
                                    break

                            if shareholder_match is not None:
                                value = shareholder_match.get("attribute_value")
                                shareholder_parts: list[str] = []
                                for holder in value if isinstance(value, list) else []:
                                    if isinstance(holder, dict):
                                        holder_name = str(holder.get("name") or "").strip()
                                        if not holder_name:
                                            continue
                                        holder_role = str(holder.get("role") or "").strip()
                                        if holder_role:
                                            shareholder_parts.append(f"{holder_name} ({holder_role})")
                                        else:
                                            shareholder_parts.append(holder_name)
                                    else:
                                        holder_text = str(holder).strip()
                                        if holder_text:
                                            shareholder_parts.append(holder_text)

                                shareholder_answer = ", ".join(shareholder_parts) if shareholder_parts else "None found."
                                entity_label = str(shareholder_match.get("entity_name") or parsed.entity_name or "the entity").strip() or "the entity"

                                attr_names_learned = [str(item.get("attribute_name") or "") for item in multi_matches if item.get("attribute_name")]
                                self._try_learn_pattern(question, parsed, "sql_exact_composite", attr_names_learned)

                                return {
                                    "question": question,
                                    "intent": parsed.intent,
                                    "answer": self._prefix_source(
                                        f"Shareholders of {entity_label}: {shareholder_answer}.",
                                        "sql_exact_composite",
                                    ),
                                    "data": self._attach_match_telemetry({
                                        "entity_name": entity_label,
                                        "attributes": [
                                            {
                                                "attribute_name": str(item.get("attribute_name") or ""),
                                                "attribute_value": item.get("attribute_value"),
                                            }
                                            for item in multi_matches
                                        ],
                                    }, match_telemetry),
                                    "source": "sql_exact_composite",
                                }

                            parts: list[str] = []
                            for match in multi_matches:
                                attr_name = str(match.get("attribute_name") or "attribute").strip() or "attribute"
                                display_attr = self._get_attribute_display_name(attr_name, parsed.language)
                                value = self._repair_mojibake_value(match.get("attribute_value"))
                                rendered_value = self._format_attribute_value_for_answer(value)
                                parts.append(f"{display_attr}: {rendered_value}")

                            # Try to learn pattern if available
                            attr_names_learned = [str(item.get("attribute_name") or "") for item in multi_matches if item.get("attribute_name")]
                            self._try_learn_pattern(question, parsed, "sql_exact_composite", attr_names_learned)
                            
                            return {
                                "question": question,
                                "intent": parsed.intent,
                                "answer": self._prefix_source(f"{parsed.entity_name} -> " + "; ".join(parts) + ".", "sql_exact_composite"),
                                "data": self._attach_match_telemetry({
                                    "entity_name": parsed.entity_name,
                                    "attributes": [
                                        {
                                            "attribute_name": str(item.get("attribute_name") or ""),
                                            "attribute_value": item.get("attribute_value"),
                                        }
                                        for item in multi_matches
                                    ],
                                }, match_telemetry),
                                "source": "sql_exact_composite",
                            }

                    direct_lookup_name = parsed.attribute_name or parsed.relation_name
                    if parsed.attribute_names and len(parsed.attribute_names) > 1 and parsed.attribute_name in parsed.attribute_names:
                        direct_lookup_name = None
                    if (
                        broad_scope
                        and not file_scope
                        and str(direct_lookup_name or "").strip() in file_info_attrs
                    ):
                        direct_lookup_name = next(
                            (item for item in requested_attrs if str(item).strip() not in file_info_attrs),
                            None,
                        )

                    direct = None
                    if direct_lookup_name:
                        direct = self.lookup_attribute(
                            direct_lookup_name,
                            lookup_entity_name,
                            relation_name=parsed.relation_name,
                            criteria=criteria_for_lookup,
                        )
                    ownership_query = bool(
                        re.search(
                            r"\b(?:shareholders?|share\s+holders?|holds?_shares?_in|owns?|ownership|owner|beneficial[_\s-]?owner|gesellschafter|aktion[aä]r(?:e)?)\b",
                            " ".join(
                                str(value or "")
                                for value in (
                                    parsed.attribute_name,
                                    parsed.relation_name,
                                    question,
                                )
                            ),
                            flags=re.IGNORECASE,
                        )
                    )
                    if not direct and ownership_query:
                        direct = self._recover_shareholder_lookup_from_question(
                            conn=conn,
                            parsed=parsed,
                            question=question,
                        )
                    if not direct and strict_identifier_mode and lookup_entity_name:
                        inferred_attr = self._infer_identifier_attribute_for_entity(
                            entity_name=lookup_entity_name,
                            question=question,
                        )
                        if inferred_attr:
                            inferred_direct = self.lookup_attribute(
                                inferred_attr,
                                lookup_entity_name,
                                relation_name=parsed.relation_name,
                                criteria=criteria_for_lookup,
                            )
                            if inferred_direct:
                                direct = inferred_direct
                                parsed.attribute_name = inferred_attr
                                if isinstance(parsed.criteria, dict):
                                    parsed.criteria.setdefault("identifier_attr_inferred", inferred_attr)
                    if direct and str(direct.get("resolution_status") or "") == "ambiguous_entity":
                        return self._build_ambiguous_entity_response(
                            question=question,
                            intent=parsed.intent,
                            language=parsed.language,
                            ambiguity_data=direct,
                        )
                    if direct and answer_attribute_filters and not self._result_satisfies_attribute_filters(conn, direct, answer_attribute_filters):
                        fallback_diagnostics.setdefault("filtered_out_matches", [])
                        if isinstance(fallback_diagnostics.get("filtered_out_matches"), list):
                            fallback_diagnostics["filtered_out_matches"].append(
                                {
                                    "attribute_name": str(direct.get("attribute_name") or direct_lookup_name or ""),
                                    "entity_name": str(direct.get("entity_name") or lookup_entity_name or ""),
                                    "reason": "attribute_filters_not_satisfied",
                                }
                            )
                        direct = None
                    if direct and self._requires_contextual_value_resolution(
                        question=question,
                        parsed=parsed,
                        criteria=criteria_for_lookup,
                        direct_result=direct,
                    ):
                        fallback_diagnostics["contextual_value_guard"] = {
                            "applied": True,
                            "reason": "contextual_value_query_requires_grounded_resolution",
                        }
                        direct = None
                    if direct:
                        is_compatible, compatibility_reason = self._result_is_relevance_compatible(
                            question=question,
                            parsed=parsed,
                            result=direct,
                        )
                        if not is_compatible:
                            fallback_diagnostics["relevance_guard"] = {
                                "applied": True,
                                "reason": compatibility_reason,
                                "attribute_name": str(direct.get("attribute_name") or parsed.attribute_name or ""),
                            }
                            direct = None
                    if direct:
                        source = "sql_exact"
                        attr_name = str(direct.get("attribute_name") or parsed.attribute_name or parsed.relation_name or "attribute").strip() or "attribute"
                        display_attr = self._get_attribute_display_name(attr_name, parsed.language) if attr_name != "attribute" else "attribute"
                        value = self._repair_mojibake_value(direct.get("attribute_value"))
                        if (
                            isinstance(value, list)
                            and len(value) > 1
                            and self._contains_singular_resolution_intent(question)
                            and not self._contains_plural_resolution_intent(question)
                            and not self._is_naturally_multi_value_attribute(attr_name)
                        ):
                            return self._build_ambiguous_value_response(
                                question=question,
                                intent=parsed.intent,
                                language=parsed.language,
                                entity_name=str(direct.get("entity_name") or parsed.entity_name or "the entity"),
                                attribute_name=attr_name,
                                values=list(value),
                                source_match=direct,
                            )
                        source_entity_name = str(direct.get("source_entity_name") or parsed.entity_name or "").strip() or parsed.entity_name
                        related_entity_name = str(direct.get("related_entity_name") or direct.get("entity_name") or parsed.entity_name or "").strip() or parsed.entity_name
                        relationship_name = str(direct.get("relationship_name") or parsed.relation_name or "").strip() or None

                        derived_answer_text = self._derived_answer_from_pattern_matches(parsed, [direct])
                        if derived_answer_text:
                            self._try_learn_pattern(question, parsed, "sql_exact_derived", [attr_name] if attr_name else None)
                            return {
                                "question": question,
                                "intent": parsed.intent,
                                "answer": self._prefix_source(derived_answer_text, "sql_exact_derived"),
                                "data": self._attach_match_telemetry(direct, match_telemetry),
                                "source": "sql_exact_derived",
                            }

                        derived_diag = self._derived_clause_diagnostics(parsed, [direct])
                        if derived_diag:
                            return {
                                "question": question,
                                "intent": parsed.intent,
                                "answer": self._prefix_source(str(derived_diag.get("message") or "Derived rule inputs are incomplete."), "derived_rule_input_missing"),
                                "data": {
                                    "direct": direct,
                                    "derived_error": derived_diag,
                                },
                                "source": "derived_rule_input_missing",
                            }

                        if attr_name == "shareholders" or str(direct.get("relationship_concept") or ""):
                            wants_counts = bool(
                                isinstance(parsed.criteria, dict)
                                and str(parsed.criteria.get("question_type") or "") == "shareholders_with_counts"
                            ) or bool(
                                re.search(
                                    r"\b(?:how\s+many\s+shares?\s+(?:each|each\s+one|per\s+shareholder)|shares?\s+each\s+(?:holds?|owns?)|each\s+holdings?|holdings?\s+each|wie\s+viele\s+anteile\s+(?:je|pro))\b",
                                    question,
                                    flags=re.IGNORECASE,
                                )
                            )

                            if isinstance(value, list):
                                parts: list[str] = []
                                for item in value:
                                    if isinstance(item, dict):
                                        name = str(item.get("name") or "").strip()
                                        if not name:
                                            continue
                                        if wants_counts and item.get("share_count") is not None:
                                            parts.append(f"{name}: {item['share_count']}")
                                        elif item.get("role"):
                                            parts.append(f"{name} ({item['role']})")
                                        else:
                                            parts.append(name)
                                    else:
                                        s = str(item).strip()
                                        if s:
                                            parts.append(s)
                                answer = ", ".join(parts) if parts else "None found."
                            else:
                                answer = str(value).strip() if str(value or "").strip() else "None found."
                            self._try_learn_pattern(question, parsed, source, [attr_name] if attr_name else None)
                            return {
                                "question": question,
                                "intent": parsed.intent,
                                "answer": self._prefix_source(
                                    f"{display_attr.capitalize()} of {direct.get('entity_name') or parsed.entity_name}: {answer}.",
                                    source,
                                ),
                                "data": self._attach_match_telemetry(direct, match_telemetry),
                                "source": source,
                            }

                        if attr_name == "governance_roles":
                            roles_value = value if isinstance(value, list) else []
                            role_parts: list[str] = []
                            for item in roles_value:
                                if not isinstance(item, dict):
                                    continue
                                person_name = str(item.get("name") or "").strip()
                                if not person_name:
                                    continue
                                role_label = str(item.get("role") or "").strip()
                                signing_label = str(item.get("signing_authority") or "").strip()
                                if role_label and signing_label:
                                    role_parts.append(f"{person_name} ({role_label}, {signing_label})")
                                elif role_label:
                                    role_parts.append(f"{person_name} ({role_label})")
                                elif signing_label:
                                    role_parts.append(f"{person_name} ({signing_label})")
                                else:
                                    role_parts.append(person_name)

                            joined_roles = ", ".join(role_parts) if role_parts else "None found."
                            entity_label = str(direct.get("entity_name") or parsed.entity_name or "the entity").strip() or "the entity"
                            if str(parsed.language or "en").lower() == "de":
                                answer_text = (
                                    f"Keine expliziten Eigentümerdaten für {entity_label} gefunden. "
                                    f"Gefundene Governance-Funktionen: {joined_roles}."
                                )
                            else:
                                answer_text = (
                                    f"No explicit ownership evidence found for {entity_label}. "
                                    f"Governance roles found: {joined_roles}."
                                )
                            self._try_learn_pattern(question, parsed, source, [attr_name] if attr_name else None)
                            return {
                                "question": question,
                                "intent": parsed.intent,
                                "answer": self._prefix_source(answer_text, source),
                                "data": self._attach_match_telemetry(direct, match_telemetry),
                                "source": source,
                            }

                        if attr_name == "eori_contact_email":
                            contact_person = str(direct.get("contact_person") or direct.get("related_entity_name") or "").strip()
                            if str(parsed.language or "en").lower() == "de":
                                if contact_person:
                                    answer_text = f"{display_attr} für {parsed.entity_name} ist {value} (Kontakt: {contact_person})."
                                else:
                                    answer_text = f"{display_attr} für {parsed.entity_name} ist {value}."
                            else:
                                if contact_person:
                                    answer_text = f"{display_attr} for {parsed.entity_name} is {value} (contact: {contact_person})."
                                else:
                                    answer_text = f"{display_attr} for {parsed.entity_name} is {value}."
                            self._try_learn_pattern(question, parsed, source, [attr_name] if attr_name else None)
                            return {
                                "question": question,
                                "intent": parsed.intent,
                                "answer": self._prefix_source(answer_text, source),
                                "data": self._attach_match_telemetry(direct, match_telemetry),
                                "source": source,
                            }

                        if attr_name == "total_capital":
                            currency = str(direct.get("currency") or "CHF").strip() or "CHF"
                            try:
                                amount = float(value)
                                amount_text = f"{amount:,.2f}".replace(",", "")
                            except (TypeError, ValueError):
                                amount_text = str(value)
                            if str(parsed.language or "en").lower() == "de":
                                answer_text = f"Gesamtkapital für {parsed.entity_name} ist {amount_text} {currency}."
                            else:
                                answer_text = f"Total capital for {parsed.entity_name} is {amount_text} {currency}."
                            self._try_learn_pattern(question, parsed, source, [attr_name] if attr_name else None)
                            return {
                                "question": question,
                                "intent": parsed.intent,
                                "answer": self._prefix_source(answer_text, source),
                                "data": self._attach_match_telemetry(direct, match_telemetry),
                                "source": source,
                            }

                        if attr_name == "document_file_info":
                            info_val = value if isinstance(value, dict) else {}
                            file_name = info_val.get("file_name")
                            full_path = info_val.get("full_path")
                            if full_path:
                                answer_text = f"Dateiname: {file_name}. Vollständiger Pfad: {full_path}."
                            else:
                                answer_text = f"Dateiname: {file_name}. Vollständiger Pfad nicht verfügbar."
                            self._try_learn_pattern(question, parsed, source, [attr_name] if attr_name else None)
                            return {
                                "question": question,
                                "intent": parsed.intent,
                                "answer": self._prefix_source(answer_text, source),
                                "data": self._attach_match_telemetry(direct, match_telemetry),
                                "source": source,
                            }

                        if relationship_name and related_entity_name and related_entity_name != source_entity_name:
                            rendered_value = self._format_attribute_value_for_answer(value)
                            if isinstance(value, (list, dict)):
                                if str(parsed.language or "en").lower() == "de":
                                    answer_text = f"{display_attr} für {related_entity_name} über {relationship_name} von {source_entity_name} sind {rendered_value}."
                                else:
                                    answer_text = f"{display_attr} for {related_entity_name} via {relationship_name} of {source_entity_name} are {rendered_value}."
                            else:
                                if str(parsed.language or "en").lower() == "de":
                                    answer_text = f"{display_attr} für {related_entity_name} über {relationship_name} von {source_entity_name} ist {value}."
                                else:
                                    answer_text = f"{display_attr} for {related_entity_name} via {relationship_name} of {source_entity_name} is {value}."
                        else:
                            rendered_value = self._format_attribute_value_for_answer(value)
                            if isinstance(value, (list, dict)):
                                if str(parsed.language or "en").lower() == "de":
                                    answer_text = f"{display_attr} für {parsed.entity_name} sind {rendered_value}."
                                else:
                                    answer_text = f"{display_attr} for {parsed.entity_name} are {rendered_value}."
                            else:
                                if str(parsed.language or "en").lower() == "de":
                                    answer_text = f"{display_attr} für {parsed.entity_name} ist {value}."
                                else:
                                    answer_text = f"{display_attr} for {parsed.entity_name} is {value}."

                        self._try_learn_pattern(question, parsed, source, [attr_name] if attr_name else None)
                        return {
                            "question": question,
                            "intent": parsed.intent,
                            "answer": self._prefix_source(answer_text, source),
                            "data": direct,
                            "source": source,
                        }

                    # Strict relation traversal safeguard:
                    # if no direct SQL match is found for a relation-driven lookup,
                    # stop and return not_found instead of drifting to broad fallback paths.
                    if strict_relation_lookup:
                        relation_label = str(parsed.relation_name or "relationship").strip() or "relationship"
                        entity_label = str(lookup_entity_name or parsed.entity_name or "the entity").strip() or "the entity"
                        fallback_diagnostics["strict_relation_lookup"] = True
                        fallback_diagnostics["relation_name"] = relation_label
                        fallback_diagnostics["entity_name"] = entity_label
                        return {
                            "question": question,
                            "intent": parsed.intent,
                            "answer": self._prefix_source(
                                f"No information found in IDMS for {relation_label} of {entity_label}.",
                                "not_found",
                            ),
                            "data": self._attach_match_telemetry(
                                {
                                    "reason": "relation_lookup_not_found",
                                    "fallback_diagnostics": fallback_diagnostics,
                                },
                                match_telemetry,
                            ),
                            "source": "not_found",
                        }

                if parsed.intent == "attribute_lookup" and parsed.attribute_name == "document_file_info":
                    semantic_file_info = self._document_file_info_from_semantic_candidates(conn, candidates)
                    if not semantic_file_info:
                        semantic_hint = str(parsed.entity_name or "").strip() or question
                        semantic_file_info = self._document_file_info_from_semantic_text(conn, semantic_hint)
                    if semantic_file_info:
                        info_val = semantic_file_info.get("attribute_value") if isinstance(semantic_file_info.get("attribute_value"), dict) else {}
                        file_name = info_val.get("file_name")
                        full_path = info_val.get("full_path")
                        answer_text = f"Dateiname: {file_name}. Vollständiger Pfad: {full_path}."
                        return {
                            "question": question,
                            "intent": parsed.intent,
                            "answer": self._prefix_source(answer_text, "qdrant_sql_fallback"),
                            "data": self._attach_match_telemetry(semantic_file_info, match_telemetry),
                            "source": "qdrant_sql_fallback",
                            "candidate_count": len(candidates),
                        }

                _ensure_semantic_context()
                fallback = self.sql_fallback_from_ids(conn, parsed, node_ids, edge_ids, doc_ids)
                if fallback:
                    is_compatible, compatibility_reason = self._result_is_relevance_compatible(
                        question=question,
                        parsed=parsed,
                        result=fallback,
                    )
                    if not is_compatible:
                        fallback_diagnostics["relevance_guard"] = {
                            "applied": True,
                            "reason": compatibility_reason,
                            "attribute_name": str(fallback.get("attribute_name") or parsed.attribute_name or ""),
                            "source": "qdrant_sql_fallback",
                        }
                        fallback = None
                if fallback:
                    _diag_flow("qdrant_sql_fallback")
                    source = "qdrant_sql_fallback"
                    # Get language-aware display name for attribute
                    fallback_attr = fallback.get('attribute_name', 'attribute')
                    display_attr = self._get_attribute_display_name(fallback_attr, parsed.language) if fallback_attr != 'attribute' else 'attribute'
                    entity_name = fallback.get('entity_name', 'the entity')
                    attr_value = fallback.get('attribute_value')
                    if isinstance(attr_value, list):
                        joined = ", ".join(str(item).strip() for item in attr_value if str(item).strip())
                        if str(parsed.language or "en").lower() == "de":
                            answer_text = f"{display_attr} für {entity_name} sind {joined}."
                        else:
                            answer_text = f"{display_attr} for {entity_name} are {joined}."
                    else:
                        answer_text = f"{display_attr} for {entity_name} is {attr_value}."

                    return {
                        "question": question,
                        "intent": parsed.intent,
                        "answer": self._prefix_source(answer_text, source),
                        "data": self._attach_match_telemetry(fallback, match_telemetry),
                        "source": source,
                        "candidate_count": len(candidates),
                    }
        except Exception:
            pass

        if strict_table_request:
            _ensure_semantic_context()
            source = "table_grounding_required"
            row_hint = table_row_label or "(row label missing)"
            column_hint = table_column or "(column missing)"
            answer_text = (
                "I did not find a grounded table-cell match for this request and stopped fallback to avoid an ungrounded answer. "
                f"Requested row='{row_hint}', column='{column_hint}'."
            )
            data = self._attach_match_telemetry(None, match_telemetry)
            if isinstance(data, dict):
                data["fallback_diagnostics"] = fallback_diagnostics
            return {
                "question": question,
                "intent": parsed.intent,
                "answer": self._prefix_source(answer_text, source),
                "data": data,
                "source": source,
                "candidate_count": len(candidates),
            }

        if parsed.intent == "attribute_lookup" and parsed.attribute_name:
            _ensure_semantic_context()
            if discovery_results:
                _diag_flow("discovery_fallback")
                discovery_match = self._lookup_attribute_in_discovery_results(
                    attribute_name=parsed.attribute_name,
                    entity_name=parsed.entity_name,
                    results=discovery_results,
                    attribute_filters=self._attribute_filters_from_criteria(parsed.criteria),
                )
                if discovery_match:
                    is_compatible, compatibility_reason = self._result_is_relevance_compatible(
                        question=question,
                        parsed=parsed,
                        result=discovery_match,
                    )
                    if not is_compatible:
                        fallback_diagnostics["relevance_guard"] = {
                            "applied": True,
                            "reason": compatibility_reason,
                            "attribute_name": str(discovery_match.get("attribute_name") or parsed.attribute_name or ""),
                            "source": "discovery_fallback",
                        }
                        discovery_match = None
                if discovery_match:
                    source = "discovery_fallback"
                    entity_label = parsed.entity_name or "the entity"
                    return {
                        "question": question,
                        "intent": parsed.intent,
                        "answer": self._prefix_source(
                            (
                                f"{parsed.attribute_name} for {entity_label} is "
                                f"{discovery_match['attribute_value']}."
                            ),
                            source,
                        ),
                        "data": self._attach_match_telemetry(discovery_match, match_telemetry),
                        "source": source,
                        "candidate_count": len(discovery_results),
                    }

        if (
            ENABLE_SEMANTIC_LLM_FALLBACK
            and parsed.intent == "attribute_lookup"
            and parsed.entity_name
            and parsed.attribute_name
            and not self._looks_like_identifier_request(question)
        ):
            _ensure_semantic_context()
            _diag_flow("semantic_llm_fallback")
            semantic_llm = self._semantic_llm_attribute_fallback(
                question=question,
                parsed=parsed,
                candidates=candidates,
                discovery_results=discovery_results,
                attribute_filters=self._attribute_filters_from_criteria(parsed.criteria),
            )
            if semantic_llm:
                is_compatible, compatibility_reason = self._result_is_relevance_compatible(
                    question=question,
                    parsed=parsed,
                    result=semantic_llm,
                )
                if not is_compatible:
                    fallback_diagnostics["relevance_guard"] = {
                        "applied": True,
                        "reason": compatibility_reason,
                        "attribute_name": str(semantic_llm.get("attribute_name") or parsed.attribute_name or ""),
                        "source": "semantic_llm_fallback",
                    }
                    semantic_llm = None
            if semantic_llm:
                source = "semantic_llm_fallback"
                self._try_learn_patterns_from_lexical_signals(
                    question=question,
                    parsed=parsed,
                    answer_source=source,
                    lexical_signals=semantic_llm.get("lexical_signals") if isinstance(semantic_llm, dict) else None,
                )
                display_attr = self._get_attribute_display_name(parsed.attribute_name, parsed.language)
                value = semantic_llm.get("attribute_value")
                if isinstance(value, list):
                    joined = ", ".join(str(item).strip() for item in value if str(item).strip())
                    if str(parsed.language or "en").lower() == "de":
                        answer_text = f"{display_attr} für {parsed.entity_name} sind {joined}."
                    else:
                        answer_text = f"{display_attr} for {parsed.entity_name} are {joined}."
                else:
                    if str(parsed.language or "en").lower() == "de":
                        answer_text = f"{display_attr} für {parsed.entity_name} ist {value}."
                    else:
                        answer_text = f"{display_attr} for {parsed.entity_name} is {value}."

                return {
                    "question": question,
                    "intent": parsed.intent,
                    "answer": self._prefix_source(answer_text, source),
                    "data": self._attach_match_telemetry(semantic_llm, match_telemetry),
                    "source": source,
                    "candidate_count": len(candidates),
                }

        # Multi-flow safety net: if primary parse path misses, try alternate explicit
        # attribute candidates and recovered entity hints before reporting not_found.
        if (
            parsed.intent == "attribute_lookup"
            and parsed.entity_name
            and not self._looks_like_identifier_request(question)
            and not self._attribute_filters_from_criteria(parsed.criteria)
        ):
            rescue_candidates = self._attribute_rescue_candidates_from_text(question)
            _diag_flow("sql_rescue_fallback")
            tried_attrs = {
                str(parsed.attribute_name or "").strip(),
                str(parsed.relation_name or "").strip(),
            }
            if parsed.attribute_names:
                for item in parsed.attribute_names:
                    tried_attrs.add(str(item or "").strip())

            if not rescue_candidates:
                token_candidates = sorted(
                    set(
                        re.findall(
                            r"[A-Za-zÄÖÜäöüß][A-Za-z0-9ÄÖÜäöüß_\-]{1,}",
                            str(question or ""),
                        )
                    ),
                    key=len,
                    reverse=True,
                )
                for token in token_candidates[:40]:
                    term_name, term_kind = self._resolve_schema_term(token, parsed.entity_name)
                    if term_name and term_kind == "attribute" and term_name not in rescue_candidates:
                        rescue_candidates.append(term_name)

            rescue_stopwords = {
                "for", "of", "in", "on", "at", "to", "from", "by", "with",
                "the", "a", "an", "and", "or", "how", "much", "was", "is",
                "are", "were", "mobile", "services", "gmbh", "aphotonix",
            }

            rescue_candidates = [
                name for name in rescue_candidates
                if (
                    str(name or "").strip()
                    and str(name).strip() not in tried_attrs
                    and str(name).strip().lower() not in rescue_stopwords
                )
            ]
            fallback_diagnostics["candidate_attributes"] = rescue_candidates[:12]

            if rescue_candidates:
                entity_candidates: list[str] = [parsed.entity_name]
                recovered_entity = self._best_entity_hint(question)
                if recovered_entity and recovered_entity not in entity_candidates:
                    entity_candidates.append(recovered_entity)
                fallback_diagnostics["candidate_entities"] = entity_candidates[:4]

                try:
                    with self._get_connection() as conn:
                        for entity_candidate in entity_candidates[:2]:
                            for attr_candidate in rescue_candidates[:8]:
                                rescue_match = self.lookup_attribute(
                                    attr_candidate,
                                    entity_candidate,
                                    relation_name=parsed.relation_name,
                                    criteria=criteria_for_lookup,
                                )
                                if rescue_match and str(rescue_match.get("resolution_status") or "") == "ambiguous_entity":
                                    return self._build_ambiguous_entity_response(
                                        question=question,
                                        intent=parsed.intent,
                                        language=parsed.language,
                                        ambiguity_data=rescue_match,
                                    )
                                if not rescue_match:
                                    continue

                                attr_name = str(rescue_match.get("attribute_name") or attr_candidate or "attribute").strip() or "attribute"
                                display_attr = self._get_attribute_display_name(attr_name, parsed.language)
                                value = rescue_match.get("attribute_value")
                                entity_label = str(rescue_match.get("entity_name") or entity_candidate or parsed.entity_name).strip() or parsed.entity_name
                                if isinstance(value, list):
                                    joined = ", ".join(str(item).strip() for item in value if str(item).strip())
                                    if str(parsed.language or "en").lower() == "de":
                                        answer_text = f"{display_attr} für {entity_label} sind {joined}."
                                    else:
                                        answer_text = f"{display_attr} for {entity_label} are {joined}."
                                else:
                                    if str(parsed.language or "en").lower() == "de":
                                        answer_text = f"{display_attr} für {entity_label} ist {value}."
                                    else:
                                        answer_text = f"{display_attr} for {entity_label} is {value}."

                                enriched = dict(rescue_match)
                                enriched["rescue_attribute"] = attr_candidate
                                enriched["rescue_entity"] = entity_candidate
                                enriched["fallback_diagnostics"] = fallback_diagnostics
                                return {
                                    "question": question,
                                    "intent": parsed.intent,
                                    "answer": self._prefix_source(answer_text, "sql_rescue_fallback"),
                                    "data": self._attach_match_telemetry(enriched, match_telemetry),
                                    "source": "sql_rescue_fallback",
                                    "candidate_count": len(candidates),
                                }
                except Exception:
                    pass

        if parsed.intent == "semantic_lookup":
            _ensure_semantic_context()
            contextual_resolution = self._contextual_llm_resolution_fallback(
                question=question,
                parsed=parsed,
                candidates=candidates,
                discovery_results=discovery_results,
            )
            if contextual_resolution:
                self._try_learn_patterns_from_lexical_signals(
                    question=question,
                    parsed=parsed,
                    answer_source="contextual_llm_resolution_fallback",
                    lexical_signals=contextual_resolution.get("lexical_signals") if isinstance(contextual_resolution, dict) else None,
                )
                return {
                    "question": question,
                    "intent": parsed.intent,
                    "answer": self._prefix_source(str(contextual_resolution.get("answer") or ""), "contextual_llm_resolution_fallback"),
                    "data": self._attach_match_telemetry(contextual_resolution, match_telemetry),
                    "source": "contextual_llm_resolution_fallback",
                    "candidate_count": len(candidates),
                }

            semantic_data = self._semantic_summary_from_candidates(candidates)
            if semantic_data:
                source = "qdrant_semantic"
                return {
                    "question": question,
                    "intent": parsed.intent,
                    "answer": self._prefix_source("Found semantically related context from indexed content.", source),
                    "data": self._attach_match_telemetry(semantic_data, match_telemetry),
                    "source": source,
                    "candidate_count": len(candidates),
                }

            discovery_semantic = self._semantic_summary_from_discovery(discovery_results)
            if discovery_semantic:
                source = "discovery_semantic"
                return {
                    "question": question,
                    "intent": parsed.intent,
                    "answer": self._prefix_source(
                        "Found semantically related media or document context from Discovery Engine.",
                        source,
                    ),
                    "data": self._attach_match_telemetry(discovery_semantic, match_telemetry),
                    "source": source,
                    "candidate_count": len(discovery_results),
                }

        # Last resort: LLM-based structured extraction (only fires when all other tiers failed)
        parsed_attribute_filters = self._attribute_filters_from_criteria(parsed.criteria)
        llm_parsed = None if self._looks_like_identifier_request(question) else self._llm_extract_intent(question)
        if llm_parsed and parsed_attribute_filters:
            llm_criteria = dict(llm_parsed.criteria) if isinstance(llm_parsed.criteria, dict) else {}
            inherited_plan = llm_criteria.get("llm_query_plan") if isinstance(llm_criteria.get("llm_query_plan"), dict) else {}
            inherited_filters = inherited_plan.get("filters") if isinstance(inherited_plan.get("filters"), dict) else {}
            if not inherited_filters.get("attribute_filters"):
                inherited_filters["attribute_filters"] = list(parsed_attribute_filters)
            inherited_plan["filters"] = inherited_filters
            llm_criteria["llm_query_plan"] = inherited_plan
            llm_criteria.setdefault("inherited_filter_context", True)
            llm_parsed.criteria = llm_criteria
        if llm_parsed and llm_parsed.entity_name and (llm_parsed.attribute_name or llm_parsed.relation_name):
            try:
                with self._get_connection() as conn:
                    llm_attribute_filters = self._attribute_filters_from_criteria(
                        llm_parsed.criteria if isinstance(llm_parsed.criteria, dict) else None
                    )
                    if llm_parsed.attribute_names:
                        multi_matches: list[dict[str, Any]] = []
                        for attr in llm_parsed.attribute_names[:4]:
                            match = self.lookup_attribute(
                                attr,
                                llm_parsed.entity_name,
                                relation_name=llm_parsed.relation_name,
                                criteria=llm_parsed.criteria if isinstance(llm_parsed.criteria, dict) else None,
                            )
                            if match and str(match.get("resolution_status") or "") == "ambiguous_entity":
                                return self._build_ambiguous_entity_response(
                                    question=question,
                                    intent=llm_parsed.intent,
                                    language=llm_parsed.language,
                                    ambiguity_data=match,
                                )
                            if match:
                                if llm_attribute_filters and not self._result_satisfies_attribute_filters(conn, match, llm_attribute_filters):
                                    continue
                                multi_matches.append(match)

                        if multi_matches:
                            parts: list[str] = []
                            for match in multi_matches:
                                attr_name = str(match.get("attribute_name") or "attribute").strip() or "attribute"
                                display_attr = self._get_attribute_display_name(attr_name, llm_parsed.language)
                                value = match.get("attribute_value")
                                if isinstance(value, list):
                                    joined = ", ".join(str(item).strip() for item in value if str(item).strip())
                                    parts.append(f"{display_attr}: {joined}")
                                else:
                                    parts.append(f"{display_attr}: {value}")

                            return {
                                "question": question,
                                "intent": llm_parsed.intent,
                                "answer": self._prefix_source(f"{llm_parsed.entity_name} -> " + "; ".join(parts) + ".", "sql_exact_llm_composite"),
                                "data": self._attach_match_telemetry({
                                    "entity_name": llm_parsed.entity_name,
                                    "attributes": [
                                        {
                                            "attribute_name": str(item.get("attribute_name") or ""),
                                            "attribute_value": item.get("attribute_value"),
                                        }
                                        for item in multi_matches
                                    ],
                                }, match_telemetry),
                                "source": "sql_exact_llm_composite",
                            }

                    if (llm_parsed.attribute_name or llm_parsed.relation_name) == "shareholders":
                        llm_shareholders = self.lookup_attribute(
                            llm_parsed.attribute_name or llm_parsed.relation_name,
                            llm_parsed.entity_name,
                            relation_name=llm_parsed.relation_name,
                            criteria=llm_parsed.criteria if isinstance(llm_parsed.criteria, dict) else None,
                        )
                        if llm_shareholders and str(llm_shareholders.get("resolution_status") or "") == "ambiguous_entity":
                            return self._build_ambiguous_entity_response(
                                question=question,
                                intent=llm_parsed.intent,
                                language=llm_parsed.language,
                                ambiguity_data=llm_shareholders,
                            )
                        if llm_shareholders:
                            names = [item.get("name") for item in (llm_shareholders.get("attribute_value") or []) if item.get("name")]
                            answer_text = ", ".join(names) if names else "No shareholders found."
                            display_attr = self._get_attribute_display_name("shareholders", llm_parsed.language)
                            return {
                                "question": question,
                                "intent": llm_parsed.intent,
                                "answer": self._prefix_source(
                                    f"{display_attr.capitalize()} of {llm_shareholders['entity_name']}: {answer_text}.",
                                    "sql_exact_llm",
                                ),
                                "data": self._attach_match_telemetry(llm_shareholders, match_telemetry),
                                "source": "sql_exact_llm",
                            }
                    direct = self.lookup_attribute(
                        llm_parsed.attribute_name or llm_parsed.relation_name,
                        llm_parsed.entity_name,
                        relation_name=llm_parsed.relation_name,
                        criteria=llm_parsed.criteria if isinstance(llm_parsed.criteria, dict) else None,
                    )
                    if direct and str(direct.get("resolution_status") or "") == "ambiguous_entity":
                        return self._build_ambiguous_entity_response(
                            question=question,
                            intent=llm_parsed.intent,
                            language=llm_parsed.language,
                            ambiguity_data=direct,
                        )
                    if direct and llm_attribute_filters and not self._result_satisfies_attribute_filters(conn, direct, llm_attribute_filters):
                        direct = None
                    if direct:
                        direct_attr_name = str(direct.get("attribute_name") or llm_parsed.attribute_name or llm_parsed.relation_name or "attribute").strip() or "attribute"
                        display_attr = self._get_attribute_display_name(direct_attr_name, llm_parsed.language)
                        value = direct.get("attribute_value")
                        if (
                            isinstance(value, list)
                            and len(value) > 1
                            and self._contains_singular_resolution_intent(question)
                            and not self._contains_plural_resolution_intent(question)
                            and not self._is_naturally_multi_value_attribute(direct_attr_name)
                        ):
                            return self._build_ambiguous_value_response(
                                question=question,
                                intent=llm_parsed.intent,
                                language=llm_parsed.language,
                                entity_name=str(direct.get("entity_name") or llm_parsed.entity_name or "the entity"),
                                attribute_name=direct_attr_name,
                                values=list(value),
                                source_match=direct,
                            )
                        if isinstance(value, list):
                            joined = ", ".join(str(item).strip() for item in value if str(item).strip())
                            if str(llm_parsed.language or "en").lower() == "de":
                                answer_text = f"{display_attr} für {direct['entity_name']} sind {joined}."
                            else:
                                answer_text = f"{display_attr} for {direct['entity_name']} are {joined}."
                        else:
                            answer_text = f"{display_attr} for {direct['entity_name']} is {value}."
                        return {
                            "question": question,
                            "intent": llm_parsed.intent,
                            "answer": self._prefix_source(answer_text, "sql_exact_llm"),
                            "data": self._attach_match_telemetry(direct, match_telemetry),
                            "source": "sql_exact_llm",
                        }
            except Exception:
                pass

        _ensure_semantic_context()
        contextual_resolution = self._contextual_llm_resolution_fallback(
            question=question,
            parsed=parsed,
            candidates=candidates,
            discovery_results=discovery_results,
        )
        if contextual_resolution:
            self._try_learn_patterns_from_lexical_signals(
                question=question,
                parsed=parsed,
                answer_source="contextual_llm_resolution_fallback",
                lexical_signals=contextual_resolution.get("lexical_signals") if isinstance(contextual_resolution, dict) else None,
            )
            return {
                "question": question,
                "intent": parsed.intent,
                "answer": self._prefix_source(str(contextual_resolution.get("answer") or ""), "contextual_llm_resolution_fallback"),
                "data": self._attach_match_telemetry(contextual_resolution, match_telemetry),
                "source": "contextual_llm_resolution_fallback",
                "candidate_count": len(candidates),
            }

        # Generic bridge: if an attribute question carries relationship filters but
        # direct attribute lookup failed, retrieve relationship-scoped documents and
        # attempt grounded contextual extraction from those documents.
        parsed_criteria_dict = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        relation_filters = []
        if isinstance(parsed_criteria_dict.get("relation_filters"), list):
            relation_filters.extend(parsed_criteria_dict.get("relation_filters") or [])
        if isinstance(parsed_criteria_dict.get("relationship_filters"), list):
            relation_filters.extend(parsed_criteria_dict.get("relationship_filters") or [])

        if (
            parsed.intent == "attribute_lookup"
            and relation_filters
            and not strict_table_request
            and not self._criteria_question_prefers_listing(question, "")
        ):
            relation_terms: list[str] = []
            if parsed.entity_name:
                relation_terms.append(str(parsed.entity_name).strip())

            for rel in relation_filters:
                if not isinstance(rel, dict):
                    continue
                for key in (
                    "target_name",
                    "target_entity_name",
                    "source_name",
                    "source_entity_name",
                    "relationship_type",
                    "relation",
                ):
                    value = str(rel.get(key) or "").strip()
                    if value and value not in relation_terms:
                        relation_terms.append(value)

            relation_terms = [term for term in relation_terms if term]
            if relation_terms:
                synthetic_criteria = {
                    "entity_type": "",
                    "must_contain": relation_terms,
                    "attribute_hints": ["description", "document", "metadata", "amount", "cost", "value"],
                    "semantic_only": True,
                    "document_hint": "all",
                    "semantic_frame": "relation_guided_document_bridge",
                    "semantic_retrieval_mode": "related",
                    "allow_indirect_relationships": True,
                }
                synthetic_parsed = ParsedQuery(
                    intent="criteria_lookup",
                    entity_name=parsed.entity_name,
                    attribute_name=None,
                    confidence=max(0.70, float(parsed.confidence or 0.0)),
                    language=parsed.language,
                    criteria=synthetic_criteria,
                )
                relation_criteria_result = self.search_by_criteria(
                    question=question,
                    parsed=synthetic_parsed,
                    scope={"doc_ids": doc_ids},
                    limit=20,
                )
                if relation_criteria_result:
                    relation_contextual = self._criteria_contextual_llm_resolution_fallback(
                        question=question,
                        parsed=parsed,
                        criteria_result=relation_criteria_result,
                        candidates=candidates,
                        discovery_results=discovery_results,
                    )
                    if relation_contextual and str(relation_contextual.get("answer") or "").strip():
                        return {
                            "question": question,
                            "intent": parsed.intent,
                            "answer": self._prefix_source(str(relation_contextual.get("answer") or "").strip(), "criteria_contextual_llm"),
                            "data": self._attach_match_telemetry(
                                {
                                    "relation_guided_criteria": relation_criteria_result,
                                    "criteria_contextual": relation_contextual,
                                },
                                match_telemetry,
                            ),
                            "source": "criteria_contextual_llm",
                            "candidate_count": len(relation_criteria_result.get("matches") or []),
                        }

        # Generic bridge for value-style questions with document context even when
        # relation filters are absent: build synthetic criteria from entity + cues.
        if (
            parsed.intent == "attribute_lookup"
            and not strict_table_request
            and self._is_value_document_context_question(question)
            and not self._criteria_question_prefers_listing(question, "")
        ):
            synthetic_terms: list[str] = []
            if parsed.entity_name:
                synthetic_terms.append(str(parsed.entity_name).strip())

            cue_tokens = [
                tok
                for tok in re.findall(r"[a-zA-Z0-9äöüÄÖÜß]{4,}", str(question or "").lower())
                if tok
                not in {
                    "what", "which", "where", "when", "how", "much", "with", "from", "into", "about",
                    "cost", "price", "value", "amount", "total", "tell", "find", "related", "documents",
                    "show", "give", "query", "that", "this", "have", "does", "did", "were", "been",
                    "was", "sind", "ist", "sowie", "oder", "aber", "eine", "einer", "eines", "dieser",
                }
            ]
            for token in cue_tokens[:8]:
                if token not in synthetic_terms:
                    synthetic_terms.append(token)

            for hint in ("invoice", "bill", "receipt", "payment", "quotation", "quote", "offer", "rechnung", "beleg", "zahlung"):
                if hint not in synthetic_terms:
                    synthetic_terms.append(hint)

            if synthetic_terms:
                synthetic_criteria = {
                    "entity_type": "",
                    "must_contain": synthetic_terms,
                    "attribute_hints": ["description", "document", "metadata", "amount", "cost", "value", "due_date"],
                    "semantic_only": True,
                    "document_hint": "all",
                    "semantic_frame": "value_document_context_bridge",
                    "semantic_retrieval_mode": "related",
                    "allow_indirect_relationships": True,
                }
                synthetic_parsed = ParsedQuery(
                    intent="criteria_lookup",
                    entity_name=parsed.entity_name,
                    attribute_name=None,
                    confidence=max(0.70, float(parsed.confidence or 0.0)),
                    language=parsed.language,
                    criteria=synthetic_criteria,
                )
                synthetic_criteria_result = self.search_by_criteria(
                    question=question,
                    parsed=synthetic_parsed,
                    scope={"doc_ids": doc_ids},
                    limit=20,
                )
                if synthetic_criteria_result:
                    synthetic_contextual = self._criteria_contextual_llm_resolution_fallback(
                        question=question,
                        parsed=parsed,
                        criteria_result=synthetic_criteria_result,
                        candidates=candidates,
                        discovery_results=discovery_results,
                    )
                    if synthetic_contextual and str(synthetic_contextual.get("answer") or "").strip():
                        return {
                            "question": question,
                            "intent": parsed.intent,
                            "answer": self._prefix_source(str(synthetic_contextual.get("answer") or "").strip(), "criteria_contextual_llm"),
                            "data": self._attach_match_telemetry(
                                {
                                    "synthetic_criteria": synthetic_criteria_result,
                                    "criteria_contextual": synthetic_contextual,
                                },
                                match_telemetry,
                            ),
                            "source": "criteria_contextual_llm",
                            "candidate_count": len(synthetic_criteria_result.get("matches") or []),
                        }

        source = "not_found"
        not_found_data = self._attach_match_telemetry(None, match_telemetry)
        if isinstance(not_found_data, dict):
            not_found_data["fallback_diagnostics"] = fallback_diagnostics
        not_found_answer = "I could not find a confident answer in IDMS_demo."
        if (
            parsed.intent == "attribute_lookup"
            and parsed.attribute_name
            and self._looks_like_identifier_request(question)
        ):
            not_found_answer = self._identifier_not_found_message(question, parsed)
            if isinstance(not_found_data, dict):
                not_found_data["identifier_suggestions"] = self._suggest_identifier_attribute_candidates(question, parsed)
        return {
            "question": question,
            "intent": parsed.intent,
            "answer": self._prefix_source(not_found_answer, source),
            "data": not_found_data,
            "source": source,
        }

    def _try_learn_pattern(
        self,
        question: str,
        parsed: ParsedQuery,
        answer_source: str,
        attribute_names: list[str] | None = None,
    ) -> None:
        """
        Attempt to learn pattern from successful query if PatternLibrary is available.
        Called after successful attribute lookups to accumulate patterns.
        """
        
        if (
            not self.pattern_library
            or not answer_source
            or answer_source == "not_found"
            or parsed.intent != "attribute_lookup"
            or not parsed.entity_name
            or parsed.confidence < 0.80
        ):
            return
        
        try:
            # Extract semantic concept from attribute names or intent
            attributes_to_learn = attribute_names or []
            if not attributes_to_learn and parsed.attribute_name:
                attributes_to_learn = [parsed.attribute_name]
            if not attributes_to_learn and parsed.relation_name:
                attributes_to_learn = [parsed.relation_name]
            
            if not attributes_to_learn:
                return
            
            # Map attributes to semantic concepts (age -> birth_date, duration -> start/end times, etc.)
            mapped_attrs = {}
            for attr in attributes_to_learn:
                attr_lower = str(attr or "").lower().strip()
                # Expand single attributes to semantic mappings
                if attr_lower in ("birth_date", "date_of_birth"):
                    mapped_attrs["age"] = "birth_date"
                elif attr_lower in ("start_time", "start_date"):
                    mapped_attrs["duration"] = ["start_time", "end_time"]
                elif attr_lower in ("departure_time",):
                    mapped_attrs["travel_duration"] = ["departure_time", "arrival_time"]
                else:
                    mapped_attrs[attr_lower] = attr_lower
            
            if not mapped_attrs:
                return
            
            # Determine entity class from entity context if available
            entity_class = None  # Could be expanded to detect from entity_name
            
            # Learn pattern with medium confidence (will be updated as pattern is used)
            for semantic_concept, attributes in mapped_attrs.items():
                # Attempt learning only for confident parsed queries
                # Confidence discount: newer learns start lower, increase with repeated success
                learn_confidence = float(parsed.confidence) * 0.75
                
                pattern_id = self.pattern_library.learn_pattern(
                    query_text=question,
                    semantic_concept=semantic_concept,
                    mapped_attributes={semantic_concept: attributes},
                    entity_class=entity_class,
                    computation_rule=None,
                    confidence=learn_confidence,
                )
                
                if pattern_id:
                    import logging
                    logging.debug(f"Learned pattern ID {pattern_id}: '{question}' -> {semantic_concept}")
        
        except Exception as e:
            import logging
            logging.debug(f"Pattern learning failed: {e}")

    def _derived_answer_from_pattern_matches(self, parsed: ParsedQuery, matches: list[dict[str, Any]]) -> str | None:
        """Compute derived answer text from matched pattern semantics and computation rules."""

        if not self.pattern_library:
            return None
        criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        pattern_id = criteria.get("pattern_id")
        if not pattern_id:
            return None
        try:
            pattern = self.pattern_library.get_pattern(int(pattern_id))
        except Exception:
            pattern = None
        if not pattern:
            return None

        concept = str(pattern.semantic_concept or "").strip().lower()
        entity_class_hint = str(pattern.entity_class or "").strip() or None

        # Prefer SQL-stored SOLF computation clause, fallback to pattern row computation_rule.
        sql_clause_rule = None
        sql_clause_body = None
        try:
            sql_clause_rule = self.pattern_library.get_active_computation_rule(concept, entity_class_hint)
            sql_clause_body = self.pattern_library.get_active_computation_clause_body(concept, entity_class_hint)
        except Exception:
            sql_clause_rule = None
            sql_clause_body = None

        value_map: dict[str, Any] = {}
        fallback_entity_name = parsed.entity_name or "the entity"
        for item in matches:
            attr = str(item.get("attribute_name") or "").strip().lower()
            if attr:
                value_map[attr] = item.get("attribute_value")
            entity_name_candidate = str(item.get("entity_name") or item.get("source_entity_name") or "").strip()
            if entity_name_candidate:
                fallback_entity_name = entity_name_candidate

        if not value_map:
            return None

        entity_label = parsed.entity_name or fallback_entity_name
        lang = str(parsed.language or "en").lower()

        # AGE / AGE_AT fallbacks by semantic concept
        if concept in {"age", "age_at_event"}:
            birth_dt = self._coerce_to_datetime(value_map.get("birth_date") or value_map.get("date_of_birth"))
            if not birth_dt:
                return None
            ref_dt = self._coerce_to_datetime(value_map.get("event_date")) if concept == "age_at_event" else None
            if not ref_dt:
                ref_dt = datetime.combine(date.today(), datetime.min.time())
            years = ref_dt.year - birth_dt.year - ((ref_dt.month, ref_dt.day) < (birth_dt.month, birth_dt.day))
            if lang == "de":
                return f"{entity_label} ist {years} Jahre alt (Geburtsdatum: {birth_dt.date().isoformat()})."
            return f"{entity_label} is {years} years old (birth date: {birth_dt.date().isoformat()})."

        rule = str(sql_clause_rule or pattern.computation_rule or "").strip()
        if sql_clause_body:
            parsed_from_clause = self._parse_richer_clause_rule(sql_clause_body)
            if parsed_from_clause:
                rule = parsed_from_clause
        if not rule:
            return None

        # YEAR_DIFF(end_date, start_date)
        year_diff_match = re.match(r"^YEAR_DIFF\(([^,]+),\s*([^)]+)\)$", rule, flags=re.IGNORECASE)
        if year_diff_match:
            end_attr = year_diff_match.group(1).strip().lower()
            start_attr = year_diff_match.group(2).strip().lower()
            end_dt = self._coerce_to_datetime(value_map.get(end_attr))
            start_dt = self._coerce_to_datetime(value_map.get(start_attr))
            if not end_dt or not start_dt:
                return None
            years = end_dt.year - start_dt.year - ((end_dt.month, end_dt.day) < (start_dt.month, start_dt.day))
            if lang == "de":
                return f"{entity_label}: Dauer beträgt {years} Jahre ({start_dt.date().isoformat()} bis {end_dt.date().isoformat()})."
            return f"{entity_label}: duration is {years} years ({start_dt.date().isoformat()} to {end_dt.date().isoformat()})."

        # UNIX_DIFF(end_attr, start_attr) -> integer seconds
        unix_diff_match = re.match(r"^UNIX_DIFF\(([^,]+),\s*([^)]+)\)$", rule, flags=re.IGNORECASE)
        if unix_diff_match:
            end_attr = unix_diff_match.group(1).strip().lower()
            start_attr = unix_diff_match.group(2).strip().lower()
            end_dt = self._coerce_to_datetime(value_map.get(end_attr))
            start_dt = self._coerce_to_datetime(value_map.get(start_attr))
            if not end_dt or not start_dt:
                return None
            delta_seconds = int((end_dt - start_dt).total_seconds())
            if lang == "de":
                return f"{entity_label}: Dauer ist {delta_seconds} Sekunden ({start_dt.isoformat()} bis {end_dt.isoformat()})."
            return f"{entity_label}: duration is {delta_seconds} seconds ({start_dt.isoformat()} to {end_dt.isoformat()})."

        # Generic subtraction rule: end_attr - start_attr
        subtract_match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*-\s*([a-zA-Z_][a-zA-Z0-9_]*)$", rule)
        if subtract_match:
            end_attr = subtract_match.group(1).strip().lower()
            start_attr = subtract_match.group(2).strip().lower()
            end_dt = self._coerce_to_datetime(value_map.get(end_attr))
            start_dt = self._coerce_to_datetime(value_map.get(start_attr))
            if not end_dt or not start_dt:
                return None
            if end_dt < start_dt:
                end_dt, start_dt = start_dt, end_dt
            delta = end_dt - start_dt
            total_seconds = int(delta.total_seconds())
            duration_text = self._format_duration(delta)
            if lang == "de":
                return f"{entity_label}: Dauer ist {duration_text} ({total_seconds} Sekunden; {start_dt.isoformat()} bis {end_dt.isoformat()})."
            return f"{entity_label}: duration is {duration_text} ({total_seconds} seconds; {start_dt.isoformat()} to {end_dt.isoformat()})."

        return None

    def _derived_clause_diagnostics(self, parsed: ParsedQuery, matches: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Return structured diagnostics when a derived rule is selected but required attributes are missing."""

        pattern, concept, rule = self._resolve_pattern_and_rule(parsed)
        if not pattern:
            return None

        available_attrs = {
            str(item.get("attribute_name") or "").strip().lower()
            for item in matches
            if str(item.get("attribute_name") or "").strip()
        }
        if not available_attrs:
            return None

        required_attrs = self._required_attributes_for_derived(concept, rule)
        if not required_attrs:
            return None

        missing: list[str] = []
        for attr in required_attrs:
            if attr == "birth_date":
                if "birth_date" not in available_attrs and "date_of_birth" not in available_attrs:
                    missing.append(attr)
            elif attr not in available_attrs:
                missing.append(attr)

        if not missing:
            return None

        entity_label = parsed.entity_name or "the entity"
        if str(parsed.language or "en").lower() == "de":
            message = (
                f"Abgeleitete Berechnung für {entity_label} konnte nicht ausgeführt werden. "
                f"Fehlende Attribute: {', '.join(missing)}. Verfügbar: {', '.join(sorted(available_attrs))}."
            )
        else:
            message = (
                f"Derived computation for {entity_label} could not run. "
                f"Missing attributes: {', '.join(missing)}. Available: {', '.join(sorted(available_attrs))}."
            )

        return {
            "code": "derived_rule_input_missing",
            "semantic_concept": concept,
            "rule": rule,
            "required_attributes": sorted(required_attrs),
            "available_attributes": sorted(available_attrs),
            "missing_attributes": missing,
            "message": message,
        }

    def _resolve_pattern_and_rule(self, parsed: ParsedQuery) -> tuple[Any | None, str | None, str | None]:
        """Resolve matched pattern and effective executable rule from SQL SOLF clause or pattern fallback."""

        if not self.pattern_library:
            return None, None, None

        criteria = parsed.criteria if isinstance(parsed.criteria, dict) else {}
        pattern_id = criteria.get("pattern_id")
        if not pattern_id:
            return None, None, None

        try:
            pattern = self.pattern_library.get_pattern(int(pattern_id))
        except Exception:
            pattern = None
        if not pattern:
            return None, None, None

        concept = str(pattern.semantic_concept or "").strip().lower() or None
        entity_class_hint = str(pattern.entity_class or "").strip() or None

        sql_clause_rule = None
        sql_clause_body = None
        try:
            if concept:
                sql_clause_rule = self.pattern_library.get_active_computation_rule(concept, entity_class_hint)
                sql_clause_body = self.pattern_library.get_active_computation_clause_body(concept, entity_class_hint)
        except Exception:
            pass
        effective_rule = str(sql_clause_rule or pattern.computation_rule or "").strip() or None
        if sql_clause_body:
            parsed_from_clause = self._parse_richer_clause_rule(sql_clause_body)
            if parsed_from_clause:
                effective_rule = parsed_from_clause

        return pattern, concept, effective_rule

    def _required_attributes_for_derived(self, concept: str | None, rule: str | None) -> set[str]:
        """Infer required source attributes from semantic concept and executable rule expression."""

        required: set[str] = set()
        concept_norm = str(concept or "").strip().lower()
        if concept_norm in {"age", "age_at_event"}:
            required.add("birth_date")
            if concept_norm == "age_at_event":
                required.add("event_date")

        rule_text = str(rule or "").strip()
        if not rule_text:
            return required

        # Generic function(arg1, arg2, ...)
        function_match = re.match(r"^([A-Z_]+)\((.*)\)$", rule_text, flags=re.IGNORECASE)
        if function_match:
            fn = function_match.group(1).strip().upper()
            args_text = function_match.group(2)
            args = [part.strip().lower() for part in args_text.split(",") if part.strip()]
            if fn in {"YEAR_DIFF", "UNIX_DIFF", "AGE", "AGE_AT"}:
                for arg in args:
                    if re.match(r"^[a-z_][a-z0-9_]*$", arg):
                        required.add(arg)
                return required

        # Generic subtraction: attr_a - attr_b
        subtract_match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*-\s*([a-zA-Z_][a-zA-Z0-9_]*)$", rule_text)
        if subtract_match:
            required.add(subtract_match.group(1).strip().lower())
            required.add(subtract_match.group(2).strip().lower())

        return required

    def _parse_richer_clause_rule(self, clause_body: str) -> str | None:
        """
        Parse richer SQL-stored SOLF-like clause syntax and reduce to executable mini-rule.
        Supported examples:
          compute: end_time - start_time
          when start_time and end_time then compute: UNIX_DIFF(end_time, start_time)
          compute: YEAR_DIFF(end_date, start_date) from timestamp
        """

        text = str(clause_body or "").strip()
        if not text:
            return None

        # Normalize single-line whitespace for easier parsing while preserving symbols.
        normalized = re.sub(r"\s+", " ", text)

        # Preferred form: ... compute: <expr> ...
        compute_match = re.search(r"compute\s*:\s*(.+?)(?:\s+from\s+timestamp)?\s*$", normalized, flags=re.IGNORECASE)
        if compute_match:
            return compute_match.group(1).strip()

        # Alternate shorthand: expr: <expr>
        expr_match = re.search(r"expr\s*:\s*(.+?)\s*$", normalized, flags=re.IGNORECASE)
        if expr_match:
            return expr_match.group(1).strip()

        # Fallback: if body is already a bare function/expression, pass through.
        bare_rule = normalized.strip()
        if re.match(r"^(?:YEAR_DIFF|UNIX_DIFF|AGE|AGE_AT)\s*\(", bare_rule, flags=re.IGNORECASE):
            return bare_rule
        if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*\s*-\s*[a-zA-Z_][a-zA-Z0-9_]*$", bare_rule):
            return bare_rule

        return None

    def _coerce_to_datetime(self, value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(float(value))
            except Exception:
                return None
        text = str(value).strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        for candidate in (normalized, normalized[:10]):
            try:
                if len(candidate) == 10:
                    d = date.fromisoformat(candidate)
                    return datetime(d.year, d.month, d.day)
                return datetime.fromisoformat(candidate)
            except Exception:
                continue
        return None

    def _format_duration(self, delta) -> str:
        total_seconds = int(delta.total_seconds())
        days, rem = divmod(total_seconds, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, seconds = divmod(rem, 60)
        parts: list[str] = []
        if days:
            parts.append(f"{days}d")
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        if seconds or not parts:
            parts.append(f"{seconds}s")
        return " ".join(parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Natural language query engine for IDMS_demo")
    parser.add_argument("question", help="Natural-language question")
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output",
    )
    return parser.parse_args()


# Backward compatibility for external scripts that still import UnifiedQueryEngine.
UnifiedQueryEngine = QueryEngine


def main() -> None:
    args = parse_args()
    engine = QueryEngine()
    result = engine.answer(args.question)
    if args.pretty:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
