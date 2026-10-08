"""
Multi-language schema keyword matching using Google text embeddings + Qdrant vectors.
Provides tiered fallback: regex -> vector embedding -> semantic search.
"""

import hashlib
import logging
import os
import re
import time
from typing import Any

try:
    import requests
except Exception:
    requests = None

from llm_fallback import generate_content_with_openrouter_fallback, get_openrouter_client
from security_context import get_tenant_id

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import FieldCondition, Filter, MatchValue, PointStruct
except Exception:
    QdrantClient = None
    FieldCondition = None
    Filter = None
    MatchValue = None
    PointStruct = None

try:
    from qdrant_client.models import VectorParams, Distance
except Exception:
    VectorParams = None
    Distance = None


class AttributeEmbeddingIndex:
    """Manages multi-language schema keyword vectorization and vector search in Qdrant."""

    KIND_ATTRIBUTE = "attribute"
    KIND_RELATIONSHIP = "relationship"

    CANONICAL_ATTRIBUTES = {
        "birth_date": [
            "birth date", "date of birth", "dob", "birthdate", "born on",
            "geburtsdatum", "geboren am",
            "date de naissance", "fecha de nacimiento", "data di nascita",
        ],
        "birth_place": [
            "birth place", "place of birth", "born in",
            "geburtsort", "geboren in",
            "lieu de naissance", "lugar de nacimiento", "luogo di nascita",
        ],
        "eori_no": [
            "eori", "eori number", "eori no", "eori-no", "eori_number", "eori nr", "eori-nr",
            "eori nummer", "eori-nummer", "economic operators registration and identification",
            "eori nummer", "eori nummern",
            "numéro eori", "número eori", "numero eori",
        ],
        "registration_no": [
            # English
            "registration number", "registration no", "company number", "company number",
            "registration", "reg no", "reg number", "CHE number", "tax id",
            "vat", "vat number", "vat no", "vat-no", "vat nr", "vat-nr",
            # German
            "firmennummer", "firma nummer", "handelsregisternummer", "handelsregister nummer",
            "unternehmensnummer", "unternehmens nummer", "uid", "uid-nummer", "registrierungsnummer",
            "mwst", "mwst nummer", "mwst-nummer", "mwst nr", "mwst-nr",
            "must", "must nummer", "must-nummer", "must nr", "must-nr",
            "ust", "ust-id", "ust id", "ust-idnr", "ust idnr", "umsatzsteuer-id",
            # French
            "numéro d'enregistrement", "numéro d'immatriculation", "numéro de registre",
            "numéro de tva", "tva", "id tva",
            # Spanish
            "número de registro", "número de empresa", "cif", "nif", "iva", "número de iva",
            # Italian
            "numero di registrazione", "numero di iscrizione", "partita iva", "iva",
        ],
        "shareholders": [
            # English
            "shareholders", "shareholder", "share holders", "share holder", "owners", "owner",
            "stakeholders", "stakeholder", "equity holders", "share capital", "shareholding",
            "shares amounts", "share amounts", "share count", "shareholding amount",
            # German
            "aktionäre", "aktionär", "gesellschafter", "gesellschafter", "anteilseigner",
            "anteilseigner", "eigentuemer", "eigentümer", "anteile", "betraege", "betrag",
            "anteil", "anteilinhaber",
            # French
            "actionnaires", "actionnaire", "propriétaires", "propriétaire", "associés",
            "associé", "parts sociales", "capital social",
            # Spanish
            "accionistas", "accionista", "propietarios", "propietario", "socios",
            "socio", "participaciones", "participación",
            # Italian
            "azionisti", "azionista", "azionariato", "titolari", "titolare",
        ],
        "registered_address": [
            "registered address", "address", "company address", "mailing address",
            "adresse", "anschrift", "firmenadresse",
            "adresse enregistrée", "dirección registrada", "indirizzo registrato",
        ],
        "document_filename": [
            "file name", "filename", "document filename", "document name", "name of file", "file title",
            "dateiname", "dokumentname", "datei name", "name der datei", "name des dokuments",
            "nom du fichier", "nombre de archivo", "nome del file",
        ],
        "document_full_path": [
            "full path", "file path", "document path", "absolute path", "source path",
            "vollständiger pfad", "vollstaendiger pfad", "dateipfad", "dokumentpfad", "pfad",
            "chemin complet", "ruta completa", "percorso completo",
        ],
        "document_file_info": [
            "filename with full path", "file info", "document file info", "file details",
            "dateiname mit vollständigem pfad", "dateiname mit vollstaendigem pfad", "datei info",
            "infos fichier", "información del archivo", "informazioni file",
        ],
        "purpose": [
            "purpose", "business purpose", "company purpose", "business activity", "description",
            "zweck", "gesellschaftszweck", "unternehmenszweck", "geschäftszweck", "geschaeftszweck",
            "objet social", "objeto social", "scopo sociale",
        ],
        "publication_organ": [
            "publication organ", "publication medium", "publication channel", "official publication",
            "publikationsorgan", "veröffentlichungsorgan", "veroeffentlichungsorgan",
            "organe de publication", "medio de publicación", "organo di pubblicazione",
        ],
        "tr_number": [
            "trade register number", "tr number", "tr-no", "register number",
            "tr-nr", "handelsregister nummer", "handelsregisternummer", "registernummer",
            "numéro du registre du commerce", "número de registro mercantil", "numero registro commercio",
        ],
        "tr_date": [
            "trade register date", "tr date", "register date", "entry date",
            "tr-datum", "handelsregister datum", "registerdatum", "eintragsdatum",
            "date du registre", "fecha de registro", "data di registro",
        ],
        "statute_date": [
            "statute date", "articles of association date", "statutes date", "charter date",
            "statutendatum", "datum der statuten", "urkundendatum",
            "date des statuts", "fecha de estatutos", "data statuto",
        ],
        "eori_contact_person": [
            "eori contact person", "eori contact", "contact person for eori", "customs contact person",
            "eori-ansprechpartner", "eori ansprechpartner", "ansprechpartner für eori", "ansprechpartner fuer eori",
            "personne de contact eori", "contacto eori", "referente eori",
        ],
        "eori_contact_email": [
            "email of eori contact person", "eori contact email", "email for eori contact",
            "e-mail des eori-ansprechpartners", "email für eori-ansprechpartner", "email fuer eori ansprechpartner",
            "courriel contact eori", "correo contacto eori", "email contatto eori",
        ],
    }

    COLLECTION_NAME = "attribute_embeddings"
    CACHE_TTL = 3600  # 1 hour
    EMBEDDING_BATCH_SIZE = 32

    # Module-level singleton so all callers (QueryEngine, etc.) share one instance
    # and its in-memory embedding cache survives across requests.
    _shared_instance: "AttributeEmbeddingIndex | None" = None

    @classmethod
    def get_shared_instance(cls) -> "AttributeEmbeddingIndex":
        """Return the process-level singleton, creating it on first call."""
        if cls._shared_instance is None:
            cls._shared_instance = cls()
        return cls._shared_instance

    @staticmethod
    def _variant_point_id(canonical_attr: str, variant: str, tenant_scope: str = "global") -> int:
        """Deterministic point id so upserts are incremental and idempotent."""
        digest = hashlib.sha256(f"{tenant_scope}::{canonical_attr}::{variant}".encode("utf-8")).hexdigest()
        return int(digest[:16], 16)

    @staticmethod
    def _humanize_term(term: str) -> str:
        cleaned = str(term or "").strip().replace("_", " ").replace("-", " ")
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()

    @staticmethod
    def _term_variants(term: str) -> list[str]:
        base = AttributeEmbeddingIndex._humanize_term(term)
        normalized = base.lower()
        variants = {
            normalized,
            normalized.replace(" ", "_"),
            normalized.replace(" ", "-"),
            base,
            base.title(),
        }
        return [variant for variant in variants if variant]

    def __init__(self, embedding_model: str | None = None, cluster_name: str = "default"):
        """
        Initialize embedding index.

        Args:
            embedding_model: OpenRouter embedding model ID
            cluster_name: Qdrant cluster identifier
        """
        self.logger = logging.getLogger("attribute_embedding_index")
        resolved_embedding_model = str(
            embedding_model
            or os.getenv("IKOS_OPENROUTER_EMBEDDING_MODEL", "openai/text-embedding-3-large")
        ).strip()
        if not resolved_embedding_model:
            resolved_embedding_model = "openai/text-embedding-3-large"
        self.embedding_model = resolved_embedding_model
        self.genai_client = self._make_genai_client()
        self.generation_client = self._make_generation_client()
        self.qdrant = self._make_qdrant_client(cluster_name)
        self.cache: dict[str, tuple[Any, float]] = {}
        self._lexical_aliases_by_tenant: dict[str, dict[str, dict[str, str]]] = {}
        self._lexical_tenant_id: str | None = None
        self._lexical_alias_to_schema: dict[str, dict[str, str]] = {}
        # Guard: when True, blocks upsert/rebuild operations so a live query path
        # never triggers a bulk embedding job.
        self._query_context: bool = False
        # Circuit breaker for repeated Qdrant 400 dimension mismatch failures.
        self._vector_search_disabled: bool = False
        self._vector_search_disable_reason: str = ""
        self._vector_search_disable_logged: bool = False
        self._embedding_dimension: int | None = None
        self._last_ingest_upsert_signature: str = ""
        if self._seed_aliases_enabled():
            self._register_terms_for_lexical_match(self.KIND_ATTRIBUTE, self.CANONICAL_ATTRIBUTES)
        self._index_built = False
        self.refresh_ready_state()

    def build_ingest_upsert_signature(
        self,
        *,
        dynamic_attribute_names: list[str] | None = None,
        relationship_names: list[str] | None = None,
    ) -> str:
        dynamic_terms = sorted(
            {
                str(name or "").strip().lower()
                for name in (dynamic_attribute_names or [])
                if str(name or "").strip()
            }
        )
        relationship_terms = sorted(
            {
                str(name or "").strip().lower()
                for name in (relationship_names or [])
                if str(name or "").strip()
            }
        )
        semantic_terms = self._fetch_semantic_attribute_terms()
        semantic_rows = [
            (canonical, tuple(sorted(str(variant).strip() for variant in variants if str(variant).strip())))
            for canonical, variants in sorted(semantic_terms.items())
        ]
        payload = {
            "embedding_model": self.embedding_model,
            "tenant_id": get_tenant_id(),
            "canonical_attribute_count": len(self.CANONICAL_ATTRIBUTES),
            "dynamic_attribute_names": dynamic_terms,
            "relationship_names": relationship_terms,
            "semantic_terms": semantic_rows,
        }
        return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    def should_skip_ingest_upsert(self, signature: str) -> bool:
        return bool(signature) and self._index_built and signature == self._last_ingest_upsert_signature

    def mark_ingest_upsert_signature(self, signature: str) -> None:
        self._last_ingest_upsert_signature = str(signature or "").strip()

    def _disable_vector_search(self, reason: str) -> None:
        self._vector_search_disabled = True
        self._vector_search_disable_reason = str(reason or "vector_search_disabled").strip()
        if not self._vector_search_disable_logged:
            self.logger.warning(
                "Disabling attribute embedding vector search for this process: %s. "
                "Lexical/schema fallback remains active.",
                self._vector_search_disable_reason,
            )
            self._vector_search_disable_logged = True

    @staticmethod
    def _seed_aliases_enabled() -> bool:
        return str(os.getenv("IKOS_ENABLE_SEED_ATTRIBUTE_ALIASES", "true")).strip().lower() in {
            "1", "true", "yes", "on"
        }

    @staticmethod
    def _llm_synonym_flag_enabled() -> bool:
        return str(os.getenv("IKOS_ENABLE_DYNAMIC_ATTRIBUTE_LLM_SYNONYMS", "true")).strip().lower() in {
            "1", "true", "yes", "on"
        }

    @staticmethod
    def _llm_synonym_limit() -> int:
        raw = str(os.getenv("IKOS_DYNAMIC_ATTRIBUTE_LLM_SYNONYM_LIMIT", "200")).strip()
        try:
            return max(0, int(raw))
        except Exception:
            return 200

    @staticmethod
    def _normalize_lexical(text: str) -> str:
        normalized = str(text or "").strip().lower().replace("-", " ").replace("_", " ")
        normalized = re.sub(r"[^a-z0-9äöüßà-ÿ\s]", "", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()

    def _register_terms_for_lexical_match(
        self,
        kind: str,
        terms: dict[str, list[str]],
        aliases: dict[str, dict[str, str]] | None = None,
    ) -> None:
        target = aliases if aliases is not None else self._lexical_alias_to_schema
        for canonical_name, variants in terms.items():
            for variant in variants:
                norm = self._normalize_lexical(variant)
                if not norm:
                    continue
                target[norm] = {"kind": kind, "canonical_name": canonical_name}

    def _ensure_tenant_lexicon(self) -> str:
        tenant_id = get_tenant_id()
        aliases = self._lexical_aliases_by_tenant.get(tenant_id)
        if aliases is None:
            aliases = {}
            if self._seed_aliases_enabled():
                self._register_terms_for_lexical_match(self.KIND_ATTRIBUTE, self.CANONICAL_ATTRIBUTES, aliases)
            semantic_terms = self._fetch_semantic_attribute_terms()
            self._register_terms_for_lexical_match(self.KIND_ATTRIBUTE, semantic_terms, aliases)
            self._lexical_aliases_by_tenant[tenant_id] = aliases
        self._lexical_alias_to_schema = aliases
        self._lexical_tenant_id = tenant_id
        return tenant_id

    def _lexical_schema_match(self, user_input: str, allowed_kinds: set[str]) -> dict[str, Any] | None:
        self._ensure_tenant_lexicon()
        normalized_input = self._normalize_lexical(user_input)
        if not normalized_input:
            return None

        tenant_id = get_tenant_id()
        aliases = self._lexical_aliases_by_tenant.get(tenant_id, {})
        exact = aliases.get(normalized_input)
        if exact and str(exact.get("kind")) in allowed_kinds:
            return {
                "kind": str(exact["kind"]),
                "canonical_name": str(exact["canonical_name"]),
                "score": 1.0,
            }

        # Soft lexical fallback: token overlap helps when embeddings are weak/noisy.
        input_tokens = set(normalized_input.split())
        best: dict[str, Any] | None = None
        best_score = 0.0
        for alias, payload in aliases.items():
            kind = str(payload.get("kind") or "")
            if kind not in allowed_kinds:
                continue
            alias_tokens = set(alias.split())
            if not alias_tokens:
                continue
            overlap = len(input_tokens.intersection(alias_tokens))
            if overlap == 0:
                continue
            score = overlap / max(1, len(alias_tokens))
            if score > best_score:
                best_score = score
                best = {
                    "kind": kind,
                    "canonical_name": str(payload.get("canonical_name") or ""),
                    "score": score,
                }

        if best and best_score >= 0.8:
            return best
        return None

    def _semantic_wording_bonus(self, user_input: str, canonical_name: str, kind: str) -> float:
        """Return a small confidence bonus when wording overlaps known aliases.

        This keeps semantic matches generic while rewarding near-lexical phrasing.
        """
        normalized_input = self._normalize_lexical(user_input)
        input_tokens = set(normalized_input.split())
        if not input_tokens:
            return 0.0

        best_overlap = 0.0
        kind_norm = str(kind or "").strip().lower()
        canonical_norm = str(canonical_name or "").strip()
        for alias, payload in self._lexical_alias_to_schema.items():
            if str(payload.get("kind") or "").strip().lower() != kind_norm:
                continue
            if str(payload.get("canonical_name") or "").strip() != canonical_norm:
                continue

            alias_tokens = set(str(alias or "").split())
            if not alias_tokens:
                continue

            overlap_alias = len(input_tokens.intersection(alias_tokens)) / max(1, len(alias_tokens))
            overlap_input = len(input_tokens.intersection(alias_tokens)) / max(1, len(input_tokens))
            overlap = (overlap_alias + overlap_input) / 2.0
            if overlap > best_overlap:
                best_overlap = overlap

        if best_overlap >= 0.85:
            return 0.18
        if best_overlap >= 0.70:
            return 0.12
        if best_overlap >= 0.55:
            return 0.07
        if best_overlap >= 0.40:
            return 0.03
        return 0.0

    def refresh_ready_state(self) -> bool:
        """Mark index as ready when an existing valid collection with points is present."""
        if not self.qdrant:
            self._index_built = False
            return False
        try:
            collections = self.qdrant.get_collections()
            existing = {c.name for c in collections.collections}
            if self.COLLECTION_NAME not in existing:
                self._index_built = False
                return False

            info = self.qdrant.get_collection(self.COLLECTION_NAME)
            vectors_cfg = getattr(info.config.params, "vectors", None)
            if not vectors_cfg:
                self._index_built = False
                return False

            count_result = self.qdrant.count(collection_name=self.COLLECTION_NAME, exact=False)
            self._index_built = int(getattr(count_result, "count", 0) or 0) > 0
            return self._index_built
        except Exception:
            self._index_built = False
            return False

    def _make_genai_client(self):
        """Create OpenRouter-backed client for embeddings."""
        return get_openrouter_client()

    def _make_generation_client(self):
        """Create OpenRouter-backed client for text generation."""
        return get_openrouter_client()

    @staticmethod
    def _extract_vector_size(vectors_cfg: Any, preferred_name: str = "dense") -> int | None:
        """Best-effort extraction of vector size from Qdrant vectors config."""
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

    def _get_embedding_dimension(self) -> int | None:
        """Return current embedding model output dimension, cached per process."""
        if self._embedding_dimension is not None:
            return int(self._embedding_dimension)

        if not self.genai_client:
            return None

        try:
            sample_response = self.genai_client.models.embed_content(
                model=self.embedding_model,
                contents=["dimension_probe"],
            )
            embeddings = list(getattr(sample_response, "embeddings", []) or [])
            if not embeddings:
                return None
            values = list(getattr(embeddings[0], "values", []) or [])
            if not values:
                return None
            self._embedding_dimension = len(values)
            return int(self._embedding_dimension)
        except Exception as exc:
            self.logger.warning("Failed to determine embedding dimension for model '%s': %s", self.embedding_model, exc)
            return None

    @staticmethod
    def _fetch_distinct_attribute_types(limit: int = 5000) -> list[str]:
        """Fetch distinct active object attribute names from DB."""
        try:
            import object_db
        except Exception:
            return []

        names: list[str] = []
        try:
            connection = object_db.get_connection()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT DISTINCT LOWER(attr_type) AS attr_type
                        FROM attribute
                        WHERE src_type = 'object'
                          AND attr_type IS NOT NULL
                          AND valid_from <= CURRENT_DATE
                          AND (valid_until IS NULL OR valid_until > CURRENT_DATE)
                        ORDER BY attr_type
                        LIMIT %s
                        """,
                        (int(limit),),
                    )
                    rows = cursor.fetchall()
            finally:
                connection.close()

            for row in rows:
                value = str((row[0] if row else "") or "").strip()
                if value:
                    names.append(value)
        except Exception:
            return []

        seen: set[str] = set()
        ordered: list[str] = []
        for name in names:
            if name not in seen:
                seen.add(name)
                ordered.append(name)
        return ordered

    def _llm_expand_attribute_synonyms(self, attribute_name: str) -> list[str]:
        """Use LLM to propose multilingual aliases for a schema attribute name."""
        if not self._llm_synonym_flag_enabled() or not self.generation_client:
            return []

        name = str(attribute_name or "").strip()
        if not name:
            return []

        prompt = (
            "Generate a JSON array of up to 8 concise aliases for the schema attribute "
            f"'{name}'. Include multilingual business wording variants (EN/DE/FR/ES/IT) "
            "when natural. Return ONLY JSON array of strings, no explanation."
        )

        try:
            from ikos_config import EXTRACT_MODEL
            response = generate_content_with_openrouter_fallback(
                primary_call=lambda: self.generation_client.models.generate_content(
                    model=EXTRACT_MODEL,
                    contents=prompt,
                ),
                model=EXTRACT_MODEL,
                contents=prompt,
                temperature=0.0,
                call_name="attribute_synonym_expand",
                complexity="simple",
            )
            text = str(getattr(response, "text", "") or "").strip()
            if not text:
                return []

            # Tolerate wrappers by extracting first JSON array span.
            m = re.search(r"\[[\s\S]*\]", text)
            raw_json = m.group(0) if m else text

            import json
            parsed = json.loads(raw_json)
            if not isinstance(parsed, list):
                return []

            out: list[str] = []
            seen: set[str] = set()
            for item in parsed[:8]:
                value = str(item or "").strip()
                if not value:
                    continue
                key = value.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(value)
            return out
        except Exception:
            return []

    def _build_dynamic_attribute_terms(self, attribute_names: list[str]) -> dict[str, list[str]]:
        terms: dict[str, list[str]] = {}
        llm_budget = self._llm_synonym_limit()
        llm_used = 0

        for raw_name in attribute_names:
            canonical_name = str(raw_name or "").strip().lower()
            if not canonical_name:
                continue

            variants: list[str] = []
            variants.extend(self._term_variants(canonical_name))
            variants.append(canonical_name)
            variants.append(canonical_name.replace("_", " "))

            if llm_used < llm_budget:
                llm_variants = self._llm_expand_attribute_synonyms(canonical_name)
                if llm_variants:
                    variants.extend(llm_variants)
                llm_used += 1

            deduped: list[str] = []
            seen: set[str] = set()
            for variant in variants:
                value = str(variant or "").strip()
                if not value:
                    continue
                key = value.lower()
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(value)

            if deduped:
                terms[canonical_name] = deduped

        return terms

    @staticmethod
    def _fetch_semantic_attribute_terms(limit: int = 5000) -> dict[str, list[str]]:
        """Fetch attribute semantic term mappings persisted in DB."""
        try:
            import object_db
        except Exception:
            return {}

        out: dict[str, list[str]] = {}
        try:
            connection = object_db.get_connection()
            try:
                rows = object_db.get_semantic_terms(connection, kind="attribute", limit=int(limit))
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT canonical_name, alias_text
                        FROM query_term_alias
                        WHERE tenant_id = %s::uuid AND is_active = TRUE AND kind = 'attribute'
                        ORDER BY priority, alias_id
                        """,
                        (get_tenant_id(),),
                    )
                    alias_rows = cursor.fetchall() or []
            finally:
                connection.close()
        except Exception:
            return {}

        for row in rows:
            canonical = str((row or {}).get("canonical_name") or "").strip().lower()
            term = str((row or {}).get("term_text") or "").strip()
            if not canonical or not term:
                continue
            bucket = out.setdefault(canonical, [])
            if term not in bucket:
                bucket.append(term)
        for canonical, alias in alias_rows:
            canonical_text = str(canonical or "").strip().lower()
            alias_text = str(alias or "").strip()
            if canonical_text and alias_text:
                bucket = out.setdefault(canonical_text, [])
                if alias_text not in bucket:
                    bucket.append(alias_text)
        return out

    def _make_qdrant_client(self, cluster_name: str):
        """Create Qdrant client."""
        if QdrantClient is None:
            self.logger.warning("qdrant-client not available; sparse vector search disabled")
            return None
        try:
            from ikos_config import QDRANT_URL, QDRANT_QUERY_API_KEY
            return QdrantClient(url=QDRANT_URL, api_key=QDRANT_QUERY_API_KEY, timeout=5.0)
        except Exception as e:
            self.logger.warning(f"Failed to create qdrant client: {e}")
            return None

    def build_index(self) -> bool:
        """
        Build vector index in Qdrant from canonical attribute variants.
        Intended to be called during ingestion/indexing jobs.

        Returns:
            True if index built successfully, False otherwise.
        """
        if not self.genai_client or not self.qdrant:
            self.logger.warning("Cannot build index: genai_client or qdrant unavailable")
            return False

        try:
            # The collection is shared by tenants. Rebuild incrementally; never
            # delete it because that would erase every tenant's vectors.
            upserted = self.upsert_default_attributes(recreate_collection=False)

            self._index_built = True
            self.logger.info(f"Built attribute embedding index with {upserted} variants")
            return True

        except Exception as e:
            self.logger.error(f"Failed to build attribute embedding index: {e}")
            return False

    def _ensure_collection(self) -> bool:
        """Ensure the Qdrant collection exists with the right vector size."""
        if not self.genai_client or not self.qdrant:
            return False
        if VectorParams is None or Distance is None:
            self.logger.error("VectorParams/Distance not available")
            return False

        expected_dim = self._get_embedding_dimension()
        if not expected_dim:
            self.logger.error("Failed to determine embedding dimension for model '%s'", self.embedding_model)
            return False

        recreate_on_mismatch = str(
            os.getenv("IKOS_RECREATE_ATTRIBUTE_EMBEDDING_COLLECTION_ON_DIM_MISMATCH", "false")
        ).strip().lower() in {"1", "true", "yes", "on"}

        try:
            collections = self.qdrant.get_collections()
            existing = {c.name for c in collections.collections}
            if self.COLLECTION_NAME in existing:
                # Validate existing collection has a usable dense vector config.
                # Earlier experiments may have created an empty vectors_config ({}),
                # which causes all upserts/searches to fail with vector name errors.
                try:
                    info = self.qdrant.get_collection(self.COLLECTION_NAME)
                    vectors_cfg = getattr(info.config.params, "vectors", None)
                    current_dim = self._extract_vector_size(vectors_cfg)
                    if vectors_cfg and current_dim is not None:
                        if int(current_dim) == int(expected_dim):
                            return True
                        message = (
                            f"Collection '{self.COLLECTION_NAME}' dimension mismatch: "
                            f"collection={current_dim}, model={expected_dim} ({self.embedding_model})"
                        )
                        # This collection is shared across tenants. Even an
                        # explicit mismatch override must not delete its data.
                        self.logger.error(
                            "%s. Destructive recreation is disabled for the shared multi-tenant collection; "
                            "use a versioned collection and migrate vectors instead.",
                            message,
                        )
                        self._disable_vector_search(message)
                        return False
                    else:
                        self.logger.error(
                            "Collection '%s' has an invalid vector configuration; refusing destructive recreation",
                            self.COLLECTION_NAME,
                        )
                        self._disable_vector_search("invalid shared collection vector configuration")
                        return False
                except Exception:
                    self.logger.exception(
                        "Could not validate shared collection '%s'; refusing destructive recreation",
                        self.COLLECTION_NAME,
                    )
                    return False

            self.qdrant.create_collection(
                collection_name=self.COLLECTION_NAME,
                vectors_config=VectorParams(size=expected_dim, distance=Distance.COSINE),
            )
            return True
        except Exception as e:
            self.logger.warning(f"Failed to ensure collection: {e}")
            return False

    def _upsert_terms(
        self,
        kind: str,
        terms: dict[str, list[str]],
        tenant_scope: str | None = None,
    ) -> int:
        if not self.genai_client or not self.qdrant:
            return 0

        self._ensure_tenant_lexicon()
        self._register_terms_for_lexical_match(kind, terms)
        resolved_scope = str(tenant_scope or get_tenant_id())
        visibility = "global" if resolved_scope == "global" else "tenant"

        upserted = 0
        flattened: list[tuple[str, str]] = []
        for canonical_name, variants in terms.items():
            for variant in variants:
                text = str(variant or "").strip()
                if not text:
                    continue
                flattened.append((canonical_name, text))

        for start in range(0, len(flattened), self.EMBEDDING_BATCH_SIZE):
            batch = flattened[start : start + self.EMBEDDING_BATCH_SIZE]
            if not batch:
                continue

            batch_texts = [variant for _, variant in batch]
            try:
                response = self.genai_client.models.embed_content(
                    model=self.embedding_model,
                    contents=batch_texts,
                )
                embeddings = list(getattr(response, "embeddings", []) or [])
                if len(embeddings) != len(batch):
                    raise RuntimeError(
                        f"Embedding batch size mismatch for {kind}: expected {len(batch)}, got {len(embeddings)}"
                    )

                points: list[Any] = []
                for (canonical_name, variant), embedding_obj in zip(batch, embeddings):
                    embedding = list(getattr(embedding_obj, "values", []) or [])
                    if not embedding:
                        continue
                    payload = {
                        "tenant_id": resolved_scope,
                        "visibility": visibility,
                        "kind": kind,
                        "canonical_name": canonical_name,
                        "variant_text": variant,
                        "language": self._detect_language(variant),
                    }
                    if kind == self.KIND_ATTRIBUTE:
                        payload["canonical_attribute"] = canonical_name
                    else:
                        payload["canonical_relationship"] = canonical_name

                    points.append(
                        PointStruct(
                            id=self._variant_point_id(f"{kind}:{canonical_name}", variant, resolved_scope),
                            vector=embedding,
                            payload=payload,
                        )
                    )

                if points:
                    self.qdrant.upsert(
                        collection_name=self.COLLECTION_NAME,
                        points=points,
                    )
                    upserted += len(points)
            except Exception as e:
                self.logger.warning(
                    "Failed to upsert %s batch starting at %s (size=%s): %s",
                    kind,
                    start,
                    len(batch),
                    e,
                )
        return upserted

    def upsert_default_attributes(self, recreate_collection: bool = False) -> int:
        """
        Incrementally upsert default canonical attribute variants.
        Returns number of variants upserted.
        """
        if getattr(self, "_query_context", False):
            self.logger.debug("upsert_default_attributes skipped: running in query context")
            return 0
        if not self.genai_client or not self.qdrant:
            self.logger.warning("Cannot upsert index: genai_client or qdrant unavailable")
            return 0

        if recreate_collection:
            self.logger.warning(
                "Ignoring destructive collection recreation for shared multi-tenant Qdrant; use tenant-scoped upserts instead"
            )

        if not self._ensure_collection():
            return 0

        upserted = self._upsert_terms(self.KIND_ATTRIBUTE, self.CANONICAL_ATTRIBUTES, tenant_scope="global")

        self._index_built = upserted > 0
        if not self._index_built:
            self.refresh_ready_state()
        return upserted

    def upsert_dynamic_attribute_terms(self, attribute_names: list[str] | None = None) -> int:
        """Upsert attribute terms discovered from DB schema (optionally LLM-expanded)."""
        if not self.genai_client or not self.qdrant:
            return 0

        if not self._ensure_collection():
            return 0

        names = attribute_names or self._fetch_distinct_attribute_types()
        if not names:
            return 0

        terms = self._build_dynamic_attribute_terms(names)
        semantic_terms = self._fetch_semantic_attribute_terms()
        for canonical, variants in semantic_terms.items():
            bucket = terms.setdefault(canonical, [])
            for variant in variants:
                value = str(variant or "").strip()
                if value and value not in bucket:
                    bucket.append(value)
        if not terms:
            return 0

        upserted = self._upsert_terms(self.KIND_ATTRIBUTE, terms)
        self._index_built = self._index_built or upserted > 0
        return upserted

    def upsert_relationship_terms(self, relationship_names: list[str]) -> int:
        """Upsert live relationship names from the database into the schema keyword index."""
        if not relationship_names:
            return 0

        terms: dict[str, list[str]] = {}
        for relationship_name in relationship_names:
            canonical_name = str(relationship_name or "").strip()
            if not canonical_name:
                continue
            variants = self._term_variants(canonical_name)
            if variants:
                terms[canonical_name] = variants

        return self._upsert_terms(self.KIND_RELATIONSHIP, terms)

    def find_schema_term(
        self,
        user_input: str,
        confidence_threshold: float = 0.65,
        kinds: set[str] | None = None,
    ) -> dict[str, Any] | None:
        """
        Find best-matching canonical schema keyword using vector similarity.

        Args:
            user_input: User's query text containing a schema keyword/description
            confidence_threshold: Minimum similarity score (0-1) to accept a match
            kinds: Optional allowed kinds: attribute, relationship.

        Returns:
            Dict containing kind, canonical_name, score, or None if no good match
        """
        self._query_context = True
        allowed_kinds = set(kinds or {self.KIND_ATTRIBUTE, self.KIND_RELATIONSHIP})

        # Tier 1: lexical aliases (fast and robust for close variants like "filename" vs "document name")
        lexical = self._lexical_schema_match(user_input, allowed_kinds)
        if lexical:
            self.logger.debug("schema_term_lexical_hit text_len=%s kind=%s canonical=%s", len(str(user_input or "")), lexical.get("kind"), lexical.get("canonical_name"))
            return lexical

        if not self._index_built or not self.genai_client or not self.qdrant:
            return None

        # Check cache first
        cache_key = self._cache_key(user_input)
        if cache_key in self.cache:
            result, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.CACHE_TTL:
                self.logger.debug("schema_term_cache_hit text_len=%s", len(str(user_input or "")))
                return result

        self.logger.debug("schema_term_cache_miss text_len=%s", len(str(user_input or "")))

        try:
            # Embed user input
            response = self.genai_client.models.embed_content(
                model=self.embedding_model,
                contents=[user_input],
            )
            if not response.embeddings:
                return None

            embedding = response.embeddings[0].values

            search_results = self._dense_search(embedding=embedding, limit=8)

            if not search_results:
                return None

            for top_match in search_results:
                payload = (top_match.get("payload") if isinstance(top_match, dict) else getattr(top_match, "payload", None)) or {}
                kind = str(payload.get("kind") or self.KIND_ATTRIBUTE).strip().lower()
                if kind not in allowed_kinds:
                    continue
                score = float((top_match.get("score") if isinstance(top_match, dict) else getattr(top_match, "score", 0.0)) or 0.0)
                canonical_name = payload.get("canonical_name") or payload.get("canonical_attribute") or payload.get("canonical_relationship")
                if not canonical_name:
                    continue
                boosted = min(1.0, score + self._semantic_wording_bonus(user_input, str(canonical_name), kind))
                if boosted >= confidence_threshold:
                    result = {
                        "kind": kind,
                        "canonical_name": str(canonical_name),
                        "score": boosted,
                    }
                    self.cache[cache_key] = (result, time.time())
                    self.logger.debug(
                        f"Embedding match: '{user_input}' → '{canonical_name}' ({kind}, score: {boosted:.2f})"
                    )
                    return result

        except Exception as e:
            self.logger.warning(f"Vector search failed for '{user_input}': {e}")

        return None

    def find_attribute(
        self,
        user_input: str,
        confidence_threshold: float = 0.65,
    ) -> tuple[str, float] | None:
        result = self.find_schema_term(user_input, confidence_threshold=confidence_threshold, kinds={self.KIND_ATTRIBUTE})
        if not result:
            return None
        return str(result["canonical_name"]), float(result["score"])

    def find_relationship(
        self,
        user_input: str,
        confidence_threshold: float = 0.65,
    ) -> tuple[str, float] | None:
        result = self.find_schema_term(user_input, confidence_threshold=confidence_threshold, kinds={self.KIND_RELATIONSHIP})
        if not result:
            return None
        return str(result["canonical_name"]), float(result["score"])

    def _detect_language(self, text: str) -> str:
        """Detect language from text heuristically."""
        import re

        lowered = text.lower()
        # Check for language-specific characters
        if re.search(r"[äöüß]", lowered):
            return "de"
        if re.search(r"[àâæéèêëïîôùûüœç]", lowered):
            return "fr"
        if re.search(r"[àáâãäåæèéêëìíîïðòóôõöøùúûüýþ]", lowered):
            return "es"
        if re.search(r"[àáâãäèéêëìíîïòóôõöùúûüì]", lowered):
            return "it"
        return "en"

    def _cache_key(self, text: str) -> str:
        """Generate cache key from text."""
        normalized = " ".join(text.lower().split())
        scoped = f"{get_tenant_id()}::{normalized}"
        return hashlib.sha256(scoped.encode()).hexdigest()[:16]

    def batch_find_schema_terms(
        self,
        inputs: list[str],
        confidence_threshold: float = 0.65,
        kinds: set[str] | None = None,
    ) -> list[dict[str, Any] | None]:
        """Batch-embed multiple inputs in a single API call and search Qdrant per result.

        All inputs that hit the lexical cache are resolved without any API call.
        The remaining uncached inputs are embedded in ONE batched Vertex call, then
        each vector is queried against Qdrant individually (Qdrant does not support
        multi-vector batch search in all versions).  Results are stored in the
        instance-level cache so subsequent per-phrase calls are free.
        """
        self._query_context = True
        if not inputs:
            return []

        allowed_kinds = set(kinds or {self.KIND_ATTRIBUTE, self.KIND_RELATIONSHIP})
        results: list[dict[str, Any] | None] = [None] * len(inputs)
        now = time.time()
        lexical_hits = 0
        cache_hits = 0

        canonical_inputs = [re.sub(r"\s+", " ", str(text or "").strip()) for text in inputs]
        if not any(canonical_inputs):
            return results

        normalized_result_cache: dict[str, dict[str, Any] | None] = {}

        # --- Pass 1: lexical + cache hits (free) ---
        pending_indexes: list[int] = []
        pending_texts: list[str] = []
        for idx, text in enumerate(canonical_inputs):
            if not text:
                continue
            if text in normalized_result_cache:
                results[idx] = normalized_result_cache[text]
                if results[idx] is not None:
                    cache_hits += 1
                continue
            # Lexical first
            lexical = self._lexical_schema_match(text, allowed_kinds)
            if lexical:
                results[idx] = lexical
                normalized_result_cache[text] = lexical
                lexical_hits += 1
                continue
            # Instance cache
            ck = self._cache_key(text)
            if ck in self.cache:
                cached_result, ts = self.cache[ck]
                if now - ts < self.CACHE_TTL:
                    results[idx] = cached_result
                    normalized_result_cache[text] = cached_result
                    cache_hits += 1
                    continue
            pending_indexes.append(idx)
            pending_texts.append(text)

        if not pending_texts or not self._index_built or not self.genai_client or not self.qdrant:
            if lexical_hits or cache_hits:
                self.logger.info(
                    "schema_term_batch_summary inputs=%s lexical_hits=%s cache_hits=%s embedded=%s resolved=%s",
                    len(inputs), lexical_hits, cache_hits, 0, sum(1 for item in results if item is not None),
                )
            return results

        unique_pending_texts: list[str] = []
        unique_pending_map: dict[str, list[int]] = {}
        for orig_idx, text in zip(pending_indexes, pending_texts):
            slots = unique_pending_map.setdefault(text, [])
            slots.append(orig_idx)
            if len(slots) == 1:
                unique_pending_texts.append(text)

        # --- Pass 2: one batched embed call for all uncached texts ---
        try:
            embed_response = self.genai_client.models.embed_content(
                model=self.embedding_model,
                contents=unique_pending_texts,
            )
            embeddings = list(getattr(embed_response, "embeddings", []) or [])
            if len(embeddings) != len(unique_pending_texts):
                # Size mismatch – fall back to individual calls
                for text in unique_pending_texts:
                    hit = self.find_schema_term(text, confidence_threshold, kinds)
                    normalized_result_cache[text] = hit
                    for i in unique_pending_map.get(text, []):
                        results[i] = hit
                return results
        except Exception as exc:
            self.logger.warning("Batch embed failed (%s); skipping vector search", exc)
            return results

        # --- Pass 3: per-embedding Qdrant search (vectors differ per phrase) ---
        for slot, (text, emb_obj) in enumerate(zip(unique_pending_texts, embeddings)):
            embedding = list(getattr(emb_obj, "values", []) or [])
            if not embedding:
                continue
            ck = self._cache_key(text)
            try:
                search_results: list[Any] = self._dense_search(embedding=embedding, limit=8)

                for top_match in search_results:
                    payload = (top_match.get("payload") if isinstance(top_match, dict) else getattr(top_match, "payload", None)) or {}
                    kind = str(payload.get("kind") or self.KIND_ATTRIBUTE).strip().lower()
                    if kind not in allowed_kinds:
                        continue
                    score = float((top_match.get("score") if isinstance(top_match, dict) else getattr(top_match, "score", 0.0)) or 0.0)
                    canonical_name = (
                        payload.get("canonical_name")
                        or payload.get("canonical_attribute")
                        or payload.get("canonical_relationship")
                    )
                    if not canonical_name:
                        continue
                    boosted = min(1.0, score + self._semantic_wording_bonus(text, str(canonical_name), kind))
                    if boosted >= confidence_threshold:
                        hit = {"kind": kind, "canonical_name": str(canonical_name), "score": boosted}
                        self.cache[ck] = (hit, now)
                        normalized_result_cache[text] = hit
                        for orig_idx in unique_pending_map.get(text, []):
                            results[orig_idx] = hit
                        break
                else:
                    # No match above threshold – cache the miss so it isn't re-tried
                    self.cache[ck] = (None, now)
                    normalized_result_cache[text] = None
            except Exception as exc:
                self.logger.warning("Qdrant search failed for '%s': %s", text, exc)

        self.logger.info(
            "schema_term_batch_summary inputs=%s lexical_hits=%s cache_hits=%s embedded=%s resolved=%s",
            len(inputs), lexical_hits, cache_hits, len(unique_pending_texts), sum(1 for item in results if item is not None),
        )

        return results

    def _dense_search(self, embedding: list[float], limit: int = 8) -> list[Any]:
        """Dense vector search with API compatibility fallback for Qdrant versions."""
        if not self.qdrant or not embedding:
            return []
        if self._vector_search_disabled:
            return []

        base_url = str(os.getenv("QDRANT_URL") or "http://localhost:6333").rstrip("/")
        api_key = str(os.getenv("QDRANT_QUERY_API_KEY") or os.getenv("QDRANT_API_KEY") or "").strip()
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["api-key"] = api_key

        payload = {
            "vector": embedding,
            "limit": int(limit),
            "with_payload": True,
            "filter": {
                "should": [
                    {"key": "tenant_id", "match": {"value": get_tenant_id()}},
                    {"key": "visibility", "match": {"value": "global"}},
                ]
            },
        }

        # Prefer REST search first because it has an explicit timeout and avoids
        # client-level hangs observed in query_points() under some local setups.
        if requests is not None:
            try:
                response = requests.post(
                    f"{base_url}/collections/{self.COLLECTION_NAME}/points/search",
                    headers=headers,
                    json=payload,
                    timeout=5,
                )
                if response.status_code == 200:
                    body = response.json() if response.content else {}
                    result = body.get("result") if isinstance(body, dict) else None
                    return list(result or [])
                if response.status_code == 400:
                    body_text = str(response.text or "")
                    if "Vector dimension error" in body_text or "expected dim" in body_text:
                        self._disable_vector_search(
                            f"qdrant_collection='{self.COLLECTION_NAME}' dimension mismatch for model '{self.embedding_model}'"
                        )
                        return []
            except Exception as exc:
                self.logger.warning(
                    "Qdrant REST search failed for collection '%s': %s; falling back to query_points",
                    self.COLLECTION_NAME,
                    exc,
                )

        # qdrant_client >= 1.10 removed .search(); use query_points() with no "using"
        # parameter so it works with both named and unnamed (simple) vector collections.
        if hasattr(self.qdrant, "query_points") and Filter is not None and FieldCondition is not None and MatchValue is not None:
            try:
                response = self.qdrant.query_points(
                    collection_name=self.COLLECTION_NAME,
                    query=embedding,
                    limit=limit,
                    query_filter=Filter(
                        should=[
                            FieldCondition(key="tenant_id", match=MatchValue(value=get_tenant_id())),
                            FieldCondition(key="visibility", match=MatchValue(value="global")),
                        ]
                    ),
                )
                return list(getattr(response, "points", []) or [])
            except Exception as exc:
                exc_text = str(exc or "")
                if "Vector dimension error" in exc_text or "expected dim" in exc_text:
                    self._disable_vector_search(
                        f"qdrant_collection='{self.COLLECTION_NAME}' dimension mismatch for model '{self.embedding_model}'"
                    )
                    return []
                self.logger.warning(
                    "Qdrant query_points failed for collection '%s': %s; falling back to REST search",
                    self.COLLECTION_NAME, exc,
                )

        if requests is None:
            return []

        try:
            response = requests.post(
                f"{base_url}/collections/{self.COLLECTION_NAME}/points/search",
                headers=headers,
                json=payload,
                timeout=5,
            )
            if response.status_code != 200:
                return []
            body = response.json() if response.content else {}
            result = body.get("result") if isinstance(body, dict) else None
            return list(result or [])
        except Exception:
            return []

    def clear_cache(self) -> None:
        """Clear embedding result cache."""
        self.cache.clear()
        self.logger.info("Embedding cache cleared")
