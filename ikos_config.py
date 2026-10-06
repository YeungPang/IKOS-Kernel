"""
Central configuration for IKOS-Demo (OpenRouter-only).
"""

import json
import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Exact, verified identity-provider subjects allowed to use platform provisioning
# endpoints. This list is deployment configuration and is never writable through
# the API. Use stable issuer-qualified subjects, not email addresses.
PLATFORM_ADMIN_SUBJECTS: frozenset[str] = frozenset(
    subject.strip()
    for subject in os.getenv("IKOS_PLATFORM_ADMIN_SUBJECTS", "").split(",")
    if subject.strip()
)
INTEGRATION_JWT_PUBLIC_KEY_PATH: str = os.getenv("IKOS_INTEGRATION_JWT_PUBLIC_KEY_PATH", "").strip()
INTEGRATION_JWT_PUBLIC_KEY_PEM: str = os.getenv("IKOS_INTEGRATION_JWT_PUBLIC_KEY_PEM", "").strip()
INTEGRATION_JWT_ISSUER: str = os.getenv("IKOS_INTEGRATION_JWT_ISSUER", "").strip()
INTEGRATION_JWT_AUDIENCE: str = os.getenv("IKOS_INTEGRATION_JWT_AUDIENCE", "ikos-api").strip()
INTEGRATION_JWT_CLOCK_SKEW_SECONDS: int = int(os.getenv("IKOS_INTEGRATION_JWT_CLOCK_SKEW_SECONDS", "30"))

# ---------------------------------------------------------------------------
# OpenRouter-only LLM and embedding settings
# ---------------------------------------------------------------------------
ENABLE_OPENROUTER_FALLBACK: bool = (
    os.getenv("IKOS_ENABLE_OPENROUTER_FALLBACK", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL: str = os.getenv("IKOS_OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL: str = os.getenv("IKOS_OPENROUTER_MODEL", "google/gemini-2.5-flash")
OPENROUTER_SIMPLE_MODEL: str = os.getenv("IKOS_OPENROUTER_MODEL_SIMPLE", "openai/gpt-4o-mini")
OPENROUTER_COMPLEX_MODEL: str = os.getenv("IKOS_OPENROUTER_MODEL_COMPLEX", "google/gemini-2.5-pro")
OPENROUTER_EMBEDDING_MODEL: str = os.getenv(
    "IKOS_OPENROUTER_EMBEDDING_MODEL",
    "openai/text-embedding-3-large",
)
OPENROUTER_TIMEOUT_SECONDS: int = int(os.getenv("IKOS_OPENROUTER_TIMEOUT_SECONDS", "60"))

# Backward-compatible model aliases used by existing modules.
ROUTER_MODEL: str = OPENROUTER_SIMPLE_MODEL
EXTRACT_MODEL: str = OPENROUTER_MODEL
CHAT_MODEL: str = OPENROUTER_COMPLEX_MODEL
CONNECTIVITY_MODEL: str = OPENROUTER_SIMPLE_MODEL
CLASSIFY_MODEL: str = OPENROUTER_SIMPLE_MODEL
ESCALATION_MODEL: str = OPENROUTER_COMPLEX_MODEL

# ---------------------------------------------------------------------------
# Discovery is intentionally disabled in demo package
# ---------------------------------------------------------------------------
ENABLE_DISCOVERY_INDEX: bool = False
DISCOVERY_DATA_STORE_ID: str = ""
DISCOVERY_LOCATION: str = ""
DISCOVERY_COLLECTION: str = ""
DISCOVERY_BRANCH: str = ""
DISCOVERY_SERVING_CONFIG: str = ""

# Compatibility placeholders (kept so copied code does not break imports).
PROJECT_ID: str = ""
LOCATION: str = ""
EMBEDDING_LOCATION: str = ""
INGEST_BUCKET: str = ""
GOOGLE_APPLICATION_CREDENTIALS: str = ""

# ---------------------------------------------------------------------------
# Query behavior flags
# ---------------------------------------------------------------------------
ENABLE_SEMANTIC_LLM_FALLBACK: bool = (
    os.getenv("IKOS_ENABLE_SEMANTIC_LLM_FALLBACK", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
ENABLE_CONTEXTUAL_LLM_RESOLUTION_FALLBACK: bool = (
    os.getenv("IKOS_ENABLE_CONTEXTUAL_LLM_RESOLUTION_FALLBACK", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
ENABLE_LLM_INTENT_PARSER: bool = (
    os.getenv("IKOS_ENABLE_LLM_INTENT_PARSER", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
ENABLE_STRICT_QUERY_EVIDENCE_GATE: bool = (
    os.getenv("IKOS_ENABLE_STRICT_QUERY_EVIDENCE_GATE", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
STRICT_QUERY_EVIDENCE_REQUIRE_EXPLANATION: bool = (
    os.getenv("IKOS_STRICT_QUERY_EVIDENCE_REQUIRE_EXPLANATION", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
FLASH_CONFIDENCE_THRESHOLD: float = float(
    os.getenv("IKOS_FLASH_CONFIDENCE_THRESHOLD", "0.78")
)

ENABLE_ATTRIBUTE_EMBEDDING_UPSERT: bool = (
    os.getenv("IKOS_ENABLE_ATTRIBUTE_EMBEDDING_UPSERT", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
ENABLE_ATTRIBUTE_EMBEDDING_UPSERT_FOR_NOTES: bool = (
    os.getenv("IKOS_ENABLE_ATTRIBUTE_EMBEDDING_UPSERT_FOR_NOTES", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)

ENABLE_INGEST_LLM_TERM_ALIAS_EXPANSION: bool = (
    os.getenv("IKOS_ENABLE_INGEST_LLM_TERM_ALIAS_EXPANSION", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
INGEST_LLM_TERM_ALIAS_MAX_CANONICAL: int = int(
    os.getenv("IKOS_INGEST_LLM_TERM_ALIAS_MAX_CANONICAL", "120")
)
INGEST_LLM_TERM_ALIAS_MAX_ALIASES_PER_TERM: int = int(
    os.getenv("IKOS_INGEST_LLM_TERM_ALIAS_MAX_ALIASES_PER_TERM", "6")
)
INGEST_LLM_TERM_ALIAS_LANGUAGES: list[str] = [
    part.strip().lower()
    for part in str(os.getenv("IKOS_INGEST_LLM_TERM_ALIAS_LANGUAGES", "en,de")).split(",")
    if part.strip()
]
ENABLE_INGEST_TERM_IMPORTANCE_LLM_RERANK: bool = (
    os.getenv("IKOS_ENABLE_INGEST_TERM_IMPORTANCE_LLM_RERANK", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
INGEST_TERM_IMPORTANCE_BORDERLINE_MIN: float = float(
    os.getenv("IKOS_INGEST_TERM_IMPORTANCE_BORDERLINE_MIN", "0.45")
)
INGEST_TERM_IMPORTANCE_BORDERLINE_MAX: float = float(
    os.getenv("IKOS_INGEST_TERM_IMPORTANCE_BORDERLINE_MAX", "0.75")
)
INGEST_TERM_IMPORTANCE_PRIMARY_THRESHOLD: float = float(
    os.getenv("IKOS_INGEST_TERM_IMPORTANCE_PRIMARY_THRESHOLD", "0.80")
)

# ---------------------------------------------------------------------------
# PostgreSQL (IKOS_demo)
# ---------------------------------------------------------------------------
DB_NAME: str = os.getenv("IKOS_DB_NAME", "ikos_dev")
DB_HOST: str = os.getenv("IKOS_DB_HOST", "localhost")
DB_USER: str = os.getenv("IKOS_DB_USER", "postgres")
DB_PASSWORD: str = os.getenv("IKOS_DB_PASSWORD", "")
DB_PORT: str = os.getenv("IKOS_DB_PORT", "5432")
EXTENSION_PACKAGE_INBOX: str = os.getenv("IKOS_EXTENSION_PACKAGE_INBOX", "").strip()

# ---------------------------------------------------------------------------
# Qdrant
# ---------------------------------------------------------------------------
ENABLE_QDRANT_INDEX: bool = (
    os.getenv("IKOS_ENABLE_QDRANT_INDEX", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
QDRANT_HOST: str = os.getenv("IKOS_QDRANT_HOST", "localhost")
QDRANT_PORT: int = int(os.getenv("IKOS_QDRANT_PORT", "6333"))
QDRANT_API_KEY: str = os.getenv("IKOS_QDRANT_API_KEY", "")
QDRANT_COLLECTION_STANDARD: str = "ikos_demo_documents"  # Single source of truth for collection naming
QDRANT_COLLECTION: str = os.getenv("QDRANT_COLLECTION", QDRANT_COLLECTION_STANDARD)
INGEST_QDRANT_COLLECTION: str = os.getenv("INGEST_QDRANT_COLLECTION", QDRANT_COLLECTION_STANDARD)
QDRANT_EMBEDDING_MODEL: str = os.getenv(
    "IKOS_QDRANT_EMBEDDING_MODEL",
    OPENROUTER_EMBEDDING_MODEL,
)
QDRANT_VECTOR_SIZE: int = int(os.getenv("IKOS_QDRANT_EMBEDDING_DIMENSION", "3072"))
QDRANT_CHUNK_SIZE: int = int(os.getenv("IKOS_QDRANT_CHUNK_SIZE", "800"))
QDRANT_CHUNK_OVERLAP: int = int(os.getenv("IKOS_QDRANT_CHUNK_OVERLAP", "100"))
USE_DUAL_INDEXING: bool = (
    os.getenv("IKOS_USE_DUAL_INDEXING", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)

QDRANT_URL: str = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_QUERY_COLLECTION: str = os.getenv("QDRANT_COLLECTION", QDRANT_COLLECTION)
QDRANT_QUERY_API_KEY: str = os.getenv("QDRANT_API_KEY", "")

# ---------------------------------------------------------------------------
# Transaction matching vector lane (separate from document RAG)
# ---------------------------------------------------------------------------
ENABLE_TX_MATCH_INDEX: bool = (
    os.getenv("IKOS_ENABLE_TX_MATCH_INDEX", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
TX_MATCH_DOC_TYPES: list[str] = [
    token.strip().lower()
    for token in os.getenv("IKOS_TX_MATCH_DOC_TYPES", "invoice,bill,receipt").split(",")
    if token.strip()
]
TX_MATCH_QDRANT_HOST: str = os.getenv("IKOS_TX_MATCH_QDRANT_HOST", QDRANT_HOST)
TX_MATCH_QDRANT_PORT: int = int(os.getenv("IKOS_TX_MATCH_QDRANT_PORT", str(QDRANT_PORT)))
TX_MATCH_QDRANT_API_KEY: str = os.getenv("IKOS_TX_MATCH_QDRANT_API_KEY", QDRANT_API_KEY)
TX_MATCH_QDRANT_COLLECTION: str = os.getenv("IKOS_TX_MATCH_QDRANT_COLLECTION", "IKOS_demo_tx_match")
TX_MATCH_EMBEDDING_MODEL: str = os.getenv("IKOS_TX_MATCH_EMBEDDING_MODEL", QDRANT_EMBEDDING_MODEL)
TX_MATCH_VECTOR_SIZE: int = int(os.getenv("IKOS_TX_MATCH_EMBEDDING_DIMENSION", str(QDRANT_VECTOR_SIZE)))
TX_MATCH_MIN_FIELDS: list[str] = [
    token.strip().lower()
    for token in os.getenv("IKOS_TX_MATCH_MIN_FIELDS", "amount,currency,date,description").split(",")
    if token.strip()
]

# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------
RECREATE_SCHEMA: bool = os.getenv("IKOS_RECREATE_SCHEMA", "false").lower() == "true"
LOG_DIR: Path = Path(os.getenv("IKOS_LOG_DIR", "log"))
MARKDOWN_CACHE_DIR: Path = Path(os.getenv("IKOS_MARKDOWN_CACHE_DIR", "generated/markdown"))
INSTANCE_CONNECTION_NAME: str = os.getenv("IKOS_INSTANCE_CONNECTION_NAME", "")

# ---------------------------------------------------------------------------
# SOLF governance
# ---------------------------------------------------------------------------
SOLF_PROPOSAL_PRIORITY_HIGH_THRESHOLD: int = int(
    os.getenv("IKOS_SOLF_PROPOSAL_PRIORITY_HIGH_THRESHOLD", "8")
)
SOLF_PROPOSAL_PRIORITY_MEDIUM_THRESHOLD: int = int(
    os.getenv("IKOS_SOLF_PROPOSAL_PRIORITY_MEDIUM_THRESHOLD", "4")
)
SOLF_PROPOSAL_SUGGEST_OCCURRENCE_THRESHOLD: int = int(
    os.getenv("IKOS_SOLF_PROPOSAL_SUGGEST_OCCURRENCE_THRESHOLD", "3")
)
SOLF_PROPOSAL_SUGGEST_PRIORITY_THRESHOLD: int = int(
    os.getenv("IKOS_SOLF_PROPOSAL_SUGGEST_PRIORITY_THRESHOLD", "8")
)

# ---------------------------------------------------------------------------
# Language / lexical generation tuning
# ---------------------------------------------------------------------------
_DEFAULT_DE_NOUN_GENDER_OVERRIDES = {
    "abschluss": "m",
    "doktorgrad": "m",
    "grad": "m",
    "titel": "m",
    "nachweis": "m",
    "zertifikat": "n",
    "dokument": "n",
    "diplom": "n",
    "zeugnis": "n",
    "zertifizierung": "f",
    "qualifikation": "f",
    "ausbildung": "f",
    "universitat": "f",
    "hochschule": "f",
}


def _load_de_noun_gender_overrides() -> dict[str, str]:
    raw = os.getenv("IKOS_DE_NOUN_GENDER_OVERRIDES", "").strip()
    merged = dict(_DEFAULT_DE_NOUN_GENDER_OVERRIDES)
    if not raw:
        return merged
    try:
        payload = json.loads(raw)
    except Exception:
        return merged
    if not isinstance(payload, dict):
        return merged
    for key, value in payload.items():
        noun = str(key or "").strip().lower()
        gender = str(value or "").strip().lower()
        if noun and gender in {"m", "f", "n"}:
            merged[noun] = gender
    return merged


GERMAN_NOUN_GENDER_OVERRIDES: dict[str, str] = _load_de_noun_gender_overrides()
