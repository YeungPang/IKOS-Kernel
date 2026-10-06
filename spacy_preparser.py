from __future__ import annotations

import logging
import os
import re
from typing import Any

try:
    import spacy
except Exception:  # pragma: no cover - optional dependency
    spacy = None


LOGGER = logging.getLogger("idms.spacy_preparser")

_SPACY_NLP: Any | None = None
_SPACY_MODEL_NAME: str | None = None
_SPACY_LOAD_ATTEMPTED = False
_SPACY_MODEL_CACHE: dict[str, Any] = {}


def _normalize_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _detect_relationship_type(sentence_text: str) -> tuple[str | None, float]:
    lowered = sentence_text.lower()

    shareholder_keywords = {
        "shareholder",
        "shareholders",
        "gesellschafter",
        "aktionar",
        "aktionaer",
        "aktionarinnen",
    }
    if any(token in lowered for token in shareholder_keywords):
        return "shareholder_of", 0.72

    contact_keywords = {
        "contact person",
        "ansprechpartner",
        "kontaktperson",
    }
    eori_context = {"eori", "customs", "zoll"}
    if any(token in lowered for token in contact_keywords) and any(token in lowered for token in eori_context):
        return "eori_contact_person", 0.68

    return None, 0.0


def _normalize_language_tag(value: Any) -> str:
    language = str(value or "").strip().lower()
    aliases = {
        "de": "german",
        "de-de": "german",
        "deutsch": "german",
        "ger": "german",
        "en": "english",
        "en-us": "english",
        "en-gb": "english",
    }
    return aliases.get(language, language)


def _model_candidates_for_language(language_hint: str | None) -> list[str]:
    normalized = _normalize_language_tag(language_hint)
    env_model = str(os.getenv("IDMS_SPACY_MODEL", "")).strip()
    strict_language = str(os.getenv("IDMS_SPACY_STRICT_LANGUAGE", "false")).strip().lower() in {"1", "true", "yes", "on"}
    allow_cross_language = str(os.getenv("IDMS_SPACY_ALLOW_CROSS_LANGUAGE_FALLBACK", "false")).strip().lower() in {"1", "true", "yes", "on"}

    candidates: list[str] = []
    if env_model:
        candidates.append(env_model)

    if normalized == "german":
        candidates.extend(["de_core_news_sm", "xx_ent_wiki_sm"])
        if not strict_language or allow_cross_language:
            candidates.append("en_core_web_sm")
    elif normalized == "english":
        candidates.extend(["en_core_web_sm", "xx_ent_wiki_sm"])
        if not strict_language or allow_cross_language:
            candidates.append("de_core_news_sm")
    else:
        candidates.extend(["xx_ent_wiki_sm", "en_core_web_sm", "de_core_news_sm"])

    unique: list[str] = []
    seen: set[str] = set()
    for model_name in candidates:
        if model_name not in seen:
            unique.append(model_name)
            seen.add(model_name)
    return unique


def _get_spacy_nlp(language_hint: str | None = None) -> tuple[Any | None, dict[str, Any]]:
    global _SPACY_NLP, _SPACY_MODEL_NAME, _SPACY_LOAD_ATTEMPTED

    model_candidates = _model_candidates_for_language(language_hint)
    normalized_language = _normalize_language_tag(language_hint)

    for model_name in model_candidates:
        if model_name in _SPACY_MODEL_CACHE:
            return _SPACY_MODEL_CACHE[model_name], {
                "enabled": True,
                "available": True,
                "model_name": model_name,
                "language_hint": normalized_language,
            }

    if spacy is None:
        return None, {
            "enabled": False,
            "available": False,
            "reason": "spacy_not_installed",
            "language_hint": normalized_language,
        }

    _SPACY_LOAD_ATTEMPTED = True

    for model_name in model_candidates:
        try:
            loaded = spacy.load(model_name)
            _SPACY_NLP = loaded
            _SPACY_MODEL_NAME = model_name
            _SPACY_MODEL_CACHE[model_name] = loaded
            LOGGER.info("spaCy model loaded: %s", model_name)
            return loaded, {
                "enabled": True,
                "available": True,
                "model_name": model_name,
                "language_hint": normalized_language,
            }
        except Exception:
            continue

    return None, {
        "enabled": False,
        "available": False,
        "reason": "spacy_model_unavailable",
        "tried_models": model_candidates,
        "language_hint": normalized_language,
    }


def extract_relationship_hints(markdown_text: str, language_hint: str | None = None) -> dict[str, Any]:
    nlp, status = _get_spacy_nlp(language_hint=language_hint)
    if nlp is None:
        return {
            "status": status,
            "relationships": [],
        }

    text = str(markdown_text or "").strip()
    if not text:
        return {
            "status": status,
            "relationships": [],
        }

    try:
        doc = nlp(text)
    except Exception:
        LOGGER.exception("spaCy parse failed")
        return {
            "status": {
                "enabled": False,
                "available": False,
                "reason": "spacy_parse_failed",
                "model_name": status.get("model_name"),
            },
            "relationships": [],
        }

    relationships: list[dict[str, Any]] = []

    for sent in getattr(doc, "sents", []):
        sentence_text = str(sent.text or "").strip()
        if not sentence_text:
            continue

        relationship_type, confidence = _detect_relationship_type(sentence_text)
        if relationship_type is None:
            continue

        persons = []
        orgs = []
        for ent in sent.ents:
            label = str(ent.label_ or "").upper()
            value = str(ent.text or "").strip(" ,.;:-")
            if len(value) < 2:
                continue
            if label == "PERSON":
                persons.append(value)
            elif label in {"ORG", "FAC"}:
                orgs.append(value)

        if not persons or not orgs:
            continue

        for person_name in persons[:3]:
            for org_name in orgs[:3]:
                relationships.append(
                    {
                        "relationship_type": relationship_type,
                        "relationship_cat": "semantic",
                        "source_name": person_name,
                        "source_class": "person",
                        "target_name": org_name,
                        "target_class": "company",
                        "confidence": confidence,
                        "evidence": sentence_text,
                    }
                )

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for rel in relationships:
        key = (
            _normalize_name(rel.get("source_name")),
            _normalize_name(rel.get("target_name")),
            str(rel.get("relationship_type") or "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rel)

    return {
        "status": status,
        "relationships": deduped,
    }


def _next_entity_id(existing_entities: list[dict[str, Any]]) -> str:
    max_idx = 0
    for entity in existing_entities:
        entity_id = str(entity.get("entity_id") or "")
        match = re.match(r"^e_spacy_(\d+)$", entity_id)
        if match:
            max_idx = max(max_idx, int(match.group(1)))
    return f"e_spacy_{max_idx + 1}"


def _name_tokens(value: Any) -> list[str]:
    return [tok for tok in re.findall(r"[A-Za-z0-9ÄÖÜäöüß]+", str(value or "").lower()) if tok]


def _match_existing_entity_id(
    entities: list[dict[str, Any]],
    name: str,
    class_name: str,
) -> str | None:
    normalized_name = _normalize_name(name)
    normalized_class = str(class_name or "entity").strip().lower() or "entity"
    name_tokens = _name_tokens(normalized_name)
    if not normalized_name:
        return None

    exact_matches: list[str] = []
    subset_matches: list[str] = []
    for entity in entities:
        entity_id = str(entity.get("entity_id") or "")
        if not entity_id:
            continue
        entity_class = str(entity.get("class_name") or "").strip().lower()
        if entity_class != normalized_class:
            continue
        entity_name = _normalize_name(entity.get("name"))
        if not entity_name:
            continue
        if entity_name == normalized_name:
            exact_matches.append(entity_id)
            continue

        # Conservative consolidation for person mentions from spaCy:
        # if all mention tokens appear in one existing extracted person name,
        # reuse that entity instead of creating a new sparse duplicate.
        if normalized_class == "person" and len(name_tokens) >= 2:
            entity_tokens = _name_tokens(entity_name)
            if entity_tokens and all(tok in entity_tokens for tok in name_tokens):
                subset_matches.append(entity_id)

    if exact_matches:
        return exact_matches[0]
    if len(set(subset_matches)) == 1:
        return subset_matches[0]
    return None


def enrich_ingested_with_spacy_hints(
    ingested: dict[str, Any],
    markdown_text: str,
    language_hint: str | None = None,
) -> dict[str, Any]:
    entities = ingested.get("solf_entities") if isinstance(ingested.get("solf_entities"), list) else []
    relationships = ingested.get("solf_relationships") if isinstance(ingested.get("solf_relationships"), list) else []

    extracted = extract_relationship_hints(markdown_text, language_hint=language_hint)
    status = extracted.get("status") if isinstance(extracted.get("status"), dict) else {}
    hints = extracted.get("relationships") if isinstance(extracted.get("relationships"), list) else []

    if not hints:
        return {
            "enabled": bool(status.get("enabled")),
            "available": bool(status.get("available")),
            "model_name": status.get("model_name"),
            "relationships_detected": 0,
            "relationships_added": 0,
            "entities_added": 0,
            "reason": status.get("reason"),
        }

    entity_index: dict[tuple[str, str], str] = {}
    for entity in entities:
        name = _normalize_name(entity.get("name"))
        class_name = str(entity.get("class_name") or "").strip().lower()
        entity_id = str(entity.get("entity_id") or "")
        if name and class_name and entity_id:
            entity_index[(name, class_name)] = entity_id

    entities_added = 0

    def ensure_entity(name: str, class_name: str, confidence: float) -> str:
        nonlocal entities_added
        normalized_name = _normalize_name(name)
        normalized_class = str(class_name or "entity").strip().lower() or "entity"

        if normalized_class == "person":
            # Avoid creating weak one-token person entities from NER fragments like surnames only.
            if len(_name_tokens(normalized_name)) < 2:
                return ""

        key = (normalized_name, normalized_class)
        existing_id = entity_index.get(key)
        if existing_id:
            return existing_id

        consolidated_id = _match_existing_entity_id(entities, name, normalized_class)
        if consolidated_id:
            entity_index[key] = consolidated_id
            return consolidated_id

        entity_id = _next_entity_id(entities)
        entities.append(
            {
                "entity_id": entity_id,
                "name": str(name).strip(),
                "class_name": normalized_class,
                "operation": "ingest",
                "effective_from": None,
                "effective_until": None,
                "attributes": {},
                "confidence": round(float(confidence), 4),
            }
        )
        entity_index[key] = entity_id
        entities_added += 1
        return entity_id

    existing_relationship_keys: set[tuple[str, str, str]] = set()
    for rel in relationships:
        src = str(rel.get("source_entity_id") or "")
        tar = str(rel.get("target_entity_id") or "")
        rel_type = str(rel.get("relationship_type") or "").strip().lower()
        if src and tar and rel_type:
            existing_relationship_keys.add((src, tar, rel_type))

    relationships_added = 0
    for hint in hints:
        src_name = str(hint.get("source_name") or "").strip()
        tar_name = str(hint.get("target_name") or "").strip()
        rel_type = str(hint.get("relationship_type") or "").strip().lower()
        if not src_name or not tar_name or not rel_type:
            continue

        src_id = ensure_entity(src_name, str(hint.get("source_class") or "person"), float(hint.get("confidence") or 0.5))
        tar_id = ensure_entity(tar_name, str(hint.get("target_class") or "company"), float(hint.get("confidence") or 0.5))
        if not src_id or not tar_id:
            continue

        rel_key = (src_id, tar_id, rel_type)
        if rel_key in existing_relationship_keys:
            continue

        relationships.append(
            {
                "source_entity_id": src_id,
                "target_entity_id": tar_id,
                "source_name": src_name,
                "target_name": tar_name,
                "relationship_type": rel_type,
                "relationship_cat": str(hint.get("relationship_cat") or "semantic"),
                "operation": "upsert",
                "exclusivity_scope": None,
                "effective_from": None,
                "effective_until": None,
                "attributes": {
                    "nlp_source": "spacy",
                    "evidence": str(hint.get("evidence") or "")[:400],
                },
                "confidence": round(float(hint.get("confidence") or 0.5), 4),
            }
        )
        existing_relationship_keys.add(rel_key)
        relationships_added += 1

    return {
        "enabled": bool(status.get("enabled")),
        "available": bool(status.get("available")),
        "model_name": status.get("model_name"),
        "relationships_detected": len(hints),
        "relationships_added": relationships_added,
        "entities_added": entities_added,
    }
