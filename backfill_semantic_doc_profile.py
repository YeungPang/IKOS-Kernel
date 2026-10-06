import argparse
import json
import re
from datetime import date
from pathlib import Path
from typing import Any

import object_db


def _normalize_term_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _split_fragments(value: str) -> list[str]:
    fragments: list[str] = []
    for piece in re.split(r"[,;/|]+", value or ""):
        fragment = _normalize_term_text(piece)
        if fragment:
            fragments.append(fragment)
    return fragments


def _build_document_semantic_profile(
    doc_cat: Any,
    doc_type: Any,
    doc_theme: Any,
    markdown_text: str = "",
) -> dict[str, Any]:
    cat_norm = _normalize_term_text(doc_cat)
    type_norm = _normalize_term_text(doc_type)
    theme_norm = _normalize_term_text(doc_theme)

    field_values = [cat_norm, type_norm, theme_norm]
    terms: set[str] = set()

    for value in field_values:
        if not value:
            continue
        terms.add(value)
        for fragment in _split_fragments(value):
            terms.add(fragment)
            for token in fragment.split():
                if len(token) >= 3:
                    terms.add(token)

    md_norm = _normalize_term_text(markdown_text)
    if md_norm:
        md_hints = {
            "invoice",
            "bill",
            "receipt",
            "amount",
            "total",
            "due",
            "telephone",
            "phone",
            "mobile",
            "telecom",
            "payment",
            "charge",
            "fee",
            "rechnung",
            "betrag",
            "gesamt",
        }
        for hint in md_hints:
            if hint in md_norm:
                terms.add(hint)

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
    if md_norm:
        haystack = f"{haystack} {md_norm}".strip()

    for concept, triggers, expansions in concept_rules:
        if any(trigger in haystack for trigger in triggers):
            concepts.append(concept)
            terms.update(expansions)

    return {
        "doc_cat": cat_norm,
        "doc_type": type_norm,
        "doc_theme": theme_norm,
        "terms": sorted(terms),
        "concepts": sorted(set(concepts)),
    }


def _load_markdown_text(markdown_path: str, metadata: dict[str, Any], max_chars: int = 120_000) -> str:
    candidates: list[Path] = []
    script_root = Path(__file__).resolve().parent

    raw_markdown_path = str(markdown_path or "").strip()
    if raw_markdown_path:
        p = Path(raw_markdown_path)
        if p.is_absolute():
            candidates.append(p)
        else:
            candidates.append(script_root / p)

    user_metadata = metadata.get("user_metadata") if isinstance(metadata.get("user_metadata"), dict) else {}
    source_ref = user_metadata.get("source_reference") if isinstance(user_metadata.get("source_reference"), dict) else {}
    cache_path = str(source_ref.get("markdown_cache_path") or "").strip()
    if cache_path:
        p = Path(cache_path)
        if p.is_absolute():
            candidates.append(p)
        else:
            candidates.append(script_root / p)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        try:
            if candidate.exists() and candidate.is_file():
                text = candidate.read_text(encoding="utf-8", errors="ignore")
                if max_chars > 0:
                    return text[:max_chars]
                return text
        except Exception:
            continue

    return ""


def _build_keyword_text(
    existing_keyword_text: str,
    doc_cat: str,
    doc_type: str,
    doc_theme: str,
    metadata: dict[str, Any],
    semantic_terms: list[str],
    semantic_concepts: list[str],
) -> str:
    user_description = str(metadata.get("user_description") or "").strip()
    user_tags = metadata.get("user_tags") if isinstance(metadata.get("user_tags"), list) else []

    parts: list[str] = []
    if existing_keyword_text:
        parts.append(existing_keyword_text)
    if user_description:
        parts.append(user_description)

    for tag in user_tags:
        if str(tag).strip():
            parts.append(str(tag).strip())

    for value in (doc_cat, doc_type, doc_theme):
        if str(value).strip():
            parts.append(str(value).strip())

    if semantic_terms:
        parts.append(" ".join(semantic_terms))
    if semantic_concepts:
        parts.append(" ".join(semantic_concepts))

    deduped: list[str] = []
    seen: set[str] = set()
    for raw in parts:
        cleaned = str(raw or "").strip()
        if not cleaned:
            continue
        normalized = _normalize_term_text(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(cleaned)

    return " ".join(deduped).strip()


def _iter_documents(
    connection: Any,
    doc_id: int | None = None,
    include_inactive: bool = False,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    sql = """
    SELECT
        d.doc_id,
        COALESCE(d.doc_name, ''),
        COALESCE(d.doc_key, ''),
        COALESCE(d.doc_path, ''),
        COALESCE(d.doc_cat, ''),
        COALESCE(d.doc_type, ''),
        COALESCE(d.doc_theme, ''),
        COALESCE(d.keyword_text, ''),
        COALESCE(d.markdown_path, ''),
        COALESCE(d.metadata, '{}'::jsonb)
    FROM document d
    WHERE (%s IS NULL OR d.doc_id = %s)
      AND (%s OR COALESCE(d.status, 'active') = 'active')
      AND d.valid_from <= %s
      AND (d.valid_until IS NULL OR d.valid_until > %s)
    ORDER BY d.doc_id
    """

    params: list[Any] = [doc_id, doc_id, include_inactive, date.today(), date.today()]
    if isinstance(limit, int) and limit > 0:
        sql += " LIMIT %s"
        params.append(int(limit))

    with connection.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall() or []

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "doc_id": int(row[0]),
                "doc_name": str(row[1] or ""),
                "doc_key": str(row[2] or ""),
                "doc_path": str(row[3] or ""),
                "doc_cat": str(row[4] or ""),
                "doc_type": str(row[5] or ""),
                "doc_theme": str(row[6] or ""),
                "keyword_text": str(row[7] or ""),
                "markdown_path": str(row[8] or ""),
                "metadata": row[9] if isinstance(row[9], dict) else {},
            }
        )
    return out


def run_backfill(
    doc_id: int | None = None,
    include_inactive: bool = False,
    limit: int | None = None,
    dry_run: bool = True,
    max_md_chars: int = 120_000,
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        object_db.create_tables(connection, recreate=False)
        docs = _iter_documents(
            connection=connection,
            doc_id=doc_id,
            include_inactive=include_inactive,
            limit=limit,
        )

        summary: dict[str, Any] = {
            "dry_run": bool(dry_run),
            "documents_scanned": 0,
            "documents_updated": 0,
            "documents_unchanged": 0,
            "documents_markdown_used": 0,
            "changes": [],
        }

        with connection.cursor() as cursor:
            for doc in docs:
                summary["documents_scanned"] += 1

                metadata = dict(doc.get("metadata") or {})
                md_text = _load_markdown_text(
                    markdown_path=str(doc.get("markdown_path") or ""),
                    metadata=metadata,
                    max_chars=max_md_chars,
                )
                if md_text:
                    summary["documents_markdown_used"] += 1

                profile = _build_document_semantic_profile(
                    doc_cat=doc.get("doc_cat"),
                    doc_type=doc.get("doc_type"),
                    doc_theme=doc.get("doc_theme"),
                    markdown_text=md_text,
                )
                semantic_terms = [
                    str(item).strip()
                    for item in (profile.get("terms") or [])
                    if str(item).strip()
                ]
                semantic_concepts = [
                    str(item).strip()
                    for item in (profile.get("concepts") or [])
                    if str(item).strip()
                ]

                new_keyword_text = _build_keyword_text(
                    existing_keyword_text=str(doc.get("keyword_text") or "").strip(),
                    doc_cat=str(doc.get("doc_cat") or "").strip(),
                    doc_type=str(doc.get("doc_type") or "").strip(),
                    doc_theme=str(doc.get("doc_theme") or "").strip(),
                    metadata=metadata,
                    semantic_terms=semantic_terms,
                    semantic_concepts=semantic_concepts,
                )

                prev_profile = metadata.get("semantic_doc_profile") if isinstance(metadata.get("semantic_doc_profile"), dict) else {}
                prev_terms = [
                    str(item).strip()
                    for item in (prev_profile.get("terms") or [])
                    if str(item).strip()
                ]
                prev_concepts = [
                    str(item).strip()
                    for item in (prev_profile.get("concepts") or [])
                    if str(item).strip()
                ]

                profile_changed = (sorted(set(prev_terms)) != sorted(set(semantic_terms))) or (
                    sorted(set(prev_concepts)) != sorted(set(semantic_concepts))
                )
                keyword_changed = _normalize_term_text(new_keyword_text) != _normalize_term_text(str(doc.get("keyword_text") or ""))

                if not profile_changed and not keyword_changed:
                    summary["documents_unchanged"] += 1
                    continue

                metadata["semantic_doc_profile"] = {
                    "terms": semantic_terms,
                    "concepts": semantic_concepts,
                    "backfill": {
                        "script": "backfill_semantic_doc_profile.py",
                        "used_markdown": bool(md_text),
                    },
                }

                cursor.execute(
                    """
                    UPDATE document
                    SET
                        keyword_text = %s,
                        metadata = %s::jsonb
                    WHERE doc_id = %s
                    """,
                    (
                        new_keyword_text,
                        json.dumps(metadata, ensure_ascii=False),
                        int(doc["doc_id"]),
                    ),
                )

                summary["documents_updated"] += 1
                if len(summary["changes"]) < 30:
                    summary["changes"].append(
                        {
                            "doc_id": int(doc["doc_id"]),
                            "doc_name": str(doc.get("doc_name") or ""),
                            "doc_key": str(doc.get("doc_key") or ""),
                            "profile_terms": len(semantic_terms),
                            "profile_concepts": semantic_concepts,
                            "keyword_changed": keyword_changed,
                            "profile_changed": profile_changed,
                        }
                    )

        if dry_run:
            connection.rollback()
        else:
            connection.commit()

        return summary
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill semantic_doc_profile and keyword_text from document rows and markdown files"
    )
    parser.add_argument("--doc-id", type=int, default=None, help="Only process one document id")
    parser.add_argument("--include-inactive", action="store_true", help="Include non-active documents")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of scanned documents")
    parser.add_argument("--max-md-chars", type=int, default=120000, help="Max markdown chars to inspect per document")
    parser.add_argument("--apply", action="store_true", help="Apply updates (default is dry-run)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_backfill(
        doc_id=args.doc_id,
        include_inactive=bool(args.include_inactive),
        limit=args.limit if args.limit and args.limit > 0 else None,
        dry_run=not bool(args.apply),
        max_md_chars=max(0, int(args.max_md_chars or 0)),
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
