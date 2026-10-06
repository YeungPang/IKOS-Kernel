import argparse
import json
import re
from datetime import date
from pathlib import Path
from typing import Any

import object_db


def _looks_like_uri(path_text: str) -> bool:
    text = str(path_text or "").strip()
    if not text:
        return False
    return bool(re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", text))


def _looks_like_markdown_artifact(path_text: str) -> bool:
    text = str(path_text or "").strip().lower()
    if not text:
        return False
    if text.endswith(".md") or text.endswith(".markdown"):
        return True
    return "generated\\markdown" in text or "generated/markdown" in text


def _looks_like_temp_upload(path_text: str) -> bool:
    text = str(path_text or "").strip().lower()
    if not text:
        return False
    return bool(re.search(r"(?:^|[\\/])idms-upload-[a-z0-9]+\.[a-z0-9]+$", text))


def _resolve_original_local_path(*candidates: str) -> str:
    """Pick best absolute local path from candidates.

    Preference:
    1) Existing absolute local path
    2) Existing relative local path that can be resolved on disk
    URI-like values are ignored.
    """
    for raw in candidates:
        value = str(raw or "").strip()
        if not value or _looks_like_uri(value):
            continue
        if _looks_like_markdown_artifact(value):
            continue
        if _looks_like_temp_upload(value):
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


def _iter_documents(
    connection: Any,
    doc_id: int | None = None,
    include_inactive: bool = False,
    limit: int | None = None,
) -> list[tuple[int, str, str, dict[str, Any]]]:
    sql = """
    SELECT d.doc_id, COALESCE(d.doc_name, ''), COALESCE(d.doc_path, ''), COALESCE(d.metadata, '{}'::jsonb)
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

    out: list[tuple[int, str, str, dict[str, Any]]] = []
    for row in rows:
        metadata = row[3] if isinstance(row[3], dict) else {}
        out.append((int(row[0]), str(row[1] or ""), str(row[2] or ""), metadata))
    return out


def run_backfill(
    doc_id: int | None = None,
    include_inactive: bool = False,
    limit: int | None = None,
    dry_run: bool = True,
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
            "documents_skipped": 0,
            "changes": [],
        }

        for did, doc_name, doc_path, metadata in docs:
            summary["documents_scanned"] += 1
            user_metadata = metadata.get("user_metadata") if isinstance(metadata.get("user_metadata"), dict) else {}
            source_ref = user_metadata.get("source_reference") if isinstance(user_metadata.get("source_reference"), dict) else {}

            existing_original = str(source_ref.get("original_local_path") or "").strip()
            if _looks_like_temp_upload(existing_original):
                existing_original = ""
            resolved_original = _resolve_original_local_path(
                existing_original,
                str(user_metadata.get("client_file_path") or "").strip(),
                str(source_ref.get("source_path_or_uri") or "").strip(),
                str(source_ref.get("gcs_uri") or "").strip(),
                doc_path,
            )

            if not resolved_original:
                if str(source_ref.get("original_local_path") or "").strip():
                    new_metadata = dict(metadata)
                    new_user_metadata = dict(user_metadata)
                    new_source_ref = dict(source_ref)
                    new_source_ref["original_local_path"] = ""
                    new_user_metadata["source_reference"] = new_source_ref
                    new_metadata["user_metadata"] = new_user_metadata
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "UPDATE document SET metadata = %s::jsonb WHERE doc_id = %s",
                            (json.dumps(new_metadata, ensure_ascii=False), int(did)),
                        )
                    summary["documents_updated"] += 1
                    if len(summary["changes"]) < 25:
                        summary["changes"].append(
                            {
                                "doc_id": int(did),
                                "doc_name": doc_name,
                                "before": str(source_ref.get("original_local_path") or "").strip(),
                                "after": "",
                            }
                        )
                else:
                    summary["documents_skipped"] += 1
                continue

            if resolved_original == existing_original:
                summary["documents_unchanged"] += 1
                continue

            new_metadata = dict(metadata)
            new_user_metadata = dict(user_metadata)
            new_source_ref = dict(source_ref)
            new_source_ref["original_local_path"] = resolved_original
            new_user_metadata["source_reference"] = new_source_ref
            new_metadata["user_metadata"] = new_user_metadata

            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE document SET metadata = %s::jsonb WHERE doc_id = %s",
                    (json.dumps(new_metadata, ensure_ascii=False), int(did)),
                )

            summary["documents_updated"] += 1
            if len(summary["changes"]) < 25:
                summary["changes"].append(
                    {
                        "doc_id": int(did),
                        "doc_name": doc_name,
                        "before": existing_original,
                        "after": resolved_original,
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
    parser = argparse.ArgumentParser(description="Backfill document source_reference.original_local_path for legacy rows")
    parser.add_argument("--doc-id", type=int, default=None, help="Only process one document id")
    parser.add_argument("--include-inactive", action="store_true", help="Include non-active documents")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of scanned documents")
    parser.add_argument("--apply", action="store_true", help="Apply updates (default is dry-run)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_backfill(
        doc_id=args.doc_id,
        include_inactive=bool(args.include_inactive),
        limit=args.limit if args.limit and args.limit > 0 else None,
        dry_run=not bool(args.apply),
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
