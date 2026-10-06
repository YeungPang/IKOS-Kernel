import argparse
import json
import logging
from datetime import date
from typing import Any

import object_db

LOGGER = logging.getLogger("idms.backfill_part")


def _doc_ids(connection: Any, requested_doc_id: int | None = None, include_inactive: bool = False, limit: int | None = None) -> list[int]:
    sql = """
    SELECT d.doc_id
    FROM document d
    WHERE (%s IS NULL OR d.doc_id = %s)
      AND (%s OR COALESCE(d.status, 'active') = 'active')
      AND d.valid_from <= %s
      AND (d.valid_until IS NULL OR d.valid_until > %s)
    ORDER BY d.doc_id
    """
    if isinstance(limit, int) and limit > 0:
        sql += " LIMIT %s"

    with connection.cursor() as cursor:
        params: list[Any] = [requested_doc_id, requested_doc_id, include_inactive, date.today(), date.today()]
        if isinstance(limit, int) and limit > 0:
            params.append(int(limit))
        cursor.execute(sql, tuple(params))
        return [int(row[0]) for row in cursor.fetchall() or []]


def _collect_doc_links(connection: Any, doc_id: int) -> tuple[set[int], set[int], set[int]]:
    object_ids: set[int] = set()
    relationship_ids: set[int] = set()
    class_ids: set[int] = set()

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT oi.object_id
            FROM object_instance oi
            WHERE COALESCE(oi.status, 'active') = 'active'
              AND NULLIF(oi.metadata->>'doc_id', '') ~ '^\\d+$'
              AND (oi.metadata->>'doc_id')::bigint = %s
            """,
            (int(doc_id),),
        )
        object_ids.update(int(row[0]) for row in cursor.fetchall() or [])

        cursor.execute(
            """
            SELECT rel.relationship_id, rel.src_object_id, rel.tar_object_id
            FROM object_relationship rel
            WHERE NULLIF(rel.metadata->>'doc_id', '') ~ '^\\d+$'
              AND (rel.metadata->>'doc_id')::bigint = %s
            """,
            (int(doc_id),),
        )
        for rel_id, src_id, tar_id in cursor.fetchall() or []:
            relationship_ids.add(int(rel_id))
            if isinstance(src_id, int):
                object_ids.add(int(src_id))
            if isinstance(tar_id, int):
                object_ids.add(int(tar_id))

        if object_ids:
            cursor.execute(
                """
                SELECT DISTINCT oc.class_id
                FROM object_instance oi
                JOIN object_class oc ON LOWER(oc.class_name) = LOWER(oi.class_name)
                WHERE oi.object_id = ANY(%s)
                """,
                (list(object_ids),),
            )
            class_ids.update(int(row[0]) for row in cursor.fetchall() or [])

    return object_ids, relationship_ids, class_ids


def _build_part_rows(doc_id: int, object_ids: set[int], relationship_ids: set[int], class_ids: set[int]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []

    rows.append(
        (
            int(doc_id),
            f"document:{int(doc_id)}",
            None,
            None,
            None,
            json.dumps({"link_source": "backfill", "doc_id": int(doc_id)}),
        )
    )

    for class_id in sorted(class_ids):
        rows.append(
            (
                int(doc_id),
                f"class:{class_id}",
                int(class_id),
                None,
                None,
                json.dumps({"link_source": "backfill", "class_id": int(class_id)}),
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
                json.dumps({"link_source": "backfill", "object_id": int(object_id)}),
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
                json.dumps({"link_source": "backfill", "relationship_id": int(relationship_id)}),
            )
        )

    return rows


def _existing_keys(connection: Any, doc_id: int) -> set[str]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT part_key FROM part WHERE doc_id = %s", (int(doc_id),))
        return {str(row[0]) for row in cursor.fetchall() or []}


def _upsert_rows(connection: Any, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO part (
                doc_id,
                part_key,
                class_id,
                object_id,
                relationship_id,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (doc_id, part_key)
            DO UPDATE SET
                class_id = COALESCE(EXCLUDED.class_id, part.class_id),
                object_id = COALESCE(EXCLUDED.object_id, part.object_id),
                relationship_id = COALESCE(EXCLUDED.relationship_id, part.relationship_id),
                metadata = COALESCE(part.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb)
            """,
            rows,
        )


def run_backfill(doc_id: int | None = None, include_inactive: bool = False, limit: int | None = None, dry_run: bool = True) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        object_db.create_tables(connection, recreate=False)

        docs = _doc_ids(
            connection=connection,
            requested_doc_id=doc_id,
            include_inactive=include_inactive,
            limit=limit,
        )

        summary = {
            "dry_run": bool(dry_run),
            "documents_scanned": 0,
            "documents_with_candidates": 0,
            "parts_candidates": 0,
            "parts_new": 0,
            "parts_existing": 0,
            "object_parts_candidates": 0,
            "relationship_parts_candidates": 0,
            "class_parts_candidates": 0,
        }

        for did in docs:
            summary["documents_scanned"] += 1
            object_ids, relationship_ids, class_ids = _collect_doc_links(connection, did)
            rows = _build_part_rows(did, object_ids, relationship_ids, class_ids)
            if not rows:
                continue

            summary["documents_with_candidates"] += 1
            summary["parts_candidates"] += len(rows)
            summary["object_parts_candidates"] += len(object_ids)
            summary["relationship_parts_candidates"] += len(relationship_ids)
            summary["class_parts_candidates"] += len(class_ids)

            existing = _existing_keys(connection, did)
            candidate_keys = {str(r[1]) for r in rows}
            summary["parts_new"] += len(candidate_keys - existing)
            summary["parts_existing"] += len(candidate_keys & existing)

            if not dry_run:
                _upsert_rows(connection, rows)

        if not dry_run:
            connection.commit()
        else:
            connection.rollback()

        return summary
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill part links (document/class/object/relationship) for existing documents")
    parser.add_argument("--doc-id", type=int, default=None, help="Optional single document ID to backfill")
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of documents to process")
    parser.add_argument("--include-inactive", action="store_true", help="Include inactive documents")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry run)")
    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_cli().parse_args()

    summary = run_backfill(
        doc_id=args.doc_id,
        include_inactive=bool(args.include_inactive),
        limit=args.limit,
        dry_run=not bool(args.apply),
    )

    LOGGER.info("part_backfill_summary %s", json.dumps(summary, ensure_ascii=True))
    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
