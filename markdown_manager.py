"""
Markdown file management for document ingestion and re-processing.

Provides robust path resolution, storage, and retrieval of markdown files
with deterministic naming based on document metadata.
"""

import hashlib
import logging
from pathlib import Path
from typing import Any

import object_db
from security_context import get_security_context

LOGGER = logging.getLogger(__name__)

# Get project root directory (where this file's module is located)
PROJECT_ROOT = Path(__file__).parent
MARKDOWN_DIR = PROJECT_ROOT / "generated" / "markdown"


def ensure_markdown_dir() -> Path:
    """Ensure markdown directory exists."""
    MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)
    return MARKDOWN_DIR


def compute_markdown_filename(doc_id: int, doc_key: str | None = None) -> str:
    """
    Compute deterministic markdown filename.
    
    Uses doc_key as primary identifier for robustness (survives doc_id changes),
    falls back to doc_id if doc_key unavailable.
    
    Args:
        doc_id: Database document ID
        doc_key: Document key (filename or identifier)
    
    Returns:
        Filename like "document-XYZ.md" or "document-12345.md"
    """
    if doc_key and str(doc_key).strip() and int(doc_id or 0) > 0:
        # Include doc_id to avoid collisions when the same doc_key is ingested multiple times.
        key_hash = hashlib.md5(str(doc_key).strip().encode()).hexdigest()[:12]
        return f"document-{int(doc_id)}-{key_hash}.md"
    if doc_key and str(doc_key).strip():
        key_hash = hashlib.md5(str(doc_key).strip().encode()).hexdigest()[:12]
        return f"document-{key_hash}.md"
    else:
        # Fallback to doc_id
        return f"document-{doc_id}.md"


def get_markdown_path(doc_id: int, doc_key: str | None = None) -> Path:
    """
    Get expected markdown file path for a document.
    
    Args:
        doc_id: Database document ID
        doc_key: Document key (filename or identifier)
    
    Returns:
        Full path to markdown file
    """
    ensure_markdown_dir()
    filename = compute_markdown_filename(doc_id, doc_key)
    return MARKDOWN_DIR / filename


def save_markdown_path_to_db(
    conn,
    doc_id: int,
    markdown_path: str | Path,
) -> bool:
    """
    Store markdown file path in document markdown_path column.
    
    Args:
        conn: Database connection
        doc_id: Document ID
        markdown_path: Path to markdown file (relative or absolute)
    
    Returns:
        True if updated, False on error
    """
    try:
        path_str = str(markdown_path)
        context = get_security_context(required=False)
        tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE document
                SET markdown_path = %s,
                    metadata = jsonb_set(
                        COALESCE(metadata, '{}'::jsonb),
                        '{markdown_path}',
                        to_jsonb(%s::text),
                        true
                    )
                WHERE doc_id = %s AND tenant_id = %s::uuid
                """,
                (path_str, path_str, doc_id, tenant_id),
            )
            updated_rows = int(cur.rowcount or 0)
        conn.commit()
        return updated_rows > 0
    except Exception as e:
        LOGGER.error("Failed to save markdown path for doc_id %s: %s", doc_id, e)
        return False


def save_markdown_content_to_db(
    conn,
    doc_id: int,
    markdown_text: str,
    *,
    source_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    """Persist Markdown as versioned database content; the filesystem path is legacy-only."""
    text = str(markdown_text or "")
    if not text.strip():
        raise ValueError("markdown_text must not be empty")
    return object_db.save_document_content(
        conn,
        doc_id=int(doc_id),
        content_type="markdown",
        content_text=text,
        source_name=source_name,
        metadata=metadata,
    )


def ensure_markdown_path_persisted(
    conn,
    doc_id: int,
    doc_key: str | None = None,
    preferred_path: str | Path | None = None,
) -> str | None:
    """Ensure markdown_path is persisted for a document when a markdown file exists.

    Returns the persisted path when successful, otherwise None.
    """
    try:
        existing = get_markdown_path_from_db(conn, doc_id)
        if existing and Path(existing).exists():
            return str(existing)

        candidate_path: Path | None = None
        if preferred_path is not None:
            candidate_path = Path(str(preferred_path))
            if not candidate_path.is_absolute():
                candidate_path = (PROJECT_ROOT / candidate_path).resolve()

        if candidate_path is None or not candidate_path.exists():
            deterministic = get_markdown_path(doc_id, doc_key)
            if deterministic.exists():
                candidate_path = deterministic

        if candidate_path is None or not candidate_path.exists():
            return None

        if save_markdown_path_to_db(conn, doc_id, candidate_path):
            return str(candidate_path)
    except Exception as e:
        LOGGER.warning("Failed to ensure markdown path persistence for doc_id %s: %s", doc_id, e)

    return None


def get_markdown_path_from_db(conn, doc_id: int) -> str | None:
    """
    Retrieve stored markdown file path from database.
    
    Args:
        conn: Database connection
        doc_id: Document ID
    
    Returns:
        Markdown path if stored, None otherwise
    """
    try:
        with conn.cursor() as cur:
            context = get_security_context(required=False)
            tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
            cur.execute(
                """
                SELECT COALESCE(markdown_path, metadata->>'markdown_path')
                FROM document
                WHERE doc_id = %s AND tenant_id = %s::uuid
                """,
                (doc_id, tenant_id),
            )
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
    except Exception as e:
        LOGGER.debug("Failed to retrieve markdown path for doc_id %s: %s", doc_id, e)
    
    return None


def get_markdown_for_document(conn, doc_id: int) -> str | None:
    """
    Retrieve markdown content for a document.
    
    Tries:
    1. The latest active Markdown version in document_content
    2. Legacy stored path in document metadata
    3. Legacy deterministic file path
    
    Args:
        conn: Database connection
        doc_id: Document ID
    
    Returns:
        Markdown file content if found, None otherwise
    """
    try:
        content = object_db.get_document_content(conn, doc_id=int(doc_id), content_type="markdown")
        if content and content.get("content_text") is not None:
            return str(content["content_text"])
    except Exception as e:
        LOGGER.warning("Failed to retrieve database Markdown for doc_id %s: %s", doc_id, e)

    # Legacy compatibility for documents ingested before document_content.
    stored_path = get_markdown_path_from_db(conn, doc_id)
    if stored_path:
        md_file = Path(stored_path)
        if md_file.exists():
            try:
                return md_file.read_text(encoding="utf-8")
            except Exception as e:
                LOGGER.warning("Failed to read markdown from stored path %s: %s", stored_path, e)
    
    # Fallback: get doc_key and try default path
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT doc_key FROM document WHERE doc_id = %s AND tenant_id = %s::uuid",
                (
                    doc_id,
                    str(get_security_context(required=False).tenant_id)
                    if get_security_context(required=False) is not None
                    else "00000000-0000-0000-0000-000000000001",
                ),
            )
            row = cur.fetchone()
            doc_key = str(row[0]) if row and row[0] else None
    except Exception as e:
        LOGGER.debug("Failed to fetch doc_key for doc_id %s: %s", doc_id, e)
        doc_key = None
    
    # Try default path
    default_path = get_markdown_path(doc_id, doc_key)
    if default_path.exists():
        try:
            return default_path.read_text(encoding="utf-8")
        except Exception as e:
            LOGGER.warning("Failed to read markdown from default path %s: %s", default_path, e)
    
    return None


def backfill_markdown_content_from_legacy_file(conn, doc_id: int) -> int | None:
    """Import one legacy path-backed Markdown artifact into document_content."""
    if object_db.get_document_content(conn, doc_id=int(doc_id), content_type="markdown"):
        return None
    stored_path = get_markdown_path_from_db(conn, int(doc_id))
    if not stored_path:
        return None
    path = Path(stored_path)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    if not path.is_file():
        return None
    try:
        markdown = path.read_text(encoding="utf-8")
        if not markdown.strip():
            return None
        return save_markdown_content_to_db(
            conn,
            int(doc_id),
            markdown,
            source_name=path.name,
            metadata={"legacy_path": str(path), "backfilled": True},
        )
    except Exception:
        LOGGER.exception("Failed to backfill legacy Markdown content doc_id=%s", doc_id)
        return None


def backfill_legacy_markdown_content(conn, limit: int = 500) -> dict[str, int]:
    """Import path-backed Markdown for the active tenant without deleting legacy files."""
    context = get_security_context(required=False)
    tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT d.doc_id
            FROM document d
            WHERE d.tenant_id = %s::uuid
              AND COALESCE(d.markdown_path, d.metadata->>'markdown_path') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM document_content c
                  WHERE c.tenant_id = d.tenant_id AND c.doc_id = d.doc_id
                    AND c.content_type = 'markdown' AND c.status = 'active'
              )
            ORDER BY d.doc_id
            LIMIT %s
            """,
            (tenant_id, max(1, min(int(limit), 5000))),
        )
        doc_ids = [int(row[0]) for row in cursor.fetchall()]

    migrated = 0
    skipped = 0
    for doc_id in doc_ids:
        if backfill_markdown_content_from_legacy_file(conn, doc_id) is None:
            skipped += 1
        else:
            migrated += 1
    return {"scanned": len(doc_ids), "migrated": migrated, "skipped": skipped}


def resolve_markdown_location(
    doc_id: int,
    doc_key: str | None,
    source_path: str | Path | None = None,
) -> Path | None:
    """
    Resolve where markdown should be stored.
    
    Priority:
    1. If source_path provided, use that
    2. Otherwise compute default path from doc_key or doc_id
    
    Args:
        doc_id: Document ID
        doc_key: Document key
        source_path: Optional explicit path
    
    Returns:
        Target markdown path, or None if cannot determine
    """
    if source_path:
        return Path(source_path)
    
    if not doc_id:
        return None
    
    ensure_markdown_dir()
    return get_markdown_path(doc_id, doc_key)


def list_all_markdown_files() -> list[dict[str, Any]]:
    """
    List all markdown files in the markdown directory.
    
    Returns:
        List of dicts with file info: {path, size_bytes, created_time}
    """
    ensure_markdown_dir()
    files = []
    
    for md_file in sorted(MARKDOWN_DIR.glob("document-*.md")):
        stat = md_file.stat()
        files.append({
            "filename": md_file.name,
            "path": str(md_file),
            "size_bytes": stat.st_size,
            "created_time": stat.st_mtime,
        })
    
    return files


def cleanup_orphaned_markdown() -> int:
    """
    Remove markdown files not referenced in database.
    
    Returns:
        Number of files deleted
    """
    from sql_db import get_connection
    
    ensure_markdown_dir()
    deleted = 0
    
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Get all referenced markdown paths
            cur.execute(
                """
                SELECT DISTINCT COALESCE(markdown_path, metadata->>'markdown_path')
                FROM document
                WHERE COALESCE(markdown_path, metadata->>'markdown_path') IS NOT NULL
                """
            )
            stored_paths = {str(row[0]).strip() for row in cur.fetchall() if row[0]}
        conn.close()
    except Exception as e:
        LOGGER.error("Failed to retrieve stored markdown paths: %s", e)
        return 0
    
    # Check local files
    for md_file in MARKDOWN_DIR.glob("document-*.md"):
        file_path = str(md_file)
        if file_path not in stored_paths:
            try:
                md_file.unlink()
                deleted += 1
                LOGGER.info("Deleted orphaned markdown: %s", md_file.name)
            except Exception as e:
                LOGGER.warning("Failed to delete %s: %s", md_file, e)
    
    return deleted
