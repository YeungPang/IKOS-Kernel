"""Note search and lookup functionality for IDMS.

Provides endpoints to search and retrieve notes by date range, tags/themes, and other metadata.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import FieldCondition, Filter, MatchValue
except Exception:  # pragma: no cover - optional dependency
    QdrantClient = None
    FieldCondition = None
    Filter = None
    MatchValue = None

import object_db
from ikos_config import QDRANT_API_KEY, QDRANT_COLLECTION, QDRANT_QUERY_API_KEY, QDRANT_URL


from ikos_api_server.deps import require_permission

router = APIRouter(prefix="/api/notes", tags=["notes"], dependencies=[Depends(require_permission("documents.read"))])
LOGGER = logging.getLogger("idms.api")
_QDRANT_CLIENT: Any | None = None


class NoteSearchRequest:
    """Request model for searching notes."""
    def __init__(
        self,
        tags: list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        search_text: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ):
        self.tags = tags or []
        self.start_date = start_date
        self.end_date = end_date
        self.search_text = search_text
        self.limit = min(limit, 100)  # Cap at 100
        self.offset = max(offset, 0)


def _get_qdrant_client() -> Any | None:
    global _QDRANT_CLIENT

    if QdrantClient is None or Filter is None or FieldCondition is None or MatchValue is None:
        return None

    if _QDRANT_CLIENT is None:
        _QDRANT_CLIENT = QdrantClient(
            url=QDRANT_URL,
            api_key=QDRANT_QUERY_API_KEY or QDRANT_API_KEY or None,
        )
    return _QDRANT_CLIENT


def _merge_chunk_texts(chunks: list[str]) -> str:
    if not chunks:
        return ""

    merged = chunks[0]
    for chunk in chunks[1:]:
        overlap = 0
        max_overlap = min(len(merged), len(chunk), 400)
        for size in range(max_overlap, 0, -1):
            if merged[-size:] == chunk[:size]:
                overlap = size
                break
        merged += chunk[overlap:]
    return merged.strip()


def _extract_note_body(raw_text: str) -> str:
    normalized = str(raw_text or "").replace("\r\n", "\n").strip()
    if not normalized:
        return ""

    if "\n---\n" in normalized:
        _, body = normalized.split("\n---\n", 1)
        return body.strip()

    return normalized


def _load_note_content(doc_id: int) -> str | None:
    client = _get_qdrant_client()
    if client is None:
        return None

    try:
        points, _ = client.scroll(
            collection_name=QDRANT_COLLECTION,
            scroll_filter=Filter(
                must=[
                    FieldCondition(
                        key="doc_id",
                        match=MatchValue(value=doc_id),
                    )
                ]
            ),
            with_payload=True,
            with_vectors=False,
            limit=256,
        )
    except Exception:
        LOGGER.warning("Failed to load note content from Qdrant for doc_id=%s", doc_id, exc_info=True)
        return None

    ordered_chunks: list[tuple[int, str]] = []
    for point in points or []:
        payload = getattr(point, "payload", None) or {}
        chunk_text = str(payload.get("chunk_text") or "").strip()
        if not chunk_text:
            continue
        try:
            chunk_index = int(payload.get("chunk_index") or 0)
        except Exception:
            chunk_index = 0
        ordered_chunks.append((chunk_index, chunk_text))

    if not ordered_chunks:
        return None

    ordered_chunks.sort(key=lambda item: item[0])
    merged_text = _merge_chunk_texts([chunk for _, chunk in ordered_chunks])
    note_body = _extract_note_body(merged_text)
    return note_body or None


def _format_note_row(
    doc_id: int,
    doc_path: str | None,
    doc_name: str | None,
    status: str | None,
    metadata: Any,
    valid_from: Any = None,
) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        metadata = {}

    content = _load_note_content(doc_id)
    if not content:
        content = str(metadata.get("note_content") or "").strip() or None
    return {
        "id": doc_id,
        "title": metadata.get("note_title", doc_name or "Untitled"),
        "date": metadata.get("note_date", ""),
        "tags": metadata.get("note_tags", []),
        "source": metadata.get("note_source", ""),
        "doc_path": doc_path,
        "status": status,
        "created_date": str(valid_from) if valid_from else None,
        "content": content,
        "content_available": bool(content),
    }


def _matches_search_text(note: dict[str, Any], search_text: str) -> bool:
    needle = str(search_text or "").strip().lower()
    if not needle:
        return True

    haystacks = [
        str(note.get("title") or ""),
        str(note.get("doc_path") or ""),
        str(note.get("content") or ""),
    ]
    return any(needle in haystack.lower() for haystack in haystacks if haystack)


@router.get("/search")
def search_notes(
    tags: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    search_text: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Search for notes by date range, tags/themes, and text content.
    
    Query parameters:
    - tags: Comma-separated list of tags to filter by
    - start_date: Start date in YYYY-MM-DD format (inclusive)
    - end_date: End date in YYYY-MM-DD format (inclusive)
    - search_text: Text to search in note content
    - limit: Max results to return (default: 50, max: 100)
    - offset: Result offset for pagination (default: 0)
    
    Returns: List of notes matching the search criteria
    """
    try:
        # Parse tags
        tag_list = []
        if tags:
            tag_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
        
        # Parse dates
        search_start_date = None
        search_end_date = None
        
        if start_date:
            try:
                search_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid start_date format: {start_date}")
        
        if end_date:
            try:
                search_end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid end_date format: {end_date}")
        
        # Default to last 30 days if no date range specified
        if not search_start_date and not search_end_date:
            search_end_date = datetime.now().date()
            search_start_date = search_end_date - timedelta(days=30)
        elif search_start_date and not search_end_date:
            search_end_date = datetime.now().date()
        elif search_end_date and not search_start_date:
            search_start_date = search_end_date - timedelta(days=365)
        
        # Query database
        with object_db.get_connection() as connection:
            with connection.cursor() as cursor:
                # Build the query
                query = """
                          SELECT doc_id, doc_path, doc_name, status, metadata, 
                           valid_from, valid_until
                    FROM document
                    WHERE metadata IS NOT NULL
                    AND metadata->>'note_source' IS NOT NULL
                    AND CAST(metadata->>'note_date' AS DATE) >= %s
                    AND CAST(metadata->>'note_date' AS DATE) <= %s
                """
                params: list[Any] = [search_start_date, search_end_date]
                
                # Add tag filter if specified
                if tag_list:
                    # Filter documents that have ALL specified tags
                    for tag in tag_list:
                        query += "\nAND metadata->'note_tags' @> %s"
                        params.append(f'"{tag}"')

                # Order by date descending, then by doc_id
                query += "\nORDER BY CAST(metadata->>'note_date' AS DATE) DESC, doc_id DESC"

                # Apply SQL pagination only when content filtering is not needed.
                if not search_text:
                    query += f"\nLIMIT {limit} OFFSET {offset}"
                
                LOGGER.debug(f"Executing note search query with params: {params}")
                cursor.execute(query, params)
                rows = cursor.fetchall()

        notes = [
            _format_note_row(doc_id, doc_path, doc_name, status, metadata, valid_from)
            for doc_id, doc_path, doc_name, status, metadata, valid_from, valid_until in rows
        ]

        if search_text:
            notes = [note for note in notes if _matches_search_text(note, search_text)]
            notes = notes[offset:offset + limit]
        
        return {
            "success": True,
            "total": len(notes),
            "notes": notes,
            "query": {
                "tags": tag_list,
                "start_date": str(search_start_date),
                "end_date": str(search_end_date),
                "search_text": search_text,
                "limit": limit,
                "offset": offset,
            }
        }
    
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Note search failed")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(exc)}") from exc


@router.get("/by-date/{date_str}")
def get_notes_by_date(date_str: str) -> dict[str, Any]:
    """Get all notes for a specific date.
    
    Path parameter:
    - date_str: Date in YYYY-MM-DD format
    
    Returns: List of notes for the specified date
    """
    try:
        note_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date_str}")
    
    try:
        with object_db.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT doc_id, doc_path, doc_name, metadata, status, valid_from
                    FROM document
                    WHERE metadata IS NOT NULL
                    AND metadata->>'note_source' IS NOT NULL
                    AND CAST(metadata->>'note_date' AS DATE) = %s
                    ORDER BY metadata->>'note_title'
                    """,
                    (note_date,)
                )
                rows = cursor.fetchall()

        notes = []
        for doc_id, doc_path, doc_name, metadata, status, valid_from in rows:
            note = _format_note_row(doc_id, doc_path, doc_name, status, metadata, valid_from)
            if not note.get("date"):
                note["date"] = date_str
            notes.append(note)
        
        return {
            "success": True,
            "date": date_str,
            "total": len(notes),
            "notes": notes,
        }
    
    except Exception as exc:
        LOGGER.exception(f"Failed to get notes for date {date_str}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/by-tag/{tag}")
def get_notes_by_tag(tag: str, limit: int = 50, offset: int = 0) -> dict[str, Any]:
    """Get all notes with a specific tag/theme.
    
    Path parameter:
    - tag: Tag name to search for
    
    Query parameters:
    - limit: Max results to return (default: 50, max: 100)
    - offset: Result offset for pagination (default: 0)
    
    Returns: List of notes with the specified tag
    """
    try:
        tag = tag.strip()
        if not tag:
            raise HTTPException(status_code=400, detail="Tag cannot be empty")
        
        limit = min(limit, 100)
        
        with object_db.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT doc_id, doc_path, doc_name, metadata, valid_from, status
                    FROM document
                    WHERE metadata IS NOT NULL
                    AND metadata->'note_tags' @> %s
                    AND metadata->>'note_source' IS NOT NULL
                    ORDER BY CAST(metadata->>'note_date' AS DATE) DESC, doc_id DESC
                    LIMIT %s OFFSET %s
                    """,
                    (f'"{tag}"', limit, offset)
                )
                rows = cursor.fetchall()
        
        notes = []
        for doc_id, doc_path, doc_name, metadata, valid_from, status in rows:
            notes.append(_format_note_row(doc_id, doc_path, doc_name, status, metadata, valid_from))
        
        return {
            "success": True,
            "tag": tag,
            "total": len(notes),
            "notes": notes,
            "limit": limit,
            "offset": offset,
        }
    
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception(f"Failed to get notes for tag {tag}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
