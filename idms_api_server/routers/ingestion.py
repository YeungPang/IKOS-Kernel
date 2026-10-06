from __future__ import annotations

import datetime
import hashlib
import logging
import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
import psycopg2

import ingest
import object_db
import markdown_manager
from ikos_api_server.deps import normalize_tags, parse_json_dict
from ikos_api_server.schemas import (
    IngestPathRequest,
    ReprocessRequest,
    NoteIngestRequest,
    RepairDocumentsRequest,
    TableProvenanceBackfillRequest,
)


from ikos_api_server.deps import require_permission

router = APIRouter(prefix="/api/ingest", tags=["ingestion"], dependencies=[Depends(require_permission("documents.write"))])
LOGGER = logging.getLogger("idms.api")


def _is_truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _require_maintenance_token(admin_token: str = "") -> None:
    required_token = str(os.getenv("IDMS_MAINTENANCE_TOKEN", "")).strip()
    if required_token and str(admin_token or "") != required_token:
        raise HTTPException(status_code=403, detail="Invalid maintenance token")


@router.get("/system-rules")
def list_ingestion_system_rules() -> dict[str, Any]:
    try:
        rules = ingest.list_system_ingestion_processing_rules()
        return {
            "success": True,
            "count": len(rules),
            "result": rules,
        }
    except Exception as exc:
        LOGGER.exception("Failed to list ingestion system rules")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/markdown-policy")
def get_ingestion_markdown_policy() -> dict[str, Any]:
    """Return current markdown parser policy and effective parser selection."""
    markdown_rule = os.getenv("IDMS_INGESTION_MARKDOWN_RULE", "force_pparser").strip().lower()
    force_pparser_env = _is_truthy(os.getenv("IDMS_FORCE_PPARSER", ""))
    force_by_rule = markdown_rule in {"force_pparser", "pparser_only", "complex_tables"}
    force_pparser = force_by_rule or force_pparser_env

    return {
        "success": True,
        "policy": {
            "ingestion_markdown_rule": markdown_rule,
            "force_pparser_env": force_pparser_env,
            "force_pparser": force_pparser,
            "effective_parser": "pparser" if force_pparser else "mparser",
            "allowed_values_hint": [
                "force_pparser",
                "pparser_only",
                "complex_tables",
                "mparser",
            ],
        },
    }


def _stable_note_identity(title: str, note_date: str, source: str, tags: list[str], content: str) -> str:
    normalized_tags = [str(tag).strip().lower() for tag in tags if str(tag).strip()]
    normalized_tags.sort()
    canonical_payload = "\n".join(
        [
            str(title or "").strip(),
            str(note_date or "").strip(),
            str(source or "").strip().lower(),
            "|".join(normalized_tags),
            str(content or "").strip(),
        ]
    )
    digest = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
    return f"note:{digest}"


def _is_quota_exhausted_error(exc: Exception) -> bool:
    text = str(exc or "").upper()
    return "RESOURCE_EXHAUSTED" in text or ("429" in text and "GOOGLE" in text)


def _raise_ingest_http_error(exc: Exception) -> None:
    error_text = str(exc or "")
    error_text_upper = error_text.upper()

    if _is_quota_exhausted_error(exc):
        raise HTTPException(
            status_code=429,
            detail="AI quota exhausted (Google Gemini RESOURCE_EXHAUSTED). Please retry later or increase quota.",
        ) from exc

    if isinstance(exc, psycopg2.OperationalError):
        raise HTTPException(
            status_code=503,
            detail=(
                "Database connection failed. Check IDMS_DB_HOST, IDMS_DB_PORT, IDMS_DB_NAME, "
                "IDMS_DB_USER, and IDMS_DB_PASSWORD in your .env."
            ),
        ) from exc

    if "OPENROUTER_API_KEY is required" in error_text or "OPENROUTER_API_KEY is not configured" in error_text:
        raise HTTPException(
            status_code=503,
            detail="OPENROUTER_API_KEY is missing. Set it in IDMS-Demo/.env and restart the API.",
        ) from exc

    if (
        "LLAMAPARSE" in error_text_upper
        or "LLAMA CLOUD" in error_text_upper
        or "MD_GEN MPARSER FAILED" in error_text_upper
        or "MD_GEN PPARSER FAILED" in error_text_upper
    ):
        raise HTTPException(
            status_code=503,
            detail=(
                "Document parsing service is temporarily unavailable (LlamaParse). "
                "Please retry in a moment."
            ),
        ) from exc

    raise HTTPException(status_code=500, detail=error_text) from exc


def _build_note_text(title: str, note_date: str, tags: list[str], source: str, content: str) -> str:
    return f"""Title: {title}
Date: {note_date}
Tags: {', '.join(tags) if tags else 'none'}
Source: {source}

---

{content}
"""


def _coerce_int_list(value: Any) -> list[int]:
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out: list[int] = []
    seen: set[int] = set()
    for item in items:
        try:
            numeric = int(item)
        except (TypeError, ValueError):
            continue
        if numeric <= 0 or numeric in seen:
            continue
        seen.add(numeric)
        out.append(numeric)
    return out


def _collect_clarification_target_run_ids(
    connection: Any,
    metadata: dict[str, Any],
    title: str = "",
    content: str = "",
    tags: list[str] | None = None,
) -> list[int]:
    """Resolve workflow run IDs from note metadata for clarification linking.

    Supported metadata keys:
      - workflow_run_id / workflow_run_ids
      - clarification_for_run_id / clarification_for_run_ids
      - workflow_key / clarification_for_workflow_key
    """
    direct_ids: list[int] = []
    for key in (
        "workflow_run_id",
        "workflow_run_ids",
        "clarification_for_run_id",
        "clarification_for_run_ids",
    ):
        direct_ids.extend(_coerce_int_list(metadata.get(key)))

    workflow_keys: list[str] = []
    for key in ("workflow_key", "clarification_for_workflow_key"):
        value = metadata.get(key)
        if isinstance(value, list):
            for item in value:
                text = str(item or "").strip()
                if text:
                    workflow_keys.append(text)
        else:
            text = str(value or "").strip()
            if text:
                workflow_keys.append(text)

    def _infer_workflow_keys_from_note() -> list[str]:
        text = " ".join(
            [
                str(title or "").strip().lower(),
                str(content or "").strip().lower(),
                " ".join(str(tag).strip().lower() for tag in (tags or []) if str(tag).strip()),
                " ".join(str(item).strip().lower() for item in (metadata.get("workflow_processes") or []) if str(item).strip()),
            ]
        )
        if not text.strip():
            return []

        inferred: list[str] = []

        def add(key: str) -> None:
            if key not in inferred:
                inferred.append(key)

        if re.search(r"\baccounting\b|\breconciliation\b|\bbank statement\b|\bledger\b", text):
            add("template::accounting_reconciliation_full")
            add("template::llm_accounting_assist")
        if re.search(r"\bcrm\b|\blead\b|\bopportunity\b", text):
            add("template::crm_lead_pipeline")
            add("template::crm_case_management")
        if re.search(r"\berp\b|\bprocure\b|\bp2p\b|\border to cash\b|\botc\b", text):
            add("template::erp_procure_to_pay")
            add("template::erp_order_to_cash")
        if re.search(r"\bproject\b|\bmilestone\b|\bchange request\b|\bgovernance\b", text):
            add("template::project_delivery_governance")
            add("template::project_change_control")
        if re.search(r"\bclarification\b|\bexplanation\b", text):
            add("template::generic_clarification_gate")

        return inferred

    if not workflow_keys:
        workflow_keys.extend(_infer_workflow_keys_from_note())

    resolved_ids: list[int] = []
    seen: set[int] = set()
    for run_id in direct_ids:
        if object_db.get_pipeline_run(connection, run_id=run_id) is None:
            continue
        if run_id in seen:
            continue
        seen.add(run_id)
        resolved_ids.append(run_id)

    for workflow_key in workflow_keys:
        for status in ("paused", "running", "pending"):
            runs = object_db.list_pipeline_runs(
                connection,
                run_status=status,
                workflow_key=workflow_key,
                limit=200,
            )
            # Prefer the most recent matching run for safer implicit linking.
            if runs:
                run_id = int((runs[0] or {}).get("run_id") or 0)
                if run_id > 0 and run_id not in seen:
                    seen.add(run_id)
                    resolved_ids.append(run_id)

    # Last-resort fallback: if exactly one paused run exists globally, use it.
    if not resolved_ids:
        paused_runs = object_db.list_pipeline_runs(
            connection,
            run_status="paused",
            workflow_key=None,
            limit=5,
        )
        if len(paused_runs) == 1:
            run_id = int((paused_runs[0] or {}).get("run_id") or 0)
            if run_id > 0 and run_id not in seen:
                resolved_ids.append(run_id)

    return resolved_ids


def _persist_note_document(
    *,
    title: str,
    content: str,
    note_date: str,
    tags: list[str],
    source: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    note_uri = f"note://{metadata['stable_source_identity']}"
    note_text = _build_note_text(title=title, note_date=note_date, tags=tags, source=source, content=content)
    user_metadata = {
        "client_file_name": f"{title}.txt",
        "client_file_path": note_uri,
        "stable_source_identity": metadata["stable_source_identity"],
        "source_reference": {
            "source_path_or_uri": note_uri,
            "gcs_uri": note_uri,
            "stable_source_identity": metadata["stable_source_identity"],
        },
    }
    metadata_for_document = dict(metadata)
    metadata_for_document["note_content"] = content
    metadata_for_document["user_description"] = content
    metadata_for_document["user_tags"] = tags
    metadata_for_document["user_metadata"] = user_metadata

    document_payload = {
        "doc_key": metadata["stable_source_identity"],
        "doc_cat": "note",
        "doc_type": "document",
        "doc_date": note_date,
        "doc_theme": ", ".join(tags) if tags else "note",
        "keywords": [title, *tags],
        "identifiers": {},
        "metadata": metadata_for_document,
        "status": "active",
        "valid_from": note_date,
    }

    link_results: list[dict[str, Any]] = []

    connection = object_db.get_connection()
    try:
        object_db.create_tables(connection, recreate=False)
        doc_id = ingest.insert_document_record(
            connection=connection,
            gcs_uri=note_uri,
            document=document_payload,
            document_effective_date=note_date,
            document_recorded_date=note_date,
        )

        target_run_ids = _collect_clarification_target_run_ids(
            connection,
            metadata_for_document,
            title=title,
            content=content,
            tags=tags,
        )
        for run_id in target_run_ids:
            inserted = object_db.link_pipeline_run_documents(
                connection,
                run_id=run_id,
                doc_refs=[doc_id],
                source="system",
                step_key="clarification_note",
                linked_by=str(source or "").strip() or None,
                metadata={
                    "link_reason": "clarification_note",
                    "note_title": title,
                    "note_source": source,
                    "metadata_keys": sorted(list(metadata_for_document.keys())),
                },
            )
            link_results.append(
                {
                    "run_id": int(run_id),
                    "linked": bool(inserted > 0),
                    "inserted": int(inserted),
                }
            )

        connection.commit()
    finally:
        connection.close()

    qdrant_result: dict[str, Any] = {"indexed": False, "skipped": True, "reason": "qdrant_disabled_or_unavailable"}
    if ingest.ENABLE_QDRANT_INDEX and ingest.QdrantClient is not None:
        try:
            client = ingest.get_openrouter_client()
            if client is None:
                raise RuntimeError("OPENROUTER_API_KEY is required for note embedding generation")
            chunks = ingest.chunk_markdown(note_text, ingest.QDRANT_CHUNK_SIZE, ingest.QDRANT_CHUNK_OVERLAP)
            embeddings = ingest.embed_text_chunks(client, chunks)
            qdrant = ingest.QdrantClient(
                host=ingest.QDRANT_HOST,
                port=ingest.QDRANT_PORT,
                api_key=ingest.QDRANT_API_KEY or None,
            )
            qdrant_result = ingest.index_chunks_in_qdrant(
                qdrant=qdrant,
                chunks=chunks,
                embeddings=embeddings,
                doc_id=doc_id,
                doc_key=metadata["stable_source_identity"],
                doc_type="document",
                gcs_uri=note_uri,
            )
        except Exception as exc:
            LOGGER.warning("Direct note Qdrant indexing skipped: %s", exc)
            qdrant_result = {"indexed": False, "skipped": True, "reason": str(exc)}

    return {
        "run_id": datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8],
        "source": note_uri,
        "markdown": note_text,
        "db_summary": {
            "doc_id": doc_id,
            "objects_upserted": 0,
            "relationships_upserted": 0,
            "ambiguities_queued": 0,
        },
        "workflow_clarification_links": {
            "count": len(link_results),
            "links": link_results,
        },
        "qdrant_index": qdrant_result,
        "indexing_strategy": {
            "qdrant_enabled_for_doc": bool(ingest.ENABLE_QDRANT_INDEX and ingest.QdrantClient is not None),
            "selected_index_backend": "qdrant" if qdrant_result.get("indexed") else "document_only",
            "used_fallback": not qdrant_result.get("indexed"),
        },
        "solf_objects": {
            "document": document_payload,
            "document_effective_date": note_date,
            "document_recorded_date": note_date,
            "solf_entities": [],
            "solf_relationships": [],
        },
    }


@router.post("/path")
def ingest_from_path(payload: IngestPathRequest) -> dict[str, Any]:
    metadata = dict(payload.metadata or {})
    if payload.file_name:
        metadata["client_file_name"] = payload.file_name
    if payload.processing_rule_names:
        metadata["processing_rule_names"] = [
            str(name).strip()
            for name in payload.processing_rule_names
            if str(name).strip()
        ]

    user_context = {
        "description": payload.note,
        "tags": payload.tags,
        "metadata": metadata,
    }
    LOGGER.info(
        "ingest_api_input endpoint=path source=%s note_len=%s tags_count=%s metadata_keys=%s",
        payload.source_path,
        len(str(payload.note or "")),
        len(payload.tags or []),
        sorted(str(key) for key in metadata.keys()),
    )

    try:
        result = ingest.run_ingest(
            source_path_or_uri=payload.source_path,
            user_context=user_context,
            prefer_markdown_input=payload.prefer_markdown_input,
        )
    except Exception as exc:
        LOGGER.exception("Ingest from path failed")
        _raise_ingest_http_error(exc)

    return {
        "success": True,
        "run_id": result.get("run_id"),
        "result": result,
    }


@router.post("/upload")
async def ingest_from_upload(
    file: UploadFile = File(...),
    source_path: str = Form(default=""),
    note: str = Form(default=""),
    processing_rule_names: str = Form(default=""),
    tags: str = Form(default=""),
    metadata_json: str = Form(default=""),
) -> dict[str, Any]:
    metadata = parse_json_dict(metadata_json, "metadata_json")
    metadata["client_file_name"] = file.filename
    if source_path:
        metadata["client_file_path"] = source_path
        metadata["stable_source_identity"] = source_path
        source_reference = metadata.get("source_reference") if isinstance(metadata.get("source_reference"), dict) else {}
        source_reference = dict(source_reference)
        source_reference["source_path_or_uri"] = source_path
        source_reference["stable_source_identity"] = source_path
        metadata["source_reference"] = source_reference
    parsed_rule_names = normalize_tags(processing_rule_names)
    if parsed_rule_names:
        metadata["processing_rule_names"] = parsed_rule_names

    user_context = {
        "description": note,
        "tags": normalize_tags(tags),
        "metadata": metadata,
    }
    LOGGER.info(
        "ingest_api_input endpoint=upload file_name=%s source_path=%s note_len=%s tags_count=%s metadata_keys=%s",
        file.filename,
        source_path,
        len(str(note or "")),
        len(user_context["tags"]),
        sorted(str(key) for key in metadata.keys()),
    )

    suffix = Path(file.filename or "upload.bin").suffix
    temp_fd, temp_name = tempfile.mkstemp(prefix="idms-upload-", suffix=suffix)
    os.close(temp_fd)
    temp_path = Path(temp_name)
    try:
        content = await file.read()
        temp_path.write_bytes(content)
        result = ingest.run_ingest(
            source_path_or_uri=str(temp_path),
            user_context=user_context,
            prefer_markdown_input=False,
        )
    except Exception as exc:
        LOGGER.exception("Ingest from upload failed")
        _raise_ingest_http_error(exc)
    finally:
        await file.close()
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            LOGGER.warning("Failed to delete temp upload file: %s", temp_path)

    return {
        "success": True,
        "run_id": result.get("run_id"),
        "uploaded_file_name": file.filename,
        "result": result,
    }


def _retrieve_ingest_parameters(doc_id: int) -> dict[str, Any]:
    """Retrieve original ingestion parameters from document metadata."""
    try:
        with object_db.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT doc_path, metadata, markdown_path FROM document WHERE doc_id = %s",
                    (doc_id,)
                )
                row = cursor.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch document: {str(exc)}") from exc
    
    if not row:
        raise HTTPException(status_code=404, detail=f"Document with doc_id={doc_id} not found")
    
    gcs_uri, metadata, markdown_path = row
    if not isinstance(metadata, dict):
        metadata = {}
    
    # Extract parameters from metadata
    user_metadata = metadata.get("user_metadata", {})
    if not isinstance(user_metadata, dict):
        user_metadata = {}

    # Preserve the original user-supplied metadata for replayable ingests,
    # especially note_* fields and stable_source_identity for note deduping.
    replay_metadata = dict(user_metadata)
    replay_metadata.update(
        {
            key: value
            for key, value in metadata.items()
            if key not in {"user_metadata", "user_description", "user_tags", "description"}
        }
    )
    
    return {
        "gcs_uri": gcs_uri,
        "markdown_path": str(markdown_path or "").strip(),
        "file_name": user_metadata.get("client_file_name", ""),
        "file_path": user_metadata.get("client_file_path", ""),
        "note": metadata.get("user_description", ""),
        "tags": metadata.get("user_tags", []),
        "metadata": replay_metadata,
        "source_reference": user_metadata.get("source_reference", {}),
        "doc_key": metadata.get("doc_key", ""),
    }


def _get_document_info(doc_id: int, conn) -> dict[str, Any] | None:
    """Get basic document info including doc_key and metadata."""
    try:
        from security_context import get_security_context
        context = get_security_context(required=False)
        tenant_id = str(context.tenant_id) if context is not None else "00000000-0000-0000-0000-000000000001"
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT doc_id, doc_key, doc_type, metadata FROM document WHERE doc_id = %s AND tenant_id = %s::uuid",
                (doc_id, tenant_id)
            )
            row = cursor.fetchone()
            if row:
                return {
                    "doc_id": row[0],
                    "doc_key": row[1],
                    "doc_type": row[2],
                    "metadata": row[3] if isinstance(row[3], dict) else {},
                }
    except Exception as e:
        LOGGER.error("Failed to fetch document info: %s", e)
    
    return None


def _prepare_reprocess_execution(doc_id: int) -> dict[str, Any]:
    """Prepare source and user context for reprocess/repair runs."""
    original_params = _retrieve_ingest_parameters(doc_id)

    metadata = dict(original_params.get("metadata", {}))
    metadata["reprocess_doc_id"] = int(doc_id)
    metadata["allow_existing_doc_markdown_reuse"] = True
    if original_params.get("file_name"):
        metadata["client_file_name"] = original_params["file_name"]
    if original_params.get("file_path"):
        metadata["client_file_path"] = original_params["file_path"]

    user_context = {
        "description": original_params.get("note", ""),
        "tags": original_params.get("tags", []),
        "metadata": metadata,
    }

    source_path_or_uri = str(original_params.get("gcs_uri") or "").strip()
    source_reference = (
        original_params.get("source_reference")
        if isinstance(original_params.get("source_reference"), dict)
        else {}
    )
    markdown_cache_path = str(source_reference.get("markdown_cache_path") or "").strip()

    source_available = bool(source_path_or_uri)
    if source_path_or_uri and not source_path_or_uri.lower().startswith("gs://"):
        source_available = Path(source_path_or_uri).exists()

    doc_key = original_params.get("doc_key", "")
    existing_markdown = None
    if not source_available:
        stored_md_path = str(original_params.get("markdown_path") or metadata.get("markdown_path") or "").strip()
        if stored_md_path and Path(stored_md_path).exists():
            existing_markdown = Path(stored_md_path)
            LOGGER.info(
                "reprocess_markdown_located_stored doc_id=%s doc_key=%s path=%s",
                doc_id,
                doc_key,
                existing_markdown,
            )
        else:
            md_path = markdown_manager.get_markdown_path(doc_id, doc_key)
            if md_path.exists():
                existing_markdown = md_path
                LOGGER.info(
                    "reprocess_markdown_located doc_id=%s doc_key=%s path=%s",
                    doc_id,
                    doc_key,
                    md_path,
                )
            elif markdown_cache_path and Path(markdown_cache_path).exists():
                existing_markdown = Path(markdown_cache_path)
                LOGGER.info(
                    "reprocess_markdown_fallback_to_cache doc_id=%s path=%s",
                    doc_id,
                    markdown_cache_path,
                )

    if not source_available and existing_markdown:
        LOGGER.info(
            "reprocess_source_fallback doc_id=%s source_missing=%s using_existing_markdown=%s",
            doc_id,
            source_path_or_uri,
            existing_markdown,
        )
        source_path_or_uri = str(existing_markdown)

    return {
        "doc_id": int(doc_id),
        "original_params": original_params,
        "user_context": user_context,
        "source_path_or_uri": source_path_or_uri,
        "existing_markdown": existing_markdown,
    }


def _select_repair_doc_ids(payload: RepairDocumentsRequest) -> list[int]:
    explicit = [int(doc_id) for doc_id in payload.doc_ids if int(doc_id) > 0]
    if explicit:
        return sorted(set(explicit))

    with object_db.get_connection() as connection:
        with connection.cursor() as cursor:
            if payload.mode == "markdown_only":
                if payload.only_missing:
                    cursor.execute(
                        """
                        SELECT doc_id
                        FROM document
                        WHERE COALESCE(markdown_path, '') = ''
                        ORDER BY doc_id DESC
                        LIMIT %s
                        """,
                        (int(payload.limit),),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT doc_id
                        FROM document
                        ORDER BY doc_id DESC
                        LIMIT %s
                        """,
                        (int(payload.limit),),
                    )
            else:
                if payload.only_missing:
                    cursor.execute(
                        """
                        SELECT doc_id
                        FROM document
                        WHERE COALESCE(doc_cat, '') = ''
                           OR COALESCE(doc_theme, '') = ''
                           OR COALESCE(keyword_text, '') = ''
                        ORDER BY doc_id DESC
                        LIMIT %s
                        """,
                        (int(payload.limit),),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT doc_id
                        FROM document
                        ORDER BY doc_id DESC
                        LIMIT %s
                        """,
                        (int(payload.limit),),
                    )

            rows = cursor.fetchall() or []
    return [int(row[0]) for row in rows if row and int(row[0]) > 0]




@router.post("/reprocess")
def reprocess_document(payload: ReprocessRequest) -> dict[str, Any]:
    """Reprocess a previously ingested document with optional parameter overrides.
    
    Retrieves the original ingestion parameters from the document record and 
    re-runs the ingestion pipeline with selective overrides (e.g., prefer_markdown_input).
    
    This endpoint is especially useful for re-enriching Qdrant payloads with keywords/entities
    using existing markdown files without re-chunking or re-embedding.
    
    Priority for markdown location:
    1. Stored markdown_path column in database
    2. Default computed path based on doc_key/doc_id
    3. Fallback: document filesystem search
    """
    prepared = _prepare_reprocess_execution(payload.doc_id)
    original_params = prepared.get("original_params") if isinstance(prepared.get("original_params"), dict) else {}
    user_context = prepared.get("user_context") if isinstance(prepared.get("user_context"), dict) else {}
    source_path_or_uri = str(prepared.get("source_path_or_uri") or "").strip()
    existing_markdown = prepared.get("existing_markdown")
    metadata = user_context.get("metadata") if isinstance(user_context.get("metadata"), dict) else {}

    LOGGER.info(
        "ingest_api_input endpoint=reprocess doc_id=%s gcs_uri=%s note_len=%s tags_count=%s metadata_keys=%s",
        payload.doc_id,
        original_params.get("gcs_uri", ""),
        len(str(user_context["description"] or "")),
        len(user_context["tags"]),
        sorted(str(key) for key in metadata.keys()),
    )

    # Apply overrides
    prefer_markdown_input = payload.prefer_markdown_input if payload.prefer_markdown_input is not None else False
    if source_path_or_uri.lower().endswith(".md") and payload.prefer_markdown_input is None:
        prefer_markdown_input = True
    reuse_markdown = payload.reuse_markdown if payload.reuse_markdown is not None else True
    # Never persist markdown during reprocess to avoid creating fresh markdown artifacts
    # that can be accidentally re-ingested as separate documents.
    persist_markdown = False
    
    try:
        result = ingest.run_ingest(
            source_path_or_uri=source_path_or_uri,
            user_context=user_context,
            prefer_markdown_input=prefer_markdown_input,
            reuse_markdown=reuse_markdown,
            persist_markdown=persist_markdown,
        )
    except Exception as exc:
        LOGGER.exception("Reprocess document failed for doc_id=%s", payload.doc_id)
        _raise_ingest_http_error(exc)
    
    return {
        "success": True,
        "doc_id_source": payload.doc_id,
        "run_id": result.get("run_id"),
        "markdown_used": bool(existing_markdown or prefer_markdown_input),
        "result": result,
    }


@router.post("/maintenance/repair")
def repair_documents(payload: RepairDocumentsRequest) -> dict[str, Any]:
    """Repair documents with one of three modes: markdown_only, metadata_only, full_reextract."""
    markdown_persistence_enabled = bool(payload.allow_persist_markdown)
    if payload.mode == "markdown_only" and not markdown_persistence_enabled:
        raise HTTPException(
            status_code=400,
            detail=(
                "mode=markdown_only requires allow_persist_markdown=true "
                "(admin-only operation)."
            ),
        )
    if markdown_persistence_enabled:
        _require_maintenance_token(payload.admin_token)

    target_doc_ids = _select_repair_doc_ids(payload)
    if not target_doc_ids:
        return {
            "success": True,
            "mode": payload.mode,
            "markdown_persistence_enabled": markdown_persistence_enabled,
            "requested_doc_ids": payload.doc_ids,
            "target_doc_ids": [],
            "processed_count": 0,
            "success_count": 0,
            "failed_count": 0,
            "results": [],
        }

    results: list[dict[str, Any]] = []
    success_count = 0
    failed_count = 0

    for doc_id in target_doc_ids:
        try:
            if payload.mode == "markdown_only":
                with object_db.get_connection() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT doc_key, markdown_path FROM document WHERE doc_id = %s", (doc_id,))
                        row = cursor.fetchone()
                        if not row:
                            raise HTTPException(status_code=404, detail=f"Document with doc_id={doc_id} not found")
                        doc_key = str(row[0] or "").strip()
                        before_path = str(row[1] or "").strip()

                    persisted = markdown_manager.ensure_markdown_path_persisted(
                        connection,
                        int(doc_id),
                        doc_key,
                        before_path or None,
                    )

                    with connection.cursor() as cursor:
                        cursor.execute("SELECT markdown_path FROM document WHERE doc_id = %s", (doc_id,))
                        after_row = cursor.fetchone()
                        after_path = str(after_row[0] or "").strip() if after_row else ""

                results.append(
                    {
                        "doc_id": int(doc_id),
                        "status": "ok" if persisted else "skipped",
                        "mode": payload.mode,
                        "before_markdown_path": before_path or None,
                        "after_markdown_path": after_path or None,
                        "persisted": bool(persisted),
                    }
                )
                if persisted:
                    success_count += 1
                else:
                    failed_count += 1
                continue

            prepared = _prepare_reprocess_execution(int(doc_id))
            user_context = prepared.get("user_context") if isinstance(prepared.get("user_context"), dict) else {}
            source_path_or_uri = str(prepared.get("source_path_or_uri") or "").strip()

            if payload.mode == "metadata_only":
                prefer_markdown_input = True
                reuse_markdown = True
                persist_markdown = markdown_persistence_enabled
                metadata_only = True
            else:
                prefer_markdown_input = False
                reuse_markdown = False
                persist_markdown = markdown_persistence_enabled
                metadata_only = False

            if source_path_or_uri.lower().endswith(".md") and payload.mode == "full_reextract":
                # Best effort: if only markdown is available, still run complete refresh from markdown source.
                prefer_markdown_input = True

            result = ingest.run_ingest(
                source_path_or_uri=source_path_or_uri,
                user_context=user_context,
                prefer_markdown_input=prefer_markdown_input,
                reuse_markdown=reuse_markdown,
                persist_markdown=persist_markdown,
                metadata_only=metadata_only,
            )

            db_summary = result.get("db_summary") if isinstance(result.get("db_summary"), dict) else {}
            results.append(
                {
                    "doc_id": int(doc_id),
                    "status": "ok",
                    "mode": payload.mode,
                    "run_id": result.get("run_id"),
                    "doc_id_result": db_summary.get("doc_id"),
                    "document_persistence_action": db_summary.get("document_persistence_action"),
                    "metadata_only": bool(metadata_only),
                }
            )
            success_count += 1
        except HTTPException as exc:
            results.append(
                {
                    "doc_id": int(doc_id),
                    "status": "error",
                    "mode": payload.mode,
                    "error": str(exc.detail),
                }
            )
            failed_count += 1
        except Exception as exc:
            results.append(
                {
                    "doc_id": int(doc_id),
                    "status": "error",
                    "mode": payload.mode,
                    "error": str(exc),
                }
            )
            failed_count += 1

    return {
        "success": True,
        "mode": payload.mode,
        "markdown_persistence_enabled": markdown_persistence_enabled,
        "requested_doc_ids": [int(doc_id) for doc_id in payload.doc_ids],
        "target_doc_ids": target_doc_ids,
        "only_missing": bool(payload.only_missing),
        "limit": int(payload.limit),
        "processed_count": len(target_doc_ids),
        "success_count": success_count,
        "failed_count": failed_count,
        "results": results,
    }


@router.post("/maintenance/backfill-table-provenance")
def backfill_table_provenance(payload: TableProvenanceBackfillRequest) -> dict[str, Any]:
    """Backfill normalized table-cell provenance rows from persisted document metadata."""
    with object_db.get_connection() as connection:
        target_doc_ids = object_db.select_document_ids_for_table_provenance_backfill(
            connection,
            doc_ids=[int(item) for item in payload.doc_ids],
            only_missing=bool(payload.only_missing),
            limit=int(payload.limit),
        )

    if not target_doc_ids:
        return {
            "success": True,
            "requested_doc_ids": [int(doc_id) for doc_id in payload.doc_ids],
            "target_doc_ids": [],
            "only_missing": bool(payload.only_missing),
            "limit": int(payload.limit),
            "processed_count": 0,
            "success_count": 0,
            "failed_count": 0,
            "results": [],
        }

    success_count = 0
    failed_count = 0
    results: list[dict[str, Any]] = []

    for doc_id in target_doc_ids:
        try:
            with object_db.get_connection() as connection:
                row_count = int(object_db.backfill_document_table_cells_from_document_metadata(connection, int(doc_id)))
                sample_rows = object_db.list_document_table_cells(connection, int(doc_id), limit=1)

            results.append(
                {
                    "doc_id": int(doc_id),
                    "status": "ok",
                    "provenance_rows": row_count,
                    "has_rows": bool(sample_rows),
                }
            )
            success_count += 1
        except Exception as exc:
            results.append(
                {
                    "doc_id": int(doc_id),
                    "status": "error",
                    "error": str(exc),
                }
            )
            failed_count += 1

    return {
        "success": True,
        "requested_doc_ids": [int(doc_id) for doc_id in payload.doc_ids],
        "target_doc_ids": [int(doc_id) for doc_id in target_doc_ids],
        "only_missing": bool(payload.only_missing),
        "limit": int(payload.limit),
        "processed_count": len(target_doc_ids),
        "success_count": success_count,
        "failed_count": failed_count,
        "results": results,
    }


@router.get("/markdown/status/{doc_id}")
def get_document_markdown_status(doc_id: int) -> dict[str, Any]:
    """Check markdown file availability and status for a document.
    
    Returns information about where the markdown file is stored, its size, and
    whether it's suitable for re-processing.
    """
    try:
        from sql_db import get_connection

        conn = get_connection()
        doc_info = _get_document_info(doc_id, conn)
        
        if not doc_info:
            raise HTTPException(
                status_code=404,
                detail=f"Document {doc_id} not found",
            )
        
        doc_key = doc_info.get("doc_key", "")
        stored_path = markdown_manager.get_markdown_path_from_db(conn, doc_id)
        markdown_content = markdown_manager.get_markdown_for_document(conn, doc_id)
        
        # Try stored path first, then default deterministic path
        md_path = Path(stored_path) if stored_path else markdown_manager.get_markdown_path(doc_id, doc_key)
        if stored_path and not md_path.exists():
            md_path = markdown_manager.get_markdown_path(doc_id, doc_key)
        
        status = {
            "doc_id": doc_id,
            "doc_key": doc_key,
            "markdown_available": False,
            "markdown_storage": "database" if markdown_content is not None else None,
            "markdown_path": None,
            "markdown_size_bytes": len(markdown_content.encode("utf-8")) if markdown_content is not None else 0,
            "stored_path_in_db": stored_path or None,
        }

        if markdown_content is not None:
            status["markdown_available"] = True
        
        if markdown_content is None and md_path.exists():
            stat = md_path.stat()
            status.update({
                "markdown_available": True,
                "markdown_storage": "legacy_file",
                "markdown_path": str(md_path),
                "markdown_size_bytes": stat.st_size,
            })
        
        conn.close()
        return status
        
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to get markdown status for doc_id=%s: %s", doc_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/markdown/list")
def list_all_markdown_files() -> dict[str, Any]:
    """List all markdown files available for re-processing.
    
    Returns information about markdown files in the default markdown directory,
    useful for understanding what documents can be re-processed.
    """
    try:
        files = markdown_manager.list_all_markdown_files()
        return {
            "markdown_dir": str(markdown_manager.MARKDOWN_DIR),
            "file_count": len(files),
            "files": files,
        }
    except Exception as exc:
        LOGGER.exception("Failed to list markdown files: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/markdown/cleanup-orphaned")
def cleanup_orphaned_markdown_files() -> dict[str, Any]:
    """Remove markdown files that are not referenced in the database.
    
    This is a maintenance operation to clean up disk space after documents
    are deleted or moved.
    """
    try:
        deleted_count = markdown_manager.cleanup_orphaned_markdown()
        return {
            "success": True,
            "orphaned_files_deleted": deleted_count,
        }
    except Exception as exc:
        LOGGER.exception("Failed to cleanup orphaned markdown: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/note")
def ingest_note(payload: NoteIngestRequest) -> dict[str, Any]:
    """Ingest a note, memo, or info entry directly from web client or chat.
    
    This creates a plain text document in a standard format with title, date, content, and tags.
    Perfect for capturing quick notes, memos, or information snippets that should be searchable
    by date and theme/tags.
    """
    # Parse note date if provided, otherwise use current date
    if payload.note_date:
        try:
            note_date = datetime.datetime.strptime(payload.note_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            note_date = datetime.datetime.now().strftime("%Y-%m-%d")
    else:
        note_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # Build metadata
    metadata = dict(payload.metadata or {})
    metadata["note_title"] = payload.title
    metadata["note_date"] = note_date
    metadata["note_source"] = payload.source
    metadata["note_content"] = payload.content
    metadata["stable_source_identity"] = _stable_note_identity(
        title=payload.title,
        note_date=note_date,
        source=payload.source,
        tags=payload.tags,
        content=payload.content,
    )
    if payload.tags:
        metadata["note_tags"] = payload.tags

    # Optional structured directives that can be passed by API clients when notes
    # are used to inject operational context and workflow/action hints.
    if isinstance(payload.metadata.get("workflow_processes"), list):
        metadata["workflow_processes"] = [
            str(item).strip()
            for item in payload.metadata.get("workflow_processes")
            if str(item).strip()
        ]
    if isinstance(payload.metadata.get("action_requests"), list):
        metadata["action_requests"] = [item for item in payload.metadata.get("action_requests") if isinstance(item, dict)]
    if payload.metadata.get("execute_action_requests") is not None:
        metadata["execute_action_requests"] = bool(payload.metadata.get("execute_action_requests"))

    # Detect update-intent phrases in title or content so the extraction
    # prompt can instruct the LLM to set operation='update' for known entities.
    _update_signals = [
        r"\bupdate\b", r"\bupdated\b", r"\bchange\b", r"\bchanged\b",
        r"\bnew address\b", r"\baddress change\b", r"\bmoved to\b",
        r"\bnow lives\b", r"\bnow located\b", r"\brelocated\b",
        r"\bcorrection\b", r"\bcorrect\b", r"\brevision\b", r"\brevised\b",
        r"\bamend\b", r"\bamended\b", r"\bmodify\b", r"\bmodified\b",
        r"\bänd(?:erung|ert)\b",  # German: Änderung / geändert
        r"\baktuali(?:sierung|siert)\b",  # German: Aktualisierung / aktualisiert
        r"\bberichtigung\b",  # German: Berichtigung (correction)
    ]
    _search_text = f"{payload.title} {payload.content}"
    import re as _re
    note_update_intent = any(
        _re.search(pat, _search_text, flags=_re.IGNORECASE)
        for pat in _update_signals
    )
    if note_update_intent:
        metadata["note_update_intent"] = True
        LOGGER.info(
            "note_update_intent_detected title=%s", payload.title
        )

    # Log the note ingest
    LOGGER.info(
        "ingest_api_input endpoint=note title=%s note_date=%s tags_count=%s content_len=%s",
        payload.title,
        note_date,
        len(payload.tags),
        len(str(payload.content or "")),
    )
    
    try:
        result = _persist_note_document(
            title=payload.title,
            content=payload.content,
            note_date=note_date,
            tags=payload.tags,
            source=payload.source,
            metadata=metadata,
        )
    except Exception as exc:
        LOGGER.exception("Ingest note failed")
        _raise_ingest_http_error(exc)
    
    return {
        "success": True,
        "title": payload.title,
        "note_date": note_date,
        "run_id": result.get("run_id"),
        "result": result,
    }
