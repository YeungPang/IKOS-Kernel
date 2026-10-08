from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

import object_db

try:
    from attribute_embedding_index import AttributeEmbeddingIndex
except Exception:  # pragma: no cover - optional dependency
    AttributeEmbeddingIndex = None

from ikos_api_server.schemas import SchemaEmbeddingsMaintenanceRequest


router = APIRouter()
APP_DIR = Path(__file__).resolve().parents[2]
WEB_UI_PATH = APP_DIR / "web" / "ikos_test_app.html"
LOGGER = logging.getLogger("idms.api")


class DbCsvExportRequest(BaseModel):
    table_names: list[str] | None = Field(
        default=None,
        description="Optional list of table names to export. Ignored when include_all=true.",
    )
    include_all: bool = Field(
        default=False,
        description="When true, export all tables in the schema.",
    )
    schema_name: str = Field(default="public", description="Database schema to export from")
    output_file_name: str | None = Field(
        default=None,
        description="Optional ZIP filename for multi-table export",
    )


class DbCsvImportRequest(BaseModel):
    source_zip_path: str | None = Field(
        default=None,
        description="Absolute path to ZIP bundle containing table CSV files",
    )
    source_dir: str | None = Field(
        default=None,
        description="Absolute path to directory containing table CSV files",
    )
    table_names: list[str] | None = Field(
        default=None,
        description="Optional subset of tables to import",
    )
    include_all: bool = Field(
        default=False,
        description="Import all CSV tables found in source that exist in DB schema",
    )
    schema_name: str = Field(default="public", description="Target schema name")
    truncate_before_import: bool = Field(
        default=False,
        description="Truncate selected tables before import",
    )
    continue_on_error: bool = Field(
        default=False,
        description="Continue importing remaining tables even if one fails",
    )
    dry_run: bool = Field(
        default=False,
        description="Validate source and import ordering without writing to DB",
    )


class TxMatchPendingRetryRequest(BaseModel):
    pending_id: int | None = Field(default=None, description="Optional specific pending_id to retry")
    statuses: list[str] = Field(default_factory=lambda: ["pending", "failed"], description="Statuses to select")
    limit: int = Field(default=20, ge=1, le=500, description="Max entries to retry")
    admin_token: str = Field(default="", description="Optional maintenance token for restricted environments")


def _require_maintenance_token(admin_token: str = "") -> None:
    required_token = str(os.getenv("IKOS_MAINTENANCE_TOKEN", "")).strip()
    if required_token and str(admin_token or "") != required_token:
        raise HTTPException(status_code=403, detail="Invalid maintenance token")


@router.get("/", response_class=HTMLResponse)
def serve_web_ui() -> str:
    if not WEB_UI_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Web UI not found at {WEB_UI_PATH}")
    return WEB_UI_PATH.read_text(encoding="utf-8")


@router.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/api/maintenance/db-export/tables")
def list_db_export_tables(schema_name: str = "public") -> dict[str, Any]:
    """List exportable tables for a schema."""
    connection = object_db.get_connection()
    try:
        tables = object_db.list_exportable_tables(connection, schema_name=schema_name)
        return {
            "success": True,
            "schema_name": str(schema_name or "public").strip().lower() or "public",
            "count": len(tables),
            "tables": tables,
        }
    except Exception as exc:
        LOGGER.exception("Failed to list DB export tables")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/api/maintenance/db-export/csv")
def export_db_tables_csv(payload: DbCsvExportRequest) -> dict[str, Any]:
    """Export one, multiple, or all DB tables into CSV files.

    For multiple tables, a ZIP bundle path is returned in result.bundle_path.
    """
    connection = object_db.get_connection()
    try:
        result = object_db.export_tables_to_csv_bundle(
            connection,
            table_names=payload.table_names,
            include_all=payload.include_all,
            schema_name=payload.schema_name,
            output_file_name=payload.output_file_name,
        )
        return {"success": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        LOGGER.exception("Failed to export DB tables as CSV")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/api/maintenance/db-import/csv")
def import_db_tables_csv(payload: DbCsvImportRequest) -> dict[str, Any]:
    """Import one, multiple, or all table CSV files into DB tables.

    Supports source ZIP bundles and source directories.
    Use dry_run=true first to inspect table ordering and selection.
    """
    connection = object_db.get_connection()
    try:
        result = object_db.import_tables_from_csv_bundle(
            connection,
            source_zip_path=payload.source_zip_path,
            source_dir=payload.source_dir,
            table_names=payload.table_names,
            include_all=payload.include_all,
            schema_name=payload.schema_name,
            truncate_before_import=payload.truncate_before_import,
            continue_on_error=payload.continue_on_error,
            dry_run=payload.dry_run,
        )
        return {"success": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        LOGGER.exception("Failed to import DB tables from CSV")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/api/maintenance/schema-embeddings/rebuild")
def rebuild_schema_embeddings(
    payload: SchemaEmbeddingsMaintenanceRequest,
) -> dict[str, Any]:
    """Manually rebuild schema embedding vectors used for canonical attribute/relation matching."""
    if AttributeEmbeddingIndex is None:
        raise HTTPException(status_code=503, detail="attribute_embedding_index is not available")

    _require_maintenance_token(payload.admin_token)

    recreate_collection = bool(payload.recreate_collection)
    include_relationships = bool(payload.include_relationships)

    try:
        idx = AttributeEmbeddingIndex()
        upserted_attributes = int(idx.upsert_default_attributes(recreate_collection=recreate_collection))
        upserted_dynamic_attributes = int(idx.upsert_dynamic_attribute_terms())

        relationship_names: list[str] = []
        if include_relationships:
            connection = object_db.get_connection()
            try:
                relationship_rows = object_db.get_relationships(connection, limit=5000)
                relationship_names = sorted(
                    {
                        str(row.get("relationship_name") or "").strip()
                        for row in relationship_rows
                        if str(row.get("relationship_name") or "").strip()
                    }
                )
            finally:
                connection.close()

        upserted_relationships = int(idx.upsert_relationship_terms(relationship_names)) if relationship_names else 0
        upserted_total = upserted_attributes + upserted_dynamic_attributes + upserted_relationships

        LOGGER.info(
            "maintenance_schema_embeddings_rebuild recreate_collection=%s include_relationships=%s upserted_attributes=%s upserted_dynamic_attributes=%s upserted_relationships=%s",
            recreate_collection,
            include_relationships,
            upserted_attributes,
            upserted_dynamic_attributes,
            upserted_relationships,
        )

        return {
            "success": True,
            "recreate_collection": recreate_collection,
            "include_relationships": include_relationships,
            "upserted_total": upserted_total,
            "upserted_attributes": upserted_attributes,
            "upserted_dynamic_attributes": upserted_dynamic_attributes,
            "upserted_relationships": upserted_relationships,
            "relationship_terms_considered": len(relationship_names),
        }
    except Exception as exc:
        LOGGER.exception("Schema embedding maintenance rebuild failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/api/maintenance/tx-match/pending")
def list_tx_match_pending(
    statuses: str = "pending,failed",
    limit: int = 100,
    doc_id: int | None = None,
    admin_token: str = "",
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        _require_maintenance_token(admin_token)
        status_list = [item.strip().lower() for item in str(statuses or "").split(",") if item.strip()]
        rows = object_db.list_tx_match_index_pending(
            connection=connection,
            statuses=status_list,
            limit=max(1, int(limit)),
            doc_id=doc_id,
        )
        return {
            "success": True,
            "count": len(rows),
            "result": rows,
        }
    except Exception as exc:
        LOGGER.exception("Failed to list tx-match pending queue")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/api/maintenance/tx-match/pending/{pending_id}")
def get_tx_match_pending(pending_id: int, admin_token: str = "") -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        _require_maintenance_token(admin_token)
        row = object_db.get_tx_match_index_pending_by_id(connection=connection, pending_id=int(pending_id))
        if row is None:
            raise HTTPException(status_code=404, detail=f"tx_match_index_pending {pending_id} not found")
        return {"success": True, "result": row}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to get tx-match pending queue item")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/api/maintenance/tx-match/pending/retry")
def retry_tx_match_pending(payload: TxMatchPendingRetryRequest) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        _require_maintenance_token(payload.admin_token)
        object_db.create_tables(connection, recreate=False)

        if payload.pending_id is not None:
            entries = [
                object_db.get_tx_match_index_pending_by_id(connection=connection, pending_id=int(payload.pending_id))
            ]
            entries = [item for item in entries if item is not None]
        else:
            entries = object_db.list_tx_match_index_pending(
                connection=connection,
                statuses=[str(item).strip().lower() for item in (payload.statuses or []) if str(item).strip()],
                limit=max(1, int(payload.limit)),
            )

        results: list[dict[str, Any]] = []
        for entry in entries:
            pending_id = int(entry.get("pending_id") or 0)
            queue_payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
            tx_rows = queue_payload.get("tx_rows") if isinstance(queue_payload.get("tx_rows"), list) else []

            if not tx_rows:
                updated = object_db.update_tx_match_index_pending_status(
                    connection=connection,
                    pending_id=pending_id,
                    status="failed",
                    error_message="missing tx_rows in pending payload",
                    retry_increment=True,
                    last_result={"indexed": False, "reason": "missing_tx_rows"},
                )
                results.append(
                    {
                        "pending_id": pending_id,
                        "ok": False,
                        "reason": "missing_tx_rows",
                        "updated": updated,
                    }
                )
                continue

            import tx_match_index

            retry_result = tx_match_index.index_rows(tx_rows)
            if bool(retry_result.get("indexed")):
                updated = object_db.update_tx_match_index_pending_status(
                    connection=connection,
                    pending_id=pending_id,
                    status="indexed",
                    error_message=None,
                    retry_increment=True,
                    last_result=retry_result,
                )
                results.append(
                    {
                        "pending_id": pending_id,
                        "ok": True,
                        "result": retry_result,
                        "updated": updated,
                    }
                )
            else:
                updated = object_db.update_tx_match_index_pending_status(
                    connection=connection,
                    pending_id=pending_id,
                    status="failed",
                    error_message=str(retry_result.get("reason") or retry_result.get("error") or "retry_failed"),
                    retry_increment=True,
                    last_result=retry_result,
                )
                results.append(
                    {
                        "pending_id": pending_id,
                        "ok": False,
                        "result": retry_result,
                        "updated": updated,
                    }
                )

        success_count = sum(1 for item in results if item.get("ok"))
        failed_count = len(results) - success_count
        return {
            "success": True,
            "retried": len(results),
            "success_count": success_count,
            "failed_count": failed_count,
            "result": results,
        }
    except Exception as exc:
        LOGGER.exception("Failed to retry tx-match pending queue")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()
