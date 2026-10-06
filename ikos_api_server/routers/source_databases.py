from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import psycopg2

import object_db
from source_database_integration import (
    discover_source_tables,
    register_source_database,
    sync_mapped_rows,
    upsert_source_table_inventory,
)


router = APIRouter(prefix="/api/source-databases", tags=["source-databases"])
LOGGER = logging.getLogger("idms.api")


class SourceDatabaseConnectionRequest(BaseModel):
    source_key: str = Field(..., description="Stable source identifier, e.g. customer_erp_prod")
    source_name: str = Field(..., description="Human-readable source name")
    db_host: str = Field(..., description="Source PostgreSQL host")
    db_port: int = Field(default=5432, ge=1, le=65535, description="Source PostgreSQL port")
    db_name: str = Field(..., description="Source PostgreSQL database name")
    db_schema: str = Field(default="", description="Optional default schema name")
    db_user: str = Field(..., description="Source PostgreSQL user")
    db_password: str = Field(..., description="Source PostgreSQL password")
    connection_hint: str = Field(default="", description="Optional free-text connection note")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Optional metadata for provenance")


class SourceSchemaDiscoveryRequest(SourceDatabaseConnectionRequest):
    schema_names: list[str] = Field(default_factory=list, description="Optional schema allowlist")
    include_views: bool = Field(default=False, description="When true, include views in inventory")


class SourceTableSyncRequest(SourceDatabaseConnectionRequest):
    schema_name: str = Field(..., description="Source schema name")
    table_name: str = Field(..., description="Source table name")
    target_class_name: str = Field(..., description="IDMS target class name")
    pk_column: str = Field(default="id", description="Source primary-key column")
    name_column: str = Field(default="", description="Optional column used as object_name")
    updated_at_column: str = Field(default="updated_at", description="Optional last-modified column")
    metadata_columns: list[str] = Field(default_factory=list, description="Optional column allowlist for row fetch")


def _connect_source_db(payload: SourceDatabaseConnectionRequest) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=payload.db_host,
        port=payload.db_port,
        database=payload.db_name,
        user=payload.db_user,
        password=payload.db_password,
    )


@router.post("/register")
def register_source_database_endpoint(payload: SourceDatabaseConnectionRequest) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        result = register_source_database(
            connection,
            source_key=payload.source_key,
            source_name=payload.source_name,
            db_name=payload.db_name,
            db_host=payload.db_host,
            db_port=payload.db_port,
            db_user=payload.db_user,
            db_schema=payload.db_schema or None,
            connection_hint=payload.connection_hint or None,
            metadata=payload.metadata,
        )
        return {"success": True, "result": result}
    except Exception as exc:
        LOGGER.exception("Failed to register source database")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/discover")
def discover_source_database_schemas(payload: SourceSchemaDiscoveryRequest) -> dict[str, Any]:
    source_connection = _connect_source_db(payload)
    metadata_connection = object_db.get_connection()
    try:
        registration = register_source_database(
            metadata_connection,
            source_key=payload.source_key,
            source_name=payload.source_name,
            db_name=payload.db_name,
            db_host=payload.db_host,
            db_port=payload.db_port,
            db_user=payload.db_user,
            db_schema=payload.db_schema or None,
            connection_hint=payload.connection_hint or None,
            metadata=payload.metadata,
        )
        tables = discover_source_tables(
            source_connection,
            schema_names=payload.schema_names or ([payload.db_schema] if payload.db_schema else None),
            include_views=payload.include_views,
        )
        for table_info in tables:
            upsert_source_table_inventory(metadata_connection, int(registration["source_id"]), table_info)
        return {
            "success": True,
            "count": len(tables),
            "result": {
                "source": registration,
                "tables": [
                    {
                        "schema_name": table.schema_name,
                        "table_name": table.table_name,
                        "object_kind": table.object_kind,
                        "fingerprint": table.fingerprint,
                        "metadata": table.metadata,
                    }
                    for table in tables
                ],
            },
        }
    except Exception as exc:
        LOGGER.exception("Failed to discover source database schemas")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        source_connection.close()
        metadata_connection.close()


@router.post("/sync-table")
def sync_source_table_to_idms(payload: SourceTableSyncRequest) -> dict[str, Any]:
    source_connection = _connect_source_db(payload)
    idms_connection = object_db.get_connection()
    try:
        metadata_connection = idms_connection
        registration = register_source_database(
            metadata_connection,
            source_key=payload.source_key,
            source_name=payload.source_name,
            db_name=payload.db_name,
            db_host=payload.db_host,
            db_port=payload.db_port,
            db_user=payload.db_user,
            db_schema=payload.db_schema or None,
            connection_hint=payload.connection_hint or None,
            metadata=payload.metadata,
        )

        def map_row_to_object(row: dict[str, Any]) -> dict[str, Any]:
            object_name_value = row.get(payload.name_column) if payload.name_column else row.get(payload.pk_column)
            metadata = {
                "source_system": payload.source_key,
                "source_schema": payload.schema_name,
                "source_table": payload.table_name,
                "source_primary_key": str(row.get(payload.pk_column) or ""),
                "source_row": row,
            }
            return {
                "object_name": str(object_name_value or row.get(payload.pk_column) or "").strip(),
                "class_name": payload.target_class_name,
                "metadata": metadata,
                "status": "active",
            }

        results = sync_mapped_rows(
            source_connection=source_connection,
            idms_connection=idms_connection,
            metadata_connection=metadata_connection,
            source_id=int(registration["source_id"]),
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            target_class_name=payload.target_class_name,
            pk_column=payload.pk_column,
            map_row_to_object=map_row_to_object,
            updated_at_column=payload.updated_at_column or None,
            metadata_columns=payload.metadata_columns or None,
        )
        return {
            "success": True,
            "count": len(results),
            "result": {
                "source": registration,
                "sync_results": [result.__dict__ for result in results],
            },
        }
    except Exception as exc:
        LOGGER.exception("Failed to sync source table to IDMS")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        source_connection.close()
        idms_connection.close()
