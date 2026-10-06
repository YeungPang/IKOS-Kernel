from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
import psycopg2

import object_db
import extension_packages
from ikos_config import EXTENSION_PACKAGE_INBOX
from ikos_api_server.deps import require_platform_admin
from security_context import SecurityContext

router = APIRouter(prefix="/api/platform", tags=["platform-administration"])


class TenantProvisionRequest(BaseModel):
    tenant_key: str = Field(..., min_length=1, max_length=128)
    display_name: str = Field(..., min_length=1, max_length=256)
    initial_admin_subject: str = Field(..., min_length=1, max_length=256)
    initial_admin_display_name: str | None = Field(default=None, max_length=256)


class GlobalBusinessRulePublishRequest(BaseModel):
    rule_name: str = Field(..., min_length=1, max_length=256)
    rule_text: str = Field(..., min_length=1)
    structured_rule: dict[str, Any] = Field(default_factory=dict)
    solf_script: str = ""
    scope: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    is_active: bool = True


class GlobalSolfClausePublishRequest(BaseModel):
    clause_name: str = Field(..., min_length=1, max_length=256)
    clause_type: str = Field(..., pattern="^(query_pattern|resolve_policy|ingest_rule|computation_rule)$")
    clause_body: str = Field(..., min_length=1)
    entity_class: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    is_active: bool = True


class GlobalWorkflowPublishRequest(BaseModel):
    workflow_key: str = Field(..., min_length=1, max_length=256)
    workflow_name: str = Field(..., min_length=1, max_length=256)
    description: str = ""
    domain: str | None = None
    status: str = Field(default="published", pattern="^(draft|review|published|deprecated|archived)$")
    metadata: dict[str, Any] = Field(default_factory=dict)
    steps: list[dict[str, Any]] = Field(default_factory=list)
    input_contract: dict[str, Any] = Field(default_factory=dict)
    output_contract: dict[str, Any] = Field(default_factory=dict)


class GlobalGeneratedScriptPublishRequest(BaseModel):
    script_key: str = Field(..., min_length=1, max_length=256)
    script_name: str = Field(..., min_length=1, max_length=256)
    script_source: str = Field(..., min_length=1)
    entrypoint: str = "run"
    approval_status: str = Field(default="draft", pattern="^(draft|approved|deprecated|archived)$")
    metadata: dict[str, Any] = Field(default_factory=dict)
    is_active: bool = True


class ExtensionPackageImportRequest(BaseModel):
    manifest: dict[str, Any] = Field(..., description="Versioned IKOS extension manifest")


class ExtensionPackageTargetRequest(BaseModel):
    target_scope: str = Field(default="tenant", pattern="^(tenant|global)$")
    target_tenant_id: UUID | None = None


class ExtensionPackagePromoteRequest(ExtensionPackageTargetRequest):
    expected_sha256: str = Field(..., pattern="^[a-fA-F0-9]{64}$")


@router.post("/tenants", status_code=201, summary="Create a tenant and its first tenant administrator")
def create_tenant(
    request: TenantProvisionRequest,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    """Provision a tenant; platform-admin identities are configured outside the API."""
    connection = object_db.get_connection()
    try:
        result = object_db.create_tenant_with_initial_admin(
            connection,
            tenant_key=request.tenant_key,
            display_name=request.display_name,
            admin_external_subject=request.initial_admin_subject,
            admin_display_name=request.initial_admin_display_name,
        )
        connection.commit()
        return {"success": True, "created_by": context.user_id, **result}
    except ValueError as exc:
        connection.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        connection.rollback()
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except psycopg2.IntegrityError as exc:
        connection.rollback()
        raise HTTPException(status_code=409, detail="Tenant key already exists or tenant provisioning conflicts with existing data") from exc
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Unable to provision tenant") from exc
    finally:
        connection.close()


@router.post("/configuration/business-rules", status_code=201)
def publish_global_business_rule(
    request: GlobalBusinessRulePublishRequest,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO business_rules (
                    tenant_id, configuration_scope, rule_name, rule_text, structured_rule,
                    solf_script, scope, metadata, is_active, created_by, created_at, modified_at
                ) VALUES (%s::uuid, 'global', %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING rule_id
                """,
                (
                    object_db.LEGACY_TENANT_ID,
                    request.rule_name.strip(), request.rule_text,
                    object_db.Json(request.structured_rule), request.solf_script,
                    object_db.Json(request.scope), object_db.Json(request.metadata),
                    request.is_active, context.user_id,
                ),
            )
            rule_id = int(cursor.fetchone()[0])
        connection.commit()
        return {"success": True, "result": {"rule_id": rule_id, "rule_name": request.rule_name, "configuration_scope": "global"}}
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Unable to publish global business rule") from exc
    finally:
        connection.close()


@router.post("/configuration/solf-clauses", status_code=201)
def publish_global_solf_clause(
    request: GlobalSolfClausePublishRequest,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO solf_clauses (
                    tenant_id, configuration_scope, clause_name, clause_type, entity_class,
                    clause_body, metadata, is_active, created_by, created_at, modified_at
                ) VALUES (%s::uuid, 'global', %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING clause_id
                """,
                (
                    object_db.LEGACY_TENANT_ID,
                    request.clause_name.strip(), request.clause_type, request.entity_class,
                    request.clause_body, object_db.Json(request.metadata), request.is_active,
                    context.user_id,
                ),
            )
            clause_id = int(cursor.fetchone()[0])
        connection.commit()
        return {"success": True, "result": {"clause_id": clause_id, "clause_name": request.clause_name, "configuration_scope": "global"}}
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Unable to publish global SOLF clause") from exc
    finally:
        connection.close()


@router.post("/configuration/workflows", status_code=201)
def publish_global_workflow(
    request: GlobalWorkflowPublishRequest,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    connection = object_db.get_connection()
    tenant_id = object_db.LEGACY_TENANT_ID
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO solf_workflow_registry (
                    tenant_id, configuration_scope, workflow_key, workflow_name, description,
                    domain, status, metadata, is_active, created_by, created_at, modified_at
                ) VALUES (%s::uuid, 'global', %s, %s, %s, %s, %s, %s, TRUE, %s, NOW(), NOW())
                ON CONFLICT (tenant_id, configuration_scope, workflow_key) DO UPDATE SET
                    workflow_name = EXCLUDED.workflow_name,
                    description = EXCLUDED.description,
                    domain = EXCLUDED.domain,
                    status = EXCLUDED.status,
                    metadata = COALESCE(solf_workflow_registry.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                    is_active = TRUE,
                    modified_at = NOW()
                RETURNING workflow_id
                """,
                (
                    tenant_id, request.workflow_key.strip().lower(), request.workflow_name.strip(),
                    request.description, request.domain, request.status,
                    object_db.Json(request.metadata), context.user_id,
                ),
            )
            workflow_id = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COALESCE(MAX(version_no), 0) + 1 FROM solf_workflow_versions WHERE tenant_id = %s::uuid AND workflow_id = %s",
                (tenant_id, workflow_id),
            )
            version_no = int(cursor.fetchone()[0])
            cursor.execute(
                """
                INSERT INTO solf_workflow_versions (
                    tenant_id, configuration_scope, workflow_id, version_no, graph_spec,
                    input_contract, output_contract, metadata, is_active, created_by, created_at, modified_at
                ) VALUES (%s::uuid, 'global', %s, %s, '{}'::jsonb, %s, %s, %s, TRUE, %s, NOW(), NOW())
                RETURNING workflow_version_id
                """,
                (
                    tenant_id, workflow_id, version_no,
                    object_db.Json(request.input_contract), object_db.Json(request.output_contract),
                    object_db.Json(request.metadata), context.user_id,
                ),
            )
            version_id = int(cursor.fetchone()[0])
            for index, step in enumerate(request.steps, start=1):
                step_key = str(step.get("step_key") or f"step_{index}").strip().lower()
                step_kind = str(step.get("step_kind") or "clause").strip().lower()
                if step_kind not in {"clause", "class_transform", "class_generate", "class_iterate", "python_binding"}:
                    raise ValueError(f"Invalid step_kind: {step_kind}")
                clause_id = int(step["clause_id"]) if step.get("clause_id") is not None else None
                if clause_id is not None:
                    cursor.execute(
                        "SELECT 1 FROM solf_clauses WHERE clause_id = %s AND configuration_scope = 'global'",
                        (clause_id,),
                    )
                    if cursor.fetchone() is None:
                        raise ValueError("Global workflows may only reference globally published SOLF clauses")
                cursor.execute(
                    """
                    INSERT INTO solf_workflow_steps (
                        tenant_id, configuration_scope, workflow_version_id, step_order, step_key,
                        step_kind, clause_id, clause_name, input_class, output_class, operation,
                        python_module, python_function, config, created_at, modified_at
                    ) VALUES (%s::uuid, 'global', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """,
                    (
                        tenant_id, version_id, index, step_key, step_kind,
                        clause_id,
                        str(step.get("clause_name") or "").strip() or None,
                        str(step.get("input_class") or "").strip() or None,
                        str(step.get("output_class") or "").strip() or None,
                        str(step.get("operation") or "").strip() or None,
                        str(step.get("python_module") or "").strip() or None,
                        str(step.get("python_function") or "").strip() or None,
                        object_db.Json(step.get("config") if isinstance(step.get("config"), dict) else {}),
                    ),
                )
        connection.commit()
        return {
            "success": True,
            "result": {
                "workflow_id": workflow_id,
                "workflow_version_id": version_id,
                "workflow_key": request.workflow_key,
                "configuration_scope": "global",
            },
        }
    except ValueError as exc:
        connection.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Unable to publish global workflow") from exc
    finally:
        connection.close()


@router.post("/configuration/generated-scripts", status_code=201)
def publish_global_generated_script(
    request: GlobalGeneratedScriptPublishRequest,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO workflow_generated_python_script (
                    tenant_id, configuration_scope, script_key, script_name, script_source,
                    entrypoint, approval_status, metadata, is_active, created_by, created_at, modified_at
                ) VALUES (%s::uuid, 'global', %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (tenant_id, configuration_scope, script_key) DO UPDATE SET
                    script_name = EXCLUDED.script_name,
                    script_source = EXCLUDED.script_source,
                    entrypoint = EXCLUDED.entrypoint,
                    approval_status = EXCLUDED.approval_status,
                    metadata = COALESCE(workflow_generated_python_script.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                    is_active = EXCLUDED.is_active,
                    modified_at = NOW()
                RETURNING script_id, script_key
                """,
                (
                    object_db.LEGACY_TENANT_ID, request.script_key.strip(), request.script_name.strip(),
                    request.script_source, request.entrypoint, request.approval_status,
                    object_db.Json(request.metadata), request.is_active, context.user_id,
                ),
            )
            script_id, script_key = cursor.fetchone()
        connection.commit()
        return {"success": True, "result": {"script_id": int(script_id), "script_key": script_key, "configuration_scope": "global"}}
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Unable to publish global generated script") from exc
    finally:
        connection.close()


@router.post("/extensions/packages/import", status_code=201)
def import_extension_package(
    request: ExtensionPackageImportRequest,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    """Validate and register an immutable extension package version."""
    connection = object_db.get_connection()
    try:
        result = extension_packages.import_package(
            connection,
            manifest_data=request.manifest,
            imported_by=context.user_id,
        )
        connection.commit()
        return {"success": True, "result": result}
    except extension_packages.ExtensionVersionConflict as exc:
        connection.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        connection.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except psycopg2.IntegrityError as exc:
        connection.rollback()
        raise HTTPException(status_code=409, detail="Extension package version conflicts with an existing immutable version") from exc
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Unable to register extension package") from exc
    finally:
        connection.close()


@router.post("/extensions/packages/inbox/{filename}/import", status_code=201)
def import_extension_package_from_inbox(
    filename: str,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    """Import one manifest from the explicitly configured, trusted deployment inbox."""
    if not EXTENSION_PACKAGE_INBOX:
        raise HTTPException(status_code=503, detail="IKOS_EXTENSION_PACKAGE_INBOX is not configured")
    try:
        manifest = extension_packages.load_manifest_from_inbox(filename, EXTENSION_PACKAGE_INBOX)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Extension manifest not found in configured inbox") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    connection = object_db.get_connection()
    try:
        result = extension_packages.import_package(
            connection,
            manifest_data=manifest,
            imported_by=context.user_id,
        )
        connection.commit()
        return {"success": True, "result": result}
    except extension_packages.ExtensionVersionConflict as exc:
        connection.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        connection.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except psycopg2.IntegrityError as exc:
        connection.rollback()
        raise HTTPException(status_code=409, detail="Extension package version conflicts with an existing immutable version") from exc
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Unable to register extension package from inbox") from exc
    finally:
        connection.close()


@router.get("/extensions/packages")
def list_extension_packages(
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        result = extension_packages.list_packages(connection)
        connection.rollback()
        return {"success": True, "count": len(result), "result": result}
    finally:
        connection.close()


@router.get("/extensions/deployments")
def list_extension_deployments(
    extension_key: str | None = None,
    target_tenant_id: UUID | None = None,
    limit: int = 100,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        result = extension_packages.list_deployments(
            connection,
            extension_key=extension_key,
            target_tenant_id=str(target_tenant_id) if target_tenant_id else None,
            limit=limit,
        )
        connection.rollback()
        return {"success": True, "count": len(result), "result": result}
    finally:
        connection.close()


@router.get("/extensions/packages/{extension_key}/versions/{version}")
def get_extension_package_version(
    extension_key: str,
    version: str,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        row = extension_packages.get_package_version(connection, extension_key, version)
        if row is None:
            raise HTTPException(status_code=404, detail="Extension package version not found")
        return {"success": True, "result": row}
    finally:
        connection.close()


@router.post("/extensions/packages/{extension_key}/versions/{version}/inspect")
def inspect_extension_package(
    extension_key: str,
    version: str,
    request: ExtensionPackageTargetRequest,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        result = extension_packages.inspect_package(
            connection,
            extension_key=extension_key,
            version=version,
            target_scope=request.target_scope,
            target_tenant_id=request.target_tenant_id,
        )
        connection.rollback()
        return {"success": True, "result": result}
    except LookupError as exc:
        connection.rollback()
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        connection.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/extensions/packages/{extension_key}/versions/{version}/promote")
def promote_extension_package(
    extension_key: str,
    version: str,
    request: ExtensionPackagePromoteRequest,
    context: SecurityContext = Depends(require_platform_admin),
) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        result = extension_packages.promote_package(
            connection,
            extension_key=extension_key,
            version=version,
            expected_sha256=request.expected_sha256,
            target_scope=request.target_scope,
            target_tenant_id=request.target_tenant_id,
            deployed_by=context.user_id,
        )
        connection.commit()
        # SOLF programs and query aliases are cached by process-local runtime objects.
        # This refreshes the current worker on its next interaction request.
        from ikos_api_server import deps

        deps.get_tools.cache_clear()
        return {"success": True, "result": result}
    except LookupError as exc:
        connection.rollback()
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        connection.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except psycopg2.IntegrityError as exc:
        connection.rollback()
        raise HTTPException(status_code=409, detail="Extension promotion conflicts with existing configuration") from exc
    except Exception as exc:
        connection.rollback()
        raise HTTPException(status_code=500, detail="Extension package promotion failed") from exc
    finally:
        connection.close()
