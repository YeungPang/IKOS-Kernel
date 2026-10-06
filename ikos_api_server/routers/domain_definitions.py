from __future__ import annotations

import logging
import re
from typing import Any

from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel

import business_rules
import domain_db
import object_db


router = APIRouter(prefix="/api/domain-definitions", tags=["domain-definitions"])
LOGGER = logging.getLogger("idms.api.domain_definitions")


# Pydantic models for request/response validation
class AccountDefinitionPayload(BaseModel):
    legal_entity_ref: str
    account_number: int
    account_name: str
    account_type: str


class DepartmentDefinitionPayload(BaseModel):
    department_code: str
    department_name: str
    active: bool = True
    metadata: dict[str, Any] | None = None


class RoleDefinitionPayload(BaseModel):
    role_code: str
    role_name: str
    role_family: str | None = None
    seniority_level: str | None = None
    active: bool = True
    metadata: dict[str, Any] | None = None


class AccountDefinitionFromNLRequest(BaseModel):
    text: str
    legal_entity_ref: str = "global"
    created_by: str | None = "api:user"


class AccountDefinitionImpactPlanRequest(BaseModel):
    legal_entity_ref: str = "global"
    account_number: int
    account_name: str
    account_type: str
    created_by: str | None = "api:user"
    persist: bool = False
    attach: bool = False


def _parse_account_definition_from_nl(text: str, legal_entity_ref: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("text is required")

    lower = raw.lower()

    number_match = re.search(r"\baccount(?:\s+number)?\s*(?:is|=|:)?\s*(\d{3,10})\b", lower)
    if not number_match:
        number_match = re.search(r"\b(\d{3,10})\b", lower)
    if not number_match:
        raise ValueError("Could not find account number in text")
    account_number = int(number_match.group(1))

    type_match = re.search(r"\baccount\s+type\s*(?:is|=|:)?\s*(asset|liability|equity|revenue|expense)\b", lower)
    if not type_match:
        type_match = re.search(r"\b(asset|liability|equity|revenue|expense)\b", lower)
    if not type_match:
        raise ValueError("Could not find account_type in text (asset|liability|equity|revenue|expense)")
    account_type = str(type_match.group(1)).strip().lower()

    name_match = re.search(r"\baccount\s+name\s*(?:is|=|:)?\s*(.+?)(?:,|\.|;|$)", raw, flags=re.IGNORECASE)
    if name_match:
        account_name = str(name_match.group(1) or "").strip(" \t\n\r\"'")
    else:
        # Fallback: try to capture phrase between account number and account type
        range_match = re.search(
            r"\baccount(?:\s+number)?\s*(?:is|=|:)?\s*\d{3,10}\b[\s,;:-]*(.+?)\baccount\s+type\s*(?:is|=|:)?\s*(?:asset|liability|equity|revenue|expense)\b",
            raw,
            flags=re.IGNORECASE,
        )
        if range_match:
            account_name = str(range_match.group(1) or "").strip(" \t\n\r\"',.-")
        else:
            raise ValueError("Could not find account_name in text")

    if not account_name:
        raise ValueError("account_name cannot be empty")

    return {
        "legal_entity_ref": str(legal_entity_ref or "global").strip() or "global",
        "account_number": int(account_number),
        "account_name": account_name,
        "account_type": account_type,
    }


@router.post("/account-definition/from-nl")
async def create_account_definition_from_nl(payload: AccountDefinitionFromNLRequest) -> dict[str, Any]:
    """Create account_definition from plain-English text.

    Example text:
    "Create a new account number 6400, account name is Travelling Expense and account type is expense"
    """
    try:
        normalized = _parse_account_definition_from_nl(payload.text, payload.legal_entity_ref)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        connection = object_db.get_connection()
        if not connection:
            raise HTTPException(status_code=500, detail="Database connection failed")

        created = domain_db.create_domain_definition(
            connection,
            "account_definition",
            normalized,
        )

        impact_payload = {
            "change_type": "account_definition",
            "change_payload": normalized,
            "domain_hint": "accounting",
            "created_by": str(payload.created_by or "api:user"),
            "persist": False,
            "attach": False,
        }

        return {
            "success": True,
            "result": {
                "created": created,
                "normalized_payload": normalized,
                "impact_confirmation": {
                    "confirmation_required": True,
                    "question": "Should this account definition also impact related accounting workflow pipelines?",
                    "suggested_endpoint": "/api/business-rules/workflow-registry/impact-auto-plan",
                    "suggested_request_json": impact_payload,
                },
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.error(f"Error creating account definition from NL: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/account-definition/impact-auto-plan")
async def account_definition_impact_auto_plan(payload: AccountDefinitionImpactPlanRequest) -> dict[str, Any]:
    """Convenience endpoint for account-definition impact planning with sane defaults."""
    try:
        change_payload = {
            "legal_entity_ref": str(payload.legal_entity_ref or "global").strip() or "global",
            "account_number": int(payload.account_number),
            "account_name": str(payload.account_name or "").strip(),
            "account_type": str(payload.account_type or "").strip().lower(),
        }
        result = business_rules.analyze_workflow_impact_and_generate_extension(
            change_type="account_definition",
            change_payload=change_payload,
            workflow_keys=None,
            domain_hint="accounting",
            top_k=5,
            created_by=payload.created_by,
            persist=bool(payload.persist),
            attach=bool(payload.attach),
            temperature=0.0,
        )
        return {"success": True, "result": result}
    except Exception as e:
        LOGGER.error(f"Error planning account-definition workflow impact: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/{definition_type}")
async def create_domain_definition(
    definition_type: str,
    payload: AccountDefinitionPayload | DepartmentDefinitionPayload | RoleDefinitionPayload,
) -> dict[str, Any]:
    """
    Create a new domain definition (account, department, or role).
    
    - **definition_type**: Type of definition (account, department, role)
    - **payload**: Definition data (varies by type)
    """
    try:
        connection = object_db.get_connection()
        if not connection:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        result = domain_db.create_domain_definition(
            connection,
            definition_type,
            payload.dict(),
        )
        return result
    except Exception as e:
        LOGGER.error(f"Error creating domain definition: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put("/{definition_type}/{key}")
async def update_domain_definition(
    definition_type: str,
    key: str,
    payload: AccountDefinitionPayload | DepartmentDefinitionPayload | RoleDefinitionPayload,
) -> dict[str, Any]:
    """
    Update an existing domain definition.
    
    - **definition_type**: Type of definition (account, department, role)
    - **key**: Primary key of the definition to update (account_number, department_code, or role_code)
    - **payload**: Updated definition data
    """
    try:
        connection = object_db.get_connection()
        if not connection:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Add the key to the payload for the update operation
        update_payload = payload.dict()
        
        # Inject the key into the payload based on definition_type
        if definition_type.lower() in ["account", "account_definition"]:
            try:
                update_payload["account_number"] = int(key)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid account_number: {key}")
        elif definition_type.lower() in ["department", "hr_department", "dept", "abteilung", "hr_department_definition"]:
            update_payload["department_code"] = key
        elif definition_type.lower() in ["role", "position", "funktion", "hr_role_definition"]:
            update_payload["role_code"] = key
        
        result = domain_db.update_domain_definition(
            connection,
            definition_type,
            update_payload,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.error(f"Error updating domain definition: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete("/{definition_type}/{key}")
async def delete_domain_definition(
    definition_type: str,
    key: str,
) -> dict[str, Any]:
    """
    Delete a domain definition.
    
    - **definition_type**: Type of definition (account, department, role)
    - **key**: Primary key of the definition to delete (account_number, department_code, or role_code)
    """
    try:
        connection = object_db.get_connection()
        if not connection:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Build delete payload with the key
        delete_payload = {}
        
        if definition_type.lower() in ["account", "account_definition"]:
            try:
                delete_payload["account_number"] = int(key)
                delete_payload["legal_entity_ref"] = "global"  # Default to global for delete
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid account_number: {key}")
        elif definition_type.lower() in ["department", "hr_department", "dept", "abteilung", "hr_department_definition"]:
            delete_payload["department_code"] = key
        elif definition_type.lower() in ["role", "position", "funktion", "hr_role_definition"]:
            delete_payload["role_code"] = key
        
        result = domain_db.delete_domain_definition(
            connection,
            definition_type,
            delete_payload,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.error(f"Error deleting domain definition: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/{definition_type}/{key}")
async def get_domain_definition(
    definition_type: str,
    key: str,
) -> dict[str, Any]:
    """
    Retrieve a specific domain definition.
    
    - **definition_type**: Type of definition (account, department, role)
    - **key**: Primary key of the definition (account_number, department_code, or role_code)
    """
    try:
        connection = object_db.get_connection()
        if not connection:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Build read payload with the key
        read_payload = {}
        
        if definition_type.lower() in ["account", "account_definition"]:
            try:
                read_payload["account_number"] = int(key)
                read_payload["legal_entity_ref"] = "global"  # Default to global for read
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid account_number: {key}")
        elif definition_type.lower() in ["department", "hr_department", "dept", "abteilung", "hr_department_definition"]:
            read_payload["department_code"] = key
        elif definition_type.lower() in ["role", "position", "funktion", "hr_role_definition"]:
            read_payload["role_code"] = key
        
        result = domain_db.get_domain_definition(
            connection,
            definition_type,
            read_payload,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        LOGGER.error(f"Error retrieving domain definition: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/{definition_type}")
async def list_domain_definitions(
    definition_type: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    include_inactive: bool = Query(False),
) -> dict[str, Any]:
    """
    List all domain definitions of a specific type.
    
    - **definition_type**: Type of definition (account, department, role)
    - **limit**: Maximum number of results (1-1000, default: 100)
    - **offset**: Offset for pagination (default: 0)
    - **include_inactive**: Include inactive definitions (default: False)
    """
    try:
        connection = object_db.get_connection()
        if not connection:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        result = domain_db.list_domain_definitions(
            connection,
            definition_type,
            filters=None,
            limit=limit,
            offset=offset,
            include_inactive=include_inactive,
        )
        return result
    except Exception as e:
        LOGGER.error(f"Error listing domain definitions: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/audit-log/{definition_type}")
async def get_audit_log(
    definition_type: str = Path(...),
    key_value: str | None = Query(None),
    operation: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """
    Retrieve audit log entries for domain definitions.
    
    - **definition_type**: Filter by definition type (optional)
    - **key_value**: Filter by record key (optional)
    - **operation**: Filter by operation (create, update, delete, read)
    - **limit**: Maximum number of results (1-1000, default: 100)
    - **offset**: Offset for pagination (default: 0)
    """
    try:
        connection = object_db.get_connection()
        if not connection:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Use passed definition_type parameter for filtering
        entries = domain_db.get_domain_definition_audit_log(
            connection,
            definition_type=definition_type,
            key_value=key_value,
            operation=operation,
            limit=limit,
            offset=offset,
        )
        
        return {
            "success": True,
            "count": len(entries),
            "limit": limit,
            "offset": offset,
            "entries": entries,
        }
    except Exception as e:
        LOGGER.error(f"Error retrieving audit log: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e
