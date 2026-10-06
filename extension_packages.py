"""Versioned, platform-admin-controlled domain extension package promotion.

Schema v1 promotes declarative SOLF, capability workflows, query vocabulary,
and tenant ingestion triggers. It never executes uploaded Python or SQL.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Literal

from jsonschema import Draft202012Validator, FormatChecker
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from psycopg2.extras import Json

import business_rules
from capability_registry import CAPABILITIES
from security_context import LEGACY_TENANT_ID

EXTENSION_PACKAGE_SCHEMA_VERSION = "1"
_SEMVER_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$")
_KEY_RE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
MAX_PACKAGE_BYTES = 2_000_000
_CAPABILITY_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:+/-]*$")
_AUTHORITY_FIELDS = frozenset({
    "tenant_id", "user_id", "permissions", "security_context", "roles",
    "workspace_ids", "platform_admin", "is_platform_admin", "principal", "caller",
})
_EXECUTABLE_FIELDS = frozenset({
    "module", "function", "functions", "source", "script", "script_source",
    "python_source", "generated_script", "generated_script_id", "generated_script_key",
    "script_id", "script_key", "entrypoint", "binding", "python_binding",
})
_RESERVED_SAVE_FIELDS = _AUTHORITY_FIELDS | frozenset({
    "run_id", "step_key", "step_order", "step_kind", "run_status", "step_status",
    "started_by", "requested_by", "on_failure_policy", "paused_at_step_key",
    "idempotency_key", "status", "success", "paused", "pause_reason",
    "interaction_prompt", "required_doc_types", "missing_data_desc", "error",
    "error_message", "next_step_key", "handled_exception", "applied_clause", "request_id",
})


def _validate_declarative_data(value: Any) -> None:
    """Reject authority/code selectors and non-JSON values, including nested data."""
    if type(value) is dict:
        for name, child in value.items():
            if type(name) is not str or name.lower() in _AUTHORITY_FIELDS | _EXECUTABLE_FIELDS:
                raise ValueError("workflow/trigger data cannot contain authority fields or executable bindings")
            _validate_declarative_data(child)
    elif type(value) is list:
        for child in value:
            _validate_declarative_data(child)
    elif type(value) not in (str, int, float, bool, type(None)):
        raise ValueError("workflow/trigger data must contain JSON values only")
    # Also reject NaN/infinity rather than persisting invalid JSONB.
    json.dumps(value, allow_nan=False)


def _validate_capability_config(config: Any, *, inverse: bool = False) -> None:
    if not isinstance(config, dict):
        raise ValueError("capability config must be an object")
    key_field, version_field = ("key", "version") if inverse else ("capability_key", "capability_version")
    allowed = {key_field, version_field, "payload", "payload_fields"}
    if not inverse:
        allowed |= {"save_as", "compensation_capability"}
    if set(config) - allowed:
        raise ValueError("unsupported capability config fields; executable bindings and authority fields are excluded")
    for name in (key_field, version_field):
        value = config.get(name)
        if not isinstance(value, str) or not _CAPABILITY_IDENTIFIER_RE.fullmatch(value):
            raise ValueError(f"{name} must be a nonempty exact identifier (no ranges or wildcards)")
    literals, fields = config.get("payload", {}), config.get("payload_fields", {})
    if not isinstance(literals, dict) or not isinstance(fields, dict):
        raise ValueError("capability payload and payload_fields must be objects")
    _validate_declarative_data(literals)
    for name, source in fields.items():
        if (not isinstance(name, str) or not name or name != name.strip() or name in literals
                or not isinstance(source, str) or not source or source != source.strip()
                or any(not part for part in source.split("."))):
            raise ValueError("payload_fields must map unique nonempty input fields to context paths")
        if any(part.lower() in _AUTHORITY_FIELDS | _EXECUTABLE_FIELDS
               for path in (name, source) for part in path.split(".")):
            raise ValueError("capability mapping cannot forward authority fields or executable bindings")
    if not inverse:
        save_as = config.get("save_as", "capability_result")
        if (not isinstance(save_as, str) or not save_as or save_as != save_as.strip()
                or "." in save_as or save_as.lower() in _RESERVED_SAVE_FIELDS | _EXECUTABLE_FIELDS
                or save_as.lower().startswith(("_", "workflow_", "execution_"))):
            raise ValueError("capability save_as cannot overwrite execution metadata")
        if "compensation_capability" in config:
            _validate_capability_config(config["compensation_capability"], inverse=True)


def load_manifest_from_inbox(filename: str, inbox_dir: str | Path) -> dict[str, Any]:
    """Read one JSON manifest from a trusted inbox, refusing traversal/symlinks."""
    name = str(filename or "").strip()
    if not name or Path(name).name != name or name in {".", ".."}:
        raise ValueError("filename must be a simple file name, not a path")
    if not name.lower().endswith(".json"):
        raise ValueError("extension package inbox accepts .json manifests only")
    root = Path(inbox_dir).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ValueError("configured extension inbox is not a directory")
    candidate = root / name
    if candidate.is_symlink():
        raise ValueError("symbolic links are not accepted in the extension inbox")
    resolved = candidate.resolve(strict=True)
    if resolved.parent != root or not resolved.is_file():
        raise ValueError("manifest must be a regular file directly inside the configured extension inbox")
    if resolved.stat().st_size > MAX_PACKAGE_BYTES:
        raise ValueError("extension manifest exceeds the 2 MB limit")
    try:
        manifest = json.loads(resolved.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("extension manifest must be valid UTF-8 JSON") from exc
    if not isinstance(manifest, dict):
        raise ValueError("extension manifest root must be a JSON object")
    return manifest


class ExtensionVersionConflict(ValueError):
    """Raised when an immutable extension key/version is reused with new bytes."""


class ExtensionAsset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["solf_script", "solf_clause", "semantic_term", "semantic_pattern", "query_term_alias", "workflow", "ingestion_trigger"]
    key: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any]

    @field_validator("key")
    @classmethod
    def validate_key(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not _KEY_RE.fullmatch(normalized):
            raise ValueError("asset key must be lowercase letters/digits separated by '.', '_' or '-'")
        return normalized


class ExtensionManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = EXTENSION_PACKAGE_SCHEMA_VERSION
    extension_key: str = Field(min_length=1, max_length=128)
    version: str = Field(min_length=5, max_length=64)
    display_name: str = Field(min_length=1, max_length=256)
    description: str = Field(default="", max_length=4000)
    assets: list[ExtensionAsset] = Field(min_length=1, max_length=500)

    @field_validator("extension_key")
    @classmethod
    def validate_extension_key(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not _KEY_RE.fullmatch(normalized):
            raise ValueError("extension_key must be a lowercase stable identifier")
        return normalized

    @field_validator("version")
    @classmethod
    def validate_semver(cls, value: str) -> str:
        normalized = value.strip()
        if not _SEMVER_RE.fullmatch(normalized):
            raise ValueError("version must be semantic versioning, for example 1.2.3")
        return normalized

    @model_validator(mode="after")
    def validate_assets(self) -> "ExtensionManifest":
        keys: set[tuple[str, str]] = set()
        database_identities: set[tuple[Any, ...]] = set()
        total_bytes = 0
        for asset in self.assets:
            identity = (asset.kind, asset.key)
            if identity in keys:
                raise ValueError(f"duplicate {asset.kind} asset key: {asset.key}")
            keys.add(identity)
            if asset.kind == "solf_script" and 5 + len(self.extension_key) + len(asset.key) > 256:
                raise ValueError(f"SOLF script asset key '{asset.key}' is too long for its stable database identifier")
            if asset.kind in {"workflow", "ingestion_trigger"} and 5 + len(self.extension_key) + len(asset.key) > 256:
                raise ValueError(f"workflow asset key '{asset.key}' is too long for its stable database identifier")
            _validate_asset_payload(asset)
            payload = asset.payload
            if asset.kind == "solf_script":
                db_identity = (asset.kind, asset.key)
            elif asset.kind == "solf_clause":
                db_identity = (asset.kind, payload["clause_name"], payload.get("entity_class"))
            elif asset.kind == "semantic_term":
                db_identity = (asset.kind, payload["kind"], payload["canonical_name"].strip().lower(),
                               payload["term_text"].strip(), str(payload.get("language") or "und").strip().lower())
            elif asset.kind == "semantic_pattern":
                db_identity = (asset.kind, payload["pattern_text"].strip().lower(),
                               payload.get("entity_class"), str(payload.get("pattern_language") or "en").strip().lower())
            elif asset.kind == "query_term_alias":
                db_identity = (asset.kind, payload["alias_text"].strip(), payload["kind"],
                               str(payload.get("language") or "und").strip().lower())
            else:
                db_identity = (asset.kind, asset.key)
            if db_identity in database_identities:
                raise ValueError(f"duplicate database identity in extension assets: {db_identity}")
            database_identities.add(db_identity)
            total_bytes += len(json.dumps(asset.model_dump(mode="json"), ensure_ascii=False).encode("utf-8"))
        if total_bytes > MAX_PACKAGE_BYTES:
            raise ValueError("combined extension assets exceed the 2 MB limit")
        workflows = {asset.key for asset in self.assets if asset.kind == "workflow"}
        for asset in self.assets:
            if asset.kind == "ingestion_trigger" and asset.payload["workflow_asset_key"] not in workflows:
                raise ValueError(f"ingestion_trigger asset '{asset.key}' must reference a bundled workflow asset")
        return self


def _validate_asset_payload(asset: ExtensionAsset) -> None:
    payload = asset.payload
    if asset.kind == "solf_script":
        script = payload.get("script")
        if not isinstance(script, str) or not script.strip():
            raise ValueError(f"solf_script asset '{asset.key}' requires a non-empty payload.script")
        if len(script.encode("utf-8")) > 1_000_000:
            raise ValueError(f"solf_script asset '{asset.key}' exceeds the 1 MB limit")
        _validate_delimiters(script, asset.key)
        review = business_rules.review_solf_script_rule(
            solf_script=script,
            rule_name=f"ext_{asset.key}",
            default_clause_type=str(payload.get("default_clause_type") or "resolve_policy"),
        )
        if not review.get("is_valid"):
            errors = review.get("syntax_errors") if isinstance(review.get("syntax_errors"), list) else []
            raise ValueError(f"invalid SOLF script '{asset.key}': {errors[:10]}")
        return

    if asset.kind == "solf_clause":
        clause_name_raw = payload.get("clause_name")
        clause_name = clause_name_raw.strip() if isinstance(clause_name_raw, str) else ""
        clause_type = str(payload.get("clause_type") or "resolve_policy").strip().lower()
        clause_body = str(payload.get("clause_body") or "")
        if not clause_name or len(clause_name) > 256:
            raise ValueError(f"solf_clause asset '{asset.key}' requires a clause_name up to 256 characters")
        if clause_type not in {"query_pattern", "resolve_policy", "ingest_rule", "computation_rule"}:
            raise ValueError(f"solf_clause asset '{asset.key}' has unsupported clause_type")
        entity_class = payload.get("entity_class")
        if entity_class is not None and (not isinstance(entity_class, str) or len(entity_class) > 128):
            raise ValueError(f"solf_clause asset '{asset.key}' entity_class must be a string up to 128 characters")
        if not clause_body.strip() or len(clause_body.encode("utf-8")) > 1_000_000:
            raise ValueError(f"solf_clause asset '{asset.key}' requires a non-empty body under 1 MB")
        _validate_delimiters(clause_body, asset.key)
        review = business_rules.review_solf_script_rule(
            solf_script=clause_body,
            rule_name=f"ext_{asset.key}",
            default_clause_type=clause_type,
        )
        parsed = review.get("artifacts", {}).get("clauses", [])
        if not review.get("is_valid") or not parsed:
            raise ValueError(f"invalid SOLF clause asset '{asset.key}': {review.get('syntax_errors', [])[:10]}")
        parsed_names = {str(item.get("clause_name") or "").strip() for item in parsed if isinstance(item, dict)}
        if clause_name not in parsed_names:
            raise ValueError(f"solf_clause asset '{asset.key}' clause_name must match its SOLF definition")
        return

    if asset.kind == "semantic_term":
        kind = str(payload.get("kind") or "").strip().lower()
        if kind not in {"attribute", "relationship", "entity_type", "category"}:
            raise ValueError(f"semantic_term asset '{asset.key}' has unsupported kind")
        for required in ("canonical_name", "term_text"):
            if not isinstance(payload.get(required), str) or not payload[required].strip():
                raise ValueError(f"semantic_term asset '{asset.key}' requires {required}")
        if len(payload["canonical_name"]) > 256 or len(payload["term_text"]) > 512:
            raise ValueError(f"semantic_term asset '{asset.key}' exceeds a field length limit")
        if payload.get("language") is not None and (not isinstance(payload["language"], str) or len(payload["language"]) > 12):
            raise ValueError(f"semantic_term asset '{asset.key}' language must be a string up to 12 characters")
        return

    if asset.kind == "semantic_pattern":
        for required in ("pattern_text", "semantic_concept"):
            if not isinstance(payload.get(required), str) or not payload[required].strip():
                raise ValueError(f"semantic_pattern asset '{asset.key}' requires {required}")
        if len(payload["pattern_text"]) > 512 or len(payload["semantic_concept"]) > 128:
            raise ValueError(f"semantic_pattern asset '{asset.key}' exceeds a field length limit")
        if payload.get("pattern_language") is not None and (not isinstance(payload["pattern_language"], str) or len(payload["pattern_language"]) > 32):
            raise ValueError(f"semantic_pattern asset '{asset.key}' pattern_language must be a string up to 32 characters")
        mapped = payload.get("mapped_attributes", {})
        if not isinstance(mapped, dict):
            raise ValueError(f"semantic_pattern asset '{asset.key}' mapped_attributes must be an object")
        if payload.get("entity_class") is not None and (not isinstance(payload["entity_class"], str) or len(payload["entity_class"]) > 128):
            raise ValueError(f"semantic_pattern asset '{asset.key}' entity_class must be a string up to 128 characters")
        confidence = payload.get("confidence", 0.9)
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise ValueError(f"semantic_pattern asset '{asset.key}' confidence must be between 0 and 1")
        synonyms = payload.get("synonyms", [])
        if not isinstance(synonyms, list) or len(synonyms) > 100:
            raise ValueError(f"semantic_pattern asset '{asset.key}' synonyms must be a list of at most 100")
        seen_synonyms: set[tuple[str, str]] = set()
        for synonym in synonyms:
            if not isinstance(synonym, dict) or not isinstance(synonym.get("synonym_text"), str) or not synonym["synonym_text"].strip():
                raise ValueError(f"semantic_pattern asset '{asset.key}' synonyms require synonym_text")
            if len(synonym["synonym_text"]) > 512:
                raise ValueError(f"semantic_pattern asset '{asset.key}' synonym exceeds 512 characters")
            language = str(synonym.get("language") or payload.get("pattern_language") or "en").strip().lower()
            identity = (synonym["synonym_text"].strip().lower(), language)
            if identity in seen_synonyms:
                raise ValueError(f"duplicate synonym in semantic_pattern asset '{asset.key}'")
            seen_synonyms.add(identity)
            distance = synonym.get("semantic_distance", 0.0)
            if isinstance(distance, bool) or not isinstance(distance, (int, float)) or not 0 <= distance <= 1:
                raise ValueError(f"semantic_pattern asset '{asset.key}' synonym semantic_distance must be between 0 and 1")
            if synonym.get("match_type", "exact") not in {"exact", "template", "fuzzy", "semantic"}:
                raise ValueError(f"semantic_pattern asset '{asset.key}' has unsupported synonym match_type")
            if "*" in synonym["synonym_text"] and synonym.get("match_type", "exact") == "exact":
                synonym["match_type"] = "template"
            elif "*" in synonym["synonym_text"] and synonym.get("match_type") != "template":
                raise ValueError(f"semantic_pattern asset '{asset.key}' wildcard synonyms require match_type='template'")
        return

    if asset.kind == "query_term_alias":
        kind = str(payload.get("kind") or "").strip().lower()
        if kind not in {"attribute", "relationship", "entity_type", "category"}:
            raise ValueError(f"query_term_alias asset '{asset.key}' has unsupported kind")
        for required in ("alias_text", "canonical_name"):
            if not isinstance(payload.get(required), str) or not payload[required].strip():
                raise ValueError(f"query_term_alias asset '{asset.key}' requires {required}")
        if len(payload["alias_text"]) > 512 or len(payload["canonical_name"]) > 256:
            raise ValueError(f"query_term_alias asset '{asset.key}' exceeds a field length limit")
        if payload.get("language") is not None and (not isinstance(payload["language"], str) or len(payload["language"]) > 32):
            raise ValueError(f"query_term_alias asset '{asset.key}' language must be a string up to 32 characters")
        priority = payload.get("priority", 100)
        if isinstance(priority, bool) or not isinstance(priority, int) or not 0 <= priority <= 10000:
            raise ValueError(f"query_term_alias asset '{asset.key}' priority must be between 0 and 10000")
        return

    if asset.kind == "workflow":
        allowed = {"workflow_name", "description", "domain", "steps", "graph_spec", "input_contract", "output_contract"}
        if set(payload) - allowed:
            raise ValueError("unsupported workflow payload fields")
        _validate_declarative_data(payload)
        for name in ("description", "domain"):
            if name in payload and not isinstance(payload[name], str):
                raise ValueError(f"workflow {name} must be a string")
        for name in ("graph_spec", "input_contract", "output_contract"):
            if name in payload and not isinstance(payload[name], dict):
                raise ValueError(f"workflow {name} must be an object")
        if not isinstance(payload.get("workflow_name"), str) or not payload["workflow_name"].strip() or len(payload["workflow_name"]) > 256:
            raise ValueError(f"workflow asset '{asset.key}' requires workflow_name")
        steps = payload.get("steps")
        if not isinstance(steps, list) or not 1 <= len(steps) <= 100:
            raise ValueError(f"workflow asset '{asset.key}' requires 1 to 100 steps")
        seen_steps: set[str] = set()
        for index, step in enumerate(steps, start=1):
            if not isinstance(step, dict):
                raise ValueError(f"workflow asset '{asset.key}' step {index} must be an object")
            kind = step.get("step_kind", "clause")
            if kind not in ("clause", "capability"):
                raise ValueError("extension workflows support SOLF clause steps only or declarative capability steps; Python bindings are excluded")
            allowed_step = {"step_key", "step_kind", "config"}
            if kind == "clause":
                allowed_step |= {"clause_name", "input_class", "output_class", "operation"}
            if set(step) - allowed_step:
                raise ValueError("unsupported workflow step fields")
            raw_key = step.get("step_key", f"step_{index}")
            if not isinstance(raw_key, str):
                raise ValueError("workflow step_key must be a string")
            step_key = raw_key.strip().lower()
            if not step_key or len(step_key) > 256 or step_key in seen_steps:
                raise ValueError(f"workflow asset '{asset.key}' steps require unique step_key")
            seen_steps.add(step_key)
            config = step.get("config", {})
            if not isinstance(config, dict):
                raise ValueError(f"workflow asset '{asset.key}' step config must be an object")
            if kind == "capability":
                _validate_capability_config(config)
            else:
                clause_name = step.get("clause_name")
                if not isinstance(clause_name, str) or not clause_name.strip() or len(clause_name) > 256:
                    raise ValueError(f"workflow asset '{asset.key}' steps require clause_name")
                if "capability_key" in config or "capability_version" in config or "compensation_capability" in config:
                    raise ValueError("capability config requires step_kind='capability'")
                for name in ("input_class", "output_class", "operation"):
                    if name in step and not isinstance(step[name], str):
                        raise ValueError(f"workflow {name} must be a string")
        return

    if asset.kind == "ingestion_trigger":
        if set(payload) - {"workflow_asset_key", "document_types", "is_active"}:
            raise ValueError("unsupported ingestion_trigger fields; arbitrary predicates are excluded")
        workflow_asset_key = payload.get("workflow_asset_key")
        if not isinstance(workflow_asset_key, str) or not _KEY_RE.fullmatch(workflow_asset_key):
            raise ValueError("ingestion_trigger requires workflow_asset_key")
        types = payload.get("document_types")
        if (not isinstance(types, list) or not types
                or any(not isinstance(value, str) or value not in {"invoice", "bill", "receipt"} for value in types)
                or len(set(types)) != len(types)):
            raise ValueError("ingestion_trigger document_types must be a nonempty unique list of invoice, bill, receipt")
        if "is_active" in payload and not isinstance(payload["is_active"], bool):
            raise ValueError("ingestion_trigger is_active must be boolean")
        return

    raise ValueError(f"unsupported asset kind: {asset.kind}")


def _validate_delimiters(script: str, asset_key: str) -> None:
    """Reject unbalanced SOLF/container delimiters without evaluating a script."""
    pairs = {")": "(", "]": "[", "}": "{", chr(0x2984): chr(0x2983)}
    opens = set(pairs.values())
    stack: list[tuple[str, int]] = []
    quote: str | None = None
    escaped = False
    line_start = True
    comment = False
    for offset, char in enumerate(script):
        if char == "\n":
            line_start = True
            comment = False
            continue
        if comment:
            continue
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
            line_start = False
            continue
        if char == "#" and line_start:
            comment = True
            continue
        if char in opens:
            stack.append((char, offset))
        elif char in pairs:
            if not stack or stack[-1][0] != pairs[char]:
                raise ValueError(f"unmatched delimiter in SOLF asset '{asset_key}' at character {offset}")
            stack.pop()
        if not char.isspace():
            line_start = False
    if quote:
        raise ValueError(f"unterminated quoted string in SOLF asset '{asset_key}'")
    if stack:
        char, offset = stack[-1]
        raise ValueError(f"unclosed delimiter '{char}' in SOLF asset '{asset_key}' at character {offset}")


def canonical_manifest(manifest: ExtensionManifest) -> dict[str, Any]:
    return manifest.model_dump(mode="json", exclude_none=True)


def manifest_sha256(manifest: ExtensionManifest | dict[str, Any]) -> str:
    payload = canonical_manifest(manifest) if isinstance(manifest, ExtensionManifest) else manifest
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _set_admin_tenant_context(connection: Any, tenant_id: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT set_config('ikos.tenant_id', %s, true)", (str(tenant_id),))
        cursor.execute(
            "SELECT set_config('ikos.permissions', %s, true)",
            ("platform.configuration.publish,tenant.configuration.manage",),
        )


def _package_version_row(connection: Any, extension_key: str, version: str) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT p.package_id, p.extension_key, p.display_name, p.description,
                      v.package_version_id, v.version, v.manifest, v.sha256, v.status,
                      v.created_by, v.created_at
               FROM ikos_extension_package p
               JOIN ikos_extension_package_version v ON v.package_id = p.package_id
               WHERE p.extension_key = %s AND v.version = %s
               LIMIT 1""",
            (extension_key, version),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return {
        "package_id": int(row[0]), "extension_key": row[1], "display_name": row[2],
        "description": row[3], "package_version_id": int(row[4]), "version": row[5],
        "manifest": row[6] if isinstance(row[6], dict) else {}, "sha256": str(row[7]).strip(),
        "status": row[8], "created_by": row[9], "created_at": row[10],
    }


def import_package(connection: Any, manifest_data: dict[str, Any], imported_by: str) -> dict[str, Any]:
    """Validate and immutably register one extension release."""
    try:
        manifest = ExtensionManifest.model_validate(manifest_data)
    except ValidationError as exc:
        raise ValueError(exc.errors(include_input=False)) from exc
    payload = canonical_manifest(manifest)
    digest = manifest_sha256(payload)
    with connection.cursor() as cursor:
        cursor.execute(
            """INSERT INTO ikos_extension_package
               (extension_key, display_name, description, created_by)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (extension_key) DO UPDATE SET
                   display_name = EXCLUDED.display_name,
                   description = EXCLUDED.description,
                   modified_at = NOW()
               RETURNING package_id""",
            (manifest.extension_key, manifest.display_name, manifest.description, imported_by),
        )
        package_id = int(cursor.fetchone()[0])
        cursor.execute(
            """SELECT package_version_id, sha256, status, created_at
               FROM ikos_extension_package_version
               WHERE package_id = %s AND version = %s""",
            (package_id, manifest.version),
        )
        existing = cursor.fetchone()
        if existing:
            if str(existing[1]).strip() != digest:
                raise ExtensionVersionConflict("This extension key/version already exists with a different content hash")
            return {
                "extension_key": manifest.extension_key, "version": manifest.version,
                "package_version_id": int(existing[0]), "sha256": digest,
                "status": existing[2], "created_at": existing[3], "idempotent": True,
            }
        cursor.execute(
            """INSERT INTO ikos_extension_package_version
               (package_id, version, manifest, sha256, status, created_by)
               VALUES (%s, %s, %s, %s, 'validated', %s)
               RETURNING package_version_id, created_at""",
            (package_id, manifest.version, Json(payload), digest, imported_by),
        )
        version_row = cursor.fetchone()
    return {
        "extension_key": manifest.extension_key, "version": manifest.version,
        "package_version_id": int(version_row[0]), "sha256": digest,
        "status": "validated", "created_at": version_row[1], "idempotent": False,
    }


def get_package_version(connection: Any, extension_key: str, version: str) -> dict[str, Any] | None:
    return _package_version_row(connection, extension_key, version)


def list_packages(connection: Any) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT p.extension_key, p.display_name, p.description, p.created_by, p.created_at,
                      v.version, v.sha256, v.status, v.created_at
               FROM ikos_extension_package p
               LEFT JOIN LATERAL (
                   SELECT version, sha256, status, created_at
                   FROM ikos_extension_package_version
                   WHERE package_id = p.package_id
                   ORDER BY created_at DESC, package_version_id DESC LIMIT 1
               ) v ON TRUE
               ORDER BY p.extension_key"""
        )
        rows = cursor.fetchall() or []
    return [
        {"extension_key": r[0], "display_name": r[1], "description": r[2],
         "created_by": r[3], "created_at": r[4], "latest_version": r[5],
         "latest_sha256": str(r[6]).strip() if r[6] else None,
         "latest_status": r[7], "latest_created_at": r[8]}
        for r in rows
    ]


def list_deployments(
    connection: Any,
    *,
    extension_key: str | None = None,
    target_tenant_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    conditions: list[str] = []
    params: list[Any] = []
    if extension_key:
        conditions.append("p.extension_key = %s")
        params.append(extension_key.strip().lower())
    if target_tenant_id:
        conditions.append("d.target_tenant_id = %s::uuid")
        params.append(target_tenant_id)
    query = """SELECT d.deployment_id, p.extension_key, v.version, d.target_scope,
                      d.target_tenant_id, d.package_sha256, d.status, d.change_summary,
                      d.deployed_by, d.deployed_at
               FROM ikos_extension_deployment d
               JOIN ikos_extension_package_version v ON v.package_version_id=d.package_version_id
               JOIN ikos_extension_package p ON p.package_id=v.package_id"""
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY d.deployed_at DESC, d.deployment_id DESC LIMIT %s"
    params.append(max(1, min(int(limit), 500)))
    with connection.cursor() as cursor:
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall() or []
    return [
        {"deployment_id": int(row[0]), "extension_key": row[1], "version": row[2],
         "target_scope": row[3], "target_tenant_id": str(row[4]),
         "package_sha256": str(row[5]).strip(), "status": row[6],
         "change_summary": row[7] if isinstance(row[7], dict) else {},
         "deployed_by": row[8], "deployed_at": row[9]}
        for row in rows
    ]


def _asset_content_hash(asset: dict[str, Any]) -> str:
    packed = json.dumps(asset, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(packed.encode("utf-8")).hexdigest()


def _workflow_key(extension_key: str, asset_key: str) -> str:
    return f"ext.{extension_key}.{asset_key}"[:256]


def _ownership_action(row: Any, extension_key: str, asset_key: str, digest: str) -> str:
    if row is None:
        return "create"
    metadata = row[0] if isinstance(row[0], dict) else {}
    if (metadata.get("managed_by") != "ikos_extension_package"
            or metadata.get("extension_key") != extension_key
            or metadata.get("asset_key") != asset_key):
        return "conflict"
    return "unchanged" if metadata.get("asset_sha256") == digest else "update"


def _inspect_capability_dependencies(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Inspect exact registrations and their inverses, never execute handlers."""
    requested: set[tuple[str, str]] = set()
    for step in steps:
        if step.get("step_kind", "clause") != "capability":
            continue
        config = step["config"]
        requested.add((config["capability_key"], config["capability_version"]))
        inverse = config.get("compensation_capability")
        if inverse is not None:
            requested.add((inverse["key"], inverse["version"]))
    inspected: dict[tuple[str, str], dict[str, Any]] = {}
    while requested - inspected.keys():
        batch = sorted(requested - inspected.keys())
        dependencies = CAPABILITIES.inspect_dependencies(
            [{"key": key, "version": version} for key, version in batch]
        )
        for dependency in dependencies:
            identity = (dependency["key"], dependency["version"])
            inspected[identity] = dependency
            metadata = dependency.get("capability") or {}
            inverse = metadata.get("inverse")
            if dependency["available"] and inverse:
                requested.add((inverse["key"], inverse["version"]))
    return [inspected[identity] for identity in sorted(inspected)]


def _schema_error_messages(
    schema: dict[str, Any] | bool,
    payload: dict[str, Any],
    *,
    auto_idempotency_key: bool = False,
) -> list[str]:
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(payload), key=lambda error: (list(error.absolute_path), error.message))
    messages = []
    for error in errors:
        path = ".".join(str(part) for part in error.absolute_path) or "$"
        if auto_idempotency_key and error.validator == "required" and isinstance(error.instance, dict):
            missing = set(error.validator_value or []) - set(error.instance)
            missing.discard("idempotency_key")
            if not missing:
                continue
            messages.append(f"{path}: missing required input fields: {', '.join(sorted(missing))}")
        else:
            messages.append(f"{path}: {error.message}")
    return messages


_PARTIAL_SCHEMA_KEYWORDS = frozenset({
    "$schema", "$id", "$comment", "title", "description", "default", "examples",
    "deprecated", "readOnly", "writeOnly", "type", "properties", "required",
    "additionalProperties",
})
_COMPOSITION_SCHEMA_KEYWORDS = frozenset({
    "$ref", "$dynamicRef", "$recursiveRef", "allOf", "anyOf", "oneOf", "not",
    "if", "then", "else", "patternProperties", "dependentSchemas", "dependencies",
    "unevaluatedProperties", "propertyNames",
})


def _contains_schema_reference(schema: Any) -> bool:
    if isinstance(schema, dict):
        return any(
            key in {"$ref", "$dynamicRef", "$recursiveRef"} or _contains_schema_reference(value)
            for key, value in schema.items()
        )
    if isinstance(schema, list):
        return any(_contains_schema_reference(value) for value in schema)
    return False


def _validate_mapped_capability_payload(
    schema: dict[str, Any] | bool,
    literals: dict[str, Any],
    mapped_fields: dict[str, Any],
    *,
    auto_idempotency_key: bool,
) -> tuple[list[str], bool]:
    """Check only constraints provable without guessing mapped runtime values."""
    if schema is False:
        return ["$: input schema rejects every payload"], False
    if schema is True:
        return [], True
    if not isinstance(schema, dict):
        return [], False

    # Enforce only constraints stated directly at the root and on literal
    # properties. Composition, refs, and object-wide conditional keywords are
    # not evaluated against a fabricated partial instance.
    supported = not (_COMPOSITION_SCHEMA_KEYWORDS & schema.keys()) and set(schema) <= _PARTIAL_SCHEMA_KEYWORDS

    errors: list[str] = []
    schema_type = schema.get("type")
    if schema_type is not None and "object" not in (schema_type if isinstance(schema_type, list) else [schema_type]):
        errors.append("$: input schema does not accept an object payload")

    supplied_keys = set(literals) | set(mapped_fields)
    if auto_idempotency_key:
        supplied_keys.add("idempotency_key")
    missing = sorted(set(schema.get("required", [])) - supplied_keys)
    if missing:
        errors.append(f"$: missing required input fields: {', '.join(missing)}")

    properties = schema.get("properties", {})
    if not isinstance(properties, dict):
        return errors, False
    additional = schema.get("additionalProperties", True)
    for name in sorted(supplied_keys):
        if name == "idempotency_key" and auto_idempotency_key and name not in literals:
            continue
        field_schema = properties.get(name, additional)
        if field_schema is False:
            errors.append(f"{name}: additional properties are not allowed")
        elif name in literals and isinstance(field_schema, (dict, bool)) and not _contains_schema_reference(field_schema):
            value = literals[name]
            field_errors = list(Draft202012Validator(field_schema, format_checker=FormatChecker()).iter_errors(value))
            for error in sorted(field_errors, key=lambda item: item.message):
                errors.append(f"{name}: {error.message}")
        elif field_schema is None:
            # `additionalProperties: null` is an invalid schema; registration
            # prevents it, but keep the analysis conservative if metadata is
            # ever supplied by a non-registry implementation.
            return errors, False

    return errors, supported


def _inspect_capability_payloads(
    steps: list[dict[str, Any]], dependencies: list[dict[str, Any]], workflow_asset_key: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_identity = {(item["key"], item["version"]): item for item in dependencies}
    validation_errors: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []

    def add_errors(step: dict[str, Any], role: str, key: str, version: str, messages: list[str]) -> None:
        if messages:
            validation_errors.append({
                "workflow_asset_key": workflow_asset_key,
                "step_key": str(step.get("step_key") or ""),
                "role": role,
                "capability_key": key,
                "capability_version": version,
                "errors": messages,
            })

    for step in steps:
        if step.get("step_kind", "clause") != "capability":
            continue
        config = step["config"]
        forward_key = config["capability_key"]
        forward_version = config["capability_version"]
        forward_dependency = by_identity.get((forward_key, forward_version), {})
        forward_metadata = forward_dependency.get("capability") or {}
        inverse_config = config.get("compensation_capability")
        advertised_inverse = forward_metadata.get("inverse")

        if inverse_config is not None and advertised_inverse is not None:
            if (inverse_config["key"], inverse_config["version"]) != (
                advertised_inverse.get("key"), advertised_inverse.get("version"),
            ):
                add_errors(step, "compensation", forward_key, forward_version, [
                    "configured compensation capability does not match the registered inverse "
                    f"{advertised_inverse.get('key')}@{advertised_inverse.get('version')}"
                ])

        payload_configs = [("forward", forward_key, forward_version, config, True)]
        if inverse_config is not None:
            payload_configs.append(("compensation", inverse_config["key"], inverse_config["version"], inverse_config, False))

        for role, key, version, payload_config, is_forward in payload_configs:
            dependency = by_identity.get((key, version), {})
            metadata = dependency.get("capability") or {}
            if not dependency.get("available") or not metadata:
                continue
            schema = metadata["input_schema"]
            literals = payload_config.get("payload", {})
            mapped_fields = payload_config.get("payload_fields", {})
            if not mapped_fields:
                messages = _schema_error_messages(
                    schema, literals, auto_idempotency_key=is_forward,
                )
                add_errors(step, role, key, version, messages)
                continue

            messages, partial_supported = _validate_mapped_capability_payload(
                schema, literals, mapped_fields,
                auto_idempotency_key=is_forward,
            )
            add_errors(step, role, key, version, messages)
            deferred.append({
                "workflow_asset_key": workflow_asset_key,
                "step_key": str(step.get("step_key") or ""),
                "role": role,
                "capability_key": key,
                "capability_version": version,
                "mapped_fields": sorted(mapped_fields),
                "reason": (
                    "Mapped values are resolved from workflow context at runtime; their runtime types and "
                    "full schema validity cannot be established during package inspection."
                    if partial_supported else
                    "Mapped values are resolved from workflow context at runtime; the full schema cannot be "
                    "evaluated soundly against a partial payload."
                ),
            })

    return validation_errors, deferred


def _inspect_asset(
    connection: Any,
    extension_key: str,
    asset: dict[str, Any],
    scope: str,
    tenant_id: str,
    bundled_clauses: set[str] | None = None,
    bundled_workflows: set[str] | None = None,
) -> dict[str, Any]:
    key, payload = str(asset["key"]), asset["payload"]
    digest = _asset_content_hash(asset)
    target_tenant = LEGACY_TENANT_ID if scope == "global" else tenant_id
    if asset["kind"] == "ingestion_trigger" and scope != "tenant":
        raise ValueError("ingestion_trigger assets are tenant-scoped only")
    with connection.cursor() as cursor:
        if asset["kind"] == "ingestion_trigger":
            trigger_key = _workflow_key(extension_key, key)
            cursor.execute(
                """SELECT metadata FROM ikos_ingestion_trigger
                   WHERE tenant_id=%s::uuid AND trigger_key=%s""",
                (target_tenant, trigger_key),
            )
            row = cursor.fetchone()
            reference = payload["workflow_asset_key"]
            return {"kind": asset["kind"], "key": key, "target_key": trigger_key,
                    "workflow_key": _workflow_key(extension_key, reference),
                    "action": _ownership_action(row, extension_key, key, digest),
                    "missing_workflows": [] if reference in (bundled_workflows or set()) else [reference]}

        if asset["kind"] == "solf_script":
            rule_name = f"ext_{extension_key}_{key}"[:256]
            cursor.execute(
                """SELECT metadata FROM business_rules
                   WHERE tenant_id=%s::uuid AND configuration_scope=%s AND rule_name=%s""",
                (target_tenant, scope, rule_name),
            )
            row = cursor.fetchone()
            return {"kind": asset["kind"], "key": key, "target_key": rule_name,
                    "action": _ownership_action(row, extension_key, key, digest)}

        if asset["kind"] == "solf_clause":
            entity_class = payload.get("entity_class")
            cursor.execute(
                """SELECT metadata FROM solf_clauses
                   WHERE tenant_id=%s::uuid AND configuration_scope=%s AND clause_name=%s
                     AND ((entity_class IS NULL AND %s IS NULL) OR entity_class=%s)
                   ORDER BY clause_id DESC LIMIT 1""",
                (target_tenant, scope, payload["clause_name"], entity_class, entity_class),
            )
            row = cursor.fetchone()
            return {"kind": asset["kind"], "key": key, "target_key": payload["clause_name"],
                    "action": _ownership_action(row, extension_key, key, digest)}

        if asset["kind"] == "workflow":
            workflow_key = _workflow_key(extension_key, key)
            cursor.execute(
                """SELECT metadata FROM solf_workflow_registry
                   WHERE tenant_id=%s::uuid AND configuration_scope=%s AND workflow_key=%s""",
                (target_tenant, scope, workflow_key),
            )
            row = cursor.fetchone()
            action = _ownership_action(row, extension_key, key, digest)
            missing_clauses: list[str] = []
            for step in payload["steps"]:
                if step.get("step_kind", "clause") == "capability":
                    continue
                clause_name = str(step.get("clause_name") or "").strip()
                if clause_name in (bundled_clauses or set()):
                    continue
                cursor.execute(
                    """SELECT 1 FROM solf_clauses
                       WHERE tenant_id=%s::uuid AND configuration_scope=%s AND clause_name=%s AND is_active=TRUE
                       LIMIT 1""",
                    (target_tenant, scope, clause_name),
                )
                if cursor.fetchone() is None:
                    missing_clauses.append(clause_name)
            dependencies = _inspect_capability_dependencies(payload["steps"])
            validation_errors, validation_deferred = _inspect_capability_payloads(
                payload["steps"], dependencies, key,
            )
            return {"kind": asset["kind"], "key": key, "target_key": workflow_key,
                    "action": action, "missing_clauses": sorted(set(missing_clauses)),
                    "capability_dependencies": dependencies,
                    "capability_validation_errors": validation_errors,
                    "capability_validation_deferred": validation_deferred,
                    "missing_capabilities": [{"key": item["key"], "version": item["version"]}
                                             for item in dependencies if not item["available"]]}

        if asset["kind"] == "semantic_pattern":
            pattern_text = payload["pattern_text"].strip().lower()
            entity_class = payload.get("entity_class")
            language = str(payload.get("pattern_language") or "en").strip().lower()
            cursor.execute(
                """SELECT pattern_id, metadata FROM semantic_patterns
                   WHERE tenant_id=%s::uuid AND pattern_text=%s AND pattern_language=%s
                     AND ((entity_class IS NULL AND %s IS NULL) OR entity_class=%s)
                   ORDER BY pattern_id DESC LIMIT 1""",
                (target_tenant, pattern_text, language, entity_class, entity_class),
            )
            row = cursor.fetchone()
            action = _ownership_action((row[1],) if row else None, extension_key, key, digest)
            synonym_conflicts: list[str] = []
            if row:
                for synonym in payload.get("synonyms", []):
                    synonym_text = synonym["synonym_text"].strip().lower()
                    synonym_language = str(synonym.get("language") or language).strip().lower()
                    cursor.execute(
                        """SELECT semantic_distance, match_type FROM pattern_synonyms
                           WHERE tenant_id=%s::uuid AND pattern_id=%s AND synonym_text=%s AND language=%s""",
                        (target_tenant, int(row[0]), synonym_text, synonym_language),
                    )
                    existing_synonym = cursor.fetchone()
                    if existing_synonym and (
                        float(existing_synonym[0]) != float(synonym.get("semantic_distance", 0.0))
                        or str(existing_synonym[1]) != str(synonym.get("match_type", "exact"))
                    ):
                        synonym_conflicts.append(synonym_text)
            if synonym_conflicts:
                action = "conflict"
            result = {"kind": asset["kind"], "key": key, "target_key": (pattern_text, entity_class, language), "action": action}
            if synonym_conflicts:
                result["synonym_conflicts"] = sorted(synonym_conflicts)
            return result

        if asset["kind"] == "semantic_term":
            identity = (payload["kind"], payload["canonical_name"].strip().lower(), payload["term_text"].strip(), str(payload.get("language") or "und").strip().lower())
            cursor.execute(
                """SELECT metadata FROM semantic_terms
                   WHERE tenant_id=%s::uuid AND kind=%s AND canonical_name=%s AND term_text=%s AND language=%s""",
                (target_tenant, *identity),
            )
        else:
            identity = (payload["alias_text"].strip(), payload["kind"], str(payload.get("language") or "und").strip().lower())
            cursor.execute(
                """SELECT metadata FROM query_term_alias
                   WHERE tenant_id=%s::uuid AND alias_text=%s AND kind=%s AND language=%s""",
                (target_tenant, *identity),
            )
        row = cursor.fetchone()
    return {"kind": asset["kind"], "key": key, "target_key": identity,
            "action": _ownership_action(row, extension_key, key, digest)}


def _validate_scope_assets(target_scope: str, manifest: ExtensionManifest) -> None:
    if target_scope == "global" and any(asset.kind == "ingestion_trigger" for asset in manifest.assets):
        raise ValueError("Global promotion excludes ingestion_trigger assets; triggers are tenant-scoped only")
    if target_scope == "global" and any(
        asset.kind in {"semantic_term", "semantic_pattern", "query_term_alias"} for asset in manifest.assets
    ):
        raise ValueError("Global promotion currently supports SOLF and workflow assets only; vocabulary assets are tenant-scoped")


def inspect_package(
    connection: Any,
    *,
    extension_key: str,
    version: str,
    target_scope: str,
    target_tenant_id: str | None,
) -> dict[str, Any]:
    if target_scope not in {"tenant", "global"}:
        raise ValueError("target_scope must be tenant or global")
    if target_scope == "tenant" and not target_tenant_id:
        raise ValueError("target_tenant_id is required for tenant scope")
    if target_scope == "global" and target_tenant_id:
        raise ValueError("target_tenant_id must be omitted for global scope")
    resolved_tenant = str(target_tenant_id) if target_scope == "tenant" else LEGACY_TENANT_ID
    with connection.cursor() as cursor:
        cursor.execute("SELECT set_config('ikos.tenant_id', %s, true)", (resolved_tenant,))
        cursor.execute("SELECT set_config('ikos.permissions', %s, true)", ("platform.configuration.publish,tenant.configuration.manage",))
    package = _package_version_row(connection, extension_key, version)
    if package is None:
        raise LookupError("Extension package version not found")
    if target_scope == "tenant":
        with connection.cursor() as cursor:
            cursor.execute("SELECT status FROM tenant WHERE tenant_id=%s::uuid", (resolved_tenant,))
            tenant = cursor.fetchone()
        if not tenant or str(tenant[0]).lower() != "active":
            raise ValueError("Target tenant does not exist or is not active")
    manifest = ExtensionManifest.model_validate(package["manifest"])
    _validate_scope_assets(target_scope, manifest)
    bundled_clauses = {
        str(asset.payload.get("clause_name") or "").strip()
        for asset in manifest.assets if asset.kind == "solf_clause"
    }
    bundled_workflows = {asset.key for asset in manifest.assets if asset.kind == "workflow"}
    changes = [
        _inspect_asset(connection, extension_key, asset.model_dump(mode="json"), target_scope,
                       resolved_tenant, bundled_clauses, bundled_workflows)
        for asset in manifest.assets
    ]
    summary = {action: sum(item["action"] == action for item in changes) for action in ("create", "update", "unchanged", "conflict")}
    missing_clauses = sorted({name for item in changes for name in item.get("missing_clauses", [])})
    dependencies = {(item["key"], item["version"]): item
                    for change in changes for item in change.get("capability_dependencies", [])}
    missing_capabilities = [{"key": key, "version": version}
                            for key, version in sorted(dependencies) if not dependencies[(key, version)]["available"]]
    required_permissions = sorted({
        str(dependency["capability"]["permission"])
        for dependency in dependencies.values()
        if dependency["available"] and dependency.get("capability", {}).get("permission")
    })
    capability_validation_errors = [
        error for item in changes for error in item.get("capability_validation_errors", [])
    ]
    capability_validation_deferred = [
        item for change in changes for item in change.get("capability_validation_deferred", [])
    ]
    missing_workflows = sorted({name for item in changes for name in item.get("missing_workflows", [])})
    return {
        "extension_key": extension_key, "version": version, "package_sha256": package["sha256"],
        "target_scope": target_scope,
        "target_tenant_id": resolved_tenant if target_scope == "tenant" else None,
        "changes": changes, "summary": summary, "missing_clauses": missing_clauses,
        "capability_dependencies": [dependencies[identity] for identity in sorted(dependencies)],
        "missing_capabilities": missing_capabilities, "required_permissions": required_permissions,
        "capability_validation_errors": capability_validation_errors,
        "capability_validation_deferred": capability_validation_deferred,
        "missing_workflows": missing_workflows,
        "can_promote": summary["conflict"] == 0 and not missing_clauses and not missing_capabilities
        and not missing_workflows and not capability_validation_errors,
    }


def promote_package(
    connection: Any,
    *,
    extension_key: str,
    version: str,
    expected_sha256: str,
    target_scope: str,
    target_tenant_id: str | None,
    deployed_by: str,
) -> dict[str, Any]:
    # Serialize promotions for one extension so two releases cannot race and
    # leave the package-owned rows in a mixed or last-committer-wins state.
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT package_id FROM ikos_extension_package WHERE extension_key=%s FOR UPDATE",
            (extension_key,),
        )
        if cursor.fetchone() is None:
            raise LookupError("Extension package not found")
    plan = inspect_package(connection, extension_key=extension_key, version=version,
                           target_scope=target_scope, target_tenant_id=target_tenant_id)
    if plan["package_sha256"] != str(expected_sha256).lower():
        raise ValueError("Package content hash changed; inspect the package again before promotion")
    if not plan["can_promote"]:
        raise ValueError(
            "Promotion has unavailable dependencies, invalid capability contracts, or conflicts "
            "with configuration not owned by this extension"
        )
    target_tenant = plan["target_tenant_id"] or LEGACY_TENANT_ID
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT deployment_id FROM ikos_extension_deployment
               WHERE package_version_id=(SELECT v.package_version_id FROM ikos_extension_package p
                   JOIN ikos_extension_package_version v USING(package_id)
                   WHERE p.extension_key=%s AND v.version=%s)
                 AND target_scope=%s AND target_tenant_id=%s::uuid AND status='applied'""",
            (extension_key, version, target_scope, target_tenant),
        )
        prior = cursor.fetchone()
        if prior:
            return {**plan, "deployment_id": int(prior[0]), "idempotent": True}

    package = _package_version_row(connection, extension_key, version)
    manifest = ExtensionManifest.model_validate(package["manifest"])
    asset_order = {"solf_script": 0, "solf_clause": 1, "semantic_pattern": 2,
                   "semantic_term": 3, "query_term_alias": 4, "workflow": 5, "ingestion_trigger": 6}
    ordered_assets = sorted(manifest.assets, key=lambda item: asset_order[item.kind])
    for asset_model in ordered_assets:
        asset = asset_model.model_dump(mode="json")
        payload = asset["payload"]
        digest = _asset_content_hash(asset)
        asset_metadata = {"managed_by": "ikos_extension_package", "extension_key": extension_key,
                  "extension_version": version, "asset_key": asset["key"], "asset_sha256": digest}
        metadata = Json(asset_metadata)
        if asset["kind"] == "solf_script":
            review = business_rules.review_solf_script_rule(
                solf_script=payload["script"], rule_name=f"ext_{asset['key']}",
                default_clause_type=str(payload.get("default_clause_type") or "resolve_policy"),
            )
            rule_name = f"ext_{extension_key}_{asset['key']}"[:256]
            structured = {"class_definitions": review["artifacts"]["classes"],
                          "script_clause_names": [c["clause_name"] for c in review["artifacts"]["clauses"]]}
            scope_json = review["artifacts"]["scope"]
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO business_rules
                       (tenant_id,configuration_scope,rule_name,rule_text,structured_rule,solf_script,scope,metadata,is_active,created_by)
                       VALUES (%s::uuid,%s,%s,%s,%s,%s,%s,%s,TRUE,%s)
                       ON CONFLICT (tenant_id,configuration_scope,rule_name) DO UPDATE SET
                         rule_text=EXCLUDED.rule_text, structured_rule=EXCLUDED.structured_rule,
                         solf_script=EXCLUDED.solf_script, scope=EXCLUDED.scope, metadata=EXCLUDED.metadata,
                         is_active=TRUE, modified_at=NOW()
                       WHERE business_rules.metadata->>'managed_by'='ikos_extension_package'
                         AND business_rules.metadata->>'extension_key'=%s
                         AND business_rules.metadata->>'asset_key'=%s RETURNING rule_id""",
                    (target_tenant, target_scope, rule_name, f"Extension package {extension_key}@{version}",
                     Json(structured), payload["script"], Json(scope_json), metadata, deployed_by, extension_key, asset["key"]),
                )
                if cursor.fetchone() is None:
                    raise ValueError(f"Ownership changed while promoting SOLF script {asset['key']}; inspect again")
        elif asset["kind"] == "solf_clause":
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO solf_clauses
                       (tenant_id,configuration_scope,clause_name,clause_type,entity_class,clause_body,metadata,is_active,created_by)
                       VALUES (%s::uuid,%s,%s,%s,%s,%s,%s,TRUE,%s)
                       ON CONFLICT (tenant_id,configuration_scope,clause_name,(COALESCE(entity_class,''))) DO UPDATE SET
                         clause_type=EXCLUDED.clause_type, clause_body=EXCLUDED.clause_body,
                         metadata=EXCLUDED.metadata, is_active=TRUE, modified_at=NOW()
                       WHERE solf_clauses.metadata->>'managed_by'='ikos_extension_package'
                         AND solf_clauses.metadata->>'extension_key'=%s
                         AND solf_clauses.metadata->>'asset_key'=%s RETURNING clause_id""",
                    (target_tenant, target_scope, payload["clause_name"], payload.get("clause_type", "resolve_policy"),
                     payload.get("entity_class"), payload["clause_body"], metadata, deployed_by, extension_key, asset["key"]),
                )
                if cursor.fetchone() is None:
                    raise ValueError(f"Ownership changed while promoting SOLF clause {asset['key']}; inspect again")
        elif asset["kind"] == "semantic_pattern":
            pattern_text = payload["pattern_text"].strip().lower()
            semantic_concept = payload["semantic_concept"].strip().lower()
            pattern_language = str(payload.get("pattern_language") or "en").strip().lower()
            entity_class = payload.get("entity_class")
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT pattern_id, metadata FROM semantic_patterns
                       WHERE tenant_id=%s::uuid AND pattern_text=%s AND pattern_language=%s
                         AND ((entity_class IS NULL AND %s IS NULL) OR entity_class=%s)
                       ORDER BY pattern_id DESC LIMIT 1 FOR UPDATE""",
                    (target_tenant, pattern_text, pattern_language, entity_class, entity_class),
                )
                existing_pattern = cursor.fetchone()
                if existing_pattern:
                    existing_metadata = existing_pattern[1] if isinstance(existing_pattern[1], dict) else {}
                    if (existing_metadata.get("managed_by") != "ikos_extension_package"
                            or existing_metadata.get("extension_key") != extension_key
                            or existing_metadata.get("asset_key") != asset["key"]):
                        raise ValueError(f"Ownership changed while promoting semantic pattern {asset['key']}; inspect again")
                    pattern_id = int(existing_pattern[0])
                    cursor.execute(
                        """UPDATE semantic_patterns SET semantic_concept=%s, mapped_attributes=%s,
                           computation_rule=%s, source_type='seeded', confidence=%s, metadata=%s,
                           modified_at=NOW() WHERE tenant_id=%s::uuid AND pattern_id=%s""",
                        (semantic_concept, Json(payload.get("mapped_attributes") or {}),
                         payload.get("computation_rule"), float(payload.get("confidence", 0.9)),
                         metadata, target_tenant, pattern_id),
                    )
                else:
                    cursor.execute(
                        """INSERT INTO semantic_patterns
                           (tenant_id,pattern_text,semantic_concept,mapped_attributes,computation_rule,
                            entity_class,source_type,confidence,pattern_language,metadata,created_by)
                           VALUES (%s::uuid,%s,%s,%s,%s,%s,'seeded',%s,%s,%s,%s)
                           RETURNING pattern_id""",
                        (target_tenant, pattern_text, semantic_concept,
                         Json(payload.get("mapped_attributes") or {}), payload.get("computation_rule"),
                         entity_class, float(payload.get("confidence", 0.9)), pattern_language,
                         metadata, deployed_by),
                    )
                    pattern_id = int(cursor.fetchone()[0])
                for synonym in payload.get("synonyms", []):
                    cursor.execute(
                        """INSERT INTO pattern_synonyms
                           (tenant_id,pattern_id,synonym_text,language,semantic_distance,match_type)
                           VALUES (%s::uuid,%s,%s,%s,%s,%s)
                           ON CONFLICT (tenant_id,pattern_id,synonym_text,language) DO NOTHING""",
                        (target_tenant, pattern_id, synonym["synonym_text"].strip().lower(),
                         str(synonym.get("language") or pattern_language).strip().lower(),
                         float(synonym.get("semantic_distance", 0.0)),
                         str(synonym.get("match_type") or "exact")),
                    )
        elif asset["kind"] == "semantic_term":
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO semantic_terms
                       (tenant_id,kind,canonical_name,term_text,language,source_type,metadata)
                       VALUES (%s::uuid,%s,%s,%s,%s,'manual',%s)
                       ON CONFLICT (tenant_id,kind,canonical_name,term_text,language) DO UPDATE SET
                         metadata=EXCLUDED.metadata, source_type='manual', modified_at=NOW()
                       WHERE semantic_terms.metadata->>'managed_by'='ikos_extension_package'
                         AND semantic_terms.metadata->>'extension_key'=%s
                         AND semantic_terms.metadata->>'asset_key'=%s RETURNING term_id""",
                    (target_tenant, payload["kind"], payload["canonical_name"].strip().lower(), payload["term_text"].strip(),
                     str(payload.get("language") or "und").strip().lower(), metadata, extension_key, asset["key"]),
                )
                if cursor.fetchone() is None:
                    raise ValueError(f"Ownership changed while promoting semantic term {asset['key']}; inspect again")
        elif asset["kind"] == "query_term_alias":
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO query_term_alias
                       (tenant_id,alias_text,canonical_name,kind,language,priority,source_type,metadata,is_active,created_by)
                       VALUES (%s::uuid,%s,%s,%s,%s,%s,'manual',%s,TRUE,%s)
                       ON CONFLICT (tenant_id,alias_text,kind,language) DO UPDATE SET
                         canonical_name=EXCLUDED.canonical_name, priority=EXCLUDED.priority,
                         metadata=EXCLUDED.metadata, is_active=TRUE, modified_at=NOW()
                       WHERE query_term_alias.metadata->>'managed_by'='ikos_extension_package'
                         AND query_term_alias.metadata->>'extension_key'=%s
                         AND query_term_alias.metadata->>'asset_key'=%s RETURNING alias_id""",
                    (target_tenant, payload["alias_text"].strip(), payload["canonical_name"].strip().lower(), payload["kind"],
                     str(payload.get("language") or "und").strip().lower(), payload.get("priority", 100), metadata,
                     deployed_by, extension_key, asset["key"]),
                )
                if cursor.fetchone() is None:
                    raise ValueError(f"Ownership changed while promoting query alias {asset['key']}; inspect again")
        elif asset["kind"] == "ingestion_trigger":
            if target_scope != "tenant":
                raise ValueError("ingestion_trigger assets are tenant-scoped only")
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO ikos_ingestion_trigger
                       (tenant_id,trigger_key,workflow_key,document_types,is_active,metadata)
                       VALUES (%s::uuid,%s,%s,%s,%s,%s)
                       ON CONFLICT (tenant_id,trigger_key) DO UPDATE SET
                         workflow_key=EXCLUDED.workflow_key, document_types=EXCLUDED.document_types,
                         is_active=EXCLUDED.is_active, metadata=EXCLUDED.metadata, modified_at=NOW()
                       WHERE ikos_ingestion_trigger.metadata->>'managed_by'='ikos_extension_package'
                         AND ikos_ingestion_trigger.metadata->>'extension_key'=%s
                         AND ikos_ingestion_trigger.metadata->>'asset_key'=%s
                       RETURNING trigger_key""",
                    (target_tenant, _workflow_key(extension_key, asset["key"]),
                     _workflow_key(extension_key, payload["workflow_asset_key"]), payload["document_types"],
                     payload.get("is_active", True), metadata, extension_key, asset["key"]),
                )
                if cursor.fetchone() is None:
                    raise ValueError(f"Ownership changed while promoting ingestion trigger {asset['key']}; inspect again")
        else:
            workflow_key = _workflow_key(extension_key, asset["key"])
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO solf_workflow_registry
                       (tenant_id,configuration_scope,workflow_key,workflow_name,description,domain,status,metadata,is_active,created_by)
                       VALUES (%s::uuid,%s,%s,%s,%s,%s,'published',%s,TRUE,%s)
                       ON CONFLICT (tenant_id,configuration_scope,workflow_key) DO UPDATE SET
                         workflow_name=EXCLUDED.workflow_name, description=EXCLUDED.description,
                         domain=EXCLUDED.domain, status='published', metadata=EXCLUDED.metadata,
                         is_active=TRUE, modified_at=NOW()
                       WHERE solf_workflow_registry.metadata->>'managed_by'='ikos_extension_package'
                         AND solf_workflow_registry.metadata->>'extension_key'=%s
                         AND solf_workflow_registry.metadata->>'asset_key'=%s
                       RETURNING workflow_id""",
                    (target_tenant, target_scope, workflow_key, payload["workflow_name"].strip(),
                     str(payload.get("description") or "").strip(), str(payload.get("domain") or "").strip() or None,
                     metadata, deployed_by, extension_key, asset["key"]),
                )
                workflow_row = cursor.fetchone()
                if workflow_row is None:
                    raise ValueError(f"Ownership changed while promoting workflow {asset['key']}; inspect again")
                workflow_id = int(workflow_row[0])
                cursor.execute(
                    """UPDATE solf_workflow_versions SET is_active=FALSE,
                       metadata=COALESCE(metadata,'{}'::jsonb) || %s::jsonb, modified_at=NOW()
                       WHERE tenant_id=%s::uuid AND workflow_id=%s AND is_active=TRUE""",
                    (Json({"status": "superseded"}), target_tenant, workflow_id),
                )
                cursor.execute(
                    """SELECT COALESCE(MAX(version_no),0)+1 FROM solf_workflow_versions
                       WHERE tenant_id=%s::uuid AND workflow_id=%s""",
                    (target_tenant, workflow_id),
                )
                version_no = int(cursor.fetchone()[0])
                cursor.execute(
                    """INSERT INTO solf_workflow_versions
                       (tenant_id,configuration_scope,workflow_id,version_no,graph_spec,input_contract,
                        output_contract,metadata,is_active,created_by)
                       VALUES (%s::uuid,%s,%s,%s,%s,%s,%s,%s,TRUE,%s)
                       RETURNING workflow_version_id""",
                    (target_tenant, target_scope, workflow_id, version_no,
                     Json(payload.get("graph_spec") if isinstance(payload.get("graph_spec"), dict) else {}),
                     Json(payload.get("input_contract") if isinstance(payload.get("input_contract"), dict) else {}),
                     Json(payload.get("output_contract") if isinstance(payload.get("output_contract"), dict) else {}),
                     Json({**asset_metadata, "status": "published"}), deployed_by),
                )
                workflow_version_id = int(cursor.fetchone()[0])
                for index, step in enumerate(payload["steps"], start=1):
                    clause_name = None
                    clause_id = None
                    step_kind = "python_binding" if step.get("step_kind", "clause") == "capability" else "clause"
                    if step_kind == "clause":
                        clause_name = str(step["clause_name"]).strip()
                        cursor.execute(
                            """SELECT clause_id FROM solf_clauses
                               WHERE tenant_id=%s::uuid AND configuration_scope=%s AND clause_name=%s AND is_active=TRUE
                               ORDER BY clause_id DESC LIMIT 1""",
                            (target_tenant, target_scope, clause_name),
                        )
                        clause_row = cursor.fetchone()
                        if clause_row is None:
                            raise ValueError(f"Workflow {asset['key']} references unavailable clause '{clause_name}'")
                        clause_id = int(clause_row[0])
                    cursor.execute(
                        """INSERT INTO solf_workflow_steps
                           (tenant_id,configuration_scope,workflow_version_id,step_order,step_key,step_kind,
                            clause_id,clause_name,input_class,output_class,operation,config)
                           VALUES (%s::uuid,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (target_tenant, target_scope, workflow_version_id, index,
                         str(step.get("step_key") or f"step_{index}").strip().lower(),
                         step_kind, clause_id, clause_name,
                         str(step.get("input_class") or "").strip() or None,
                         str(step.get("output_class") or "").strip() or None,
                         str(step.get("operation") or "").strip() or None,
                         Json(step.get("config") if isinstance(step.get("config"), dict) else {})),
                    )

    package = _package_version_row(connection, extension_key, version)
    with connection.cursor() as cursor:
        cursor.execute(
            """INSERT INTO ikos_extension_deployment
               (package_version_id,target_scope,target_tenant_id,package_sha256,status,change_summary,deployed_by)
               VALUES (%s,%s,%s::uuid,%s,'applied',%s,%s)
               ON CONFLICT (package_version_id,target_scope,target_tenant_id) DO UPDATE SET
                 status='applied',package_sha256=EXCLUDED.package_sha256,
                 change_summary=EXCLUDED.change_summary,deployed_by=EXCLUDED.deployed_by,deployed_at=NOW()
               RETURNING deployment_id""",
            (package["package_version_id"], target_scope, target_tenant, plan["package_sha256"],
             Json(plan["summary"]), deployed_by),
        )
        deployment_id = int(cursor.fetchone()[0])
    return {**plan, "deployment_id": deployment_id, "idempotent": False}
