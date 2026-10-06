"""Domain packages are declarative: all persistence here uses a recording fake."""
from __future__ import annotations

from copy import deepcopy

import psycopg2
import pytest
from pydantic import ValidationError

import extension_packages as packages
from capability_registry import CapabilityRegistry


TENANT = "11111111-1111-1111-1111-111111111111"


def manifest():
    return {
        "schema_version": "1", "extension_key": "accounts", "version": "1.0.0",
        "display_name": "Accounting domain",
        # Deliberately put the trigger before its workflow.
        "assets": [
            {"kind": "ingestion_trigger", "key": "incoming", "payload": {
                "workflow_asset_key": "book", "document_types": ["invoice", "bill", "receipt"],
            }},
            {"kind": "workflow", "key": "book", "payload": {
                "workflow_name": "Book invoice", "steps": [{
                    "step_key": "post", "step_kind": "capability", "config": {
                        "capability_key": "fake.post", "capability_version": "1.0.0",
                        "payload_fields": {"amount": "invoice.amount"}, "payload": {"currency": "CHF"},
                        "save_as": "booking", "compensation_capability": {
                            "key": "fake.reverse", "version": "2.0.0",
                            "payload_fields": {"transaction_id": "booking.transaction_id"},
                            "payload": {"reason": "workflow undo"},
                        },
                    },
                }],
            }},
        ],
    }


def config(data):
    return data["assets"][1]["payload"]["steps"][0]["config"]


class RecordingConnection:
    def __init__(self):
        self.calls = []
        self.trigger_owner = None
        self.workflow_owner = None
        self.fail_trigger_write = False
        self.fail_workflow_write = False
        self.prior = None
        self.clause_available = True
        self.tenant_active = True

    def cursor(self):
        return RecordingCursor(self)


class RecordingCursor:
    def __init__(self, connection):
        self.connection = connection
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        sql = " ".join(sql.split())
        conn = self.connection
        conn.calls.append((sql, params))
        self.row = None
        if sql.startswith("SELECT set_config"):
            return
        if sql.startswith("SELECT status FROM tenant"):
            self.row = ("active" if conn.tenant_active else "disabled",)
        elif sql.startswith("SELECT metadata FROM ikos_ingestion_trigger"):
            self.row = None if conn.trigger_owner is None else (conn.trigger_owner,)
        elif sql.startswith("SELECT metadata FROM solf_workflow_registry"):
            self.row = None if conn.workflow_owner is None else (conn.workflow_owner,)
        elif sql.startswith("SELECT metadata FROM solf_clauses"):
            self.row = None
        elif sql.startswith(("SELECT 1 FROM solf_clauses", "SELECT clause_id FROM solf_clauses")):
            self.row = (17,) if conn.clause_available else None
        elif sql.startswith("SELECT deployment_id FROM ikos_extension_deployment"):
            self.row = conn.prior
        elif sql.startswith("SELECT package_id FROM ikos_extension_package"):
            self.row = (1,)
        elif sql.startswith("SELECT package_version_id, sha256"):
            self.row = None
        elif sql.startswith("SELECT COALESCE(MAX(version_no)"):
            self.row = (1,)
        elif sql.startswith("INSERT INTO ikos_extension_package_version"):
            self.row = (2, "now")
        elif sql.startswith("INSERT INTO ikos_ingestion_trigger"):
            self.row = None if conn.fail_trigger_write else (params[1],)
        elif sql.startswith("INSERT INTO solf_workflow_registry"):
            self.row = None if conn.fail_workflow_write else (3,)
        elif sql.startswith("INSERT INTO"):
            self.row = (4,)
        elif sql.startswith("UPDATE solf_workflow_versions"):
            return
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self):
        return self.row


@pytest.fixture(autouse=True)
def forbid_real_database(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("These tests must never connect to a database")
    monkeypatch.setattr(psycopg2, "connect", forbidden)


@pytest.fixture
def registry(monkeypatch):
    registry = CapabilityRegistry()
    monkeypatch.setattr(packages, "CAPABILITIES", registry)
    return registry


def register(registry, key="fake.post", version="1.0.0", input_schema=None, permission="fake.write", **kwargs):
    def never_execute(payload, caller):
        raise AssertionError("Package import/inspection/promotion must not execute capabilities")
    return registry.register(
        key=key, version=version, handler=never_execute, permission=permission,
        input_schema={"type": "object"} if input_schema is None else input_schema,
        output_schema={"type": "object"}, **kwargs,
    )


def stored(monkeypatch, data):
    validated = packages.ExtensionManifest.model_validate(data)
    row = {"manifest": packages.canonical_manifest(validated),
           "sha256": packages.manifest_sha256(validated), "package_version_id": 2}
    monkeypatch.setattr(packages, "_package_version_row", lambda *args: deepcopy(row))
    return row


def inspect(conn):
    return packages.inspect_package(conn, extension_key="accounts", version="1.0.0",
                                    target_scope="tenant", target_tenant_id=TENANT)


def promote(conn, row):
    return packages.promote_package(conn, extension_key="accounts", version="1.0.0",
                                    expected_sha256=row["sha256"], target_scope="tenant",
                                    target_tenant_id=TENANT, deployed_by="author")


def test_authoring_and_import_do_not_require_or_inspect_registrations(registry, monkeypatch):
    def forbidden(*args):
        raise AssertionError("Authoring must not inspect the runtime registry")
    monkeypatch.setattr(registry, "inspect_dependencies", forbidden)
    data = manifest()
    validated = packages.ExtensionManifest.model_validate(data)
    assert packages.canonical_manifest(validated)["assets"][1]["payload"]["steps"][0]["step_kind"] == "capability"
    conn = RecordingConnection()
    imported = packages.import_package(conn, data, "author")
    assert imported["status"] == "validated"
    assert not any("ingestion_trigger" in sql or "workflow_registry" in sql for sql, _ in conn.calls)


def test_missing_forward_and_compensation_block_inspection_and_promotion(registry, monkeypatch):
    row = stored(monkeypatch, manifest())
    conn = RecordingConnection()
    plan = inspect(conn)
    assert plan["missing_capabilities"] == [
        {"key": "fake.post", "version": "1.0.0"}, {"key": "fake.reverse", "version": "2.0.0"},
    ]
    assert not plan["can_promote"] and plan["missing_clauses"] == []
    assert all(item["capability"] is None for item in plan["capability_dependencies"])
    with pytest.raises(ValueError, match="unavailable dependencies"):
        promote(conn, row)
    assert not any(sql.startswith("INSERT") for sql, _ in conn.calls)
    assert not any("solf_clauses" in sql for sql, _ in conn.calls)


def test_exact_registration_metadata_and_inverse_dependencies(registry, monkeypatch):
    data = manifest()
    config(data)["compensation_capability"]["key"] = "fake.declared-inverse"
    config(data)["compensation_capability"]["version"] = "3.0.0"
    stored(monkeypatch, data)
    register(registry, version="1.0.1")  # Must not satisfy the exact requested version.
    register(registry, key="fake.reverse", version="2.0.0")
    assert inspect(RecordingConnection())["missing_capabilities"] == [
        {"key": "fake.declared-inverse", "version": "3.0.0"}, {"key": "fake.post", "version": "1.0.0"},
    ]
    metadata = register(registry, inverse_key="fake.declared-inverse", inverse_version="3.0.0")
    plan = inspect(RecordingConnection())
    assert not plan["can_promote"]
    assert plan["missing_capabilities"] == [{"key": "fake.declared-inverse", "version": "3.0.0"}]
    assert plan["capability_validation_errors"] == []
    assert next(item["capability"] for item in plan["capability_dependencies"] if item["key"] == "fake.post") == metadata
    register(registry, key="fake.declared-inverse", version="3.0.0",
             inverse_key="fake.post", inverse_version="1.0.0")  # Dependency cycles terminate.
    plan = inspect(RecordingConnection())
    assert plan["can_promote"]
    assert all(item["capability"]["input_schema"] == {"type": "object"} for item in plan["capability_dependencies"])


def test_inspection_reports_registered_permissions_without_granting_them(registry, monkeypatch):
    stored(monkeypatch, manifest())
    register(registry, permission="fake.write")
    register(registry, key="fake.reverse", version="2.0.0", permission="fake.reverse.write")
    inventory_before = registry.inventory()

    plan = inspect(RecordingConnection())

    assert plan["required_permissions"] == ["fake.reverse.write", "fake.write"]
    assert registry.inventory() == inventory_before
    assert plan["can_promote"]


def test_invalid_literal_payload_schema_is_aggregated_and_blocks_promotion(registry, monkeypatch):
    data = manifest()
    config(data)["payload"].update({"currency": "CHF", "timestamp": "not-a-date"})
    register(registry, input_schema={
        "type": "object",
        "properties": {
            "currency": {"type": "string", "enum": ["USD"]},
            "timestamp": {"type": "string", "format": "date"},
        },
    })
    register(registry, key="fake.reverse", version="2.0.0")
    stored(monkeypatch, data)

    plan = inspect(RecordingConnection())

    assert not plan["can_promote"]
    errors = plan["capability_validation_errors"]
    forward = next(item for item in errors if item["role"] == "forward")
    assert forward["capability_key"] == "fake.post"
    assert any("currency" in message and "USD" in message for message in forward["errors"])
    assert any("timestamp" in message and "date" in message for message in forward["errors"])


def test_mapped_payload_checks_literal_fields_and_known_required_fields(registry, monkeypatch):
    data = manifest()
    config(data)["payload"].pop("currency")
    register(registry, input_schema={
        "type": "object",
        "required": ["amount", "currency", "idempotency_key"],
        "properties": {
            "amount": {"type": "number"},
            "currency": {"type": "string"},
            "idempotency_key": {"type": "string"},
        },
    })
    register(registry, key="fake.reverse", version="2.0.0")
    stored(monkeypatch, data)

    plan = inspect(RecordingConnection())

    assert not plan["can_promote"]
    forward = next(item for item in plan["capability_validation_errors"] if item["role"] == "forward")
    assert any("currency" in message for message in forward["errors"])
    assert not any("amount" in message or "idempotency_key" in message for message in forward["errors"])
    assert plan["capability_validation_deferred"][0]["mapped_fields"] == ["amount"]


def test_mapped_runtime_types_are_deferred_not_claimed_valid(registry, monkeypatch):
    register(registry, input_schema={
        "type": "object",
        "required": ["amount", "currency", "idempotency_key"],
        "properties": {
            "amount": {"type": "number"},
            "currency": {"type": "string", "enum": ["CHF"]},
            "idempotency_key": {"type": "string"},
        },
    })
    register(registry, key="fake.reverse", version="2.0.0")
    stored(monkeypatch, manifest())

    plan = inspect(RecordingConnection())

    assert plan["can_promote"]
    assert plan["capability_validation_errors"] == []
    deferred = next(item for item in plan["capability_validation_deferred"] if item["role"] == "forward")
    assert deferred["mapped_fields"] == ["amount"]
    assert "runtime types" in deferred["reason"]


def test_configured_compensation_must_match_registered_inverse_metadata(registry, monkeypatch):
    stored(monkeypatch, manifest())
    register(registry, inverse_key="fake.declared-inverse", inverse_version="3.0.0")
    register(registry, key="fake.reverse", version="2.0.0")
    register(registry, key="fake.declared-inverse", version="3.0.0")

    plan = inspect(RecordingConnection())

    assert not plan["can_promote"]
    assert any("does not match the registered inverse" in message
               for error in plan["capability_validation_errors"] for message in error["errors"])


def test_promotion_normalizes_capabilities_and_orders_owned_triggers_last(registry, monkeypatch):
    register(registry)
    register(registry, key="fake.reverse", version="2.0.0")
    data = manifest()
    data["assets"][0]["payload"]["is_active"] = False
    row = stored(monkeypatch, data)
    conn = RecordingConnection()
    result = promote(conn, row)
    assert result["can_promote"] and result["deployment_id"] == 4
    step_sql, step_params = next((sql, params) for sql, params in conn.calls if sql.startswith("INSERT INTO solf_workflow_steps"))
    assert step_params[0] == TENANT
    assert step_params[5:8] == ("python_binding", None, None)
    assert step_params[-1].adapted == config(data)
    assert step_sql.count("%s") == len(step_params)
    assert not any("solf_clauses" in sql for sql, _ in conn.calls)
    trigger_index = next(i for i, (sql, _) in enumerate(conn.calls) if sql.startswith("INSERT INTO ikos_ingestion_trigger"))
    step_index = next(i for i, (sql, _) in enumerate(conn.calls) if sql.startswith("INSERT INTO solf_workflow_steps"))
    assert trigger_index > step_index
    sql, params = conn.calls[trigger_index]
    assert params[:5] == (TENANT, "ext.accounts.incoming", "ext.accounts.book", ["invoice", "bill", "receipt"], False)
    assert params[5].adapted == {
        "managed_by": "ikos_extension_package", "extension_key": "accounts", "extension_version": "1.0.0",
        "asset_key": "incoming", "asset_sha256": packages._asset_content_hash(data["assets"][0]),
    }
    assert "ON CONFLICT (tenant_id,trigger_key)" in sql
    for field in ("managed_by", "extension_key", "asset_key"):
        assert f"ikos_ingestion_trigger.metadata->>'{field}'" in sql
    assert params[-2:] == ("accounts", "incoming")
    assert not any("CREATE TABLE" in sql or "configuration_scope" in sql for sql, _ in conn.calls if "ikos_ingestion_trigger" in sql)


@pytest.mark.parametrize("owner", [
    {}, {"managed_by": "other", "extension_key": "accounts", "asset_key": "incoming"},
    {"managed_by": "ikos_extension_package", "extension_key": "other", "asset_key": "incoming"},
    {"managed_by": "ikos_extension_package", "extension_key": "accounts", "asset_key": "other"},
])
def test_trigger_ownership_conflicts_block_promotion(owner, registry, monkeypatch):
    register(registry)
    register(registry, key="fake.reverse", version="2.0.0")
    row = stored(monkeypatch, manifest())
    conn = RecordingConnection()
    conn.trigger_owner = owner
    plan = inspect(conn)
    assert plan["changes"][0]["action"] == "conflict" and not plan["can_promote"]
    with pytest.raises(ValueError, match="conflicts"):
        promote(conn, row)
    assert not any(sql.startswith("INSERT") for sql, _ in conn.calls)


def test_owned_trigger_hash_actions_and_tenant_reference(registry, monkeypatch):
    data = manifest()
    stored(monkeypatch, data)
    conn = RecordingConnection()
    owner = {"managed_by": "ikos_extension_package", "extension_key": "accounts", "asset_key": "incoming"}
    conn.trigger_owner = owner
    assert inspect(conn)["changes"][0]["action"] == "update"
    owner["asset_sha256"] = packages._asset_content_hash(data["assets"][0])
    change = inspect(conn)["changes"][0]
    assert change["action"] == "unchanged"
    assert change["workflow_key"] == "ext.accounts.book" and change["missing_workflows"] == []
    assert all(params == (TENANT, "ext.accounts.incoming") for sql, params in conn.calls
               if sql.startswith("SELECT metadata FROM ikos_ingestion_trigger"))


def test_bundled_workflow_ownership_conflict_blocks_trigger(registry, monkeypatch):
    register(registry)
    register(registry, key="fake.reverse", version="2.0.0")
    row = stored(monkeypatch, manifest())
    conn = RecordingConnection()
    conn.workflow_owner = {}
    assert not inspect(conn)["can_promote"]
    with pytest.raises(ValueError, match="conflicts"):
        promote(conn, row)


@pytest.mark.parametrize("target", ["trigger", "workflow"])
def test_promotion_ownership_race_fails_closed(target, registry, monkeypatch):
    register(registry)
    register(registry, key="fake.reverse", version="2.0.0")
    row = stored(monkeypatch, manifest())
    conn = RecordingConnection()
    setattr(conn, f"fail_{target}_write", True)
    with pytest.raises(ValueError, match="Ownership changed"):
        promote(conn, row)
    assert not any(sql.startswith("INSERT INTO ikos_extension_deployment") for sql, _ in conn.calls)


def test_package_hash_and_prior_deployment_guards(registry, monkeypatch):
    register(registry)
    register(registry, key="fake.reverse", version="2.0.0")
    row = stored(monkeypatch, manifest())
    conn = RecordingConnection()
    with pytest.raises(ValueError, match="content hash changed"):
        promote(conn, {**row, "sha256": "0" * 64})
    conn.prior = (42,)
    result = promote(conn, row)
    assert result["idempotent"] and result["deployment_id"] == 42
    assert not any(sql.startswith("INSERT") for sql, _ in conn.calls)


def test_global_trigger_scope_rejected_without_trigger_queries(registry, monkeypatch):
    data = manifest()
    validated = packages.ExtensionManifest.model_validate(data)
    with pytest.raises(ValueError, match="tenant-scoped"):
        packages._validate_scope_assets("global", validated)
    stored(monkeypatch, data)
    conn = RecordingConnection()
    with pytest.raises(ValueError, match="tenant-scoped"):
        packages.inspect_package(conn, extension_key="accounts", version="1.0.0",
                                 target_scope="global", target_tenant_id=None)
    assert not any("ikos_ingestion_trigger" in sql for sql, _ in conn.calls)


@pytest.mark.parametrize("value", [None, [], "invoice", [""], [None], ["other"], ["Invoice"], ["invoice", "invoice"]])
def test_trigger_requires_nonempty_supported_document_types(value):
    data = manifest()
    data["assets"][0]["payload"]["document_types"] = value
    with pytest.raises(ValidationError, match="document_types"):
        packages.ExtensionManifest.model_validate(data)


@pytest.mark.parametrize("field", ["workflow_key", "predicate", "predicates", "filter", "module", "tenant_id", "metadata"])
def test_trigger_rejects_non_declarative_fields(field):
    data = manifest()
    data["assets"][0]["payload"][field] = "untrusted"
    with pytest.raises(ValidationError, match="unsupported ingestion_trigger"):
        packages.ExtensionManifest.model_validate(data)


@pytest.mark.parametrize("reference", [None, "", "missing", "ext.accounts.book", "incoming", 12])
def test_trigger_must_reference_bundled_workflow(reference):
    data = manifest()
    data["assets"][0]["payload"]["workflow_asset_key"] = reference
    with pytest.raises(ValidationError, match="workflow"):
        packages.ExtensionManifest.model_validate(data)


def test_trigger_required_types_boolean_and_stable_key_length():
    data = manifest()
    del data["assets"][0]["payload"]["document_types"]
    with pytest.raises(ValidationError, match="document_types"):
        packages.ExtensionManifest.model_validate(data)
    data = manifest()
    data["assets"][0]["payload"]["is_active"] = "true"
    with pytest.raises(ValidationError, match="boolean"):
        packages.ExtensionManifest.model_validate(data)
    data = manifest()
    data["extension_key"] = "a" * 128
    data["assets"][0]["key"] = "b" * 128
    with pytest.raises(ValidationError, match="too long"):
        packages.ExtensionManifest.model_validate(data)


@pytest.mark.parametrize("field", ["module", "function", "functions", "source", "script_source", "script_id", "script_key",
                                  "generated_script_id", "generated_script_key", "entrypoint", "tenant_id", "permissions"])
@pytest.mark.parametrize("location", ["workflow", "step", "config", "inverse"])
def test_capability_rejects_binding_and_authority_selectors(field, location):
    data = manifest()
    workflow = data["assets"][1]["payload"]
    targets = {"workflow": workflow, "step": workflow["steps"][0], "config": config(data),
               "inverse": config(data)["compensation_capability"]}
    targets[location][field] = "untrusted"
    with pytest.raises(ValidationError):
        packages.ExtensionManifest.model_validate(data)


@pytest.mark.parametrize("authority", ["tenant_id", "user_id", "permissions", "security_context", "roles", "platform_admin"])
@pytest.mark.parametrize("location", ["literal", "mapped_target", "mapped_source", "inverse_literal", "inverse_mapping"])
def test_nested_payloads_and_mappings_cannot_forward_authority(authority, location):
    data = manifest()
    forward = config(data)
    inverse = forward["compensation_capability"]
    if location == "literal":
        forward["payload"]["nested"] = [{authority: "spoof"}]
    elif location == "mapped_target":
        forward["payload_fields"][authority] = "invoice.amount"
    elif location == "mapped_source":
        forward["payload_fields"]["other"] = f"nested.{authority}.value"
    elif location == "inverse_literal":
        inverse["payload"]["nested"] = {authority: "spoof"}
    else:
        inverse["payload_fields"]["other"] = authority
    with pytest.raises(ValidationError, match="authority"):
        packages.ExtensionManifest.model_validate(data)


@pytest.mark.parametrize("name,value", [
    ("capability_key", None), ("capability_key", ""), ("capability_key", " fake.post"),
    ("capability_version", "*"), ("capability_version", ">=1.0.0"), ("capability_version", 1),
    ("payload", []), ("payload_fields", []), ("payload_fields", {"amount": 1}),
    ("payload_fields", {"amount": ""}), ("payload_fields", {"amount": "invoice..amount"}),
    ("payload_fields", {"currency": "invoice.currency"}), ("payload", {"amount": float("nan")}),
    ("compensation_capability", None), ("compensation_capability", {}),
    ("compensation_capability", {"key": "fake.reverse", "version": "*"}),
])
def test_capability_config_structure_is_strict(name, value):
    data = manifest()
    config(data)[name] = value
    with pytest.raises(ValidationError):
        packages.ExtensionManifest.model_validate(data)


@pytest.mark.parametrize("value", [None, "", " booking", "nested.result", "tenant_id", "run_id", "status", "workflow_key", "execution_id", "_private"])
def test_save_as_cannot_overwrite_runtime_metadata(value):
    data = manifest()
    config(data)["save_as"] = value
    with pytest.raises(ValidationError, match="save_as"):
        packages.ExtensionManifest.model_validate(data)


def test_mixed_clause_and_capability_workflow_preserves_clause_lookup(registry, monkeypatch):
    register(registry)
    register(registry, key="fake.reverse", version="2.0.0")
    data = manifest()
    data["assets"][1]["payload"]["steps"].insert(0, {
        "step_key": "check", "clause_name": "invoice_policy", "config": {"mode": "review"},
    })
    row = stored(monkeypatch, data)
    conn = RecordingConnection()
    assert inspect(conn)["missing_clauses"] == []
    promote(conn, row)
    steps = [params for sql, params in conn.calls if sql.startswith("INSERT INTO solf_workflow_steps")]
    assert steps[0][5:8] == ("clause", 17, "invoice_policy")
    assert steps[0][-1].adapted == {"mode": "review"}
    assert steps[1][5:8] == ("python_binding", None, None)
    lookups = [params for sql, params in conn.calls if "FROM solf_clauses" in sql]
    assert all(params[-1] == "invoice_policy" for params in lookups)
    conn.clause_available = False
    plan = inspect(conn)
    assert plan["missing_clauses"] == ["invoice_policy"] and not plan["can_promote"]


def test_bundled_clause_still_satisfies_workflow_dependency(registry, monkeypatch):
    data = manifest()
    data["assets"][1]["payload"]["steps"] = [{"clause_name": "invoice_policy"}]
    data["assets"].append({"kind": "solf_clause", "key": "policy", "payload": {
        "clause_name": "invoice_policy", "clause_body": "invoice_policy(_context) ⦃ ↲(true) ⦄",
    }})
    row = stored(monkeypatch, data)
    conn = RecordingConnection()
    conn.clause_available = False
    plan = inspect(conn)
    assert plan["can_promote"] and plan["missing_clauses"] == [] and plan["missing_capabilities"] == []
    assert not any(sql.startswith("SELECT 1 FROM solf_clauses") for sql, _ in conn.calls)
    conn.clause_available = True
    promote(conn, row)
    inserts = [sql for sql, _ in conn.calls if sql.startswith("INSERT")]
    assert inserts.index(next(sql for sql in inserts if "INTO solf_clauses" in sql)) < inserts.index(next(sql for sql in inserts if "INTO solf_workflow_registry" in sql))


def test_invalid_step_kinds_and_clause_capability_smuggling():
    for kind in ("python_binding", "generated_script", "class_transform", None, 1):
        data = manifest()
        data["assets"][1]["payload"]["steps"][0]["step_kind"] = kind
        with pytest.raises(ValidationError):
            packages.ExtensionManifest.model_validate(data)
    data = manifest()
    step = data["assets"][1]["payload"]["steps"][0]
    step.update(step_kind="clause", clause_name="invoice_policy")
    with pytest.raises(ValidationError, match="requires step_kind"):
        packages.ExtensionManifest.model_validate(data)