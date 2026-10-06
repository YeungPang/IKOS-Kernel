"""Reserved ingestion-run adapter and extension endpoint contracts; no live DB."""
from __future__ import annotations

from contextlib import AbstractContextManager
from unittest.mock import Mock

import psycopg2
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import ikos_api_server.deps as api_deps
import ikos_api_server.routers.extension_runtime as extension_runtime
import object_db
import workflow_pipeline_executor as pipeline
from security_context import SecurityContext, reset_security_context, set_security_context


TENANT = "11111111-1111-1111-1111-111111111111"
OTHER_TENANT = "22222222-2222-2222-2222-222222222222"
RUN_ID = 71
VERSION_ID = 19
IDEMPOTENCY_KEY = "ingestion:tenant-a:delivery-5"
INPUT_CONTEXT = {"doc_refs": [8], "document_id": 8, "idempotency_key": IDEMPOTENCY_KEY}
PERMISSIONS = frozenset({"documents.write", "workflows.run"})


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split())
        self.connection.statements.append((normalized, params))
        self.result = None
        if normalized.upper().startswith("UPDATE WORKFLOW_PIPELINE_RUN"):
            self.result = (RUN_ID,) if self.connection.claim_succeeds else None
        elif "FROM workflow_pipeline_run" in normalized and normalized.lstrip().upper().startswith("SELECT"):
            self.result = (self.connection.run_tenant,) if self.connection.tenant_matches else None

    def fetchone(self):
        return self.result


class FakeConnection(AbstractContextManager):
    def __init__(self, *, tenant_matches=True, claim_succeeds=True):
        self.tenant_matches = tenant_matches
        self.claim_succeeds = claim_succeeds
        self.run_tenant = TENANT
        self.statements = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def no_real_database(monkeypatch):
    monkeypatch.setattr(psycopg2, "connect", Mock(side_effect=AssertionError("tests must not connect to a database")))


@pytest.fixture
def caller():
    token = set_security_context(SecurityContext(TENANT, "user-a", PERMISSIONS))
    try:
        yield
    finally:
        reset_security_context(token)


def _run(status="pending", *, version=VERSION_ID, input_context=None):
    return {
        "run_id": RUN_ID,
        "workflow_version_id": version,
        "run_status": status,
        "input_context": dict(INPUT_CONTEXT if input_context is None else input_context),
        "current_context": {},
    }


@pytest.fixture
def executor_env(monkeypatch):
    connection = FakeConnection()
    run = _run()
    steps = [{"step_key": "ingest", "step_order": 1, "step_kind": "clause"}]
    run_lookup = Mock(side_effect=lambda *_args, **_kwargs: dict(run))
    step_lookup = Mock(side_effect=lambda *_args, **_kwargs: list(steps))
    create_run = Mock(side_effect=AssertionError("reserved adapter must not allocate another run"))
    link_documents = Mock()
    monkeypatch.setattr(object_db, "get_pipeline_run", run_lookup)
    monkeypatch.setattr(object_db, "list_solf_workflow_steps", step_lookup)
    monkeypatch.setattr(object_db, "create_pipeline_run", create_run)
    monkeypatch.setattr(object_db, "link_pipeline_run_documents", link_documents)
    executor = pipeline.WorkflowPipelineExecutor(lambda: connection)
    execute = Mock(return_value={"run_id": RUN_ID, "run_status": "completed"})
    executor._execute_run = execute
    return {
        "connection": connection,
        "run": run,
        "steps": steps,
        "run_lookup": run_lookup,
        "step_lookup": step_lookup,
        "create_run": create_run,
        "link_documents": link_documents,
        "executor": executor,
        "execute": execute,
    }


def _execute_reserved(env, *, run_id=RUN_ID, version_id=VERSION_ID,
                      input_context=None, idempotency_key=IDEMPOTENCY_KEY):
    return env["executor"].execute_ingestion_run(
        run_id=run_id,
        workflow_version_id=version_id,
        input_context=dict(INPUT_CONTEXT if input_context is None else input_context),
        idempotency_key=idempotency_key,
    )


@pytest.mark.parametrize("context", [
    None,
    SecurityContext("", "user-a", PERMISSIONS),
    SecurityContext(TENANT, "", PERMISSIONS),
    SecurityContext(TENANT, "user-a", frozenset({"workflows.run"})),
    SecurityContext(TENANT, "user-a", frozenset({"documents.write"})),
], ids=["absent", "missing-tenant", "missing-principal", "missing-documents-write", "missing-workflows-run"])
def test_executor_requires_explicit_tenant_principal_and_both_permissions(executor_env, context):
    token = set_security_context(context)
    try:
        with pytest.raises((PermissionError, RuntimeError)):
            _execute_reserved(executor_env)
    finally:
        reset_security_context(token)
    executor_env["run_lookup"].assert_not_called()
    executor_env["execute"].assert_not_called()


def test_executor_checks_run_tenant_with_explicit_tenant_filter(executor_env, caller):
    connection = executor_env["connection"]
    connection.tenant_matches = False
    connection.run_tenant = OTHER_TENANT
    with pytest.raises(PermissionError, match="tenant"):
        _execute_reserved(executor_env)
    tenant_selects = [
        (sql, params) for sql, params in connection.statements
        if sql.lstrip().upper().startswith("SELECT") and "workflow_pipeline_run" in sql
    ]
    assert tenant_selects, "adapter must verify the reserved run in SQL"
    assert all("tenant_id" in sql and TENANT in params for sql, params in tenant_selects)
    executor_env["execute"].assert_not_called()


@pytest.mark.parametrize("change", ["version", "input", "idempotency"])
@pytest.mark.parametrize("status", ["pending", "completed"])
def test_adapter_rejects_version_or_immutable_event_mismatch(executor_env, caller, change, status):
    executor_env["run"].update(_run(status))
    version = VERSION_ID
    context = dict(INPUT_CONTEXT)
    key = IDEMPOTENCY_KEY
    if change == "version":
        version = VERSION_ID + 1
    elif change == "input":
        context["document_id"] = 999
    else:
        key = "different-idempotency-key"

    with pytest.raises((PermissionError, ValueError)):
        _execute_reserved(executor_env, version_id=version, input_context=context, idempotency_key=key)
    executor_env["execute"].assert_not_called()
    assert not any("UPDATE workflow_pipeline_run" in sql for sql, _ in executor_env["connection"].statements)


def test_completed_matching_run_is_returned_without_replay(executor_env, caller):
    completed = _run("completed")
    completed["output_context"] = {"document_id": 8}
    executor_env["run"].update(completed)

    result = _execute_reserved(executor_env)

    assert result["run_status"] == "completed"
    executor_env["execute"].assert_not_called()
    executor_env["step_lookup"].assert_not_called()
    assert not any("UPDATE workflow_pipeline_run" in sql for sql, _ in executor_env["connection"].statements)


def test_running_run_is_not_replayed(executor_env, caller):
    executor_env["run"].update(_run("running"))
    with pytest.raises(ValueError, match="replay|pending|reconcil"):
        _execute_reserved(executor_env)
    executor_env["execute"].assert_not_called()
    executor_env["step_lookup"].assert_not_called()


def test_pending_run_without_steps_is_rejected_before_claim(executor_env, caller):
    executor_env["steps"].clear()
    with pytest.raises(ValueError, match="step"):
        _execute_reserved(executor_env)
    executor_env["step_lookup"].assert_called_once()
    executor_env["execute"].assert_not_called()
    assert not any("UPDATE workflow_pipeline_run" in sql for sql, _ in executor_env["connection"].statements)


def test_lost_atomic_claim_rejects_concurrent_executor(executor_env, caller):
    executor_env["connection"].claim_succeeds = False
    with pytest.raises((RuntimeError, ValueError)):
        _execute_reserved(executor_env)
    claim = [
        (sql, params) for sql, params in executor_env["connection"].statements
        if sql.lstrip().upper().startswith("UPDATE WORKFLOW_PIPELINE_RUN")
    ]
    assert len(claim) == 1
    sql, params = claim[0]
    assert "run_status = 'running'" in sql
    assert "run_status = 'pending'" in sql
    assert "run_id" in sql and "tenant_id" in sql and TENANT in params and RUN_ID in params
    assert "RETURNING run_id" in sql
    executor_env["execute"].assert_not_called()


def test_valid_reserved_run_claims_commits_and_executes_without_allocating(executor_env, caller):
    connection = executor_env["connection"]
    execute = executor_env["execute"]

    def execute_after_durable_claim(run_id, steps, context):
        assert run_id == RUN_ID
        assert steps == executor_env["steps"]
        assert context == INPUT_CONTEXT
        assert connection.commits >= 1, "claim must commit before workflow execution"
        return {"run_id": run_id, "run_status": "completed"}

    execute.side_effect = execute_after_durable_claim
    result = _execute_reserved(executor_env)

    assert result == {"run_id": RUN_ID, "run_status": "completed"}
    claim_sql = [sql for sql, _ in connection.statements if sql.lstrip().upper().startswith("UPDATE WORKFLOW_PIPELINE_RUN")]
    assert len(claim_sql) == 1
    assert "run_status = 'running'" in claim_sql[0] and "run_status = 'pending'" in claim_sql[0]
    assert "RETURNING run_id" in claim_sql[0]
    execute.assert_called_once_with(RUN_ID, executor_env["steps"], dict(INPUT_CONTEXT))
    executor_env["create_run"].assert_not_called()


def test_document_links_reuse_the_adapter_connection(executor_env, caller):
    context = {**INPUT_CONTEXT, "doc_refs": [8, 9]}
    executor_env["run"]["input_context"] = dict(context)
    _execute_reserved(executor_env, input_context=context)
    link = executor_env["link_documents"]
    if link.called:
        assert link.call_args.args[0] is executor_env["connection"]
        assert link.call_args.kwargs["run_id"] == RUN_ID
        assert link.call_args.kwargs["doc_refs"] == [8, 9]


def _api_client(monkeypatch, context):
    app = FastAPI()
    app.include_router(extension_runtime.router)
    app.dependency_overrides[api_deps.get_security_context] = lambda: context
    return TestClient(app)


def test_extension_inventory_and_execute_routes_are_permission_gated(monkeypatch):
    inventory = Mock(return_value=[{"key": "public.description"}])
    execute = Mock(return_value={"ok": True})
    monkeypatch.setattr(extension_runtime.CAPABILITIES, "inventory", inventory)
    monkeypatch.setattr(extension_runtime.CAPABILITIES, "execute", execute)

    denied_inventory = _api_client(monkeypatch, SecurityContext(TENANT, "user-a", frozenset({"workflows.run"})))
    assert denied_inventory.get("/api/extensions/capabilities").status_code == 403

    denied_execution = _api_client(
        monkeypatch, SecurityContext(TENANT, "user-a", frozenset({"tenant.configuration.manage"})),
    )
    response = denied_execution.post("/api/extensions/capabilities/execute", json={
        "key": "public.description", "version": "1.0.0", "payload": {},
    })
    assert response.status_code == 403
    execute.assert_not_called()

    authorized = _api_client(
        monkeypatch, SecurityContext(TENANT, "user-a", frozenset({"tenant.configuration.manage", "workflows.run"})),
    )
    response = authorized.get("/api/extensions/capabilities")
    assert response.status_code == 200
    assert response.json()["result"] == [{"key": "public.description"}]
    response = authorized.post("/api/extensions/capabilities/execute", json={
        "key": "public.description", "version": "1.0.0", "payload": {},
    })
    assert response.status_code == 200 and response.json()["result"] == {"ok": True}
    inventory.assert_called_once()
    execute.assert_called_once_with("public.description", "1.0.0", {})


def test_dispatch_route_requires_documents_write_before_opening_connection(monkeypatch):
    get_connection = Mock(side_effect=AssertionError("unauthorized dispatch must not open a connection"))
    monkeypatch.setattr(extension_runtime.object_db, "get_connection", get_connection)
    client = _api_client(monkeypatch, SecurityContext(TENANT, "user-a", frozenset({"workflows.run"})))
    response = client.post("/api/extensions/ingestion-deliveries/dispatch-one")
    assert response.status_code == 403
    get_connection.assert_not_called()


def test_dispatch_route_uses_executor_factory_and_always_closes_connection(monkeypatch):
    connection = FakeConnection()
    get_connection = Mock(return_value=connection)
    dispatch = Mock(return_value={"delivery_id": 5, "status": "completed", "run_id": RUN_ID})
    adapter = object()
    executor_type = Mock(return_value=adapter)
    monkeypatch.setattr(extension_runtime.object_db, "get_connection", get_connection)
    monkeypatch.setattr(extension_runtime, "dispatch_one", dispatch)
    monkeypatch.setattr(extension_runtime, "WorkflowPipelineExecutor", executor_type)
    # The route's factory must create a fresh executor configured with the shared DB factory.
    def dispatch_and_build(conn, factory):
        assert conn is connection
        assert factory() is adapter
        return {"delivery_id": 5, "status": "completed", "run_id": RUN_ID}
    dispatch.side_effect = dispatch_and_build

    client = _api_client(monkeypatch, SecurityContext(TENANT, "user-a", PERMISSIONS))
    response = client.post("/api/extensions/ingestion-deliveries/dispatch-one")

    assert response.status_code == 200
    assert response.json()["result"]["run_id"] == RUN_ID
    executor_type.assert_called_once_with(extension_runtime.object_db.get_connection)
    dispatch.assert_called_once()
    assert connection.closed


def test_retry_endpoint_if_registered_is_tenant_permission_gated(monkeypatch):
    retry_routes = [
        route for route in extension_runtime.router.routes
        if "retry" in route.path.lower() and "POST" in getattr(route, "methods", set())
    ]
    if not retry_routes:
        pytest.skip("extension runtime does not currently expose a retry endpoint")
    route = retry_routes[0]
    path = route.path
    for parameter in getattr(route, "param_convertors", {}):
        path = path.replace("{" + parameter + "}", "5")

    connection = FakeConnection()
    get_connection = Mock(return_value=connection)
    retry = Mock(return_value=True)
    monkeypatch.setattr(extension_runtime.object_db, "get_connection", get_connection)
    monkeypatch.setattr(extension_runtime, "retry_failed", retry, raising=False)

    missing_document_permission = _api_client(
        monkeypatch, SecurityContext(TENANT, "user-a", frozenset({"workflows.run"})),
    )
    response = missing_document_permission.post(path)
    assert response.status_code == 403

    authorized = _api_client(monkeypatch, SecurityContext(TENANT, "user-a", PERMISSIONS))
    response = authorized.post(path)
    assert response.status_code < 500
    assert connection.closed
