"""Generic workflow capability contracts; all persistence and domains are fake.

python_binding config uses capability_key, capability_version, payload_fields
(input -> context, including dotted dictionary paths), optional literal payload,
and save_as (default capability_result). compensation_capability uses key/version
and the same mapping/literals, evaluated against the original output_snapshot.
Mutating domain contracts should accept/require idempotency_key: the executor adds
run:{run_id}:step:{step_key}:capability only when the input schema accepts it.
"""

from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock
import sys

import pytest

import capability_registry
import mutation_undo as undo
import workflow_pipeline_executor as pipeline
from capability_registry import CapabilityRegistry
from security_context import SecurityContext, reset_security_context, set_security_context


def schema(properties, required=None):
    return {"type": "object", "properties": properties, "additionalProperties": False,
            "required": list(properties) if required is None else required}


@pytest.fixture
def env(monkeypatch):
    registry = CapabilityRegistry()
    # The application owns the singleton; do not install test handlers globally.
    monkeypatch.setattr(capability_registry, "CAPABILITIES", registry, raising=False)
    token = set_security_context(SecurityContext("tenant-a", "user-a", frozenset({"fake.post", "fake.reverse"})))
    journal, marks = [], []
    conn = object()

    def append(connection, **kwargs):
        assert connection is conn
        journal.append(deepcopy({"journal_id": len(journal) + 1, "status": "applied", **kwargs}))
        return len(journal)

    monkeypatch.setattr(pipeline.object_db, "append_workflow_mutation_journal", append)
    monkeypatch.setattr(undo.object_db, "get_workflow_mutation_journal_entry",
                        lambda connection, journal_id: deepcopy(journal[journal_id - 1]))
    monkeypatch.setattr(undo.object_db, "mark_workflow_mutation_undone", lambda connection, **kw: marks.append(kw))
    # Any generated-script lookup or dynamic import on this path is a test failure.
    monkeypatch.setattr(pipeline, "_load_generated_script_for_execution", Mock(side_effect=AssertionError("generated script lookup")))
    executor = pipeline.WorkflowPipelineExecutor(lambda: nullcontext(conn))
    try:
        yield SimpleNamespace(registry=registry, journal=journal, marks=marks, conn=conn, executor=executor)
    finally:
        reset_security_context(token)


def register(env, *, key="fake.post", version="1.0.0", permission="fake.post", handler=None, input_schema=None):
    handler = handler or (lambda payload, caller: {"transaction_id": "actual-42"})
    env.registry.register(key=key, version=version, permission=permission, handler=handler,
                          input_schema=input_schema if input_schema is not None else schema({"amount": {"type": "integer"}}),
                          output_schema=True)


def step(**config):
    return {"step_key": "post", "step_order": 1, "step_kind": "python_binding",
            "python_module": "never.import.this", "python_function": "unsafe",
            "config": {"capability_key": "fake.post", "capability_version": "1.0.0",
                       "payload_fields": {"amount": "amount"}, **config}}


def execute(env, definition=None, context=None, run_id=7):
    return env.executor._execute_step(definition or step(), context or {"amount": 12}, run_id=run_id)


def inverse_config():
    return {"key": "fake.reverse", "version": "2.0.0",
            "payload_fields": {"transaction_id": "capability_result.transaction_id"},
            "payload": {"reason": "workflow undo"}}


def register_reverse(env, calls):
    def reverse(payload, caller):
        calls.append((payload, caller.tenant_id))
        return {"ok": True, "reversed": payload["transaction_id"]}

    register(env, key="fake.reverse", version="2.0.0", permission="fake.reverse", handler=reverse,
             input_schema=schema({"transaction_id": {"type": "string"}, "reason": {"type": "string"}}))


def setup_run(env, monkeypatch, definition, snapshots, current=None):
    updates = []
    monkeypatch.setattr(pipeline.object_db, "get_pipeline_run", lambda *a: {
        "workflow_version_id": 3, "run_status": "completed", "current_context": current or {}})
    monkeypatch.setattr(pipeline.object_db, "get_pipeline_run_steps", lambda *a: deepcopy(snapshots))
    monkeypatch.setattr(pipeline.object_db, "list_solf_workflow_steps", lambda *a, **kw: [definition])
    monkeypatch.setattr(pipeline.object_db, "update_pipeline_run_status", lambda *a, **kw: updates.append(kw))
    return updates


def test_dispatch_precedes_generated_script_and_module_import(env, monkeypatch):
    register(env)
    monkeypatch.setattr(pipeline.importlib, "import_module", Mock(side_effect=AssertionError("dynamic import")))
    outcome = execute(env, step(script_key="untrusted", script_id=1))
    assert outcome.success
    assert outcome.output["capability_result"] == {"transaction_id": "actual-42"}
    assert env.journal[0]["operation_kind"] == "capability_execute"


def test_explicit_mapping_literals_and_result_isolation(env):
    received = []
    register(env, handler=lambda payload, caller: received.append((payload, caller.tenant_id)) or
             {"run_id": "spoof", "permissions": ["*"], "paused": True},
             input_schema=schema({"amount": {"type": "integer"}, "label": {"type": "string"}}))
    context = {"document": {"amount": 8}, "run_id": 7, "secret": "not forwarded", "tenant_id": "spoof"}
    outcome = execute(env, step(payload_fields={"amount": "document.amount"}, payload={"label": "literal"}, save_as="posting"), context)
    assert outcome.success and not outcome.paused
    assert received == [({"amount": 8, "label": "literal"}, "tenant-a")]
    assert outcome.output["run_id"] == 7
    assert outcome.output["posting"]["run_id"] == "spoof"
    assert context == {"document": {"amount": 8}, "run_id": 7, "secret": "not forwarded", "tenant_id": "spoof"}


@pytest.mark.parametrize("field", ["tenant_id", "user_id", "permissions"])
@pytest.mark.parametrize("mode", ["literal", "mapping", "nested", "source"])
def test_authority_overrides_rejected_before_handler(env, field, mode):
    handler = Mock(return_value={})
    register(env, handler=handler, input_schema=True)
    options = {"payload_fields": {}}
    if mode == "literal":
        options["payload"] = {field: "spoof"}
    elif mode == "mapping":
        options["payload_fields"] = {field: "amount"}
    elif mode == "nested":
        options["payload"] = {"nested": [{field: "spoof"}]}
    else:
        options["payload_fields"] = {"alias": field}
    outcome = execute(env, step(**options), {"amount": 1, field: "spoof"})
    assert not outcome.success
    handler.assert_not_called()
    assert env.journal == []


@pytest.mark.parametrize("name", ["run_id", "tenant_id", "permissions", "step_key", "workflow_key", "_runtime", "execution_metadata", "status", "paused", "next_step_key"])
def test_save_as_cannot_replace_metadata(env, name):
    handler = Mock(return_value={})
    register(env, handler=handler)
    assert not execute(env, step(save_as=name)).success
    handler.assert_not_called()


@pytest.mark.parametrize("changes", [
    {"payload_fields": {"amount": "missing"}}, {"payload_fields": []},
    {"payload": []}, {"payload": {"amount": 3}},
    {"capability_version": "latest"}, {"capability_key": "unknown"},
    {"capability_key": ""},
])
def test_invalid_config_fails_closed_without_legacy_fallback(env, changes):
    handler = Mock(return_value={})
    register(env, handler=handler)
    assert not execute(env, step(**changes)).success
    handler.assert_not_called()


def test_exact_post_permission_and_schema_enforced(env):
    handler = Mock(return_value={})
    register(env, handler=handler)
    token = set_security_context(SecurityContext("tenant-a", "user", frozenset({"fake.post.extra"}), platform_admin=True))
    try:
        assert not execute(env).success
    finally:
        reset_security_context(token)
    assert not execute(env, context={"amount": "not-an-integer"}).success
    handler.assert_not_called()


def test_idempotency_only_for_contract_accepting_field(env):
    calls = []
    handler = lambda payload, caller: calls.append(payload) or {}
    register(env, handler=handler)
    assert execute(env).success
    assert calls == [{"amount": 12}]
    register(env, version="2.0.0", handler=handler, input_schema=schema({
        "amount": {"type": "integer"}, "idempotency_key": {"type": "string"}}))
    definition = step(capability_version="2.0.0")
    assert execute(env, definition).success
    assert execute(env, definition).success
    assert calls[-1]["idempotency_key"] == calls[-2]["idempotency_key"] == "run:7:step:post:capability"
    assert execute(env, definition, run_id=8).success
    assert calls[-1]["idempotency_key"] != calls[-2]["idempotency_key"]


def test_fake_posting_and_journal_reversal_exact_version(env):
    register(env)
    calls = []
    register_reverse(env, calls)
    assert execute(env, step(compensation_capability=inverse_config())).success
    row = env.journal[0]
    assert row["inverse_action"] == {"kind": "capability", "key": "fake.reverse", "version": "2.0.0",
                                     "payload": {"transaction_id": "actual-42", "reason": "workflow undo"}}
    # Strings in a journal never select executable code.
    row["inverse_action"].update(module="evil", function="unsafe")
    result = undo.undo_journal_entry(env.conn, 1, requested_by="not-authority")
    assert result["ok"] and result["result"]["reversed"] == "actual-42"
    assert calls == [({"transaction_id": "actual-42", "reason": "workflow undo"}, "tenant-a")]
    assert env.marks[0]["status"] == "undone"


@pytest.mark.parametrize("dry_run", [False, True])
def test_inverse_permission_rechecked_for_current_caller(env, dry_run):
    register(env)
    calls = []
    register_reverse(env, calls)
    assert execute(env, step(compensation_capability=inverse_config())).success
    token = set_security_context(SecurityContext("tenant-a", "user", frozenset({"fake.post"})))
    try:
        result = undo.undo_journal_entry(env.conn, 1, dry_run=dry_run)
    finally:
        reset_security_context(token)
    assert not result["ok"] and not result["executable"]
    assert "fake.reverse" in result["reason"]
    assert calls == [] and env.marks == []


def test_journal_dry_run_does_not_execute_or_mark(env):
    register(env)
    calls = []
    register_reverse(env, calls)
    execute(env, step(compensation_capability=inverse_config()))
    assert undo.undo_journal_entry(env.conn, 1, dry_run=True)["status"] == "planned"
    assert calls == [] and env.marks == []


@pytest.mark.parametrize("dry_run", [False, True])
def test_pipeline_compensation_uses_original_snapshot_not_live_result(env, monkeypatch, dry_run):
    register(env)
    calls = []
    register_reverse(env, calls)
    definition = step(compensation_capability=inverse_config())
    posted = execute(env, definition)
    assert posted.success
    updates = setup_run(env, monkeypatch, definition, [{
        "step_key": "post", "step_order": 1, "step_status": "completed", "output_snapshot": posted.output}],
        current={"capability_result": {"transaction_id": "later-unrelated"}})
    result = env.executor.undo_pipeline_run(7, dry_run=dry_run)
    assert result["summary"]["planned" if dry_run else "compensated"] == 1
    if dry_run:
        assert calls == [] and updates == [] and len(env.journal) == 1
    else:
        assert calls[0][0]["transaction_id"] == "actual-42"
        assert env.journal[-1]["operation_kind"] == "compensation_execute"
        assert env.journal[-1]["inverse_action"] is None


def test_reverse_order_and_each_original_output(env, monkeypatch):
    calls = []
    register_reverse(env, calls)
    definition = step(compensation_capability=inverse_config())
    setup_run(env, monkeypatch, definition, [
        {"step_key": "post", "step_order": order, "step_status": "completed",
         "output_snapshot": {"capability_result": {"transaction_id": f"tx-{order}"}}}
        for order in [1, 2]])
    assert env.executor.undo_pipeline_run(7)["summary"]["compensated"] == 2
    assert [call[0]["transaction_id"] for call in calls] == ["tx-2", "tx-1"]


def test_missing_compensation_registration_not_executable(env, monkeypatch):
    env.journal.append({"journal_id": 1, "inverse_action": {
        "kind": "capability", "key": "missing", "version": "1.0.0", "payload": {}}})
    result = undo.undo_journal_entry(env.conn, 1, dry_run=True)
    assert not result["ok"] and not result["executable"]
    assert "capability" not in undo.get_supported_inverse_actions()
    definition = step(compensation_capability=inverse_config())
    setup_run(env, monkeypatch, definition, [{"step_key": "post", "step_order": 1,
              "step_status": "completed", "output_snapshot": {"capability_result": {"transaction_id": "42"}}}])
    assert env.executor.undo_pipeline_run(7, dry_run=True)["summary"]["failed"] == 1
    assert env.marks == [] and len(env.journal) == 1


def test_unavailable_inverse_prevents_original_call(env):
    handler = Mock(return_value={})
    register(env, handler=handler)
    assert not execute(env, step(compensation_capability=inverse_config())).success
    handler.assert_not_called()


def test_missing_output_snapshot_never_falls_back_to_live_context(env, monkeypatch):
    calls = []
    register_reverse(env, calls)
    setup_run(env, monkeypatch, step(compensation_capability=inverse_config()), [{
        "step_key": "post", "step_order": 1, "step_status": "completed", "output_snapshot": {}}],
        current={"capability_result": {"transaction_id": "spoof"}})
    assert env.executor.undo_pipeline_run(7)["summary"]["failed"] == 1
    assert calls == []


def test_failed_inverse_not_marked_undone(env):
    register(env, key="fake.reverse", permission="fake.reverse", input_schema=True,
             handler=lambda payload, caller: {"ok": False, "reason": "rejected"})
    env.journal.append({"journal_id": 1, "inverse_action": {
        "kind": "capability", "key": "fake.reverse", "version": "1.0.0", "payload": {}}})
    result = undo.undo_journal_entry(env.conn, 1)
    assert not result["ok"] and result["status"] == "failed"
    assert env.marks[0]["status"] == "undo_failed"


def test_legacy_handlers_preserved_but_unavailable_not_advertised(env, monkeypatch):
    monkeypatch.setitem(sys.modules, "domain_db", SimpleNamespace())
    assert "reverse_transaction" not in undo.get_supported_inverse_actions()
    env.journal.append({"journal_id": 1, "inverse_action": {"kind": "reverse_transaction", "transaction_id": "42"}})
    assert not undo.undo_journal_entry(env.conn, 1, dry_run=True)["executable"]
    reverse = Mock(return_value={"ok": True, "reversal_transaction_id": "undo-42"})
    monkeypatch.setitem(sys.modules, "domain_db", SimpleNamespace(reverse_transaction=reverse))
    assert "reverse_transaction" in undo.get_supported_inverse_actions()
    assert undo.undo_journal_entry(env.conn, 1)["ok"]
    reverse.assert_called_once()


def test_execute_run_stores_snapshot_and_capability_journal(env, monkeypatch):
    register(env)
    calls = []
    register_reverse(env, calls)
    definition = step(compensation_capability=inverse_config())
    states = []
    setup_run(env, monkeypatch, definition, [])
    monkeypatch.setattr(pipeline.object_db, "upsert_pipeline_run_step", lambda *a, **kw: states.append(deepcopy(kw)))
    env.executor._execute_run(7, [definition], {"amount": 12, "run_id": 7})
    assert states[-1]["step_status"] == "completed"
    assert states[-1]["output_snapshot"]["capability_result"]["transaction_id"] == "actual-42"
    assert [row["operation_kind"] for row in env.journal] == ["capability_execute", "step_output_merge"]
    assert env.journal[0]["inverse_action"]["payload"]["transaction_id"] == "actual-42"


@pytest.mark.parametrize("change", [
    {"version": "1.0.0"}, {"payload": {"transaction_id": "42", "permissions": ["*"]}},
    {"payload": {"transaction_id": 42, "reason": "undo"}},
])
def test_invalid_journal_inverse_never_executable(env, change):
    calls = []
    register_reverse(env, calls)
    action = {"kind": "capability", "key": "fake.reverse", "version": "2.0.0",
              "payload": {"transaction_id": "42", "reason": "undo"}, **change}
    env.journal.append({"journal_id": 1, "inverse_action": action})
    result = undo.undo_journal_entry(env.conn, 1, dry_run=True)
    assert not result["ok"] and not result["executable"]
    assert calls == [] and env.marks == []


def test_missing_singleton_does_not_fall_back_or_create_registry(env, monkeypatch):
    monkeypatch.delattr(capability_registry, "CAPABILITIES")
    assert not execute(env).success
    assert "capability" not in undo.get_supported_inverse_actions()
    env.journal.append({"journal_id": 1, "inverse_action": {
        "kind": "capability", "key": "fake.reverse", "version": "2.0.0", "payload": {}}})
    assert not undo.undo_journal_entry(env.conn, 1, dry_run=True)["executable"]
    assert env.marks == []


@pytest.mark.parametrize("dry_run", [False, True])
def test_pipeline_inverse_missing_permission_never_executes(env, monkeypatch, dry_run):
    calls = []
    register_reverse(env, calls)
    setup_run(env, monkeypatch, step(compensation_capability=inverse_config()), [{
        "step_key": "post", "step_order": 1, "step_status": "completed",
        "output_snapshot": {"capability_result": {"transaction_id": "42"}}}])
    token = set_security_context(SecurityContext("tenant-a", "user", frozenset({"fake.post"})))
    try:
        result = env.executor.undo_pipeline_run(7, dry_run=dry_run)
    finally:
        reset_security_context(token)
    assert result["summary"]["failed"] == 1 and not result["actions"][0]["executable"]
    assert calls == [] and env.journal == []


def test_no_active_caller_cannot_execute(env):
    handler = Mock(return_value={})
    register(env, handler=handler)
    token = set_security_context(None)
    try:
        assert not execute(env).success
    finally:
        reset_security_context(token)
    handler.assert_not_called()