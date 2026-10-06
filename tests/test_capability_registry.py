from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest
from jsonschema import SchemaError, ValidationError

from capability_registry import CapabilityRegistry, DuplicateCapabilityError, UnknownCapabilityError
from security_context import SecurityContext, get_security_context, reset_security_context, set_security_context


OBJECT = {"type": "object", "additionalProperties": False,
          "properties": {"value": {"type": "integer"}}, "required": ["value"]}


def echo(payload, caller):
    return payload


def register(registry, **changes):
    spec = dict(key="test.echo", version="1.0.0", handler=echo, permission="domain.post",
                input_schema=OBJECT, output_schema=OBJECT)
    spec.update(changes)
    return registry.register(**spec)


@pytest.fixture(autouse=True)
def no_ambient_context():
    token = set_security_context(None)
    try:
        yield
    finally:
        reset_security_context(token)


@pytest.fixture
def authorized():
    context = SecurityContext("tenant-a", "user-a", frozenset({"domain.post"}), request_id="request-a")
    token = set_security_context(context)
    try:
        yield context
    finally:
        reset_security_context(token)


def test_missing_context_is_fail_closed_even_with_payload_authority():
    registry = CapabilityRegistry()
    register(registry)
    with pytest.raises(RuntimeError, match="security context"):
        registry.execute("test.echo", "1.0.0", {"tenant_id": "fake", "permissions": ["*"]})


@pytest.mark.parametrize("context", [
    SecurityContext("", "user", frozenset({"domain.post"})),
    SecurityContext("   ", "user", frozenset({"domain.post"})),
    SecurityContext("tenant", "user"),
    SecurityContext("tenant", "user", platform_admin=True),
    SecurityContext("tenant", "user", roles=frozenset({"admin"})),
    SecurityContext("tenant", "user", permissions="domain.post"),
    {"tenant_id": "tenant", "permissions": ["*"]},
])
def test_tenant_and_permissions_fail_closed(context):
    registry = CapabilityRegistry()
    register(registry)
    token = set_security_context(context)
    try:
        with pytest.raises(PermissionError):
            registry.execute("test.echo", "1.0.0", {"value": 1})
    finally:
        reset_security_context(token)


def test_explicit_wildcard_matches_existing_security_context_policy(authorized):
    registry = CapabilityRegistry()
    register(registry)
    token = set_security_context(replace(authorized, permissions=frozenset({"*"})))
    try:
        assert registry.execute("test.echo", "1.0.0", {"value": 1}) == {"value": 1}
    finally:
        reset_security_context(token)


def test_exact_versions_and_unknown_capabilities(authorized):
    registry = CapabilityRegistry()
    register(registry)
    register(registry, version="2.0.0", handler=lambda payload, caller: {"value": 2})
    assert registry.execute("test.echo", "1.0.0", {"value": 1}) == {"value": 1}
    assert registry.execute("test.echo", "2.0.0", {"value": 1}) == {"value": 2}
    for key, version in [("unknown", "1.0.0"), ("test.echo", "1.0.1"), ("test.echo", "latest")]:
        with pytest.raises(UnknownCapabilityError):
            registry.execute(key, version, {"value": 1})
    for version in ["", "*", ">=1.0.0", "1.0.0 "]:
        with pytest.raises(ValueError):
            registry.execute("test.echo", version, {"value": 1})


@pytest.mark.parametrize("payload", [{}, {"value": "1"}, {"value": True}, {"value": 1, "tenant_id": "spoof"}])
def test_input_validation_precedes_handler(authorized, payload):
    registry = CapabilityRegistry()
    calls = []
    register(registry, handler=lambda value, caller: calls.append(value))
    with pytest.raises(ValidationError):
        registry.execute("test.echo", "1.0.0", payload)
    assert calls == []


def test_output_validation_and_handler_errors_propagate(authorized):
    registry = CapabilityRegistry()
    register(registry, handler=lambda value, caller: {"value": "bad"})
    with pytest.raises(ValidationError):
        registry.execute("test.echo", "1.0.0", {"value": 1})
    error = RuntimeError("domain rejected posting")

    def failing(value, caller):
        raise error

    register(registry, version="2.0.0", handler=failing)
    with pytest.raises(RuntimeError) as caught:
        registry.execute("test.echo", "2.0.0", {"value": 1})
    assert caught.value is error


@pytest.mark.parametrize("result", [object(), {1: "coerced"}, (1, 2), float("nan"), float("inf")])
def test_non_json_results_rejected(authorized, result):
    registry = CapabilityRegistry()
    register(registry, output_schema=True, handler=lambda value, caller: result)
    with pytest.raises((TypeError, ValueError)):
        registry.execute("test.echo", "1.0.0", {"value": 1})


def test_registration_validation_and_no_code_loading():
    registry = CapabilityRegistry()
    with pytest.raises(TypeError, match="trusted callable"):
        register(registry, handler="domain_module:post")
    with pytest.raises(ValueError, match="together"):
        register(registry, inverse_key="domain.reverse")
    with pytest.raises(ValueError):
        register(registry, permission="*")
    with pytest.raises(SchemaError):
        register(registry, input_schema={"type": "not-a-type"})
    for ref in ["https://example.invalid/schema", "file:///secret", "other.json#/x"]:
        with pytest.raises(ValueError, match="external retrieval"):
            register(registry, input_schema={"$ref": ref})


def test_local_schema_references_and_formats(authorized):
    registry = CapabilityRegistry()
    schema = {"$defs": {"value": OBJECT}, "$ref": "#/$defs/value"}
    register(registry, input_schema=schema)
    assert registry.execute("test.echo", "1.0.0", {"value": 3}) == {"value": 3}
    register(registry, version="2.0.0", input_schema={"type": "string", "format": "uuid"}, output_schema=True)
    with pytest.raises(ValidationError):
        registry.execute("test.echo", "2.0.0", "not-a-uuid")


def test_inventory_dependencies_and_schema_mutation_isolation():
    registry = CapabilityRegistry()
    schema = json.loads(json.dumps(OBJECT))
    metadata = register(registry, input_schema=schema, inverse_key="test.reverse", inverse_version="1.0.0")
    schema["type"] = "string"
    metadata["input_schema"]["type"] = "array"
    inventory = registry.inventory()
    assert inventory[0]["input_schema"]["type"] == "object"
    assert inventory[0]["inverse"] == {"key": "test.reverse", "version": "1.0.0"}
    assert "handler" not in inventory[0]
    report = registry.inspect_dependencies([
        {"key": "test.echo", "version": "1.0.0"},
        {"key": "test.echo", "version": "2.0.0"},
        {"key": "test.reverse", "version": "1.0.0"},
    ])
    assert [item["available"] for item in report] == [True, False, False]
    assert report[1]["capability"] is None
    inventory[0]["permission"] = "spoof"
    assert registry.resolve("test.echo", "1.0.0")["permission"] == "domain.post"


def test_duplicate_registration_is_idempotent_only_when_unchanged():
    registry = CapabilityRegistry()
    assert register(registry) == register(registry)
    for changes in [dict(handler=lambda value, caller: value), dict(permission="domain.read"),
                    dict(input_schema=True), dict(output_schema=True),
                    dict(inverse_key="test.reverse", inverse_version="1.0.0")]:
        with pytest.raises(DuplicateCapabilityError):
            register(registry, **changes)
    assert len(registry.inventory()) == 1


def test_concurrent_identical_registration_is_idempotent():
    registry = CapabilityRegistry()
    barrier = Barrier(8)

    def run(_):
        barrier.wait()
        return register(registry)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(run, range(8)))
    assert all(result == results[0] for result in results)
    assert len(registry.inventory()) == 1


def test_concurrent_conflicting_registration_has_one_winner():
    registry = CapabilityRegistry()
    barrier = Barrier(8)

    def run(number):
        barrier.wait()
        try:
            register(registry, permission=f"domain.permission{number}")
            return True
        except DuplicateCapabilityError:
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(run, range(8))) == 1


def test_nested_calls_cannot_elevate_permissions(authorized):
    registry = CapabilityRegistry()
    register(registry, key="domain.reverse", permission="domain.reverse")
    register(registry, handler=lambda payload, caller: caller.call("domain.reverse", "1.0.0", payload))
    with pytest.raises(PermissionError, match="domain.reverse"):
        registry.execute("test.echo", "1.0.0", {"value": 1})
    assert get_security_context() is authorized


def test_caller_cannot_be_reused_in_another_or_absent_context(authorized):
    registry = CapabilityRegistry()
    callers = []

    def capture(payload, caller):
        callers.append(caller)
        return payload

    register(registry, handler=capture)
    registry.execute("test.echo", "1.0.0", {"value": 1})
    for context in [replace(authorized, tenant_id="tenant-b"), replace(authorized), None]:
        token = set_security_context(context)
        try:
            with pytest.raises((PermissionError, RuntimeError)):
                callers[0].call("test.echo", "1.0.0", {"value": 2})
        finally:
            reset_security_context(token)


def test_context_change_during_handler_is_rejected(authorized):
    registry = CapabilityRegistry()
    tokens = []

    def change_context(payload, caller):
        tokens.append(set_security_context(replace(authorized, permissions=frozenset({"*"}))))
        return payload

    register(registry, handler=change_context)
    try:
        with pytest.raises(PermissionError, match="original security context"):
            registry.execute("test.echo", "1.0.0", {"value": 1})
    finally:
        reset_security_context(tokens[0])


def test_fake_domain_posting_and_explicit_reversal(authorized):
    registry = CapabilityRegistry()
    postings = {}
    receipt_schema = {"type": "object", "properties": {
        "posting_id": {"type": "string"}, "tenant_id": {"type": "string"},
        "reversed": {"type": "boolean"}},
        "required": ["posting_id", "tenant_id", "reversed"], "additionalProperties": False}

    def post(payload, caller):
        assert caller.security_context.user_id == authorized.user_id
        assert caller.security_context.request_id == authorized.request_id
        assert get_security_context() is authorized
        receipt = {"posting_id": "fake-1", "tenant_id": caller.tenant_id, "reversed": False}
        postings[(caller.tenant_id, receipt["posting_id"])] = receipt.copy()
        return receipt

    def reverse(payload, caller):
        receipt = postings[(caller.tenant_id, payload["posting_id"])]
        receipt["reversed"] = True
        return receipt

    register(registry, key="fake.post", handler=post, output_schema=receipt_schema,
             inverse_key="fake.reverse", inverse_version="1.0.0")
    register(registry, key="fake.reverse", handler=reverse, permission="domain.reverse",
             input_schema={"type": "object", "properties": {"posting_id": {"type": "string"}},
                           "required": ["posting_id"], "additionalProperties": False},
             output_schema=receipt_schema)
    receipt = registry.execute("fake.post", "1.0.0", {"value": 10})
    assert receipt["tenant_id"] == "tenant-a"
    with pytest.raises(PermissionError):
        registry.execute("fake.reverse", "1.0.0", {"posting_id": "fake-1"})
    assert not postings[("tenant-a", "fake-1")]["reversed"]
    token = set_security_context(replace(authorized, permissions=frozenset({"domain.reverse"})))
    try:
        reversed_receipt = registry.execute("fake.reverse", "1.0.0", {"posting_id": "fake-1"})
    finally:
        reset_security_context(token)
    assert reversed_receipt["reversed"] is True
    assert receipt["reversed"] is False  # returned results are detached JSON values
    assert json.loads(json.dumps(reversed_receipt)) == reversed_receipt