# Trusted domain capabilities

`capability_registry.py` provides an in-memory, process-local registry exposed
as `CAPABILITIES`. Trusted Python application/bootstrap code explicitly
registers callable objects into that registry. Package manifests can reference
capability keys and exact versions, but cannot register handlers, import
modules, supply executable source, or trigger code loading. Extension package
inspection checks these registrations as promotion dependencies; it does not
load or register them.

## Exact interface

```python
registry = CapabilityRegistry()
registry.register(
    *, key: str, version: str,
    handler: Callable[[Any, CapabilityCaller], Any], permission: str,
    input_schema: dict[str, Any] | bool,
    output_schema: dict[str, Any] | bool,
    inverse_key: str | None = None, inverse_version: str | None = None,
) -> dict[str, Any]
registry.execute(key: str, version: str, payload: Any) -> Any
registry.resolve(key: str, version: str) -> dict[str, Any]
registry.inventory() -> list[dict[str, Any]]
registry.inspect_dependencies(
    dependencies: Iterable[Mapping[str, str]],
) -> list[dict[str, Any]]

caller.tenant_id: str
caller.security_context: SecurityContext
caller.call(key: str, version: str, payload: Any) -> Any
```

The handler is called as `handler(validated_payload, caller)`. Handlers are
synchronous; async handlers and non-JSON results are not supported. Workflow
capability execution and extension dependency inspection use the module's
`CAPABILITIES` singleton; trusted startup code must register handlers into it
in every process that can inspect/promote or execute workflows. Independent
`CapabilityRegistry` instances are useful for isolation in tests. Identifiers
are case-sensitive and match
`[A-Za-z0-9][A-Za-z0-9_.:+/-]*`; versions are opaque exact identifiers, not
semantic-version ranges. `latest` has no special meaning or fallback.

Registration returns detached metadata with `key`, `version`, `permission`,
`input_schema`, `output_schema`, and `inverse` (null or `{key, version}`).
`resolve` and `inventory` return the same metadata, never handler references.
Inventory is sorted by key/version. Each dependency report contains `key`,
`version`, `available`, and `capability` (metadata or null). Inspection requires
no security context and executes nothing. It reports installed dependencies,
not whether a caller is authorized. For example a package can declare
`[{"key": "inventory.reserve", "version": "1.0.0"}]` for inspection; this is
data only, not an import specification.

## Authorization and context binding

Execution requires `security_context.get_security_context(required=True)` to
return an actual `SecurityContext` with a nonblank tenant and an explicit
set/frozenset of permission strings. It never uses the legacy tenant fallback or
accepts authority through arguments/payloads. The registered permission must be
allowed by that context. Explicit `*` permissions retain the existing
`SecurityContext.allows` policy; platform-admin flags and roles alone do not
bypass permission checks. A registration must name a concrete permission.

The handler receives a frozen snapshot of the active context (including user,
tenant, permissions, workspace IDs, roles and request provenance). Nested
`caller.call` requests resolve and authorize independently, with no elevation.
The original context must still be active and match the snapshot on each call
and after each handler returns. A caller cannot be reused after a context switch,
including a switch to a different but equal context or another tenant. The
registry does not set or reset the security context.

This is a boundary for **trusted** Python, not a sandbox for malicious Python.
Trusted handlers remain responsible for tenant-filtering their persistence,
transaction handling and side effects. No thread-safety is imposed on handlers.

## Validation, errors and lifecycle

Input/output use the already-installed `jsonschema` library and JSON Schema
Draft 2020-12. If `$schema` is present, it must be
`https://json-schema.org/draft/2020-12/schema`. Schema validation happens at
registration. Only local fragment references are allowed; external references
are rejected, so schema validation cannot fetch package-controlled URLs/files.
`FormatChecker` enforces formats it recognizes; unknown formats follow
JSON Schema's annotation behavior. Schemas are snapshotted and inspection
results cannot mutate the registered definitions.

Values must be native JSON objects with string keys, lists, strings, finite
numbers, booleans or null. Input is detached before validating/invoking the
handler. Output is detached, validated and returned as JSON-serializable data.
There is no coercion, custom serializer or Pydantic dependency in this interface.

- Missing context: existing `RuntimeError` from the security-context accessor.
- Missing tenant, permissions or original caller context: `PermissionError`.
- Unknown exact key/version: `UnknownCapabilityError` (`LookupError`).
- Conflicting duplicate: `DuplicateCapabilityError` (`ValueError`).
- Invalid schemas/data: native `jsonschema.SchemaError` / `ValidationError`.
- Invalid identifiers, external references, or incomplete inverse pairs:
  `ValueError`; non-JSON values/noncallable handlers: `TypeError`.
- Handler exceptions propagate unchanged; no exception is converted to success.

Permission checks happen before input validation or handler invocation. Output
validation happens after invocation; it cannot undo side effects. There is no
automatic rollback, retry or inverse execution. Inverse metadata identifies an
optional independently registered capability at an exact version. It can be
registered later; inspect its availability before use. The inverse requires its
own permission and caller-supplied valid input, not automatically transformed
posting output.

A lock protects registration, resolution and inventory/dependency snapshots.
The same callable **object identity** and identical metadata make registration
idempotent; every other duplicate key/version fails atomically. Different
versions coexist. Execution releases the lock before running handlers. There
is no replacement/unregister API, persisted state, or cross-process registry.

## Tests

`tests/test_capability_registry.py` uses isolated request contexts and in-memory
fake posting/reversal handlers only: no real finance, database or service calls.
It covers authorization, missing context/tenant, exact versions, unknown keys,
input/output schemas, format/local references, remote-reference rejection,
serialization, propagated errors, inventory dependencies, defensive copies,
context-bound nested calls, permission non-elevation, and concurrent registration.

Run from the kernel directory with the configured interpreter:

```text
python -m pytest tests/test_capability_registry.py -q
```