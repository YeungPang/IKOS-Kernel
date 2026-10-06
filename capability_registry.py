"""Explicit, trusted Python capabilities; package data never loads executable code."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from threading import RLock
from typing import Any, Callable, Iterable, Mapping

from jsonschema import Draft202012Validator, FormatChecker

from security_context import SecurityContext, get_security_context


class UnknownCapabilityError(LookupError):
    """The requested key and exact version have not been registered."""


class DuplicateCapabilityError(ValueError):
    """A different registration already owns this key and version."""


def _identifier(value: str, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:+/-]*", value):
        raise ValueError(f"{label} must be a nonempty exact identifier (no ranges or wildcards)")
    return value


def _json_text(value: Any) -> str:
    """Accept JSON values only, without coercing keys or non-JSON containers."""
    def check(item: Any) -> None:
        if type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    raise TypeError("JSON object keys must be strings")
                check(child)
        elif type(item) is list:
            for child in item:
                check(child)
        elif type(item) not in (str, int, float, bool, type(None)):
            raise TypeError(f"Not a JSON value: {type(item).__name__}")

    check(value)
    return json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":"))


def _schema_text(schema: dict[str, Any] | bool) -> str:
    text = _json_text(schema)
    copied = json.loads(text)
    Draft202012Validator.check_schema(copied)

    def check_refs(node: Any) -> None:
        if isinstance(node, dict):
            if "$schema" in node and node["$schema"] != "https://json-schema.org/draft/2020-12/schema":
                raise ValueError("Only JSON Schema Draft 2020-12 is supported")
            for key, value in node.items():
                if key in ("$ref", "$dynamicRef", "$recursiveRef"):
                    if not isinstance(value, str) or not value.startswith("#"):
                        raise ValueError("Schema references must be local fragments; external retrieval is forbidden")
                check_refs(value)
        elif isinstance(node, list):
            for value in node:
                check_refs(value)

    check_refs(copied)
    return text


@dataclass(frozen=True)
class _Capability:
    key: str
    version: str
    handler: Callable[[Any, "CapabilityCaller"], Any]
    permission: str
    input_schema: str
    output_schema: str
    inverse_key: str | None
    inverse_version: str | None

    def describe(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "version": self.version,
            "permission": self.permission,
            "input_schema": json.loads(self.input_schema),
            "output_schema": json.loads(self.output_schema),
            "inverse": None if self.inverse_key is None else {
                "key": self.inverse_key, "version": self.inverse_version,
            },
        }


@dataclass(frozen=True)
class CapabilityCaller:
    """Request-bound caller. Every nested call rechecks the active authority."""

    _registry: "CapabilityRegistry"
    _active_context: SecurityContext
    security_context: SecurityContext

    @property
    def tenant_id(self) -> str:
        return self.security_context.tenant_id

    def call(self, key: str, version: str, payload: Any) -> Any:
        return self._registry._execute(key, version, payload, self)

    def _check_active(self) -> None:
        active = get_security_context(required=True)
        if active is not self._active_context or active != self.security_context:
            raise PermissionError("Capability caller is outside its original security context")


class CapabilityRegistry:
    """Process-local registry populated exclusively by trusted application code."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._capabilities: dict[tuple[str, str], _Capability] = {}

    def register(
        self, *, key: str, version: str,
        handler: Callable[[Any, CapabilityCaller], Any], permission: str,
        input_schema: dict[str, Any] | bool, output_schema: dict[str, Any] | bool,
        inverse_key: str | None = None, inverse_version: str | None = None,
    ) -> dict[str, Any]:
        """Register a callable; identical callable identity and metadata are idempotent."""
        _identifier(key, "key")
        _identifier(version, "version")
        _identifier(permission, "permission")
        if not callable(handler):
            raise TypeError("handler must be a trusted callable, not an import path or source")
        if (inverse_key is None) != (inverse_version is None):
            raise ValueError("inverse_key and inverse_version must be supplied together")
        if inverse_key is not None:
            _identifier(inverse_key, "inverse_key")
            _identifier(inverse_version, "inverse_version")
        capability = _Capability(
            key, version, handler, permission, _schema_text(input_schema),
            _schema_text(output_schema), inverse_key, inverse_version,
        )
        with self._lock:
            existing = self._capabilities.get((key, version))
            if existing is not None:
                # Do not invoke callable-defined equality during registration.
                if existing.handler is not handler or existing.describe() != capability.describe():
                    raise DuplicateCapabilityError(f"Capability already registered: {key}@{version}")
                return existing.describe()
            self._capabilities[(key, version)] = capability
        return capability.describe()

    def _resolve(self, key: str, version: str) -> _Capability:
        _identifier(key, "key")
        _identifier(version, "version")
        with self._lock:
            capability = self._capabilities.get((key, version))
        if capability is None:
            raise UnknownCapabilityError(f"Unknown capability: {key}@{version}")
        return capability

    def resolve(self, key: str, version: str) -> dict[str, Any]:
        """Inspect exact-version metadata without exposing the executable handler."""
        return self._resolve(key, version).describe()

    def inventory(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._capabilities[key].describe() for key in sorted(self._capabilities)]

    def inspect_dependencies(self, dependencies: Iterable[Mapping[str, str]]) -> list[dict[str, Any]]:
        """Inspect package requirements without importing, registering, or executing code."""
        requested = [(_identifier(item["key"], "key"), _identifier(item["version"], "version"))
                     for item in dependencies]
        with self._lock:
            return [{
                "key": key, "version": version,
                "available": (key, version) in self._capabilities,
                "capability": self._capabilities[(key, version)].describe()
                if (key, version) in self._capabilities else None,
            } for key, version in requested]

    def execute(self, key: str, version: str, payload: Any) -> Any:
        """Execute using only the actual active tenant and permissions, never payload authority."""
        active = get_security_context(required=True)
        if not isinstance(active, SecurityContext):
            raise PermissionError("An actual SecurityContext is required")
        if not isinstance(active.tenant_id, str) or not active.tenant_id.strip():
            raise PermissionError("An active tenant is required")
        if not isinstance(active.permissions, (set, frozenset)) or any(
            not isinstance(permission, str) for permission in active.permissions
        ):
            raise PermissionError("An explicit permission set is required")
        snapshot = replace(
            active, permissions=frozenset(active.permissions),
            workspace_ids=frozenset(active.workspace_ids), roles=frozenset(active.roles),
        )
        caller = CapabilityCaller(self, active, snapshot)
        return self._execute(key, version, payload, caller)

    def _execute(self, key: str, version: str, payload: Any, caller: CapabilityCaller) -> Any:
        caller._check_active()
        capability = self._resolve(key, version)
        if not caller.security_context.allows(capability.permission):
            raise PermissionError(f"Missing capability permission: {capability.permission}")
        validated_input = json.loads(_json_text(payload))
        Draft202012Validator(
            json.loads(capability.input_schema), format_checker=FormatChecker(),
        ).validate(validated_input)
        result = capability.handler(validated_input, caller)
        caller._check_active()
        serializable_result = json.loads(_json_text(result))
        Draft202012Validator(
            json.loads(capability.output_schema), format_checker=FormatChecker(),
        ).validate(serializable_result)
        return serializable_result


# Populated by the web application's trusted startup code, never package data.
CAPABILITIES = CapabilityRegistry()