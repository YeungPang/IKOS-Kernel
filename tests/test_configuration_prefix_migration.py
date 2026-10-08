"""Configuration contracts; no private dotenv reads, live DB, or network calls."""
from __future__ import annotations

import __future__
import ast
import importlib.util
import logging
import os
from pathlib import Path
import re
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, Mock, call

import pytest
from fastapi import HTTPException


ROOT = Path(__file__).resolve().parents[1]
LEGACY_PREFIX = "ID" + "MS_"
LEGACY_CONFIG = re.compile(r"\b" + LEGACY_PREFIX + r"[A-Z][A-Z0-9_]*")
EXCLUDED_DIRS = {".git", "generated", "__pycache__", ".pytest_cache", ".venv", "venv", "log"}


def test_no_legacy_uppercase_configuration_in_python():
    violations = []
    artifact = LEGACY_PREFIX + "SOLF_LLM_GENERATION_PACK.md"
    for path in ROOT.rglob("*.py"):
        if EXCLUDED_DIRS.intersection(path.relative_to(ROOT).parts):
            continue
        text = path.read_text(encoding="utf-8")
        # Only this exact persisted artifact filename is exempt, never env reads.
        tree = ast.parse(text, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in {"getenv", "get", "__getitem__"}:
                    for argument in node.args:
                        if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                            assert not LEGACY_CONFIG.search(argument.value), (path, node.lineno)
        for number, line in enumerate(text.splitlines(), 1):
            if LEGACY_CONFIG.search(line.replace(artifact, "")):
                violations.append(f"{path.relative_to(ROOT)}:{number}")
    assert not violations, violations
    assert not LEGACY_CONFIG.search("idms_connection")
    assert not LEGACY_CONFIG.search("IDMSInteractionTools")


def load_module(relative_path):
    """Load a fresh module without changing the application's cached modules."""
    spec = importlib.util.spec_from_file_location("_migration_test_module", ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_runtime_nodes(relative_path, names, **globals_):
    """Execute actual AST bodies in isolation from heavy module startup clients.

    Used only for pure helpers/import-time configuration in modules whose full
    import creates parser/LLM clients. No implementation is copied into tests.
    """
    tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
    selected = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in names:
            node.decorator_list = []
            selected.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in names for target in node.targets
        ):
            selected.append(node)
    assert len(selected) == len(names), (relative_path, names)
    namespace = {"os": os, "re": re, **globals_}
    module = ast.Module(body=selected, type_ignores=[])
    exec(compile(module, str(ROOT / relative_path), "exec", flags=__future__.annotations.compiler_flag), namespace)
    return SimpleNamespace(**namespace)


def test_notifier_new_environment(monkeypatch):
    values = {
        "NOTIFICATION_CHANNELS": "log,email,webhook", "SMTP_HOST": "smtp.invalid",
        "SMTP_PORT": "2525", "EMAIL_FROM": "sender@example.invalid",
        "EMAIL_USER": "test-user", "EMAIL_PASSWORD": "test-only-value",
        "WEBHOOK_URL": "https://example.invalid/notify",
    }
    for key, value in values.items():
        monkeypatch.setenv("IKOS_" + key, value)
        monkeypatch.setenv(LEGACY_PREFIX + key, "ignored")
    notifier = load_module("notifier.py")
    instance = notifier.create_notifier_from_config()
    email = instance.channels[notifier.NotificationChannel.EMAIL]
    assert (email.smtp_host, email.smtp_port, email.from_address, email.username, email.password) == (
        "smtp.invalid", 2525, "sender@example.invalid", "test-user", "test-only-value",
    )
    assert instance.channels[notifier.NotificationChannel.WEBHOOK].webhook_url == values["WEBHOOK_URL"]


def test_logging_new_environment(monkeypatch, tmp_path):
    module = load_module("runtime_logging.py")
    root = logging.Logger("isolated-migration-test")
    monkeypatch.setattr(module.logging, "getLogger", lambda: root)
    monkeypatch.setenv("IKOS_LOG_LEVEL", "WARNING")
    monkeypatch.setenv("IKOS_LOG_DIR", str(tmp_path))
    try:
        assert module.configure_logging() is root
        assert root.level == logging.WARNING
        handler = next(h for h in root.handlers if isinstance(h, module.DailyDatedFileHandler))
        assert handler.log_dir == tmp_path
    finally:
        for handler in root.handlers[:]:
            root.removeHandler(handler)
            handler.close()


def test_spacy_new_environment(monkeypatch):
    monkeypatch.setitem(sys.modules, "spacy", Mock())
    module = load_module("spacy_preparser.py")
    monkeypatch.setenv("IKOS_SPACY_MODEL", "custom_model")
    monkeypatch.setenv("IKOS_SPACY_STRICT_LANGUAGE", "true")
    monkeypatch.setenv("IKOS_SPACY_ALLOW_CROSS_LANGUAGE_FALLBACK", "false")
    assert module._model_candidates_for_language("de") == ["custom_model", "de_core_news_sm", "xx_ent_wiki_sm"]
    monkeypatch.setenv("IKOS_SPACY_ALLOW_CROSS_LANGUAGE_FALLBACK", "true")
    assert "en_core_web_sm" in module._model_candidates_for_language("de")


def test_transaction_matching_new_environment(monkeypatch):
    module = load_runtime_nodes("tx_match_index.py", {
        "_normalize_collection_name", "_resolve_default_collection", "_allow_collection_override",
        "_allowed_purposes", "is_allowed_purpose", "resolve_collection_name",
    }, Any=object, COLLECTION_NAME_REGEX=re.compile(r"^[A-Za-z0-9_\-]{3,96}$"),
        TX_MATCH_QDRANT_COLLECTION="unchanged_collection")
    monkeypatch.setenv("IKOS_MATCH_QDRANT_COLLECTION", "configured_collection")
    monkeypatch.setenv("IKOS_MATCH_ALLOW_COLLECTION_OVERRIDE", "false")
    monkeypatch.setenv("IKOS_MATCH_ALLOWED_PURPOSES", "invoice,receipt")
    assert module.resolve_collection_name("requested_collection") == "configured_collection"
    assert module.is_allowed_purpose("INVOICE")
    assert not module.is_allowed_purpose("transaction_match")
    monkeypatch.setenv("IKOS_MATCH_ALLOW_COLLECTION_OVERRIDE", "true")
    assert module.resolve_collection_name("requested_collection") == "requested_collection"


@pytest.mark.parametrize("namespace", ["idms_api_server", "ikos_api_server"])
def test_router_new_environment(monkeypatch, namespace):
    module = load_runtime_nodes(f"{namespace}/routers/ingestion.py", {
        "_is_truthy", "_require_maintenance_token", "get_ingestion_markdown_policy",
    }, HTTPException=HTTPException, Any=object)
    monkeypatch.setenv("IKOS_MAINTENANCE_TOKEN", "test-admin")
    monkeypatch.setenv("IKOS_INGESTION_MARKDOWN_RULE", "mparser")
    monkeypatch.setenv("IKOS_FORCE_PPARSER", "true")
    with pytest.raises(HTTPException) as failure:
        module._require_maintenance_token("wrong")
    assert failure.value.status_code == 403
    module._require_maintenance_token("test-admin")
    assert module.get_ingestion_markdown_policy()["policy"]["force_pparser"] is True
    monkeypatch.setenv("IKOS_FORCE_PPARSER", "false")
    assert module.get_ingestion_markdown_policy()["policy"]["effective_parser"] == "mparser"


def test_markdown_new_environment(monkeypatch):
    for key, value in {
        "OPENROUTER_BASE_URL": "https://example.invalid/v1/",
        "OPENROUTER_MD_VISION_MODEL": "test/vision",
        "OPENROUTER_MD_TEXT_MODEL": "test/text",
        "INGESTION_MARKDOWN_RULE": "custom", "LLAMAPARSE_TIMEOUT_SECONDS": "73",
        "FORCE_PPARSER": "true",
    }.items():
        monkeypatch.setenv("IKOS_" + key, value)
    module = load_runtime_nodes("md_gen.py", {
        "OPENROUTER_URL", "OPENROUTER_VISION_MODEL", "OPENROUTER_TEXT_MODEL",
        "INGESTION_MARKDOWN_RULE", "LLAMAPARSE_TIMEOUT_SECONDS", "_is_truthy", "_should_force_pparser",
    })
    assert module.OPENROUTER_URL == "https://example.invalid/v1/chat/completions"
    assert (module.OPENROUTER_VISION_MODEL, module.OPENROUTER_TEXT_MODEL) == ("test/vision", "test/text")
    assert module.LLAMAPARSE_TIMEOUT_SECONDS == 73
    assert module._should_force_pparser()
    assert not module._should_force_pparser("mparser")


@pytest.mark.parametrize("path,helper", [
    ("interaction.py", "_resolve_source_db_password"),
    ("query_engine.py", "_resolve_source_password"),
])
def test_dynamic_source_password_new_environment(monkeypatch, path, helper):
    module = load_runtime_nodes(path, {helper})
    resolve = getattr(module, helper)
    monkeypatch.setenv("IKOS_SOURCE_DB_PASSWORD_SALES_EU", "source-test-value")
    monkeypatch.setenv("IKOS_SOURCE_DB_PASSWORD", "shared-test-value")
    monkeypatch.setenv("IKOS_DB_PASSWORD", "central-test-value")
    monkeypatch.setenv(LEGACY_PREFIX + "SOURCE_DB_PASSWORD_SALES_EU", "ignored")
    assert resolve(None, "sales-eu") == "source-test-value"
    monkeypatch.delenv("IKOS_SOURCE_DB_PASSWORD_SALES_EU")
    assert resolve(None, "sales-eu") == "shared-test-value"
    monkeypatch.delenv("IKOS_SOURCE_DB_PASSWORD")
    assert resolve(None, "sales-eu") == "central-test-value"


@pytest.mark.parametrize("active_context", [False, True])
@pytest.mark.parametrize("password", ["", "mock-config-value"])
def test_sql_connection_uses_central_configuration_and_security(monkeypatch, active_context, password):
    config = ModuleType("ikos_config")
    values = dict(DB_NAME="central-db", DB_HOST="central-host.invalid", DB_USER="central-user",
                  DB_PASSWORD=password, DB_PORT="6543")
    for key, value in values.items():
        setattr(config, key, value)
        monkeypatch.setenv("IKOS_" + key, "must-not-be-read-again")
        monkeypatch.setenv(LEGACY_PREFIX + key, "ignored")
    monkeypatch.setitem(sys.modules, "ikos_config", config)
    module = load_module("sql_db.py")
    connection = MagicMock()
    connect = Mock(return_value=connection)
    monkeypatch.setattr(module.psycopg2, "connect", connect)
    context = SimpleNamespace(tenant_id="test-tenant", user_id="test-user",
                              permissions=frozenset({"z.read", "a.write"})) if active_context else None
    get_context = Mock(return_value=context)
    monkeypatch.setattr(module, "get_security_context", get_context)
    assert module.get_connection() is connection
    connect.assert_called_once_with(database=values["DB_NAME"], host=values["DB_HOST"],
                                   user=values["DB_USER"], password=password, port=values["DB_PORT"])
    get_context.assert_called_once_with(required=False)
    connection.cursor.return_value.__enter__.return_value.execute.assert_has_calls([
        call("SELECT set_config('ikos.tenant_id', %s, false)",
             ("test-tenant" if active_context else "00000000-0000-0000-0000-000000000001",)),
        call("SELECT set_config('ikos.user_id', %s, false)", ("test-user" if active_context else "",)),
        call("SELECT set_config('ikos.permissions', %s, false)", ("a.write,z.read" if active_context else "",)),
    ])