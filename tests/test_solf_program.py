"""Real bundled parser/interpreter contracts, with no DB/network effects."""
from __future__ import annotations

import importlib
import logging
import re
import socket
import urllib.request
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg2
import pytest
import httpx
import requests

import solf_parser
from solf_interpreter import SOLFInterpreter
from solf_program import BASELINE_PATH, PROGRAM_PATH, expand_bundled_program, read_program


CLASSES = {
    "entity", "person", "organization", "company", "document", "email", "event",
    "meeting", "location", "venue", "workflow_case", "approval",
}
MAPS = {
    "workflow_case_status_by_ref", "workflow_case_sla_due_by_ref",
    "approval_decision_by_ref", "approval_escalation_level_by_ref",
}


@pytest.fixture(autouse=True)
def no_external_effects(monkeypatch):
    reject = Mock(side_effect=AssertionError("SOLF program tests must not access DB/network"))
    monkeypatch.setattr(psycopg2, "connect", reject)
    monkeypatch.setattr(socket, "create_connection", reject)
    # Leave Windows asyncio's internal socketpair bootstrap alone; reject the
    # actual HTTP/LLM/vector clients instead of breaking event-loop construction.
    monkeypatch.setattr(urllib.request, "urlopen", reject)
    monkeypatch.setattr(requests.sessions.Session, "request", reject)
    monkeypatch.setattr(httpx.Client, "send", reject)
    monkeypatch.setattr(httpx.AsyncClient, "send", reject)
    # The real interpreter uses transaction callbacks even for pure clauses.
    adapters = SimpleNamespace(
        create_savepoint=Mock(return_value=False),
        release_savepoint=reject,
        rollback_to_savepoint=reject,
        db_ingest=Mock(side_effect=lambda payload: {"stub": "ingest", "payload": payload}),
        db_update=Mock(side_effect=lambda payload: {"stub": "update", "payload": payload}),
        db_delete=Mock(side_effect=lambda payload: {"stub": "delete", "payload": payload}),
        get_entity_id=Mock(side_effect=lambda reference: reference),
        merge=Mock(side_effect=lambda first, second: {"canonical": first, "duplicate": second}),
    )
    monkeypatch.setattr(SOLFInterpreter, "_python_extension_modules", {
        "solf_function": adapters, "domain_function": None,
    })
    yield adapters


def make_interpreter():
    interpreter = SOLFInterpreter()
    interpreter.set_parser(SimpleNamespace(parse=solf_parser.parse_script))
    interpreter.set_debug(False)
    # Policy consumers use existential/first-success evaluation, not all solutions.
    interpreter._stop_on_first = True
    return interpreter


@pytest.fixture
def program():
    interpreter = make_interpreter()
    interpreter.load_program_script(read_program(), clear_existing=True)
    return interpreter


def test_full_program_parses_without_missing_definitions(program):
    assert set(program.objects) == CLASSES
    assert set(program.facts) == CLASSES | MAPS
    declared = re.findall(r"(?m)^([A-Za-z_]\w*)\([^\n]*?\)\s*⦃", read_program())
    assert sum(len(items) for items in program.clauses.values()) == len(declared)
    assert set(program.clauses) == set(declared)
    for definitions in program.clauses.values():
        assert all(item["parsed_body"] is not None for item in definitions)
    for definition in program.objects.values():
        parents = definition.get("extends", [])
        assert set([parents] if isinstance(parents, str) else parents) <= CLASSES


def test_loading_class_method_literals_has_no_adapter_effects(program, no_external_effects):
    for action in ("ingest", "update", "delete"):
        getattr(no_external_effects, f"db_{action}").assert_not_called()
        assert isinstance(program.objects["entity"][action], tuple)


def test_dictionary_values_evaluate_once_and_methods_remain_callable():
    interpreter = make_interpreter()
    compute = Mock(return_value=7)
    interpreter.predefined_functions["compute"] = compute
    assert interpreter.execute_predicate(solf_parser.parse_script("{value: compute()}")) == {"value": 7}
    compute.assert_called_once_with()
    compute.reset_mock()
    # Legacy flat dictionary AST shape still evaluates values once.
    assert interpreter.execute_predicate(("ℳ", ["value", ("compute", [])])) == {"value": 7}
    compute.assert_called_once_with()
    interpreter.load_program_script("base ≔ {objectType: class, value: 7, get_value: ⦃↲(self.value)⦄}\ninstance ≔ {objectType: base}")
    assert interpreter.execute_predicate(solf_parser.parse_script("instance.get_value()")) == 7


@pytest.mark.parametrize("source", [None, "solf_script.txt", "kernel_script.txt", PROGRAM_PATH, BASELINE_PATH])
def test_bundled_paths_ignore_cwd(source, monkeypatch, tmp_path, program):
    expected = read_program()
    monkeypatch.chdir(tmp_path)
    # Decoy filenames in cwd must not shadow repository-owned bare names.
    (tmp_path / "solf_script.txt").write_text("decoy ≔ {}", encoding="utf-8")
    assert read_program(source) == expected
    interpreter = make_interpreter()
    interpreter.load_program_file(str(source or "solf_script.txt"), clear_existing=True)
    assert interpreter.clauses == program.clauses
    assert interpreter.objects == program.objects


def test_raw_entry_point_and_legacy_baseline_do_not_diverge(program):
    raw = PROGRAM_PATH.read_text(encoding="utf-8")
    baseline = BASELINE_PATH.read_text(encoding="utf-8")
    assert read_program().startswith(baseline)
    assert "entity ≔" not in raw
    interpreter = make_interpreter()
    interpreter.load_program_script(raw, clear_existing=True)
    assert interpreter.objects == program.objects
    assert interpreter.clauses == program.clauses
    # Unmarked text never receives an implicit baseline or arbitrary imports.
    assert expand_bundled_program("# include: other.txt\nx ≔ {}") == "# include: other.txt\nx ≔ {}"


def test_custom_paths_preserve_explicit_file_semantics(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    custom = tmp_path / "solf_script.txt"
    custom.write_text("custom ≔ {objectType: class}", encoding="utf-8")
    assert read_program(custom) == custom.read_text(encoding="utf-8")
    interpreter = make_interpreter()
    interpreter.load_program_file(str(custom))
    assert set(interpreter.objects) == {"custom"}
    with pytest.raises(FileNotFoundError):
        read_program(tmp_path / "missing" / "solf_script.txt")


@pytest.mark.parametrize("scope,method", [
    ("internal", "semantic_lookup"), ("external", "sql"), ("document", "rag"),
])
def test_category_routing(program, scope, method):
    result = program._invoke_clause(f"resolve_category_{scope}", [{}])
    assert result["access_method"] == method
    assert result["source_scope"] == ("document_kb" if scope == "document" else scope)


def test_matching_policies_are_neutral(program):
    person = program._invoke_clause("person_resolve_policy", [{}])
    assert person["threshold"] == 0.78
    assert person["strong_keys"] == ["email", "phone", "birth_date", "birth_place"]
    for name in ("organization", "company"):
        policy = program._invoke_clause(f"{name}_resolve_policy", [{}])
        assert policy["strong_keys"] == ["registration_no"]
        assert policy["medium_keys"] == ["legal_name", "address", "country"]
        assert "mandatory_existing_keys" not in policy
    assert program._invoke_clause("entity_resolve_policy", [{}])["threshold"] == 0.72


@pytest.mark.parametrize("action", ["ingest", "update", "delete"])
def test_generic_crud_and_merge_use_only_stubbed_adapters(program, no_external_effects, action):
    payload = {"class_name": "person", "name": "Alex Example"}
    result = program._invoke_clause(f"entity_{action}", [payload])
    assert result == {"stub": action, "payload": payload}
    getattr(no_external_effects, f"db_{action}").assert_called_once_with(payload)
    assert program._invoke_clause("merge_entities", [7, 8]) == {"canonical": 7, "duplicate": 8}


@pytest.mark.parametrize("name", [
    "can_update_entity", "can_reverse_mutation", "can_record_approval",
    "event_publish_policy", "event_subscription_policy", "scheduler_command_policy",
])
def test_generic_guards(program, name):
    assert program._invoke_clause(name, [{}])["allowed"] is True


def test_workflow_transition_and_scheduler_baseline_preserved(program):
    terminal = {"entity_type": "workflow_case", "to_state": "approved", "event": "manual", "actor_ref": "user"}
    assert program._invoke_clause("workflow_transition_policy", [terminal])["allowed"] is False
    terminal["event"] = "approval_recorded"
    assert program._invoke_clause("workflow_transition_policy", [terminal])["allowed"] is True
    terminal["actor_ref"] = "system"
    assert program._invoke_clause("workflow_transition_policy", [terminal])["allowed"] is False
    assert program._invoke_clause("scheduler_command", ["list_jobs", {}]) == {"command": "list_jobs", "args": {}}
    assert program._invoke_clause("query_style_policy", [{"style_hint": "criteria_list", "initial_action": "lookup_attribute"}])["initial_action"] == "search_criteria"


def test_generic_mutation_and_approval_plans_resolve(program):
    reverse = {"reference": "record-7", "dry_run": True}
    assert program._invoke_clause("action_plan", ["reverse_mutation", reverse]) == {
        "action": "reverse_mutation", "guard": "can_reverse_mutation", "args": reverse,
    }
    for payload in (
        {"entity_type": "person", "entity_id": 7, "field": "name", "new_value": "Alex"},
        {"entity_type": "person", "natural_key_field": "name", "natural_key_value": "A", "field": "name", "new_value": "Alex"},
    ):
        plan = program._invoke_clause("action_plan", ["update_entity", payload])
        sql = program._invoke_clause(plan["sql_builder"], [plan["args"]])
        assert sql["operation"] == "update"
        assert sql["values"] == {"name": "Alex"}
    payload = {"approval_no": "approval-7", "case_ref": "case-7", "approver_ref": "person-7", "decision": "approved", "decision_at": "2026-10-08", "comment": "", "escalation_level": 0}
    plan = program._invoke_clause("action_plan", ["record_approval", payload])
    assert plan["guard"] == "can_record_approval"
    assert program._invoke_clause(plan["sql_builder"], [plan["args"]])["table"] == "approvals"
    assert program._invoke_clause("approval_is_final", ["approved", 0]) is True
    assert program._invoke_clause("approval_is_final", ["pending", 0]) is False
    assert program._invoke_clause("workflow_case_is_overdue", ["open", 20, 10]) is True
    assert program._invoke_clause("workflow_case_is_overdue", ["closed", 20, 10]) is False


def test_generic_policy_maps_resolve_without_domain_seed_helpers(program):
    # Program-mode maps are promoted to facts; named index lookups use that
    # authoritative fact store, not the loader's original assignment snapshot.
    program.facts["workflow_case_status_by_ref"]["case-7"] = "open"
    program.facts["workflow_case_sla_due_by_ref"]["case-7"] = 10
    program.facts["approval_decision_by_ref"]["approval-7"] = "approved"
    program.facts["approval_escalation_level_by_ref"]["approval-7"] = 0
    assert program._invoke_clause("workflow_case_is_overdue_by_ref", ["case-7", 20]) is True
    assert program._invoke_clause("approval_is_final_by_ref", ["approval-7"]) is True


def test_removed_domains_and_unresolved_calls_are_absent(program):
    forbidden = re.compile(
        r"accounting|ledger|journal|invoice|payment|receipt|quotation|tax|vat|payroll|"
        r"employment|employee|child|allowance|social_contribution|crm|customer|supplier|"
        r"inventory|warehouse|product|sales_order|purchase_order|delivery|project|task|"
        r"budget|simulation|scenario|compliance|regression|adk_seed|reset_policy_maps"
    )
    assert not any(forbidden.search(name) for name in set(program.clauses) | set(program.facts))
    text = "\n".join(line for line in read_program().splitlines() if not line.lstrip().startswith("#"))
    assert not re.search(r"\b(customer_code|vat_no|tax_no|uid_che|mwst_no|eori_no)\b", text)
    calls = set(re.findall(r"\b([A-Za-z_]\w*)\s*\(", text))
    assert calls <= set(program.clauses) | {"db_ingest", "db_update", "db_delete", "get_entity_id", "merge"}
    for declarations in program.clauses.values():
        for declaration in declarations:
            for node in walk(declaration["parsed_body"]):
                if isinstance(node, tuple) and len(node) == 2 and isinstance(node[0], str) and re.fullmatch(r"[A-Za-z_]\w*", node[0]):
                    assert node[0] in calls


def walk(node):
    yield node
    if isinstance(node, (tuple, list)):
        for child in node:
            yield from walk(child)


def test_real_ingestion_initializer_and_class_discovery(monkeypatch, tmp_path):
    import ingest

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(ingest.business_rules, "load_active_business_rule_solf_script_chunks", Mock(return_value=([], [])))
    assert set(ingest.parse_solf_classes(read_program())) == CLASSES
    interpreter = ingest.build_solf_interpreter(PROGRAM_PATH.read_text(encoding="utf-8"))
    assert set(interpreter.objects) == CLASSES
    # Optional runtime-rule discovery failure must not suppress the bundled program.
    monkeypatch.setattr(ingest.business_rules, "load_active_business_rule_solf_script_chunks", Mock(side_effect=RuntimeError("offline")))
    assert set(ingest.build_solf_interpreter(read_program()).objects) == CLASSES
    monkeypatch.setattr(ingest.business_rules, "load_active_business_rule_solf_script_chunks", Mock(return_value=(['pre_rule(_x) ⦃↲(_x)⦄'], ['post_rule(_x) ⦃↲(_x)⦄'])))
    hydrated = ingest.build_solf_interpreter(PROGRAM_PATH.read_text(encoding="utf-8"))
    assert set(hydrated.objects) == CLASSES
    assert {"pre_rule", "post_rule"} <= set(hydrated.clauses)


def test_real_interaction_loaders_and_planner_symbols(monkeypatch, tmp_path):
    import interaction

    monkeypatch.chdir(tmp_path)
    # Do not initialize services, scheduler, LLM client, tables, or hydration.
    tools = object.__new__(interaction.IDMSInteractionTools)
    tools.logger = logging.getLogger("test.solf")
    interpreter = tools._build_solf_policy_interpreter()
    assert interpreter is not None and set(interpreter.objects) == CLASSES
    symbols = tools._get_solf_symbols_snapshot()
    assert symbols["available"] is True
    assert "entity_ingest" in symbols["clauses"]
    assert "person_resolve_policy" in symbols["clauses"]
    monkeypatch.setattr(interaction.business_rules, "list_solf_workflow_registry_entries", Mock(return_value=[]))
    actions = tools._registered_interaction_capabilities()["actions"]
    assert {"ingest_document", "update_entity", "record_approval", "reverse_mutation"} <= set(actions)
    assert "create_task" not in actions


@pytest.mark.parametrize("namespace", ["ikos_api_server", "idms_api_server"])
def test_schema_preview_uses_complete_bundle(namespace, monkeypatch, tmp_path):
    router = importlib.import_module(f"{namespace}.routers.schema_proposals")
    connection = Mock()
    monkeypatch.setattr(router.object_db, "get_connection", Mock(return_value=connection))
    monkeypatch.setattr(router.domain_db, "get_approved_solf_attribute_extensions", Mock(return_value={"person": ["email"]}))
    monkeypatch.chdir(tmp_path)
    assert router._solf_script_path() == PROGRAM_PATH
    result = router.get_solf_patch_draft("person")["result"]["classes"][0]
    assert result["class_exists_in_script"] is True
    assert result["missing_attributes"] == []
    assert "email" in result["existing_attributes"]
    connection.close.assert_called_once()


@pytest.mark.parametrize("namespace", ["ikos_api_server", "idms_api_server"])
def test_schema_audit_hash_covers_baseline_and_additions(namespace, monkeypatch, tmp_path):
    router = importlib.import_module(f"{namespace}.routers.schema_proposals")
    monkeypatch.setattr(router.object_db, "get_connection", Mock(return_value=Mock()))
    update = Mock(return_value={"batch_id": 7})
    monkeypatch.setattr(router.domain_db, "update_solf_schema_promotion_batch_audit_link", update)
    monkeypatch.chdir(tmp_path)
    router.link_schema_promotion_batch_audit(7, router.SolfSchemaPromotionBatchAuditLinkRequest())
    metadata = update.call_args.kwargs["audit_metadata"]
    assert metadata["solf_script_hash"] == router._sha256_text(read_program())
    assert metadata["solf_script_hash"] != router._sha256_text(PROGRAM_PATH.read_text(encoding="utf-8"))