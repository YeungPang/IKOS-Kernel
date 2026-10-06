from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

import extension_packages
from scripts.package_ikos_extension import build_manifest


def _manifest() -> dict:
    return {
        "schema_version": "1",
        "extension_key": "laser-crm",
        "version": "1.2.0",
        "display_name": "Laser CRM",
        "description": "Domain vocabulary and policies",
        "assets": [
            {
                "kind": "solf_script",
                "key": "crm-core",
                "payload": {
                    "script": "(_aphotonix_extension_version ≔ 1)",
                },
            },
            {
                "kind": "semantic_term",
                "key": "cutting-application",
                "payload": {
                    "kind": "category",
                    "canonical_name": "laser_cutting",
                    "term_text": "laser cutting",
                    "language": "en",
                },
            },
            {
                "kind": "query_term_alias",
                "key": "customer-account",
                "payload": {
                    "alias_text": "OEM customer",
                    "canonical_name": "customer",
                    "kind": "entity_type",
                    "language": "en",
                    "priority": 50,
                },
            },
        ],
    }


def test_manifest_validates_supported_assets_and_is_canonical() -> None:
    manifest = extension_packages.ExtensionManifest.model_validate(_manifest())
    canonical = extension_packages.canonical_manifest(manifest)
    assert canonical["extension_key"] == "laser-crm"
    assert len(extension_packages.manifest_sha256(manifest)) == 64


def test_example_folder_builds_a_self_contained_manifest() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest = build_manifest(root / "extensions" / "examples" / "minimal")
    assert manifest["extension_key"] == "example-laser-domain"
    script = next(item for item in manifest["assets"] if item["kind"] == "solf_script")
    assert "script_file" not in script["payload"]
    assert "laser_application" in script["payload"]["script"]


def test_core_knowledge_package_contains_common_generic_patterns_and_terms() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest = build_manifest(root / "extensions" / "core-knowledge" / "1.0.0")
    assert manifest["extension_key"] == "ikos-core-knowledge"
    kinds = [asset["kind"] for asset in manifest["assets"]]
    assert "semantic_term" in kinds
    assert "query_term_alias" in kinds
    assert "semantic_pattern" in kinds
    patterns = [asset["payload"] for asset in manifest["assets"] if asset["kind"] == "semantic_pattern"]
    age = next(pattern for pattern in patterns if pattern["semantic_concept"] == "age")
    assert age["mapped_attributes"] == {"age": "birth_date"}
    assert all(
        synonym["match_type"] == "template"
        for pattern in patterns
        for synonym in pattern.get("synonyms", [])
        if "*" in synonym["synonym_text"]
    )


def test_core_knowledge_110_contains_domain_neutral_linguistic_lexicon() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest = build_manifest(root / "extensions" / "core-knowledge" / "1.1.0")
    assert manifest["extension_key"] == "ikos-core-knowledge"
    assert manifest["version"] == "1.1.0"
    terms = [asset["payload"] for asset in manifest["assets"] if asset["kind"] == "semantic_term"]
    categories = {term["canonical_name"] for term in terms}
    expected = {
        "core_temporal_point", "core_temporal_interval", "core_temporal_boundary",
        "core_temporal_recurrence", "core_quantitative_aggregation",
        "core_quantitative_comparator", "core_spatial_structure",
        "core_spatial_coordinate", "core_interrogative", "core_inquiry_directive",
        "core_inquiry_modality", "core_document_type", "core_document_provenance",
        "core_document_processing_state", "core_workflow_state",
        "core_workflow_directive", "core_workflow_governance",
        "core_logic_connective", "core_logic_quantifier", "core_logic_set_operation",
        "core_conversation_etiquette", "core_conversation_clarification",
        "core_system_meta_command",
    }
    assert expected <= categories
    assert len(terms) >= 150
    assert all(";" not in term["term_text"] for term in terms)
    phrases = {term["term_text"] for term in terms}
    assert {"yesterday", "fiscal year", "greater than", "bounding box", "whose",
            "provenance", "awaiting input", "unless", "could you clarify", "audit trail"} <= phrases


def test_inbox_reader_rejects_path_traversal_and_symlinks(tmp_path: Path) -> None:
    manifest = _manifest()
    (tmp_path / "release.json").write_text(json.dumps(manifest), encoding="utf-8")
    assert extension_packages.load_manifest_from_inbox("release.json", tmp_path)["extension_key"] == "laser-crm"
    with pytest.raises(ValueError, match="simple file name"):
        extension_packages.load_manifest_from_inbox("..\\outside.json", tmp_path)
    link = tmp_path / "linked.json"
    try:
        link.symlink_to(tmp_path / "release.json")
    except (OSError, NotImplementedError):
        pytest.skip("Symlink creation is not available for this test account")
    with pytest.raises(ValueError, match="symbolic links"):
        extension_packages.load_manifest_from_inbox("linked.json", tmp_path)


def test_manifest_hash_is_independent_of_json_key_order() -> None:
    manifest = _manifest()
    reordered = json.loads(json.dumps(manifest, sort_keys=True))
    assert extension_packages.manifest_sha256(manifest) == extension_packages.manifest_sha256(reordered)


def test_same_package_version_cannot_be_changed() -> None:
    manifest = extension_packages.ExtensionManifest.model_validate(_manifest())
    changed = _manifest()
    changed["description"] = "changed bytes under the same version"
    assert extension_packages.manifest_sha256(manifest) != extension_packages.manifest_sha256(changed)


def test_unknown_executable_asset_kind_is_rejected() -> None:
    manifest = _manifest()
    manifest["assets"].append({"kind": "python_action", "key": "run-code", "payload": {"source": ""}})
    with pytest.raises(ValidationError):
        extension_packages.ExtensionManifest.model_validate(manifest)


def test_unbalanced_solf_delimiter_is_rejected_without_execution() -> None:
    manifest = _manifest()
    manifest["assets"] = [{
        "kind": "solf_script",
        "key": "broken-script",
        "payload": {"script": "broken(_context) ⦃ ↲((_context) ⦄"},
    }]
    with pytest.raises(ValidationError, match="unmatched delimiter|unclosed delimiter"):
        extension_packages.ExtensionManifest.model_validate(manifest)


def test_duplicate_asset_identity_is_rejected() -> None:
    manifest = _manifest()
    manifest["assets"].append(dict(manifest["assets"][0]))
    with pytest.raises(ValidationError, match="duplicate solf_script asset key"):
        extension_packages.ExtensionManifest.model_validate(manifest)


def test_duplicate_database_identity_is_rejected_even_with_different_asset_keys() -> None:
    manifest = _manifest()
    duplicate = dict(manifest["assets"][2])
    duplicate["key"] = "customer-account-alias-duplicate"
    manifest["assets"].append(duplicate)
    with pytest.raises(ValidationError, match="duplicate database identity"):
        extension_packages.ExtensionManifest.model_validate(manifest)


def test_global_promotion_is_limited_to_shared_solF_assets() -> None:
    manifest = extension_packages.ExtensionManifest.model_validate(_manifest())
    with pytest.raises(ValueError, match="Global promotion"):
        extension_packages._validate_scope_assets("global", manifest)
    solf_only = extension_packages.ExtensionManifest.model_validate({
        **_manifest(),
        "assets": [_manifest()["assets"][0]],
    })
    extension_packages._validate_scope_assets("global", solf_only)


def test_solf_clause_name_must_match_the_definition() -> None:
    manifest = _manifest()
    manifest["assets"] = [{
        "kind": "solf_clause",
        "key": "quote-policy",
        "payload": {
            "clause_name": "expected_policy",
            "clause_type": "resolve_policy",
            "clause_body": "actual_policy(_context) ⦃ ↲(true) ⦄",
        },
    }]
    with pytest.raises(ValidationError, match="clause_name must match"):
        extension_packages.ExtensionManifest.model_validate(manifest)


def test_solf_clause_asset_is_accepted_when_name_matches() -> None:
    clause_open = chr(0x2983)
    clause_close = chr(0x2984)
    return_operator = chr(0x21B2)
    manifest = _manifest()
    manifest["assets"] = [{
        "kind": "solf_clause",
        "key": "customer-policy",
        "payload": {
            "clause_name": "customer_policy",
            "clause_type": "resolve_policy",
            "clause_body": f"customer_policy(_context) {clause_open}{return_operator}({{mode: allow}}){clause_close}",
        },
    }]
    validated = extension_packages.ExtensionManifest.model_validate(manifest)
    assert validated.assets[0].kind == "solf_clause"


def test_clause_only_workflow_is_valid_but_python_binding_is_rejected() -> None:
    manifest = _manifest()
    manifest["assets"] = [{
        "kind": "workflow",
        "key": "quote-review",
        "payload": {
            "workflow_name": "Quote review",
            "steps": [{"step_key": "check", "step_kind": "clause", "clause_name": "quote_check"}],
        },
    }]
    assert extension_packages.ExtensionManifest.model_validate(manifest).assets[0].kind == "workflow"
    manifest["assets"][0]["payload"]["steps"][0]["step_kind"] = "python_binding"
    with pytest.raises(ValidationError, match="clause steps only"):
        extension_packages.ExtensionManifest.model_validate(manifest)


def test_platform_extension_routes_are_admin_control_plane_routes() -> None:
    from ikos_api_server.routers.platform_admin import router

    paths = {route.path for route in router.routes}
    assert "/api/platform/extensions/packages/import" in paths
    assert "/api/platform/extensions/packages/inbox/{filename}/import" in paths
    assert "/api/platform/extensions/deployments" in paths
    assert "/api/platform/extensions/packages/{extension_key}/versions/{version}/inspect" in paths
    assert "/api/platform/extensions/packages/{extension_key}/versions/{version}/promote" in paths


def test_extension_package_api_requires_platform_authentication() -> None:
    from fastapi.testclient import TestClient
    from ikos_api_server.application import app

    response = TestClient(app).get("/api/platform/extensions/packages")
    assert response.status_code == 401
