---
name: IKOS extension package authoring
description: Rules for creating and validating versioned IKOS domain-extension packages; never deploy or apply packages from development.
applyTo: "extensions/**"
---

# IKOS extension package authoring rules

- Follow the schema and promotion lifecycle in `doc/EXTENSION_PACKAGE_PROMOTION.md`.
- Author a release in its own folder with `extension.json` plus referenced `.solf` files. Use a stable lowercase `extension_key`, a new semantic version for every content change, and stable unique asset keys.
- Only package supported asset kinds: `solf_script`, `solf_clause`, `workflow`, `semantic_term`, `semantic_pattern`, `query_term_alias`, and `ingestion_trigger`. Workflows may contain `clause` steps and declarative `capability` steps; Python bindings are not package content. Do not add Python, SQL, credentials, secrets, customer/production data, arbitrary prompts, or unsupported workflow bindings.
- For SOLF text, put scripts/clauses in `assets/*.solf` and reference them through `payload.script_file` or `payload.clause_file`; the packager embeds text into the final manifest. Clause steps must reference a clause included in the same package or already installed in the target.
- Capability steps must specify exact `capability_key` and `capability_version` values; versions are opaque identifiers, never ranges, wildcards, or `latest`. `payload` contains JSON literals. `payload_fields` maps capability input-field names (keys) to dotted workflow-context paths (values); do not map authority or execution metadata. `save_as` defaults to `capability_result` and must not collide with workflow metadata.
- If configured, `compensation_capability` uses exact `key`/`version` and optional `payload`/`payload_fields`; its mappings read the original capability output snapshot. Ensure the posting result contains the mapped inverse inputs and that the inverse handler is independently registered and authorized. Compensation is not a guarantee of exactly-once effects or a general rollback.
- Capability handlers are trusted application code, not package assets. Require the domain application to register each exact version in `CAPABILITIES` at trusted startup on every API/worker process before inspection/promotion and execution. Inspect must show every requested capability/inverse available; missing capabilities make promotion ineligible. Never add placeholder handlers or weaken the dependency gate.
- Handlers must use the active tenant security context and enforce durable run/step/domain idempotency at every mutation boundary. Workflow idempotency metadata alone does not guarantee exactly-once side effects; external services need their own idempotency contract.
- `ingestion_trigger` is tenant-only, accepts only `document_types` from the supported invoice/bill/receipt set, and must reference a workflow asset bundled in the same manifest via `workflow_asset_key`. It does not execute during promotion. The reviewed additive migration `ddl/migrations/002_ingestion_triggers.sql` must be applied separately through deployment change control; never execute DDL from package data.
- Preserve existing tenant-owned knowledge. Treat a conflict with configuration not owned by this package as a stop condition requiring human resolution; do not rename assets to bypass conflict inspection.
- Build/validate with `python scripts/package_ikos_extension.py <extension-folder> --check`, then build the release artifact with the same command without `--check`. Include the printed SHA-256 in the release request/change ticket.
- Do not call production promotion endpoints, alter a production database, change runtime/deployment configuration, or claim a release is deployed. Import, inspect, and promote are explicit platform-operator approval steps.
- Ask for human review if the extension introduces security policy, actions, external resources, destructive/deprecation behavior, schema migrations, or non-SOLF code. This v1 packager does not support those capabilities.
