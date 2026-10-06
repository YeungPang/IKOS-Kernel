---
name: package-ikos-extension
description: Create or update a versioned IKOS domain-extension package for review and promotion.
---

Create or update a versioned IKOS domain extension package under `extensions/`.

Before editing, inspect `doc/EXTENSION_PACKAGE_PROMOTION.md` and follow `.github/instructions/ikos-extension-packaging.instructions.md`. Ask for the extension key, display name, new semantic version, target domain, tenant/global intent, and requested knowledge assets if not already supplied. Do not guess production tenant IDs or silently change an existing release version.

Requirements:

1. Create/update a release folder such as `extensions/<extension-key>/<version>/` with `extension.json` and referenced `.solf` files in `assets/`.
2. Use only the supported v1 kinds: `solf_script`, `solf_clause`, `workflow` (clause and declarative capability steps), `semantic_term`, `semantic_pattern`, `query_term_alias`, and tenant-only `ingestion_trigger`.
3. Validate unique logical asset keys and target identities; ensure clause steps reference bundled or documented installed clauses. Each ingestion trigger must reference a workflow asset bundled in this manifest and use only supported document types (`invoice`, `bill`, `receipt`). Do not add a global trigger.
4. For every capability step, use exact `capability_key` and `capability_version` identifiers (no ranges, wildcards, or `latest`). Explain required trusted registrations and schemas; do not invent that a handler already exists. Map `payload_fields` from capability input names to dotted workflow-context paths, keep `payload` to inert JSON literals, and exclude authority/execution metadata.
5. If the workflow needs an inverse, use `compensation_capability` with exact `key` and `version`; its field mappings resolve from the posting step's output snapshot. Ensure the posting output supplies those values and include the inverse as a dependency. State that durable domain idempotency and tenant-scoped trusted handlers are still required; an inverse/idempotency key is not an exactly-once guarantee.
6. Do not include arbitrary Python/SQL, credentials, prompts, operational/customer facts, or unsupported workflow bindings. Missing exact capability registrations must remain a promotion-blocking dependency, never be hidden by a placeholder.
7. Run `python scripts/package_ikos_extension.py <release-folder> --check`; fix errors; then build the JSON artifact and record its SHA-256. Do not call the production import, inspect, or promote endpoints.
8. Report files created/changed, asset inventory, package hash, validation output, any assumptions, and operator steps: deploy/register trusted capabilities on all workers, apply reviewed schema migrations (including `ddl/migrations/002_ingestion_triggers.sql` if triggers are included), copy the artifact into the configured deployment inbox, import it, inspect target diff and dependency availability, resolve conflicts, and explicitly approve promotion.
