# IKOS domain extension packages (schema v1)

IKOS now includes a platform-admin-only registry for immutable, versioned domain extension manifests, inspection against a target tenant, and an explicit transactional promotion step. This is the foundation for development-to-production configuration promotion; it is not a replacement for deploying application code or running reviewed schema migrations.

## Supported package assets

Schema v1 intentionally supports only asset kinds with a known kernel runtime path:

- `solf_script`: a SOLF program saved as a managed, active `business_rules` record. Existing IKOS runtime hydration loads active scripts for that tenant; the script can define SOLF clauses, top-level facts, and SOLF objects/classes.
- `solf_clause`: a syntax-reviewed clause stored in the tenant/global SOLF clause catalog. Workflow clause steps can resolve these by name.
- `workflow`: a versioned registry entry with SOLF clause steps and declarative domain-capability steps. Promotion publishes a new active version and retains earlier versions; package-supplied Python bindings are rejected.
- `ingestion_trigger`: a tenant-only, declarative document-ingestion outbox trigger. Each trigger must point to a workflow asset bundled in the same manifest; promotion does not enqueue or execute it.
- `semantic_term`: a tenant vocabulary term.
- `semantic_pattern`: a tenant-scoped common query phrase, canonical concept, mapped attributes, optional computation hint and synonyms. Wildcard synonyms are promoted as template matches. Existing synonyms are additive: v1 will add missing rows but will not delete synonyms omitted by a later package version.
- `query_term_alias`: a tenant-scoped, deterministic query vocabulary alias.

Unknown asset kinds (including Python source, SQL, prompts, and arbitrary action bindings) are rejected rather than silently claimed as deployed. Prompt templates, action implementations, and external resource mappings retain their existing separate lifecycle and need adapters before they can be part of a package promotion. An ingestion trigger's DDL is separate from package promotion; apply the reviewed additive [ingestion-trigger migration](../ddl/migrations/002_ingestion_triggers.sql) before using trigger assets.

Package scripts are parsed/reviewed, not executed during import or inspection. They are executed by normal SOLF runtime use after promotion. IKOS does not permit package-supplied arbitrary Python or SQL. Package facts inside SOLF are interpreter facts; this does not turn customer/operational data into package configuration.

## Manifest shape

For Copilot-assisted authoring, use the repository prompt
`.github/prompts/package-ikos-extension.prompt.md`. It instructs Copilot to
create a versioned source folder without deploying it. Reusable constraints
are in `.github/instructions/ikos-extension-packaging.instructions.md`, and
`extensions/examples/minimal/` is a working source-folder example. Build and
validate it with:

```powershell
python scripts/package_ikos_extension.py extensions/examples/minimal --check
python scripts/package_ikos_extension.py extensions/examples/minimal
```

The second command creates a self-contained JSON release under the folder's
`dist/` directory. Source folders use `extension.json` plus referenced `.solf`
files. File references must stay within the extension folder; the builder
embeds the script text, validates the result against the current schema, and
prints the exact manifest SHA-256. Commit/review the source files; transfer the
generated release artifact through the approved release channel.

```json
{
  "schema_version": "1",
  "extension_key": "aphotonix-crm",
  "version": "1.0.0",
  "display_name": "Aphotonix CRM",
  "description": "Curated laser-component and CRM vocabulary",
  "assets": [
    {
      "kind": "solf_script",
      "key": "crm-core",
      "payload": {
        "script": "laser_component(_part) ⦃ ... ⦄"
      }
    },
    {
      "kind": "semantic_term",
      "key": "cutting-application",
      "payload": {
        "kind": "category",
        "canonical_name": "laser_cutting",
        "term_text": "laser cutting",
        "language": "en"
      }
    },
    {
      "kind": "query_term_alias",
      "key": "oem-account",
      "payload": {
        "alias_text": "OEM customer",
        "canonical_name": "customer",
        "kind": "entity_type",
        "language": "en",
        "priority": 50
      }
    },
    {
      "kind": "semantic_pattern",
      "key": "person-age",
      "payload": {
        "pattern_text": "how old is *",
        "semantic_concept": "age",
        "mapped_attributes": {"age": "birth_date"},
        "computation_rule": "AGE(birth_date)",
        "entity_class": "person",
        "confidence": 0.95,
        "pattern_language": "en",
        "synonyms": [
          {"synonym_text": "what is the age of *", "language": "en", "semantic_distance": 0.05, "match_type": "template"}
        ]
      }
    },
    {
      "kind": "workflow",
      "key": "quote-review",
      "payload": {
        "workflow_name": "Quote review",
        "steps": [
          {"step_key": "policy-check", "step_kind": "clause", "clause_name": "aphotonix_quote_policy"},
          {
            "step_key": "domain-review",
            "step_kind": "capability",
            "config": {
              "capability_key": "domain.booking.review",
              "capability_version": "1.0.0",
              "payload": {"review_mode": "pre_posting"},
              "payload_fields": {"booking_id": "booking.id"},
              "save_as": "booking_review"
            }
          },
          {
            "step_key": "domain-post",
            "step_kind": "capability",
            "config": {
              "capability_key": "domain.booking.post",
              "capability_version": "1.0.0",
              "payload_fields": {"booking_id": "booking_review.booking_id", "review_token": "booking_review.review_token"},
              "compensation_capability": {
                "key": "domain.booking.reverse",
                "version": "1.0.0",
                "payload_fields": {"booking_id": "booking_id", "posting_reference": "posting_reference"}
              }
            }
          }
        ]
      }
    },
    {
      "kind": "ingestion_trigger",
      "key": "quote-document-intake",
      "payload": {
        "workflow_asset_key": "quote-review",
        "document_types": ["invoice"],
        "is_active": true
      }
    }
  ]
}
```

A release is identified by extension key + semantic version and the SHA-256 of canonical JSON (sorted keys, compact UTF-8 encoding). Re-importing identical bytes is idempotent; changing bytes under an existing key/version returns a conflict. Current limits are 500 assets, 1 MB per SOLF body and 2 MB total assets. SOLF syntax review uses the kernel's clause parser and is not a proof of semantic correctness or safety; test with representative tenant data before promotion.

### Capability workflow steps

Capability references are exact, case-sensitive `(capability_key,
capability_version)` identifiers. Versions are opaque identifiers: ranges,
wildcards, `latest`, and fallback resolution are not supported. The package
contains only declarative data; it cannot register a handler, import code, or
select a Python module/function.

A capability step uses `step_kind: "capability"` and `config` with required
`capability_key` and `capability_version`, optional JSON-object `payload`
literals, optional `payload_fields`, and optional `save_as` (default
`capability_result`). Each `payload_fields` entry maps a capability input name
to a dotted path in the workflow's current context: the left side is the input
field and the right side is the source context path. Values are copied as JSON;
missing paths fail execution. Authority, execution metadata, and executable
bindings cannot be mapped or supplied. `save_as` stores the output for later
steps and cannot overwrite workflow execution metadata.

`compensation_capability`, when present, is declarative inverse configuration
with exact `key` and `version`, plus optional `payload` literals and
`payload_fields`. Its mappings are resolved against the original capability's
persisted output snapshot (not the later live workflow context), so the posting
capability must return the fields required by its inverse. It is not an
automatic reverse transformation or a guarantee that an external side effect
can be undone. The registered capability may also advertise inverse metadata;
inspection reports reachable inverse dependencies, while the workflow's
explicit `compensation_capability` is itself checked as a required dependency.
If both the forward registration and explicit compensation advertise an inverse,
inspection requires their exact key/version pairs to agree; explicit
compensation remains valid when the forward registration has no inverse metadata.

Capability handlers are trusted application code registered in the process-local
`CAPABILITIES` registry during trusted domain/application bootstrap using
`CapabilityRegistry.register()` and callable objects. Deploy that registration
code to every API/worker process before package inspection/promotion and before
workflow execution. Package import never imports or registers handlers. Each
handler registration supplies an exact version, permission, Draft 2020-12 input
and output schemas, and optionally inverse metadata. Execution uses the active
tenant `SecurityContext`; every capability permission is checked independently,
including nested calls. A package dependency absent from the current registry is
reported as missing, makes `can_promote` false, and blocks promotion. Do not
work around this gate by weakening the registry or inventing a placeholder
handler.

Inspection reports the unique `required_permissions` declared by available
registrations; this is an inventory only, not a grant, permission check, or
authority elevation. It validates fully literal payloads against registered
Draft 2020-12 input schemas (including recognized formats). For mapped payloads,
inspection reports definite literal/required-field errors where the schema is
simple enough for sound partial checks, while mapped runtime values and complex
schema constraints remain deferred until execution; a deferred check is not a
claim that runtime values satisfy the schema.

The workflow executor supplies a stable `run:{run_id}:step:{step_key}:capability`
idempotency key and stores run/step evidence, but this does not by itself make
arbitrary domain effects exactly-once. Trusted handlers must enforce durable
domain idempotency at each mutation boundary (ideally storing a unique operation
key and mutation in the same transaction); external systems must honor their
own idempotency key. Persist and validate all state against the caller's tenant.
Do not include tenant/user/permission authority in package payloads.

The working generic example is
`extensions/domain-capability-example/1.0.0/extension.json`. Its capability
names are illustrative contracts, not kernel accounting APIs or implementations;
the domain application must supply and schema-check its own trusted handlers.

#### Trusted handler example (domain application code)

Register capabilities in trusted application startup code, not in the manifest.
The example below shows the registration contract only; replace the service
calls with the domain application's own tenant-scoped, transactional operations.
Import and run this registration in every API/worker process before promotion
inspection or workflow execution:

```python
from capability_registry import CAPABILITIES

BOOKING_POST_INPUT = {
  "type": "object",
  "properties": {
    "booking_id": {"type": "string"},
    "review_token": {"type": "string"},
    "idempotency_key": {"type": "string", "minLength": 1},
  },
  "required": ["booking_id", "review_token", "idempotency_key"],
  "additionalProperties": False,
}

def post_booking(payload, caller):
  # Deduplicate the key and apply the domain mutation in the same transaction,
  # scoped to caller.tenant_id. This handler is illustrative, not kernel logic.
  return domain_booking_service.post_once(
    tenant_id=caller.tenant_id,
    booking_id=payload["booking_id"],
    review_token=payload["review_token"],
    idempotency_key=payload["idempotency_key"],
  )

CAPABILITIES.register(
  key="domain.booking.post",
  version="1.0.0",
  handler=post_booking,
  permission="bookings.post",
  input_schema=BOOKING_POST_INPUT,
  output_schema={
    "type": "object",
    "properties": {
      "booking_id": {"type": "string"},
      "posting_reference": {"type": "string"},
    },
    "required": ["booking_id", "posting_reference"],
    "additionalProperties": False,
  },
  inverse_key="domain.booking.reverse",
  inverse_version="1.0.0",
)
```

The workflow executor supplies `run:{run_id}:step:{step_key}:capability` only
when the registered input schema accepts an `idempotency_key` property. That
value is not idempotency by itself: the trusted handler/domain store must enforce
it at the effect boundary. Register the reverse handler separately with its own
permission and schema. Never grant permissions by registering a handler or
including a permission in package JSON.

### Ingestion trigger assets

An `ingestion_trigger` payload accepts only `workflow_asset_key`, a nonempty
unique list of document types (`invoice`, `bill`, or `receipt`), and optional
`is_active`. `workflow_asset_key` must identify a workflow asset in the same
manifest. Triggers are always tenant-scoped; global inspection/promotion rejects
any package containing one. Promotion derives tenant identity from the
authenticated target, stores package ownership metadata, and promotes workflows
before their triggers. It does not enqueue events or run workflows. Trigger
collisions with assets not owned by the same extension are conflicts.

The trigger table's workflow key is a logical reference to the promoted workflow.
Ingestion enqueues only an immutable document reference event; the trigger is
not an arbitrary predicate or a channel for extracted text/authority. DDL is
separate and additive, and must be reviewed/applied by the deployment migration
process before trigger promotion. See
[`doc/INGESTION_EXTENSION_TRIGGERS.md`](INGESTION_EXTENSION_TRIGGERS.md) for
transaction, RLS, worker and crash-recovery contracts.

The stored SHA-256 detects content changes relative to the registry, but is not a publisher signature. Authenticate the package source and require a trusted platform administrator to import/promote it; signed artifact provenance is a future enhancement.

## API lifecycle

### End-to-end tenant promotion example

This is an operator sequence, not one combined command: build/review the artifact,
install trusted capability code and schema/permission migrations where needed,
import an immutable version, inspect the target tenant, then explicitly promote.
The core vocabulary package has no capability dependencies and is a simple first
promotion. The domain capability example intentionally reports missing
capabilities until its domain application has registered all exact key/version
handlers.

From the kernel directory, validate/package either example folder:

```powershell
python scripts/package_ikos_extension.py extensions/core-knowledge/1.1.0 --check
python scripts/package_ikos_extension.py extensions/core-knowledge/1.1.0
python scripts/package_ikos_extension.py extensions/domain-capability-example/1.0.0 --check
```

The checked manifest is `extensions/core-knowledge/1.1.0/extension.json`;
the generated artifact is written under that source folder's `dist/` directory.
Before promoting a domain workflow package, deploy its domain registration code
to every target API/worker process, ensure the tenant has the reported
permissions, and apply reviewed schema/permission migrations. The generic example
does not implement or seed accounting or booking handlers.

The following PowerShell illustrates the API calls. `$headers` must be created
by the trusted operator environment with the configured trusted-proxy header
and a short-lived platform-admin identity accepted by the deployment; do not
store proxy secrets or bearer assertions in source files or package manifests.
`$tenantId` is the already-provisioned active tenant UUID:

```powershell
$api = "https://ikos.example"
$tenantId = "<active-tenant-uuid>"
$manifest = Get-Content -Raw "extensions/core-knowledge/1.1.0/dist/ikos-core-knowledge-1.1.0.json" | ConvertFrom-Json
$import = Invoke-RestMethod -Method Post `
  -Uri "$api/api/platform/extensions/packages/import" `
  -Headers $headers -ContentType "application/json" `
  -Body (@{ manifest = $manifest } | ConvertTo-Json -Depth 100)
$hash = $import.result.sha256

$target = @{ target_scope = "tenant"; target_tenant_id = $tenantId }
$inspect = Invoke-RestMethod -Method Post `
  -Uri "$api/api/platform/extensions/packages/ikos-core-knowledge/versions/1.1.0/inspect" `
  -Headers $headers -ContentType "application/json" `
  -Body ($target | ConvertTo-Json)
if (-not $inspect.result.can_promote) { throw "Inspection found promotion blockers" }
$inspect.result.changes | Format-Table kind, key, action

$promotion = @{ target_scope = "tenant"; target_tenant_id = $tenantId; expected_sha256 = $hash }
$deployed = Invoke-RestMethod -Method Post `
  -Uri "$api/api/platform/extensions/packages/ikos-core-knowledge/versions/1.1.0/promote" `
  -Headers $headers -ContentType "application/json" `
  -Body ($promotion | ConvertTo-Json)
$deployed.result
```

For the domain example, substitute the package key/version and artifact path.
Do not proceed unless inspection says `can_promote: true`, the digest matches
the reviewed artifact, `missing_capabilities` and `capability_validation_errors`
are empty, and `required_permissions` have been deliberately provisioned for
the appropriate tenant roles/service identity. Import succeeds before runtime
dependencies exist so teams can author/review in development; inspection and
promotion remain blocked until exact trusted dependencies are deployed.

To deploy trigger assets, first review/apply migrations 002 and 003 (or use the
documented non-destructive bootstrap), verify runtime grants with a non-superuser
role, then promote to a tenant. Promotion installs trigger configuration only;
it does not ingest historical documents, execute a workflow, or grant worker
permissions. Use the authorized runtime dispatch/reconciliation endpoints and
review delivery/run records separately.

All endpoints are under `/api/platform/extensions` and require the existing platform-admin allowlist context. The API must be deployed with the new kernel schema before these routes can operate.

1. `POST /api/platform/extensions/packages/import` with `{"manifest": {...}}` validates and stores a release.
2. `GET /api/platform/extensions/packages` lists registered packages and their latest versions; `GET /api/platform/extensions/deployments` provides the promotion audit history.
3. `GET /api/platform/extensions/packages/{extension_key}/versions/{version}` retrieves the stored immutable manifest and digest.
4. `POST /api/platform/extensions/packages/{extension_key}/versions/{version}/inspect` with `{"target_scope":"tenant","target_tenant_id":"<active-tenant-uuid>"}` returns create/update/unchanged/conflict results, missing clauses, missing workflows, and exact capability/inverse dependencies without applying configuration. Missing exact capability registrations block promotion. `target_scope=global` excludes ingestion triggers and vocabulary; terms, patterns, synonyms, aliases, and triggers are tenant-scoped.
5. Resolve every conflict, verify the SHA, then call `POST /api/platform/extensions/packages/{extension_key}/versions/{version}/promote` with the same target plus `expected_sha256`. Promotion upserts only rows previously created by that same extension asset; it never overwrites unrelated tenant-authored configuration. Successful deployments are recorded and repeat promotion of the same release to the same target is idempotent.

### Deployment-platform inbox

To use a drop-folder, configure `IKOS_EXTENSION_PACKAGE_INBOX` on the trusted
IKOS API deployment to an absolute directory outside the web root. Make it
writable only by the release operator and readable by the IKOS service account;
do not expose it as a static route or mount it from an untrusted upload area.
Place the generated `<extension-key>-<version>.json` there, then a platform
administrator requests import using:

```http
POST /api/platform/extensions/packages/inbox/<filename>/import
```

The handler accepts only a simple `.json` filename directly inside that
configured directory; it rejects traversal, symlinks, non-regular files, and
oversized content. It validates and stores the immutable package but **does not
promote it**. Then call the existing inspect endpoint, review the diff and
target tenant, and explicitly call promote with the returned package hash.
The inbox should be monitored/cleaned by deployment operations after import;
IKOS stores the versioned manifest and hash in its DB. If the inbox setting is
blank, folder import is disabled and the authenticated JSON import endpoint
remains available.

The DB records package metadata, immutable version manifest/hash, and deployment history in `ikos_extension_package`, `ikos_extension_package_version`, and `ikos_extension_deployment`. All three tables use forced RLS and require `platform.configuration.publish`.

## Deployment and rollback

- Apply the additive [extension registry migration](../ddl/migrations/001_extension_package_registry.sql), or use `object_db.create_tables(recreate=False)` on a trusted, backed-up IKOS database. For ingestion-trigger packages, separately review and apply [migration 002](../ddl/migrations/002_ingestion_triggers.sql). Both require the referenced base tenant/document/workflow tables to exist. Do not use `recreate=True` for a production upgrade.
- Export the validated manifest from development and import it into the production kernel DB; do not copy the development DB or tenant rows.
- Inspection/promotion operate against a selected active tenant (or supported global configuration scope). Package metadata is platform-admin-owned; promoted business rules, clauses, workflows, patterns, terms, aliases, and ingestion triggers are tenant/global configuration as permitted by each asset kind.
- To roll back, inspect and promote a previously imported package version. This restores the prior package-owned asset content but does not delete assets introduced by newer versions. Removed assets are deliberately retained; deprecation/removal reconciliation is a future lifecycle feature.
- Promotion refreshes the `get_tools` cache in the current API worker. Other workers/processes may retain cached vocabulary/interpreters; restart them or add deployment-wide cache invalidation before relying on immediate cluster-wide activation.
- Schema migrations, trusted capability handler deployments/registrations, prompt-template consumers, and distributed rollout coordination remain separate release steps. Capability registry state is process-local; deploy compatible handlers to all workers. Promotion history is a database audit record, not a substitute for change approval, signed artifact provenance, backups, or staging verification.
