# Declarative ingestion extension triggers (v1)

Implementation: [ingestion_triggers.py](../ingestion_triggers.py). Contract tests:
[tests/test_ingestion_triggers.py](../tests/test_ingestion_triggers.py).

This service adds an ingestion outbox, not a new inline posting path. Ingestion
only queues events. A separately invoked worker executes a trusted, published
workflow using the worker's explicitly authorized tenant principal. No document
text, extracted permissions, trigger metadata, or package Python becomes authority.

## Interfaces

| Interface | Contract |
| --- | --- |
| `TRIGGER_DDL: str` | Reviewed PostgreSQL migration text; included in explicit kernel schema bootstrap, never request-time DDL. |
| `inspect_trigger(trigger: dict) -> dict` | Pure normalization/shape validation; no DB calls or effects. Only `trigger_key`, `workflow_key`, `document_types`, `is_active`, `metadata` are accepted. |
| `enqueue_document_ingested(connection, doc_id: int, document_type: str) -> list[int]` | Newly inserted delivery IDs. Requires a non-autocommit ingestion connection; never commits or executes a workflow. Repeat enqueue returns an empty list for existing deliveries. |
| `dispatch_one(connection, executor_factory) -> dict \| None` | Processes one queued record. Returns `delivery_id`, `status`, `run_id`, `error`; `None` means no unlocked queued record. Owns commits on a dedicated worker connection. |
| `retry_failed(connection, delivery_id: int) -> bool` | Explicitly requeues a failed delivery, preserving event/run identity. Caller commits. Never reclaims running rows. |
| `IngestionExecutor.execute_ingestion_run(*, run_id, workflow_version_id, input_context, idempotency_key) -> dict` | Trusted adapter executes an already reserved run under the unchanged active security context; returns persisted `run_id` and `run_status`. |

`document_types=[]` matches every nonempty stored document type. Otherwise this is
an exact allowlist after normalization: trim, lowercase, convert whitespace and
hyphens/underscores to single underscores. No regex, wildcard string, predicate
script, arbitrary import, or extracted-content matcher is supported. Inactive
triggers do not enqueue and cannot dispatch pending deliveries. Metadata is inert.

## DDL integration (not applied by this change)

Run `cursor.execute(TRIGGER_DDL)` and commit in the reviewed schema migration
path. The existing `tenant`, `document`, `solf_workflow_registry`,
`solf_workflow_versions`, and `workflow_pipeline_run` schema must already exist.
Do not run DDL during ingestion, inspection, or dispatch. These definitions are
additive fresh-table DDL, not an automatic repair migration for incompatible
preexisting tables or policies.

The trigger registry has `(tenant_id, trigger_key)` as its primary key.
`workflow_key` is a **logical** reference: no FK is imposed because the existing
resolver supports tenant overrides and global configuration. Resolve dependencies
during promotion and again at dispatch. Deliveries reference the tenant-owned
trigger; do not delete historical triggers, deactivate them instead.

Delivery identity is unique on `(tenant_id, trigger_key, event_key)`. Status is one
of `queued/running/completed/failed`. `run_id` and the resolved workflow key/version
are recorded durably. An update trigger rejects changes to event tenant/key,
payload/hash, and any already assigned run identity. Event hashing uses sorted,
compact UTF-8 JSON with non-finite values rejected, not PostgreSQL JSONB rendering.
The worker recomputes the hash; this is an integrity check, **not a signature**.

Both new tables have ENABLE and FORCE RLS, explicit tenant checks on USING and
WITH CHECK, and restrictive tenant policies that remain effective if another
permissive policy is added. Missing/blank tenant settings match no rows; invalid
UUID settings error closed. There is no legacy tenant fallback or global trigger
scope. FORCE does not constrain PostgreSQL superusers or BYPASSRLS roles: use a
non-superuser/NOBYPASSRLS runtime role and protect the ability to change policies.

Grant only the necessary table/sequence rights to trusted application services.
Do not expose raw delivery INSERT/UPDATE/DELETE, mutable session GUCs, or the
database role to customers. RLS establishes tenant isolation, not permission-level
configuration administration or cryptographic event authenticity. Enforce
configuration promotion authorization separately. Do not grant delivery DELETE:
deleting/reinserting rows would defeat event dedupe and historical immutability.

## Ingestion hook integration

`run_ingest()` now calls `enqueue_persisted_document()` after persistence under
the active authenticated tenant. This wrapper reads the authoritative document
type and checks migration availability; older installs report
`schema_not_installed` without creating tables. Unclassified documents report
`unclassified`. Successful results expose newly queued IDs in
`db_summary.extension_triggers`.

**The current ingestion path is not globally transactional:** persistence helpers
commit individually. This hook commits a separate outbox transaction and cannot
erase the crash gap after document persistence. Reprocessing or the authorized
`POST /api/extensions/documents/{doc_id}/enqueue` reconciliation endpoint closes
that gap using the same deduplicated event. Security/DB failures are not silently
treated as successful enqueue. A future atomic ingestion coordinator should use
the low-level hook below before its own final commit.

On the **same connection and transaction** that persists the document and its
authoritative classification, call:

```python
from ingestion_triggers import enqueue_document_ingested

# After document persistence/classification, before the ingestion transaction commits:
delivery_ids = enqueue_document_ingested(connection, doc_id, stored_document_type)
# Existing ingestion coordinator commits document + deliveries together.
```

Do not use a separate connection, an after-commit callback, or an existing helper
that commits midway through this atomic section. Existing ingestion functions
must be inspected by the integrating agent: several kernel DB helpers commit
internally. Calling this hook after those commits does not retroactively provide
document/outbox atomicity. On failure, roll back the ingestion transaction.

Enqueue requires an active `SecurityContext` with tenant UUID, nonempty user ID,
and `documents.write`. It verifies that DB session tenant, user and exact
permissions match that context rather than setting/elevating them. Document
ownership and stored type are read with `FOR SHARE` and an explicit tenant filter,
in addition to existing document RLS/access-level restrictions. The supplied type
must match the stored type. Neither unknown nor inaccessible documents queue.

The stable event key is `document:{doc_id}:ingested`. The immutable payload is
exactly `doc_refs: [doc_id]`, `document_id: doc_id`, `document_type: canonical_type`.
No amount, account, extracted field, metadata, permissions, or identity assertions
are forwarded. Workflows fetch data through normal authorized kernel mechanisms.
Reclassification/reingestion does not mutate an existing event or create another
delivery for the same trigger. Use a separately designed versioned event if that
behavior is needed. New triggers may receive this event on a later enqueue call.

## Worker and executor integration

The scheduler must install a real, explicit tenant `SecurityContext` and use a
dedicated connection whose **session-scoped** `ikos.tenant_id`, `ikos.user_id`, and
`ikos.permissions` match it. Existing `object_db.get_connection()` binds these
settings from the current context; do not call it without context. Transaction-
local settings expire at worker commits and fail the subsequent context check.
Never share the outbox connection with another task or the executor.

The worker requires `documents.write` plus the explicit service permission
`workflows.run` (an integration permission to grant/register in the deployment).
It retains the caller's permissions: it does not inherit the ingesting user's
permissions, synthesize platform-admin authority, or restore authority from the
event. All workflow/domain-specific permissions are additionally enforced at the
normal mutation boundaries. A platform-admin flag alone is not sufficient.

Execution phases:

1. Claim one queued tenant delivery with `FOR UPDATE SKIP LOCKED`; mark running,
   increment attempts, **commit**. Helpers cannot release an unpersisted claim.
2. Revalidate payload/hash, document ownership/type, trigger activation and
   workflow publication. Resolve using `object_db.get_solf_workflow_registry_by_key`
   and `object_db.get_workflow_active_version`. These lookup helpers are currently
   read-only. Registry must be active/published; version must be active with
   `metadata.status == "published"`. Active alone is insufficient.
3. Validate the executor contract. Insert a pending tenant `workflow_pipeline_run`
   and attach its ID/key/version to the delivery in **one transaction**, then
   **commit**. This uses local SQL deliberately: `object_db.create_pipeline_run`
   commits internally and would leave a crash window before attaching the run.
   A retry reuses the same reserved run/version, rejects cross-tenant run links,
   changed trigger targets, and changed active versions.
4. Execute with no open transaction on the outbox connection. The adapter uses
   separate same-context connections. Runtime input adds only the trusted stable
   key `ingestion:{tenant_id}:{delivery_id}` to a copy of the immutable payload.
5. Verify returned and persisted tenant run completion, then commit completed.
   Non-completed/paused runs and exceptions mark failed. Errors retain only the
   exception class, not potentially sensitive exception text. If identity or DB
   authority changes, stop without finalizing under that changed authority; the
   committed delivery remains running for reconciliation.

**Required adapter contract:** the factory takes no arguments and returns an
object implementing `execute_ingestion_run`. It must use the supplied durable
run ID, not create/start another run, serialize execution of that run, consult
durable completion/step evidence, and enforce domain idempotency before every
mutation. Use durable unique run/step/operation keys such as
`run:{run_id}:step:{step_key}:capability`. Domain posting must put the dedupe record
and SQL mutation in the same domain transaction; external systems need their own
idempotency-key contract. Passing a key in a JSON payload alone is not enforcement.
Only trusted application code can supply this adapter; no trigger/package can.

The native `WorkflowPipelineExecutor.execute_ingestion_run()` now implements
the reserved-run adapter. It verifies tenant, version and immutable input, returns
an already completed matching run without replay, and atomically claims a pending
run as running before execution. It persists document links and never allocates
another run. Non-pending/non-completed runs require explicit reconciliation or
the normal resume path; failures are not automatically replayed.
`start_pipeline_run()` always creates a new run and is deliberately rejected
as the outbox adapter. The private `_execute_run()` alone is not safe for retries.
Existing run-scoped capability keys help but do not make all arbitrary SOLF/domain
operations idempotent. This change does not silently claim to retrofit them.
The adapter should also persist normal run-to-document audit links through
`object_db.link_pipeline_run_documents` on its own authorized connection; the
reserved run already contains `doc_refs` in its input/current context.

## Crash/retry semantics: no exactly-once guarantee

Delivery uniqueness and committed claims prevent two healthy workers from
claiming the same queued record. **They do not guarantee exactly-once effects.**

- Crash before claim commit: PostgreSQL releases the row lock; it remains queued.
- Crash after claim commit but before run reservation: it remains running without
  a run. No business effect has started; an operator can reconcile/requeue after
  proving the previous worker is dead.
- Crash after run reservation: it remains running with its durable run ID.
  Inspect the run and domain idempotency evidence before recovery. If the run
  already completed, finalize the delivery after verifying tenant/version.
- Crash after an external/domain effect but before recording step or delivery
  completion: delivery state cannot distinguish success from failure. Only the
  domain/external system's durable idempotency contract can prevent repetition.
- Failed deliveries require explicit `retry_failed()` and caller commit. The
  stable run/key survive retries. There is no automatic retry loop, timeout,
  lease expiry, or automatic stealing of running work.
- A paused pipeline is failed from the delivery worker's perspective, not
  completed. Resume/resolve it through the normal authorized pipeline path and
  reconcile delivery status; do not blindly replay it as a new pipeline.

The administrator recovery path is intentionally manual in v1. Never requeue a
running delivery while its original worker might still execute; SKIP LOCKED is
not a lease and cannot fence a worker after an operator forces requeue. Upstream
publication can change between validation and execution; the reserved version
is the execution snapshot, not a lock held throughout business execution.

## Runtime API and authorization rollout

- `GET /api/extensions/capabilities`: metadata inventory; requires
  `tenant.configuration.manage`.
- `POST /api/extensions/capabilities/execute`: exact key/version/payload;
  requires `workflows.run` and the registered domain permission.
- `POST /api/extensions/ingestion-deliveries/dispatch-one`: dedicated connection,
  one queued tenant delivery; requires `workflows.run` and `documents.write`.
- `POST /api/extensions/ingestion-deliveries/{delivery_id}/retry`: explicit
  failed-only requeue with the same authority; it cannot reset a running run.
- `POST /api/extensions/documents/{doc_id}/enqueue`: authoritative reconciliation;
  requires `documents.write`. No workflow executes in this request.

The additive [permission migration](../ddl/migrations/003_workflow_execution_permission.sql)
registers `workflows.run` and grants it to the existing tenant-admin role, matching
new-install bootstrap. Review that grant before deployment. Worker roles and
domain permissions must be provisioned explicitly; finance operators receive no
implicit workflow-execution grant. Application middleware supplies the actual
tenant/principal context. No JSON package grants permissions.

## Package promotion integration

Schema-v1 package promotion supports `ingestion_trigger` assets in
`extension_packages.py`. An asset may contain only `workflow_asset_key`,
`document_types`, and optional `is_active`; it must reference a workflow bundled
in the same package, and supported types are `invoice`, `bill`, and `receipt`.
Global promotion is rejected. The tenant comes from the authenticated promotion
target, never the asset payload. Promotion resolves the bundled workflow to its
package-namespaced logical key, upserts the trigger transactionally, and records
platform-derived package ownership in inert metadata. An unrelated existing
trigger with the same key is a conflict. Promotion neither enqueues events nor
executes workflows.

Apply the standalone additive [ingestion-trigger migration](../ddl/migrations/002_ingestion_triggers.sql)
through deployment change control before promoting trigger assets. The script is
the reviewed standalone form of `TRIGGER_DDL`; application startup and package
promotion do not execute it. Historical delivery identity/payloads remain
untouched on trigger deactivation or package updates. Already reserved runs do
not retarget when the workflow is later published at a new version.

## Verification

The mocked tests cover transaction rollback, repeat dedupe, type allowlists,
inactive triggers, missing/wrong tenant/context/permissions, immutable payload
integrity, no inline effects, queued-only SKIP LOCKED claims, durable run reuse,
publication gates, executor contract rejection, persisted-completion checks and
retry safety. They execute no live database, LLM, SOLF, or domain action.

Before production, add PostgreSQL integration tests under a NOBYPASSRLS role for
DDL application, actual RLS isolation, payload/run immutability, concurrent
enqueue/claim and kill-at-each-commit crash recovery. Mocked SQL assertions are
not evidence of real PostgreSQL concurrency or domain exactly-once behavior.