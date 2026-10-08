# IKOS Kernel Snapshot

This folder is a kernel-focused snapshot extracted from IDMS-Demo.

## What Is Included

- Core object graph, document, part, and attribute runtime
- Full SOLF runtime files (parser, interpreter, SOLF function bridge)
- SOLF rule/fact/clause support modules
- Full RAG core pipeline files (ingest, markdown flow, semantic/vector query engine)
- API package and interaction runtime needed to expose kernel features
- DDL variants under `ddl/`

## Install

Local setup is a virtualenv, PostgreSQL, and Qdrant. See [doc/INSTALL.md](doc/INSTALL.md).

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/python scripts/initialize_ikos_runtime_schema.py --database ikos_dev
.venv/bin/python ikos_api.py --host 127.0.0.1 --port 8010
```

## DDL Options

- `ddl/ikos_kernel_core.sql`
	- Core object/document/semantic kernel only
	- No SOLF tables, no workflow execution tables

- `ddl/ikos_kernel_full.sql`
	- Basic kernel baseline with SOLF + rule/inference + RAG support tables
	- Includes SOLF clauses/rules/facts/predicates, workflow execution runtime, interaction clarification runtime, and event runtime tables
	- Excludes domain-specific tables (ledger/accounting/hr/crm/project, etc.)

## Explicit Exclusions

- Domain-specific business tables and workflows
- Generated artifacts, logs, and UI assets

The Python runtime bootstrap no longer creates HR employee/department/role,
employment/payroll, sales-order/shipment, or sales-pipeline tables. The
sales-pipeline endpoints return HTTP 410 until their storage is implemented in
a domain-specific application. Legacy accounting, CRM, procurement, and project
schema code still exists in the Python runtime and is not covered by this
exclusion; the two baseline SQL files exclude those domain tables.

## Platform Administrator Bootstrap

Platform administrators are allowlisted by deployment configuration, not by an
API endpoint or tenant role. Set `IKOS_PLATFORM_ADMIN_SUBJECTS` to a comma-
separated list of exact, issuer-qualified identity-provider subjects, for
example `https://idp.example/|00u123...`. The trusted authentication proxy must
authenticate the principal, overwrite `X-IKOS-User-ID` with that exact subject,
and set `X-IKOS-Auth-Secret`; never forward caller-supplied identity headers.

An allowlisted subject can call `POST /api/platform/tenants` without a tenant
header. This control-plane identity has no tenant context and can only access
the platform provisioning route. The endpoint creates the tenant and its first
`tenant_admin` membership atomically. New tenant administrators then manage
their tenant's users and assign existing roles through `/api/security/members`.
Platform-admin allowlist changes require an out-of-band deployment config
change and API restart; they cannot be made through IKOS.

For offline provisioning, run `scripts/provision_tenant.py` from a trusted host
with PostgreSQL reachable and the IKOS database settings configured. This
command connects directly to PostgreSQL; it does not start the API or client.
Initialize the runtime schema first so the built-in `tenant_admin` role exists.

## Versioned Domain Extension Packages

Platform administrators can register immutable extension releases, inspect
tenant-scoped changes, and promote supported SOLF, clause/capability workflow,
vocabulary, and tenant-only ingestion-trigger assets through
`/api/platform/extensions`. Capability references require exact trusted
registrations on each application/worker process; ingestion triggers must refer
to a workflow bundled in the same package. The package registry is stored in
the IKOS database and guarded by forced RLS plus
`platform.configuration.publish`. See
[`doc/EXTENSION_PACKAGE_PROMOTION.md`](doc/EXTENSION_PACKAGE_PROMOTION.md) for
the manifest, API sequence, limitations, and rollback behavior. Apply the
additive registry migration or run the normal non-destructive schema bootstrap
before using these endpoints.

An updated, tenant-installable domain-neutral vocabulary/query baseline is
provided at [`extensions/core-knowledge/1.1.0/extension.json`](extensions/core-knowledge/1.1.0/extension.json).
It expands the common terms to cover temporal, quantitative, spatial, inquiry,
document, workflow, logical, and conversational vocabulary. It is a versioned
package artifact; it is not automatically installed into existing databases
or tenants. See [`doc/CORE_KNOWLEDGE_LEXICON.md`](doc/CORE_KNOWLEDGE_LEXICON.md)
for coverage, limitations, and promotion steps. The `1.0.0` manifest remains
unchanged as an immutable release.
Example PowerShell invocation:

```powershell
$env:IKOS_DB_NAME = "ikos_dev"
$env:IKOS_PLATFORM_ADMIN_SUBJECTS = "https://idp.example/|00u123..."
..\.venv\Scripts\python.exe .\scripts\provision_tenant.py `
	--actor-subject "https://idp.example/|00u123..." `
	--tenant-key "acme" `
	--display-name "Acme Corporation" `
	--admin-subject "https://idp.example/|00u456..." `
	--admin-display-name "Acme Tenant Administrator"
```

The actor-subject allowlist check prevents accidental use of an unconfigured
identity, but it does not authenticate the local operator by itself. Protect
the host, environment configuration, and PostgreSQL provisioning credentials;
restrict database privileges and record local command execution according to
your operational audit policy. Never pass passwords on the command line.

For application-to-IKOS requests, configure an external application's RS256

## Global and Tenant Configuration

Developer/system-admin-published rules, SOLF clauses, workflows, extensions,
and generated script definitions can be marked `configuration_scope='global'`.
Active global definitions are readable and usable in every active tenant;
tenant-specific definitions remain private and override a global definition
with the same key where the runtime supports overrides. Ordinary tenant
requests cannot publish or modify global definitions. Publishing requires the
`platform.configuration.publish` permission in the database request context or
an out-of-band deployment/database administration process. Workflow runs,
generated-script audit records, action plans, and clarification state remain
tenant-private.

New user-authored definitions default to tenant scope. Existing configuration
rows are migrated into the legacy tenant, not automatically shared. The runtime
database account must not be PostgreSQL superuser or have `BYPASSRLS`; use a
restricted application role so forced row-level security is effective.
public key with `IKOS_INTEGRATION_JWT_PUBLIC_KEY_PATH` or
`IKOS_INTEGRATION_JWT_PUBLIC_KEY_PEM`, plus `IKOS_INTEGRATION_JWT_ISSUER` and
`IKOS_INTEGRATION_JWT_AUDIENCE`. Send short-lived assertions in the
`Authorization: Bearer` header with `sub`, `tenant_id`, `permissions`, `iat`,
and `exp` claims. The existing trusted-proxy header contract remains available
for migration. Put mTLS at the internal reverse proxy or service mesh boundary;
IKOS application code validates the assertion and enforces its permissions.
