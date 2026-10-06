# IKOS Security, External Application Integration, and Bootstrap

## Purpose

This document defines the security model for an organization that integrates an
external application directly with IKOS-Kernel. The external application owns
its users, authentication, sessions, tenants, roles, and business permissions.
IKOS is an internal trusted service that receives API requests, enforces the
provided authorization context, and retrieves or mutates authorized data.

The design also defines the IKOS system administrator used during deployment
and bootstrap. This administrator is configured outside the IKOS API and is not
a normal tenant user.

## Security Boundaries

```text
+----------------------+       authenticated service request       +----------------+
| External application | ----------------------------------------> | IKOS API       |
|                      |  user, tenant, permissions, request ID   |                |
| - login              |                                           | - validate     |
| - users              |                                           | - authorize    |
| - tenants            |                                           | - retrieve     |
| - roles              |                                           | - audit        |
+----------------------+                                           +--------+-------+
                                                                            |
                                                                            | tenant/RLS,
                                                                            | source policy
                                                                            v
                                                               +------------+-------------+
                                                               | IKOS DB and source DBs  |
                                                               +--------------------------+
```

The external application is the **identity and authorization authority**. IKOS
is the **enforcement point** for every IKOS query, document retrieval, external
source read, and workflow action. Network isolation alone is not an
authorization mechanism.

## Responsibilities

### External application

The external application is responsible for:

- authenticating the human user;
- managing login sessions and account lifecycle;
- selecting the active tenant;
- managing users, tenant membership, roles, and permissions;
- disabling users and revoking access;
- creating a short-lived, authenticated request assertion for IKOS;
- sending only the permissions applicable to the current user and tenant.

The external application must never send passwords to IKOS. IKOS does not need
to know or store external user passwords.

### IKOS-Kernel

IKOS is responsible for:

- authenticating the external application request;
- validating the request identity and tenant context;
- rejecting expired, malformed, replayed, or insufficiently scoped requests;
- creating a request-scoped security context;
- applying tenant filtering and PostgreSQL row-level security;
- enforcing permission checks at retrieval and execution time;
- applying document sensitivity rules such as finance and restricted access;
- validating external source ownership and source access policy;
- recording security-relevant audit events;
- never trusting query text, category routing, or client payloads as authorization.

## Request Authentication Contract

### Preferred production contract

Use both:

1. **Mutual TLS** to authenticate the external application service; and
2. **A short-lived signed request assertion** to carry the user context.

The assertion should be signed by the external application or its local
security component. IKOS should validate the signature against a configured
public key. No third-party identity provider is required for this integration.

Example claims:

```json
{
  "iss": "external-application",
  "aud": "ikos-api",
  "sub": "external-user-123",
  "tenant_id": "tenant-uuid",
  "permissions": ["documents.read", "finance.read"],
  "roles": ["finance_viewer"],
  "request_id": "request-uuid",
  "iat": 1760000000,
  "exp": 1760000300,
  "jti": "assertion-uuid"
}
```

IKOS should validate:

- certificate/client identity;
- signature and configured public key;
- issuer and audience;
- issued-at and expiration time;
- tenant ID format and tenant mapping;
- subject presence;
- permission format and allowed scopes;
- `jti` replay protection when assertions can be replayed;
- request ID for tracing and audit.

### Current implementation contract

IKOS now supports the signed assertion contract above using RS256. Configure
the public key and claim constraints outside the API:

```text
IKOS_INTEGRATION_JWT_PUBLIC_KEY_PATH=C:\secrets\external-app-public.pem
IKOS_INTEGRATION_JWT_ISSUER=external-application
IKOS_INTEGRATION_JWT_AUDIENCE=ikos-api
IKOS_INTEGRATION_JWT_CLOCK_SKEW_SECONDS=30
```

Send the assertion as `Authorization: Bearer <compact-jws>`. The middleware
validates the signature and claims, verifies that the tenant is active, and
places the external permissions and roles in the request context. Endpoints
use those permissions without replacing them with a local membership lookup.
The external application remains responsible for revocation by stopping new
assertions; keep assertion lifetimes short.

The API also retains the trusted-proxy header path during migration:

The current IKOS API uses a trusted authentication proxy contract:

- `X-IKOS-Auth-Secret`
- `X-IKOS-User-ID`
- `X-IKOS-Tenant-ID`

The proxy must authenticate the external application/user and overwrite these
headers. It must remove any caller-provided copies before forwarding the
request. The shared secret authenticates the trusted proxy path; it is not a
replacement for signed assertions or mTLS in a higher-risk deployment.

Relevant implementation locations:

- `ikos_api_server/application.py`: request authentication and context setup;
- `security_context.py`: request-scoped security context;
- `ikos_api_server/deps.py`: permission dependencies;
- `object_db.py`: tenant membership and permission resolution.

Authenticated request decisions are persisted in `security_audit_event` with
the subject, tenant, request ID, endpoint, authorization source, outcome, and
HTTP status. Assertions, passwords, and request payloads are not stored.

The application validates one configured public key at a time. Certificate
rotation, multiple `kid` values, mTLS, replay-cache enforcement for `jti`, and
external-database authorization views remain deployment/integration work. Put
mTLS at the internal proxy or service mesh, rotate the configured public key
through deployment configuration, and add a replay store if the threat model
requires protection against assertion reuse.

## Security Context

Each ordinary tenant request should resolve to a context equivalent to:

```text
external_subject = stable external user subject
tenant_id         = active external tenant mapped to IKOS
roles             = external roles, for audit and policy context
permissions       = effective permissions for this request
request_id        = external request identifier
authorization_source = external_application
```

The subject must be a stable external identifier, not an email address. The
IKOS internal `ikos_user.user_id` can be maintained as a local projection for
references and audit, but it must not become an independently conflicting
identity authority.

## Authorization Enforcement

Authorization must happen after parsing and before retrieval or execution.
Query classification, category registry resolution, semantic matching, and
source routing answer where data may be located. They do not answer whether the
caller may access it.

The enforcement sequence is:

1. Authenticate the external application and request assertion.
2. Resolve the external subject and tenant mapping.
3. Build the security context.
4. Check the endpoint permission.
5. Apply tenant scope to every database query.
6. Apply resource permissions, for example:
   - `documents.read`
   - `documents.write`
   - `documents.restricted.read`
   - `finance.read`
   - `finance.write`
   - source-specific permissions such as `source.invoice.read`
7. Enforce the same scope in federated source reads.
8. Apply RLS and sensitivity policies in PostgreSQL.
9. Return only authorized evidence and provenance.
10. Audit the request, decision, source, and result classification.

A successful category match, a valid SQL plan, or a caller-supplied role must
never bypass authorization.

## External Database Inspection

IKOS may inspect the external application's database, but direct inspection
should be supplemental rather than the sole authorization mechanism.

If database-backed authorization is required, expose a versioned, read-only
integration view or stored procedure such as:

```text
ikos_user_access(
    external_subject,
    external_tenant_id,
    permission_key,
    valid_until,
    access_version
)
```

Recommended rules:

- use a read-only database account;
- expose only required columns;
- never expose password hashes or session secrets;
- require tenant and subject parameters;
- include an authorization version or update timestamp;
- use a short cache only when revocation requirements allow it;
- fail closed when the authorization source is unavailable for sensitive data.

Do not store external database passwords in `source_database_registry` metadata,
`connection_hint`, category metadata, or query payloads. Use environment
secrets, a local secret store, or deployment-managed credentials.

## IKOS System Administrator Bootstrap

The system administrator is a platform control-plane identity. It is not
created through an IKOS endpoint and does not need membership in a tenant.

Configure exact, stable administrator subjects outside the API using:

```text
IKOS_PLATFORM_ADMIN_SUBJECTS=external-platform-admin-subject
```

Multiple subjects may be comma-separated. Use issuer-qualified subjects when
possible. Do not use email addresses as the allowlist key.

The bootstrap administrator may:

- initialize or verify the IKOS schema;
- register the external application's integration and public key;
- configure tenant mappings;
- create a tenant;
- create the first tenant administrator membership;
- rotate integration keys or disable an integration;
- perform audited platform maintenance.

The bootstrap administrator must not automatically receive access to tenant
documents, finance records, external source records, or workflow data.

### Online bootstrap endpoint

The isolated control-plane endpoint is:

```text
POST /api/platform/tenants
```

It creates the following atomically:

1. tenant row;
2. external subject projection in `ikos_user`;
3. active `tenant_membership` row;
4. fixed `tenant_admin` role assignment.

The platform administrator does not select arbitrary permissions in this
operation. The first tenant administrator subsequently manages tenant users and
roles through the tenant-scoped security API.

### Publishing shared definitions

The allowlisted platform administrator can publish shared definitions through
the separate platform control plane:

- `POST /api/platform/configuration/business-rules`
- `POST /api/platform/configuration/solf-clauses`
- `POST /api/platform/configuration/workflows`
- `POST /api/platform/configuration/generated-scripts`
- `POST /api/platform/configuration/templates` (multipart upload)

These routes set `configuration_scope = 'global'` server-side; callers cannot
choose a scope on tenant APIs. A global workflow may reference only globally
published SOLF clauses. Shared entries are readable/executable in active
tenants, but tenant users cannot change their definitions or activation state.
The tenant-admin role receives `tenant.configuration.manage` for tenant-local
configuration. Only the out-of-band platform-admin allowlist receives
`platform.configuration.publish`.

### Offline bootstrap command

A trusted deployment operator can provision without starting the API or client:

```powershell
Set-Location C:\Project\WebTech\python\IKOS-Kernel
$env:IKOS_DB_NAME = "ikos_dev"
$env:IKOS_PLATFORM_ADMIN_SUBJECTS = "external-platform-admin-subject"

& ..\.venv\Scripts\python.exe .\scripts\provision_tenant.py `
  --actor-subject "external-platform-admin-subject" `
  --tenant-key "acme" `
  --display-name "Acme Corporation" `
  --admin-subject "external-tenant-admin-subject" `
  --admin-display-name "Acme Tenant Administrator"
```

The command uses the PostgreSQL settings loaded by `ikos_config.py`, checks that
the actor is in the out-of-band platform-admin allowlist, and performs the
same atomic tenant-plus-first-admin operation.

## Tenant Administration After Bootstrap

Once the first tenant administrator exists:

- the external application authenticates tenant users;
- the external application sends their current tenant and permissions to IKOS;
- IKOS validates and enforces those permissions;
- tenant administrators manage membership through the tenant-scoped API;
- platform administrators remain outside ordinary tenant data access.

The tenant role `tenant_admin` is therefore different from the platform
administrator. `tenant_admin` controls one tenant; the platform administrator
controls IKOS deployment and tenant provisioning.

## Global and Tenant-Owned Workflows, Rules, Actions, and Templates

Executable configuration has two explicit scopes:

- **Tenant scope (default):** created for one tenant and invisible to every
  other tenant.
- **Global scope (explicit publication):** created by developers or the IKOS
  system administrator, readable and executable in every active tenant, and
  immutable to ordinary tenant users.

Configuration records use `configuration_scope = 'tenant' | 'global'` alongside
their `tenant_id`. Runtime RLS exposes a row when it belongs to the active
tenant or is explicitly global. RLS `WITH CHECK` allows tenant-owned writes but
requires `platform.configuration.publish` to insert or modify a global row.
Tenant APIs default to tenant scope and must not accept a client-supplied scope
as authorization. Global configuration publication belongs to a trusted
deployment/admin path. Global definitions should be active/published by their
maintainer; tenant users may invoke them through normal tenant-authorized
requests but cannot edit their definition or global active state.

Tenant isolation is applied to business rules, SOLF clauses, workflow
definitions/versions/steps, workflow links, resource aliases, extensions, and
generated script definitions. Workflow runs, generated-script execution audits,
action plans/logs, and clarification state remain tenant-private even when the
definition they execute is global. Composite tenant-aware foreign keys prevent
tenant-owned children from accidentally referring to another tenant's private
configuration.

The universal query-pattern registry and static Python built-in workflows are
kernel-wide code/policy, not tenant-authored database configuration. A tenant
may use active global definitions and its own definitions; tenant-specific
definitions take precedence when a key or SOLF concept overlaps.

Document template reference registries and files are stored in tenant-specific
directories, with a separately managed `_global` registry. Lookup prefers a
tenant-specific template and falls back to the global registry. Existing
template files at the former shared location remain assigned to the legacy
tenant; platform administrators may explicitly republish a template globally.

The additive schema migration assigns existing unscoped workflow/rule/SOLF
rows to the legacy tenant with `configuration_scope = 'tenant'`; it does not
silently publish old records globally. The application database role must not
be a superuser or have `BYPASSRLS`, because those roles bypass PostgreSQL RLS.

RLS is a defense-in-depth boundary. The application's PostgreSQL role must not
be a superuser or have `BYPASSRLS`; use a restricted runtime DB role. Keep
creation and migration privileges separate from normal API runtime credentials
where practical.

## Key Rotation and Revocation

The external application integration should support:

- active and retiring public keys with key IDs;
- short assertion lifetimes;
- immediate integration disablement;
- subject and tenant revocation;
- request replay detection where needed;
- audit records for authorization failures;
- clock synchronization across systems.

When the external application revokes access, new assertions must fail or no
longer contain the permission. IKOS should not continue serving sensitive data
from a stale authorization cache.

## Audit Requirements

Record at least:

- request ID;
- external subject;
- tenant ID;
- endpoint and operation;
- permission decision;
- source scope and access method;
- document/object/source identifiers where appropriate;
- denial reason;
- platform-admin provisioning actor;
- tenant and initial-admin identifiers created;
- key or integration version.

Avoid storing access tokens, passwords, private keys, or complete sensitive
payloads in logs.

## Deployment Checklist

### Before first tenant

- [ ] Initialize the IKOS runtime schema.
- [ ] Configure database credentials outside source control.
- [ ] Configure the platform-admin subject allowlist out of band.
- [ ] Configure the external application's mTLS identity or signing public key.
- [ ] Establish the tenant-ID mapping contract.
- [ ] Verify the external application strips and overwrites identity headers.
- [ ] Run a denied-request test with an unconfigured platform subject.
- [ ] Provision the first tenant and tenant administrator.

### Before production traffic

- [ ] Ensure IKOS is reachable only from the external application network path.
- [ ] Use mTLS and signed short-lived assertions where possible.
- [ ] Confirm all retrieval and action paths enforce tenant and permission scope.
- [ ] Confirm PostgreSQL RLS is enabled and forced for tenant-owned tables.
- [ ] Use read-only credentials for external database inspection.
- [ ] Test user deactivation and tenant revocation.
- [ ] Test cross-tenant and finance/restricted access denial.
- [ ] Enable audit collection and protect logs.
- [ ] Define key rotation and emergency integration disablement procedures.

## Security Principle

> The external application owns identity and authorization policy. IKOS owns
> enforcement for every retrieval and action. The bootstrap administrator owns
> only platform setup and tenant provisioning, not tenant business data.
