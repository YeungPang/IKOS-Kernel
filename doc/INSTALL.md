# Installing IKOS-Kernel

This is the normal local install: a Python virtualenv, one PostgreSQL database, and Qdrant. The API can start before any documents are ingested. Model calls need an OpenRouter key. PDF ingest needs the extra requirements file and a LlamaCloud key.

## What you need

- Python 3.13
- PostgreSQL 14 or newer, with the `pg_trgm` extension available
- Qdrant listening on port 6333
- Network access to install Python packages

The application role must not be a superuser and must not have `BYPASSRLS`. Row-level security is ignored for those roles.

## PostgreSQL

Create a database and a restricted login role. The role that owns the database can create the trusted `pg_trgm` extension.

```sql
CREATE ROLE ikos_app WITH LOGIN PASSWORD 'replace-me'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
CREATE DATABASE ikos_dev OWNER ikos_app;
\c ikos_dev
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

## Qdrant

Run Qdrant so `http://127.0.0.1:6333/healthz` returns HTTP 200. An existing Qdrant process is fine. Point `QDRANT_COLLECTION` at a new collection name. IKOS creates that collection on the first ingest, using dense vectors of size `IKOS_QDRANT_EMBEDDING_DIMENSION` (3072 for `openai/text-embedding-3-large`). Reusing an older collection with a different vector size fails until the collection is rebuilt.

## Python

From this directory:

```bash
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Edit `.env`. At minimum set `IKOS_DB_PASSWORD` and `IKOS_TRUSTED_AUTH_PROXY_SECRET`. Leave `OPENROUTER_API_KEY` empty until you want live answers. `.env` stays in this directory; `ikos_config.py` loads it from here.

PDF and office ingest also needs:

```bash
.venv/bin/pip install -r requirements-ingest.txt
```

Set `LLAMA_CLOUD_API_KEY` before ingesting those files. spaCy is optional and is used only when the package is installed.

## Schema

```bash
.venv/bin/python scripts/initialize_ikos_runtime_schema.py --database ikos_dev
```

This creates the runtime tables. It does not drop existing tables. Run it before provisioning a tenant, so the built-in `tenant_admin` role exists.

## API

```bash
.venv/bin/python ikos_api.py --host 127.0.0.1 --port 8010
```

Check that the process is serving:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8010/openapi.json
curl -sS -w '\n%{http_code}\n' http://127.0.0.1:8010/api/health
```

`/openapi.json` and `/docs` return HTTP 200 with no credentials. `/api/health` returns HTTP 401, `Trusted authentication is required`, until the request includes the proxy secret. A missing Google credentials file is logged at startup and does not stop the server.

A request that should reach a route also needs a user and a tenant:

```bash
curl -sS \
  -H "X-IKOS-Auth-Secret: $IKOS_TRUSTED_AUTH_PROXY_SECRET" \
  -H "X-IKOS-User-ID: external-user-subject" \
  -H "X-IKOS-Tenant-ID: 00000000-0000-0000-0000-000000000000" \
  http://127.0.0.1:8010/api/health
```

The header path loads permissions from IKOS membership. The preferred path is a short-lived RS256 bearer assertion. See [SECURITY_EXTERNAL_APP_INTEGRATION_AND_BOOTSTRAP.md](SECURITY_EXTERNAL_APP_INTEGRATION_AND_BOOTSTRAP.md).

Document question answering is `POST /api/query` with JSON `{"question": "...", "max_steps": 3}`. File ingest is `POST /api/ingest/upload` and requires `documents.write`.

## First tenant

Set `IKOS_PLATFORM_ADMIN_SUBJECTS` to the operator subject, then:

```bash
.venv/bin/python scripts/provision_tenant.py \
  --actor-subject "external-platform-admin-subject" \
  --tenant-key "acme" \
  --display-name "Acme Corporation" \
  --admin-subject "external-tenant-admin-subject" \
  --admin-display-name "Acme Tenant Administrator"
```

The command prints the tenant UUID. That UUID is the `tenant_id` later requests must send. The actor subject must match `IKOS_PLATFORM_ADMIN_SUBJECTS` exactly.
