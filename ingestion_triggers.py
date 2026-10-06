"""Tenant-scoped transactional ingestion outbox; no inline workflow execution.

Install TRIGGER_DDL explicitly. dispatch_one owns commits on a dedicated worker
connection; enqueue_document_ingested never commits. See the integration guide.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Callable, Protocol
from uuid import UUID

from security_context import SecurityContext, get_security_context


TRIGGER_DDL = """
CREATE TABLE IF NOT EXISTS ikos_ingestion_trigger (
    tenant_id UUID NOT NULL REFERENCES tenant(tenant_id),
    trigger_key TEXT NOT NULL CHECK (btrim(trigger_key) <> ''),
    workflow_key TEXT NOT NULL CHECK (btrim(workflow_key) <> ''),
    document_types TEXT[] NOT NULL DEFAULT '{}',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(metadata) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, trigger_key),
    CHECK (array_position(document_types, NULL) IS NULL)
);
CREATE TABLE IF NOT EXISTS ikos_ingestion_delivery (
    delivery_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    trigger_key TEXT NOT NULL,
    event_key TEXT NOT NULL,
    event_payload JSONB NOT NULL CHECK (jsonb_typeof(event_payload) = 'object'),
    event_sha256 TEXT NOT NULL CHECK (event_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    run_id BIGINT REFERENCES workflow_pipeline_run(run_id),
    workflow_key TEXT,
    workflow_version_id BIGINT REFERENCES solf_workflow_versions(workflow_version_id),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, trigger_key, event_key),
    UNIQUE (run_id),
    CHECK ((run_id IS NULL AND workflow_key IS NULL AND workflow_version_id IS NULL)
        OR (run_id IS NOT NULL AND workflow_key IS NOT NULL AND workflow_version_id IS NOT NULL)),
    CHECK (status <> 'completed' OR run_id IS NOT NULL),
    FOREIGN KEY (tenant_id, trigger_key)
        REFERENCES ikos_ingestion_trigger(tenant_id, trigger_key)
);
CREATE INDEX IF NOT EXISTS ikos_ingestion_delivery_queue
    ON ikos_ingestion_delivery(tenant_id, delivery_id) WHERE status = 'queued';

CREATE OR REPLACE FUNCTION ikos_ingestion_delivery_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.tenant_id, NEW.trigger_key, NEW.event_key,
           NEW.event_payload, NEW.event_sha256)
       IS DISTINCT FROM ROW(OLD.tenant_id, OLD.trigger_key, OLD.event_key,
                            OLD.event_payload, OLD.event_sha256) THEN
        RAISE EXCEPTION 'ingestion event identity and payload are immutable';
    END IF;
    IF OLD.run_id IS NOT NULL AND
       ROW(NEW.run_id, NEW.workflow_key, NEW.workflow_version_id)
       IS DISTINCT FROM ROW(OLD.run_id, OLD.workflow_key, OLD.workflow_version_id) THEN
        RAISE EXCEPTION 'ingestion run identity is immutable';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS ikos_ingestion_delivery_immutable ON ikos_ingestion_delivery;
CREATE TRIGGER ikos_ingestion_delivery_immutable BEFORE UPDATE ON ikos_ingestion_delivery
    FOR EACH ROW EXECUTE FUNCTION ikos_ingestion_delivery_immutable();

ALTER TABLE ikos_ingestion_trigger ENABLE ROW LEVEL SECURITY;
ALTER TABLE ikos_ingestion_trigger FORCE ROW LEVEL SECURITY;
ALTER TABLE ikos_ingestion_delivery ENABLE ROW LEVEL SECURITY;
ALTER TABLE ikos_ingestion_delivery FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ikos_ingestion_trigger_tenant ON ikos_ingestion_trigger;
CREATE POLICY ikos_ingestion_trigger_tenant ON ikos_ingestion_trigger AS RESTRICTIVE
    USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);
DROP POLICY IF EXISTS ikos_ingestion_trigger_access ON ikos_ingestion_trigger;
CREATE POLICY ikos_ingestion_trigger_access ON ikos_ingestion_trigger
    USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);
DROP POLICY IF EXISTS ikos_ingestion_delivery_tenant ON ikos_ingestion_delivery;
CREATE POLICY ikos_ingestion_delivery_tenant ON ikos_ingestion_delivery AS RESTRICTIVE
    USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);
DROP POLICY IF EXISTS ikos_ingestion_delivery_access ON ikos_ingestion_delivery;
CREATE POLICY ikos_ingestion_delivery_access ON ikos_ingestion_delivery
    USING (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = NULLIF(current_setting('ikos.tenant_id', true), '')::uuid);
"""


class IngestionExecutor(Protocol):
    """Trusted adapter: execute the reserved run, never allocate another one.

    Must enforce run/step idempotency at every domain mutation boundary, use
    connections bound to the current SecurityContext, and return a persisted
    run_id/run_status. This is NOT start_pipeline_run's contract.
    """

    def execute_ingestion_run(
        self, *, run_id: int, workflow_version_id: int,
        input_context: dict[str, Any], idempotency_key: str,
    ) -> dict[str, Any]: ...


def _context(connection: Any, *, worker: bool = False) -> SecurityContext:
    context = get_security_context(required=True)
    if context is None or not context.tenant_id or not context.user_id:
        raise PermissionError('An explicit tenant and principal are required')
    tenant_id = str(UUID(context.tenant_id))
    required = ['documents.write'] + (['workflows.run'] if worker else [])
    if any(not context.allows(permission) for permission in required):
        raise PermissionError('Missing ingestion permission: ' + ', '.join(required))
    # Never manufacture DB authority or use get_tenant_id's legacy fallback.
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_setting('ikos.tenant_id', true), "
                       "current_setting('ikos.user_id', true), "
                       "current_setting('ikos.permissions', true)")
        row = cursor.fetchone()
    if not row or not row[0] or str(UUID(row[0])) != tenant_id or row[1] != context.user_id:
        raise PermissionError('Database security context does not match active principal')
    if frozenset(filter(None, (row[2] or '').split(','))) != context.permissions:
        raise PermissionError('Database permissions do not match active principal')
    return context


def _canonical_type(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError('document_type must be a nonempty string')
    canonical = '_'.join(value.strip().lower().replace('-', ' ').replace('_', ' ').split())
    if not canonical:
        raise ValueError('document_type must contain a type name')
    return canonical


def _encoded(payload: dict[str, Any]) -> tuple[str, str]:
    text = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False)
    return text, hashlib.sha256(text.encode('utf-8')).hexdigest()


def inspect_trigger(trigger: dict[str, Any]) -> dict[str, Any]:
    """Validate/normalize declarative fields without DB access or execution.

    Empty document_types matches every authoritative type. Unknown fields are
    rejected so metadata cannot become executable predicates or authority.
    """
    allowed = {'trigger_key', 'workflow_key', 'document_types', 'is_active', 'metadata'}
    if not isinstance(trigger, dict) or set(trigger) - allowed:
        raise ValueError('Unsupported trigger fields')
    result: dict[str, Any] = {}
    for key in ('trigger_key', 'workflow_key'):
        value = trigger.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(key + ' is required')
        result[key] = value.strip()
    types = trigger.get('document_types', [])
    if not isinstance(types, list):
        raise ValueError('document_types must be a list')
    result['document_types'] = sorted({_canonical_type(value) for value in types})
    active = trigger.get('is_active', True)
    metadata = trigger.get('metadata', {})
    if not isinstance(active, bool) or not isinstance(metadata, dict):
        raise ValueError('is_active must be boolean and metadata must be an object')
    result['is_active'] = active
    result['metadata'] = json.loads(_encoded(metadata)[0])
    return result


def _document(connection: Any, tenant_id: str, doc_id: int, document_type: str) -> str:
    with connection.cursor() as cursor:
        cursor.execute('SELECT tenant_id::text, doc_type FROM document '
                       'WHERE doc_id = %s AND tenant_id = %s::uuid FOR SHARE',
                       (doc_id, tenant_id))
        row = cursor.fetchone()
    if not row or str(row[0]) != tenant_id:
        raise PermissionError('Document is not accessible in the active tenant')
    authoritative = _canonical_type(row[1])
    if authoritative != _canonical_type(document_type):
        raise ValueError('document_type does not match the stored document')
    return authoritative


def enqueue_document_ingested(connection: Any, doc_id: int, document_type: str) -> list[int]:
    """Insert matching queued deliveries in the caller's ingestion transaction.

    Returns only newly inserted IDs. No commits, workflow imports, or effects.
    """
    if isinstance(doc_id, bool) or not isinstance(doc_id, int) or doc_id <= 0:
        raise ValueError('doc_id must be a positive integer')
    if getattr(connection, 'autocommit', False):
        raise ValueError('Enqueue requires autocommit=False for transactional ingestion')
    context = _context(connection)
    tenant_id = str(UUID(context.tenant_id))
    authoritative = _document(connection, tenant_id, doc_id, document_type)
    payload = {'doc_refs': [doc_id], 'document_id': doc_id, 'document_type': authoritative}
    text, digest = _encoded(payload)
    with connection.cursor() as cursor:
        cursor.execute('SELECT trigger_key, document_types FROM ikos_ingestion_trigger '
                       'WHERE tenant_id = %s::uuid AND is_active = TRUE', (tenant_id,))
        triggers = cursor.fetchall()
    inserted = []
    for trigger_key, types in triggers:
        if types and authoritative not in {_canonical_type(value) for value in types}:
            continue
        with connection.cursor() as cursor:
            cursor.execute('INSERT INTO ikos_ingestion_delivery '
                           '(tenant_id, trigger_key, event_key, event_payload, event_sha256) '
                           'VALUES (%s::uuid, %s, %s, %s::jsonb, %s) '
                           'ON CONFLICT (tenant_id, trigger_key, event_key) DO NOTHING '
                           'RETURNING delivery_id',
                           (tenant_id, trigger_key, f'document:{doc_id}:ingested', text, digest))
            row = cursor.fetchone()
        if row:
            inserted.append(int(row[0]))
    return inserted


def _workflow(connection: Any, key: str) -> tuple[int, int]:
    # Fixed kernel import only; never import a module named in a trigger/package.
    import object_db

    workflow = object_db.get_solf_workflow_registry_by_key(connection, workflow_key=key)
    if not workflow or not workflow.get('is_active') or workflow.get('status') != 'published':
        raise ValueError('Target workflow must be active and published')
    version = object_db.get_workflow_active_version(connection, workflow_id=workflow['workflow_id'])
    if (not version or not version.get('is_active')
            or version.get('metadata', {}).get('status') != 'published'
            or version.get('workflow_id') != workflow['workflow_id']):
        raise ValueError('Target workflow version must be active and explicitly published')
    return int(workflow['workflow_id']), int(version['workflow_version_id'])


def dispatch_one(
    connection: Any, executor_factory: Callable[[], IngestionExecutor],
) -> dict[str, Any] | None:
    """Claim one queued delivery and execute outside the outbox transaction.

    Dedicated connection only: commits the claim, reserved run, then final status.
    Never reclaims running/failed rows automatically. Retry via retry_failed.
    """
    context = _context(connection, worker=True)
    if getattr(connection, 'autocommit', False):
        raise ValueError('Worker requires autocommit=False')
    tenant_id = str(UUID(context.tenant_id))
    with connection.cursor() as cursor:
        cursor.execute("SELECT delivery_id, trigger_key, event_key, event_payload, event_sha256, "
                       "run_id, workflow_key, workflow_version_id FROM ikos_ingestion_delivery "
                       "WHERE tenant_id = %s::uuid AND status = 'queued' "
                       "ORDER BY delivery_id FOR UPDATE SKIP LOCKED LIMIT 1", (tenant_id,))
        row = cursor.fetchone()
        if not row:
            connection.commit()
            return None
        delivery_id, trigger_key, event_key, payload, digest, run_id, key, version_id = row
        cursor.execute("UPDATE ikos_ingestion_delivery SET status = 'running', "
                       "attempts = attempts + 1, last_error = NULL, modified_at = NOW() "
                       "WHERE delivery_id = %s AND tenant_id = %s::uuid AND status = 'queued'",
                       (delivery_id, tenant_id))
        if cursor.rowcount != 1:
            raise RuntimeError('Delivery claim was lost')
    connection.commit()  # Durable before ANY helper with possible internal commits.
    durable_run_id = run_id
    try:
        _context(connection, worker=True)
        if not isinstance(payload, dict) or set(payload) != {'doc_refs', 'document_id', 'document_type'}:
            raise ValueError('Invalid authoritative event payload')
        doc_id = payload['document_id']
        if isinstance(doc_id, bool) or not isinstance(doc_id, int) or doc_id <= 0:
            raise ValueError('Invalid event document ID')
        if (_encoded(payload)[1] != digest or payload['doc_refs'] != [doc_id]
                or event_key != f'document:{doc_id}:ingested'):
            raise ValueError('Event identity/hash mismatch')
        _document(connection, tenant_id, doc_id, payload['document_type'])
        with connection.cursor() as cursor:
            cursor.execute('SELECT workflow_key, is_active FROM ikos_ingestion_trigger '
                           'WHERE tenant_id = %s::uuid AND trigger_key = %s',
                           (tenant_id, trigger_key))
            trigger = cursor.fetchone()
        if not trigger or trigger[1] is not True:
            raise ValueError('Trigger is missing or inactive')
        if run_id is None:
            key = trigger[0]
            _, version_id = _workflow(connection, key)
        else:
            # A retry may not silently switch the workflow/version beneath a run.
            if key != trigger[0] or not version_id:
                raise ValueError('Reserved workflow identity no longer matches trigger')
            with connection.cursor() as cursor:
                cursor.execute('SELECT tenant_id::text, workflow_version_id, run_status '
                               'FROM workflow_pipeline_run WHERE run_id = %s AND tenant_id = %s::uuid',
                               (run_id, tenant_id))
                reserved = cursor.fetchone()
            if not reserved or reserved[0] != tenant_id or reserved[1] != version_id:
                raise PermissionError('Reserved run does not belong to this tenant/version')
            _, active_id = _workflow(connection, key)
            if active_id != version_id:
                raise ValueError('Reserved workflow version is no longer the published active version')
        executor = executor_factory()
        if not callable(getattr(executor, 'execute_ingestion_run', None)):
            raise TypeError('Executor must implement the run-scoped execute_ingestion_run contract')
        if get_security_context() != context:
            raise PermissionError('Executor factory changed the active security context')
        _context(connection, worker=True)
        # Runtime context is constructed here, not extracted from a document.
        idempotency_key = f'ingestion:{tenant_id}:{delivery_id}'
        input_context = dict(payload, idempotency_key=idempotency_key)
        if run_id is None:
            with connection.cursor() as cursor:
                cursor.execute("INSERT INTO workflow_pipeline_run "
                               "(tenant_id, workflow_version_id, workflow_key, run_status, "
                               "input_context, current_context, output_context, started_by, started_at) "
                               "VALUES (%s::uuid, %s, %s, 'pending', %s::jsonb, %s::jsonb, "
                               "'{}'::jsonb, %s, NOW()) RETURNING run_id",
                               (tenant_id, version_id, key, _encoded(input_context)[0],
                                _encoded(input_context)[0], context.user_id))
                run_id = int(cursor.fetchone()[0])
                cursor.execute('UPDATE ikos_ingestion_delivery SET run_id = %s, workflow_key = %s, '
                               'workflow_version_id = %s WHERE delivery_id = %s AND tenant_id = %s::uuid '
                               "AND status = 'running' AND run_id IS NULL",
                               (run_id, key, version_id, delivery_id, tenant_id))
                if cursor.rowcount != 1:
                    raise RuntimeError('Run reservation was lost')
        connection.commit()  # Run + delivery link atomic; no helper that commits here.
        durable_run_id = run_id
        # Do not touch this connection until the adapter returns. It must use
        # separate same-context connections and durable run/step domain keys.
        result = executor.execute_ingestion_run(
            run_id=run_id, workflow_version_id=version_id,
            input_context=input_context, idempotency_key=idempotency_key,
        )
        if not isinstance(result, dict) or result.get('run_id') != run_id:
            raise ValueError('Executor returned a different/missing reserved run_id')
        if result.get('run_status') != 'completed':
            raise RuntimeError('Workflow did not complete; reconcile the reserved run before retry')
        if get_security_context() != context:
            raise PermissionError('Executor changed the active security context')
        _context(connection, worker=True)
        with connection.cursor() as cursor:
            cursor.execute('SELECT tenant_id::text, workflow_version_id, run_status '
                           'FROM workflow_pipeline_run WHERE run_id = %s AND tenant_id = %s::uuid',
                           (run_id, tenant_id))
            persisted = cursor.fetchone()
        if not persisted or persisted != (tenant_id, version_id, 'completed'):
            raise ValueError('Executor did not persist completion of the reserved tenant run')
        status, error = 'completed', None
    except Exception as exc:
        connection.rollback()  # Never undo the already committed claim/reservation.
        run_id = durable_run_id
        # Do not finalize with switched identity or elevated/revoked permissions.
        if get_security_context() != context:
            raise PermissionError('Worker security context changed; reconcile running delivery') from exc
        _context(connection, worker=True)
        status, error = 'failed', type(exc).__name__  # No extracted data/secrets in last_error.
    with connection.cursor() as cursor:
        cursor.execute('UPDATE ikos_ingestion_delivery SET status = %s, last_error = %s, '
                       'modified_at = NOW() WHERE delivery_id = %s AND tenant_id = %s::uuid '
                       "AND status = 'running'", (status, error, delivery_id, tenant_id))
        if cursor.rowcount != 1:
            raise RuntimeError('Delivery finalization was lost')
    connection.commit()
    return {'delivery_id': delivery_id, 'status': status, 'run_id': run_id, 'error': error}


def enqueue_persisted_document(connection: Any, doc_id: int) -> dict[str, Any]:
    """Post-persistence/reconciliation hook; caller commits, legacy installs skip.

    This cannot make previously committed document writes atomic with the outbox.
    Explicit reruns or reconciliation close that crash gap using event dedupe.
    """
    if isinstance(doc_id, bool) or not isinstance(doc_id, int) or doc_id <= 0:
        raise ValueError('doc_id must be a positive integer')
    context = _context(connection)
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass('ikos_ingestion_trigger'), "
                       "to_regclass('ikos_ingestion_delivery')")
        tables = cursor.fetchone()
        if not tables or not all(tables):
            return {'status': 'schema_not_installed', 'delivery_ids': []}
        cursor.execute('SELECT doc_type FROM document WHERE doc_id=%s AND tenant_id=%s::uuid',
                       (doc_id, str(UUID(context.tenant_id))))
        document = cursor.fetchone()
    if not document:
        raise PermissionError('Document is not accessible in the active tenant')
    if not document[0] or not str(document[0]).strip():
        return {'status': 'unclassified', 'delivery_ids': []}
    return {'status': 'enqueued', 'delivery_ids': enqueue_document_ingested(connection, doc_id, document[0])}


def retry_failed(connection: Any, delivery_id: int) -> bool:
    """Queue a failed delivery explicitly, retaining payload and run identity.

    Caller commits. Paused/cancelled/uncertain runs require reconciliation first;
    the executor must never blindly replay non-idempotent domain effects.
    """
    context = _context(connection, worker=True)
    with connection.cursor() as cursor:
        cursor.execute("UPDATE ikos_ingestion_delivery SET status = 'queued', modified_at = NOW() "
                       "WHERE tenant_id = %s::uuid AND delivery_id = %s AND status = 'failed'",
                       (str(UUID(context.tenant_id)), delivery_id))
        return cursor.rowcount == 1