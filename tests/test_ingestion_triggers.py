"""Outbox contract tests with stateful DB mocks; no live DB/domain execution."""
from __future__ import annotations

from copy import deepcopy
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import ingestion_triggers as triggers
from security_context import SecurityContext, reset_security_context, set_security_context


TENANT = '11111111-1111-1111-1111-111111111111'
OTHER = '22222222-2222-2222-2222-222222222222'
PERMISSIONS = frozenset({'documents.write', 'workflows.run'})


class Connection:
    """Small transactional SQL fake; intentionally rejects unrecognized SQL."""

    def __init__(self):
        self.autocommit = False
        self.settings = (TENANT, 'worker', ','.join(sorted(PERMISSIONS)))
        self.documents = {7: (TENANT, 'Purchase Invoice')}
        self.registry = {'invoice': ('invoice-flow', ['purchase_invoice'], True)}
        self.deliveries = {}
        self.runs = {}
        self.commits = 0
        self.rollbacks = 0
        self.statements = []
        self.in_transaction = False
        self.locked = set()
        self.saved = ({}, {})

    def cursor(self):
        return Cursor(self)

    def commit(self):
        self.commits += 1
        self.saved = deepcopy((self.deliveries, self.runs))
        self.in_transaction = False

    def rollback(self):
        self.rollbacks += 1
        self.deliveries, self.runs = deepcopy(self.saved)
        self.in_transaction = False


class Cursor:
    def __init__(self, connection):
        self.c = connection
        self.rows = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        return self.rows

    def execute(self, sql, params=()):
        c = self.c
        c.in_transaction = True
        c.statements.append((sql, params))
        self.rows, self.rowcount = [], 0
        if sql.startswith('SELECT current_setting'):
            self.rows = [c.settings]
        elif sql.startswith('SELECT tenant_id::text, doc_type'):
            row = c.documents.get(params[0])
            self.rows = [row] if row and row[0] == params[1] else []
        elif sql.startswith('SELECT trigger_key, document_types'):
            self.rows = [(key, value[1]) for key, value in c.registry.items() if value[2]]
        elif sql.startswith('INSERT INTO ikos_ingestion_delivery'):
            tenant, key, event, text, digest = params
            if any((d['tenant'], d['trigger'], d['event']) == (tenant, key, event)
                   for d in c.deliveries.values()):
                return
            delivery_id = len(c.deliveries) + 1
            c.deliveries[delivery_id] = dict(tenant=tenant, trigger=key, event=event,
                payload=json.loads(text), digest=digest, status='queued', run=None,
                key=None, version=None, attempts=0, error=None)
            self.rows = [(delivery_id,)]
        elif sql.startswith('SELECT delivery_id, trigger_key'):
            assert 'FOR UPDATE SKIP LOCKED' in sql
            for delivery_id, d in sorted(c.deliveries.items()):
                if d['tenant'] == params[0] and d['status'] == 'queued' and delivery_id not in c.locked:
                    self.rows = [(delivery_id, d['trigger'], d['event'], deepcopy(d['payload']),
                                  d['digest'], d['run'], d['key'], d['version'])]
                    break
        elif sql.startswith("UPDATE ikos_ingestion_delivery SET status = 'running'"):
            d = c.deliveries[params[0]]
            if d['status'] == 'queued' and d['tenant'] == params[1]:
                d.update(status='running', attempts=d['attempts'] + 1, error=None)
                self.rowcount = 1
        elif sql.startswith('SELECT workflow_key, is_active'):
            trigger = c.registry.get(params[1])
            self.rows = [(trigger[0], trigger[2])] if trigger else []
        elif sql.startswith('INSERT INTO workflow_pipeline_run'):
            run_id = len(c.runs) + 10
            c.runs[run_id] = dict(tenant=params[0], version=params[1], status='pending',
                                  payload=json.loads(params[3]))
            self.rows = [(run_id,)]
        elif sql.startswith('UPDATE ikos_ingestion_delivery SET run_id'):
            run_id, key, version, delivery_id, tenant = params
            d = c.deliveries[delivery_id]
            if d['status'] == 'running' and d['tenant'] == tenant and d['run'] is None:
                d.update(run=run_id, key=key, version=version)
                self.rowcount = 1
        elif sql.startswith('SELECT tenant_id::text, workflow_version_id'):
            run = c.runs.get(params[0])
            if run and run['tenant'] == params[1]:
                self.rows = [(run['tenant'], run['version'], run['status'])]
        elif sql.startswith('UPDATE ikos_ingestion_delivery SET status = %s'):
            status, error, delivery_id, tenant = params
            d = c.deliveries[delivery_id]
            if d['status'] == 'running' and d['tenant'] == tenant:
                d.update(status=status, error=error)
                self.rowcount = 1
        elif sql.startswith("UPDATE ikos_ingestion_delivery SET status = 'queued'"):
            tenant, delivery_id = params
            d = c.deliveries.get(delivery_id)
            if d and d['tenant'] == tenant and d['status'] == 'failed':
                d['status'] = 'queued'
                self.rowcount = 1
        else:
            raise AssertionError('Unexpected SQL: ' + sql)


@pytest.fixture(autouse=True)
def context():
    token = set_security_context(SecurityContext(TENANT, 'worker', PERMISSIONS))
    yield
    reset_security_context(token)


@pytest.fixture
def db():
    return Connection()


@pytest.fixture
def object_db(monkeypatch):
    module = SimpleNamespace(
        get_solf_workflow_registry_by_key=Mock(return_value={
            'workflow_id': 4, 'is_active': True, 'status': 'published'}),
        get_workflow_active_version=Mock(return_value={
            'workflow_id': 4, 'workflow_version_id': 9, 'is_active': True,
            'metadata': {'status': 'published'}}),
    )
    monkeypatch.setitem(sys.modules, 'object_db', module)
    return module


def enqueue(db):
    assert triggers.enqueue_document_ingested(db, 7, 'purchase-invoice') == [1]
    db.commit()


def executor(db, *, status='completed'):
    def execute(**kwargs):
        assert not db.in_transaction, 'Execution must be outside the outbox transaction'
        assert db.deliveries[1]['status'] == 'running'
        assert db.deliveries[1]['run'] == kwargs['run_id']
        assert kwargs['input_context']['idempotency_key'] == kwargs['idempotency_key']
        db.runs[kwargs['run_id']]['status'] = status
        # Simulates an independently committed executor connection.
        db.saved = deepcopy((db.deliveries, db.runs))
        return {'run_id': kwargs['run_id'], 'run_status': status}
    return SimpleNamespace(execute_ingestion_run=Mock(side_effect=execute))


def test_enqueue_is_transactional_deduplicated_and_never_executes(db, monkeypatch):
    poison = SimpleNamespace(get_solf_workflow_registry_by_key=Mock(side_effect=AssertionError))
    monkeypatch.setitem(sys.modules, 'object_db', poison)
    assert triggers.enqueue_document_ingested(db, 7, 'purchase_invoice') == [1]
    assert triggers.enqueue_document_ingested(db, 7, 'Purchase Invoice') == []
    assert db.commits == 0 and not db.runs
    assert db.deliveries[1]['event'] == 'document:7:ingested'
    assert db.deliveries[1]['payload'] == {
        'doc_refs': [7], 'document_id': 7, 'document_type': 'purchase_invoice'}
    assert len(db.deliveries[1]['digest']) == 64
    poison.get_solf_workflow_registry_by_key.assert_not_called()
    db.rollback()
    assert not db.deliveries


def test_predicate_matching_and_inactive_triggers(db):
    db.registry.update(all=('all', [], True), wrong=('wrong', ['receipt'], True),
                       inactive=('inactive', [], False))
    assert triggers.enqueue_document_ingested(db, 7, 'purchase_invoice') == [1, 2]
    assert {d['trigger'] for d in db.deliveries.values()} == {'invoice', 'all'}


@pytest.mark.parametrize('doc_id', [True, 0, -1, '7', 7.0])
def test_invalid_document_ids(db, doc_id):
    with pytest.raises(ValueError):
        triggers.enqueue_document_ingested(db, doc_id, 'invoice')
    assert not db.deliveries


def test_wrong_tenant_document(db):
    db.documents[7] = (OTHER, 'purchase_invoice')
    with pytest.raises(PermissionError):
        triggers.enqueue_document_ingested(db, 7, 'purchase_invoice')
    assert not db.deliveries


def test_authoritative_type_cannot_be_forged(db):
    with pytest.raises(ValueError, match='stored document'):
        triggers.enqueue_document_ingested(db, 7, 'receipt')


@pytest.mark.parametrize('settings', [('', 'worker', ''), (OTHER, 'worker', ''),
                                     (TENANT, 'other-user', ''), (TENANT, 'worker', '*')])
def test_db_context_fail_closed(db, settings):
    db.settings = settings
    with pytest.raises((PermissionError, ValueError)):
        triggers.enqueue_document_ingested(db, 7, 'purchase_invoice')


@pytest.mark.parametrize('permissions', [frozenset(), frozenset({'workflows.run'})])
def test_no_permission_or_admin_bypass(db, permissions):
    token = set_security_context(SecurityContext(TENANT, 'worker', permissions, platform_admin=True))
    try:
        with pytest.raises(PermissionError):
            triggers.enqueue_document_ingested(db, 7, 'purchase_invoice')
    finally:
        reset_security_context(token)


def test_missing_active_context_fails_closed(db, monkeypatch):
    monkeypatch.setattr(triggers, 'get_security_context', Mock(side_effect=RuntimeError('missing')))
    with pytest.raises(RuntimeError):
        triggers.enqueue_document_ingested(db, 7, 'purchase_invoice')


def test_worker_requires_explicit_workflow_permission(db):
    token = set_security_context(SecurityContext(TENANT, 'worker', frozenset({'documents.write'})))
    try:
        with pytest.raises(PermissionError):
            triggers.dispatch_one(db, Mock())
    finally:
        reset_security_context(token)


def test_dispatch_reserves_run_before_execution_and_never_duplicates(db, object_db):
    enqueue(db)
    adapter = executor(db)
    result = triggers.dispatch_one(db, lambda: adapter)
    assert result == {'delivery_id': 1, 'status': 'completed', 'run_id': 10, 'error': None}
    assert triggers.dispatch_one(db, lambda: adapter) is None
    assert len(db.runs) == 1
    adapter.execute_ingestion_run.assert_called_once()
    assert db.deliveries[1]['attempts'] == 1
    object_db.get_solf_workflow_registry_by_key.assert_called_once_with(db, workflow_key='invoice-flow')


def test_skip_locked_and_never_reclaim_running(db, object_db):
    enqueue(db)
    factory = Mock()
    db.locked.add(1)
    assert triggers.dispatch_one(db, factory) is None
    db.locked.clear()
    db.deliveries[1]['status'] = 'running'
    assert triggers.dispatch_one(db, factory) is None
    factory.assert_not_called()


def test_worker_cannot_claim_other_tenant(db, object_db):
    enqueue(db)
    db.deliveries[1]['tenant'] = OTHER
    assert triggers.dispatch_one(db, Mock()) is None


@pytest.mark.parametrize('invalid', ['draft', 'inactive', 'unpublished_version', 'missing'])
def test_unpublished_workflow_fails_without_domain_effects(db, object_db, invalid):
    enqueue(db)
    if invalid == 'draft':
        object_db.get_solf_workflow_registry_by_key.return_value['status'] = 'draft'
    elif invalid == 'inactive':
        object_db.get_solf_workflow_registry_by_key.return_value['is_active'] = False
    elif invalid == 'missing':
        object_db.get_solf_workflow_registry_by_key.return_value = None
    else:
        object_db.get_workflow_active_version.return_value['metadata'] = {}
    factory = Mock()
    assert triggers.dispatch_one(db, factory)['status'] == 'failed'
    assert not db.runs
    factory.assert_not_called()


def test_disabled_trigger_does_not_execute(db, object_db):
    enqueue(db)
    db.registry['invoice'] = ('invoice-flow', [], False)
    factory = Mock()
    assert triggers.dispatch_one(db, factory)['status'] == 'failed'
    factory.assert_not_called()


@pytest.mark.parametrize('tamper', ['digest', 'authority', 'event', 'doc_refs'])
def test_payload_integrity_and_permission_injection_rejected(db, object_db, tamper):
    enqueue(db)
    d = db.deliveries[1]
    if tamper == 'digest':
        d['digest'] = '0' * 64
    elif tamper == 'authority':
        d['payload']['permissions'] = ['*']
        d['digest'] = triggers._encoded(d['payload'])[1]
    elif tamper == 'event':
        d['event'] = 'document:8:ingested'
    else:
        d['payload']['doc_refs'] = [8]
        d['digest'] = triggers._encoded(d['payload'])[1]
    factory = Mock()
    assert triggers.dispatch_one(db, factory)['status'] == 'failed'
    factory.assert_not_called()


def test_start_pipeline_run_alone_is_not_a_safe_executor(db, object_db):
    enqueue(db)
    unsafe = SimpleNamespace(start_pipeline_run=Mock())
    assert triggers.dispatch_one(db, lambda: unsafe)['status'] == 'failed'
    unsafe.start_pipeline_run.assert_not_called()
    assert not db.runs


def test_retry_keeps_reserved_run_and_idempotency_key(db, object_db):
    enqueue(db)
    adapter = executor(db, status='failed')
    assert triggers.dispatch_one(db, lambda: adapter)['status'] == 'failed'
    first = adapter.execute_ingestion_run.call_args.kwargs
    assert triggers.dispatch_one(db, lambda: adapter) is None  # no implicit retry
    assert triggers.retry_failed(db, 1)
    db.commit()
    adapter = executor(db)
    assert triggers.dispatch_one(db, lambda: adapter)['status'] == 'completed'
    assert adapter.execute_ingestion_run.call_args.kwargs == first
    assert len(db.runs) == 1 and db.deliveries[1]['attempts'] == 2
    assert not triggers.retry_failed(db, 1)


def test_executor_exception_retains_durable_run(db, object_db):
    enqueue(db)
    adapter = SimpleNamespace(execute_ingestion_run=Mock(side_effect=RuntimeError('secret')))
    result = triggers.dispatch_one(db, lambda: adapter)
    assert result['run_id'] == 10 and result['status'] == 'failed'
    assert result['error'] == 'RuntimeError'
    assert db.deliveries[1]['run'] == 10


def test_reject_executor_lying_about_completion(db, object_db):
    enqueue(db)
    adapter = SimpleNamespace(execute_ingestion_run=Mock(return_value={'run_id': 10, 'run_status': 'completed'}))
    assert triggers.dispatch_one(db, lambda: adapter)['status'] == 'failed'


def test_retry_rejects_cross_tenant_run_and_new_active_version(db, object_db):
    enqueue(db)
    assert triggers.dispatch_one(db, lambda: executor(db, status='failed'))['status'] == 'failed'
    assert triggers.retry_failed(db, 1)
    db.commit()
    db.runs[10]['tenant'] = OTHER
    factory = Mock()
    assert triggers.dispatch_one(db, factory)['status'] == 'failed'
    factory.assert_not_called()
    db.runs[10]['tenant'] = TENANT
    assert triggers.retry_failed(db, 1)
    db.commit()
    object_db.get_workflow_active_version.return_value['workflow_version_id'] = 11
    assert triggers.dispatch_one(db, factory)['status'] == 'failed'
    factory.assert_not_called()


def test_factory_cannot_switch_security_context(db, object_db):
    enqueue(db)
    tokens = []
    def factory():
        tokens.append(set_security_context(SecurityContext(OTHER, 'worker', PERMISSIONS)))
        return executor(db)
    try:
        with pytest.raises(PermissionError, match='changed'):
            triggers.dispatch_one(db, factory)
        assert db.deliveries[1]['status'] == 'running' and not db.runs
    finally:
        for token in reversed(tokens):
            reset_security_context(token)


def test_autocommit_worker_rejected(db):
    db.autocommit = True
    with pytest.raises(ValueError, match='autocommit'):
        triggers.dispatch_one(db, Mock())


def test_autocommit_enqueue_rejected(db):
    db.autocommit = True
    with pytest.raises(ValueError, match='autocommit'):
        triggers.enqueue_document_ingested(db, 7, 'purchase_invoice')
    assert not db.deliveries


def test_claim_is_durable_even_if_workflow_lookup_commits(db, object_db):
    enqueue(db)
    workflow = deepcopy(object_db.get_solf_workflow_registry_by_key.return_value)
    def lookup(connection, **kwargs):
        connection.commit()
        assert db.saved[0][1]['status'] == 'running'
        assert triggers.dispatch_one(db, Mock()) is None
        return workflow
    object_db.get_solf_workflow_registry_by_key.side_effect = lookup
    assert triggers.dispatch_one(db, lambda: executor(db))['status'] == 'completed'


def test_process_crash_leaves_durable_run_running_without_automatic_reclaim(db, object_db):
    enqueue(db)
    adapter = SimpleNamespace(execute_ingestion_run=Mock(side_effect=KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt):
        triggers.dispatch_one(db, lambda: adapter)
    db.rollback()  # Session closes after process death; committed claim/run survive.
    assert db.deliveries[1]['status'] == 'running'
    assert db.deliveries[1]['run'] == 10 and 10 in db.runs
    assert not triggers.retry_failed(db, 1)
    assert triggers.dispatch_one(db, Mock()) is None


def test_hash_is_canonical_and_stable():
    first = {'document_id': 7, 'document_type': 'purchase_invoice', 'doc_refs': [7]}
    second = {'doc_refs': [7], 'document_type': 'purchase_invoice', 'document_id': 7}
    assert triggers._encoded(first) == triggers._encoded(second)


def test_inspect_trigger_is_declarative_and_normalized():
    manifest = {'trigger_key': ' invoice ', 'workflow_key': ' wf ',
                'document_types': ['Purchase Invoice', 'purchase-invoice'],
                'metadata': {'package': {'extension_key': 'finance', 'sha256': 'abc'}}}
    result = triggers.inspect_trigger(manifest)
    assert result['document_types'] == ['purchase_invoice']
    assert result['trigger_key'] == 'invoice'
    assert result['metadata'] == manifest['metadata']
    assert result['metadata'] is not manifest['metadata']


@pytest.mark.parametrize('patch', [{'python_module': 'evil'}, {'document_types': 'invoice'},
    {'document_types': [None]}, {'document_types': ['---']}, {'is_active': 'true'},
    {'metadata': []}, {'workflow_key': ''}])
def test_inspect_rejects_executable_or_invalid_configuration(patch):
    with pytest.raises(ValueError):
        triggers.inspect_trigger(dict(trigger_key='a', workflow_key='b') | patch)


def test_ddl_forces_fail_closed_rls_and_immutable_event_identity():
    ddl = triggers.TRIGGER_DDL
    for table in ('ikos_ingestion_trigger', 'ikos_ingestion_delivery'):
        assert f'ALTER TABLE {table} ENABLE ROW LEVEL SECURITY' in ddl
        assert f'ALTER TABLE {table} FORCE ROW LEVEL SECURITY' in ddl
    assert ddl.count('AS RESTRICTIVE') == 2
    assert "NULLIF(current_setting('ikos.tenant_id', true), '')::uuid" in ddl
    assert 'COALESCE' not in ddl and '00000000-' not in ddl
    assert 'UNIQUE (tenant_id, trigger_key, event_key)' in ddl
    assert 'PRIMARY KEY (tenant_id, trigger_key)' in ddl
    assert 'BEFORE UPDATE ON ikos_ingestion_delivery' in ddl
    assert 'ingestion run identity is immutable' in ddl