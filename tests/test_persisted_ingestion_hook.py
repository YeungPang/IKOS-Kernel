"""Authoritative post-persistence reconciliation hook; no live database."""
from unittest.mock import MagicMock, Mock

import pytest

import ingestion_triggers as triggers
from security_context import SecurityContext


@pytest.fixture
def hook(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    context = SecurityContext('11111111-1111-1111-1111-111111111111', 'worker',
                              frozenset({'documents.write'}))
    monkeypatch.setattr(triggers, '_context', Mock(return_value=context))
    enqueue = Mock(return_value=[42])
    monkeypatch.setattr(triggers, 'enqueue_document_ingested', enqueue)
    return connection, cursor, enqueue


def test_legacy_schema_skips_without_ddl_or_execution(hook):
    connection, cursor, enqueue = hook
    cursor.fetchone.return_value = (None, None)
    assert triggers.enqueue_persisted_document(connection, 7) == {
        'status': 'schema_not_installed', 'delivery_ids': []}
    enqueue.assert_not_called()
    assert cursor.execute.call_count == 1
    connection.commit.assert_not_called()


def test_stored_type_only_is_forwarded_and_caller_owns_commit(hook):
    connection, cursor, enqueue = hook
    cursor.fetchone.side_effect = [('ikos_ingestion_trigger', 'ikos_ingestion_delivery'), ('Bill',)]
    assert triggers.enqueue_persisted_document(connection, 7) == {'status': 'enqueued', 'delivery_ids': [42]}
    enqueue.assert_called_once_with(connection, 7, 'Bill')
    assert 'tenant_id=%s::uuid' in cursor.execute.call_args.args[0]
    connection.commit.assert_not_called()


def test_unclassified_skips(hook):
    connection, cursor, enqueue = hook
    cursor.fetchone.side_effect = [('trigger', 'delivery'), ('',)]
    assert triggers.enqueue_persisted_document(connection, 7)['status'] == 'unclassified'
    enqueue.assert_not_called()


def test_inaccessible_document_fails_closed(hook):
    connection, cursor, enqueue = hook
    cursor.fetchone.side_effect = [('trigger', 'delivery'), None]
    with pytest.raises(PermissionError):
        triggers.enqueue_persisted_document(connection, 7)
    enqueue.assert_not_called()


def test_authorization_errors_are_not_swallowed(hook, monkeypatch):
    connection, cursor, enqueue = hook
    monkeypatch.setattr(triggers, '_context', Mock(side_effect=PermissionError('denied')))
    with pytest.raises(PermissionError):
        triggers.enqueue_persisted_document(connection, 7)
    cursor.execute.assert_not_called()
    enqueue.assert_not_called()