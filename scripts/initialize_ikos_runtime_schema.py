"""Safely initialize the IKOS runtime schema in a PostgreSQL database.

This is an additive bootstrap: it never drops existing tables. It invokes the
runtime DDL owners so the schema stays aligned with the Python implementation.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2

import domain_db
import object_db
import sql_db
from ikos_config import DB_HOST, DB_PASSWORD, DB_PORT, DB_USER


def _ensure_component_tables(connection: Any) -> None:
    """Create tables owned by runtime components outside object_db/domain_db."""
    connection_factory: Callable[[], Any] = lambda: connection

    # Legacy SOLF storage tables are still used by parser/runtime integrations.
    with connection.cursor() as cursor:
        for statement in (
            sql_db.create_predicate_table,
            sql_db.create_fact_table,
            sql_db.create_clause_table,
        ):
            cursor.execute(statement)

    from audit_compliance_manager import AuditComplianceManager
    from event_bus import EventBus
    from interaction import IDMSInteractionTools
    from notifier import Notifier
    from notifier_engine import NotifierEngine
    from project_simulator import ProjectSimulator
    from scheduler import TaskScheduler
    from workflow_monitor import WorkflowMonitor
    from workflow_pipeline_executor import (
        _ensure_generated_script_audit_table,
        _ensure_generated_script_table,
    )
    from workflow_reliability_manager import WorkflowReliabilityManager
    from workflow_state_machine import WorkflowStateMachine
    from ikos_api_server.routers.documents import _ensure_invoice_reference_state_table

    # These components create their persistence tables during ensure_tables().
    EventBus(db_connection_fn=connection_factory)
    monitor = WorkflowMonitor(db_connection_factory)
    reliability = WorkflowReliabilityManager(
        db_connection_factory,
        monitor=monitor,
        event_bus=None,
    )
    WorkflowStateMachine(db_connection_factory)
    AuditComplianceManager(db_connection_factory, reliability_manager=reliability)
    NotifierEngine(db_connection_factory)
    Notifier(db_connection_fn=connection_factory)
    ProjectSimulator(db_connection_factory)
    TaskScheduler(lambda _query: {}, connection_factory).ensure_tables()

    # Interaction initialization normally runs as part of API tool construction;
    # call its table initializer without initializing LLM/network clients.
    IDMSInteractionTools._ensure_interaction_runtime_tables(
        SimpleNamespace(get_connection=connection_factory)
    )

    # API- and pipeline-specific runtime tables.
    _ensure_generated_script_table(connection)
    _ensure_generated_script_audit_table(connection)
    _ensure_invoice_reference_state_table(connection)


def initialize(database: str) -> list[str]:
    connection = psycopg2.connect(
        database=database,
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )
    try:
        # Keep the runtime's normal tenant context for the bootstrap session.
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT set_config('ikos.tenant_id', %s, false)",
                ("00000000-0000-0000-0000-000000000001",),
            )
            cursor.execute("SELECT set_config('ikos.user_id', '', false)")
            cursor.execute("SELECT set_config('ikos.permissions', '', false)")

        object_db.create_tables(connection, recreate=False)
        domain_db.create_tables(connection, recreate=False)
        _ensure_component_tables(connection)
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )
            return [str(row[0]) for row in cursor.fetchall()]
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default="ikos_dev",
        help="Target PostgreSQL database (default: ikos_dev)",
    )
    args = parser.parse_args()

    table_names = initialize(args.database)
    print(f"Initialized IKOS runtime schema in database '{args.database}'.")
    print(f"Verified {len(table_names)} base tables in the current schema.")
    for table_name in table_names:
        print(f"  {table_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
