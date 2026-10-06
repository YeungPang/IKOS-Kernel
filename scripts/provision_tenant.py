"""Provision one tenant and its initial tenant administrator from a trusted host.

This is an offline control-plane command: it calls PostgreSQL directly and does
not start the IKOS API, browser client, LLM, or vector services. Access to the
host and configured database credentials is the authorization boundary.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2

import object_db
from ikos_config import DB_NAME, PLATFORM_ADMIN_SUBJECTS


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create an IKOS tenant and assign its first tenant administrator."
    )
    parser.add_argument("--tenant-key", required=True, help="Unique lowercase tenant slug")
    parser.add_argument("--display-name", required=True, help="Tenant display name")
    parser.add_argument(
        "--admin-subject",
        required=True,
        help="Exact issuer-qualified identity-provider subject for the initial tenant admin",
    )
    parser.add_argument(
        "--actor-subject",
        required=True,
        help="Configured platform-admin subject to record as the provisioning actor",
    )
    parser.add_argument("--admin-display-name", default=None, help="Optional initial admin display name")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if not PLATFORM_ADMIN_SUBJECTS:
        print(
            "Error: IKOS_PLATFORM_ADMIN_SUBJECTS is empty; configure the bootstrap administrator out of band.",
            file=sys.stderr,
        )
        return 2
    if args.actor_subject not in PLATFORM_ADMIN_SUBJECTS:
        print("Error: actor subject is not in IKOS_PLATFORM_ADMIN_SUBJECTS.", file=sys.stderr)
        return 2

    connection = object_db.get_connection()
    try:
        result = object_db.create_tenant_with_initial_admin(
            connection,
            tenant_key=args.tenant_key,
            display_name=args.display_name,
            admin_external_subject=args.admin_subject,
            admin_display_name=args.admin_display_name,
        )
        connection.commit()
    except ValueError as exc:
        connection.rollback()
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except psycopg2.IntegrityError:
        connection.rollback()
        print("Error: tenant key already exists or provisioning conflicts with existing data.", file=sys.stderr)
        return 3
    except Exception as exc:
        connection.rollback()
        print(f"Error: tenant provisioning failed: {exc}", file=sys.stderr)
        return 1
    finally:
        connection.close()

    print(f"Tenant provisioned in database '{DB_NAME}'.")
    print(f"Tenant ID: {result['tenant_id']}")
    print(f"Tenant key: {result['tenant_key']}")
    print(f"Initial administrator subject: {args.admin_subject}")
    print(f"Assigned role: {result['role_key']}")
    print(f"Provisioning actor: {args.actor_subject}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
