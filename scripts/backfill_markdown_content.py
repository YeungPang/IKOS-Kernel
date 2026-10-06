from __future__ import annotations

import argparse
import logging

import markdown_manager
import object_db
from security_context import SecurityContext, reset_security_context, set_security_context

LOGGER = logging.getLogger("ikos.markdown_backfill")


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill tenant Markdown files into document_content.")
    parser.add_argument("--tenant-id", required=True, help="Tenant UUID to backfill")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--user-id", default="system:markdown-backfill")
    args = parser.parse_args()

    token = set_security_context(
        SecurityContext(
            tenant_id=str(args.tenant_id),
            user_id=str(args.user_id),
            permissions=frozenset({"finance.read", "finance.write", "documents.restricted.read", "*"}),
        )
    )
    try:
        connection = object_db.get_connection()
        try:
            result = markdown_manager.backfill_legacy_markdown_content(
                connection,
                limit=max(1, min(int(args.batch_size), 5000)),
            )
            connection.commit()
            print(result)
            return 0
        except Exception:
            connection.rollback()
            LOGGER.exception("Markdown content backfill failed tenant_id=%s", args.tenant_id)
            raise
        finally:
            connection.close()
    finally:
        reset_security_context(token)


if __name__ == "__main__":
    raise SystemExit(main())
