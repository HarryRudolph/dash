"""PostgreSQL service helpers.

The psycopg2 connection is stored on app.state.pg at startup.
Pass it to these functions.

Update table names below to match your deployment.
"""

from __future__ import annotations

from typing import Any


def get_table_stats(pg, table: str) -> dict[str, Any]:
    """Return row count and status for a PostgreSQL table."""
    if pg is None:
        return {"status": "down", "total_count": None, "last_record": None}
    try:
        with pg.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {table}")  # noqa: S608
            count = cur.fetchone()[0]
            return {"status": "up", "total_count": count, "last_record": None}
    except Exception:
        return {"status": "down", "total_count": None, "last_record": None}
