# storage/freshness.py

import datetime
import logging
import sqlite3

from ._utils import (
    CANONICAL_EQUITY_SNAPSHOTS_TABLE,
    DATA_STORE_PATH,
    connect,
    ttl_seconds,
)

logger = logging.getLogger(__name__)


def ensure_fresh_database(refresh_fn: callable = None) -> bool:
    """
    Ensure the database is fresh, refreshing if stale and refresh function provided.

    Args:
        refresh_fn (callable, optional): Function to call if database is stale.
            Should download/refresh the database (e.g., download_canonical_equities).

    Returns:
        bool: True if refresh was performed, False if database was already fresh.
    """
    if _is_database_stale() and refresh_fn:
        logger.info("Database is stale, refreshing...")
        refresh_fn()
        return True

    return False


def _is_database_stale() -> bool:
    """
    Check if the local database is stale based on snapshot dates.

    The database is considered stale if the most recent snapshot date is
    before today, or if no snapshots exist.

    Returns:
        bool: True if database is stale or doesn't exist, False if fresh.
    """
    if ttl_seconds() == 0:
        return False

    db_path = DATA_STORE_PATH / "data_store.db"
    if not db_path.exists():
        return True

    return _latest_snapshot_is_stale()


def _latest_snapshot_is_stale() -> bool:
    """
    Checks whether the latest snapshot date is before today.

    Returns:
        bool: True if no snapshots exist or the latest is before today.
    """
    with connect() as conn:
        latest = _get_latest_snapshot_date(conn)
        return latest is None or latest < datetime.date.today().isoformat()


def _get_latest_snapshot_date(
    conn: sqlite3.Connection,
) -> str | None:
    """
    Gets the most recent snapshot date from the equity snapshots table.

    Args:
        conn (sqlite3.Connection): The SQLite database connection.

    Returns:
        str | None: The latest snapshot date as YYYY-MM-DD, or None.
    """
    try:
        row = conn.execute(
            f"SELECT MAX(snapshot_date) FROM {CANONICAL_EQUITY_SNAPSHOTS_TABLE}"
        ).fetchone()
        return row[0] if row and row[0] else None
    except sqlite3.OperationalError:
        return None
