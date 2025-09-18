# storage/metadata.py

import logging
import sqlite3
import time

from ._utils import (
    CANONICAL_EQUITIES_TABLE,
    DATA_STORE_PATH,
    METADATA_TABLE,
    connect,
    ttl_seconds,
)

logger = logging.getLogger(__name__)


def update_canonical_equities_timestamp(conn: sqlite3.Connection) -> None:
    """
    Updates the last_updated timestamp for canonical equities table.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use.

    Returns:
        None
    """
    _init_metadata_table(conn)
    conn.execute(
        (
            f"INSERT OR REPLACE INTO {METADATA_TABLE} "
            "(table_name, last_updated) VALUES (?, ?)"
        ),
        (
            CANONICAL_EQUITIES_TABLE,
            int(time.time()),
        ),
    )


def _init_metadata_table(conn: sqlite3.Connection) -> None:
    """
    Initialises the metadata table for tracking data freshness.

    Creates a table with the name specified by the variable `METADATA_TABLE`
    if it does not already exist. The table tracks when different data sources
    were last updated.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use for table
            creation.

    Returns:
        None
    """
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (
            table_name TEXT PRIMARY KEY,
            last_updated INTEGER NOT NULL
        )
    """)


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
    Check if the local database is stale based on TTL configuration.

    Returns:
        bool: True if database is stale or doesn't exist, False if fresh.
    """
    ttl = ttl_seconds()
    if ttl == 0:  # TTL disabled
        return False

    db_path = DATA_STORE_PATH / "data_store.db"
    if not db_path.exists():
        return True

    with connect() as conn:
        age = _get_canonical_equities_age_seconds(conn)
        return age is None or age > ttl


def _get_canonical_equities_age_seconds(conn: sqlite3.Connection) -> int | None:
    """
    Gets the age in seconds of the canonical equities data.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use.

    Returns:
        int | None: Age in seconds, or None if no timestamp exists.
    """
    _init_metadata_table(conn)
    row = conn.execute(
        f"SELECT last_updated FROM {METADATA_TABLE} WHERE table_name = ?",
        (CANONICAL_EQUITIES_TABLE,),
    ).fetchone()
    return int(time.time()) - row[0] if row else None
