# storage/_utils.py

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from platformdirs import user_data_dir

# Table names
CANONICAL_EQUITIES_TABLE = "canonical_equities"
CANONICAL_JSONL_ASSET = "canonical_equities.jsonl.gz"
CACHE_TABLE = "object_cache"
METADATA_TABLE = "data_metadata"


def get_data_store_path() -> Path:
    """
    Get the path to the data store directory.

    Checks for an override in the DATA_STORE_DIR environment variable.
    If not set, defaults to the user data directory for this application.

    Returns:
        Path: Path to the data store directory.
    """
    if override := os.getenv("DATA_STORE_DIR"):
        return Path(override)
    return Path(user_data_dir("equity-aggregator", "equity-aggregator"))


DATA_STORE_PATH: Path = get_data_store_path()


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    """
    Context manager for establishing a SQLite database connection.

    Opens a connection to the database at the configured data store path.
    Enables foreign key support for the session and ensures the connection
    is properly closed after use.

    Yields:
        sqlite3.Connection: An active SQLite database connection.

    Returns:
        Iterator[sqlite3.Connection]: Iterator yielding the database connection.
    """
    DATA_STORE_PATH.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        DATA_STORE_PATH / "data_store.db",
        isolation_level=None,
        check_same_thread=False,
    )
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def ttl_seconds() -> int:
    """
    Calculate the cache time-to-live (TTL) in seconds from environment.

    Reads CACHE_TTL_MINUTES environment variable. If not set, defaults
    to 1440 minutes (24 hours).

    Returns:
        int: The TTL value in seconds.

    Raises:
        ValueError: If CACHE_TTL_MINUTES is set to a negative value.
    """
    ttl_min = int(os.getenv("CACHE_TTL_MINUTES", "1440"))

    if ttl_min < 0:
        raise ValueError("CACHE_TTL_MINUTES must be ≥ 0")
    return ttl_min * 60


def validate_table_exists_with_data(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    Validates that the specified table exists and contains data.

    Args:
        conn (sqlite3.Connection): Database connection to validate against.
        table_name (str): Name of the table to validate.

    Returns:
        bool: True if the table exists and has at least one row, otherwise False.
    """
    try:
        return (
            conn.execute(
                f"SELECT 1 FROM {table_name} LIMIT 1",
            ).fetchone()
            is not None
        )
    except sqlite3.OperationalError:
        return False
