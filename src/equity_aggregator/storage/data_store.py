# storage/data_store.py

import logging
import sqlite3
from collections.abc import Callable, Iterable, Iterator

from equity_aggregator.schemas import CanonicalEquity

from ._utils import (
    CANONICAL_EQUITIES_TABLE,
    connect,
)
from .metadata import ensure_fresh_database, update_canonical_equities_timestamp

logger = logging.getLogger(__name__)


def load_canonical_equity(share_class_figi: str) -> CanonicalEquity | None:
    """
    Retrieve a single CanonicalEquity by its exact share_class_figi value.

    Args:
        share_class_figi (str): The FIGI identifier of the equity to load.

    Returns:
        CanonicalEquity | None: The CanonicalEquity instance if found, else None.
    """
    with connect() as conn:
        _init_canonical_equities_table(conn)
        row = conn.execute(
            (
                f"SELECT payload FROM {CANONICAL_EQUITIES_TABLE} "
                "WHERE share_class_figi = ? LIMIT 1"
            ),
            (share_class_figi,),
        ).fetchone()
        return CanonicalEquity.model_validate_json(row[0]) if row and row[0] else None


def _init_canonical_equities_table(conn: sqlite3.Connection) -> None:
    """
    Initialises the canonical equities table in the provided SQLite database connection.

    Creates a table with the name specified by the variable `CANONICAL_EQUITIES_TABLE`
    if it does not already exist. The table contains two columns: 'share_class_figi' as
    the primary key and 'payload' as a text field.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use for table
            creation.

    Returns:
        None
    """
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CANONICAL_EQUITIES_TABLE} (
            share_class_figi TEXT PRIMARY KEY,
            payload          TEXT NOT NULL
        ) WITHOUT ROWID;
        """,
    )


def load_canonical_equities(
    refresh_fn: Callable | None = None,
) -> list[CanonicalEquity]:
    """
    Loads and rehydrates all CanonicalEquity objects from the database.

    Iterates over all JSON payloads stored in the canonical_equities table,
    deserialises each payload using CanonicalEquity.model_validate_json, and
    returns a list of CanonicalEquity instances.

    Args:
        refresh_fn (Callable | None, optional): Function to refresh database if stale.

    Returns:
        list[CanonicalEquity]: List of all rehydrated CanonicalEquity objects.
    """
    # Ensure database is fresh before loading
    ensure_fresh_database(refresh_fn)

    return [
        CanonicalEquity.model_validate_json(payload)
        for payload in _iter_canonical_equity_json_payloads()
    ]


def _iter_canonical_equity_json_payloads() -> Iterator[str]:
    """
    Yields JSON payload strings from canonical_equities table in deterministic order.

    Args:
        None

    Returns:
        Iterator[str]: Iterator over JSON payload strings, ordered by share_class_figi.
    """
    with connect() as conn:
        _init_canonical_equities_table(conn)
        cursor = conn.execute(
            (
                f"SELECT payload FROM {CANONICAL_EQUITIES_TABLE} "
                "ORDER BY share_class_figi"
            ),
        )
        for (payload_str,) in cursor:
            if payload_str:
                yield payload_str


def save_canonical_equities(canonical_equities: Iterable[CanonicalEquity]) -> None:
    """
    Saves a collection of CanonicalEquity objects to the database.

    Each equity is serialised and inserted or replaced in the database table. The
    function ensures the database connection is established and initialised before
    performing the operation. Updates the last_updated timestamp for freshness tracking.

    Args:
        equities (Iterable[CanonicalEquity]): An iterable of CanonicalEquity objects to
            be saved to the database.

    Returns:
        None
    """
    canonical_equities = list(canonical_equities)

    logger.info("Saving %d canonical equities to database", len(canonical_equities))

    with connect() as conn:
        _init_canonical_equities_table(conn)

        conn.executemany(
            f"INSERT OR REPLACE INTO {CANONICAL_EQUITIES_TABLE} "
            "(share_class_figi, payload) VALUES (?, ?)",
            map(_serialise_equity, canonical_equities),
        )

        # Update freshness timestamp
        update_canonical_equities_timestamp(conn)


def _serialise_equity(canonical_equity: CanonicalEquity) -> tuple[str, str]:
    """
    Serialise a CanonicalEquity object into (figi, payload) tuple for database
    storage.

    Args:
        canonical_equity (CanonicalEquity): The CanonicalEquity instance to serialise.

    Returns:
        tuple[str, str]: A tuple containing the share class FIGI as a string and
            the JSON-serialised CanonicalEquity object as a string.

    Raises:
        ValueError: If 'share_class_figi' is missing or empty in the provided object.
    """
    figi = canonical_equity.identity.share_class_figi

    json_data = canonical_equity.model_dump_json()
    return figi, json_data
