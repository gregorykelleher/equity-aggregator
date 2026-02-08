# storage/data_store.py

import datetime
import logging
import sqlite3
from collections.abc import Callable, Iterable

from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity

from ._utils import (
    CANONICAL_EQUITY_IDENTITIES_TABLE,
    CANONICAL_EQUITY_SNAPSHOTS_TABLE,
    connect,
)
from .freshness import ensure_fresh_database

logger = logging.getLogger(__name__)


def load_canonical_equity(share_class_figi: str) -> CanonicalEquity | None:
    """
    Retrieve a single CanonicalEquity by its exact share_class_figi value.

    Joins the identity and latest snapshot for the given FIGI. Returns None
    if the FIGI is not found or has no snapshots.

    Args:
        share_class_figi (str): The FIGI identifier of the equity to load.

    Returns:
        CanonicalEquity | None: The CanonicalEquity instance if found, else None.
    """
    with connect() as conn:
        _init_tables(conn)
        row = conn.execute(
            f"""
            SELECT i.payload, s.payload, s.snapshot_date
            FROM {CANONICAL_EQUITY_IDENTITIES_TABLE} i
            JOIN {CANONICAL_EQUITY_SNAPSHOTS_TABLE} s
                ON i.share_class_figi = s.share_class_figi
            WHERE i.share_class_figi = ?
            ORDER BY s.snapshot_date DESC
            LIMIT 1
            """,
            (share_class_figi,),
        ).fetchone()

        if not row:
            return None

        # unpack: i.payload, s.payload, s.snapshot_date
        identity_payload, financials_payload, snapshot_date = row

        # reassemble into a single CanonicalEquity
        return _build_canonical_equity_from_row(
            identity_payload, financials_payload, snapshot_date
        )


def load_canonical_equities(
    refresh_fn: Callable | None = None,
) -> list[CanonicalEquity]:
    """
    Loads and rehydrates all CanonicalEquity objects from the database.

    For each identity, joins with its latest snapshot and returns a list of
    CanonicalEquity instances ordered by share_class_figi.

    Args:
        refresh_fn (Callable | None, optional): Function to refresh database if stale.

    Returns:
        list[CanonicalEquity]: List of all rehydrated CanonicalEquity objects.
    """
    ensure_fresh_database(refresh_fn)

    with connect() as conn:
        _init_tables(conn)
        rows = conn.execute(
            f"""
            SELECT i.payload, s.payload, s.snapshot_date
            FROM {CANONICAL_EQUITY_IDENTITIES_TABLE} i
            JOIN {CANONICAL_EQUITY_SNAPSHOTS_TABLE} s
                ON i.share_class_figi = s.share_class_figi
            WHERE s.snapshot_date = (
                SELECT MAX(s2.snapshot_date)
                FROM {CANONICAL_EQUITY_SNAPSHOTS_TABLE} s2
                WHERE s2.share_class_figi = i.share_class_figi
            )
            ORDER BY i.share_class_figi
            """,
        ).fetchall()

        return [
            _build_canonical_equity_from_row(
                identity_payload, financials_payload, snapshot_date
            )
            for identity_payload, financials_payload, snapshot_date in rows
        ]


def load_canonical_equity_history(
    share_class_figi: str,
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[CanonicalEquity]:
    """
    Load historical snapshots for a given equity.

    Joins the identity with all matching snapshots for the given FIGI, optionally
    filtered by date range. Returns results ordered by snapshot_date ascending.

    Args:
        share_class_figi (str): The FIGI identifier of the equity.
        from_date (str | None, optional): Inclusive start date (YYYY-MM-DD).
        to_date (str | None, optional): Inclusive end date (YYYY-MM-DD).

    Returns:
        list[CanonicalEquity]: List of CanonicalEquity objects with snapshot_date
            populated, ordered by snapshot_date ascending. Empty if not found.
    """
    with connect() as conn:
        _init_tables(conn)

        query = f"""
            SELECT i.payload, s.payload, s.snapshot_date
            FROM {CANONICAL_EQUITY_IDENTITIES_TABLE} i
            JOIN {CANONICAL_EQUITY_SNAPSHOTS_TABLE} s
                ON i.share_class_figi = s.share_class_figi
            WHERE i.share_class_figi = ?
        """
        params: list[str] = [share_class_figi]

        if from_date is not None:
            query += " AND s.snapshot_date >= ?"
            params.append(from_date)

        if to_date is not None:
            query += " AND s.snapshot_date <= ?"
            params.append(to_date)

        query += " ORDER BY s.snapshot_date ASC"

        rows = conn.execute(query, params).fetchall()

        return [
            _build_canonical_equity_from_row(
                identity_payload, financials_payload, snapshot_date
            )
            for identity_payload, financials_payload, snapshot_date in rows
        ]


def save_canonical_equities(
    canonical_equities: Iterable[CanonicalEquity],
    snapshot_date: str | None = None,
) -> None:
    """
    Saves a collection of CanonicalEquity objects to the database.

    Each equity is split into identity and financial payloads and stored in
    separate tables. Identity rows are upserted; snapshot rows are inserted
    with the given date (defaulting to today).

    Args:
        canonical_equities (Iterable[CanonicalEquity]): An iterable of CanonicalEquity
            objects to be saved to the database.
        snapshot_date (str | None, optional): The snapshot date in YYYY-MM-DD format.
            Defaults to today's date.

    Returns:
        None
    """
    canonical_equities = list(canonical_equities)
    date = snapshot_date or datetime.date.today().isoformat()

    logger.info("Saving %d canonical equities to database", len(canonical_equities))

    with connect() as conn:
        _init_tables(conn)

        conn.execute("BEGIN")

        conn.executemany(
            f"INSERT OR REPLACE INTO {CANONICAL_EQUITY_IDENTITIES_TABLE} "
            "(share_class_figi, payload) VALUES (?, ?)",
            (_serialise_identity(e) for e in canonical_equities),
        )

        conn.executemany(
            f"INSERT OR REPLACE INTO {CANONICAL_EQUITY_SNAPSHOTS_TABLE} "
            "(share_class_figi, snapshot_date, payload) VALUES (?, ?, ?)",
            (_serialise_snapshot(e, date) for e in canonical_equities),
        )

        conn.execute("COMMIT")


def _init_tables(conn: sqlite3.Connection) -> None:
    """
    Initialises both the equity identities and equity snapshots tables.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use.

    Returns:
        None
    """
    _init_canonical_equity_identities_table(conn)
    _init_canonical_equity_snapshots_table(conn)


def _init_canonical_equity_identities_table(conn: sqlite3.Connection) -> None:
    """
    Initialises the equity identities table in the database.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use.

    Returns:
        None
    """
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CANONICAL_EQUITY_IDENTITIES_TABLE} (
            share_class_figi TEXT PRIMARY KEY,
            payload          TEXT NOT NULL
        ) WITHOUT ROWID;
        """,
    )


def _init_canonical_equity_snapshots_table(conn: sqlite3.Connection) -> None:
    """
    Initialises the equity snapshots table in the database.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use.

    Returns:
        None
    """
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CANONICAL_EQUITY_SNAPSHOTS_TABLE} (
            share_class_figi TEXT NOT NULL,
            snapshot_date    TEXT NOT NULL,
            payload          TEXT NOT NULL,
            PRIMARY KEY (share_class_figi, snapshot_date),
            FOREIGN KEY (share_class_figi)
                REFERENCES {CANONICAL_EQUITY_IDENTITIES_TABLE}(share_class_figi)
        ) WITHOUT ROWID;
        """,
    )


def _build_canonical_equity_from_row(
    identity_payload: str,
    financials_payload: str,
    snapshot_date: str,
) -> CanonicalEquity:
    """
    Builds a CanonicalEquity from a database row's payload columns.

    Deserialises the identity and financials payload columns and combines
    them with the snapshot date into a single CanonicalEquity instance.

    Args:
        identity_payload (str): Serialised EquityIdentity payload.
        financials_payload (str): Serialised EquityFinancials payload.
        snapshot_date (str): The snapshot date in YYYY-MM-DD format.

    Returns:
        CanonicalEquity: A fully constructed CanonicalEquity instance.
    """
    identity = EquityIdentity.model_validate_json(identity_payload)
    financials = EquityFinancials.model_validate_json(financials_payload)
    return CanonicalEquity(
        identity=identity,
        financials=financials,
        snapshot_date=snapshot_date,
    )


def _serialise_identity(canonical_equity: CanonicalEquity) -> tuple[str, str]:
    """
    Serialise the identity portion of a CanonicalEquity for database storage.

    Args:
        canonical_equity (CanonicalEquity): The CanonicalEquity instance to serialise.

    Returns:
        tuple[str, str]: A tuple of (share_class_figi, identity_payload).
    """
    figi = canonical_equity.identity.share_class_figi
    identity_payload = canonical_equity.identity.model_dump_json()
    return figi, identity_payload


def _serialise_snapshot(
    canonical_equity: CanonicalEquity,
    snapshot_date: str,
) -> tuple[str, str, str]:
    """
    Serialise the financial snapshot of a CanonicalEquity for database storage.

    Args:
        canonical_equity (CanonicalEquity): The CanonicalEquity instance to serialise.
        snapshot_date (str): The snapshot date in YYYY-MM-DD format.

    Returns:
        tuple[str, str, str]: A tuple of (figi, date, financials_payload).
    """
    figi = canonical_equity.identity.share_class_figi
    financials_payload = canonical_equity.financials.model_dump_json()
    return figi, snapshot_date, financials_payload
