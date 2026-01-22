# storage/export.py

import gzip
import json
import logging
import sqlite3
from collections.abc import Iterable, Iterator
from pathlib import Path

from ._utils import (
    CANONICAL_EQUITIES_TABLE,
    CANONICAL_JSONL_ASSET,
    DATA_STORE_PATH,
    connect,
    validate_table_exists_with_data,
)
from .metadata import ensure_fresh_database, update_canonical_equities_timestamp

logger = logging.getLogger(__name__)


def export_canonical_equities(output_dir: str, refresh_fn: callable = None) -> None:
    """
    Export canonical equities as newline-delimited JSON (NDJSON), compressed with gzip.

    Each line contains one equity JSON object, matching the schema in the 'payload'
    column. Output is ordered by share_class_figi for deterministic results.

    Args:
        output_dir (str): Directory where canonical_equities.jsonl.gz will be created.
        refresh_fn (callable, optional): Function to refresh database if stale.

    Returns:
        None

    Raises:
        FileNotFoundError: If no database exists or no canonical equities are found.
    """

    # Check if database file exists - if not, fail early
    db_path = DATA_STORE_PATH / "data_store.db"
    if not db_path.exists():
        raise FileNotFoundError(
            "No canonical equities found. Run 'seed' or 'download' first.",
        )

    # Ensure database is fresh before export (only if it exists)
    ensure_fresh_database(refresh_fn)

    logger.info("Exporting canonical equities to JSONL")

    with connect() as conn:
        validate_table_exists_with_data(conn, CANONICAL_EQUITIES_TABLE)

        cursor = conn.execute(
            (
                f"SELECT payload FROM {CANONICAL_EQUITIES_TABLE} "
                "ORDER BY share_class_figi"
            ),
        )

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        with gzip.open(
            output_path / CANONICAL_JSONL_ASSET,
            mode="wt",
            encoding="utf-8",
            compresslevel=9,
        ) as gz:
            for (payload_str,) in cursor:
                gz.write(payload_str)
                gz.write("\n")


def rebuild_canonical_equities_from_jsonl_gz() -> None:
    """
    Rebuilds the canonical_equities table in the SQLite database from a gzip-compressed
    JSONL file. Drops and recreates the table, then populates it with canonical equity
    records from the source file. This operation is idempotent and optimises the
    database after completion.

    Args:
        None

    Returns:
        None
    """
    src_path = DATA_STORE_PATH / CANONICAL_JSONL_ASSET
    dest_path = DATA_STORE_PATH / "data_store.db"

    logger.info("Rebuilding database from %s", src_path)

    with sqlite3.connect(dest_path, isolation_level=None) as conn:
        _rebuild_canonical_equities_schema(conn)
        _rebuild_canonical_equities_table(conn, src_path)
        update_canonical_equities_timestamp(conn)
        conn.execute("VACUUM")  # Optimise database

    logger.info("Database rebuild completed successfully")


def _rebuild_canonical_equities_schema(conn: sqlite3.Connection) -> None:
    """
    Drops and rebuilds the canonical_equities table with performance optimisations.

    This function disables SQLite journaling and synchronous writes for faster bulk
    operations, then drops the canonical_equities table if it exists and rebuilds it
    with the required schema.

    Args:
        conn (sqlite3.Connection): The SQLite database connection to use.

    Returns:
        None
    """
    conn.executescript(f"""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        DROP TABLE IF EXISTS {CANONICAL_EQUITIES_TABLE};
        CREATE TABLE {CANONICAL_EQUITIES_TABLE}(
            share_class_figi TEXT PRIMARY KEY,
            payload          TEXT NOT NULL
        ) WITHOUT ROWID;
    """)


def _rebuild_canonical_equities_table(
    conn: sqlite3.Connection,
    src_path: Path,
) -> None:
    """
    Populates the canonical equities table in the database from a compressed JSONL file.

    Reads equity data from a gzip-compressed JSON Lines file, extracts relevant
    rows, and inserts or replaces them into the canonical equities table.

    Args:
        conn (sqlite3.Connection): SQLite database connection object.
        src_path (Path): Path to the gzip-compressed JSONL file containing equity data.

    Returns:
        None
    """
    with gzip.open(src_path, "rt", encoding="utf-8") as file_handler:
        equity_rows = _rebuild_canonical_equity_rows(file_handler)
        conn.executemany(
            (
                f"INSERT OR REPLACE INTO {CANONICAL_EQUITIES_TABLE} "
                "(share_class_figi, payload) VALUES (?, ?)"
            ),
            equity_rows,
        )


def _rebuild_canonical_equity_rows(
    file_handler: Iterable[str],
) -> Iterator[tuple[str, str]]:
    """
    Extracts (figi, payload) pairs from a JSON Lines file handler and rebuilds
    each valid line into a tuple containing the share class FIGI and
    the original JSON payload string.

    Args:
        file_handler: An iterable file-like object yielding JSONL strings.

    Returns:
        Iterator[tuple[str, str]]: An iterator of (figi, payload) tuples.
    """
    loads = json.loads

    for line in file_handler:
        json_line = line.strip()  # Remove leading/trailing whitespace

        if not json_line:
            continue

        figi = loads(json_line)["identity"]["share_class_figi"]
        yield figi, json_line
