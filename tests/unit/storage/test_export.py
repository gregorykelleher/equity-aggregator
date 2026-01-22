# storage/test_export.py

import gzip
import json
import os
from pathlib import Path

import pytest

from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity
from equity_aggregator.storage._utils import (
    CANONICAL_EQUITIES_TABLE,
    CANONICAL_JSONL_ASSET,
    DATA_STORE_PATH,
    connect,
    validate_table_exists_with_data,
)
from equity_aggregator.storage.data_store import save_canonical_equities
from equity_aggregator.storage.export import (
    _rebuild_canonical_equity_rows,
    export_canonical_equities,
    rebuild_canonical_equities_from_jsonl_gz,
)

pytestmark = pytest.mark.unit


def _create_canonical_equity(figi: str, name: str = "TEST EQUITY") -> CanonicalEquity:
    """
    Create a CanonicalEquity instance for testing purposes.

    Args:
        figi (str): The FIGI identifier for the equity.
        name (str): The name of the equity, defaults to "TEST EQUITY".

    Returns:
        CanonicalEquity: A properly constructed CanonicalEquity instance.
    """
    identity = EquityIdentity(
        name=name,
        symbol="TST",
        share_class_figi=figi,
    )
    financials = EquityFinancials()

    return CanonicalEquity(identity=identity, financials=financials)


def _read_ndjson_gz(path: Path) -> list[dict]:
    """
    Reads a gzipped newline-delimited JSON (NDJSON) file and parses each line.

    Args:
        path (Path): Path to the gzipped NDJSON file.

    Returns:
        list[dict]: List of parsed JSON objects from each line in the file.
    """
    with gzip.open(path, mode="rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _count_rows(table: str) -> int:
    """
    Counts the number of rows in the specified database table.

    Args:
        table (str): The name of the table to count rows from.

    Returns:
        int: The total number of rows present in the specified table.
    """
    with connect() as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_export_canonical_equities_sorted() -> None:
    """
    ARRANGE: save two equities with out-of-order FIGIs
    ACT:     export_canonical_equities
    ASSERT:  exported list is sorted by share_class_figi (deterministic)
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"

    dummy_equities = [
        _create_canonical_equity("BBG000BKQV61", "EQUITY TWO"),
        _create_canonical_equity("BBG000B9XRY4", "EQUITY ONE"),
    ]

    save_canonical_equities(dummy_equities)

    export_canonical_equities(str(DATA_STORE_PATH))

    dest_path = DATA_STORE_PATH / CANONICAL_JSONL_ASSET

    exported = _read_ndjson_gz(dest_path)

    # restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert [equity["identity"]["share_class_figi"] for equity in exported] == [
        "BBG000B9XRY4",
        "BBG000BKQV61",
    ]


def test_rebuild_canonical_equities_from_jsonl_gz_rebuilds_table() -> None:
    """
    ARRANGE: export three equities to the module's default JSONL.GZ path
    ACT:     rebuild_canonical_equities_from_jsonl_gz
    ASSERT:  row count equals number of exported equities
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"

    dummy_equities = [
        _create_canonical_equity("BBG000B9XRY4", "ONE"),
        _create_canonical_equity("BBG000BKQV61", "TWO"),
        _create_canonical_equity("BBG000C6K6G9", "THREE"),
    ]

    save_canonical_equities(dummy_equities)

    export_canonical_equities(str(DATA_STORE_PATH))

    rebuild_canonical_equities_from_jsonl_gz()

    # Restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert _count_rows(CANONICAL_EQUITIES_TABLE) == len(dummy_equities)


def test__rebuild_canonical_equity_rows_skips_blank_lines() -> None:
    """
    ARRANGE: iterable with blanks and two valid JSON lines
    ACT:     _rebuild_canonical_equity_rows
    ASSERT:  extracted (figi, payload) tuples match inputs and preserve payloads
    """
    first_equity = _create_canonical_equity("BBG000B9XRY4", "ONE").model_dump_json()
    second_equity = _create_canonical_equity("BBG000BKQV61", "TWO").model_dump_json()

    lines = ["\n", "   \n", f"{first_equity}\n", "\n", f"{second_equity}\n"]

    rows = list(_rebuild_canonical_equity_rows(lines))

    assert rows == [
        ("BBG000B9XRY4", json.loads(first_equity) and first_equity),
        ("BBG000BKQV61", json.loads(second_equity) and second_equity),
    ]


def test_export_then_read_back_jsonl_gz() -> None:
    """
    ARRANGE: two CanonicalEquity objects
    ACT:     export_canonical_equities
    ASSERT:  parsed JSON objects equal original payloads
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"

    dummy_equities = [
        _create_canonical_equity("BBG000B9XRY4", "EQUITY ONE"),
        _create_canonical_equity("BBG000BKQV61", "EQUITY TWO"),
    ]

    save_canonical_equities(dummy_equities)

    dest_path = DATA_STORE_PATH / CANONICAL_JSONL_ASSET

    export_canonical_equities(str(DATA_STORE_PATH))

    parsed = _read_ndjson_gz(dest_path)

    # Restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert parsed == [json.loads(equity.model_dump_json()) for equity in dummy_equities]


def test_export_canonical_equities_raises_when_no_database() -> None:
    """
    ARRANGE: clear data directory to ensure no database exists
    ACT:     export_canonical_equities
    ASSERT:  FileNotFoundError is raised
    """
    DATA_STORE_PATH.mkdir(parents=True, exist_ok=True)

    db_path = DATA_STORE_PATH / "data_store.db"

    if db_path.exists():
        db_path.unlink()

    with pytest.raises(FileNotFoundError, match="No canonical equities found"):
        export_canonical_equities(str(DATA_STORE_PATH))


def test_validate_table_exists_with_data_handles_invalid_table() -> None:
    """
    ARRANGE: connection to database
    ACT:     validate_table_exists_with_data with nonexistent table
    ASSERT:  returns False
    """
    with connect() as conn:
        actual = validate_table_exists_with_data(conn, "nonexistent_table")

    assert actual is False
