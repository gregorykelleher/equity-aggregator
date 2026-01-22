# storage/test_data_store.py

import os

import pytest

from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity
from equity_aggregator.storage._utils import (
    CANONICAL_EQUITIES_TABLE,
    connect,
)
from equity_aggregator.storage.data_store import (
    _init_canonical_equities_table,
    _iter_canonical_equity_json_payloads,
    load_canonical_equities,
    load_canonical_equity,
    save_canonical_equities,
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


def test_load_canonical_equity_returns_none_when_not_found() -> None:
    """
    ARRANGE: no row for the FIGI
    ACT:     load_canonical_equity
    ASSERT:  returns None
    """
    assert load_canonical_equity("BBG000NOTFOUND") is None


def test_load_canonical_equity_returns_none_when_payload_empty() -> None:
    """
    ARRANGE: insert row with empty payload for a FIGI
    ACT:     load_canonical_equity
    ASSERT:  returns None
    """
    figi = "BBG000EMPTY1"

    with connect() as conn:
        _init_canonical_equities_table(conn)
        conn.execute(
            f"INSERT OR REPLACE INTO {CANONICAL_EQUITIES_TABLE} "
            "(share_class_figi, payload) VALUES (?, ?)",
            (figi, ""),
        )

    assert load_canonical_equity(figi) is None


def test_load_canonical_equity_returns_object_when_found() -> None:
    """
    ARRANGE: save a CanonicalEquity for a FIGI
    ACT:     load_canonical_equity
    ASSERT:  returns a CanonicalEquity with matching FIGI
    """
    figi = "BBG000FOUND1"
    equity = _create_canonical_equity(figi, "FOUND")

    save_canonical_equities([equity])

    loaded = load_canonical_equity(figi)
    assert loaded.identity.share_class_figi == figi


def test_save_equities_inserts_rows() -> None:
    """
    ARRANGE: two CanonicalEquity objects
    ACT:     save_canonical_equities
    ASSERT:  row count == 2
    """
    expected_row_count = 2
    equities = [
        _create_canonical_equity("BBG000B9XRY4", "EQUITY ONE"),
        _create_canonical_equity("BBG000BKQV61", "EQUITY TWO"),
    ]

    save_canonical_equities(equities)

    assert _count_rows(CANONICAL_EQUITIES_TABLE) == expected_row_count


def test_save_equities_upsert_single_row() -> None:
    """
    ARRANGE: same FIGI twice
    ACT:     save_canonical_equities twice
    ASSERT:  row count == 1
    """
    equity = _create_canonical_equity("BBG000C6K6G9")

    save_canonical_equities([equity])
    save_canonical_equities([equity])

    assert _count_rows(CANONICAL_EQUITIES_TABLE) == 1


def test_load_canonical_equities_rehydrates_objects() -> None:
    """
    ARRANGE: save two CanonicalEquity objects
    ACT:     load_canonical_equities
    ASSERT:  loaded objects equal original identities
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"

    equities = [
        _create_canonical_equity("BBG000B9XRY4", "ONE"),
        _create_canonical_equity("BBG000BKQV61", "TWO"),
    ]

    save_canonical_equities(equities)

    loaded = load_canonical_equities()

    # Restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert [equity.identity.share_class_figi for equity in loaded] == [
        "BBG000B9XRY4",
        "BBG000BKQV61",
    ]


def test__iter_canonical_equity_json_payloads_skips_empty_payloads() -> None:
    """
    ARRANGE: insert one row with empty-string payload into the table
    ACT:     iterate _iter_canonical_equity_json_payloads
    ASSERT:  yields no results
    """
    with connect() as conn:
        _init_canonical_equities_table(conn)
        conn.execute(
            f"INSERT OR REPLACE INTO {CANONICAL_EQUITIES_TABLE} "
            "(share_class_figi, payload) VALUES (?, ?)",
            ("BBG000EMPTY", ""),
        )

    assert list(_iter_canonical_equity_json_payloads()) == []
