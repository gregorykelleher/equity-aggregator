# storage/test_data_store.py

import datetime
import os

import pytest

from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity
from equity_aggregator.storage._utils import (
    CANONICAL_EQUITY_IDENTITIES_TABLE,
    CANONICAL_EQUITY_SNAPSHOTS_TABLE,
    connect,
)
from equity_aggregator.storage.data_store import (
    load_canonical_equities,
    load_canonical_equity,
    load_canonical_equity_history,
    save_canonical_equities,
)

pytestmark = pytest.mark.unit


def _create_canonical_equity(
    figi: str,
    name: str = "TEST EQUITY",
    last_price: float | None = None,
) -> CanonicalEquity:
    """
    Create a CanonicalEquity instance for testing purposes.

    Args:
        figi (str): The FIGI identifier for the equity.
        name (str): The name of the equity, defaults to "TEST EQUITY".
        last_price (float | None): Optional last price for the equity.

    Returns:
        CanonicalEquity: A properly constructed CanonicalEquity instance.
    """
    identity = EquityIdentity(
        name=name,
        symbol="TST",
        share_class_figi=figi,
    )
    financials = EquityFinancials(last_price=last_price)

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


def test_save_equities_inserts_identity_rows() -> None:
    """
    ARRANGE: two CanonicalEquity objects
    ACT:     save_canonical_equities
    ASSERT:  identity row count == 2
    """
    expected_row_count = 2
    equities = [
        _create_canonical_equity("BBG000B9XRY4", "EQUITY ONE"),
        _create_canonical_equity("BBG000BKQV61", "EQUITY TWO"),
    ]

    save_canonical_equities(equities)

    assert _count_rows(CANONICAL_EQUITY_IDENTITIES_TABLE) >= expected_row_count


def test_save_equities_inserts_snapshot_rows() -> None:
    """
    ARRANGE: two CanonicalEquity objects
    ACT:     save_canonical_equities
    ASSERT:  snapshot row count == 2
    """
    equities = [
        _create_canonical_equity("BBG000SNAP01", "EQUITY ONE"),
        _create_canonical_equity("BBG000SNAP02", "EQUITY TWO"),
    ]

    save_canonical_equities(equities, snapshot_date="2025-01-15")

    expected_snapshot_count = 2

    with connect() as conn:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {CANONICAL_EQUITY_SNAPSHOTS_TABLE} WHERE snapshot_date = ?",
            ("2025-01-15",),
        ).fetchone()[0]

    assert count == expected_snapshot_count


def test_save_equities_upsert_single_identity_row() -> None:
    """
    ARRANGE: same FIGI saved twice on same date
    ACT:     save_canonical_equities twice
    ASSERT:  identity row count == 1
    """
    figi = "BBG000C6K6G9"
    equity = _create_canonical_equity(figi)

    save_canonical_equities([equity], snapshot_date="2025-01-01")
    save_canonical_equities([equity], snapshot_date="2025-01-01")

    with connect() as conn:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {CANONICAL_EQUITY_IDENTITIES_TABLE} "
            "WHERE share_class_figi = ?",
            (figi,),
        ).fetchone()[0]

    assert count == 1


def test_load_canonical_equities_rehydrates_objects() -> None:
    """
    ARRANGE: save two CanonicalEquity objects
    ACT:     load_canonical_equities
    ASSERT:  loaded objects match original identities
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"

    equities = [
        _create_canonical_equity("BBG000B9XRY4", "ONE"),
        _create_canonical_equity("BBG000BKQV61", "TWO"),
    ]

    save_canonical_equities(equities)

    loaded = load_canonical_equities()

    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert [equity.identity.share_class_figi for equity in loaded] == [
        "BBG000B9XRY4",
        "BBG000BKQV61",
    ]


def test_save_equities_appends_snapshots_for_different_dates() -> None:
    """
    ARRANGE: same equity saved on two different dates
    ACT:     save_canonical_equities with two dates
    ASSERT:  1 identity row, 2 snapshot rows
    """
    figi = "BBG000MULTI1"
    equity = _create_canonical_equity(figi, "MULTI")

    save_canonical_equities([equity], snapshot_date="2025-01-01")
    save_canonical_equities([equity], snapshot_date="2025-01-02")

    with connect() as conn:
        identity_count = conn.execute(
            f"SELECT COUNT(*) FROM {CANONICAL_EQUITY_IDENTITIES_TABLE} "
            "WHERE share_class_figi = ?",
            (figi,),
        ).fetchone()[0]

    assert identity_count == 1


def test_save_equities_appends_snapshots_for_different_dates_snapshot_count() -> None:
    """
    ARRANGE: same equity saved on two different dates
    ACT:     save_canonical_equities with two dates
    ASSERT:  2 snapshot rows
    """
    figi = "BBG000MULTI2"
    equity = _create_canonical_equity(figi, "MULTI")

    save_canonical_equities([equity], snapshot_date="2025-01-01")
    save_canonical_equities([equity], snapshot_date="2025-01-02")

    expected_snapshot_count = 2

    with connect() as conn:
        snapshot_count = conn.execute(
            f"SELECT COUNT(*) FROM {CANONICAL_EQUITY_SNAPSHOTS_TABLE} WHERE share_class_figi = ?",
            (figi,),
        ).fetchone()[0]

    assert snapshot_count == expected_snapshot_count


def test_save_equities_same_day_is_idempotent() -> None:
    """
    ARRANGE: same equity saved twice on same day
    ACT:     save_canonical_equities twice with same date
    ASSERT:  1 snapshot row
    """
    figi = "BBG000IDEMP1"
    equity = _create_canonical_equity(figi, "IDEMPOTENT")

    save_canonical_equities([equity], snapshot_date="2025-01-01")
    save_canonical_equities([equity], snapshot_date="2025-01-01")

    with connect() as conn:
        snapshot_count = conn.execute(
            f"SELECT COUNT(*) FROM {CANONICAL_EQUITY_SNAPSHOTS_TABLE} WHERE share_class_figi = ?",
            (figi,),
        ).fetchone()[0]

    assert snapshot_count == 1


def test_load_canonical_equity_returns_latest_snapshot() -> None:
    """
    ARRANGE: save equity with two different dates and different prices
    ACT:     load_canonical_equity
    ASSERT:  returns the financials from the latest snapshot
    """
    figi = "BBG000LAEST1"
    equity_old = _create_canonical_equity(figi, "LATEST", last_price=100.0)
    equity_new = _create_canonical_equity(figi, "LATEST", last_price=200.0)

    save_canonical_equities([equity_old], snapshot_date="2025-01-01")
    save_canonical_equities([equity_new], snapshot_date="2025-01-02")

    expected_price = 200.0

    loaded = load_canonical_equity(figi)

    assert loaded.financials.last_price == expected_price


def test_load_canonical_equities_returns_latest_snapshots() -> None:
    """
    ARRANGE: save two equities with multiple dates
    ACT:     load_canonical_equities
    ASSERT:  returns the latest financials for each equity
    """
    os.environ["CACHE_TTL_MINUTES"] = "0"

    figi_a = "BBG000LATA01"
    figi_b = "BBG000LATB01"

    save_canonical_equities(
        [
            _create_canonical_equity(figi_a, "A", last_price=10.0),
            _create_canonical_equity(figi_b, "B", last_price=20.0),
        ],
        snapshot_date="2025-01-01",
    )

    save_canonical_equities(
        [
            _create_canonical_equity(figi_a, "A", last_price=11.0),
            _create_canonical_equity(figi_b, "B", last_price=22.0),
        ],
        snapshot_date="2025-01-02",
    )

    loaded = load_canonical_equities()

    expected_price = 11.0

    actual = {e.identity.share_class_figi: e.financials.last_price for e in loaded}

    assert actual[figi_a] == expected_price


def test_load_canonical_equity_history_returns_all_snapshots() -> None:
    """
    ARRANGE: save equity with 3 dates
    ACT:     load_canonical_equity_history with no filters
    ASSERT:  returns all 3 snapshots
    """
    figi = "BBG000HIST01"

    for date in ["2025-01-01", "2025-01-02", "2025-01-03"]:
        save_canonical_equities(
            [_create_canonical_equity(figi, "HIST")],
            snapshot_date=date,
        )

    expected_count = 3

    actual = load_canonical_equity_history(figi)

    assert len(actual) == expected_count


def test_load_canonical_equity_history_filters_by_from_date() -> None:
    """
    ARRANGE: save equity with 3 dates
    ACT:     load_canonical_equity_history with from_date filter
    ASSERT:  returns last 2 snapshots
    """
    figi = "BBG000HIST02"

    for date in ["2025-01-01", "2025-01-02", "2025-01-03"]:
        save_canonical_equities(
            [_create_canonical_equity(figi, "HIST")],
            snapshot_date=date,
        )

    expected_count = 2

    actual = load_canonical_equity_history(figi, from_date="2025-01-02")

    assert len(actual) == expected_count


def test_load_canonical_equity_history_filters_by_to_date() -> None:
    """
    ARRANGE: save equity with 3 dates
    ACT:     load_canonical_equity_history with to_date filter
    ASSERT:  returns first 2 snapshots
    """
    figi = "BBG000HIST03"

    for date in ["2025-01-01", "2025-01-02", "2025-01-03"]:
        save_canonical_equities(
            [_create_canonical_equity(figi, "HIST")],
            snapshot_date=date,
        )

    expected_count = 2

    actual = load_canonical_equity_history(figi, to_date="2025-01-02")

    assert len(actual) == expected_count


def test_load_canonical_equity_history_filters_by_date_range() -> None:
    """
    ARRANGE: save equity with 3 dates
    ACT:     load_canonical_equity_history with from_date and to_date
    ASSERT:  returns middle snapshot only
    """
    figi = "BBG000HIST04"

    for date in ["2025-01-01", "2025-01-02", "2025-01-03"]:
        save_canonical_equities(
            [_create_canonical_equity(figi, "HIST")],
            snapshot_date=date,
        )

    actual = load_canonical_equity_history(figi, from_date="2025-01-02", to_date="2025-01-02")

    assert len(actual) == 1


def test_load_canonical_equity_history_returns_empty_for_unknown_figi() -> None:
    """
    ARRANGE: no equity with the given FIGI
    ACT:     load_canonical_equity_history
    ASSERT:  returns empty list
    """
    actual = load_canonical_equity_history("BBG000UNKNOWN")

    assert actual == []


def test_load_canonical_equity_history_populates_snapshot_date() -> None:
    """
    ARRANGE: save equity with 3 dates
    ACT:     load_canonical_equity_history
    ASSERT:  each returned equity has the correct snapshot_date
    """
    figi = "BBG000HIST05"
    expected_dates = ["2025-01-01", "2025-01-02", "2025-01-03"]

    for date in expected_dates:
        save_canonical_equities(
            [_create_canonical_equity(figi, "HIST")],
            snapshot_date=date,
        )

    actual = load_canonical_equity_history(figi)

    assert [e.snapshot_date for e in actual] == expected_dates


def test_save_equities_defaults_snapshot_date_to_today() -> None:
    """
    ARRANGE: save equity without explicit snapshot_date
    ACT:     save_canonical_equities
    ASSERT:  snapshot_date defaults to today
    """
    figi = "BBG000TODAY1"
    equity = _create_canonical_equity(figi, "TODAY")

    save_canonical_equities([equity])

    loaded = load_canonical_equity(figi)
    expected = datetime.date.today().isoformat()

    assert loaded.snapshot_date == expected
