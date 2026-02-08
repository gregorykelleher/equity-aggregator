# storage/test_freshness.py

import datetime
import os
import sqlite3

import pytest

from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity
from equity_aggregator.storage._utils import (
    DATA_STORE_PATH,
)
from equity_aggregator.storage.data_store import save_canonical_equities
from equity_aggregator.storage.freshness import (
    ensure_fresh_database,
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


def test_ensure_fresh_database_calls_refresh_when_stale() -> None:
    """
    ARRANGE: save equity with yesterday's snapshot_date
    ACT:     ensure_fresh_database
    ASSERT:  refresh function was called
    """
    os.environ["CACHE_TTL_MINUTES"] = "1"
    equity = _create_canonical_equity("BBG000TEST01")
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    save_canonical_equities([equity], snapshot_date=yesterday)

    refresh_called = False

    def mock_refresh() -> None:
        nonlocal refresh_called
        refresh_called = True

    ensure_fresh_database(mock_refresh)

    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert refresh_called is True


def test_ensure_fresh_database_returns_true_when_stale() -> None:
    """
    ARRANGE: save equity with yesterday's snapshot_date
    ACT:     ensure_fresh_database
    ASSERT:  returns True
    """
    os.environ["CACHE_TTL_MINUTES"] = "1"
    equity = _create_canonical_equity("BBG000TEST04")
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    save_canonical_equities([equity], snapshot_date=yesterday)

    def mock_refresh() -> None:
        pass

    actual = ensure_fresh_database(mock_refresh)

    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert actual is True


def test_ensure_fresh_database_skips_refresh_when_fresh() -> None:
    """
    ARRANGE: save equity with today's snapshot_date
    ACT:     ensure_fresh_database
    ASSERT:  refresh function was not called
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"
    equity = _create_canonical_equity("BBG000TEST02")
    today = datetime.date.today().isoformat()
    save_canonical_equities([equity], snapshot_date=today)

    refresh_called = False

    def mock_refresh() -> None:
        nonlocal refresh_called
        refresh_called = True

    ensure_fresh_database(mock_refresh)

    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert refresh_called is False


def test_ensure_fresh_database_returns_false_when_fresh() -> None:
    """
    ARRANGE: save equity with today's snapshot_date
    ACT:     ensure_fresh_database
    ASSERT:  returns False
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"
    equity = _create_canonical_equity("BBG000TEST05")
    today = datetime.date.today().isoformat()
    save_canonical_equities([equity], snapshot_date=today)

    def mock_refresh() -> None:
        pass

    actual = ensure_fresh_database(mock_refresh)

    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert actual is False


def test_ensure_fresh_database_skips_refresh_when_no_refresh_fn() -> None:
    """
    ARRANGE: stale database but no refresh function
    ACT:     ensure_fresh_database
    ASSERT:  returns False (no refresh performed)
    """
    os.environ["CACHE_TTL_MINUTES"] = "1"
    equity = _create_canonical_equity("BBG000TEST03")
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    save_canonical_equities([equity], snapshot_date=yesterday)

    actual = ensure_fresh_database(None)

    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert actual is False


def test_ensure_fresh_database_with_ttl_disabled() -> None:
    """
    ARRANGE: TTL set to 0 (disabled)
    ACT:     ensure_fresh_database
    ASSERT:  returns False (no refresh needed)
    """
    os.environ["CACHE_TTL_MINUTES"] = "0"

    def mock_refresh() -> None:
        pass

    actual = ensure_fresh_database(mock_refresh)

    assert actual is False


def test_ensure_fresh_database_with_ttl_disabled_skips_refresh() -> None:
    """
    ARRANGE: TTL set to 0 (disabled) and refresh function
    ACT:     ensure_fresh_database
    ASSERT:  refresh function was not called
    """
    os.environ["CACHE_TTL_MINUTES"] = "0"
    refresh_called = False

    def mock_refresh() -> None:
        nonlocal refresh_called
        refresh_called = True

    ensure_fresh_database(mock_refresh)

    assert refresh_called is False


def test_ensure_fresh_database_calls_refresh_when_no_database() -> None:
    """
    ARRANGE: no database file exists and refresh function
    ACT:     ensure_fresh_database
    ASSERT:  refresh function was called
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"

    DATA_STORE_PATH.mkdir(parents=True, exist_ok=True)
    db_path = DATA_STORE_PATH / "data_store.db"

    if db_path.exists():
        db_path.unlink()

    refresh_called = False

    def mock_refresh() -> None:
        nonlocal refresh_called
        refresh_called = True

    ensure_fresh_database(mock_refresh)

    os.environ["CACHE_TTL_MINUTES"] = "0"
    assert refresh_called is True


def test_ensure_fresh_database_stale_when_no_snapshot_table() -> None:
    """
    ARRANGE: database file exists but has no snapshot table
    ACT:     ensure_fresh_database
    ASSERT:  refresh function was called
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"

    DATA_STORE_PATH.mkdir(parents=True, exist_ok=True)
    db_path = DATA_STORE_PATH / "data_store.db"

    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS dummy (id INTEGER)")
    conn.commit()
    conn.close()

    refresh_called = False

    def mock_refresh() -> None:
        nonlocal refresh_called
        refresh_called = True

    ensure_fresh_database(mock_refresh)

    os.environ["CACHE_TTL_MINUTES"] = "0"
    assert refresh_called is True
