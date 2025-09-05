# storage/test_metadata.py

import os

import pytest

from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity
from equity_aggregator.storage._utils import (
    CANONICAL_EQUITIES_TABLE,
    DATA_STORE_PATH,
    connect,
)
from equity_aggregator.storage.data_store import save_canonical_equities
from equity_aggregator.storage.metadata import (
    ensure_fresh_database,
    update_canonical_equities_timestamp,
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
    ARRANGE: stale database and refresh function
    ACT:     ensure_fresh_database
    ASSERT:  refresh function was called
    """
    os.environ["CACHE_TTL_MINUTES"] = "1"
    equity = _create_canonical_equity("BBG000TEST01")
    save_canonical_equities([equity])

    with connect() as conn:
        update_canonical_equities_timestamp(conn)
        conn.execute(
            "UPDATE data_metadata SET last_updated = last_updated - 120 "
            "WHERE table_name = ?",
            (CANONICAL_EQUITIES_TABLE,),
        )

    refresh_called = False

    def mock_refresh() -> None:
        nonlocal refresh_called
        refresh_called = True

    ensure_fresh_database(mock_refresh)

    # Restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert refresh_called is True


def test_ensure_fresh_database_returns_true_when_stale() -> None:
    """
    ARRANGE: stale database and refresh function
    ACT:     ensure_fresh_database
    ASSERT:  returns True
    """
    os.environ["CACHE_TTL_MINUTES"] = "1"
    equity = _create_canonical_equity("BBG000TEST04")
    save_canonical_equities([equity])

    with connect() as conn:
        update_canonical_equities_timestamp(conn)
        conn.execute(
            "UPDATE data_metadata SET last_updated = last_updated - 120 "
            "WHERE table_name = ?",
            (CANONICAL_EQUITIES_TABLE,),
        )

    def mock_refresh() -> None:
        pass

    actual = ensure_fresh_database(mock_refresh)

    # Restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert actual is True


def test_ensure_fresh_database_skips_refresh_when_fresh() -> None:
    """
    ARRANGE: fresh database and refresh function
    ACT:     ensure_fresh_database
    ASSERT:  refresh function was not called
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"
    equity = _create_canonical_equity("BBG000TEST02")
    save_canonical_equities([equity])

    refresh_called = False

    def mock_refresh() -> None:
        nonlocal refresh_called
        refresh_called = True

    ensure_fresh_database(mock_refresh)

    # Restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert refresh_called is False


def test_ensure_fresh_database_returns_false_when_fresh() -> None:
    """
    ARRANGE: fresh database and refresh function
    ACT:     ensure_fresh_database
    ASSERT:  returns False
    """
    os.environ["CACHE_TTL_MINUTES"] = "60"
    equity = _create_canonical_equity("BBG000TEST05")
    save_canonical_equities([equity])

    def mock_refresh() -> None:
        pass

    actual = ensure_fresh_database(mock_refresh)

    # Restore cache ttl minutes to original value
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
    save_canonical_equities([equity])

    with connect() as conn:
        update_canonical_equities_timestamp(conn)
        conn.execute(
            "UPDATE data_metadata SET last_updated = last_updated - 120 "
            "WHERE table_name = ?",
            (CANONICAL_EQUITIES_TABLE,),
        )

    actual = ensure_fresh_database(None)

    # Restore cache ttl minutes to original value
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

    # Restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"
    assert refresh_called is True
