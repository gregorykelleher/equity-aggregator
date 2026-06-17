# storage/test_cache.py

import os

import pytest

from equity_aggregator.storage._utils import CACHE_TABLE, connect, ttl_seconds
from equity_aggregator.storage.cache import (
    load_cache,
    load_cache_entry,
    save_cache,
    save_cache_entry,
)

pytestmark = pytest.mark.unit


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


def test_save_and_load_cache_roundtrip() -> None:
    """
    ARRANGE: save_cache
    ACT:     load_cache
    ASSERT:  loaded equals saved
    """
    payload = {"x": 1}
    save_cache("rt", payload)

    assert load_cache("rt") == payload


def test_save_and_load_cache_entry_roundtrip() -> None:
    """
    ARRANGE: save_cache_entry
    ACT:     load_cache_entry
    ASSERT:  loaded equals saved
    """
    payload = [1, 2]
    save_cache_entry("rt2", "k", payload)

    assert load_cache_entry("rt2", "k") == payload


def test_load_cache_returns_none_when_expired() -> None:
    """
    ARRANGE: positive TTL and artificially age entry
    ACT:     load_cache
    ASSERT:  returns None
    """
    os.environ["CACHE_TTL_MINUTES"] = "1"
    save_cache("exp", value=True)

    ttl = ttl_seconds()
    with connect() as conn:
        conn.execute(
            f"UPDATE {CACHE_TABLE} SET created_at = created_at - ?",
            (ttl + 1,),
        )

    assert load_cache("exp") is None

    # restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"


def test_purge_expired_sweeps_all_keys() -> None:
    """
    ARRANGE: two aged entries under different keys, positive TTL
    ACT:     read one key (triggers a table-wide purge)
    ASSERT:  the other, unread key's entry is also removed
    """
    os.environ["CACHE_TTL_MINUTES"] = "1"
    save_cache_entry("sweep", "k1", value=1)
    save_cache_entry("sweep", "k2", value=2)

    ttl = ttl_seconds()
    with connect() as conn:
        conn.execute(
            f"UPDATE {CACHE_TABLE} SET created_at = created_at - ?",
            (ttl + 1,),
        )

    load_cache_entry("sweep", "k1")
    actual = load_cache_entry("sweep", "k2")

    # restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"

    assert actual is None


def test_ttl_seconds_negative_raises_value_error() -> None:
    """
    ARRANGE: CACHE_TTL_MINUTES = -5
    ACT:     ttl_seconds
    ASSERT:  ValueError raised with correct message
    """
    os.environ["CACHE_TTL_MINUTES"] = "-5"

    with pytest.raises(ValueError, match="≥ 0"):
        ttl_seconds()

    # Restore cache ttl minutes to original value
    os.environ["CACHE_TTL_MINUTES"] = "0"


def test_save_cache_entry_noop_when_cache_name_none() -> None:
    """
    ARRANGE: ensure cache table exists and capture row count
    ACT:     save_cache_entry with cache_name=None
    ASSERT:  row count unchanged
    """
    save_cache("warmup", value=True)
    before = _count_rows(CACHE_TABLE)

    save_cache_entry(None, "ignored", {"x": 1})

    assert _count_rows(CACHE_TABLE) == before


def test_load_cache_returns_none_when_cache_name_none() -> None:
    """
    ARRANGE: none
    ACT:     load_cache with cache_name=None
    ASSERT:  returns None
    """
    assert load_cache(None) is None
