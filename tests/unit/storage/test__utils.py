# tests/test__utils.py

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

from equity_aggregator.storage._utils import (
    connect,
    get_data_store_path,
    ttl_seconds,
    validate_table_exists_with_data,
)

pytestmark = pytest.mark.unit


def test_ttl_seconds_returns_default_when_env_not_set() -> None:
    """
    ARRANGE: CACHE_TTL_MINUTES environment variable not set
    ACT:     ttl_seconds
    ASSERT:  returns 24 hours in seconds (86400)
    """
    default_seconds = 86400  # 1440 minutes * 60 seconds
    original_value = os.environ.get("CACHE_TTL_MINUTES")

    if "CACHE_TTL_MINUTES" in os.environ:
        del os.environ["CACHE_TTL_MINUTES"]

    try:
        result = ttl_seconds()
        assert result == default_seconds
    finally:
        if original_value is not None:
            os.environ["CACHE_TTL_MINUTES"] = original_value


def test_ttl_seconds_converts_minutes_to_seconds() -> None:
    """
    ARRANGE: CACHE_TTL_MINUTES set to 30 minutes
    ACT:     ttl_seconds
    ASSERT:  returns 1800 seconds
    """
    expected_seconds = 1800
    original_value = os.environ.get("CACHE_TTL_MINUTES")
    os.environ["CACHE_TTL_MINUTES"] = "30"

    try:
        result = ttl_seconds()
        assert result == expected_seconds
    finally:
        if original_value is None:
            os.environ.pop("CACHE_TTL_MINUTES", None)
        else:
            os.environ["CACHE_TTL_MINUTES"] = original_value


def test_ttl_seconds_handles_zero_minutes() -> None:
    """
    ARRANGE: CACHE_TTL_MINUTES set to 0
    ACT:     ttl_seconds
    ASSERT:  returns 0 seconds
    """
    original_value = os.environ.get("CACHE_TTL_MINUTES")
    os.environ["CACHE_TTL_MINUTES"] = "0"

    try:
        result = ttl_seconds()
        assert result == 0
    finally:
        if original_value is None:
            os.environ.pop("CACHE_TTL_MINUTES", None)
        else:
            os.environ["CACHE_TTL_MINUTES"] = original_value


def test_ttl_seconds_raises_on_negative_value() -> None:
    """
    ARRANGE: CACHE_TTL_MINUTES set to negative value
    ACT:     ttl_seconds
    ASSERT:  raises ValueError
    """
    original_value = os.environ.get("CACHE_TTL_MINUTES")
    os.environ["CACHE_TTL_MINUTES"] = "-1"

    try:
        with pytest.raises(ValueError):
            ttl_seconds()
    finally:
        if original_value is None:
            os.environ.pop("CACHE_TTL_MINUTES", None)
        else:
            os.environ["CACHE_TTL_MINUTES"] = original_value


def test_get_data_store_path_returns_path_object() -> None:
    """
    ARRANGE: Default configuration
    ACT:     get_data_store_path
    ASSERT:  returns Path instance
    """
    result = get_data_store_path()

    assert isinstance(result, Path)


def test_validate_table_exists_with_data_returns_false_for_missing_table() -> None:
    """
    ARRANGE: Database connection with no tables
    ACT:     validate_table_exists_with_data with non-existent table
    ASSERT:  returns False
    """
    with tempfile.NamedTemporaryFile() as tmp_file:
        conn = sqlite3.connect(tmp_file.name)

        result = validate_table_exists_with_data(conn, "missing_table")

        conn.close()
        assert result is False


def test_validate_table_exists_with_data_returns_false_for_empty_table() -> None:
    """
    ARRANGE: Database with empty table
    ACT:     validate_table_exists_with_data
    ASSERT:  returns False
    """
    with tempfile.NamedTemporaryFile() as tmp_file:
        conn = sqlite3.connect(tmp_file.name)
        conn.execute("CREATE TABLE test_table (id INTEGER)")

        result = validate_table_exists_with_data(conn, "test_table")

        conn.close()
        assert result is False


def test_validate_table_exists_with_data_returns_true_for_table_with_data() -> None:
    """
    ARRANGE: Database with table containing data
    ACT:     validate_table_exists_with_data
    ASSERT:  returns True
    """
    with tempfile.NamedTemporaryFile() as tmp_file:
        conn = sqlite3.connect(tmp_file.name)
        conn.execute("CREATE TABLE test_table (id INTEGER)")
        conn.execute("INSERT INTO test_table (id) VALUES (1)")

        result = validate_table_exists_with_data(conn, "test_table")

        conn.close()
        assert result is True


def test_connect_creates_database_connection() -> None:
    """
    ARRANGE: Temporary data store path
    ACT:     connect context manager
    ASSERT:  yields sqlite3.Connection instance
    """
    with connect() as conn:
        assert isinstance(conn, sqlite3.Connection)
