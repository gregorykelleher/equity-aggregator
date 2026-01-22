# tests/test__utils.py

import os
import sqlite3
import tempfile

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
        actual = ttl_seconds()
        assert actual == default_seconds
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
        actual = ttl_seconds()
        assert actual == expected_seconds
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
        actual = ttl_seconds()
        assert actual == 0
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


def test_validate_table_exists_with_data_returns_false_for_missing_table() -> None:
    """
    ARRANGE: Database connection with no tables
    ACT:     validate_table_exists_with_data with non-existent table
    ASSERT:  returns False
    """
    with tempfile.NamedTemporaryFile() as tmp_file:
        conn = sqlite3.connect(tmp_file.name)

        actual = validate_table_exists_with_data(conn, "missing_table")

        conn.close()
        assert actual is False


def test_validate_table_exists_with_data_returns_false_for_empty_table() -> None:
    """
    ARRANGE: Database with empty table
    ACT:     validate_table_exists_with_data
    ASSERT:  returns False
    """
    with tempfile.NamedTemporaryFile() as tmp_file:
        conn = sqlite3.connect(tmp_file.name)
        conn.execute("CREATE TABLE test_table (id INTEGER)")

        actual = validate_table_exists_with_data(conn, "test_table")

        conn.close()
        assert actual is False


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

        actual = validate_table_exists_with_data(conn, "test_table")

        conn.close()
        assert actual is True


def test_connect_creates_database_connection() -> None:
    """
    ARRANGE: Temporary data store path
    ACT:     connect context manager
    ASSERT:  yields sqlite3.Connection instance
    """
    with connect() as conn:
        assert isinstance(conn, sqlite3.Connection)


def test_get_data_store_path_with_override() -> None:
    """
    ARRANGE: Set DATA_STORE_DIR environment variable
    ACT:     get_data_store_path
    ASSERT:  Returns override path
    """
    original = os.environ.get("DATA_STORE_DIR")
    os.environ["DATA_STORE_DIR"] = "/custom/path"

    try:
        actual = get_data_store_path()
        assert str(actual) == "/custom/path"
    finally:
        if original is not None:
            os.environ["DATA_STORE_DIR"] = original
        elif "DATA_STORE_DIR" in os.environ:
            del os.environ["DATA_STORE_DIR"]


def test_get_data_store_path_default() -> None:
    """
    ARRANGE: Remove DATA_STORE_DIR environment variable
    ACT:     get_data_store_path
    ASSERT:  Returns user_data_dir path
    """
    original = os.environ.get("DATA_STORE_DIR")
    if "DATA_STORE_DIR" in os.environ:
        del os.environ["DATA_STORE_DIR"]

    try:
        actual = get_data_store_path()
        assert "equity-aggregator" in str(actual)
    finally:
        if original is not None:
            os.environ["DATA_STORE_DIR"] = original
