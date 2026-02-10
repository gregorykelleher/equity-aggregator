# feeds/test_staleness.py

from datetime import UTC, datetime, timedelta

import pytest

from equity_aggregator.schemas.feeds._staleness import (
    is_trade_stale,
    nullify_price_fields,
)

pytestmark = pytest.mark.unit


def test_none_is_not_stale() -> None:
    """
    ARRANGE: last_trade_time is None
    ACT:     call is_trade_stale
    ASSERT:  returns False (fail-open)
    """
    actual = is_trade_stale(None)

    assert actual is False


def test_recent_trade_is_not_stale() -> None:
    """
    ARRANGE: last_trade_time is now (0h elapsed)
    ACT:     call is_trade_stale
    ASSERT:  returns False
    """
    recent = datetime.now(UTC)

    actual = is_trade_stale(recent)

    assert actual is False


def test_old_trade_is_stale() -> None:
    """
    ARRANGE: last_trade_time is 48h ago (exceeds 36h default)
    ACT:     call is_trade_stale
    ASSERT:  returns True
    """
    hours_ago = 48
    old = datetime.now(UTC) - timedelta(hours=hours_ago)

    actual = is_trade_stale(old)

    assert actual is True


def test_custom_max_age_is_respected() -> None:
    """
    ARRANGE: last_trade_time is 2h ago, max_age_hours=1
    ACT:     call is_trade_stale
    ASSERT:  returns True
    """
    hours_ago = 2
    two_hours_ago = datetime.now(UTC) - timedelta(hours=hours_ago)

    actual = is_trade_stale(two_hours_ago, max_age_hours=1)

    assert actual is True


def test_naive_datetime_treated_as_utc() -> None:
    """
    ARRANGE: timezone-naive datetime representing now (~0h elapsed)
    ACT:     call is_trade_stale
    ASSERT:  returns False (naive assumed UTC)
    """
    naive_now = datetime.now(UTC).replace(tzinfo=None)

    actual = is_trade_stale(naive_now)

    assert actual is False


def test_price_fields_are_nullified() -> None:
    """
    ARRANGE: dict with all five price-sensitive fields set to values
    ACT:     call nullify_price_fields
    ASSERT:  all price fields become None
    """
    fields = {
        "last_price": 180.01,
        "fifty_two_week_min": 120.00,
        "fifty_two_week_max": 280.00,
        "market_volume": 1_000_000,
        "market_cap": 3_000_000_000,
    }

    actual = nullify_price_fields(fields)

    expected = {
        "last_price": None,
        "fifty_two_week_min": None,
        "fifty_two_week_max": None,
        "market_volume": None,
        "market_cap": None,
    }
    assert actual == expected


def test_non_price_fields_are_preserved() -> None:
    """
    ARRANGE: dict with only non-price identity fields
    ACT:     call nullify_price_fields
    ASSERT:  all values are unchanged
    """
    fields = {
        "name": "APPLE INC.",
        "symbol": "AAPL",
        "currency": "USD",
    }

    actual = nullify_price_fields(fields)

    assert actual == fields


def test_already_none_fields_stay_none() -> None:
    """
    ARRANGE: dict with price fields already set to None
    ACT:     call nullify_price_fields
    ASSERT:  fields remain None (idempotent)
    """
    fields = {
        "last_price": None,
        "market_cap": None,
        "name": "APPLE INC.",
    }

    actual = nullify_price_fields(fields)

    expected = {
        "last_price": None,
        "market_cap": None,
        "name": "APPLE INC.",
    }
    assert actual == expected
