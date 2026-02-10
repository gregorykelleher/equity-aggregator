# feeds/test_yfinance_feed_data.py

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import YFinanceFeedData
from equity_aggregator.schemas.feeds.yfinance_feed_data import _parse_last_time

pytestmark = pytest.mark.unit


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct YFinanceFeedData
    ASSERT:  extra field is not present on the model
    """
    raw = {
        "longName": "Foo Inc",
        "underlyingSymbol": "FOO",
        "currency": "USD",
        "currentPrice": None,
        "marketCap": None,
    }

    actual = YFinanceFeedData(**raw, unexpected="FIELD")

    assert not hasattr(actual, "unexpected")


def test_preserves_optional_none_fields() -> None:
    """
    ARRANGE: currency and marketCap set to None
    ACT:     construct YFinanceFeedData
    ASSERT:  optional field 'currency' is preserved as None
    """
    raw = {
        "longName": "Foo Inc",
        "underlyingSymbol": "FOO",
        "currency": None,
        "currentPrice": 1.23,
        "marketCap": None,
    }

    actual = YFinanceFeedData(**raw)

    assert actual.currency is None


def test_accepts_various_numeric_types() -> None:
    """
    ARRANGE: currentPrice and marketCap with several numeric/string/Decimal types
    ACT:     construct YFinanceFeedData for each payload
    ASSERT:  values are preserved exactly
    """
    for price in (123, 123.45, "123.45", Decimal("123.45")):
        raw = {
            "longName": "Foo Inc",
            "underlyingSymbol": "FOO",
            "currency": "USD",
            "currentPrice": price,
            "marketCap": price,
        }

        actual = YFinanceFeedData(**raw)

        assert (actual.last_price, actual.market_cap) == (price, price)


def test_missing_required_raises() -> None:
    """
    ARRANGE: omit required 'longName'
    ACT:     construct YFinanceFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "underlyingSymbol": "FOO",
        "currency": "USD",
        "currentPrice": 1,
    }

    with pytest.raises(ValidationError):
        YFinanceFeedData(**incomplete)


def test_normalises_and_preserves_whitespace() -> None:
    """
    ARRANGE: raw fields include padding/whitespace
    ACT:     construct YFinanceFeedData
    ASSERT:  whitespace in 'name' is retained (no trimming at this layer)
    """
    raw = {
        "longName": "  Padded Name  ",
        "underlyingSymbol": " PAD ",
        "currency": " usd ",
        "currentPrice": "1,23",
        "marketCap": None,
    }

    actual = YFinanceFeedData(**raw)

    assert actual.name == "  Padded Name  "


def test_last_price_string_with_comma_preserved() -> None:
    """
    ARRANGE: currentPrice as string using comma decimal
    ACT:     construct YFinanceFeedData
    ASSERT:  last_price is preserved as string
    """
    raw = {
        "longName": "Foo Inc",
        "underlyingSymbol": "FOO",
        "currency": "USD",
        "currentPrice": "1,23",
        "marketCap": None,
    }

    actual = YFinanceFeedData(**raw)

    assert actual.last_price == "1,23"


def test_stale_trade_nullifies_price_fields() -> None:
    """
    ARRANGE: payload with regularMarketTime 48h ago (exceeds 36h default)
    ACT:     construct YFinanceFeedData
    ASSERT:  last_price is nullified
    """
    hours_ago = 48
    stale_time = datetime.now(UTC) - timedelta(hours=hours_ago)

    raw = {
        "longName": "Stale Corp",
        "underlyingSymbol": "STALE",
        "currency": "USD",
        "currentPrice": 100.0,
        "marketCap": 5000000,
        "regularMarketTime": stale_time.timestamp(),
    }

    actual = YFinanceFeedData(**raw)

    assert actual.last_price is None


def test_stale_trade_preserves_identity_fields() -> None:
    """
    ARRANGE: payload with regularMarketTime 48h ago (exceeds 36h default)
    ACT:     construct YFinanceFeedData
    ASSERT:  non-price fields are preserved
    """
    hours_ago = 48
    stale_time = datetime.now(UTC) - timedelta(hours=hours_ago)

    raw = {
        "longName": "Stale Corp",
        "underlyingSymbol": "STALE",
        "currency": "USD",
        "currentPrice": 100.0,
        "marketCap": 5000000,
        "regularMarketTime": stale_time.timestamp(),
    }

    actual = YFinanceFeedData(**raw)

    assert actual.symbol == "STALE"


def test_fresh_trade_preserves_price_fields() -> None:
    """
    ARRANGE: payload with regularMarketTime just now (within 36h default)
    ACT:     construct YFinanceFeedData
    ASSERT:  last_price is preserved
    """
    expected_price = 250.0
    fresh_time = datetime.now(UTC)

    raw = {
        "longName": "Fresh Corp",
        "underlyingSymbol": "FRESH",
        "currency": "USD",
        "currentPrice": expected_price,
        "marketCap": 5000000,
        "regularMarketTime": fresh_time.timestamp(),
    }

    actual = YFinanceFeedData(**raw)

    assert actual.last_price == expected_price


def test_missing_regular_market_time_preserves_price_fields() -> None:
    """
    ARRANGE: payload without regularMarketTime
    ACT:     construct YFinanceFeedData
    ASSERT:  last_price is preserved (fail-open)
    """
    expected_price = 300.0

    raw = {
        "longName": "No Time Corp",
        "underlyingSymbol": "NOTM",
        "currency": "USD",
        "currentPrice": expected_price,
        "marketCap": 5000000,
    }

    actual = YFinanceFeedData(**raw)

    assert actual.last_price == expected_price


def test_malformed_regular_market_time_preserves_price_fields() -> None:
    """
    ARRANGE: payload with non-numeric regularMarketTime
    ACT:     construct YFinanceFeedData
    ASSERT:  last_price is preserved (fail-open)
    """
    expected_price = 150.0

    raw = {
        "longName": "Bad Time Corp",
        "underlyingSymbol": "BADTM",
        "currency": "USD",
        "currentPrice": expected_price,
        "marketCap": 5000000,
        "regularMarketTime": "not-a-timestamp",
    }

    actual = YFinanceFeedData(**raw)

    assert actual.last_price == expected_price


def test_parse_last_time_converts_unix_timestamp() -> None:
    """
    ARRANGE: Unix timestamp as integer
    ACT:     parse last time
    ASSERT:  returns UTC datetime
    """
    expected = datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)

    actual = _parse_last_time(1700000000)

    assert actual == expected


def test_parse_last_time_returns_none_for_none() -> None:
    """
    ARRANGE: None value
    ACT:     parse last time
    ASSERT:  returns None
    """
    actual = _parse_last_time(None)

    assert actual is None


def test_parse_last_time_returns_none_for_invalid_value() -> None:
    """
    ARRANGE: non-numeric string
    ACT:     parse last time
    ASSERT:  returns None
    """
    actual = _parse_last_time("not-a-timestamp")

    assert actual is None
