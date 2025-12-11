# feeds/test_intrinio_feed_data.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import IntrinioFeedData

pytestmark = pytest.mark.unit


def test_normalises_nested_security_fields() -> None:
    """
    ARRANGE: raw Intrinio payload with nested security object
    ACT:     construct IntrinioFeedData
    ASSERT:  security fields are flattened correctly
    """
    raw = {
        "security": {
            "ticker": "AAPL",
            "name": "Apple Inc",
            "currency": "USD",
            "share_class_figi": "BBG001S5N8V8",
        },
        "last": 150.0,
        "marketcap": 2500000000000,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.name == "Apple Inc"


def test_maps_ticker_to_symbol() -> None:
    """
    ARRANGE: raw payload with security.ticker
    ACT:     construct IntrinioFeedData
    ASSERT:  ticker is mapped to symbol
    """
    raw = {
        "security": {
            "ticker": "MSFT",
            "name": "Microsoft",
        },
        "last": 300.0,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.symbol == "MSFT"


def test_share_class_figi_always_none() -> None:
    """
    ARRANGE: raw payload with security.share_class_figi
    ACT:     construct IntrinioFeedData
    ASSERT:  share_class_figi is None (not from API, injected from discovery)
    """
    raw = {
        "security": {
            "ticker": "GOOGL",
            "name": "Alphabet Inc",
            "share_class_figi": "BBG001S5N8V8",
        },
        "last": 100.0,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.share_class_figi is None


def test_maps_last_to_last_price() -> None:
    """
    ARRANGE: raw payload with 'last' field
    ACT:     construct IntrinioFeedData
    ASSERT:  last is mapped to last_price
    """
    expected_price = 250.50

    raw = {
        "security": {
            "ticker": "TSLA",
            "name": "Tesla Inc",
        },
        "last": expected_price,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.last_price == expected_price


def test_maps_fifty_two_week_fields() -> None:
    """
    ARRANGE: raw payload with eod_fifty_two_week_low and eod_fifty_two_week_high
    ACT:     construct IntrinioFeedData
    ASSERT:  fields mapped to fifty_two_week_min and fifty_two_week_max
    """
    expected_low = 100.0

    raw = {
        "security": {
            "ticker": "NVDA",
            "name": "NVIDIA Corp",
        },
        "last": 500.0,
        "eod_fifty_two_week_low": expected_low,
        "eod_fifty_two_week_high": 600.0,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.fifty_two_week_min == expected_low


def test_maps_marketcap_to_market_cap() -> None:
    """
    ARRANGE: raw payload with 'marketcap' field
    ACT:     construct IntrinioFeedData
    ASSERT:  marketcap is mapped to market_cap
    """
    expected_market_cap = 900000000000

    raw = {
        "security": {
            "ticker": "META",
            "name": "Meta Platforms",
        },
        "last": 350.0,
        "marketcap": expected_market_cap,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.market_cap == expected_market_cap


def test_maps_dividendyield_to_dividend_yield() -> None:
    """
    ARRANGE: raw payload with 'dividendyield' field
    ACT:     construct IntrinioFeedData
    ASSERT:  dividendyield is mapped to dividend_yield
    """
    expected_yield = 3.2

    raw = {
        "security": {
            "ticker": "KO",
            "name": "Coca-Cola",
        },
        "last": 60.0,
        "dividendyield": expected_yield,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.dividend_yield == expected_yield


def test_converts_percentage_to_decimal() -> None:
    """
    ARRANGE: raw payload with change_percent_365_days as percentage
    ACT:     construct IntrinioFeedData
    ASSERT:  performance_1_year converted from percentage to decimal
    """
    raw = {
        "security": {
            "ticker": "AMZN",
            "name": "Amazon",
        },
        "last": 140.0,
        "change_percent_365_days": 25.5,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.performance_1_year == Decimal("0.255")


def test_percentage_conversion_handles_none() -> None:
    """
    ARRANGE: raw payload with change_percent_365_days as None
    ACT:     construct IntrinioFeedData
    ASSERT:  performance_1_year is None
    """
    raw = {
        "security": {
            "ticker": "NFLX",
            "name": "Netflix",
        },
        "last": 450.0,
        "change_percent_365_days": None,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.performance_1_year is None


def test_percentage_conversion_handles_string() -> None:
    """
    ARRANGE: raw payload with change_percent_365_days as string
    ACT:     construct IntrinioFeedData
    ASSERT:  performance_1_year converted correctly
    """
    raw = {
        "security": {
            "ticker": "DIS",
            "name": "Disney",
        },
        "last": 90.0,
        "change_percent_365_days": "15.75",
    }

    actual = IntrinioFeedData(**raw)

    assert actual.performance_1_year == Decimal("0.1575")


def test_percentage_conversion_handles_negative() -> None:
    """
    ARRANGE: raw payload with negative change_percent_365_days
    ACT:     construct IntrinioFeedData
    ASSERT:  performance_1_year is negative decimal
    """
    raw = {
        "security": {
            "ticker": "PYPL",
            "name": "PayPal",
        },
        "last": 60.0,
        "change_percent_365_days": -12.5,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.performance_1_year == Decimal("-0.125")


def test_accepts_various_numeric_types() -> None:
    """
    ARRANGE: last_price with various numeric types
    ACT:     construct IntrinioFeedData for each type
    ASSERT:  values are preserved
    """
    for price in (100, 100.50, "100.50", Decimal("100.50")):
        raw = {
            "security": {
                "ticker": "IBM",
                "name": "IBM Corp",
            },
            "last": price,
        }

        actual = IntrinioFeedData(**raw)

        assert actual.last_price == price


def test_missing_required_name_raises() -> None:
    """
    ARRANGE: payload missing security.name
    ACT:     construct IntrinioFeedData
    ASSERT:  raises ValidationError
    """
    raw = {
        "security": {
            "ticker": "ORCL",
        },
        "last": 110.0,
    }

    with pytest.raises(ValidationError):
        IntrinioFeedData(**raw)


def test_missing_required_symbol_raises() -> None:
    """
    ARRANGE: payload missing security.ticker
    ACT:     construct IntrinioFeedData
    ASSERT:  raises ValidationError
    """
    raw = {
        "security": {
            "name": "Oracle Corp",
        },
        "last": 110.0,
    }

    with pytest.raises(ValidationError):
        IntrinioFeedData(**raw)


def test_empty_name_raises() -> None:
    """
    ARRANGE: payload with empty security.name
    ACT:     construct IntrinioFeedData
    ASSERT:  raises ValidationError
    """
    raw = {
        "security": {
            "ticker": "ADBE",
            "name": "",
        },
        "last": 550.0,
    }

    with pytest.raises(ValidationError):
        IntrinioFeedData(**raw)


def test_empty_symbol_raises() -> None:
    """
    ARRANGE: payload with empty security.ticker
    ACT:     construct IntrinioFeedData
    ASSERT:  raises ValidationError
    """
    raw = {
        "security": {
            "ticker": "",
            "name": "Adobe Inc",
        },
        "last": 550.0,
    }

    with pytest.raises(ValidationError):
        IntrinioFeedData(**raw)


def test_strips_extra_fields() -> None:
    """
    ARRANGE: payload with unexpected extra field
    ACT:     construct IntrinioFeedData
    ASSERT:  extra field is not present on the model
    """
    raw = {
        "security": {
            "ticker": "CRM",
            "name": "Salesforce",
        },
        "last": 200.0,
        "unexpected_field": "ignored",
    }

    actual = IntrinioFeedData(**raw, another_extra="field")

    assert not hasattr(actual, "unexpected_field")


def test_optional_fields_default_to_none() -> None:
    """
    ARRANGE: minimal payload with only required fields
    ACT:     construct IntrinioFeedData
    ASSERT:  optional fields are None
    """
    raw = {
        "security": {
            "ticker": "INTC",
            "name": "Intel Corp",
        },
        "last": 45.0,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.market_cap is None


def test_preserves_whitespace_in_name() -> None:
    """
    ARRANGE: security.name with leading/trailing whitespace
    ACT:     construct IntrinioFeedData
    ASSERT:  whitespace is preserved
    """
    raw = {
        "security": {
            "ticker": "AMD",
            "name": "  Advanced Micro Devices  ",
        },
        "last": 120.0,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.name == "  Advanced Micro Devices  "


def test_missing_security_object_raises() -> None:
    """
    ARRANGE: payload without security object
    ACT:     construct IntrinioFeedData
    ASSERT:  raises ValidationError
    """
    raw = {
        "last": 100.0,
        "marketcap": 50000000000,
    }

    with pytest.raises(ValidationError):
        IntrinioFeedData(**raw)


def test_maps_market_volume() -> None:
    """
    ARRANGE: raw payload with market_volume field
    ACT:     construct IntrinioFeedData
    ASSERT:  market_volume is mapped correctly
    """
    expected_volume = 25000000

    raw = {
        "security": {
            "ticker": "QCOM",
            "name": "Qualcomm",
        },
        "last": 150.0,
        "market_volume": expected_volume,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.market_volume == expected_volume


def test_all_optional_fields_can_be_none() -> None:
    """
    ARRANGE: payload with all optional fields set to None
    ACT:     construct IntrinioFeedData
    ASSERT:  all optional fields are None
    """
    raw = {
        "security": {
            "ticker": "BA",
            "name": "Boeing",
            "currency": None,
        },
        "last": None,
        "marketcap": None,
        "eod_fifty_two_week_low": None,
        "eod_fifty_two_week_high": None,
        "market_volume": None,
        "dividendyield": None,
        "change_percent_365_days": None,
    }

    actual = IntrinioFeedData(**raw)

    assert actual.currency is None
