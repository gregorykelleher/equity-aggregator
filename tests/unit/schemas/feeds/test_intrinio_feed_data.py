# feeds/test_intrinio_feed_data.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import IntrinioFeedData

pytestmark = pytest.mark.unit


def test_maps_ticker_to_symbol() -> None:
    """
    ARRANGE: raw payload with ticker field
    ACT:     construct IntrinioFeedData
    ASSERT:  ticker is mapped to symbol
    """
    raw = {
        "name": "Microsoft",
        "ticker": "MSFT",
        "quote": {"last": 300.0},
    }

    actual = IntrinioFeedData(**raw)

    assert actual.symbol == "MSFT"


def test_share_class_figi_preserved() -> None:
    """
    ARRANGE: raw payload with share_class_figi
    ACT:     construct IntrinioFeedData
    ASSERT:  share_class_figi is preserved
    """
    raw = {
        "name": "Alphabet Inc",
        "ticker": "GOOGL",
        "share_class_figi": "BBG001S5N8V8",
        "quote": {"last": 100.0},
    }

    actual = IntrinioFeedData(**raw)

    assert actual.share_class_figi == "BBG001S5N8V8"


def test_maps_last_to_last_price() -> None:
    """
    ARRANGE: raw payload with quote.last field
    ACT:     construct IntrinioFeedData
    ASSERT:  last is mapped to last_price
    """
    expected_price = 250.50

    raw = {
        "name": "Tesla Inc",
        "ticker": "TSLA",
        "quote": {"last": expected_price},
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
        "name": "NVIDIA Corp",
        "ticker": "NVDA",
        "quote": {
            "last": 500.0,
            "eod_fifty_two_week_low": expected_low,
            "eod_fifty_two_week_high": 600.0,
        },
    }

    actual = IntrinioFeedData(**raw)

    assert actual.fifty_two_week_min == expected_low


def test_maps_marketcap_to_market_cap() -> None:
    """
    ARRANGE: raw payload with quote.marketcap field
    ACT:     construct IntrinioFeedData
    ASSERT:  marketcap is mapped to market_cap
    """
    expected_market_cap = 900000000000

    raw = {
        "name": "Meta Platforms",
        "ticker": "META",
        "quote": {
            "last": 350.0,
            "marketcap": expected_market_cap,
        },
    }

    actual = IntrinioFeedData(**raw)

    assert actual.market_cap == expected_market_cap


def test_maps_dividendyield_to_dividend_yield() -> None:
    """
    ARRANGE: raw payload with quote.dividendyield field
    ACT:     construct IntrinioFeedData
    ASSERT:  dividendyield is mapped to dividend_yield
    """
    expected_yield = 3.2

    raw = {
        "name": "Coca-Cola",
        "ticker": "KO",
        "quote": {
            "last": 60.0,
            "dividendyield": expected_yield,
        },
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
        "name": "Amazon",
        "ticker": "AMZN",
        "quote": {
            "last": 140.0,
            "change_percent_365_days": 25.5,
        },
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
        "name": "Netflix",
        "ticker": "NFLX",
        "quote": {
            "last": 450.0,
            "change_percent_365_days": None,
        },
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
        "name": "Disney",
        "ticker": "DIS",
        "quote": {
            "last": 90.0,
            "change_percent_365_days": "15.75",
        },
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
        "name": "PayPal",
        "ticker": "PYPL",
        "quote": {
            "last": 60.0,
            "change_percent_365_days": -12.5,
        },
    }

    actual = IntrinioFeedData(**raw)

    assert actual.performance_1_year == Decimal("-0.125")


def test_percentage_conversion_handles_invalid_input() -> None:
    """
    ARRANGE: raw payload with change_percent_365_days raising ValueError on str()
    ACT:     construct IntrinioFeedData
    ASSERT:  performance_1_year falls back to None
    """

    class BadPercent:
        def __str__(self) -> str:
            raise ValueError("unparseable percent")

    raw = {
        "name": "SAP SE",
        "ticker": "SAP",
        "quote": {
            "last": 135.0,
            "change_percent_365_days": BadPercent(),
        },
    }

    actual = IntrinioFeedData(**raw)

    assert actual.performance_1_year is None


def test_accepts_various_numeric_types() -> None:
    """
    ARRANGE: last_price with various numeric types
    ACT:     construct IntrinioFeedData for each type
    ASSERT:  values are preserved
    """
    for price in (100, 100.50, "100.50", Decimal("100.50")):
        raw = {
            "name": "IBM Corp",
            "ticker": "IBM",
            "quote": {"last": price},
        }

        actual = IntrinioFeedData(**raw)

        assert actual.last_price == price


def test_missing_required_name_raises() -> None:
    """
    ARRANGE: payload missing name
    ACT:     construct IntrinioFeedData
    ASSERT:  raises ValidationError
    """
    raw = {
        "ticker": "ORCL",
        "quote": {"last": 110.0},
    }

    with pytest.raises(ValidationError):
        IntrinioFeedData(**raw)


def test_missing_required_symbol_raises() -> None:
    """
    ARRANGE: payload missing ticker
    ACT:     construct IntrinioFeedData
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "Oracle Corp",
        "quote": {"last": 110.0},
    }

    with pytest.raises(ValidationError):
        IntrinioFeedData(**raw)


def test_empty_name_raises() -> None:
    """
    ARRANGE: payload with empty name
    ACT:     construct IntrinioFeedData
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "",
        "ticker": "ADBE",
        "quote": {"last": 550.0},
    }

    with pytest.raises(ValidationError):
        IntrinioFeedData(**raw)


def test_empty_symbol_raises() -> None:
    """
    ARRANGE: payload with empty ticker
    ACT:     construct IntrinioFeedData
    ASSERT:  raises ValidationError
    """
    raw = {
        "name": "Adobe Inc",
        "ticker": "",
        "quote": {"last": 550.0},
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
        "name": "Salesforce",
        "ticker": "CRM",
        "quote": {"last": 200.0},
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
        "name": "Intel Corp",
        "ticker": "INTC",
        "quote": {"last": 45.0},
    }

    actual = IntrinioFeedData(**raw)

    assert actual.market_cap is None


def test_preserves_whitespace_in_name() -> None:
    """
    ARRANGE: name with leading/trailing whitespace
    ACT:     construct IntrinioFeedData
    ASSERT:  whitespace is preserved
    """
    raw = {
        "name": "  Advanced Micro Devices  ",
        "ticker": "AMD",
        "quote": {"last": 120.0},
    }

    actual = IntrinioFeedData(**raw)

    assert actual.name == "  Advanced Micro Devices  "


def test_missing_quote_object_uses_defaults() -> None:
    """
    ARRANGE: payload without quote object
    ACT:     construct IntrinioFeedData
    ASSERT:  quote-derived fields are None
    """
    raw = {
        "name": "Test Corp",
        "ticker": "TEST",
    }

    actual = IntrinioFeedData(**raw)

    assert actual.last_price is None


def test_maps_market_volume() -> None:
    """
    ARRANGE: raw payload with quote.market_volume field
    ACT:     construct IntrinioFeedData
    ASSERT:  market_volume is mapped correctly
    """
    expected_volume = 25000000

    raw = {
        "name": "Qualcomm",
        "ticker": "QCOM",
        "quote": {
            "last": 150.0,
            "market_volume": expected_volume,
        },
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
        "name": "Boeing",
        "ticker": "BA",
        "currency": None,
        "quote": {
            "last": None,
            "marketcap": None,
            "eod_fifty_two_week_low": None,
            "eod_fifty_two_week_high": None,
            "market_volume": None,
            "dividendyield": None,
            "change_percent_365_days": None,
        },
    }

    actual = IntrinioFeedData(**raw)

    assert actual.currency is None


def test_preserves_cik() -> None:
    """
    ARRANGE: payload with CIK (already zero-padded from API)
    ACT:     construct IntrinioFeedData
    ASSERT:  cik is preserved as-is
    """
    raw = {
        "name": "Apple Inc",
        "ticker": "AAPL",
        "cik": "0000320193",
        "quote": {"last": 150.0},
    }

    actual = IntrinioFeedData(**raw)

    assert actual.cik == "0000320193"


def test_maps_exchange_mic_to_mics_list() -> None:
    """
    ARRANGE: payload with exchange_mic field
    ACT:     construct IntrinioFeedData
    ASSERT:  exchange_mic is mapped to mics list
    """
    raw = {
        "name": "Apple Inc",
        "ticker": "AAPL",
        "exchange_mic": "XNAS",
        "quote": {"last": 150.0},
    }

    actual = IntrinioFeedData(**raw)

    assert actual.mics == ["XNAS"]


def test_preserves_lei() -> None:
    """
    ARRANGE: payload with LEI from company data
    ACT:     construct IntrinioFeedData
    ASSERT:  lei is preserved as-is
    """
    raw = {
        "name": "Apple Inc",
        "ticker": "AAPL",
        "lei": "HWUPKR0MPOU8FGXBT394",
        "quote": {"last": 150.0},
    }

    actual = IntrinioFeedData(**raw)

    assert actual.lei == "HWUPKR0MPOU8FGXBT394"


def test_lei_defaults_to_none() -> None:
    """
    ARRANGE: payload without lei field
    ACT:     construct IntrinioFeedData
    ASSERT:  lei is None
    """
    raw = {
        "name": "Test Corp",
        "ticker": "TEST",
        "quote": {"last": 100.0},
    }

    actual = IntrinioFeedData(**raw)

    assert actual.lei is None

