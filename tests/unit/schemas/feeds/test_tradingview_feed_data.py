# feeds/test_tradingview_feed_data.py

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import TradingViewFeedData
from equity_aggregator.schemas.feeds.tradingview_feed_data import _extract_field

pytestmark = pytest.mark.unit


def test_extract_field_returns_value_at_index() -> None:
    """
    ARRANGE: data array with values
    ACT:     extract field at index 0
    ASSERT:  returns value at that index
    """
    data = ["AAPL", "Apple Inc.", "NYSE"]

    actual = _extract_field(data, 0)

    assert actual == "AAPL"


def test_extract_field_returns_none_for_missing_array() -> None:
    """
    ARRANGE: None data array
    ACT:     extract field
    ASSERT:  returns None
    """
    actual = _extract_field(None, 0)

    assert actual is None


def test_extract_field_returns_none_for_out_of_bounds() -> None:
    """
    ARRANGE: short data array
    ACT:     extract field beyond array length
    ASSERT:  returns None
    """
    data = ["AAPL"]

    actual = _extract_field(data, 5)

    assert actual is None


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct TradingViewFeedData
    ASSERT:  extra field is not present on the model
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 18,
    }

    actual = TradingViewFeedData(**raw, unexpected="FIELD")

    assert not hasattr(actual, "unexpected")


def test_missing_required_name_raises() -> None:
    """
    ARRANGE: data array with None name
    ACT:     construct TradingViewFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", None] + [None] * 18,
    }

    with pytest.raises(ValidationError):
        TradingViewFeedData(**incomplete)


def test_missing_required_symbol_raises() -> None:
    """
    ARRANGE: data array with None symbol
    ACT:     construct TradingViewFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "s": "NYSE:AAPL",
        "d": [None, "Apple Inc."] + [None] * 18,
    }

    with pytest.raises(ValidationError):
        TradingViewFeedData(**incomplete)


def test_normalises_symbol_field() -> None:
    """
    ARRANGE: raw data with symbol in d[0]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[0] is mapped to 'symbol'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 18,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.symbol == "AAPL"


def test_normalises_name_field() -> None:
    """
    ARRANGE: raw data with name in d[1]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[1] is mapped to 'name'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 18,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.name == "Apple Inc."


def test_currency_field_preserved() -> None:
    """
    ARRANGE: raw data with currency in d[3]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[3] is mapped to 'currency'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc.", "NYSE", "USD"] + [None] * 16,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.currency == "USD"


def test_last_price_maps_from_close() -> None:
    """
    ARRANGE: raw data with close price in d[4]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[4] is mapped to 'last_price'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc.", None, None, 150.50] + [None] * 15,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.last_price == 150.50  # noqa: PLR2004


def test_market_cap_maps_from_market_cap_basic() -> None:
    """
    ARRANGE: raw data with market cap in d[5]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[5] is mapped to 'market_cap'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 3 + [2500000000000] + [None] * 14,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.market_cap == 2500000000000  # noqa: PLR2004


def test_market_volume_maps_from_volume() -> None:
    """
    ARRANGE: raw data with volume in d[6]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[6] is mapped to 'market_volume'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 4 + [50000000] + [None] * 13,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.market_volume == 50000000  # noqa: PLR2004


def test_dividend_yield_maps_from_dividends_yield_current() -> None:
    """
    ARRANGE: raw data with dividend yield in d[7]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[7] is mapped to 'dividend_yield'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 5 + [0.005] + [None] * 12,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.dividend_yield == Decimal("0.005")


def test_shares_outstanding_maps_from_total_shares() -> None:
    """
    ARRANGE: raw data with total shares in d[9]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[9] is mapped to 'shares_outstanding'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 7 + [16000000000] + [None] * 10,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.shares_outstanding == 16000000000  # noqa: PLR2004


def test_revenue_maps_from_total_revenue_ttm() -> None:
    """
    ARRANGE: raw data with revenue in d[10]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[10] is mapped to 'revenue'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 8 + [500000000000] + [None] * 9,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.revenue == 500000000000  # noqa: PLR2004


def test_ebitda_maps_from_ebitda_ttm() -> None:
    """
    ARRANGE: raw data with EBITDA in d[11]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[11] is mapped to 'ebitda'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 9 + [100000000000] + [None] * 8,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.ebitda == 100000000000  # noqa: PLR2004


def test_trailing_pe_maps_from_price_earnings_ttm() -> None:
    """
    ARRANGE: raw data with P/E ratio in d[12]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[12] is mapped to 'trailing_pe'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 10 + [25.5] + [None] * 7,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.trailing_pe == 25.5  # noqa: PLR2004


def test_price_to_book_maps_from_price_book_fq() -> None:
    """
    ARRANGE: raw data with price-to-book in d[13]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[13] is mapped to 'price_to_book'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 11 + [5.5] + [None] * 6,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.price_to_book == 5.5  # noqa: PLR2004


def test_trailing_eps_maps_from_earnings_per_share() -> None:
    """
    ARRANGE: raw data with EPS in d[14]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[14] is mapped to 'trailing_eps'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 12 + [6.5] + [None] * 5,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.trailing_eps == 6.5  # noqa: PLR2004


def test_return_on_equity_converted_from_percentage() -> None:
    """
    ARRANGE: raw data with ROE as percentage (15.5) in d[15]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[15] is converted to decimal (0.155)
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 13 + [15.5] + [None] * 4,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.return_on_equity == Decimal("0.155")


def test_return_on_assets_converted_from_percentage() -> None:
    """
    ARRANGE: raw data with ROA as percentage (8.5) in d[16]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[16] is converted to decimal (0.085)
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 14 + [8.5] + [None] * 3,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.return_on_assets == Decimal("0.085")


def test_sector_field_preserved() -> None:
    """
    ARRANGE: raw data with sector in d[17]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[17] is mapped to 'sector'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 15 + ["Technology", None, None],
    }

    actual = TradingViewFeedData(**raw)

    assert actual.sector == "Technology"


def test_industry_field_preserved() -> None:
    """
    ARRANGE: raw data with industry in d[18]
    ACT:     construct TradingViewFeedData
    ASSERT:  d[18] is mapped to 'industry'
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 16 + ["Consumer Electronics", None],
    }

    actual = TradingViewFeedData(**raw)

    assert actual.industry == "Consumer Electronics"


def test_all_financial_fields_can_be_none() -> None:
    """
    ARRANGE: raw data with all financial fields as None
    ACT:     construct TradingViewFeedData
    ASSERT:  all financial fields are preserved as None
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 18,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.currency is None


def test_decimal_values_handled() -> None:
    """
    ARRANGE: raw data with Decimal price value
    ACT:     construct TradingViewFeedData
    ASSERT:  Decimal is preserved
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc.", None, None, Decimal("150.50")] + [None] * 15,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.last_price == Decimal("150.50")


def test_omits_unmapped_fields() -> None:
    """
    ARRANGE: raw data includes 's' field with no direct mapping
    ACT:     construct TradingViewFeedData
    ASSERT:  's' field is not present on the model
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 18,
    }

    actual = TradingViewFeedData(**raw)

    assert not hasattr(actual, "s")


def test_whitespace_in_name_preserved() -> None:
    """
    ARRANGE: raw data with padded name
    ACT:     construct TradingViewFeedData
    ASSERT:  whitespace in 'name' is retained
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "  Padded Name  "] + [None] * 18,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.name == "  Padded Name  "


def test_whitespace_in_symbol_preserved() -> None:
    """
    ARRANGE: raw data with padded symbol
    ACT:     construct TradingViewFeedData
    ASSERT:  whitespace in 'symbol' is retained
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": ["  AAPL  ", "Apple Inc."] + [None] * 18,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.symbol == "  AAPL  "


def test_handles_missing_data_array() -> None:
    """
    ARRANGE: raw data without 'd' key
    ACT:     construct TradingViewFeedData
    ASSERT:  raises ValidationError
    """
    raw = {
        "s": "NYSE:AAPL",
    }

    with pytest.raises(ValidationError):
        TradingViewFeedData(**raw)


def test_handles_empty_data_array() -> None:
    """
    ARRANGE: raw data with empty 'd' array
    ACT:     construct TradingViewFeedData
    ASSERT:  raises ValidationError (missing required fields)
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": [],
    }

    with pytest.raises(ValidationError):
        TradingViewFeedData(**raw)


def test_all_fields_populated() -> None:
    """
    ARRANGE: raw data with all fields populated
    ACT:     construct TradingViewFeedData
    ASSERT:  all fields are correctly mapped
    """
    raw = {
        "s": "NYSE:AAPL",
        "d": [
            "AAPL",  # symbol
            "Apple Inc.",  # name
            "NYSE",  # exchange (not mapped)
            "USD",  # currency
            150.0,  # last_price
            2500000000000,  # market_cap
            50000000,  # market_volume
            0.005,  # dividend_yield
            15000000000,  # float_shares (not mapped)
            16000000000,  # shares_outstanding
            500000000000,  # revenue
            100000000000,  # ebitda
            25.5,  # trailing_pe
            5.5,  # price_to_book
            6.5,  # trailing_eps
            15.5,  # return_on_equity (percentage)
            8.5,  # return_on_assets (percentage)
            "Technology",  # sector
            "Consumer Electronics",  # industry
            1700000000,  # last-price-update-time (Unix timestamp)
        ],
    }

    actual = TradingViewFeedData(**raw)

    assert actual.symbol == "AAPL"


def test_stale_trade_nullifies_price_fields() -> None:
    """
    ARRANGE: payload with last-price-update-time 48h ago (exceeds 36h default)
    ACT:     construct TradingViewFeedData
    ASSERT:  last_price is nullified
    """
    hours_ago = 48
    stale_time = datetime.now(UTC) - timedelta(hours=hours_ago)

    raw = {
        "s": "NYSE:STALE",
        "d": ["STALE", "Stale Corp", None, "USD", 100.0]
        + [None] * 14
        + [stale_time.timestamp()],
    }

    actual = TradingViewFeedData(**raw)

    assert actual.last_price is None


def test_stale_trade_preserves_identity_fields() -> None:
    """
    ARRANGE: payload with last-price-update-time 48h ago (exceeds 36h default)
    ACT:     construct TradingViewFeedData
    ASSERT:  non-price fields are preserved
    """
    hours_ago = 48
    stale_time = datetime.now(UTC) - timedelta(hours=hours_ago)

    raw = {
        "s": "NYSE:STALE",
        "d": ["STALE", "Stale Corp", None, "USD", 100.0]
        + [None] * 14
        + [stale_time.timestamp()],
    }

    actual = TradingViewFeedData(**raw)

    assert actual.symbol == "STALE"


def test_fresh_trade_preserves_price_fields() -> None:
    """
    ARRANGE: payload with last-price-update-time just now (within 36h default)
    ACT:     construct TradingViewFeedData
    ASSERT:  last_price is preserved
    """
    expected_price = 250.0
    fresh_time = datetime.now(UTC)

    raw = {
        "s": "NYSE:FRESH",
        "d": ["FRESH", "Fresh Corp", None, "USD", expected_price]
        + [None] * 14
        + [fresh_time.timestamp()],
    }

    actual = TradingViewFeedData(**raw)

    assert actual.last_price == expected_price


def test_missing_last_time_preserves_price_fields() -> None:
    """
    ARRANGE: payload without last-price-update-time (None at d[19])
    ACT:     construct TradingViewFeedData
    ASSERT:  last_price is preserved (fail-open)
    """
    expected_price = 300.0

    raw = {
        "s": "NYSE:NOTM",
        "d": ["NOTM", "No Time Corp", None, "USD", expected_price] + [None] * 15,
    }

    actual = TradingViewFeedData(**raw)

    assert actual.last_price == expected_price


def test_malformed_last_time_preserves_price_fields() -> None:
    """
    ARRANGE: payload with non-numeric last-price-update-time
    ACT:     construct TradingViewFeedData
    ASSERT:  last_price is preserved (fail-open)
    """
    expected_price = 150.0

    raw = {
        "s": "NYSE:BADTM",
        "d": ["BADTM", "Bad Time Corp", None, "USD", expected_price]
        + [None] * 14
        + ["not-a-timestamp"],
    }

    actual = TradingViewFeedData(**raw)

    assert actual.last_price == expected_price
