# feeds/test_stock_analysis_feed_data.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import StockAnalysisFeedData

pytestmark = pytest.mark.unit


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  extra field is not present on the model
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": "123456789",
        "isin": "US1234567890",
    }

    actual = StockAnalysisFeedData(**raw, unexpected="FIELD")

    assert not hasattr(actual, "unexpected")


def test_missing_required_name_raises() -> None:
    """
    ARRANGE: omit required 'n' (name)
    ACT:     construct StockAnalysisFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "s": "FOO",
        "cusip": "123456789",
        "isin": "US1234567890",
    }

    with pytest.raises(ValidationError):
        StockAnalysisFeedData(**incomplete)


def test_missing_required_symbol_raises() -> None:
    """
    ARRANGE: omit required 's' (symbol)
    ACT:     construct StockAnalysisFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "n": "Foo Inc",
        "cusip": "123456789",
        "isin": "US1234567890",
    }

    with pytest.raises(ValidationError):
        StockAnalysisFeedData(**incomplete)


def test_normalises_symbol_field() -> None:
    """
    ARRANGE: raw data with 's' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  's' is mapped to 'symbol'
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": "123456789",
        "isin": "US1234567890",
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.symbol == "FOO"


def test_normalises_name_field() -> None:
    """
    ARRANGE: raw data with 'n' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'n' is mapped to 'name'
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": "123456789",
        "isin": "US1234567890",
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.name == "Foo Inc"


def test_cusip_field_preserved() -> None:
    """
    ARRANGE: raw data with 'cusip' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'cusip' is preserved unchanged
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": "123456789",
        "isin": "US1234567890",
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.cusip == "123456789"


def test_isin_field_preserved() -> None:
    """
    ARRANGE: raw data with 'isin' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'isin' is preserved unchanged
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": "123456789",
        "isin": "US1234567890",
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.isin == "US1234567890"


def test_market_cap_maps_from_market_cap() -> None:
    """
    ARRANGE: raw data with 'marketCap' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'marketCap' is mapped to 'market_cap'
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "marketCap": 1000000,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.market_cap == 1000000  # noqa: PLR2004


def test_last_price_maps_from_price() -> None:
    """
    ARRANGE: raw data with 'price' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'price' is mapped to 'last_price'
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "price": 100.50,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.last_price == 100.50  # noqa: PLR2004


def test_market_volume_maps_from_volume() -> None:
    """
    ARRANGE: raw data with 'volume' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'volume' is mapped to 'market_volume'
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "volume": 5000000,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.market_volume == 5000000  # noqa: PLR2004


def test_trailing_pe_maps_from_pe_ratio() -> None:
    """
    ARRANGE: raw data with 'peRatio' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'peRatio' is mapped to 'trailing_pe'
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "peRatio": 25.5,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.trailing_pe == 25.5  # noqa: PLR2004


def test_sector_field_preserved() -> None:
    """
    ARRANGE: raw data with 'sector' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'sector' is preserved unchanged
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "sector": "Technology",
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.sector == "Technology"


def test_industry_field_preserved() -> None:
    """
    ARRANGE: raw data with 'industry' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'industry' is preserved unchanged
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "industry": "Software",
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.industry == "Software"


def test_revenue_field_preserved() -> None:
    """
    ARRANGE: raw data with 'revenue' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'revenue' is preserved unchanged
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "revenue": 10000000,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.revenue == 10000000  # noqa: PLR2004


def test_free_cash_flow_maps_from_fcf() -> None:
    """
    ARRANGE: raw data with 'fcf' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'fcf' is mapped to 'free_cash_flow'
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "fcf": 5000000,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.free_cash_flow == 5000000  # noqa: PLR2004


def test_return_on_equity_maps_from_roe() -> None:
    """
    ARRANGE: raw data with 'roe' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'roe' is mapped to 'return_on_equity'
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "roe": 15.5,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.return_on_equity == 15.5  # noqa: PLR2004


def test_return_on_assets_maps_from_roa() -> None:
    """
    ARRANGE: raw data with 'roa' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'roa' is mapped to 'return_on_assets'
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "roa": 8.5,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.return_on_assets == 8.5  # noqa: PLR2004


def test_ebitda_field_preserved() -> None:
    """
    ARRANGE: raw data with 'ebitda' field
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'ebitda' is preserved unchanged
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "ebitda": 2000000,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.ebitda == 2000000  # noqa: PLR2004


def test_all_financial_fields_can_be_none() -> None:
    """
    ARRANGE: raw data with all financial fields as None
    ACT:     construct StockAnalysisFeedData
    ASSERT:  all financial fields are preserved as None
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "marketCap": None,
        "price": None,
        "volume": None,
        "peRatio": None,
        "sector": None,
        "industry": None,
        "revenue": None,
        "fcf": None,
        "roe": None,
        "roa": None,
        "ebitda": None,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.market_cap is None


def test_decimal_values_handled() -> None:
    """
    ARRANGE: raw data with Decimal price value
    ACT:     construct StockAnalysisFeedData
    ASSERT:  Decimal is preserved
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "price": Decimal("100.50"),
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.last_price == Decimal("100.50")


def test_omits_unmapped_fields() -> None:
    """
    ARRANGE: raw data includes 'change' field with no RawEquity mapping
    ACT:     construct StockAnalysisFeedData
    ASSERT:  'change' field is not present on the model
    """
    raw = {
        "s": "FOO",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
        "change": 5.5,
    }

    actual = StockAnalysisFeedData(**raw)

    assert not hasattr(actual, "change")


def test_whitespace_in_name_preserved() -> None:
    """
    ARRANGE: raw data with padded name
    ACT:     construct StockAnalysisFeedData
    ASSERT:  whitespace in 'name' is retained
    """
    raw = {
        "s": "FOO",
        "n": "  Padded Name  ",
        "cusip": None,
        "isin": None,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.name == "  Padded Name  "


def test_whitespace_in_symbol_preserved() -> None:
    """
    ARRANGE: raw data with padded symbol
    ACT:     construct StockAnalysisFeedData
    ASSERT:  whitespace in 'symbol' is retained
    """
    raw = {
        "s": "  FOO  ",
        "n": "Foo Inc",
        "cusip": None,
        "isin": None,
    }

    actual = StockAnalysisFeedData(**raw)

    assert actual.symbol == "  FOO  "
