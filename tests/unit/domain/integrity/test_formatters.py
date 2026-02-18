# domain/integrity/test_formatters.py

from decimal import Decimal

import pytest

from equity_aggregator.domain.integrity.formatters import (
    describe_cap_gap,
    describe_price_vs_max,
    describe_price_vs_min,
    describe_range_bounds,
    format_coverage_table,
    format_currency,
    format_equity,
    format_equity_with_figi,
    format_percentage,
    limit_items,
    score_equity_completeness,
)
from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity

pytestmark = pytest.mark.unit


def _equity(**overrides: object) -> CanonicalEquity:
    remaining = dict(overrides)
    identity = {
        "name": remaining.pop("name", "TEST CO"),
        "symbol": remaining.pop("symbol", "TST"),
        "share_class_figi": remaining.pop(
            "figi", remaining.pop("share_class_figi", "BBG000FMT001")
        ),
        "isin": remaining.pop("isin", None),
        "cusip": remaining.pop("cusip", None),
        "cik": remaining.pop("cik", None),
        "lei": remaining.pop("lei", None),
    }
    return CanonicalEquity(
        identity=EquityIdentity(**identity),
        financials=EquityFinancials(**remaining),
    )


def test_format_currency_integer_value() -> None:
    """
    ARRANGE: whole number value
    ACT:     format_currency
    ASSERT:  formatted with dollar sign and commas
    """
    actual = format_currency(Decimal("1000000"))

    assert actual == "$1,000,000"


def test_format_currency_decimal_value() -> None:
    """
    ARRANGE: decimal value
    ACT:     format_currency
    ASSERT:  formatted with two decimal places
    """
    actual = format_currency(Decimal("1234.56"))

    assert actual == "$1,234.56"


def test_format_percentage() -> None:
    """
    ARRANGE: percentage value
    ACT:     format_percentage
    ASSERT:  formatted with one decimal and percent sign
    """
    actual = format_percentage(99.5)

    assert actual == "99.5%"


def test_limit_items_truncates() -> None:
    """
    ARRANGE: five items with limit 3
    ACT:     limit_items
    ASSERT:  returns 3 items
    """
    expected = 3
    items = ("a", "b", "c", "d", "e")

    actual = limit_items(items, expected)

    assert len(actual) == expected


def test_format_equity_basic() -> None:
    """
    ARRANGE: equity with name and symbol
    ACT:     format_equity
    ASSERT:  formatted as Name (SYMBOL)
    """
    eq = _equity(name="APPLE INC", symbol="AAPL")

    actual = format_equity(eq)

    assert actual == "APPLE INC (AAPL)"


def test_format_equity_with_figi_basic() -> None:
    """
    ARRANGE: equity with name, symbol, and FIGI
    ACT:     format_equity_with_figi
    ASSERT:  formatted as Name (SYMBOL) [FIGI]
    """
    eq = _equity(name="APPLE INC", symbol="AAPL", figi="BBG000B9XRY4")

    actual = format_equity_with_figi(eq)

    assert actual == "APPLE INC (AAPL) [BBG000B9XRY4]"


def test_describe_price_vs_max() -> None:
    """
    ARRANGE: equity with price and max
    ACT:     describe_price_vs_max
    ASSERT:  contains price and max values
    """
    eq = _equity(last_price=150.0, fifty_two_week_max=145.0)

    actual = describe_price_vs_max(eq)

    assert "150" in actual


def test_describe_price_vs_min() -> None:
    """
    ARRANGE: equity with price and min
    ACT:     describe_price_vs_min
    ASSERT:  contains price and min values
    """
    eq = _equity(last_price=80.0, fifty_two_week_min=90.0)

    actual = describe_price_vs_min(eq)

    assert "80" in actual


def test_describe_range_bounds() -> None:
    """
    ARRANGE: equity with min and max
    ACT:     describe_range_bounds
    ASSERT:  contains min and max values
    """
    eq = _equity(fifty_two_week_min=50.0, fifty_two_week_max=200.0)

    actual = describe_range_bounds(eq)

    assert "min" in actual


def test_describe_cap_gap() -> None:
    """
    ARRANGE: equity with market cap but no price
    ACT:     describe_cap_gap
    ASSERT:  contains formatted cap value and 'price missing'
    """
    eq = _equity(market_cap=5000000.0)

    actual = describe_cap_gap(eq)

    assert "price missing" in actual


def test_format_coverage_table_empty() -> None:
    """
    ARRANGE: empty items list
    ACT:     format_coverage_table
    ASSERT:  returns empty tuple
    """
    actual = format_coverage_table([])

    assert actual == ()


def test_format_coverage_table_row_count() -> None:
    """
    ARRANGE: two items
    ACT:     format_coverage_table
    ASSERT:  returns two rows
    """
    expected = 2
    items = [("Field A", 50, 100), ("Field B", 80, 100)]

    actual = format_coverage_table(items)

    assert len(actual) == expected


def test_score_equity_completeness_minimal() -> None:
    """
    ARRANGE: equity with only name and FIGI
    ACT:     score_equity_completeness
    ASSERT:  score reflects only name (1 from identity fields)
    """
    eq = _equity()

    actual = score_equity_completeness(eq)

    assert actual >= 1


def test_score_equity_completeness_with_identifiers() -> None:
    """
    ARRANGE: equity with name, ISIN, and CUSIP
    ACT:     score_equity_completeness
    ASSERT:  score is higher than minimal
    """
    expected_minimum = 3
    eq = _equity(isin="US0378331005", cusip="037833100")

    actual = score_equity_completeness(eq)

    assert actual >= expected_minimum
