# domain/integrity/analysers/test_extreme_financial_values.py

import pytest

from equity_aggregator.domain.integrity.analysers.extreme_financial_values import (
    detect_extreme_dividends,
    detect_extreme_financial_values,
    detect_negative_price_to_book,
    detect_penny_stocks,
    detect_profit_margin_extremes,
    detect_round_price_clusters,
)
from equity_aggregator.domain.integrity.models import AnalysisSettings, default_settings
from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity

pytestmark = pytest.mark.unit


def _equity(**overrides: object) -> CanonicalEquity:
    remaining = dict(overrides)
    identity = {
        "name": remaining.pop("name", "TEST CO"),
        "symbol": remaining.pop("symbol", "TST"),
        "share_class_figi": remaining.pop(
            "figi", remaining.pop("share_class_figi", "BBG000ANA001")
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


def _settings() -> AnalysisSettings:
    return default_settings()


def test_detect_extreme_dividends() -> None:
    """
    ARRANGE: equity with very high dividend yield
    ACT:     detect_extreme_dividends
    ASSERT:  returns finding about extreme yield
    """
    equities = [_equity(dividend_yield=20.0)]

    actual = detect_extreme_dividends(equities, _settings())

    assert len(actual) >= 1


def test_detect_penny_stocks() -> None:
    """
    ARRANGE: equity with price below 1 cent
    ACT:     detect_penny_stocks
    ASSERT:  returns finding about penny stocks
    """
    equities = [_equity(last_price=0.005)]

    actual = detect_penny_stocks(equities, _settings())

    assert len(actual) >= 1


def test_detect_profit_margin_extremes() -> None:
    """
    ARRANGE: equity with profit margin above 100%
    ACT:     detect_profit_margin_extremes
    ASSERT:  returns finding
    """
    equities = [_equity(profit_margin=200.0)]

    actual = detect_profit_margin_extremes(equities, _settings())

    assert len(actual) >= 1


def test_detect_negative_price_to_book() -> None:
    """
    ARRANGE: equity with negative P/B ratio
    ACT:     detect_negative_price_to_book
    ASSERT:  returns finding
    """
    equities = [_equity(price_to_book=-2.0)]

    actual = detect_negative_price_to_book(equities, _settings())

    assert len(actual) >= 1


def test_detect_round_price_clusters() -> None:
    """
    ARRANGE: equity with round dollar price
    ACT:     detect_round_price_clusters
    ASSERT:  returns finding
    """
    equities = [_equity(last_price=100.0)]

    actual = detect_round_price_clusters(equities, _settings())

    assert len(actual) >= 1


def test_detect_extreme_financial_values_returns_section() -> None:
    """
    ARRANGE: equities list
    ACT:     detect_extreme_financial_values
    ASSERT:  returns SectionReport with correct title
    """
    equities = [_equity()]

    actual = detect_extreme_financial_values(equities, _settings())

    assert actual.title == "Extreme Financial Values"


def test_detect_round_price_clusters_low_ratio() -> None:
    """
    ARRANGE: many non-round prices with one round price (ratio below threshold)
    ACT:     detect_round_price_clusters
    ASSERT:  returns finding without concentration warning
    """
    equities = [_equity(figi=f"BBG0NR{i:06}", last_price=50.25 + i) for i in range(20)]
    equities.append(_equity(figi="BBG0ROUND001", last_price=100.0))

    actual = detect_round_price_clusters(equities, _settings())

    assert actual[0].highlights == ()
