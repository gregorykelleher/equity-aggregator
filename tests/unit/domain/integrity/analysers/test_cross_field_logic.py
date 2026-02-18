# domain/integrity/analysers/test_cross_field_logic.py

import pytest

from equity_aggregator.domain.integrity.analysers.cross_field_logic import (
    analyse_cross_field_logic,
    detect_cap_without_price,
    detect_missing_price_and_cap,
    detect_partial_range,
    detect_price_without_cap,
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


def test_detect_price_without_cap_finding() -> None:
    """
    ARRANGE: equity with price but no market cap
    ACT:     detect_price_without_cap
    ASSERT:  returns at least one finding
    """
    equities = [_equity(last_price=50.0)]

    actual = detect_price_without_cap(equities, _settings())

    assert len(actual) >= 1


def test_detect_cap_without_price_finding() -> None:
    """
    ARRANGE: equity with market cap but no price
    ACT:     detect_cap_without_price
    ASSERT:  returns finding
    """
    equities = [_equity(market_cap=5000000.0)]

    actual = detect_cap_without_price(equities, _settings())

    assert len(actual) >= 1


def test_detect_missing_price_and_cap() -> None:
    """
    ARRANGE: equity with neither price nor cap
    ACT:     detect_missing_price_and_cap
    ASSERT:  returns finding
    """
    equities = [_equity()]

    actual = detect_missing_price_and_cap(equities)

    assert len(actual) >= 1


def test_detect_partial_range_finding() -> None:
    """
    ARRANGE: equity with only 52-week min
    ACT:     detect_partial_range
    ASSERT:  returns finding about partial range
    """
    equities = [_equity(fifty_two_week_min=50.0)]

    actual = detect_partial_range(equities, _settings())

    assert len(actual) >= 1


def test_analyse_cross_field_logic_returns_section() -> None:
    """
    ARRANGE: equities list
    ACT:     analyse_cross_field_logic
    ASSERT:  returns SectionReport with correct title
    """
    equities = [_equity()]

    actual = analyse_cross_field_logic(equities, _settings())

    assert actual.title == "Cross-field Logic Consistency"
