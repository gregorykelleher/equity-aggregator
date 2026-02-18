# domain/integrity/analysers/test_temporal_anomalies.py

import pytest

from equity_aggregator.domain.integrity.analysers.temporal_anomalies import (
    detect_price_below_min,
    detect_range_inversions,
    detect_stale_range_data,
    detect_temporal_anomalies,
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


def test_detect_range_inversions_inverted_range() -> None:
    """
    ARRANGE: equity with min > max
    ACT:     detect_range_inversions
    ASSERT:  returns finding about inversion
    """
    equities = [_equity(fifty_two_week_min=200.0, fifty_two_week_max=100.0)]

    actual = detect_range_inversions(equities, _settings())

    assert "inversions" in actual[0].message


def test_detect_stale_range_data_all_equal() -> None:
    """
    ARRANGE: equity where price = min = max
    ACT:     detect_stale_range_data
    ASSERT:  returns finding about stale data
    """
    equities = [
        _equity(
            last_price=50.0,
            fifty_two_week_min=50.0,
            fifty_two_week_max=50.0,
        ),
    ]

    actual = detect_stale_range_data(equities, _settings())

    assert "stale" in actual[0].message.lower()


def test_detect_price_below_min() -> None:
    """
    ARRANGE: equity with price well below 52-week min
    ACT:     detect_price_below_min
    ASSERT:  returns finding
    """
    equities = [_equity(last_price=10.0, fifty_two_week_min=100.0)]

    actual = detect_price_below_min(equities, _settings())

    assert len(actual) >= 1


def test_detect_temporal_anomalies_returns_section() -> None:
    """
    ARRANGE: equities list
    ACT:     detect_temporal_anomalies
    ASSERT:  returns SectionReport with correct title
    """
    equities = [_equity()]

    actual = detect_temporal_anomalies(equities, _settings())

    assert actual.title == "Price Range Integrity"
