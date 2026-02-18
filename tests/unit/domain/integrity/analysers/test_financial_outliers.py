# domain/integrity/analysers/test_financial_outliers.py

import pytest

from equity_aggregator.domain.integrity.analysers.financial_outliers import (
    analyse_financial_outliers,
    compute_market_cap_findings,
    compute_negative_metric_findings,
    compute_pe_findings,
    compute_price_range_findings,
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


def test_compute_pe_findings_insufficient_sample() -> None:
    """
    ARRANGE: fewer equities than min_sample_size
    ACT:     compute_pe_findings
    ASSERT:  returns empty tuple
    """
    equities = [_equity(trailing_pe=15.0)]

    actual = compute_pe_findings(equities, _settings())

    assert actual == ()


def test_compute_pe_findings_sufficient_sample() -> None:
    """
    ARRANGE: 15 equities with positive P/E
    ACT:     compute_pe_findings
    ASSERT:  returns at least one finding
    """
    equities = [_equity(figi=f"BBG0PE{i:06}", trailing_pe=20.0) for i in range(15)]

    actual = compute_pe_findings(equities, _settings())

    assert len(actual) >= 1


def test_compute_market_cap_findings_sufficient_sample() -> None:
    """
    ARRANGE: 15 equities with market caps
    ACT:     compute_market_cap_findings
    ASSERT:  returns at least one finding
    """
    equities = [
        _equity(figi=f"BBG0MC{i:06}", market_cap=float(i * 1_000_000))
        for i in range(1, 16)
    ]

    actual = compute_market_cap_findings(equities, _settings())

    assert len(actual) >= 1


def test_compute_negative_metric_findings_negative_pe() -> None:
    """
    ARRANGE: equity with negative P/E
    ACT:     compute_negative_metric_findings
    ASSERT:  returns finding about negative P/E
    """
    equities = [_equity(trailing_pe=-5.0)]

    actual = compute_negative_metric_findings(equities, _settings())

    assert "Negative P/E" in actual[0].message


def test_compute_negative_metric_findings_zero_cap() -> None:
    """
    ARRANGE: equity with zero market cap
    ACT:     compute_negative_metric_findings
    ASSERT:  returns finding about zero market cap
    """
    equities = [_equity(market_cap=0.0)]

    actual = compute_negative_metric_findings(equities, _settings())

    assert "Zero or negative" in actual[0].message


def test_compute_price_range_findings_price_exceeds_max() -> None:
    """
    ARRANGE: equity with price well above 52-week max
    ACT:     compute_price_range_findings
    ASSERT:  returns finding about exceeding max
    """
    equities = [
        _equity(
            last_price=200.0,
            fifty_two_week_max=100.0,
            fifty_two_week_min=50.0,
        ),
    ]

    actual = compute_price_range_findings(equities, _settings())

    assert len(actual) >= 1


def test_analyse_financial_outliers_returns_section() -> None:
    """
    ARRANGE: equities with financial data
    ACT:     analyse_financial_outliers
    ASSERT:  returns SectionReport with correct title
    """
    equities = [_equity()]

    actual = analyse_financial_outliers(equities, _settings())

    assert actual.title == "Financial Metric Outliers"


def test_compute_pe_findings_includes_deviation_highlight() -> None:
    """
    ARRANGE: 15 equities with varied P/E producing non-zero std deviation
    ACT:     compute_pe_findings
    ASSERT:  highlights include std deviation line
    """
    equities = [
        _equity(figi=f"BBG0PE{i:06}", trailing_pe=float(10 + i * 5)) for i in range(15)
    ]

    actual = compute_pe_findings(equities, _settings())

    assert "Std deviation" in actual[0].highlights[2]


def test_compute_pe_findings_includes_outlier_highlights() -> None:
    """
    ARRANGE: 14 equities with normal P/E and 1 extreme outlier
    ACT:     compute_pe_findings
    ASSERT:  highlights mention extreme ratios
    """
    equities = [_equity(figi=f"BBG0PE{i:06}", trailing_pe=15.0) for i in range(14)]
    equities.append(_equity(figi="BBG0PE000014", trailing_pe=9999.0))

    actual = compute_pe_findings(equities, _settings())

    assert any("Extreme ratios" in h for h in actual[0].highlights)


def test_compute_market_cap_findings_with_mega_cap() -> None:
    """
    ARRANGE: 15 equities including one above mega cap threshold
    ACT:     compute_market_cap_findings
    ASSERT:  highlights include largest market cap line
    """
    mega_cap_value = 300_000_000_000.0
    equities = [
        _equity(figi=f"BBG0MC{i:06}", market_cap=float(i * 1_000_000))
        for i in range(1, 15)
    ]
    equities.append(_equity(figi="BBG0MC000015", market_cap=mega_cap_value))

    actual = compute_market_cap_findings(equities, _settings())

    assert any("Largest market cap" in h for h in actual[0].highlights)
