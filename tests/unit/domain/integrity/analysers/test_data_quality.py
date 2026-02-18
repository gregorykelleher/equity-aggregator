# domain/integrity/analysers/test_data_quality.py

import pytest

from equity_aggregator.domain.integrity.analysers.data_quality import (
    analyse_data_quality,
    identity_completeness,
    top_complete_profiles,
    valuation_coverage,
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


def test_identity_completeness_with_equities() -> None:
    """
    ARRANGE: one equity
    ACT:     identity_completeness
    ASSERT:  returns finding about field coverage
    """
    equities = [_equity()]

    actual = identity_completeness(equities)

    assert "coverage" in actual[0].message.lower()


def test_top_complete_profiles_with_equities() -> None:
    """
    ARRANGE: one equity
    ACT:     top_complete_profiles
    ASSERT:  returns finding about most complete profiles
    """
    equities = [_equity()]

    actual = top_complete_profiles(equities, _settings())

    assert "complete" in actual[0].message.lower()


def test_valuation_coverage_with_equities() -> None:
    """
    ARRANGE: one equity
    ACT:     valuation_coverage
    ASSERT:  returns finding about metric coverage
    """
    equities = [_equity(last_price=100.0)]

    actual = valuation_coverage(equities)

    assert "coverage" in actual[0].message.lower()


def test_analyse_data_quality_returns_section() -> None:
    """
    ARRANGE: equities list
    ACT:     analyse_data_quality
    ASSERT:  returns SectionReport with correct title
    """
    equities = [_equity()]

    actual = analyse_data_quality(equities, _settings())

    assert actual.title == "Data Completeness"
