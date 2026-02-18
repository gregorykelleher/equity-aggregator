# domain/integrity/analysers/test_identifier_quality.py

import pytest

from equity_aggregator.domain.integrity.analysers.identifier_quality import (
    analyse_identifier_quality,
    missing_identifier_counts,
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


def test_missing_identifier_counts_empty() -> None:
    """
    ARRANGE: empty list
    ACT:     missing_identifier_counts
    ASSERT:  returns empty tuple
    """
    actual = missing_identifier_counts([], _settings())

    assert actual == ()


def test_missing_identifier_counts_with_equities() -> None:
    """
    ARRANGE: equity without ISIN
    ACT:     missing_identifier_counts
    ASSERT:  returns finding about identifier gaps
    """
    equities = [_equity()]

    actual = missing_identifier_counts(equities, _settings())

    assert "Identifier coverage" in actual[0].message


def test_analyse_identifier_quality_returns_section() -> None:
    """
    ARRANGE: equities list
    ACT:     analyse_identifier_quality
    ASSERT:  returns SectionReport with correct title
    """
    equities = [_equity()]

    actual = analyse_identifier_quality(equities, _settings())

    assert actual.title == "Identifier Quality"
