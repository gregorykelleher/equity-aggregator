# domain/integrity/analysers/test_data_consistency.py

import pytest

from equity_aggregator.domain.integrity.analysers.data_consistency import (
    _collect_duplicate_name_groups,
    analyse_currency_rarity,
    analyse_data_consistency,
    analyse_duplicate_names,
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


def test_analyse_data_consistency_returns_section() -> None:
    """
    ARRANGE: equities list
    ACT:     analyse_data_consistency
    ASSERT:  returns SectionReport with correct title
    """
    equities = [_equity()]

    actual = analyse_data_consistency(equities, _settings())

    assert actual.title == "Symbol and Naming Consistency"


def test_analyse_currency_rarity_with_rare_currency() -> None:
    """
    ARRANGE: equity with a rare currency (count < 10)
    ACT:     analyse_currency_rarity
    ASSERT:  returns at least one finding
    """
    equities = [_equity(figi="BBG000RARE01", currency="XAF")]

    actual = analyse_currency_rarity(equities, _settings())

    assert len(actual) >= 1


def test_analyse_duplicate_names_with_duplicates() -> None:
    """
    ARRANGE: two equities with the same name
    ACT:     analyse_duplicate_names
    ASSERT:  returns finding about duplicates
    """
    equities = [
        _equity(figi="BBG000DUP001", name="SAME CO"),
        _equity(figi="BBG000DUP002", name="SAME CO"),
    ]

    actual = analyse_duplicate_names(equities, _settings())

    assert len(actual) >= 1


def test_collect_duplicate_name_groups_skips_none_name() -> None:
    """
    ARRANGE: CanonicalEquity constructed with name set to None post-validation
    ACT:     _collect_duplicate_name_groups
    ASSERT:  returns empty dict (no groups formed)
    """
    equity = _equity()
    object.__setattr__(equity.identity, "name", None)

    actual = _collect_duplicate_name_groups([equity])

    assert actual == {}
