# domain/integrity/analysers/test_dataset_overview.py

import pytest

from equity_aggregator.domain.integrity.analysers.dataset_overview import (
    build_dataset_overview,
)
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


def test_build_dataset_overview_empty_list() -> None:
    """
    ARRANGE: empty equity list
    ACT:     build_dataset_overview
    ASSERT:  findings mention no equities
    """
    actual = build_dataset_overview([])

    assert "No equities" in actual.findings[0].message


def test_build_dataset_overview_with_equities() -> None:
    """
    ARRANGE: list with one equity
    ACT:     build_dataset_overview
    ASSERT:  findings mention loaded count
    """
    equities = [_equity(sector="TECH", currency="USD")]

    actual = build_dataset_overview(equities)

    assert "1" in actual.findings[0].message
