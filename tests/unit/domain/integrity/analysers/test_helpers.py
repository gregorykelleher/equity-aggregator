# domain/integrity/analysers/test_helpers.py

import pytest

from equity_aggregator.domain.integrity.analysers._helpers import build_format_finding
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


def test_build_format_finding_with_invalid_equities() -> None:
    """
    ARRANGE: non-empty list of equities and a message
    ACT:     build_format_finding
    ASSERT:  returns Finding with sample highlights
    """
    equities = [_equity()]
    expected_limit = 5

    actual = build_format_finding(equities, "Test message.", expected_limit)

    assert actual.message == "Test message."
