# domain/integrity/analysers/test_currency_and_geography.py

import pytest

from equity_aggregator.domain.integrity.analysers.currency_and_geography import (
    analyse_currency_and_geography,
    currency_distribution,
    geography_proxies,
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


def test_currency_distribution_with_data() -> None:
    """
    ARRANGE: equities with currencies
    ACT:     currency_distribution
    ASSERT:  returns finding about distribution
    """
    equities = [
        _equity(figi="BBG000CUR001", currency="USD"),
        _equity(figi="BBG000CUR002", currency="EUR"),
    ]

    actual = currency_distribution(equities)

    assert "distribution" in actual[0].message.lower()


def test_geography_proxies_with_data() -> None:
    """
    ARRANGE: equity with CUSIP
    ACT:     geography_proxies
    ASSERT:  returns finding about geographic indicators
    """
    equities = [_equity(cusip="037833100")]

    actual = geography_proxies(equities)

    assert "Geographic" in actual[0].message


def test_analyse_currency_and_geography_returns_section() -> None:
    """
    ARRANGE: equities list
    ACT:     analyse_currency_and_geography
    ASSERT:  returns SectionReport with correct title
    """
    equities = [_equity(currency="USD")]

    actual = analyse_currency_and_geography(equities)

    assert actual.title == "Currency and Geography"
