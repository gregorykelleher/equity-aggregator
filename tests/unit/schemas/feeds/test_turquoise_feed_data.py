# feeds/test_turquoise_feed_data.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import TurquoiseFeedData

pytestmark = pytest.mark.unit


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct TurquoiseFeedData
    ASSERT:  extra field is not present on the model
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": None,
        "lastvalue": None,
        "marketcapitalization": None,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload, unexpected="FIELD")

    assert not hasattr(actual, "unexpected")


def test_missing_required_raises() -> None:
    """
    ARRANGE: input missing required 'name' field
    ACT:     construct TurquoiseFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "symbol": "F",
        "currency": None,
        "lastvalue": None,
        "marketcapitalization": None,
        "mics": ["XLON"],
    }

    with pytest.raises(ValidationError):
        TurquoiseFeedData(**incomplete)


def test_mics_default_to_none() -> None:
    """
    ARRANGE: omit 'mics' field
    ACT:     construct TurquoiseFeedData
    ASSERT:  mics defaults to None
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": "GBP",
        "lastvalue": 1.0,
        "marketcapitalization": 1000,
    }

    actual = TurquoiseFeedData(**payload)

    assert actual.mics is None


def test_symbol_maps_from_symbol() -> None:
    """
    ARRANGE: provide 'symbol' field
    ACT:     construct TurquoiseFeedData
    ASSERT:  symbol is set from symbol
    """
    payload = {
        "name": "Foo",
        "symbol": "TIDM123",
        "currency": "GBP",
        "lastvalue": 1.0,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload)

    assert actual.symbol == "TIDM123"


def test_last_price_and_market_cap_types() -> None:
    """
    ARRANGE: lastvalue and marketcapitalization as int, float, str, Decimal
    ACT:     construct TurquoiseFeedData for each type
    ASSERT:  values are preserved as given
    """
    for candidate in (123, 123.45, "123.45", Decimal("123.45")):
        payload = {
            "name": "Foo",
            "symbol": "F",
            "currency": "GBP",
            "lastvalue": candidate,
            "marketcapitalization": candidate,
            "mics": ["XLON"],
        }

        actual = TurquoiseFeedData(**payload)

        assert actual.last_price == candidate


def test_last_price_can_be_none() -> None:
    """
    ARRANGE: lastvalue is None
    ACT:     construct TurquoiseFeedData
    ASSERT:  last_price is preserved as None
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": "GBP",
        "lastvalue": None,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload)

    assert actual.last_price is None


def test_marketcapitalization_can_be_none() -> None:
    """
    ARRANGE: marketcapitalization is None
    ACT:     construct TurquoiseFeedData
    ASSERT:  marketcapitalization field is accepted but not exposed on model
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": "GBP",
        "lastvalue": 1.0,
        "marketcapitalization": None,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload)

    # The model accepts marketcapitalization but doesn't expose it as a field
    assert actual.name == "Foo"


def test_currency_case_and_whitespace_preserved() -> None:
    """
    ARRANGE: currency is lowercase and padded
    ACT:     construct TurquoiseFeedData
    ASSERT:  currency is preserved as given (no uppercase enforcement)
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": " gbp ",
        "lastvalue": 10,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload)

    assert actual.currency == " gbp "


def test_omits_isin_sets_none() -> None:
    """
    ARRANGE: omit 'isin' field
    ACT:     construct TurquoiseFeedData
    ASSERT:  isin is set to None
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": "GBP",
        "lastvalue": 1.0,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload)

    assert actual.isin is None


def test_last_price_string_with_comma() -> None:
    """
    ARRANGE: lastvalue as string with comma decimal
    ACT:     construct TurquoiseFeedData
    ASSERT:  last_price is preserved as string
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": "GBP",
        "lastvalue": "1,23",
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload)

    assert actual.last_price == "1,23"


def test_mics_from_field() -> None:
    """
    ARRANGE: provide 'mics' field
    ACT:     construct TurquoiseFeedData
    ASSERT:  mics is set as None (field gets normalized away)
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": "GBP",
        "lastvalue": 1.0,
        "marketcapitalization": 1000,
        "mics": ["XLON", "XOFF"],
    }

    actual = TurquoiseFeedData(**payload)

    # mics field gets set to None in the normalization process
    assert actual.mics is None


def test_gbx_currency_converts_price_and_currency() -> None:
    """
    ARRANGE: currency is GBX and lastvalue is pence string
    ACT:     construct TurquoiseFeedData
    ASSERT:  last_price is converted to pounds and currency to GBP
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": "GBX",
        "lastvalue": "123,45",
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload)

    assert actual.last_price == Decimal("1.2345")


def test_gbx_currency_converts_currency_to_gbp() -> None:
    """
    ARRANGE: currency is GBX
    ACT:     construct TurquoiseFeedData
    ASSERT:  currency is converted to GBP
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": "GBX",
        "lastvalue": "123,45",
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload)

    assert actual.currency == "GBP"


def test_gbx_currency_handles_invalid_lastvalue() -> None:
    """
    ARRANGE: currency is GBX and lastvalue is not a number
    ACT:     construct TurquoiseFeedData
    ASSERT:  last_price is None (conversion fails)
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": "GBX",
        "lastvalue": "not_a_number",
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload)

    assert actual.last_price is None and actual.currency == "GBP"


def test_gbx_currency_with_none_lastvalue() -> None:
    """
    ARRANGE: currency is GBX and lastvalue is None
    ACT:     construct TurquoiseFeedData
    ASSERT:  last_price is None
    """
    payload = {
        "name": "Foo",
        "symbol": "F",
        "currency": "GBX",
        "lastvalue": None,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
    }

    actual = TurquoiseFeedData(**payload)

    assert actual.last_price is None and actual.currency == "GBP"


def test_extra_field_is_ignored() -> None:
    """
    ARRANGE: input with an extra unexpected field
    ACT:     construct TurquoiseFeedData
    ASSERT:  extra field is not present on the model
    """
    payload = {
        "name": "Real Name",
        "symbol": "SYM",
        "currency": "GBP",
        "lastvalue": 1.0,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
        "extra": "should be ignored",
    }

    actual = TurquoiseFeedData(**payload)

    assert not hasattr(actual, "extra")


def test_accepts_various_last_price_types() -> None:
    """
    ARRANGE: lastvalue as int, float, str, Decimal
    ACT:     construct TurquoiseFeedData for each type
    ASSERT:  last_price is preserved as given
    """
    for candidate in (123, 123.45, "123.45", Decimal("123.45")):
        payload = {
            "name": "Foo",
            "symbol": "F",
            "currency": "GBP",
            "lastvalue": candidate,
            "marketcapitalization": 1000,
            "mics": ["XLON"],
        }

        actual = TurquoiseFeedData(**payload)

        assert actual.last_price == candidate
