# feeds/test_lseg_feed_data.py

from decimal import Decimal

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas import LsegFeedData

pytestmark = pytest.mark.unit


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct LsegFeedData
    ASSERT:  extra field is not present on the model
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": None,
        "lastprice": None,
        "marketcapitalization": None,
    }

    actual = LsegFeedData(**payload, unexpected="FIELD")

    assert not hasattr(actual, "unexpected")


def test_missing_required_raises() -> None:
    """
    ARRANGE: input missing required 'issuername' field
    ACT:     construct LsegFeedData
    ASSERT:  raises ValidationError
    """
    incomplete = {
        "tidm": "F",
        "currency": None,
        "lastprice": None,
        "marketcapitalization": None,
    }

    with pytest.raises(ValidationError):
        LsegFeedData(**incomplete)


def test_isin_defaults_to_none() -> None:
    """
    ARRANGE: omit 'isin' field
    ACT:     construct LsegFeedData
    ASSERT:  isin defaults to None
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": 1000,
    }

    actual = LsegFeedData(**payload)

    assert actual.isin is None


def test_symbol_maps_from_tidm() -> None:
    """
    ARRANGE: provide 'tidm' field
    ACT:     construct LsegFeedData
    ASSERT:  symbol is set from tidm
    """
    payload = {
        "issuername": "Foo",
        "tidm": "TIDM123",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": 1000,
    }

    actual = LsegFeedData(**payload)

    assert actual.symbol == "TIDM123"


def test_last_price_and_market_cap_types() -> None:
    """
    ARRANGE: lastprice and marketcapitalization as int, float, str, Decimal
    ACT:     construct LsegFeedData for each type
    ASSERT:  values are preserved as given
    """
    for candidate in (123, 123.45, "123.45", Decimal("123.45")):
        payload = {
            "issuername": "Foo",
            "tidm": "F",
            "currency": "GBP",
            "lastprice": candidate,
            "marketcapitalization": candidate,
        }

        actual = LsegFeedData(**payload)

        assert actual.last_price == candidate


def test_last_price_can_be_none() -> None:
    """
    ARRANGE: lastprice is None
    ACT:     construct LsegFeedData
    ASSERT:  last_price is preserved as None
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": None,
        "marketcapitalization": 1000,
    }

    actual = LsegFeedData(**payload)

    assert actual.last_price is None


def test_marketcapitalization_can_be_none() -> None:
    """
    ARRANGE: marketcapitalization is None
    ACT:     construct LsegFeedData
    ASSERT:  marketcapitalization field is accepted but not exposed on model
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": None,
    }

    actual = LsegFeedData(**payload)

    # The model accepts marketcapitalization but doesn't expose it as a field
    assert actual.name == "Foo"


def test_currency_case_and_whitespace_preserved() -> None:
    """
    ARRANGE: currency is lowercase and padded
    ACT:     construct LsegFeedData
    ASSERT:  currency is preserved as given (no uppercase enforcement)
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": " gbp ",
        "lastprice": 10,
        "marketcapitalization": 1000,
    }

    actual = LsegFeedData(**payload)

    assert actual.currency == " gbp "


def test_omits_isin_sets_none() -> None:
    """
    ARRANGE: omit 'isin' field
    ACT:     construct LsegFeedData
    ASSERT:  isin is set to None
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": 1000,
    }

    actual = LsegFeedData(**payload)

    assert actual.isin is None


def test_last_price_string_with_comma() -> None:
    """
    ARRANGE: lastprice as string with comma decimal
    ACT:     construct LsegFeedData
    ASSERT:  last_price is preserved as string (no conversion for non-GBX)
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBP",
        "lastprice": "1,23",
        "marketcapitalization": 1000,
    }

    actual = LsegFeedData(**payload)

    assert actual.last_price == "1,23"


def test_gbx_currency_converts_price_and_currency() -> None:
    """
    ARRANGE: currency is GBX and lastprice is pence string
    ACT:     construct LsegFeedData
    ASSERT:  last_price is converted to pounds and currency to GBP
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBX",
        "lastprice": "123,45",
        "marketcapitalization": 1000,
    }

    actual = LsegFeedData(**payload)

    assert actual.last_price == Decimal("1.2345")


def test_gbx_currency_converts_currency_to_gbp() -> None:
    """
    ARRANGE: currency is GBX
    ACT:     construct LsegFeedData
    ASSERT:  currency is converted to GBP
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBX",
        "lastprice": "123,45",
        "marketcapitalization": 1000,
    }

    actual = LsegFeedData(**payload)

    assert actual.currency == "GBP"


def test_gbx_currency_handles_invalid_lastprice() -> None:
    """
    ARRANGE: currency is GBX and lastprice is not a number
    ACT:     construct LsegFeedData
    ASSERT:  last_price is None (conversion fails)
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBX",
        "lastprice": "not_a_number",
        "marketcapitalization": 1000,
    }

    actual = LsegFeedData(**payload)

    assert actual.last_price is None and actual.currency == "GBP"


def test_gbx_currency_with_none_lastprice() -> None:
    """
    ARRANGE: currency is GBX and lastprice is None
    ACT:     construct LsegFeedData
    ASSERT:  last_price is None
    """
    payload = {
        "issuername": "Foo",
        "tidm": "F",
        "currency": "GBX",
        "lastprice": None,
        "marketcapitalization": 1000,
    }

    actual = LsegFeedData(**payload)

    assert actual.last_price is None and actual.currency == "GBP"


def test_extra_field_is_ignored() -> None:
    """
    ARRANGE: input with an extra unexpected field
    ACT:     construct LsegFeedData
    ASSERT:  extra field is not present on the model
    """
    payload = {
        "issuername": "Real Name",
        "tidm": "SYM",
        "currency": "GBP",
        "lastprice": 1.0,
        "marketcapitalization": 1000,
        "mics": ["XLON"],
        "extra": "should be ignored",
    }

    actual = LsegFeedData(**payload)

    assert not hasattr(actual, "extra")


def test_accepts_various_last_price_types() -> None:
    """
    ARRANGE: lastprice as int, float, str, Decimal
    ACT:     construct LsegFeedData for each type
    ASSERT:  last_price is preserved as given
    """
    for candidate in (123, 123.45, "123.45", Decimal("123.45")):
        payload = {
            "issuername": "Foo",
            "tidm": "F",
            "currency": "GBP",
            "lastprice": candidate,
            "marketcapitalization": 1000,
        }

        actual = LsegFeedData(**payload)

        assert actual.last_price == candidate
