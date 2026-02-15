# feeds/_utils/test_converters.py

from decimal import Decimal

import pytest

from equity_aggregator.schemas.feeds._utils._converters import percent_to_decimal

pytestmark = pytest.mark.unit


def test_converts_positive_float() -> None:
    """
    ARRANGE: positive float percentage
    ACT:     convert to decimal
    ASSERT:  returns correct decimal ratio
    """
    actual = percent_to_decimal(20.6)

    assert actual == Decimal("0.206")


def test_converts_negative_float() -> None:
    """
    ARRANGE: negative float percentage
    ACT:     convert to decimal
    ASSERT:  returns correct negative decimal ratio
    """
    actual = percent_to_decimal(-5.5)

    assert actual == Decimal("-0.055")


def test_converts_zero() -> None:
    """
    ARRANGE: zero percentage
    ACT:     convert to decimal
    ASSERT:  returns zero
    """
    actual = percent_to_decimal(0.0)

    assert actual == Decimal("0")


def test_converts_string() -> None:
    """
    ARRANGE: percentage as string
    ACT:     convert to decimal
    ASSERT:  returns correct decimal ratio
    """
    actual = percent_to_decimal("15.75")

    assert actual == Decimal("0.1575")


def test_converts_integer() -> None:
    """
    ARRANGE: percentage as integer
    ACT:     convert to decimal
    ASSERT:  returns correct decimal ratio
    """
    actual = percent_to_decimal(25)

    assert actual == Decimal("0.25")


def test_returns_none_for_none() -> None:
    """
    ARRANGE: None input
    ACT:     convert to decimal
    ASSERT:  returns None
    """
    actual = percent_to_decimal(None)

    assert actual is None


def test_returns_none_for_invalid_string() -> None:
    """
    ARRANGE: non-numeric string
    ACT:     convert to decimal
    ASSERT:  returns None
    """
    actual = percent_to_decimal("not-a-number")

    assert actual is None


def test_returns_none_for_invalid_type() -> None:
    """
    ARRANGE: unconvertible object
    ACT:     convert to decimal
    ASSERT:  returns None
    """
    actual = percent_to_decimal({"invalid": "object"})

    assert actual is None
