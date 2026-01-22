# feeds/test_gleif_feed_data.py

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas.feeds import GleifFeedData

pytestmark = pytest.mark.unit


def test_creates_instance_with_valid_data() -> None:
    """
    ARRANGE: valid name, symbol, and lei
    ACT:     construct GleifFeedData
    ASSERT:  instance is created successfully
    """
    actual = GleifFeedData(name="Apple Inc.", symbol="AAPL", lei="529900T8BM49AURSDO55")

    assert isinstance(actual, GleifFeedData)


def test_preserves_name_field() -> None:
    """
    ARRANGE: valid input with name
    ACT:     construct GleifFeedData
    ASSERT:  name field is preserved
    """
    actual = GleifFeedData(name="Apple Inc.", symbol="AAPL", lei="529900T8BM49AURSDO55")

    assert actual.name == "Apple Inc."


def test_preserves_symbol_field() -> None:
    """
    ARRANGE: valid input with symbol
    ACT:     construct GleifFeedData
    ASSERT:  symbol field is preserved
    """
    actual = GleifFeedData(name="Apple Inc.", symbol="AAPL", lei="529900T8BM49AURSDO55")

    assert actual.symbol == "AAPL"


def test_preserves_lei_field() -> None:
    """
    ARRANGE: valid input with lei
    ACT:     construct GleifFeedData
    ASSERT:  lei field is preserved
    """
    actual = GleifFeedData(name="Apple Inc.", symbol="AAPL", lei="529900T8BM49AURSDO55")

    assert actual.lei == "529900T8BM49AURSDO55"


def test_accepts_none_lei() -> None:
    """
    ARRANGE: valid name and symbol, lei set to None
    ACT:     construct GleifFeedData
    ASSERT:  lei field is None
    """
    actual = GleifFeedData(name="Apple Inc.", symbol="AAPL", lei=None)

    assert actual.lei is None


def test_defaults_lei_to_none() -> None:
    """
    ARRANGE: valid name and symbol, lei not provided
    ACT:     construct GleifFeedData
    ASSERT:  lei field defaults to None
    """
    actual = GleifFeedData(name="Apple Inc.", symbol="AAPL")

    assert actual.lei is None


def test_strips_extra_fields() -> None:
    """
    ARRANGE: input with unexpected extra field
    ACT:     construct GleifFeedData
    ASSERT:  extra field is not present on the model
    """
    actual = GleifFeedData(
        name="Apple Inc.",
        symbol="AAPL",
        lei="529900T8BM49AURSDO55",
        unexpected="FIELD",
    )

    assert not hasattr(actual, "unexpected")


def test_raises_when_name_missing() -> None:
    """
    ARRANGE: omit required 'name' field
    ACT:     construct GleifFeedData
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        GleifFeedData(symbol="AAPL", lei="529900T8BM49AURSDO55")


def test_raises_when_symbol_missing() -> None:
    """
    ARRANGE: omit required 'symbol' field
    ACT:     construct GleifFeedData
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        GleifFeedData(name="Apple Inc.", lei="529900T8BM49AURSDO55")


def test_raises_when_name_is_empty_string() -> None:
    """
    ARRANGE: name field set to empty string
    ACT:     construct GleifFeedData
    ASSERT:  raises ValidationError (required decorator)
    """
    with pytest.raises(ValidationError) as exc_info:
        GleifFeedData(name="", symbol="AAPL", lei="529900T8BM49AURSDO55")

    assert "name is required" in str(exc_info.value)


def test_raises_when_symbol_is_empty_string() -> None:
    """
    ARRANGE: symbol field set to empty string
    ACT:     construct GleifFeedData
    ASSERT:  raises ValidationError (required decorator)
    """
    with pytest.raises(ValidationError) as exc_info:
        GleifFeedData(name="Apple Inc.", symbol="", lei="529900T8BM49AURSDO55")

    assert "symbol is required" in str(exc_info.value)


def test_raises_when_name_is_whitespace_only() -> None:
    """
    ARRANGE: name field set to whitespace-only string
    ACT:     construct GleifFeedData
    ASSERT:  raises ValidationError (required decorator)
    """
    with pytest.raises(ValidationError) as exc_info:
        GleifFeedData(name="   ", symbol="AAPL", lei="529900T8BM49AURSDO55")

    assert "name is required" in str(exc_info.value)


def test_raises_when_symbol_is_whitespace_only() -> None:
    """
    ARRANGE: symbol field set to whitespace-only string
    ACT:     construct GleifFeedData
    ASSERT:  raises ValidationError (required decorator)
    """
    with pytest.raises(ValidationError) as exc_info:
        GleifFeedData(name="Apple Inc.", symbol="   ", lei="529900T8BM49AURSDO55")

    assert "symbol is required" in str(exc_info.value)


def test_preserves_whitespace_in_name() -> None:
    """
    ARRANGE: name with leading/trailing whitespace
    ACT:     construct GleifFeedData
    ASSERT:  whitespace is preserved (no trimming at this layer)
    """
    actual = GleifFeedData(name="  Apple Inc.  ", symbol="AAPL")

    assert actual.name == "  Apple Inc.  "


def test_preserves_whitespace_in_symbol() -> None:
    """
    ARRANGE: symbol with leading/trailing whitespace
    ACT:     construct GleifFeedData
    ASSERT:  whitespace is preserved (no trimming at this layer)
    """
    actual = GleifFeedData(name="Apple Inc.", symbol=" AAPL ")

    assert actual.symbol == " AAPL "
