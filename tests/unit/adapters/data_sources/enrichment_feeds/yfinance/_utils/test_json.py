# _utils/test_json.py

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance._utils.json import (
    safe_json_parse,
)

pytestmark = pytest.mark.unit


def test_safe_json_parse_returns_dict_on_success() -> None:
    """
    ARRANGE: httpx response with valid JSON and correct content-type
    ACT:     call safe_json_parse
    ASSERT:  returns parsed dict
    """
    response = httpx.Response(
        200,
        json={"symbol": "AAPL", "price": 150},
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "https://example.com"),
    )

    actual = safe_json_parse(response, "AAPL")

    assert actual == {"symbol": "AAPL", "price": 150}


def test_safe_json_parse_raises_on_non_json_content_type() -> None:
    """
    ARRANGE: httpx response with non-JSON content-type
    ACT:     call safe_json_parse
    ASSERT:  raises LookupError with content-type in message
    """
    response = httpx.Response(
        200,
        content=b"<html>Error</html>",
        headers={"content-type": "text/html"},
        request=httpx.Request("GET", "https://example.com"),
    )

    with pytest.raises(LookupError) as exc_info:
        safe_json_parse(response, "AAPL")

    assert "Non-JSON response" in str(exc_info.value)
    assert "text/html" in str(exc_info.value)
    assert "AAPL" in str(exc_info.value)


def test_safe_json_parse_raises_on_invalid_json() -> None:
    """
    ARRANGE: httpx response with application/json content-type but invalid JSON body
    ACT:     call safe_json_parse
    ASSERT:  raises LookupError with context in message
    """
    response = httpx.Response(
        200,
        content=b"{invalid json}",
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "https://example.com"),
    )

    with pytest.raises(LookupError) as exc_info:
        safe_json_parse(response, "MSFT")

    assert "Invalid JSON response" in str(exc_info.value)
    assert "MSFT" in str(exc_info.value)


def test_safe_json_parse_accepts_content_type_with_charset() -> None:
    """
    ARRANGE: httpx response with content-type including charset parameter
    ACT:     call safe_json_parse
    ASSERT:  returns parsed dict (content-type check uses 'in' operator)
    """
    response = httpx.Response(
        200,
        json={"data": "test"},
        headers={"content-type": "application/json; charset=utf-8"},
        request=httpx.Request("GET", "https://example.com"),
    )

    actual = safe_json_parse(response, "TEST")

    assert actual == {"data": "test"}


def test_safe_json_parse_raises_when_content_type_header_missing() -> None:
    """
    ARRANGE: httpx response without content-type header
    ACT:     call safe_json_parse
    ASSERT:  raises LookupError (defaults to empty string)
    """
    response = httpx.Response(
        200,
        content=b'{"data": "test"}',
        headers={},
        request=httpx.Request("GET", "https://example.com"),
    )

    with pytest.raises(LookupError) as exc_info:
        safe_json_parse(response, "NFLX")

    assert "Non-JSON response" in str(exc_info.value)
    assert "NFLX" in str(exc_info.value)
