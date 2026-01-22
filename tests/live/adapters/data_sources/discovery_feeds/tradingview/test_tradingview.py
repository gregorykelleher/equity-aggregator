# tradingview/test_tradingview.py

import httpx
import pytest
from pydantic import ValidationError

from equity_aggregator.adapters.data_sources.discovery_feeds.tradingview import (
    tradingview,
)
from equity_aggregator.schemas.feeds import TradingViewFeedData

pytestmark = pytest.mark.live

_HTTP_OK = 200


def _build_request_body(start: int = 0, end: int = tradingview._PAGE_SIZE) -> dict:
    """
    Build request body with pagination range.
    """
    return {**tradingview._REQUEST_BODY_TEMPLATE, "range": [start, end]}


@pytest.fixture(scope="module")
def tradingview_response() -> httpx.Response:
    """
    Fetch TradingView API response once and share across all tests in this module.
    """
    with httpx.Client() as client:
        return client.post(
            tradingview._TRADINGVIEW_SCAN_URL,
            json=_build_request_body(),
        )


def test_tradingview_endpoint_returns_ok(tradingview_response: httpx.Response) -> None:
    """
    ARRANGE: live TradingView scanner API endpoint
    ACT:     send POST request
    ASSERT:  returns HTTP 200
    """
    assert tradingview_response.status_code == _HTTP_OK


def test_tradingview_response_contains_data_key(
    tradingview_response: httpx.Response,
) -> None:
    """
    ARRANGE: live TradingView API response
    ACT:     parse JSON payload
    ASSERT:  contains 'data' key
    """
    payload = tradingview_response.json()

    assert "data" in payload


def test_tradingview_response_data_is_non_empty_list(
    tradingview_response: httpx.Response,
) -> None:
    """
    ARRANGE: live TradingView API response
    ACT:     extract 'data' field
    ASSERT:  is a non-empty list
    """
    payload = tradingview_response.json()
    data = payload.get("data", [])

    assert isinstance(data, list) and len(data) > 0


def test_tradingview_record_validates_against_schema(
    tradingview_response: httpx.Response,
) -> None:
    """
    ARRANGE: first parsed record from live TradingView API response
    ACT:     validate with TradingViewFeedData schema
    ASSERT:  no ValidationError raised
    """
    payload = tradingview_response.json()
    first_row = payload["data"][0]
    record = tradingview._parse_row(first_row)

    try:
        TradingViewFeedData.model_validate(record)
        validated = True
    except ValidationError:
        validated = False

    assert validated
