# sec/test_sec.py

import httpx
import pytest
from pydantic import ValidationError

from equity_aggregator.adapters.data_sources.discovery_feeds.sec.sec import (
    _HEADERS,
    _SEC_SEARCH_URL,
    _parse_row,
)
from equity_aggregator.schemas.feeds import SecFeedData

pytestmark = pytest.mark.live

_HTTP_OK = 200


@pytest.fixture(scope="module")
def sec_response() -> httpx.Response:
    """
    Fetch SEC API response once and share across all tests in this module.
    """
    with httpx.Client(headers=_HEADERS) as client:
        return client.get(_SEC_SEARCH_URL)


def test_sec_endpoint_returns_ok(sec_response: httpx.Response) -> None:
    """
    ARRANGE: live SEC API endpoint
    ACT:     send GET request
    ASSERT:  returns HTTP 200
    """
    assert sec_response.status_code == _HTTP_OK


def test_sec_response_contains_data_key(sec_response: httpx.Response) -> None:
    """
    ARRANGE: live SEC API response
    ACT:     parse JSON payload
    ASSERT:  contains 'data' key
    """
    payload = sec_response.json()

    assert "data" in payload


def test_sec_response_data_is_non_empty_list(sec_response: httpx.Response) -> None:
    """
    ARRANGE: live SEC API response
    ACT:     extract 'data' field
    ASSERT:  is a non-empty list
    """
    payload = sec_response.json()
    data = payload.get("data", [])

    assert isinstance(data, list) and len(data) > 0


def test_sec_record_validates_against_schema(sec_response: httpx.Response) -> None:
    """
    ARRANGE: first row from live SEC API response, parsed into record
    ACT:     validate with SecFeedData schema
    ASSERT:  no ValidationError raised
    """
    payload = sec_response.json()
    first_row = payload["data"][0]
    record = _parse_row(first_row)

    try:
        SecFeedData.model_validate(record)
        validated = True
    except ValidationError:
        validated = False

    assert validated
