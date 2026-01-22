# xetra/test_xetra.py

import httpx
import pytest
from pydantic import ValidationError

from equity_aggregator.adapters.data_sources.discovery_feeds.xetra.xetra import (
    _HEADERS,
    _XETRA_SEARCH_URL,
    _build_payload,
    _extract_records,
)
from equity_aggregator.schemas.feeds import XetraFeedData

pytestmark = pytest.mark.live

_HTTP_OK = 200


@pytest.fixture(scope="module")
def xetra_response() -> httpx.Response:
    """
    Fetch XETRA API response once and share across all tests in this module.
    """
    with httpx.Client(headers=_HEADERS) as client:
        return client.post(_XETRA_SEARCH_URL, json=_build_payload(offset=0))


def test_xetra_endpoint_returns_ok(xetra_response: httpx.Response) -> None:
    """
    ARRANGE: live XETRA API endpoint
    ACT:     send POST request
    ASSERT:  returns HTTP 200
    """
    assert xetra_response.status_code == _HTTP_OK


def test_xetra_response_contains_data_key(xetra_response: httpx.Response) -> None:
    """
    ARRANGE: live XETRA API response
    ACT:     parse JSON payload
    ASSERT:  contains 'data' key
    """
    payload = xetra_response.json()

    assert "data" in payload


def test_xetra_response_data_is_non_empty_list(xetra_response: httpx.Response) -> None:
    """
    ARRANGE: live XETRA API response
    ACT:     extract 'data' field
    ASSERT:  is a non-empty list
    """
    payload = xetra_response.json()
    data = payload.get("data", [])

    assert isinstance(data, list) and len(data) > 0


def test_xetra_extracted_record_validates_against_schema(
    xetra_response: httpx.Response,
) -> None:
    """
    ARRANGE: first extracted record from live XETRA API response
    ACT:     validate with XetraFeedData schema
    ASSERT:  no ValidationError raised
    """
    payload = xetra_response.json()
    records = _extract_records(payload)
    first_record = records[0]

    try:
        XetraFeedData.model_validate(first_record)
        validated = True
    except ValidationError:
        validated = False

    assert validated
