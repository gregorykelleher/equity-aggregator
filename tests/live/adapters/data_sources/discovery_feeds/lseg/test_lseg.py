# lseg/test_lseg.py

import time

import httpx
import pytest
from pydantic import ValidationError

from equity_aggregator.adapters.data_sources.discovery_feeds.lseg._utils import (
    parse_response,
)
from equity_aggregator.adapters.data_sources.discovery_feeds.lseg.lseg import (
    _HEADERS,
    _LSEG_BASE_PARAMS,
    _LSEG_PATH,
    _LSEG_SEARCH_URL,
)
from equity_aggregator.schemas.feeds import LsegFeedData

pytestmark = pytest.mark.live

_HTTP_OK = 200
_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=None)

# LSEG's edge intermittently serves a 405 CAPTCHA challenge to datacenter IPs
# (e.g. CI runners). The block is probabilistic and clears on retry, so attempt
# a few times with short backoff before giving up. Delays sum to ~7s, well
# within the suite's per-test timeout.
_MAX_ATTEMPTS = 4
_RETRY_BASE_DELAY = 1.0


@pytest.fixture(scope="module")
def lseg_response() -> httpx.Response:
    """
    Fetch LSEG API response once and share across all tests in this module.

    Note:
        Retries on a fresh client when LSEG returns a non-200 anti-bot challenge,
        as a new connection re-rolls the edge's bot score. Returns the last
        response once a 200 is received or attempts are exhausted.

    Returns:
        httpx.Response: The first successful (or final) LSEG API response.
    """
    response = _request_lseg()

    for attempt in range(1, _MAX_ATTEMPTS):
        if response.status_code == _HTTP_OK:
            return response
        time.sleep(_RETRY_BASE_DELAY * 2 ** (attempt - 1))
        response = _request_lseg()

    return response


def _request_lseg() -> httpx.Response:
    """
    Send a single GET request to the LSEG price-explorer endpoint.

    Note:
        Uses a fresh client per call so a retry does not resend any challenge
        cookie pinned to a prior connection.

    Returns:
        httpx.Response: The raw LSEG API response.
    """
    with httpx.Client(headers=_HEADERS, timeout=_TIMEOUT) as client:
        return client.get(
            _LSEG_SEARCH_URL,
            params={
                "path": _LSEG_PATH,
                "parameters": f"{_LSEG_BASE_PARAMS}&page=0",
            },
        )


def test_lseg_endpoint_returns_ok(lseg_response: httpx.Response) -> None:
    """
    ARRANGE: live LSEG API endpoint
    ACT:     send GET request
    ASSERT:  returns HTTP 200
    """
    assert lseg_response.status_code == _HTTP_OK


def test_lseg_response_contains_components_key(lseg_response: httpx.Response) -> None:
    """
    ARRANGE: live LSEG API response
    ACT:     parse JSON payload
    ASSERT:  contains 'components' key
    """
    payload = lseg_response.json()

    assert "components" in payload


def test_lseg_response_parses_to_non_empty_records(
    lseg_response: httpx.Response,
) -> None:
    """
    ARRANGE: live LSEG API response
    ACT:     parse response using parse_response
    ASSERT:  returns non-empty list of records
    """
    payload = lseg_response.json()
    records, _ = parse_response(payload)

    assert isinstance(records, list) and len(records) > 0


def test_lseg_record_validates_against_schema(lseg_response: httpx.Response) -> None:
    """
    ARRANGE: first record from live LSEG API response
    ACT:     validate with LsegFeedData schema
    ASSERT:  no ValidationError raised
    """
    payload = lseg_response.json()
    records, _ = parse_response(payload)
    first_record = records[0]

    try:
        LsegFeedData.model_validate(first_record)
        validated = True
    except ValidationError:
        validated = False

    assert validated
