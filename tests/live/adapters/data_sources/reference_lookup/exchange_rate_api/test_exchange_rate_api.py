# exchange_rate_api/test_exchange_rate_api.py

import httpx
import pytest

from equity_aggregator.adapters.data_sources.reference_lookup.exchange_rate_api import (
    _build_url,
    _get_api_key,
)

pytestmark = pytest.mark.live

_HTTP_OK = 200


@pytest.fixture(scope="module")
def exchange_rate_response() -> httpx.Response:
    """
    Fetch Exchange Rate API response once and share across all tests in this module.
    """
    api_key = _get_api_key()
    url = _build_url(api_key)

    with httpx.Client() as client:
        return client.get(url)


def test_exchange_rate_api_endpoint_returns_ok(
    exchange_rate_response: httpx.Response,
) -> None:
    """
    ARRANGE: live Exchange Rate API endpoint
    ACT:     send GET request
    ASSERT:  returns HTTP 200
    """
    assert exchange_rate_response.status_code == _HTTP_OK


def test_exchange_rate_api_response_contains_result_key(
    exchange_rate_response: httpx.Response,
) -> None:
    """
    ARRANGE: live Exchange Rate API response
    ACT:     parse JSON payload
    ASSERT:  contains 'result' key
    """
    payload = exchange_rate_response.json()

    assert "result" in payload


def test_exchange_rate_api_response_result_is_success(
    exchange_rate_response: httpx.Response,
) -> None:
    """
    ARRANGE: live Exchange Rate API response
    ACT:     extract 'result' field
    ASSERT:  value is 'success'
    """
    payload = exchange_rate_response.json()

    assert payload.get("result") == "success"


def test_exchange_rate_api_response_contains_conversion_rates_key(
    exchange_rate_response: httpx.Response,
) -> None:
    """
    ARRANGE: live Exchange Rate API response
    ACT:     parse JSON payload
    ASSERT:  contains 'conversion_rates' key
    """
    payload = exchange_rate_response.json()

    assert "conversion_rates" in payload


def test_exchange_rate_api_conversion_rates_is_non_empty_dict(
    exchange_rate_response: httpx.Response,
) -> None:
    """
    ARRANGE: live Exchange Rate API response
    ACT:     extract 'conversion_rates' field
    ASSERT:  is a non-empty dictionary
    """
    payload = exchange_rate_response.json()
    rates = payload.get("conversion_rates", {})

    assert isinstance(rates, dict) and len(rates) > 0
