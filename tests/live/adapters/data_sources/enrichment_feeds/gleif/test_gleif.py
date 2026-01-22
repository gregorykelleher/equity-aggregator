# gleif/test_gleif.py

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif.api import (
    GLEIF_ISIN_LEI_URL,
)

pytestmark = pytest.mark.live

_HTTP_OK = 200


@pytest.fixture(scope="module")
def gleif_response() -> httpx.Response:
    """
    Fetch GLEIF API response once and share across all tests in this module.
    """
    with httpx.Client() as client:
        return client.get(GLEIF_ISIN_LEI_URL)


def test_gleif_api_endpoint_returns_ok(gleif_response: httpx.Response) -> None:
    """
    ARRANGE: live GLEIF ISIN-LEI mapping API endpoint
    ACT:     send GET request
    ASSERT:  returns HTTP 200
    """
    assert gleif_response.status_code == _HTTP_OK


def test_gleif_api_response_contains_data_key(gleif_response: httpx.Response) -> None:
    """
    ARRANGE: live GLEIF API response
    ACT:     parse JSON payload
    ASSERT:  contains 'data' key
    """
    payload = gleif_response.json()

    assert "data" in payload


def test_gleif_api_response_data_contains_attributes(
    gleif_response: httpx.Response,
) -> None:
    """
    ARRANGE: live GLEIF API response
    ACT:     extract 'data' field
    ASSERT:  contains 'attributes' key
    """
    payload = gleif_response.json()
    data = payload.get("data", {})

    assert "attributes" in data


def test_gleif_api_response_contains_download_link(
    gleif_response: httpx.Response,
) -> None:
    """
    ARRANGE: live GLEIF API response
    ACT:     extract attributes from response
    ASSERT:  contains 'downloadLink' key
    """
    payload = gleif_response.json()
    attrs = payload.get("data", {}).get("attributes", {})

    assert "downloadLink" in attrs


def test_gleif_api_download_link_is_valid_url(gleif_response: httpx.Response) -> None:
    """
    ARRANGE: live GLEIF API response
    ACT:     extract download link
    ASSERT:  is a non-empty string starting with https
    """
    payload = gleif_response.json()
    download_link = payload.get("data", {}).get("attributes", {}).get("downloadLink")

    assert isinstance(download_link, str) and download_link.startswith("https://")
