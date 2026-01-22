# intrinio/test_intrinio.py

import os

import httpx
import pytest

from equity_aggregator.adapters.data_sources.discovery_feeds.intrinio._utils import (
    parse_companies_response,
    parse_securities_response,
)
from equity_aggregator.adapters.data_sources.discovery_feeds.intrinio.intrinio import (
    _INTRINIO_COMPANIES_URL,
)
from equity_aggregator.schemas.feeds import IntrinioFeedData

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        not os.getenv("INTRINIO_API_KEY"),
        reason="INTRINIO_API_KEY environment variable not set",
    ),
]

_HTTP_OK = 200


def _get_api_key() -> str:
    return os.getenv("INTRINIO_API_KEY", "")


@pytest.fixture(scope="module")
def intrinio_companies_response() -> httpx.Response:
    """
    Fetch Intrinio companies API response once and share across all tests.
    """
    params = {"api_key": _get_api_key(), "page_size": "10"}
    with httpx.Client() as client:
        return client.get(_INTRINIO_COMPANIES_URL, params=params)


@pytest.fixture(scope="module")
def intrinio_security_record(intrinio_companies_response: httpx.Response) -> dict:
    """
    Fetch a security record from Intrinio for schema validation.
    """
    records, _ = parse_companies_response(intrinio_companies_response.json())
    first_company = records[0]
    ticker = first_company["company_ticker"]
    securities_url = f"{_INTRINIO_COMPANIES_URL}/{ticker}/securities"

    with httpx.Client() as client:
        securities_response = client.get(
            securities_url,
            params={"api_key": _get_api_key()},
        )

    security_records = parse_securities_response(securities_response.json())

    if not security_records:
        pytest.skip("No securities found for first company")

    return security_records[0]


def test_intrinio_endpoint_returns_ok(
    intrinio_companies_response: httpx.Response,
) -> None:
    """
    ARRANGE: live Intrinio companies API endpoint
    ACT:     send GET request
    ASSERT:  returns HTTP 200
    """
    assert intrinio_companies_response.status_code == _HTTP_OK


def test_intrinio_record_validates_against_schema(
    intrinio_security_record: dict,
) -> None:
    """
    ARRANGE: security record from live Intrinio API response
    ACT:     validate with IntrinioFeedData schema
    ASSERT:  no ValidationError raised
    """
    assert IntrinioFeedData.model_validate(intrinio_security_record)
