# stock_analysis/test_stock_analysis.py

import httpx
import pytest
from pydantic import ValidationError

from equity_aggregator.adapters.data_sources.discovery_feeds.stock_analysis import (
    stock_analysis,
)
from equity_aggregator.schemas.feeds import StockAnalysisFeedData

pytestmark = pytest.mark.live

_HTTP_OK = 200


@pytest.fixture(scope="module")
def stock_analysis_response() -> httpx.Response:
    """
    Fetch Stock Analysis API response once and share across all tests in this module.
    """
    with httpx.Client() as client:
        return client.get(
            stock_analysis._STOCK_ANALYSIS_SEARCH_URL,
            params=stock_analysis._PARAMS,
        )


def test_stock_analysis_endpoint_returns_ok(
    stock_analysis_response: httpx.Response,
) -> None:
    """
    ARRANGE: live Stock Analysis screener API endpoint
    ACT:     send GET request
    ASSERT:  returns HTTP 200
    """
    assert stock_analysis_response.status_code == _HTTP_OK


def test_stock_analysis_response_contains_data_key(
    stock_analysis_response: httpx.Response,
) -> None:
    """
    ARRANGE: live Stock Analysis API response
    ACT:     parse JSON payload
    ASSERT:  contains 'data' key
    """
    payload = stock_analysis_response.json()

    assert "data" in payload


def test_stock_analysis_response_data_is_non_empty_list(
    stock_analysis_response: httpx.Response,
) -> None:
    """
    ARRANGE: live Stock Analysis API response
    ACT:     extract nested data array
    ASSERT:  is a non-empty list
    """
    payload = stock_analysis_response.json()
    data_wrapper = payload.get("data", {})
    records = data_wrapper.get("data", [])

    assert isinstance(records, list) and len(records) > 0


def test_stock_analysis_record_validates_against_schema(
    stock_analysis_response: httpx.Response,
) -> None:
    """
    ARRANGE: first record from live Stock Analysis API response
    ACT:     validate with StockAnalysisFeedData schema
    ASSERT:  no ValidationError raised
    """
    payload = stock_analysis_response.json()
    data_wrapper = payload.get("data", {})
    records = data_wrapper.get("data", [])
    first_record = records[0]

    try:
        StockAnalysisFeedData.model_validate(first_record)
        validated = True
    except ValidationError:
        validated = False

    assert validated
