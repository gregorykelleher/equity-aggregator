# yfinance/test_yfinance.py

import asyncio

import pytest
from pydantic import ValidationError

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.api import (
    get_quote_summary,
    search_quotes,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.config import (
    FeedConfig,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.session import (
    YFSession,
)
from equity_aggregator.schemas.feeds import YFinanceFeedData

pytestmark = pytest.mark.live

_TEST_TICKER = "AAPL"
_TEST_QUERY = "Apple"


@pytest.fixture(scope="module")
def yf_session() -> YFSession:
    """
    Provide a YFSession for tests.
    """
    config = FeedConfig()
    return YFSession(config)


@pytest.fixture(scope="module")
def yfinance_search_quotes(yf_session: YFSession) -> list:
    """
    Fetch Yahoo Finance search quotes once and share across tests.
    """
    return asyncio.new_event_loop().run_until_complete(
        search_quotes(yf_session, _TEST_QUERY)
    )


@pytest.fixture(scope="module")
def yfinance_quote_summary(yf_session: YFSession) -> dict:
    """
    Fetch Yahoo Finance quote summary once and share across tests.
    """
    return asyncio.new_event_loop().run_until_complete(
        get_quote_summary(yf_session, _TEST_TICKER)
    )


def test_yfinance_search_returns_quotes(yfinance_search_quotes: list) -> None:
    """
    ARRANGE: live Yahoo Finance search API via YFSession
    ACT:     search for a known query
    ASSERT:  returns non-empty list of quotes
    """
    assert isinstance(yfinance_search_quotes, list) and len(yfinance_search_quotes) > 0


def test_yfinance_quote_summary_returns_data(yfinance_quote_summary: dict) -> None:
    """
    ARRANGE: live Yahoo Finance quote summary API via YFSession
    ACT:     fetch quote summary for known ticker
    ASSERT:  returns non-empty dict
    """
    assert isinstance(yfinance_quote_summary, dict) and len(yfinance_quote_summary) > 0


def test_yfinance_quote_summary_validates_against_schema(
    yfinance_quote_summary: dict,
) -> None:
    """
    ARRANGE: quote summary from live Yahoo Finance API
    ACT:     validate with YFinanceFeedData schema
    ASSERT:  no ValidationError raised
    """
    try:
        YFinanceFeedData.model_validate(yfinance_quote_summary)
        validated = True
    except ValidationError:
        validated = False

    assert validated
