# intrinio/test_intrinio.py

import asyncio
import os
from unittest import mock

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.intrinio.intrinio import (
    IntrinioFeed,
    open_intrinio_feed,
)
from equity_aggregator.storage import load_cache_entry, save_cache_entry

pytestmark = pytest.mark.unit


def make_intrinio_response(
    ticker: str = "AAPL",
    name: str = "Apple Inc",
    figi: str = "BBG001S5N8V8",
) -> httpx.Response:
    """Create a mock Intrinio API quote response."""
    return httpx.Response(
        200,
        json={
            "security": {
                "ticker": ticker,
                "name": name,
                "share_class_figi": figi,
                "currency": "USD",
            },
            "last": 150.0,
            "marketcap": 2500000000000,
            "eod_fifty_two_week_low": 125.0,
            "eod_fifty_two_week_high": 175.0,
        },
    )


def make_client_with_response(response: httpx.Response) -> httpx.AsyncClient:
    """Create a mock AsyncClient that returns the given response."""
    transport = httpx.MockTransport(lambda request: response)
    return httpx.AsyncClient(transport=transport)


def test_context_manager_requires_api_key() -> None:
    """
    ARRANGE: INTRINIO_API_KEY not set
    ACT:     call open_intrinio_feed
    ASSERT:  raises ValueError
    """

    async def run() -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            if "INTRINIO_API_KEY" in os.environ:
                del os.environ["INTRINIO_API_KEY"]

            with pytest.raises(ValueError, match="INTRINIO_API_KEY"):
                async with open_intrinio_feed():
                    pass

    asyncio.run(run())


def test_context_manager_yields_intrinio_feed() -> None:
    """
    ARRANGE: INTRINIO_API_KEY is set
    ACT:     call open_intrinio_feed
    ASSERT:  yields IntrinioFeed instance
    """

    async def run() -> IntrinioFeed | None:
        with mock.patch.dict(os.environ, {"INTRINIO_API_KEY": "test_key"}):
            async with open_intrinio_feed() as feed:
                return feed

    feed = asyncio.run(run())

    assert isinstance(feed, IntrinioFeed)


def test_fetch_equity_returns_cached_data() -> None:
    """
    ARRANGE: cached data for symbol exists
    ACT:     call fetch_equity
    ASSERT:  returns cached data without API call
    """
    save_cache_entry("intrinio_equities", "CACHED", {"cached": True})

    client = make_client_with_response(httpx.Response(404))  # Should not be called
    feed = IntrinioFeed(client, "test_key")

    actual = asyncio.run(
        feed.fetch_equity(symbol="CACHED", name="Cached Corp"),
    )

    assert actual == {"cached": True}


def test_fetch_equity_tries_share_class_figi_first() -> None:
    """
    ARRANGE: share_class_figi provided
    ACT:     call fetch_equity
    ASSERT:  uses share_class_figi in API request
    """
    response = make_intrinio_response()

    def check_url(request: httpx.Request) -> httpx.Response:
        assert "BBG000BLNNH6" in str(request.url)
        return response

    transport = httpx.MockTransport(check_url)
    client = httpx.AsyncClient(transport=transport)
    feed = IntrinioFeed(client, "test_key")

    asyncio.run(
        feed.fetch_equity(
            symbol="AAPL",
            name="Apple",
            share_class_figi="BBG000BLNNH6",
        ),
    )


def test_fetch_equity_falls_back_to_symbol() -> None:
    """
    ARRANGE: share_class_figi fails, symbol available
    ACT:     call fetch_equity
    ASSERT:  falls back to symbol
    """
    figi_response = httpx.Response(404)
    symbol_response = make_intrinio_response()

    attempts = []
    expected_attempts = 2

    def handle_request(request: httpx.Request) -> httpx.Response:
        attempts.append(str(request.url))
        if "BBG000" in str(request.url):
            return figi_response
        return symbol_response

    transport = httpx.MockTransport(handle_request)
    client = httpx.AsyncClient(transport=transport)
    feed = IntrinioFeed(client, "test_key")

    asyncio.run(
        feed.fetch_equity(
            symbol="AAPL",
            name="Apple",
            share_class_figi="BBG000BLNNH6",
        ),
    )

    assert len(attempts) == expected_attempts


def test_fetch_equity_raises_lookup_error_when_all_fail() -> None:
    """
    ARRANGE: all identifiers return 404
    ACT:     call fetch_equity
    ASSERT:  raises LookupError
    """
    client = make_client_with_response(httpx.Response(404))
    feed = IntrinioFeed(client, "test_key")

    with pytest.raises(LookupError, match="No Intrinio data found"):
        asyncio.run(
            feed.fetch_equity(
                symbol="MISSING",
                name="Missing Corp",
            ),
        )


def test_fetch_equity_accepts_kwargs() -> None:
    """
    ARRANGE: fetch_equity called with extra kwargs
    ACT:     call fetch_equity with **kwargs
    ASSERT:  succeeds without error
    """
    client = make_client_with_response(make_intrinio_response())
    feed = IntrinioFeed(client, "test_key")

    asyncio.run(
        feed.fetch_equity(
            symbol="AAPL",
            name="Apple",
            extra_param="ignored",
            another_param=123,
        ),
    )


def test_fetch_equity_caches_successful_result() -> None:
    """
    ARRANGE: API returns valid data
    ACT:     call fetch_equity
    ASSERT:  result is cached
    """
    client = make_client_with_response(make_intrinio_response())
    feed = IntrinioFeed(client, "test_key")

    asyncio.run(
        feed.fetch_equity(
            symbol="NEW",
            name="New Corp",
        ),
    )

    cached = load_cache_entry("intrinio_equities", "NEW")

    assert cached is not None


def test_fetch_equity_tries_figi_first() -> None:
    """
    ARRANGE: share_class_figi and other identifiers provided
    ACT:     call fetch_equity with all failing except symbol
    ASSERT:  first attempt uses FIGI
    """
    attempts = []
    total_identifiers = 4

    def handle_request(request: httpx.Request) -> httpx.Response:
        attempts.append(str(request.url))
        return (
            httpx.Response(404)
            if len(attempts) < total_identifiers
            else make_intrinio_response()
        )

    transport = httpx.MockTransport(handle_request)
    client = httpx.AsyncClient(transport=transport)
    feed = IntrinioFeed(client, "test_key")

    asyncio.run(
        feed.fetch_equity(
            symbol="TEST",
            name="Test",
            share_class_figi="FIGI123",
            isin="ISIN456",
            cusip="CUSIP789",
        ),
    )

    assert "FIGI123" in attempts[0]


def test_fetch_equity_tries_isin_second() -> None:
    """
    ARRANGE: ISIN and other identifiers provided
    ACT:     call fetch_equity with all failing except symbol
    ASSERT:  second attempt uses ISIN
    """
    attempts = []
    total_identifiers = 4

    def handle_request(request: httpx.Request) -> httpx.Response:
        attempts.append(str(request.url))
        return (
            httpx.Response(404)
            if len(attempts) < total_identifiers
            else make_intrinio_response()
        )

    transport = httpx.MockTransport(handle_request)
    client = httpx.AsyncClient(transport=transport)
    feed = IntrinioFeed(client, "test_key")

    asyncio.run(
        feed.fetch_equity(
            symbol="TEST",
            name="Test",
            share_class_figi="FIGI123",
            isin="ISIN456",
            cusip="CUSIP789",
        ),
    )

    assert "ISIN456" in attempts[1]


def test_fetch_equity_tries_cusip_third() -> None:
    """
    ARRANGE: CUSIP and other identifiers provided
    ACT:     call fetch_equity with all failing except symbol
    ASSERT:  third attempt uses CUSIP
    """
    attempts = []
    total_identifiers = 4

    def handle_request(request: httpx.Request) -> httpx.Response:
        attempts.append(str(request.url))
        return (
            httpx.Response(404)
            if len(attempts) < total_identifiers
            else make_intrinio_response()
        )

    transport = httpx.MockTransport(handle_request)
    client = httpx.AsyncClient(transport=transport)
    feed = IntrinioFeed(client, "test_key")

    asyncio.run(
        feed.fetch_equity(
            symbol="TEST",
            name="Test",
            share_class_figi="FIGI123",
            isin="ISIN456",
            cusip="CUSIP789",
        ),
    )

    assert "CUSIP789" in attempts[2]


def test_fetch_equity_tries_symbol_last() -> None:
    """
    ARRANGE: symbol and other identifiers provided
    ACT:     call fetch_equity with all failing except symbol
    ASSERT:  fourth attempt uses symbol
    """
    attempts = []
    total_identifiers = 4

    def handle_request(request: httpx.Request) -> httpx.Response:
        attempts.append(str(request.url))
        return (
            httpx.Response(404)
            if len(attempts) < total_identifiers
            else make_intrinio_response()
        )

    transport = httpx.MockTransport(handle_request)
    client = httpx.AsyncClient(transport=transport)
    feed = IntrinioFeed(client, "test_key")

    asyncio.run(
        feed.fetch_equity(
            symbol="TEST",
            name="Test",
            share_class_figi="FIGI123",
            isin="ISIN456",
            cusip="CUSIP789",
        ),
    )

    assert "TEST" in attempts[3]
