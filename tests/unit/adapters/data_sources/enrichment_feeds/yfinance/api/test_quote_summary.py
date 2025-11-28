# _api/test_quote_summary.py

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.api.quote_summary import (
    _flatten_module_dicts,
    get_quote_summary,
)
from tests.unit.adapters.data_sources.enrichment_feeds.yfinance._helpers import (
    make_session,
)

pytestmark = pytest.mark.unit


def test_flatten_module_dicts_merges_and_overwrites() -> None:
    """
    ARRANGE: two overlapping module dictionaries
    ACT:     call _flatten_module_dicts
    ASSERT:  keys from later module overwrite earlier ones
    """

    modules = ("price", "summaryDetail")
    payload = {
        "price": {"currency": "USD", "regularMarketPrice": 100},
        "summaryDetail": {"currency": "GBP", "dividendYield": 0.02},
    }

    merged = _flatten_module_dicts(modules, payload)

    assert merged == {
        "currency": "GBP",
        "regularMarketPrice": 100,
        "dividendYield": 0.02,
    }


async def test_get_quote_summary_returns_flattened_data_on_success() -> None:
    """
    ARRANGE: quoteSummary endpoint returns two modules
    ACT:     call get_quote_summary
    ASSERT:  flattened dict is returned
    """

    raw = {
        "quoteSummary": {
            "result": [
                {
                    "price": {"regularMarketPrice": 150},
                    "summaryDetail": {"marketCap": 2_000_000_000},
                },
            ],
        },
    }
    session = make_session(lambda r: httpx.Response(200, json=raw, request=r))

    actual = await get_quote_summary(
        session,
        "AAPL",
        modules=("price", "summaryDetail"),
    )

    assert actual == {"regularMarketPrice": 150, "marketCap": 2_000_000_000}


async def test_get_quote_summary_uses_fallback_on_500() -> None:
    """
    ARRANGE: mock 500 response for quoteSummary, 200 for fallback
    ACT:     call get_quote_summary
    ASSERT:  fallback endpoint is used and returns data
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if "quoteSummary" in str(request.url):
            return httpx.Response(500, json={}, request=request)
        if "v7/finance/quote" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "quoteResponse": {
                        "result": [{"symbol": "MSFT", "regularMarketPrice": 150}],
                    },
                },
                request=request,
            )
        return httpx.Response(200, json={}, request=request)

    session = make_session(handler)

    actual = await get_quote_summary(session, "MSFT")

    assert actual is not None


async def test_get_quote_summary_returns_none_when_empty_result() -> None:
    """
    ARRANGE: quoteSummary returns empty result array
    ACT:     call get_quote_summary
    ASSERT:  returns None
    """
    raw = {"quoteSummary": {"result": []}}
    session = make_session(lambda r: httpx.Response(200, json=raw, request=r))

    actual = await get_quote_summary(session, "NFLX")

    assert actual is None


async def test_get_quote_summary_raises_on_429() -> None:
    """
    ARRANGE: stub session that always returns HTTP 429
    ACT:     invoke get_quote_summary
    ASSERT:  LookupError with 'HTTP 429' is raised
    """
    resp_429 = httpx.Response(
        httpx.codes.TOO_MANY_REQUESTS,
        request=httpx.Request(
            "GET",
            "https://example.com/v10/finance/quoteSummary/XYZ",
        ),
    )

    class _Config:
        modules = ("price",)
        quote_summary_primary_url = "https://example.com/v10/finance/quoteSummary/"
        quote_summary_fallback_url = "https://example.com/v7/finance/quote"

    class _Session:
        def __init__(self) -> None:
            self.config = _Config()

        async def get(self, _url: str, *, params: dict | None = None) -> httpx.Response:
            return resp_429

    session = _Session()

    with pytest.raises(LookupError) as exc_info:
        await get_quote_summary(session, "XYZ", modules=("price",))

    assert "HTTP 429" in str(exc_info.value)


async def test_get_quote_summary_fallback_raises_on_non_200() -> None:
    """
    ARRANGE: primary returns 500, fallback returns 404
    ACT:     call get_quote_summary
    ASSERT:  LookupError is raised for fallback 404 status
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if "quoteSummary" in str(request.url):
            return httpx.Response(500, json={}, request=request)
        return httpx.Response(404, json={}, request=request)

    session = make_session(handler)

    with pytest.raises(LookupError) as exc_info:
        await get_quote_summary(session, "INVALID")

    assert "HTTP 404" in str(exc_info.value)


async def test_get_quote_summary_fallback_returns_none_on_empty_result() -> None:
    """
    ARRANGE: primary returns 500, fallback returns empty result array
    ACT:     call get_quote_summary
    ASSERT:  returns None
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if "quoteSummary" in str(request.url):
            return httpx.Response(500, json={}, request=request)
        return httpx.Response(
            200,
            json={"quoteResponse": {"result": []}},
            request=request,
        )

    session = make_session(handler)

    actual = await get_quote_summary(session, "EMPTY")

    assert actual is None


def test_flatten_module_dicts_skips_non_dict_values() -> None:
    """
    ARRANGE: payload with non-dict module values
    ACT:     call _flatten_module_dicts
    ASSERT:  non-dict values are skipped
    """
    modules = ("price", "summaryDetail", "invalid")
    payload = {
        "price": {"currency": "USD"},
        "summaryDetail": None,
        "invalid": "not a dict",
    }

    merged = _flatten_module_dicts(modules, payload)

    assert merged == {"currency": "USD"}
