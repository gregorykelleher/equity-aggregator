# yfinance/test_auth.py

import asyncio

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.auth import (
    CrumbManager,
)

pytestmark = pytest.mark.unit


async def test_init_creates_manager_with_crumb_url() -> None:
    """
    ARRANGE: crumb URL string
    ACT:     create CrumbManager instance
    ASSERT:  manager is created with URL
    """
    manager = CrumbManager("https://example.com/crumb")

    assert manager._crumb_url == "https://example.com/crumb"


async def test_init_sets_crumb_to_none() -> None:
    """
    ARRANGE: create CrumbManager
    ACT:     check initial crumb state
    ASSERT:  crumb is None
    """
    manager = CrumbManager("https://example.com/crumb")

    assert manager._crumb is None


async def test_init_creates_lock() -> None:
    """
    ARRANGE: create CrumbManager
    ACT:     check lock instance
    ASSERT:  lock is asyncio.Lock
    """
    manager = CrumbManager("https://example.com/crumb")

    assert isinstance(manager._lock, asyncio.Lock)


async def test_crumb_property_returns_none_when_not_fetched() -> None:
    """
    ARRANGE: fresh CrumbManager instance
    ACT:     access crumb property
    ASSERT:  returns None
    """
    manager = CrumbManager("https://example.com/crumb")

    assert manager.crumb is None


async def test_crumb_property_returns_cached_value() -> None:
    """
    ARRANGE: CrumbManager with cached crumb
    ACT:     access crumb property
    ASSERT:  returns cached value
    """
    manager = CrumbManager("https://example.com/crumb")
    manager._crumb = "test-crumb-value"

    assert manager.crumb == "test-crumb-value"


async def test_clear_sets_crumb_to_none() -> None:
    """
    ARRANGE: CrumbManager with cached crumb
    ACT:     call clear()
    ASSERT:  crumb is set to None
    """
    manager = CrumbManager("https://example.com/crumb")
    manager._crumb = "existing-crumb"

    manager.clear()

    assert manager._crumb is None


async def test_ensure_crumb_returns_cached_crumb_on_fast_path() -> None:
    """
    ARRANGE: CrumbManager with pre-cached crumb
    ACT:     call ensure_crumb()
    ASSERT:  returns cached crumb without fetching
    """
    manager = CrumbManager("https://example.com/crumb")
    manager._crumb = "cached-crumb"

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        raise AssertionError("fetch should not be called")

    actual = await manager.ensure_crumb("AAPL", fetch)

    assert actual == "cached-crumb"


async def test_ensure_crumb_bootstraps_and_returns_crumb() -> None:
    """
    ARRANGE: CrumbManager without cached crumb, mock fetch returning crumb
    ACT:     call ensure_crumb()
    ASSERT:  returns bootstrapped crumb
    """
    manager = CrumbManager("https://example.com/crumb")
    call_count = []

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        call_count.append(url)
        if "crumb" in url:
            return httpx.Response(
                200,
                text="new-crumb-value",
                request=httpx.Request("GET", url),
            )
        return httpx.Response(200, text="", request=httpx.Request("GET", url))

    actual = await manager.ensure_crumb("MSFT", fetch)

    assert actual == "new-crumb-value"


async def test_ensure_crumb_stores_fetched_crumb() -> None:
    """
    ARRANGE: CrumbManager without cached crumb, mock fetch
    ACT:     call ensure_crumb()
    ASSERT:  crumb is cached in _crumb
    """
    manager = CrumbManager("https://example.com/crumb")

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        if "crumb" in url:
            return httpx.Response(
                200,
                text="stored-crumb",
                request=httpx.Request("GET", url),
            )
        return httpx.Response(200, text="", request=httpx.Request("GET", url))

    await manager.ensure_crumb("NFLX", fetch)

    assert manager._crumb == "stored-crumb"


async def test_ensure_crumb_double_checked_locking() -> None:
    """
    ARRANGE: CrumbManager, concurrent calls to ensure_crumb
    ACT:     call ensure_crumb() concurrently
    ASSERT:  fetch is called only once
    """
    manager = CrumbManager("https://example.com/crumb")
    fetch_count = []

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        if "crumb" in url:
            fetch_count.append(1)
            await asyncio.sleep(0.01)
            return httpx.Response(
                200,
                text="concurrent-crumb",
                request=httpx.Request("GET", url),
            )
        return httpx.Response(200, text="", request=httpx.Request("GET", url))

    await asyncio.gather(
        manager.ensure_crumb("AAPL", fetch),
        manager.ensure_crumb("AAPL", fetch),
        manager.ensure_crumb("AAPL", fetch),
    )

    assert len(fetch_count) == 1


async def test_bootstrap_makes_seed_requests() -> None:
    """
    ARRANGE: CrumbManager, mock fetch tracking URLs
    ACT:     call _bootstrap()
    ASSERT:  all seed URLs are requested
    """
    manager = CrumbManager("https://example.com/crumb")
    requested_urls = []

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        requested_urls.append(url)
        if "crumb" in url:
            return httpx.Response(200, text="crumb", request=httpx.Request("GET", url))
        return httpx.Response(200, text="", request=httpx.Request("GET", url))

    await manager._bootstrap("TSLA", fetch)

    assert "https://fc.yahoo.com" in requested_urls


async def test_bootstrap_includes_ticker_in_seed() -> None:
    """
    ARRANGE: CrumbManager, mock fetch tracking URLs
    ACT:     call _bootstrap() with specific ticker
    ASSERT:  ticker is included in quote URL
    """
    manager = CrumbManager("https://example.com/crumb")
    requested_urls = []

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        requested_urls.append(url)
        if "crumb" in url:
            return httpx.Response(200, text="crumb", request=httpx.Request("GET", url))
        return httpx.Response(200, text="", request=httpx.Request("GET", url))

    await manager._bootstrap("GOOG", fetch)

    assert "https://finance.yahoo.com/quote/GOOG" in requested_urls


async def test_bootstrap_fetches_crumb_url() -> None:
    """
    ARRANGE: CrumbManager with specific crumb URL, mock fetch
    ACT:     call _bootstrap()
    ASSERT:  crumb URL is requested
    """
    manager = CrumbManager("https://custom.example.com/api/crumb")
    requested_urls = []

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        requested_urls.append(url)
        if "crumb" in url:
            return httpx.Response(200, text="crumb", request=httpx.Request("GET", url))
        return httpx.Response(200, text="", request=httpx.Request("GET", url))

    await manager._bootstrap("AMZN", fetch)

    assert "https://custom.example.com/api/crumb" in requested_urls


async def test_bootstrap_strips_whitespace_from_crumb() -> None:
    """
    ARRANGE: CrumbManager, fetch returns crumb with whitespace
    ACT:     call _bootstrap()
    ASSERT:  crumb is stripped of whitespace
    """
    manager = CrumbManager("https://example.com/crumb")

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        if "crumb" in url:
            return httpx.Response(
                200,
                text="  crumb-value  ",
                request=httpx.Request("GET", url),
            )
        return httpx.Response(200, text="", request=httpx.Request("GET", url))

    await manager._bootstrap("AAPL", fetch)

    assert manager._crumb == "crumb-value"


async def test_bootstrap_strips_quotes_from_crumb() -> None:
    """
    ARRANGE: CrumbManager, fetch returns quoted crumb
    ACT:     call _bootstrap()
    ASSERT:  quotes are stripped from crumb
    """
    manager = CrumbManager("https://example.com/crumb")

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        if "crumb" in url:
            return httpx.Response(
                200,
                text='"quoted-crumb"',
                request=httpx.Request("GET", url),
            )
        return httpx.Response(200, text="", request=httpx.Request("GET", url))

    await manager._bootstrap("MSFT", fetch)

    assert manager._crumb == "quoted-crumb"


async def test_bootstrap_raises_on_crumb_fetch_failure() -> None:
    """
    ARRANGE: CrumbManager, fetch returns 403 for crumb URL
    ACT:     call _bootstrap()
    ASSERT:  raises HTTPStatusError
    """
    manager = CrumbManager("https://example.com/crumb")

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        if "crumb" in url:
            response = httpx.Response(403, text="", request=httpx.Request("GET", url))
            response.request = httpx.Request("GET", url)
            return response
        return httpx.Response(200, text="", request=httpx.Request("GET", url))

    with pytest.raises(httpx.HTTPStatusError):
        await manager._bootstrap("AAPL", fetch)


async def test_ensure_crumb_returns_same_crumb_after_bootstrap() -> None:
    """
    ARRANGE: CrumbManager, multiple ensure_crumb calls
    ACT:     call ensure_crumb() twice sequentially
    ASSERT:  both calls return same crumb
    """
    manager = CrumbManager("https://example.com/crumb")

    async def fetch(url: str, params: dict[str, str]) -> httpx.Response:
        if "crumb" in url:
            return httpx.Response(
                200,
                text="persistent-crumb",
                request=httpx.Request("GET", url),
            )
        return httpx.Response(200, text="", request=httpx.Request("GET", url))

    first = await manager.ensure_crumb("AAPL", fetch)
    second = await manager.ensure_crumb("MSFT", fetch)

    assert first == second
    