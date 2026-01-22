# gleif/test_gleif.py

import asyncio
import io
import zipfile

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif.gleif import (
    GleifFeed,
    _download_and_cache,
    _get_index,
    _load_from_cache,
    open_gleif_feed,
)
from equity_aggregator.storage import load_cache, save_cache

from ._helpers import make_client_factory

pytestmark = pytest.mark.unit


def _create_zip_bytes(csv_content: str) -> bytes:
    """
    Create a ZIP file containing a CSV in memory.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("isin_lei.csv", csv_content)
    return buffer.getvalue()


def _make_gleif_handler(zip_bytes: bytes) -> callable:
    """
    Create a handler that returns metadata then ZIP content.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)

        if "isin-lei/latest" in url:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "test",
                        "attributes": {
                            "downloadLink": "https://example.com/download.zip",
                        },
                    },
                },
                request=request,
            )

        return httpx.Response(200, content=zip_bytes, request=request)

    return handler


def _make_feed_with_index(index: dict[str, str] | None) -> GleifFeed:
    """
    Create a GleifFeed with a pre-populated index for testing.
    """
    feed = GleifFeed(cache_key=None, client_factory=None)
    feed._index = index
    feed._loaded = True
    return feed


def test_gleif_feed_init_starts_with_none_index() -> None:
    """
    ARRANGE: create GleifFeed with configuration
    ACT:     check internal index
    ASSERT:  index starts as None (lazy loading)
    """
    feed = GleifFeed(cache_key="test", client_factory=None)

    assert feed._index is None


async def test_fetch_equity_raises_when_index_is_none() -> None:
    """
    ARRANGE: GleifFeed with None index (download failed)
    ACT:     call fetch_equity
    ASSERT:  raises LookupError indicating index unavailable
    """
    feed = _make_feed_with_index(None)

    with pytest.raises(LookupError) as exc_info:
        await feed.fetch_equity(symbol="AAPL", name="Apple Inc.", isin="US0378331005")

    assert "GLEIF index unavailable" in str(exc_info.value)


async def test_fetch_equity_raises_when_isin_is_none() -> None:
    """
    ARRANGE: GleifFeed with valid index, but no ISIN provided
    ACT:     call fetch_equity without isin parameter
    ASSERT:  raises LookupError indicating no ISIN provided
    """
    feed = _make_feed_with_index({"US0378331005": "529900T8BM49AURSDO55"})

    with pytest.raises(LookupError) as exc_info:
        await feed.fetch_equity(symbol="AAPL", name="Apple Inc.", isin=None)

    assert "No ISIN provided" in str(exc_info.value)


async def test_fetch_equity_raises_when_isin_not_in_index() -> None:
    """
    ARRANGE: GleifFeed with valid index, ISIN not present in index
    ACT:     call fetch_equity with unknown ISIN
    ASSERT:  raises LookupError indicating no LEI found
    """
    feed = _make_feed_with_index({"US0378331005": "529900T8BM49AURSDO55"})

    with pytest.raises(LookupError) as exc_info:
        await feed.fetch_equity(
            symbol="MSFT",
            name="Microsoft Corp",
            isin="US5949181045",
        )

    assert "No LEI found for ISIN US5949181045" in str(exc_info.value)


async def test_fetch_equity_returns_dict_on_success() -> None:
    """
    ARRANGE: GleifFeed with valid index containing ISIN
    ACT:     call fetch_equity with known ISIN
    ASSERT:  returns dictionary
    """
    feed = _make_feed_with_index({"US0378331005": "529900T8BM49AURSDO55"})

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0378331005",
    )

    assert isinstance(actual, dict)


async def test_fetch_equity_returns_name() -> None:
    """
    ARRANGE: GleifFeed with valid index
    ACT:     call fetch_equity with known ISIN
    ASSERT:  returned dict contains passed name
    """
    feed = _make_feed_with_index({"US0378331005": "529900T8BM49AURSDO55"})

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0378331005",
    )

    assert actual["name"] == "Apple Inc."


async def test_fetch_equity_returns_symbol() -> None:
    """
    ARRANGE: GleifFeed with valid index
    ACT:     call fetch_equity with known ISIN
    ASSERT:  returned dict contains passed symbol
    """
    feed = _make_feed_with_index({"US0378331005": "529900T8BM49AURSDO55"})

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0378331005",
    )

    assert actual["symbol"] == "AAPL"


async def test_fetch_equity_returns_lei() -> None:
    """
    ARRANGE: GleifFeed with valid index
    ACT:     call fetch_equity with known ISIN
    ASSERT:  returned dict contains LEI from index
    """
    feed = _make_feed_with_index({"US0378331005": "529900T8BM49AURSDO55"})

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0378331005",
    )

    assert actual["lei"] == "529900T8BM49AURSDO55"


async def test_fetch_equity_uppercases_isin_for_lookup() -> None:
    """
    ARRANGE: GleifFeed with uppercase ISIN in index
    ACT:     call fetch_equity with lowercase ISIN
    ASSERT:  successfully finds LEI (lookup is case-insensitive)
    """
    feed = _make_feed_with_index({"US0378331005": "529900T8BM49AURSDO55"})

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="us0378331005",
    )

    assert actual["lei"] == "529900T8BM49AURSDO55"


async def test_fetch_equity_ignores_extra_kwargs() -> None:
    """
    ARRANGE: GleifFeed with valid index
    ACT:     call fetch_equity with extra keyword arguments
    ASSERT:  returns successfully, extra kwargs are ignored
    """
    feed = _make_feed_with_index({"US0378331005": "529900T8BM49AURSDO55"})

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0378331005",
        figi="BBG000B9XRY4",
        exchange="NASDAQ",
    )

    assert actual["lei"] == "529900T8BM49AURSDO55"


async def test_fetch_equity_with_empty_index_raises() -> None:
    """
    ARRANGE: GleifFeed with empty index
    ACT:     call fetch_equity
    ASSERT:  raises LookupError indicating no LEI found
    """
    feed = _make_feed_with_index({})

    with pytest.raises(LookupError) as exc_info:
        await feed.fetch_equity(symbol="AAPL", name="Apple Inc.", isin="US0378331005")

    assert "No LEI found" in str(exc_info.value)


async def test_open_gleif_feed_yields_gleif_feed_instance() -> None:
    """
    ARRANGE: client_factory returning valid metadata and ZIP
    ACT:     enter open_gleif_feed context
    ASSERT:  yields GleifFeed instance
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    async with open_gleif_feed(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    ) as feed:
        actual = feed

    assert isinstance(actual, GleifFeed)


async def test_open_gleif_feed_loads_index_on_fetch() -> None:
    """
    ARRANGE: client_factory returning ZIP with ISIN->LEI mapping
    ACT:     enter open_gleif_feed context and call fetch_equity
    ASSERT:  feed successfully looks up LEI
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    async with open_gleif_feed(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    ) as feed:
        actual = await feed.fetch_equity(
            symbol="AAPL",
            name="Apple Inc.",
            isin="US0378331005",
        )

    assert actual["lei"] == "529900T8BM49AURSDO55"


async def test_open_gleif_feed_skips_reload_on_subsequent_fetch() -> None:
    """
    ARRANGE: client_factory returning ZIP, index already loaded via first fetch
    ACT:     call fetch_equity a second time on same feed
    ASSERT:  returns LEI without reloading (exercises _loaded early return)
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    async with open_gleif_feed(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    ) as feed:
        await feed.fetch_equity(symbol="AAPL", name="Apple Inc.", isin="US0378331005")
        actual = await feed.fetch_equity(
            symbol="AAPL",
            name="Apple Inc.",
            isin="US0378331005",
        )

    assert actual["lei"] == "529900T8BM49AURSDO55"


async def test_open_gleif_feed_handles_concurrent_fetch() -> None:
    """
    ARRANGE: client_factory returning ZIP, multiple concurrent fetch requests
    ACT:     call fetch_equity concurrently from multiple tasks
    ASSERT:  all tasks return correct LEI (exercises double-checked locking)
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    async with open_gleif_feed(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    ) as feed:
        results = await asyncio.gather(
            feed.fetch_equity(symbol="AAPL", name="Apple Inc.", isin="US0378331005"),
            feed.fetch_equity(symbol="AAPL", name="Apple Inc.", isin="US0378331005"),
            feed.fetch_equity(symbol="AAPL", name="Apple Inc.", isin="US0378331005"),
        )

    assert all(result["lei"] == "529900T8BM49AURSDO55" for result in results)


async def test_ensure_index_loaded_returns_when_loaded_inside_lock() -> None:
    """
    ARRANGE: feed with lock held, waiter passes first check while _loaded=False
    ACT:     waiter acquires lock after loader sets _loaded=True
    ASSERT:  returns immediately (exercises inner _loaded check at line 118)
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)
    feed = GleifFeed(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )
    waiter_ready = asyncio.Event()

    async def loader() -> None:
        async with feed._lock:
            await waiter_ready.wait()
            feed._index = {"US0378331005": "529900T8BM49AURSDO55"}
            feed._loaded = True

    async def waiter() -> dict[str, object]:
        waiter_ready.set()
        return await feed.fetch_equity(
            symbol="AAPL", name="Apple Inc.", isin="US0378331005"
        )

    _, actual = await asyncio.gather(loader(), waiter())

    assert actual["lei"] == "529900T8BM49AURSDO55"


async def test_open_gleif_feed_raises_on_fetch_when_download_fails() -> None:
    """
    ARRANGE: client_factory returning error
    ACT:     enter open_gleif_feed context and call fetch_equity
    ASSERT:  raises LookupError indicating index unavailable
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "Server error"}, request=request)

    async with open_gleif_feed(
        cache_key=None,
        client_factory=make_client_factory(handler),
    ) as feed:
        with pytest.raises(LookupError) as exc_info:
            await feed.fetch_equity(
                symbol="AAPL",
                name="Apple Inc.",
                isin="US0378331005",
            )

    assert "GLEIF index unavailable" in str(exc_info.value)


async def test_get_index_returns_dict_on_success() -> None:
    """
    ARRANGE: client_factory returning valid metadata and ZIP
    ACT:     call _get_index with cache disabled
    ASSERT:  returns dictionary
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await _get_index(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert isinstance(actual, dict)


async def test_get_index_returns_mappings() -> None:
    """
    ARRANGE: client_factory returning ZIP with ISIN->LEI mappings
    ACT:     call _get_index with cache disabled
    ASSERT:  returns index with correct mappings
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await _get_index(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert actual["US0378331005"] == "529900T8BM49AURSDO55"


async def test_get_index_returns_none_on_failure() -> None:
    """
    ARRANGE: client_factory returning error for metadata
    ACT:     call _get_index with cache disabled
    ASSERT:  returns None
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "Server error"}, request=request)

    actual = await _get_index(
        cache_key=None,
        client_factory=make_client_factory(handler),
    )

    assert actual is None


async def test_get_index_returns_cached_index_when_available() -> None:
    """
    ARRANGE: pre-seeded cache with index
    ACT:     call _get_index with cache enabled
    ASSERT:  returns cached index without downloading
    """
    cache_key = "gleif_cached_index"
    cached_index = {"US0378331005": "529900T8BM49AURSDO55"}
    save_cache(cache_key, cached_index)

    def raising_handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Client should not be called when cache exists")

    actual = await _get_index(
        cache_key=cache_key,
        client_factory=make_client_factory(raising_handler),
    )

    assert actual == cached_index


async def test_get_index_saves_to_cache_after_download() -> None:
    """
    ARRANGE: empty cache, client_factory returning valid data
    ACT:     call _get_index with cache enabled
    ASSERT:  index is saved to cache
    """
    cache_key = "gleif_save_cache"
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    await _get_index(
        cache_key=cache_key,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert load_cache(cache_key) == {"US0378331005": "529900T8BM49AURSDO55"}


async def test_get_index_returns_empty_dict_when_csv_has_no_data() -> None:
    """
    ARRANGE: client_factory returning ZIP with headers-only CSV
    ACT:     call _get_index with cache disabled
    ASSERT:  returns empty dictionary
    """
    csv_content = "LEI,ISIN\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await _get_index(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert actual == {}


def test_load_from_cache_returns_none_when_cache_key_is_none() -> None:
    """
    ARRANGE: None cache key
    ACT:     call _load_from_cache
    ASSERT:  returns None without attempting cache lookup
    """
    actual = _load_from_cache(None)

    assert actual is None


def test_load_from_cache_returns_none_when_not_cached() -> None:
    """
    ARRANGE: cache key for non-existent entry
    ACT:     call _load_from_cache
    ASSERT:  returns None
    """
    actual = _load_from_cache("nonexistent_cache_key")

    assert actual is None


def test_load_from_cache_returns_cached_index() -> None:
    """
    ARRANGE: pre-seeded cache with index
    ACT:     call _load_from_cache
    ASSERT:  returns cached index
    """
    cache_key = "gleif_load_from_cache_test"
    cached_index = {"US0378331005": "529900T8BM49AURSDO55"}
    save_cache(cache_key, cached_index)

    actual = _load_from_cache(cache_key)

    assert actual == cached_index


async def test_download_and_cache_returns_index_on_success() -> None:
    """
    ARRANGE: client_factory returning valid data
    ACT:     call _download_and_cache
    ASSERT:  returns index
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await _download_and_cache(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert actual == {"US0378331005": "529900T8BM49AURSDO55"}


async def test_download_and_cache_saves_to_cache() -> None:
    """
    ARRANGE: client_factory returning valid data, cache key provided
    ACT:     call _download_and_cache
    ASSERT:  index is saved to cache
    """
    cache_key = "gleif_download_and_cache_test"
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    await _download_and_cache(
        cache_key=cache_key,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert load_cache(cache_key) == {"US0378331005": "529900T8BM49AURSDO55"}


async def test_download_and_cache_returns_none_on_failure() -> None:
    """
    ARRANGE: client_factory returning error
    ACT:     call _download_and_cache
    ASSERT:  returns None
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "Server error"}, request=request)

    actual = await _download_and_cache(
        cache_key=None,
        client_factory=make_client_factory(handler),
    )

    assert actual is None


async def test_download_and_cache_skips_cache_when_key_is_none() -> None:
    """
    ARRANGE: client_factory returning valid data, no cache key
    ACT:     call _download_and_cache
    ASSERT:  returns index without caching
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await _download_and_cache(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert actual == {"US0378331005": "529900T8BM49AURSDO55"}
