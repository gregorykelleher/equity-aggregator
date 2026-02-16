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
from equity_aggregator.storage import (
    load_cache,
    save_cache,
    save_cache_entry,
)

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
                            "downloadLink": ("https://example.com/download.zip"),
                        },
                    },
                },
                request=request,
            )

        return httpx.Response(200, content=zip_bytes, request=request)

    return handler


def _make_feed_with_index(
    isin_index: dict[str, str] | None,
    *,
    client_factory: object = None,
) -> GleifFeed:
    """
    Create a GleifFeed with a pre-populated ISIN index for testing.
    """
    feed = GleifFeed(cache_key=None, client_factory=client_factory)
    feed._isin_index = isin_index
    feed._loaded = True
    return feed


def _make_autocompletions_response(
    entries: list[tuple[str, str]],
) -> dict:
    """
    Build a GLEIF autocompletions API response payload.
    """
    return {
        "data": [
            {
                "attributes": {"value": name},
                "relationships": {
                    "lei-records": {"data": {"id": lei}},
                },
            }
            for name, lei in entries
        ],
    }


def _make_lei_records_response(
    entries: list[tuple[str, str]],
) -> dict:
    """
    Build a GLEIF lei-records API response payload.
    """
    return {
        "data": [
            {
                "id": lei,
                "attributes": {
                    "entity": {
                        "legalName": {"name": name},
                    },
                },
            }
            for name, lei in entries
        ],
    }


def _make_name_search_handler(
    autocompletions: list[tuple[str, str]],
    *,
    parents: list[tuple[str, str]] | None = None,
) -> callable:
    """
    Create a handler for name search API calls.

    Routes autocompletions, active-filter lei-records (filter[lei]),
    and parent lei-records (filter[owns]) to appropriate responses.
    """
    auto_payload = _make_autocompletions_response(autocompletions)
    active_leis = [lei for _, lei in autocompletions]
    active_payload = {"data": [{"id": lei} for lei in active_leis]}
    parent_payload = _make_lei_records_response(parents or [])

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "autocompletions" in url:
            return httpx.Response(200, json=auto_payload, request=request)
        if "filter%5Bowns%5D" in url or "filter[owns]" in url:
            return httpx.Response(200, json=parent_payload, request=request)
        if "lei-records" in url:
            return httpx.Response(200, json=active_payload, request=request)
        return httpx.Response(404, text="Not found", request=request)

    return handler


# --- GleifFeed initialisation ---


def test_gleif_feed_init_starts_with_none_index() -> None:
    """
    ARRANGE: create GleifFeed with configuration
    ACT:     check internal index
    ASSERT:  index starts as None (lazy loading)
    """
    feed = GleifFeed(cache_key="test", client_factory=None)

    assert feed._isin_index is None


# --- ISIN lookup (tier 1) ---


async def test_fetch_equity_returns_lei_from_isin_index() -> None:
    """
    ARRANGE: GleifFeed with valid ISIN index
    ACT:     call fetch_equity with known ISIN
    ASSERT:  returns LEI from index
    """
    feed = _make_feed_with_index(
        {"US0378331005": "529900T8BM49AURSDO55"},
    )

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0378331005",
    )

    assert actual["lei"] == "529900T8BM49AURSDO55"


async def test_fetch_equity_returns_dict_on_success() -> None:
    """
    ARRANGE: GleifFeed with valid ISIN index
    ACT:     call fetch_equity with known ISIN
    ASSERT:  returns dictionary
    """
    feed = _make_feed_with_index(
        {"US0378331005": "529900T8BM49AURSDO55"},
    )

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
    feed = _make_feed_with_index(
        {"US0378331005": "529900T8BM49AURSDO55"},
    )

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
    feed = _make_feed_with_index(
        {"US0378331005": "529900T8BM49AURSDO55"},
    )

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0378331005",
    )

    assert actual["symbol"] == "AAPL"


async def test_fetch_equity_ignores_extra_kwargs() -> None:
    """
    ARRANGE: GleifFeed with valid index
    ACT:     call fetch_equity with extra keyword arguments
    ASSERT:  returns successfully, extra kwargs are ignored
    """
    feed = _make_feed_with_index(
        {"US0378331005": "529900T8BM49AURSDO55"},
    )

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0378331005",
        figi="BBG000B9XRY4",
        exchange="NASDAQ",
    )

    assert actual["lei"] == "529900T8BM49AURSDO55"


# --- Name-based fallback (tier 2 & 3) ---


async def test_fetch_equity_falls_back_to_name_search() -> None:
    """
    ARRANGE: GleifFeed with empty ISIN index, API returning match
    ACT:     call fetch_equity with ISIN not in index
    ASSERT:  returns LEI from name-based API search
    """
    handler = _make_name_search_handler(
        [("Apple Inc.", "APPLE_API_LEI")],
    )
    feed = _make_feed_with_index(
        {},
        client_factory=make_client_factory(handler),
    )

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0000000000",
    )

    assert actual["lei"] == "APPLE_API_LEI"


async def test_fetch_equity_name_cache_hit_returns_cached_lei() -> None:
    """
    ARRANGE: GleifFeed with empty index, name cache pre-populated
    ACT:     call fetch_equity
    ASSERT:  returns cached LEI without API call
    """
    save_cache_entry("gleif_names", "Apple Inc.", "CACHED_LEI")

    def raising_handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("API should not be called on cache hit")

    feed = _make_feed_with_index(
        {},
        client_factory=make_client_factory(raising_handler),
    )

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0000000000",
    )

    assert actual["lei"] == "CACHED_LEI"


async def test_fetch_equity_cached_no_match_raises() -> None:
    """
    ARRANGE: GleifFeed with empty index, "no match" sentinel cached
    ACT:     call fetch_equity
    ASSERT:  raises LookupError without API call
    """
    save_cache_entry("gleif_names", "Unknown Corp", "")

    def raising_handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("API should not be called on cache hit")

    feed = _make_feed_with_index(
        {},
        client_factory=make_client_factory(raising_handler),
    )

    with pytest.raises(LookupError):
        await feed.fetch_equity(
            symbol="UNK",
            name="Unknown Corp",
            isin="US0000000000",
        )


async def test_fetch_equity_parent_traversal_returns_parent() -> None:
    """
    ARRANGE: API returns candidate, parent is a better match
    ACT:     call fetch_equity
    ASSERT:  returns the parent's LEI
    """
    handler = _make_name_search_handler(
        [("Volkswagen Financial Services AG", "VW_FIN_LEI")],
        parents=[("Volkswagen AG", "VW_PARENT_LEI")],
    )
    feed = _make_feed_with_index(
        {},
        client_factory=make_client_factory(handler),
    )

    actual = await feed.fetch_equity(
        symbol="VOW",
        name="Volkswagen AG",
        isin="DE0000000000",
    )

    assert actual["lei"] == "VW_PARENT_LEI"


async def test_fetch_equity_no_parents_returns_candidate() -> None:
    """
    ARRANGE: API returns candidate, no parents found
    ACT:     call fetch_equity
    ASSERT:  returns the candidate's LEI
    """
    handler = _make_name_search_handler(
        [("Apple Inc.", "APPLE_LEI")],
        parents=[],
    )
    feed = _make_feed_with_index(
        {},
        client_factory=make_client_factory(handler),
    )

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0000000000",
    )

    assert actual["lei"] == "APPLE_LEI"


async def test_fetch_equity_raises_when_both_lookups_fail() -> None:
    """
    ARRANGE: empty ISIN index, API returns no matching candidates
    ACT:     call fetch_equity
    ASSERT:  raises LookupError
    """
    handler = _make_name_search_handler([])
    feed = _make_feed_with_index(
        {},
        client_factory=make_client_factory(handler),
    )

    with pytest.raises(LookupError):
        await feed.fetch_equity(
            symbol="XYZ",
            name="Nonexistent Corp",
            isin="US9999999999",
        )


async def test_fetch_equity_isin_match_skips_name_search() -> None:
    """
    ARRANGE: GleifFeed with ISIN in index, API handler that raises
    ACT:     call fetch_equity with known ISIN
    ASSERT:  returns LEI without calling API
    """

    def raising_handler(request: httpx.Request) -> httpx.Response:
        if "autocompletions" in str(request.url):
            raise AssertionError("Name search should not be called")
        if "lei-records" in str(request.url):
            raise AssertionError("Parent lookup should not be called")
        return httpx.Response(404, text="Not found", request=request)

    feed = _make_feed_with_index(
        {"US0378331005": "529900T8BM49AURSDO55"},
        client_factory=make_client_factory(raising_handler),
    )

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0378331005",
    )

    assert actual["lei"] == "529900T8BM49AURSDO55"


async def test_fetch_equity_none_isin_falls_back_to_name() -> None:
    """
    ARRANGE: GleifFeed with valid index, isin=None, API returns match
    ACT:     call fetch_equity without ISIN
    ASSERT:  returns LEI from name-based search
    """
    handler = _make_name_search_handler(
        [("Apple Inc.", "APPLE_NAME_LEI")],
    )
    feed = _make_feed_with_index(
        {"US0378331005": "529900T8BM49AURSDO55"},
        client_factory=make_client_factory(handler),
    )

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin=None,
    )

    assert actual["lei"] == "APPLE_NAME_LEI"


async def test_fetch_equity_none_index_falls_back_to_name() -> None:
    """
    ARRANGE: GleifFeed with None index (download failed), API match
    ACT:     call fetch_equity
    ASSERT:  returns LEI from name-based search
    """
    handler = _make_name_search_handler(
        [("Apple Inc.", "APPLE_FALLBACK_LEI")],
    )
    feed = _make_feed_with_index(
        None,
        client_factory=make_client_factory(handler),
    )

    actual = await feed.fetch_equity(
        symbol="AAPL",
        name="Apple Inc.",
        isin="US0378331005",
    )

    assert actual["lei"] == "APPLE_FALLBACK_LEI"


# --- open_gleif_feed context manager ---


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
    ARRANGE: client_factory returning ZIP, index loaded via first fetch
    ACT:     call fetch_equity a second time on same feed
    ASSERT:  returns LEI without reloading
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    async with open_gleif_feed(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    ) as feed:
        await feed.fetch_equity(
            symbol="AAPL",
            name="Apple Inc.",
            isin="US0378331005",
        )
        actual = await feed.fetch_equity(
            symbol="AAPL",
            name="Apple Inc.",
            isin="US0378331005",
        )

    assert actual["lei"] == "529900T8BM49AURSDO55"


async def test_open_gleif_feed_handles_concurrent_fetch() -> None:
    """
    ARRANGE: client_factory returning ZIP, concurrent fetch requests
    ACT:     call fetch_equity concurrently from multiple tasks
    ASSERT:  all tasks return correct LEI
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    async with open_gleif_feed(
        cache_key=None,
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    ) as feed:
        results = await asyncio.gather(
            feed.fetch_equity(
                symbol="AAPL",
                name="Apple Inc.",
                isin="US0378331005",
            ),
            feed.fetch_equity(
                symbol="AAPL",
                name="Apple Inc.",
                isin="US0378331005",
            ),
            feed.fetch_equity(
                symbol="AAPL",
                name="Apple Inc.",
                isin="US0378331005",
            ),
        )

    assert all(result["lei"] == "529900T8BM49AURSDO55" for result in results)


async def test_ensure_index_loaded_returns_inside_lock() -> None:
    """
    ARRANGE: feed with lock held, waiter passes first check
    ACT:     waiter acquires lock after loader sets _loaded=True
    ASSERT:  returns immediately (inner _loaded check)
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
            feed._isin_index = {"US0378331005": "529900T8BM49AURSDO55"}
            feed._loaded = True

    async def waiter() -> dict[str, object]:
        waiter_ready.set()
        return await feed.fetch_equity(
            symbol="AAPL",
            name="Apple Inc.",
            isin="US0378331005",
        )

    _, actual = await asyncio.gather(loader(), waiter())

    assert actual["lei"] == "529900T8BM49AURSDO55"


async def test_open_gleif_feed_raises_when_both_fail() -> None:
    """
    ARRANGE: client_factory returning error, no name match possible
    ACT:     enter open_gleif_feed and call fetch_equity
    ASSERT:  raises LookupError
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "autocompletions" in url:
            return httpx.Response(200, json={"data": []}, request=request)
        return httpx.Response(500, json={"error": "Server error"}, request=request)

    async with open_gleif_feed(
        cache_key=None,
        client_factory=make_client_factory(handler),
    ) as feed:
        with pytest.raises(LookupError):
            await feed.fetch_equity(
                symbol="AAPL",
                name="Apple Inc.",
                isin="US0378331005",
            )


# --- Internal functions ---


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


async def test_get_index_returns_cached_index() -> None:
    """
    ARRANGE: pre-seeded cache with index
    ACT:     call _get_index with cache enabled
    ASSERT:  returns cached index without downloading
    """
    cache_key = "gleif_cached_index"
    cached_index = {"US0378331005": "529900T8BM49AURSDO55"}
    save_cache(cache_key, cached_index)

    def raising_handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Client should not be called")

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


async def test_get_index_returns_empty_dict_for_empty_csv() -> None:
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


def test_load_from_cache_returns_none_when_key_is_none() -> None:
    """
    ARRANGE: None cache key
    ACT:     call _load_from_cache
    ASSERT:  returns None
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


async def test_download_and_cache_skips_cache_when_none() -> None:
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
