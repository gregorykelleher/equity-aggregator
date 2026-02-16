# gleif/gleif.py

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import httpx

from equity_aggregator.storage import (
    load_cache,
    load_cache_entry,
    save_cache,
    save_cache_entry,
)

from ._utils import rank_candidates, select_best_parent
from .api import fetch_parents, search_by_name
from .download import download_and_build_isin_index

logger = logging.getLogger(__name__)

_NAME_CACHE_KEY = "gleif_names"
_NO_MATCH_SENTINEL = ""


@asynccontextmanager
async def open_gleif_feed(
    *,
    cache_key: str | None = "gleif",
    client_factory: Callable[[], httpx.AsyncClient] | None = None,
) -> AsyncIterator["GleifFeed"]:
    """
    Context manager to create a GleifFeed.

    Args:
        cache_key: Cache key for the index; defaults to "gleif".
        client_factory: Factory for HTTP client; defaults to make_client.

    Yields:
        GleifFeed with lazy-loaded index.
    """
    yield GleifFeed(cache_key=cache_key, client_factory=client_factory)


class GleifFeed:
    """
    Async GLEIF feed for LEI enrichment.

    Provides fetch_equity() to retrieve LEI data via a three-tier lookup:
    ISIN index, per-entry name cache, and GLEIF API name search with parent
    traversal. The ISIN index is loaded lazily on first call.
    """

    __slots__ = (
        "_api_semaphore",
        "_cache_key",
        "_client_factory",
        "_isin_index",
        "_loaded",
        "_lock",
    )

    def __init__(
        self,
        *,
        cache_key: str | None,
        client_factory: Callable[[], httpx.AsyncClient] | None,
    ) -> None:
        """
        Initialise with lazy loading configuration.

        Args:
            cache_key: Cache key for the index, or None to disable caching.
            client_factory: Factory for HTTP client, or None for default.
        """
        self._cache_key = cache_key
        self._client_factory = client_factory
        self._isin_index: dict[str, str] | None = None
        self._loaded = False
        self._lock = asyncio.Lock()
        self._api_semaphore = asyncio.Semaphore(1)

    async def fetch_equity(
        self,
        *,
        symbol: str,
        name: str,
        isin: str | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        """
        Fetch LEI data for an equity using ISIN lookup with name-based
        fallback.

        Attempts three tiers: ISIN index lookup, per-entry name cache, and
        GLEIF API name search with parent traversal.

        Returns:
            dict[str, object]: Dict containing name, symbol, isin, and lei.

        Raises:
            LookupError: If no LEI can be found via any method.
        """
        await self._ensure_index_loaded()

        lei = self._lookup_by_isin(isin)

        if lei is None:
            lei = await self._lookup_by_name(name)

        if lei is None:
            raise LookupError(f"No LEI found for {name} (ISIN: {isin})")

        return {
            "name": name,
            "symbol": symbol,
            "isin": isin,
            "lei": lei,
        }

    def _lookup_by_isin(self, isin: str | None) -> str | None:
        """
        Look up an LEI by ISIN in the bulk index.

        Returns:
            str | None: The LEI if found, otherwise None.
        """
        if isin is None or self._isin_index is None:
            return None

        return self._isin_index.get(isin)

    async def _lookup_by_name(self, name: str) -> str | None:
        """
        Look up an LEI by equity name via cache or GLEIF API search.

        Checks the per-entry cache first. On cache miss, queries the GLEIF
        autocompletions API, ranks candidates by fuzzy similarity, and
        optionally traverses parent entities for a better match.

        Returns:
            str | None: The LEI if found, otherwise None.
        """
        cached = load_cache_entry(_NAME_CACHE_KEY, name)
        if cached is not None:
            return _resolve_cached_value(cached)

        return await self._search_gleif_api(name)

    async def _search_gleif_api(
        self,
        name: str,
    ) -> str | None:
        """
        Query the GLEIF API for an LEI by name, with parent traversal.

        Acquires the API semaphore to serialise requests, searches by name,
        picks the best candidate via fuzzy matching, then checks parent
        entities for a potentially better match.

        Returns:
            str | None: The resolved LEI, or None if no match is found.
        """
        async with self._api_semaphore, self._client_factory() as client:
            lei = await _resolve_lei_from_api(name, client)

        _cache_name_result(name, lei)
        return lei

    async def _ensure_index_loaded(self) -> None:
        """
        Ensure the ISIN->LEI index is loaded exactly once.

        Uses a lock to prevent concurrent download attempts when multiple
        tasks call fetch_equity simultaneously before the index is loaded.
        """
        if self._loaded:
            return

        async with self._lock:
            if self._loaded:
                return

            self._isin_index = await _get_index(
                self._cache_key,
                client_factory=self._client_factory,
            )
            self._loaded = True


def _resolve_cached_value(cached: object) -> str | None:
    """
    Interpret a cached value, treating the sentinel as no match.

    Returns:
        str | None: The cached LEI, or None if the sentinel was stored.
    """
    if cached == _NO_MATCH_SENTINEL:
        return None
    return cached


def _cache_name_result(cache_key: str, lei: str | None) -> None:
    """
    Cache the result of a name-based LEI lookup.

    Stores the LEI on success, or a sentinel value to prevent repeated
    lookups for equities with no GLEIF match.
    """
    value = lei if lei is not None else _NO_MATCH_SENTINEL
    save_cache_entry(_NAME_CACHE_KEY, cache_key, value)


async def _resolve_lei_from_api(
    name: str,
    client: httpx.AsyncClient,
) -> str | None:
    """
    Resolve an LEI from the GLEIF API by name search and parent traversal.

    Returns:
        str | None: The best-matching LEI, or None if no match qualifies.
    """
    candidates = await search_by_name(name, client)
    ranked = rank_candidates(name, candidates)

    if not ranked:
        return None

    _, best_lei = ranked[0]
    return await _try_parent_traversal(name, best_lei, client)


async def _try_parent_traversal(
    equity_name: str,
    candidate_lei: str,
    client: httpx.AsyncClient,
) -> str:
    """
    Traverse to the parent issuer entity if one exists.

    Parent entities are the root issuers whose LEIs are required for
    XBRL lookups. Returns the parent's LEI when a parent exists,
    otherwise keeps the original candidate's LEI.

    Returns:
        str: The parent's LEI if one exists, otherwise the candidate's
            LEI.
    """
    parents = await fetch_parents(candidate_lei, client)
    parent_lei = select_best_parent(equity_name, parents)
    return parent_lei if parent_lei is not None else candidate_lei


async def _get_index(
    cache_key: str | None,
    *,
    client_factory: Callable[[], httpx.AsyncClient] | None = None,
) -> dict[str, str] | None:
    """
    Retrieve or build the ISIN->LEI index.

    Returns:
        dict[str, str] | None: ISIN->LEI mapping dict, or None if
            unavailable.
    """
    cached = _load_from_cache(cache_key)
    if cached is not None:
        return cached

    return await _download_and_cache(cache_key, client_factory)


def _load_from_cache(cache_key: str | None) -> dict[str, str] | None:
    """
    Load index from cache if available.

    Returns:
        dict[str, str] | None: ISIN->LEI mapping dict, or None if not
            cached.
    """
    if not cache_key:
        return None

    cached = load_cache(cache_key)
    if cached is not None:
        logger.info("Loaded %d GLEIF ISIN->LEI mappings from cache.", len(cached))

    return cached


async def _download_and_cache(
    cache_key: str | None,
    client_factory: Callable[[], httpx.AsyncClient] | None,
) -> dict[str, str] | None:
    """
    Download index and save to cache.

    Returns:
        dict[str, str] | None: ISIN->LEI mapping dict, or None if download
            failed.
    """
    try:
        index = await download_and_build_isin_index(client_factory=client_factory)
    except Exception as error:
        logger.error(
            "Failed to build GLEIF ISIN->LEI index: %s",
            error,
            exc_info=True,
        )
        return None

    if index and cache_key:
        save_cache(cache_key, index)

    return index
