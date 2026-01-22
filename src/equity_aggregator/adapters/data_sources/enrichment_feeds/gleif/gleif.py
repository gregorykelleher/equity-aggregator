# gleif/gleif.py

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import httpx

from equity_aggregator.storage import load_cache, save_cache

from .download import download_and_build_index

logger = logging.getLogger(__name__)


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

    Provides fetch_equity() to retrieve LEI data by ISIN.
    The ISIN->LEI index is loaded lazily on first call.
    """

    __slots__ = ("_cache_key", "_client_factory", "_index", "_loaded", "_lock")

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
        self._index: dict[str, str] | None = None
        self._loaded = False
        self._lock = asyncio.Lock()

    async def fetch_equity(
        self,
        *,
        symbol: str,
        name: str,
        isin: str | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        """
        Fetch LEI data for an equity using its ISIN.

        Args:
            symbol: Ticker symbol of the equity.
            name: Full name of the equity.
            isin: ISIN identifier for LEI lookup.
            **kwargs: Additional identifiers (ignored by GLEIF).

        Returns:
            Dict containing name, symbol, and lei.

        Raises:
            LookupError: If no LEI can be found.
        """
        if isin is None:
            raise LookupError("No ISIN provided for LEI lookup")

        await self._ensure_index_loaded()

        if self._index is None:
            raise LookupError("GLEIF index unavailable")

        lei = self._index.get(isin.upper())

        if lei is None:
            raise LookupError(f"No LEI found for ISIN {isin}")

        return {
            "name": name,
            "symbol": symbol,
            "isin": isin,
            "lei": lei,
        }

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

            self._index = await _get_index(
                self._cache_key,
                client_factory=self._client_factory,
            )
            self._loaded = True


async def _get_index(
    cache_key: str | None,
    *,
    client_factory: Callable[[], httpx.AsyncClient] | None = None,
) -> dict[str, str] | None:
    """
    Retrieve or build the ISIN->LEI index.

    Args:
        cache_key: Cache key for the index, or None to disable caching.
        client_factory: Factory for HTTP client, or None for default.

    Returns:
        ISIN->LEI mapping dict, or None if unavailable.
    """
    cached = _load_from_cache(cache_key)
    if cached is not None:
        return cached

    return await _download_and_cache(cache_key, client_factory)


def _load_from_cache(cache_key: str | None) -> dict[str, str] | None:
    """
    Load index from cache if available.

    Args:
        cache_key: Cache key for the index, or None to disable caching.

    Returns:
        ISIN->LEI mapping dict, or None if not cached.
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

    Args:
        cache_key: Cache key for the index, or None to disable caching.
        client_factory: Factory for HTTP client, or None for default.

    Returns:
        ISIN->LEI mapping dict, or None if download failed.
    """
    try:
        index = await download_and_build_index(client_factory=client_factory)
    except Exception as error:
        logger.error("Failed to build GLEIF ISIN->LEI index: %s", error, exc_info=True)
        return None

    if index and cache_key:
        save_cache(cache_key, index)
        logger.info("Saved %d GLEIF ISIN->LEI mappings to cache.", len(index))

    return index
