# intrinio/intrinio.py

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from httpx import AsyncClient

from equity_aggregator.adapters.data_sources._utils import make_client
from equity_aggregator.schemas import IntrinioFeedData
from equity_aggregator.storage import (
    load_cache_entry,
    save_cache_entry,
)

logger = logging.getLogger(__name__)

_INTRINIO_BASE_URL = "https://api-v2.intrinio.com/securities"


@asynccontextmanager
async def open_intrinio_feed() -> AsyncIterator["IntrinioFeed"]:
    """
    Context manager to create and close an IntrinioFeed.

    Reads the INTRINIO_API_KEY from environment variables and creates
    an HTTP client for making API requests.

    Yields:
        IntrinioFeed with active HTTP client.

    Raises:
        ValueError: If INTRINIO_API_KEY environment variable is not set.
    """
    api_key = os.getenv("INTRINIO_API_KEY")
    if not api_key:
        raise ValueError("INTRINIO_API_KEY environment variable not set")

    client = make_client()
    try:
        yield IntrinioFeed(client, api_key)
    finally:
        await client.aclose()


class IntrinioFeed:
    """
    Async Intrinio feed with caching and identifier fallback.

    Provides fetch_equity() to retrieve equity data by trying multiple
    identifiers in priority order: share_class_figi, ISIN, CUSIP, symbol.

    Attributes:
        model: IntrinioFeedData schema class.
    """

    __slots__ = ("_client", "_api_key")

    model = IntrinioFeedData

    def __init__(self, client: AsyncClient, api_key: str) -> None:
        """
        Initialise with an active AsyncClient and API key.

        Args:
            client: The HTTP client for making requests.
            api_key: Intrinio API key for authentication.
        """
        self._client = client
        self._api_key = api_key

    async def fetch_equity(
        self,
        *,
        symbol: str,
        name: str,
        isin: str | None = None,
        cusip: str | None = None,
        share_class_figi: str | None = None,
        **kwargs: object,
    ) -> dict:
        """
        Fetch enriched equity data using identifier fallback chain.

        Tries identifiers in priority order until one succeeds.

        Args:
            symbol: Ticker symbol of the equity.
            name: Full name of the equity.
            isin: ISIN identifier, if available.
            cusip: CUSIP identifier, if available.
            share_class_figi: Share class FIGI identifier, if available.
            **kwargs: Additional parameters (ignored).

        Returns:
            Enriched equity data from Intrinio API.

        Raises:
            LookupError: If no matching equity data is found for any identifier.
        """
        if record := load_cache_entry("intrinio_equities", symbol):
            return record

        identifiers = tuple(filter(None, (share_class_figi, isin, cusip, symbol)))

        for identifier in identifiers:
            try:
                data = await self._fetch_quote(identifier)
                save_cache_entry("intrinio_equities", symbol, data)
                return data
            except LookupError:
                continue

        raise LookupError(f"No Intrinio data found for {symbol}")

    async def _fetch_quote(self, identifier: str) -> dict:
        """
        Fetch quote data from Intrinio API for the given identifier.

        Args:
            identifier: Security identifier (FIGI, ISIN, CUSIP, or symbol).

        Returns:
            Raw JSON response from Intrinio API.

        Raises:
            LookupError: If the request fails or returns non-2xx status.
        """
        url = f"{_INTRINIO_BASE_URL}/{identifier}/quote"
        params = {"api_key": self._api_key}

        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as error:
            raise LookupError(f"Failed to fetch quote for {identifier}") from error
