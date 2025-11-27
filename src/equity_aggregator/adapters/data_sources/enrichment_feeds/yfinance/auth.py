# yfinance/auth.py

import asyncio
import logging
from collections.abc import Awaitable, Callable

import httpx

logger: logging.Logger = logging.getLogger(__name__)

# Type alias for the fetch function signature
FetchFn = Callable[[str, dict[str, str]], Awaitable[httpx.Response]]


class CrumbManager:
    """
    Manages Yahoo Finance anti-CSRF crumb lifecycle.

    Handles crumb acquisition with double-checked locking for thread safety.
    The crumb is lazily fetched on first use and cleared when the underlying
    HTTP client is reset.

    Args:
        crumb_url (str): URL to fetch the crumb from.

    Returns:
        None
    """

    __slots__ = ("_crumb", "_crumb_url", "_lock")

    def __init__(self, crumb_url: str) -> None:
        """
        Initialise CrumbManager with the crumb endpoint URL.

        Args:
            crumb_url (str): URL to fetch the crumb from.

        Returns:
            None
        """
        self._crumb: str | None = None
        self._crumb_url: str = crumb_url
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def crumb(self) -> str | None:
        """
        Get the current crumb value, if available.

        Args:
            None

        Returns:
            str | None: The cached crumb, or None if not yet fetched.
        """
        return self._crumb

    def clear(self) -> None:
        """
        Clear the cached crumb.

        Called when the HTTP client is reset, as the crumb is tied to
        session cookies that are invalidated on client replacement.

        Args:
            None

        Returns:
            None
        """
        self._crumb = None

    async def ensure_crumb(self, ticker: str, fetch: FetchFn) -> str:
        """
        Ensure a valid crumb is available, bootstrapping if necessary.

        Uses double-checked locking: fast path returns cached crumb,
        slow path acquires lock and bootstraps session if needed.

        Args:
            ticker (str): Symbol to use for session priming requests.
            fetch (FetchFn): Async function to perform HTTP GET requests.

        Returns:
            str: Valid crumb string.

        Raises:
            httpx.HTTPStatusError: If crumb fetch fails.
        """
        if self._crumb is not None:
            return self._crumb

        async with self._lock:
            if self._crumb is not None:
                return self._crumb

            await self._bootstrap(ticker, fetch)
            return self._crumb

    async def _bootstrap(self, ticker: str, fetch: FetchFn) -> None:
        """
        Prime session cookies and fetch the crumb.

        Makes requests to Yahoo Finance endpoints to establish session
        cookies, then fetches the crumb from the crumb endpoint.

        Args:
            ticker (str): Symbol to use for session priming.
            fetch (FetchFn): Async function to perform HTTP GET requests.

        Returns:
            None

        Raises:
            httpx.HTTPStatusError: If crumb fetch fails.
        """
        seeds: tuple[str, ...] = (
            "https://fc.yahoo.com",
            "https://finance.yahoo.com",
            f"https://finance.yahoo.com/quote/{ticker}",
        )

        for seed in seeds:
            await fetch(seed, {})

        response: httpx.Response = await fetch(self._crumb_url, {})
        response.raise_for_status()

        self._crumb = response.text.strip().strip('"')
