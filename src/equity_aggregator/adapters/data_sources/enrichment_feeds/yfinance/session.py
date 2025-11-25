# yfinance/session.py

import asyncio
import logging
from collections.abc import Mapping

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

from ._utils import backoff_delays
from .config import FeedConfig

logger: logging.Logger = logging.getLogger(__name__)


class YFSession:
    """
    Asynchronous session for Yahoo Finance JSON endpoints.

    This class manages HTTP requests to Yahoo Finance, handling authentication,
    rate limits, and crumb renewal. It is lightweight and reusable, maintaining
    only a client and session state. Concurrency is limited by a shared
    semaphore to respect Yahoo's HTTP/2 stream restriction.

    Args:
        config (FeedConfig): Immutable feed configuration.
        client (httpx.AsyncClient | None): Optional pre-configured HTTP client.

    Returns:
        None
    """

    __slots__: tuple[str, ...] = (
        "_client",
        "_client_lock",
        "_client_ready",
        "_config",
        "_crumb",
        "_crumb_lock",
    )

    # Limit HTTP/2 concurrent streams to 10 for maximum throughput.
    _concurrent_streams: asyncio.Semaphore = asyncio.Semaphore(10)

    # Define retryable status codes once to avoid duplication
    _RETRYABLE_STATUS_CODES = frozenset(
        {
            httpx.codes.TOO_MANY_REQUESTS,  # 429
            httpx.codes.BAD_GATEWAY,  # 502
            httpx.codes.SERVICE_UNAVAILABLE,  # 503
            httpx.codes.GATEWAY_TIMEOUT,  # 504
        },
    )

    def __init__(
        self,
        config: FeedConfig,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        """
        Initialise a new YFSession for Yahoo Finance JSON endpoints.

        Args:
            config (FeedConfig): Immutable feed configuration.
            client (httpx.AsyncClient | None): Optional pre-configured HTTP client.

        Returns:
            None
        """
        self._config: FeedConfig = config
        self._client: httpx.AsyncClient = client or make_client()
        self._client_lock: asyncio.Lock = asyncio.Lock()
        self._client_ready: asyncio.Event = asyncio.Event()
        self._client_ready.set()
        self._crumb: str | None = None
        self._crumb_lock: asyncio.Lock = asyncio.Lock()

    @property
    def config(self) -> FeedConfig:
        """
        Gets the immutable configuration associated with this session.

        Args:
            None

        Returns:
            FeedConfig: The configuration object bound to this session instance.
        """
        return self._config

    async def aclose(self) -> None:
        """
        Asynchronously close the underlying HTTP client.

        Args:
            None

        Returns:
            None
        """
        async with self._client_lock:
            client = self._client
        await client.aclose()

    async def get(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """
        Perform a resilient asynchronous GET request to Yahoo Finance endpoints.

        This method renews the crumb on a single 401 response and applies
        exponential backoff on 429 responses. Concurrency is limited to comply
        with Yahoo's HTTP/2 stream limits.

        All httpx exceptions are converted to LookupError for consistent
        error handling at the domain boundary.

        Args:
            url (str): Absolute URL to request.
            params (Mapping[str, str] | None): Optional query parameters.

        Returns:
            httpx.Response: The successful HTTP response.

        Raises:
            LookupError: If the request fails due to network or HTTP errors.
        """
        async with self.__class__._concurrent_streams:
            merged_params: dict[str, str] = dict(params or {})

            try:
                return await self._fetch_with_retry(url, merged_params)

            except httpx.HTTPError as error:
                raise LookupError("Request failed") from error

    async def _safe_get(
        self,
        url: str,
        params: dict[str, str],
        *,
        retries_remaining: int = 3,
    ) -> httpx.Response:
        """
        Perform a GET request with optimistic retry on connection errors.

        When a request fails due to a connection error, checks if another task
        already reset the client. If so, retries immediately without consuming
        the retry budget ("free retry"). If the client hasn't been reset, this
        task resets it and retries with decremented budget.

        This approach prevents in-flight requests from failing when another task
        closes the shared client, ensuring equities get retried with the fresh
        client instead of being marked as permanent failures.

        Args:
            url (str): The absolute URL to request.
            params (dict[str, str]): Query parameters for the request.
            retries_remaining (int): Number of retries left with fresh client.

        Returns:
            httpx.Response: The successful HTTP response.

        Raises:
            LookupError: If all retry attempts fail due to connection errors.
        """
        if retries_remaining <= 0:
            raise LookupError("Connection failed after retries") from None

        await self._client_ready.wait()

        async with self._client_lock:
            client = self._client
            client_id = id(client)

        try:
            return await client.get(url, params=params)
        except (httpx.ProtocolError, RuntimeError):
            was_stale = await self._handle_connection_error(client_id)
            next_retries = retries_remaining if was_stale else retries_remaining - 1
            return await self._safe_get(url, params, retries_remaining=next_retries)

    async def _handle_connection_error(self, failed_client_id: int) -> bool:
        """
        Handle HTTP/2 connection errors by resetting clients if needed.

        Checks if the client was already reset by another task (stale client). If
        stale, logs and returns True to indicate a free retry. If not stale, this
        task triggers the reset and returns False to indicate retry budget should
        be consumed.

        Args:
            failed_client_id (int): The id() of the client that failed.

        Returns:
            bool: True if another client already reset the client (free retry),
            otherwise False if this task reset it (counts against retries).
        """
        async with self._client_lock:
            already_reset = failed_client_id != id(self._client)

        if already_reset:
            return True

        logger.debug("CLIENT_RESET: HTTP/2 connection broken, resetting client")
        await self._reset_client()
        return False

    async def _fetch_with_retry(
        self,
        url: str,
        params: Mapping[str, str],
    ) -> httpx.Response:
        """
        Perform GET request with unified 401 and rate limit handling.

        Each attempt (initial + retries) passes through the full response handling
        chain: connection retry → 401 check/crumb renewal → retryable status check.
        This ensures that if a retry hits 401 (e.g., crumb cleared by client reset),
        the crumb is renewed before continuing.

        Args:
            url (str): The absolute URL to request.
            params (Mapping[str, str]): Query parameters.

        Returns:
            httpx.Response: The successful HTTP response.

        Raises:
            LookupError: If response is still retryable after all attempts.
        """
        params = dict(params)
        max_backoff_attempts = 5

        for backoff_attempt, delay in enumerate(
            [0, *backoff_delays(attempts=max_backoff_attempts)],
        ):
            if delay > 0:
                logger.debug(
                    "RATE_LIMIT: YFinance feed data request paused. "
                    "Retrying in %.1fs (attempt %d/%d)",
                    delay,
                    backoff_attempt,
                    max_backoff_attempts,
                )
                await asyncio.sleep(delay)

            response = await self._safe_get(url, params)

            # Handle 401 by renewing crumb (could happen after client reset)
            if response.status_code == httpx.codes.UNAUTHORIZED:
                response = await self._renew_crumb_once(url, params)

            # If response is not retryable, return it (success or permanent error)
            if response.status_code not in self._RETRYABLE_STATUS_CODES:
                return response

        # All attempts exhausted, response still retryable
        raise LookupError(f"HTTP {response.status_code} after retries for {url}")

    async def _reset_client(self) -> None:
        """
        Reset the HTTP client instance asynchronously.

        Closes the current client and creates a new one. Also clears the crumb
        to ensure session state is refreshed after protocol errors.

        Args:
            None

        Returns:
            None
        """
        async with self._client_lock:
            old_client = self._client
            self._client_ready.clear()
            try:
                new_client = make_client()

                # Verify new client can actually connect
                await new_client.get("https://finance.yahoo.com", timeout=5.0)

            except Exception:
                self._client_ready.set()
                raise

            self._client = new_client
            self._crumb = None
            self._client_ready.set()
        await old_client.aclose()

    async def _renew_crumb_once(
        self,
        url: str,
        params: dict[str, str],
    ) -> httpx.Response:
        """
        Refresh the crumb after a 401 Unauthorized and retry the request.

        This method extracts the ticker from the URL, fetches a new crumb,
        updates the query parameters, and replays the GET request.

        Args:
            url (str): The original request URL.
            params (dict[str, str]): Mutable query parameters.

        Returns:
            httpx.Response: Response after retrying with a fresh crumb.
        """
        ticker: str = self._extract_ticker(url)

        await self._bootstrap_and_fetch_crumb(ticker)

        params["crumb"] = self._crumb

        return await self._safe_get(url, params)

    def _extract_ticker(self, url: str) -> str:
        """
        Extract the ticker symbol from a Yahoo Finance quote-summary URL.

        Args:
            url (str): The quote-summary endpoint URL.

        Returns:
            str: The ticker symbol found in the URL path.
        """
        remainder: str = url[len(self._config.quote_summary_primary_url) :]

        first_segment: str = remainder.split("/", 1)[0]

        return first_segment.split("?", 1)[0].split("#", 1)[0]

    async def _bootstrap_and_fetch_crumb(self, ticker: str) -> None:
        """
        Initialise session cookies and retrieve the anti-CSRF crumb.

        This method primes the session by making requests to Yahoo Finance endpoints
        using the provided ticker, then fetches the crumb required for authenticated
        requests. The crumb is cached for future use and protected by a lock.

        Args:
            ticker (str): Symbol used to prime the session.

        Returns:
            None
        """
        if self._crumb is not None:
            return

        async with self._crumb_lock:
            if self._crumb is not None:
                return
            seeds: tuple[str, ...] = (
                "https://fc.yahoo.com",
                "https://finance.yahoo.com",
                f"https://finance.yahoo.com/quote/{ticker}",
            )
            for seed in seeds:
                await self._safe_get(seed, {})

            resp: httpx.Response = await self._safe_get(self._config.crumb_url, {})
            resp.raise_for_status()
            self._crumb = resp.text.strip().strip('"')
