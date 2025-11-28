# yfinance/session.py

import asyncio
import logging
from collections.abc import Mapping

import httpx

from ._utils import backoff_delays
from .auth import CrumbManager
from .config import FeedConfig
from .transport import HttpTransport

logger: logging.Logger = logging.getLogger(__name__)


class YFSession:
    """
    Asynchronous session for Yahoo Finance JSON endpoints.

    Composes HttpTransport for connection management, CrumbManager for
    authentication, and applies retry policies for rate limiting.
    Concurrency is limited by a shared semaphore.

    Args:
        config (FeedConfig): Immutable feed configuration.
        client (httpx.AsyncClient | None): Optional pre-configured HTTP client.

    Returns:
        None
    """

    __slots__ = ("_auth", "_config", "_transport")

    # Limit HTTP/2 concurrent streams to 10 for maximum throughput.
    _concurrent_streams: asyncio.Semaphore = asyncio.Semaphore(10)

    _RETRYABLE_STATUS_CODES: frozenset[int] = frozenset(
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
        Initialise YFSession with configuration.

        Args:
            config (FeedConfig): Immutable feed configuration.
            client (httpx.AsyncClient | None): Optional pre-configured HTTP client.

        Returns:
            None
        """
        self._config: FeedConfig = config
        self._auth: CrumbManager = CrumbManager(config.crumb_url)
        self._transport: HttpTransport = HttpTransport(
            client=client,
            on_reset=self._auth.clear,
        )

    @property
    def config(self) -> FeedConfig:
        """
        Get the immutable configuration associated with this session.

        Args:
            None

        Returns:
            FeedConfig: The configuration object bound to this session.
        """
        return self._config

    async def aclose(self) -> None:
        """
        Close the underlying HTTP transport.

        Args:
            None

        Returns:
            None
        """
        await self._transport.aclose()

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
            params_dict: dict[str, str] = dict(params or {})

            try:
                return await self._fetch_with_retry(url, params_dict)
            except httpx.HTTPError as error:
                raise LookupError("Request failed") from error

    async def _fetch_with_retry(
        self,
        url: str,
        params: dict[str, str],
        *,
        delays: list[float] | None = None,
    ) -> httpx.Response:
        """
        Perform GET request with unified 401 and rate limit handling.

        Each attempt (initial + retries) passes through the full response handling
        chain: connection retry → 401 check/crumb renewal → retryable status check.
        This ensures that if a retry hits 401 (e.g., crumb cleared by client reset),
        the crumb is renewed before continuing.

        Args:
            url (str): The absolute URL to request.
            params (dict[str, str]): Query parameters (mutated with crumb).
            delays (list[float] | None): Optional delay sequence for testing.
                If None, uses exponential backoff with 5 retry attempts.

        Returns:
            httpx.Response: The successful HTTP response.

        Raises:
            LookupError: If response is still retryable after all attempts.
        """
        max_backoff_attempts = 5

        if delays is None:
            delays = [0, *backoff_delays(attempts=max_backoff_attempts)]

        for backoff_attempt, delay in enumerate(delays):
            if delay > 0:
                logger.debug(
                    "RATE_LIMIT: YFinance feed data request paused. "
                    "Retrying in %.1fs (attempt %d/%d)",
                    delay,
                    backoff_attempt,
                    max_backoff_attempts,
                )
                await asyncio.sleep(delay)

            response = await self._attempt_request(url, params)

            # If response is not retryable, return it (success or permanent error)
            if response.status_code not in self._RETRYABLE_STATUS_CODES:
                return response

        # All attempts exhausted, response still retryable
        raise LookupError(f"HTTP {response.status_code} after retries for {url}")

    async def _attempt_request(
        self,
        url: str,
        params: dict[str, str],
    ) -> httpx.Response:
        """
        Perform a single request attempt with 401 handling.

        Args:
            url (str): The absolute URL to request.
            params (dict[str, str]): Query parameters (mutated with crumb on 401).

        Returns:
            httpx.Response: The HTTP response.
        """
        response = await self._transport.get(url, params)

        # Handle 401 by renewing crumb (could happen after client reset)
        if response.status_code == httpx.codes.UNAUTHORIZED:
            response = await self._renew_crumb_once(url, params)

        return response

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

        crumb = await self._auth.ensure_crumb(ticker, self._transport.get)

        params["crumb"] = crumb

        return await self._transport.get(url, params)

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
