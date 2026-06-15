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
        crumb_ticker: str | None = None,
    ) -> httpx.Response:
        """
        Perform a resilient asynchronous GET request to Yahoo Finance endpoints.

        When crumb_ticker is provided, a crumb is attached proactively before the
        first attempt, as the endpoint requires anti-CSRF authentication. A 401
        then becomes a meaningful signal that the crumb has expired server-side,
        triggering a herd-safe refresh and a single replay. Endpoints that do not
        require a crumb (e.g. search) omit crumb_ticker and never bootstrap.

        Exponential backoff is applied on 429 responses. Concurrency is limited
        to comply with Yahoo's HTTP/2 stream limits. All httpx exceptions are
        converted to LookupError for consistent error handling at the domain
        boundary.

        Args:
            url (str): Absolute URL to request.
            params (Mapping[str, str] | None): Optional query parameters.
            crumb_ticker (str | None): Symbol used to prime and fetch the crumb
                for crumb-requiring endpoints; None disables crumb handling.

        Returns:
            httpx.Response: The successful HTTP response.

        Raises:
            LookupError: If the request fails due to network or HTTP errors.
        """
        async with self.__class__._concurrent_streams:
            params_dict: dict[str, str] = dict(params or {})

            try:
                if crumb_ticker is not None:
                    await self._apply_crumb(params_dict, crumb_ticker)
                return await self._fetch_with_retry(url, params_dict, crumb_ticker)
            except httpx.HTTPError as error:
                raise LookupError("Request failed") from error

    async def _apply_crumb(self, params: dict[str, str], ticker: str) -> None:
        """
        Attach a crumb to the query parameters before the first request.

        Uses the cached crumb when available, bootstrapping one only on first
        use for the session.

        Args:
            params (dict[str, str]): Query parameters mutated with the crumb.
            ticker (str): Symbol used to prime and fetch the crumb.

        Returns:
            None
        """
        params["crumb"] = await self._auth.ensure_crumb(ticker, self._transport.get)

    async def _fetch_with_retry(
        self,
        url: str,
        params: dict[str, str],
        crumb_ticker: str | None = None,
        *,
        delays: list[float] | None = None,
    ) -> httpx.Response:
        """
        Perform GET request with unified 401 and rate limit handling.

        Each attempt (initial + retries) passes through the full response handling
        chain: connection retry → 401 check/crumb refresh → retryable status check.
        This ensures that if a retry hits 401 (e.g., crumb cleared by client reset),
        the crumb is refreshed before continuing.

        Args:
            url (str): The absolute URL to request.
            params (dict[str, str]): Query parameters (mutated with crumb).
            crumb_ticker (str | None): Symbol used to refresh the crumb on a 401
                for crumb-requiring endpoints; None disables crumb handling.
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

            response = await self._attempt_request(url, params, crumb_ticker)

            # If response is not retryable, return it (success or permanent error)
            if response.status_code not in self._RETRYABLE_STATUS_CODES:
                return response

        # All attempts exhausted, response still retryable
        raise LookupError(f"HTTP {response.status_code} after retries for {url}")

    async def _attempt_request(
        self,
        url: str,
        params: dict[str, str],
        crumb_ticker: str | None = None,
    ) -> httpx.Response:
        """
        Perform a single request attempt, refreshing the crumb on a 401.

        A 401 is only actionable for crumb-bearing endpoints; for those it means
        the crumb has expired and is refreshed before a single replay.

        Args:
            url (str): The absolute URL to request.
            params (dict[str, str]): Query parameters (mutated with crumb on 401).
            crumb_ticker (str | None): Symbol used to refresh the crumb on a 401;
                None leaves the 401 response untouched.

        Returns:
            httpx.Response: The HTTP response.
        """
        response = await self._transport.get(url, params)

        if (
            crumb_ticker is not None
            and response.status_code == httpx.codes.UNAUTHORIZED
        ):
            response = await self._refresh_crumb_and_replay(url, params, crumb_ticker)

        return response

    async def _refresh_crumb_and_replay(
        self,
        url: str,
        params: dict[str, str],
        crumb_ticker: str,
    ) -> httpx.Response:
        """
        Refresh an expired crumb after a 401 and replay the request once.

        The crumb currently in the query parameters is the one that just failed;
        it is passed as the stale value so the CrumbManager can compare-and-swap,
        avoiding a refetch herd across concurrent requests.

        Args:
            url (str): The original request URL.
            params (dict[str, str]): Mutable query parameters.
            crumb_ticker (str): Symbol used to prime and fetch the fresh crumb.

        Returns:
            httpx.Response: Response after replaying with the refreshed crumb.
        """
        stale_crumb = params.get("crumb")

        params["crumb"] = await self._auth.renew_crumb(
            crumb_ticker,
            self._transport.get,
            stale_crumb=stale_crumb,
        )

        return await self._transport.get(url, params)
