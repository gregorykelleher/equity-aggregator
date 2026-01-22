# intrinio/session.py

import asyncio
import logging
from collections.abc import Mapping

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

from ._utils import backoff_delays

logger: logging.Logger = logging.getLogger(__name__)


class IntrinioSession:
    """
    Asynchronous session for Intrinio API endpoints.

    Manages HTTP client lifecycle with automatic recovery on HTTP/2 connection
    errors. Uses a class-level semaphore to limit concurrent streams and prevent
    connection exhaustion.

    Args:
        client (httpx.AsyncClient | None): Optional pre-configured HTTP client.

    Returns:
        None
    """

    __slots__ = ("_client", "_lock")

    # Limit HTTP/2 concurrent streams to prevent connection exhaustion
    _concurrent_streams: asyncio.Semaphore = asyncio.Semaphore(10)

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        """
        Initialise IntrinioSession with optional HTTP client.

        Args:
            client (httpx.AsyncClient | None): Optional pre-configured HTTP client.

        Returns:
            None
        """
        self._client: httpx.AsyncClient = client or make_client()
        self._lock: asyncio.Lock = asyncio.Lock()

    async def aclose(self) -> None:
        """
        Close the underlying HTTP client.

        Args:
            None

        Returns:
            None
        """
        await self._client.aclose()

    async def get(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """
        Perform a GET request with concurrency limiting and connection recovery.

        Automatically resets the HTTP client on protocol errors and retries
        the request with exponential backoff. Handles 429 rate limit responses
        with exponential backoff before returning.

        Args:
            url (str): Absolute URL to request.
            params (Mapping[str, str] | None): Optional query parameters.

        Returns:
            httpx.Response: The HTTP response.

        Raises:
            httpx.HTTPError: If the request fails after all retries.
        """
        async with self.__class__._concurrent_streams:
            return await self._get_with_rate_limit_retry(url, params=dict(params or {}))

    async def _get_with_rate_limit_retry(
        self,
        url: str,
        params: dict[str, str],
    ) -> httpx.Response:
        """
        Perform GET request with 429 rate limit retry logic.

        Applies exponential backoff when receiving 429 Too Many Requests responses
        from the Intrinio API.

        Args:
            url (str): The absolute URL to request.
            params (dict[str, str]): Query parameters for the request.

        Returns:
            httpx.Response: The HTTP response.

        Raises:
            httpx.HTTPStatusError: If still rate limited after all retries.
        """
        response = await self._get_with_retry(url, params=params)

        if response.status_code != httpx.codes.TOO_MANY_REQUESTS:
            return response

        max_attempts = 5

        for attempt, delay in enumerate(backoff_delays(attempts=max_attempts), 1):
            logger.debug(
                "429 Too Many Requests %s - sleeping %.1fs (attempt %d/%d)",
                url,
                delay,
                attempt,
                max_attempts,
            )
            await asyncio.sleep(delay)

            response = await self._get_with_retry(url, params=params)

            if response.status_code != httpx.codes.TOO_MANY_REQUESTS:
                return response

        return response

    async def _get_with_retry(
        self,
        url: str,
        params: dict[str, str],
        *,
        retries_remaining: int = 3,
    ) -> httpx.Response:
        """
        Perform GET request with automatic client recovery on connection errors.

        Uses optimistic retry: if the client was already reset by another task,
        retries without consuming the retry budget.

        Args:
            url (str): The absolute URL to request.
            params (dict[str, str]): Query parameters for the request.
            retries_remaining (int): Number of retries left for connection errors.

        Returns:
            httpx.Response: The HTTP response.

        Raises:
            httpx.HTTPError: If all retry attempts fail.
        """
        if retries_remaining <= 0:
            raise httpx.ConnectError("Connection failed after retries")

        async with self._lock:
            client, client_id = self._client, id(self._client)

        try:
            return await client.get(url, params=params)

        except (httpx.TransportError, RuntimeError):
            was_stale = await self._reset_if_needed(client_id)
            next_retries = retries_remaining if was_stale else retries_remaining - 1

            return await self._get_with_retry(
                url,
                params,
                retries_remaining=next_retries,
            )

    async def _reset_if_needed(self, failed_client_id: int) -> bool:
        """
        Reset the HTTP client if it hasn't already been reset by another task.

        Args:
            failed_client_id (int): The id() of the client that failed.

        Returns:
            bool: True if client was already reset (free retry),
                  False if this task reset it (counts against budget).
        """
        async with self._lock:
            if failed_client_id != id(self._client):
                return True  # Already reset by another task

            old_client = self._client
            self._client = make_client()

        await old_client.aclose()
        logger.debug(
            "CLIENT_RESET: Transport error encountered, resetting Intrinio client",
        )
        return False
