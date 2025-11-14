# lseg/session.py

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

from ._utils import backoff_delays

logger: logging.Logger = logging.getLogger(__name__)

# Type alias for HTTP request functions
HttpRequestFunc = Callable[..., Awaitable[httpx.Response]]


class LsegSession:
    """
    Asynchronous session for LSEG JSON endpoints.

    This class manages HTTP requests to the LSEG trading platform API, handling
    rate limits and 403 Forbidden responses with exponential backoff retry logic.
    It is lightweight and reusable, maintaining only a client and session state.

    Args:
        client (httpx.AsyncClient | None): Optional pre-configured HTTP client.

    Returns:
        None
    """

    __slots__: tuple[str, ...] = ("_client",)

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        """
        Initialise a new LsegSession for LSEG JSON endpoints.

        Args:
            client (httpx.AsyncClient | None): Optional pre-configured HTTP client.

        Returns:
            None
        """
        self._client: httpx.AsyncClient = client or make_client()

    async def aclose(self) -> None:
        """
        Asynchronously close the underlying HTTP client.

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
        Perform a resilient asynchronous GET request to LSEG endpoints.

        This method applies exponential backoff on 403 Forbidden responses
        to handle rate limiting gracefully.

        Args:
            url (str): Absolute URL to request.
            params (Mapping[str, str] | None): Optional query parameters.

        Returns:
            httpx.Response: The successful HTTP response.
        """
        return await self._request_with_retry(
            self._client.get,
            url,
            params=dict(params or {}),
        )

    async def post(
        self,
        url: str,
        *,
        json: dict[str, object] | None = None,
    ) -> httpx.Response:
        """
        Perform a resilient asynchronous POST request to LSEG endpoints.

        This method applies exponential backoff on 403 Forbidden responses
        to handle rate limiting gracefully.

        Args:
            url (str): Absolute URL to request.
            json (dict[str, object] | None): Optional JSON payload.

        Returns:
            httpx.Response: The successful HTTP response.
        """
        return await self._request_with_retry(
            self._client.post,
            url,
            json=json or {},
        )

    async def _request_with_retry(
        self,
        request_func: HttpRequestFunc,
        url: str,
        **kwargs: object,
    ) -> httpx.Response:
        """
        Perform a HTTP request with 403 Forbidden retry logic.

        This method is the core retry mechanism that handles exponential backoff
        for 403 responses regardless of the HTTP method used.

        Args:
            request_func (HttpRequestFunc): The HTTP client method to call (get/post).
            url (str): The absolute URL to request.
            **kwargs: Additional keyword arguments to pass to the request function.

        Returns:
            httpx.Response: The successful HTTP response.

        Raises:
            LookupError: If the final response is still 403 after all retries.
        """
        response = await request_func(url, **kwargs)

        # If not a 403, return immediately
        if response.status_code != httpx.codes.FORBIDDEN:
            return response

        # Apply exponential backoff for 403 responses
        max_attempts = 5

        for attempt, delay in enumerate(backoff_delays(attempts=max_attempts), 1):
            logger.debug(
                "403 Forbidden %s - sleeping %.1fs (attempt %d/%d)",
                url,
                delay,
                attempt,
                max_attempts,
            )
            await asyncio.sleep(delay)

            response = await request_func(url, **kwargs)

            # If we get a non-403 response, return it
            if response.status_code != httpx.codes.FORBIDDEN:
                return response

        # If we still have a 403 after all retries, raise an error
        raise LookupError(f"HTTP 403 Forbidden after retries for {url}")
