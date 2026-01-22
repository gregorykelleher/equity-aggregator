# yfinance/transport.py

import asyncio
import logging
from collections.abc import Callable

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

logger: logging.Logger = logging.getLogger(__name__)

# Type aliases
OnResetFn = Callable[[], None]
ClientFactory = Callable[[], httpx.AsyncClient]


class HttpTransport:
    """
    Manages HTTP client lifecycle with automatic recovery on transport errors.

    When a request fails due to transport errors (connection failures, protocol
    errors, timeouts), the transport resets the client and retries. Uses
    optimistic concurrency: if another task already reset the client, the retry
    is "free" (doesn't consume budget).

    Args:
        client (httpx.AsyncClient | None): Optional pre-configured HTTP client.
        on_reset (OnResetFn | None): Optional callback invoked after client reset.
        client_factory (ClientFactory | None): Optional factory function to create
            new clients during reset. Defaults to make_client.

    Returns:
        None
    """

    __slots__ = ("_client", "_client_factory", "_lock", "_on_reset", "_ready")

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        on_reset: OnResetFn | None = None,
        *,
        client_factory: ClientFactory | None = None,
    ) -> None:
        """
        Initialise HttpTransport with optional client and reset callback.

        Args:
            client (httpx.AsyncClient | None): Optional pre-configured HTTP client.
            on_reset (OnResetFn | None): Optional callback invoked after client reset.
            client_factory (ClientFactory | None): Optional factory function to create
                new clients during reset. Defaults to make_client.

        Returns:
            None
        """
        self._client_factory: ClientFactory = client_factory or make_client
        self._client: httpx.AsyncClient = client or self._client_factory()
        self._lock: asyncio.Lock = asyncio.Lock()
        self._ready: asyncio.Event = asyncio.Event()
        self._ready.set()
        self._on_reset: OnResetFn | None = on_reset

    async def aclose(self) -> None:
        """
        Close the underlying HTTP client.

        Args:
            None

        Returns:
            None
        """
        async with self._lock:
            client = self._client
        await client.aclose()

    async def get(
        self,
        url: str,
        params: dict[str, str],
        *,
        retries_remaining: int = 3,
    ) -> httpx.Response:
        """
        Perform GET request with automatic client recovery on connection errors.

        Uses optimistic retry: if the client was already reset by another task,
        retries without consuming the retry budget. Only failures on a fresh
        client decrement the budget.

        Args:
            url (str): The absolute URL to request.
            params (dict[str, str]): Query parameters for the request.
            retries_remaining (int): Number of retries left for fresh client failures.

        Returns:
            httpx.Response: The HTTP response.

        Raises:
            LookupError: If all retry attempts fail due to connection errors.
        """
        if retries_remaining <= 0:
            raise LookupError("Connection failed after retries") from None

        await self._ready.wait()

        async with self._lock:
            client, client_id = self._client, id(self._client)

        try:
            return await client.get(url, params=params)

        except (httpx.TransportError, RuntimeError):
            # Check if client was already reset by another task (returns True if stale)
            was_stale = await self._handle_connection_error(client_id)

            # Free retry if stale, otherwise decrement retry budget
            next_retries = retries_remaining if was_stale else retries_remaining - 1

            # Recursively retry request with updated retry budget
            return await self.get(url, params, retries_remaining=next_retries)

    async def _handle_connection_error(self, failed_client_id: int) -> bool:
        """
        Handle connection error, resetting client if necessary.

        Checks if another task already reset the client (stale client).
        If not, this task triggers the reset.

        Args:
            failed_client_id (int): The id() of the client that failed.

        Returns:
            bool: True if client was already reset (free retry),
                  False if this task reset it (counts against budget).
        """
        async with self._lock:
            already_reset = failed_client_id != id(self._client)

        if already_reset:
            return True

        logger.debug(
            "CLIENT_RESET: Transport error encountered, resetting YFinance client",
        )
        await self._reset()
        return False

    async def _reset(self) -> None:
        """
        Reset the HTTP client instance.

        Creates a new client, verifies it can connect, then replaces the
        old client. Invokes the on_reset callback if configured.

        Args:
            None

        Returns:
            None

        Raises:
            Exception: If new client creation or health check fails.
        """
        async with self._lock:
            # Save old client before replacement, blocking new requests until ready
            old_client = self._client
            self._ready.clear()

            try:
                # Create and verify new client with health check
                new_client = self._client_factory()
                await new_client.get("https://finance.yahoo.com", timeout=5.0)

            except Exception:
                # Restore ready state if health check fails
                self._ready.set()
                raise

            # Atomically swap in new client and unblock requests
            self._client = new_client
            self._ready.set()

        # Clean up old client outside lock to avoid blocking
        await old_client.aclose()

        # Notify listeners of client reset
        if self._on_reset is not None:
            self._on_reset()
