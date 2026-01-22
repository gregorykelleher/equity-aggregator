# gleif/_helpers.py

from collections.abc import Callable

import httpx


def make_client_factory(
    handler: Callable[[httpx.Request], httpx.Response],
) -> Callable[[], httpx.AsyncClient]:
    """
    Create a factory that returns an async HTTPX client with a mock handler.

    Args:
        handler: A callable that takes an httpx.Request and returns an
            httpx.Response, used to simulate HTTP responses.

    Returns:
        Factory function that creates an httpx.AsyncClient configured with
        the provided mock transport handler.
    """
    return lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))
