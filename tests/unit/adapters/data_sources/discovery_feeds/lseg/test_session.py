# lseg/test_session.py

import asyncio
import json
from collections import deque
from collections.abc import Callable

import httpx
import pytest

from equity_aggregator.adapters.data_sources.discovery_feeds.lseg.session import (
    LsegSession,
)

pytestmark = pytest.mark.unit


def make_client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> httpx.AsyncClient:
    """
    Create an asynchronous test HTTPX client using a custom request handler.

    Args:
        handler (Callable[[httpx.Request], httpx.Response]): A callable that
            takes an httpx.Request and returns an httpx.Response, used to mock
            HTTP responses.

    Returns:
        httpx.AsyncClient: An asynchronous HTTP client configured with the
            provided mock transport handler.
    """
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def close(client: httpx.AsyncClient) -> None:
    """
    Asynchronously close the provided HTTPX AsyncClient instance.

    Args:
        client (httpx.AsyncClient): The asynchronous HTTP client to close.

    Returns:
        None
    """
    await client.aclose()


def _setup_retry_handler() -> tuple[
    Callable[[httpx.Request], httpx.Response],
    Callable[[], int],
]:
    """
    Create a mock handler that tracks call count and simulates retry behavior.

    Returns a handler that returns HTTP 403 for the first RETRY_ATTEMPTS calls,
    then returns HTTP 200 for subsequent calls. Also returns a function to get
    the current call count.

    Returns:
        tuple[Callable[[httpx.Request], httpx.Response], Callable[[], int]]: A
            tuple containing the mock request handler and a callable that
            returns the current call count.
    """
    call_count = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return (
            httpx.Response(HTTP_FORBIDDEN)
            if call_count <= RETRY_ATTEMPTS
            else httpx.Response(HTTP_OK)
        )

    return handler, lambda: call_count


HTTP_OK = 200
HTTP_FORBIDDEN = 403
HTTP_NOT_FOUND = 404
HTTP_INTERNAL_ERROR = 500

RETRY_ATTEMPTS = 3
EXPECTED_CALL_COUNT = 4


async def test_get_defaults_params_to_empty_dict() -> None:
    """
    ARRANGE: handler that captures query parameters
    ACT:     call get() without params
    ASSERT:  empty params dict is captured
    """
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(HTTP_OK)

    session = LsegSession(make_client(handler))
    await session.get("https://dummy.com")
    await close(session._client)

    assert captured["params"] == {}


async def test_get_passes_through_params() -> None:
    """
    ARRANGE: handler that captures query parameters and specific params
    ACT:     call get() with params
    ASSERT:  provided params are captured
    """
    captured: dict[str, object] = {}
    expected_params = {"key": "value"}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(HTTP_OK)

    session = LsegSession(make_client(handler))
    await session.get("https://dummy.com", params=expected_params)
    await close(session._client)

    assert captured["params"] == expected_params


async def test_post_sends_json_payload() -> None:
    """
    ARRANGE: handler that captures JSON payload and expected JSON
    ACT:     call post() with JSON data
    ASSERT:  expected JSON is captured
    """
    captured: dict[str, object] = {}
    expected_json = {"key": "value"}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["json"] = json.loads(request.content)
        return httpx.Response(HTTP_OK)

    session = LsegSession(make_client(handler))
    await session.post("https://dummy.com", json=expected_json)
    await close(session._client)

    assert captured["json"] == expected_json


async def test_aclose_marks_client_closed() -> None:
    """
    ARRANGE: fresh session with HTTP client
    ACT:     call aclose()
    ASSERT:  client reports closed
    """
    client = make_client(lambda r: httpx.Response(HTTP_OK))
    session = LsegSession(client)

    await session.aclose()

    assert client.is_closed


async def _instant_sleep(_: float) -> None:
    pass


async def test_get_retries_after_403_forbidden() -> None:
    """
    ARRANGE: handler that returns 403 then 200
    ACT:     call get()
    ASSERT:  final response is 200
    """
    responses = deque([httpx.Response(HTTP_FORBIDDEN), httpx.Response(HTTP_OK)])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return responses.popleft()

    real_sleep, asyncio.sleep = asyncio.sleep, _instant_sleep

    try:
        session = LsegSession(make_client(handler))
        response = await session.get("https://dummy.com")
        await close(session._client)
    finally:
        asyncio.sleep = real_sleep

    assert response.status_code == HTTP_OK


async def test_post_retries_after_403_forbidden() -> None:
    """
    ARRANGE: handler that returns 403 then 200
    ACT:     call post()
    ASSERT:  final response is 200
    """
    responses = deque([httpx.Response(HTTP_FORBIDDEN), httpx.Response(HTTP_OK)])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return responses.popleft()

    real_sleep, asyncio.sleep = asyncio.sleep, _instant_sleep

    try:
        session = LsegSession(make_client(handler))
        response = await session.post("https://dummy.com", json={"test": "data"})
        await close(session._client)
    finally:
        asyncio.sleep = real_sleep

    assert response.status_code == HTTP_OK


async def test_get_returns_404_immediately() -> None:
    """
    ARRANGE: handler that always returns 404
    ACT:     call get()
    ASSERT:  response status is 404
    """

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(HTTP_NOT_FOUND)

    session = LsegSession(make_client(handler))
    response = await session.get("https://dummy.com")
    await close(session._client)

    assert response.status_code == HTTP_NOT_FOUND


async def test_post_returns_500_immediately() -> None:
    """
    ARRANGE: handler that always returns 500
    ACT:     call post()
    ASSERT:  response status is 500
    """

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(HTTP_INTERNAL_ERROR)

    session = LsegSession(make_client(handler))
    response = await session.post("https://dummy.com", json={"test": "data"})
    await close(session._client)

    assert response.status_code == HTTP_INTERNAL_ERROR


async def test_get_raises_lookup_error_after_max_retries() -> None:
    """
    ARRANGE: handler that always returns 403
    ACT:     call get()
    ASSERT:  LookupError is raised after max retries
    """

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(HTTP_FORBIDDEN)

    # Mock the sleep function to return immediately
    real_sleep = asyncio.sleep

    async def _instant(_delay: float) -> None:
        return None

    asyncio.sleep = _instant

    try:
        session = LsegSession(make_client(handler))

        with pytest.raises(LookupError, match="HTTP 403 Forbidden after retries"):
            await session.get("https://dummy.com")

        await close(session._client)
    finally:
        asyncio.sleep = real_sleep


async def test_get_succeeds_with_non_403_after_retries() -> None:
    """
    ARRANGE: handler that returns 403 once then 500
    ACT:     call get()
    ASSERT:  final response status is 500 (non-403)
    """
    responses = deque(
        [
            httpx.Response(HTTP_FORBIDDEN),
            httpx.Response(HTTP_INTERNAL_ERROR),
        ],
    )

    async def handler(_: httpx.Request) -> httpx.Response:
        return responses.popleft()

    session = LsegSession(make_client(handler))
    response = await session.get("https://dummy.com")
    await close(session._client)

    assert response.status_code == HTTP_INTERNAL_ERROR


async def test_post_performs_backoff_retry_attempts() -> None:
    """
    ARRANGE: handler that returns 403 multiple times
    ACT:     call post()
    ASSERT:  multiple retry attempts are made
    """

    async def _instant_sleep(_: float) -> None:
        pass

    handler, get_call_count = _setup_retry_handler()
    real_sleep, asyncio.sleep = asyncio.sleep, _instant_sleep

    try:
        session = LsegSession(make_client(handler))
        await session.post("https://dummy.com", json={"test": "data"})
        await close(session._client)
        assert get_call_count() == EXPECTED_CALL_COUNT
    finally:
        asyncio.sleep = real_sleep


async def test_post_succeeds_after_backoff_retries() -> None:
    """
    ARRANGE: handler that returns 403 multiple times
    ACT:     call post()
    ASSERT:  final response is successful
    """

    async def _instant_sleep(_: float) -> None:
        pass

    handler, _ = _setup_retry_handler()
    real_sleep, asyncio.sleep = asyncio.sleep, _instant_sleep

    try:
        session = LsegSession(make_client(handler))
        response = await session.post("https://dummy.com", json={"test": "data"})
        await close(session._client)
        assert response.status_code == HTTP_OK
    finally:
        asyncio.sleep = real_sleep
