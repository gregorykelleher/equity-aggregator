# discovery_feeds/intrinio/test_session.py

import asyncio

import httpx
import pytest

from equity_aggregator.adapters.data_sources.discovery_feeds.intrinio.session import (
    IntrinioSession,
)

pytestmark = pytest.mark.unit

HTTP_OK = 200
HTTP_NOT_FOUND = 404
HTTP_TOO_MANY_REQUESTS = 429
HTTP_INTERNAL_ERROR = 500

RETRY_ATTEMPTS_BEFORE_SUCCESS = 2
EXPECTED_ATTEMPTS_AFTER_SUCCESS = 3
EXPECTED_TOTAL_ATTEMPTS = 6  # 1 initial + 5 retries


def make_client(
    handler: callable,
) -> httpx.AsyncClient:
    """Create an AsyncClient with a mock transport handler."""
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def test_init_creates_client_when_none_provided() -> None:
    """
    ARRANGE: no client provided
    ACT:     create IntrinioSession
    ASSERT:  session has a client
    """
    session = IntrinioSession()

    assert session._client is not None


def test_init_uses_provided_client() -> None:
    """
    ARRANGE: pre-configured client
    ACT:     create IntrinioSession with client
    ASSERT:  session uses provided client
    """
    client = make_client(lambda _: httpx.Response(HTTP_OK))

    session = IntrinioSession(client)

    assert session._client is client


async def test_aclose_closes_client() -> None:
    """
    ARRANGE: session with open client
    ACT:     call aclose
    ASSERT:  client is closed
    """
    client = make_client(lambda _: httpx.Response(HTTP_OK))
    session = IntrinioSession(client)

    await session.aclose()

    assert client.is_closed


async def test_get_returns_response() -> None:
    """
    ARRANGE: handler returns 200
    ACT:     call get
    ASSERT:  returns response with status 200
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(HTTP_OK)

    session = IntrinioSession(make_client(handler))
    response = await session.get("https://dummy.com")
    await session.aclose()

    assert response.status_code == HTTP_OK


async def test_get_passes_params() -> None:
    """
    ARRANGE: handler captures params
    ACT:     call get with params
    ASSERT:  params are passed through
    """
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(HTTP_OK)

    session = IntrinioSession(make_client(handler))
    await session.get("https://dummy.com", params={"key": "value"})
    await session.aclose()

    assert captured["params"]["key"] == "value"


async def test_get_defaults_params_to_empty() -> None:
    """
    ARRANGE: handler captures params
    ACT:     call get without params
    ASSERT:  empty params dict
    """
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(HTTP_OK)

    session = IntrinioSession(make_client(handler))
    await session.get("https://dummy.com")
    await session.aclose()

    assert captured["params"] == {}


async def test_get_returns_404_directly() -> None:
    """
    ARRANGE: handler returns 404
    ACT:     call get
    ASSERT:  returns 404 response
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(HTTP_NOT_FOUND)

    session = IntrinioSession(make_client(handler))
    response = await session.get("https://dummy.com")
    await session.aclose()

    assert response.status_code == HTTP_NOT_FOUND


async def test_get_returns_500_directly() -> None:
    """
    ARRANGE: handler returns 500
    ACT:     call get
    ASSERT:  returns 500 response
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(HTTP_INTERNAL_ERROR)

    session = IntrinioSession(make_client(handler))
    response = await session.get("https://dummy.com")
    await session.aclose()

    assert response.status_code == HTTP_INTERNAL_ERROR


async def test_concurrent_streams_semaphore_exists() -> None:
    """
    ARRANGE: IntrinioSession class
    ACT:     access class-level semaphore
    ASSERT:  semaphore exists
    """
    semaphore = IntrinioSession._concurrent_streams

    assert semaphore is not None


async def test_get_respects_semaphore() -> None:
    """
    ARRANGE: session with handler
    ACT:     make concurrent requests
    ASSERT:  all requests complete successfully
    """
    call_count = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(HTTP_OK)

    session = IntrinioSession(make_client(handler))

    tasks = [session.get("https://dummy.com") for _ in range(5)]
    responses = await asyncio.gather(*tasks)
    await session.aclose()

    assert all(r.status_code == HTTP_OK for r in responses)


async def test_reset_if_needed_returns_true_when_already_reset() -> None:
    """
    ARRANGE: client already reset (different ID)
    ACT:     call _reset_if_needed with old ID
    ASSERT:  returns True (was stale)
    """
    session = IntrinioSession(make_client(lambda _: httpx.Response(HTTP_OK)))
    old_id = id(session._client) - 1  # Simulate stale ID

    was_stale = await session._reset_if_needed(old_id)
    await session.aclose()

    assert was_stale is True


async def test_reset_if_needed_returns_false_when_reset_performed() -> None:
    """
    ARRANGE: current client ID matches failed ID
    ACT:     call _reset_if_needed
    ASSERT:  returns False (reset performed)
    """
    session = IntrinioSession(make_client(lambda _: httpx.Response(HTTP_OK)))
    current_id = id(session._client)

    was_stale = await session._reset_if_needed(current_id)
    await session.aclose()

    assert was_stale is False


async def test_reset_if_needed_creates_new_client() -> None:
    """
    ARRANGE: call _reset_if_needed with current client ID
    ACT:     check client after reset
    ASSERT:  new client has different ID
    """
    session = IntrinioSession(make_client(lambda _: httpx.Response(HTTP_OK)))
    original_id = id(session._client)

    await session._reset_if_needed(original_id)
    new_id = id(session._client)
    await session.aclose()

    assert new_id != original_id


async def test_get_with_retry_raises_connect_error_when_exhausted() -> None:
    """
    ARRANGE: session with valid handler
    ACT:     call _get_with_retry with retries_remaining=0
    ASSERT:  raises ConnectError
    """
    session = IntrinioSession(make_client(lambda _: httpx.Response(HTTP_OK)))

    with pytest.raises(httpx.ConnectError, match="Connection failed after retries"):
        await session._get_with_retry("https://dummy.com", {}, retries_remaining=0)

    await session.aclose()


async def test_get_with_retry_retries_on_transport_error_until_exhausted() -> None:
    """
    ARRANGE: handler that raises TransportError
    ACT:     call _get_with_retry with retries_remaining=1
    ASSERT:  raises ConnectError after retry exhaustion
    """

    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ProtocolError("Simulated protocol error")

    session = IntrinioSession(make_client(handler))

    with pytest.raises(httpx.ConnectError, match="Connection failed after retries"):
        await session._get_with_retry("https://dummy.com", {}, retries_remaining=1)

    await session.aclose()


async def test_get_with_retry_retries_on_runtime_error_until_exhausted() -> None:
    """
    ARRANGE: handler that raises RuntimeError
    ACT:     call _get_with_retry with retries_remaining=1
    ASSERT:  raises ConnectError after retry exhaustion
    """

    def handler(_: httpx.Request) -> httpx.Response:
        raise RuntimeError("Simulated runtime error")

    session = IntrinioSession(make_client(handler))

    with pytest.raises(httpx.ConnectError, match="Connection failed after retries"):
        await session._get_with_retry("https://dummy.com", {}, retries_remaining=1)

    await session.aclose()


async def _instant_sleep(_: float) -> None:
    pass


async def test_get_returns_non_429_immediately() -> None:
    """
    ARRANGE: handler returns 200
    ACT:     call get
    ASSERT:  returns 200 without retry
    """
    call_count = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(HTTP_OK)

    session = IntrinioSession(make_client(handler))
    await session.get("https://dummy.com")
    await session.aclose()

    assert call_count == 1


async def test_get_retries_on_429_until_success() -> None:
    """
    ARRANGE: handler returns 429 twice then 200
    ACT:     call get
    ASSERT:  returns 200 after retries
    """
    call_count = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= RETRY_ATTEMPTS_BEFORE_SUCCESS:
            return httpx.Response(HTTP_TOO_MANY_REQUESTS)
        return httpx.Response(HTTP_OK)

    real_sleep, asyncio.sleep = asyncio.sleep, _instant_sleep

    try:
        session = IntrinioSession(make_client(handler))
        response = await session.get("https://dummy.com")
        await session.aclose()
    finally:
        asyncio.sleep = real_sleep

    assert response.status_code == HTTP_OK


async def test_get_makes_multiple_attempts_on_429() -> None:
    """
    ARRANGE: handler returns 429 twice then 200
    ACT:     call get
    ASSERT:  handler called 3 times
    """
    call_count = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= RETRY_ATTEMPTS_BEFORE_SUCCESS:
            return httpx.Response(HTTP_TOO_MANY_REQUESTS)
        return httpx.Response(HTTP_OK)

    real_sleep, asyncio.sleep = asyncio.sleep, _instant_sleep

    try:
        session = IntrinioSession(make_client(handler))
        await session.get("https://dummy.com")
        await session.aclose()
    finally:
        asyncio.sleep = real_sleep

    assert call_count == EXPECTED_ATTEMPTS_AFTER_SUCCESS


async def test_get_returns_429_after_max_attempts() -> None:
    """
    ARRANGE: handler always returns 429, sleep mocked to zero-delay
    ACT:     call get
    ASSERT:  returns 429 after exhausting retries
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(HTTP_TOO_MANY_REQUESTS)

    real_sleep, asyncio.sleep = asyncio.sleep, _instant_sleep

    try:
        session = IntrinioSession(make_client(handler))
        response = await session.get("https://dummy.com")
        await session.aclose()
    finally:
        asyncio.sleep = real_sleep

    assert response.status_code == HTTP_TOO_MANY_REQUESTS


async def test_get_makes_expected_attempts_on_persistent_429() -> None:
    """
    ARRANGE: handler always returns 429, sleep mocked to zero-delay
    ACT:     call get
    ASSERT:  handler called expected number of times (1 initial + 5 retries)
    """
    call_count = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(HTTP_TOO_MANY_REQUESTS)

    real_sleep, asyncio.sleep = asyncio.sleep, _instant_sleep

    try:
        session = IntrinioSession(make_client(handler))
        await session.get("https://dummy.com")
        await session.aclose()
    finally:
        asyncio.sleep = real_sleep

    assert call_count == EXPECTED_TOTAL_ATTEMPTS
