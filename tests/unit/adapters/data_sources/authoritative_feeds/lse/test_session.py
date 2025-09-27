# lse/test_session.py

import asyncio
from collections import deque

import httpx
import pytest

from equity_aggregator.adapters.data_sources.authoritative_feeds.lse.session import (
    LseSession,
)
from tests.unit.adapters.data_sources.enrichment_feeds.yfinance._helpers import (
    close,
    make_client,
)

pytestmark = pytest.mark.unit


async def test_get_defaults_params_to_empty_dict() -> None:
    """
    ARRANGE: handler that records received query parameters
    ACT:     call get() without params
    ASSERT:  handler sees an empty param dict
    """
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200)

    session = LseSession(make_client(handler))

    await session.get("https://dummy.com")
    await close(session._client)

    assert captured["params"] == {}


async def test_get_passes_through_params() -> None:
    """
    ARRANGE: handler that records received query parameters
    ACT:     call get() with specific params
    ASSERT:  handler sees the provided params
    """
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200)

    session = LseSession(make_client(handler))

    await session.get("https://dummy.com", params={"key": "value"})
    await close(session._client)

    assert captured["params"] == {"key": "value"}


async def test_post_sends_json_payload() -> None:
    """
    ARRANGE: handler that records received JSON payload
    ACT:     call post() with specific JSON
    ASSERT:  handler sees the provided JSON
    """
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        import json

        captured["json"] = json.loads(request.content)
        return httpx.Response(200)

    session = LseSession(make_client(handler))

    await session.post("https://dummy.com", json={"key": "value"})
    await close(session._client)

    assert captured["json"] == {"key": "value"}


async def test_aclose_marks_client_closed() -> None:
    """
    ARRANGE: fresh session
    ACT:     call aclose()
    ASSERT:  client reports closed
    """
    client = make_client(lambda r: httpx.Response(200))
    session = LseSession(client)

    await session.aclose()

    assert client.is_closed


async def test_get_retries_after_403_forbidden() -> None:
    """
    ARRANGE: server replies [403, 200]
    ACT:     session.get()
    ASSERT:  final status code is 200
    """
    responses = deque([httpx.Response(403), httpx.Response(200)])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return responses.popleft()

    session = LseSession(make_client(handler))

    response = await session.get("https://dummy.com")
    await close(session._client)

    assert response.status_code == 200


async def test_post_retries_after_403_forbidden() -> None:
    """
    ARRANGE: server replies [403, 200]
    ACT:     session.post()
    ASSERT:  final status code is 200
    """
    responses = deque([httpx.Response(403), httpx.Response(200)])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return responses.popleft()

    session = LseSession(make_client(handler))

    response = await session.post("https://dummy.com", json={"test": "data"})
    await close(session._client)

    assert response.status_code == 200


async def test_get_raises_lookup_error_after_max_retries() -> None:
    """
    ARRANGE: server always replies 403
    ACT:     session.get()
    ASSERT:  LookupError is raised with descriptive message
    """

    async def always_403(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    # Mock the sleep function to return immediately
    real_sleep = asyncio.sleep

    async def instant_sleep(_delay: float) -> None:
        return None

    asyncio.sleep = instant_sleep

    try:
        session = LseSession(make_client(always_403))

        with pytest.raises(LookupError, match="HTTP 403 Forbidden after retries"):
            await session.get("https://dummy.com")

        await close(session._client)

    finally:
        asyncio.sleep = real_sleep


async def test_post_raises_lookup_error_after_max_retries() -> None:
    """
    ARRANGE: server always replies 403
    ACT:     session.post()
    ASSERT:  LookupError is raised with descriptive message
    """

    async def always_403(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    # Mock the sleep function to return immediately
    real_sleep = asyncio.sleep

    async def instant_sleep(_delay: float) -> None:
        return None

    asyncio.sleep = instant_sleep

    try:
        session = LseSession(make_client(always_403))

        with pytest.raises(LookupError, match="HTTP 403 Forbidden after retries"):
            await session.post("https://dummy.com", json={"test": "data"})

        await close(session._client)

    finally:
        asyncio.sleep = real_sleep


async def test_get_returns_non_403_immediately() -> None:
    """
    ARRANGE: server replies with 404 (not 403)
    ACT:     session.get()
    ASSERT:  404 response is returned without retries
    """
    call_count = {"count": 0}

    async def handler(_request: httpx.Request) -> httpx.Response:
        call_count["count"] += 1
        return httpx.Response(404)

    session = LseSession(make_client(handler))

    response = await session.get("https://dummy.com")
    await close(session._client)

    assert response.status_code == 404
    assert call_count["count"] == 1


async def test_post_returns_non_403_immediately() -> None:
    """
    ARRANGE: server replies with 500 (not 403)
    ACT:     session.post()
    ASSERT:  500 response is returned without retries
    """
    call_count = {"count": 0}

    async def handler(_request: httpx.Request) -> httpx.Response:
        call_count["count"] += 1
        return httpx.Response(500)

    session = LseSession(make_client(handler))

    response = await session.post("https://dummy.com", json={"test": "data"})
    await close(session._client)

    assert response.status_code == 500
    assert call_count["count"] == 1


async def test_get_with_backoff_respects_max_attempts() -> None:
    """
    ARRANGE: server replies 403 for first 3 calls, then 200
    ACT:     session.get() with mocked sleep
    ASSERT:  exactly 3 retries occur before success
    """
    call_count = {"count": 0}

    async def handler(_request: httpx.Request) -> httpx.Response:
        call_count["count"] += 1
        if call_count["count"] <= 3:
            return httpx.Response(403)
        return httpx.Response(200)

    # Mock the sleep function to return immediately
    real_sleep = asyncio.sleep

    async def instant_sleep(_delay: float) -> None:
        return None

    asyncio.sleep = instant_sleep

    try:
        session = LseSession(make_client(handler))

        response = await session.get("https://dummy.com")
        await close(session._client)

        assert response.status_code == 200
        assert call_count["count"] == 4  # 1 initial + 3 retries

    finally:
        asyncio.sleep = real_sleep


async def test_post_with_backoff_respects_max_attempts() -> None:
    """
    ARRANGE: server replies 403 for first 2 calls, then 200
    ACT:     session.post() with mocked sleep
    ASSERT:  exactly 2 retries occur before success
    """
    call_count = {"count": 0}

    async def handler(_request: httpx.Request) -> httpx.Response:
        call_count["count"] += 1
        if call_count["count"] <= 2:
            return httpx.Response(403)
        return httpx.Response(200)

    # Mock the sleep function to return immediately
    real_sleep = asyncio.sleep

    async def instant_sleep(_delay: float) -> None:
        return None

    asyncio.sleep = instant_sleep

    try:
        session = LseSession(make_client(handler))

        response = await session.post("https://dummy.com", json={"test": "data"})
        await close(session._client)

        assert response.status_code == 200
        assert call_count["count"] == 3  # 1 initial + 2 retries

    finally:
        asyncio.sleep = real_sleep
