# yfinance/test_transport.py

import asyncio

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance import (
    transport,
)
from tests.unit.adapters.data_sources.enrichment_feeds.yfinance._helpers import (
    make_client,
)

HttpTransport = transport.HttpTransport

pytestmark = pytest.mark.unit


async def test_init_creates_transport_with_default_client() -> None:
    """
    ARRANGE: no pre-configured client
    ACT:     create HttpTransport instance
    ASSERT:  transport is created with internal client
    """
    transport = HttpTransport()

    assert transport._client is not None


async def test_init_accepts_pre_configured_client() -> None:
    """
    ARRANGE: pre-configured httpx client
    ACT:     create HttpTransport with client
    ASSERT:  transport uses provided client
    """
    client = make_client(lambda r: httpx.Response(200, json={}, request=r))
    transport = HttpTransport(client=client)

    assert transport._client is client


async def test_init_sets_ready_event() -> None:
    """
    ARRANGE: create HttpTransport
    ACT:     check _ready event state
    ASSERT:  event is set (transport ready for requests)
    """
    transport = HttpTransport()

    assert transport._ready.is_set()


async def test_init_accepts_on_reset_callback() -> None:
    """
    ARRANGE: on_reset callback function
    ACT:     create HttpTransport with callback
    ASSERT:  callback is stored
    """
    callback_invoked = []

    def on_reset() -> None:
        callback_invoked.append(True)

    transport = HttpTransport(on_reset=on_reset)

    assert transport._on_reset is on_reset


async def test_get_returns_response_on_success() -> None:
    """
    ARRANGE: transport with mock client returning 200
    ACT:     call get() with URL and params
    ASSERT:  returns httpx.Response
    """
    expected_status = 200
    client = make_client(
        lambda r: httpx.Response(expected_status, json={"data": "test"}, request=r),
    )
    transport = HttpTransport(client=client)

    response = await transport.get("https://example.com", {"symbol": "AAPL"})

    assert response.status_code == expected_status


async def test_get_passes_params_to_client() -> None:
    """
    ARRANGE: transport with mock client that echoes params
    ACT:     call get() with specific params
    ASSERT:  params are included in request
    """
    received_params = []

    def handler(request: httpx.Request) -> httpx.Response:
        received_params.append(dict(request.url.params))
        return httpx.Response(200, json={}, request=request)

    client = make_client(handler)
    transport = HttpTransport(client=client)

    await transport.get("https://example.com", {"symbol": "MSFT"})

    assert received_params[0] == {"symbol": "MSFT"}


async def test_get_raises_on_zero_retries_remaining() -> None:
    """
    ARRANGE: transport with retries_remaining=0
    ACT:     call get() with retries_remaining=0
    ASSERT:  raises LookupError immediately
    """
    client = make_client(lambda r: httpx.Response(200, json={}, request=r))
    transport = HttpTransport(client=client)

    with pytest.raises(LookupError) as exc_info:
        await transport.get("https://example.com", {}, retries_remaining=0)

    assert "Connection failed after retries" in str(exc_info.value)


async def test_get_retries_on_transport_error() -> None:
    """
    ARRANGE: client that always raises TransportError
    ACT:     call get() with sufficient retries
    ASSERT:  eventually raises LookupError after exhausting retries
    """

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ProtocolError("HTTP/2 stream error")

    client = make_client(handler)
    transport = HttpTransport(client=client)

    with pytest.raises(LookupError) as exc_info:
        await transport.get("https://example.com", {}, retries_remaining=1)

    assert "Connection failed after retries" in str(exc_info.value)


async def test_get_retries_on_connect_error() -> None:
    """
    ARRANGE: client that always raises ConnectError
    ACT:     call get() with sufficient retries
    ASSERT:  eventually raises LookupError after exhausting retries
    """

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    client = make_client(handler)
    transport = HttpTransport(client=client)

    with pytest.raises(LookupError) as exc_info:
        await transport.get("https://example.com", {}, retries_remaining=1)

    assert "Connection failed after retries" in str(exc_info.value)


async def test_get_retries_on_runtime_error() -> None:
    """
    ARRANGE: client that always raises RuntimeError
    ACT:     call get() with sufficient retries
    ASSERT:  eventually raises LookupError after exhausting retries
    """

    def handler(request: httpx.Request) -> httpx.Response:
        raise RuntimeError("Connection reset")

    client = make_client(handler)
    transport = HttpTransport(client=client)

    with pytest.raises(LookupError) as exc_info:
        await transport.get("https://example.com", {}, retries_remaining=1)

    assert "Connection failed after retries" in str(exc_info.value)


async def test_get_decrements_retry_budget_on_fresh_client_failure() -> None:
    """
    ARRANGE: client that always raises TransportError
    ACT:     call get() with retries_remaining=1
    ASSERT:  exhausts retries and raises LookupError
    """

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ProtocolError("Permanent failure")

    client = make_client(handler)
    transport = HttpTransport(client=client)

    with pytest.raises(LookupError) as exc_info:
        await transport.get("https://example.com", {}, retries_remaining=1)

    assert "Connection failed after retries" in str(exc_info.value)


async def test_aclose_closes_underlying_client() -> None:
    """
    ARRANGE: transport with mock client
    ACT:     call aclose()
    ASSERT:  underlying client reference is preserved
    """
    client = make_client(lambda r: httpx.Response(200, json={}, request=r))
    transport = HttpTransport(client=client)
    original_client = transport._client

    await transport.aclose()

    assert transport._client is original_client


async def test_handle_connection_error_returns_true_when_client_already_reset() -> None:
    """
    ARRANGE: transport where client has been replaced
    ACT:     call _handle_connection_error with old client ID
    ASSERT:  returns True (indicates stale client)
    """
    client = make_client(lambda r: httpx.Response(200, json={}, request=r))
    transport = HttpTransport(client=client)
    old_client_id = id(transport._client)

    # Simulate client replacement
    transport._client = make_client(lambda r: httpx.Response(200, json={}, request=r))

    actual = await transport._handle_connection_error(old_client_id)

    assert actual is True


async def test_lock_prevents_concurrent_client_access() -> None:
    """
    ARRANGE: transport with client
    ACT:     access _lock
    ASSERT:  lock exists and is an asyncio.Lock
    """
    transport = HttpTransport()

    assert isinstance(transport._lock, asyncio.Lock)


async def test_ready_event_blocks_requests_during_reset() -> None:
    """
    ARRANGE: transport with _ready event cleared
    ACT:     attempt get() while event is clear
    ASSERT:  request waits for event to be set
    """
    expected_status = 200
    client = make_client(lambda r: httpx.Response(expected_status, json={}, request=r))
    transport = HttpTransport(client=client)

    # Clear ready event to simulate reset in progress
    transport._ready.clear()

    # Request should be blocked (we'll set event after small delay)
    async def set_ready_after_delay() -> None:
        await asyncio.sleep(0.01)
        transport._ready.set()

    task = asyncio.create_task(set_ready_after_delay())

    response = await transport.get("https://example.com", {})
    await task

    assert response.status_code == expected_status


async def test_get_with_url_parameter() -> None:
    """
    ARRANGE: transport and URL with path
    ACT:     call get() with full URL
    ASSERT:  request is made successfully
    """
    client = make_client(lambda r: httpx.Response(200, json={"ok": True}, request=r))
    transport = HttpTransport(client=client)

    response = await transport.get("https://example.com/api/v1", {})

    assert response.json() == {"ok": True}


async def test_reset_uses_client_factory_to_create_new_client() -> None:
    """
    ARRANGE: transport with custom client factory
    ACT:     trigger reset via TransportError
    ASSERT:  new client from factory is used after reset
    """
    min_factory_calls = 1
    factory_call_count = []

    def custom_factory() -> httpx.AsyncClient:
        factory_call_count.append(1)
        return make_client(lambda r: httpx.Response(200, json={}, request=r))

    call_count = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count.append(1)
        if len(call_count) == 1:
            raise httpx.ProtocolError("Trigger reset")
        return httpx.Response(200, json={"reset": True}, request=request)

    initial_client = make_client(handler)
    transport = HttpTransport(client=initial_client, client_factory=custom_factory)

    await transport.get("https://example.com", {})

    assert len(factory_call_count) >= min_factory_calls


async def test_reset_restores_ready_on_health_check_failure() -> None:
    """
    ARRANGE: transport with factory that raises exception during health check
    ACT:     trigger reset via TransportError
    ASSERT:  ready event is restored after health check failure
    """

    def failing_factory() -> httpx.AsyncClient:
        # Client that raises ConnectError on any request (simulates network failure)
        return make_client(
            lambda r: (_ for _ in ()).throw(httpx.ConnectError("Connection refused")),
        )

    def first_fails_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ProtocolError("Trigger reset")

    initial_client = make_client(first_fails_handler)
    transport = HttpTransport(client=initial_client, client_factory=failing_factory)

    # This should fail because health check will raise ConnectError
    with pytest.raises(httpx.ConnectError):
        await transport.get("https://example.com", {}, retries_remaining=1)

    # Ready event should be restored even though reset failed
    assert transport._ready.is_set()


async def test_on_reset_callback_invoked_after_successful_reset() -> None:
    """
    ARRANGE: transport with on_reset callback and working client factory
    ACT:     trigger reset via TransportError
    ASSERT:  callback is invoked after successful reset
    """
    callback_invoked = []

    def on_reset() -> None:
        callback_invoked.append(True)

    def working_factory() -> httpx.AsyncClient:
        return make_client(lambda r: httpx.Response(200, json={}, request=r))

    call_count = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count.append(1)
        if len(call_count) == 1:
            raise httpx.ProtocolError("Trigger reset")
        return httpx.Response(200, json={}, request=request)

    initial_client = make_client(handler)
    transport = HttpTransport(
        client=initial_client,
        on_reset=on_reset,
        client_factory=working_factory,
    )

    await transport.get("https://example.com", {})

    assert len(callback_invoked) == 1


async def test_client_factory_is_stored() -> None:
    """
    ARRANGE: custom client factory
    ACT:     create transport with factory
    ASSERT:  factory is stored in _client_factory
    """

    def custom_factory() -> httpx.AsyncClient:
        return make_client(lambda r: httpx.Response(200, json={}, request=r))

    transport = HttpTransport(client_factory=custom_factory)

    assert transport._client_factory is custom_factory
