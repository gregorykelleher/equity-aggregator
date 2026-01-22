# yfinance/test_session.py

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.config import (
    FeedConfig,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.session import (
    YFSession,
)
from tests.unit.adapters.data_sources.enrichment_feeds.yfinance._helpers import (
    make_client,
    make_session,
)

pytestmark = pytest.mark.unit


async def test_init_creates_session_with_config() -> None:
    """
    ARRANGE: FeedConfig instance
    ACT:     create YFSession
    ASSERT:  session has config
    """
    config = FeedConfig()
    session = YFSession(config)

    assert session.config is config


async def test_init_creates_session_with_custom_client() -> None:
    """
    ARRANGE: custom httpx client
    ACT:     create YFSession with client
    ASSERT:  transport uses provided client
    """
    client = make_client(lambda r: httpx.Response(200, json={}, request=r))
    config = FeedConfig()
    session = YFSession(config, client=client)

    assert session._transport._client is client


async def test_config_property_returns_feed_config() -> None:
    """
    ARRANGE: YFSession with config
    ACT:     access config property
    ASSERT:  returns FeedConfig
    """
    config = FeedConfig()
    session = YFSession(config)

    assert session.config == config


async def test_aclose_closes_transport() -> None:
    """
    ARRANGE: YFSession instance
    ACT:     call aclose()
    ASSERT:  transport client is preserved
    """
    session = make_session(lambda r: httpx.Response(200, json={}, request=r))
    original_client = session._transport._client

    await session.aclose()

    assert session._transport._client is original_client


async def test_get_returns_response_on_success() -> None:
    """
    ARRANGE: session with handler returning 200
    ACT:     call get() with URL
    ASSERT:  returns successful response
    """
    expected_status = 200
    session = make_session(
        lambda r: httpx.Response(expected_status, json={"data": "test"}, request=r),
    )

    response = await session.get("https://example.com", params={"symbol": "AAPL"})

    assert response.status_code == expected_status


async def test_get_converts_httpx_error_to_lookup_error() -> None:
    """
    ARRANGE: session with handler that raises HTTPError
    ACT:     call get()
    ASSERT:  raises LookupError
    """

    def handler(request: httpx.Request) -> httpx.Response:
        # Use TooManyRedirects - an HTTPError that's not a TransportError,
        # so it propagates to session layer without triggering transport retries
        raise httpx.TooManyRedirects("Too many redirects", request=request)

    session = make_session(handler)

    with pytest.raises(LookupError) as exc_info:
        await session.get("https://example.com", params={})

    assert "Request failed" in str(exc_info.value)


async def test_fetch_with_retry_returns_success_on_first_attempt() -> None:
    """
    ARRANGE: session with handler returning 200
    ACT:     call _fetch_with_retry()
    ASSERT:  returns response immediately
    """
    expected_status = 200
    session = make_session(
        lambda r: httpx.Response(expected_status, json={}, request=r),
    )

    response = await session._fetch_with_retry("https://example.com", {})

    assert response.status_code == expected_status


async def test_fetch_with_retry_retries_on_429_rate_limit() -> None:
    """
    ARRANGE: handler returning 429 then 200
    ACT:     call _fetch_with_retry()
    ASSERT:  retries and returns successful response
    """
    expected_status = 200
    call_count = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count.append(1)
        if len(call_count) == 1:
            return httpx.Response(429, json={}, request=request)
        return httpx.Response(expected_status, json={"success": True}, request=request)

    session = make_session(handler)

    response = await session._fetch_with_retry("https://example.com", {}, delays=[0, 0])

    assert response.status_code == expected_status


async def test_fetch_with_retry_retries_on_502_bad_gateway() -> None:
    """
    ARRANGE: handler returning 502 then 200
    ACT:     call _fetch_with_retry()
    ASSERT:  retries and returns successful response
    """
    expected_attempts = 2
    call_count = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count.append(1)
        if len(call_count) == 1:
            return httpx.Response(502, json={}, request=request)
        return httpx.Response(200, json={}, request=request)

    session = make_session(handler)

    await session._fetch_with_retry("https://example.com", {}, delays=[0, 0])

    assert len(call_count) == expected_attempts


async def test_fetch_with_retry_retries_on_503_service_unavailable() -> None:
    """
    ARRANGE: handler returning 503 then 200
    ACT:     call _fetch_with_retry()
    ASSERT:  retries and returns successful response
    """
    expected_attempts = 2
    call_count = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count.append(1)
        if len(call_count) == 1:
            return httpx.Response(503, json={}, request=request)
        return httpx.Response(200, json={}, request=request)

    session = make_session(handler)

    await session._fetch_with_retry("https://example.com", {}, delays=[0, 0])

    assert len(call_count) == expected_attempts


async def test_fetch_with_retry_retries_on_504_gateway_timeout() -> None:
    """
    ARRANGE: handler returning 504 then 200
    ACT:     call _fetch_with_retry()
    ASSERT:  retries and returns successful response
    """
    expected_attempts = 2
    call_count = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count.append(1)
        if len(call_count) == 1:
            return httpx.Response(504, json={}, request=request)
        return httpx.Response(200, json={}, request=request)

    session = make_session(handler)

    await session._fetch_with_retry("https://example.com", {}, delays=[0, 0])

    assert len(call_count) == expected_attempts


async def test_fetch_with_retry_handles_401_by_renewing_crumb() -> None:
    """
    ARRANGE: handler returning 401 then 200
    ACT:     call _fetch_with_retry()
    ASSERT:  renews crumb and retries successfully
    """
    expected_status = 200
    crumb_call_threshold = 2
    call_count = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count.append(1)
        url = str(request.url)

        # Handle crumb endpoint
        if "crumb" in url:
            return httpx.Response(expected_status, text="test-crumb", request=request)

        # First call to quote endpoint returns 401
        if len(call_count) <= crumb_call_threshold:  # First quote call + crumb call
            return httpx.Response(401, json={}, request=request)

        # Second call succeeds
        return httpx.Response(expected_status, json={"success": True}, request=request)

    config = FeedConfig()
    session = YFSession(config, client=make_client(handler))

    response = await session._fetch_with_retry(
        f"{config.quote_summary_primary_url}AAPL",
        {},
        delays=[0, 0, 0],
    )

    assert response.status_code == expected_status


async def test_fetch_with_retry_returns_non_retryable_status_immediately() -> None:
    """
    ARRANGE: handler returning 404
    ACT:     call _fetch_with_retry()
    ASSERT:  returns 404 without retrying
    """
    expected_status = 404
    call_count = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count.append(1)
        return httpx.Response(expected_status, json={}, request=request)

    session = make_session(handler)

    response = await session._fetch_with_retry("https://example.com", {})

    assert response.status_code == expected_status


async def test_renew_crumb_once_extracts_ticker_and_fetches_crumb() -> None:
    """
    ARRANGE: session with quote-summary URL
    ACT:     call _renew_crumb_once()
    ASSERT:  extracts ticker and adds crumb to params
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)

        if "crumb" in url:
            return httpx.Response(200, text="new-crumb", request=request)

        return httpx.Response(200, json={}, request=request)

    config = FeedConfig()
    session = YFSession(config, client=make_client(handler))
    params = {"symbol": "AAPL"}

    await session._renew_crumb_once(f"{config.quote_summary_primary_url}AAPL", params)

    assert params["crumb"] == "new-crumb"


async def test_renew_crumb_once_updates_params_dict() -> None:
    """
    ARRANGE: session with empty params
    ACT:     call _renew_crumb_once()
    ASSERT:  params dict is mutated with crumb
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "crumb" in url:
            return httpx.Response(200, text="fetched-crumb", request=request)
        return httpx.Response(200, json={}, request=request)

    config = FeedConfig()
    session = YFSession(config, client=make_client(handler))
    params = {}

    await session._renew_crumb_once(f"{config.quote_summary_primary_url}MSFT", params)

    assert "crumb" in params


async def test_renew_crumb_once_calls_transport_with_updated_params() -> None:
    """
    ARRANGE: session and quote-summary URL
    ACT:     call _renew_crumb_once()
    ASSERT:  returns successful response
    """
    expected_status = 200

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)

        if "crumb" in url:
            return httpx.Response(expected_status, text="retry-crumb", request=request)

        return httpx.Response(expected_status, json={"ok": True}, request=request)

    config = FeedConfig()
    session = YFSession(config, client=make_client(handler))

    response = await session._renew_crumb_once(
        f"{config.quote_summary_primary_url}TSLA",
        {},
    )

    assert response.status_code == expected_status


async def test_extract_ticker_parses_ticker_from_url() -> None:
    """
    ARRANGE: quote-summary URL with ticker
    ACT:     call _extract_ticker()
    ASSERT:  returns ticker symbol
    """
    config = FeedConfig()
    session = YFSession(config)
    url = f"{config.quote_summary_primary_url}AAPL"

    ticker = session._extract_ticker(url)

    assert ticker == "AAPL"


async def test_extract_ticker_handles_url_with_query_params() -> None:
    """
    ARRANGE: URL with ticker and query parameters
    ACT:     call _extract_ticker()
    ASSERT:  returns ticker without params
    """
    config = FeedConfig()
    session = YFSession(config)
    url = f"{config.quote_summary_primary_url}MSFT?crumb=abc123&modules=price"

    ticker = session._extract_ticker(url)

    assert ticker == "MSFT"


async def test_extract_ticker_handles_url_with_fragment() -> None:
    """
    ARRANGE: URL with ticker and fragment
    ACT:     call _extract_ticker()
    ASSERT:  returns ticker without fragment
    """
    config = FeedConfig()
    session = YFSession(config)
    url = f"{config.quote_summary_primary_url}GOOG#section"

    ticker = session._extract_ticker(url)

    assert ticker == "GOOG"


async def test_extract_ticker_handles_url_with_trailing_slash() -> None:
    """
    ARRANGE: URL with ticker and trailing slash
    ACT:     call _extract_ticker()
    ASSERT:  returns ticker only
    """
    config = FeedConfig()
    session = YFSession(config)
    url = f"{config.quote_summary_primary_url}NFLX/"

    ticker = session._extract_ticker(url)

    assert ticker == "NFLX"


async def test_get_passes_params_to_fetch() -> None:
    """
    ARRANGE: session with param-tracking handler
    ACT:     call get() with params
    ASSERT:  params are passed through
    """
    received_params = []

    def handler(request: httpx.Request) -> httpx.Response:
        received_params.append(dict(request.url.params))
        return httpx.Response(200, json={}, request=request)

    session = make_session(handler)

    await session.get("https://example.com", params={"symbol": "AAPL"})

    assert received_params[0] == {"symbol": "AAPL"}


async def test_fetch_with_retry_handles_mixed_retryable_statuses() -> None:
    """
    ARRANGE: handler returning 502, 503, then 200
    ACT:     call _fetch_with_retry()
    ASSERT:  retries through different errors
    """
    expected_attempts = 3
    second_attempt = 2
    call_count = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count.append(1)
        if len(call_count) == 1:
            return httpx.Response(502, json={}, request=request)
        if len(call_count) == second_attempt:
            return httpx.Response(503, json={}, request=request)
        return httpx.Response(200, json={}, request=request)

    session = make_session(handler)

    await session._fetch_with_retry("https://example.com", {}, delays=[0, 0, 0])

    assert len(call_count) == expected_attempts


async def test_fetch_with_retry_sleeps_on_retry() -> None:
    """
    ARRANGE: handler returning 429 then 200, with non-zero delay
    ACT:     call _fetch_with_retry() with delay_iter
    ASSERT:  sleeps briefly and retries successfully
    """
    expected_status = 200
    expected_call_count = 2
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(429, json={}, request=request)  # Rate limit
        return httpx.Response(expected_status, json={"success": True}, request=request)

    session = make_session(handler)

    # Use tiny delay to hit sleep path without timing out
    response = await session._fetch_with_retry(
        "https://example.com",
        {},
        delays=iter([0, 0.001]),
    )

    assert response.status_code == expected_status
    assert call_count == expected_call_count


async def test_fetch_with_retry_raises_after_exhausting_all_attempts() -> None:
    """
    ARRANGE: handler always returning 429
    ACT:     call _fetch_with_retry() with limited delays
    ASSERT:  raises LookupError after all attempts exhausted
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={}, request=request)

    session = make_session(handler)

    with pytest.raises(LookupError) as exc_info:
        await session._fetch_with_retry(
            "https://example.com/test",
            {},
            delays=[0, 0, 0],
        )

    assert "HTTP 429 after retries" in str(exc_info.value)
