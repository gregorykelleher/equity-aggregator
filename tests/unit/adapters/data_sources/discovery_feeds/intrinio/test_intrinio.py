# discovery_feeds/intrinio/test_intrinio.py

import os
from collections.abc import Generator

import httpx
import pytest

from equity_aggregator.adapters.data_sources.discovery_feeds.intrinio.intrinio import (
    _attach_quote,
    _attach_quotes_to_all,
    _deduplicate_by_share_class_figi,
    _fetch_all_companies,
    _fetch_all_securities,
    _fetch_companies_page,
    _fetch_company_securities,
    _fetch_quote,
    _get_api_key,
    _stream_and_cache,
    fetch_equity_records,
)
from equity_aggregator.adapters.data_sources.discovery_feeds.intrinio.session import (
    IntrinioSession,
)
from equity_aggregator.storage import save_cache

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def set_intrinio_api_key() -> Generator[None, None, None]:
    """
    Set INTRINIO_API_KEY for tests that require HTTP calls.
    """
    prev = os.environ.get("INTRINIO_API_KEY")
    os.environ["INTRINIO_API_KEY"] = "test_key"
    yield
    if prev is None:
        os.environ.pop("INTRINIO_API_KEY", None)
    else:
        os.environ["INTRINIO_API_KEY"] = prev


def make_session(
    handler: callable,
) -> IntrinioSession:
    """
    Create an IntrinioSession with a mock transport handler.
    """
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return IntrinioSession(client)


def test_get_api_key_returns_key_when_set() -> None:
    """
    ARRANGE: INTRINIO_API_KEY environment variable is set
    ACT:     call _get_api_key
    ASSERT:  returns the API key value
    """
    os.environ["INTRINIO_API_KEY"] = "test_key_123"

    actual = _get_api_key()

    assert actual == "test_key_123"


def test_get_api_key_raises_when_not_set() -> None:
    """
    ARRANGE: INTRINIO_API_KEY environment variable is not set
    ACT:     call _get_api_key
    ASSERT:  raises ValueError
    """
    os.environ.pop("INTRINIO_API_KEY", None)

    with pytest.raises(ValueError, match="INTRINIO_API_KEY environment variable"):
        _get_api_key()


def test_deduplicate_by_share_class_figi_removes_duplicates() -> None:
    """
    ARRANGE: two records with same share_class_figi
    ACT:     call _deduplicate_by_share_class_figi
    ASSERT:  returns single record
    """
    records = [
        {"share_class_figi": "BBG000B9XRY4", "ticker": "AAPL"},
        {"share_class_figi": "BBG000B9XRY4", "ticker": "AAPL2"},
    ]

    actual = _deduplicate_by_share_class_figi(records)

    assert len(actual) == 1


def test_deduplicate_by_share_class_figi_preserves_first() -> None:
    """
    ARRANGE: two records with same share_class_figi
    ACT:     call _deduplicate_by_share_class_figi
    ASSERT:  first record is preserved
    """
    records = [
        {"share_class_figi": "BBG000B9XRY4", "ticker": "FIRST"},
        {"share_class_figi": "BBG000B9XRY4", "ticker": "SECOND"},
    ]

    actual = _deduplicate_by_share_class_figi(records)

    assert actual[0]["ticker"] == "FIRST"


def test_deduplicate_by_share_class_figi_skips_missing_figi() -> None:
    """
    ARRANGE: record without share_class_figi
    ACT:     call _deduplicate_by_share_class_figi
    ASSERT:  returns empty list
    """
    records = [{"ticker": "NOTK"}]

    actual = _deduplicate_by_share_class_figi(records)

    assert len(actual) == 0


def test_deduplicate_by_share_class_figi_skips_none_figi() -> None:
    """
    ARRANGE: record with None share_class_figi
    ACT:     call _deduplicate_by_share_class_figi
    ASSERT:  returns empty list
    """
    records = [{"share_class_figi": None, "ticker": "NOTK"}]

    actual = _deduplicate_by_share_class_figi(records)

    assert len(actual) == 0


def test_deduplicate_by_share_class_figi_preserves_unique() -> None:
    """
    ARRANGE: three records with unique share_class_figis
    ACT:     call _deduplicate_by_share_class_figi
    ASSERT:  all three records preserved
    """
    expected_unique_count = 3
    records = [
        {"share_class_figi": "BBG000B9XRY4"},
        {"share_class_figi": "BBG000BVPV84"},
        {"share_class_figi": "BBG000BPH459"},
    ]

    actual = _deduplicate_by_share_class_figi(records)

    assert len(actual) == expected_unique_count


def test_deduplicate_by_share_class_figi_maintains_order() -> None:
    """
    ARRANGE: unique records in specific order
    ACT:     call _deduplicate_by_share_class_figi
    ASSERT:  order is maintained
    """
    records = [
        {"share_class_figi": "BBG000C", "ticker": "C"},
        {"share_class_figi": "BBG000A", "ticker": "A"},
        {"share_class_figi": "BBG000B", "ticker": "B"},
    ]

    actual = _deduplicate_by_share_class_figi(records)

    assert [r["ticker"] for r in actual] == ["C", "A", "B"]


async def test_fetch_companies_page_returns_records() -> None:
    """
    ARRANGE: mock response with one company
    ACT:     call _fetch_companies_page
    ASSERT:  returns one record
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "companies": [{"id": "com_1", "ticker": "AAPL", "name": "Apple Inc"}],
            },
        )

    session = make_session(handler)
    records, _ = await _fetch_companies_page(session, None)

    assert len(records) == 1


async def test_fetch_companies_page_returns_next_page_token() -> None:
    """
    ARRANGE: mock response with next_page token
    ACT:     call _fetch_companies_page
    ASSERT:  returns next_page token
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"companies": [], "next_page": "token123"})

    session = make_session(handler)
    _, next_page = await _fetch_companies_page(session, None)

    assert next_page == "token123"


async def test_fetch_companies_page_includes_next_page_param() -> None:
    """
    ARRANGE: call with next_page token
    ACT:     call _fetch_companies_page
    ASSERT:  request includes next_page param
    """
    captured_params = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_params.update(dict(request.url.params))
        return httpx.Response(200, json={"companies": []})

    session = make_session(handler)
    await _fetch_companies_page(session, "prev_token")

    assert captured_params.get("next_page") == "prev_token"


async def test_fetch_all_companies_single_page() -> None:
    """
    ARRANGE: single page response (no next_page)
    ACT:     call _fetch_all_companies
    ASSERT:  returns all records from single page
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"companies": [{"id": "com_1", "ticker": "AAPL", "name": "Apple"}]},
        )

    session = make_session(handler)
    records = await _fetch_all_companies(session)

    assert len(records) == 1


async def test_fetch_all_companies_multiple_pages() -> None:
    """
    ARRANGE: first page returns next_page, second page has no next_page
    ACT:     call _fetch_all_companies
    ASSERT:  returns records from both pages
    """
    expected_total_records = 2
    call_count = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(
                200,
                json={
                    "companies": [{"id": "1", "ticker": "A", "name": "A Inc"}],
                    "next_page": "page2",
                },
            )
        return httpx.Response(
            200,
            json={"companies": [{"id": "2", "ticker": "B", "name": "B Inc"}]},
        )

    session = make_session(handler)
    records = await _fetch_all_companies(session)

    assert len(records) == expected_total_records


async def test_fetch_company_securities_returns_securities() -> None:
    """
    ARRANGE: mock response with one security and company data
    ACT:     call _fetch_company_securities
    ASSERT:  returns one security record
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "securities": [
                    {"ticker": "AAPL", "share_class_figi": "BBG000B9XRY4"},
                ],
                "company": {
                    "id": "com_123",
                    "ticker": "AAPL",
                    "name": "Apple Inc",
                    "lei": "LEI123",
                },
            },
        )

    session = make_session(handler)
    company = {"company_ticker": "AAPL", "name": "Apple Inc"}
    records = await _fetch_company_securities(session, company)

    assert len(records) == 1


async def test_fetch_company_securities_returns_empty_on_error() -> None:
    """
    ARRANGE: mock response returns 404
    ACT:     call _fetch_company_securities
    ASSERT:  returns empty list
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    session = make_session(handler)
    company = {"company_ticker": "NOTK", "name": "Not Found Inc"}
    records = await _fetch_company_securities(session, company)

    assert records == []


async def test_fetch_company_securities_uses_response_company_data() -> None:
    """
    ARRANGE: response company data differs from input company
    ACT:     call _fetch_company_securities
    ASSERT:  record uses LEI from response, not input company
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "securities": [{"ticker": "B", "share_class_figi": "BBG001S5N9P3"}],
                "company": {
                    "id": "com_correct",
                    "ticker": "B",
                    "name": "Barrick Gold Corp",
                    "lei": "CORRECT_LEI_FROM_RESPONSE",
                },
            },
        )

    session = make_session(handler)
    stale_company = {
        "company_ticker": "B",
        "name": "Barnes Group Inc",
        "lei": "STALE_LEI_FROM_COMPANIES_ENDPOINT",
    }
    records = await _fetch_company_securities(session, stale_company)

    assert records[0]["lei"] == "CORRECT_LEI_FROM_RESPONSE"


async def test_fetch_all_securities_flattens_results() -> None:
    """
    ARRANGE: two companies each with one security
    ACT:     call _fetch_all_securities
    ASSERT:  returns flattened list of two securities
    """
    expected_total_securities = 2
    call_count = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(
            200,
            json={
                "securities": [
                    {
                        "ticker": f"T{call_count}",
                        "share_class_figi": f"BBG{call_count}",
                    },
                ],
                "company": {
                    "id": f"com_{call_count}",
                    "ticker": f"T{call_count}",
                    "name": f"Company {call_count}",
                },
            },
        )

    session = make_session(handler)
    companies = [
        {"company_ticker": "A", "name": "A Inc"},
        {"company_ticker": "B", "name": "B Inc"},
    ]
    records = await _fetch_all_securities(session, companies)

    assert len(records) == expected_total_securities


async def test_fetch_quote_returns_quote_data() -> None:
    """
    ARRANGE: mock response with quote data
    ACT:     call _fetch_quote
    ASSERT:  returns quote dictionary
    """
    expected_price = 150.0

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"last": 150.0, "market_volume": 1000000})

    session = make_session(handler)
    quote = await _fetch_quote(session, "BBG000B9XRY4")

    assert quote["last"] == expected_price


async def test_fetch_quote_returns_none_on_error() -> None:
    """
    ARRANGE: mock response returns 404
    ACT:     call _fetch_quote
    ASSERT:  returns None
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    session = make_session(handler)
    quote = await _fetch_quote(session, "BBG000NOTFOUND")

    assert quote is None


async def test_attach_quote_adds_quote_to_security() -> None:
    """
    ARRANGE: security record and mock quote response
    ACT:     call _attach_quote
    ASSERT:  returned record includes quote
    """
    expected_price = 100.0

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"last": 100.0})

    session = make_session(handler)
    security = {"share_class_figi": "BBG000B9XRY4", "ticker": "AAPL"}
    actual = await _attach_quote(session, security)

    assert actual["quote"]["last"] == expected_price


async def test_attach_quote_preserves_security_fields() -> None:
    """
    ARRANGE: security record with existing fields
    ACT:     call _attach_quote
    ASSERT:  original fields are preserved
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    session = make_session(handler)
    security = {"share_class_figi": "BBG000B9XRY4", "ticker": "AAPL", "name": "Apple"}
    actual = await _attach_quote(session, security)

    assert actual["ticker"] == "AAPL"


async def test_attach_quotes_to_all_processes_all_securities() -> None:
    """
    ARRANGE: two securities
    ACT:     call _attach_quotes_to_all
    ASSERT:  returns two records with quotes
    """
    expected_count = 2

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"last": 50.0})

    session = make_session(handler)
    securities = [
        {"share_class_figi": "BBG000A", "ticker": "A"},
        {"share_class_figi": "BBG000B", "ticker": "B"},
    ]
    actual = await _attach_quotes_to_all(session, securities)

    assert len(actual) == expected_count


async def test_stream_and_cache_yields_unique_records() -> None:
    """
    ARRANGE: mock responses for companies, securities, quotes
    ACT:     collect from _stream_and_cache
    ASSERT:  yields unique records
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/securities" in url and "/quote" in url:
            return httpx.Response(200, json={"last": 100.0})
        if "/securities" in url:
            return httpx.Response(
                200,
                json={
                    "securities": [
                        {"ticker": "AAPL", "share_class_figi": "BBG000B9XRY4"},
                    ],
                    "company": {
                        "id": "1",
                        "ticker": "AAPL",
                        "name": "Apple",
                        "lei": "LEI123",
                    },
                },
            )
        return httpx.Response(
            200,
            json={"companies": [{"id": "1", "ticker": "AAPL", "name": "Apple"}]},
        )

    session = make_session(handler)
    records = [r async for r in _stream_and_cache(session, cache_key="test_stream")]

    assert len(records) == 1


async def test_fetch_equity_records_uses_cache() -> None:
    """
    ARRANGE: cache primed with records
    ACT:     collect via fetch_equity_records
    ASSERT:  yields cached records
    """
    cached = [{"share_class_figi": "BBG000CACHED", "ticker": "CACH"}]
    save_cache("intrinio_test_cache", cached)

    records = [r async for r in fetch_equity_records(cache_key="intrinio_test_cache")]

    assert records == cached


async def test_fetch_equity_records_streams_with_session() -> None:
    """
    ARRANGE: IntrinioSession with mock transport and empty cache
    ACT:     iterate fetch_equity_records
    ASSERT:  yields records from HTTP
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/securities" in url and "/quote" in url:
            return httpx.Response(200, json={"last": 50.0})
        if "/securities" in url:
            return httpx.Response(
                200,
                json={
                    "securities": [{"ticker": "TST", "share_class_figi": "BBG000TEST"}],
                    "company": {
                        "id": "1",
                        "ticker": "TST",
                        "name": "Test Inc",
                        "lei": "LEI456",
                    },
                },
            )
        return httpx.Response(
            200,
            json={"companies": [{"id": "1", "ticker": "TST", "name": "Test Inc"}]},
        )

    session = make_session(handler)
    records = [
        r
        async for r in fetch_equity_records(
            session=session,
            cache_key="intrinio_no_cache_test",
        )
    ]

    assert records[0]["ticker"] == "TST"
