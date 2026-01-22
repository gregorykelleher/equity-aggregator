# lseg/test_lseg.py

import httpx
import pytest

from equity_aggregator.adapters.data_sources.discovery_feeds.lseg.lseg import (
    _deduplicate_by_isin,
    _extract_total_pages,
    _fetch_all_records,
    _fetch_page,
    _fetch_remaining_pages,
    _stream_and_cache,
    fetch_equity_records,
)
from equity_aggregator.adapters.data_sources.discovery_feeds.lseg.session import (
    LsegSession,
)
from equity_aggregator.storage import save_cache

pytestmark = pytest.mark.unit


def test_deduplicate_by_isin_preserves_first_occurrence() -> None:
    """
    ARRANGE: two records with identical ISIN
    ACT:     call _deduplicate_by_isin
    ASSERT:  first record is preserved
    """
    records = [
        {"isin": "GB0001", "tidm": "FIRST"},
        {"isin": "GB0001", "tidm": "SECOND"},
    ]

    actual = _deduplicate_by_isin(records)

    assert actual[0]["tidm"] == "FIRST"


def test_deduplicate_by_isin_filters_duplicates() -> None:
    """
    ARRANGE: two records with identical ISIN
    ACT:     call _deduplicate_by_isin
    ASSERT:  only one record is returned
    """
    records = [
        {"isin": "GB0001", "tidm": "AAA"},
        {"isin": "GB0001", "tidm": "BBB"},
    ]

    actual = _deduplicate_by_isin(records)

    assert len(actual) == 1


def test_deduplicate_by_isin_filters_missing_isin() -> None:
    """
    ARRANGE: record with None ISIN
    ACT:     call _deduplicate_by_isin
    ASSERT:  record is filtered out
    """
    records = [
        {"isin": None, "tidm": "MISSING"},
    ]

    actual = _deduplicate_by_isin(records)

    assert len(actual) == 0


def test_deduplicate_by_isin_filters_empty_string_isin() -> None:
    """
    ARRANGE: record with empty string ISIN
    ACT:     call _deduplicate_by_isin
    ASSERT:  record is filtered out
    """
    records = [
        {"isin": "", "tidm": "EMPTY"},
    ]

    actual = _deduplicate_by_isin(records)

    assert len(actual) == 0


def test_deduplicate_by_isin_preserves_unique_records() -> None:
    """
    ARRANGE: three records with unique ISINs
    ACT:     call _deduplicate_by_isin
    ASSERT:  all three records are preserved
    """
    expected_unique_records_count = 3

    records = [
        {"isin": "GB0001", "tidm": "AAA"},
        {"isin": "GB0002", "tidm": "BBB"},
        {"isin": "GB0003", "tidm": "CCC"},
    ]

    actual = _deduplicate_by_isin(records)

    assert len(actual) == expected_unique_records_count


def test_deduplicate_by_isin_maintains_insertion_order() -> None:
    """
    ARRANGE: unique records in known order
    ACT:     call _deduplicate_by_isin
    ASSERT:  order is maintained
    """
    records = [
        {"isin": "GB0003", "tidm": "CCC"},
        {"isin": "GB0001", "tidm": "AAA"},
        {"isin": "GB0002", "tidm": "BBB"},
    ]

    actual = _deduplicate_by_isin(records)

    assert [r["tidm"] for r in actual] == ["CCC", "AAA", "BBB"]


def test_deduplicate_by_isin_handles_mixed_valid_and_invalid() -> None:
    """
    ARRANGE: mix of valid ISINs, None, and empty string
    ACT:     call _deduplicate_by_isin
    ASSERT:  only valid ISINs are returned
    """
    expected_valid_isin_count = 2

    records = [
        {"isin": "GB0001", "tidm": "AAA"},
        {"isin": None, "tidm": "BBB"},
        {"isin": "", "tidm": "CCC"},
        {"isin": "GB0002", "tidm": "DDD"},
    ]

    actual = _deduplicate_by_isin(records)

    assert len(actual) == expected_valid_isin_count


def test_extract_total_pages_returns_total_from_dict() -> None:
    """
    ARRANGE: pagination info with totalPages field
    ACT:     call _extract_total_pages
    ASSERT:  totalPages value is returned
    """
    expected_total_pages = 5

    pagination_info = {"totalPages": 5}

    actual = _extract_total_pages(pagination_info)

    assert actual == expected_total_pages


def test_extract_total_pages_defaults_to_one_when_none() -> None:
    """
    ARRANGE: pagination info is None
    ACT:     call _extract_total_pages
    ASSERT:  defaults to 1
    """
    actual = _extract_total_pages(None)

    assert actual == 1


def test_extract_total_pages_defaults_to_one_when_missing_field() -> None:
    """
    ARRANGE: pagination info dict without totalPages field
    ACT:     call _extract_total_pages
    ASSERT:  defaults to 1
    """
    pagination_info = {"otherField": "value"}

    actual = _extract_total_pages(pagination_info)

    assert actual == 1


async def test_fetch_equity_records_uses_cache() -> None:
    """
    ARRANGE: cache primed with LSEG records
    ACT:     collect via fetch_equity_records (no HTTP invoked)
    ASSERT:  yielded records equal the cached payload
    """
    payload = [
        {"isin": "GB0001", "tidm": "CACHED1"},
        {"isin": "GB0002", "tidm": "CACHED2"},
    ]

    save_cache("lseg_records", payload)

    async def collect() -> list[dict]:
        return [record async for record in fetch_equity_records()]

    actual = await collect()

    assert actual == payload


async def test_fetch_page_returns_records_and_pagination() -> None:
    """
    ARRANGE: mock response with one record and totalPages
    ACT:     call _fetch_page
    ASSERT:  returns records and pagination info
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "components": [
                    {
                        "type": "price-explorer",
                        "content": [
                            {
                                "name": "priceexplorersearch",
                                "value": {
                                    "content": [
                                        {
                                            "issuername": "Test Co",
                                            "tidm": "TST",
                                            "isin": "GB0001",
                                        },
                                    ],
                                    "totalPages": 5,
                                },
                            },
                        ],
                    },
                ],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    actual, pagination = await _fetch_page(session, page=0)

    assert len(actual) == 1


async def test_fetch_page_extracts_pagination_info() -> None:
    """
    ARRANGE: mock response with totalPages metadata
    ACT:     call _fetch_page
    ASSERT:  pagination info contains totalPages
    """
    expected_total_pages = 10

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "components": [
                    {
                        "type": "price-explorer",
                        "content": [
                            {
                                "name": "priceexplorersearch",
                                "value": {
                                    "content": [],
                                    "totalPages": 10,
                                },
                            },
                        ],
                    },
                ],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    _, pagination = await _fetch_page(session, page=0)

    assert pagination["totalPages"] == expected_total_pages


async def test_fetch_page_raises_on_404() -> None:
    """
    ARRANGE: handler returns 404
    ACT:     call _fetch_page
    ASSERT:  HTTPStatusError is raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    with pytest.raises(httpx.HTTPStatusError):
        await _fetch_page(session, page=0)


async def test_fetch_page_raises_on_500() -> None:
    """
    ARRANGE: handler returns 500
    ACT:     call _fetch_page
    ASSERT:  HTTPStatusError is raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    with pytest.raises(httpx.HTTPStatusError):
        await _fetch_page(session, page=0)


async def test_fetch_all_records_single_page() -> None:
    """
    ARRANGE: first page indicates totalPages is 1
    ACT:     call _fetch_all_records
    ASSERT:  only first page data is returned
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "components": [
                    {
                        "type": "price-explorer",
                        "content": [
                            {
                                "name": "priceexplorersearch",
                                "value": {
                                    "content": [{"isin": "GB0001", "tidm": "A"}],
                                    "totalPages": 1,
                                },
                            },
                        ],
                    },
                ],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    actual = await _fetch_all_records(session)

    assert len(actual) == 1


async def test_fetch_all_records_multiple_pages() -> None:
    """
    ARRANGE: first page indicates 2 total pages
    ACT:     call _fetch_all_records
    ASSERT:  records from both pages are returned
    """
    call_count = 0
    expected_total_pages = 2

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        content = [{"isin": f"GB000{call_count}", "tidm": f"T{call_count}"}]
        return httpx.Response(
            200,
            json={
                "components": [
                    {
                        "type": "price-explorer",
                        "content": [
                            {
                                "name": "priceexplorersearch",
                                "value": {
                                    "content": content,
                                    "totalPages": expected_total_pages,
                                },
                            },
                        ],
                    },
                ],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    actual = await _fetch_all_records(session)

    assert len(actual) == expected_total_pages


async def test_fetch_remaining_pages_stops_on_first_error() -> None:
    """
    ARRANGE: first remaining page returns 500
    ACT:     call _fetch_remaining_pages
    ASSERT:  returns empty list and stops
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    actual = await _fetch_remaining_pages(session, total_pages=3)

    assert actual == []


async def test_fetch_remaining_pages_collects_successful_pages() -> None:
    """
    ARRANGE: handler returns valid page data
    ACT:     call _fetch_remaining_pages with total_pages=2
    ASSERT:  one page worth of records returned
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "components": [
                    {
                        "type": "price-explorer",
                        "content": [
                            {
                                "name": "priceexplorersearch",
                                "value": {
                                    "content": [{"isin": "GB0001", "tidm": "A"}],
                                    "totalPages": 2,
                                },
                            },
                        ],
                    },
                ],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    actual = await _fetch_remaining_pages(session, total_pages=2)

    assert len(actual) == 1


async def test_stream_and_cache_deduplicates_records() -> None:
    """
    ARRANGE: mock response with duplicate ISINs
    ACT:     collect from _stream_and_cache
    ASSERT:  only unique records are yielded
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "components": [
                    {
                        "type": "price-explorer",
                        "content": [
                            {
                                "name": "priceexplorersearch",
                                "value": {
                                    "content": [
                                        {"isin": "GB0001", "tidm": "A"},
                                        {"isin": "GB0001", "tidm": "B"},
                                    ],
                                    "totalPages": 1,
                                },
                            },
                        ],
                    },
                ],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    actual = [
        record async for record in _stream_and_cache(session, cache_key="test_cache")
    ]

    assert len(actual) == 1


async def test_stream_and_cache_filters_missing_isins() -> None:
    """
    ARRANGE: mock response with one valid ISIN and one None
    ACT:     collect from _stream_and_cache
    ASSERT:  only record with valid ISIN is yielded
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "components": [
                    {
                        "type": "price-explorer",
                        "content": [
                            {
                                "name": "priceexplorersearch",
                                "value": {
                                    "content": [
                                        {"isin": "GB0001", "tidm": "A"},
                                        {"isin": None, "tidm": "B"},
                                    ],
                                    "totalPages": 1,
                                },
                            },
                        ],
                    },
                ],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    actual = [
        record
        async for record in _stream_and_cache(session, cache_key="test_cache_filter")
    ]

    assert len(actual) == 1


async def test_fetch_equity_records_streams_with_session() -> None:
    """
    ARRANGE: LsegSession with mock transport and no cache
    ACT:     iterate fetch_equity_records with session
    ASSERT:  records are streamed from HTTP
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "components": [
                    {
                        "type": "price-explorer",
                        "content": [
                            {
                                "name": "priceexplorersearch",
                                "value": {
                                    "content": [{"isin": "GB0001", "tidm": "HTTP"}],
                                    "totalPages": 1,
                                },
                            },
                        ],
                    },
                ],
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    session = LsegSession(client)

    actual = [
        record
        async for record in fetch_equity_records(
            session=session,
            cache_key="test_no_cache",
        )
    ]

    assert actual[0]["tidm"] == "HTTP"
