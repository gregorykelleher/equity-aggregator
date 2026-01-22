# tradingview/test_tradingview.py

import httpx
import pytest

from equity_aggregator.adapters.data_sources.discovery_feeds.tradingview.tradingview import (  # noqa: E501
    _deduplicate_by_symbol,
    _fetch_all_records,
    _fetch_page,
    _parse_response,
    _parse_row,
    _stream_and_cache,
    fetch_equity_records,
)
from equity_aggregator.storage import save_cache

pytestmark = pytest.mark.unit


def test_parse_row_extracts_symbol_and_name() -> None:
    """
    ARRANGE: valid TradingView row with data array
    ACT:     parse row
    ASSERT:  extracts symbol from d[0]
    """
    row = {
        "s": "NYSE:AAPL",
        "d": [
            "AAPL",
            "Apple Inc.",
            "NYSE",
            "USD",
            150.0,
            2500000000000,
            50000000,
            0.005,
            15000000000,
            16000000000,
            500000000000,
            100000000000,
            25.5,
            5.5,
            6.5,
            15.5,
            8.5,
            "Technology",
            "Consumer Electronics",
        ],
    }

    record = _parse_row(row)

    assert record is not None


def test_parse_row_preserves_exchange_symbol() -> None:
    """
    ARRANGE: valid TradingView row
    ACT:     parse row
    ASSERT:  preserves 's' field with exchange:symbol format
    """
    row = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."] + [None] * 17,
    }

    record = _parse_row(row)

    assert record["s"] == "NYSE:AAPL"


def test_parse_row_returns_none_for_missing_row() -> None:
    """
    ARRANGE: None row
    ACT:     parse row
    ASSERT:  returns None
    """
    record = _parse_row(None)

    assert record is None


def test_parse_row_returns_none_for_short_array() -> None:
    """
    ARRANGE: row with data array shorter than expected
    ACT:     parse row
    ASSERT:  returns None
    """
    row = {
        "s": "NYSE:AAPL",
        "d": ["AAPL"],  # Only 1 element instead of 19
    }

    record = _parse_row(row)

    assert record is None


def test_parse_row_logs_warning_for_short_array(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    ARRANGE: row with short data array
    ACT:     parse row
    ASSERT:  warning is logged about invalid array length
    """
    row = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc."],  # Only 2 elements instead of 19
    }

    _parse_row(row)

    assert "Invalid data array length: expected 19, got 2" in caplog.text


def test_parse_row_returns_none_for_null_data_array() -> None:
    """
    ARRANGE: row with null data array
    ACT:     parse row
    ASSERT:  returns None
    """
    row = {
        "s": "NYSE:AAPL",
        "d": None,  # Explicitly null
    }

    record = _parse_row(row)

    assert record is None


def test_parse_row_returns_none_for_missing_symbol() -> None:
    """
    ARRANGE: row with None symbol
    ACT:     parse row
    ASSERT:  returns None
    """
    row = {
        "s": "NYSE:AAPL",
        "d": [None, "Apple Inc."] + [None] * 17,
    }

    record = _parse_row(row)

    assert record is None


def test_parse_row_returns_none_for_missing_name() -> None:
    """
    ARRANGE: row with None name
    ACT:     parse row
    ASSERT:  returns None
    """
    row = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", None] + [None] * 17,
    }

    record = _parse_row(row)

    assert record is None


def test_parse_response_extracts_records() -> None:
    """
    ARRANGE: TradingView response with one record
    ACT:     parse response
    ASSERT:  returns one record
    """
    payload = {
        "totalCount": 1,
        "data": [
            {
                "s": "NYSE:AAPL",
                "d": ["AAPL", "Apple Inc."] + [None] * 17,
            },
        ],
    }

    records, total_count = _parse_response(payload)

    assert len(records) == 1


def test_parse_response_extracts_total_count() -> None:
    """
    ARRANGE: TradingView response with totalCount
    ACT:     parse response
    ASSERT:  extracts total count
    """
    payload = {
        "totalCount": 4664,
        "data": [],
    }

    _, total_count = _parse_response(payload)

    assert total_count == 4664  # noqa: PLR2004


def test_parse_response_handles_missing_data() -> None:
    """
    ARRANGE: response without data key
    ACT:     parse response
    ASSERT:  returns empty records list
    """
    payload = {"totalCount": 0}

    records, _ = _parse_response(payload)

    assert len(records) == 0


def test_parse_response_filters_invalid_rows() -> None:
    """
    ARRANGE: response with one valid and one invalid row
    ACT:     parse response
    ASSERT:  returns only valid record
    """
    payload = {
        "totalCount": 2,
        "data": [
            {"s": "NYSE:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
            {"s": "NYSE:INVALID", "d": ["INVALID"]},  # Short array
        ],
    }

    records, _ = _parse_response(payload)

    assert len(records) == 1


async def test_fetch_page_returns_records_and_count() -> None:
    """
    ARRANGE: mock HTTP response with TradingView data
    ACT:     fetch page
    ASSERT:  returns records and total count
    """
    payload = {
        "totalCount": 1000,
        "data": [
            {"s": "NYSE:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
        ],
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records, total_count = await _fetch_page(client, 0, 1000)

    assert len(records) == 1


async def test_fetch_page_raises_on_http_error() -> None:
    """
    ARRANGE: mock transport returns 500 error
    ACT:     fetch page
    ASSERT:  HTTPStatusError is raised
    """

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await _fetch_page(client, 0, 1000)


async def test_fetch_all_records_single_page() -> None:
    """
    ARRANGE: mock response with totalCount <= batch size
    ACT:     fetch all records
    ASSERT:  makes only one request
    """
    payload = {
        "totalCount": 500,
        "data": [
            {"s": "NYSE:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
        ],
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = await _fetch_all_records(client)

    assert len(records) == 1


async def test_fetch_all_records_multiple_pages() -> None:
    """
    ARRANGE: mock response requiring pagination (totalCount > batch size)
    ACT:     fetch all records
    ASSERT:  returns records from all pages
    """
    request_count = 0

    def _respond(_: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        # First request returns totalCount indicating 2 pages needed
        if request_count == 1:
            payload = {
                "totalCount": 1500,
                "data": [
                    {"s": "NYSE:FOO", "d": ["FOO", "Foo Inc."] + [None] * 17},
                ],
            }
        else:
            payload = {
                "totalCount": 1500,
                "data": [
                    {"s": "NYSE:BAR", "d": ["BAR", "Bar Inc."] + [None] * 17},
                ],
            }

        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = await _fetch_all_records(client)

    assert len(records) == 2  # noqa: PLR2004


async def test_fetch_all_records_handles_page_failure() -> None:
    """
    ARRANGE: second page request fails with error
    ACT:     fetch all records
    ASSERT:  returns partial results from first page only
    """
    request_count = 0

    def _respond(_: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        if request_count == 1:
            return httpx.Response(
                200,
                json={
                    "totalCount": 2000,
                    "data": [
                        {"s": "NYSE:FOO", "d": ["FOO", "Foo Inc."] + [None] * 17},
                    ],
                },
            )
        return httpx.Response(500)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = await _fetch_all_records(client)

    assert len(records) == 1


def test_deduplicate_by_symbol_filters_duplicates() -> None:
    """
    ARRANGE: two records with identical symbol
    ACT:     deduplicate by symbol
    ASSERT:  returns single unique record
    """
    records = [
        {"s": "NYSE:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
        {"s": "NASDAQ:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
    ]

    unique = _deduplicate_by_symbol(records)

    assert len(unique) == 1


def test_deduplicate_by_symbol_preserves_first_occurrence() -> None:
    """
    ARRANGE: duplicate records where first has different exchange
    ACT:     deduplicate by symbol
    ASSERT:  first occurrence is preserved
    """
    records = [
        {"s": "NYSE:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
        {"s": "NASDAQ:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
    ]

    unique = _deduplicate_by_symbol(records)

    assert unique[0]["s"] == "NYSE:AAPL"


def test_deduplicate_by_symbol_preserves_unique_records() -> None:
    """
    ARRANGE: three records with unique symbols
    ACT:     deduplicate by symbol
    ASSERT:  all three records are preserved
    """
    records = [
        {"s": "NYSE:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
        {"s": "NYSE:MSFT", "d": ["MSFT", "Microsoft Corp."] + [None] * 17},
        {"s": "NYSE:GOOGL", "d": ["GOOGL", "Alphabet Inc."] + [None] * 17},
    ]

    unique = _deduplicate_by_symbol(records)

    assert len(unique) == 3  # noqa: PLR2004


def test_deduplicate_by_symbol_skips_missing_symbol() -> None:
    """
    ARRANGE: record with missing symbol in data array
    ACT:     deduplicate by symbol
    ASSERT:  record is skipped
    """
    records = [
        {"s": "NYSE:AAPL", "d": []},  # Empty array, no symbol
    ]

    unique = _deduplicate_by_symbol(records)

    assert len(unique) == 0


async def test_stream_and_cache_deduplicates_and_caches() -> None:
    """
    ARRANGE: mock response with duplicate records
    ACT:     stream and cache
    ASSERT:  only unique records are yielded
    """
    payload = {
        "totalCount": 2,
        "data": [
            {"s": "NYSE:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
            {"s": "NASDAQ:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
        ],
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [
            record
            async for record in _stream_and_cache(
                client,
                cache_key="tradingview_test_cache",
            )
        ]

    assert len(records) == 1


async def test_stream_and_cache_preserves_record_order() -> None:
    """
    ARRANGE: three unique records in mock response
    ACT:     stream and cache
    ASSERT:  first record symbol is "AAPL"
    """
    payload = {
        "totalCount": 3,
        "data": [
            {"s": "NYSE:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
            {"s": "NYSE:MSFT", "d": ["MSFT", "Microsoft Corp."] + [None] * 17},
            {"s": "NYSE:GOOGL", "d": ["GOOGL", "Alphabet Inc."] + [None] * 17},
        ],
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [
            record
            async for record in _stream_and_cache(
                client,
                cache_key="tradingview_order_test",
            )
        ]

    assert records[0]["d"][0] == "AAPL"


async def test_fetch_equity_records_uses_existing_cache() -> None:
    """
    ARRANGE: pre-populate cache with a record
    ACT:     call fetch_equity_records
    ASSERT:  record comes from cache (no HTTP required)
    """
    expected = [
        {"s": "NYSE:CCC", "d": ["CCC", "Cached Co."] + [None] * 17},
    ]
    save_cache("tradingview_records", expected)

    records = [record async for record in fetch_equity_records()]

    assert records == expected


async def test_fetch_equity_records_streams_with_supplied_client() -> None:
    """
    ARRANGE: AsyncClient with MockTransport serves duplicate records
    ACT:     call fetch_equity_records with that client
    ASSERT:  only unique records are yielded (deduplicated)
    """
    payload = {
        "totalCount": 2,
        "data": [
            {"s": "NYSE:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
            {"s": "NASDAQ:AAPL", "d": ["AAPL", "Apple Inc."] + [None] * 17},
        ],
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)
    client = httpx.AsyncClient(transport=transport)

    records = [
        record
        async for record in fetch_equity_records(
            client=client,
            cache_key="tradingview_branch_cov",
        )
    ]

    assert len(records) == 1


async def test_fetch_equity_records_handles_empty_response() -> None:
    """
    ARRANGE: mock response with no data
    ACT:     call fetch_equity_records
    ASSERT:  no records are yielded
    """
    payload = {
        "totalCount": 0,
        "data": [],
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)
    client = httpx.AsyncClient(transport=transport)

    records = [
        record
        async for record in fetch_equity_records(
            client=client,
            cache_key="tradingview_empty_test",
        )
    ]

    assert len(records) == 0


def test_parse_row_handles_null_in_data_array() -> None:
    """
    ARRANGE: row with null values in data array
    ACT:     parse row
    ASSERT:  record is created successfully
    """
    row = {
        "s": "NYSE:AAPL",
        "d": ["AAPL", "Apple Inc.", None, "USD", None] + [None] * 14,
    }

    record = _parse_row(row)

    assert record is not None


def test_parse_response_preserves_all_fields() -> None:
    """
    ARRANGE: response with record containing all financial data
    ACT:     parse response
    ASSERT:  record contains complete data array
    """
    payload = {
        "totalCount": 1,
        "data": [
            {
                "s": "NYSE:AAPL",
                "d": [
                    "AAPL",
                    "Apple Inc.",
                    "NYSE",
                    "USD",
                    150.0,
                    2500000000000,
                    50000000,
                    0.005,
                    15000000000,
                    16000000000,
                    500000000000,
                    100000000000,
                    25.5,
                    5.5,
                    6.5,
                    15.5,
                    8.5,
                    "Technology",
                    "Consumer Electronics",
                ],
            },
        ],
    }

    records, _ = _parse_response(payload)

    assert len(records[0]["d"]) == 19  # noqa: PLR2004
