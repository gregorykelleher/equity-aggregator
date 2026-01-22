# stock_analysis/test_stock_analysis.py

from collections.abc import AsyncGenerator

import httpx
import pytest

from equity_aggregator.adapters.data_sources.discovery_feeds.stock_analysis.stock_analysis import (  # noqa: E501
    _deduplicate_records,
    _stream_and_cache,
    _stream_stock_analysis,
    fetch_equity_records,
)
from equity_aggregator.storage import save_cache

pytestmark = pytest.mark.unit


async def test_deduplicate_records_filters_duplicates() -> None:
    """
    ARRANGE: two records share identical ISIN
    ACT:     run deduplicator
    ASSERT:  yields a single unique record
    """

    async def _gen() -> AsyncGenerator[dict, None]:
        yield {"isin": "US1234567890"}
        yield {"isin": "US1234567890"}

    deduplicator = _deduplicate_records(lambda record: record.get("isin"))

    uniques = [record async for record in deduplicator(_gen())]

    assert len(uniques) == 1


async def test_deduplicate_records_preserves_first_occurrence() -> None:
    """
    ARRANGE: first record differs from duplicate that follows
    ACT:     run deduplicator
    ASSERT:  first record is preserved
    """

    async def _gen() -> AsyncGenerator[dict, None]:
        yield {"isin": "US1234567890", "name": "FIRST"}
        yield {"isin": "US1234567890", "name": "SECOND"}

    deduplicator = _deduplicate_records(lambda record: record.get("isin"))

    uniques = [record async for record in deduplicator(_gen())]

    assert uniques[0]["name"] == "FIRST"


async def test_stream_stock_analysis_yields_records() -> None:
    """
    ARRANGE: mock HTTP response carrying one record
    ACT:     iterate _stream_stock_analysis
    ASSERT:  yielded record contains expected symbol
    """
    payload = {
        "data": {
            "data": [
                {
                    "s": "FOO",
                    "n": "Foo Inc",
                    "cusip": "123456789",
                    "isin": "US1234567890",
                },
            ],
        },
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [record async for record in _stream_stock_analysis(client)]

    assert records[0]["s"] == "FOO"


async def test_stream_stock_analysis_empty_data() -> None:
    """
    ARRANGE: mock HTTP response with empty data list
    ACT:     iterate _stream_stock_analysis
    ASSERT:  no records yielded
    """
    payload = {"data": {"data": []}}

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [record async for record in _stream_stock_analysis(client)]

    assert len(records) == 0


async def test_stream_stock_analysis_missing_data_key() -> None:
    """
    ARRANGE: mock HTTP response without data key
    ACT:     iterate _stream_stock_analysis
    ASSERT:  no records yielded
    """
    payload = {"data": {}}

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [record async for record in _stream_stock_analysis(client)]

    assert len(records) == 0


async def test_stream_stock_analysis_filters_empty_records() -> None:
    """
    ARRANGE: mock HTTP response with one null record and one valid record
    ACT:     iterate _stream_stock_analysis
    ASSERT:  only valid record is yielded
    """
    payload = {
        "data": {
            "data": [
                None,
                {
                    "s": "FOO",
                    "n": "Foo Inc",
                    "cusip": "123456789",
                    "isin": "US1234567890",
                },
            ],
        },
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [record async for record in _stream_stock_analysis(client)]

    assert len(records) == 1


async def test_stream_stock_analysis_raises_on_http_error() -> None:
    """
    ARRANGE: mock transport returns 500 error
    ACT:     iterate _stream_stock_analysis
    ASSERT:  HTTPStatusError is raised
    """

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(httpx.HTTPStatusError):
            async for _ in _stream_stock_analysis(client):
                pass


async def test_stream_and_cache_deduplicates_and_caches() -> None:
    """
    ARRANGE: duplicate records in mocked transport
    ACT:     run _stream_and_cache
    ASSERT:  only one record yielded (deduplicated)
    """
    payload = {
        "data": {
            "data": [
                {
                    "s": "FOO",
                    "n": "Foo Inc",
                    "cusip": "123456789",
                    "isin": "US1234567890",
                },
                {
                    "s": "FOO",
                    "n": "Foo Inc",
                    "cusip": "123456789",
                    "isin": "US1234567890",
                },
            ],
        },
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [
            record
            async for record in _stream_and_cache(
                client,
                cache_key="stock_analysis_test_cache",
            )
        ]

    assert len(records) == 1


async def test_fetch_equity_records_uses_existing_cache() -> None:
    """
    ARRANGE: pre-populate cache with a single record
    ACT:     call fetch_equity_records
    ASSERT:  record comes straight from cache (no HTTP required)
    """
    expected = [
        {
            "s": "CCC",
            "n": "Cached Co",
            "cusip": "999999999",
            "isin": "US9999999999",
        },
    ]
    save_cache("stock_analysis_records", expected)

    records = [record async for record in fetch_equity_records()]

    assert records == expected


async def test_fetch_equity_records_streams_with_supplied_client() -> None:
    """
    ARRANGE: AsyncClient with MockTransport serves two identical records
    ACT:     call fetch_equity_records with that client and an empty cache
    ASSERT:  only one unique record is yielded (deduplicated)
    """
    payload = {
        "data": {
            "data": [
                {
                    "s": "FOO",
                    "n": "Foo Inc",
                    "cusip": "123456789",
                    "isin": "US1234567890",
                },
                {
                    "s": "FOO",
                    "n": "Foo Inc",
                    "cusip": "123456789",
                    "isin": "US1234567890",
                },
            ],
        },
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=200, json=payload)

    transport = httpx.MockTransport(_respond)
    client = httpx.AsyncClient(transport=transport)

    records = [
        record
        async for record in fetch_equity_records(
            client=client,
            cache_key="stock_analysis_branch_cov",
        )
    ]

    assert len(records) == 1


async def test_stream_stock_analysis_yields_multiple_records() -> None:
    """
    ARRANGE: mock HTTP response with three records
    ACT:     iterate _stream_stock_analysis
    ASSERT:  three records are yielded
    """
    payload = {
        "data": {
            "data": [
                {"s": "FOO", "n": "Foo Inc", "cusip": None, "isin": "US1111111111"},
                {"s": "BAR", "n": "Bar Inc", "cusip": None, "isin": "US2222222222"},
                {"s": "BAZ", "n": "Baz Inc", "cusip": None, "isin": "US3333333333"},
            ],
        },
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [record async for record in _stream_stock_analysis(client)]

    assert len(records) == 3  # noqa: PLR2004


async def test_stream_stock_analysis_preserves_all_fields() -> None:
    """
    ARRANGE: mock HTTP response with record containing financial data
    ACT:     iterate _stream_stock_analysis
    ASSERT:  record contains all expected fields
    """
    payload = {
        "data": {
            "data": [
                {
                    "s": "FOO",
                    "n": "Foo Inc",
                    "cusip": "123456789",
                    "isin": "US1234567890",
                    "marketCap": 1000000,
                    "price": 100.50,
                    "volume": 5000000,
                    "peRatio": 25.5,
                    "sector": "Technology",
                    "industry": "Software",
                    "revenue": 10000000,
                    "fcf": 5000000,
                    "roe": 15.5,
                    "roa": 8.5,
                    "ebitda": 2000000,
                },
            ],
        },
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [record async for record in _stream_stock_analysis(client)]

    assert records[0]["marketCap"] == 1000000  # noqa: PLR2004


async def test_deduplicate_records_preserves_unique_records() -> None:
    """
    ARRANGE: three records with unique ISINs
    ACT:     run deduplicator
    ASSERT:  all three records are yielded
    """

    async def _gen() -> AsyncGenerator[dict, None]:
        yield {"isin": "US1111111111", "symbol": "FOO"}
        yield {"isin": "US2222222222", "symbol": "BAR"}
        yield {"isin": "US3333333333", "symbol": "BAZ"}

    deduplicator = _deduplicate_records(lambda record: record.get("isin"))

    uniques = [record async for record in deduplicator(_gen())]

    assert len(uniques) == 3  # noqa: PLR2004


async def test_stream_and_cache_preserves_record_order() -> None:
    """
    ARRANGE: three unique records in mocked transport
    ACT:     run _stream_and_cache
    ASSERT:  first record symbol is "FOO"
    """
    payload = {
        "data": {
            "data": [
                {"s": "FOO", "n": "Foo Inc", "cusip": None, "isin": "US1111111111"},
                {"s": "BAR", "n": "Bar Inc", "cusip": None, "isin": "US2222222222"},
                {"s": "BAZ", "n": "Baz Inc", "cusip": None, "isin": "US3333333333"},
            ],
        },
    }

    def _respond(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(_respond)

    async with httpx.AsyncClient(transport=transport) as client:
        records = [
            record
            async for record in _stream_and_cache(
                client,
                cache_key="stock_analysis_order_test",
            )
        ]

    assert records[0]["s"] == "FOO"
