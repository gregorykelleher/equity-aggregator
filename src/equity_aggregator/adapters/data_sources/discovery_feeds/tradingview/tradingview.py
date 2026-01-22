# tradingview/tradingview.py

import logging
import math

from httpx import AsyncClient

from equity_aggregator.adapters.data_sources._utils import make_client
from equity_aggregator.adapters.data_sources._utils._record_types import (
    EquityRecord,
    RecordStream,
)
from equity_aggregator.storage import load_cache, save_cache

logger = logging.getLogger(__name__)

_TRADINGVIEW_SCAN_URL = "https://scanner.tradingview.com/america/scan"
_PAGE_SIZE = 1000
_EXPECTED_ARRAY_LENGTH = 19

_REQUEST_BODY_TEMPLATE = {
    "markets": ["america"],
    "symbols": {
        "query": {"types": ["stock"]},
        "tickers": [],
    },
    "options": {"lang": "en"},
    "filter": [],  # Empty to fetch all stocks
    "columns": [
        "name",
        "description",
        "exchange",
        "currency",
        "close",
        "market_cap_basic",
        "volume",
        "dividends_yield_current",
        "float_shares_outstanding",
        "total_shares_outstanding_fundamental",
        "total_revenue_ttm",
        "ebitda_ttm",
        "price_earnings_ttm",
        "price_book_fq",
        "earnings_per_share_basic_ttm",
        "return_on_equity_fq",
        "return_on_assets_fq",
        "sector",
        "industry",
    ],
    "sort": {"sortBy": "name", "sortOrder": "asc"},
    # range is set per request for pagination
}


async def fetch_equity_records(
    client: AsyncClient | None = None,
    *,
    cache_key: str = "tradingview_records",
) -> RecordStream:
    """
    Yield each TradingView equity record exactly once, using cache if available.

    If a cache is present, loads and yields records from cache. Otherwise, fetches
    all equity records from the TradingView scanner endpoint by paginating through
    all available pages, deduplicates by symbol, yields records, and caches results.

    Args:
        client (AsyncClient | None): Optional HTTP client to use for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Parsed TradingView equity record.
    """
    cached = load_cache(cache_key)
    if cached:
        logger.info("Loaded %d TradingView records from cache.", len(cached))
        for record in cached:
            yield record
        return

    client = client or make_client()

    async with client:
        async for record in _stream_and_cache(client, cache_key=cache_key):
            yield record


async def _stream_and_cache(
    client: AsyncClient,
    *,
    cache_key: str,
) -> RecordStream:
    """
    Stream TradingView equity records, deduplicate by symbol, cache them, and yield.

    Fetches all records using pagination, deduplicates by symbol to ensure uniqueness,
    then yields each record and caches the complete set.

    Args:
        client (AsyncClient): The HTTP client used for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Each unique TradingView equity record as retrieved.

    Side Effects:
        Saves all streamed records to cache after streaming completes.
    """
    all_records = await _fetch_all_records(client)
    unique_records = _deduplicate_by_symbol(all_records)

    for record in unique_records:
        yield record

    save_cache(cache_key, unique_records)
    logger.info("Saved %d TradingView records to cache.", len(unique_records))


async def _fetch_all_records(client: AsyncClient) -> list[EquityRecord]:
    """
    Fetch all equity records from TradingView scanner, handling pagination.

    Retrieves the first page to determine total count, then fetches remaining
    pages sequentially. Stops on first error to avoid cascade failures.

    Args:
        client (AsyncClient): The HTTP client used for requests.

    Returns:
        list[EquityRecord]: All fetched equity records across all pages.
    """
    # Fetch first page to get total count
    first_page_records, total_count = await _fetch_page(client, 0, _PAGE_SIZE)

    if total_count <= _PAGE_SIZE:
        return first_page_records

    # Calculate total pages needed
    total_pages = math.ceil(total_count / _PAGE_SIZE)
    all_records = first_page_records

    # Fetch remaining pages sequentially
    for page in range(1, total_pages):
        start = page * _PAGE_SIZE
        end = start + _PAGE_SIZE

        try:
            page_records, _ = await _fetch_page(client, start, end)
            all_records.extend(page_records)
        except Exception as error:
            logger.warning(
                "Failed to fetch page range [%d, %d]: %s. Returning partial results.",
                start,
                end,
                error,
            )
            break

    return all_records


async def _fetch_page(
    client: AsyncClient,
    start: int,
    end: int,
) -> tuple[list[EquityRecord], int]:
    """
    Fetch a single page of results from TradingView scanner.

    Args:
        client (AsyncClient): The HTTP client used for requests.
        start (int): Starting index for pagination range.
        end (int): Ending index for pagination range.

    Returns:
        tuple[list[EquityRecord], int]: Tuple of (parsed records, total count from API).
    """
    request_body = {**_REQUEST_BODY_TEMPLATE, "range": [start, end]}

    response = await client.post(_TRADINGVIEW_SCAN_URL, json=request_body)
    response.raise_for_status()

    payload = response.json()
    return _parse_response(payload)


def _parse_response(payload: dict) -> tuple[list[EquityRecord], int]:
    """
    Parse TradingView API response into equity records.

    Extracts the data array and total count from the response payload,
    then parses each item into an EquityRecord.

    Args:
        payload (dict): The JSON response from TradingView API.

    Returns:
        tuple[list[EquityRecord], int]: Tuple of (parsed records, total count).
    """
    data = payload.get("data", [])
    total_count = payload.get("totalCount", 0)

    records = []
    for row in data:
        record = _parse_row(row)
        if record:
            records.append(record)

    return records, total_count


def _parse_row(row: dict | None) -> EquityRecord | None:
    """
    Parse a single TradingView API response row into an EquityRecord.

    Args:
        row (dict | None): A single row from the TradingView API response.

    Returns:
        EquityRecord | None: The parsed equity record, or None if invalid.
    """
    # Validate row exists and has data array
    if not row:
        return None

    d = row.get("d", [])
    if not d or len(d) < _EXPECTED_ARRAY_LENGTH:
        if d is not None:
            logger.warning(
                "Invalid data array length: expected %d, got %d",
                _EXPECTED_ARRAY_LENGTH,
                len(d),
            )
        return None

    # Extract and validate required fields, then build the equity record
    symbol = d[0]
    name = d[1]

    return (
        {
            "s": row.get("s"),  # Preserve original exchange:symbol format
            "d": d,  # Pass the full data array to schema for processing
        }
        if symbol and name
        else None
    )


def _deduplicate_by_symbol(records: list[EquityRecord]) -> list[EquityRecord]:
    """
    Deduplicate records by symbol, maintaining insertion order.

    Args:
        records (list[EquityRecord]): The list of equity records to deduplicate.

    Returns:
        list[EquityRecord]: List of unique records, preserving first occurrence.
    """
    seen_symbols: set[str] = set()
    unique: list[EquityRecord] = []

    for record in records:
        # Extract symbol from the data array
        d = record.get("d", [])
        symbol = d[0] if d and len(d) > 0 else None

        if not symbol:
            continue

        if symbol not in seen_symbols:
            seen_symbols.add(symbol)
            unique.append(record)

    return unique
