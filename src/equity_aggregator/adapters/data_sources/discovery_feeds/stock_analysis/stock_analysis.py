# stock_analysis/stock_analysis.py

import logging

from httpx import AsyncClient

from equity_aggregator.adapters.data_sources._utils import make_client
from equity_aggregator.adapters.data_sources._utils._record_types import (
    EquityRecord,
    RecordStream,
    RecordUniqueKeyExtractor,
    UniqueRecordStream,
)
from equity_aggregator.storage import load_cache, save_cache

logger = logging.getLogger(__name__)

_STOCK_ANALYSIS_SEARCH_URL = "https://stockanalysis.com/api/screener/s/f"

_PARAMS = {
    # Primary metric to use for screening/sorting
    "m": "marketCap",
    # Sort order (desc = descending, asc = ascending)
    "s": "desc",
    # Comma-separated list of columns/fields to return in the response
    "c": (
        "s,n,cusip,isin,marketCap,price,volume,peRatio,sector,"
        "industry,revenue,fcf,roe,roa,ebitda"
    ),
    # Instrument type/universe to screen (allstocks = all available stocks)
    "i": "allstocks",
}


async def fetch_equity_records(
    client: AsyncClient | None = None,
    *,
    cache_key: str = "stock_analysis_records",
) -> RecordStream:
    """
    Yield each Stock Analysis equity record exactly once, using cache if available.

    If a cache is present, loads and yields records from cache. Otherwise, streams
    all records in a single request, yields records as they arrive, and caches the
    results.

    Args:
        client (AsyncClient | None): Optional HTTP client to use for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Parsed Stock Analysis equity record.
    """
    cached = load_cache(cache_key)

    if cached:
        logger.info("Loaded %d Stock Analysis records from cache.", len(cached))
        for record in cached:
            yield record
        return

    # use provided client or create a bespoke stock analysis client
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
    Stream Stock Analysis equity records, deduplicate by ISIN, cache them, and yield
    each record.

    Args:
        client (AsyncClient): HTTP client for Stock Analysis requests.
        cache_key (str): Key under which to store cached records.

    Returns:
        RecordStream: Async iterator yielding unique EquityRecord objects.
    """
    buffer: list[EquityRecord] = []

    async for record in _deduplicate_records(lambda record: record.get("isin"))(
        _stream_stock_analysis(client),
    ):
        buffer.append(record)
        yield record

    save_cache(cache_key, buffer)
    logger.info("Saved %d Stock Analysis records to cache.", len(buffer))


async def _stream_stock_analysis(client: AsyncClient) -> RecordStream:
    """
    Fetch and stream Stock Analysis equity records from the screener endpoint.

    Args:
        client (AsyncClient): HTTP client for making requests.

    Yields:
        EquityRecord: Each valid Stock Analysis equity record.
    """
    response = await client.get(_STOCK_ANALYSIS_SEARCH_URL, params=_PARAMS)
    response.raise_for_status()

    payload = response.json()
    data_wrapper = payload.get("data", {})
    records = data_wrapper.get("data", [])

    for record in records:
        if record:
            yield record


def _deduplicate_records(extract_key: RecordUniqueKeyExtractor) -> UniqueRecordStream:
    """
    Creates a deduplication coroutine for async iterators of dictionaries, yielding only
    unique records based on a key extracted from each record.
    Args:
        extract_key (RecordUniqueKeyExtractor): A function that takes a
            dictionary record and returns a value used to determine uniqueness.
    Returns:
        UniqueRecordStream: A coroutine that accepts an async iterator of dictionaries,
            yields only unique records, as determined by the extracted key.
    """

    async def deduplicator(records: RecordStream) -> RecordStream:
        """
        Deduplicate async iterator of dicts by a key extracted from each record.

        Args:
            records (RecordStream): Async iterator of records to deduplicate.

        Yields:
            EquityRecord: Unique records, as determined by the extracted key.
        """
        seen: set[object] = set()
        async for record in records:
            key = extract_key(record)
            if key in seen:
                continue
            seen.add(key)
            yield record

    return deduplicator
