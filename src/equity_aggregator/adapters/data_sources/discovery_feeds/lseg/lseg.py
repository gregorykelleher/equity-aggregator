# lseg/lseg.py

import logging

from equity_aggregator.adapters.data_sources._utils import make_client
from equity_aggregator.adapters.data_sources._utils._record_types import (
    EquityRecord,
    RecordStream,
)
from equity_aggregator.storage import load_cache, save_cache

from ._utils import parse_response
from .session import (
    LsegSession,
)

logger = logging.getLogger(__name__)

_LSEG_SEARCH_URL = "https://api.londonstockexchange.com/api/v1/pages"
_LSEG_PATH = "live-markets/market-data-dashboard/price-explorer"
_LSEG_BASE_PARAMS = "categories=EQUITY"

_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Origin": "https://www.londonstockexchange.com",
    "Pragma": "no-cache",
    "Referer": "https://www.londonstockexchange.com/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


async def fetch_equity_records(
    session: LsegSession | None = None,
    *,
    cache_key: str = "lseg_records",
) -> RecordStream:
    """
    Yield each LSEG equity record exactly once, using cache if available.

    If a cache is present, loads and yields records from cache. Otherwise, fetches
    all equity records from the LSEG price-explorer endpoint by paginating through
    all available pages, deduplicates by ISIN, yields records, and caches the results.

    Args:
        session (LsegSession | None): Optional LSEG session for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Parsed LSEG equity record.
    """
    cached = load_cache(cache_key)
    if cached:
        logger.info("Loaded %d LSEG records from cache.", len(cached))
        for record in cached:
            yield record
        return

    session = session or LsegSession(make_client(headers=_HEADERS))

    try:
        async for record in _stream_and_cache(session, cache_key=cache_key):
            yield record
    finally:
        await session.aclose()


async def _stream_and_cache(
    session: LsegSession,
    *,
    cache_key: str,
) -> RecordStream:
    """
    Stream unique LSEG equity records, cache them, and yield each.

    Fetches all records, deduplicates by ISIN (filtering out records with
    missing or empty ISINs), then yields and caches the unique records. The
    cache write is skipped when the underlying fetch was incomplete, so a
    partial dataset is never persisted.

    Args:
        session (LsegSession): The LSEG session used for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Each unique LSEG equity record as retrieved.

    Side Effects:
        Saves all streamed records to cache after a complete fetch.
    """
    all_records, is_complete = await _fetch_all_records(session)
    unique_records = _deduplicate_by_isin(all_records)

    for record in unique_records:
        yield record

    if not is_complete:
        logger.warning("LSEG fetch incomplete; skipping cache write.")
        return

    save_cache(cache_key, unique_records)
    logger.info("Saved %d LSEG records to cache.", len(unique_records))


async def _fetch_all_records(
    session: LsegSession,
) -> tuple[list[EquityRecord], bool]:
    """
    Fetch all equity records from the price-explorer endpoint, handling pagination.

    Retrieves the first page, determines total pages, then fetches remaining pages
    sequentially. A first-page failure propagates; a later-page failure stops
    paging and reports the result as incomplete.

    Args:
        session: HTTP session for API requests.

    Returns:
        tuple[list[EquityRecord], bool]: All fetched records and a flag that is
            True only when every page was fetched successfully.
    """
    # Fetch first page and extract pagination metadata
    first_page_data, pagination_info = await _fetch_page(session, 0)
    total_pages = _extract_total_pages(pagination_info)

    if total_pages <= 1:
        return first_page_data, True

    # Fetch remaining pages with error resilience
    remaining_pages_data, is_complete = await _fetch_remaining_pages(
        session,
        total_pages,
    )

    return first_page_data + remaining_pages_data, is_complete


async def _fetch_page(
    session: LsegSession,
    page: int,
) -> tuple[list[EquityRecord], dict | None]:
    """
    Fetch a single page of results from LSEG price-explorer endpoint.

    Sends GET request to LSEG pages endpoint with the specified page number,
    returns parsed equity records and pagination metadata.

    Args:
        session (LsegSession): LSEG session used to send the request.
        page (int): Zero-based page number to fetch.

    Returns:
        tuple[list[EquityRecord], dict | None]: Tuple containing parsed equity
            records and pagination metadata from LSEG feed.

    Raises:
        httpx.HTTPStatusError: If response status is not successful.
        httpx.ReadError: If there is a network or connection error.
        ValueError: If response body cannot be parsed as JSON.
    """
    parameters = f"{_LSEG_BASE_PARAMS}&page={page}"
    response = await session.get(
        _LSEG_SEARCH_URL,
        params={
            "path": _LSEG_PATH,
            "parameters": parameters,
        },
    )
    response.raise_for_status()
    return parse_response(response.json())


def _extract_total_pages(pagination_info: dict | None) -> int:
    """
    Extract the total page count from LSEG API pagination metadata.

    Safely retrieves the totalPages field from pagination info, providing a
    sensible default of 1 when pagination data is missing or invalid.

    Args:
        pagination_info (dict | None): Pagination metadata from API response,
            expected to contain a 'totalPages' field, or None if unavailable.

    Returns:
        int: Total number of pages available, defaulting to 1 if pagination
            info is missing or does not contain the totalPages field.
    """
    return pagination_info.get("totalPages", 1) if pagination_info else 1


async def _fetch_remaining_pages(
    session: LsegSession,
    total_pages: int,
) -> tuple[list[EquityRecord], bool]:
    """
    Fetch all remaining pages sequentially with error handling.

    Stops at the first page that fails and reports the result as incomplete, so
    the caller can avoid caching a partial dataset.

    Args:
        session: HTTP session for API requests.
        total_pages: Total number of pages to fetch.

    Returns:
        tuple[list[EquityRecord], bool]: Records gathered so far and a flag that
            is True only when every remaining page was fetched successfully.
    """
    all_remaining_records = []

    for page in range(1, total_pages):
        try:
            page_data, _ = await _fetch_page(session, page)
        except Exception as error:
            logger.warning(
                "Failed to fetch LSEG page %d; caching skipped: %s",
                page,
                error,
            )
            return all_remaining_records, False
        all_remaining_records.extend(page_data)

    return all_remaining_records, True


def _deduplicate_by_isin(records: list[EquityRecord]) -> list[EquityRecord]:
    """
    Deduplicate equity records by ISIN, maintaining insertion order.

    Filters out records with missing or empty ISINs, then removes duplicates
    by keeping the first occurrence of each unique ISIN.

    Args:
        records (list[EquityRecord]): List of equity records to deduplicate.

    Returns:
        list[EquityRecord]: Deduplicated list of equity records.
    """
    seen_isins: set[str] = set()
    unique: list[EquityRecord] = []

    for record in records:
        isin = record.get("isin")

        if not isin:
            continue

        if isin not in seen_isins:
            seen_isins.add(isin)
            unique.append(record)

    return unique
