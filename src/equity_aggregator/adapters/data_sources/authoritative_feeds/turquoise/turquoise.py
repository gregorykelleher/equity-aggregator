# turquoise/turquoise.py

import asyncio
import logging

from equity_aggregator.adapters.data_sources._utils import make_client
from equity_aggregator.adapters.data_sources._utils._record_types import (
    EquityRecord,
    RecordStream,
)
from equity_aggregator.storage import load_cache, save_cache

from ._utils import (
    consume_queue,
    create_deduplication_stream,
    enqueue_records,
    extract_available_exchanges,
    extract_exchange_page_data,
)
from .session import (
    TurquoiseSession,
)

logger = logging.getLogger(__name__)

_TURQUOISE_SEARCH_URL = "https://api.londonstockexchange.com/api/v1/pages"
_TURQUOISE_MARKETS_INSTRUMENTS_URL = "trade/turquoise-markets-and-instruments"

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
    session: TurquoiseSession | None = None,
    *,
    cache_key: str = "turquoise_records",
) -> RecordStream:
    """
    Yield each Turquoise equity record exactly once, using cache if available.

    If a cache is present, loads and yields records from cache. Otherwise, fetches
    available exchanges from all supported markets, streams equity records from each
    exchange concurrently, yields records as they arrive, and caches the results.

    Args:
        session (TurquoiseSession | None): Optional Turquoise session for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Parsed Turquoise equity record.
    """
    cached = load_cache(cache_key)
    if cached:
        logger.info("Loaded %d Turquoise records from cache.", len(cached))
        for record in cached:
            yield record
        return

    session = session or TurquoiseSession(make_client(headers=_HEADERS))

    try:
        async for record in _stream_and_cache(session, cache_key=cache_key):
            yield record
    finally:
        await session.aclose()


async def _stream_and_cache(
    session: TurquoiseSession,
    *,
    cache_key: str,
) -> RecordStream:
    """
    Stream unique Turquoise equity records, cache them, and yield each.

    Args:
        session (TurquoiseSession): The Turquoise session used for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Each unique Turquoise equity record as retrieved.

    Side Effects:
        Saves all streamed records to cache after streaming completes.
    """
    # collect all records in a buffer to cache them later
    buffer: list[EquityRecord] = []

    # stream all records and deduplicate by ISIN
    async for record in create_deduplication_stream(lambda record: record.get("isin"))(
        _stream_all_exchanges(session),
    ):
        buffer.append(record)
        yield record

    save_cache(cache_key, buffer)
    logger.info("Saved %d Turquoise records to cache.", len(buffer))


async def _stream_all_exchanges(
    session: TurquoiseSession,
) -> RecordStream:
    """
    Stream all Turquoise equity records across all exchanges.

    Fetches equity records from all exchanges concurrently using producer tasks.
    Each exchange is processed independently and records are yielded as they
    become available through a shared queue.

    Args:
        session (TurquoiseSession): The Turquoise session used for requests.

    Yields:
        EquityRecord: Each equity record from all exchanges, as soon as it is available.
    """
    exchanges = await _fetch_available_exchanges(session)

    if not exchanges:
        msg = "No exchanges found - cannot proceed without exchange data"
        raise ValueError(msg)

    # shared queue for all producers to enqueue records
    queue: asyncio.Queue[EquityRecord | None] = asyncio.Queue()
    exchange_ids = _extract_exchange_ids(exchanges)
    producers = _create_producer_tasks(session, exchange_ids, queue)

    # consume queue until every producer sends its sentinel
    async for record in consume_queue(queue, expected_sentinels=len(producers)):
        yield record

    await _log_producer_results(producers, exchange_ids)


def _extract_exchange_ids(exchanges: list[dict[str, str]]) -> list[str]:
    """
    Extract market IDs from list of exchange metadata dictionaries.

    Filters exchanges to only include those with valid marketid fields,
    returning a clean list of market identifiers for processing.

    Args:
        exchanges (list[dict[str, str]]): List of exchange metadata dicts
            containing marketid and other exchange information.

    Returns:
        list[str]: List of valid market IDs extracted from exchanges.
    """
    return [exchange["marketid"] for exchange in exchanges if exchange.get("marketid")]


def _create_producer_tasks(
    session: TurquoiseSession,
    exchange_ids: list[str],
    queue: asyncio.Queue[EquityRecord | None],
) -> list[asyncio.Task]:
    """
    Create async producer tasks for concurrent exchange processing.

    Each task handles fetching equity records from a single exchange,
    allowing parallel processing of multiple exchanges simultaneously.

    Args:
        session (TurquoiseSession): HTTP session for API requests.
        exchange_ids (list[str]): List of market IDs to process.
        queue (asyncio.Queue[EquityRecord | None]): Shared queue for
            collecting records from all exchanges.

    Returns:
        list[asyncio.Task]: List of async tasks, one per exchange.
    """
    return [
        asyncio.create_task(
            _produce_exchange(session, market_id, queue),
            name=f"exchange-{market_id}",
        )
        for market_id in exchange_ids
    ]


async def _log_producer_results(
    producers: list[asyncio.Task],
    exchange_ids: list[str],
) -> None:
    """
    Wait for all producer tasks to complete and log any failures.

    Gathers results from all exchange producer tasks, identifies which
    exchanges failed during processing, and logs detailed error information
    for debugging and monitoring purposes.

    Args:
        producers (list[asyncio.Task]): List of producer tasks to monitor.
        exchange_ids (list[str]): Corresponding exchange IDs for tasks,
            used to identify which exchanges failed.

    Returns:
        None
    """
    results = await asyncio.gather(*producers, return_exceptions=True)
    failed_exchanges = [
        exchange_ids[i]
        for i, result in enumerate(results)
        if isinstance(result, Exception)
    ]

    if failed_exchanges:
        logger.error(
            "Failed to process %d/%d exchanges: %s",
            len(failed_exchanges),
            len(exchange_ids),
            failed_exchanges,
        )
    # All exchanges processed successfully


async def _produce_exchange(
    session: TurquoiseSession,
    market_id: str,
    queue: asyncio.Queue[EquityRecord | None],
) -> None:
    """
    Fetch all equity records for a single exchange, label with MIC code, and
    signal completion.

    Args:
        session (TurquoiseSession): The Turquoise session for making requests.
        market_id (str): The market ID (MIC code) to fetch equities for.
        queue (asyncio.Queue[EquityRecord | None]): Queue to put records and sentinel.

    Returns:
        None

    Side Effects:
        Fetches all equity records for the exchange, labelling each record with the
        MIC code, enqueues each record into the queue, and pushes a `None` sentinel
        to signal completion. Logs fatal and raises on any error.
    """
    try:
        all_records = await _fetch_all_exchange_records(session, market_id)

        # Label all records with the MIC code for this exchange
        for record in all_records:
            record["mic_code"] = market_id

        await enqueue_records(queue, all_records)
        logger.debug("Exchange %s completed: %d records", market_id, len(all_records))

    except Exception as error:
        logger.error(
            "Exchange %s failed: %s",
            market_id,
            error,
            exc_info=True,
        )
    finally:
        await queue.put(None)


async def _fetch_all_exchange_records(
    session: TurquoiseSession,
    market_id: str,
) -> list[EquityRecord]:
    """
    Fetch all records from an exchange, handling pagination functionally.

    Retrieves the first page, determines total pages, then concurrently fetches
    remaining pages with resilient error handling and safety limits.

    Args:
        session: HTTP session for API requests.
        market_id: Exchange market identifier.

    Returns:
        Complete list of equity records from all pages.
    """
    # Fetch first page and extract pagination metadata
    first_page_data, pagination_info = await _fetch_exchange_page(session, market_id, 0)
    total_pages = _extract_total_pages(pagination_info)

    if total_pages <= 1:
        return first_page_data

    logger.debug(
        "Exchange %s: %d total pages to process",
        market_id,
        total_pages,
    )

    # Fetch remaining pages with error resilience
    remaining_pages_data = await _fetch_remaining_pages(session, market_id, total_pages)

    return first_page_data + remaining_pages_data


def _extract_total_pages(pagination_info: dict | None) -> int:
    """
    Extract the total page count from Turquoise API pagination metadata.

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
    session: TurquoiseSession,
    market_id: str,
    total_pages: int,
) -> list[EquityRecord]:
    """
    Fetch all remaining pages sequentially with error handling.

    Args:
        session: HTTP session for API requests.
        market_id: Exchange market identifier.
        total_pages: Total number of pages to fetch.

    Returns:
        Combined records from all successfully fetched remaining pages.
    """
    all_remaining_records = []
    max_pages = min(total_pages, 100)  # Safety limit

    for page in range(1, max_pages):
        try:
            page_data, _ = await _fetch_exchange_page(session, market_id, page)
            all_remaining_records.extend(page_data)
        except Exception as error:
            logger.warning(
                "Failed to fetch page %d for market %s: %s",
                page,
                market_id,
                error,
            )
            break  # Stop on first error to avoid cascade failures

    return all_remaining_records


async def _fetch_available_exchanges(session: TurquoiseSession) -> list[dict[str, str]]:
    """
    Fetch available exchanges from the Turquoise markets API.

    Args:
        session (TurquoiseSession): Session for making API requests.

    Returns:
        list[dict[str, str]]: List of available exchanges with metadata.
    """
    try:
        response = await session.get(
            _TURQUOISE_SEARCH_URL,
            params={"path": _TURQUOISE_MARKETS_INSTRUMENTS_URL},
        )
        response.raise_for_status()
        return extract_available_exchanges(response.json())
    except Exception as error:
        logger.error("Failed to fetch Turquoise exchanges: %s", error, exc_info=True)
        return []


async def _fetch_exchange_page(
    session: TurquoiseSession,
    market_id: str,
    page: int,
) -> tuple[list[EquityRecord], dict | None]:
    """
    Fetch a single page of results from Turquoise feed for specific exchange.

    Sends GET request to Turquoise pages endpoint with specified market ID and
    page number, returns parsed equity records and pagination metadata.

    Args:
        session (TurquoiseSession): Turquoise session used to send the request.
        market_id (str): Market ID to fetch equities for.
        page (int): Zero-based page number to fetch.

    Returns:
        tuple[list[EquityRecord], dict | None]: Tuple containing parsed equity
            records and pagination metadata from Turquoise feed.

    Raises:
        httpx.HTTPStatusError: If response status is not successful.
        httpx.ReadError: If there is a network or connection error.
        ValueError: If response body cannot be parsed as JSON.
    """
    parameters = f"marketid={market_id}&page={page}"
    response = await session.get(
        _TURQUOISE_SEARCH_URL,
        params={
            "path": _TURQUOISE_MARKETS_INSTRUMENTS_URL,
            "parameters": parameters,
        },
    )
    response.raise_for_status()
    return extract_exchange_page_data(response.json())
