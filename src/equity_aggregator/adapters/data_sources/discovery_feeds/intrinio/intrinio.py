# intrinio/intrinio.py

import asyncio
import logging
import os

from equity_aggregator.adapters.data_sources._utils import make_client
from equity_aggregator.adapters.data_sources._utils._record_types import (
    EquityRecord,
    RecordStream,
)
from equity_aggregator.storage import load_cache, save_cache

from ._utils import parse_companies_response, parse_securities_response
from .session import IntrinioSession

logger = logging.getLogger(__name__)

_INTRINIO_COMPANIES_URL = "https://api-v2.intrinio.com/companies"
_INTRINIO_SECURITIES_URL = "https://api-v2.intrinio.com/securities"
_PAGE_SIZE = 5000


async def fetch_equity_records(
    session: IntrinioSession | None = None,
    *,
    cache_key: str = "intrinio_records",
) -> RecordStream:
    """
    Yield each Intrinio security record with quote data, using cache if available.

    Fetches all companies, then for each company fetches its securities. Each
    security (keyed by share_class_figi) becomes a separate equity record with
    quote data attached.

    Args:
        session (IntrinioSession | None): Optional Intrinio session for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Parsed Intrinio security record with company and quote data.

    Raises:
        ValueError: If INTRINIO_API_KEY environment variable is not set.
    """
    cached = load_cache(cache_key)

    if cached:
        logger.info("Loaded %d Intrinio records from cache.", len(cached))
        for record in cached:
            yield record
        return

    _get_api_key()  # Validate API key is set before proceeding

    session = session or IntrinioSession(make_client())

    try:
        async for record in _stream_and_cache(session, cache_key=cache_key):
            yield record
    finally:
        await session.aclose()


async def _stream_and_cache(
    session: IntrinioSession,
    *,
    cache_key: str,
) -> RecordStream:
    """
    Stream unique Intrinio security records with quotes, cache them, and yield each.

    Fetches all companies, then fetches securities for each company. Each security
    gets quote data attached. Records are deduplicated by share_class_figi.

    Args:
        session (IntrinioSession): The Intrinio session used for requests.
        cache_key (str): The key under which to cache the records.

    Yields:
        EquityRecord: Each unique security record with company and quote data.

    Side Effects:
        Saves all streamed records to cache after streaming completes.
    """
    all_companies = await _fetch_all_companies(session)
    all_securities = await _fetch_all_securities(session, all_companies)
    all_records = await _attach_quotes_to_all(session, all_securities)
    unique_records = _deduplicate_by_share_class_figi(all_records)

    for record in unique_records:
        yield record

    save_cache(cache_key, unique_records)
    logger.info("Saved %d Intrinio records to cache.", len(unique_records))


async def _fetch_all_companies(session: IntrinioSession) -> list[EquityRecord]:
    """
    Fetch all company records from Intrinio, handling pagination.

    Args:
        session (IntrinioSession): The Intrinio session used for requests.

    Returns:
        list[EquityRecord]: Complete list of company records from all pages.
    """
    all_companies: list[EquityRecord] = []
    next_page: str | None = None

    while True:
        page_records, next_page = await _fetch_companies_page(session, next_page)
        all_companies.extend(page_records)

        if not next_page:
            return all_companies


async def _fetch_all_securities(
    session: IntrinioSession,
    companies: list[EquityRecord],
) -> list[EquityRecord]:
    """
    Fetch securities for all companies concurrently.

    For each company, fetches its securities from the securities endpoint.
    A company may have multiple securities (e.g., common stock, preferred shares).

    Args:
        session (IntrinioSession): The Intrinio session used for requests.
        companies (list[EquityRecord]): List of company records.

    Returns:
        list[EquityRecord]: Flattened list of security records with company data.
    """
    tasks = [_fetch_company_securities(session, company) for company in companies]
    results = await asyncio.gather(*tasks)

    all_securities = []
    for securities in results:
        all_securities.extend(securities)

    return all_securities


async def _attach_quotes_to_all(
    session: IntrinioSession,
    securities: list[EquityRecord],
) -> list[EquityRecord]:
    """
    Attach quote data to all securities concurrently.

    Args:
        session (IntrinioSession): The Intrinio session used for requests.
        securities (list[EquityRecord]): List of security records.

    Returns:
        list[EquityRecord]: Security records with quote data attached.
    """
    tasks = [_attach_quote(session, security) for security in securities]
    return await asyncio.gather(*tasks)


def _deduplicate_by_share_class_figi(
    records: list[EquityRecord],
) -> list[EquityRecord]:
    """
    Deduplicate records by share_class_figi, maintaining insertion order.

    Args:
        records (list[EquityRecord]): The list of equity records to deduplicate.

    Returns:
        list[EquityRecord]: Deduplicated list of equity records.
    """
    seen: set[str] = set()
    unique: list[EquityRecord] = []

    for record in records:
        share_class_figi = record.get("share_class_figi")

        if not share_class_figi or share_class_figi in seen:
            continue

        seen.add(share_class_figi)
        unique.append(record)

    return unique


async def _fetch_companies_page(
    session: IntrinioSession,
    next_page: str | None,
) -> tuple[list[EquityRecord], str | None]:
    """
    Fetch a single page of results from the Intrinio companies endpoint.

    Args:
        session (IntrinioSession): The Intrinio session used for requests.
        next_page (str | None): The pagination token for the next page.

    Returns:
        tuple[list[EquityRecord], str | None]: Tuple of (parsed records,
            next_page token).
    """
    params = {"api_key": _get_api_key(), "page_size": str(_PAGE_SIZE)}

    if next_page:
        params["next_page"] = next_page

    response = await session.get(_INTRINIO_COMPANIES_URL, params=params)
    response.raise_for_status()
    return parse_companies_response(response.json())


async def _fetch_company_securities(
    session: IntrinioSession,
    company: EquityRecord,
) -> list[EquityRecord]:
    """
    Fetch securities for a single company.

    A company may have multiple securities. Company data is extracted from the
    securities response itself (not the passed company record) to avoid ticker
    reassignment issues where stale company records have incorrect identifiers.

    Args:
        session (IntrinioSession): The Intrinio session used for requests.
        company (EquityRecord): Company record containing company_ticker.

    Returns:
        list[EquityRecord]: List of security records for this company.
    """
    company_ticker = company.get("company_ticker")
    url = f"{_INTRINIO_COMPANIES_URL}/{company_ticker}/securities"
    params = {"api_key": _get_api_key()}

    try:
        response = await session.get(url, params=params)
        response.raise_for_status()
        return parse_securities_response(response.json())
    except Exception:
        return []


async def _attach_quote(
    session: IntrinioSession,
    security: EquityRecord,
) -> EquityRecord:
    """
    Fetch quote data for a security and attach it to the record.

    Uses share_class_figi as the identifier for the quote lookup.

    Args:
        session (IntrinioSession): The Intrinio session used for requests.
        security (EquityRecord): Security record to attach quote to.

    Returns:
        EquityRecord: Security record with quote data attached.
    """
    share_class_figi = security.get("share_class_figi")
    quote = await _fetch_quote(session, share_class_figi)
    return {**security, "quote": quote}


async def _fetch_quote(
    session: IntrinioSession,
    share_class_figi: str,
) -> dict | None:
    """
    Fetch quote data from Intrinio API using share_class_figi.

    Args:
        session (IntrinioSession): The Intrinio session used for requests.
        share_class_figi (str): The share class FIGI identifier.

    Returns:
        dict | None: Quote data dictionary, or None if fetch fails.
    """
    url = f"{_INTRINIO_SECURITIES_URL}/{share_class_figi}/quote"
    params = {"api_key": _get_api_key()}

    try:
        response = await session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception:
        return None


def _get_api_key() -> str:
    """
    Retrieve the Intrinio API key from environment variables.

    Returns:
        str: The Intrinio API key.

    Raises:
        ValueError: If INTRINIO_API_KEY environment variable is not set.
    """
    api_key = os.getenv("INTRINIO_API_KEY")
    if not api_key:
        raise ValueError("INTRINIO_API_KEY environment variable not set")
    return api_key
