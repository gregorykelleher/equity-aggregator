# gleif/api.py

import asyncio
import logging
from collections.abc import Callable

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

from ._utils import backoff_delays, strip_corporate_suffix
from .config import GleifConfig

logger = logging.getLogger(__name__)

_config = GleifConfig()

_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset(
    {
        httpx.codes.TOO_MANY_REQUESTS,  # 429
        httpx.codes.BAD_GATEWAY,  # 502
        httpx.codes.SERVICE_UNAVAILABLE,  # 503
        httpx.codes.GATEWAY_TIMEOUT,  # 504
    },
)


async def fetch_metadata(
    *,
    client_factory: Callable[[], httpx.AsyncClient] | None = None,
) -> dict[str, object] | None:
    """
    Fetch the latest GLEIF ISIN->LEI mapping metadata from the API.

    Calls the GLEIF mapping API to retrieve metadata about the latest
    ISIN->LEI relationship file, including the download link.

    Returns:
        dict[str, object] | None: Metadata dict with id, file_name,
            uploaded_at, and download_link keys, or None on failure.
    """
    logger.info("Fetching GLEIF ISIN->LEI metadata from API.")

    factory = client_factory or make_client

    try:
        async with factory() as client:
            return await _fetch_metadata_with_client(client)
    except Exception as error:
        logger.error(
            "Failed to fetch GLEIF metadata: %s",
            error,
            exc_info=True,
        )
        return None


async def _fetch_metadata_with_client(
    client: httpx.AsyncClient,
) -> dict[str, object]:
    """
    Fetch metadata using the provided HTTP client.

    Returns:
        dict[str, object]: Metadata dict with id, file_name, uploaded_at,
            and download_link.
    """
    response = await client.get(_config.isin_lei_url)
    response.raise_for_status()
    payload = response.json()

    data = payload.get("data", {})
    attrs = data.get("attributes", {})

    return {
        "id": data.get("id"),
        "file_name": attrs.get("fileName"),
        "uploaded_at": attrs.get("uploadedAt"),
        "download_link": attrs.get("downloadLink"),
    }


async def search_by_name(
    name: str,
    client: httpx.AsyncClient,
) -> list[tuple[str, str]]:
    """
    Search the GLEIF autocompletions API for active entities matching a
    name.

    Strips corporate suffixes (PLC, INC, AG, etc.) from the name before
    querying to improve match quality. Filters results to only active
    entities via the lei-records endpoint. Retries on transient HTTP
    errors using exponential backoff.

    Returns:
        list[tuple[str, str]]: Pairs of (legal_name, lei) from matching
            active entities, or an empty list on failure.
    """
    query = strip_corporate_suffix(name)

    candidates = await _fetch_with_retry(
        client,
        _config.autocompletions_url,
        params={"field": "fulltext", "q": query},
        parser=_parse_autocompletions,
    )

    return await _filter_active(candidates, client)


async def _filter_active(
    candidates: list[tuple[str, str]],
    client: httpx.AsyncClient,
) -> list[tuple[str, str]]:
    """
    Filter candidates to only those with an active entity status.

    Batch-queries the GLEIF lei-records endpoint with all candidate LEIs
    and an active status filter. Returns only candidates whose LEI
    appears in the active result set.

    Returns:
        list[tuple[str, str]]: Candidates with active entity status.
    """
    if not candidates:
        return []

    leis = ",".join(lei for _, lei in candidates)
    active_leis = await _fetch_active_leis(client, leis)

    return [(name, lei) for name, lei in candidates if lei in active_leis]


async def _fetch_active_leis(
    client: httpx.AsyncClient,
    leis: str,
) -> frozenset[str]:
    """
    Fetch the set of LEIs that have active entity status.

    Returns:
        frozenset[str]: LEIs with active entity status.
    """
    records = await _fetch_with_retry(
        client,
        _config.lei_records_url,
        params={
            "filter[lei]": leis,
            "filter[entity.status]": "ACTIVE",
        },
        parser=_parse_lei_ids,
    )
    return frozenset(lei for _, lei in records)


async def fetch_parents(
    lei: str,
    client: httpx.AsyncClient,
) -> list[tuple[str, str]]:
    """
    Fetch parent entities that own the given LEI via the GLEIF
    lei-records API.

    Retries on transient HTTP errors using exponential backoff. Returns
    an empty list on failure for graceful degradation.

    Returns:
        list[tuple[str, str]]: Pairs of (legal_name, lei) for each parent
            entity, or an empty list on failure.
    """
    return await _fetch_with_retry(
        client,
        _config.lei_records_url,
        params={"filter[owns]": lei},
        parser=_parse_lei_records,
    )


async def _fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, str],
    parser: Callable[[dict], list[tuple[str, str]]],
) -> list[tuple[str, str]]:
    """
    Perform a GET request with exponential backoff on retryable status
    codes.

    Returns:
        list[tuple[str, str]]: Parsed results from the response, or an
            empty list if all attempts fail.
    """
    max_backoff_attempts = 5
    delays = [0, *backoff_delays(attempts=max_backoff_attempts)]

    for attempt, delay in enumerate(delays):
        if delay > 0:
            logger.debug(
                "RATE_LIMIT: GLEIF API request paused. "
                "Retrying in %.1fs (attempt %d/%d)",
                delay,
                attempt,
                max_backoff_attempts,
            )
            await asyncio.sleep(delay)

        try:
            response = await client.get(url, params=params)
        except httpx.HTTPError as error:
            logger.error("GLEIF API request failed: %s", error)
            return []

        if response.status_code not in _RETRYABLE_STATUS_CODES:
            break

    return _safe_parse(response, parser)


def _safe_parse(
    response: httpx.Response,
    parser: Callable[[dict], list[tuple[str, str]]],
) -> list[tuple[str, str]]:
    """
    Parse an HTTP response, returning an empty list on any failure.

    Returns:
        list[tuple[str, str]]: Parsed results, or an empty list on error.
    """
    try:
        response.raise_for_status()
        return parser(response.json())
    except Exception as error:
        logger.error("GLEIF API parse failed: %s", error)
        return []


def _parse_autocompletions(payload: dict) -> list[tuple[str, str]]:
    """
    Extract (legal_name, lei) pairs from an autocompletions API response.

    Returns:
        list[tuple[str, str]]: Pairs of (legal_name, lei).
    """
    return [
        (
            _extract_completion_name(item),
            item["relationships"]["lei-records"]["data"]["id"],
        )
        for item in payload.get("data", [])
        if _has_valid_completion_fields(item)
    ]


def _has_valid_completion_fields(item: dict) -> bool:
    """
    Check whether an autocompletion item contains the required fields.

    Returns:
        bool: True if the item has both a value and a LEI id.
    """
    has_name = bool(item.get("attributes", {}).get("value"))
    has_lei = bool(
        item.get("relationships", {}).get("lei-records", {}).get("data", {}).get("id")
    )
    return has_name and has_lei


def _extract_completion_name(item: dict) -> str:
    """
    Extract the entity name from an autocompletion item.

    Returns:
        str: The entity legal name.
    """
    return item["attributes"]["value"]


def _parse_lei_records(payload: dict) -> list[tuple[str, str]]:
    """
    Extract (legal_name, lei) pairs from a lei-records API response.

    Returns:
        list[tuple[str, str]]: Pairs of (legal_name, lei).
    """
    return [
        (
            item["attributes"]["entity"]["legalName"]["name"],
            item["id"],
        )
        for item in payload.get("data", [])
        if _has_valid_lei_record_fields(item)
    ]


def _parse_lei_ids(payload: dict) -> list[tuple[str, str]]:
    """
    Extract (empty, lei) pairs from a lei-records API response.

    Used for active-status filtering where only the LEI ids are needed.

    Returns:
        list[tuple[str, str]]: Pairs of ("", lei) for each record.
    """
    return [("", item["id"]) for item in payload.get("data", []) if item.get("id")]


def _has_valid_lei_record_fields(item: dict) -> bool:
    """
    Check whether a lei-record item contains the required fields.

    Returns:
        bool: True if the item has both a legal name and an id.
    """
    has_id = bool(item.get("id"))
    has_name = bool(
        item.get("attributes", {}).get("entity", {}).get("legalName", {}).get("name")
    )
    return has_id and has_name
