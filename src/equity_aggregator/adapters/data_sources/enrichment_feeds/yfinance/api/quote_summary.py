# api/quote_summary.py

import logging
from collections.abc import Iterable, Mapping

import httpx

from .._utils import safe_json_parse
from ..session import YFSession

logger = logging.getLogger(__name__)


async def get_quote_summary(
    session: YFSession,
    ticker: str,
    modules: Iterable[str] | None = None,
) -> dict[str, object] | None:
    """
    Fetch and flatten Yahoo Finance quoteSummary modules for a given ticker.

    This coroutine retrieves detailed equity data for the specified ticker symbol
    from Yahoo Finance's quoteSummary endpoint. It requests all specified modules
    in a single call, then merges the resulting module dictionaries into a single
    flat mapping for convenience.

    If the primary endpoint returns 500 (Internal Server Error), automatically
    try the fallback quote endpoint which may have better availability.
    This handles cases where quoteSummary has issues but the fallback endpoint works.

    Args:
        session (YFSession): The Yahoo Finance session for making HTTP requests.
        ticker (str): The stock symbol to fetch (e.g., "AAPL").
        modules (Iterable[str] | None): Optional iterable of module names to
            retrieve. If None, uses the default modules from the session config.

    Returns:
        dict[str, object] | None: A flattened dictionary containing all fields from
        the requested modules, or None if no data is found.
    """

    modules = tuple(modules or session.config.modules)

    url = session.config.quote_summary_primary_url + ticker

    response = await session.get(
        url,
        params={
            "modules": ",".join(modules),
            "corsDomain": "finance.yahoo.com",
            "formatted": "false",
            "symbol": ticker,
            "lang": "en-US",
            "region": "US",
        },
    )

    status = response.status_code

    # 500 → try fallback endpoint
    if status == httpx.codes.INTERNAL_SERVER_ERROR:
        return await _get_quote_summary_fallback(session, ticker)

    # Other non-200 status codes are errors
    if status != httpx.codes.OK:
        raise LookupError(f"HTTP {status} from quote summary endpoint for {ticker}")

    # Parse and flatten the response
    json = safe_json_parse(response, context=f"quote summary for {ticker}")
    raw_data = json.get("quoteSummary", {}).get("result", [])

    if not raw_data:
        return None

    return _flatten_module_dicts(modules, raw_data[0])


async def _get_quote_summary_fallback(
    session: YFSession,
    ticker: str,
) -> dict[str, object] | None:
    """
    Fetch quote data from Yahoo Finance fallback endpoint.

    This endpoint returns a simpler data structure compared to quoteSummary,
    with different field names.

    Args:
        session (YFSession): The Yahoo Finance session for making HTTP requests.
        ticker (str): The stock symbol to fetch (e.g., "AAPL").

    Returns:
        dict[str, object] | None: Quote data from the fallback endpoint,
        or None if no data is found.
    """
    url = session.config.quote_summary_fallback_url

    response = await session.get(
        url,
        params={
            "symbols": ticker,
            "formatted": "false",
            "lang": "en-US",
            "region": "US",
        },
    )

    status = response.status_code

    if status != httpx.codes.OK:
        raise LookupError(
            f"HTTP {status} from quote fallback endpoint for {ticker}",
        )

    json = safe_json_parse(response, context=f"quote fallback for {ticker}")
    raw_data = json.get("quoteResponse", {}).get("result", [])

    if raw_data and len(raw_data) > 0:
        return raw_data[0]

    return None


def _flatten_module_dicts(
    modules: Iterable[str],
    payload: Mapping[str, object],
) -> dict[str, object]:
    """
    Merge and flatten module dictionaries from a Yahoo Finance API payload.

    For each module name in `modules`, if the corresponding value in `payload` is a
    dictionary, its key-value pairs are merged into a single dictionary. Keys from
    later modules can overwrite those from earlier modules.

    Args:
        modules (Iterable[str]): Module names to extract and merge from the payload.
        payload (Mapping[str, object]): Mapping of module names to their data
            (typically from the Yahoo Finance API response).

    Returns:
        dict[str, object]: A merged dictionary containing all key-value pairs from
        the specified module dictionaries found in the payload.
    """
    merged: dict[str, object] = {}
    for module in modules:
        if (value := payload.get(module)) and isinstance(value, dict):
            merged.update(value)
    return merged
