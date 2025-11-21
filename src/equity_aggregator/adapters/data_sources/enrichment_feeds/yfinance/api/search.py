# api/search.py

import logging

import httpx

from .._utils import safe_json_parse
from ..session import YFSession

logger: logging.Logger = logging.getLogger(__name__)


async def search_quotes(
    session: YFSession,
    query: str,
) -> list[dict]:
    """
    Asynchronously search Yahoo Finance for equities matching a query string.

    This coroutine sends a GET request to Yahoo Finance's search API using the
    provided query. The response is parsed for a "quotes" field, which should
    contain a list of quote dictionaries. Only quotes where "quoteType" is
    "EQUITY" are included in the result.

    Args:
        session (YFSession): The Yahoo Finance session for making HTTP requests.
        query (str): The search query (symbol, name, ISIN, or CUSIP).

    Returns:
        list[dict]: List of quote dictionaries for equities matching the query.

    Raises:
        LookupError: If the search endpoint returns an HTTP error or network
            error occurs.
    """
    response = await session.get(session.config.search_url, params={"q": query})

    if response.status_code != httpx.codes.OK:
        raise LookupError(f"Search endpoint returned HTTP {response.status_code}")

    json = safe_json_parse(response, context=f"search query '{query}'")
    raw_data = json.get("quotes", [])

    return [quote for quote in raw_data if quote.get("quoteType") == "EQUITY"]
