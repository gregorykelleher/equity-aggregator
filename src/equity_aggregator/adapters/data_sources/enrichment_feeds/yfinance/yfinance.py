# yfinance/yfinance.py

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from equity_aggregator.schemas import YFinanceFeedData
from equity_aggregator.storage import (
    load_cache_entry,
    save_cache_entry,
)

from .api import (
    get_quote_summary,
    search_quotes,
)
from .config import FeedConfig
from .ranking import filter_equities, rank_symbols
from .session import YFSession

logger = logging.getLogger(__name__)


@asynccontextmanager
async def open_yfinance_feed(
    *,
    config: FeedConfig | None = None,
) -> AsyncIterator["YFinanceFeed"]:
    """
    Context manager to create and close a YFinanceFeed.

    Args:
        config: Custom feed configuration; defaults to FeedConfig().

    Yields:
        YFinanceFeed with active session.
    """
    config = config or FeedConfig()
    session = YFSession(config)
    try:
        yield YFinanceFeed(session)
    finally:
        await session.aclose()


class YFinanceFeed:
    """
    Async Yahoo Finance feed with caching and fuzzy lookup.

    Provides fetch_equity() to retrieve equity data by symbol, name, ISIN or CUSIP.

    Attributes:
        model: YFinanceFeedData schema class.
        default_min_score: Default minimum fuzzy matching score threshold.
    """

    __slots__ = ("_session",)

    model = YFinanceFeedData
    default_min_score = 160

    def __init__(self, session: YFSession) -> None:
        """
        Initialise with an active YFSession.

        Args:
            session: The Yahoo Finance HTTP session.
        """
        self._session = session

    async def fetch_equity(
        self,
        *,
        symbol: str,
        name: str,
        isin: str | None = None,
        cusip: str | None = None,
        **kwargs: object,
    ) -> dict:
        """
        Fetch enriched equity data using symbol, name, ISIN, or CUSIP.

        Steps:
            1. Check cache for existing entry
            2. Resolve candidate symbols via identifiers or search
            3. Fetch and validate quote summary for first viable candidate
            4. Cache and return result

        Args:
            symbol: Ticker symbol of the equity.
            name: Full name of the equity.
            isin: ISIN identifier, if available.
            cusip: CUSIP identifier, if available.
            **kwargs: Additional identifiers (ignored by Yahoo Finance).

        Returns:
            Enriched equity data.

        Raises:
            LookupError: If no matching equity data is found.
        """
        if record := load_cache_entry("yfinance_equities", symbol):
            return record

        try:
            candidates = await self._resolve_candidates(
                symbol=symbol,
                name=name,
                isin=isin,
                cusip=cusip,
            )

            data = await self._fetch_first_valid_quote(candidates)

            save_cache_entry("yfinance_equities", symbol, data)
            return data

        except LookupError:
            raise LookupError(f"No enrichment data found for {symbol}.") from None

    async def _resolve_candidates(
        self,
        *,
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> list[str]:
        """
        Resolve candidate symbols using full fallback chain.

        Resolution order:
            1. ISIN lookup (if provided)
            2. CUSIP lookup (if provided)
            3. Name/symbol search (fallback)

        Args:
            symbol: Expected ticker symbol.
            name: Expected company name.
            isin: ISIN identifier or None.
            cusip: CUSIP identifier or None.

        Returns:
            Ranked candidate symbols (best first).

        Raises:
            LookupError: If all resolution strategies fail.
        """
        identifiers = _build_identifier_sequence(isin, cusip)

        for identifier in identifiers:
            candidates = await self._resolve_by_identifier_safe(
                identifier=identifier,
                expected_name=name,
                expected_symbol=symbol,
            )
            if candidates:
                return candidates

        return await self._resolve_by_search_terms(
            query=name or symbol,
            expected_name=name,
            expected_symbol=symbol,
        )

    async def _resolve_by_identifier_safe(
        self,
        *,
        identifier: str,
        expected_name: str,
        expected_symbol: str,
    ) -> list[str]:
        """
        Attempt identifier resolution, returning empty list on failure.

        Args:
            identifier: ISIN or CUSIP to search.
            expected_name: Expected company name.
            expected_symbol: Expected ticker symbol.

        Returns:
            Ranked symbols or empty list on any failure.
        """
        try:
            return await self._resolve_by_identifier(
                identifier=identifier,
                expected_name=expected_name,
                expected_symbol=expected_symbol,
            )
        except LookupError:
            return []

    async def _resolve_by_identifier(
        self,
        *,
        identifier: str,
        expected_name: str,
        expected_symbol: str,
    ) -> list[str]:
        """
        Search by ISIN/CUSIP and return ranked candidate symbols.

        Args:
            identifier: ISIN or CUSIP to search.
            expected_name: Expected company name for ranking.
            expected_symbol: Expected ticker symbol for ranking.

        Returns:
            Ranked symbols (best first).

        Raises:
            LookupError: If search returns no results or no viable candidates.
        """
        quotes = await search_quotes(self._session, identifier)

        if not quotes:
            raise LookupError("Quote Search endpoint returned no results")

        viable = filter_equities(quotes)

        if not viable:
            raise LookupError("No viable candidates found")

        min_score = _select_identifier_min_score(len(viable), self.default_min_score)

        return rank_symbols(
            viable,
            expected_name=expected_name,
            expected_symbol=expected_symbol,
            min_score=min_score,
        )

    async def _resolve_by_search_terms(
        self,
        *,
        query: str,
        expected_name: str,
        expected_symbol: str,
    ) -> list[str]:
        """
        Search by query and return ranked candidate symbols.

        Tries query first, then expected_symbol if they differ.

        Args:
            query: Primary search query (typically company name or symbol).
            expected_name: Expected company name for ranking.
            expected_symbol: Expected ticker symbol for ranking and fallback search.

        Returns:
            Ranked symbols (best first).

        Raises:
            LookupError: If no viable candidates found.
        """
        terms = _build_search_terms(query, expected_symbol)

        for term in terms:
            quotes = await search_quotes(self._session, term)

            if not quotes:
                continue

            ranked = _rank_viable_candidates(
                quotes,
                expected_name=expected_name,
                expected_symbol=expected_symbol,
                min_score=self.default_min_score,
            )

            if ranked:
                return ranked

        raise LookupError("No symbol candidates found")

    async def _fetch_first_valid_quote(self, symbols: list[str]) -> dict:
        """
        Fetch and validate quote summary for first viable symbol.

        Args:
            symbols: Ranked candidate symbols to try.

        Returns:
            Validated quote summary data.

        Raises:
            LookupError: If all candidates fail validation.
        """
        for symbol in symbols:
            data = await get_quote_summary(
                self._session,
                symbol,
                modules=self._session.config.modules,
            )
            try:
                return _validate_quote_summary(data, symbol)
            except LookupError:
                continue

        raise LookupError("All candidates failed validation")


def _build_identifier_sequence(
    isin: str | None,
    cusip: str | None,
) -> tuple[str, ...]:
    """
    Return non-None identifiers in resolution priority order.

    Args:
        isin: ISIN identifier or None.
        cusip: CUSIP identifier or None.

    Returns:
        Tuple of identifiers with None values filtered out.
    """
    return tuple(filter(None, (isin, cusip)))


def _build_search_terms(query: str, symbol: str) -> tuple[str, ...]:
    """
    Return deduplicated search terms in priority order.

    Args:
        query: Primary search query (typically company name or symbol).
        symbol: Ticker symbol (fallback search term).

    Returns:
        Tuple of unique search terms, query first.
    """
    return tuple(dict.fromkeys((query, symbol)))


def _select_identifier_min_score(viable_count: int, default_min_score: int) -> int:
    """
    Select fuzzy match threshold based on result count.

    Single-result identifier lookups use a reduced threshold (120) since
    ISIN/CUSIP identifiers are globally unique. Multiple results use the
    default threshold for stricter ranking.

    Args:
        viable_count: Number of viable candidates.
        default_min_score: Default minimum score threshold.

    Returns:
        Minimum score threshold.
    """
    return 120 if viable_count == 1 else default_min_score


def _validate_quote_summary(data: dict | None, symbol: str) -> dict:
    """
    Validate quote summary meets EQUITY criteria.

    Checks:
        1. Data is not empty
        2. quoteType is "EQUITY"
        3. longName or shortName is present

    Args:
        data: Quote summary data or None.
        symbol: Symbol for error messages.

    Returns:
        Validated data dict.

    Raises:
        LookupError: If any validation check fails.
    """
    if not data:
        raise LookupError(f"Quote summary returned no data for {symbol}")

    quote_type = data.get("quoteType")
    if quote_type != "EQUITY":
        raise LookupError(
            f"Symbol {symbol} has quoteType '{quote_type}', expected 'EQUITY'",
        )

    if not data.get("longName") and not data.get("shortName"):
        raise LookupError(f"Symbol {symbol} has no company name")

    return data


def _rank_viable_candidates(
    quotes: list[dict],
    expected_name: str,
    expected_symbol: str,
    min_score: int,
) -> list[str]:
    """
    Filter and rank quote candidates by fuzzy match quality.

    Args:
        quotes: Raw quotes from search API.
        expected_name: Expected company name.
        expected_symbol: Expected ticker symbol.
        min_score: Minimum fuzzy score threshold.

    Returns:
        Ranked symbols (best first), empty if none viable.
    """
    viable = filter_equities(quotes)
    if not viable:
        return []

    return rank_symbols(
        viable,
        expected_name=expected_name,
        expected_symbol=expected_symbol,
        min_score=min_score,
    )
