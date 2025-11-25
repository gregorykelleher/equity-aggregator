# yfinance/yfinance.py

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from equity_aggregator.schemas import YFinanceFeedData
from equity_aggregator.storage import (
    load_cache_entry,
    save_cache_entry,
)

from ._utils import rank_all_symbols
from .api import (
    get_quote_summary,
    search_quotes,
)
from .config import FeedConfig
from .session import YFSession

logger = logging.getLogger(__name__)


@asynccontextmanager
async def open_yfinance_feed(
    *,
    config: FeedConfig | None = None,
) -> AsyncIterator["YFinanceFeed"]:
    """
    Context manager to create and close a YFinanceFeed instance.

    Args:
        config (FeedConfig | None, optional): Custom feed configuration; defaults to
            default FeedConfig.

    Yields:
        YFinanceFeed: An initialised feed with an active session.
    """
    config = config or FeedConfig()
    session = YFSession(config)
    try:
        yield YFinanceFeed(session, config)
    finally:
        await session.aclose()


class YFinanceFeed:
    """
    Asynchronous Yahoo Finance feed with caching and fuzzy lookup.

    Provides fetch_equity() to retrieve equity data by symbol, name, ISIN or CUSIP.

    Attributes:
        _session (YFSession): HTTP session for Yahoo Finance.
        _config (FeedConfig): Endpoints and modules configuration.
        _default_min_score (int): Minimum fuzzy score threshold.
    """

    __slots__ = ("_session", "_config")

    # Data model associated with the Yahoo Finance feed
    model = YFinanceFeedData

    # Default minimum fuzzy matching score
    default_min_score = 160

    def __init__(self, session: YFSession, config: FeedConfig | None = None) -> None:
        """
        Initialise with an active YFSession and optional custom FeedConfig.

        Args:
            session (YFSession): The Yahoo Finance HTTP session.
            config (FeedConfig | None, optional): Feed configuration; defaults to
                session.config.
        """
        self._session = session
        self._config = config or session.config

    async def fetch_equity(
        self,
        *,
        symbol: str,
        name: str,
        isin: str | None = None,
        cusip: str | None = None,
    ) -> dict | None:
        """
        Fetch enriched equity data using symbol, name, ISIN, or CUSIP.

        The method performs the following steps:
          1. Checks for a cached entry for the given symbol and returns it if found.
          2. Resolves candidate Yahoo Finance symbols via ISIN, CUSIP, or fuzzy search.
          3. Fetches and validates quote summary for the first viable candidate.
          4. Caches and returns the enriched equity data.

        Args:
            symbol (str): Ticker symbol of the equity.
            name (str): Full name of the equity.
            isin (str | None): ISIN identifier, if available.
            cusip (str | None): CUSIP identifier, if available.

        Returns:
            dict | None: Enriched equity data if found, otherwise None.

        Raises:
            LookupError: If no matching equity data is found.
        """
        # Check cache first
        if record := load_cache_entry("yfinance_equities", symbol):
            return record

        try:
            # Resolve symbol candidates
            candidate_symbols = await self._resolve_candidate_symbols(
                symbol=symbol,
                name=name,
                isin=isin,
                cusip=cusip,
            )

            # Fetch first valid quote from candidates
            quote_summary_data = await self._fetch_first_valid_quote(candidate_symbols)

            # Cache and return
            save_cache_entry("yfinance_equities", symbol, quote_summary_data)
            return quote_summary_data

        except LookupError:
            raise LookupError(f"No enrichment data found for {symbol}.") from None

    async def _resolve_candidate_symbols(
        self,
        *,
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> list[str]:
        """
        Resolve a ranked list of candidate Yahoo Finance symbols.

        Resolution strategies applied in order of preference:
          1. ISIN identifier lookup (if provided)
          2. CUSIP identifier lookup (if provided)
          3. Fuzzy name/symbol search (fallback)

        Args:
            symbol (str): Expected ticker symbol.
            name (str): Expected company or equity name.
            isin (str | None): ISIN identifier, if available.
            cusip (str | None): CUSIP identifier, if available.

        Returns:
            list[str]: Ranked candidate symbols (best match first).

        Raises:
            LookupError: If no candidates are found via any strategy.
        """
        # Try ISIN identifier
        if isin:
            candidates = await self._try_rank_by_identifier(isin, name, symbol)
            if candidates:
                return candidates

        # Try CUSIP identifier
        if cusip:
            candidates = await self._try_rank_by_identifier(cusip, name, symbol)
            if candidates:
                return candidates

        # Fallback to fuzzy search
        return await self._rank_symbols_by_name_or_symbol(
            query=name or symbol,
            expected_name=name,
            expected_symbol=symbol,
        )

    async def _try_rank_by_identifier(
        self,
        identifier: str,
        expected_name: str,
        expected_symbol: str,
    ) -> list[str]:
        """
        Attempt to rank symbols by identifier, returning empty list on failure.

        Args:
            identifier (str): ISIN or CUSIP identifier.
            expected_name (str): Expected company name.
            expected_symbol (str): Expected ticker symbol.

        Returns:
            list[str]: Ranked candidates, or empty list if strategy fails.
        """
        try:
            return await self._rank_symbols_by_identifier(
                identifier=identifier,
                expected_name=expected_name,
                expected_symbol=expected_symbol,
            )
        except LookupError:
            return []

    async def _fetch_first_valid_quote(self, symbols: list[str]) -> dict:
        """
        Fetch validated quote summary for the first viable symbol from a ranked list.

        Iterates through candidate symbols in order, attempting to fetch and validate
        each quote summary. Returns the first quote that passes validation (EQUITY type
        with valid company name). If all candidates fail validation, raises LookupError.

        Args:
            symbols (list[str]): Ranked list of candidate Yahoo Finance symbols to try.

        Returns:
            dict: Validated quote summary data for the first successful candidate.

        Raises:
            LookupError: If all candidates fail validation.
        """
        for symbol in symbols:
            try:
                return await self._fetch_and_validate_quote_summary(symbol)
            except LookupError:
                continue

        raise LookupError("All candidates failed validation")

    async def _rank_symbols_by_identifier(
        self,
        identifier: str,
        expected_name: str,
        expected_symbol: str,
    ) -> list[str]:
        """
        Search by ISIN/CUSIP identifier and return ranked candidate symbols.

        This method:
          1. Searches Yahoo Finance for quotes matching the identifier.
          2. Filters results to those with both a symbol and a name.
          3. Ranks all viable candidates using fuzzy matching.
          4. Returns the ranked list of symbols.

        Note:
          Uses adaptive thresholds: reduced 120 score for a result with a single equity
          (more lenient since ISIN/CUISIP identifiers are globally unique), default
          minimum score for multiple results (stricter ranking to select best match).

        Args:
            identifier (str): The ISIN or CUSIP to search for.
            expected_name (str): The expected company or equity name.
            expected_symbol (str): The expected ticker symbol.

        Returns:
            list[str]: Ranked candidate symbols (best match first).

        Raises:
            LookupError: If search returns no viable candidates.
        """
        quotes = await search_quotes(self._session, identifier)

        if not quotes:
            raise LookupError("Quote Search endpoint returned no results")

        viable_equities = _filter_equities(quotes)

        if not viable_equities:
            raise LookupError("No viable candidates found")

        # Use lower threshold for single-result identifier lookups
        identifier_min_score = (
            120 if len(viable_equities) == 1 else self.default_min_score
        )

        return _rank_symbols(
            viable_equities,
            expected_name=expected_name,
            expected_symbol=expected_symbol,
            min_score=identifier_min_score,
        )

    async def _rank_symbols_by_name_or_symbol(
        self,
        query: str,
        expected_name: str,
        expected_symbol: str,
    ) -> list[str]:
        """
        Search by name/symbol query and return ranked candidate symbols.

        Tries multiple search terms:
          1. Primary query (usually company name)
          2. Expected symbol (if different from query)

        For each search term, this method:
          1. Retrieves quote candidates.
          2. Filters out entries lacking a name or symbol.
          3. Ranks all viable candidates using fuzzy matching.
          4. Returns the ranked list from the first successful search.

        Args:
            query (str): Primary search string, typically a company name or symbol.
            expected_name (str): Expected equity name for fuzzy matching.
            expected_symbol (str): Expected ticker symbol for fuzzy matching.

        Returns:
            list[str]: Ranked candidate symbols (best match first).

        Raises:
            LookupError: If no viable candidates are found.
        """
        searches = tuple(dict.fromkeys((query, expected_symbol)))

        for term in searches:
            quotes = await search_quotes(self._session, term)

            if not quotes:
                continue

            # filter out any without name or symbol
            viable_equities = _filter_equities(quotes)

            if not viable_equities:
                continue

            ranked_symbols = _rank_symbols(
                viable_equities,
                expected_name=expected_name,
                expected_symbol=expected_symbol,
                min_score=self.default_min_score,
            )

            if ranked_symbols:
                return ranked_symbols

        raise LookupError("No symbol candidates found")

    async def _fetch_and_validate_quote_summary(self, symbol: str) -> dict:
        """
        Fetch and validate quote summary data for a Yahoo Finance symbol.

        This method combines the quote summary retrieval with validation to ensure
        the data meets required criteria before being cached or processed further.
        The validation checks that:
          1. The quote type is "EQUITY" (not MUTUALFUND, ETF, etc.)
          2. A company name is present (either longName or shortName)

        Args:
            symbol (str): The Yahoo Finance symbol to fetch and validate.

        Returns:
            dict: The validated quote summary data.

        Raises:
            LookupError: If the fetch fails, returns no data, or validation fails.
        """
        data = await get_quote_summary(
            self._session,
            symbol,
            modules=self._config.modules,
        )

        if not data:
            raise LookupError(f"Quote summary returned no data for {symbol}")

        # Validate quoteType is EQUITY
        quote_type = data.get("quoteType")
        if quote_type != "EQUITY":
            raise LookupError(
                f"Symbol {symbol} has quoteType '{quote_type}', expected 'EQUITY'",
            )

        # Validate company name presence
        if not data.get("longName") and not data.get("shortName"):
            raise LookupError(f"Symbol {symbol} has no company name")

        return data


def _filter_equities(quotes: list[dict]) -> list[dict]:
    """
    Filter out any quotes lacking a longname or symbol.

    Note:
        The Yahoo Finance search quote query endpoint returns 'longname' and 'shortname'
        fields in lowercase.

    Args:
        quotes (list[dict]): Raw list of quote dicts from Yahoo Finance.

    Returns:
        list[dict]: Only those quotes that have both 'longname' and 'symbol'.
    """
    return [
        quote
        for quote in quotes
        if (quote.get("longname") or quote.get("shortname")) and quote.get("symbol")
    ]


def _rank_symbols(
    viable: list[dict],
    *,
    expected_name: str,
    expected_symbol: str,
    min_score: int,
) -> list[str]:
    """
    Rank Yahoo Finance quote candidates by fuzzy match quality.

    Returns ALL viable candidates as a ranked list ordered by match confidence
    (best match first), filtered by minimum score threshold. All candidates are
    scored and validated, even if there's only one or they share identical names.

    Args:
        viable (list[dict]): List of filtered Yahoo Finance quote dictionaries.
        expected_name (str): Expected company or equity name for fuzzy matching.
        expected_symbol (str): Expected ticker symbol for fuzzy matching.
        min_score (int): Minimum fuzzy score required to accept a match.

    Returns:
        list[str]: Ranked symbols (best first), empty if none meet threshold.
    """
    # Try longname first, then shortname
    for name_key in ("longname", "shortname"):
        ranked = _rank_by_name_key(
            viable,
            name_key=name_key,
            expected_name=expected_name,
            expected_symbol=expected_symbol,
            min_score=min_score,
        )
        if ranked:
            return ranked

    return []


def _rank_by_name_key(
    viable: list[dict],
    *,
    name_key: str,
    expected_name: str,
    expected_symbol: str,
    min_score: int,
) -> list[str]:
    """
    Rank symbols using specified name field (longname or shortname).

    Args:
        viable (list[dict]): List of quote dictionaries to rank.
        name_key (str): The key to use for name comparison.
        expected_name (str): Expected company or equity name.
        expected_symbol (str): Expected ticker symbol.
        min_score (int): Minimum fuzzy score threshold.

    Returns:
        list[str]: Ranked symbols, or empty list if no matches meet threshold.
    """
    candidates_with_name = [quote for quote in viable if quote.get(name_key)]

    if not candidates_with_name:
        return []

    return rank_all_symbols(
        candidates_with_name,
        name_key=name_key,
        expected_name=expected_name,
        expected_symbol=expected_symbol,
        min_score=min_score,
    )
