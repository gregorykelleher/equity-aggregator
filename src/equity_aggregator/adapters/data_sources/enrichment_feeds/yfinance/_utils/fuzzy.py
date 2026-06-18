# _utils/fuzzy.py

import logging

from rapidfuzz import fuzz, utils

logger: logging.Logger = logging.getLogger(__name__)


def rank_all_symbols(
    quotes: list[dict],
    *,
    name_key: str,
    expected_name: str,
    expected_symbol: str,
    min_score: int = 0,
) -> list[str]:
    """
    Rank all matching symbols from a list of Yahoo Finance quotes using fuzzy matching.

    For each quote, computes a combined fuzzy score based on similarity between the
    quote's symbol and expected symbol, and between the quote's name and expected name.
    Returns all symbols that meet or exceed the minimum score threshold, sorted by
    score in descending order (best match first).

    Args:
        quotes (list[dict]): List of quote dictionaries, each with at least a
            "symbol" key and a name field specified by `name_key`.
        name_key (str): The key in each quote dict for equity name (e.g., "longname").
        expected_name (str): The expected equity name to match against.
        expected_symbol (str): The expected ticker symbol to match against.
        min_score (int, optional): Minimum combined fuzzy score required to accept a
            match. Defaults to 0.

    Returns:
        list[str]: Ranked symbols (best first), empty if none meet threshold.
    """
    if not quotes:
        return []

    # Compute fuzzy scores for each quote
    scored = [
        _score_quote(
            quote,
            name_key=name_key,
            expected_symbol=expected_symbol,
            expected_name=expected_name,
        )
        for quote in quotes
    ]

    # Filter by minimum score and sort by score descending
    filtered = [
        (score, symbol, name) for score, symbol, name in scored if score >= min_score
    ]
    ranked = sorted(filtered, key=lambda t: t[0], reverse=True)

    # Log accepted matches (those clearing min_score) with their scores
    for score, symbol, name in ranked:
        logger.debug(
            "FUZZY_MATCH accepted symbol=%s name=%s score=%d expected_symbol=%s",
            symbol,
            name,
            score,
            expected_symbol,
        )

    # Return symbols in ranked order
    return [symbol for _, symbol, _ in ranked]


def _score_quote(
    quote: dict,
    *,
    name_key: str,
    expected_symbol: str,
    expected_name: str,
) -> tuple[int, str, str]:
    """
    Compute a combined fuzzy score for a Yahoo Finance quote.

    This function calculates the sum of the fuzzy string similarity between the
    quote's symbol and the expected symbol, and between the quote's name (using
    `name_key`) and the expected name. Applies minimum score thresholds to prevent
    matching completely unrelated equities.

    Note:
        Symbol comparison is gated on the root symbol. Exchange symbols follow a
        `[prefix]ROOT[.suffix]` pattern (e.g. `MELE.BR` vs `2MELE`), so the root
        is extracted by dropping the exchange suffix and any numeric prefix. When
        the roots match exactly the symbol earns the full bonus; otherwise a
        strict full-string `ratio` is used so that a mere substring overlap (e.g.
        `AAP` vs `AAPL`) no longer scores a perfect 100 and cannot outrank an
        exact match.

    Args:
        quote (dict): The quote dictionary containing at least a "symbol" key and
            a name field specified by `name_key`.
        name_key (str): The key in the quote dict for the equity name.
        expected_symbol (str): The expected ticker symbol to match against.
        expected_name (str): The expected equity name to match against.

    Returns:
        tuple[int, str, str]: A tuple of (total_score, actual_symbol, actual_name),
            where total_score is the sum of the symbol and name fuzzy scores.
            Returns (0, symbol, name) if either score is below its minimum threshold.
    """
    actual_symbol = quote["symbol"]
    actual_name = quote.get(name_key, "<no-name>")

    symbol_score = _score_symbol(actual_symbol, expected_symbol)
    name_score = fuzz.WRatio(
        actual_name,
        expected_name,
        processor=utils.default_process,
        score_cutoff=70,
    )

    # Reject if either score is below threshold
    if name_score == 0 or symbol_score == 0:
        return 0, actual_symbol, actual_name

    total_score = symbol_score + name_score
    return total_score, actual_symbol, actual_name


def _score_symbol(actual_symbol: str, expected_symbol: str) -> float:
    """
    Score the similarity between a candidate symbol and the expected symbol.

    An exact root-symbol match earns the full bonus, which lets legitimate
    exchange variants (e.g. `MELE.BR` vs `2MELE`) match on their shared root.
    When the roots differ, a strict full-string `ratio` is used so that a mere
    substring overlap (e.g. `AAP` vs `AAPL`) no longer scores a perfect 100.

    Args:
        actual_symbol (str): The candidate symbol from the Yahoo Finance quote.
        expected_symbol (str): The expected ticker symbol to match against.

    Returns:
        float: 100.0 when the root symbols match exactly, otherwise the
            full-string `ratio` (0.0 if below the score cutoff of 70).
    """
    full_bonus = 100.0

    if _root_symbol(actual_symbol) == _root_symbol(expected_symbol):
        return full_bonus

    return fuzz.ratio(
        actual_symbol,
        expected_symbol,
        processor=utils.default_process,
        score_cutoff=70,
    )


def _root_symbol(symbol: str) -> str:
    """
    Extract the root of an exchange symbol following the `[prefix]ROOT[.suffix]`
    pattern.

    Drops the exchange suffix (everything from the first ".") and any leading or
    trailing numeric exchange prefix, e.g. `MELE.BR` -> `MELE`, `2MELE` -> `MELE`,
    `BRK.B` -> `BRK`.

    Args:
        symbol (str): The exchange symbol to reduce to its root.

    Returns:
        str: The uppercased root symbol.
    """
    root = symbol.upper().split(".", 1)[0]
    return root.strip("0123456789")
