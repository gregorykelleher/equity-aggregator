# yfinance/ranking.py

from ._utils import rank_all_symbols


def filter_equities(quotes: list[dict]) -> list[dict]:
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


def rank_symbols(
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
        ranked = rank_by_name_key(
            viable,
            name_key=name_key,
            expected_name=expected_name,
            expected_symbol=expected_symbol,
            min_score=min_score,
        )
        if ranked:
            return ranked

    return []


def rank_by_name_key(
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
