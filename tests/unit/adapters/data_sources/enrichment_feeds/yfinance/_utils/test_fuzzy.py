# _utils/test_fuzzy.py

import pytest
from rapidfuzz import fuzz
from rapidfuzz import utils as rf_utils

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance._utils.fuzzy import (
    _score_quote,
    rank_all_symbols,
)

pytestmark = pytest.mark.unit


def test_rank_all_symbols_returns_empty_when_quotes_empty() -> None:
    """
    ARRANGE: empty quotes list
    ACT:     call rank_all_symbols
    ASSERT:  returns empty list
    """

    actual = rank_all_symbols(
        quotes=[],
        name_key="longname",
        expected_name="Microsoft Corporation",
        expected_symbol="MSFT",
    )

    assert actual == []


def test_rank_all_symbols_returns_best_symbol_first() -> None:
    """
    ARRANGE: list with close and distant matches
    ACT:     call rank_all_symbols
    ASSERT:  returns list with best symbol first
    """

    quotes = [
        {"symbol": "MSFT", "longname": "Microsoft Corporation"},
        {"symbol": "MSTF", "longname": "Microsoft Corp"},
        {"symbol": "AAPL", "longname": "Apple Inc."},
    ]

    actual = rank_all_symbols(
        quotes=quotes,
        name_key="longname",
        expected_name="Microsoft Corporation",
        expected_symbol="MSFT",
    )

    assert actual[0] == "MSFT"


def test_rank_all_symbols_respects_min_score() -> None:
    """
    ARRANGE: min_score set above any attainable score
    ACT:     call rank_all_symbols
    ASSERT:  returns empty list
    """

    quotes = [{"symbol": "MSFT", "longname": "Microsoft Corporation"}]
    actual = rank_all_symbols(
        quotes=quotes,
        name_key="longname",
        expected_name="Microsoft Corporation",
        expected_symbol="MSFT",
        min_score=250,
    )

    assert actual == []


def test_score_quote_total_matches_component_sum() -> None:
    """
    ARRANGE: single quote with known strings
    ACT:     call _score_quote
    ASSERT:  total_score equals symbol_score + name_score
    """

    quote = {"symbol": "MSFT", "longname": "Microsoft Corporation"}
    total, symbol, name = _score_quote(
        quote=quote,
        name_key="longname",
        expected_symbol="MSFT",
        expected_name="Microsoft Corporation",
    )

    symbol_score = fuzz.ratio(symbol, "MSFT", processor=rf_utils.default_process)
    name_score = fuzz.WRatio(
        name,
        "Microsoft Corporation",
        processor=rf_utils.default_process,
    )

    assert total == symbol_score + name_score
