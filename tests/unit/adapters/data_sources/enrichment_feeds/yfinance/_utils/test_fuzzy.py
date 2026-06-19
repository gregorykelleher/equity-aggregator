# _utils/test_fuzzy.py

import logging

import pytest
from rapidfuzz import fuzz
from rapidfuzz import utils as rf_utils

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance._utils.fuzzy import (
    _score_quote,
    _score_symbol,
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

    symbol_score = _score_symbol(symbol, "MSFT")
    name_score = fuzz.WRatio(
        name,
        "Microsoft Corporation",
        processor=rf_utils.default_process,
    )

    assert total == symbol_score + name_score


def test_score_quote_accepts_cross_exchange_symbol_variant() -> None:
    """
    ARRANGE: quote with exchange-decorated symbol (MELE.BR) vs prefixed symbol (2MELE)
    ACT:     call _score_quote
    ASSERT:  total_score is non-zero (shared root symbol matched exactly)
    """

    quote = {"symbol": "MELE.BR", "longname": "Melexis NV"}
    total, _, _ = _score_quote(
        quote=quote,
        name_key="longname",
        expected_symbol="2MELE",
        expected_name="MELEXIS NV",
    )

    assert total > 0


def test_score_quote_rejects_when_name_score_below_threshold() -> None:
    """
    ARRANGE: quote where the name has no similarity to expected name
    ACT:     call _score_quote
    ASSERT:  total_score is zero (candidate rejected)
    """

    quote = {"symbol": "MELE.BR", "longname": "Unrelated Corp"}
    total, _, _ = _score_quote(
        quote=quote,
        name_key="longname",
        expected_symbol="2MELE",
        expected_name="MELEXIS NV",
    )

    assert total == 0


def test_score_quote_substring_symbol_with_similar_name_is_not_rejected() -> None:
    """
    ARRANGE: quote whose symbol is a superstring of the expected symbol
             AND whose name is similar (META/MET, MetLife/Meta Platforms)
    ACT:     call _score_quote
    ASSERT:  total_score is non-zero - the symbol score alone cannot reject
             these; the caller's min_score ranking ensures the exact match (MET)
             outscores the substring match (META)

    Note:
        With root-gated scoring the roots differ (META vs MET), so the strict
        fuzz.ratio("META", "MET") = 85.7 is used, which still clears the
        score_cutoff=70 gate. The name gate (WRatio) is the primary
        discriminator, but when two companies share a lexical root (Met-) it
        cannot distinguish them either. In practice, ISIN lookups return the
        correct ticker, and an exact root match (100) always outranks the 85.7
        substring score.
    """

    quote = {"symbol": "META", "longname": "Meta Platforms Inc"}
    total, _, _ = _score_quote(
        quote=quote,
        name_key="longname",
        expected_symbol="MET",
        expected_name="MetLife Inc",
    )

    assert total > 0


def test_score_symbol_exact_root_match_earns_full_bonus() -> None:
    """
    ARRANGE: exchange-decorated symbol (MELE.BR) vs prefixed symbol (2MELE)
    ACT:     call _score_symbol
    ASSERT:  full bonus (100.0) awarded for the shared root symbol
    """

    expected = 100.0
    actual = _score_symbol("MELE.BR", "2MELE")

    assert actual == expected


def test_score_symbol_substring_no_longer_scores_full_bonus() -> None:
    """
    ARRANGE: substring symbol (AAP) vs expected symbol (AAPL), differing roots
    ACT:     call _score_symbol
    ASSERT:  score is below the full bonus (no substring inflation)
    """

    full_bonus = 100.0
    actual = _score_symbol("AAP", "AAPL")

    assert actual < full_bonus


def test_substring_symbol_ranks_below_exact_symbol() -> None:
    """
    ARRANGE: two quotes with identical names; symbols AAPL (exact) and AAP
             (substring) against expected symbol AAPL
    ACT:     call rank_all_symbols
    ASSERT:  exact symbol ranks first (substring no longer ties at 100)
    """

    quotes = [
        {"symbol": "AAP", "longname": "Apple Inc"},
        {"symbol": "AAPL", "longname": "Apple Inc"},
    ]

    actual = rank_all_symbols(
        quotes=quotes,
        name_key="longname",
        expected_name="Apple Inc",
        expected_symbol="AAPL",
    )

    assert actual[0] == "AAPL"


def test_rank_all_symbols_logs_accepted_match(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    ARRANGE: a single matching quote and DEBUG-level log capture
    ACT:     call rank_all_symbols
    ASSERT:  an accepted-match record is logged
    """

    quotes = [{"symbol": "MSFT", "longname": "Microsoft Corporation"}]

    with caplog.at_level(
        logging.DEBUG,
        logger=(
            "equity_aggregator.adapters.data_sources."
            "enrichment_feeds.yfinance._utils.fuzzy"
        ),
    ):
        rank_all_symbols(
            quotes=quotes,
            name_key="longname",
            expected_name="Microsoft Corporation",
            expected_symbol="MSFT",
        )

    assert "FUZZY_MATCH accepted symbol=MSFT" in caplog.text
