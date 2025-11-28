# yfinance/test_ranking.py

import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.yfinance.ranking import (
    filter_equities,
    rank_by_name_key,
    rank_symbols,
)

pytestmark = pytest.mark.unit


def test_filter_equities_keeps_quotes_with_longname_and_symbol() -> None:
    """
    ARRANGE: quotes with longname and symbol
    ACT:     call filter_equities
    ASSERT:  quotes are kept
    """
    expected_count = 2
    quotes = [
        {"longname": "Apple Inc.", "symbol": "AAPL"},
        {"longname": "Microsoft Corp", "symbol": "MSFT"},
    ]

    actual = filter_equities(quotes)

    assert len(actual) == expected_count


def test_filter_equities_keeps_quotes_with_shortname_and_symbol() -> None:
    """
    ARRANGE: quotes with shortname and symbol
    ACT:     call filter_equities
    ASSERT:  quotes are kept
    """
    expected_count = 2
    quotes = [
        {"shortname": "Apple", "symbol": "AAPL"},
        {"shortname": "Microsoft", "symbol": "MSFT"},
    ]

    actual = filter_equities(quotes)

    assert len(actual) == expected_count


def test_filter_equities_removes_quotes_without_symbol() -> None:
    """
    ARRANGE: quotes missing symbol
    ACT:     call filter_equities
    ASSERT:  quotes are filtered out
    """
    quotes = [
        {"longname": "Apple Inc."},
        {"shortname": "Microsoft"},
    ]

    actual = filter_equities(quotes)

    assert actual == []


def test_filter_equities_removes_quotes_without_name() -> None:
    """
    ARRANGE: quotes missing both longname and shortname
    ACT:     call filter_equities
    ASSERT:  quotes are filtered out
    """
    quotes = [
        {"symbol": "AAPL"},
        {"symbol": "MSFT"},
    ]

    actual = filter_equities(quotes)

    assert actual == []


def test_filter_equities_keeps_quotes_with_either_name_field() -> None:
    """
    ARRANGE: mixed quotes with longname or shortname
    ACT:     call filter_equities
    ASSERT:  all valid quotes are kept
    """
    expected_count = 2
    quotes = [
        {"longname": "Apple Inc.", "symbol": "AAPL"},
        {"shortname": "Microsoft", "symbol": "MSFT"},
        {"symbol": "INVALID"},
    ]

    actual = filter_equities(quotes)

    assert len(actual) == expected_count


def test_rank_symbols_returns_ranked_list_from_longname() -> None:
    """
    ARRANGE: viable quotes with longname, expected name and symbol
    ACT:     call rank_symbols
    ASSERT:  returns ranked symbols based on fuzzy matching
    """
    viable = [
        {"longname": "Apple Inc.", "symbol": "AAPL"},
        {"longname": "Apple Hospitality REIT", "symbol": "APLE"},
    ]

    actual = rank_symbols(
        viable,
        expected_name="Apple Inc",
        expected_symbol="AAPL",
        min_score=100,
    )

    assert "AAPL" in actual


def test_rank_symbols_falls_back_to_shortname() -> None:
    """
    ARRANGE: viable quotes with only shortname
    ACT:     call rank_symbols
    ASSERT:  returns ranked symbols using shortname
    """
    viable = [
        {"shortname": "Microsoft", "symbol": "MSFT"},
        {"shortname": "Micro Corp", "symbol": "MICR"},
    ]

    actual = rank_symbols(
        viable,
        expected_name="Microsoft Corp",
        expected_symbol="MSFT",
        min_score=100,
    )

    assert "MSFT" in actual


def test_rank_symbols_returns_empty_when_no_matches() -> None:
    """
    ARRANGE: viable quotes with low similarity scores
    ACT:     call rank_symbols with high min_score
    ASSERT:  returns empty list
    """
    viable = [
        {"longname": "Completely Different Company", "symbol": "DIFF"},
    ]

    actual = rank_symbols(
        viable,
        expected_name="Apple Inc",
        expected_symbol="AAPL",
        min_score=150,
    )

    assert actual == []


def test_rank_by_name_key_returns_ranked_symbols() -> None:
    """
    ARRANGE: viable quotes with specified name_key
    ACT:     call rank_by_name_key
    ASSERT:  returns ranked symbols
    """
    viable = [
        {"longname": "Apple Inc.", "symbol": "AAPL"},
        {"longname": "Apple Hospitality", "symbol": "APLE"},
    ]

    actual = rank_by_name_key(
        viable,
        name_key="longname",
        expected_name="Apple Inc",
        expected_symbol="AAPL",
        min_score=100,
    )

    assert "AAPL" in actual


def test_rank_by_name_key_returns_empty_when_no_name_key() -> None:
    """
    ARRANGE: viable quotes without the specified name_key
    ACT:     call rank_by_name_key
    ASSERT:  returns empty list
    """
    viable = [
        {"shortname": "Apple", "symbol": "AAPL"},
    ]

    actual = rank_by_name_key(
        viable,
        name_key="longname",
        expected_name="Apple Inc",
        expected_symbol="AAPL",
        min_score=100,
    )

    assert actual == []
