# _utils/test_fuzzy.py

import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif._utils import (
    rank_candidates,
    select_best_parent,
)

pytestmark = pytest.mark.unit


def test_rank_candidates_returns_sorted_by_score_descending() -> None:
    """
    ARRANGE: two candidates, one a close match and one weaker
    ACT:     rank against equity name
    ASSERT:  best match appears first
    """
    candidates = [
        ("Siemens Healthineers AG", "HEALTH_LEI"),
        ("Siemens AG", "SIEMENS_LEI"),
    ]

    actual = rank_candidates("Siemens AG", candidates)

    assert actual[0][1] == "SIEMENS_LEI"


def test_rank_candidates_filters_below_cutoff() -> None:
    """
    ARRANGE: one candidate with a completely unrelated name
    ACT:     rank against equity name
    ASSERT:  returns empty list
    """
    candidates = [("Zxywvut Corp", "RANDOM_LEI")]

    actual = rank_candidates("Apple Inc.", candidates)

    assert actual == []


def test_rank_candidates_returns_empty_for_empty_input() -> None:
    """
    ARRANGE: empty candidates list
    ACT:     rank against equity name
    ASSERT:  returns empty list
    """
    actual = rank_candidates("Apple Inc.", [])

    assert actual == []


def test_rank_candidates_returns_all_qualifying_candidates() -> None:
    """
    ARRANGE: two candidates that both score above cutoff
    ACT:     rank against equity name
    ASSERT:  both candidates are returned
    """
    expected = 2
    candidates = [
        ("Apple Inc.", "APPLE_LEI"),
        ("Apple Incorporated", "APPLE2_LEI"),
    ]

    actual = rank_candidates("Apple Inc.", candidates)

    assert len(actual) == expected


def test_select_best_parent_returns_parent_lei() -> None:
    """
    ARRANGE: single parent entity
    ACT:     select best parent
    ASSERT:  returns the parent's LEI
    """
    parents = [("Volkswagen AG", "VW_PARENT_LEI")]

    actual = select_best_parent("Volkswagen AG", parents)

    assert actual == "VW_PARENT_LEI"


def test_select_best_parent_always_returns_parent_over_candidate() -> None:
    """
    ARRANGE: parent name is a weaker match than the candidate would be
    ACT:     select best parent
    ASSERT:  still returns the parent's LEI (parent is the issuer)
    """
    parents = [("Some Unrelated Corp", "PARENT_LEI")]

    actual = select_best_parent("Apple Inc.", parents)

    assert actual == "PARENT_LEI"


def test_select_best_parent_picks_best_among_multiple() -> None:
    """
    ARRANGE: two parent entities, one a closer name match
    ACT:     select best parent
    ASSERT:  returns the closer-matching parent's LEI
    """
    parents = [
        ("Volkswagen Financial Services AG", "VW_FIN_LEI"),
        ("Volkswagen AG", "VW_PARENT_LEI"),
    ]

    actual = select_best_parent("Volkswagen AG", parents)

    assert actual == "VW_PARENT_LEI"


def test_select_best_parent_returns_none_for_empty_parents() -> None:
    """
    ARRANGE: empty parents list
    ACT:     select best parent
    ASSERT:  returns None
    """
    actual = select_best_parent("Apple Inc.", [])

    assert actual is None
