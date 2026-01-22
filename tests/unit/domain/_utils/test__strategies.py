# test__strategies.py

from decimal import Decimal

import pytest

from equity_aggregator.domain._utils._strategies import (
    filter_by_deviation,
    fuzzy_cluster_mode,
    median_decimal,
    mode_first,
    union_ordered,
)

pytestmark = pytest.mark.unit


def test_mode_first_empty_sequence_returns_none() -> None:
    """
    ARRANGE: empty sequence
    ACT:     mode_first
    ASSERT:  None returned
    """
    assert mode_first([]) is None


def test_mode_first_single_value() -> None:
    """
    ARRANGE: single value
    ACT:     mode_first
    ASSERT:  that value returned
    """
    assert mode_first(["A"]) == "A"


def test_mode_first_clear_majority() -> None:
    """
    ARRANGE: values with clear mode
    ACT:     mode_first
    ASSERT:  most frequent value returned
    """
    assert mode_first(["A", "B", "A", "C", "A"]) == "A"


def test_mode_first_tie_returns_first_occurrence() -> None:
    """
    ARRANGE: values with tied frequency
    ACT:     mode_first
    ASSERT:  first occurring tied value returned
    """
    assert mode_first(["B", "A", "B", "A"]) == "B"


def test_mode_first_with_integers() -> None:
    """
    ARRANGE: integer values
    ACT:     mode_first
    ASSERT:  most frequent integer returned
    """
    expected_mode = 2
    assert mode_first([1, 2, 3, 2, 2]) == expected_mode


def test_median_decimal_empty_sequence_returns_none() -> None:
    """
    ARRANGE: empty sequence
    ACT:     median_decimal
    ASSERT:  None returned
    """
    assert median_decimal([]) is None


def test_median_decimal_single_value() -> None:
    """
    ARRANGE: single decimal value
    ACT:     median_decimal
    ASSERT:  that value returned
    """
    assert median_decimal([Decimal("5.5")]) == Decimal("5.5")


def test_median_decimal_odd_count() -> None:
    """
    ARRANGE: odd number of values
    ACT:     median_decimal
    ASSERT:  middle value returned
    """
    assert median_decimal([Decimal("1"), Decimal("3"), Decimal("5")]) == Decimal("3")


def test_median_decimal_even_count() -> None:
    """
    ARRANGE: even number of values
    ACT:     median_decimal
    ASSERT:  average of two middle values returned
    """
    assert median_decimal([Decimal("1"), Decimal("9")]) == Decimal("5")


def test_median_decimal_preserves_precision() -> None:
    """
    ARRANGE: decimal values with high precision
    ACT:     median_decimal
    ASSERT:  precision maintained
    """
    values = [Decimal("1.111"), Decimal("2.222"), Decimal("3.333")]
    assert median_decimal(values) == Decimal("2.222")


def test_union_ordered_empty_lists_returns_none() -> None:
    """
    ARRANGE: empty lists
    ACT:     union_ordered
    ASSERT:  None returned
    """
    assert union_ordered([]) is None


def test_union_ordered_single_list() -> None:
    """
    ARRANGE: single list with values
    ACT:     union_ordered
    ASSERT:  deduplicated list returned
    """
    assert union_ordered([["A", "B", "A"]]) == ["A", "B"]


def test_union_ordered_multiple_lists_preserves_order() -> None:
    """
    ARRANGE: multiple lists with overlapping values
    ACT:     union_ordered
    ASSERT:  first occurrence order preserved
    """
    assert union_ordered([["A", "B"], ["C", "A"], ["D"]]) == ["A", "B", "C", "D"]


def test_union_ordered_filters_empty_strings() -> None:
    """
    ARRANGE: lists containing empty and whitespace strings
    ACT:     union_ordered
    ASSERT:  empty strings filtered out
    """
    assert union_ordered([["A", "", "B", "   "]]) == ["A", "B"]


def test_union_ordered_handles_none_lists() -> None:
    """
    ARRANGE: sequence containing None values
    ACT:     union_ordered
    ASSERT:  None values skipped
    """
    assert union_ordered([["A"], None, ["B"]]) == ["A", "B"]


def test_union_ordered_all_none_returns_none() -> None:
    """
    ARRANGE: all lists are None
    ACT:     union_ordered
    ASSERT:  None returned
    """
    assert union_ordered([None, None]) is None


def test_fuzzy_cluster_mode_empty_sequence_returns_none() -> None:
    """
    ARRANGE: empty sequence
    ACT:     fuzzy_cluster_mode
    ASSERT:  None returned
    """
    assert fuzzy_cluster_mode([]) is None


def test_fuzzy_cluster_mode_single_value() -> None:
    """
    ARRANGE: single string
    ACT:     fuzzy_cluster_mode
    ASSERT:  that string returned
    """
    assert fuzzy_cluster_mode(["Apple Inc"]) == "Apple Inc"


def test_fuzzy_cluster_mode_exact_matches() -> None:
    """
    ARRANGE: multiple identical strings
    ACT:     fuzzy_cluster_mode
    ASSERT:  that string returned
    """
    assert fuzzy_cluster_mode(["Apple", "Apple", "Apple"]) == "Apple"


def test_fuzzy_cluster_mode_similar_strings() -> None:
    """
    ARRANGE: similar strings with minor variations
    ACT:     fuzzy_cluster_mode
    ASSERT:  first occurrence from best cluster returned
    """
    actual = fuzzy_cluster_mode(["Apple Inc", "Apple Inc.", "APPLE INC"])
    assert actual == "Apple Inc"


def test_fuzzy_cluster_mode_best_cluster_by_weight() -> None:
    """
    ARRANGE: two clusters with different weights
    ACT:     fuzzy_cluster_mode
    ASSERT:  representative from heavier cluster returned
    """
    values = ["Apple Inc", "Apple Inc", "Microsoft Corp"]
    actual = fuzzy_cluster_mode(values)
    assert actual == "Apple Inc"


def test_fuzzy_cluster_mode_tie_returns_earliest() -> None:
    """
    ARRANGE: tied cluster weights
    ACT:     fuzzy_cluster_mode
    ASSERT:  first occurrence from earliest cluster returned
    """
    values = ["Banana Corp", "Apple Inc"]
    actual = fuzzy_cluster_mode(values)
    assert actual == "Banana Corp"


def test_fuzzy_cluster_mode_custom_threshold() -> None:
    """
    ARRANGE: strings with varying similarity and low threshold
    ACT:     fuzzy_cluster_mode with threshold=50
    ASSERT:  groups more loosely
    """
    values = ["Apple", "Apples", "Pear"]
    actual = fuzzy_cluster_mode(values, threshold=50)
    assert actual in values


def test_filter_by_deviation_empty_sequence() -> None:
    """
    ARRANGE: empty sequence
    ACT:     filter_by_deviation
    ASSERT:  empty list returned
    """
    assert filter_by_deviation([]) == []


def test_filter_by_deviation_below_minimum_samples() -> None:
    """
    ARRANGE: fewer values than min_samples
    ACT:     filter_by_deviation
    ASSERT:  all values returned unfiltered
    """
    values = [Decimal("1"), Decimal("2")]
    assert filter_by_deviation(values, min_samples=3) == values


def test_filter_by_deviation_removes_outliers() -> None:
    """
    ARRANGE: values with clear outlier
    ACT:     filter_by_deviation with 50% threshold
    ASSERT:  outlier removed
    """
    values = [Decimal("10"), Decimal("11"), Decimal("100")]
    actual = filter_by_deviation(values, max_deviation=Decimal("0.5"))
    assert actual == [Decimal("10"), Decimal("11")]


def test_filter_by_deviation_median_zero() -> None:
    """
    ARRANGE: values with median of zero
    ACT:     filter_by_deviation
    ASSERT:  all values returned (division by zero avoided)
    """
    values = [Decimal("-1"), Decimal("0"), Decimal("1")]
    assert filter_by_deviation(values) == values


def test_filter_by_deviation_all_within_threshold() -> None:
    """
    ARRANGE: values all within threshold
    ACT:     filter_by_deviation
    ASSERT:  all values returned
    """
    values = [Decimal("9"), Decimal("10"), Decimal("11")]
    actual = filter_by_deviation(values, max_deviation=Decimal("0.5"))
    assert actual == values


def test_filter_by_deviation_custom_threshold() -> None:
    """
    ARRANGE: values and tight 10% threshold
    ACT:     filter_by_deviation
    ASSERT:  only values within 10% of median returned
    """
    values = [Decimal("9"), Decimal("10"), Decimal("15")]
    actual = filter_by_deviation(values, max_deviation=Decimal("0.1"), min_samples=3)
    expected_values = [Decimal("9"), Decimal("10")]
    assert actual == expected_values
