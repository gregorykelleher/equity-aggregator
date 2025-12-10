# _utils/_strategies.py


from collections import Counter
from collections.abc import Sequence
from decimal import Decimal
from functools import cache
from statistics import median

from rapidfuzz import fuzz


def filter_by_deviation(
    values: Sequence[Decimal],
    max_deviation: Decimal = Decimal("0.5"),
    min_samples: int = 3,
) -> list[Decimal]:
    """
    Filter values that deviate more than a threshold percentage from the median.

    Args:
        values: Sequence of Decimal values.
        max_deviation: Maximum allowed deviation as decimal (0.5 = 50%).
        min_samples: Minimum sample size to apply filtering. Below this,
            returns values unfiltered.

    Returns:
        List of values within threshold, or all values if filtering not applicable.
    """
    if len(values) < min_samples:
        return list(values)

    med = median(values)

    if med == 0:
        return list(values)

    return [v for v in values if abs(v - med) / abs(med) <= max_deviation]


def mode_first[T](values: Sequence[T]) -> T | None:
    """
    Selects the most frequently occurring value from a sequence.

    If multiple values share the highest frequency (a tie), the value that appears
    first in the sequence is returned. Returns None if the sequence is empty.

    Args:
        values (Sequence[T]): A sequence of values from which to select the mode.

    Returns:
        T | None: The most frequent value with ties broken by first occurrence,
            or None if the sequence is empty.
    """
    if not values:
        return None

    counts = Counter(values)
    best_freq = max(counts.values())
    return next(v for v in values if counts[v] == best_freq)


def median_decimal(values: Sequence[Decimal]) -> Decimal | None:
    """
    Calculates the median value of a sequence of Decimal values.

    Args:
        values (Sequence[Decimal]): A sequence of Decimal values.

    Returns:
        Decimal | None: The median of the sequence as a Decimal, or None if
            the sequence is empty.
    """
    return median(values) if values else None


def union_ordered[T](lists: Sequence[list[T] | None]) -> list[T] | None:
    """
    Merges multiple lists into a single deduplicated list, preserving the order of
    first occurrence.

    Flattens all input lists, removes duplicates while maintaining the order in which
    elements first appear, and filters out empty or blank string values. Returns None
    if the result is empty.

    Args:
        lists (Sequence[list[T] | None]): A sequence of lists (or None values) to merge.

    Returns:
        list[T] | None: A deduplicated list in order of first appearance, or None
            if no valid elements exist.
    """
    seen: dict[T, None] = {}
    for lst in lists:
        for item in lst or []:
            if item and str(item).strip():
                seen.setdefault(item, None)
    return list(seen) or None


def fuzzy_cluster_mode(
    values: Sequence[str],
    threshold: int = 90,
) -> str | None:
    """
    Selects a representative string from a sequence using fuzzy clustering.

    This function clusters similar strings using fuzzy matching (token-set ratio),
    then selects the cluster with the highest total occurrence count. Within the
    chosen cluster, it returns the earliest original spelling found in the input
    sequence.

    Args:
        values (Sequence[str]): A sequence of strings to cluster and select from.
        threshold (int, optional): Similarity threshold (0-100) for clustering strings.
            Defaults to 90.

    Returns:
        str | None: The selected representative string from the group, or None if
            the sequence is empty.
    """
    if not values:
        return None

    clusters = _cluster(list(values), threshold)
    weights = Counter(values)

    best_cluster = max(clusters, key=lambda c: sum(weights[v] for v in c))
    return next(v for v in values if v in best_cluster)


@cache
def _token_ratio(a: str, b: str) -> int:
    """
    Compute the token-set ratio between two strings using fuzzy matching.

    Args:
        a (str): The first string to compare.
        b (str): The second string to compare.

    Returns:
        int: The token-set similarity ratio (0-100) between the two strings.
    """
    return fuzz.token_set_ratio(a, b)


def _cluster(names: list[str], threshold: int = 90) -> list[list[str]]:
    """
    Groups similar strings into clusters using single-link clustering based on token-set
    ratio.

    Each name is compared to the representative (first item) of each existing cluster.
    If the token-set ratio between the name and a cluster's representative is greater
    than or equal to the specified threshold, the name is added to that cluster.

    Otherwise, a new cluster is created for the name.

    Args:
        names (list[str]): List of strings to be clustered.
        threshold (int, optional): Minimum token-set ratio (0-100) required to join an
            existing cluster. Defaults to 90.

    Returns:
        list[list[str]]: A list of clusters, where each cluster is a list of similar
            strings.
    """
    clusters: list[list[str]] = []

    for name in names:
        target = next(
            (c for c in clusters if _token_ratio(name, c[0]) >= threshold),
            None,
        )

        if target:
            target.append(name)
        else:
            clusters.append([name])

    return clusters
