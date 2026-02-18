# integrity/analysers/data_consistency.py

from collections import Counter, defaultdict
from collections.abc import Sequence
from itertools import islice

from equity_aggregator.schemas import CanonicalEquity

from ..formatters import format_equity_with_figi, limit_items
from ..models import AnalysisSettings, Finding, SectionReport


def analyse_data_consistency(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Combine symbol, naming, and currency consistency checks.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined data consistency findings.
    """
    findings = (
        analyse_symbol_patterns(equities, settings)
        + analyse_duplicate_names(equities, settings)
        + analyse_currency_rarity(equities, settings)
    )
    return SectionReport("Symbol and Naming Consistency", findings)


def analyse_symbol_patterns(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Evaluate basic symbol shape metrics.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing ticker symbol patterns.
    """
    total = len(equities)

    if total == 0:
        return ()

    dots = sum(1 for equity in equities if _symbol_contains_dot(equity))

    numerics = sum(1 for equity in equities if _symbol_contains_digit(equity))

    long_symbols = [
        equity
        for equity in equities
        if _symbol_exceeds_limit(equity, settings.symbol_length_limit)
    ]

    symbol_limit = settings.symbol_length_limit
    highlights = (
        f"Symbols with dots: {dots:,}",
        f"Symbols containing digits: {numerics:,}",
        f"Symbols longer than {symbol_limit} chars: {len(long_symbols):,}",
    )
    return (Finding("Ticker symbol pattern review completed.", highlights),)


def analyse_duplicate_names(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Detect repeated company names.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing duplicate company names.
    """
    groups = _collect_duplicate_name_groups(equities)
    duplicates = {name: members for name, members in groups.items() if len(members) > 1}
    if not duplicates:
        return ()

    total_entries = sum(len(group) for group in duplicates.values())

    samples = _duplicate_sample_lines(duplicates, settings)

    message = (
        f"Duplicate company names detected for {len(duplicates):,} labels"
        f" affecting {total_entries:,} entries."
    )
    return (Finding(message, tuple(samples)),)


def analyse_currency_rarity(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Highlight currencies with sparse coverage.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing rare currency usage.
    """
    counts = Counter(
        equity.financials.currency for equity in equities if equity.financials.currency
    )

    threshold = settings.rare_currency_count

    rare = sorted((code, count) for code, count in counts.items() if count < threshold)

    if not rare:
        return ()

    sample_lines = [f"{code}: {count} companies" for code, count in rare]

    return (
        Finding(
            f"Currencies with fewer than {threshold} entries: {len(rare):,}.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def _collect_duplicate_name_groups(
    equities: Sequence[CanonicalEquity],
) -> dict[str, list[CanonicalEquity]]:
    """
    Group equities by normalised name.

    Args:
        equities: Sequence of canonical equities to assess.

    Returns:
        dict[str, list[CanonicalEquity]]: Mapping of normalised names to equities.
    """
    groups: dict[str, list[CanonicalEquity]] = defaultdict(list)
    for equity in equities:
        name = (equity.identity.name or "").strip().upper()
        if name:
            groups[name].append(equity)
    return groups


def _duplicate_sample_lines(
    duplicates: dict[str, list[CanonicalEquity]],
    settings: AnalysisSettings,
) -> tuple[str, ...]:
    """
    Build sample lines for duplicate name groups.

    Args:
        duplicates: Mapping of normalised names to their equities.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[str, ...]: Highlight lines summarising duplicate names.
    """
    samples: list[str] = []
    sorted_groups = sorted(
        duplicates.items(),
        key=lambda item: len(item[1]),
        reverse=True,
    )
    for name, group in islice(sorted_groups, settings.duplicate_group_limit):
        samples.append(f"{name} -> {len(group)} entries")
        member_labels = limit_items(
            (format_equity_with_figi(equity) for equity in group),
            settings.finding_sample_limit,
        )
        samples.extend(f"  - {label}" for label in member_labels)
    return tuple(samples)


def _symbol_contains_dot(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity's ticker symbol contains a dot.

    Returns:
        bool: True when the symbol includes a period character.
    """
    return "." in (equity.identity.symbol or "")


def _symbol_contains_digit(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity's ticker symbol contains a numeric digit.

    Returns:
        bool: True when at least one character in the symbol is a digit.
    """
    return any(char.isdigit() for char in equity.identity.symbol or "")


def _symbol_exceeds_limit(
    equity: CanonicalEquity,
    limit: int,
) -> bool:
    """
    Check whether the equity's ticker symbol exceeds a length threshold.

    Returns:
        bool: True when the symbol is present and longer than the limit.
    """
    symbol = equity.identity.symbol
    return symbol is not None and len(symbol) > limit
