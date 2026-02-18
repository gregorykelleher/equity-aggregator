# integrity/analysers/cross_field_logic.py

from collections.abc import Sequence

from equity_aggregator.schemas import CanonicalEquity

from ..formatters import (
    describe_cap_gap,
    describe_range_bounds,
    format_equity,
    limit_items,
)
from ..models import AnalysisSettings, Finding, SectionReport


def analyse_cross_field_logic(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Gather cross-field logic inconsistencies.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined cross-field consistency findings.
    """
    findings = (
        detect_price_without_cap(equities, settings)
        + detect_cap_without_price(equities, settings)
        + detect_missing_price_and_cap(equities)
        + detect_partial_range(equities, settings)
    )
    return SectionReport("Cross-field Logic Consistency", findings)


def detect_price_without_cap(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify price records lacking market capitalisation.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing prices without market capitalisation.
    """
    entries = [
        equity
        for equity in equities
        if _has_positive_price(equity) and not equity.financials.market_cap
    ]
    if not entries:
        return ()

    sample_lines = (
        f"{format_equity(equity)} -> price {equity.financials.last_price}, cap missing"
        for equity in entries
    )
    return (
        Finding(
            (f"Price recorded without market cap for {len(entries):,} equities."),
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_cap_without_price(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify market capitalisation entries that lack a price.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing market capitalisations without prices.
    """
    entries = [
        equity
        for equity in equities
        if _has_positive_cap(equity) and not equity.financials.last_price
    ]
    if not entries:
        return ()

    sample_lines = (describe_cap_gap(equity) for equity in entries)
    return (
        Finding(
            (f"Market cap recorded without price for {len(entries):,} equities."),
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_missing_price_and_cap(
    equities: Sequence[CanonicalEquity],
) -> tuple[Finding, ...]:
    """
    Report equities missing both price and market cap.

    Args:
        equities: Sequence of canonical equities to assess.

    Returns:
        tuple[Finding, ...]: Findings describing missing price and cap.
    """
    missing = [equity for equity in equities if _lacks_price_and_cap(equity)]
    if not missing:
        return ()

    with_other_metrics = sum(
        1 for equity in missing if _has_any_supplementary_metric(equity)
    )
    highlights = (
        f"Total entries missing both fields: {len(missing):,}.",
        f"Entries that still carry other metrics: {with_other_metrics:,}.",
    )
    return (Finding("Price and market cap simultaneously missing.", highlights),)


def detect_partial_range(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify equities with only one side of the price range populated.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing partial 52-week ranges.
    """
    partial = [equity for equity in equities if _has_one_sided_range(equity)]
    if not partial:
        return ()

    sample_lines = (describe_range_bounds(equity) for equity in partial)
    return (
        Finding(
            f"Partial 52-week ranges for {len(partial):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def _has_positive_price(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity carries a positive last price.

    Returns:
        bool: True when last_price is present and greater than zero.
    """
    return equity.financials.last_price is not None and equity.financials.last_price > 0


def _has_positive_cap(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity carries a positive market capitalisation.

    Returns:
        bool: True when market_cap is present and greater than zero.
    """
    return equity.financials.market_cap is not None and equity.financials.market_cap > 0


def _has_one_sided_range(equity: CanonicalEquity) -> bool:
    """
    Check whether exactly one side of the 52-week range is populated.

    Returns:
        bool: True when only one of the min/max bounds is present.
    """
    has_min = equity.financials.fifty_two_week_min is not None
    has_max = equity.financials.fifty_two_week_max is not None
    return has_min != has_max


def _lacks_price_and_cap(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity is missing both price and market capitalisation.

    Returns:
        bool: True when neither last_price nor market_cap is populated.
    """
    return not equity.financials.last_price and not equity.financials.market_cap


def _has_any_supplementary_metric(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity carries at least one supplementary metric.

    Returns:
        bool: True when revenue, trailing PE, or dividend yield is present.
    """
    return bool(
        equity.financials.revenue
        or equity.financials.trailing_pe
        or equity.financials.dividend_yield
    )
