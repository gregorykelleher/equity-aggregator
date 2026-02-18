# integrity/analysers/temporal_anomalies.py

from collections.abc import Sequence

from equity_aggregator.schemas import CanonicalEquity

from ..formatters import (
    describe_price_vs_min,
    describe_range_bounds,
    format_equity,
    limit_items,
)
from ..models import AnalysisSettings, Finding, SectionReport


def detect_temporal_anomalies(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Combine range-based temporal checks.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined temporal anomaly findings.
    """
    findings = (
        detect_range_inversions(equities, settings)
        + detect_stale_range_data(equities, settings)
        + detect_price_below_min(equities, settings)
    )
    return SectionReport("Price Range Integrity", findings)


def detect_range_inversions(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify impossible 52-week ranges.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing range inversions.
    """
    matches = [equity for equity in equities if _has_inverted_range(equity)]
    if not matches:
        return ()

    sample_lines = (describe_range_bounds(equity) for equity in matches)

    return (
        Finding(
            f"Range inversions detected for {len(matches):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_stale_range_data(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Look for stale price data where price equals both range endpoints.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing stale price data.
    """
    matches = [equity for equity in equities if _has_stale_range(equity)]
    if not matches:
        return ()

    sample_lines = (
        f"{format_equity(equity)} -> all values {equity.financials.last_price}"
        for equity in matches
    )

    return (
        Finding(
            f"Potentially stale price data for {len(matches):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_price_below_min(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Flag prices that sit well below the 52-week minimum.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing prices beneath range minima.
    """
    matches = [
        equity
        for equity in equities
        if _price_below_minimum(equity, settings.price_to_min_factor)
    ]

    if not matches:
        return ()

    sample_lines = (describe_price_vs_min(equity) for equity in matches)

    return (
        Finding(
            f"Prices materially below 52-week minimum for {len(matches):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def _has_inverted_range(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity's 52-week minimum exceeds its maximum.

    Returns:
        bool: True when both bounds are present and the minimum is higher.
    """
    low = equity.financials.fifty_two_week_min
    high = equity.financials.fifty_two_week_max
    return low is not None and high is not None and low > high


def _has_stale_range(equity: CanonicalEquity) -> bool:
    """
    Check whether the price, 52-week minimum, and maximum are all identical.

    Returns:
        bool: True when all three values are present and equal.
    """
    price = equity.financials.last_price
    low = equity.financials.fifty_two_week_min
    high = equity.financials.fifty_two_week_max
    return (
        price is not None
        and low is not None
        and high is not None
        and price == low == high
    )


def _price_below_minimum(
    equity: CanonicalEquity,
    factor: float,
) -> bool:
    """
    Check whether the equity's price falls below a fraction of its 52-week minimum.

    Returns:
        bool: True when the price is present and beneath the adjusted minimum.
    """
    price = equity.financials.last_price
    low = equity.financials.fifty_two_week_min
    return price is not None and low is not None and price < low * factor
