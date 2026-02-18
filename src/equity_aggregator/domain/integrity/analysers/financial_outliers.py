# integrity/analysers/financial_outliers.py

from collections.abc import Sequence
from decimal import Decimal
from statistics import mean, median, stdev

from equity_aggregator.schemas import CanonicalEquity

from ..formatters import (
    describe_price_vs_max,
    format_currency,
    format_equity,
    limit_items,
)
from ..models import AnalysisSettings, Finding, SectionReport


def analyse_financial_outliers(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Analyse financial metric outliers.

    Args:
        equities: Canonical equities to analyse.
        settings: Analysis thresholds.

    Returns:
        SectionReport: Section report with outlier findings.
    """
    findings = (
        compute_pe_findings(equities, settings)
        + compute_market_cap_findings(equities, settings)
        + compute_negative_metric_findings(equities, settings)
        + compute_price_range_findings(equities, settings)
    )
    return SectionReport("Financial Metric Outliers", findings)


def compute_pe_findings(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Analyse trailing P/E ratio distribution.

    Args:
        equities: Canonical equities to analyse.
        settings: Analysis thresholds.

    Returns:
        tuple[Finding, ...]: Findings tuple.
    """
    ratios = _extract_positive_pe_ratios(equities)

    if len(ratios) <= settings.min_sample_size:
        return ()

    deviation = stdev(ratios) if len(ratios) > 1 else 0.0
    limit = mean(ratios) + (3 * deviation) if deviation else None

    outliers = [
        format_equity(equity) for equity in equities if _pe_exceeds_limit(equity, limit)
    ]

    samples = limit_items(outliers, settings.finding_sample_limit)

    highlights = _build_pe_highlights(ratios, deviation, limit, samples)
    return (Finding("P/E ratio distribution reviewed.", highlights),)


def compute_market_cap_findings(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Analyse market capitalisation distribution.

    Args:
        equities: Canonical equities to analyse.
        settings: Analysis thresholds.

    Returns:
        tuple[Finding, ...]: Findings tuple.
    """
    caps = _extract_positive_market_caps(equities)
    if len(caps) <= settings.min_sample_size:
        return ()

    mega_caps = [cap for cap in caps if cap > settings.mega_cap_threshold]
    micro_caps = [cap for cap in caps if cap < settings.micro_cap_threshold]
    mega_threshold = format_currency(settings.mega_cap_threshold)
    micro_threshold = format_currency(settings.micro_cap_threshold)

    highlights = [
        f"Median market cap: {format_currency(median(caps))}",
        f"Mean market cap: {format_currency(mean(caps))}",
        f"Mega caps > {mega_threshold}: {len(mega_caps):,}",
        f"Micro caps < {micro_threshold}: {len(micro_caps):,}",
    ]

    if mega_caps:
        highlights.append(f"Largest market cap: {format_currency(max(mega_caps))}")

    return (
        Finding(
            "Market capitalisation distribution summarised.",
            tuple(highlights),
        ),
    )


def compute_negative_metric_findings(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify negative or zero financial metrics.

    Args:
        equities: Canonical equities to analyse.
        settings: Analysis thresholds.

    Returns:
        tuple[Finding, ...]: Findings tuple.
    """
    return _negative_pe_finding(equities, settings) + _zero_cap_finding(
        equities, settings
    )


def compute_price_range_findings(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify prices exceeding 52-week maximum.

    Args:
        equities: Canonical equities to analyse.
        settings: Analysis thresholds.

    Returns:
        tuple[Finding, ...]: Findings tuple.
    """
    anomalies = [
        equity
        for equity in equities
        if _price_exceeds_range(equity, settings.price_tolerance)
    ]
    if not anomalies:
        return ()

    samples = limit_items(
        (describe_price_vs_max(equity) for equity in anomalies),
        settings.finding_sample_limit,
    )
    return (
        Finding(
            f"Price exceeds 52-week max (+10% tolerance)"
            f" for {len(anomalies):,} equities.",
            samples,
        ),
    )

def _negative_pe_finding(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Build a finding for equities with negative P/E ratios.

    Returns:
        tuple[Finding, ...]: Single finding if any match, empty otherwise.
    """
    matches = [equity for equity in equities if _has_negative_pe(equity)]
    if not matches:
        return ()

    samples = limit_items(
        (format_equity(equity) for equity in matches),
        settings.finding_sample_limit,
    )
    return (
        Finding(
            f"Negative P/E ratios present: {len(matches):,} companies.",
            samples,
        ),
    )


def _zero_cap_finding(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Build a finding for equities with zero or negative market cap.

    Returns:
        tuple[Finding, ...]: Single finding if any match, empty otherwise.
    """
    matches = [equity for equity in equities if _has_zero_or_negative_cap(equity)]
    if not matches:
        return ()

    sample_lines = (
        f"{format_equity(equity)} -> value {equity.financials.market_cap}"
        for equity in matches
    )

    samples = limit_items(sample_lines, settings.finding_sample_limit)
    
    return (
        Finding(
            f"Zero or negative market cap entries: {len(matches):,} companies.",
            samples,
        ),
    )


def _extract_positive_pe_ratios(
    equities: Sequence[CanonicalEquity],
) -> list[Decimal]:
    """
    Extract positive trailing P/E ratios from the dataset.

    Returns:
        list[Decimal]: List of positive P/E ratios.
    """
    return [
        equity.financials.trailing_pe for equity in equities if _has_positive_pe(equity)
    ]


def _extract_positive_market_caps(
    equities: Sequence[CanonicalEquity],
) -> list[Decimal]:
    """
    Extract positive market capitalisation values from the dataset.

    Returns:
        list[Decimal]: List of positive market caps.
    """
    return [
        equity.financials.market_cap for equity in equities if _has_positive_cap(equity)
    ]


def _build_pe_highlights(
    ratios: list[Decimal],
    deviation: Decimal | float,
    limit: float | None,
    outliers: tuple[str, ...],
) -> tuple[str, ...]:
    """
    Build highlight lines for P/E ratio analysis.

    Returns:
        tuple[str, ...]: Formatted highlight lines.
    """
    lines = [
        f"Median ratio: {float(median(ratios)):.2f}",
        f"Mean ratio: {float(mean(ratios)):.2f}",
    ]
    if deviation:
        lines.append(f"Std deviation: {float(deviation):.2f}")
    if limit and outliers:
        lines.append(f"Extreme ratios above {limit:.1f}: {len(outliers)} samples")
        lines.extend(f"  - {sample}" for sample in outliers)
    return tuple(lines)


def _pe_exceeds_limit(
    equity: CanonicalEquity,
    limit: float | None,
) -> bool:
    """
    Check whether the equity's P/E ratio exceeds the outlier threshold.

    Returns:
        bool: True when P/E is present and above the limit.
    """
    pe = equity.financials.trailing_pe
    return limit is not None and pe is not None and pe > limit


def _has_positive_pe(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity has a positive trailing P/E ratio.

    Returns:
        bool: True when trailing_pe is present and greater than zero.
    """
    pe = equity.financials.trailing_pe
    return pe is not None and pe > 0


def _has_negative_pe(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity has a negative trailing P/E ratio.

    Returns:
        bool: True when trailing_pe is present and less than zero.
    """
    pe = equity.financials.trailing_pe
    return pe is not None and pe < 0


def _has_positive_cap(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity has a positive market capitalisation.

    Returns:
        bool: True when market_cap is present and greater than zero.
    """
    cap = equity.financials.market_cap
    return cap is not None and cap > 0


def _has_zero_or_negative_cap(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity has a zero or negative market capitalisation.

    Returns:
        bool: True when market_cap is present and at or below zero.
    """
    cap = equity.financials.market_cap
    return cap is not None and cap <= 0


def _price_exceeds_range(
    equity: CanonicalEquity,
    tolerance: Decimal,
) -> bool:
    """
    Check whether the equity's price exceeds its 52-week maximum within tolerance.

    Returns:
        bool: True when the price surpasses the adjusted 52-week high.
    """
    price = equity.financials.last_price
    high = equity.financials.fifty_two_week_max
    low = equity.financials.fifty_two_week_min
    return (
        price is not None
        and high is not None
        and low is not None
        and price > high * tolerance
    )
