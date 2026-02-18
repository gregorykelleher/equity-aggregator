# integrity/analysers/extreme_financial_values.py

from collections.abc import Sequence

from equity_aggregator.schemas import CanonicalEquity

from ..formatters import format_equity, format_percentage, limit_items
from ..models import AnalysisSettings, Finding, SectionReport


def detect_extreme_financial_values(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Combine extreme value detections.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined extreme financial value findings.
    """
    findings = (
        detect_extreme_dividends(equities, settings)
        + detect_penny_stocks(equities, settings)
        + detect_profit_margin_extremes(equities, settings)
        + detect_negative_price_to_book(equities, settings)
        + detect_round_price_clusters(equities, settings)
    )
    return SectionReport("Extreme Financial Values", findings)


def detect_extreme_dividends(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Highlight dividend yields that exceed alert thresholds.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing extreme dividend yields.
    """
    threshold = settings.dividend_yield_alert

    matches = [equity for equity in equities if _has_dividend_above(equity, threshold)]

    if not matches:
        return ()

    ranked = sorted(
        matches,
        key=lambda entry: entry.financials.dividend_yield,
        reverse=True,
    )

    sample_lines = (
        f"{format_equity(equity)} -> yield {equity.financials.dividend_yield:.2f}%"
        for equity in ranked
    )

    return (
        Finding(
            f"Dividend yields above {threshold}%: {len(matches):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_penny_stocks(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify prices below the configured penny threshold.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing ultra-low traded prices.
    """
    matches = [
        equity
        for equity in equities
        if _is_penny_stock(equity, settings.penny_stock_threshold)
    ]

    if not matches:
        return ()

    sample_lines = (
        f"{format_equity(equity)} -> price {equity.financials.last_price}"
        for equity in matches
    )

    return (
        Finding(
            f"Prices below one cent for {len(matches):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_profit_margin_extremes(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Highlight profit margins that sit outside realistic ranges.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing extreme profit margins.
    """
    matches = [
        equity
        for equity in equities
        if _has_extreme_margin(
            equity,
            settings.profit_margin_low,
            settings.profit_margin_high,
        )
    ]
    if not matches:
        return ()

    ranked = sorted(
        matches,
        key=lambda entry: abs(entry.financials.profit_margin),
        reverse=True,
    )

    sample_lines = (
        f"{format_equity(equity)} -> margin {equity.financials.profit_margin:.2f}%"
        for equity in ranked
    )

    return (
        Finding(
            "Profit margins beyond +/-100% detected.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_negative_price_to_book(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Flag negative price-to-book ratios.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing negative price-to-book results.
    """
    matches = [equity for equity in equities if _has_negative_price_to_book(equity)]
    if not matches:
        return ()

    sample_lines = (
        f"{format_equity(equity)} -> P/B {equity.financials.price_to_book:.2f}"
        for equity in matches
    )

    return (
        Finding(
            f"Negative price-to-book ratios for {len(matches):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_round_price_clusters(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Highlight clustering of round pound or dollar prices.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing round-price clustering.
    """
    matches = [equity for equity in equities if _has_round_price(equity)]
    if not matches:
        return ()

    ratio = (len(matches) / len(equities)) * 100 if equities else 0.0

    message = (
        f"Round dollar price clustering across {len(matches):,}"
        f" equities ({format_percentage(ratio)} of dataset)."
    )

    highlights = _round_price_warning(ratio, settings.round_price_threshold)
    return (Finding(message, highlights),)


def _has_dividend_above(
    equity: CanonicalEquity,
    threshold: float,
) -> bool:
    """
    Check whether the equity's dividend yield exceeds a threshold.

    Returns:
        bool: True when dividend_yield is present and above the threshold.
    """
    dividend = equity.financials.dividend_yield
    return dividend is not None and dividend > threshold


def _is_penny_stock(
    equity: CanonicalEquity,
    threshold: float,
) -> bool:
    """
    Check whether the equity's price falls below the penny stock threshold.

    Returns:
        bool: True when last_price is positive but below the threshold.
    """
    price = equity.financials.last_price
    return price is not None and 0 < price < threshold


def _has_extreme_margin(
    equity: CanonicalEquity,
    low: float,
    high: float,
) -> bool:
    """
    Check whether the equity's profit margin falls outside realistic bounds.

    Returns:
        bool: True when profit_margin is present and beyond the given range.
    """
    margin = equity.financials.profit_margin
    return margin is not None and (margin > high or margin < low)


def _has_negative_price_to_book(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity has a negative price-to-book ratio.

    Returns:
        bool: True when price_to_book is present and below zero.
    """
    ptb = equity.financials.price_to_book
    return ptb is not None and ptb < 0


def _has_round_price(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity's price is a round whole number above one.

    Returns:
        bool: True when last_price is an integer value greater than one.
    """
    price = equity.financials.last_price
    return price is not None and price > 1 and price % 1 == 0


def _round_price_warning(
    ratio: float,
    threshold: float,
) -> tuple[str, ...]:
    """
    Produce a warning highlight when round-price concentration is high.

    Returns:
        tuple[str, ...]: A single warning line if above threshold, empty otherwise.
    """
    if ratio > threshold:
        return ("High concentration of round prices may indicate defaults.",)
    return ()
