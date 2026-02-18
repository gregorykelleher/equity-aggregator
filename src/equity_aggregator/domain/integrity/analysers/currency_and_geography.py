# integrity/analysers/currency_and_geography.py

from collections import Counter
from collections.abc import Sequence

from equity_aggregator.schemas import CanonicalEquity

from ..formatters import format_percentage
from ..models import Finding, SectionReport


def analyse_currency_and_geography(
    equities: Sequence[CanonicalEquity],
) -> SectionReport:
    """
    Aggregate currency and geographic proxy metrics.

    Args:
        equities: Sequence of canonical equities to assess.

    Returns:
        SectionReport: Combined currency and geography findings.
    """
    findings = currency_distribution(equities) + geography_proxies(equities)
    return SectionReport("Currency and Geography", findings)


def currency_distribution(
    equities: Sequence[CanonicalEquity],
) -> tuple[Finding, ...]:
    """
    Summarise currency usage within the dataset.

    Args:
        equities: Sequence of canonical equities to assess.

    Returns:
        tuple[Finding, ...]: Findings describing currency distribution.
    """
    counts = Counter(
        eq.financials.currency for eq in equities if eq.financials.currency
    )
    if not counts:
        return ()

    total = sum(counts.values())
    lines = [
        (f"{code}: {count:,} entries ({format_percentage((count / total) * 100)})")
        for code, count in counts.most_common()
    ]
    return (Finding("Currency distribution summary.", tuple(lines)),)


def geography_proxies(
    equities: Sequence[CanonicalEquity],
) -> tuple[Finding, ...]:
    """
    Use identifier presence as a proxy for geography.

    Args:
        equities: Sequence of canonical equities to assess.

    Returns:
        tuple[Finding, ...]: Findings describing geographic indicator coverage.
    """
    counts = {
        "CUSIP": sum(1 for eq in equities if eq.identity.cusip),
        "ISIN": sum(1 for eq in equities if eq.identity.isin),
        "CIK": sum(1 for eq in equities if eq.identity.cik),
    }
    lines = [f"{label} present: {count:,}" for label, count in counts.items()]
    return (Finding("Geographic indicator coverage.", tuple(lines)),)
