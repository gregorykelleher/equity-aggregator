# integrity/analysers/data_quality.py

from collections.abc import Sequence
from itertools import islice

from equity_aggregator.schemas import CanonicalEquity

from ..formatters import (
    format_coverage_table,
    format_equity,
    limit_items,
    score_equity_completeness,
)
from ..models import AnalysisSettings, Finding, SectionReport


def analyse_data_quality(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Aggregate completeness and coverage insights.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined data completeness findings.
    """
    findings = (
        identity_completeness(equities)
        + top_complete_profiles(equities, settings)
        + valuation_coverage(equities)
    )
    return SectionReport("Data Completeness", findings)


def identity_completeness(
    equities: Sequence[CanonicalEquity],
) -> tuple[Finding, ...]:
    """
    Summarise completion of core identity fields.

    Args:
        equities: Sequence of canonical equities to assess.

    Returns:
        tuple[Finding, ...]: Findings describing identity field coverage.
    """
    total = len(equities)
    if total == 0:
        return ()

    identity_fields = (
        ("name", "Name"),
        ("symbol", "Symbol"),
        ("share_class_figi", "FIGI"),
        ("isin", "ISIN"),
        ("cusip", "CUSIP"),
        ("cik", "CIK"),
        ("lei", "LEI"),
    )
    items = [
        (label, _count_populated(equities, field, "identity"), total)
        for field, label in identity_fields
    ]
    lines = format_coverage_table(items)
    return (Finding("Identity field coverage summary.", lines),)


def top_complete_profiles(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Score equities by completeness and return top samples.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings listing the most complete profiles.
    """
    scored = [(score_equity_completeness(equity), equity) for equity in equities]
    if not scored:
        return ()

    ranked = sorted(scored, key=lambda item: item[0], reverse=True)

    sample_lines = (
        f"Score {score:02} -> {format_equity(equity)}"
        for score, equity in islice(ranked, settings.finding_sample_limit)
    )
    return (
        Finding(
            "Most complete equity profiles.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def valuation_coverage(
    equities: Sequence[CanonicalEquity],
) -> tuple[Finding, ...]:
    """
    Provide coverage stats for core financial metrics.

    Args:
        equities: Sequence of canonical equities to assess.

    Returns:
        tuple[Finding, ...]: Findings describing financial metric coverage.
    """
    total = len(equities)
    if total == 0:
        return ()

    field_labels = (
        ("mics", "MICs"),
        ("currency", "Currency"),
        ("last_price", "Last price"),
        ("market_cap", "Market cap"),
        ("fifty_two_week_min", "52-week low"),
        ("fifty_two_week_max", "52-week high"),
        ("dividend_yield", "Dividend yield"),
        ("market_volume", "Market volume"),
        ("held_insiders", "Held by insiders"),
        ("held_institutions", "Held by institutions"),
        ("short_interest", "Short interest"),
        ("share_float", "Share float"),
        ("shares_outstanding", "Shares outstanding"),
        ("revenue_per_share", "Revenue per share"),
        ("profit_margin", "Profit margin"),
        ("gross_margin", "Gross margin"),
        ("operating_margin", "Operating margin"),
        ("free_cash_flow", "Free cash flow"),
        ("operating_cash_flow", "Operating cash flow"),
        ("return_on_equity", "Return on equity"),
        ("return_on_assets", "Return on assets"),
        ("performance_1_year", "1-year performance"),
        ("total_debt", "Total debt"),
        ("revenue", "Revenue"),
        ("ebitda", "EBITDA"),
        ("trailing_pe", "P/E ratio"),
        ("price_to_book", "Price to book"),
        ("trailing_eps", "Trailing EPS"),
        ("analyst_rating", "Analyst rating"),
        ("industry", "Industry"),
        ("sector", "Sector"),
    )

    items = [
        (label, _count_populated(equities, field, "financials"), total)
        for field, label in field_labels
    ]

    lines = format_coverage_table(items)
    return (Finding("Financial metric coverage summary.", lines),)


def _count_populated(
    equities: Sequence[CanonicalEquity],
    field: str,
    group: str,
) -> int:
    """
    Count how many equities have a non-None value for a given field.

    Returns:
        int: Number of equities where the field is populated.
    """
    return sum(
        1 for equity in equities if getattr(getattr(equity, group), field) is not None
    )
