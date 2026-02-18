# integrity/analysers/dataset_overview.py

from collections.abc import Sequence

from equity_aggregator.schemas import CanonicalEquity

from ..models import Finding, SectionReport


def build_dataset_overview(equities: Sequence[CanonicalEquity]) -> SectionReport:
    """
    Summarise dataset size and diversity.

    Args:
        equities: Canonical equities to analyse.

    Returns:
        SectionReport: Section report with overview findings.
    """
    if not equities:
        return SectionReport(
            "Dataset Overview",
            (Finding("No equities available for analysis."),),
        )

    sectors = {eq.financials.sector for eq in equities if eq.financials.sector}
    currencies = {eq.financials.currency for eq in equities if eq.financials.currency}

    message = f"Loaded {len(equities):,} canonical equities."
    highlights = (
        f"Distinct sectors: {len(sectors):,}",
        f"Distinct currencies: {len(currencies):,}",
    )
    return SectionReport("Dataset Overview", (Finding(message, highlights),))
