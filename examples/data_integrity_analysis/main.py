# data_integrity_analysis/main.py

"""
Data Integrity Analysis for Equity Aggregator.

This module provides comprehensive data quality analysis for canonical equity
records. It examines multiple dimensions including:

- Financial metric outliers (P/E ratios, market capitalisation extremes)
- Temporal anomalies (price range inversions, stale data)
- Extreme financial values (dividends, penny stocks, profit margins)
- Data consistency (symbol patterns, duplicate names, currency coverage)
- Identifier quality (FIGI, ISIN, CUSIP, CIK, LEI completeness and formats)
- Cross-field logic (price/cap relationships, partial ranges)
- Data completeness (identity and financial field coverage)
- Currency and geography distribution

Usage:
    python -m examples.data_integrity_analysis
"""

from collections.abc import Sequence

from equity_aggregator import CanonicalEquity
from equity_aggregator.storage import load_canonical_equities

from .analyzers import (
    analyse_cross_field_logic,
    analyse_currency_and_geography,
    analyse_data_consistency,
    analyse_data_quality,
    analyse_financial_outliers,
    analyse_identifier_quality,
    build_dataset_overview,
    detect_extreme_financial_values,
    detect_temporal_anomalies,
)
from .models import AnalysisSettings, SectionReport, default_settings
from .rendering import print_summary_header, render_reports


def run_analysis(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings | None = None,
) -> tuple[SectionReport, ...]:
    """
    Execute complete data integrity analysis suite.

    Args:
        equities: Equities to analyse.
        settings: Analysis thresholds (defaults to standard settings).

    Returns:
        Ordered section reports with findings.
    """
    active_settings = settings or default_settings()

    return (
        build_dataset_overview(equities),
        analyse_financial_outliers(equities, active_settings),
        detect_temporal_anomalies(equities, active_settings),
        detect_extreme_financial_values(equities, active_settings),
        analyse_data_consistency(equities, active_settings),
        analyse_identifier_quality(equities, active_settings),
        analyse_cross_field_logic(equities, active_settings),
        analyse_data_quality(equities, active_settings),
        analyse_currency_and_geography(equities),
    )


def main() -> None:
    """
    Run data integrity analysis on stored canonical equities.

    Loads equities from local database and generates comprehensive
    analysis report covering multiple data quality dimensions.
    """
    try:
        equities = load_canonical_equities(refresh_fn=None)
    except Exception as exc:  # pragma: no cover - defensive for CLI usage
        print(f"\n✗ Failed to load canonical equities: {exc}")
        print(
            "  Ensure the local database exists at"
            " ~/Library/Application Support/equity-aggregator/data_store.db\n",
        )
        return

    if not equities:
        print("\n✗ No canonical equities were loaded; aborting analysis.")
        print("  Run 'equity-aggregator download' to populate the data store.\n")
        return

    reports = run_analysis(equities, default_settings())
    print_summary_header(len(equities), reports)
    render_reports(reports)
    print("\n" + "=" * 80)
    print("  Analysis complete.")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
