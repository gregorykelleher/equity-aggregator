# integrity/analyse.py

import logging

from equity_aggregator.schemas.integrity import IntegrityReport
from equity_aggregator.storage.data_store import (
    count_snapshots,
    load_canonical_equities,
)

from .analysers import (
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
from .models import AnalysisSettings, default_settings
from .report import build_integrity_report, save_integrity_report

logger = logging.getLogger(__name__)


def analyse_canonical_equities(
    settings: AnalysisSettings | None = None,
) -> IntegrityReport:
    """
    Run the complete data integrity analysis on stored canonical equities.

    Loads equities from the database, executes all analysis sections,
    builds a structured report, and persists it as JSON.

    Args:
        settings: Optional analysis thresholds (defaults to standard settings).

    Returns:
        IntegrityReport: The completed integrity analysis report.
    """
    equities = load_canonical_equities(refresh_fn=None)
    snapshot_count = count_snapshots()

    reports = _run_analysis(equities, settings)

    report = build_integrity_report(
        reports,
        dataset_size=len(equities),
        snapshot_count=snapshot_count,
    )

    save_integrity_report(report)

    logger.info(
        "Integrity analysis complete: %d findings across %d sections",
        report.total_findings,
        report.sections_analysed,
    )

    return report


def _run_analysis(
    equities: list,
    settings: AnalysisSettings | None = None,
) -> tuple:
    """
    Execute the complete analysis suite against a set of equities.

    Args:
        equities: Canonical equities to analyse.
        settings: Optional analysis thresholds.

    Returns:
        tuple[SectionReport, ...]: Ordered section reports.
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
