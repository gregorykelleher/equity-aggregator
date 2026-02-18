# integrity/report.py

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from equity_aggregator.schemas.integrity import (
    FindingOutput,
    IntegrityReport,
    SectionOutput,
)
from equity_aggregator.storage._utils import get_data_store_path

from .models import SectionReport

logger = logging.getLogger(__name__)


def build_integrity_report(
    reports: tuple[SectionReport, ...],
    dataset_size: int,
    snapshot_count: int,
) -> IntegrityReport:
    """
    Convert internal dataclass reports into a Pydantic IntegrityReport.

    Args:
        reports: Analysis section reports from the analysers.
        dataset_size: Number of equities in the dataset.
        snapshot_count: Number of distinct snapshot dates.

    Returns:
        IntegrityReport: Machine-readable report envelope.
    """
    sections = tuple(_convert_section(report) for report in reports)
    total_findings = sum(len(section.findings) for section in sections)
    sections_with_findings = sum(1 for section in sections if section.findings)

    return IntegrityReport(
        generated_at=datetime.now(UTC).isoformat(),
        dataset_size=dataset_size,
        snapshot_count=snapshot_count,
        sections_analysed=len(sections),
        sections_with_findings=sections_with_findings,
        total_findings=total_findings,
        sections=sections,
    )


def save_integrity_report(report: IntegrityReport) -> Path:
    """
    Write the integrity report as JSON to the data store directory.

    Args:
        report: The integrity report to persist.

    Returns:
        Path: Path to the written JSON file.
    """
    dest = get_data_store_path() / "integrity_report.json"
    dest.parent.mkdir(parents=True, exist_ok=True)

    dest.write_text(
        json.dumps(
            report.model_dump(),
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
    )

    logger.info("Integrity report saved to %s", dest)
    return dest


def _convert_section(report: SectionReport) -> SectionOutput:
    """
    Convert an internal SectionReport dataclass to a Pydantic SectionOutput.

    Args:
        report: Internal section report to convert.

    Returns:
        SectionOutput: Pydantic-serialisable section output.
    """
    findings = tuple(
        FindingOutput(
            message=finding.message,
            highlights=finding.highlights,
        )
        for finding in report.findings
    )
    return SectionOutput(title=report.title, findings=findings)
