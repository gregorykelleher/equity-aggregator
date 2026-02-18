# domain/integrity/test_report.py

import json
import os

import pytest

from equity_aggregator.domain.integrity.models import Finding, SectionReport
from equity_aggregator.domain.integrity.report import (
    build_integrity_report,
    save_integrity_report,
)

pytestmark = pytest.mark.unit


def _sample_reports() -> tuple[SectionReport, ...]:
    """
    Build a minimal set of section reports for testing.

    Returns:
        tuple[SectionReport, ...]: Two section reports with sample findings.
    """
    return (
        SectionReport(
            title="Overview",
            findings=(Finding(message="Loaded 10 equities.", highlights=("a",)),),
        ),
        SectionReport(
            title="Empty Section",
            findings=(),
        ),
    )


def test_build_integrity_report_sets_dataset_size() -> None:
    """
    ARRANGE: sample reports with dataset_size=10
    ACT:     build_integrity_report
    ASSERT:  report.dataset_size == 10
    """
    expected = 10

    actual = build_integrity_report(_sample_reports(), dataset_size=expected, snapshot_count=1)

    assert actual.dataset_size == expected


def test_build_integrity_report_sets_snapshot_count() -> None:
    """
    ARRANGE: sample reports with snapshot_count=5
    ACT:     build_integrity_report
    ASSERT:  report.snapshot_count == 5
    """
    expected = 5

    actual = build_integrity_report(_sample_reports(), dataset_size=10, snapshot_count=expected)

    assert actual.snapshot_count == expected


def test_build_integrity_report_counts_sections() -> None:
    """
    ARRANGE: two section reports
    ACT:     build_integrity_report
    ASSERT:  sections_analysed == 2
    """
    expected = 2

    actual = build_integrity_report(_sample_reports(), dataset_size=10, snapshot_count=1)

    assert actual.sections_analysed == expected


def test_build_integrity_report_counts_sections_with_findings() -> None:
    """
    ARRANGE: one section with findings, one empty
    ACT:     build_integrity_report
    ASSERT:  sections_with_findings == 1
    """
    expected = 1

    actual = build_integrity_report(_sample_reports(), dataset_size=10, snapshot_count=1)

    assert actual.sections_with_findings == expected


def test_build_integrity_report_counts_total_findings() -> None:
    """
    ARRANGE: one finding across reports
    ACT:     build_integrity_report
    ASSERT:  total_findings == 1
    """
    expected = 1

    actual = build_integrity_report(_sample_reports(), dataset_size=10, snapshot_count=1)

    assert actual.total_findings == expected


def test_build_integrity_report_includes_generated_at() -> None:
    """
    ARRANGE: sample reports
    ACT:     build_integrity_report
    ASSERT:  generated_at is a non-empty string
    """
    actual = build_integrity_report(_sample_reports(), dataset_size=10, snapshot_count=1)

    assert len(actual.generated_at) > 0


def test_build_integrity_report_converts_findings() -> None:
    """
    ARRANGE: section with one finding
    ACT:     build_integrity_report
    ASSERT:  first section's first finding message matches
    """
    expected = "Loaded 10 equities."

    actual = build_integrity_report(_sample_reports(), dataset_size=10, snapshot_count=1)

    assert actual.sections[0].findings[0].message == expected


def test_save_integrity_report_writes_json_file() -> None:
    """
    ARRANGE: built integrity report
    ACT:     save_integrity_report
    ASSERT:  JSON file exists at expected path
    """
    report = build_integrity_report(_sample_reports(), dataset_size=10, snapshot_count=1)

    path = save_integrity_report(report)

    assert path.exists()


def test_save_integrity_report_contains_valid_json() -> None:
    """
    ARRANGE: built integrity report
    ACT:     save_integrity_report and read file
    ASSERT:  file parses as valid JSON with expected key
    """
    report = build_integrity_report(_sample_reports(), dataset_size=10, snapshot_count=1)

    path = save_integrity_report(report)
    data = json.loads(path.read_text())

    assert data["dataset_size"] == 10


def test_save_integrity_report_path_is_in_data_store() -> None:
    """
    ARRANGE: built integrity report
    ACT:     save_integrity_report
    ASSERT:  file is inside the DATA_STORE_DIR
    """
    report = build_integrity_report(_sample_reports(), dataset_size=10, snapshot_count=1)

    path = save_integrity_report(report)
    expected_dir = os.environ["DATA_STORE_DIR"]

    assert expected_dir in str(path)
