# schemas/test_integrity.py

import pytest
from pydantic import ValidationError

from equity_aggregator.schemas.integrity import (
    FindingOutput,
    IntegrityReport,
    SectionOutput,
)

pytestmark = pytest.mark.unit


def test_finding_output_accepts_valid_data() -> None:
    """
    ARRANGE: valid message and highlights
    ACT:     construct FindingOutput
    ASSERT:  message matches input
    """
    actual = FindingOutput(message="Test finding.", highlights=("a", "b"))

    assert actual.message == "Test finding."


def test_finding_output_highlights_are_tuple() -> None:
    """
    ARRANGE: valid FindingOutput
    ACT:     construct FindingOutput
    ASSERT:  highlights is a tuple
    """
    actual = FindingOutput(message="Test.", highlights=("x",))

    assert isinstance(actual.highlights, tuple)


def test_section_output_accepts_valid_data() -> None:
    """
    ARRANGE: valid title and findings
    ACT:     construct SectionOutput
    ASSERT:  title matches input
    """
    finding = FindingOutput(message="msg", highlights=())
    actual = SectionOutput(title="Test Section", findings=(finding,))

    assert actual.title == "Test Section"


def test_section_output_findings_count() -> None:
    """
    ARRANGE: section with two findings
    ACT:     construct SectionOutput
    ASSERT:  findings has length 2
    """
    expected = 2
    findings = (
        FindingOutput(message="a", highlights=()),
        FindingOutput(message="b", highlights=()),
    )
    actual = SectionOutput(title="S", findings=findings)

    assert len(actual.findings) == expected


def test_integrity_report_accepts_valid_data() -> None:
    """
    ARRANGE: complete valid report data
    ACT:     construct IntegrityReport
    ASSERT:  dataset_size matches input
    """
    expected_dataset_size = 100
    report = IntegrityReport(
        generated_at="2026-01-01T00:00:00+00:00",
        dataset_size=expected_dataset_size,
        snapshot_count=5,
        sections_analysed=9,
        sections_with_findings=3,
        total_findings=10,
        sections=(),
    )

    assert report.dataset_size == expected_dataset_size


def test_integrity_report_snapshot_count() -> None:
    """
    ARRANGE: report with snapshot_count=42
    ACT:     construct IntegrityReport
    ASSERT:  snapshot_count is 42
    """
    expected = 42
    report = IntegrityReport(
        generated_at="2026-01-01T00:00:00+00:00",
        dataset_size=100,
        snapshot_count=expected,
        sections_analysed=9,
        sections_with_findings=3,
        total_findings=10,
        sections=(),
    )

    assert report.snapshot_count == expected


def test_integrity_report_serialises_to_json() -> None:
    """
    ARRANGE: complete IntegrityReport
    ACT:     model_dump_json
    ASSERT:  produces valid JSON containing dataset_size
    """
    report = IntegrityReport(
        generated_at="2026-01-01T00:00:00+00:00",
        dataset_size=50,
        snapshot_count=3,
        sections_analysed=1,
        sections_with_findings=1,
        total_findings=1,
        sections=(
            SectionOutput(
                title="Overview",
                findings=(
                    FindingOutput(message="Found 50.", highlights=()),
                ),
            ),
        ),
    )

    actual = report.model_dump_json()

    assert '"dataset_size":50' in actual


def test_integrity_report_is_frozen() -> None:
    """
    ARRANGE: valid IntegrityReport
    ACT:     attempt to mutate dataset_size
    ASSERT:  raises ValidationError
    """
    report = IntegrityReport(
        generated_at="2026-01-01T00:00:00+00:00",
        dataset_size=10,
        snapshot_count=1,
        sections_analysed=0,
        sections_with_findings=0,
        total_findings=0,
        sections=(),
    )

    with pytest.raises(ValidationError):
        report.dataset_size = 99


def test_integrity_report_rejects_missing_field() -> None:
    """
    ARRANGE: incomplete data missing snapshot_count
    ACT:     construct IntegrityReport
    ASSERT:  raises ValidationError
    """
    with pytest.raises(ValidationError):
        IntegrityReport(
            generated_at="2026-01-01T00:00:00+00:00",
            dataset_size=10,
            sections_analysed=0,
            sections_with_findings=0,
            total_findings=0,
            sections=(),
        )
