# schemas/integrity.py

from pydantic import BaseModel, ConfigDict


class FindingOutput(BaseModel):
    """
    A single insight or anomaly detected during integrity analysis.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    message: str
    highlights: tuple[str, ...]


class SectionOutput(BaseModel):
    """
    Structured results for one analysis section.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    title: str
    findings: tuple[FindingOutput, ...]


class IntegrityReport(BaseModel):
    """
    Machine-readable envelope for a complete data integrity analysis.

    Contains metadata about the dataset and the full set of section results,
    serialisable as JSON for downstream consumers.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    generated_at: str
    dataset_size: int
    snapshot_count: int
    sections_analysed: int
    sections_with_findings: int
    total_findings: int
    sections: tuple[SectionOutput, ...]
