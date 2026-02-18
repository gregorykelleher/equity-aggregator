# integrity/analysers/identifier_quality.py

from collections.abc import Sequence

from equity_aggregator.schemas import CanonicalEquity

from ..formatters import format_percentage
from ..models import AnalysisSettings, Finding, SectionReport
from ._helpers import build_format_finding


def analyse_identifier_quality(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Summarise identifier completeness and format validity.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined identifier quality findings.
    """
    coverage = missing_identifier_counts(equities, settings)
    validity = validate_identifier_formats(equities, settings)
    findings = coverage + validity
    return SectionReport("Identifier Quality", findings)


def missing_identifier_counts(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Quantify coverage for the core identifiers.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing identifier completeness.
    """
    total = len(equities)
    if total == 0:
        return ()

    identifier_fields = (
        ("share_class_figi", "FIGI"),
        ("isin", "ISIN"),
        ("cusip", "CUSIP"),
        ("cik", "CIK"),
        ("lei", "LEI"),
    )

    lines = [
        _format_gap_line(label, _count_missing(equities, field), total, settings)
        for field, label in identifier_fields
    ]

    return (Finding("Identifier coverage review.", tuple(lines)),)


def validate_identifier_formats(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Validate identifier format adherence across all equity records.

    Checks ISIN length, CUSIP length, CIK numeric format, and LEI length
    against expected standards, returning findings for any violations.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing identifier format concerns.
    """
    validations = (
        _validate_isin_length(equities, settings),
        _validate_cusip_length(equities, settings),
        _validate_cik_numeric(equities, settings),
        _validate_lei_length(equities, settings),
    )
    return tuple(finding for finding in validations if finding)


def _count_missing(
    equities: Sequence[CanonicalEquity],
    field: str,
) -> int:
    """
    Count how many equities lack a value for a given identity field.

    Returns:
        int: Number of equities where the field is absent.
    """
    return sum(1 for equity in equities if not getattr(equity.identity, field))


def _format_gap_line(
    label: str,
    count: int,
    total: int,
    settings: AnalysisSettings,
) -> str:
    """
    Format a single identifier gap summary line.

    Returns:
        str: A line describing the gap count and percentage.
    """
    percentage = (count / total) * 100 if total else 0.0
    prefix = "High gap" if percentage > settings.identifier_gap_alert else "Gap"
    return f"{prefix} for {label}: {count:,} entries ({format_percentage(percentage)})."


def _validate_isin_length(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> Finding | None:
    """
    Check ISIN identifiers for correct length.

    Returns:
        Finding | None: Finding if invalid ISINs exist, None otherwise.
    """
    invalid = [
        equity
        for equity in equities
        if _has_wrong_length_isin(equity, settings.isin_length)
    ]

    return build_format_finding(
        invalid,
        f"Unexpected ISIN length for {len(invalid):,} equities.",
        settings.finding_sample_limit,
    )


def _validate_cusip_length(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> Finding | None:
    """
    Check CUSIP identifiers for correct length.

    Returns:
        Finding | None: Finding if invalid CUSIPs exist, None otherwise.
    """
    invalid = [
        equity
        for equity in equities
        if _has_wrong_length_cusip(equity, settings.cusip_length)
    ]

    return build_format_finding(
        invalid,
        f"Unexpected CUSIP length for {len(invalid):,} equities.",
        settings.finding_sample_limit,
    )


def _validate_cik_numeric(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> Finding | None:
    """
    Check CIK identifiers contain only numeric characters.

    Returns:
        Finding | None: Finding if non-numeric CIKs exist, None otherwise.
    """
    invalid = [equity for equity in equities if _has_non_numeric_cik(equity)]

    return build_format_finding(
        invalid,
        f"Non-numeric CIK values for {len(invalid):,} equities.",
        settings.finding_sample_limit,
    )


def _validate_lei_length(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> Finding | None:
    """
    Check LEI identifiers for correct length.

    Returns:
        Finding | None: Finding if invalid LEIs exist, None otherwise.
    """
    invalid = [
        equity
        for equity in equities
        if _has_wrong_length_lei(equity, settings.lei_length)
    ]

    return build_format_finding(
        invalid,
        f"Unexpected LEI length for {len(invalid):,} equities.",
        settings.finding_sample_limit,
    )


def _has_wrong_length_isin(
    equity: CanonicalEquity,
    expected: int,
) -> bool:
    """
    Check whether the equity's ISIN has an unexpected length.

    Returns:
        bool: True when ISIN is present but not the expected length.
    """
    isin = equity.identity.isin
    return isin is not None and len(isin) != expected


def _has_wrong_length_cusip(
    equity: CanonicalEquity,
    expected: int,
) -> bool:
    """
    Check whether the equity's CUSIP has an unexpected length.

    Returns:
        bool: True when CUSIP is present but not the expected length.
    """
    cusip = equity.identity.cusip
    return cusip is not None and len(cusip) != expected


def _has_non_numeric_cik(equity: CanonicalEquity) -> bool:
    """
    Check whether the equity's CIK contains non-numeric characters.

    Returns:
        bool: True when CIK is present and not purely numeric.
    """
    cik = equity.identity.cik
    return cik is not None and not cik.isdigit()


def _has_wrong_length_lei(
    equity: CanonicalEquity,
    expected: int,
) -> bool:
    """
    Check whether the equity's LEI has an unexpected length.

    Returns:
        bool: True when LEI is present but not the expected length.
    """
    lei = equity.identity.lei
    return lei is not None and len(lei) != expected
