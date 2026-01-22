# data_integrity_analysis/rendering.py

from collections.abc import Sequence

from .models import Finding, SectionReport


def print_separator(title: str) -> None:
    """
    Print formatted section separator.

    Args:
        title: Section heading.
    """
    print()
    print()
    print("─" * 80)
    print(f"  {title.upper()}")
    print("─" * 80)


def render_finding(finding: Finding, *, add_spacing: bool) -> None:
    """
    Render finding with message and highlights.

    Args:
        finding: Finding to render.
        add_spacing: Whether to add spacing before finding.
    """
    if add_spacing:
        print()
    print(f"\n  • {finding.message}")
    if finding.highlights:
        print()
        for highlight in finding.highlights:
            print(f"      {highlight}")


def render_reports(reports: Sequence[SectionReport]) -> None:
    """
    Render analysis reports to stdout.

    Args:
        reports: Section reports to render.
    """
    for report in reports:
        print_separator(report.title)
        if not report.findings:
            print("\n  ✓ No notable findings in this section.\n")
            continue
        for idx, finding in enumerate(report.findings, 1):
            render_finding(finding, add_spacing=(idx > 1))


def print_summary_header(total_equities: int, reports: Sequence[SectionReport]) -> None:
    """
    Print analysis summary header.

    Args:
        total_equities: Total number of equities analysed.
        reports: Section reports for summary statistics.
    """
    total_findings = sum(len(report.findings) for report in reports)
    sections_with_findings = sum(1 for report in reports if report.findings)

    print("\n" + "=" * 80)
    print("  EQUITY AGGREGATOR DATA INTEGRITY ANALYSIS")
    print("=" * 80)
    print(f"\n  Dataset Size:          {total_equities:,} equities")
    print(f"  Sections Analysed:     {len(reports)}")
    print(f"  Sections with Issues:  {sections_with_findings}")
    print(f"  Total Findings:        {total_findings}")
    print()
