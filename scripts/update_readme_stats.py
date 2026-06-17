#!/usr/bin/env python3
"""
Derive presentation statistics for the README and badges from the canonical
equity database.

Computes the canonical equity count (emitted to the GitHub Actions step output
so the badge can render it) and the population coverage of each identity field
(used to rewrite the README Identity Metadata table between marker comments).

Usage:
    python3 scripts/update_readme_stats.py <database_path> <readme_path>
"""

import os
import re
import sqlite3
import sys
from pathlib import Path

IDENTITIES_TABLE = "canonical_equity_identities"

COVERAGE_START = "<!-- COVERAGE:START -->"
COVERAGE_END = "<!-- COVERAGE:END -->"

# (label, description, payload field). A payload field of None marks a required
# column that is always present, so its coverage is reported as 100%.
IDENTITY_FIELDS = (
    ("name", "Full company name", None),
    ("symbol", "Trading symbol", None),
    ("share class figi", "Definitive OpenFIGI identifier", None),
    ("isin", "International Securities Identification Number", "isin"),
    ("cusip", "CUSIP identifier", "cusip"),
    ("cik", "Central Index Key for SEC filings", "cik"),
    ("lei", "Legal Entity Identifier (ISO 17442)", "lei"),
)


def main(argv: list[str]) -> None:
    """
    Update the README coverage table and emit the equity count for the badge.

    Args:
        argv (list[str]): Command-line arguments; argv[1] is the database path
            and argv[2] is the README path.

    Returns:
        None
    """
    database_path, readme_path = argv[1], argv[2]
    with sqlite3.connect(database_path) as connection:
        count = _equity_count(connection)
        table = _render_coverage_table(connection)
    _emit_count(count)
    _rewrite_coverage_table(Path(readme_path), table)


def _equity_count(connection: sqlite3.Connection) -> int:
    """
    Count the canonical equities in the database.

    Returns:
        int: The total number of canonical equity identities.
    """
    (count,) = connection.execute(
        f"SELECT COUNT(*) FROM {IDENTITIES_TABLE}",
    ).fetchone()
    return count


def _render_coverage_table(connection: sqlite3.Connection) -> str:
    """
    Build the Markdown identity coverage table from live field coverage.

    Returns:
        str: The complete Markdown table, header and rows, without markers.
    """
    header = (
        "| Field | Description | Populated |",
        "|-------|-------------|-----------|",
    )
    rows = tuple(_render_row(connection, field) for field in IDENTITY_FIELDS)
    return "\n".join((*header, *rows))


def _render_row(
    connection: sqlite3.Connection,
    field: tuple[str, str, str | None],
) -> str:
    """
    Render a single Markdown table row for an identity field.

    Returns:
        str: The Markdown table row, including its populated percentage.
    """
    label, description, payload_field = field
    if payload_field is None:
        populated = "100%"
    else:
        populated = f"{_coverage(connection, payload_field)}%"
    return f"| **{label}** | {description} | {populated} |"


def _coverage(connection: sqlite3.Connection, payload_field: str) -> int:
    """
    Compute the percentage of identities with a non-null payload field.

    Returns:
        int: The coverage percentage, rounded to the nearest integer.
    """
    (percentage,) = connection.execute(
        f"SELECT ROUND(100.0 * AVG("
        f"json_extract(payload, '$.{payload_field}') IS NOT NULL)) "
        f"FROM {IDENTITIES_TABLE}",
    ).fetchone()
    return int(percentage or 0)


def _emit_count(count: int) -> None:
    """
    Emit the formatted equity count to the GitHub Actions step output.

    Note:
        Falls back to standard output when not running inside GitHub Actions.

    Returns:
        None
    """
    formatted = f"{count:,}"
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        print(formatted)
        return
    with open(output_path, "a", encoding="utf-8") as output:
        output.write(f"count={formatted}\n")


def _rewrite_coverage_table(readme_path: Path, table: str) -> None:
    """
    Replace the README content between the coverage markers with the table.

    Returns:
        None
    """
    text = readme_path.read_text(encoding="utf-8")
    pattern = re.compile(
        rf"({re.escape(COVERAGE_START)}\n).*?(\n{re.escape(COVERAGE_END)})",
        flags=re.DOTALL,
    )
    updated = pattern.sub(lambda match: match.group(1) + table + match.group(2), text)
    readme_path.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv)
