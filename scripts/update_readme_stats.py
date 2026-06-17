#!/usr/bin/env python3
"""
Derive presentation statistics for the README and badges from the canonical
equity database.

Computes the canonical equity count (emitted to the GitHub Actions step output
so the badge can render it), the population coverage of each identity field, and
an "at a glance" summary of scale, composition, market capitalisation, and
internal consistency. The coverage table and the summary are written back into
the README between marker comments.

Usage:
    python3 scripts/update_readme_stats.py <database_path> <readme_path>
"""

import os
import re
import sqlite3
import sys
from pathlib import Path

IDENTITIES_TABLE = "canonical_equity_identities"
SNAPSHOTS_TABLE = "canonical_equity_snapshots"

COVERAGE_START = "<!-- COVERAGE:START -->"
COVERAGE_END = "<!-- COVERAGE:END -->"
STATS_START = "<!-- STATS:START -->"
STATS_END = "<!-- STATS:END -->"
CAPDIST_START = "<!-- CAPDIST:START -->"
CAPDIST_END = "<!-- CAPDIST:END -->"

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

# (label, lower bound inclusive, upper bound exclusive) for market cap tiers.
# An upper bound of None means unbounded above.
CAP_TIERS = (
    ("Mega (> $200B)", 200e9, None),
    ("Large ($10B–$200B)", 10e9, 200e9),
    ("Mid ($2B–$10B)", 2e9, 10e9),
    ("Small ($300M–$2B)", 300e6, 2e9),
    ("Micro (< $300M)", 0, 300e6),
)


def main(argv: list[str]) -> None:
    """
    Refresh the README statistics and emit the equity count for the badge.

    Args:
        argv (list[str]): Command-line arguments; argv[1] is the database path
            and argv[2] is the README path.

    Returns:
        None
    """
    database_path, readme_path = argv[1], argv[2]
    with sqlite3.connect(database_path) as connection:
        latest = _latest_snapshot_date(connection)
        count = _equity_count(connection)
        blocks = {
            (COVERAGE_START, COVERAGE_END): _render_coverage_table(connection),
            (STATS_START, STATS_END): _render_key_figures(connection, latest, count),
            (CAPDIST_START, CAPDIST_END): _render_cap_distribution(connection, latest),
        }
    _emit_count(count)
    _rewrite_readme(Path(readme_path), blocks)


def _rewrite_readme(readme_path: Path, blocks: dict[tuple[str, str], str]) -> None:
    """
    Write each generated block back into the README between its markers.

    Returns:
        None
    """
    text = readme_path.read_text(encoding="utf-8")
    for (start, end), content in blocks.items():
        text = _replace_block(text, start, end, content)
    readme_path.write_text(text, encoding="utf-8")


def _replace_block(text: str, start: str, end: str, content: str) -> str:
    """
    Replace the text between a pair of marker comments with new content.

    Returns:
        str: The text with the marked region's body replaced.
    """
    pattern = re.compile(
        rf"({re.escape(start)}\n).*?(\n{re.escape(end)})",
        flags=re.DOTALL,
    )
    return pattern.sub(lambda match: match.group(1) + content + match.group(2), text)


# ──────────────────────────── Identity coverage ────────────────────────────


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
    rows = tuple(_render_coverage_row(connection, field) for field in IDENTITY_FIELDS)
    return "\n".join((*header, *rows))


def _render_coverage_row(
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
        populated = f"{_identity_coverage(connection, payload_field)}%"
    return f"| {label} | {description} | {populated} |"


def _identity_coverage(connection: sqlite3.Connection, payload_field: str) -> int:
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


# ────────────────────────── Equity data summary ───────────────────────────


def _render_key_figures(
    connection: sqlite3.Connection,
    latest: str,
    count: int,
) -> str:
    """
    Build the headline key-figures table of scale, composition, and integrity.

    Returns:
        str: The Markdown key-figures table.
    """
    sectors = _distinct(connection, latest, "sector")
    industries = _distinct(connection, latest, "industry")
    venues = _distinct_mics(connection, latest)
    snapshots = _scalar(
        connection,
        f"SELECT COUNT(DISTINCT snapshot_date) FROM {SNAPSHOTS_TABLE}",
    )
    earliest = _scalar(connection, f"SELECT MIN(snapshot_date) FROM {SNAPSHOTS_TABLE}")
    total = _humanise_usd(_market_cap_aggregate(connection, latest, "SUM"))
    largest = _humanise_usd(_market_cap_aggregate(connection, latest, "MAX"))
    median = _humanise_usd(_market_cap_median(connection, latest))
    in_range = _coherence_in_52_week_range(connection, latest)
    cap_consistent = _coherence_cap_versus_price(connection, latest)
    rows = (
        "| Metric | Value |",
        "|--------|------:|",
        f"| Canonical equities | {count:,} |",
        f"| Sectors | {sectors} |",
        f"| Industries | {industries} |",
        f"| Listing venues (MICs) | {venues} |",
        f"| Daily snapshots | {snapshots} |",
        f"| History since | {earliest} |",
        f"| Aggregate market cap | {total} |",
        f"| Largest market cap | {largest} |",
        f"| Median market cap | {median} |",
        f"| Price within 52-week range | {in_range}% |",
        f"| Market cap within 25% of price × shares | {cap_consistent}% |",
    )
    return "\n".join(rows)


def _render_cap_distribution(connection: sqlite3.Connection, latest: str) -> str:
    """
    Build the market capitalisation tier distribution table.

    Returns:
        str: The Markdown distribution table.
    """
    market_cap = _real("market_cap")
    rows = ["| Cap tier | Canonical Equities |", "|----------|---------:|"]
    for label, lower, upper in CAP_TIERS:
        tier_count = _cap_tier_count(connection, latest, market_cap, lower, upper)
        rows.append(f"| {label} | {tier_count:,} |")
    return "\n".join(rows)


def _cap_tier_count(
    connection: sqlite3.Connection,
    latest: str,
    market_cap: str,
    lower: float,
    upper: float | None,
) -> int:
    """
    Count equities whose market cap falls within a tier's bounds.

    Returns:
        int: The number of equities in the tier on the latest snapshot.
    """
    if upper is None:
        query = (
            f"SELECT COUNT(*) FROM {SNAPSHOTS_TABLE} "
            f"WHERE snapshot_date = ? AND {market_cap} >= ?"
        )
        return _scalar(connection, query, (latest, lower))
    query = (
        f"SELECT COUNT(*) FROM {SNAPSHOTS_TABLE} "
        f"WHERE snapshot_date = ? AND {market_cap} >= ? AND {market_cap} < ?"
    )
    return _scalar(connection, query, (latest, lower, upper))


# ──────────────────────────── Summary helpers ─────────────────────────────


def _distinct(connection: sqlite3.Connection, latest: str, field: str) -> int:
    """
    Count distinct non-null values of a snapshot field on the latest snapshot.

    Returns:
        int: The number of distinct values.
    """
    query = (
        f"SELECT COUNT(DISTINCT json_extract(payload, '$.{field}')) "
        f"FROM {SNAPSHOTS_TABLE} WHERE snapshot_date = ?"
    )
    return _scalar(connection, query, (latest,))


def _distinct_mics(connection: sqlite3.Connection, latest: str) -> int:
    """
    Count distinct Market Identifier Codes across the latest snapshot.

    Returns:
        int: The number of distinct MICs represented in the dataset.
    """
    query = (
        f"SELECT COUNT(DISTINCT entry.value) "
        f"FROM {SNAPSHOTS_TABLE} snapshot, "
        f"json_each(json_extract(snapshot.payload, '$.mics')) entry "
        f"WHERE snapshot.snapshot_date = ?"
    )
    return _scalar(connection, query, (latest,))


def _market_cap_aggregate(
    connection: sqlite3.Connection,
    latest: str,
    function: str,
) -> float:
    """
    Aggregate market capitalisation on the latest snapshot.

    Returns:
        float: The aggregated market cap, or 0 when none is present.
    """
    market_cap = _real("market_cap")
    query = (
        f"SELECT {function}({market_cap}) FROM {SNAPSHOTS_TABLE} "
        f"WHERE snapshot_date = ? AND {market_cap} IS NOT NULL"
    )
    return _scalar(connection, query, (latest,)) or 0


def _market_cap_median(connection: sqlite3.Connection, latest: str) -> float:
    """
    Compute the median market capitalisation on the latest snapshot.

    Returns:
        float: The median market cap, or 0 when none is present.
    """
    market_cap = _real("market_cap")
    total = _scalar(
        connection,
        f"SELECT COUNT(*) FROM {SNAPSHOTS_TABLE} "
        f"WHERE snapshot_date = ? AND {market_cap} IS NOT NULL",
        (latest,),
    )
    if not total:
        return 0
    query = (
        f"SELECT {market_cap} FROM {SNAPSHOTS_TABLE} "
        f"WHERE snapshot_date = ? AND {market_cap} IS NOT NULL "
        f"ORDER BY {market_cap} LIMIT 1 OFFSET ?"
    )
    return _scalar(connection, query, (latest, total // 2))


def _coherence_in_52_week_range(connection: sqlite3.Connection, latest: str) -> int:
    """
    Percentage of priced equities whose price sits within their 52-week range.

    Returns:
        int: The coherent share, rounded to the nearest integer.
    """
    price = _real("last_price")
    low = _real("fifty_two_week_min")
    high = _real("fifty_two_week_max")
    return _ratio(
        connection,
        latest,
        predicate=f"{low} <= {price} AND {price} <= {high}",
        required=("last_price", "fifty_two_week_min", "fifty_two_week_max"),
    )


def _coherence_cap_versus_price(connection: sqlite3.Connection, latest: str) -> int:
    """
    Percentage of equities whose market cap reconciles with price × shares.

    Note:
        Reconciliation is within a 25% tolerance of the reported market cap.

    Returns:
        int: The reconciling share, rounded to the nearest integer.
    """
    market_cap = _real("market_cap")
    implied = f"{_real('last_price')} * {_real('shares_outstanding')}"
    return _ratio(
        connection,
        latest,
        predicate=f"ABS({market_cap} - {implied}) <= 0.25 * {market_cap}",
        required=("market_cap", "last_price", "shares_outstanding"),
    )


def _ratio(
    connection: sqlite3.Connection,
    latest: str,
    predicate: str,
    required: tuple[str, ...],
) -> int:
    """
    Percentage of latest-snapshot rows satisfying a predicate, over rows where
    every required field is present.

    Returns:
        int: The satisfying share, rounded to the nearest integer.
    """
    present = " AND ".join(
        f"json_extract(payload, '$.{field}') IS NOT NULL" for field in required
    )
    query = (
        f"SELECT ROUND(100.0 * AVG({predicate})) FROM {SNAPSHOTS_TABLE} "
        f"WHERE snapshot_date = ? AND {present}"
    )
    return int(_scalar(connection, query, (latest,)) or 0)


def _real(field: str) -> str:
    """
    Build a SQL expression casting a snapshot payload field to a real number.

    Returns:
        str: The CAST expression for use within a query.
    """
    return f"CAST(json_extract(payload, '$.{field}') AS REAL)"


def _humanise_usd(value: float) -> str:
    """
    Format a USD amount with a trillion, billion, or million suffix.

    Returns:
        str: The human-readable currency string.
    """
    scales = (("T", 1e12, 2), ("B", 1e9, 2), ("M", 1e6, 0))
    for suffix, threshold, precision in scales:
        if value >= threshold:
            return f"${value / threshold:.{precision}f}{suffix}"
    return f"${value:,.0f}"


# ──────────────────────────── Shared helpers ──────────────────────────────


def _latest_snapshot_date(connection: sqlite3.Connection) -> str:
    """
    Find the most recent snapshot date in the database.

    Returns:
        str: The latest snapshot date in YYYY-MM-DD format.
    """
    return _scalar(connection, f"SELECT MAX(snapshot_date) FROM {SNAPSHOTS_TABLE}")


def _equity_count(connection: sqlite3.Connection) -> int:
    """
    Count the canonical equities in the database.

    Returns:
        int: The total number of canonical equity identities.
    """
    return _scalar(connection, f"SELECT COUNT(*) FROM {IDENTITIES_TABLE}")


def _scalar(
    connection: sqlite3.Connection,
    query: str,
    parameters: tuple = (),
) -> object:
    """
    Execute a query expected to return a single scalar value.

    Returns:
        object: The first column of the first row.
    """
    (value,) = connection.execute(query, parameters).fetchone()
    return value


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


if __name__ == "__main__":
    main(sys.argv)
