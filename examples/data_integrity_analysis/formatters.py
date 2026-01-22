# data_integrity_analysis/formatters.py

from collections.abc import Iterable
from decimal import Decimal
from itertools import islice

from equity_aggregator import CanonicalEquity


def limit_items(items: Iterable[str], limit: int) -> tuple[str, ...]:
    """
    Limit items to specified maximum count.

    Args:
        items: Items to limit.
        limit: Maximum count.

    Returns:
        Limited items.
    """
    return tuple(islice(items, limit))


def format_equity(eq: CanonicalEquity) -> str:
    """
    Format equity as 'Name (SYMBOL)'.

    Args:
        eq: Canonical equity to format.

    Returns:
        Formatted equity string.
    """
    name = (eq.identity.name or "Unknown")[:40]
    symbol = eq.identity.symbol or "N/A"
    return f"{name} ({symbol})"


def format_equity_with_figi(eq: CanonicalEquity) -> str:
    """
    Format equity as 'Name (SYMBOL) [FIGI]'.

    Args:
        eq: Canonical equity to format.

    Returns:
        Formatted equity string with FIGI.
    """
    name = (eq.identity.name or "Unknown")[:40]
    symbol = eq.identity.symbol or "N/A"
    figi = eq.identity.share_class_figi or "No FIGI"
    return f"{name} ({symbol}) [{figi}]"


def format_currency(value: Decimal | float) -> str:
    """
    Format value as currency with thousands separators.

    Args:
        value: Monetary amount.

    Returns:
        Formatted currency string.
    """
    number = float(value)
    return f"${int(number):,}" if number.is_integer() else f"${number:,.2f}"


def format_percentage(value: float) -> str:
    """
    Format value as percentage with one decimal place.

    Args:
        value: Percentage value.

    Returns:
        Formatted percentage string.
    """
    return f"{value:.1f}%"


def format_coverage_table(items: list[tuple[str, int, int]]) -> tuple[str, ...]:
    """
    Format coverage data as aligned table rows.

    Args:
        items: List of (label, count, total) tuples.

    Returns:
        Formatted table rows.
    """
    if not items:
        return ()

    max_label = max(len(label) for label, _, _ in items)
    max_count = max(len(f"{count:,}") for _, count, _ in items)
    max_total = max(len(f"{total:,}") for _, _, total in items)

    return tuple(
        _format_coverage_row(label, count, total, (max_label, max_count, max_total))
        for label, count, total in items
    )


def _format_coverage_row(
    label: str,
    count: int,
    total: int,
    widths: tuple[int, int, int],
) -> str:
    """
    Format a single coverage table row.

    Args:
        label: Field label.
        count: Number of populated instances.
        total: Total number of instances.
        widths: Column widths (label, count, total).

    Returns:
        Formatted row string.
    """
    max_label, max_count, max_total = widths
    percentage = (count / total * 100) if total > 0 else 0.0
    count_str = f"{count:,}".rjust(max_count)
    total_str = f"{total:,}".rjust(max_total)
    return (
        f"{label.ljust(max_label)}  {count_str} / {total_str}  "
        f"({format_percentage(percentage)})"
    )


def describe_price_vs_max(eq: CanonicalEquity) -> str:
    """
    Describe price relative to 52-week maximum.

    Args:
        eq: Canonical equity.

    Returns:
        Formatted comparison string.
    """
    price = eq.financials.last_price
    maximum = eq.financials.fifty_two_week_max
    return f"{format_equity(eq)} -> price {price} vs max {maximum}"


def describe_price_vs_min(eq: CanonicalEquity) -> str:
    """
    Describe price relative to 52-week minimum.

    Args:
        eq: Canonical equity.

    Returns:
        Formatted comparison string.
    """
    price = eq.financials.last_price
    minimum = eq.financials.fifty_two_week_min
    return f"{format_equity(eq)} -> price {price} vs min {minimum}"


def describe_range_bounds(eq: CanonicalEquity) -> str:
    """
    Describe 52-week price range.

    Args:
        eq: Canonical equity.

    Returns:
        Formatted range string.
    """
    minimum = eq.financials.fifty_two_week_min
    maximum = eq.financials.fifty_two_week_max
    return f"{format_equity(eq)} -> min {minimum}, max {maximum}"


def describe_cap_gap(eq: CanonicalEquity) -> str:
    """
    Describe market cap without corresponding price.

    Args:
        eq: Canonical equity.

    Returns:
        Formatted gap description.
    """
    cap_value = eq.financials.market_cap
    return f"{format_equity(eq)} -> cap {format_currency(cap_value)}, price missing"


def score_equity_completeness(equity: CanonicalEquity) -> int:
    """
    Calculate completeness score based on populated fields.

    Args:
        equity: Canonical equity to score.

    Returns:
        Completeness score (higher is more complete).
    """
    identity_score = sum(
        1
        for field in (
            equity.identity.name,
            equity.identity.isin,
            equity.identity.cusip,
            equity.identity.cik,
            equity.identity.lei,
        )
        if field
    )
    financial_score = sum(
        1
        for field in (
            equity.financials.sector,
            equity.financials.last_price,
            equity.financials.trailing_pe,
            equity.financials.dividend_yield,
        )
        if field
    )
    market_cap_bonus = 2 if equity.financials.market_cap else 0
    return identity_score + financial_score + market_cap_bonus
