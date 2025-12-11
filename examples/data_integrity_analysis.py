# examples/data_integrity_analysis.py

from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from itertools import islice
from statistics import mean, median, stdev

from equity_aggregator import CanonicalEquity
from equity_aggregator.storage import load_canonical_equities


@dataclass(frozen=True)
class AnalysisSettings:
    """Configuration values controlling analysis thresholds."""

    min_sample_size: int = 10
    mega_cap_threshold: int = 200_000_000_000
    micro_cap_threshold: int = 300_000_000
    rare_currency_count: int = 10
    round_price_threshold: float = 30.0
    identifier_gap_alert: float = 50.0
    symbol_length_limit: int = 5
    duplicate_group_limit: int = 3
    finding_sample_limit: int = 5
    dividend_yield_alert: Decimal = Decimal("15")
    profit_margin_high: Decimal = Decimal("100")
    profit_margin_low: Decimal = Decimal("-100")
    price_tolerance: Decimal = Decimal("1.1")
    price_to_min_factor: Decimal = Decimal("0.9")
    isin_length: int = 12
    cusip_length: int = 9
    penny_stock_threshold: Decimal = Decimal("0.01")


def default_settings() -> AnalysisSettings:
    """
    Return default analysis thresholds.

    Returns:
        Default configuration values.
    """
    return AnalysisSettings()


@dataclass(frozen=True)
class Finding:
    """Captures a single insight or anomaly."""

    message: str
    highlights: tuple[str, ...] = ()


@dataclass(frozen=True)
class SectionReport:
    """Structured results for an analysis section."""

    title: str
    findings: tuple[Finding, ...]


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


def build_dataset_overview(equities: Sequence[CanonicalEquity]) -> SectionReport:
    """
    Summarise dataset size and diversity.

    Args:
        equities: Canonical equities to analyse.

    Returns:
        Section report with overview findings.
    """
    if not equities:
        return SectionReport(
            "Dataset Overview",
            (Finding("No equities available for analysis."),),
        )

    sectors = {eq.financials.sector for eq in equities if eq.financials.sector}
    currencies = {eq.financials.currency for eq in equities if eq.financials.currency}

    message = f"Loaded {len(equities):,} canonical equities."
    highlights = (
        f"Distinct sectors: {len(sectors):,}",
        f"Distinct currencies: {len(currencies):,}",
    )
    return SectionReport("Dataset Overview", (Finding(message, highlights),))


def _extract_positive_pe_ratios(equities: Sequence[CanonicalEquity]) -> list[Decimal]:
    """
    Extract positive trailing P/E ratios.

    Args:
        equities: Canonical equities to extract from.

    Returns:
        List of positive P/E ratios.
    """
    return [
        eq.financials.trailing_pe
        for eq in equities
        if eq.financials.trailing_pe and eq.financials.trailing_pe > 0
    ]


def _build_pe_highlights(
    ratios: list[Decimal],
    deviation: Decimal | float,
    limit: float | None,
    outliers: tuple[str, ...],
) -> tuple[str, ...]:
    """
    Build highlight lines for P/E ratio analysis.

    Args:
        ratios: P/E ratio values.
        deviation: Standard deviation.
        limit: Outlier threshold.
        outliers: Outlier samples.

    Returns:
        Formatted highlight lines.
    """
    lines = [
        f"Median ratio: {float(median(ratios)):.2f}",
        f"Mean ratio: {float(mean(ratios)):.2f}",
    ]
    if deviation:
        lines.append(f"Std deviation: {float(deviation):.2f}")
    if limit and outliers:
        lines.append(f"Extreme ratios above {limit:.1f}: {len(outliers)} samples")
        lines.extend(f"  - {sample}" for sample in outliers)
    return tuple(lines)


def compute_pe_findings(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Analyse trailing P/E ratio distribution.

    Args:
        equities: Canonical equities to analyse.
        settings: Analysis thresholds.

    Returns:
        Findings tuple.
    """
    ratios = _extract_positive_pe_ratios(equities)
    if len(ratios) <= settings.min_sample_size:
        return ()

    deviation = stdev(ratios) if len(ratios) > 1 else 0.0
    limit = mean(ratios) + (3 * deviation) if deviation else None

    outliers = (
        format_equity(eq)
        for eq in equities
        if limit and eq.financials.trailing_pe and eq.financials.trailing_pe > limit
    )
    limited_outliers = limit_items(outliers, settings.finding_sample_limit)

    highlights = _build_pe_highlights(ratios, deviation, limit, limited_outliers)
    return (Finding("P/E ratio distribution reviewed.", highlights),)


def _extract_positive_market_caps(equities: Sequence[CanonicalEquity]) -> list[Decimal]:
    """
    Extract positive market capitalisation values.

    Args:
        equities: Canonical equities to extract from.

    Returns:
        List of positive market caps.
    """
    return [
        eq.financials.market_cap
        for eq in equities
        if eq.financials.market_cap and eq.financials.market_cap > 0
    ]


def compute_market_cap_findings(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Analyse market capitalisation distribution.

    Args:
        equities: Canonical equities to analyse.
        settings: Analysis thresholds.

    Returns:
        Findings tuple.
    """
    caps = _extract_positive_market_caps(equities)
    if len(caps) <= settings.min_sample_size:
        return ()

    mega_caps = [cap for cap in caps if cap > settings.mega_cap_threshold]
    micro_caps = [cap for cap in caps if cap < settings.micro_cap_threshold]
    mega_threshold = format_currency(settings.mega_cap_threshold)
    micro_threshold = format_currency(settings.micro_cap_threshold)

    highlights = [
        f"Median market cap: {format_currency(median(caps))}",
        f"Mean market cap: {format_currency(mean(caps))}",
        f"Mega caps > {mega_threshold}: {len(mega_caps):,}",
        f"Micro caps < {micro_threshold}: {len(micro_caps):,}",
    ]
    if mega_caps:
        highlights.append(f"Largest market cap: {format_currency(max(mega_caps))}")

    message = "Market capitalisation distribution summarised."
    return (Finding(message, tuple(highlights)),)


def _filter_negative_pe(equities: Sequence[CanonicalEquity]) -> list[CanonicalEquity]:
    """
    Filter equities with negative P/E ratios.

    Args:
        equities: Canonical equities to filter.

    Returns:
        List of equities with negative P/E.
    """
    return [
        eq
        for eq in equities
        if eq.financials.trailing_pe and eq.financials.trailing_pe < 0
    ]


def _filter_zero_market_cap(
    equities: Sequence[CanonicalEquity],
) -> list[CanonicalEquity]:
    """
    Filter equities with zero or negative market cap.

    Args:
        equities: Canonical equities to filter.

    Returns:
        List of equities with invalid market cap.
    """
    return [
        eq
        for eq in equities
        if eq.financials.market_cap is not None and eq.financials.market_cap <= 0
    ]


def compute_negative_metric_findings(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify negative or zero financial metrics.

    Args:
        equities: Canonical equities to analyse.
        settings: Analysis thresholds.

    Returns:
        Findings tuple.
    """
    findings: list[Finding] = []

    negative_pe = _filter_negative_pe(equities)
    if negative_pe:
        samples = limit_items(
            (format_equity(eq) for eq in negative_pe),
            settings.finding_sample_limit,
        )
        message = f"Negative P/E ratios present: {len(negative_pe):,} companies."
        findings.append(Finding(message, samples))

    zero_cap = _filter_zero_market_cap(equities)
    if zero_cap:
        sample_lines = (
            f"{format_equity(eq)} -> value {eq.financials.market_cap}"
            for eq in zero_cap
        )
        samples = limit_items(sample_lines, settings.finding_sample_limit)
        message = f"Zero or negative market cap entries: {len(zero_cap):,} companies."
        findings.append(Finding(message, samples))

    return tuple(findings)


def compute_price_range_findings(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify prices exceeding 52-week maximum.

    Args:
        equities: Canonical equities to analyse.
        settings: Analysis thresholds.

    Returns:
        Findings tuple.
    """
    anomalies = [
        eq
        for eq in equities
        if (
            eq.financials.last_price
            and eq.financials.fifty_two_week_max
            and eq.financials.fifty_two_week_min
            and eq.financials.last_price
            > eq.financials.fifty_two_week_max * settings.price_tolerance
        )
    ]
    if not anomalies:
        return ()

    samples = limit_items(
        (describe_price_vs_max(eq) for eq in anomalies),
        settings.finding_sample_limit,
    )
    message = (
        f"Price exceeds 52-week max (+10% tolerance) for {len(anomalies):,} equities."
    )
    return (Finding(message, samples),)


def analyse_financial_outliers(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Analyse financial metric outliers.

    Args:
        equities: Canonical equities to analyse.
        settings: Analysis thresholds.

    Returns:
        Section report with outlier findings.
    """
    findings = (
        compute_pe_findings(equities, settings)
        + compute_market_cap_findings(equities, settings)
        + compute_negative_metric_findings(equities, settings)
        + compute_price_range_findings(equities, settings)
    )
    return SectionReport("Financial Metric Outliers", findings)


def detect_range_inversions(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify impossible 52-week ranges.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing range inversions.
    """

    inversions = [
        eq
        for eq in equities
        if (
            eq.financials.fifty_two_week_min
            and eq.financials.fifty_two_week_max
            and eq.financials.fifty_two_week_min > eq.financials.fifty_two_week_max
        )
    ]
    if not inversions:
        return ()

    sample_lines = (describe_range_bounds(eq) for eq in inversions)
    return (
        Finding(
            f"Range inversions detected for {len(inversions):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_stale_range_data(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Look for stale price data where price equals both range endpoints.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing stale price data.
    """

    stale_records = [
        eq
        for eq in equities
        if (
            eq.financials.last_price
            and eq.financials.fifty_two_week_min
            and eq.financials.fifty_two_week_max
            and eq.financials.last_price == eq.financials.fifty_two_week_min
            and eq.financials.last_price == eq.financials.fifty_two_week_max
        )
    ]
    if not stale_records:
        return ()

    sample_lines = (
        f"{format_equity(eq)} -> all values {eq.financials.last_price}"
        for eq in stale_records
    )
    return (
        Finding(
            f"Potentially stale price data for {len(stale_records):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_price_below_min(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Flag prices that sit well below the 52-week minimum.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing prices beneath range minima.
    """

    below_min = [
        eq
        for eq in equities
        if (
            eq.financials.last_price
            and eq.financials.fifty_two_week_min
            and eq.financials.last_price
            < eq.financials.fifty_two_week_min * settings.price_to_min_factor
        )
    ]
    if not below_min:
        return ()

    sample_lines = (describe_price_vs_min(eq) for eq in below_min)
    return (
        Finding(
            f"Prices materially below 52-week minimum for {len(below_min):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_temporal_anomalies(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Combine range-based temporal checks.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined temporal anomaly findings.
    """

    findings = (
        detect_range_inversions(equities, settings)
        + detect_stale_range_data(equities, settings)
        + detect_price_below_min(equities, settings)
    )
    return SectionReport("Price Range Integrity", findings)


def detect_extreme_dividends(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Highlight dividend yields that exceed alert thresholds.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing extreme dividend yields.
    """

    high_yield = [
        eq
        for eq in equities
        if (
            eq.financials.dividend_yield
            and eq.financials.dividend_yield > settings.dividend_yield_alert
        )
    ]
    if not high_yield:
        return ()

    sample_lines = (
        f"{format_equity(eq)} -> yield {eq.financials.dividend_yield:.2f}%"
        for eq in sorted(
            high_yield,
            key=lambda entry: entry.financials.dividend_yield,
            reverse=True,
        )
    )
    return (
        Finding(
            (
                f"Dividend yields above {settings.dividend_yield_alert}%: "
                f"{len(high_yield):,} equities."
            ),
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_penny_stocks(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify prices below the configured penny threshold.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing ultra-low traded prices.
    """

    penny_entries = [
        eq
        for eq in equities
        if (
            eq.financials.last_price
            and eq.financials.last_price > 0
            and eq.financials.last_price < settings.penny_stock_threshold
        )
    ]
    if not penny_entries:
        return ()

    sample_lines = (
        f"{format_equity(eq)} -> price {eq.financials.last_price}"
        for eq in penny_entries
    )
    return (
        Finding(
            f"Prices below one cent for {len(penny_entries):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_profit_margin_extremes(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Highlight profit margins that sit outside realistic ranges.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing extreme profit margins.
    """

    margins = [
        eq
        for eq in equities
        if (
            eq.financials.profit_margin
            and (
                eq.financials.profit_margin > settings.profit_margin_high
                or eq.financials.profit_margin < settings.profit_margin_low
            )
        )
    ]
    if not margins:
        return ()

    sample_lines = (
        f"{format_equity(eq)} -> margin {eq.financials.profit_margin:.2f}%"
        for eq in sorted(
            margins,
            key=lambda entry: abs(entry.financials.profit_margin),
            reverse=True,
        )
    )
    return (
        Finding(
            "Profit margins beyond +/-100% detected.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_negative_price_to_book(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Flag negative price-to-book ratios.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing negative price-to-book results.
    """

    negative_values = [
        eq
        for eq in equities
        if eq.financials.price_to_book and eq.financials.price_to_book < 0
    ]
    if not negative_values:
        return ()

    sample_lines = (
        f"{format_equity(eq)} -> P/B {eq.financials.price_to_book:.2f}"
        for eq in negative_values
    )
    return (
        Finding(
            f"Negative price-to-book ratios for {len(negative_values):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_round_price_clusters(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Highlight clustering of round pound or dollar prices.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing round-price clustering.
    """

    round_prices = [
        eq
        for eq in equities
        if (
            eq.financials.last_price
            and eq.financials.last_price > 1
            and eq.financials.last_price % 1 == 0
        )
    ]
    if not round_prices:
        return ()

    ratio = (len(round_prices) / len(equities)) * 100 if equities else 0.0
    message = (
        f"Round dollar price clustering across {len(round_prices):,} equities"
        f" ({format_percentage(ratio)} of dataset)."
    )
    highlights: tuple[str, ...]
    if ratio > settings.round_price_threshold:
        highlights = ("High concentration of round prices may indicate defaults.",)
    else:
        highlights = ()
    return (Finding(message, highlights),)


def detect_extreme_financial_values(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Combine extreme value detections.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined extreme financial value findings.
    """

    findings = (
        detect_extreme_dividends(equities, settings)
        + detect_penny_stocks(equities, settings)
        + detect_profit_margin_extremes(equities, settings)
        + detect_negative_price_to_book(equities, settings)
        + detect_round_price_clusters(equities, settings)
    )
    return SectionReport("Extreme Financial Values", findings)


def analyse_symbol_patterns(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Evaluate basic symbol shape metrics.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing ticker symbol patterns.
    """

    total = len(equities)
    if total == 0:
        return ()

    dots = sum(1 for eq in equities if "." in (eq.identity.symbol or ""))
    numerics = sum(
        1 for eq in equities if any(char.isdigit() for char in eq.identity.symbol or "")
    )
    long_symbols = [
        eq
        for eq in equities
        if eq.identity.symbol and len(eq.identity.symbol) > settings.symbol_length_limit
    ]

    symbol_limit = settings.symbol_length_limit
    highlights = (
        f"Symbols with dots: {dots:,}",
        f"Symbols containing digits: {numerics:,}",
        f"Symbols longer than {symbol_limit} chars: {len(long_symbols):,}",
    )
    return (Finding("Ticker symbol pattern review completed.", highlights),)


def _collect_duplicate_name_groups(
    equities: Sequence[CanonicalEquity],
) -> dict[str, list[CanonicalEquity]]:
    """
    Group equities by normalised name.

    Args:
        equities: Sequence of canonical equities to assess.

    Returns:
        dict[str, list[CanonicalEquity]]: Mapping of normalised names to equities.
    """

    groups: dict[str, list[CanonicalEquity]] = defaultdict(list)
    for equity in equities:
        name = (equity.identity.name or "").strip().upper()
        if name:
            groups[name].append(equity)
    return groups


def _duplicate_sample_lines(
    duplicates: dict[str, list[CanonicalEquity]],
    settings: AnalysisSettings,
) -> tuple[str, ...]:
    """
    Build sample lines for duplicate name groups.

    Args:
        duplicates: Mapping of normalised names to their equities.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[str, ...]: Highlight lines summarising duplicate names.
    """

    samples: list[str] = []
    sorted_groups = sorted(
        duplicates.items(),
        key=lambda item: len(item[1]),
        reverse=True,
    )
    for name, group in islice(sorted_groups, settings.duplicate_group_limit):
        samples.append(f"{name} -> {len(group)} entries")
        member_labels = limit_items(
            (format_equity(eq) for eq in group),
            settings.finding_sample_limit,
        )
        samples.extend(f"  - {label}" for label in member_labels)
    return tuple(samples)


def analyse_duplicate_names(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Detect repeated company names.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing duplicate company names.
    """

    groups = _collect_duplicate_name_groups(equities)
    duplicates = {name: sample for name, sample in groups.items() if len(sample) > 1}
    if not duplicates:
        return ()

    total_entries = sum(len(group) for group in duplicates.values())

    samples = _duplicate_sample_lines(duplicates, settings)

    message = (
        f"Duplicate company names detected for {len(duplicates):,} labels"
        f" affecting {total_entries:,} entries."
    )
    return (Finding(message, tuple(samples)),)


def analyse_currency_rarity(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Highlight currencies with sparse coverage.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing rare currency usage.
    """

    currencies = Counter(
        eq.financials.currency for eq in equities if eq.financials.currency
    )
    rare = {
        code: count
        for code, count in currencies.items()
        if count < settings.rare_currency_count
    }
    if not rare:
        return ()

    sorted_lines = [
        f"{code}: {count} companies" for code, count in sorted(rare.items())
    ]
    currency_count = settings.rare_currency_count
    message = f"Currencies with fewer than {currency_count} entries: {len(rare):,}."
    return (
        Finding(
            message,
            limit_items(sorted_lines, settings.finding_sample_limit),
        ),
    )


def analyse_data_consistency(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Combine symbol, naming, and currency consistency checks.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined data consistency findings.
    """

    findings = (
        analyse_symbol_patterns(equities, settings)
        + analyse_duplicate_names(equities, settings)
        + analyse_currency_rarity(equities, settings)
    )
    return SectionReport("Symbol and Naming Consistency", findings)


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

    missing = {
        "FIGI": sum(1 for eq in equities if not eq.identity.share_class_figi),
        "ISIN": sum(1 for eq in equities if not eq.identity.isin),
        "CUSIP": sum(1 for eq in equities if not eq.identity.cusip),
        "CIK": sum(1 for eq in equities if not eq.identity.cik),
    }
    lines = []
    for label, count in missing.items():
        percentage = (count / total) * 100 if total else 0.0
        prefix = "High gap" if percentage > settings.identifier_gap_alert else "Gap"
        lines.append(
            (
                f"{prefix} for {label}: {count:,} entries "
                f"({format_percentage(percentage)})."
            ),
        )
    return (Finding("Identifier coverage review.", tuple(lines)),)


def validate_identifier_formats(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Validate identifier format adherence.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing identifier format concerns.
    """

    invalid_isin = [
        eq
        for eq in equities
        if eq.identity.isin and len(eq.identity.isin) != settings.isin_length
    ]
    invalid_cusip = [
        eq
        for eq in equities
        if eq.identity.cusip and len(eq.identity.cusip) != settings.cusip_length
    ]
    invalid_cik = [
        eq for eq in equities if eq.identity.cik and not eq.identity.cik.isdigit()
    ]

    findings: list[Finding] = []
    if invalid_isin:
        isin_labels = (format_equity(eq) for eq in invalid_isin)
        findings.append(
            Finding(
                f"Unexpected ISIN length for {len(invalid_isin):,} equities.",
                limit_items(isin_labels, settings.finding_sample_limit),
            ),
        )
    if invalid_cusip:
        cusip_labels = (format_equity(eq) for eq in invalid_cusip)
        findings.append(
            Finding(
                f"Unexpected CUSIP length for {len(invalid_cusip):,} equities.",
                limit_items(cusip_labels, settings.finding_sample_limit),
            ),
        )
    if invalid_cik:
        cik_labels = (format_equity(eq) for eq in invalid_cik)
        findings.append(
            Finding(
                f"Non-numeric CIK values for {len(invalid_cik):,} equities.",
                limit_items(cik_labels, settings.finding_sample_limit),
            ),
        )
    return tuple(findings)


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


def detect_price_without_cap(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify price records lacking market capitalisation.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing prices without market capitalisation.
    """

    entries = [
        eq
        for eq in equities
        if eq.financials.last_price
        and eq.financials.last_price > 0
        and not eq.financials.market_cap
    ]
    if not entries:
        return ()

    sample_lines = (
        f"{format_equity(eq)} -> price {eq.financials.last_price}, cap missing"
        for eq in entries
    )
    return (
        Finding(
            f"Price recorded without market cap for {len(entries):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_cap_without_price(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify market capitalisation entries that lack a price.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing market capitalisations without prices.
    """

    entries = [
        eq
        for eq in equities
        if eq.financials.market_cap
        and eq.financials.market_cap > 0
        and not eq.financials.last_price
    ]
    if not entries:
        return ()

    sample_lines = (describe_cap_gap(eq) for eq in entries)
    return (
        Finding(
            f"Market cap recorded without price for {len(entries):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def detect_missing_price_and_cap(
    equities: Sequence[CanonicalEquity],
) -> tuple[Finding, ...]:
    """
    Report equities missing both price and market cap.
    """

    missing = [
        eq
        for eq in equities
        if not eq.financials.last_price and not eq.financials.market_cap
    ]
    if not missing:
        return ()

    with_other_metrics = sum(
        1
        for eq in missing
        if eq.financials.revenue
        or eq.financials.trailing_pe
        or eq.financials.dividend_yield
    )
    highlights = (
        f"Total entries missing both fields: {len(missing):,}.",
        f"Entries that still carry other metrics: {with_other_metrics:,}.",
    )
    return (Finding("Price and market cap simultaneously missing.", highlights),)


def detect_partial_range(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Identify equities with only one side of the price range populated.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings describing partial 52-week ranges.
    """

    partial = [
        eq
        for eq in equities
        if (
            (eq.financials.fifty_two_week_min and not eq.financials.fifty_two_week_max)
            or (
                eq.financials.fifty_two_week_max
                and not eq.financials.fifty_two_week_min
            )
        )
    ]
    if not partial:
        return ()

    sample_lines = (describe_range_bounds(eq) for eq in partial)
    return (
        Finding(
            f"Partial 52-week ranges for {len(partial):,} equities.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def analyse_cross_field_logic(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Gather cross-field logic inconsistencies.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined cross-field consistency findings.
    """

    findings = (
        detect_price_without_cap(equities, settings)
        + detect_cap_without_price(equities, settings)
        + detect_missing_price_and_cap(equities)
        + detect_partial_range(equities, settings)
    )
    return SectionReport("Cross-field Logic Consistency", findings)


def identity_completeness(
    equities: Sequence[CanonicalEquity],
) -> tuple[Finding, ...]:
    """
    Summarise completion of core identity fields.
    """

    total = len(equities)
    if total == 0:
        return ()

    fields = [
        ("Name", sum(1 for eq in equities if eq.identity.name)),
        ("Symbol", sum(1 for eq in equities if eq.identity.symbol)),
        ("FIGI", sum(1 for eq in equities if eq.identity.share_class_figi)),
        ("ISIN", sum(1 for eq in equities if eq.identity.isin)),
        ("CUSIP", sum(1 for eq in equities if eq.identity.cusip)),
        ("CIK", sum(1 for eq in equities if eq.identity.cik)),
    ]
    items = [(label, count, total) for label, count in fields]
    lines = format_coverage_table(items)
    return (Finding("Identity field coverage summary.", lines),)


def top_complete_profiles(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> tuple[Finding, ...]:
    """
    Score equities by completeness and return top samples.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        tuple[Finding, ...]: Findings listing the most complete profiles.
    """

    scores = [(score_equity_completeness(eq), eq) for eq in equities]

    if not scores:
        return ()

    top_entries = sorted(scores, key=lambda item: item[0], reverse=True)
    sample_lines = (
        f"Score {score:02} -> {format_equity(eq)}"
        for score, eq in islice(top_entries, settings.finding_sample_limit)
    )
    return (
        Finding(
            "Most complete equity profiles.",
            limit_items(sample_lines, settings.finding_sample_limit),
        ),
    )


def valuation_coverage(
    equities: Sequence[CanonicalEquity],
) -> tuple[Finding, ...]:
    """
    Provide coverage stats for core financial metrics.
    """

    total = len(equities)
    if total == 0:
        return ()

    field_labels = (
        ("mics", "MICs"),
        ("currency", "Currency"),
        ("last_price", "Last price"),
        ("market_cap", "Market cap"),
        ("fifty_two_week_min", "52-week low"),
        ("fifty_two_week_max", "52-week high"),
        ("dividend_yield", "Dividend yield"),
        ("market_volume", "Market volume"),
        ("held_insiders", "Held by insiders"),
        ("held_institutions", "Held by institutions"),
        ("short_interest", "Short interest"),
        ("share_float", "Share float"),
        ("shares_outstanding", "Shares outstanding"),
        ("revenue_per_share", "Revenue per share"),
        ("profit_margin", "Profit margin"),
        ("gross_margin", "Gross margin"),
        ("operating_margin", "Operating margin"),
        ("free_cash_flow", "Free cash flow"),
        ("operating_cash_flow", "Operating cash flow"),
        ("return_on_equity", "Return on equity"),
        ("return_on_assets", "Return on assets"),
        ("performance_1_year", "1-year performance"),
        ("total_debt", "Total debt"),
        ("revenue", "Revenue"),
        ("ebitda", "EBITDA"),
        ("trailing_pe", "P/E ratio"),
        ("price_to_book", "Price to book"),
        ("trailing_eps", "Trailing EPS"),
        ("analyst_rating", "Analyst rating"),
        ("industry", "Industry"),
        ("sector", "Sector"),
    )
    items = [
        (
            label,
            sum(1 for eq in equities if getattr(eq.financials, field) is not None),
            total,
        )
        for field, label in field_labels
    ]
    lines = format_coverage_table(items)
    return (Finding("Financial metric coverage summary.", lines),)


def analyse_data_quality(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings,
) -> SectionReport:
    """
    Aggregate completeness and coverage insights.

    Args:
        equities: Sequence of canonical equities to assess.
        settings: Thresholds and sampling configuration for the analysis.

    Returns:
        SectionReport: Combined data completeness findings.
    """

    findings = (
        identity_completeness(equities)
        + top_complete_profiles(equities, settings)
        + valuation_coverage(equities)
    )
    return SectionReport("Data Completeness", findings)


def currency_distribution(
    equities: Sequence[CanonicalEquity],
) -> tuple[Finding, ...]:
    """
    Summarise currency usage within the dataset.
    """

    counts = Counter(
        eq.financials.currency for eq in equities if eq.financials.currency
    )
    if not counts:
        return ()

    total = sum(counts.values())
    lines = [
        (f"{code}: {count:,} entries ({format_percentage((count / total) * 100)})")
        for code, count in counts.most_common()
    ]
    return (Finding("Currency distribution summary.", tuple(lines)),)


def geography_proxies(
    equities: Sequence[CanonicalEquity],
) -> tuple[Finding, ...]:
    """
    Use identifier presence as a proxy for geography.
    """

    counts = {
        "CUSIP": sum(1 for eq in equities if eq.identity.cusip),
        "ISIN": sum(1 for eq in equities if eq.identity.isin),
        "CIK": sum(1 for eq in equities if eq.identity.cik),
    }
    lines = [f"{label} present: {count:,}" for label, count in counts.items()]
    return (Finding("Geographic indicator coverage.", tuple(lines)),)


def analyse_currency_and_geography(
    equities: Sequence[CanonicalEquity],
) -> SectionReport:
    """
    Aggregate currency and geographic proxy metrics.
    """

    findings = currency_distribution(equities) + geography_proxies(equities)
    return SectionReport("Currency and Geography", findings)


def run_analysis(
    equities: Sequence[CanonicalEquity],
    settings: AnalysisSettings | None = None,
) -> tuple[SectionReport, ...]:
    """
    Execute complete data integrity analysis suite.

    Args:
        equities: Equities to analyse.
        settings: Analysis thresholds (defaults to standard settings).

    Returns:
        Ordered section reports with findings.
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


def main() -> None:
    """
    Run data integrity analysis on stored canonical equities.

    Loads equities from local database and generates comprehensive
    analysis report covering multiple data quality dimensions.
    """
    try:
        equities = load_canonical_equities(refresh_fn=None)
    except Exception as exc:  # pragma: no cover - defensive for CLI usage
        print(f"\n✗ Failed to load canonical equities: {exc}")
        print(
            "  Ensure the local database exists at"
            " ~/Library/Application Support/equity-aggregator/data_store.db\n",
        )
        return

    if not equities:
        print("\n✗ No canonical equities were loaded; aborting analysis.")
        print("  Run 'equity-aggregator download' to populate the data store.\n")
        return

    reports = run_analysis(equities, default_settings())
    print_summary_header(len(equities), reports)
    render_reports(reports)
    print("\n" + "=" * 80)
    print("  Analysis complete.")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
