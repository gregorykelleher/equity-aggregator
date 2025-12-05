#!/usr/bin/env python3
"""
Data Integrity Analysis Tool for equity-aggregator package.

This script performs comprehensive data quality analysis and anomaly detection
on the canonical equity dataset. It identifies outliers, validates data consistency,
and highlights potential data integrity issues across financial metrics, identifiers,
and geographic distributions.
"""

import re
import sys
from decimal import Decimal
from statistics import median, stdev

from equity_aggregator import CanonicalEquity, retrieve_canonical_equities


def print_separator(title: str) -> None:
    """Print a formatted section separator."""
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print(f"{'=' * 60}")


def load_equity_dataset() -> list[CanonicalEquity]:
    """Load canonical equity dataset for analysis."""
    print_separator("📊 LOADING EQUITY DATASET FOR INTEGRITY ANALYSIS")

    try:
        print("Loading canonical equity dataset...")
        equities = retrieve_canonical_equities()

        print(f"✅ Successfully loaded {len(equities):,} equities")

        # Basic dataset overview
        sectors = [eq.financials.sector for eq in equities if eq.financials.sector]
        currencies = [
            eq.financials.currency for eq in equities if eq.financials.currency
        ]

        print(f"📈 Unique sectors: {len(set(sectors)):,}")
        print(f"💱 Unique currencies: {len(set(currencies)):,}")

        return equities

    except Exception as e:
        print(f"❌ Error loading dataset: {e}")
        return []


def detect_financial_outliers(equities: list[CanonicalEquity]) -> None:
    """Detect outliers and anomalies in financial metrics."""
    if not equities:
        print("⚠️  No equities available for outlier detection")
        return

    print_separator("🚨 FINANCIAL METRICS OUTLIER DETECTION")

    # P/E Ratio Analysis
    pe_ratios = [
        eq.financials.trailing_pe
        for eq in equities
        if eq.financials.trailing_pe and eq.financials.trailing_pe > 0
    ]

    if len(pe_ratios) > 10:
        pe_median = median(pe_ratios)
        pe_avg = sum(pe_ratios) / len(pe_ratios)
        pe_std = stdev(pe_ratios) if len(pe_ratios) > 1 else 0

        print("💰 P/E RATIO ANOMALY ANALYSIS:")
        print(f"   Median P/E: {pe_median:.2f}")
        print(f"   Average P/E: {pe_avg:.2f}")
        print(f"   Std Dev: {pe_std:.2f}")

        # Find extreme outliers (3+ standard deviations)
        extreme_pe = [pe for pe in pe_ratios if pe > pe_avg + 3 * pe_std]
        if extreme_pe:
            print(
                f"   ⚠️  EXTREME P/E OUTLIERS: {len(extreme_pe):,} companies with P/E > {pe_avg + 3 * pe_std:.1f}",
            )
            print(f"   Max P/E: {max(extreme_pe):.1f}")

    # Market Cap Analysis
    market_caps = [
        eq.financials.market_cap
        for eq in equities
        if eq.financials.market_cap and eq.financials.market_cap > 0
    ]

    if len(market_caps) > 10:
        cap_median = median(market_caps)
        cap_avg = sum(market_caps) / len(market_caps)

        print("\n📊 MARKET CAP DISTRIBUTION ANALYSIS:")
        print(f"   Median Market Cap: ${cap_median:,.0f}")
        print(f"   Average Market Cap: ${cap_avg:,.0f}")

        # Identify mega-caps vs micro-caps
        mega_caps = [cap for cap in market_caps if cap > 200_000_000_000]  # $200B+
        micro_caps = [cap for cap in market_caps if cap < 300_000_000]  # <$300M

        print(f"   Mega-caps (>$200B): {len(mega_caps):,} companies")
        print(f"   Micro-caps (<$300M): {len(micro_caps):,} companies")

        if mega_caps:
            print(f"   Largest company: ${max(mega_caps):,.0f}")

    # Negative/Zero Values Detection
    print("\n🔍 SUSPICIOUS VALUES DETECTION:")

    negative_pe = [
        eq
        for eq in equities
        if eq.financials.trailing_pe and eq.financials.trailing_pe < 0
    ]
    if negative_pe:
        print(f"   ⚠️  Companies with negative P/E: {len(negative_pe):,}")

    zero_market_cap = [
        eq
        for eq in equities
        if eq.financials.market_cap is not None and eq.financials.market_cap <= 0
    ]
    if zero_market_cap:
        print(
            f"   ⚠️  Companies with zero/negative market cap: {len(zero_market_cap):,}",
        )
        for eq in zero_market_cap[:5]:  # Show first 5
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): ${eq.financials.market_cap}",
            )

    # Price consistency checks
    price_issues = []
    for eq in equities:
        if (
            eq.financials.last_price
            and eq.financials.last_price > 0
            and eq.financials.fifty_two_week_max
            and eq.financials.fifty_two_week_min
        ):
            if eq.financials.last_price > (
                eq.financials.fifty_two_week_max * Decimal("1.1")
            ):  # 10% tolerance
                price_issues.append(eq)

    if price_issues:
        print(f"   ⚠️  Price vs 52-week range anomalies: {len(price_issues):,}")
        for eq in price_issues[:3]:  # Show first 3
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): Current ${eq.financials.last_price}, 52W Max ${eq.financials.fifty_two_week_max}",
            )


def analyze_data_consistency(equities: list[CanonicalEquity]) -> None:
    """Analyze data consistency and identify potential quality issues."""
    if not equities:
        print("⚠️  No equities available for consistency analysis")
        return

    print_separator("🔬 DATA CONSISTENCY & QUALITY ANALYSIS")

    # Symbol format analysis
    print("🏷️  SYMBOL FORMAT ANALYSIS:")
    symbols_with_dots = [eq for eq in equities if "." in eq.identity.symbol]
    symbols_with_numbers = [
        eq for eq in equities if any(c.isdigit() for c in eq.identity.symbol)
    ]
    long_symbols = [eq for eq in equities if len(eq.identity.symbol) > 5]

    print(f"   Symbols with dots: {len(symbols_with_dots):,}")
    print(f"   Symbols with numbers: {len(symbols_with_numbers):,}")
    print(f"   Symbols >5 characters: {len(long_symbols):,}")

    # Name consistency analysis
    print("\n📛 NAME CONSISTENCY ANALYSIS:")
    duplicate_names = {}
    for eq in equities:
        name = eq.identity.name.strip().upper()
        if name in duplicate_names:
            duplicate_names[name].append(eq)
        else:
            duplicate_names[name] = [eq]

    name_duplicates = {
        name: eqs for name, eqs in duplicate_names.items() if len(eqs) > 1
    }
    if name_duplicates:
        print(f"   ⚠️  Duplicate company names: {len(name_duplicates):,}")
        print(
            f"   Total affected companies: {sum(len(eqs) for eqs in name_duplicates.values()):,}",
        )

        # Show examples of duplicate names (first 3 groups)
        print("   Examples of duplicate names:")
        for i, (name, eqs) in enumerate(
            sorted(name_duplicates.items(), key=lambda x: len(x[1]), reverse=True)[:3],
        ):
            print(f"      • '{name}' appears {len(eqs)} times:")
            for eq in eqs[:3]:  # Show first 3 companies with this name
                print(
                    f"        - {eq.identity.symbol} (FIGI: {eq.identity.share_class_figi or 'N/A'})",
                )

    # Identifier coverage gaps
    print("\n🆔 IDENTIFIER COVERAGE GAPS:")
    missing_identifiers = {
        "FIGI": len([eq for eq in equities if not eq.identity.share_class_figi]),
        "ISIN": len([eq for eq in equities if not eq.identity.isin]),
        "CUSIP": len([eq for eq in equities if not eq.identity.cusip]),
        "CIK": len([eq for eq in equities if not eq.identity.cik]),
    }

    for identifier, count in missing_identifiers.items():
        percentage = (count / len(equities)) * 100
        if percentage > 50:  # Highlight significant gaps
            print(f"   ⚠️  Missing {identifier}: {count:,} ({percentage:.1f}%)")
        else:
            print(f"   Missing {identifier}: {count:,} ({percentage:.1f}%)")

    # Currency inconsistencies
    print("\n💱 CURRENCY CONSISTENCY:")
    currencies = [eq.financials.currency for eq in equities if eq.financials.currency]
    currency_counts = {}
    for currency in currencies:
        currency_counts[currency] = currency_counts.get(currency, 0) + 1

    rare_currencies = {
        curr: count for curr, count in currency_counts.items() if count < 10
    }
    if rare_currencies:
        print(f"   ⚠️  Rare currencies (<10 companies): {len(rare_currencies):,}")
        for curr, count in sorted(rare_currencies.items()):
            print(f"      {curr}: {count} companies")
            # Show details for rare currency companies
            rare_companies = [eq for eq in equities if eq.financials.currency == curr]
            for eq in rare_companies:
                print(
                    f"        • {eq.identity.name} ({eq.identity.symbol}) - FIGI: {eq.identity.share_class_figi or 'N/A'}",
                )


def demo_equity_exploration(equities: list[CanonicalEquity]) -> None:
    """Demonstrate comprehensive aggregate analysis of equity data."""
    if not equities:
        print("⚠️  No equities available for exploration demo")
        return

    print_separator("🔬 COMPREHENSIVE EQUITY AGGREGATE ANALYSIS")

    # Market cap analysis
    print("💰 MARKET CAPITALIZATION ANALYSIS:")
    market_cap_companies = [
        eq for eq in equities if eq.financials.market_cap is not None
    ]

    if market_cap_companies:
        total_market_cap = sum(eq.financials.market_cap for eq in market_cap_companies)
        avg_market_cap = total_market_cap / len(market_cap_companies)

        print(f"   Total Market Cap: ${total_market_cap:,.0f}")
        print(f"   Average Market Cap: ${avg_market_cap:,.0f}")
        print(f"   Companies with Market Cap: {len(market_cap_companies):,}")

        # Top 10 companies by market cap
        print("\n🏆 TOP 10 COMPANIES BY MARKET CAP:")
        top_companies = sorted(
            market_cap_companies,
            key=lambda x: x.financials.market_cap,
            reverse=True,
        )[:10]
        for i, company in enumerate(top_companies, 1):
            print(
                f"   {i:2d}. {company.identity.name[:40]:40} ${company.financials.market_cap:>15,.0f}",
            )

    # Sector analysis
    print("\n📊 SECTOR DISTRIBUTION:")
    sectors = [eq.financials.sector for eq in equities if eq.financials.sector]
    if sectors:
        sector_counts = {}
        for sector in sectors:
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        print(f"   Total sectors represented: {len(sector_counts)}")
        for sector, count in sorted(
            sector_counts.items(),
            key=lambda x: x[1],
            reverse=True,
        ):
            percentage = (count / len(sectors)) * 100
            print(f"   {sector:25} {count:>4,} companies ({percentage:5.1f}%)")

    # Currency analysis
    print("\n💱 CURRENCY DISTRIBUTION:")
    currencies = [eq.financials.currency for eq in equities if eq.financials.currency]
    if currencies:
        currency_counts = {}
        for currency in currencies:
            currency_counts[currency] = currency_counts.get(currency, 0) + 1

        for currency, count in sorted(
            currency_counts.items(),
            key=lambda x: x[1],
            reverse=True,
        ):
            percentage = (count / len(currencies)) * 100
            print(f"   {currency} {count:>4,} companies ({percentage:5.1f}%)")

    # Geographic distribution (based on data sources)
    print("\n🌍 GEOGRAPHIC INSIGHTS:")
    # Count companies with different identifier types to infer geography
    cusip_count = len([eq for eq in equities if eq.identity.cusip])
    isin_count = len([eq for eq in equities if eq.identity.isin])
    cik_count = len([eq for eq in equities if eq.identity.cik])

    print(f"   Companies with CUSIP (US focus): {cusip_count:,}")
    print(f"   Companies with ISIN (International): {isin_count:,}")
    print(f"   Companies with CIK (SEC registered): {cik_count:,}")

    # Financial metrics coverage
    print("\n📈 FINANCIAL DATA COVERAGE:")
    metrics = [
        (
            "Market Cap",
            len([eq for eq in equities if eq.financials.market_cap is not None]),
        ),
        (
            "Last Price",
            len([eq for eq in equities if eq.financials.last_price is not None]),
        ),
        (
            "Dividend Yield",
            len([eq for eq in equities if eq.financials.dividend_yield is not None]),
        ),
        (
            "P/E Ratio",
            len([eq for eq in equities if eq.financials.trailing_pe is not None]),
        ),
        ("Revenue", len([eq for eq in equities if eq.financials.revenue is not None])),
        (
            "Profit Margin",
            len([eq for eq in equities if eq.financials.profit_margin is not None]),
        ),
    ]

    for metric_name, count in metrics:
        percentage = (count / len(equities)) * 100
        print(f"   {metric_name:15} {count:>4,} companies ({percentage:5.1f}%)")


def demo_data_quality_insights(equities: list[CanonicalEquity]) -> None:
    """Demonstrate data quality and completeness insights."""
    if not equities:
        print("⚠️  No equities available for data quality demo")
        return

    print_separator("🏗️  DATA QUALITY & COMPLETENESS INSIGHTS")

    # Identity completeness
    print("🆔 IDENTITY DATA COMPLETENESS:")
    identity_fields = [
        ("Name", len([eq for eq in equities if eq.identity.name])),
        ("Symbol", len([eq for eq in equities if eq.identity.symbol])),
        ("FIGI", len([eq for eq in equities if eq.identity.share_class_figi])),
        ("ISIN", len([eq for eq in equities if eq.identity.isin])),
        ("CUSIP", len([eq for eq in equities if eq.identity.cusip])),
        ("CIK", len([eq for eq in equities if eq.identity.cik])),
    ]

    for field_name, count in identity_fields:
        percentage = (count / len(equities)) * 100
        print(
            f"   {field_name:10} {count:>5,} / {len(equities):,} ({percentage:5.1f}%)",
        )

    # Find companies with most complete data
    print("\n⭐ MOST COMPLETE EQUITY PROFILES:")
    scored_equities = []
    for equity in equities:
        score = 0
        # Identity score
        if equity.identity.name:
            score += 1
        if equity.identity.isin:
            score += 1
        if equity.identity.cusip:
            score += 1
        if equity.identity.cik:
            score += 1
        # Financial score
        if equity.financials.market_cap:
            score += 2
        if equity.financials.sector:
            score += 1
        if equity.financials.last_price:
            score += 1
        if equity.financials.trailing_pe:
            score += 1
        if equity.financials.dividend_yield:
            score += 1

        scored_equities.append((equity, score))

    # Show top 5 most complete profiles
    top_complete = sorted(scored_equities, key=lambda x: x[1], reverse=True)[:5]
    for i, (equity, score) in enumerate(top_complete, 1):
        market_cap_str = (
            f"${equity.financials.market_cap:,.0f}"
            if equity.financials.market_cap
            else "N/A"
        )
        print(
            f"   {i}. {equity.identity.name[:35]:35} (Score: {score}/10, Cap: {market_cap_str})",
        )

    # Valuation metrics insights
    print("\n📊 VALUATION METRICS INSIGHTS:")
    pe_ratios = [
        eq.financials.trailing_pe
        for eq in equities
        if eq.financials.trailing_pe and eq.financials.trailing_pe > 0
    ]
    if pe_ratios:
        avg_pe = sum(pe_ratios) / len(pe_ratios)
        print(f"   Average P/E Ratio: {avg_pe:.2f} (from {len(pe_ratios):,} companies)")

    dividend_yields = [
        eq.financials.dividend_yield
        for eq in equities
        if eq.financials.dividend_yield and eq.financials.dividend_yield > 0
    ]
    if dividend_yields:
        avg_dividend = sum(dividend_yields) / len(dividend_yields)
        print(
            f"   Average Dividend Yield: {avg_dividend:.2f}% (from {len(dividend_yields):,} companies)",
        )


def detect_temporal_anomalies(equities: list[CanonicalEquity]) -> None:
    """Detect temporal anomalies in price range data."""
    if not equities:
        print("⚠️  No equities available for temporal analysis")
        return

    print_separator("⏰ TEMPORAL & RANGE ANOMALY DETECTION")

    # 52-week range inversions (min > max)
    print("🔄 52-WEEK RANGE INVERSIONS:")
    inverted_ranges = [
        eq
        for eq in equities
        if eq.financials.fifty_two_week_min
        and eq.financials.fifty_two_week_max
        and eq.financials.fifty_two_week_min > eq.financials.fifty_two_week_max
    ]

    if inverted_ranges:
        print(
            f"   ⚠️  CRITICAL: {len(inverted_ranges):,} equities with min > max (impossible)",
        )
        for eq in inverted_ranges[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): Min ${eq.financials.fifty_two_week_min}, Max ${eq.financials.fifty_two_week_max}",
            )
    else:
        print("   ✅ No range inversions detected")

    # Stale data indicators (price == min == max)
    print("\n📊 STALE DATA INDICATORS:")
    stale_data = [
        eq
        for eq in equities
        if eq.financials.last_price
        and eq.financials.fifty_two_week_min
        and eq.financials.fifty_two_week_max
        and eq.financials.last_price == eq.financials.fifty_two_week_min
        and eq.financials.last_price == eq.financials.fifty_two_week_max
    ]

    if stale_data:
        print(
            f"   ⚠️  {len(stale_data):,} equities with identical price/min/max (possibly stale)",
        )
        for eq in stale_data[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): All values = ${eq.financials.last_price}",
            )
    else:
        print("   ✅ No obvious stale data patterns detected")

    # Price below 52-week min
    print("\n📉 PRICE BELOW 52-WEEK MINIMUM:")
    below_min = [
        eq
        for eq in equities
        if eq.financials.last_price
        and eq.financials.fifty_two_week_min
        and eq.financials.last_price < (eq.financials.fifty_two_week_min * Decimal("0.9"))
    ]

    if below_min:
        print(
            f"   ⚠️  {len(below_min):,} equities with price significantly below 52W min",
        )
        for eq in below_min[:3]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): Price ${eq.financials.last_price}, Min ${eq.financials.fifty_two_week_min}",
            )


def detect_identifier_format_issues(equities: list[CanonicalEquity]) -> None:
    """Validate identifier format compliance."""
    if not equities:
        print("⚠️  No equities available for identifier validation")
        return

    print_separator("🔍 IDENTIFIER FORMAT VALIDATION")

    # ISIN format: 2-letter country code + 9 alphanumeric + 1 check digit
    print("🌐 ISIN FORMAT VALIDATION:")
    isin_pattern = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
    invalid_isins = [
        eq
        for eq in equities
        if eq.identity.isin and not isin_pattern.match(eq.identity.isin)
    ]

    if invalid_isins:
        print(f"   ⚠️  {len(invalid_isins):,} equities with invalid ISIN format")
        for eq in invalid_isins[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): ISIN={eq.identity.isin}",
            )
    else:
        isin_count = len([eq for eq in equities if eq.identity.isin])
        print(f"   ✅ All {isin_count:,} ISINs have valid format")

    # CUSIP format: 9 characters (alphanumeric)
    print("\n🇺🇸 CUSIP FORMAT VALIDATION:")
    cusip_pattern = re.compile(r"^[A-Z0-9]{9}$")
    invalid_cusips = [
        eq
        for eq in equities
        if eq.identity.cusip and not cusip_pattern.match(eq.identity.cusip)
    ]

    if invalid_cusips:
        print(f"   ⚠️  {len(invalid_cusips):,} equities with invalid CUSIP format")
        for eq in invalid_cusips[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): CUSIP={eq.identity.cusip}",
            )
    else:
        cusip_count = len([eq for eq in equities if eq.identity.cusip])
        print(f"   ✅ All {cusip_count:,} CUSIPs have valid format")

    # CIK format: numeric, typically 10 digits
    print("\n📋 CIK FORMAT VALIDATION:")
    cik_pattern = re.compile(r"^[0-9]{1,10}$")
    invalid_ciks = [
        eq for eq in equities if eq.identity.cik and not cik_pattern.match(eq.identity.cik)
    ]

    if invalid_ciks:
        print(f"   ⚠️  {len(invalid_ciks):,} equities with invalid CIK format")
        for eq in invalid_ciks[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): CIK={eq.identity.cik}",
            )
    else:
        cik_count = len([eq for eq in equities if eq.identity.cik])
        print(f"   ✅ All {cik_count:,} CIKs have valid format")

    # Symbol anomalies
    print("\n🏷️  SYMBOL ANOMALY DETECTION:")
    very_long_symbols = [eq for eq in equities if len(eq.identity.symbol) > 10]
    special_char_symbols = [
        eq
        for eq in equities
        if not eq.identity.symbol.replace(".", "").replace("-", "").isalnum()
    ]

    if very_long_symbols:
        print(f"   ⚠️  {len(very_long_symbols):,} symbols >10 characters (unusual)")
        for eq in very_long_symbols[:3]:
            print(
                f"      • {eq.identity.symbol} ({eq.identity.name[:30]}...)",
            )

    if special_char_symbols:
        print(
            f"   ⚠️  {len(special_char_symbols):,} symbols with unusual characters",
        )
        for eq in special_char_symbols[:3]:
            print(
                f"      • {eq.identity.symbol} ({eq.identity.name[:30]}...)",
            )


def detect_cross_field_logic_issues(equities: list[CanonicalEquity]) -> None:
    """Detect logical inconsistencies between related fields."""
    if not equities:
        print("⚠️  No equities available for cross-field analysis")
        return

    print_separator("🔗 CROSS-FIELD LOGICAL CONSISTENCY")

    # Companies with price but no market cap
    print("💰 PRICE WITHOUT MARKET CAP:")
    price_no_cap = [
        eq
        for eq in equities
        if eq.financials.last_price
        and eq.financials.last_price > 0
        and not eq.financials.market_cap
    ]

    if price_no_cap:
        print(
            f"   ⚠️  {len(price_no_cap):,} equities with price but missing market cap",
        )
        for eq in price_no_cap[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): Price ${eq.financials.last_price}, Cap=None",
            )

    # Companies with market cap but no price
    print("\n📊 MARKET CAP WITHOUT PRICE:")
    cap_no_price = [
        eq
        for eq in equities
        if eq.financials.market_cap
        and eq.financials.market_cap > 0
        and not eq.financials.last_price
    ]

    if cap_no_price:
        print(
            f"   ⚠️  {len(cap_no_price):,} equities with market cap but missing price",
        )
        for eq in cap_no_price[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): Cap ${eq.financials.market_cap:,.0f}, Price=None",
            )

    # Missing both price and market cap
    print("\n❌ MISSING BOTH PRICE AND MARKET CAP:")
    missing_both = [
        eq
        for eq in equities
        if not eq.financials.last_price and not eq.financials.market_cap
    ]

    if missing_both:
        print(
            f"   ⚠️  {len(missing_both):,} equities missing both price and market cap",
        )
        # Check if these have other financial data
        with_other_data = [
            eq
            for eq in missing_both
            if eq.financials.revenue
            or eq.financials.trailing_pe
            or eq.financials.dividend_yield
        ]
        if with_other_data:
            print(
                f"      ⚠️  {len(with_other_data):,} of these have other financial metrics (orphaned data)",
            )

    # Partial 52-week range (only min or only max)
    print("\n📉 INCOMPLETE 52-WEEK RANGE:")
    partial_range = [
        eq
        for eq in equities
        if (eq.financials.fifty_two_week_min and not eq.financials.fifty_two_week_max)
        or (eq.financials.fifty_two_week_max and not eq.financials.fifty_two_week_min)
    ]

    if partial_range:
        print(
            f"   ⚠️  {len(partial_range):,} equities with only min or max (incomplete range)",
        )
        for eq in partial_range[:3]:
            min_val = (
                f"${eq.financials.fifty_two_week_min}"
                if eq.financials.fifty_two_week_min
                else "None"
            )
            max_val = (
                f"${eq.financials.fifty_two_week_max}"
                if eq.financials.fifty_two_week_max
                else "None"
            )
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): Min={min_val}, Max={max_val}",
            )


def detect_extreme_financial_values(equities: list[CanonicalEquity]) -> None:
    """Detect extreme or impossible financial values."""
    if not equities:
        print("⚠️  No equities available for extreme value detection")
        return

    print_separator("⚡ EXTREME FINANCIAL VALUE DETECTION")

    # Extreme dividend yields (>15%)
    print("💸 EXTREME DIVIDEND YIELDS:")
    extreme_dividends = [
        eq
        for eq in equities
        if eq.financials.dividend_yield and eq.financials.dividend_yield > 15
    ]

    if extreme_dividends:
        print(
            f"   ⚠️  {len(extreme_dividends):,} equities with dividend yield >15% (suspicious)",
        )
        for eq in sorted(
            extreme_dividends,
            key=lambda x: x.financials.dividend_yield,
            reverse=True,
        )[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): {eq.financials.dividend_yield:.2f}%",
            )

    # Very low stock prices (<$0.01)
    print("\n💵 PENNY STOCK DETECTION:")
    penny_stocks = [
        eq
        for eq in equities
        if eq.financials.last_price
        and eq.financials.last_price > 0
        and eq.financials.last_price < Decimal("0.01")
    ]

    if penny_stocks:
        print(
            f"   ⚠️  {len(penny_stocks):,} equities with price <$0.01 (extreme penny stocks)",
        )
        for eq in penny_stocks[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): ${eq.financials.last_price}",
            )

    # Extreme profit margins
    print("\n📊 EXTREME PROFIT MARGINS:")
    extreme_margins = [
        eq
        for eq in equities
        if eq.financials.profit_margin
        and (
            eq.financials.profit_margin > 100 or eq.financials.profit_margin < -100
        )
    ]

    if extreme_margins:
        print(
            f"   ⚠️  {len(extreme_margins):,} equities with profit margin >100% or <-100%",
        )
        for eq in sorted(
            extreme_margins,
            key=lambda x: abs(x.financials.profit_margin),
            reverse=True,
        )[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): {eq.financials.profit_margin:.2f}%",
            )

    # Negative price-to-book
    print("\n📖 NEGATIVE PRICE-TO-BOOK RATIO:")
    negative_pb = [
        eq
        for eq in equities
        if eq.financials.price_to_book and eq.financials.price_to_book < 0
    ]

    if negative_pb:
        print(
            f"   ⚠️  {len(negative_pb):,} equities with negative P/B ratio (possible distress)",
        )
        for eq in negative_pb[:5]:
            print(
                f"      • {eq.identity.name} ({eq.identity.symbol}): P/B={eq.financials.price_to_book:.2f}",
            )

    # Round number clustering (prices ending in .00)
    print("\n🎯 ROUND NUMBER CLUSTERING:")
    round_prices = [
        eq
        for eq in equities
        if eq.financials.last_price
        and eq.financials.last_price > 1
        and eq.financials.last_price % 1 == 0
    ]

    if round_prices:
        round_percentage = (len(round_prices) / len(equities)) * 100
        print(
            f"   ℹ️  {len(round_prices):,} equities with round dollar prices ({round_percentage:.1f}%)",
        )
        if round_percentage > 30:
            print(
                "      ⚠️  High concentration of round prices may indicate placeholder values",
            )


def main() -> None:
    """Main data integrity analysis function."""
    print("🚀 EQUITY AGGREGATOR - DATA INTEGRITY ANALYSIS TOOL")
    print(
        "This tool performs comprehensive data quality analysis and anomaly detection",
    )
    print(
        "on the canonical equity dataset to identify potential data integrity issues.",
    )

    # Load equity dataset
    equities = load_equity_dataset()

    if not equities:
        print("\n❌ Cannot continue analysis without equity data.")
        print(
            "💡 Tip: Make sure you have equity data available or run 'equity-aggregator download' first.",
        )
        sys.exit(1)

    # Data integrity analysis suite
    detect_financial_outliers(equities)
    detect_temporal_anomalies(equities)
    detect_extreme_financial_values(equities)
    analyze_data_consistency(equities)
    detect_identifier_format_issues(equities)
    detect_cross_field_logic_issues(equities)
    demo_data_quality_insights(equities)

    # Keep original currency and geographic analysis as they're useful for integrity
    print_separator("💱 CURRENCY DISTRIBUTION ANALYSIS")
    currencies = [eq.financials.currency for eq in equities if eq.financials.currency]
    if currencies:
        currency_counts = {}
        for currency in currencies:
            currency_counts[currency] = currency_counts.get(currency, 0) + 1

        for currency, count in sorted(
            currency_counts.items(),
            key=lambda x: x[1],
            reverse=True,
        ):
            percentage = (count / len(currencies)) * 100
            print(f"   {currency} {count:>4,} companies ({percentage:5.1f}%)")

    print_separator("🌍 GEOGRAPHIC DISTRIBUTION INSIGHTS")
    cusip_count = len([eq for eq in equities if eq.identity.cusip])
    isin_count = len([eq for eq in equities if eq.identity.isin])
    cik_count = len([eq for eq in equities if eq.identity.cik])

    print(f"   Companies with CUSIP (US focus): {cusip_count:,}")
    print(f"   Companies with ISIN (International): {isin_count:,}")
    print(f"   Companies with CIK (SEC registered): {cik_count:,}")

    print_separator("✅ DATA INTEGRITY ANALYSIS COMPLETE")
    print("Analysis Summary:")
    print(f"• Analyzed {len(equities):,} canonical equities")
    print("• Detected outliers and anomalies in financial metrics")
    print("• Identified temporal anomalies and range inversions")
    print("• Flagged extreme financial values and suspicious patterns")
    print("• Validated identifier format compliance (ISIN, CUSIP, CIK)")
    print("• Detected cross-field logical inconsistencies")
    print("• Assessed data completeness and quality across all fields")
    print(
        "\nUse this analysis to improve data quality and identify cleaning opportunities.",
    )
    print(
        "For more information, visit: https://github.com/gregorykelleher/equity-aggregator",
    )


if __name__ == "__main__":
    main()
