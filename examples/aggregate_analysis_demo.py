#!/usr/bin/env python3
"""
Aggregate Analysis Demo for equity-aggregator package.

This script demonstrates the retrieve_canonical_equities() function and provides
comprehensive analysis of the entire equity dataset, showcasing global insights
and aggregate statistics from the canonical equity collection.
"""

import sys

from equity_aggregator import CanonicalEquity, retrieve_canonical_equities


def print_separator(title: str) -> None:
    """Print a formatted section separator."""
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print(f"{'=' * 60}")


def demo_retrieve_all_equities() -> list[CanonicalEquity]:
    """Demonstrate retrieving all canonical equities."""
    print_separator("📊 RETRIEVING ALL CANONICAL EQUITIES")

    try:
        print("Fetching all canonical equities...")
        equities = retrieve_canonical_equities()

        print(f"✅ Successfully retrieved {len(equities)} equities")

        # Show some statistics
        sectors = [eq.financials.sector for eq in equities if eq.financials.sector]
        currencies = [
            eq.financials.currency for eq in equities if eq.financials.currency
        ]

        print(f"📈 Unique sectors: {len(set(sectors))}")
        print(f"💱 Unique currencies: {len(set(currencies))}")

        return equities

    except Exception as e:
        print(f"❌ Error retrieving equities: {e}")
        return []


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
            market_cap_companies, key=lambda x: x.financials.market_cap, reverse=True
        )[:10]
        for i, company in enumerate(top_companies, 1):
            print(
                f"   {i:2d}. {company.identity.name[:40]:40} ${company.financials.market_cap:>15,.0f}"
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
            sector_counts.items(), key=lambda x: x[1], reverse=True
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
            currency_counts.items(), key=lambda x: x[1], reverse=True
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
            f"   {field_name:10} {count:>5,} / {len(equities):,} ({percentage:5.1f}%)"
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
            f"   {i}. {equity.identity.name[:35]:35} (Score: {score}/10, Cap: {market_cap_str})"
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
            f"   Average Dividend Yield: {avg_dividend:.2f}% (from {len(dividend_yields):,} companies)"
        )


def main() -> None:
    """Main demonstration function."""
    print("🚀 EQUITY AGGREGATOR - AGGREGATE ANALYSIS DEMONSTRATION")
    print(
        "This demo showcases comprehensive analysis of the entire canonical equity dataset."
    )

    # Retrieve all equities
    equities = demo_retrieve_all_equities()

    if not equities:
        print("\n❌ Cannot continue demo without equity data.")
        print(
            "💡 Tip: Make sure you have equity data available or run 'equity-aggregator download' first."
        )
        sys.exit(1)

    # Comprehensive aggregate analysis
    demo_equity_exploration(equities)

    # Data quality and completeness insights
    demo_data_quality_insights(equities)

    print_separator("✅ AGGREGATE ANALYSIS COMPLETE")
    print("The retrieve_canonical_equities() function provides access to:")
    print(f"• {len(equities):,} canonical equities from global markets")
    print("• Comprehensive financial metrics and identity data")
    print("• Rich aggregate insights across sectors and geographies")
    print("• High-quality, normalized data ready for analysis")
    print("\nThis dataset enables powerful financial analysis and research.")
    print(
        "For more information, visit: https://github.com/gregorykelleher/equity-aggregator"
    )


if __name__ == "__main__":
    main()
