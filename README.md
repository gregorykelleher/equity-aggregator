# Equity Aggregator

[![Python Version](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/downloads/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Licence](https://img.shields.io/badge/license-MIT-green)](LICENCE.txt)
[![Validation Status](https://img.shields.io/github/actions/workflow/status/gregorykelleher/equity-aggregator/validate-push.yml?branch=master&label=build)](https://github.com/gregorykelleher/equity-aggregator/actions/workflows/validate-push.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/gregorykelleher/ce6ce2d7e3c247c34aba66dedcd7ede3/raw/coverage-badge.json)](https://github.com/gregorykelleher/equity-aggregator/actions/workflows/validate-push.yml)
[![Canonical Equities](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/gregorykelleher/ce6ce2d7e3c247c34aba66dedcd7ede3/raw/canonical-equities-badge.json)](https://github.com/gregorykelleher/equity-aggregator/releases/latest)

## Description

Equity Aggregator is a financial data tool that collects and normalises raw equity data from discovery sources (Intrinio, LSEG, SEC, XETRA, Stock Analysis, TradingView), before enriching it with third-party market vendor data from enrichment feeds (Yahoo Finance and Global LEI Foundation) to produce a unified canonical dataset of unique equities.

> [!NOTE]
> **"Canonical"** here means a single, authoritative record per equity: each company appears exactly once, normalised to a consistent schema and reconciled (deduplicated) across every source that lists it.

Altogether, this tool makes it possible to retrieve up-to-date information on over 18,000 equities from countries worldwide.

## What kind of Equity Data is available?

Equity Aggregator provides a structured profile for each equity in its canonical collection, defined through validated schemas that cleanly separate essential identity metadata from extensive financial metrics:

### Identity Metadata

The essential metadata that uniquely identifies each equity, with live population coverage across the canonical collection:

<!-- COVERAGE:START -->
| Field | Description | Populated |
|-------|-------------|-----------|
| name | Full company name | 100% |
| symbol | Trading symbol | 100% |
| share class figi | Definitive OpenFIGI identifier | 100% |
| isin | International Securities Identification Number | 51% |
| cusip | CUSIP identifier | 27% |
| cik | Central Index Key for SEC filings | 41% |
| lei | Legal Entity Identifier (ISO 17442) | 36% |
<!-- COVERAGE:END -->

> [!NOTE]
> The **Populated** column reports the share of canonical equities for which
> each field is present, refreshed automatically by the nightly pipeline.

### Financial Metrics

The supplementary market and fundamental metrics, grouped into the following categories:

| Category | Fields |
|----------|--------|
| Market Data | `last_price`, `market_cap`, `currency`, `market_volume` |
| Trading Venues | `mics` |
| Price Performance | `fifty_two_week_min`, `fifty_two_week_max`, `performance_1_year` |
| Share Structure | `shares_outstanding`, `share_float`, `dividend_yield` |
| Ownership | `held_insiders`, `held_institutions`, `short_interest` |
| Profitability | `profit_margin`, `gross_margin`, `operating_margin` |
| Cash Flow | `free_cash_flow`, `operating_cash_flow` |
| Valuation | `trailing_pe`, `price_to_book`, `trailing_eps` |
| Returns | `return_on_equity`, `return_on_assets` |
| Fundamentals | `revenue`, `revenue_per_share`, `ebitda`, `total_debt` |
| Classification | `industry`, `sector`, `analyst_rating` |

> [!NOTE]
> The OpenFIGI Share Class FIGI is the only definitive unique identifier for each equity in this dataset. While other identifiers like ISIN, CUSIP, CIK and LEI are also collected, they may not be universally available across all global markets or may have inconsistencies in formatting and coverage.
>
> OpenFIGI provides standardised, globally unique identifiers that work consistently across all equity markets and exchanges, hence its selection for Equity Aggregator.

## Equity Data at a Glance

A live view of the canonical dataset's scale, composition, market capitalisation, and internal consistency. All figures are computed over the latest daily snapshot and refreshed automatically by the nightly pipeline. Consistency ratios are measured over equities for which the relevant fields are present.

### Key Figures

The headline numbers for the latest snapshot:

<!-- STATS:START -->
| Metric | Value |
|--------|------:|
| Canonical equities | 18,459 |
| Sectors | 35 |
| Industries | 254 |
| Listing venues (MICs) | 10 |
| Daily snapshots | 92 |
| History since | 16/02/2026 |
| Aggregate market cap | $104.65T |
| Largest market cap | $4.93T |
| Median market cap | $331M |
| Price within 52-week range | 99% |
| Market cap within 25% of price × shares | 94% |
<!-- STATS:END -->

### Market Capitalisation Distribution

The number of canonical equities falling within each market capitalisation tier:

<!-- CAPDIST:START -->
| Cap tier | Canonical Equities |
|----------|---------:|
| Mega (> $200B) | 71 |
| Large ($10B–$200B) | 967 |
| Mid ($2B–$10B) | 1,227 |
| Small ($300M–$2B) | 1,657 |
| Micro (< $300M) | 3,677 |
<!-- CAPDIST:END -->

## Where does the Equity Data come from?

Equity Aggregator draws on two complementary kinds of data feed. Discovery feeds are the primary market sources that establish the universe of equities and their core identifiers, while enrichment feeds layer supplementary market data and fundamentals on top to complete each canonical profile.

### Discovery Feeds

Discovery feeds provide raw equity data from primary market sources:

| Source | Coverage | Description |
|----------|---------|-------------|
| 🇺🇸 Intrinio | United States | Intrinio |
| 🇺🇸 SEC | United States | Securities and Exchange Commission |
| 🇺🇸 Stock Analysis | International | Stock Analysis |
| 🇺🇸 TradingView | International | TradingView |
| 🇬🇧 LSEG | International | London Stock Exchange Group |
| 🇩🇪 XETRA | International | Deutsche Börse electronic trading platform |

### Enrichment Feeds

Enrichment feeds provide supplementary data to enhance the canonical equity dataset:

| Source | Description |
|--------|-------------|
| Yahoo Finance | Market data, financial metrics, and equity metadata |
| GLEIF | Legal Entity Identifier (LEI) lookups via the Global LEI Foundation |

## How do I get started?

### Package Installation

Equity Aggregator is available to download via `pip` as the `equity-aggregator` package:

```bash
pip install equity-aggregator
```

### Python API

Equity Aggregator exposes a small, focused public API for integration. It automatically detects and downloads the latest canonical equity dataset from remote sources when needed, so users always work with up-to-date data.

#### Retrieving All Equities

The `retrieve_canonical_equities()` function downloads and returns the complete dataset of canonical equities. This function automatically handles data retrieval and local database management, downloading the latest canonical equity dataset when needed.

```python
from equity_aggregator import retrieve_canonical_equities

# Retrieve all canonical equities (downloads if database doesn't exist locally)
equities = retrieve_canonical_equities()
print(f"Retrieved {len(equities)} canonical equities")

# Iterate through equities
for equity in equities[:3]:  # Show first 3
    print(f"{equity.identity.symbol}: {equity.identity.name}")
```

**Example Output:**
```
Retrieved 18056 canonical equities
AAPL: APPLE INC
MSFT: MICROSOFT CORP
GOOGL: ALPHABET INC
```

#### Retrieving Individual Equities

The `retrieve_canonical_equity()` function retrieves a single equity by its Share Class FIGI identifier. This function works independently and automatically downloads data if needed.

```python
from equity_aggregator import retrieve_canonical_equity

# Retrieve a specific equity by FIGI identifier
apple_equity = retrieve_canonical_equity("BBG000B9XRY4")

print(f"Company: {apple_equity.identity.name}")
print(f"Symbol: {apple_equity.identity.symbol}")
print(f"Market Cap: ${apple_equity.financials.market_cap:,.0f}")
print(f"Currency: {apple_equity.financials.currency}")
```

**Example Output:**
```
Company: APPLE INC
Symbol: AAPL
Market Cap: $3,500,000,000,000
Currency: USD
```

#### Retrieving Historical Equity Data

The `retrieve_canonical_equity_history()` function returns historical daily snapshots for a given equity, optionally filtered by date range. Each nightly pipeline run appends a new snapshot, building a time series of financial metrics.

```python
from equity_aggregator import retrieve_canonical_equity_history

# Retrieve all historical snapshots for Apple
snapshots = retrieve_canonical_equity_history("BBG000B9XRY4")
print(f"Retrieved {len(snapshots)} snapshots")

# Filter by date range (inclusive, YYYY-MM-DD)
recent = retrieve_canonical_equity_history(
    "BBG000B9XRY4",
    from_date="2026-03-01",
    to_date="2026-03-31",
)

for snapshot in recent:
    print(f"{snapshot.snapshot_date}: {snapshot.financials.last_price}")
```

**Example Output:**
```
Retrieved 90 snapshots
2026-02-16: 243.85
2026-02-17: 245.00
2026-02-18: 244.12
```

> [!TIP]
> All retrieval functions work independently and download the database if needed, so there's no need to call `retrieve_canonical_equities()` first. They're synchronous too; to call one from inside a running event loop, offload it with `asyncio.to_thread` (the blocking lookup and any first-run download then run off the loop):
>
> ```python
> import asyncio
> from equity_aggregator import retrieve_canonical_equity
>
> async def main() -> None:
>     equity = await asyncio.to_thread(retrieve_canonical_equity, "BBG000B9XRY4")
> ```

#### Data Models

All data is returned as type-safe Pydantic models, ensuring data validation and integrity. The `CanonicalEquity` model provides structured access to identity metadata and financial metrics.

```python
from equity_aggregator import retrieve_canonical_equity, CanonicalEquity

equity: CanonicalEquity = retrieve_canonical_equity("BBG000B9XRY4")

# Access identity metadata
identity = equity.identity
print(f"FIGI: {identity.share_class_figi}")
print(f"ISIN: {identity.isin}")
print(f"CUSIP: {identity.cusip}")

# Access financial metrics
financials = equity.financials
print(f"P/E Ratio: {financials.trailing_pe}")
print(f"Market Cap: {financials.market_cap}")
```

**Example Output:**
```
FIGI: BBG000B9XRY4
ISIN: US0378331005
CUSIP: 037833100
P/E Ratio: 28.5
Market Cap: 3500000000000
```

### CLI Usage

Once installed, Equity Aggregator provides a command-line interface for managing equity data. The CLI offers two main commands:

- **seed** - Aggregate and populate the local database with fresh equity data
- **download** - Download the latest canonical equity database from remote repository

Run `equity-aggregator --help` for more information:

```bash
usage: equity-aggregator [-h] [-v] [-d] [-q] {seed,download} ...

aggregate and download canonical equity data

options:
  -h, --help            show this help message and exit
  -v, --verbose         enable verbose logging (INFO level)
  -d, --debug           enable debug logging (DEBUG level)
  -q, --quiet           quiet mode - only show warnings and errors

commands:
  available operations

  {seed,download}
    seed                aggregate enriched canonical equity data sourced from data feeds
    download            download latest canonical equity data from remote repository

use 'equity-aggregator <command> --help' for help
```

#### Download Command

The `download` command retrieves the latest canonical equity database from GitHub Releases, eliminating the need to run the full aggregation pipeline via `seed` locally. This command:

- Downloads the compressed database (`data_store.db.xz`) from the latest nightly build
- Decompresses and atomically replaces the local database
- Provides access to 18,000+ equities with full historical snapshots

> [!TIP]
> **Optional: Increase Rate Limits**
>
> Set `GITHUB_TOKEN` to increase download limits from 60/hour to 5,000/hour:
> ```bash
> export GITHUB_TOKEN="your_personal_access_token_here"
> ```
> Create a token at [GitHub Settings](https://github.com/settings/tokens) - no special scopes needed. Recommended for frequent downloads or CI/CD pipelines.

#### Seed Command

The `seed` command executes the complete equity aggregation pipeline, collecting raw data from discovery sources (Intrinio, SEC, Stock Analysis, TradingView, LSEG, XETRA), enriching it with market data from enrichment feeds, and storing the processed results in the local database. This command runs the full transformation pipeline to create a fresh canonical equity dataset.

This command requires that the following API keys are set prior:

```bash
export EXCHANGE_RATE_API_KEY="your_key_here"
export OPENFIGI_API_KEY="your_key_here"
```

```bash
# Run the main aggregation pipeline (requires API keys)
equity-aggregator seed
```

> [!IMPORTANT]
> Note that the `seed` command processes thousands of equities and is intentionally rate-limited to respect external API constraints. A full run typically takes 60 minutes depending on network conditions and API response times.
>
> This is mitigated by the automated nightly CI pipeline that runs `seed` and publishes the latest canonical equity dataset. Users can download this pre-built data using `equity-aggregator download` instead of running the full aggregation pipeline locally.

### Data Storage

Equity Aggregator automatically stores its database (i.e. `data_store.db`) in system-appropriate locations using platform-specific directories:

- **macOS**: `~/Library/Application Support/equity-aggregator/`
- **Windows**: `%APPDATA%\equity-aggregator\`
- **Linux**: `~/.local/share/equity-aggregator/`

Log files are also automatically written to the system-appropriate log directory:

- **macOS**: `~/Library/Logs/equity-aggregator/`
- **Windows**: `%LOCALAPPDATA%\equity-aggregator\Logs\`
- **Linux**: `~/.local/state/equity-aggregator/`

This ensures consistent integration with the host operating system's data and log management practices.

## Documentation

Further documentation lives in the [`documentation/`](documentation) directory:

- **[Architecture](documentation/architecture.md)** - project structure, dependencies, the transformation pipeline, and the clean-architecture layers
- **[Testing](documentation/testing.md)** - the unit and live test suites, conventions, and coverage configuration
- **[Development](documentation/development.md)** - local setup, environment variables, running the tests, and Docker usage

## Limitations

### Data Depth and Scope

- Equity Aggregator is intrinsically bound by the quality and coverage of its upstream discovery and enrichment feeds. Data retrieved and processed by Equity Aggregator reflects the quality and scope inherited from these data sources.

- Normalisation, outlier detection, coherency validation checks and other statistical techniques catch most upstream issues, yet occasional gaps or data aberrations can persist and should be handled defensively by downstream consumers.

### Venue-Specific Financial Metrics and Secondary Listings

- Certain equities may be sourced solely from secondary listings (e.g. OTC Markets or cross-listings) rather than their primary exchange. This occurs when the primary venue's data is unavailable from equity-aggregator's data sources.

- Company-level metrics such as `market_cap`, `shares_outstanding`, `revenue`, and valuation ratios remain accurate regardless of sourcing venue, as they reflect the underlying company rather than the trading venue.

- However, venue-specific metrics, particularly `market_volume` reflect trading activity only on the captured venues, not _total_ market-wide volume. An equity showing low volume may simply indicate minimal OTC activity despite substantial trading on its primary exchange.

- Attention should therefore be paid to the  `mics` field, indicating which Market Identifier Codes are represented in the data (i.e. whether it's the equity's primary exchange MIC or a secondary listing).

### Data Update Cadence

- Equity Aggregator publishes nightly batch snapshots and does not aim to serve as a real-time market data service. The primary objective of Equity Aggregator is to provide equity identification metadata with limited financial metrics for fundamental analysis.

- Downstream services should therefore treat Equity Aggregator as a discovery catalogue, using its authoritative identifiers to discover equities and then poll specialised market data providers for time-sensitive pricing metrics.

- Delivering real-time quotes directly through Equity Aggregator would be infeasible because the upstream data sources enforce strict rate limits and the pipeline is network-bound; attempting live polling would exhaust quotas quickly and degrade reliability for all consumers.

### Unadjusted Historical Data

- Historical snapshots record raw financial metrics as observed on the date of capture. Prices, shares outstanding, and other per-share figures are **not adjusted** for corporate actions such as stock splits, reverse splits, share dilution, spin-offs, mergers, or dividend reinvestments.

- This means that comparing a snapshot from before a 4-for-1 stock split with one taken after it will show an apparent price drop of roughly 75%, even though no real loss of value occurred. Similarly, metrics like `shares_outstanding`, `trailing_eps`, and `revenue_per_share` can shift discontinuously across corporate action boundaries without reflecting any underlying change in the company's fundamentals.

- Consumers requiring split-adjusted or corporate-action-adjusted time series for backtesting, charting, or quantitative analysis should source adjusted data from a dedicated market data provider. The historical snapshots in Equity Aggregator are best suited for point-in-time discovery and broad trend observation rather than precise longitudinal analysis.

### Single Identifier Authority

- Share Class FIGI remains the authoritative identifier because OpenFIGI supplies globally unique, deduplicated mappings across discovery feeds. Other identifiers such as ISIN, CUSIP, CIK or LEI depend on regional registries, are frequently absent for specific markets, and are prone to formatting discrepancies, so they should be treated as supplementary identifiers only.

### Performance

- The end-to-end aggregation pipeline is network-bound and respects vendor rate limits, meaning a full `seed` run can take close to an hour in steady-state conditions. This is mitigated by comprehensive caching used throughout the application, as well as the automated nightly CI pipeline that publishes the latest canonical equity dataset, made available via `download`.

### External Service Reliance

- As the entirety of Equity Aggregator is built around the use of third-party APIs for discovery, enrichment, as well as other services, its robustness is fundamentally fragile. Upstream outages, schema shifts, bot protection revocations, API churn and rate-limit policy changes can easily degrade the pipeline without warning, with remediation often relying on vendor response times outside of the project's remit.

- As this is an inherent architectural constraint, the only viable response centres on providing robust mitigation controls. Monitoring, retry strategies and graceful degradation paths lessen the impact; they cannot eliminate the dependency risk entirely.

## Disclaimer

> [!IMPORTANT]
> **Important Legal Notice**
>
> This software aggregates data from various third-party sources including Intrinio, Yahoo Finance, LSEG trading platform, SEC, Stock Analysis, and XETRA. Equity Aggregator is **not** affiliated, endorsed, or vetted by any of these organisations.
>
> **Data Sources and Terms:**
>
> - **Yahoo Finance**: This tool uses Yahoo's publicly available APIs. Refer to [Yahoo!'s terms of use](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm) for details on your rights to use the actual data downloaded. Yahoo! finance API is intended for personal use only.
> - **Intrinio**: This tool requires a valid Intrinio subscription and API key. Refer to [Intrinio's terms of use](https://about.intrinio.com/terms) for permitted usage, rate limits, and redistribution policies.
> - **Market Data**: All market data is obtained from publicly available sources and is intended for research and educational purposes only.
>
> **Usage Responsibility:**
>
> - Users are responsible for complying with all applicable terms of service and legal requirements of the underlying data providers
> - This software is provided for informational and educational purposes only
> - No warranty is provided regarding data accuracy, completeness, or fitness for any particular purpose
> - Users should independently verify any data before making financial decisions
>
> **Commercial Use:** Users intending commercial use should review and comply with the terms of service of all underlying data providers.
