# Equity Aggregator

[![Python Version](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/downloads/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Licence](https://img.shields.io/badge/license-MIT-green)](LICENCE.txt)
[![Validation Status](https://img.shields.io/github/actions/workflow/status/gregorykelleher/equity-aggregator/validate-push.yml?branch=master&label=build)](https://github.com/gregorykelleher/equity-aggregator/actions/workflows/validate-push.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/gregorykelleher/ce6ce2d7e3c247c34aba66dedcd7ede3/raw/coverage-badge.json)](https://github.com/gregorykelleher/equity-aggregator/actions/workflows/validate-push.yml)

## Description

Equity Aggregator is a financial data tool that collects and normalises raw equity data from discovery sources (Intrinio, LSEG, SEC, XETRA, Stock Analysis, TradingView), before enriching it with third-party market vendor data from enrichment feeds (Yahoo Finance and Global LEI Foundation) to produce a unified canonical dataset of unique equities.

Altogether, this tool makes it possible to retrieve up-to-date information on over 15,000+ equities from countries worldwide.

### Discovery Feeds

Discovery feeds provide raw equity data from primary market sources:

| Source | Coverage | Description |
|----------|---------|-------------|
| 🇺🇸 Intrinio | United States | Intrinio - US-listed equities |
| 🇬🇧 LSEG | International | London Stock Exchange Group - Global equities |
| 🇺🇸 SEC | United States | Securities and Exchange Commission - US-listed equities |
| 🇺🇸 Stock Analysis | United States | Stock Analysis - Global equities |
| 🇺🇸 TradingView | United States | TradingView - US-listed equities |
| 🇩🇪 XETRA | International | Deutsche Börse electronic trading platform - Global listed equities |

### Enrichment Feeds

Enrichment feeds provide supplementary data to enhance the canonical equity dataset:

| Source | Description |
|--------|-------------|
| Yahoo Finance | Market data, financial metrics, and equity metadata |
| GLEIF | Legal Entity Identifier (LEI) lookups via the Global LEI Foundation |

## What kind of Equity Data is available?

Equity Aggregator provides a comprehensive profile for each equity in its canonical collection, structured through validated schemas that ensure clean separation between essential identity metadata and extensive financial metrics:

### Identity Metadata
| Field | Description |
|-------|-------------|
| **name** | Full company name |
| **symbol** | Trading symbol |
| **share class figi** | Definitive OpenFIGI identifier |
| **isin** | International Securities Identification Number |
| **cusip** | CUSIP identifier |
| **cik** | Central Index Key for SEC filings |
| **lei** | Legal Entity Identifier (ISO 17442) |

### Financial Metrics
| Category | Fields |
|----------|--------|
| **Market Data** | `last_price`, `market_cap`, `currency`, `market_volume` |
| **Trading Venues** | `mics`
| **Price Performance** | `fifty_two_week_min`, `fifty_two_week_max`, `performance_1_year` |
| **Share Structure** | `shares_outstanding`, `share_float`, `dividend_yield` |
| **Ownership** | `held_insiders`, `held_institutions`, `short_interest` |
| **Profitability** | `profit_margin`, `gross_margin`, `operating_margin` |
| **Cash Flow** | `free_cash_flow`, `operating_cash_flow` |
| **Valuation** | `trailing_pe`, `price_to_book`, `trailing_eps` |
| **Returns** | `return_on_equity`, `return_on_assets` |
| **Fundamentals** | `revenue`, `revenue_per_share`, `ebitda`, `total_debt` |
| **Classification** | `industry`, `sector`, `analyst_rating` |

> [!NOTE]
> The OpenFIGI Share Class FIGI is the only definitive unique identifier for each equity in this dataset. While other identifiers like ISIN, CUSIP, CIK and LEI are also collected, they may not be universally available across all global markets or may have inconsistencies in formatting and coverage.
>
> OpenFIGI provides standardised, globally unique identifiers that work consistently across all equity markets and exchanges, hence its selection for Equity Aggregator.

## How do I get started?

### Package Installation

Equity Aggregator is available to download via `pip` as the `equity-aggregator` package:

```bash
pip install equity-aggregator
```

### Python API

Equity Aggregator exposes a focused public API that enables seamless integration opportunities. The API automatically detects and downloads the latest canonical equity dataset from remote sources when needed, ensuring users always work with up-to-date data.

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
Retrieved 10000 canonical equities
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
print(f"Currency: {apple_equity.pricing.currency}")
```

**Example Output:**
```
Company: APPLE INC
Symbol: AAPL
Market Cap: $3,500,000,000,000
Currency: USD
```

#### Data Models

All data is returned as type-safe Pydantic models, ensuring data validation and integrity. The `CanonicalEquity` model provides structured access to identity metadata, pricing information, and financial metrics.

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

> [!NOTE]
> Both functions work independently - `retrieve_canonical_equity()` automatically downloads data if needed, so there's no requirement to call `retrieve_canonical_equities()` first.

### CLI Usage

Once installed, Equity Aggregator provides a comprehensive command-line interface for managing equity data operations. The CLI offers three main commands:

- **seed** - Aggregate and populate the local database with fresh equity data
- **export** - Export the local canonical equity database to compressed JSONL format
- **download** - Download the latest canonical equity data from remote repository

Run `equity-aggregator --help` for more information:

```bash
usage: equity-aggregator [-h] [-v] [-d] [-q] {seed,export,download} ...

aggregate, download, and export canonical equity data

options:
  -h, --help            show this help message and exit
  -v, --verbose         enable verbose logging (INFO level)
  -d, --debug           enable debug logging (DEBUG level)
  -q, --quiet           quiet mode - only show warnings and errors

commands:
  Available operations

  {seed,export,download}
    seed                aggregate enriched canonical equity data sourced from data feeds
    export              export local canonical equity data to compressed JSONL format
    download            download latest canonical equity data from remote repository

Use 'equity-aggregator <command> --help' for help
```

#### Download Command

The `download` command retrieves the latest pre-processed canonical equity dataset from GitHub Releases, eliminating the need to run the full aggregation pipeline via `seed` locally. This command:

- Downloads compressed equity data (`canonical_equities.jsonl.gz`) from the latest nightly build
- Automatically rebuilds the database locally from the downloaded data
- Provides access to 15,000+ equities with immediate effect

> [!TIP]
> **Optional: Increase Rate Limits**
>
> Set `GITHUB_TOKEN` to increase download limits from 60/hour to 5,000/hour:
> ```bash
> export GITHUB_TOKEN="your_personal_access_token_here"
> ```
> Create a token at [GitHub Settings](https://github.com/settings/tokens) - no special scopes needed. Recommended for frequent downloads or CI/CD pipelines.

#### Export Command

The `export` command extracts canonical equity data from the local database and exports it as compressed JSONL (JSON Lines) format. It reads all canonical equities from the local database and exports the data to `canonical_equities.jsonl.gz` in the specified output directory.

This creates a portable, standardised dataset suitable for analysis, sharing, or backup while preserving all equity metadata and financial metrics in structured JSON format.

```bash
# Export aggregated data to compressed JSON in specified directory
equity-aggregator export --output-dir ~/Downloads
equity-aggregator export --output-dir /path/to/export/location
```

#### Seed Command

The `seed` command executes the complete equity aggregation pipeline, collecting raw data from discovery sources (LSEG, SEC, XETRA, Stock Analysis, TradingView), enriching it with market data from enrichment feeds, and storing the processed results in the local database. This command runs the full transformation pipeline to create a fresh canonical equity dataset.

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

### Development Setup

Follow these steps to set up the development environment for the Equity Aggregator application.

#### Prerequisites

Before starting, ensure the following conditions have been met:

- **Python 3.12+**: The application requires Python 3.12 or later
- **uv**: Python package manager
- **Git**: For version control
- **Docker** (optional): For containerised development and deployment

#### Environment Setup

#### Clone the repository:

```bash
git clone <repository-url>
cd equity-aggregator
```

#### Create and activate virtual environment:

```bash
# Create virtual environment with Python 3.12
uv venv --python 3.12

# Activate the virtual environment
source .venv/bin/activate
```

#### Install dependencies:

```bash
# Install all dependencies and sync workspace
uv sync --all-packages
```

#### Environment Variables

The application requires API keys for external data sources. A template file `.env_example` is provided in the project root for guidance.

#### Copy the example environment file:

```bash
cp .env_example .env
```

#### Configure API keys by editing `.env` and adding the following:

#### Mandatory Keys:

- `EXCHANGE_RATE_API_KEY` - Required for currency conversion
  - Retrieve from: [ExchangeRate-API](https://exchangerate-api.com/)
  - Used for converting equity prices to USD reference currency

- `OPENFIGI_API_KEY` - Required for equity identification
  - Retrieve from: [OpenFIGI](https://www.openfigi.com/api)
  - Used for equity identification and deduplication

#### Optional Keys:

- `INTRINIO_API_KEY` - For Intrinio discovery feed
  - Retrieve from: [Intrinio](https://intrinio.com/)
  - Provides US equity data with comprehensive quote information

- `GITHUB_TOKEN` - For increased GitHub API rate limits
  - Retrieve from: [GitHub Settings](https://github.com/settings/tokens)
  - Increases release download rate limits from 60/hour to 5,000/hour
  - No special scopes required for public repositories

#### Verify Installation

This setup provides access to the full development environment with all dependencies, testing frameworks, and development tools configured.

It should therefore be possible to verify correct operation by running the following commands using `uv`:

```bash
# Verify the application is properly installed
uv run equity-aggregator --help

# Run unit tests to confirm functionality
uv run pytest -m unit

# Check code formatting and linting
uv run ruff check src

# Test API key configuration
uv run --env-file .env equity-aggregator seed
```

#### Running Tests

Run the test suites using the following commands:

```bash
# Run all unit tests
uv run pytest -m unit

# Run with verbose output
uv run pytest -m unit -v

# Run with coverage reporting
uv run pytest -m unit --cov=equity_aggregator --cov-report=term-missing

# Run with detailed coverage and HTML report
uv run pytest -vvv -m unit --cov=equity_aggregator --cov-report=term-missing --cov-report=html

# Run live tests (requires API keys and internet connection)
uv run pytest -m live

# Run all tests
uv run pytest
```

#### Code Quality and Linting

The project uses `ruff` for static analysis, code formatting, and linting:

```bash
# Format code automatically
uv run ruff format

# Check for linting issues
uv run ruff check

# Fix auto-fixable linting issues
uv run ruff check --fix

# Check formatting without making changes
uv run ruff format --check

# Run linting on specific directory
uv run ruff check src
```

> [!NOTE]
> Ruff checks only apply to the `src` directory - tests are excluded from formatting and linting requirements.

### Docker

The Equity Aggregator project can optionally be containerised using Docker. The `docker-compose.yml` defines the equity-aggregator service.

#### Docker Commands

```bash
# Build and run the container
docker compose up --build

# Run in background
docker compose up -d

# Stop and remove containers
docker compose down

# View container logs
docker logs equity-aggregator

# Execute commands in running container
docker compose exec equity-aggregator bash
```

> [!NOTE]
> The Docker setup uses named volumes for persistent database storage and automatically handles all directory creation and permissions.

## Architecture

### Project Structure

The codebase is organised following best practices, ensuring a clear separation between core domain logic, external adapters, and infrastructure components:

```
equity-aggregator/
├── src/equity_aggregator/           # Main application source
│   ├── cli/                         # Command-line interface
│   ├── domain/pipeline/             # Core aggregation pipeline
│   │   └── transforms/              # Transformation stages
│   ├── adapters/data_sources/       # External data integrations
│   │   ├── discovery_feeds/         # Primary sources (Intrinio, LSEG, SEC, Stock Analysis, TradingView, XETRA)
│   │   └── enrichment_feeds/        # Enrichment feed integrations (Yahoo Finance)
│   ├── schemas/                     # Data validation and types
│   └── storage/                     # Database operations
├── data/                            # Database and cache
├── tests/                           # Unit and integration tests
├── docker-compose.yml               # Container configuration
└── pyproject.toml                   # Project metadata and dependencies
```

### Project Dependencies (Production)

The dependency listing is intentionally minimal, relying only on the following core packages:

| Dependency | Use case |
|------------|----------|
| pydantic | Type-safe models and validation for data |
| rapidfuzz | Fast fuzzy matching to reconcile data sourced by multiple data feeds |
| httpx | HTTP client with HTTP/2 support for data feed retrieval |
| openfigipy | OpenFIGI integration that anchors equities to a definitive identifier |
| platformdirs | Consistent storage paths for caches, logs, and data stores on every OS |

Keeping such a small set of dependencies reduces upgrade risk and maintenance costs, whilst still providing all the functionality required for comprehensive equity data aggregation and processing.

### Data Transformation Pipeline

The aggregation pipeline consists of six sequential transformation stages, each with a specific responsibility:

1. **Parse**: Extract and validate raw equity data from discovery feed data
2. **Convert**: Normalise currency values to USD reference currency using live exchange rates
3. **Identify**: Attach definitive identification metadata (i.e. Share Class FIGI) via OpenFIGI
4. **Group**: Group equities by Share Class FIGI, preserving all discovery feed sources
5. **Enrich**: Fetch enrichment data and perform single comprehensive merge of all sources (discovery + enrichment)
6. **Canonicalise**: Transform enriched data into the final canonical equity schema

### Clean Architecture Layers

The codebase adheres to clean architecture principles with distinct layers:

- **Domain Layer** (`domain/`): Contains core business logic, pipeline orchestration, and transformation rules independent of external dependencies
- **Adapter Layer** (`adapters/`): Implements interfaces for external systems including data feeds, APIs, and third-party services
- **Infrastructure Layer** (`storage/`, `cli/`): Handles system concerns, regarding database operations and command-line tooling
- **Schema Layer** (`schemas/`): Defines data contracts and validation rules using Pydantic models for type safety

### Test Suites

The project maintains two distinct test suites, each serving a specific purpose in the testing strategy:

#### Unit Tests (`-m unit`)

Unit tests provide comprehensive coverage of all internal application logic. These tests are fully isolated and do not make any external network calls, ensuring fast and deterministic execution. The suite contains over 1,000 test cases and executes in under 30 seconds, enforcing a **minimum coverage threshold of 99%** with the goal of maintaining **100% coverage** across all source code.

Unit tests follow strict conventions:
- **AAA Pattern**: All tests are structured using the Arrange-Act-Assert pattern for clarity and consistency
- **Single Assertion**: Each test case contains exactly one assertion, ensuring focused and maintainable tests
- **No Mocking**: Monkey-patching and Python mocking techniques (e.g. `monkeypatch`, `unittest.mock`) are strictly forbidden, promoting testable design through dependency injection and explicit interfaces

#### Live Tests (`-m live`)

Live tests serve as **sanity tests** that validate external API endpoints are available and responding correctly. These tests hit real external services to verify that:
- Discovery and enrichment feed endpoints are accessible
- API response schemas match expected Pydantic models
- Authentication and rate limiting are functioning as expected

Live tests act as an early warning system, catching upstream API changes or outages before they impact the main aggregation pipeline.

#### Continuous Integration

Both test suites are executed as part of the GitHub Actions CI pipeline:

- **[validate-push.yml](.github/workflows/validate-push.yml)**: Runs unit tests with coverage enforcement on every push to master, ensuring code quality and the 99% coverage threshold are maintained
- **[publish-build-release.yml](.github/workflows/publish-build-release.yml)**: Runs live sanity tests before executing the nightly aggregation pipeline, validating that all external APIs are operational before publishing a new release

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
