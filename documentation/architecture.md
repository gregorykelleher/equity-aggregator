# Architecture

An overview of how Equity Aggregator is organised. For local development setup
see [development](development.md); for the test suites see [testing](testing.md);
for what the project does and how to use it see the [README](../README.md).

## Project Structure

The codebase is organised following best practices, ensuring a clear separation between core domain logic, external adapters, and infrastructure components:

```
equity-aggregator/
├── src/equity_aggregator/           # Main application source
│   ├── cli/                         # Command-line interface
│   ├── domain/                      # Core business logic
│   │   ├── pipeline/                # Aggregation pipeline
│   │   │   └── transforms/          # Transformation stages
│   │   └── retrieval/               # Data download and retrieval
│   ├── adapters/data_sources/       # External data integrations
│   │   ├── discovery_feeds/         # Primary sources (Intrinio, SEC, Stock Analysis, TradingView, LSEG, XETRA)
│   │   └── enrichment_feeds/        # Enrichment feed integrations (Yahoo Finance, GLEIF)
│   ├── schemas/                     # Data validation and types
│   └── storage/                     # Database operations
├── data/                            # Database and cache
├── tests/                           # Unit and integration tests
├── docker-compose.yml               # Container configuration
└── pyproject.toml                   # Project metadata and dependencies
```

## Project Dependencies (Production)

The dependency listing is intentionally minimal, relying only on the following core packages:

| Dependency | Use case |
|------------|----------|
| pydantic | Type-safe models and validation for data |
| rapidfuzz | Fast fuzzy matching to reconcile data sourced by multiple data feeds |
| httpx | HTTP client with HTTP/2 support for data feed retrieval |
| openfigipy | OpenFIGI integration that anchors equities to a definitive identifier |
| platformdirs | Consistent storage paths for caches, logs, and data stores on every OS |

Keeping such a small set of dependencies reduces upgrade risk and maintenance costs, whilst still providing all the functionality required for comprehensive equity data aggregation and processing.

## Data Transformation Pipeline

The aggregation pipeline consists of six sequential transformation stages, each with a specific responsibility:

1. **Parse**: Extract and validate raw equity data from discovery feed data
2. **Convert**: Normalise currency values to USD reference currency using live exchange rates
3. **Identify**: Attach definitive identification metadata (i.e. Share Class FIGI) via OpenFIGI
4. **Group**: Group equities by Share Class FIGI, preserving all discovery feed sources
5. **Enrich**: Fetch enrichment data and perform single comprehensive merge of all sources (discovery + enrichment)
6. **Canonicalise**: Transform enriched data into the final canonical equity schema

## Clean Architecture Layers

The codebase adheres to clean architecture principles with distinct layers:

- **Domain Layer** (`domain/`): Contains core business logic, pipeline orchestration, and transformation rules independent of external dependencies
- **Adapter Layer** (`adapters/`): Implements interfaces for external systems including data feeds, APIs, and third-party services
- **Infrastructure Layer** (`storage/`, `cli/`): Handles system concerns, regarding database operations and command-line tooling
- **Schema Layer** (`schemas/`): Defines data contracts and validation rules using Pydantic models for type safety
