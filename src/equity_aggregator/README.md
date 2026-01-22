# Equity Aggregator Source Code Documentation

## Overview

The equity aggregator is a sophisticated financial data processing system that aggregates equity information from multiple discovery sources (LSEG, SEC, Stock Analysis, TradingView, XETRA and Intrinio) and enriches it with supplementary data from Yahoo Finance and the Global LEI Foundation.

## Architecture & Design

### Clean Architecture Layers

The codebase follows strict clean architecture principles with clear separation of concerns:

```
src/equity_aggregator/
├── cli/                    # Presentation Layer - User Interface
├── domain/                 # Business Logic Layer - Core Domain
│   ├── pipeline/           # Aggregation pipeline orchestration
│   ├── retrieval/          # Canonical equity download and retrieval
│   └── _utils/             # Domain-specific utilities
├── adapters/               # Infrastructure Layer - External Integrations
│   └── data_sources/       # Data source adapters
├── schemas/                # Data Models & Validation
│   └── feeds/              # Feed-specific schemas
└── storage/                # Persistence Layer - Database Operations
```

## Pipeline Architecture

### Core Data Flow

The system processes equity data through a six-stage async streaming pipeline:

```
Raw Data Sources → Parse → Convert → Identify → Group → Enrich → Canonicalise → Storage
```

### Pipeline Stages

#### 1. **Resolve**

Orchestrates parallel data fetching from discovery feeds:

- Fetches data from LSEG, SEC, Stock Analysis, TradingView, XETRA and Intrinio concurrently
- Combines all feed data into a single stream for processing
- Maintains feed source metadata for downstream processing

#### 2. **Parse**

Validates and structures raw feed data:

- Applies feed-specific schemas (`LsegFeedData`, `SecFeedData`, etc.)
- Filters out invalid records early in the pipeline
- Normalises data formats across different sources

#### 3. **Convert**

Standardises financial data to USD reference currency:

- Fetches real-time exchange rates for non-USD prices
- Converts prices while preserving original currency metadata
- Handles currency conversion failures gracefully

#### 4. **Identify**

Enriches records with global identification metadata:

- Queries OpenFIGI API for FIGI identifiers
- Creates globally unique equity identities

#### 5. **Group**

Groups equities by Share Class FIGI:

- Groups records with identical Share Class FIGI values
- Preserves all discovery feed source data for later merging
- Each group represents the same equity from multiple discovery sources
- Yields groups as `list[RawEquity]` for enrichment processing

#### 6. **Enrich**

Fetches enrichment data and performs comprehensive single merge:

- Fetches representative identifiers from discovery data sources
- Queries enrichment feeds (Yahoo Finance, Global LEI Foundation) using these identifiers
- Performs single merge of all sources (discovery + enrichment) for optimal data quality
- Applies controlled concurrency to enrichment feeds to respect API limits

#### 7. **Canonicalise**

Converts to final canonical schema:

- Maps all fields to `CanonicalEquity` format
- Applies final validation and type checking
- Prepares data for database persistence

## Asynchronous Processing

The pipeline uses asynchronous operations to process thousands of equity records efficiently:

### Key Implementation Features

**Parallel Data Fetching**

- All discovery feeds (LSEG, SEC, Stock Analysis, TradingView, XETRA, Intrinio) are fetched simultaneously

**Streaming Pipeline**

- Each transformation stage uses async generators to process records one at a time without loading everything into memory

**Controlled Concurrency**

- External API calls (OpenFIGI, Yahoo Finance, GLEIF) use semaphores to limit concurrent requests and respect rate limits
- Each enrichment feed has a configurable concurrency limit
- Fetch operations include timeout protection to prevent indefinite blocking

**Non-blocking Operations**

- HTTP requests and database operations run asynchronously to avoid blocking the main thread


Illustration of Pipeline Flow:

```python
async def aggregate_canonical_equities() -> list[CanonicalEquity]:

    # Resolve creates an async stream from multiple sources
    stream = resolve()

    # Each transform receives and returns an async iterator
    transforms = (parse, convert, identify, group, enrich, canonicalise)

    # Pipe stream through each transform sequentially
    for stage in transforms:
        stream = stage(stream)

    # Materialise the final result
    return [equity async for equity in stream]
```

## Schema System & Data Mapping

### Schema Hierarchy

```
schemas/
├── raw.py                    # RawEquity - intermediate pipeline format
├── canonical.py              # CanonicalEquity - final standardised format
├── types.py                  # Type definitions and validators
├── validators.py             # Reusable validators for identifiers and financial data
└── feeds/                    # Feed-specific data models
    ├── lseg_feed_data.py
    ├── sec_feed_data.py
    ├── stock_analysis_feed_data.py
    ├── tradingview_feed_data.py
    ├── gleif_feed_data.py
    ├── feed_validators.py
    ├── xetra_feed_data.py
    ├── yfinance_feed_data.py
    └── intrinio_feed_data.py
```

### Critical Role of Schemas

#### 1. **Data Validation at Boundaries**

Each feed has a dedicated Pydantic schema that:
- Validates incoming data structure and types
- Normalises field names and formats
- Filters out malformed records before pipeline processing
- Provides clear error messages for debugging

#### 2. **Type Safety Throughout Pipeline**
```python
# Strong typing ensures compile-time error detection
def parse(stream: AsyncIterable[FeedRecord]) -> AsyncIterator[RawEquity]:
    async for record in stream:
        # Pydantic validation ensures type safety
        validated = record.model.model_validate(record.raw_data)
```

#### 3. **Field Mapping & Normalisation**
```python
class LsegFeedData(BaseModel):
    issuername: str = Field(..., description="Company name")
    tidm: str = Field(..., description="Trading symbol")
    isin: str = Field(..., description="ISIN identifier")
    mics: list[str] | None = Field(..., description="Market identifiers")

    # Automatic field mapping from raw feed data
    @field_validator('symbol')
    def normalise_symbol(cls, v):
        return v.upper().strip()
```

### Data Transformation Flow

1. **Raw Feed Data** → Feed-specific schema validation
2. **Validated Feed Data** → Conversion to RawEquity format
3. **RawEquity** → Pipeline transformations (convert, identify, etc.)
4. **Enriched RawEquity** → Final canonicalisation
5. **CanonicalEquity** → Database persistence

## Discovery vs Enrichment Feeds

### Discovery Feeds (Primary Sources)

- **LSEG**: London Stock Exchange Group trading platform
- **SEC**: US Securities and Exchange Commission
- **Stock Analysis**: US equities with comprehensive financial metrics
- **TradingView**: US equities with comprehensive financial metrics
- **XETRA**: Deutsche Börse Stock Exchange
- **Intrinio**: US financial data API providing company, securities, and real-time quote data

**Characteristics**:

- Provide core equity identifier data (names, symbols, codes)
- Multiple discovery sources for the same equity are merged with enrichment data

### Enrichment Feeds (Supplementary Sources)

- **Yahoo Finance**: Market data and financial metrics
- **Global LEI Foundation**: ISIN->LEI mapping for Legal Entity Identifier enrichment

**Characteristics**

- Provides additional financial metrics (market cap, analyst ratings, etc.)
- Uses representative identifiers from discovery sources for look-up
- Applied after grouping but before final merge

## Equity Aggregator Components

### CLI Layer

- **main.py**: Entry point and argument parsing
- **dispatcher.py**: Command routing (seed, export, download)
- **parser.py**: Command-line interface definition
- **config.py**: Configuration management

### Domain Layer

- **pipeline/runner.py**: Main aggregation orchestrator
- **pipeline/resolve.py**: Multi-source data resolution
- **pipeline/transforms/**: Six-stage transformation pipeline
- **_utils/**: Domain-specific utilities (currency conversion, merging)

### Adapters Layer

- **data_sources/discovery_feeds/**: Primary data source integrations
- **data_sources/enrichment_feeds/**: Supplementary data integrations
- **data_sources/reference_lookup/**: External API services (OpenFIGI, exchange rates)

### Storage Layer

- **data_store.py**: SQLite database operations
- **cache.py**: Caching for API responses
- **export.py**: Data export functionality
