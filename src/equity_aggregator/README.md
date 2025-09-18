# Equity Aggregator Source Code Documentation

## Overview

The equity aggregator is a sophisticated financial data processing system that aggregates equity information from multiple authoritative sources (Euronext, LSE, SEC, XETRA) and enriches it with supplementary data from Yahoo Finance.

## Architecture & Design

### Clean Architecture Layers

The codebase follows strict clean architecture principles with clear separation of concerns:

```
src/equity_aggregator/
├── cli/                    # Presentation Layer - User Interface
├── domain/                 # Business Logic Layer - Core Domain
│   ├── pipeline/           # Aggregation pipeline orchestration
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
Raw Data Sources → Parse → Convert → Identify → Deduplicate → Enrich → Canonicalise → Storage
```

### Pipeline Stages

#### 1. **Resolve**

Orchestrates parallel data fetching from authoritative feeds:

- Fetches data from Euronext, LSE, SEC, and XETRA concurrently
- Combines all feed data into a single stream for processing
- Maintains feed source metadata for downstream processing

#### 2. **Parse**

Validates and structures raw feed data:

- Applies feed-specific schemas (`EuronextFeedData`, `LseFeedData`, etc.)
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
- Adds CUSIP, ISIN, and other standard identifiers
- Creates globally unique equity identities

#### 5. **Deduplicate**

Merges duplicate equity records by FIGI identifier:

- Groups records with identical share_class_figi values
- Uses fuzzy matching to merge similar company names within groups
- Preserves the most complete data when merging

#### 6. **Enrich**

Adds supplementary data from enrichment feeds:

- Fetches additional data from Yahoo Finance
- Only fills missing fields from authoritative sources
- Preserves data integrity and source hierarchy
- Applies controlled concurrency to respect API limits

#### 7. **Canonicalise**

Converts to final canonical schema:

- Maps all fields to `CanonicalEquity` format
- Applies final validation and type checking
- Prepares data for database persistence

## Asynchronous Processing

The pipeline uses asynchronous operations to process thousands of equity records efficiently:

### Key Implementation Features

**Parallel Data Fetching**

- All authoritative feeds (Euronext, LSE, SEC, XETRA) are fetched simultaneously

**Streaming Pipeline**

- Each transformation stage uses async generators to process records one at a time without loading everything into memory

**Controlled Concurrency**

- External API calls (OpenFIGI, Yahoo Finance) use semaphores to limit concurrent requests and respect rate limits

**Non-blocking Operations**

- HTTP requests and database operations run asynchronously to avoid blocking the main thread


Illustration of Pipeline Flow:

```python
async def aggregate_canonical_equities() -> list[CanonicalEquity]:
    
    # Resolve creates an async stream from multiple sources
    stream = resolve()

    # Each transform receives and returns an async iterator
    transforms = (parse, convert, identify, deduplicate, enrich, canonicalise)

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
└── feeds/                    # Feed-specific data models
    ├── euronext_feed_data.py
    ├── lse_feed_data.py
    ├── sec_feed_data.py
    ├── xetra_feed_data.py
    └── yfinance_feed_data.py
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
class EuronextFeedData(BaseModel):
    name: str = Field(..., description="Company name")
    symbol: str = Field(..., description="Trading symbol")
    isin: str = Field(..., description="ISIN identifier")
    mics: list[str] = Field(..., description="Market identifiers")

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

## Authoritative vs Enrichment Feeds

### Authoritative Feeds (Primary Sources)

- **Euronext**: Pan-European Börse Stock Exchange
- **LSE**: London Stock Exchange
- **XETRA**: Deutsche Börse Stock Exchange
- **SEC**: US Securities and Exchange Commission

**Characteristics**:

- Provide core equity data (names, symbols, identifiers)
- Considered source of truth for fundamental information
- Data from these feeds is **never** overwritten by enrichment

### Enrichment Feeds (Supplementary Sources)

- **Yahoo Finance**: Market data and financial metrics

**Characteristics**

- Only supplements missing data; never overwrites authoritative values
- Provides additional financial metrics (market cap, analyst ratings, etc.)
- Respects data hierarchy and source priority
- Applied after authoritative data processing is complete

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

- **data_sources/authoritative_feeds/**: Primary data source integrations
- **data_sources/enrichment_feeds/**: Supplementary data integrations
- **data_sources/reference_lookup/**: External API services (OpenFIGI, exchange rates)

### Storage Layer

- **data_store.py**: SQLite database operations
- **cache.py**: Caching for API responses
- **export.py**: Data export functionality
