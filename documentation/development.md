# Development

This guide covers setting up a local development environment for Equity
Aggregator: installing dependencies, configuring API keys, running the test
suites, the project's code-quality standards, and optional Docker usage. For an
overview of what the project does and how to use it, see the
[README](../README.md). For the test suites in detail, see [testing](testing.md);
for the system design, see [architecture](architecture.md).

## Prerequisites

Before starting, ensure the following conditions have been met:

- **Python 3.12+**: The application requires Python 3.12 or later
- **uv**: Python package manager
- **Git**: For version control
- **Docker** (optional): For containerised development and deployment

## Environment Setup

### Clone the repository

```bash
git clone <repository-url>
cd equity-aggregator
```

### Create and activate virtual environment

```bash
# Create virtual environment with Python 3.12
uv venv --python 3.12

# Activate the virtual environment
source .venv/bin/activate
```

### Install dependencies

```bash
# Install all dependencies and sync workspace
uv sync --all-packages
```

## Environment Variables

The application requires API keys for external data sources. A template file
`.env_example` is provided in the project root for guidance.

Copy the example environment file:

```bash
cp .env_example .env
```

Then configure the API keys by editing `.env` and adding the following:

### Mandatory Keys

- `EXCHANGE_RATE_API_KEY` - Required for currency conversion
  - Retrieve from: [ExchangeRate-API](https://exchangerate-api.com/)
  - Used for converting equity prices to USD reference currency

- `OPENFIGI_API_KEY` - Required for equity identification
  - Retrieve from: [OpenFIGI](https://www.openfigi.com/api)
  - Used for equity identification and deduplication

### Optional Keys

- `INTRINIO_API_KEY` - For Intrinio discovery feed
  - Retrieve from: [Intrinio](https://intrinio.com/)
  - Provides US equity data with comprehensive quote information

- `GITHUB_TOKEN` - For increased GitHub API rate limits
  - Retrieve from: [GitHub Settings](https://github.com/settings/tokens)
  - Increases release download rate limits from 60/hour to 5,000/hour
  - No special scopes required for public repositories

## Verify Installation

This setup provides the full development environment with all dependencies,
testing frameworks, and development tools configured.

It should therefore be possible to verify correct operation by running the
following commands using `uv`:

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

## Running Tests

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

## Code Quality and Linting

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

## Docker

The Equity Aggregator project can optionally be containerised using Docker. The
`docker-compose.yml` defines the equity-aggregator service.

### Docker Commands

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
