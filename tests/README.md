# Test Suite Documentation

## Overview

The equity aggregator test suite achieves 99% coverage without monkey-patching or mocking. It uses dependency injection and temporary data stores to create isolated, fast, and robust offline tests.

## Architecture

### Test Markers

- **`unit`**: Component validation in isolation
- **`live`**: Live endpoint integration (requires network)

```bash
# Run unit tests
uv run pytest -m unit

# Run live tests
uv run pytest -m live

# Run with coverage
uv run pytest -m unit --cov=equity_aggregator --cov-report=term-missing --cov-report=html
```

### Infrastructure

The testing infrastructure is configured in `tests/conftest.py`, which provides essential fixtures and environment setup for isolated, reproducible testing. The configuration automatically creates temporary data stores and manages database lifecycle.

#### Temporary Data Store
- Creates isolated `data_store` directory in `.pytest_cache`
- Sets `DATA_STORE_DIR` environment variable
- Ensures test isolation from production data

#### Fresh Database
- `fresh_data_store` fixture runs before each test
- Deletes existing `data_store.db`
- Guarantees clean state


## Test Structure

### AAA Pattern

All tests follow the Arrange-Act-Assert (AAA) pattern, which structures tests into three distinct phases for clarity and maintainability:

1. **Arrange**: Set up test data, configure dependencies, and prepare the test environment
2. **Act**: Execute the specific functionality being tested (single operation)
3. **Assert**: Verify the expected outcome with a single, focused assertion

This pattern ensures tests are readable, focused, and follow a consistent structure that makes debugging easier when tests fail.

```python
def test_to_upper_basic() -> None:
    """
    ARRANGE: simple lower-case string
    ACT:     to_upper
    ASSERT:  returns upper-cased string
    """
    # Arrange: Set up test data
    value = "foo"

    # Act: Execute the function under test
    actual = validators.to_upper(value)

    # Assert: Verify the expected result
    assert actual == "FOO"
```

### HTTP Request Testing

For components that make HTTP requests, the test suite uses `httpx.MockTransport` to intercept and mock HTTP calls without external dependencies:

```python
async def test_api_client_fetches_data() -> None:
    """
    ARRANGE: mock HTTP response with test data
    ACT:     call API client
    ASSERT:  returns expected data
    """
    # Arrange: Set up mock HTTP response
    def mock_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "success"})

    transport = httpx.MockTransport(mock_response)

    # Act: Execute function with mocked transport
    async with httpx.AsyncClient(transport=transport) as client:
        result = await api_client.fetch_data(client)

    # Assert: Verify expected result
    assert result["status"] == "success"
```


## Coverage Configuration

```toml
[tool.coverage.run]
source = ["src"]
branch = true
data_file = "data/.coverage"
omit = ["*/__init__.py", "*/__main__.py", "*/logging_config.py"]

[tool.coverage.report]
show_missing = true
skip_empty = false
fail_under = 99
```

### Achieving 99% Without Mocks

1. **Dependency Injection**: Components accept dependencies as parameters
2. **Temporary Data Stores**: Real SQLite in isolated environments
3. **Async Support**: Native pytest-asyncio for async pipelines
4. **Data-Driven Tests**: Real data structures in controlled scenarios

## Guidelines

### Unit Test Requirements

- **Isolation**: Independent tests without external state
- **Speed**: 30-second timeout enforced
- **Offline**: No network dependencies
- **Real Components**: Actual application code, not mocks
- **Single Assertion**: One assertion per test case (AAA pattern)

### Organisation

```
tests/
├── conftest.py                # Fixtures and configuration
├── unit/                      # Unit tests
│   ├── adapters/              # Adapter layer
│   ├── domain/                # Domain logic
│   │   └── pipeline/          # Pipeline transformations
│   ├── schemas/               # Data validation
│   └── storage/               # Database operations
└── integration/               # Integration tests
```

## Configuration Reference

### pytest.ini_options

```toml
addopts = "-ra -q"                    # Short summary, quiet mode
testpaths = ["tests"]                 # Discovery path
asyncio_mode = "auto"                 # Automatic async detection
timeout = 30                          # Test timeout
env = [
  "CACHE_DIR=data/cache/.pytest_cache",
  "CACHE_TTL_MINUTES=0",              # Disable caching in tests
]
```
