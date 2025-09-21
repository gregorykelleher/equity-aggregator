# tests/conftest.py

import os
from pathlib import Path

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """
    Configures pytest to create a temporary folder within .pytest_cache for test data.

    This function is automatically called by pytest before running tests. It creates a
    directory named 'data_store' inside the .pytest_cache folder and sets the
    environment variable 'DATA_STORE_DIR' to the absolute path of this directory.
    This allows tests to access a temporary, isolated data store location.

    Args:
        config (pytest.Config): The pytest configuration object.

    Returns:
        None
    """
    root = Path(config.cache.makedir("data_store").strpath)

    os.environ["DATA_STORE_DIR"] = root.as_posix()


@pytest.fixture
def data_sql_store_dir() -> Path:
    """
    Fixture that provides the path to the temporary data sql store directory for test
    inspection.

    Returns:
        Path: The path to the temporary data sql store directory, as specified by the
            'DATA_STORE_DIR' environment variable.
    """
    return Path(os.environ["DATA_STORE_DIR"])


@pytest.fixture(autouse=True)
def fresh_data_store() -> None:
    """
    Ensures each test starts with a clean SQLite data store file.

    This fixture runs automatically before each test. It deletes the 'data_store.db'
    file from the temporary data store directory if it exists, guaranteeing a pristine
    state for every test.

    Args:
        None

    Returns:
        None
    """
    db_file = Path(os.environ["DATA_STORE_DIR"]) / "data_store.db"
    if db_file.exists():
        db_file.unlink()
