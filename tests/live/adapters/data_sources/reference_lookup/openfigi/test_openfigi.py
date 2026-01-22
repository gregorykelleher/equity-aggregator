# openfigi/test_openfigi.py

import pandas as pd
import pytest

from equity_aggregator.adapters.data_sources.reference_lookup.openfigi import (
    _get_api_key,
    _make_openfigi_client,
    _OpenFigiClient,
)

pytestmark = pytest.mark.live

# Apple Inc. ISIN for a reliable test query
_TEST_ISIN = "US0378331005"


@pytest.fixture(scope="module")
def openfigi_client() -> _OpenFigiClient | None:
    """
    Create OpenFIGI client once and share across all tests in this module.
    """
    return _make_openfigi_client()


@pytest.fixture(scope="module")
def openfigi_map_result(openfigi_client: _OpenFigiClient | None) -> pd.DataFrame:
    """
    Fetch OpenFIGI map result once and share across all tests in this module.
    """
    query_df = pd.DataFrame([{"idType": "ID_ISIN", "idValue": _TEST_ISIN}])
    return openfigi_client.map(query_df)


def test_openfigi_api_key_is_available() -> None:
    """
    ARRANGE: environment with OPENFIGI_API_KEY
    ACT:     retrieve API key
    ASSERT:  API key is not None
    """
    api_key = _get_api_key()

    assert api_key is not None


def test_openfigi_client_connects_successfully(
    openfigi_client: _OpenFigiClient | None,
) -> None:
    """
    ARRANGE: valid OpenFIGI API key
    ACT:     create and connect client
    ASSERT:  client is not None (connection succeeded)
    """
    assert openfigi_client is not None


def test_openfigi_client_map_returns_dataframe(
    openfigi_map_result: pd.DataFrame,
) -> None:
    """
    ARRANGE: connected OpenFIGI client with test query
    ACT:     perform mapping query
    ASSERT:  returns a DataFrame
    """
    assert isinstance(openfigi_map_result, pd.DataFrame)


def test_openfigi_client_map_returns_non_empty_response(
    openfigi_map_result: pd.DataFrame,
) -> None:
    """
    ARRANGE: connected OpenFIGI client with known ISIN query
    ACT:     perform mapping query
    ASSERT:  response DataFrame is non-empty
    """
    assert len(openfigi_map_result) > 0
