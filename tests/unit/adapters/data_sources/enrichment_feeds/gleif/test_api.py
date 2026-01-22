# gleif/test_api.py

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif.api import (
    GLEIF_ISIN_LEI_URL,
    fetch_metadata,
)

from ._helpers import make_client_factory

pytestmark = pytest.mark.unit


async def test_fetch_metadata_returns_dict_on_success() -> None:
    """
    ARRANGE: client_factory returning valid GLEIF API response
    ACT:     call fetch_metadata
    ASSERT:  returns dictionary
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "id": "test-uuid",
                    "attributes": {
                        "fileName": "isin_lei.zip",
                        "uploadedAt": "2024-01-01T00:00:00Z",
                        "downloadLink": "https://example.com/download",
                    },
                },
            },
            request=request,
        )

    actual = await fetch_metadata(client_factory=make_client_factory(handler))

    assert isinstance(actual, dict)


async def test_fetch_metadata_extracts_id_from_response() -> None:
    """
    ARRANGE: client_factory returning GLEIF response with id
    ACT:     call fetch_metadata
    ASSERT:  result contains id
    """
    expected_id = "abc-123-uuid"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": {"id": expected_id, "attributes": {}}},
            request=request,
        )

    actual = await fetch_metadata(client_factory=make_client_factory(handler))

    assert actual["id"] == expected_id


async def test_fetch_metadata_extracts_file_name_from_attributes() -> None:
    """
    ARRANGE: client_factory returning GLEIF response with fileName
    ACT:     call fetch_metadata
    ASSERT:  result contains file_name
    """
    expected_filename = "isin_lei_2024.zip"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {"id": "test", "attributes": {"fileName": expected_filename}},
            },
            request=request,
        )

    actual = await fetch_metadata(client_factory=make_client_factory(handler))

    assert actual["file_name"] == expected_filename


async def test_fetch_metadata_extracts_uploaded_at_from_attributes() -> None:
    """
    ARRANGE: client_factory returning GLEIF response with uploadedAt
    ACT:     call fetch_metadata
    ASSERT:  result contains uploaded_at
    """
    expected_timestamp = "2024-06-15T12:30:00Z"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "id": "test",
                    "attributes": {"uploadedAt": expected_timestamp},
                },
            },
            request=request,
        )

    actual = await fetch_metadata(client_factory=make_client_factory(handler))

    assert actual["uploaded_at"] == expected_timestamp


async def test_fetch_metadata_extracts_download_link_from_attributes() -> None:
    """
    ARRANGE: client_factory returning GLEIF response with downloadLink
    ACT:     call fetch_metadata
    ASSERT:  result contains download_link
    """
    expected_link = "https://gleif.org/download/isin_lei.zip"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {"id": "test", "attributes": {"downloadLink": expected_link}},
            },
            request=request,
        )

    actual = await fetch_metadata(client_factory=make_client_factory(handler))

    assert actual["download_link"] == expected_link


async def test_fetch_metadata_calls_correct_gleif_url() -> None:
    """
    ARRANGE: client_factory that tracks request URL
    ACT:     call fetch_metadata
    ASSERT:  request was made to GLEIF_ISIN_LEI_URL
    """
    received_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received_urls.append(str(request.url))
        return httpx.Response(
            200,
            json={"data": {"id": "test", "attributes": {}}},
            request=request,
        )

    await fetch_metadata(client_factory=make_client_factory(handler))

    assert received_urls[0] == GLEIF_ISIN_LEI_URL


async def test_fetch_metadata_returns_none_on_http_error() -> None:
    """
    ARRANGE: client_factory returning 500 error
    ACT:     call fetch_metadata
    ASSERT:  returns None
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "Server error"}, request=request)

    actual = await fetch_metadata(client_factory=make_client_factory(handler))

    assert actual is None


async def test_fetch_metadata_returns_none_on_connection_error() -> None:
    """
    ARRANGE: client_factory that raises connection error
    ACT:     call fetch_metadata
    ASSERT:  returns None
    """

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    actual = await fetch_metadata(client_factory=make_client_factory(handler))

    assert actual is None


async def test_fetch_metadata_returns_none_for_missing_attributes() -> None:
    """
    ARRANGE: client_factory returning response with empty attributes
    ACT:     call fetch_metadata
    ASSERT:  missing fields are None
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": {"id": "test", "attributes": {}}},
            request=request,
        )

    actual = await fetch_metadata(client_factory=make_client_factory(handler))

    assert actual["file_name"] is None


async def test_fetch_metadata_handles_missing_data_key() -> None:
    """
    ARRANGE: client_factory returning response without data key
    ACT:     call fetch_metadata
    ASSERT:  returns dict with None values
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={}, request=request)

    actual = await fetch_metadata(client_factory=make_client_factory(handler))

    assert actual["id"] is None
