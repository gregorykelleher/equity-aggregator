# gleif/test_api.py

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif.api import (
    fetch_metadata,
    fetch_parents,
    search_by_name,
)
from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif.config import (
    GleifConfig,
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

    expected = GleifConfig().isin_lei_url
    assert received_urls[0] == expected


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


def _make_autocompletions_response(
    entries: list[tuple[str, str]],
) -> dict:
    """
    Build a GLEIF autocompletions API response payload.
    """
    return {
        "data": [
            {
                "attributes": {"value": name},
                "relationships": {
                    "lei-records": {"data": {"id": lei}},
                },
            }
            for name, lei in entries
        ],
    }


def _make_active_leis_response(
    leis: list[str],
) -> dict:
    """
    Build a GLEIF lei-records response for active LEI filtering.
    """
    return {
        "data": [{"id": lei} for lei in leis],
    }


def _make_lei_records_response(
    entries: list[tuple[str, str]],
) -> dict:
    """
    Build a GLEIF lei-records API response payload.
    """
    return {
        "data": [
            {
                "id": lei,
                "attributes": {
                    "entity": {
                        "legalName": {"name": name},
                    },
                },
            }
            for name, lei in entries
        ],
    }


def _make_search_handler(
    entries: list[tuple[str, str]],
) -> callable:
    """
    Create a handler for search_by_name that serves both the
    autocompletions and active-filter lei-records requests.
    """
    auto_payload = _make_autocompletions_response(entries)
    leis = [lei for _, lei in entries]
    active_payload = _make_active_leis_response(leis)

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "autocompletions" in url:
            return httpx.Response(
                200,
                json=auto_payload,
                request=request,
            )
        return httpx.Response(
            200,
            json=active_payload,
            request=request,
        )

    return handler


async def test_search_by_name_parses_valid_response() -> None:
    """
    ARRANGE: client returning autocompletions with two active entities
    ACT:     call search_by_name
    ASSERT:  returns list of (name, lei) tuples
    """
    expected = [
        ("Apple Inc.", "APPLE_LEI_001"),
        ("Apple Corp", "APPLE_LEI_002"),
    ]

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_make_search_handler(expected)),
    ) as client:
        actual = await search_by_name("Apple", client)

    assert actual == expected


async def test_search_by_name_returns_empty_on_http_error() -> None:
    """
    ARRANGE: client returning 500 error
    ACT:     call search_by_name
    ASSERT:  returns empty list
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Error", request=request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        actual = await search_by_name("Apple", client)

    assert actual == []


async def test_search_by_name_returns_empty_on_empty_data() -> None:
    """
    ARRANGE: client returning empty data array
    ACT:     call search_by_name
    ASSERT:  returns empty list
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": []}, request=request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        actual = await search_by_name("Apple", client)

    assert actual == []


async def test_search_by_name_filters_inactive_candidates() -> None:
    """
    ARRANGE: two candidates, one active and one inactive
    ACT:     call search_by_name
    ASSERT:  returns only the active candidate
    """
    candidates = [
        ("Apple Inc.", "ACTIVE_LEI"),
        ("Apple Old Corp", "INACTIVE_LEI"),
    ]
    auto_payload = _make_autocompletions_response(candidates)
    active_payload = _make_active_leis_response(["ACTIVE_LEI"])

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "autocompletions" in url:
            return httpx.Response(
                200,
                json=auto_payload,
                request=request,
            )
        return httpx.Response(
            200,
            json=active_payload,
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        actual = await search_by_name("Apple", client)

    assert actual == [("Apple Inc.", "ACTIVE_LEI")]


async def test_search_by_name_retries_on_429_then_succeeds() -> None:
    """
    ARRANGE: client returning 429 once then 200 with data
    ACT:     call search_by_name
    ASSERT:  returns parsed results after retry
    """
    expected = [("Apple Inc.", "APPLE_LEI")]
    auto_payload = _make_autocompletions_response(expected)
    active_payload = _make_active_leis_response(["APPLE_LEI"])
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if "lei-records" in str(request.url):
            return httpx.Response(
                200,
                json=active_payload,
                request=request,
            )
        if call_count == 1:
            return httpx.Response(
                429,
                text="Rate limited",
                request=request,
            )
        return httpx.Response(
            200,
            json=auto_payload,
            request=request,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        actual = await search_by_name("Apple", client)

    assert actual == expected


async def test_fetch_parents_parses_valid_response() -> None:
    """
    ARRANGE: client returning lei-records with parent entities
    ACT:     call fetch_parents
    ASSERT:  returns list of (name, lei) tuples
    """
    expected = [
        ("Alphabet Inc.", "ALPHABET_LEI"),
        ("Google LLC", "GOOGLE_LEI"),
    ]
    payload = _make_lei_records_response(expected)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload, request=request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        actual = await fetch_parents("CHILD_LEI", client)

    assert actual == expected


async def test_fetch_parents_returns_empty_on_http_error() -> None:
    """
    ARRANGE: client returning 500 error
    ACT:     call fetch_parents
    ASSERT:  returns empty list
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Error", request=request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        actual = await fetch_parents("SOME_LEI", client)

    assert actual == []


async def test_search_by_name_returns_empty_on_connection_error() -> None:
    """
    ARRANGE: client that raises a connection error
    ACT:     call search_by_name
    ASSERT:  returns empty list
    """

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        actual = await search_by_name("Apple", client)

    assert actual == []
