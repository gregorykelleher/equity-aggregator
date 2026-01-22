# gleif/test_download.py

import io
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif.download import (
    _stream_to_file,
    download_and_build_index,
)

from ._helpers import make_client_factory

pytestmark = pytest.mark.unit


def _create_zip_bytes(csv_content: str) -> bytes:
    """
    Create a ZIP file containing a CSV in memory and return as bytes.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("isin_lei.csv", csv_content)
    return buffer.getvalue()


def _make_gleif_handler(
    zip_bytes: bytes,
    download_url: str = "https://example.com/download.zip",
) -> callable:
    """
    Create a handler that returns metadata then ZIP content.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)

        if "isin-lei/latest" in url:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "test",
                        "attributes": {"downloadLink": download_url},
                    },
                },
                request=request,
            )

        return httpx.Response(200, content=zip_bytes, request=request)

    return handler


async def test_download_and_build_index_returns_dict() -> None:
    """
    ARRANGE: client_factory returning valid metadata and ZIP with CSV
    ACT:     call download_and_build_index
    ASSERT:  returns dictionary
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await download_and_build_index(
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert isinstance(actual, dict)


async def test_download_and_build_index_parses_single_mapping() -> None:
    """
    ARRANGE: client_factory returning ZIP with one ISIN->LEI row
    ACT:     call download_and_build_index
    ASSERT:  result contains the mapping
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await download_and_build_index(
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert actual["US0378331005"] == "529900T8BM49AURSDO55"


async def test_download_and_build_index_parses_multiple_mappings() -> None:
    """
    ARRANGE: client_factory returning ZIP with multiple ISIN->LEI rows
    ACT:     call download_and_build_index
    ASSERT:  result contains all mappings
    """
    expected_isins = {"US0378331005", "US5949181045"}
    csv_content = (
        "LEI,ISIN\n"
        "529900T8BM49AURSDO55,US0378331005\n"
        "HWUPKR0MPOU8FGXBT394,US5949181045\n"
    )
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await download_and_build_index(
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert set(actual.keys()) == expected_isins


async def test_download_and_build_index_raises_when_metadata_fails() -> None:
    """
    ARRANGE: client_factory returning error for metadata request
    ACT:     call download_and_build_index
    ASSERT:  raises ValueError
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "Server error"}, request=request)

    with pytest.raises(ValueError) as exc_info:
        await download_and_build_index(client_factory=make_client_factory(handler))

    assert "Failed to retrieve GLEIF ISIN->LEI metadata" in str(exc_info.value)


async def test_download_and_build_index_raises_when_download_link_missing() -> None:
    """
    ARRANGE: client_factory returning metadata without downloadLink
    ACT:     call download_and_build_index
    ASSERT:  raises ValueError
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": {"id": "test", "attributes": {}}},
            request=request,
        )

    with pytest.raises(ValueError) as exc_info:
        await download_and_build_index(client_factory=make_client_factory(handler))

    assert "missing download_link" in str(exc_info.value)


async def test_download_and_build_index_uses_link_from_metadata() -> None:
    """
    ARRANGE: client_factory tracking download URL
    ACT:     call download_and_build_index
    ASSERT:  downloads from URL provided in metadata
    """
    expected_download_url = "https://gleif.org/files/isin_lei_2024.zip"
    received_urls: list[str] = []
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        received_urls.append(url)

        if "isin-lei/latest" in url:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "test",
                        "attributes": {"downloadLink": expected_download_url},
                    },
                },
                request=request,
            )

        return httpx.Response(200, content=zip_bytes, request=request)

    await download_and_build_index(client_factory=make_client_factory(handler))

    assert expected_download_url in received_urls


async def test_download_and_build_index_raises_when_download_fails() -> None:
    """
    ARRANGE: client_factory returning error for download request
    ACT:     call download_and_build_index
    ASSERT:  raises HTTPStatusError
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)

        if "isin-lei/latest" in url:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "test",
                        "attributes": {
                            "downloadLink": "https://example.com/download.zip",
                        },
                    },
                },
                request=request,
            )

        return httpx.Response(404, content=b"Not found", request=request)

    with pytest.raises(httpx.HTTPStatusError):
        await download_and_build_index(client_factory=make_client_factory(handler))


async def test_download_and_build_index_handles_empty_csv() -> None:
    """
    ARRANGE: client_factory returning ZIP with empty CSV (headers only)
    ACT:     call download_and_build_index
    ASSERT:  returns empty dict
    """
    csv_content = "LEI,ISIN\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await download_and_build_index(
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert actual == {}


async def test_stream_to_file_writes_content() -> None:
    """
    ARRANGE: mock client returning content via stream
    ACT:     call _stream_to_file
    ASSERT:  file contains the streamed content
    """
    expected_content = b"test content for streaming"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=expected_content, request=request)

    with TemporaryDirectory() as temp_dir:
        destination = Path(temp_dir) / "test_file.bin"

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await _stream_to_file(client, "https://example.com/file", destination)

        assert destination.read_bytes() == expected_content
