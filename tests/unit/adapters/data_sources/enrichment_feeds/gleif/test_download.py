# gleif/test_download.py

import io
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif.download import (
    _parse_zip,
    _stream_to_file,
    download_and_build_isin_index,
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


def _create_test_zip(zip_path: Path, csv_name: str, csv_content: str) -> None:
    """
    Helper to create a ZIP file containing a CSV for testing.
    """
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(csv_name, csv_content)


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
                        "attributes": {
                            "downloadLink": download_url,
                        },
                    },
                },
                request=request,
            )

        return httpx.Response(200, content=zip_bytes, request=request)

    return handler


# --- download_and_build_isin_index ---


async def test_download_and_build_isin_index_returns_dict() -> None:
    """
    ARRANGE: client_factory returning valid metadata and ZIP with CSV
    ACT:     call download_and_build_isin_index
    ASSERT:  returns dictionary
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await download_and_build_isin_index(
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert isinstance(actual, dict)


async def test_download_and_build_isin_index_parses_single() -> None:
    """
    ARRANGE: client_factory returning ZIP with one ISIN->LEI row
    ACT:     call download_and_build_isin_index
    ASSERT:  result contains the mapping
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await download_and_build_isin_index(
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert actual["US0378331005"] == "529900T8BM49AURSDO55"


async def test_download_and_build_isin_index_parses_multiple() -> None:
    """
    ARRANGE: client_factory returning ZIP with multiple ISIN->LEI rows
    ACT:     call download_and_build_isin_index
    ASSERT:  result contains all mappings
    """
    expected_isins = {"US0378331005", "US5949181045"}
    csv_content = (
        "LEI,ISIN\n"
        "529900T8BM49AURSDO55,US0378331005\n"
        "HWUPKR0MPOU8FGXBT394,US5949181045\n"
    )
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await download_and_build_isin_index(
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert set(actual.keys()) == expected_isins


async def test_download_and_build_isin_index_raises_when_metadata_fails() -> None:
    """
    ARRANGE: client_factory returning error for metadata request
    ACT:     call download_and_build_isin_index
    ASSERT:  raises ValueError
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "Server error"}, request=request)

    with pytest.raises(ValueError) as exc_info:
        await download_and_build_isin_index(
            client_factory=make_client_factory(handler),
        )

    assert "Failed to retrieve GLEIF ISIN->LEI metadata" in str(exc_info.value)


async def test_download_and_build_isin_index_raises_when_link_missing() -> None:
    """
    ARRANGE: client_factory returning metadata without downloadLink
    ACT:     call download_and_build_isin_index
    ASSERT:  raises ValueError
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": {"id": "test", "attributes": {}}},
            request=request,
        )

    with pytest.raises(ValueError) as exc_info:
        await download_and_build_isin_index(
            client_factory=make_client_factory(handler),
        )

    assert "missing download_link" in str(exc_info.value)


async def test_download_and_build_isin_index_uses_link_from_metadata() -> None:
    """
    ARRANGE: client_factory tracking download URL
    ACT:     call download_and_build_isin_index
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
                        "attributes": {
                            "downloadLink": expected_download_url,
                        },
                    },
                },
                request=request,
            )

        return httpx.Response(200, content=zip_bytes, request=request)

    await download_and_build_isin_index(
        client_factory=make_client_factory(handler),
    )

    assert expected_download_url in received_urls


async def test_download_and_build_isin_index_raises_when_download_fails() -> None:
    """
    ARRANGE: client_factory returning error for download request
    ACT:     call download_and_build_isin_index
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
                            "downloadLink": ("https://example.com/download.zip"),
                        },
                    },
                },
                request=request,
            )

        return httpx.Response(404, content=b"Not found", request=request)

    with pytest.raises(httpx.HTTPStatusError):
        await download_and_build_isin_index(
            client_factory=make_client_factory(handler),
        )


async def test_download_and_build_isin_index_handles_empty_csv() -> None:
    """
    ARRANGE: client_factory returning ZIP with empty CSV (headers only)
    ACT:     call download_and_build_isin_index
    ASSERT:  returns empty dict
    """
    csv_content = "LEI,ISIN\n"
    zip_bytes = _create_zip_bytes(csv_content)

    actual = await download_and_build_isin_index(
        client_factory=make_client_factory(_make_gleif_handler(zip_bytes)),
    )

    assert actual == {}


# --- _stream_to_file ---


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

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as client:
            await _stream_to_file(client, "https://example.com/file", destination)

        assert destination.read_bytes() == expected_content


# --- _parse_zip ---


def test_parse_zip_returns_empty_dict_for_empty_csv() -> None:
    """
    ARRANGE: ZIP file containing CSV with only headers
    ACT:     call _parse_zip
    ASSERT:  returns empty dictionary
    """
    csv_content = "LEI,ISIN\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert actual == {}


def test_parse_zip_returns_single_mapping() -> None:
    """
    ARRANGE: ZIP file containing CSV with one valid row
    ACT:     call _parse_zip
    ASSERT:  returns dictionary with one ISIN->LEI mapping
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert actual == {"US0378331005": "529900T8BM49AURSDO55"}


def test_parse_zip_returns_multiple_mappings() -> None:
    """
    ARRANGE: ZIP file containing CSV with multiple valid rows
    ACT:     call _parse_zip
    ASSERT:  returns dictionary with all ISIN->LEI mappings
    """
    expected_isins = {
        "US0378331005",
        "US5949181045",
        "GB00B03MLX29",
    }
    csv_content = (
        "LEI,ISIN\n"
        "529900T8BM49AURSDO55,US0378331005\n"
        "HWUPKR0MPOU8FGXBT394,US5949181045\n"
        "549300GGN6YROH77Y439,GB00B03MLX29\n"
    )

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "data.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert set(actual.keys()) == expected_isins


def test_parse_zip_maps_isin_to_lei() -> None:
    """
    ARRANGE: ZIP file containing CSV with known ISIN->LEI pair
    ACT:     call _parse_zip
    ASSERT:  ISIN key maps to correct LEI value
    """
    csv_content = "LEI,ISIN\nHWUPKR0MPOU8FGXBT394,US5949181045\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert actual["US5949181045"] == "HWUPKR0MPOU8FGXBT394"


def test_parse_zip_uppercases_isin() -> None:
    """
    ARRANGE: ZIP file containing CSV with lowercase ISIN
    ACT:     call _parse_zip
    ASSERT:  ISIN key is uppercase
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,us0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert "US0378331005" in actual


def test_parse_zip_uppercases_lei() -> None:
    """
    ARRANGE: ZIP file containing CSV with lowercase LEI
    ACT:     call _parse_zip
    ASSERT:  LEI value is uppercase
    """
    csv_content = "LEI,ISIN\n529900t8bm49aursdo55,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert actual["US0378331005"] == "529900T8BM49AURSDO55"


def test_parse_zip_strips_whitespace_from_isin() -> None:
    """
    ARRANGE: ZIP file containing CSV with whitespace around ISIN
    ACT:     call _parse_zip
    ASSERT:  ISIN key has whitespace stripped
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,  US0378331005  \n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert "US0378331005" in actual


def test_parse_zip_strips_whitespace_from_lei() -> None:
    """
    ARRANGE: ZIP file containing CSV with whitespace around LEI
    ACT:     call _parse_zip
    ASSERT:  LEI value has whitespace stripped
    """
    csv_content = "LEI,ISIN\n  529900T8BM49AURSDO55  ,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert actual["US0378331005"] == "529900T8BM49AURSDO55"


def test_parse_zip_skips_row_with_empty_isin() -> None:
    """
    ARRANGE: ZIP file containing CSV with empty ISIN field
    ACT:     call _parse_zip
    ASSERT:  row is skipped, empty string not present
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert actual == {}


def test_parse_zip_skips_row_with_empty_lei() -> None:
    """
    ARRANGE: ZIP file containing CSV with empty LEI field
    ACT:     call _parse_zip
    ASSERT:  row is skipped, ISIN not present
    """
    csv_content = "LEI,ISIN\n,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert "US0378331005" not in actual


def test_parse_zip_skips_whitespace_only_isin() -> None:
    """
    ARRANGE: ZIP file containing CSV with whitespace-only ISIN
    ACT:     call _parse_zip
    ASSERT:  row is skipped
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,   \n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert actual == {}


def test_parse_zip_skips_whitespace_only_lei() -> None:
    """
    ARRANGE: ZIP file containing CSV with whitespace-only LEI
    ACT:     call _parse_zip
    ASSERT:  row is skipped
    """
    csv_content = "LEI,ISIN\n   ,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert actual == {}


def test_parse_zip_finds_csv_with_uppercase_extension() -> None:
    """
    ARRANGE: ZIP file containing CSV with .CSV uppercase extension
    ACT:     call _parse_zip
    ASSERT:  CSV is found and parsed successfully
    """
    csv_content = "LEI,ISIN\n529900T8BM49AURSDO55,US0378331005\n"

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "MAPPING.CSV", csv_content)

        actual = _parse_zip(zip_path)

    assert "US0378331005" in actual


def test_parse_zip_raises_when_no_csv_in_archive() -> None:
    """
    ARRANGE: ZIP file containing no CSV files
    ACT:     call _parse_zip
    ASSERT:  raises ValueError with descriptive message
    """
    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("readme.txt", "No CSV here")

        with pytest.raises(ValueError) as exc_info:
            _parse_zip(zip_path)

    assert "No CSV file found" in str(exc_info.value)


def test_parse_zip_handles_mixed_valid_and_invalid_rows() -> None:
    """
    ARRANGE: ZIP file with mix of valid and invalid rows
    ACT:     call _parse_zip
    ASSERT:  only valid rows are included
    """
    expected_isins = {"US0378331005", "GB00B03MLX29"}
    csv_content = (
        "LEI,ISIN\n"
        "529900T8BM49AURSDO55,US0378331005\n"
        ",US1234567890\n"
        "HWUPKR0MPOU8FGXBT394,\n"
        "549300GGN6YROH77Y439,GB00B03MLX29\n"
    )

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert set(actual.keys()) == expected_isins


def test_parse_zip_last_mapping_wins_for_duplicate_isin() -> None:
    """
    ARRANGE: ZIP file containing CSV with duplicate ISIN values
    ACT:     call _parse_zip
    ASSERT:  last LEI value for the ISIN is preserved
    """
    csv_content = (
        "LEI,ISIN\n"
        "FIRSTLEI00000000000000,US0378331005\n"
        "SECONDLEI0000000000000,US0378331005\n"
    )

    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "test.zip"
        _create_test_zip(zip_path, "mapping.csv", csv_content)

        actual = _parse_zip(zip_path)

    assert actual["US0378331005"] == "SECONDLEI0000000000000"
