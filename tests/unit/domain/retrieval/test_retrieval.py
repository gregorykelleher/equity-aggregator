# retrieval/test_retrieval.py

import gzip
import json
import os
from collections.abc import AsyncGenerator

import httpx
import pytest

from equity_aggregator.domain.retrieval.retrieval import (
    _DATA_STORE_PATH,
    _asset_browser_url,
    _download_to_temp,
    _finalise_download,
    _get_github_headers,
    _get_release_by_tag,
    _open_client,
    _stream_download,
    _write_chunks_to_file,
    download_canonical_equities,
    retrieve_canonical_equity,
)
from equity_aggregator.storage import get_data_store_path

pytestmark = pytest.mark.unit


class _Stream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def aiter_bytes(self) -> "AsyncGenerator[bytes, None]":
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        return None

    def __aiter__(self) -> None:
        return self.aiter_bytes()


def _mock_github_client(content: bytes = b"") -> httpx.AsyncClient:
    """
    Creates a mock httpx.AsyncClient simulating GitHub release and asset download.

    This mock client intercepts requests to GitHub release endpoints and returns a
    predefined JSON response containing a single asset. For all other requests, it
    returns the provided content compressed with gzip, simulating the download of a
    release asset.

    Args:
        content (bytes, optional): The raw bytes to be compressed and returned as the
            asset content. Defaults to an empty bytes object.

    Returns:
        httpx.AsyncClient: An asynchronous HTTP client with a mock transport handler
            for testing GitHub release and asset download flows.
    """
    """Create mock client that returns GitHub release with content."""
    gz = gzip.compress(content)

    def handler(request: httpx.Request) -> httpx.Response:
        if "releases" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "assets": [
                        {
                            "name": "canonical_equities.jsonl.gz",
                            "browser_download_url": "https://x/f",
                        },
                    ],
                },
            )
        return httpx.Response(200, content=gz, headers={"Content-Length": str(len(gz))})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_write_chunks_to_file_writes_all_bytes() -> None:
    """
    ARRANGE: Response with two byte chunks
    ACT:     _write_chunks_to_file
    ASSERT:  file content equals concatenated chunks
    """
    response = httpx.Response(200, stream=_Stream([b"ab", b"cd"]))
    out_path = _DATA_STORE_PATH / "out.gz"

    await _write_chunks_to_file(response, out_path)

    assert out_path.read_bytes() == b"abcd"


async def test_download_to_temp_returns_counts_and_writes() -> None:
    """
    ARRANGE: MockTransport serves 4 bytes with Content-Length header
    ACT:     _download_to_temp
    ASSERT:  returns (4, 4)
    """
    payload = b"ABCD"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Content-Length": "4"}, content=payload)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    dest = _DATA_STORE_PATH / "file.tmp"

    written, expected = await _download_to_temp(
        client,
        "https://example/file",
        dest,
    )

    assert (written, expected) == (4, 4)


async def test_stream_download_creates_final_file() -> None:
    """
    ARRANGE: MockTransport serves bytes with matching length
    ACT:     _stream_download
    ASSERT:  final file exists with expected content
    """
    body = b"hello"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Length": str(len(body))},
            content=body,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    dest = _DATA_STORE_PATH / "final.gz"

    returned = await _stream_download(client, "https://example/a", dest)

    assert returned.read_bytes() == body


def test_finalise_download_raises_on_mismatch() -> None:
    """
    ARRANGE: tmp contains 2 bytes but expected=3
    ACT:     _finalise_download
    ASSERT:  OSError raised
    """
    tmp = _DATA_STORE_PATH / "y.tmp"
    dest = _DATA_STORE_PATH / "y.bin"
    tmp.write_bytes(b"ab")

    with pytest.raises(OSError):
        _finalise_download(tmp, dest, (2, 3))


async def test_open_client_yields_supplied_instance() -> None:
    """
    ARRANGE: AsyncClient instance
    ACT:     _open_client(client)
    ASSERT:  yielded object is the same
    """
    client = httpx.AsyncClient()
    async with _open_client(client) as yielded:
        assert yielded is client
    await client.aclose()


async def test_open_client_creates_when_none() -> None:
    """
    ARRANGE: None client
    ACT:     _open_client(None)
    ASSERT:  yielded is an AsyncClient
    """
    async with _open_client(None) as yielded:
        assert isinstance(yielded, httpx.AsyncClient)


async def test_get_release_by_tag_404_raises_file_not_found() -> None:
    """
    ARRANGE: MockTransport returns 404
    ACT:     _get_release_by_tag
    ASSERT:  FileNotFoundError raised
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"message": "Not Found"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(FileNotFoundError):
        await _get_release_by_tag(client, "o", "r", "t")


async def test_get_release_by_tag_success() -> None:
    """
    ARRANGE: MockTransport returns 200 with empty assets
    ACT:     _get_release_by_tag
    ASSERT:  returns expected release dict
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"assets": []})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    release = await _get_release_by_tag(client, "o", "r", "t")
    assert release == {"assets": []}


async def test_get_release_by_tag_5xx_raises_httpstatus() -> None:
    """
    ARRANGE: MockTransport returns 503 with JSON error message
    ACT:     Call _get_release_by_tag with mocked client
    ASSERT:  httpx.HTTPStatusError is raised
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"message": "unavailable"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        await _get_release_by_tag(client, "o", "r", "t")


def test_asset_browser_url_returns_expected() -> None:
    """
    ARRANGE: release dict with matching asset
    ACT:     _asset_browser_url
    ASSERT:  returns expected URL
    """
    release = {
        "assets": [
            {"name": "a.gz", "browser_download_url": "https://example/a.gz"},
        ],
    }

    url = _asset_browser_url(release, "a.gz")

    assert url == "https://example/a.gz"


def test_asset_browser_url_raises_when_missing() -> None:
    """
    ARRANGE: release dict without target asset
    ACT:     _asset_browser_url
    ASSERT:  FileNotFoundError raised
    """
    release = {"assets": [{"name": "b.gz", "browser_download_url": "x"}]}

    with pytest.raises(FileNotFoundError):
        _asset_browser_url(release, "a.gz")


def test_download_canonical_equities_raises_on_missing_asset() -> None:
    """
    ARRANGE: Mock client with release but missing asset
    ACT:     Call download_canonical_equities
    ASSERT:  FileNotFoundError raised
    """

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"assets": []})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(FileNotFoundError):
        download_canonical_equities(client)


def test_retrieve_canonical_equity_raises_lookup_error_when_not_found() -> None:
    """
    ARRANGE: Database exists but doesn't contain target FIGI
    ACT:     Call retrieve_canonical_equity with non-existent FIGI
    ASSERT:  Raises LookupError
    """
    # Create database with some data
    download_canonical_equities(_mock_github_client())

    with pytest.raises(LookupError):
        retrieve_canonical_equity("BBG000NOTFOUND")


def test_retrieve_canonical_equity_returns_found_equity() -> None:
    """
    ARRANGE: Database exists with target equity
    ACT:     Call retrieve_canonical_equity with existing FIGI
    ASSERT:  Returns the equity
    """
    equity_data = {
        "identity": {
            "share_class_figi": "BBG000B9XRY4",
            "name": "Test Equity",
            "ticker": "TEST",
            "symbol": "TEST",
        },
        "pricing": {"currency": "USD", "price": 100.0},
        "financials": {"market_cap": 1000000.0},
    }
    jsonl_bytes = json.dumps(equity_data).encode() + b"\n"

    # Create database with the equity
    download_canonical_equities(_mock_github_client(jsonl_bytes))

    actual = retrieve_canonical_equity("BBG000B9XRY4")

    assert actual.identity.share_class_figi == "BBG000B9XRY4"


def test_download_canonical_equities_rebuilds_database() -> None:
    """
    ARRANGE: Mock client with empty JSONL
    ACT:     download_canonical_equities
    ASSERT:  Database file exists after rebuild
    """
    asset_path = _DATA_STORE_PATH / "canonical_equities.jsonl.gz"
    db_path = _DATA_STORE_PATH / "data_store.db"

    asset_path.unlink(missing_ok=True)
    db_path.unlink(missing_ok=True)

    download_canonical_equities(_mock_github_client())

    assert db_path.exists()


def test_get_github_headers_without_token() -> None:
    """
    ARRANGE: Remove GITHUB_TOKEN temporarily
    ACT:     _get_github_headers
    ASSERT:  No Authorization header present
    """
    original = os.environ.get("GITHUB_TOKEN")
    if "GITHUB_TOKEN" in os.environ:
        del os.environ["GITHUB_TOKEN"]

    try:
        headers = _get_github_headers()
        assert "Authorization" not in headers
    finally:
        if original is not None:
            os.environ["GITHUB_TOKEN"] = original


def test_get_github_headers_with_token() -> None:
    """
    ARRANGE: Set GITHUB_TOKEN temporarily
    ACT:     _get_github_headers
    ASSERT:  Authorization header present
    """
    original = os.environ.get("GITHUB_TOKEN")
    os.environ["GITHUB_TOKEN"] = "test_token"

    try:
        headers = _get_github_headers()
        assert "Authorization" in headers
    finally:
        if original is not None:
            os.environ["GITHUB_TOKEN"] = original
        else:
            del os.environ["GITHUB_TOKEN"]


def test_get_data_store_path_with_override() -> None:
    """
    ARRANGE: Set DATA_STORE_DIR environment variable
    ACT:     get_data_store_path
    ASSERT:  Returns override path
    """
    original = os.environ.get("DATA_STORE_DIR")
    os.environ["DATA_STORE_DIR"] = "/custom/path"

    try:
        actual = get_data_store_path()
        assert str(actual) == "/custom/path"
    finally:
        if original is not None:
            os.environ["DATA_STORE_DIR"] = original
        elif "DATA_STORE_DIR" in os.environ:
            del os.environ["DATA_STORE_DIR"]


def test_get_data_store_path_default() -> None:
    """
    ARRANGE: Remove DATA_STORE_DIR environment variable
    ACT:     get_data_store_path
    ASSERT:  Returns user_data_dir path
    """
    original = os.environ.get("DATA_STORE_DIR")
    if "DATA_STORE_DIR" in os.environ:
        del os.environ["DATA_STORE_DIR"]

    try:
        actual = get_data_store_path()
        assert "equity-aggregator" in str(actual)
    finally:
        if original is not None:
            os.environ["DATA_STORE_DIR"] = original
