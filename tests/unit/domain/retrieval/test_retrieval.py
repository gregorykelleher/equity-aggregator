# retrieval/test_retrieval.py

import gzip
import os
import sqlite3
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path

import httpx
import pytest

from equity_aggregator.domain.retrieval.retrieval import (
    _DATA_STORE_PATH,
    _asset_browser_url,
    _decompress_db,
    _download_to_temp,
    _finalise_download,
    _get_github_headers,
    _get_release_by_tag,
    _open_client,
    _stream_download,
    _write_chunks_to_file,
    download_canonical_equities,
    retrieve_canonical_equity,
    retrieve_canonical_equity_history,
)
from equity_aggregator.schemas import (
    CanonicalEquity,
    EquityFinancials,
    EquityIdentity,
)
from equity_aggregator.storage import get_data_store_path
from equity_aggregator.storage.data_store import save_canonical_equities

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


def _create_canonical_equity(figi: str, name: str = "TEST EQUITY") -> CanonicalEquity:
    """
    Create a CanonicalEquity instance for testing purposes.

    Args:
        figi (str): The FIGI identifier for the equity.
        name (str): The name of the equity, defaults to "TEST EQUITY".

    Returns:
        CanonicalEquity: A properly constructed CanonicalEquity instance.
    """
    identity = EquityIdentity(
        name=name,
        symbol="TST",
        share_class_figi=figi,
    )
    financials = EquityFinancials()

    return CanonicalEquity(identity=identity, financials=financials)


def _mock_github_client_db(db_bytes: bytes = b"") -> httpx.AsyncClient:
    """
    Creates a mock httpx.AsyncClient simulating GitHub release with a DB asset.

    This mock client intercepts requests to GitHub release endpoints and returns a
    predefined JSON response containing data_store.db.gz as the asset. For all other
    requests, it returns the provided content compressed with gzip.

    Args:
        db_bytes (bytes, optional): The raw database bytes to be compressed.

    Returns:
        httpx.AsyncClient: An asynchronous HTTP client with mock transport.
    """
    gz = gzip.compress(db_bytes)

    def handler(request: httpx.Request) -> httpx.Response:
        if "releases" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "assets": [
                        {
                            "name": "data_store.db.gz",
                            "browser_download_url": "https://x/f",
                        },
                    ],
                },
            )
        return httpx.Response(200, content=gz, headers={"Content-Length": str(len(gz))})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _create_test_db() -> bytes:
    """
    Creates a minimal SQLite database with equity tables and returns its bytes.

    Returns:
        bytes: The raw bytes of a SQLite database file.
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    conn = sqlite3.connect(tmp_path)
    conn.execute("PRAGMA foreign_keys = ON")
    _create_test_tables(conn)
    _insert_test_equity(conn)
    conn.commit()
    conn.close()

    db_bytes = tmp_path.read_bytes()
    tmp_path.unlink()
    return db_bytes


def _create_test_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE canonical_equity_identities (
            share_class_figi TEXT PRIMARY KEY,
            payload TEXT NOT NULL
        ) WITHOUT ROWID
    """)
    conn.execute("""
        CREATE TABLE canonical_equity_snapshots (
            share_class_figi TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (share_class_figi, snapshot_date),
            FOREIGN KEY (share_class_figi)
                REFERENCES canonical_equity_identities(share_class_figi)
        ) WITHOUT ROWID
    """)


def _insert_test_equity(conn: sqlite3.Connection) -> None:
    identity_json = EquityIdentity(
        name="TEST EQUITY",
        symbol="TST",
        share_class_figi="BBG000B9XRY4",
    ).model_dump_json()

    financials_json = EquityFinancials().model_dump_json()

    conn.execute(
        "INSERT INTO canonical_equity_identities (share_class_figi, payload) "
        "VALUES (?, ?)",
        ("BBG000B9XRY4", identity_json),
    )
    conn.execute(
        "INSERT INTO canonical_equity_snapshots "
        "(share_class_figi, snapshot_date, payload) "
        "VALUES (?, ?, ?)",
        ("BBG000B9XRY4", "2025-01-01", financials_json),
    )


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
    db_bytes = _create_test_db()
    download_canonical_equities(_mock_github_client_db(db_bytes))

    with pytest.raises(LookupError):
        retrieve_canonical_equity("BBG000NOTFOUND")


def test_retrieve_canonical_equity_returns_found_equity() -> None:
    """
    ARRANGE: Database exists with target equity
    ACT:     Call retrieve_canonical_equity with existing FIGI
    ASSERT:  Returns the equity
    """
    db_bytes = _create_test_db()
    download_canonical_equities(_mock_github_client_db(db_bytes))

    actual = retrieve_canonical_equity("BBG000B9XRY4")

    assert actual.identity.share_class_figi == "BBG000B9XRY4"


def test_download_canonical_equities_creates_database() -> None:
    """
    ARRANGE: Mock client with valid DB
    ACT:     download_canonical_equities
    ASSERT:  Database file exists after download
    """
    db_path = _DATA_STORE_PATH / "data_store.db"
    db_path.unlink(missing_ok=True)

    db_bytes = _create_test_db()
    download_canonical_equities(_mock_github_client_db(db_bytes))

    assert db_path.exists()


def test_decompress_db_creates_database_from_gz() -> None:
    """
    ARRANGE: gzipped database file
    ACT:     _decompress_db
    ASSERT:  decompressed database file exists with correct content
    """
    original = b"test database content"
    gz_path = _DATA_STORE_PATH / "test_decompress.db.gz"
    db_path = _DATA_STORE_PATH / "test_decompress.db"

    with gzip.open(gz_path, "wb") as f:
        f.write(original)

    _decompress_db(gz_path, db_path)

    assert db_path.read_bytes() == original


def test_decompress_db_removes_gz_file() -> None:
    """
    ARRANGE: gzipped database file
    ACT:     _decompress_db
    ASSERT:  gz file is removed after decompression
    """
    gz_path = _DATA_STORE_PATH / "test_cleanup.db.gz"
    db_path = _DATA_STORE_PATH / "test_cleanup.db"

    with gzip.open(gz_path, "wb") as f:
        f.write(b"data")

    _decompress_db(gz_path, db_path)

    assert not gz_path.exists()


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


def test_retrieve_canonical_equity_history_raises_for_unknown_figi() -> None:
    """
    ARRANGE: Database exists but no snapshots for given FIGI
    ACT:     retrieve_canonical_equity_history
    ASSERT:  LookupError raised
    """
    save_canonical_equities(
        [_create_canonical_equity("BBG000EXIST1")],
        snapshot_date="2025-01-01",
    )

    with pytest.raises(LookupError):
        retrieve_canonical_equity_history("BBG000NOTHIST")


def test_retrieve_canonical_equity_history_returns_snapshots() -> None:
    """
    ARRANGE: Database exists with snapshots for a FIGI
    ACT:     retrieve_canonical_equity_history
    ASSERT:  returns list of snapshots
    """
    figi = "BBG000RETHST"
    save_canonical_equities(
        [_create_canonical_equity(figi)],
        snapshot_date="2025-01-01",
    )

    actual = retrieve_canonical_equity_history(figi)

    assert len(actual) == 1
