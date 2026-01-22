# gleif/download.py

from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

from .api import _fetch_metadata_with_client
from .parser import parse_zip


async def download_and_build_index(
    *,
    client_factory: Callable[[], httpx.AsyncClient] | None = None,
) -> dict[str, str]:
    """
    Download the GLEIF ISIN->LEI mapping file and build a lookup index.

    Fetches metadata to get the download link, downloads the ZIP file using
    streaming, extracts the CSV, and builds a dictionary mapping ISINs to LEIs.

    Args:
        client_factory: Factory function to create an HTTP client.
            Defaults to make_client from _utils.

    Returns:
        Dictionary mapping ISIN codes to LEI codes.

    Raises:
        ValueError: If metadata or download link is unavailable.
    """
    factory = client_factory or make_client

    async with factory() as client:
        try:
            metadata = await _fetch_metadata_with_client(client)
        except Exception as error:
            raise ValueError("Failed to retrieve GLEIF ISIN->LEI metadata.") from error

        download_link = metadata.get("download_link")
        if not download_link:
            raise ValueError("GLEIF metadata missing download_link.")

        return await _download_and_parse(client, str(download_link))


async def _download_and_parse(
    client: httpx.AsyncClient,
    download_link: str,
) -> dict[str, str]:
    """
    Download the GLEIF mapping ZIP file and parse it into an index.

    Uses a temporary directory for the download to avoid persisting large files.

    Args:
        client: HTTP client to use for the download.
        download_link: URL to download the ZIP file from.

    Returns:
        Dictionary mapping ISIN codes to LEI codes.
    """
    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "isin_lei.zip"
        await _stream_download(client, download_link, zip_path)
        return parse_zip(zip_path)


async def _stream_download(
    client: httpx.AsyncClient,
    url: str,
    destination: Path,
) -> None:
    """
    Stream download a file from a URL to a local path.

    Uses chunked transfer to handle large files efficiently without
    loading the entire response into memory.

    Args:
        client: HTTP client to use for the download.
        url: URL to download from.
        destination: Local file path to write to.
    """
    await _stream_to_file(client, url, destination)


async def _stream_to_file(
    client: httpx.AsyncClient,
    url: str,
    destination: Path,
) -> None:
    """
    Stream response body to a file.

    Args:
        client: HTTP client to use for the request.
        url: URL to download from.
        destination: Local file path to write to.
    """
    async with client.stream("GET", url) as response:
        response.raise_for_status()

        with destination.open("wb") as f:
            async for chunk in response.aiter_bytes(chunk_size=65536):
                f.write(chunk)
