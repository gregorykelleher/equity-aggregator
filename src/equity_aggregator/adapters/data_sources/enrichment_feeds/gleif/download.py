# gleif/download.py

import csv
import io
import zipfile
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

from .api import _fetch_metadata_with_client


async def download_and_build_isin_index(
    *,
    client_factory: Callable[[], httpx.AsyncClient] | None = None,
) -> dict[str, str]:
    """
    Download the GLEIF ISIN->LEI mapping file and build a lookup index.

    Fetches metadata to get the download link, downloads the ZIP file using
    streaming, extracts the CSV, and builds a dictionary mapping ISINs to
    LEIs.

    Returns:
        dict[str, str]: Dictionary mapping ISIN codes to LEI codes.

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

    Uses a temporary directory for the download to avoid persisting
    large files.

    Returns:
        dict[str, str]: Dictionary mapping ISIN codes to LEI codes.
    """
    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / "isin_lei.zip"
        await _stream_to_file(client, download_link, zip_path)
        return _parse_zip(zip_path)


async def _stream_to_file(
    client: httpx.AsyncClient,
    url: str,
    destination: Path,
) -> None:
    """
    Stream response body to a file.

    Returns:
        None
    """
    async with client.stream("GET", url) as response:
        response.raise_for_status()

        with destination.open("wb") as f:
            async for chunk in response.aiter_bytes(chunk_size=65536):
                f.write(chunk)


def _parse_zip(zip_path: Path) -> dict[str, str]:
    """
    Extract and parse the CSV from a ZIP file into an ISIN->LEI index.

    Finds the first CSV file in the archive and parses it row by row,
    building a dictionary that maps ISIN codes to LEI codes.

    Returns:
        dict[str, str]: Dictionary mapping ISIN codes to LEI codes.

    Raises:
        ValueError: If no CSV file is found in the archive.
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_name = _find_csv(zf)
        if csv_name is None:
            raise ValueError("No CSV file found in GLEIF ZIP archive.")

        with zf.open(csv_name) as csv_file:
            return _parse_csv(csv_file)


def _find_csv(zf: zipfile.ZipFile) -> str | None:
    """
    Find the first CSV file in a ZIP archive.

    Returns:
        str | None: Name of the first CSV file found, or None.
    """
    return next(
        (name for name in zf.namelist() if name.lower().endswith(".csv")),
        None,
    )


def _parse_csv(csv_file: io.BufferedReader) -> dict[str, str]:
    """
    Parse the GLEIF ISIN->LEI CSV file into a look-up dictionary.

    The CSV has columns: LEI, ISIN.

    Returns:
        dict[str, str]: Dictionary mapping ISIN codes to LEI codes.
    """
    text_wrapper = io.TextIOWrapper(csv_file, encoding="utf-8")
    reader = csv.DictReader(text_wrapper)

    index: dict[str, str] = {}

    for row in reader:
        isin = row.get("ISIN", "").strip().upper() or None
        lei = row.get("LEI", "").strip().upper() or None

        if isin and lei:
            index[isin] = lei

    return index
