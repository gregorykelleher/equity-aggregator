# gleif/api.py

import logging
from collections.abc import Callable

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

logger = logging.getLogger(__name__)

GLEIF_ISIN_LEI_URL = "https://mapping.gleif.org/api/v2/isin-lei/latest"


async def fetch_metadata(
    *,
    client_factory: Callable[[], httpx.AsyncClient] | None = None,
) -> dict[str, object] | None:
    """
    Fetch the latest GLEIF ISIN->LEI mapping metadata from the API.

    Calls the GLEIF mapping API to retrieve metadata about the latest
    ISIN->LEI relationship file, including the download link.

    Args:
        client_factory: Factory function to create an HTTP client.

    Returns:
        Metadata dict with the following keys, or None on failure:
            - id: UUID of the mapping file
            - file_name: Name of the ZIP file
            - uploaded_at: ISO-8601 upload timestamp
            - download_link: URL to download the ZIP
    """
    logger.info("Fetching GLEIF ISIN->LEI metadata from API.")

    factory = client_factory or make_client

    try:
        async with factory() as client:
            return await _fetch_metadata_with_client(client)
    except Exception as error:
        logger.error("Failed to fetch GLEIF metadata: %s", error, exc_info=True)
        return None


async def _fetch_metadata_with_client(
    client: httpx.AsyncClient,
) -> dict[str, object]:
    """
    Fetch metadata using the provided HTTP client.

    Args:
        client: HTTP client to use for the request.

    Returns:
        Metadata dict with id, file_name, uploaded_at, and download_link.
    """
    response = await client.get(GLEIF_ISIN_LEI_URL)
    response.raise_for_status()
    payload = response.json()

    data = payload.get("data", {})
    attrs = data.get("attributes", {})

    return {
        "id": data.get("id"),
        "file_name": attrs.get("fileName"),
        "uploaded_at": attrs.get("uploadedAt"),
        "download_link": attrs.get("downloadLink"),
    }
