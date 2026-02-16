# gleif/config.py

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GleifConfig:
    """
    Immutable configuration for GLEIF API endpoints.

    Centralises the URL patterns used to fetch LEI data from the GLEIF API.
    Ensures a single source of truth for all endpoints required by the
    enrichment feed.

    Returns:
        GleifConfig: Immutable configuration object with GLEIF endpoints.
    """

    # ISIN->LEI bulk mapping metadata endpoint
    isin_lei_url: str = "https://mapping.gleif.org/api/v2/isin-lei/latest"

    # entity name autocompletion search endpoint
    autocompletions_url: str = "https://api.gleif.org/api/v1/autocompletions"

    # LEI records endpoint for parent entity lookups
    lei_records_url: str = "https://api.gleif.org/api/v1/lei-records"
