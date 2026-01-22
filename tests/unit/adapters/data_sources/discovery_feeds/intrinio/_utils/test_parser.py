# _utils/test_parser.py

import pytest

from equity_aggregator.adapters.data_sources.discovery_feeds.intrinio._utils import (
    parse_companies_response,
    parse_securities_response,
)

pytestmark = pytest.mark.unit


def test_parse_companies_response_extracts_records() -> None:
    """
    ARRANGE: payload with one valid company
    ACT:     call parse_companies_response
    ASSERT:  returns one record with correct fields
    """
    payload = {
        "companies": [
            {"id": "com_123", "ticker": "AAPL", "name": "Apple Inc", "lei": "LEI123"},
        ],
    }

    records, _ = parse_companies_response(payload)

    assert len(records) == 1


def test_parse_companies_response_maps_company_fields() -> None:
    """
    ARRANGE: payload with company data
    ACT:     call parse_companies_response
    ASSERT:  record contains mapped fields
    """
    payload = {
        "companies": [
            {
                "id": "com_123",
                "ticker": "AAPL",
                "name": "Apple Inc",
                "lei": "LEI123",
                "cik": "0000320193",
            },
        ],
    }

    records, _ = parse_companies_response(payload)

    assert records[0]["company_ticker"] == "AAPL"


def test_parse_companies_response_extracts_next_page() -> None:
    """
    ARRANGE: payload with next_page token
    ACT:     call parse_companies_response
    ASSERT:  returns next_page token
    """
    payload = {"companies": [], "next_page": "abc123"}

    _, next_page = parse_companies_response(payload)

    assert next_page == "abc123"


def test_parse_companies_response_returns_none_when_no_next_page() -> None:
    """
    ARRANGE: payload without next_page
    ACT:     call parse_companies_response
    ASSERT:  returns None for next_page
    """
    payload = {"companies": []}

    _, next_page = parse_companies_response(payload)

    assert next_page is None


def test_parse_companies_response_skips_missing_ticker() -> None:
    """
    ARRANGE: payload with company missing ticker
    ACT:     call parse_companies_response
    ASSERT:  returns empty records
    """
    payload = {"companies": [{"id": "com_123", "name": "No Ticker Inc"}]}

    records, _ = parse_companies_response(payload)

    assert len(records) == 0


def test_parse_companies_response_skips_missing_name() -> None:
    """
    ARRANGE: payload with company missing name
    ACT:     call parse_companies_response
    ASSERT:  returns empty records
    """
    payload = {"companies": [{"id": "com_123", "ticker": "NOTK"}]}

    records, _ = parse_companies_response(payload)

    assert len(records) == 0


def test_parse_companies_response_skips_none_company() -> None:
    """
    ARRANGE: payload with None in companies list
    ACT:     call parse_companies_response
    ASSERT:  returns empty records
    """
    payload = {"companies": [None]}

    records, _ = parse_companies_response(payload)

    assert len(records) == 0


def test_parse_securities_response_extracts_security() -> None:
    """
    ARRANGE: payload with one valid security and company data
    ACT:     call parse_securities_response
    ASSERT:  returns one record
    """
    payload = {
        "securities": [
            {"ticker": "AAPL", "share_class_figi": "BBG000B9XRY4", "currency": "USD"},
        ],
        "company": {
            "id": "com_123",
            "ticker": "AAPL",
            "name": "Apple Inc",
            "lei": "LEI123",
            "cik": "0000320193",
        },
    }

    records = parse_securities_response(payload)

    assert len(records) == 1


def test_parse_securities_response_merges_company_data() -> None:
    """
    ARRANGE: payload with security and embedded company data
    ACT:     call parse_securities_response
    ASSERT:  record contains company name from payload
    """
    payload = {
        "securities": [{"ticker": "AAPL", "share_class_figi": "BBG000B9XRY4"}],
        "company": {
            "id": "com_123",
            "ticker": "AAPL",
            "name": "Apple Inc",
            "lei": "LEI123",
        },
    }

    records = parse_securities_response(payload)

    assert records[0]["name"] == "Apple Inc"


def test_parse_securities_response_skips_missing_share_class_figi() -> None:
    """
    ARRANGE: security without share_class_figi
    ACT:     call parse_securities_response
    ASSERT:  returns empty records
    """
    payload = {
        "securities": [{"ticker": "AAPL"}],
        "company": {"id": "com_123", "ticker": "AAPL", "name": "Apple Inc"},
    }

    records = parse_securities_response(payload)

    assert len(records) == 0


def test_parse_securities_response_includes_exchange_mic() -> None:
    """
    ARRANGE: security with exchange_mic
    ACT:     call parse_securities_response
    ASSERT:  record contains exchange_mic
    """
    payload = {
        "securities": [
            {
                "ticker": "AAPL",
                "share_class_figi": "BBG000B9XRY4",
                "exchange_mic": "XNAS",
            },
        ],
        "company": {"id": "com_123", "ticker": "AAPL", "name": "Apple Inc"},
    }

    records = parse_securities_response(payload)

    assert records[0]["exchange_mic"] == "XNAS"


def test_parse_securities_response_handles_missing_company() -> None:
    """
    ARRANGE: payload with securities but no company data
    ACT:     call parse_securities_response
    ASSERT:  returns records with empty company fields
    """
    payload = {
        "securities": [{"ticker": "AAPL", "share_class_figi": "BBG000B9XRY4"}],
    }

    records = parse_securities_response(payload)

    assert records[0]["lei"] is None
