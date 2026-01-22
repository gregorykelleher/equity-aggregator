# _utils/parser.py

from equity_aggregator.adapters.data_sources._utils._record_types import (
    EquityRecord,
)


def parse_companies_response(payload: dict) -> tuple[list[EquityRecord], str | None]:
    """
    Parse Intrinio companies API response.

    Args:
        payload (dict): The JSON response from Intrinio API.

    Returns:
        tuple[list[EquityRecord], str | None]: Tuple of (parsed records,
            next_page token).
    """
    companies = payload.get("companies", [])
    next_page = payload.get("next_page")
    valid_companies = [c for c in companies if _is_valid_company(c)]
    records = [_extract_company_record(company) for company in valid_companies]
    return records, next_page


def parse_securities_response(payload: dict) -> list[EquityRecord]:
    """
    Parse Intrinio securities API response.

    Uses the company data embedded in the response (which is authoritative
    for these securities) rather than external company data, to avoid
    ticker reassignment issues where stale company records have incorrect
    identifiers.

    Args:
        payload (dict): The JSON response from Intrinio API.

    Returns:
        list[EquityRecord]: List of security records with company data merged.
    """
    securities = payload.get("securities", [])
    company_data = payload.get("company", {})
    company = _extract_company_record(company_data) if company_data else {}

    return [
        _extract_security_record(security, company)
        for security in securities
        if security.get("share_class_figi")
    ]


def _is_valid_company(company: dict | None) -> bool:
    """
    Check if a company record has required fields.

    Args:
        company (dict | None): Raw company data from API.

    Returns:
        bool: True if company has ticker and name, False otherwise.
    """
    return bool(company and company.get("ticker") and company.get("name"))


def _extract_company_record(company: dict) -> EquityRecord:
    """
    Extract a company record from raw API data.

    Args:
        company (dict): Raw company data from Intrinio API.

    Returns:
        EquityRecord: Normalised company record.
    """
    return {
        "company_id": company.get("id"),
        "company_ticker": company.get("ticker"),
        "name": company.get("name"),
        "lei": company.get("lei"),
        "cik": company.get("cik"),
    }


def _extract_security_record(security: dict, company: EquityRecord) -> EquityRecord:
    """
    Extract a security record merged with company data.

    Args:
        security (dict): Raw security data from Intrinio API.
        company (EquityRecord): Company record to merge with security data.

    Returns:
        EquityRecord: Security record with company data merged.
    """
    return {
        # Company data
        "name": company.get("name"),
        "lei": company.get("lei"),
        "cik": company.get("cik"),
        # Security data
        "ticker": security.get("ticker"),
        "share_class_figi": security.get("share_class_figi"),
        "figi": security.get("figi"),
        "composite_figi": security.get("composite_figi"),
        "currency": security.get("currency"),
        "exchange_mic": security.get("exchange_mic"),
    }
