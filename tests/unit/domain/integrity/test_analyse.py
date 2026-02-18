# domain/integrity/test_analyse.py

import pytest

from equity_aggregator.domain.integrity.analyse import analyse_canonical_equities
from equity_aggregator.schemas import CanonicalEquity, EquityFinancials, EquityIdentity
from equity_aggregator.schemas.integrity import IntegrityReport
from equity_aggregator.storage.data_store import save_canonical_equities

pytestmark = pytest.mark.unit


def _create_equity(figi: str, name: str = "TEST") -> CanonicalEquity:
    """
    Create a minimal CanonicalEquity for testing.

    Args:
        figi: FIGI identifier.
        name: Company name.

    Returns:
        CanonicalEquity: Test equity instance.
    """
    return CanonicalEquity(
        identity=EquityIdentity(name=name, symbol="TST", share_class_figi=figi),
        financials=EquityFinancials(),
    )


def test_analyse_returns_integrity_report() -> None:
    """
    ARRANGE: database with one equity
    ACT:     analyse_canonical_equities
    ASSERT:  returns IntegrityReport instance
    """
    save_canonical_equities([_create_equity("BBG000ANLY01")])

    actual = analyse_canonical_equities()

    assert isinstance(actual, IntegrityReport)


def test_analyse_report_dataset_size() -> None:
    """
    ARRANGE: database with two equities
    ACT:     analyse_canonical_equities
    ASSERT:  dataset_size == 2
    """
    expected = 2
    equities = [
        _create_equity("BBG000ANLY02"),
        _create_equity("BBG000ANLY03"),
    ]
    save_canonical_equities(equities)

    actual = analyse_canonical_equities()

    assert actual.dataset_size == expected


def test_analyse_report_snapshot_count() -> None:
    """
    ARRANGE: equities saved across two dates
    ACT:     analyse_canonical_equities
    ASSERT:  snapshot_count == 2
    """
    expected = 2
    equity = _create_equity("BBG000ANLY04")
    save_canonical_equities([equity], snapshot_date="2025-01-01")
    save_canonical_equities([equity], snapshot_date="2025-01-02")

    actual = analyse_canonical_equities()

    assert actual.snapshot_count == expected


def test_analyse_report_sections_analysed() -> None:
    """
    ARRANGE: database with one equity
    ACT:     analyse_canonical_equities
    ASSERT:  sections_analysed == 9
    """
    expected_sections = 9
    save_canonical_equities([_create_equity("BBG000ANLY05")])

    actual = analyse_canonical_equities()

    assert actual.sections_analysed == expected_sections


def test_analyse_saves_json_report(data_sql_store_dir) -> None:
    """
    ARRANGE: database with one equity
    ACT:     analyse_canonical_equities
    ASSERT:  integrity_report.json exists in data store
    """
    save_canonical_equities([_create_equity("BBG000ANLY06")])

    analyse_canonical_equities()

    report_path = data_sql_store_dir / "integrity_report.json"
    assert report_path.exists()


def test_analyse_empty_database_returns_report() -> None:
    """
    ARRANGE: empty database (no equities)
    ACT:     analyse_canonical_equities
    ASSERT:  returns report with dataset_size == 0
    """
    actual = analyse_canonical_equities()

    assert actual.dataset_size == 0
