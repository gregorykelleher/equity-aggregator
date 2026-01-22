# _utils/test__merge_config.py

from decimal import Decimal

import pytest

from equity_aggregator.domain._utils._merge_config import (
    FIELD_CONFIG,
    PRICE_RANGE_FIELDS,
    FieldSpec,
    Strategy,
)

pytestmark = pytest.mark.unit


def test_strategy_enum_has_expected_members() -> None:
    """
    ARRANGE: Strategy enum
    ACT:     inspect members
    ASSERT:  contains MODE, MEDIAN, FUZZY_CLUSTER, UNION
    """
    expected_members = {
        "MODE",
        "MEDIAN",
        "FUZZY_CLUSTER",
        "UNION",
    }
    assert {member.name for member in Strategy} == expected_members


def test_field_spec_default_threshold() -> None:
    """
    ARRANGE: FieldSpec with only strategy
    ACT:     create instance
    ASSERT:  threshold defaults to 90
    """
    spec = FieldSpec(Strategy.MODE)
    expected_threshold = 90

    assert spec.threshold == expected_threshold


def test_field_spec_default_min_sources() -> None:
    """
    ARRANGE: FieldSpec with only strategy
    ACT:     create instance
    ASSERT:  min_sources defaults to 1
    """
    spec = FieldSpec(Strategy.MODE)
    expected_min_sources = 1

    assert spec.min_sources == expected_min_sources


def test_field_spec_default_max_deviation() -> None:
    """
    ARRANGE: FieldSpec with only strategy
    ACT:     create instance
    ASSERT:  max_deviation defaults to None
    """
    spec = FieldSpec(Strategy.MODE)
    expected_deviation = None

    assert spec.max_deviation is expected_deviation


def test_field_spec_custom_values() -> None:
    """
    ARRANGE: FieldSpec with all custom parameters
    ACT:     create instance
    ASSERT:  all values set correctly
    """
    custom_threshold = 85
    custom_min_sources = 3
    custom_max_deviation = Decimal("0.25")

    spec = FieldSpec(
        Strategy.MEDIAN,
        threshold=custom_threshold,
        min_sources=custom_min_sources,
        max_deviation=custom_max_deviation,
    )

    expected = FieldSpec(
        Strategy.MEDIAN,
        threshold=custom_threshold,
        min_sources=custom_min_sources,
        max_deviation=custom_max_deviation,
    )

    assert spec == expected


def test_field_config_contains_all_raw_equity_fields() -> None:
    """
    ARRANGE: FIELD_CONFIG dictionary
    ACT:     check keys
    ASSERT:  contains all expected RawEquity fields
    """
    expected_fields = {
        "name",
        "symbol",
        "isin",
        "cusip",
        "cik",
        'lei',
        "currency",
        "analyst_rating",
        "industry",
        "sector",
        "mics",
        "market_cap",
        "last_price",
        "fifty_two_week_min",
        "fifty_two_week_max",
        "dividend_yield",
        "market_volume",
        "held_insiders",
        "held_institutions",
        "short_interest",
        "share_float",
        "shares_outstanding",
        "revenue_per_share",
        "profit_margin",
        "gross_margin",
        "operating_margin",
        "free_cash_flow",
        "operating_cash_flow",
        "return_on_equity",
        "return_on_assets",
        "performance_1_year",
        "total_debt",
        "revenue",
        "ebitda",
        "trailing_pe",
        "price_to_book",
        "trailing_eps",
    }
    assert set(FIELD_CONFIG.keys()) == expected_fields


def test_field_config_identifier_fields_use_mode() -> None:
    """
    ARRANGE: FIELD_CONFIG dictionary
    ACT:     check identifier field strategies
    ASSERT:  symbol, isin, cusip, cik use MODE strategy
    """
    identifier_fields = {
        "symbol",
        "isin",
        "cusip",
        "cik",
    }
    assert all(
        FIELD_CONFIG[field].strategy == Strategy.MODE for field in identifier_fields
    )


def test_field_config_text_fields_use_fuzzy_cluster() -> None:
    """
    ARRANGE: FIELD_CONFIG dictionary
    ACT:     check text field strategies
    ASSERT:  name, industry, sector use FUZZY_CLUSTER strategy
    """
    text_fields = {
        "name",
        "industry",
        "sector",
    }
    assert all(
        FIELD_CONFIG[field].strategy == Strategy.FUZZY_CLUSTER for field in text_fields
    )


def test_field_config_mics_uses_union() -> None:
    """
    ARRANGE: FIELD_CONFIG dictionary
    ACT:     check mics field strategy
    ASSERT:  uses UNION strategy
    """
    assert FIELD_CONFIG["mics"].strategy == Strategy.UNION


def test_field_config_financial_fields_use_median() -> None:
    """
    ARRANGE: FIELD_CONFIG dictionary
    ACT:     check financial field strategies
    ASSERT:  numeric financial fields use MEDIAN strategy
    """
    financial_fields = [
        "market_cap",
        "last_price",
        "dividend_yield",
        "revenue",
        "ebitda",
        "trailing_pe",
    ]
    assert all(
        FIELD_CONFIG[field].strategy == Strategy.MEDIAN for field in financial_fields
    )


def test_field_config_financial_fields_require_multiple_sources() -> None:
    """
    ARRANGE: FIELD_CONFIG dictionary
    ACT:     check financial field min_sources
    ASSERT:  critical financial fields require min_sources=2
    """
    critical_fields = [
        "market_cap",
        "last_price",
        "fifty_two_week_min",
        "fifty_two_week_max",
    ]

    expected_min_sources = 2

    assert all(
        FIELD_CONFIG[field].min_sources == expected_min_sources
        for field in critical_fields
    )


def test_field_config_low_coverage_fields_accept_single_source() -> None:
    """
    ARRANGE: FIELD_CONFIG dictionary
    ACT:     check low-coverage financial field min_sources
    ASSERT:  fields with <5% coverage accept single source (min_sources=1)
    """
    low_coverage_fields = [
        "dividend_yield",
        "held_insiders",
        "held_institutions",
        "short_interest",
        "share_float",
        "shares_outstanding",
        "revenue_per_share",
        "profit_margin",
        "gross_margin",
        "operating_margin",
        "operating_cash_flow",
        "total_debt",
        "price_to_book",
        "trailing_eps",
    ]

    single_source_required = 1

    assert all(
        FIELD_CONFIG[field].min_sources == single_source_required
        for field in low_coverage_fields
    )


def test_field_config_financial_fields_have_deviation_filter() -> None:
    """
    ARRANGE: FIELD_CONFIG dictionary
    ACT:     check financial field max_deviation
    ASSERT:  median fields have 50% deviation threshold
    """
    allowed_deviation = Decimal("0.5")
    expected_deviation = {
        "market_cap": allowed_deviation,
        "last_price": allowed_deviation,
        "revenue": allowed_deviation,
    }

    assert {
        field: FIELD_CONFIG[field].max_deviation for field in expected_deviation
    } == expected_deviation


def test_field_config_identifier_fields_accept_single_source() -> None:
    """
    ARRANGE: FIELD_CONFIG dictionary
    ACT:     check identifier field min_sources
    ASSERT:  identifier fields accept single source (min_sources=1)
    """
    identifier_fields = ["symbol", "isin", "cusip", "cik", "name"]
    single_source_required = 1

    assert all(
        FIELD_CONFIG[field].min_sources == single_source_required
        for field in identifier_fields
    )


def test_price_range_fields_contains_expected_fields() -> None:
    """
    ARRANGE: PRICE_RANGE_FIELDS frozenset
    ACT:     check members
    ASSERT:  contains last_price, fifty_two_week_min, fifty_two_week_max
    """
    expected_fields = {
        "last_price",
        "fifty_two_week_min",
        "fifty_two_week_max",
    }
    assert expected_fields <= PRICE_RANGE_FIELDS


def test_price_range_fields_has_exactly_three_members() -> None:
    """
    ARRANGE: PRICE_RANGE_FIELDS frozenset
    ACT:     count members
    ASSERT:  contains exactly 3 fields
    """
    expected_member_count = 3
    assert len(PRICE_RANGE_FIELDS) == expected_member_count


def test_price_range_fields_is_immutable() -> None:
    """
    ARRANGE: PRICE_RANGE_FIELDS frozenset
    ACT:     attempt to modify
    ASSERT:  frozenset is immutable (type check)
    """
    assert isinstance(PRICE_RANGE_FIELDS, frozenset)
