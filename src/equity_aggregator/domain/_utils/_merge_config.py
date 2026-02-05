# _utils/_merge_config.py


from decimal import Decimal
from enum import Enum, auto
from typing import NamedTuple

DEFAULT_MAX_DEVIATION = Decimal("0.5")


class Strategy(Enum):
    """
    Enumeration of available merge strategies for RawEquity fields.

    Attributes:
        MODE: Most frequent value, ties broken by first occurrence.
        MEDIAN: Median of numeric values.
        FUZZY_CLUSTER: Fuzzy clustering with frequency weighting.
        UNION: Union of all lists, order-preserving and deduplicated.
    """

    MODE = auto()
    MEDIAN = auto()
    FUZZY_CLUSTER = auto()
    UNION = auto()


class FieldSpec(NamedTuple):
    """
    Specification for how to merge a particular field.

    Attributes:
        strategy: The merge strategy to apply.
        threshold: Similarity threshold for FUZZY_CLUSTER strategy (0-100).
            Ignored for other strategies.
        min_sources: Minimum number of non-None sources required to accept merged value.
            If fewer sources provide data, returns None instead. Defaults to 1.
        max_deviation: Maximum allowed deviation from median
            (as decimal, e.g., 0.5 = 50%). Only applies to MEDIAN strategy.
            Defaults to DEFAULT_MAX_DEVIATION. Set to None to disable
            deviation filtering for a specific field.
    """

    strategy: Strategy
    threshold: int = 90
    min_sources: int = 1
    max_deviation: Decimal | None = DEFAULT_MAX_DEVIATION


# Field-to-strategy mapping for all RawEquity fields
FIELD_CONFIG: dict[str, FieldSpec] = {
    # Identifier and metadata fields (single source acceptable)
    "name": FieldSpec(Strategy.FUZZY_CLUSTER, min_sources=1),
    "symbol": FieldSpec(Strategy.MODE, min_sources=1),
    "isin": FieldSpec(Strategy.MODE, min_sources=1),
    "cusip": FieldSpec(Strategy.MODE, min_sources=1),
    "cik": FieldSpec(Strategy.MODE, min_sources=1),
    "lei": FieldSpec(Strategy.MODE, min_sources=1),
    "currency": FieldSpec(Strategy.MODE, min_sources=1),
    "analyst_rating": FieldSpec(Strategy.MODE, min_sources=1),
    "industry": FieldSpec(Strategy.FUZZY_CLUSTER, min_sources=1),
    "sector": FieldSpec(Strategy.FUZZY_CLUSTER, min_sources=1),
    "mics": FieldSpec(Strategy.UNION, min_sources=1),
    # Critical price and market data (require corroboration from multiple sources)
    # Fields with >50% multi-source coverage that benefit from cross-validation
    "market_cap": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "last_price": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "fifty_two_week_min": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "fifty_two_week_max": FieldSpec(Strategy.MEDIAN, min_sources=2),
    # Other financial metrics
    # Fields with low coverage (<5%) accept single source to prevent data loss
    # Fields with moderate coverage (>20%) require corroboration for quality
    "dividend_yield": FieldSpec(Strategy.MEDIAN),
    "market_volume": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "held_insiders": FieldSpec(Strategy.MEDIAN),
    "held_institutions": FieldSpec(Strategy.MEDIAN),
    "short_interest": FieldSpec(Strategy.MEDIAN),
    "share_float": FieldSpec(Strategy.MEDIAN),
    "shares_outstanding": FieldSpec(Strategy.MEDIAN),
    "revenue_per_share": FieldSpec(Strategy.MEDIAN),
    "profit_margin": FieldSpec(Strategy.MEDIAN),
    "gross_margin": FieldSpec(Strategy.MEDIAN),
    "operating_margin": FieldSpec(Strategy.MEDIAN),
    "free_cash_flow": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "operating_cash_flow": FieldSpec(Strategy.MEDIAN),
    "return_on_equity": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "return_on_assets": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "performance_1_year": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "total_debt": FieldSpec(Strategy.MEDIAN),
    "revenue": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "ebitda": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "trailing_pe": FieldSpec(Strategy.MEDIAN, min_sources=2),
    "price_to_book": FieldSpec(Strategy.MEDIAN),
    "trailing_eps": FieldSpec(Strategy.MEDIAN),
}

# Coherent field groups requiring joint validation
PRICE_RANGE_FIELDS: frozenset[str] = frozenset(
    {
        "last_price",
        "fifty_two_week_min",
        "fifty_two_week_max",
    },
)
