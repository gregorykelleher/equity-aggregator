# _utils/_merge.py


from collections.abc import Sequence
from decimal import Decimal
from enum import Enum, auto
from functools import partial
from typing import NamedTuple

from equity_aggregator.schemas.raw import RawEquity

from ._strategies import fuzzy_cluster_mode, median_decimal, mode_first, union_ordered


def _extract_field(
    group: Sequence[RawEquity],
    field: str,
    *,
    filter_none: bool = True,
) -> list:
    """
    Extract field values from a group of RawEquity objects.

    Retrieves the specified field from each RawEquity object in the group.
    Optionally filters out None values from the result.

    Args:
        group (Sequence[RawEquity]): Sequence of RawEquity objects to extract from.
        field (str): Name of the field to extract from each object.
        filter_none (bool, optional): If True, exclude None values from the result.
            Defaults to True.

    Returns:
        list: Extracted field values, optionally filtered to exclude None values.
    """
    values = [getattr(eq, field) for eq in group]
    return [v for v in values if v is not None] if filter_none else values


class EquityIdentifiers(NamedTuple):
    """
    Representative identifiers extracted from a group of RawEquity records.

    Attributes:
        symbol: Representative ticker symbol.
        name: Representative equity name.
        isin: Representative ISIN identifier.
        cusip: Representative CUSIP identifier.
        cik: Representative CIK identifier.
        share_class_figi: Validated share class FIGI (must be identical across group).
    """

    symbol: str
    name: str
    isin: str | None
    cusip: str | None
    cik: str | None
    share_class_figi: str


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
    """

    strategy: Strategy
    threshold: int = 90


# Field-to-strategy mapping for all RawEquity fields
FIELD_CONFIG: dict[str, FieldSpec] = {
    # Identifier and metadata fields
    "name": FieldSpec(Strategy.FUZZY_CLUSTER),
    "symbol": FieldSpec(Strategy.MODE),
    "isin": FieldSpec(Strategy.MODE),
    "cusip": FieldSpec(Strategy.MODE),
    "cik": FieldSpec(Strategy.MODE),
    "currency": FieldSpec(Strategy.MODE),
    "analyst_rating": FieldSpec(Strategy.MODE),
    "industry": FieldSpec(Strategy.FUZZY_CLUSTER),
    "sector": FieldSpec(Strategy.FUZZY_CLUSTER),
    "mics": FieldSpec(Strategy.UNION),
    # Decimal financial metrics (all use MEDIAN strategy)
    "market_cap": FieldSpec(Strategy.MEDIAN),
    "dividend_yield": FieldSpec(Strategy.MEDIAN),
    "market_volume": FieldSpec(Strategy.MEDIAN),
    "held_insiders": FieldSpec(Strategy.MEDIAN),
    "held_institutions": FieldSpec(Strategy.MEDIAN),
    "short_interest": FieldSpec(Strategy.MEDIAN),
    "share_float": FieldSpec(Strategy.MEDIAN),
    "shares_outstanding": FieldSpec(Strategy.MEDIAN),
    "revenue_per_share": FieldSpec(Strategy.MEDIAN),
    "profit_margin": FieldSpec(Strategy.MEDIAN),
    "gross_margin": FieldSpec(Strategy.MEDIAN),
    "operating_margin": FieldSpec(Strategy.MEDIAN),
    "free_cash_flow": FieldSpec(Strategy.MEDIAN),
    "operating_cash_flow": FieldSpec(Strategy.MEDIAN),
    "return_on_equity": FieldSpec(Strategy.MEDIAN),
    "return_on_assets": FieldSpec(Strategy.MEDIAN),
    "performance_1_year": FieldSpec(Strategy.MEDIAN),
    "total_debt": FieldSpec(Strategy.MEDIAN),
    "revenue": FieldSpec(Strategy.MEDIAN),
    "ebitda": FieldSpec(Strategy.MEDIAN),
    "trailing_pe": FieldSpec(Strategy.MEDIAN),
    "price_to_book": FieldSpec(Strategy.MEDIAN),
    "trailing_eps": FieldSpec(Strategy.MEDIAN),
    # Price range fields (merged via _merge_price_range for coherent validation)
    "last_price": FieldSpec(Strategy.MEDIAN),
    "fifty_two_week_min": FieldSpec(Strategy.MEDIAN),
    "fifty_two_week_max": FieldSpec(Strategy.MEDIAN),
}

# Coherent field groups requiring joint validation
PRICE_RANGE_FIELDS: frozenset[str] = frozenset(
    {
        "last_price",
        "fifty_two_week_min",
        "fifty_two_week_max",
    },
)


def merge(group: Sequence[RawEquity]) -> RawEquity:
    """
    Merge a group of RawEquity records into a single, representative RawEquity instance.

    Each field is merged using a configurable strategy defined in FIELD_CONFIG:
      - Most fields use one of: mode (most frequent), median (for numerics), fuzzy
      clustering (for similar strings), or union (for lists).
      - Price range fields (last_price, fifty_two_week_min, fifty_two_week_max) are
      merged together with additional consistency checks.

    The merging process ensures that all records in the group share the same
    share_class_figi; otherwise, a ValueError is raised.

    Args:
        group (Sequence[RawEquity]): Non-empty sequence of RawEquity objects to merge.
        All must have identical share_class_figi.

    Returns:
        RawEquity: A new RawEquity instance with merged values for each field, according
        to the configured strategies.

    Raises:
        ValueError: If the group is empty or contains multiple distinct share_class_figi
        values.
    """
    share_class_figi = _validate_share_class_figi(group)

    merged = {
        "share_class_figi": share_class_figi,
        **{
            field: _apply_strategy(group, field, spec)
            for field, spec in FIELD_CONFIG.items()
            if field not in PRICE_RANGE_FIELDS
        },
        **_merge_price_range(group),
    }

    return RawEquity.model_validate(merged)


def extract_identifiers(group: Sequence[RawEquity]) -> EquityIdentifiers:
    """
    Compute representative identifiers from a group of RawEquity records.

    Uses the same resolution algorithms as merge() — mode for IDs,
    fuzzy clustering for name, frequency for symbol.

    Args:
        group: A non-empty sequence of RawEquity objects from which to extract
            identifiers. All records must share the same share_class_figi.

    Returns:
        EquityIdentifiers: Representative identifiers resolved from the group.

    Raises:
        ValueError: If the group is empty or contains multiple distinct
            share_class_figi values.
    """
    share_class_figi = _validate_share_class_figi(group)

    return EquityIdentifiers(
        symbol=mode_first(_extract_field(group, "symbol")),
        name=fuzzy_cluster_mode(_extract_field(group, "name")),
        isin=mode_first(_extract_field(group, "isin")),
        cusip=mode_first(_extract_field(group, "cusip")),
        cik=mode_first(_extract_field(group, "cik")),
        share_class_figi=share_class_figi,
    )


def _validate_share_class_figi(group: Sequence[RawEquity]) -> str:
    """
    Validates that all RawEquity objects in the group share the same
    share_class_figi value.

    Args:
        group (Sequence[RawEquity]): A non-empty sequence of RawEquity objects to
            validate.

    Raises:
        ValueError: If the group is empty or contains multiple distinct
            share_class_figi values.

    Returns:
        str: The single shared share_class_figi value present in the group.
    """
    if not group:
        raise ValueError("Cannot merge an empty group of equities")

    figis = {raw_equity.share_class_figi for raw_equity in group}
    if len(figis) != 1:
        raise ValueError(
            "All raw equities in the group must have identical share_class_figi values "
            f"(found: {sorted(figis)})",
        )
    return figis.pop()


def _apply_strategy(
    group: Sequence[RawEquity],
    field: str,
    spec: FieldSpec,
) -> object:
    """
    Apply a specific merge strategy to a field.

    Extracts field values from the group and applies the configured strategy.

    Args:
        group (Sequence[RawEquity]): Sequence of RawEquity objects to merge.
        field (str): Name of the field to merge.
        spec (FieldSpec): Strategy specification for this field.

    Returns:
        object: The merged value for this field.
    """
    values = _extract_field(group, field, filter_none=(spec.strategy != Strategy.UNION))

    dispatch = {
        Strategy.MODE: mode_first,
        Strategy.FUZZY_CLUSTER: partial(fuzzy_cluster_mode, threshold=spec.threshold),
        Strategy.UNION: union_ordered,
        Strategy.MEDIAN: median_decimal,
    }

    return dispatch[spec.strategy](values)


def _merge_price_range(
    group: Sequence[RawEquity],
) -> dict[str, Decimal | None]:
    """
    Merge last_price, fifty_two_week_min, and fifty_two_week_max as a coherent unit,
    filtering out records where the price violates the 52-week range constraint.

    Records missing any of the three fields are excluded from consistency checks.
    A 10% tolerance above fifty_two_week_max accommodates timing drift between feeds.

    Falls back to independent field-wise merge if no consistent complete records exist.

    Args:
        group (Sequence[RawEquity]): Sequence of RawEquity objects to merge.

    Returns:
        dict[str, Decimal | None]: Dictionary containing merged last_price,
            fifty_two_week_min, and fifty_two_week_max values.
    """
    consistent = tuple(
        filter(_is_price_consistent, filter(_is_price_complete, group)),
    )

    if consistent:
        return {
            "last_price": median_decimal([eq.last_price for eq in consistent]),
            "fifty_two_week_min": median_decimal(
                [eq.fifty_two_week_min for eq in consistent],
            ),
            "fifty_two_week_max": median_decimal(
                [eq.fifty_two_week_max for eq in consistent],
            ),
        }

    return {
        "last_price": median_decimal(
            _extract_field(group, "last_price"),
        ),
        "fifty_two_week_min": median_decimal(
            _extract_field(group, "fifty_two_week_min"),
        ),
        "fifty_two_week_max": median_decimal(
            _extract_field(group, "fifty_two_week_max"),
        ),
    }


def _is_price_complete(eq: RawEquity) -> bool:
    """
    Checks if a RawEquity record has non-null values for last_price, fifty_two_week_min,
    and fifty_two_week_max.

    Args:
        eq (RawEquity): The RawEquity instance to check.

    Returns:
        bool: True if all three price fields are not None, False otherwise.
    """
    return (
        eq.last_price is not None
        and eq.fifty_two_week_min is not None
        and eq.fifty_two_week_max is not None
    )


def _is_price_consistent(eq: RawEquity) -> bool:
    """
    Checks if the last_price of a RawEquity record falls within its fifty_two_week_min
    and fifty_two_week_max range, allowing a 10% tolerance above the max.

    Args:
        eq (RawEquity): The RawEquity instance to check.

    Returns:
        bool: True if last_price is between fifty_two_week_min and up to 10% above
              fifty_two_week_max, False otherwise.
    """
    price_tolerance = Decimal("1.1")
    return (
        eq.fifty_two_week_min
        <= eq.last_price
        <= eq.fifty_two_week_max * price_tolerance
    )
