# _utils/_merge.py


from collections.abc import Sequence
from decimal import Decimal
from functools import partial
from typing import NamedTuple

from equity_aggregator.schemas.raw import RawEquity

from ._merge_config import (
    FIELD_CONFIG,
    PRICE_RANGE_FIELDS,
    FieldSpec,
    Strategy,
)
from ._strategies import (
    filter_by_deviation,
    fuzzy_cluster_mode,
    median_decimal,
    mode_first,
    union_ordered,
)


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
        lei: Representative LEI identifier.
        share_class_figi: Validated share class FIGI (must be identical across group).
    """

    symbol: str
    name: str
    isin: str | None
    cusip: str | None
    cik: str | None
    lei: str | None
    share_class_figi: str


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
        lei=mode_first(_extract_field(group, "lei")),
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
    If fewer than min_sources non-None values exist, returns None to prevent
    accepting dubious single-source data.

    Args:
        group (Sequence[RawEquity]): Sequence of RawEquity objects to merge.
        field (str): Name of the field to merge.
        spec (FieldSpec): Strategy specification for this field.

    Returns:
        object: The merged value for this field, or None if quorum not met.
    """
    values = _extract_field(group, field, filter_none=(spec.strategy != Strategy.UNION))

    if spec.max_deviation is not None and spec.strategy == Strategy.MEDIAN:
        values = filter_by_deviation(values, spec.max_deviation)

    if len(values) < spec.min_sources:
        return None

    dispatch = {
        Strategy.MODE: mode_first,
        Strategy.FUZZY_CLUSTER: partial(fuzzy_cluster_mode, threshold=spec.threshold),
        Strategy.UNION: union_ordered,
        Strategy.MEDIAN: median_decimal,
    }

    return dispatch[spec.strategy](values)


def _merge_price_range(
    group: Sequence[RawEquity],
    min_consistent: int = 2,
) -> dict[str, Decimal | None]:
    """
    Merge last_price, fifty_two_week_min, and fifty_two_week_max with tiered quality
    checks.

    Attempts to merge price fields as a coherent triplet when possible, falling back to
    independent field merging when complete records are unavailable. This preserves data
    quality through consistency checks whilst avoiding unnecessary data loss.

    Primary strategy (preferred):
      - Requires records with all three price fields populated (complete records).
      - Filters out records where last_price violates the 52-week range constraint.
      - A 10% tolerance above fifty_two_week_max accommodates timing drift between
        feeds.
      - If quorum of consistent complete records is met (default: 2), returns median
        values.

    Fallback strategy (when quorum not met):
      - Merges each price field independently using per-field configuration from
        FIELD_CONFIG.
      - Each field still requires its own min_sources threshold (typically 2).
      - Allows partial price data when complete triplets are unavailable across sources.

    Args:
        group (Sequence[RawEquity]): Sequence of RawEquity objects to merge.
        min_consistent (int): Minimum number of consistent complete records required
            for primary strategy. Defaults to 2.

    Returns:
        dict[str, Decimal | None]: Dictionary containing merged last_price,
            fifty_two_week_min, and fifty_two_week_max values. Fields may be None
            if neither strategy can satisfy quorum requirements.
    """
    consistent = tuple(
        filter(_is_price_consistent, filter(_is_price_complete, group)),
    )

    if len(consistent) >= min_consistent:
        return {
            "last_price": median_decimal([eq.last_price for eq in consistent]),
            "fifty_two_week_min": median_decimal(
                [eq.fifty_two_week_min for eq in consistent],
            ),
            "fifty_two_week_max": median_decimal(
                [eq.fifty_two_week_max for eq in consistent],
            ),
        }

    # Fallback: merge fields independently
    return {
        field: _apply_strategy(group, field, FIELD_CONFIG[field])
        for field in PRICE_RANGE_FIELDS
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
              fifty_two_week_max, False otherwise. Returns False if any price field
              is None.
    """
    if (
        eq.last_price is None
        or eq.fifty_two_week_min is None
        or eq.fifty_two_week_max is None
    ):
        return False

    price_tolerance = Decimal("1.1")
    return (
        eq.fifty_two_week_min
        <= eq.last_price
        <= eq.fifty_two_week_max * price_tolerance
    )
