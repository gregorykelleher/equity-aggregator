# feeds/_staleness.py

from datetime import UTC, datetime, timedelta

from equity_aggregator.schemas.raw import RawEquity

DEFAULT_MAX_TRADE_AGE_HOURS = 36

PRICE_SENSITIVE_FIELDS = frozenset(
    {
        "last_price",
        "fifty_two_week_min",
        "fifty_two_week_max",
        "market_volume",
        "market_cap",
    }
)

# Validate that all price-sensitive fields are present in RawEquity
# (prevents silent drift if fields are renamed or removed)
assert set(RawEquity.model_fields) >= PRICE_SENSITIVE_FIELDS, (
    "PRICE_SENSITIVE_FIELDS contains fields not present in RawEquity"
)


def is_trade_stale(
    last_trade_time: datetime | None,
    *,
    max_age_hours: int = DEFAULT_MAX_TRADE_AGE_HOURS,
) -> bool:
    """
    Determine whether a last-trade timestamp is stale.

    Compares the elapsed time since `last_trade_time` against
    `max_age_hours`. Timezone-naive datetimes are assumed UTC.

    Note:
        Returns False (fail-open) when `last_trade_time` is None,
        since absence of a timestamp does not imply staleness.

    Returns:
        bool: True if the trade data is stale, False otherwise.
    """
    if last_trade_time is None:
        return False

    now = datetime.now(UTC)
    trade_time = _ensure_utc(last_trade_time)
    elapsed = now - trade_time

    return elapsed > timedelta(hours=max_age_hours)


def nullify_price_fields(fields: dict[str, object]) -> dict[str, object]:
    """
    Return a new dict with price-sensitive fields set to None.

    Non-price fields are preserved unchanged.

    Returns:
        dict[str, object]: A copy of the input with price fields nullified.
    """
    return {
        key: None if key in PRICE_SENSITIVE_FIELDS else value
        for key, value in fields.items()
    }


def _ensure_utc(dt: datetime) -> datetime:
    """
    Coerce a datetime to UTC-aware.

    Naive datetimes are assumed to already represent UTC.

    Returns:
        datetime: A timezone-aware datetime in UTC.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
