# feeds/intrinio_feed_data.py

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator

from .feed_validators import required


@required("name", "symbol")
class IntrinioFeedData(BaseModel):
    """
    IntrinioFeedData represents a single record from the Intrinio feed, normalising
    and transforming incoming fields to align with the RawEquity model.

    Args:
        name (str): The equity name, mapped from "security.name".
        symbol (str): The equity symbol, mapped from "security.ticker".
        share_class_figi (str | None): The share class FIGI identifier.
        currency (str | None): The trading currency.
        last_price (str | float | int | Decimal | None): Last traded price.
        fifty_two_week_min (str | float | int | Decimal | None): 52-week low price.
        fifty_two_week_max (str | float | int | Decimal | None): 52-week high price.
        market_volume (str | float | int | Decimal | None): Latest trading volume.
        dividend_yield (str | float | int | Decimal | None): Annual dividend yield.
        market_cap (str | float | int | Decimal | None): Market capitalisation.
        performance_1_year (str | float | int | Decimal | None): 1-year performance.

    Returns:
        IntrinioFeedData: Instance with fields normalised for RawEquity validation.
    """

    # Fields exactly match RawEquity's signature
    name: str
    symbol: str
    share_class_figi: str | None
    currency: str | None
    last_price: str | float | Decimal | None
    fifty_two_week_min: str | float | Decimal | None
    fifty_two_week_max: str | float | Decimal | None
    market_volume: str | float | Decimal | None
    dividend_yield: str | float | Decimal | None = None
    market_cap: str | float | Decimal | None = None
    performance_1_year: str | float | Decimal | None = None

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw Intrinio feed record into the flat schema expected by RawEquity.

        Extracts nested fields from the "security" object and maps top-level quote
        fields to the appropriate RawEquity attributes. Handles the calculation of
        1-year performance from the change_percent_365_days field.

        Args:
            self (dict[str, object]): Raw payload containing Intrinio feed data.

        Returns:
            dict[str, object]: A new dictionary with renamed and flattened keys
                suitable for the RawEquity schema.
        """
        security = self.get("security", {})

        return {
            # security.name → RawEquity.name
            "name": security.get("name"),
            # security.ticker → RawEquity.symbol
            "symbol": security.get("ticker"),
            # share_class_figi injected from discovery sources (do not override)
            "share_class_figi": None,
            # security.currency → RawEquity.currency
            "currency": security.get("currency"),
            # last → RawEquity.last_price
            "last_price": self.get("last"),
            # eod_fifty_two_week_low → RawEquity.fifty_two_week_min
            "fifty_two_week_min": self.get("eod_fifty_two_week_low"),
            # eod_fifty_two_week_high → RawEquity.fifty_two_week_max
            "fifty_two_week_max": self.get("eod_fifty_two_week_high"),
            # market_volume → RawEquity.market_volume
            "market_volume": self.get("market_volume"),
            # dividendyield → RawEquity.dividend_yield
            "dividend_yield": self.get("dividendyield"),
            # marketcap → RawEquity.market_cap
            "market_cap": self.get("marketcap"),
            # change_percent_365_days → RawEquity.performance_1_year
            # Convert from percentage (e.g., 14.6572) to decimal (0.146572)
            "performance_1_year": _percent_to_decimal(
                self.get("change_percent_365_days"),
            ),
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming Intrinio raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )


def _percent_to_decimal(percent: str | float | None) -> Decimal | None:
    """
    Convert a percentage value to decimal format.

    Converts percentage values (e.g., 14.6572 representing 14.6572%) to decimal
    format (0.146572) for consistency with RawEquity's performance_1_year field.

    Args:
        percent (str | float | None): The percentage value to convert.

    Returns:
        Decimal | None: The decimal value, or None if input is None or invalid.
    """
    if percent is None:
        return None

    try:
        return Decimal(str(percent)) / Decimal("100")
    except (ValueError, TypeError):
        return None
