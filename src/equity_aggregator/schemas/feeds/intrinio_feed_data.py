# feeds/intrinio_feed_data.py

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator

from .feed_validators import required


@required("name", "symbol")
class IntrinioFeedData(BaseModel):
    """
    Represents a single Intrinio feed record, transforming and normalising
    incoming fields to match the RawEquity model's expected attributes.

    Combines company metadata from the /companies endpoint, security identifiers from
    the /companies/{ticker}/securities endpoint, and quote data from the
    /securities/{share_class_figi}/quote endpoint.

    Args:
        name (str): Company name.
        symbol (str): Equity symbol, mapped from security "ticker".
        cik (str | None): Central Index Key (10-digit zero-padded string from API).
        lei (str | None): Legal Entity Identifier from company data.
        share_class_figi (str | None): Share class FIGI identifier.
        mics (list[str] | None): Market Identifier Codes from exchange_mic.
        currency (str | None): The trading currency.
        last_price (str | float | Decimal | None): Last traded price.
        fifty_two_week_min (str | float | Decimal | None): 52-week low price.
        fifty_two_week_max (str | float | Decimal | None): 52-week high price.
        market_volume (str | float | Decimal | None): Latest trading volume.
        dividend_yield (str | float | Decimal | None): Annual dividend yield.
        market_cap (str | float | Decimal | None): Market capitalisation.
        performance_1_year (str | float | Decimal | None): 1-year performance.

    Returns:
        IntrinioFeedData: An instance with fields normalised for RawEquity
            validation.
    """

    # Fields match RawEquity's signature
    name: str
    symbol: str
    cik: str | None = None
    lei: str | None = None
    share_class_figi: str | None = None
    mics: list[str] | None = None
    currency: str | None = None
    last_price: str | float | Decimal | None = None
    fifty_two_week_min: str | float | Decimal | None = None
    fifty_two_week_max: str | float | Decimal | None = None
    market_volume: str | float | Decimal | None = None
    dividend_yield: str | float | Decimal | None = None
    market_cap: str | float | Decimal | None = None
    performance_1_year: str | float | Decimal | None = None

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw Intrinio feed record into the flat schema expected
        by RawEquity.

        Combines company data, security data, and quote data, mapping fields to
        RawEquity attributes.

        Args:
            self (dict[str, object]): Raw payload containing Intrinio feed
                data with quote data.

        Returns:
            dict[str, object]: A new dictionary with renamed keys suitable for the
                RawEquity schema.
        """
        # Extract quote data if present
        quote = self.get("quote", {}) or {}

        # Build MICs list from exchange_mic if present
        exchange_mic = self.get("exchange_mic")
        mics = [exchange_mic] if exchange_mic else None

        return {
            # name → RawEquity.name
            "name": self.get("name"),
            # cik → RawEquity.cik
            "cik": self.get("cik"),
            # lei → RawEquity.lei
            "lei": self.get("lei"),
            # ticker → RawEquity.symbol
            "symbol": self.get("ticker"),
            # share_class_figi → RawEquity.share_class_figi
            "share_class_figi": self.get("share_class_figi"),
            # exchange_mic → RawEquity.mics
            "mics": mics,
            # security.currency → RawEquity.currency
            "currency": self.get("currency"),
            # last → RawEquity.last_price
            "last_price": quote.get("last"),
            # eod_fifty_two_week_low → RawEquity.fifty_two_week_min
            "fifty_two_week_min": quote.get("eod_fifty_two_week_low"),
            # eod_fifty_two_week_high → RawEquity.fifty_two_week_max
            "fifty_two_week_max": quote.get("eod_fifty_two_week_high"),
            # market_volume → RawEquity.market_volume
            "market_volume": quote.get("market_volume"),
            # dividendyield → RawEquity.dividend_yield
            "dividend_yield": quote.get("dividendyield"),
            # marketcap → RawEquity.market_cap
            "market_cap": quote.get("marketcap"),
            # change_percent_365_days → RawEquity.performance_1_year
            # Convert from percentage (e.g., 14.6572) to decimal (0.146572)
            "performance_1_year": _percent_to_decimal(
                quote.get("change_percent_365_days"),
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
