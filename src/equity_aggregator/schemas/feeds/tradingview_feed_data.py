# feeds/tradingview_feed_data.py

from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, model_validator

from .feed_validators import required


@required("name", "symbol")
class TradingViewFeedData(BaseModel):
    """
    TradingViewFeedData represents a single record from the TradingView feed,
    normalising and transforming incoming fields to align with the RawEquity model.

    Args:
        name (str): The equity name.
        symbol (str): The equity symbol.
        currency (str | None): The trading currency.
        ...: Additional fields are mapped and normalised from the TradingView feed.

    Returns:
        TradingViewFeedData: Instance with fields normalised for RawEquity validation.
    """

    # Fields exactly match RawEquity's signature
    name: str
    symbol: str
    currency: str | None
    last_price: Decimal | None
    market_cap: Decimal | None
    market_volume: Decimal | None
    dividend_yield: Decimal | None
    shares_outstanding: Decimal | None
    revenue: Decimal | None
    ebitda: Decimal | None
    trailing_pe: Decimal | None
    price_to_book: Decimal | None
    trailing_eps: Decimal | None
    return_on_equity: Decimal | None
    return_on_assets: Decimal | None
    sector: str | None
    industry: str | None

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw TradingView feed record into the flat schema expected
        by RawEquity.

        TradingView provides data in an array format where field 'd' contains
        19 elements, each at a specific index position corresponding to a
        particular metric.

        Args:
            self (dict[str, object]): Raw payload containing TradingView feed data.

        Returns:
            dict[str, object]: A new dictionary with renamed keys suitable for the
                RawEquity schema.
        """
        # Extract the data array
        d = self.get("d", [])

        return {
            # d[0] → RawEquity.symbol (ticker)
            "symbol": _extract_field(d, 0),
            # d[1] → RawEquity.name (company name)
            "name": _extract_field(d, 1),
            # no ISIN, CUSIP, CIK, FIGI or MICS in TradingView feed,
            # so omitting from model
            # d[3] → RawEquity.currency
            "currency": _extract_field(d, 3),
            # d[4] → RawEquity.last_price (close price)
            "last_price": _extract_field(d, 4),
            # d[5] → RawEquity.market_cap
            "market_cap": _extract_field(d, 5),
            # d[6] → RawEquity.market_volume
            "market_volume": _extract_field(d, 6),
            # d[7] → RawEquity.dividend_yield (already in decimal format)
            "dividend_yield": _extract_field(d, 7),
            # d[9] → RawEquity.shares_outstanding
            "shares_outstanding": _extract_field(d, 9),
            # d[10] → RawEquity.revenue
            "revenue": _extract_field(d, 10),
            # d[11] → RawEquity.ebitda
            "ebitda": _extract_field(d, 11),
            # d[12] → RawEquity.trailing_pe
            "trailing_pe": _extract_field(d, 12),
            # d[13] → RawEquity.price_to_book
            "price_to_book": _extract_field(d, 13),
            # d[14] → RawEquity.trailing_eps
            "trailing_eps": _extract_field(d, 14),
            # d[15] → RawEquity.return_on_equity (convert from percentage to decimal)
            "return_on_equity": _convert_percentage_to_decimal(_extract_field(d, 15)),
            # d[16] → RawEquity.return_on_assets (convert from percentage to decimal)
            "return_on_assets": _convert_percentage_to_decimal(_extract_field(d, 16)),
            # d[17] → RawEquity.sector
            "sector": _extract_field(d, 17),
            # d[18] → RawEquity.industry
            "industry": _extract_field(d, 18),
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming TradingView raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )


def _extract_field(data_array: list | None, index: int) -> object | None:
    """
    Safely extract a field from a data array at the given index.

    Args:
        data_array (list | None): The array containing field data.
        index (int): The index position to extract.

    Returns:
        object | None: The field value at the index, or None if unavailable.
    """
    if not data_array or len(data_array) <= index:
        return None
    return data_array[index]


def _convert_percentage_to_decimal(value: float | None) -> Decimal | None:
    """
    Convert a percentage value to decimal representation.

    Args:
        value (float | None): The percentage value (e.g., 20.6 for 20.6%).

    Returns:
        Decimal | None: The decimal representation (e.g., 0.206), or None if
            input is None.
    """
    if value is None:
        return None
    try:
        return Decimal(str(value)) / Decimal("100")
    except (ValueError, TypeError, InvalidOperation):
        return None
