# feeds/lseg_feed_data.py

import re
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator

from .feed_validators import required


@required("name", "symbol")
class LsegFeedData(BaseModel):
    """
    Represents single LSEG feed record, transforming and normalising incoming
    fields to match the RawEquity model's expected attributes. If the currency is "GBX",
    price fields such as "last_price" are automatically converted from pence to
    pounds (GBP) for consistency.

    Args:
        name (str): The issuer's full name, mapped from "issuername".
        symbol (str): The tradable instrument symbol, mapped from "tidm".
        isin (str | None): The ISIN identifier, if available.
        currency (str | None): The trading currency code, with "GBX" converted to
            "GBP" if applicable.
        last_price (str | float | int | Decimal | None): Last traded price, mapped
            from "lastprice" and converted from pence to pounds if currency is "GBX".
        market_cap (str | float | int | Decimal | None): Market capitalisation,
            mapped from "marketcapitalization".
        fifty_two_week_min (str | float | int | Decimal | None): 52-week low,
            mapped from "fiftyTwoWeeksMin" (converted from pence to GBP if needed).
        fifty_two_week_max (str | float | int | Decimal | None): 52-week high,
            mapped from "fiftyTwoWeeksMax" (converted from pence to GBP if needed).

    Returns:
        LsegFeedData: An instance with fields normalised for RawEquity validation,
            including automatic GBX to GBP conversion where relevant.
    """

    # Fields exactly match RawEquity's signature
    name: str
    symbol: str
    isin: str | None
    currency: str | None
    last_price: str | float | int | Decimal | None
    market_cap: str | float | int | Decimal | None
    fifty_two_week_min: str | float | int | Decimal | None
    fifty_two_week_max: str | float | int | Decimal | None

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise raw LSEG feed record into the flat schema expected by RawEquity.

        Extracts and renames nested fields to match the RawEquity signature. If the
        currency is "GBX", automatically converts price fields from pence to pounds
        (GBP) using the convert_gbx_to_gbp helper. Treats 0 as None for monetary
        fields since LSEG API uses 0 to represent missing data for certain fields.

        Args:
            self (dict[str, object]): Raw payload containing raw LSEG feed data.

        Returns:
            dict[str, object]: A new dictionary with renamed keys and, if applicable,
            price and currency fields converted from GBX to GBP, suitable for the
            RawEquity schema.
        """

        # convert GBX to GBP and sanitise zero monetary values
        raw = _convert_gbx_to_gbp(self)
        raw = _sanitise_zero_monetary_values(raw)

        return {
            "name": raw.get("issuername"),
            "symbol": raw.get("tidm"),
            "isin": raw.get("isin"),
            # no CUSIP, CIK or FIGI in LSEG feed, so omitting from model
            "currency": raw.get("currency"),
            "last_price": raw.get("lastprice"),
            "market_cap": raw.get("marketcapitalization"),
            "fifty_two_week_min": raw.get("fiftyTwoWeeksMin"),
            "fifty_two_week_max": raw.get("fiftyTwoWeeksMax"),
            # no additional fields in LSEG feed, so omitting from model
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming LSEG raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )


def _convert_gbx_to_gbp(raw: dict) -> dict:
    """
    Convert price and currency fields from GBX (pence) to GBP (pounds).

    If the input dictionary has a "currency" field set to "GBX", this function
    divides all price fields (lastprice, fiftyTwoWeeksMin, fiftyTwoWeeksMax)
    by 100 to convert from pence to pounds, sets the "currency" field to "GBP",
    and returns a new dictionary with these updates. All other fields remain
    unchanged. If the currency is not "GBX", the original dictionary is returned
    unmodified.

    Args:
        raw (dict): A dictionary containing at least a "currency" field, and
            optionally price fields representing values in pence.

    Returns:
        dict: A new dictionary with price fields converted to pounds and
        "currency" set to "GBP" if original currency was "GBX". Otherwise,
        returns original dict.
    """
    if raw.get("currency") != "GBX":
        return raw

    updates = {"currency": "GBP"}

    # Convert lastprice
    lastprice = _gbx_to_decimal(raw.get("lastprice"))
    updates["lastprice"] = lastprice / Decimal("100") if lastprice else None

    # Convert fiftyTwoWeeksMin
    min_price = _gbx_to_decimal(raw.get("fiftyTwoWeeksMin"))
    updates["fiftyTwoWeeksMin"] = min_price / Decimal("100") if min_price else None

    # Convert fiftyTwoWeeksMax
    max_price = _gbx_to_decimal(raw.get("fiftyTwoWeeksMax"))
    updates["fiftyTwoWeeksMax"] = max_price / Decimal("100") if max_price else None

    # return a new dict rather than mutating in place
    return {**raw, **updates}


def _gbx_to_decimal(pence: str | None) -> Decimal | None:
    """
    Convert a pence string (e.g., "150", "1,50") to a Decimal value.

    Accepts strings representing pence values, optionally using a comma as a decimal
    separator (e.g., "1,23" is treated as "1.23"). Returns None if the input is None or
    does not match a positive number format.

    Args:
        pence (str | None): The pence value as a string, possibly with a comma decimal
            separator, or None.

    Returns:
        Decimal | None: The parsed Decimal value, or None if input is invalid.
    """
    if pence is None:
        return None

    s = str(pence).strip()
    # allow "1,23" → "1.23"
    if "," in s and "." not in s:
        s = s.replace(",", ".")

    # only digits with optional single decimal point
    if not re.fullmatch(r"\d+(?:\.\d+)?", s):
        return None

    return Decimal(s)


def _sanitise_zero_monetary_values(raw: dict) -> dict:
    """
    Treat 0 as None for LSEG monetary fields.

    LSEG API returns 0 for missing monetary data. Since 0 is not a valid
    price or market cap, we treat it as None to allow enrichment feeds
    to provide valid data downstream.

    Args:
        raw: Dictionary containing LSEG API fields.

    Returns:
        A new dictionary with 0 values converted to None for monetary fields.
    """
    monetary_fields = [
        "lastprice",
        "marketcapitalization",
        "fiftyTwoWeeksMin",
        "fiftyTwoWeeksMax",
    ]

    updates = {
        field: None if raw.get(field) == 0 else raw.get(field)
        for field in monetary_fields
    }

    return {**raw, **updates}
