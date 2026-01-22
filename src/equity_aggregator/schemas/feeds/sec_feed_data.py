# feeds/sec_feed_data.py

from pydantic import BaseModel, ConfigDict, model_validator

from .feed_validators import required


@required("name", "symbol")
class SecFeedData(BaseModel):
    """
    Represents a single SEC feed record, transforming and normalising incoming
    fields to match the RawEquity model's expected attributes.

    Args:
        name (str): Company name, mapped from "name".
        symbol (str): Equity symbol, mapped from "symbol".
        cik (str): Central Index Key, converted from int to 10-digit zero-padded string.
        mics (list[str]): List of MIC codes; defaults to an empty list if missing.

    Returns:
        SecFeedData: An instance with fields normalised for RawEquity validation.
    """

    # Fields exactly match RawEquity's signature
    cik: str
    name: str
    symbol: str
    mics: list[str]

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw SEC feed record into the flat schema expected by RawEquity.

        Args:
            self (dict[str, object]): Raw payload containing SEC feed data.

        Returns:
            dict[str, object]: A new dictionary with renamed keys suitable for the
                RawEquity schema.
        """
        # convert int CIK to string
        raw = convert_cik_to_str(self)

        return {
            "cik": raw.get("cik"),
            "name": raw.get("name"),
            "symbol": raw.get("symbol"),
            # no CUSIP, ISIN or FIGI in SEC feed, so omitting from model
            "mics": raw.get("mics"),
            # no currency or last_price in SEC feed, so omitting from model
            # no more additional fields in SEC feed, so omitting from model
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming SEC raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )


def convert_cik_to_str(raw: dict) -> dict:
    """
    Normalise SEC CIK integer value to ensure compatibility with RawEquity schema.

    The SEC API returns CIK values as integers, but the RawEquity schema expects
    10-digit zero-padded string values for all CIK fields. This function converts
    integer CIK values to properly formatted strings while preserving all other
    fields unchanged.

    Args:
        raw (dict): A dictionary containing raw SEC feed data with potentially
            integer CIK values.

    Returns:
        dict: A new dictionary with CIK converted to 10-digit zero-padded string
            if present and not None. All other fields remain unchanged.
    """
    # Convert integer CIK to 10-digit zero-padded string
    cik_value = raw.get("cik")
    updates = {"cik": str(cik_value).zfill(10)}

    # Return new dict rather than mutating in place
    return {**raw, **updates}
