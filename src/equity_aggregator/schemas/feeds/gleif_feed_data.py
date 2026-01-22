# feeds/gleif_feed_data.py

from pydantic import BaseModel, ConfigDict

from .feed_validators import required


@required("name", "symbol")
class GleifFeedData(BaseModel):
    """
    GleifFeedData represents a single record from the GLEIF ISIN->LEI mapping feed.

    This is a minimal enrichment feed that provides LEI (Legal Entity Identifier)
    data based on ISIN lookups. The name and symbol fields are passed through from
    the source equity to satisfy RawEquity requirements.

    Args:
        name (str): The equity name (passed through from source).
        symbol (str): The equity symbol (passed through from source).
        isin (str | None): The ISIN used for the LEI lookup.
        lei (str | None): The Legal Entity Identifier from GLEIF mapping.
    """

    # Fields exactly match RawEquity's signature
    name: str
    symbol: str
    isin: str | None = None
    lei: str | None = None

    model_config = ConfigDict(
        # ignore extra fields in incoming GLEIF raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )
