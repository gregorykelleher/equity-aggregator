# feeds/stock_analysis_feed_data.py

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator

from .feed_validators import required


@required("name", "symbol")
class StockAnalysisFeedData(BaseModel):
    """
    Represents a single Stock Analysis feed record, transforming and normalising
    incoming fields to match the RawEquity model's expected attributes.

    Args:
        name (str): Company name, mapped from "n".
        symbol (str): Equity symbol, mapped from "s".
        cusip (str | None): CUSIP identifier, passed through unchanged.
        isin (str | None): ISIN identifier, passed through unchanged.
        market_cap (Decimal | None): Market capitalisation, mapped from "marketCap".
        last_price (Decimal | None): Last known price, mapped from "price".
        market_volume (Decimal | None): Trading volume, mapped from "volume".
        trailing_pe (Decimal | None): Price-to-earnings ratio, mapped from "peRatio".
        sector (str | None): Sector classification, mapped from "sector".
        industry (str | None): Industry classification, mapped from "industry".
        revenue (Decimal | None): Total revenue, mapped from "revenue".
        free_cash_flow (Decimal | None): Free cash flow, mapped from "fcf".
        return_on_equity (Decimal | None): Return on equity, mapped from "roe".
        return_on_assets (Decimal | None): Return on assets, mapped from "roa".
        ebitda (Decimal | None): EBITDA, mapped from "ebitda".

    Returns:
        StockAnalysisFeedData: An instance with fields normalised for RawEquity
            validation.
    """

    # Fields exactly match RawEquity's signature
    name: str
    symbol: str
    cusip: str | None
    isin: str | None
    market_cap: Decimal | None
    last_price: Decimal | None
    market_volume: Decimal | None
    trailing_pe: Decimal | None
    sector: str | None
    industry: str | None
    revenue: Decimal | None
    free_cash_flow: Decimal | None
    return_on_equity: Decimal | None
    return_on_assets: Decimal | None
    ebitda: Decimal | None

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw Stock Analysis feed record into the flat schema expected
        by RawEquity.

        Args:
            self (dict[str, object]): Raw payload containing Stock Analysis feed data.

        Returns:
            dict[str, object]: A new dictionary with renamed keys suitable for the
                RawEquity schema.
        """
        return {
            # s → RawEquity.symbol
            "symbol": self.get("s"),
            # n → RawEquity.name
            "name": self.get("n"),
            # cusip → RawEquity.cusip
            "cusip": self.get("cusip"),
            # isin → RawEquity.isin
            "isin": self.get("isin"),
            # no CIK, FIGI, MICS or currency in Stock Analysis feed, so omitting
            # marketCap → RawEquity.market_cap
            "market_cap": self.get("marketCap"),
            # price → RawEquity.last_price
            "last_price": self.get("price"),
            # volume → RawEquity.market_volume
            "market_volume": self.get("volume"),
            # peRatio → RawEquity.trailing_pe
            "trailing_pe": self.get("peRatio"),
            # sector → RawEquity.sector
            "sector": self.get("sector"),
            # industry → RawEquity.industry
            "industry": self.get("industry"),
            # revenue → RawEquity.revenue
            "revenue": self.get("revenue"),
            # fcf → RawEquity.free_cash_flow
            "free_cash_flow": self.get("fcf"),
            # roe → RawEquity.return_on_equity
            "return_on_equity": self.get("roe"),
            # roa → RawEquity.return_on_assets
            "return_on_assets": self.get("roa"),
            # ebitda → RawEquity.ebitda
            "ebitda": self.get("ebitda"),
        }

    model_config = ConfigDict(
        # ignore extra fields in incoming Stock Analysis raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )
