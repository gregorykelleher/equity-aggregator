# feeds/yfinance_feed_data.py

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator

from ._utils import (
    is_trade_stale,
    nullify_price_fields,
    parse_unix_timestamp,
    percent_to_decimal,
    required,
)


@required("name", "symbol")
class YFinanceFeedData(BaseModel):
    """
    YFinanceFeedData represents a single record from the YFinance feed, normalising
    and transforming incoming fields to align with the RawEquity model.

    Args:
        name (str): The equity name.
        symbol (str): The equity symbol.
        currency (str | None): The trading currency.
        ...: Additional fields are mapped and normalised from the YFinance feed.

    Returns:
        YFinanceFeedData: Instance with fields normalised for RawEquity validation.
    """

    # Fields exactly match RawEquity's signature
    name: str
    symbol: str
    currency: str | None
    last_price: str | float | int | Decimal | None
    market_cap: str | float | int | Decimal | None
    fifty_two_week_min: str | float | int | Decimal | None
    fifty_two_week_max: str | float | int | Decimal | None
    dividend_yield: str | float | int | Decimal | None = None
    market_volume: str | float | int | Decimal | None = None
    held_insiders: str | float | int | Decimal | None = None
    held_institutions: str | float | int | Decimal | None = None
    short_interest: str | float | int | Decimal | None = None
    share_float: str | float | int | Decimal | None = None
    shares_outstanding: str | float | int | Decimal | None = None
    revenue_per_share: str | float | int | Decimal | None = None
    profit_margin: str | float | int | Decimal | None = None
    gross_margin: str | float | int | Decimal | None = None
    operating_margin: str | float | int | Decimal | None = None
    free_cash_flow: str | float | int | Decimal | None = None
    operating_cash_flow: str | float | int | Decimal | None = None
    return_on_equity: str | float | int | Decimal | None = None
    return_on_assets: str | float | int | Decimal | None = None
    performance_1_year: str | float | int | Decimal | None = None
    total_debt: str | float | int | Decimal | None = None
    revenue: str | float | int | Decimal | None = None
    ebitda: str | float | int | Decimal | None = None
    trailing_pe: str | float | int | Decimal | None = None
    price_to_book: str | float | int | Decimal | None = None
    trailing_eps: str | float | int | Decimal | None = None
    analyst_rating: str | None = None
    industry: str | None = None
    sector: str | None = None

    @model_validator(mode="before")
    def _normalise_fields(self: dict[str, object]) -> dict[str, object]:
        """
        Normalise a raw YFinance feed record into the flat schema expected by RawEquity.

        This validator supports both Yahoo Finance endpoints:
        - quote_summary_primary_url (i.e. '/v10/finance/quoteSummary/')
        - quote_summary_fallback_url (i.e. '/v7/finance/quote')

        Note:
            The fallback endpoint lacks many financial metrics and uses
            different scales for some fields. The primary endpoint returns
            dividendYield and 52WeekChange as decimal ratios (e.g. 0.0041),
            while the fallback returns dividendYield and
            fiftyTwoWeekChangePercent as percentages (e.g. 18.62). We detect
            the source by checking for the primary-only key ``52WeekChange``
            and apply percent_to_decimal where needed.

        Args:
            self (dict[str, object]): Raw payload containing YFinance feed data.

        Returns:
            dict[str, object]: A new dictionary with renamed keys suitable for the
                RawEquity schema.
        """
        fields = {
            # longName/shortName -> RawEquity.name
            "name": self.get("longName") or self.get("shortName"),
            # underlyingSymbol or symbol -> RawEquity.symbol
            "symbol": self.get("underlyingSymbol") or self.get("symbol"),
            # no ISIN, CUSIP, CIK, FIGI or MICS in YFinance feed, so omitting from model
            "currency": self.get("currency"),
            # currentPrice or regularMarketPrice
            # -> RawEquity.last_price
            "last_price": self.get("currentPrice") or self.get("regularMarketPrice"),
            # marketCap -> RawEquity.market_cap
            "market_cap": self.get("marketCap"),
            # fiftyTwoWeekLow -> RawEquity.fifty_two_week_min
            "fifty_two_week_min": self.get("fiftyTwoWeekLow"),
            # fiftyTwoWeekHigh -> RawEquity.fifty_two_week_max
            "fifty_two_week_max": self.get("fiftyTwoWeekHigh"),
            # dividendYield -> RawEquity.dividend_yield
            # The primary endpoint returns this as a decimal ratio
            # (e.g. 0.0041) but the fallback returns a percentage
            # (e.g. 18.62). We detect the source via the primary-only
            # key "52WeekChange" and convert accordingly.
            "dividend_yield": (
                self.get("dividendYield")
                if _is_primary(self)
                else percent_to_decimal(self.get("dividendYield"))
            ),
            # volume or regularMarketVolume -> RawEquity.market_volume
            "market_volume": self.get("volume") or self.get("regularMarketVolume"),
            # heldPercentInsiders -> RawEquity.held_insiders
            "held_insiders": self.get("heldPercentInsiders"),
            # heldPercentInstitutions -> RawEquity.held_institutions
            "held_institutions": self.get("heldPercentInstitutions"),
            # shortPercentOfFloat -> RawEquity.short_interest
            "short_interest": self.get("shortPercentOfFloat"),
            # floatShares -> RawEquity.share_float
            "share_float": self.get("floatShares"),
            # sharesOutstanding -> RawEquity.shares_outstanding
            "shares_outstanding": self.get("sharesOutstanding"),
            # revenuePerShare -> RawEquity.revenue_per_share
            "revenue_per_share": self.get("revenuePerShare"),
            # profitMargins -> RawEquity.profit_margin
            "profit_margin": self.get("profitMargins"),
            # grossMargins -> RawEquity.gross_margin
            "gross_margin": self.get("grossMargins"),
            # operatingMargins -> RawEquity.operating_margin
            "operating_margin": self.get("operatingMargins"),
            # freeCashflow -> RawEquity.free_cash_flow
            "free_cash_flow": self.get("freeCashflow"),
            # operatingCashflow -> RawEquity.operating_cash_flow
            "operating_cash_flow": self.get("operatingCashflow"),
            # returnOnEquity -> RawEquity.return_on_equity
            "return_on_equity": self.get("returnOnEquity"),
            # returnOnAssets -> RawEquity.return_on_assets
            "return_on_assets": self.get("returnOnAssets"),
            # 52WeekChange -> RawEquity.performance_1_year (primary, ratio)
            # fiftyTwoWeekChangePercent -> same (fallback, percentage)
            "performance_1_year": (
                self.get("52WeekChange")
                if _is_primary(self)
                else percent_to_decimal(
                    self.get("fiftyTwoWeekChangePercent")
                )
            ),
            # totalDebt -> RawEquity.total_debt
            "total_debt": self.get("totalDebt"),
            # totalRevenue -> RawEquity.revenue
            "revenue": self.get("totalRevenue"),
            # ebitda -> RawEquity.ebitda
            "ebitda": self.get("ebitda"),
            # trailingPE -> RawEquity.trailing_pe
            "trailing_pe": self.get("trailingPE"),
            # priceToBook -> RawEquity.price_to_book
            "price_to_book": self.get("priceToBook"),
            # trailingEps or epsTrailingTwelveMonths -> RawEquity.trailing_eps
            "trailing_eps": self.get("trailingEps")
            or self.get("epsTrailingTwelveMonths"),
            # recommendationKey or averageAnalystRating -> RawEquity.analyst_rating
            "analyst_rating": self.get("recommendationKey")
            or self.get("averageAnalystRating"),
            # industry -> RawEquity.industry
            "industry": self.get("industry"),
            # sector -> RawEquity.sector
            "sector": self.get("sector"),
        }

        # Null out price-sensitive fields when the last trade timestamp
        # is too old, preventing stale quotes from being passed through.
        last_time = parse_unix_timestamp(self.get("regularMarketTime"))
        if is_trade_stale(last_time):
            fields = nullify_price_fields(fields)

        return fields

    model_config = ConfigDict(
        # ignore extra fields in incoming YFinance raw data feed
        extra="ignore",
        # defer strict type validation to RawEquity
        strict=False,
    )


def _is_primary(payload: dict[str, object]) -> bool:
    """
    Detect whether a raw payload came from the primary quoteSummary endpoint.

    The primary endpoint (v10/quoteSummary) includes the ``52WeekChange`` key
    from its ``defaultKeyStatistics`` module. The fallback endpoint (v7/quote)
    never includes this key, using ``fiftyTwoWeekChangePercent`` instead.

    Returns:
        bool: True when the payload originates from the primary endpoint.
    """
    return "52WeekChange" in payload
