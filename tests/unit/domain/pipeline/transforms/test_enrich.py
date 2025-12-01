# transforms/test_enrich.py

import asyncio
from collections.abc import AsyncIterable
from decimal import Decimal

import pytest

from equity_aggregator.domain.pipeline.transforms.enrich import (
    EnrichmentFeed,
    _enrich_equity,
    _enrich_from_feed,
    _has_missing_fields,
    _replace_none_fields,
    _safe_fetch,
    _to_usd,
    _validate,
    enrich,
)
from equity_aggregator.schemas.raw import RawEquity

pytestmark = pytest.mark.unit


class GoodFeedData:
    @staticmethod
    def model_validate(record: dict[str, object]) -> "GoodFeedData":
        class _Inner:
            def model_dump(self) -> dict[str, object]:
                return record

        return _Inner()


class BadFeedData:
    @staticmethod
    def model_validate(record: dict[str, object]) -> "BadFeedData":
        raise ValueError("invalid")


class PartialFeedData:
    @staticmethod
    def model_validate(record: dict[str, object]) -> "PartialFeedData":
        class _Inner:
            # drop market_cap so RawEquity.model_validate will fail
            def model_dump(self) -> dict[str, object]:
                d = record.copy()
                d.pop("market_cap", None)
                return d

        return _Inner()


class ErrorFeedData:
    @staticmethod
    def model_validate(record: dict[str, object]) -> "ErrorFeedData":
        class _ValidationError(Exception):
            def errors(self) -> list[dict[str, tuple[str]]]:
                # mimic both currency and market_cap are invalid
                return [{"loc": ("currency",)}, {"loc": ("market_cap",)}]

        raise _ValidationError("validation failed")


def test_has_missing_fields_true_when_any_field_none() -> None:
    """
    ARRANGE: a RawEquity with some None fields
    ACT:     call _has_missing_fields
    ASSERT:  returns True
    """
    incomplete = RawEquity(
        name="ABC",
        symbol="ABC",
        isin=None,  # missing
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("10"),
        market_cap=Decimal("1000"),
    )

    assert _has_missing_fields(incomplete) is True


def test_has_missing_fields_false_when_all_fields_present() -> None:
    """
    ARRANGE: a RawEquity with no None fields
    ACT:     call _has_missing_fields
    ASSERT:  returns False
    """
    complete = RawEquity(
        name="XYZ",
        symbol="XYZ",
        isin="ISIN00000001",
        cusip="037833100",
        cik="0000320193",
        share_class_figi="BBG000BLNNH6",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("5"),
        market_cap=Decimal("500"),
        fifty_two_week_min=Decimal("1"),
        fifty_two_week_max=Decimal("10"),
        dividend_yield=Decimal("0.02"),
        market_volume=Decimal("1000"),
        held_insiders=Decimal("0.10"),
        held_institutions=Decimal("0.60"),
        short_interest=Decimal("0.05"),
        share_float=Decimal("1000000"),
        shares_outstanding=Decimal("1200000"),
        revenue_per_share=Decimal("20"),
        profit_margin=Decimal("0.15"),
        gross_margin=Decimal("0.40"),
        operating_margin=Decimal("0.25"),
        free_cash_flow=Decimal("100000"),
        operating_cash_flow=Decimal("150000"),
        return_on_equity=Decimal("0.12"),
        return_on_assets=Decimal("0.08"),
        performance_1_year=Decimal("0.11"),
        total_debt=Decimal("200000"),
        revenue=Decimal("1000000"),
        ebitda=Decimal("300000"),
        trailing_pe=Decimal("15"),
        price_to_book=Decimal("2"),
        trailing_eps=Decimal("3.5"),
        analyst_rating="HOLD",
        industry="TECH",
        sector="TECHNOLOGY",
    )

    assert _has_missing_fields(complete) is False


def test_enrich_from_feed_falls_back_on_validation_failure() -> None:
    """
    ARRANGE: source with missing fields, fetcher returns data that fails validation
    ACT:     call _enrich_from_feed
    ASSERT:  returns the original source after validation fails
    """

    async def bad_data_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> dict[str, object]:
        # Return non-empty data that will fail validation
        return {"invalid": "data", "more": "fields"}

    source = RawEquity(
        name="BAD",
        symbol="BAD",
        isin="ISIN00000014",
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=None,
    )

    mock_feed = EnrichmentFeed(fetch=bad_data_fetcher, model=BadFeedData)

    actual = asyncio.run(_enrich_from_feed(source, mock_feed))

    assert actual is source


def test_enrich_equity_returns_source_when_no_missing_fields() -> None:
    """
    ARRANGE: complete RawEquity, feed that errors if called
    ACT:     call _enrich_equity
    ASSERT:  returns source unchanged, feed never invoked
    """

    async def should_not_be_called(**_: object) -> dict[str, object]:
        raise AssertionError("feed was called")

    complete = RawEquity(
        name="FULL",
        symbol="FULL",
        isin="US0378331005",
        cusip="037833100",
        cik="0000320193",
        share_class_figi="BBG000BLNNH6",
        mics=["XNAS"],
        currency="USD",
        last_price=Decimal("150"),
        market_cap=Decimal("250000000000"),
        fifty_two_week_min=Decimal("120"),
        fifty_two_week_max=Decimal("180"),
        dividend_yield=Decimal("0.006"),
        market_volume=Decimal("20000000"),
        held_insiders=Decimal("0.10"),
        held_institutions=Decimal("0.60"),
        short_interest=Decimal("0.01"),
        share_float=Decimal("16000000000"),
        shares_outstanding=Decimal("17000000000"),
        revenue_per_share=Decimal("20"),
        profit_margin=Decimal("0.22"),
        gross_margin=Decimal("0.43"),
        operating_margin=Decimal("0.30"),
        free_cash_flow=Decimal("95000000000"),
        operating_cash_flow=Decimal("110000000000"),
        return_on_equity=Decimal("0.28"),
        return_on_assets=Decimal("0.18"),
        performance_1_year=Decimal("0.12"),
        total_debt=Decimal("98000000000"),
        revenue=Decimal("1000000000"),
        ebitda=Decimal("120000000000"),
        trailing_pe=Decimal("28"),
        price_to_book=Decimal("35"),
        trailing_eps=Decimal("5.40"),
        analyst_rating="BUY",
        industry="TECH",
        sector="TECHNOLOGY",
    )

    mock_feed = EnrichmentFeed(fetch=should_not_be_called, model=object)

    actual = asyncio.run(_enrich_equity(complete, (mock_feed,)))

    assert actual is complete


def test_enrich_from_feed_short_circuits_when_equity_complete() -> None:
    """
    ARRANGE: RawEquity with all fields and a fetcher that errors if called
    ACT:     call _enrich_from_feed
    ASSERT:  the original object is returned and the fetcher is *not* executed
    """
    complete = RawEquity(
        name="FULL",
        symbol="FULL",
        isin="US0378331005",
        cusip="037833100",
        cik="0000320193",
        share_class_figi="BBG000BLNNH6",
        mics=["XNAS"],
        currency="USD",
        last_price=Decimal("150"),
        market_cap=Decimal("250000000000"),
        fifty_two_week_min=Decimal("120"),
        fifty_two_week_max=Decimal("180"),
        dividend_yield=Decimal("0.006"),
        market_volume=Decimal("20000000"),
        held_insiders=Decimal("0.10"),
        held_institutions=Decimal("0.60"),
        short_interest=Decimal("0.01"),
        share_float=Decimal("16000000000"),
        shares_outstanding=Decimal("17000000000"),
        revenue_per_share=Decimal("20"),
        profit_margin=Decimal("0.22"),
        gross_margin=Decimal("0.43"),
        operating_margin=Decimal("0.30"),
        free_cash_flow=Decimal("95000000000"),
        operating_cash_flow=Decimal("110000000000"),
        return_on_equity=Decimal("0.28"),
        return_on_assets=Decimal("0.18"),
        performance_1_year=Decimal("0.12"),
        total_debt=Decimal("98000000000"),
        revenue=Decimal("365000000000"),
        ebitda=Decimal("120000000000"),
        trailing_pe=Decimal("28"),
        price_to_book=Decimal("35"),
        trailing_eps=Decimal("5.40"),
        analyst_rating="BUY",
        industry="TECH",
        sector="TECHNOLOGY",
    )

    class MockFeed:
        async def fetch_equity(self, **_: dict[str, object]) -> dict[str, object]:
            raise AssertionError("fetcher was called")

        model = object

    mock_feed = EnrichmentFeed(fetch=MockFeed().fetch_equity, model=object)

    actual = asyncio.run(_enrich_from_feed(complete, mock_feed))

    assert actual is complete


def test_replace_none_fields_fills_only_none_fields() -> None:
    """
    ARRANGE: source with last_price None, enriched with both fields set
    ACT:     call _replace_none_fields
    ASSERT:  new object has last_price from enriched, but keeps source market_cap
    """
    source = RawEquity(
        name="SRC",
        symbol="SRC",
        isin="ISIN00000002",
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=Decimal("300"),
    )

    enriched = RawEquity(
        name="SRC",
        symbol="SRC",
        isin="ISIN00000002",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("25"),
        market_cap=Decimal("999"),
    )

    merged = _replace_none_fields(source, enriched)

    assert (merged.last_price, merged.market_cap) == (
        Decimal("25"),
        Decimal("300"),
    )


def test_enrich_passes_through_when_no_missing_fields() -> None:
    """
    ARRANGE: an async stream of fully-populated RawEquity objects
    ACT:     run enrich() over that stream
    ASSERT:  yields the same objects in order, unchanged
    """
    first_equity = RawEquity(
        name="ONE",
        symbol="ONE",
        isin="ISIN00000003",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("1"),
        market_cap=Decimal("100"),
    )

    second_equity = RawEquity(
        name="TWO",
        symbol="TWO",
        isin="ISIN00000004",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("2"),
        market_cap=Decimal("200"),
    )

    async def source() -> AsyncIterable[RawEquity]:
        yield first_equity
        yield second_equity

    async def runner() -> list[RawEquity]:
        return [equity async for equity in enrich(source())]

    actual = asyncio.run(runner())

    symbols = sorted(equity.symbol for equity in actual)

    assert symbols == ["ONE", "TWO"]


def test__enrich_from_feed_skips_when_no_missing() -> None:
    """
    ARRANGE: fully populated RawEquity, dummy fetcher that would error if called
    ACT:     call _enrich_from_feed
    ASSERT:  returns the same object without calling fetcher
    """

    async def should_not_be_called() -> dict[str, object]:
        raise AssertionError("Fetcher was called")

    full = RawEquity(
        name="FULL",
        symbol="FULL",
        isin="ISIN00000004",
        cusip="037833100",
        cik="0000320193",
        share_class_figi="BBG000BLNNH6",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("4"),
        market_cap=Decimal("40"),
    )

    mock_feed = EnrichmentFeed(fetch=should_not_be_called, model=object)

    actual = asyncio.run(_enrich_from_feed(full, mock_feed))

    assert actual is full


def test_safe_fetch_timeout_returns_none() -> None:
    """
    ARRANGE: a slow fetcher wrapped with timeout that raises TimeoutError
    ACT:     call _safe_fetch
    ASSERT:  returns None
    """

    async def slow_fetcher(**kwargs: object) -> dict[str, object]:
        await asyncio.sleep(0.05)
        return {"foo": "bar"}

    async def timeout_fetcher(**kwargs: object) -> dict[str, object]:
        # Simulate what _rate_limited does with timeout
        return await asyncio.wait_for(slow_fetcher(**kwargs), timeout=0.01)

    src = RawEquity(
        name="TST",
        symbol="TST",
        isin="ISIN00000004",
        cusip="037833100",
        share_class_figi="BBG000BLNNH6",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("2"),
        market_cap=Decimal("20"),
    )

    actual = asyncio.run(_safe_fetch(src, timeout_fetcher, "Slow"))

    assert actual is None


def test_safe_fetch_exception_returns_none() -> None:
    """
    ARRANGE: a fetcher that raises an exception
    ACT:     call _safe_fetch
    ASSERT:  returns None
    """

    async def bad_fetcher(**kwargs: object) -> dict[str, object]:
        raise RuntimeError("failure")

    source = RawEquity(
        name="TST",
        symbol="TST",
        isin="ISIN00000004",
        cusip="037833100",
        share_class_figi="BBG000BLNNH6",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("2"),
        market_cap=Decimal("20"),
    )

    actual = asyncio.run(_safe_fetch(source, bad_fetcher, "Bad"))

    assert actual is None


def test_safe_fetch_success_returns_dict() -> None:
    """
    ARRANGE: a fetcher that returns quickly
    ACT:     call _safe_fetch
    ASSERT:  returns the dict unchanged
    """

    async def quick_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> dict[str, object]:
        _ = (symbol, name, isin, cusip)
        return {"foo": "bar"}

    source = RawEquity(
        name="A",
        symbol="A",
        isin="ISIN00000004",
        cusip="037833100",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("1"),
        market_cap=Decimal("1"),
    )

    actual = asyncio.run(_safe_fetch(source, quick_fetcher, "Quick"))

    assert actual == {"foo": "bar"}


def test_enrich_empty_stream_yields_nothing() -> None:
    """
    ARRANGE: an async stream that never yields
    ACT:     run enrich()
    ASSERT:  yields empty list
    """

    async def empty_src() -> AsyncIterable[RawEquity]:
        if False:
            yield

    async def runner() -> list[RawEquity]:
        return await asyncio.gather(*[equity async for equity in enrich(empty_src())])

    actual = asyncio.run(runner())
    assert actual == []


def test_has_missing_fields_counts_optional_fields() -> None:
    """
    ARRANGE: a RawEquity missing an optional field (cusip)
    ACT:     call _has_missing_fields
    ASSERT:  returns True
    """
    # cusip and share_class_figi default to None if not provided
    incomplete = RawEquity(
        name="OPT",
        symbol="OPT",
        isin="ISIN00000004",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("1"),
        market_cap=Decimal("10"),
    )

    assert _has_missing_fields(incomplete) is True


def test_replace_none_fields_leaves_none_when_enriched_also_none() -> None:
    """
    ARRANGE: source has two None fields, enriched also None for those
    ACT:     call _replace_none_fields
    ASSERT:  both fields remain None
    """
    source = RawEquity(
        name="SRC2",
        symbol="SRC2",
        isin=None,
        cusip=None,
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=None,
    )

    enriched = RawEquity(
        name="SRC2",
        symbol="SRC2",
        isin=None,
        cusip=None,
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=None,
    )

    merged = _replace_none_fields(source, enriched)

    assert (
        merged.isin,
        merged.cusip,
        merged.last_price,
        merged.market_cap,
    ) == (
        None,
        None,
        None,
        None,
    )


def test_validate_returns_raw_equity() -> None:
    """
    ARRANGE: a record and GoodFeedData model
    ACT:     call _validate
    ASSERT:  returns a RawEquity
    """
    raw_record = {
        "name": "VAL",
        "symbol": "VAL",
        "isin": "ISIN00000004",
        "mics": ["XLON"],
        "currency": "USD",
        "last_price": Decimal("3"),
        "market_cap": Decimal("30"),
    }
    source = RawEquity.model_validate(raw_record)

    actual = _validate(raw_record, source, GoodFeedData, "GoodFeed")

    assert isinstance(actual, RawEquity)


def test_validate_returns_source_on_error() -> None:
    """
    ARRANGE: a record and BadFeedData model
    ACT:     call _validate
    ASSERT:  returns source
    """
    raw_record = {
        "name": "VAL",
        "symbol": "VAL",
        "isin": "ISIN00000004",
        "mics": ["XLON"],
        "currency": "USD",
        "last_price": Decimal("3"),
        "market_cap": Decimal("30"),
    }

    source = RawEquity.model_validate(raw_record)

    actual = _validate(raw_record, source, BadFeedData, "BadFeed")

    assert actual is source


def test_enrich_from_feed_falls_back_on_empty_dict() -> None:
    """
    ARRANGE: a RawEquity instance
    ACT:     call _enrich_from_feed with an empty fetcher
    ASSERT:  returns the original RawEquity unchanged
    """

    async def empty_fetcher() -> dict[str, object]:
        return {}

    source = RawEquity(
        name="E",
        symbol="E",
        isin=None,
        cusip=None,
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=None,
    )

    mock_feed = EnrichmentFeed(fetch=empty_fetcher, model=GoodFeedData)

    actual = asyncio.run(_enrich_from_feed(source, mock_feed))

    assert actual is source


def test_safe_fetch_times_out_and_returns_none() -> None:
    """
    ARRANGE: slow fetcher wrapped with timeout that raises TimeoutError
    ACT:     call _safe_fetch
    ASSERT:  returns None (TimeoutError branch)
    """

    async def slow_fetcher(
        **kwargs: dict[str, object],
    ) -> dict[str, object]:
        await asyncio.sleep(0.05)
        return {"ignored": True}

    async def timeout_fetcher(**kwargs: object) -> dict[str, object]:
        # Simulate what _rate_limited does with timeout
        return await asyncio.wait_for(slow_fetcher(**kwargs), timeout=0.01)

    src = RawEquity(
        name="TO",
        symbol="TO",
        isin="ISIN00000005",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("0"),
        market_cap=Decimal("0"),
    )

    actual = asyncio.run(_safe_fetch(src, timeout_fetcher, "Slow"))

    assert actual is None


def test_enrich_from_feed_completes_success_path() -> None:
    """
    ARRANGE:  source missing financials; fetcher returns a full record
    ACT:      call _enrich_from_feed
    ASSERT:   enriched RawEquity contains the fetched last_price & market_cap
    """

    async def good_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> dict[str, object]:
        _ = (symbol, name, isin, cusip)
        return {
            "name": name,
            "symbol": symbol,
            "isin": isin,
            "cusip": cusip,
            "mics": ["XLON"],
            "currency": "USD",  # already USD ⇒ converter is no-op
            "last_price": Decimal("123"),
            "market_cap": Decimal("4567"),
        }

    source = RawEquity(
        name="OK",
        symbol="OK",
        isin="ISIN00000006",
        cusip="037833100",
        mics=["XLON"],
        currency="USD",
        last_price=None,
        market_cap=None,
    )

    mock_feed = EnrichmentFeed(fetch=good_fetcher, model=GoodFeedData)

    enriched = asyncio.run(_enrich_from_feed(source, mock_feed))

    assert (enriched.last_price, enriched.market_cap) == (
        Decimal("123"),
        Decimal("4567"),
    )


def test_safe_fetch_lookup_error_returns_none() -> None:
    """
    ARRANGE: a fetcher that raises LookupError
    ACT:     call _safe_fetch
    ASSERT:  returns None  (the call routes through _log_outcome)
    """

    async def not_found_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> dict[str, object]:
        raise LookupError("no data")

    src = RawEquity(
        name="NF",
        symbol="NF",
        isin="ISIN00000007",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("0"),
        market_cap=Decimal("0"),
    )

    actual = asyncio.run(
        _safe_fetch(src, not_found_fetcher, "NotFoundFeed"),
    )

    assert actual is None


def test_validate_handles_error_feed() -> None:
    """
    ARRANGE: ErrorFeedData model that raises with .errors()
    ACT:     call _validate to trigger the exception
    ASSERT:  returns the original RawEquity (handles hasattr(error, "errors"))
    """
    raw_record = {
        "name": "X",
        "symbol": "X",
        "isin": "ISIN00000008",
        "mics": ["XLON"],
        "currency": "USD",
        "last_price": Decimal("9"),
        "market_cap": Decimal("90"),
    }
    source = RawEquity.model_validate(raw_record)

    actual = _validate(raw_record, source, ErrorFeedData, "ErrorFeed")

    assert actual is source


def test_to_usd_handles_converter_returning_none() -> None:
    """
    ARRANGE: a validated equity whose `model_copy` is overridden to return None,
             making the USD-converter return None.
    ACT:     call _to_usd
    ASSERT:  falls back to the original source object
    """

    class _NoCopyRawEquity(RawEquity):
        def model_copy(
            self,
            *,
            update: dict[str, object] | None = None,
            deep: bool = False,
        ) -> None:
            return None

    validated = _NoCopyRawEquity(
        name="NONE",
        symbol="NONE",
        isin="ISIN00000010",
        mics=["XLON"],
        currency="EUR",
        last_price=Decimal("10"),
        market_cap=None,
    )

    source = RawEquity(
        name="NONE",
        symbol="NONE",
        isin="ISIN00000010",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("1"),
        market_cap=Decimal("10"),
    )

    actual = asyncio.run(_to_usd(validated, source, "FxFeed"))

    assert actual is source


def test_to_usd_logs_success_on_enrichment() -> None:
    """
    ARRANGE: validated equity differs from source (enrichment occurred) with EUR currency
    ACT:     call _to_usd
    ASSERT:  returns the converted equity in USD and logs success
    """
    source = RawEquity(
        name="SRC",
        symbol="SRC",
        isin="ISIN00000011",
        mics=["XLON"],
        currency="EUR",
        last_price=Decimal("10"),
        market_cap=None,
    )

    validated = RawEquity(
        name="SRC",
        symbol="SRC",
        isin="ISIN00000011",
        mics=["XLON"],
        currency="EUR",
        last_price=Decimal("10"),
        market_cap=Decimal("1000"),
    )

    actual = asyncio.run(_to_usd(validated, source, "TestFeed"))

    assert actual.currency == "USD"


def test_to_usd_converts_currency() -> None:
    """
    ARRANGE: source equity in EUR
    ACT:     call _to_usd
    ASSERT:  returns converted equity in USD
    """
    source = RawEquity(
        name="SAME",
        symbol="SAME",
        isin="ISIN00000012",
        mics=["XLON"],
        currency="EUR",
        last_price=Decimal("100"),
        market_cap=Decimal("5000"),
    )

    actual = asyncio.run(_to_usd(source, source, "TestFeed"))

    assert actual.currency == "USD"
