# transforms/test_enrich.py

import asyncio
from collections.abc import AsyncIterable
from decimal import Decimal

import pytest

from equity_aggregator.domain._utils import EquityIdentifiers, extract_identifiers
from equity_aggregator.domain.pipeline.transforms.enrich import (
    EnrichmentFeed,
    _enrich_equity_group,
    _enrich_from_feed,
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


class ErrorFeedData:
    @staticmethod
    def model_validate(record: dict[str, object]) -> "ErrorFeedData":
        class _ValidationError(Exception):
            def errors(self) -> list[dict[str, tuple[str]]]:
                # mimic both currency and market_cap are invalid
                return [{"loc": ("currency",)}, {"loc": ("market_cap",)}]

        raise _ValidationError("validation failed")


def test_extract_identifiers_single_source() -> None:
    """
    ARRANGE: single RawEquity with identifiers
    ACT:     call extract_identifiers
    ASSERT:  returns EquityIdentifiers with all identifiers from that source
    """
    source = RawEquity(
        name="TEST",
        symbol="TST",
        isin="US0378331005",
        cusip="037833100",
        cik="0000320193",
        share_class_figi="BBG000BLNNH6",
        mics=["XNAS"],
        currency="USD",
        last_price=Decimal("100"),
        market_cap=Decimal("1000000"),
    )

    identifiers = extract_identifiers([source])

    assert identifiers == EquityIdentifiers(
        symbol="TST",
        name="TEST",
        isin="US0378331005",
        cusip="037833100",
        cik="0000320193",
        share_class_figi="BBG000BLNNH6",
    )


def test_extract_identifiers_multiple_sources() -> None:
    """
    ARRANGE: three RawEquity sources with different symbols
    ACT:     call extract_identifiers
    ASSERT:  returns representative symbol (most frequent)
    """
    first = RawEquity(
        name="APPLE",
        symbol="AAPL",
        share_class_figi="BBG000BLNNH6",
        mics=["XNAS"],
        currency="USD",
        last_price=Decimal("150"),
        market_cap=Decimal("2500000000000"),
    )

    second = RawEquity(
        name="APPLE INC",
        symbol="AAPL",
        share_class_figi="BBG000BLNNH6",
        mics=["XNAS"],
        currency="USD",
        last_price=Decimal("151"),
        market_cap=Decimal("2500000000000"),
    )

    third = RawEquity(
        name="Apple Inc.",
        symbol="AAPL.US",
        share_class_figi="BBG000BLNNH6",
        mics=["XNAS"],
        currency="USD",
        last_price=Decimal("150.5"),
        market_cap=Decimal("2500000000000"),
    )

    identifiers = extract_identifiers([first, second, third])

    assert identifiers.symbol == "AAPL"


def test_enrich_from_feed_returns_none_on_validation_failure() -> None:
    """
    ARRANGE: EquityIdentifiers, fetcher returns data that fails validation
    ACT:     call _enrich_from_feed
    ASSERT:  returns None after validation fails
    """

    async def bad_data_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> dict[str, object]:
        return {"invalid": "data", "more": "fields"}

    identifiers = EquityIdentifiers(
        symbol="BAD",
        name="BAD",
        isin="ISIN00000014",
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    mock_feed = EnrichmentFeed(fetch=bad_data_fetcher, model=BadFeedData)

    actual = asyncio.run(_enrich_from_feed(identifiers, mock_feed))

    assert actual is None


def test_enrich_equity_group_merges_all_sources() -> None:
    """
    ARRANGE: two discovery sources with different data, good enrichment feed
    ACT:     call _enrich_equity_group
    ASSERT:  returns RawEquity respecting price range quorum
    """
    first_source = RawEquity(
        name="FULL",
        symbol="FULL",
        isin="US0378331005",
        cusip="037833100",
        share_class_figi="BBG000BLNNH6",
        mics=["XNAS"],
        currency="USD",
        last_price=Decimal("150"),
        market_cap=None,
        fifty_two_week_min=Decimal("125"),
    )

    second_source = RawEquity(
        name="FULL INC",
        symbol="FULL",
        isin="US0378331005",
        share_class_figi="BBG000BLNNH6",
        mics=["XNAS"],
        currency="USD",
        last_price=None,
        market_cap=Decimal("250000000000"),
        fifty_two_week_min=Decimal("120"),
    )

    async def good_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> dict[str, object]:
        return {
            "name": name,
            "symbol": symbol,
            "isin": isin,
            "cusip": cusip,
            "mics": ["XNAS"],
            "currency": "USD",
            "last_price": Decimal("150"),
            "market_cap": Decimal("250000000000"),
            "fifty_two_week_min": Decimal("122"),
        }

    mock_feed = EnrichmentFeed(fetch=good_fetcher, model=GoodFeedData)

    merged = asyncio.run(
        _enrich_equity_group([first_source, second_source], (mock_feed,)),
    )

    assert merged.fifty_two_week_min is None


def test_safe_fetch_timeout_returns_none() -> None:
    """
    ARRANGE: slow fetcher wrapped with timeout that raises TimeoutError
    ACT:     call _safe_fetch
    ASSERT:  returns None
    """

    async def slow_fetcher(**kwargs: object) -> dict[str, object]:
        await asyncio.sleep(0.05)
        return {"foo": "bar"}

    async def timeout_fetcher(**kwargs: object) -> dict[str, object]:
        return await asyncio.wait_for(slow_fetcher(**kwargs), timeout=0.01)

    identifiers = EquityIdentifiers(
        symbol="TST",
        name="TST",
        isin="ISIN00000004",
        cusip="037833100",
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = asyncio.run(_safe_fetch(identifiers, timeout_fetcher, "Slow"))

    assert actual is None


def test_safe_fetch_exception_returns_none() -> None:
    """
    ARRANGE: fetcher that raises an exception
    ACT:     call _safe_fetch
    ASSERT:  returns None
    """

    async def bad_fetcher(**kwargs: object) -> dict[str, object]:
        raise RuntimeError("failure")

    identifiers = EquityIdentifiers(
        symbol="TST",
        name="TST",
        isin="ISIN00000004",
        cusip="037833100",
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = asyncio.run(_safe_fetch(identifiers, bad_fetcher, "Bad"))

    assert actual is None


def test_safe_fetch_success_returns_dict() -> None:
    """
    ARRANGE: fetcher that returns quickly
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

    identifiers = EquityIdentifiers(
        symbol="A",
        name="A",
        isin="ISIN00000004",
        cusip="037833100",
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = asyncio.run(_safe_fetch(identifiers, quick_fetcher, "Quick"))

    assert actual == {"foo": "bar"}


def test_enrich_empty_stream_yields_nothing() -> None:
    """
    ARRANGE: async stream that never yields
    ACT:     run enrich()
    ASSERT:  yields empty list
    """

    async def empty_src() -> AsyncIterable[list[RawEquity]]:
        if False:
            yield

    async def runner() -> list[RawEquity]:
        return [equity async for equity in enrich(empty_src())]

    actual = asyncio.run(runner())
    assert actual == []


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
        return await asyncio.wait_for(slow_fetcher(**kwargs), timeout=0.01)

    identifiers = EquityIdentifiers(
        symbol="TO",
        name="TO",
        isin="ISIN00000005",
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = asyncio.run(_safe_fetch(identifiers, timeout_fetcher, "Slow"))

    assert actual is None


def test_enrich_from_feed_completes_success_path() -> None:
    """
    ARRANGE: identifiers dict, fetcher returns full record
    ACT:     call _enrich_from_feed
    ASSERT:  enriched RawEquity contains the fetched last_price & market_cap
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
            "currency": "USD",
            "last_price": Decimal("123"),
            "market_cap": Decimal("4567"),
        }

    identifiers = EquityIdentifiers(
        symbol="OK",
        name="OK",
        isin="ISIN00000006",
        cusip="037833100",
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    mock_feed = EnrichmentFeed(fetch=good_fetcher, model=GoodFeedData)

    enriched = asyncio.run(_enrich_from_feed(identifiers, mock_feed))

    assert (enriched.last_price, enriched.market_cap) == (
        Decimal("123"),
        Decimal("4567"),
    )


def test_safe_fetch_lookup_error_returns_none() -> None:
    """
    ARRANGE: fetcher that raises LookupError
    ACT:     call _safe_fetch
    ASSERT:  returns None (the call routes through _log_outcome)
    """

    async def not_found_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> dict[str, object]:
        raise LookupError("no data")

    identifiers = EquityIdentifiers(
        symbol="NF",
        name="NF",
        isin="ISIN00000007",
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = asyncio.run(
        _safe_fetch(identifiers, not_found_fetcher, "NotFoundFeed"),
    )

    assert actual is None


def test_validate_returns_raw_equity() -> None:
    """
    ARRANGE: record and GoodFeedData model
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

    identifiers = EquityIdentifiers(
        symbol="VAL",
        name="VAL",
        isin="ISIN00000004",
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = _validate(raw_record, GoodFeedData, "GoodFeed", identifiers)

    assert isinstance(actual, RawEquity)


def test_validate_returns_none_on_error() -> None:
    """
    ARRANGE: record and BadFeedData model
    ACT:     call _validate
    ASSERT:  returns None
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

    identifiers = EquityIdentifiers(
        symbol="VAL",
        name="VAL",
        isin="ISIN00000004",
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = _validate(raw_record, BadFeedData, "BadFeed", identifiers)

    assert actual is None


def test_enrich_from_feed_returns_none_on_empty_dict() -> None:
    """
    ARRANGE: identifiers dict
    ACT:     call _enrich_from_feed with an empty fetcher
    ASSERT:  returns None (fetch returns empty dict, validation fails)
    """

    async def empty_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
    ) -> dict[str, object]:
        return {}

    identifiers = EquityIdentifiers(
        symbol="E",
        name="E",
        isin=None,
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    mock_feed = EnrichmentFeed(fetch=empty_fetcher, model=GoodFeedData)

    actual = asyncio.run(_enrich_from_feed(identifiers, mock_feed))

    assert actual is None


def test_validate_handles_error_feed() -> None:
    """
    ARRANGE: ErrorFeedData model that raises with .errors()
    ACT:     call _validate to trigger the exception
    ASSERT:  returns None (handles hasattr(error, "errors"))
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

    identifiers = EquityIdentifiers(
        symbol="X",
        name="X",
        isin="ISIN00000008",
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = _validate(raw_record, ErrorFeedData, "ErrorFeed", identifiers)

    assert actual is None


def test_to_usd_handles_converter_returning_none() -> None:
    """
    ARRANGE: validated equity whose `model_copy` is overridden to return None,
             making the USD-converter return None.
    ACT:     call _to_usd
    ASSERT:  returns None
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

    identifiers = EquityIdentifiers(
        symbol="NONE",
        name="NONE",
        isin="ISIN00000010",
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = asyncio.run(_to_usd(validated, "FxFeed", identifiers))

    assert actual is None


def test_to_usd_converts_currency() -> None:
    """
    ARRANGE: equity in EUR
    ACT:     call _to_usd
    ASSERT:  returns converted equity in USD
    """
    validated = RawEquity(
        name="SAME",
        symbol="SAME",
        isin="ISIN00000012",
        mics=["XLON"],
        currency="EUR",
        last_price=Decimal("100"),
        market_cap=Decimal("5000"),
    )

    identifiers = EquityIdentifiers(
        symbol="SAME",
        name="SAME",
        isin="ISIN00000012",
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = asyncio.run(_to_usd(validated, "TestFeed", identifiers))

    assert actual.currency == "USD"


def test_enrich_passes_through_groups() -> None:
    """
    ARRANGE: async stream of equity groups
    ACT:     run enrich() over that stream
    ASSERT:  yields merged equities
    """
    first_equity = RawEquity(
        name="ONE",
        symbol="ONE",
        share_class_figi="FIGI00000001",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("1"),
        market_cap=Decimal("100"),
    )

    second_equity = RawEquity(
        name="TWO",
        symbol="TWO",
        share_class_figi="FIGI00000002",
        mics=["XLON"],
        currency="USD",
        last_price=Decimal("2"),
        market_cap=Decimal("200"),
    )

    async def source() -> AsyncIterable[list[RawEquity]]:
        yield [first_equity]
        yield [second_equity]

    async def runner() -> list[RawEquity]:
        return [equity async for equity in enrich(source())]

    actual = asyncio.run(runner())

    symbols = sorted(equity.symbol for equity in actual)

    assert symbols == ["ONE", "TWO"]


def test_extract_identifiers_handles_none_values() -> None:
    """
    ARRANGE: sources with None identifiers
    ACT:     call extract_identifiers
    ASSERT:  returns EquityIdentifiers with None for missing identifiers
    """
    source = RawEquity(
        name="INCOMPLETE",
        symbol="INC",
        isin=None,
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
        mics=["XNAS"],
        currency="USD",
        last_price=Decimal("100"),
        market_cap=Decimal("1000000"),
    )

    identifiers = extract_identifiers([source])

    assert identifiers == EquityIdentifiers(
        symbol="INC",
        name="INCOMPLETE",
        isin=None,
        cusip=None,
        cik=None,
        share_class_figi="BBG000BLNNH6",
    )
