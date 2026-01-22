# transforms/test_enrich.py

import asyncio
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from decimal import Decimal

import pytest

from equity_aggregator.domain._utils import EquityIdentifiers, extract_identifiers
from equity_aggregator.domain.pipeline.transforms.enrich import (
    EnrichmentFeed,
    FeedSpec,
    _enrich_equity_group,
    _enrich_from_feed,
    _init_feed,
    _open_feeds,
    _process_stream,
    _rate_limited,
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


class StubFeed:
    """
    Test feed that returns a fixed response.
    """

    def __init__(self, response: dict[str, object]) -> None:
        self.response = response

    async def fetch_equity(self, **kwargs: object) -> dict[str, object]:
        return self.response


def stub_feed_factory(response: dict[str, object]) -> object:
    """
    Create an async context manager factory for StubFeed.
    """

    @asynccontextmanager
    async def factory() -> AsyncIterator[StubFeed]:
        yield StubFeed(response)

    return factory


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
        lei=None,
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
        lei=None,
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
        **kwargs: object,
    ) -> dict[str, object]:
        return {"invalid": "data", "more": "fields"}

    identifiers = EquityIdentifiers(
        symbol="BAD",
        name="BAD",
        isin="ISIN00000014",
        cusip=None,
        cik=None,
        lei=None,
        share_class_figi="BBG000BLNNH6",
    )

    mock_feed = EnrichmentFeed(fetch=bad_data_fetcher, model=BadFeedData)

    actual = asyncio.run(_enrich_from_feed(identifiers, mock_feed))

    assert actual is None


def test_enrich_equity_group_merges_all_sources() -> None:
    """
    ARRANGE: two discovery sources with different data, good enrichment feed
    ACT:     call _enrich_equity_group
    ASSERT:  returns RawEquity with fallback merge of partial price data
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
        **kwargs: object,
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

    assert merged.fifty_two_week_min == Decimal("122")


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
        lei=None,
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
        lei=None,
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
        **kwargs: object,
    ) -> dict[str, object]:
        _ = (symbol, name, isin, cusip)
        return {"foo": "bar"}

    identifiers = EquityIdentifiers(
        symbol="A",
        name="A",
        isin="ISIN00000004",
        cusip="037833100",
        cik=None,
        lei=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = asyncio.run(_safe_fetch(identifiers, quick_fetcher, "Quick"))

    assert actual == {"foo": "bar"}


def test_process_stream_empty_stream_yields_nothing() -> None:
    """
    ARRANGE: async stream that never yields and test feed
    ACT:     run _process_stream
    ASSERT:  yields empty list
    """

    async def good_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
        **kwargs: object,
    ) -> dict[str, object]:
        return {}

    mock_feed = EnrichmentFeed(fetch=good_fetcher, model=GoodFeedData)

    async def empty_src() -> AsyncIterable[list[RawEquity]]:
        if False:
            yield

    async def runner() -> list[RawEquity]:
        return [equity async for equity in _process_stream(empty_src(), (mock_feed,))]

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
        lei=None,
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
        **kwargs: object,
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
        lei=None,
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
        **kwargs: object,
    ) -> dict[str, object]:
        raise LookupError("no data")

    identifiers = EquityIdentifiers(
        symbol="NF",
        name="NF",
        isin="ISIN00000007",
        cusip=None,
        cik=None,
        lei=None,
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
        lei=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = _validate(raw_record, GoodFeedData, "GoodFeed", identifiers)

    assert isinstance(actual, RawEquity)


def test_validate_injects_figi_when_missing() -> None:
    """
    ARRANGE: record without share_class_figi
    ACT:     call _validate
    ASSERT:  share_class_figi is injected from identifiers
    """

    identifiers = EquityIdentifiers(
        symbol="VAL",
        name="VAL",
        isin="ISIN00000004",
        cusip=None,
        cik=None,
        lei=None,
        share_class_figi="BBG000BLNNH6",
    )

    raw_record = {
        "name": "VAL",
        "symbol": "VAL",
        "isin": "ISIN00000004",
        "mics": ["XLON"],
        "currency": "USD",
        "last_price": Decimal("3"),
        "market_cap": Decimal("30"),
    }

    actual = _validate(raw_record, GoodFeedData, "GoodFeed", identifiers)

    assert getattr(actual, "share_class_figi", None) == "BBG000BLNNH6"


def test_validate_injects_figi_when_none() -> None:
    """
    ARRANGE: record with share_class_figi set to None
    ACT:     call _validate
    ASSERT:  share_class_figi is injected from identifiers
    """

    identifiers = EquityIdentifiers(
        symbol="VAL",
        name="VAL",
        isin="ISIN00000004",
        cusip=None,
        cik=None,
        lei=None,
        share_class_figi="BBG000BLNNH6",
    )

    raw_record = {
        "name": "VAL",
        "symbol": "VAL",
        "isin": "ISIN00000004",
        "share_class_figi": None,
        "mics": ["XLON"],
        "currency": "USD",
        "last_price": Decimal("3"),
        "market_cap": Decimal("30"),
    }

    actual = _validate(raw_record, GoodFeedData, "GoodFeed", identifiers)

    assert getattr(actual, "share_class_figi", None) == "BBG000BLNNH6"


def test_validate_preserves_figi_when_present() -> None:
    """
    ARRANGE: record with share_class_figi already populated
    ACT:     call _validate
    ASSERT:  share_class_figi remains unchanged from record
    """

    identifiers = EquityIdentifiers(
        symbol="VAL",
        name="VAL",
        isin="ISIN00000004",
        cusip=None,
        cik=None,
        lei=None,
        share_class_figi="BBG000BLNNH6",
    )

    raw_record = {
        "name": "VAL",
        "symbol": "VAL",
        "isin": "ISIN00000004",
        "share_class_figi": "BBG000BLNNH7",
        "mics": ["XLON"],
        "currency": "USD",
        "last_price": Decimal("3"),
        "market_cap": Decimal("30"),
    }

    actual = _validate(raw_record, GoodFeedData, "GoodFeed", identifiers)

    assert getattr(actual, "share_class_figi", None) == "BBG000BLNNH7"


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
        lei=None,
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
        **kwargs: object,
    ) -> dict[str, object]:
        return {}

    identifiers = EquityIdentifiers(
        symbol="E",
        name="E",
        isin=None,
        cusip=None,
        cik=None,
        lei=None,
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
        lei=None,
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
        lei=None,
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
        lei=None,
        share_class_figi="BBG000BLNNH6",
    )

    actual = asyncio.run(_to_usd(validated, "TestFeed", identifiers))

    assert actual.currency == "USD"


def test_process_stream_passes_through_groups() -> None:
    """
    ARRANGE: async stream of equity groups and test feeds
    ACT:     run _process_stream over that stream
    ASSERT:  yields merged equities for each group
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

    async def good_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
        **kwargs: object,
    ) -> dict[str, object]:
        return {
            "name": name,
            "symbol": symbol,
            "mics": ["XLON"],
            "currency": "USD",
            "last_price": Decimal("10"),
            "market_cap": Decimal("1000"),
        }

    mock_feed = EnrichmentFeed(fetch=good_fetcher, model=GoodFeedData)

    async def source() -> AsyncIterable[list[RawEquity]]:
        yield [first_equity]
        yield [second_equity]

    async def runner() -> list[RawEquity]:
        return [equity async for equity in _process_stream(source(), (mock_feed,))]

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
        lei=None,
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
        lei=None,
        share_class_figi="BBG000BLNNH6",
    )


def test_enrich_from_feed_returns_validated_when_currency_is_none() -> None:
    """
    ARRANGE: fetcher returns non-monetary data (no currency), like GLEIF
    ACT:     call _enrich_from_feed
    ASSERT:  returns validated RawEquity without USD conversion
    """

    async def non_monetary_fetcher(
        symbol: str,
        name: str,
        isin: str | None,
        cusip: str | None,
        **kwargs: object,
    ) -> dict[str, object]:
        return {
            "name": name,
            "symbol": symbol,
            "isin": isin,
            "lei": "G1MSYD2DQZX30V5DMX12",
        }

    identifiers = EquityIdentifiers(
        symbol="LEI",
        name="LEI Corp",
        isin="US0378331005",
        cusip=None,
        cik=None,
        lei=None,
        share_class_figi="BBG000BLNNH6",
    )

    mock_feed = EnrichmentFeed(fetch=non_monetary_fetcher, model=GoodFeedData)

    actual = asyncio.run(_enrich_from_feed(identifiers, mock_feed))

    assert actual.lei == "G1MSYD2DQZX30V5DMX12"


def test_rate_limited_passes_through_result() -> None:
    """
    ARRANGE: async function wrapped with _rate_limited
    ACT:     call the wrapped function
    ASSERT:  returns the original function's result
    """

    async def simple_fetcher(**kwargs: object) -> dict[str, object]:
        return {"result": "success"}

    semaphore = asyncio.Semaphore(10)
    wrapped = _rate_limited(simple_fetcher, semaphore)

    actual = asyncio.run(wrapped(symbol="TEST"))

    assert actual == {"result": "success"}


def test_rate_limited_serialises_calls_with_semaphore_of_one() -> None:
    """
    ARRANGE: semaphore with limit of 1, two concurrent calls
    ACT:     start both calls simultaneously
    ASSERT:  first call completes before second call starts
    """
    call_order: list[str] = []

    async def tracking_fetcher(order: str, **kwargs: object) -> dict[str, object]:
        call_order.append(f"{order}_start")
        await asyncio.sleep(0.01)
        call_order.append(f"{order}_end")
        return {}

    semaphore = asyncio.Semaphore(1)
    wrapped = _rate_limited(tracking_fetcher, semaphore)

    async def runner() -> None:
        await asyncio.gather(
            wrapped(order="first"),
            wrapped(order="second"),
        )

    asyncio.run(runner())

    assert call_order.index("first_end") < call_order.index("second_start")


def test_rate_limited_raises_timeout_error() -> None:
    """
    ARRANGE: slow function with short timeout
    ACT:     call the wrapped function
    ASSERT:  raises TimeoutError
    """

    async def slow_fetcher(**kwargs: object) -> dict[str, object]:
        await asyncio.sleep(0.1)
        return {}

    semaphore = asyncio.Semaphore(10)
    wrapped = _rate_limited(slow_fetcher, semaphore, timeout=0.01)

    with pytest.raises(TimeoutError):
        asyncio.run(wrapped())


def test_open_feeds_yields_single_feed_for_single_spec() -> None:
    """
    ARRANGE: single FeedSpec with stub factory
    ACT:     open feeds using _open_feeds
    ASSERT:  yields tuple containing one feed
    """
    specs = (FeedSpec(stub_feed_factory({}), GoodFeedData, 5),)

    async def runner() -> int:
        async with _open_feeds(specs) as feeds:
            return len(feeds)

    actual = asyncio.run(runner())

    assert actual == 1


def test_open_feeds_fetch_returns_stub_response() -> None:
    """
    ARRANGE: FeedSpec with stub factory returning specific data
    ACT:     open feeds and call fetch
    ASSERT:  returns the stub response
    """
    specs = (FeedSpec(stub_feed_factory({"key": "value"}), GoodFeedData, 5),)

    async def runner() -> dict[str, object]:
        async with _open_feeds(specs) as feeds:
            return await feeds[0].fetch(symbol="X", name="X")

    actual = asyncio.run(runner())

    assert actual == {"key": "value"}


def test_init_feed_returns_functional_enrichment_feed() -> None:
    """
    ARRANGE: FeedSpec with stub factory
    ACT:     initialise feed using _init_feed
    ASSERT:  fetch returns the stub response
    """
    spec = FeedSpec(stub_feed_factory({"result": "ok"}), GoodFeedData, 5)

    async def runner() -> dict[str, object]:
        async with AsyncExitStack() as stack:
            feed = await _init_feed(spec, stack)
            return await feed.fetch(symbol="X", name="X")

    actual = asyncio.run(runner())

    assert actual == {"result": "ok"}


def test_enrich_yields_merged_equity() -> None:
    """
    ARRANGE: async stream with one equity group
    ACT:     run enrich() over that stream
    ASSERT:  yields the merged equity (enrichment failures are handled gracefully)
    """
    equity = RawEquity(
        name="TEST",
        symbol="TEST",
        share_class_figi="BBG000TEST01",
        mics=["XNAS"],
        currency="USD",
        last_price=Decimal("100"),
        market_cap=Decimal("1000000"),
    )

    async def source() -> AsyncIterable[list[RawEquity]]:
        yield [equity]

    async def runner() -> list[RawEquity]:
        return [e async for e in enrich(source())]

    actual = asyncio.run(runner())

    assert actual[0].symbol == "TEST"
