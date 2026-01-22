# tests/unit/domain/pipeline/transforms/test_parse.py

import asyncio
from collections.abc import AsyncIterable
from decimal import Decimal

import pytest

from equity_aggregator.domain.pipeline.resolve import FeedRecord
from equity_aggregator.domain.pipeline.transforms.parse import parse
from equity_aggregator.schemas import (
    LsegFeedData,
    XetraFeedData,
)
from equity_aggregator.schemas.raw import RawEquity

pytestmark = pytest.mark.unit


def _run_parse(records: list[FeedRecord]) -> list[RawEquity]:
    """
    Runs the asynchronous 'parse' function on a list of FeedRecord objects and returns
    the resulting list of RawEquity objects.

    Args:
        records (list[FeedRecord]): A list of FeedRecord instances to be parsed.

    Returns:
        list[RawEquity]: A list of RawEquity objects produced by parsing input records.
    """

    async def source() -> AsyncIterable[FeedRecord]:
        for record in records:
            yield record

    async def runner() -> list[RawEquity]:
        return [equity async for equity in parse(source())]

    return asyncio.run(runner())


def test_parse_valid_lseg_record_converts_gbx_and_defaults_mics() -> None:
    """
    ARRANGE: a LsegFeedData record with GBX currency, pence lastprice, and no isin
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with converted price
    """
    raw = {
        "issuername": "LSEG CO",
        "tidm": "LSEC",
        "isin": None,
        "currency": "GBX",
        "lastprice": "123,45",  # 123.45 pence => £1.2345
        "marketcapitalization": None,
        "fiftyTwoWeeksMin": None,
        "fiftyTwoWeeksMax": None,
    }
    record = FeedRecord(LsegFeedData, raw)

    actual = _run_parse([record])

    assert [
        (
            equity.name,
            equity.symbol,
            equity.currency,
            equity.last_price,
            equity.market_cap,
            equity.isin,
        )
        for equity in actual
    ] == [("LSEG CO", "LSEC", "GBP", Decimal("1.2345"), None, None)]


def test_parse_valid_xetra_record_defaults_mics_and_flattens_fields() -> None:
    """
    ARRANGE: a XetraFeedData record with nested overview/key_data and missing mic
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with flattened fields and default mic
    """
    raw = {
        "name": "XETRA LTD",
        "wkn": "XTL",
        "isin": "DE1234567890",
        "mic": None,
        "currency": "EUR",
        "overview": {"lastPrice": "50.00"},
        "key_data": {"marketCapitalisation": 5000},
    }
    record = FeedRecord(XetraFeedData, raw)

    actual = _run_parse([record])

    assert [
        (
            equity.name,
            equity.symbol,
            equity.isin,
            equity.mics,
            equity.currency,
            equity.last_price,
            equity.market_cap,
        )
        for equity in actual
    ] == [
        (
            "XETRA LTD",
            "XTL",
            "DE1234567890",
            ["XETR"],
            "EUR",
            Decimal("50.00"),
            Decimal("5000"),
        ),
    ]


def test_parse_skips_invalid_records_across_feeds() -> None:
    """
    ARRANGE: a mix of valid and invalid records from multiple feeds
    ACT:     run parse() over that list
    ASSERT:  yields exactly one RawEquity with the valid symbol
    """

    missing_name_gbx = {
        "symbol": "B2",
        "isin": None,
        "mics": [],
        "currency": "GBX",
        "lastvalue": "not_a_number",
    }
    missing_overview = {
        "name": "X",
        "wkn": "WX",
        "isin": None,
        "mic": "XETR",
        "currency": "EUR",
        "overview": {},
        "key_data": {},
    }

    records = [
        FeedRecord(LsegFeedData, missing_name_gbx),
        FeedRecord(XetraFeedData, missing_overview),
    ]

    actual = _run_parse(records)

    assert [equity.symbol for equity in actual] == ["WX"]


def test_parse_lseg_record_non_gbx_pass_through() -> None:
    """
    ARRANGE: a LsegFeedData record with non-GBX currency and numeric lastprice
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with last_price unchanged & currency unchanged
    """

    raw = {
        "issuername": "ABC LTD",
        "tidm": "ABCL",
        "isin": None,
        "currency": "GBP",
        "lastprice": "250",
        "marketcapitalization": None,
        "fiftyTwoWeeksMin": None,
        "fiftyTwoWeeksMax": None,
    }

    record = FeedRecord(LsegFeedData, raw)

    actual = _run_parse([record])

    assert [(equity.last_price, equity.currency) for equity in actual] == [
        (Decimal("250"), "GBP"),
    ]


def test_parse_lseg_gbx_with_none_lastprice() -> None:
    """
    ARRANGE: a LsegFeedData record with GBX currency and no lastprice
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with last_price None and currency 'GBP'
    """
    raw = {
        "issuername": "XYZ PLC",
        "tidm": "XYZ",
        "isin": None,
        "currency": "GBX",
        "lastprice": None,
        "marketcapitalization": None,
        "fiftyTwoWeeksMin": None,
        "fiftyTwoWeeksMax": None,
    }
    record = FeedRecord(LsegFeedData, raw)

    actual = _run_parse([record])

    assert [
        (
            equity.name,
            equity.symbol,
            equity.currency,
            equity.last_price,
            equity.market_cap,
            equity.isin,
        )
        for equity in actual
    ] == [("XYZ PLC", "XYZ", "GBP", None, None, None)]


def test_parse_xetra_only_key_data() -> None:
    """
    ARRANGE: a XetraFeedData record with only key_data and empty overview
    ACT:     run parse() over that single record
    ASSERT:  yields exactly one RawEquity with last_price None and market_cap set
    """
    raw = {
        "name": "KEY CORP",
        "wkn": "KEY",
        "isin": "DE000KEY0001",
        "mic": "XETR",
        "currency": "EUR",
        "overview": {},
        "key_data": {"marketCapitalisation": Decimal("3000")},
    }
    record = FeedRecord(XetraFeedData, raw)

    actual = _run_parse([record])

    assert [(equity.last_price, equity.market_cap) for equity in actual] == [
        (None, Decimal("3000")),
    ]


def test_parse_preserves_input_order_across_feeds() -> None:
    """
    ARRANGE: one LSEG, one Xetra record in a known order
    ACT:     run parse() over that list
    ASSERT:  yields RawEquity instances in the same order
    """

    raw_lseg_data = {
        "issuername": "L2",
        "tidm": "L2",
        "isin": None,
        "currency": "GBP",
        "lastprice": "200",
        "marketcapitalization": None,
        "fiftyTwoWeeksMin": None,
        "fiftyTwoWeeksMax": None,
    }
    raw_xetra_data = {
        "name": "X3",
        "wkn": "X3",
        "isin": "DE0000000003",
        "mic": "XETR",
        "currency": "EUR",
        "overview": {"lastPrice": "5"},
        "key_data": {"marketCapitalisation": "50"},
    }

    records = [
        FeedRecord(LsegFeedData, raw_lseg_data),
        FeedRecord(XetraFeedData, raw_xetra_data),
    ]

    actual = _run_parse(records)

    assert [equity.symbol for equity in actual] == ["L2", "X3"]
