# transforms/test_canonicalise.py

from collections.abc import AsyncIterable

import pytest

from equity_aggregator.domain.pipeline.transforms.canonicalise import canonicalise
from equity_aggregator.schemas import CanonicalEquity
from equity_aggregator.schemas.raw import RawEquity

pytestmark = pytest.mark.unit


async def _async_list(items: list) -> AsyncIterable:
    """Helper to convert list to async iterable."""
    for item in items:
        yield item


async def test_canonicalise_single_equity_yields_one_result() -> None:
    """
    ARRANGE: Single RawEquity with valid data
    ACT:     canonicalise
    ASSERT:  yields exactly one result
    """
    raw_equity = RawEquity(
        share_class_figi="BBG000B9XRY4",
        name="Apple Inc",
        ticker="AAPL",
        symbol="AAPL",
        currency="USD",
        price=150.0,
        market_cap=2_500_000_000_000.0,
    )

    actual = [equity async for equity in canonicalise(_async_list([raw_equity]))]

    assert len(actual) == 1


async def test_canonicalise_returns_canonical_equity_type() -> None:
    """
    ARRANGE: Single RawEquity with valid data
    ACT:     canonicalise
    ASSERT:  yields CanonicalEquity instance
    """
    raw_equity = RawEquity(
        share_class_figi="BBG000B9XRY4",
        name="Apple Inc",
        ticker="AAPL",
        symbol="AAPL",
        currency="USD",
        price=150.0,
        market_cap=2_500_000_000_000.0,
    )

    actual = [equity async for equity in canonicalise(_async_list([raw_equity]))]

    assert isinstance(actual[0], CanonicalEquity)


async def test_canonicalise_preserves_figi_identity() -> None:
    """
    ARRANGE: RawEquity with specific FIGI
    ACT:     canonicalise
    ASSERT:  CanonicalEquity preserves same FIGI
    """
    figi = "BBG000BMHYD1"
    raw_equity = RawEquity(
        share_class_figi=figi,
        name="Tesla Inc",
        ticker="TSLA",
        symbol="TSLA",
        currency="USD",
        price=800.0,
        market_cap=800_000_000_000.0,
    )

    actual = [equity async for equity in canonicalise(_async_list([raw_equity]))]

    assert actual[0].identity.share_class_figi == figi


async def test_canonicalise_empty_stream_yields_no_results() -> None:
    """
    ARRANGE: Empty async iterable
    ACT:     canonicalise
    ASSERT:  yields no results
    """
    actual = [equity async for equity in canonicalise(_async_list([]))]

    assert len(actual) == 0


async def test_canonicalise_multiple_equities_yields_correct_count() -> None:
    """
    ARRANGE: Two RawEquity objects
    ACT:     canonicalise
    ASSERT:  yields exactly two results
    """
    expected_count = 2
    raw_equities = [
        RawEquity(
            share_class_figi="BBG000B9XRY4",
            name="Apple Inc",
            ticker="AAPL",
            symbol="AAPL",
            currency="USD",
            price=150.0,
            market_cap=2_500_000_000_000.0,
        ),
        RawEquity(
            share_class_figi="BBG000BVPV84",
            name="Amazon.com Inc",
            ticker="AMZN",
            symbol="AMZN",
            currency="USD",
            price=3200.0,
            market_cap=1_600_000_000_000.0,
        ),
    ]

    actual = [equity async for equity in canonicalise(_async_list(raw_equities))]

    assert len(actual) == expected_count
