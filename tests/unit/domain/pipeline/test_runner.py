# pipeline/test_runner.py

from decimal import Decimal

import pytest

from equity_aggregator.domain.pipeline.runner import aggregate_canonical_equities
from equity_aggregator.schemas import CanonicalEquity
from equity_aggregator.storage import save_cache

pytestmark = pytest.mark.unit


async def test_aggregate_canonical_equities_returns_list() -> None:
    """
    ARRANGE: Set up mock exchange rate cache
    ACT:     Call aggregate_canonical_equities
    ASSERT:  Returns a list
    """
    save_cache(
        "exchange_rate_api",
        {
            "EUR": Decimal("0.5"),
            "GBP": Decimal("0.25"),
            "NOK": Decimal("0.1"),
            "SEK": Decimal("0.09"),
            "DKK": Decimal("0.15"),
            "CHF": Decimal("1.1"),
            "CAD": Decimal("0.75"),
        },
    )

    actual = await aggregate_canonical_equities()

    assert isinstance(actual, list)


async def test_aggregate_canonical_equities_returns_canonical_equities() -> None:
    """
    ARRANGE: Set up mock exchange rate cache
    ACT:     Call aggregate_canonical_equities
    ASSERT:  All items are CanonicalEquity instances
    """
    save_cache(
        "exchange_rate_api",
        {
            "EUR": Decimal("0.5"),
            "GBP": Decimal("0.25"),
            "NOK": Decimal("0.1"),
            "SEK": Decimal("0.09"),
            "DKK": Decimal("0.15"),
            "CHF": Decimal("1.1"),
            "CAD": Decimal("0.75"),
        },
    )

    actual = await aggregate_canonical_equities()

    assert all(isinstance(equity, CanonicalEquity) for equity in actual)


async def test_aggregate_canonical_equities_is_async_function() -> None:
    """
    ARRANGE: Set up mock exchange rate cache
    ACT:     Call aggregate_canonical_equities
    ASSERT:  Function is awaitable
    """
    save_cache(
        "exchange_rate_api",
        {
            "EUR": Decimal("0.5"),
            "GBP": Decimal("0.25"),
            "NOK": Decimal("0.1"),
            "SEK": Decimal("0.09"),
            "DKK": Decimal("0.15"),
            "CHF": Decimal("1.1"),
            "CAD": Decimal("0.75"),
        },
    )

    coro = aggregate_canonical_equities()

    assert hasattr(coro, "__await__")

    await coro
