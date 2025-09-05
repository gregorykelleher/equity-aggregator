# pipeline/test_runner.py

import pytest

from equity_aggregator.domain.pipeline.runner import aggregate_canonical_equities
from equity_aggregator.schemas import CanonicalEquity

pytestmark = pytest.mark.unit


async def test_aggregate_canonical_equities_returns_list() -> None:
    """
    ARRANGE: No setup required
    ACT:     Call aggregate_canonical_equities
    ASSERT:  Returns a list
    """
    actual = await aggregate_canonical_equities()

    assert isinstance(actual, list)


async def test_aggregate_canonical_equities_returns_canonical_equities() -> None:
    """
    ARRANGE: No setup required
    ACT:     Call aggregate_canonical_equities
    ASSERT:  All items are CanonicalEquity instances
    """
    actual = await aggregate_canonical_equities()

    assert all(isinstance(equity, CanonicalEquity) for equity in actual)


async def test_aggregate_canonical_equities_is_async_function() -> None:
    """
    ARRANGE: No setup required
    ACT:     Call aggregate_canonical_equities
    ASSERT:  Function is awaitable
    """
    coro = aggregate_canonical_equities()

    assert hasattr(coro, "__await__")

    await coro
