# transforms/test_group.py

from collections.abc import AsyncIterator
from typing import Any

import pytest

from equity_aggregator.domain.pipeline.transforms.group import group
from equity_aggregator.schemas.raw import RawEquity

pytestmark = pytest.mark.unit


async def _convert_to_async_iterable(records: list[Any]) -> AsyncIterator[Any]:
    """
    Convert a list of records into an asynchronous iterable.

    Args:
        records (list): The list of records to yield asynchronously.

    Yields:
        Any: Each record from the input list, yielded asynchronously.
    """
    for record in records:
        yield record


async def test_group_empty_list_returns_empty() -> None:
    """
    ARRANGE: no raw equities
    ACT:     group
    ASSERT:  returns empty list
    """
    actual = [
        equity_group async for equity_group in group(_convert_to_async_iterable([]))
    ]

    assert actual == []


async def test_single_equity_yields_single_group() -> None:
    """
    ARRANGE: one equity, one share_class_figi
    ACT:     group
    ASSERT:  yields single group containing that equity
    """
    raw_equity = RawEquity(
        name="SOLO CORP",
        symbol="SOLO",
        share_class_figi="FIGI00000001",
    )

    actual = [
        equity_group
        async for equity_group in group(_convert_to_async_iterable([raw_equity]))
    ]

    assert actual == [[raw_equity]]


async def test_grouping_by_share_class_figi_yields_two_groups() -> None:
    """
    ARRANGE: three records, two distinct FIGIs
    ACT:     group
    ASSERT:  yields two groups
    """
    first_equity = RawEquity(name="A", symbol="A", share_class_figi="FIGI00000001")
    second_equity = RawEquity(name="B", symbol="B", share_class_figi="FIGI00000002")
    third_equity = RawEquity(name="C", symbol="C", share_class_figi="FIGI00000001")

    actual = [
        equity_group
        async for equity_group in group(
            _convert_to_async_iterable([first_equity, second_equity, third_equity]),
        )
    ]

    assert len(actual) == 2


async def test_grouping_preserves_all_sources() -> None:
    """
    ARRANGE: three records, two distinct FIGIs
    ACT:     group
    ASSERT:  first group contains two equities with same FIGI
    """
    first_equity = RawEquity(name="A", symbol="A", share_class_figi="FIGI00000001")
    second_equity = RawEquity(name="B", symbol="B", share_class_figi="FIGI00000002")
    third_equity = RawEquity(name="C", symbol="C", share_class_figi="FIGI00000001")

    actual = [
        equity_group
        async for equity_group in group(
            _convert_to_async_iterable([first_equity, second_equity, third_equity]),
        )
    ]

    assert actual[0] == [first_equity, third_equity]
