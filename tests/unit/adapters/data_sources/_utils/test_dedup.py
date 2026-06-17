# _utils/test_dedup.py

from collections.abc import AsyncGenerator

import pytest

from equity_aggregator.adapters.data_sources._utils.dedup import deduplicate_records

pytestmark = pytest.mark.unit


async def test_deduplicate_records_filters_duplicates() -> None:
    """
    ARRANGE: two records share an identical extracted key
    ACT:     run the deduplicator
    ASSERT:  yields a single unique record
    """

    async def _gen() -> AsyncGenerator[dict, None]:
        yield {"key": 1}
        yield {"key": 1}

    deduplicator = deduplicate_records(lambda record: record["key"])

    uniques = [record async for record in deduplicator(_gen())]

    assert len(uniques) == 1


async def test_deduplicate_records_preserves_first_occurrence() -> None:
    """
    ARRANGE: first record differs from the duplicate that follows
    ACT:     run the deduplicator
    ASSERT:  the first record is preserved
    """

    async def _gen() -> AsyncGenerator[dict, None]:
        yield {"key": 1, "name": "FIRST"}
        yield {"key": 1, "name": "SECOND"}

    deduplicator = deduplicate_records(lambda record: record["key"])

    uniques = [record async for record in deduplicator(_gen())]

    assert uniques[0]["name"] == "FIRST"


async def test_deduplicate_records_empty_stream_yields_nothing() -> None:
    """
    ARRANGE: an empty record stream
    ACT:     run the deduplicator
    ASSERT:  yields no records
    """

    async def _gen() -> AsyncGenerator[dict, None]:
        return
        yield  # pragma: no cover - makes _gen an async generator

    deduplicator = deduplicate_records(lambda record: record["key"])

    uniques = [record async for record in deduplicator(_gen())]

    assert uniques == []
