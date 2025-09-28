# _utils/queue.py

import asyncio

from equity_aggregator.adapters.data_sources._utils._record_types import (
    EquityRecord,
    RecordStream,
)


async def enqueue_records(
    queue: asyncio.Queue[EquityRecord | None],
    records: list[EquityRecord],
) -> None:
    """
    Enqueue all records into the queue.

    Args:
        queue (asyncio.Queue[EquityRecord | None]): Queue to enqueue records into.
        records (list[EquityRecord]): List of equity records to enqueue.

    Returns:
        None
    """
    for record in records:
        await queue.put(record)


async def consume_queue(
    queue: asyncio.Queue[EquityRecord | None],
    expected_sentinels: int,
) -> RecordStream:
    """
    Yield records from queue until expected sentinel values are received.

    Args:
        queue (asyncio.Queue[EquityRecord | None]): Queue to consume equity
            records or sentinel values from.
        expected_sentinels (int): Number of sentinel (None) values to wait for
            before stopping iteration.

    Yields:
        EquityRecord: Each equity record retrieved from the queue as they arrive.
    """
    completed = 0
    while completed < expected_sentinels:
        item = await queue.get()
        if item is None:
            completed += 1
        else:
            yield item
