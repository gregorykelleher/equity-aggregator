# pipeline/resolve.py

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from typing import NamedTuple

from equity_aggregator.adapters import (
    fetch_equity_records_intrinio,
    fetch_equity_records_lseg,
    fetch_equity_records_sec,
    fetch_equity_records_stock_analysis,
    fetch_equity_records_tradingview,
    fetch_equity_records_xetra,
)
from equity_aggregator.schemas import (
    IntrinioFeedData,
    LsegFeedData,
    SecFeedData,
    StockAnalysisFeedData,
    TradingViewFeedData,
    XetraFeedData,
)

logger = logging.getLogger(__name__)

FetchFunc = Callable[[], AsyncIterator[dict[str, object]]]
FeedPair = tuple[FetchFunc, type]


# Named tuple to hold the feed model and its raw data
class FeedRecord(NamedTuple):
    model: type
    raw_data: dict[str, object]


# List of discovery feed fetchers and their corresponding data models
_DISCOVERY_FEEDS: list[FeedPair] = [
    (fetch_equity_records_xetra, XetraFeedData),
    (fetch_equity_records_lseg, LsegFeedData),
    (fetch_equity_records_stock_analysis, StockAnalysisFeedData),
    (fetch_equity_records_tradingview, TradingViewFeedData),
    (fetch_equity_records_sec, SecFeedData),
    (fetch_equity_records_intrinio, IntrinioFeedData),
]


async def resolve(
    feeds: tuple[FeedPair, ...] | None = None,
) -> AsyncIterator[FeedRecord]:
    """
    Merge all discovery feed streams into a single asynchronous output.

    Args:
        feeds

    Returns:
        AsyncIterator[FeedRecord]: Yields FeedRecord objects as soon as they are
        available from any feed. Stops when all feeds are exhausted.

    This function launches a producer task for each feed, enqueuing FeedRecord
    items into a shared queue. Records are yielded as they arrive, ensuring
    minimal latency and efficient merging of multiple asynchronous sources.
    """
    logger.info("Resolving raw equities from discovery feeds...")

    feeds = feeds or _DISCOVERY_FEEDS
    queue: asyncio.Queue[FeedRecord | None] = asyncio.Queue()

    total_yielded = 0
    async with asyncio.TaskGroup() as task_group:
        for fetcher, model in feeds:
            task_group.create_task(_produce(fetcher, model, queue))

        # consume the queue until all producers are exhausted
        async for record in _consume(queue, len(feeds)):
            total_yielded += 1
            yield record

    if total_yielded == 0:
        raise RuntimeError(
            "All discovery feeds returned zero records; aborting seed.",
        )


async def _produce(
    fetcher: FetchFunc,
    model: type,
    queue: asyncio.Queue[FeedRecord | None],
) -> None:
    """
    Asynchronously fetches records from a data source and enqueues them for processing.

    Iterates over records from the given asynchronous fetcher, wraps each record in a
    FeedRecord using the specified model, and puts them onto the provided asyncio queue.
    After all records are enqueued, signals completion by putting None onto the queue.

    Args:
        fetcher (FetchFunc): An async function yielding records as dictionaries.
        model (type): The data model class to associate with each fetched record.
        queue (asyncio.Queue[FeedRecord | None]): Queue for FeedRecord instances and
            completion signals.

    Returns:
        None
    """
    feed_name = model.__name__
    count = 0
    try:
        async for record in fetcher():
            await queue.put(FeedRecord(model, record))
            count += 1
    except Exception as error:
        logger.error(
            "Discovery feed %s failed after %d records: %s",
            feed_name,
            count,
            error,
            exc_info=True,
        )
    else:
        _log_feed_outcome(feed_name, count)
    finally:
        await queue.put(None)


def _log_feed_outcome(feed_name: str, count: int) -> None:
    """
    Log the outcome of a successfully completed discovery feed producer.

    Args:
        feed_name (str): Name of the feed's data model.
        count (int): Number of records the feed produced.

    Returns:
        None
    """
    if count == 0:
        logger.error("Discovery feed %s returned zero records", feed_name)
    else:
        logger.info("Discovery feed %s produced %d records", feed_name, count)


async def _consume(
    queue: asyncio.Queue[FeedRecord | None],
    total_producers: int,
) -> AsyncIterator[FeedRecord]:
    """
    Consumes items from the queue, yielding FeedRecord objects as they arrive.

    Tracks the number of completed producers by counting None signals. Yields each
    FeedRecord until all producers have finished.

    Args:
        queue (asyncio.Queue[FeedRecord | None]): The queue containing FeedRecord
            instances and completion signals (None).
        total_producers (int): The total number of producer tasks to wait for.

    Returns:
        AsyncIterator[FeedRecord]: An asynchronous iterator yielding FeedRecord
            objects as they are dequeued.
    """
    completed = 0
    while completed < total_producers:
        item = await queue.get()
        if item is None:
            completed += 1
        else:
            yield item
