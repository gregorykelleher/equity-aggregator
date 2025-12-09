# transforms/group.py

import logging
from collections.abc import AsyncIterable
from itertools import groupby

from equity_aggregator.schemas.raw import RawEquity

logger = logging.getLogger(__name__)


async def group(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterable[list[RawEquity]]:
    """
    Group equities by share_class_figi, preserving all source data points.

    Groups equities sharing the same share_class_figi identifier into lists,
    preserving all discovery feed sources. This allows the enrichment stage
    to compute median identifiers and perform a single merge of all sources
    (discovery + enrichment) for optimal data quality.

    Args:
        raw_equities: Async iterable of RawEquity records to group.

    Yields:
        list[RawEquity]: Groups of equities with the same share_class_figi.
    """
    aggregated_raw_equities = [raw_equity async for raw_equity in raw_equities]

    total = len(aggregated_raw_equities)
    unique = len({equity.share_class_figi for equity in aggregated_raw_equities})
    duplicates = total - unique

    logger.info(
        "Grouped %d raw equities with %d duplicates into %d unique groups",
        total,
        duplicates,
        unique,
    )

    aggregated_raw_equities.sort(key=lambda equity: equity.share_class_figi)

    for _, equity_group in groupby(
        aggregated_raw_equities,
        key=lambda equity: equity.share_class_figi,
    ):
        yield list(equity_group)
