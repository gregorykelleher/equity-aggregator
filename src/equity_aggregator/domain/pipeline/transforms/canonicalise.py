# transforms/canonicalise.py

import logging
from collections.abc import AsyncIterable, AsyncIterator

from pydantic import ValidationError

from equity_aggregator.schemas.canonical import CanonicalEquity
from equity_aggregator.schemas.raw import RawEquity

logger = logging.getLogger(__name__)


async def canonicalise(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterator[CanonicalEquity]:
    """
    Asynchronously converts a stream of RawEquity objects into CanonicalEquity objects.

    Each RawEquity is validated and transformed into a CanonicalEquity. Records that
    fail validation are logged and skipped.

    Args:
        raw_equities (AsyncIterable[RawEquity]): An asynchronous iterable of RawEquity
            instances to be canonicalised.

    Yields:
        CanonicalEquity: The canonicalised equity object corresponding to each input.
    """
    canonicalised_count = 0
    skipped_count = 0

    async for raw_equity in raw_equities:
        try:
            yield CanonicalEquity.from_raw(raw_equity)
            canonicalised_count += 1
        except ValidationError:
            skipped_count += 1
            logger.warning(
                "Skipping equity %s (%s): failed canonical validation",
                raw_equity.symbol,
                raw_equity.share_class_figi,
            )

    logger.info(
        "Canonicalised %d equities (skipped %d).",
        canonicalised_count,
        skipped_count,
    )
