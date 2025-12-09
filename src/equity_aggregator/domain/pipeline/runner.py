# pipeline/runner.py

import logging

from equity_aggregator.domain.pipeline.resolve import resolve
from equity_aggregator.schemas import CanonicalEquity

from .transforms import canonicalise, convert, enrich, group, identify, parse

logger = logging.getLogger(__name__)


async def aggregate_canonical_equities() -> list[CanonicalEquity]:
    """
    Aggregates and processes raw equity data from discovery feeds, returning
    a list of unique, canonical equities.

    The pipeline applies the following transforms in order:
      - parse: Parse raw equity data.
      - convert: Convert prices to reference currency (USD).
      - identify: Attach identification metadata.
      - group: Group equities by share_class_figi.
      - enrich: Add supplementary data.
      - canonicalise: Convert to canonical equity format.

    Args:
        None

    Returns:
        list[RawEquity]: Unique, fully enriched canonical equities.
    """
    # resolve the stream of raw equities
    stream = resolve()

    # arrange the pipeline stages
    transforms = (
        parse,
        convert,
        identify,
        group,
        enrich,
        canonicalise,
    )

    # pipe stream through each transform sequentially
    for stage in transforms:
        stream = stage(stream)

    # materialise and return the stream
    return [equity async for equity in stream]
