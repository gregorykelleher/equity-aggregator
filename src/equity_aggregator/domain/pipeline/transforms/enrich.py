# transforms/enrich.py

import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from typing import NamedTuple

from equity_aggregator.adapters import open_intrinio_feed, open_yfinance_feed
from equity_aggregator.domain._utils import (
    EquityIdentifiers,
    extract_identifiers,
    get_usd_converter,
    merge,
)
from equity_aggregator.schemas import IntrinioFeedData, YFinanceFeedData
from equity_aggregator.schemas.raw import RawEquity

logger = logging.getLogger(__name__)

# Type alias for an async function that fetches enrichment data for an equity
type FetchFunc = Callable[..., Awaitable[dict[str, object]]]


class FeedSpec(NamedTuple):
    """
    Static specification for an enrichment feed.

    Attributes:
        factory: Async context manager factory that yields a feed instance.
        model: Pydantic model for validating feed data.
        limit: Maximum number of concurrent requests to this feed.
    """

    factory: Callable
    model: type
    limit: int


class EnrichmentFeed(NamedTuple):
    """
    Runtime instance of an enrichment feed with rate limiting applied.

    Attributes:
        fetch: Rate-limited async function to fetch enrichment data.
        model: Pydantic model for validating feed data.
    """

    fetch: FetchFunc
    model: type


# Specification for all enrichment feeds
enrichment_feed_specs: tuple[FeedSpec, ...] = (
    FeedSpec(open_yfinance_feed, YFinanceFeedData, 20),
    FeedSpec(open_intrinio_feed, IntrinioFeedData, 20),
)


async def enrich(
    equity_groups: AsyncIterable[list[RawEquity]],
) -> AsyncIterable[RawEquity]:
    """
    Enrich equity groups and merge all sources (discovery + enrichment).

    For each group of discovery feed equities, computes median identifiers,
    queries enrichment feeds, then performs a single merge of all sources
    for optimal data quality.

    Args:
        equity_groups: Stream of equity groups (discovery feed sources).

    Yields:
        RawEquity: Fully merged and enriched equities.
    """
    async with _open_feeds(enrichment_feed_specs) as feeds:
        async for enriched in _process_stream(equity_groups, feeds):
            yield enriched


@asynccontextmanager
async def _open_feeds(
    specs: tuple[FeedSpec, ...],
) -> AsyncIterator[tuple[EnrichmentFeed, ...]]:
    """
    Open and initialise all enrichment feeds with lifecycle management.

    Creates an async context that initialises each feed with rate-limited fetch
    functions, manages their lifecycle through AsyncExitStack, and logs completion
    when the context exits. All feeds are initialised sequentially to ensure
    proper resource allocation.

    Args:
        specs: Tuple of feed specifications to initialise.

    Yields:
        tuple[EnrichmentFeed, ...]: Initialised feeds with rate limiting applied,
            ready for concurrent enrichment operations.
    """
    async with AsyncExitStack() as stack:
        feeds = tuple([await _init_feed(spec, stack) for spec in specs])
        yield feeds

    logger.info(
        "Enrichment finished using feeds: %s",
        ", ".join(_feed_name(f.model) for f in feeds),
    )


async def _init_feed(spec: FeedSpec, stack: AsyncExitStack) -> EnrichmentFeed:
    """
    Initialise a single enrichment feed with rate limiting and lifecycle management.

    Opens the feed using its factory, registers it with the provided AsyncExitStack
    for automatic cleanup, wraps the fetch function with semaphore-based rate
    limiting, and returns a ready-to-use EnrichmentFeed instance.

    Args:
        spec: Feed specification containing factory, model, and concurrency limit.
        stack: AsyncExitStack to manage the feed's async context lifecycle.

    Returns:
        EnrichmentFeed: Initialised feed with rate-limited fetch function and
            validation model.
    """
    feed_instance = await stack.enter_async_context(spec.factory())

    return EnrichmentFeed(
        fetch=_rate_limited(feed_instance.fetch_equity, asyncio.Semaphore(spec.limit)),
        model=spec.model,
    )


def _rate_limited(
    fn: FetchFunc,
    semaphore: asyncio.Semaphore,
    *,
    timeout: float = 300.0,
) -> FetchFunc:
    """
    Wrap an async fetch function with semaphore-based rate limiting and timeout.

    The timeout applies only to the actual fetch operation, not the semaphore
    wait time. This ensures tasks waiting in the queue don't timeout before
    they get their turn to execute.

    Args:
        fn: Async function to wrap.
        semaphore: Semaphore to control concurrent calls.
        timeout: Maximum time in seconds for the fetch operation (default: 300s).

    Returns:
        FetchFunc: Wrapped function that acquires semaphore before calling fn
            with timeout protection.
    """

    async def wrapper(*args: object, **kwargs: object) -> object:
        async with semaphore:
            return await asyncio.wait_for(fn(*args, **kwargs), timeout=timeout)

    return wrapper


def _feed_name(model: type) -> str:
    """
    Extract a concise feed name from a model class.

    Args:
        model: The Pydantic model class (e.g., YFinanceFeedData).

    Returns:
        str: The feed name (e.g., "YFinance").
    """
    return model.__name__.removesuffix("FeedData")


async def _process_stream(
    equity_groups: AsyncIterable[list[RawEquity]],
    feeds: tuple[EnrichmentFeed, ...],
) -> AsyncIterable[RawEquity]:
    """
    Schedule enrichment for each equity group and yield merged results.

    Creates an enrichment task for each group in the input stream, then
    yields enriched equities as their tasks complete (potentially out of
    original order).

    Args:
        equity_groups: Stream of equity groups to enrich.
        feeds: Active feeds to use for enrichment.

    Yields:
        RawEquity: Merged equities from all sources (discovery + enrichment).
    """
    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(_enrich_equity_group(group, feeds))
            async for group in equity_groups
        ]
        for coro in asyncio.as_completed(tasks):
            yield await coro


async def _enrich_equity_group(
    discovery_sources: list[RawEquity],
    feeds: tuple[EnrichmentFeed, ...],
) -> RawEquity:
    """
    Enrich an equity group and merge all sources.

    Extracts representative identifiers from discovery sources, queries
    enrichment feeds with those identifiers, then performs a single merge
    of all data points (discovery + enrichment).

    Args:
        discovery_sources: Discovery feed equities for this group.
        feeds: Active enrichment feeds.

    Returns:
        RawEquity: Merged equity from all available sources.
    """
    # Extract representative identifiers for enrichment queries
    identifiers = extract_identifiers(discovery_sources)

    # Fetch enrichment data using identifiers
    enrichment_results = await asyncio.gather(
        *(_enrich_from_feed(identifiers, feed) for feed in feeds),
    )

    # Filter out None results (failed enrichment attempts)
    enrichment_sources = [r for r in enrichment_results if r is not None]

    # Single merge of all sources: discovery + enrichment
    all_sources = discovery_sources + enrichment_sources
    return merge(all_sources)


async def _enrich_from_feed(
    identifiers: EquityIdentifiers,
    feed: EnrichmentFeed,
) -> RawEquity | None:
    """
    Fetch, validate, and convert enrichment data from a single feed.

    Uses representative identifiers to query the feed, validates the response,
    converts to USD, and returns the enriched equity. Returns None on
    any failure.

    Args:
        identifiers: Representative identifiers for querying the feed.
        feed: Active feed to use.

    Returns:
        RawEquity | None: Enriched equity in USD, or None if enrichment fails.
    """
    feed_name = _feed_name(feed.model)

    fetched = await _safe_fetch(identifiers, feed.fetch, feed_name)
    if not fetched:
        return None

    validated = _validate(fetched, feed.model, feed_name, identifiers)
    if validated is None:
        return None

    return await _to_usd(validated, feed_name, identifiers)


async def _safe_fetch(
    identifiers: EquityIdentifiers,
    fetch: FetchFunc,
    feed_name: str,
) -> dict[str, object] | None:
    """
    Safely fetch raw data using identifiers from an enrichment feed.

    Handles errors, returning None on failure. Logs all errors with
    appropriate context. Timeout is handled by the _rate_limited wrapper.

    Note:
        The CIK (Central Index Key) is intentionally omitted as an identifier
        for enrichment feeds, as it lacks broad support.

    Args:
        identifiers: Representative identifiers for the equity.
        fetch: Async fetch function for the enrichment feed (already
            wrapped with timeout protection via _rate_limited).
        feed_name: Feed name for logging context.

    Returns:
        dict[str, object] | None: Fetched data as dictionary, or None on failure.
    """
    try:
        return await fetch(
            symbol=identifiers.symbol,
            name=identifiers.name,
            isin=identifiers.isin,
            cusip=identifiers.cusip,
            share_class_figi=identifiers.share_class_figi,
        )

    except LookupError as e:
        _log_outcome(feed_name, identifiers, e)

    except TimeoutError:
        logger.error(
            "Timed out fetching from %s for symbol=%s (isin=%s, cusip=%s). "
            "Request exceeded timeout waiting for response.",
            feed_name,
            identifiers.symbol,
            identifiers.isin or "<none>",
            identifiers.cusip or "<none>",
        )

    except Exception as e:
        logger.error(
            "Error fetching from %s for symbol=%s: %s: %s",
            feed_name,
            identifiers.symbol,
            type(e).__name__,
            e or "<empty>",
        )

    return None


def _validate(
    record: dict[str, object],
    model: type,
    feed_name: str,
    identifiers: EquityIdentifiers,
) -> RawEquity | None:
    """
    Validate record against model schema and convert to RawEquity.

    Validates the fetched record using the feed's Pydantic model, then
    converts the validated data to a RawEquity instance. Only injects
    share_class_figi from discovery sources if the enrichment feed didn't
    provide one. Returns None on validation failure.

    Args:
        record: Raw record to validate.
        model: Pydantic model class for validating feed data.
        feed_name: Feed name for logging context.
        identifiers: Representative ids for logging context and share_class_figi.

    Returns:
        RawEquity | None: Validated equity, with share_class_figi from discovery
            sources if enrichment feed didn't provide one, or None on failure.
    """
    try:
        coerced = model.model_validate(record).model_dump()

        # Only inject share_class_figi if enrichment feed didn't provide one
        if "share_class_figi" not in coerced or coerced["share_class_figi"] is None:
            coerced["share_class_figi"] = identifiers.share_class_figi

        return RawEquity.model_validate(coerced)

    except Exception as e:
        summary = (
            f"invalid {', '.join(sorted(err['loc'][0] for err in e.errors()))}"
            if hasattr(e, "errors")
            else str(e)
        )
        _log_outcome(feed_name, identifiers, summary)
        return None


async def _to_usd(
    validated: RawEquity,
    feed_name: str,
    identifiers: EquityIdentifiers,
) -> RawEquity | None:
    """
    Convert a validated RawEquity instance to USD.

    Applies currency conversion using the global USD converter. Returns
    None on conversion failure.

    Args:
        validated: RawEquity instance to convert to USD.
        feed_name: Feed name for logging context.
        identifiers: Representative identifiers for logging context.

    Returns:
        RawEquity | None: USD-converted equity or None on failure.
    """
    converter = await get_usd_converter()

    try:
        converted = converter(validated)

        if converted is None or converted.currency != "USD":
            raise ValueError(
                f"USD conversion failed: {converted.currency if converted else None}",
            )

        _log_outcome(feed_name, identifiers, None)
        return converted

    except Exception as e:
        _log_outcome(feed_name, identifiers, e)
        return None


def _log_outcome(
    feed_name: str,
    identifiers: EquityIdentifiers,
    error: object | None,
    *,
    level: int = logging.DEBUG,
) -> None:
    """
    Log a standardised message for enrichment feed data retrieval outcomes.

    Logs SUCCESS when data is retrieved successfully (error is None), or
    FAILURE when enrichment fails with error details.

    Args:
        feed_name: Name of the enrichment feed.
        identifiers: Representative identifiers for logging context.
        error: Error or context for failed retrieval. If None, logs success;
            otherwise logs failure with error details.
        level: Logging level (default: logging.DEBUG).
    """
    status = "SUCCESS" if error is None else "FAILURE"
    symbol = identifiers.symbol
    prefix = f"[{feed_name}:{symbol}]"

    msg = (
        f"{prefix:<24} {status}: {feed_name} feed for symbol={symbol}, "
        f"name={identifiers.name} "
        f"(share_class_figi={identifiers.share_class_figi or '<none>'}, "
        f"isin={identifiers.isin or '<none>'}, "
        f"cusip={identifiers.cusip or '<none>'}, "
        f"cik={identifiers.cik or '<none>'})"
    )

    if error is not None:
        msg += f". {error}"

    logger.log(level, msg)
