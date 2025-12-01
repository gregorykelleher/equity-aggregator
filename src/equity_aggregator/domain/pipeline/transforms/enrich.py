# transforms/enrich.py

import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from typing import NamedTuple

from equity_aggregator.adapters import open_yfinance_feed
from equity_aggregator.domain._utils import get_usd_converter, merge
from equity_aggregator.schemas import YFinanceFeedData
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
)


async def enrich(
    raw_equities: AsyncIterable[RawEquity],
) -> AsyncIterable[RawEquity]:
    """
    Enrich a stream of RawEquity objects concurrently using enrichment feeds.

    Each RawEquity is scheduled for enrichment and yielded as soon as its
    enrichment completes. Enrichment is performed concurrently, respecting
    per-feed concurrency limits.

    Args:
        raw_equities: Async iterable stream of RawEquity objects to enrich.

    Yields:
        RawEquity: Each enriched RawEquity as soon as enrichment finishes.
    """
    async with _open_feeds(enrichment_feed_specs) as feeds:
        async for enriched in _process_stream(raw_equities, feeds):
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
    equities: AsyncIterable[RawEquity],
    feeds: tuple[EnrichmentFeed, ...],
) -> AsyncIterable[RawEquity]:
    """
    Schedule enrichment for each equity and yield results as they complete.

    Creates an enrichment task for each equity in the input stream, then
    yields enriched equities as their tasks complete (potentially out of
    original order).

    Args:
        equities: Stream of equities to enrich.
        feeds: Active feeds to use for enrichment.

    Yields:
        RawEquity: Enriched equities as they complete.
    """
    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(_enrich_equity(eq, feeds), name=eq.symbol)
            async for eq in equities
        ]
        for coro in asyncio.as_completed(tasks):
            yield await coro


async def _enrich_equity(
    source: RawEquity,
    feeds: tuple[EnrichmentFeed, ...],
) -> RawEquity:
    """
    Enrich a single equity from all enrichment feeds and merge results.

    If the source has no missing fields, returns it unchanged. Otherwise,
    fetches enrichment data from all feeds concurrently, merges the results,
    and fills in missing fields from the source.

    Args:
        source: The equity to enrich, possibly with missing fields.
        feeds: Active feeds to use for enrichment.

    Returns:
        RawEquity: The enriched equity with missing fields filled where possible.
    """
    if not _has_missing_fields(source):
        return source

    enriched_from_feeds = await asyncio.gather(
        *(_enrich_from_feed(source, feed) for feed in feeds),
    )
    return _replace_none_fields(source, merge(enriched_from_feeds))


def _has_missing_fields(equity: RawEquity) -> bool:
    """
    Check if any field in a RawEquity instance is missing (set to None).

    Args:
        equity: The RawEquity instance to check for missing fields.

    Returns:
        bool: True if any field is None, indicating a missing value; False
            otherwise.
    """
    return any(v is None for v in equity.model_dump().values())


async def _enrich_from_feed(
    source: RawEquity,
    feed: EnrichmentFeed,
) -> RawEquity:
    """
    Fetch, validate, and convert enrichment data from a single feed.

    Attempts to enrich the source equity using the provided feed. Returns
    the original source if any step fails.

    Args:
        source: The equity to enrich.
        feed: The active feed to use.

    Returns:
        RawEquity: The enriched equity in USD, or the original source if
            enrichment fails.
    """
    feed_name = _feed_name(feed.model)

    fetched = await _safe_fetch(source, feed.fetch, feed_name)
    if not fetched:
        return source

    validated = _validate(fetched, source, feed.model, feed_name)
    if validated is source:
        return source

    return await _to_usd(validated, source, feed_name)


def _replace_none_fields(
    source: RawEquity,
    enriched: RawEquity,
) -> RawEquity:
    """
    Fill missing fields in source with values from enriched.

    For each field, if source has a non-None value, it is kept. If source
    has None, the value from enriched is used, but only if it is not None.
    None values in enriched never overwrite any value in source.

    Args:
        source: The original RawEquity instance, possibly with missing fields.
        enriched: The RawEquity instance to use for filling missing fields.

    Returns:
        RawEquity: A new RawEquity instance with missing fields filled from
            enriched.
    """
    updates = {
        k: v
        for k, v in enriched.model_dump(exclude_none=True).items()
        if getattr(source, k) is None
    }
    return source.model_copy(update=updates)


async def _safe_fetch(
    source: RawEquity,
    fetch: FetchFunc,
    feed_name: str,
) -> dict[str, object] | None:
    """
    Safely fetch raw data for a RawEquity from an enrichment feed.

    Handles errors, returning None on failure. Logs all errors with
    appropriate context. Timeout is handled by the _rate_limited wrapper.

    Note:
        The CIK (Central Index Key) is intentionally omitted as an identifier
        for enrichment feeds, as it lacks broad support.

    Args:
        source: The RawEquity instance to fetch data for.
        fetch: The async fetch function for the enrichment feed (already
            wrapped with timeout protection via _rate_limited).
        feed_name: The name of the enrichment feed for logging context.

    Returns:
        dict[str, object] | None: The fetched data as a dictionary, or None if
            an exception occurs or the data is empty.
    """
    try:
        return await fetch(
            symbol=source.symbol,
            name=source.name,
            isin=source.isin,
            cusip=source.cusip,
        )

    except LookupError as e:
        _log_outcome(feed_name, source, e)

    except TimeoutError:
        logger.error(
            "Timed out fetching from %s for symbol=%s (isin=%s, cusip=%s). "
            "Request exceeded timeout waiting for response.",
            feed_name,
            source.symbol,
            source.isin or "<none>",
            source.cusip or "<none>",
        )

    except Exception as e:
        logger.error(
            "Error fetching from %s for symbol=%s: %s: %s",
            feed_name,
            source.symbol,
            type(e).__name__,
            e or "<empty>",
        )

    return None


def _validate(
    record: dict[str, object],
    source: RawEquity,
    model: type,
    feed_name: str,
) -> RawEquity:
    """
    Validate record against model schema and convert to RawEquity.

    Validates the fetched record using the feed's Pydantic model, then
    converts the validated data to a RawEquity instance. Returns the
    original source on validation failure.

    Args:
        record: The raw record to validate and coerce.
        source: The original RawEquity to return on failure.
        model: Pydantic model class for validating feed data.
        feed_name: The name of the feed for logging context.

    Returns:
        RawEquity: The validated RawEquity, or the original source if
            validation fails.
    """
    try:
        coerced = model.model_validate(record).model_dump()
        return RawEquity.model_validate(coerced)

    except Exception as e:
        summary = (
            f"invalid {', '.join(sorted(err['loc'][0] for err in e.errors()))}"
            if hasattr(e, "errors")
            else str(e)
        )

        _log_outcome(feed_name, source, summary)

        return source


async def _to_usd(
    validated: RawEquity,
    source: RawEquity,
    feed_name: str,
) -> RawEquity:
    """
    Convert a validated RawEquity instance to USD.

    Applies currency conversion using the global USD converter. Falls back
    to the original source on conversion failure.

    Args:
        validated: The RawEquity instance to convert to USD.
        source: The original RawEquity to return on conversion failure.
        feed_name: The name of the enrichment feed for logging context.

    Returns:
        RawEquity: The USD-converted RawEquity if successful, otherwise the
            original source RawEquity.
    """
    converter = await get_usd_converter()

    try:
        converted = converter(validated)

        if converted is None or converted.currency != "USD":
            raise ValueError(
                f"USD conversion failed: {converted.currency if converted else None}",
            )

        _log_outcome(feed_name, source, None)

        return converted

    except Exception as e:
        _log_outcome(feed_name, source, e)
        return source


def _log_outcome(
    feed_name: str,
    source: RawEquity,
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
        source: Equity instance with identifying fields.
        error: Error or context for failed retrieval. If None, logs success;
            otherwise logs failure with error details.
        level: Logging level (default: logging.DEBUG).
    """
    status = "SUCCESS" if error is None else "FAILURE"
    prefix = f"[{feed_name}:{source.symbol}]"

    msg = (
        f"{prefix:<24} {status}: {feed_name} feed for symbol={source.symbol}, "
        f"name={source.name} (isin={source.isin or '<none>'}, "
        f"cusip={source.cusip or '<none>'}, cik={source.cik or '<none>'}, "
        f"share_class_figi={source.share_class_figi or '<none>'})"
    )

    if error is not None:
        msg += f". {error}"

    logger.log(level, msg)
