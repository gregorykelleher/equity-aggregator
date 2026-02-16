# gleif/debug.py
#
# Temporary debug script for live-testing GLEIF enrichment.
# Run with: uv run python -m equity_aggregator.adapters.data_sources.enrichment_feeds.gleif.debug

import asyncio
import logging

import httpx

from equity_aggregator.adapters.data_sources._utils import make_client

from ._utils import rank_candidates, select_best_parent
from .api import fetch_parents, search_by_name

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

_DIVIDER = "-" * 60

_TEST_EQUITIES = [
    {"symbol": "AIBRF", "name": "PRADA SPA", "isin": ""},
]


async def _debug_equity(
    client: httpx.AsyncClient,
    equity: dict[str, str],
) -> None:
    name = equity["name"]
    logger.info(_DIVIDER)
    logger.info("EQUITY: %s (%s)", name, equity["symbol"])
    logger.info(_DIVIDER)

    logger.info("Step 1: search_by_name(%r)", name)
    candidates = await search_by_name(name, client)
    logger.info("  candidates: %d found", len(candidates))
    for legal_name, lei in candidates:
        logger.info("    - %s  [%s]", legal_name, lei)

    ranked = rank_candidates(name, candidates)
    logger.info("Step 2: rank_candidates → %d above cutoff", len(ranked))
    for legal_name, lei in ranked:
        logger.info("    - %s  [%s]", legal_name, lei)

    if not ranked:
        logger.warning("  No candidates above cutoff — skipping.")
        return

    _, best_lei = ranked[0]
    logger.info("Step 3: fetch_parents(%s)", best_lei)
    parents = await fetch_parents(best_lei, client)
    logger.info("  parents: %d found", len(parents))
    for parent_name, parent_lei in parents:
        logger.info("    - %s  [%s]", parent_name, parent_lei)

    parent_lei = select_best_parent(name, parents)
    resolved_lei = parent_lei if parent_lei is not None else best_lei

    logger.info("Step 4: select_best_parent → %s", parent_lei)
    logger.info("RESULT: %s → LEI %s", name, resolved_lei)


async def main() -> None:
    logger.info("Starting GLEIF debug run")
    async with make_client() as client:
        for equity in _TEST_EQUITIES:
            await _debug_equity(client, equity)
            await asyncio.sleep(1.5)

    logger.info(_DIVIDER)
    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
