# pipeline/test_seed.py

import pytest

from equity_aggregator.domain.pipeline.seed import seed_canonical_equities

pytestmark = pytest.mark.unit


def test_seed_canonical_equities_completes_without_error() -> None:
    """
    ARRANGE: No setup required
    ACT:     Call seed_canonical_equities
    ASSERT:  Function completes and returns None
    """
    actual = seed_canonical_equities()

    assert actual is None
