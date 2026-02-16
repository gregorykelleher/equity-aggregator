# _utils/test_backoff.py

import pytest

from equity_aggregator.adapters.data_sources.enrichment_feeds.gleif._utils import (
    backoff_delays,
)

pytestmark = pytest.mark.unit


def test_backoff_generates_correct_number_of_delays() -> None:
    """
    ARRANGE: request 3 attempts
    ACT:     consume the iterator
    ASSERT:  yields exactly 3 delay values
    """
    expected = 3

    actual = list(backoff_delays(attempts=expected))

    assert len(actual) == expected


def test_backoff_first_delay_is_near_base() -> None:
    """
    ARRANGE: base of 1.0 with 10% jitter
    ACT:     take the first delay
    ASSERT:  first delay is within jitter bounds of base
    """
    base = 1.0
    jitter = 0.10
    lower_bound = base * (1 - jitter)

    actual = next(iter(backoff_delays(base=base, jitter=jitter)))

    assert actual >= lower_bound


def test_backoff_first_delay_does_not_exceed_upper_bound() -> None:
    """
    ARRANGE: base of 1.0 with 10% jitter
    ACT:     take the first delay
    ASSERT:  first delay does not exceed base * (1 + jitter)
    """
    base = 1.0
    jitter = 0.10
    upper_bound = base * (1 + jitter)

    actual = next(iter(backoff_delays(base=base, jitter=jitter)))

    assert actual <= upper_bound


def test_backoff_delays_do_not_exceed_cap() -> None:
    """
    ARRANGE: low cap with many attempts
    ACT:     consume all delays
    ASSERT:  no delay exceeds the cap
    """
    cap = 4.0

    actual = list(backoff_delays(base=1.0, cap=cap, attempts=10))

    assert all(d <= cap for d in actual)


def test_backoff_delays_are_non_negative() -> None:
    """
    ARRANGE: default parameters
    ACT:     consume all delays
    ASSERT:  all delays are non-negative
    """
    actual = list(backoff_delays())

    assert all(d >= 0.0 for d in actual)


def test_backoff_second_delay_is_roughly_double_first() -> None:
    """
    ARRANGE: zero jitter so delays are exact
    ACT:     take first two delays
    ASSERT:  second delay is double the first
    """
    expected_ratio = 2.0
    delays = list(backoff_delays(base=1.0, jitter=0.0, attempts=2))

    actual = delays[1] / delays[0]

    assert actual == expected_ratio
