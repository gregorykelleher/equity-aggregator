# _utils/backoff.py


import random
from collections.abc import Iterator


def backoff_delays(
    *,
    base: float = 1.0,
    cap: float = 64.0,
    jitter: float = 0.10,
    attempts: int = 5,
) -> Iterator[float]:
    """
    Yield an exponential backoff sequence with bounded jitter for retry delays.

    Each delay doubles from the base up to the cap, with jitter applied as a
    fraction of the current delay.

    Returns:
        Iterator[float]: Sequence of delay values in seconds.
    """
    delay: float = base
    for _ in range(attempts):
        delta: float = delay * jitter * (2 * random.random() - 1)
        yield max(0.0, min(delay + delta, cap))
        delay = min(delay * 2, cap)
