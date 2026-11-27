"""Retry policy: bounded exponential backoff with jitter."""

import random

from dtq.broker.retry_policy import backoff_delay


# ------------------------------------------------------------------ retry policy
def test_backoff_grows_exponentially_and_is_capped():
    delays = [backoff_delay(n, 1.0, 30.0) for n in range(1, 8)]
    assert delays == [1, 2, 4, 8, 16, 30, 30]


def test_backoff_jitter_bounds():
    rng = random.Random(1)
    for n in range(1, 6):
        d = backoff_delay(n, 1.0, 30.0, jitter=0.1, rng=rng)
        base = min(30.0, 2 ** (n - 1))
        assert base * 0.9 <= d <= base * 1.1
