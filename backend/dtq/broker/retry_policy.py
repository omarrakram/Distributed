"""Retry policy: bounded exponential backoff with optional jitter.

delay(n) = min(max_s, base_s * 2**(n-1)) * (1 + U(-jitter, +jitter))

``n`` is the number of attempts already made. Jitter de-synchronises retries
of many tasks that failed together (e.g. after one worker crash), avoiding a
thundering herd (Brooker, "Exponential Backoff And Jitter", AWS 2015).
"""

from __future__ import annotations

import random


def backoff_delay(attempts_made: int, base_s: float, max_s: float, jitter: float = 0.0,
                  rng: random.Random | None = None) -> float:
    if attempts_made < 1:
        attempts_made = 1
    raw = min(max_s, base_s * (2 ** (attempts_made - 1)))
    if jitter > 0:
        r = (rng or random).uniform(-jitter, jitter)
        raw *= 1.0 + r
    return max(0.0, raw)
