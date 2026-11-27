import hashlib
import random

import pytest

from dtq.common.task_specs import (DelayPayload, FailPermanentPayload, FailTransientPayload, ImageProcessPayload,
                                   MatrixMultiplyPayload, PrimeCountPayload, Sha256Payload)
from dtq.worker.errors import ExecContext, TaskError
from dtq.worker.handlers.cpu import image_process, matrix_multiply, prime_count, sha256_chain
from dtq.worker.handlers.io import delay, fail_permanent, fail_transient

CTX = ExecContext(task_id="t", attempt_no=1, worker_name="w")
noop = lambda *_: None  # noqa: E731


def naive_primes(lo, hi):
    return [n for n in range(max(lo, 2), hi) if all(n % d for d in range(2, int(n ** 0.5) + 1))]


@pytest.mark.parametrize("start,end", [(0, 10), (0, 1000), (2, 3), (997, 1010), (10_000, 12_345), (0, 300_000)])
def test_prime_count_matches_reference(start, end):
    ref = naive_primes(start, end) if end <= 20_000 else None
    out = prime_count(PrimeCountPayload(start=start, end=end), CTX, noop)
    if ref is not None:
        assert out["count"] == len(ref)
        assert out["first_primes"] == ref[:10]
        assert out["last_primes"] == ref[-10:]
    else:
        assert out["count"] == 25_997  # pi(300000), known value


def test_prime_count_known_value_one_million():
    assert prime_count(PrimeCountPayload(start=0, end=1_000_000), CTX, noop)["count"] == 78_498


def test_matrix_multiply_is_deterministic_and_correct():
    p = MatrixMultiplyPayload(size=6, seed=3, value_range=5)
    out1, out2 = matrix_multiply(p, CTX, noop), matrix_multiply(p, CTX, noop)
    assert out1 == out2
    rng = random.Random(3)
    a = [[rng.randint(-5, 5) for _ in range(6)] for _ in range(6)]
    b = [[rng.randint(-5, 5) for _ in range(6)] for _ in range(6)]
    c = [[sum(a[i][k] * b[k][j] for k in range(6)) for j in range(6)] for i in range(6)]
    assert out1["product"] == c
    assert out1["trace"] == sum(c[i][i] for i in range(6))
    assert out1["checksum"] == sum(map(sum, c))


def test_sha256_chain_matches_hashlib():
    out = sha256_chain(Sha256Payload(data="abc", iterations=3), CTX, noop)
    d = hashlib.sha256(b"abc").digest()
    d = hashlib.sha256(hashlib.sha256(d).digest()).digest()
    assert out["digest"] == d.hex()


def test_image_process_is_deterministic_with_thumbnail():
    p = ImageProcessPayload(width=128, height=96, operations=["grayscale", "blur", "edge_detect", "resize"])
    a, b = image_process(p, CTX, noop), image_process(p, CTX, noop)
    assert a["sha256"] == b["sha256"]
    assert (a["width"], a["height"]) == (64, 48)
    assert a["thumbnail"].startswith("data:image/jpeg;base64,")


async def test_delay_and_failure_handlers():
    assert (await delay(DelayPayload(seconds=0.05), CTX, noop))["slept_seconds"] >= 0.05
    p = FailTransientPayload(fail_times=2, seconds=0)
    for attempt in (1, 2):
        with pytest.raises(TaskError) as exc:
            await fail_transient(p, ExecContext("t", attempt, "w"), noop)
        assert exc.value.retryable and exc.value.error_type == "TRANSIENT"
    assert (await fail_transient(p, ExecContext("t", 3, "w"), noop))["succeeded_on_attempt"] == 3
    with pytest.raises(TaskError) as exc:
        await fail_permanent(FailPermanentPayload(retryable=False, seconds=0), CTX, noop)
    assert exc.value.retryable is False
