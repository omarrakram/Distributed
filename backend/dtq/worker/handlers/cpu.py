"""CPU-bound task handlers.

They run inside a separate, killable child process (see ``dtq.worker.executor``),
so a long computation never blocks the worker's event loop, can be terminated
on timeout or lease loss, and is memory-limited with RLIMIT_AS.
All handlers are deterministic: same payload -> same result.
"""

from __future__ import annotations

import base64
import hashlib
import io
import math
import random
from typing import Callable

from dtq.common.task_specs import (
    ImageProcessPayload, MatrixMultiplyPayload, PrimeCountPayload, Sha256Payload,
)
from dtq.worker.errors import ExecContext, TaskError

Progress = Callable[[float, str | None], None]


# ------------------------------------------------------------------- primes
def _base_primes(limit: int) -> list[int]:
    sieve = bytearray(b"\x01") * (limit + 1)
    sieve[0:2] = b"\x00\x00"
    for i in range(2, math.isqrt(limit) + 1):
        if sieve[i]:
            sieve[i * i :: i] = bytes(len(range(i * i, limit + 1, i)))
    return [i for i in range(limit + 1) if sieve[i]]


def prime_count(p: PrimeCountPayload, ctx: ExecContext, progress: Progress) -> dict:
    """Segmented sieve of Eratosthenes over [start, end)."""
    start, end = p.start, p.end
    lo = max(start, 2)
    base = _base_primes(math.isqrt(max(end - 1, 1)) + 1)
    count = 0
    first: list[int] = []
    last: list[int] = []
    segment = 1 << 18
    span = max(1, end - lo)
    while lo < end:
        hi = min(lo + segment, end)
        seg = bytearray(b"\x01") * (hi - lo)
        for q in base:
            qq = q * q
            if qq >= hi:
                break
            s = max(qq, ((lo + q - 1) // q) * q)
            if s < hi:
                seg[s - lo :: q] = bytes((hi - 1 - s) // q + 1)
        count += seg.count(1)
        if len(first) < 10:
            idx = seg.find(1)
            while idx != -1 and len(first) < 10:
                first.append(lo + idx)
                idx = seg.find(1, idx + 1)
        idx = seg.rfind(1)
        tail: list[int] = []
        while idx != -1 and len(tail) < 10:
            tail.append(lo + idx)
            idx = seg.rfind(1, 0, idx)
        if tail:
            last = (last + sorted(tail))[-10:]
        lo = hi
        progress(min(1.0, (hi - max(start, 2)) / span), f"sieved up to {hi:,}")
    return {
        "start": start, "end": end, "count": count,
        "first_primes": first, "last_primes": last, "largest": last[-1] if last else None,
    }


# ------------------------------------------------------------------- matrix
def matrix_multiply(p: MatrixMultiplyPayload, ctx: ExecContext, progress: Progress) -> dict:
    n, r = p.size, p.value_range
    rng = random.Random(p.seed)
    a = [[rng.randint(-r, r) for _ in range(n)] for _ in range(n)]
    b = [[rng.randint(-r, r) for _ in range(n)] for _ in range(n)]
    bt = list(zip(*b))
    digest = hashlib.sha256()
    checksum = 0
    trace = 0
    small = [] if n <= 8 else None
    step = max(1, n // 20)
    for i, row in enumerate(a):
        out_row = [sum(x * y for x, y in zip(row, col)) for col in bt]
        checksum += sum(out_row)
        trace += out_row[i]
        digest.update((",".join(map(str, out_row)) + ";").encode())
        if small is not None:
            small.append(out_row)
        if i % step == 0:
            progress((i + 1) / n, f"row {i + 1}/{n}")
    result = {"size": n, "seed": p.seed, "checksum": checksum, "trace": trace,
              "sha256": digest.hexdigest(), "multiply_adds": n ** 3}
    if small is not None:
        result["product"] = small
    return result


# -------------------------------------------------------------------- image
def _synthetic_image(p: ImageProcessPayload):
    from PIL import Image, ImageDraw

    w, h = p.width, p.height
    if p.pattern == "rings":
        g = Image.radial_gradient("L").resize((w, h))
        width = 6 + p.seed % 10
        rings = g.point(lambda v: 255 if (v // width) % 2 else 40)
        return Image.merge("RGB", (rings, g, rings.rotate(90).resize((w, h))))
    if p.pattern == "gradient":
        g = Image.linear_gradient("L").resize((w, h))
        return Image.merge("RGB", (g, g.rotate(90).resize((w, h)), g.transpose(Image.Transpose.FLIP_LEFT_RIGHT)))
    if p.pattern == "checker":
        rng = random.Random(p.seed)
        img = Image.new("RGB", (w, h))
        draw = ImageDraw.Draw(img)
        cell = 16 + p.seed % 32
        palette = [tuple(rng.randrange(256) for _ in range(3)) for _ in range(4)]
        for y in range(0, h, cell):
            for x in range(0, w, cell):
                draw.rectangle([x, y, x + cell - 1, y + cell - 1], fill=palette[((x // cell) + (y // cell)) % 4])
        return img
    rng = random.Random(p.seed)
    return Image.frombytes("RGB", (w, h), rng.randbytes(w * h * 3))


def image_process(p: ImageProcessPayload, ctx: ExecContext, progress: Progress) -> dict:
    from PIL import ImageFilter, ImageOps, ImageStat

    img = _synthetic_image(p)
    total = len(p.operations)
    for i, op in enumerate(p.operations):
        if op == "grayscale":
            img = ImageOps.grayscale(img)
        elif op == "blur":
            img = img.filter(ImageFilter.GaussianBlur(p.blur_radius))
        elif op == "sharpen":
            img = img.filter(ImageFilter.SHARPEN)
        elif op == "edge_detect":
            img = img.filter(ImageFilter.FIND_EDGES)
        elif op == "emboss":
            img = img.filter(ImageFilter.EMBOSS)
        elif op == "contour":
            img = img.filter(ImageFilter.CONTOUR)
        elif op == "invert":
            img = ImageOps.invert(img)
        elif op == "equalize":
            img = ImageOps.equalize(img)
        elif op == "threshold":
            t = p.threshold
            img = img.convert("L").point(lambda v, t=t: 255 if v >= t else 0)
        elif op == "rotate":
            img = img.rotate(p.rotate_degrees, expand=True)
        elif op == "resize":
            img = img.resize((max(1, min(2048, int(img.width * p.resize_factor))),
                              max(1, min(2048, int(img.height * p.resize_factor)))))
        else:  # unreachable: operations are validated against a whitelist
            raise TaskError(f"unsupported operation {op}", "PERMANENT", retryable=False)
        progress((i + 1) / total, f"applied {op}")
    stat = ImageStat.Stat(img)
    gray_hist = img.convert("L").histogram()
    bins = [sum(gray_hist[i : i + 32]) for i in range(0, 256, 32)]
    result = {
        "width": img.width, "height": img.height, "mode": img.mode, "operations": p.operations,
        "pattern": p.pattern, "mean": [round(m, 3) for m in stat.mean],
        "histogram_8_bins": bins, "sha256": hashlib.sha256(img.tobytes()).hexdigest(),
    }
    if p.return_thumbnail:
        thumb = img.convert("RGB").copy()
        thumb.thumbnail((96, 96))
        buf = io.BytesIO()
        thumb.save(buf, format="JPEG", quality=70)
        result["thumbnail"] = "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()
    return result


# ------------------------------------------------------------------- sha256
def sha256_chain(p: Sha256Payload, ctx: ExecContext, progress: Progress) -> dict:
    data = p.data.encode()
    digest = hashlib.sha256(data).digest()
    step = max(1, p.iterations // 20)
    for i in range(1, p.iterations):
        digest = hashlib.sha256(digest).digest()
        if i % step == 0:
            progress(i / p.iterations, f"{i:,}/{p.iterations:,} rounds")
    return {"iterations": p.iterations, "input_bytes": len(data), "digest": digest.hex()}


CPU_HANDLERS = {
    "prime_count": prime_count,
    "matrix_multiply": matrix_multiply,
    "image_process": image_process,
    "sha256": sha256_chain,
}
