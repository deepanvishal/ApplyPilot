"""Benchmark inference latency of the segment classifier."""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from applypilot.database import get_connection
from transformers import pipeline

MODEL_PATH = Path(__file__).parent.parent / "models" / "segment_classifier"

clf = pipeline(
    "text-classification",
    model=str(MODEL_PATH),
    tokenizer=str(MODEL_PATH),
    device=0,
    truncation=True,
    max_length=512,
)

conn = get_connection()
rows = conn.execute(
    "SELECT full_description FROM jobs WHERE full_description IS NOT NULL LIMIT 1000"
).fetchall()
texts = [r[0] for r in rows]
print(f"Loaded {len(texts)} samples\n")

# Single
start = time.perf_counter()
clf(texts[0])
single_ms = (time.perf_counter() - start) * 1000
print(f"Single inference:      {single_ms:.1f} ms")

# Warmup
clf(texts[:10])

# Batch sizes
for batch_size in [1, 8, 32, 64, 128]:
    subset = texts[:batch_size]
    times = []
    for _ in range(5):
        start = time.perf_counter()
        clf(subset, batch_size=batch_size)
        times.append((time.perf_counter() - start) * 1000)
    avg = sum(times) / len(times)
    per_sample = avg / batch_size
    print(f"Batch {batch_size:<4}  total={avg:6.1f}ms  per_sample={per_sample:.2f}ms  throughput={1000/per_sample:.0f}/s")

# Full 1000
start = time.perf_counter()
clf(texts, batch_size=64)
elapsed = time.perf_counter() - start
print(f"\n1000 samples @ batch=64: {elapsed:.2f}s  ({1000/elapsed:.0f} samples/sec)")
