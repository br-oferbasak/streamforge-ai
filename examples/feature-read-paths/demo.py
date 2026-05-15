"""
Feature Read Paths Demo
========================
Runs both read paths side-by-side and prints a latency / throughput comparison.

  Offline (Batch)   — reads a date range from MinIO using manifests or prefix scan.
                       Falls back to synthetic records when MinIO is unreachable.

  Online (Low-lat)  — single-key and batch lookups from Redis.
                       Falls back to an in-memory stub when Redis is unreachable.

No external services needed: both paths have synthetic/in-memory fallbacks.

Usage
-----
  python demo.py                 # runs with defaults
  python demo.py --users 100     # batch lookup for 100 users
"""

from __future__ import annotations

import argparse
import datetime
import statistics
import time
from typing import List

from offline_batch_reader import OfflineBatchReader, _synthetic_records
from online_lowlatency_reader import OnlineFeatureReader


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------

def _timeit(fn, iterations: int = 1):
    times = []
    result = None
    for _ in range(iterations):
        t0 = time.perf_counter()
        result = fn()
        times.append((time.perf_counter() - t0) * 1000)
    return result, times


def _print_row(label: str, ms_list: List[float], extra: str = ""):
    mn  = min(ms_list)
    med = statistics.median(ms_list)
    mx  = max(ms_list)
    print(f"  {label:<42}  min={mn:6.2f}ms  median={med:6.2f}ms  max={mx:6.2f}ms  {extra}")


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def demo(batch_user_count: int = 50, offline_warmup: bool = False) -> None:
    separator = "─" * 70

    print(separator)
    print("  StreamForge Feature Read Paths Demo")
    print(separator)

    # ── Offline batch reader ────────────────────────────────────────────────
    print("\n[1] Offline (Batch) Reader")
    batch_reader = OfflineBatchReader()
    print(f"    backend  : MinIO ({'connected' if batch_reader.is_live else 'unreachable → synthetic fallback'})")

    today     = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    if offline_warmup:
        # One warm-up read (not measured)
        batch_reader.read_date_range(yesterday, today)

    records, times = _timeit(lambda: batch_reader.read_date_range(yesterday, today), iterations=3)
    _print_row("read_date_range (yesterday → today)", times,
               extra=f"→ {len(records):,} records")

    if records:
        sample = records[0]
        print(f"    sample   : user_id={sample['user_id']}  "
              f"event_count={sample['event_count']}  "
              f"event_rate={sample['event_rate']:.3f}/s")

    # ── Online low-latency reader ───────────────────────────────────────────
    print("\n[2] Online (Low-latency) Reader")
    online_reader = OnlineFeatureReader()
    print(f"    backend  : {online_reader.backend}")
    print(f"    stats    : {online_reader.stats()}")

    # Single-key lookup
    _, times_single = _timeit(lambda: online_reader.get(42), iterations=20)
    feat = online_reader.get(42)
    _print_row("get(user_id=42)  — single lookup", times_single,
               extra=f"→ event_count={feat['event_count'] if feat else 'miss'}")

    # Batch lookup (pipelined)
    user_ids = list(range(1, batch_user_count + 1))
    _, times_batch = _timeit(lambda: online_reader.get_batch(user_ids), iterations=10)
    batch_result = online_reader.get_batch(user_ids)
    _print_row(f"get_batch({batch_user_count} users) — pipelined", times_batch,
               extra=f"→ {len(batch_result)}/{batch_user_count} hits")

    # History lookup
    _, times_hist = _timeit(lambda: online_reader.get_history(42, limit=48), iterations=10)
    history = online_reader.get_history(42, limit=48)
    _print_row("get_history(user_id=42, limit=48)", times_hist,
               extra=f"→ {len(history)} windows")

    # ── Summary ────────────────────────────────────────────────────────────
    print(f"\n{separator}")
    print("  Summary")
    print(separator)
    print(f"  Offline: {len(records):>7,} records  (full date-range scan, designed for batch)")
    print(f"  Online:  {len(batch_result):>7,} users    (sub-ms point lookups, designed for serving)")
    print()
    print("  When to use each path:")
    print("    Offline  → ML training, batch scoring, data-warehouse exports")
    print("    Online   → model serving, real-time dashboards, feature-on-demand APIs")
    print(separator)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Feature read paths demo")
    parser.add_argument("--users",  type=int, default=50,
                        help="Number of users for the batch lookup benchmark (default: 50)")
    parser.add_argument("--warmup", action="store_true",
                        help="Run one offline warm-up read before benchmarking")
    args = parser.parse_args()
    demo(batch_user_count=args.users, offline_warmup=args.warmup)
