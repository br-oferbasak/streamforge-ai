#!/usr/bin/env python3
"""
StreamForge AI — Prefetch Cache Hit-Ratio vs Cold-Start Time Benchmark

Isolates and quantifies the relationship between the prefetch-engine's
cache hit-ratio and the ML job cold-start time, sweeping across:

  - Hit-ratio   : 0 %, 25 %, 50 %, 75 %, 100 %
  - Dataset size: small (10 files), medium (50 files), large (200 files)

Unlike the broader prefetch/benchmark.py (which compares prefetch vs.
no-prefetch across fixed scenarios), this benchmark answers the specific
question: "how much does each additional 25 pp of cache coverage reduce
cold-start latency?"

Metrics reported per (dataset-size, hit-ratio) cell
----------------------------------------------------
  cold_start_ms   Total job startup time (time to first processed record)
  ttfb_ms         Time-to-first-batch  (first window of records ready)
  hit_ratio       Actual cache hit rate (may differ from target)
  miss_penalty_ms Average per-miss latency cost (remote I/O penalty)
  speedup_x       Cold-start speedup vs. 0 % hit-ratio baseline

Reproducibility
---------------
File content and hot/cold selection are seeded; latency constants mirror
the values used in prefetch-engine/benchmark.py so results are directly
comparable across benchmark runs.
"""

import os
import random
import shutil
import statistics
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# Latency constants (matching prefetch-engine/benchmark.py)
REMOTE_LATENCY_S = 0.050   # 50ms  — simulated MinIO/S3 round-trip
LOCAL_LATENCY_S  = 0.002   # 2ms   — simulated local cache/SSD read
COLD_START_OVERHEAD_S = 0.010  # 10ms — framework init cost (JVM/Python runtime)


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

@dataclass
class _FileMeta:
    path: Path
    size_bytes: int
    is_hot: bool


def _create_dataset(base: Path, n_files: int, size_bytes: int,
                    hot_ratio: float, seed: int = RANDOM_SEED) -> List[_FileMeta]:
    base.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    files = []
    for i in range(n_files):
        p = base / f"part_{i:05d}.bin"
        p.write_bytes(bytes(rng.getrandbits(8) for _ in range(size_bytes)))
        files.append(_FileMeta(path=p, size_bytes=size_bytes,
                               is_hot=i < int(n_files * hot_ratio)))
    return files


def _prefetch_hot(files: List[_FileMeta], cache_dir: Path) -> float:
    """Copy hot files to cache_dir; return elapsed seconds."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    for f in files:
        if f.is_hot:
            time.sleep(REMOTE_LATENCY_S)          # simulate remote read
            dest = cache_dir / f.path.name
            shutil.copy2(f.path, dest)            # write to local cache
    return time.perf_counter() - t0


def _simulate_job_run(files: List[_FileMeta], cache_dir: Path) -> Tuple[float, float, int, int]:
    """
    Simulate a streaming ML job reading all files.
    Returns: (cold_start_s, ttfb_s, hits, misses)
    cold_start_s = time until first record processed
    ttfb_s       = time until first full batch (all files in first window)
    """
    hits = 0
    misses = 0
    t_start = time.perf_counter()
    first_record_t: Optional[float] = None

    # Simulate framework startup overhead
    time.sleep(COLD_START_OVERHEAD_S)

    for i, f in enumerate(files):
        cached = (cache_dir / f.path.name).exists()
        if cached:
            time.sleep(LOCAL_LATENCY_S)
            hits += 1
        else:
            time.sleep(REMOTE_LATENCY_S + LOCAL_LATENCY_S)
            misses += 1

        now = time.perf_counter()
        if first_record_t is None:
            first_record_t = now  # time to first record

    total_s = time.perf_counter() - t_start
    ttfb_s = first_record_t - t_start if first_record_t else total_s
    return total_s, ttfb_s, hits, misses


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

HIT_RATIOS = [0.0, 0.25, 0.50, 0.75, 1.0]

@dataclass
class DatasetConfig:
    label: str
    n_files: int
    file_size_bytes: int
    repeat: int = 3


@dataclass
class HitRatioResult:
    dataset_label: str
    hit_ratio_target: float
    hit_ratio_actual: float
    cold_start_ms: float
    ttfb_ms: float
    miss_penalty_ms: float   # average latency cost per cache miss
    speedup_x: float         # vs. 0% hit-ratio baseline for same dataset
    prefetch_overhead_ms: float  # time spent in prefetch phase


DATASETS: List[DatasetConfig] = [
    DatasetConfig("Small  (10 files, 50 KB)", n_files=10,  file_size_bytes=51_200),
    DatasetConfig("Medium (50 files, 50 KB)", n_files=50,  file_size_bytes=51_200),
    DatasetConfig("Large  (200 files, 50 KB)", n_files=200, file_size_bytes=51_200),
]


# ---------------------------------------------------------------------------
# Benchmark execution
# ---------------------------------------------------------------------------

def run_dataset_sweep(cfg: DatasetConfig,
                      tmp: Path) -> Dict[float, HitRatioResult]:
    """Run all hit-ratio points for one dataset config."""
    results: Dict[float, HitRatioResult] = {}
    baseline_ms: Optional[float] = None

    for hr in HIT_RATIOS:
        src_dir   = tmp / f"{cfg.label.replace(' ', '_')}_src"
        cache_dir = tmp / f"{cfg.label.replace(' ', '_')}_cache_{int(hr*100)}"

        # Rebuild source files (hot flag changes per hr)
        if src_dir.exists():
            shutil.rmtree(src_dir)
        files = _create_dataset(src_dir, cfg.n_files, cfg.file_size_bytes,
                                hot_ratio=hr)

        all_cold_start: List[float] = []
        all_ttfb: List[float] = []
        all_hits: List[int] = []
        all_misses: List[int] = []
        all_prefetch: List[float] = []

        for _ in range(cfg.repeat):
            # Clear cache between runs
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
            prefetch_s = _prefetch_hot(files, cache_dir)
            cs_s, ttfb_s, hits, misses = _simulate_job_run(files, cache_dir)

            all_cold_start.append(cs_s)
            all_ttfb.append(ttfb_s)
            all_hits.append(hits)
            all_misses.append(misses)
            all_prefetch.append(prefetch_s)

        avg_cs_ms = statistics.mean(all_cold_start) * 1000
        avg_ttfb_ms = statistics.mean(all_ttfb) * 1000
        avg_hits = statistics.mean(all_hits)
        avg_misses = statistics.mean(all_misses)
        avg_prefetch_ms = statistics.mean(all_prefetch) * 1000

        actual_hr = avg_hits / cfg.n_files if cfg.n_files > 0 else 0.0
        miss_penalty = (REMOTE_LATENCY_S * 1000) if avg_misses > 0 else 0.0

        if baseline_ms is None:
            baseline_ms = avg_cs_ms  # 0% hit-ratio is the baseline

        speedup = baseline_ms / avg_cs_ms if avg_cs_ms > 0 else 1.0

        results[hr] = HitRatioResult(
            dataset_label=cfg.label,
            hit_ratio_target=hr,
            hit_ratio_actual=actual_hr,
            cold_start_ms=avg_cs_ms,
            ttfb_ms=avg_ttfb_ms,
            miss_penalty_ms=miss_penalty,
            speedup_x=speedup,
            prefetch_overhead_ms=avg_prefetch_ms,
        )

    return results


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _print_results(all_results: Dict[str, Dict[float, HitRatioResult]]) -> None:
    w = 105
    print("\n" + "=" * w)
    print("      StreamForge AI: Prefetch Cache Hit-Ratio vs Cold-Start Time")
    print("=" * w)

    for dataset_label, hr_map in all_results.items():
        print(f"\n  Dataset: {dataset_label}")
        print(f"  {'Hit Ratio':>10} {'Cold-start ms':>15} {'TTFB ms':>10} "
              f"{'Speedup':>9} {'Prefetch ms':>13} {'Miss penalty ms':>17}")
        print("  " + "-" * (w - 2))
        for hr in HIT_RATIOS:
            r = hr_map[hr]
            baseline_marker = "  ← baseline" if hr == 0.0 else ""
            print(
                f"  {r.hit_ratio_actual:>9.0%}"
                f" {r.cold_start_ms:>14.1f}"
                f" {r.ttfb_ms:>10.1f}"
                f" {r.speedup_x:>8.2f}×"
                f" {r.prefetch_overhead_ms:>13.1f}"
                f" {r.miss_penalty_ms:>16.1f}"
                f"{baseline_marker}"
            )

    print("\n" + "=" * w)
    print("  Hit Ratio    = actual fraction of files served from local cache")
    print("  Cold-start   = total time (ms) from job start to all files processed")
    print("  TTFB         = time-to-first-batch: latency to first record read")
    print("  Speedup      = cold-start improvement vs 0% hit-ratio (same dataset)")
    print("  Prefetch ms  = time spent copying hot files into local cache")
    print("  Miss penalty = simulated remote I/O penalty per cache miss (ms)")
    print()

    # Cross-dataset summary: speedup at 100% hit-ratio
    print("  100% hit-ratio cold-start speedup by dataset:")
    for dataset_label, hr_map in all_results.items():
        r100 = hr_map[1.0]
        r0   = hr_map[0.0]
        reduction_pct = (r0.cold_start_ms - r100.cold_start_ms) / r0.cold_start_ms * 100
        print(f"    {dataset_label:<30}  {r100.speedup_x:.2f}×  "
              f"({reduction_pct:.1f}% cold-start reduction)")
    print("=" * w)


def run_benchmark() -> Dict[str, Dict[float, HitRatioResult]]:
    print("=" * 70)
    print("      StreamForge AI: Prefetch Cache Hit-Ratio vs Cold-Start")
    print("=" * 70)
    print(f"  Remote latency : {REMOTE_LATENCY_S*1000:.0f} ms/file")
    print(f"  Local latency  : {LOCAL_LATENCY_S*1000:.0f} ms/file")
    print(f"  Startup overhead: {COLD_START_OVERHEAD_S*1000:.0f} ms")
    print(f"  Hit ratios     : {[f'{h:.0%}' for h in HIT_RATIOS]}")
    print(f"  Datasets       : {len(DATASETS)}\n")

    all_results: Dict[str, Dict[float, HitRatioResult]] = {}

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        for cfg in DATASETS:
            print(f"  Dataset: {cfg.label}")
            for hr in HIT_RATIOS:
                print(f"    hit-ratio={hr:.0%} ...", end=" ", flush=True)
                # We need to run one at a time; run_dataset_sweep handles all hr for this dataset
            # Run the full sweep for this dataset
            hr_map = run_dataset_sweep(cfg, tmp)
            all_results[cfg.label] = hr_map
            r0   = hr_map[0.0]
            r100 = hr_map[1.0]
            speedup = r100.speedup_x
            print(f"\r    Completed — 0%→100% hit-ratio speedup: {speedup:.2f}×"
                  f"  (cold-start: {r0.cold_start_ms:.0f}ms → {r100.cold_start_ms:.0f}ms)")

    _print_results(all_results)
    return all_results


if __name__ == "__main__":
    run_benchmark()
