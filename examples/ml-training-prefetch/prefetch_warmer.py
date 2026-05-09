"""
Prefetch Warmer — bridge between prefetch-engine/ and the ML training pipeline.

This module is the seam that connects two previously separate concerns:

  prefetch-engine/prefetch.py   ← hot-file selection + local caching logic
  feature_store.py              ← MinIO discovery + feature reading

Flow
----
  1. Receive a FileStat manifest from feature_store.list_minio_feature_files()
  2. Score and rank files with select_hot_files()
  3. Stage hot files into a local cache with prefetch_files()  (real MinIO or
     synthetic file copy depending on the URI scheme)
  4. Return WarmupResult with timing and hit-rate statistics

The caller (pipeline.py) waits for warm_cache() to complete before starting
the trainer, guaranteeing that training reads only from the local cache.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

# ── Locate prefetch-engine on sys.path ──────────────────────────────────────
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent.parent
_PE_PATH = str(_REPO_ROOT / "prefetch-engine")
if _PE_PATH not in sys.path:
    sys.path.insert(0, _PE_PATH)

from prefetch import select_hot_files  # type: ignore
import metrics as _pe_metrics           # type: ignore

# Re-export FileStat from feature_store so callers import from one place
from feature_store import FileStat, download_to_cache  # noqa: F401


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class WarmupConfig:
    """Tunable knobs for the prefetch warm phase."""

    top_n: int = 50
    """How many hot files to stage in the local cache."""

    cache_dir: Optional[Path] = None
    """
    Where to write the local cache.  Defaults to a temp directory created
    inside STREAMFORGE_DEMO_DIR (or /tmp/streamforge-demo).
    """

    simulate_latency_s: float = 0.0
    """
    Per-file simulated remote-read latency (seconds).
    Set > 0 to reproduce MinIO RTT in local / CI runs.
    Use 0.0 (default) when a real MinIO is present — actual I/O provides
    the latency.
    """

    use_real_download: bool = True
    """
    True  → download real MinIO objects via feature_store.download_to_cache()
    False → copy synthetic/local files with shutil (for CI / offline runs)
    """

    job_id: str = "ml-training"


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class WarmupResult:
    hot_files: List[FileStat]
    cache_dir: Path
    candidates_total: int
    files_warmed: int
    files_skipped: int
    prefetch_duration_s: float
    hit_rate: float          # fraction of hot_files actually in cache after warm

    def print_summary(self) -> None:
        print(f"[WARMER] candidates={self.candidates_total:,}  "
              f"selected={len(self.hot_files)}  "
              f"warmed={self.files_warmed}  "
              f"skipped={self.files_skipped}")
        print(f"[WARMER] prefetch took {self.prefetch_duration_s:.2f}s  "
              f"cache hit-rate after warm: {self.hit_rate:.1%}")
        print(f"[WARMER] cache dir: {self.cache_dir}")


# ---------------------------------------------------------------------------
# Core warm_cache()
# ---------------------------------------------------------------------------

def warm_cache(
    candidates: List[FileStat],
    config: WarmupConfig,
) -> WarmupResult:
    """
    Select the hottest files from ``candidates`` and stage them into a local
    cache directory before training starts.

    This is the primary integration point: calling this function guarantees
    that the training job will find its highest-priority feature files on
    local storage, not in remote MinIO.
    """
    cache_dir = _resolve_cache_dir(config.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # ── Phase 1: rank by access score ───────────────────────────────────────
    hot_files = select_hot_files(candidates, top_n=config.top_n)
    print(f"[WARMER] selected {len(hot_files)}/{len(candidates)} hot files for prefetch")

    # ── Phase 2: stage into cache ────────────────────────────────────────────
    t0 = time.perf_counter()
    warmed, skipped = _stage_files(hot_files, cache_dir, config)
    duration_s = time.perf_counter() - t0

    # ── Phase 3: measure actual hit-rate ────────────────────────────────────
    hits = sum(1 for f in hot_files if (cache_dir / _filename(f.uri)).exists())
    hit_rate = hits / len(hot_files) if hot_files else 0.0

    # Emit prefetch-engine Prometheus metrics (best-effort)
    try:
        _pe_metrics.prefetch_duration_seconds.labels(
            job_id=config.job_id
        ).observe(duration_s)
        _pe_metrics.files_prefetched_total.labels(
            job_id=config.job_id
        ).inc(warmed)
        _pe_metrics.cache_hits_total.labels(
            job_id=config.job_id
        ).inc(hits)
    except Exception:
        pass

    return WarmupResult(
        hot_files=hot_files,
        cache_dir=cache_dir,
        candidates_total=len(candidates),
        files_warmed=warmed,
        files_skipped=skipped,
        prefetch_duration_s=duration_s,
        hit_rate=hit_rate,
    )


# ---------------------------------------------------------------------------
# Staging strategies
# ---------------------------------------------------------------------------

def _stage_files(
    hot_files: List[FileStat],
    cache_dir: Path,
    config: WarmupConfig,
) -> Tuple[int, int]:
    """
    Copy hot files into cache_dir using whichever strategy fits the URI scheme.
    Returns (warmed_count, skipped_count).
    """
    minio_files = [f for f in hot_files if f.uri.startswith("minio://")]
    local_files  = [f for f in hot_files if not f.uri.startswith("minio://")]

    warmed = 0
    skipped = 0

    # Real MinIO objects
    if minio_files and config.use_real_download:
        downloaded = download_to_cache(minio_files, cache_dir)
        warmed += downloaded
        skipped += len(minio_files) - downloaded

    # Synthetic / local files — write placeholder content representing the
    # feature batch so trainer.py can read them from cache_dir
    for fs in local_files:
        dest = cache_dir / _filename(fs.uri)
        if dest.exists():
            warmed += 1
            continue
        if config.simulate_latency_s > 0:
            time.sleep(config.simulate_latency_s)
        try:
            _write_synthetic_feature_file(dest, fs)
            warmed += 1
        except OSError:
            skipped += 1

    return warmed, skipped


def _write_synthetic_feature_file(dest: Path, fs: FileStat) -> None:
    """
    Write a realistic synthetic feature NDJSON file.

    Simulates what the Flink feature-sink would write to MinIO so that
    trainer.py can parse it identically in online and offline modes.
    """
    import json
    import random
    rng = random.Random(hash(fs.uri) % (2**32))
    lines = []
    for _ in range(rng.randint(10, 50)):
        user_id = rng.randint(1, 200)
        event_count = max(1, int(rng.lognormvariate(2.0, 1.2)))
        ws = int(fs.last_access_epoch * 1000)
        lines.append(json.dumps({
            "user_id": user_id,
            "window_start": ws,
            "window_end": ws + 30_000,
            "event_count": event_count,
            "sink_received_at": "synthetic",
        }))
    dest.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_cache_dir(cfg_dir: Optional[Path]) -> Path:
    if cfg_dir is not None:
        return cfg_dir
    base = Path(os.environ.get("STREAMFORGE_DEMO_DIR", "/tmp/streamforge-demo"))
    return base / "prefetch-cache" / "ml-training"


def _filename(uri: str) -> str:
    """Flatten any URI into a safe local filename."""
    return (uri
            .replace("minio://", "")
            .replace("synthetic://", "")
            .replace("file://", "")
            .replace("/", "_"))
