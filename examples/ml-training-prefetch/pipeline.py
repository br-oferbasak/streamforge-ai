#!/usr/bin/env python3
"""
StreamForge AI — ML Training Pipeline with Prefetch Cache Warm-up

End-to-end example that wires together the CDC → Flink → MinIO/Iceberg
feature pipeline with the prefetch-engine and a sample ML training job.

Pipeline stages
---------------
  1. [DISCOVER]  List feature files written by the Flink feature-sink to
                 MinIO (bucket: processed, prefix: streamforge/features).
                 Falls back to synthetic data when MinIO is unreachable.

  2. [WARM]      Score and rank feature files by access frequency, then
                 stage the hottest N files into a local cache directory
                 using prefetch-engine/prefetch.py before training starts.

  3. [READ]      Read FeatureRecord objects from the local cache (or
                 from the Iceberg table when pyiceberg + catalog are
                 configured via ICEBERG_CATALOG_URI).

  4. [TRAIN]     Build a labelled dataset, train a user-activity classifier
                 (scikit-learn RandomForest or pure-Python logistic
                 regression), evaluate on a held-out test split.

  5. [UPLOAD]    Persist the trained model artifact to MinIO at
                 models/streamforge/ml/<job_id>/model.pkl.

  6. [REPORT]    Print a structured timing and accuracy report.

Usage
-----
  # Standalone — no external services needed:
  python examples/ml-training-prefetch/pipeline.py

  # Against the running demo stack (deploy/cdc-flink-minio-demo):
  MINIO_ENDPOINT=localhost:9000 \\
  MINIO_ACCESS_KEY=minioadmin   \\
  MINIO_SECRET_KEY=minioadmin   \\
  python examples/ml-training-prefetch/pipeline.py

  # With Iceberg catalog:
  ICEBERG_CATALOG_URI=http://localhost:8181 \\
  python examples/ml-training-prefetch/pipeline.py

  # Via Docker Compose (see docker-compose.yml in this directory):
  docker compose up ml-trainer

Environment variables
---------------------
  MINIO_ENDPOINT        MinIO host:port  (default: localhost:9000)
  MINIO_ACCESS_KEY      (default: minioadmin)
  MINIO_SECRET_KEY      (default: minioadmin)
  MINIO_BUCKET          (default: processed)
  MINIO_PREFIX          feature prefix   (default: streamforge/features)
  MINIO_SECURE          (default: false)
  ICEBERG_CATALOG_URI   REST catalog URL (optional; skipped if unset)
  ICEBERG_TABLE         (default: streamforge.features.user_event_counts)
  STREAMFORGE_JOB_ID    job identifier   (default: ml-<timestamp>)
  STREAMFORGE_DEMO_DIR  base cache dir   (default: /tmp/streamforge-demo)
  PREFETCH_TOP_N        hot files to warm (default: 50)
  PREFETCH_LATENCY_S    simulated RTT     (default: 0.0)
  N_SYNTHETIC_RECORDS   synthetic records when offline (default: 2000)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

# ── Add this directory to sys.path so sibling modules resolve cleanly ────────
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from feature_store import (
    FeatureRecord,
    IcebergFeatureReader,
    list_minio_feature_files,
    read_feature_records_from_cache,
    read_synthetic_feature_records,
)
from prefetch_warmer import WarmupConfig, WarmupResult, warm_cache
from trainer import TrainingResult, train


# ---------------------------------------------------------------------------
# Pipeline result
# ---------------------------------------------------------------------------

@dataclass
class PipelineResult:
    job_id: str
    mode: str                      # "live-minio" | "live-iceberg" | "synthetic"
    n_candidates: int
    n_feature_records: int
    warmup: WarmupResult
    training: TrainingResult
    total_duration_s: float

    def print_report(self) -> None:
        sep = "=" * 70
        print(f"\n{sep}")
        print("  StreamForge AI — ML Training Pipeline Report")
        print(sep)
        print(f"  Job ID          : {self.job_id}")
        print(f"  Data mode       : {self.mode}")
        print(f"  Feature files   : {self.n_candidates:,} discovered")
        print(f"  Feature records : {self.n_feature_records:,} read")
        print()
        print("  ── Prefetch Warm-up ──────────────────────────────")
        print(f"  Candidates      : {self.warmup.candidates_total:,}")
        print(f"  Files warmed    : {self.warmup.files_warmed:,}")
        print(f"  Cache hit-rate  : {self.warmup.hit_rate:.1%}")
        print(f"  Prefetch time   : {self.warmup.prefetch_duration_s:.2f}s")
        print()
        print("  ── Model Training ────────────────────────────────")
        print(f"  Backend         : {self.training.backend}")
        print(f"  Train samples   : {self.training.n_train:,}")
        print(f"  Test samples    : {self.training.n_test:,}")
        print(f"  Accuracy        : {self.training.accuracy:.4f}")
        print(f"  Train time      : {self.training.train_duration_s:.2f}s")
        if self.training.minio_model_key:
            print(f"  Model (MinIO)   : {self.training.minio_model_key}")
        elif self.training.model_path:
            print(f"  Model (local)   : {self.training.model_path}")
        print()
        print(f"  ── Total pipeline time: {self.total_duration_s:.2f}s ──")
        warmup_pct = self.warmup.prefetch_duration_s / self.total_duration_s * 100
        train_pct  = self.training.train_duration_s  / self.total_duration_s * 100
        print(f"     Prefetch warm-up : {warmup_pct:.1f}%")
        print(f"     Model training   : {train_pct:.1f}%")
        print(sep + "\n")


# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------

def run_pipeline(
    job_id: Optional[str] = None,
    prefetch_top_n: int = 50,
    prefetch_latency_s: float = 0.0,
    n_synthetic: int = 2000,
    cache_dir: Optional[Path] = None,
) -> PipelineResult:

    if job_id is None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        job_id = f"ml-{ts}"

    t_pipeline_start = time.perf_counter()

    # ── Stage 1: Discover feature files ─────────────────────────────────────
    print(f"\n[PIPELINE] Job: {job_id}")
    print("[PIPELINE] Stage 1/5 — discovering feature files …")

    bucket = os.environ.get("MINIO_BUCKET",  "processed")
    prefix = os.environ.get("MINIO_PREFIX",  "streamforge/features")

    candidates, is_live_minio = list_minio_feature_files(
        bucket=bucket, prefix=prefix, max_files=500
    )
    mode = "live-minio" if is_live_minio else "synthetic"
    print(f"[PIPELINE]   mode={mode}  candidates={len(candidates):,}")

    # ── Stage 2: Prefetch warm-up ────────────────────────────────────────────
    print("[PIPELINE] Stage 2/5 — warming prefetch cache …")
    warmup_cfg = WarmupConfig(
        top_n=prefetch_top_n,
        simulate_latency_s=prefetch_latency_s,
        use_real_download=is_live_minio,
        cache_dir=cache_dir,
        job_id=job_id,
    )
    warmup_result = warm_cache(candidates, warmup_cfg)
    warmup_result.print_summary()

    # ── Stage 3: Read feature records ────────────────────────────────────────
    print("[PIPELINE] Stage 3/5 — reading feature records from cache …")

    # Try Iceberg first (highest fidelity)
    iceberg = IcebergFeatureReader()
    if iceberg.is_available():
        print("[PIPELINE]   reading from Iceberg catalog …")
        records: List[FeatureRecord] = iceberg.read()
        mode = "live-iceberg"
    elif is_live_minio and warmup_result.files_warmed > 0:
        records = read_feature_records_from_cache(
            warmup_result.cache_dir, warmup_result.hot_files
        )
        print(f"[PIPELINE]   read {len(records):,} records from MinIO cache")
    else:
        records = read_synthetic_feature_records(n_records=n_synthetic)
        print(f"[PIPELINE]   generated {len(records):,} synthetic records")

    if not records:
        # Cache files present but empty — generate synthetics as fallback
        print("[PIPELINE]   warn: no records in cache; using synthetic data")
        records = read_synthetic_feature_records(n_records=n_synthetic)

    # ── Stage 4: Train ───────────────────────────────────────────────────────
    print(f"[PIPELINE] Stage 4/5 — training on {len(records):,} feature records …")
    training_result = train(records, job_id=job_id)

    # ── Stage 5: Report ──────────────────────────────────────────────────────
    total_s = time.perf_counter() - t_pipeline_start
    print("[PIPELINE] Stage 5/5 — building report …")

    result = PipelineResult(
        job_id=job_id,
        mode=mode,
        n_candidates=len(candidates),
        n_feature_records=len(records),
        warmup=warmup_result,
        training=training_result,
        total_duration_s=total_s,
    )
    result.print_report()
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="StreamForge AI ML training pipeline with prefetch cache warm-up"
    )
    p.add_argument("--job-id",     default=None,
                   help="Job identifier (default: ml-<timestamp>)")
    p.add_argument("--top-n",      type=int,   default=int(os.environ.get("PREFETCH_TOP_N", 50)),
                   help="Number of hot files to prefetch (default: 50)")
    p.add_argument("--latency",    type=float, default=float(os.environ.get("PREFETCH_LATENCY_S", 0.0)),
                   help="Simulated per-file prefetch latency in seconds (default: 0.0)")
    p.add_argument("--synthetic",  type=int,   default=int(os.environ.get("N_SYNTHETIC_RECORDS", 2000)),
                   help="Synthetic records to generate when offline (default: 2000)")
    p.add_argument("--cache-dir",  default=None,
                   help="Override local cache directory path")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    run_pipeline(
        job_id=args.job_id,
        prefetch_top_n=args.top_n,
        prefetch_latency_s=args.latency,
        n_synthetic=args.synthetic,
        cache_dir=cache_dir,
    )


if __name__ == "__main__":
    main()
