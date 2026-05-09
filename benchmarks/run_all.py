#!/usr/bin/env python3
"""
StreamForge AI — Master Benchmark Runner

Runs all component benchmarks and prints a consolidated summary.

Usage:
    python benchmarks/run_all.py [--component <name>]

Available components
--------------------
  prefetch        Prefetch engine multi-scenario benchmark (existing)
  rag             RAG engine benchmark
  workflow        Agent-workflow benchmark
  cdc             CDC ingestion throughput (events/s, parse latency)
  flink           Flink job latency (pipeline stage breakdown)
  minio           MinIO write throughput (ops/s, MB/s, PUT latency)
  cache_hitrate   Prefetch cache hit-ratio vs cold-start time sweep
"""
import argparse
import sys
import time
from pathlib import Path


def _add_to_path(rel: str):
    p = Path(__file__).parent.parent / rel
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _add_bench_path():
    p = Path(__file__).parent
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


# ---------------------------------------------------------------------------
# Existing component runners
# ---------------------------------------------------------------------------

def run_prefetch():
    _add_to_path("prefetch-engine")
    from benchmark import run_benchmark
    return run_benchmark()


def run_rag():
    _add_to_path("rag-engine")
    from benchmark_rag import run_benchmark
    return run_benchmark()


def run_workflow():
    _add_to_path("agent-workflow")
    from benchmark_workflow import run_benchmark
    return run_benchmark()


# ---------------------------------------------------------------------------
# New component runners
# ---------------------------------------------------------------------------

def run_cdc():
    _add_bench_path()
    from cdc_ingestion_throughput import run_benchmark
    return run_benchmark()


def run_flink():
    _add_bench_path()
    from flink_job_latency import run_benchmark
    return run_benchmark()


def run_minio():
    _add_bench_path()
    from minio_write_throughput import run_benchmark
    return run_benchmark()


def run_cache_hitrate():
    _add_bench_path()
    from prefetch_cache_hitrate import run_benchmark
    return run_benchmark()


# ---------------------------------------------------------------------------
# Component registry
# ---------------------------------------------------------------------------

COMPONENTS = {
    "prefetch":     ("Prefetch Engine",                 run_prefetch),
    "rag":          ("RAG Engine",                      run_rag),
    "workflow":     ("Agent Workflow",                  run_workflow),
    "cdc":          ("CDC Ingestion Throughput",        run_cdc),
    "flink":        ("Flink Job Latency",               run_flink),
    "minio":        ("MinIO Write Throughput",          run_minio),
    "cache_hitrate":("Prefetch Cache Hit-Ratio",        run_cache_hitrate),
}


def main():
    parser = argparse.ArgumentParser(description="StreamForge AI Benchmark Runner")
    parser.add_argument(
        "--component",
        choices=list(COMPONENTS.keys()),
        default=None,
        help="Run only one component benchmark (default: all)",
    )
    args = parser.parse_args()

    targets = ({args.component: COMPONENTS[args.component]}
               if args.component else COMPONENTS)

    print("\n" + "=" * 70)
    print("         StreamForge AI — Performance Benchmark Suite")
    print("=" * 70)
    print(f"  Components: {', '.join(targets.keys())}")
    print("=" * 70 + "\n")

    timings = {}
    for key, (label, fn) in targets.items():
        print(f"\n{'='*70}")
        print(f"  Running: {label}")
        print(f"{'='*70}")
        t0 = time.time()
        try:
            fn()
            timings[label] = time.time() - t0
        except Exception as e:
            print(f"\n  [ERROR] {label} benchmark failed: {e}")
            timings[label] = None

    # Consolidated summary
    print("\n" + "=" * 70)
    print("                    BENCHMARK SUITE SUMMARY")
    print("=" * 70)
    for label, elapsed in timings.items():
        status = f"{elapsed:.2f}s" if elapsed is not None else "FAILED"
        print(f"  {label:<38} {status:>10}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
