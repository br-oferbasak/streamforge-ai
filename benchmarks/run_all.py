#!/usr/bin/env python3
"""
StreamForge AI — Master Benchmark Runner

Runs all component benchmarks and prints a consolidated summary.

Usage:
    python benchmarks/run_all.py [--component prefetch|rag|workflow]
"""
import argparse
import sys
import time
from pathlib import Path


def _add_to_path(rel: str):
    p = Path(__file__).parent.parent / rel
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


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


COMPONENTS = {
    "prefetch": ("Prefetch Engine", run_prefetch),
    "rag":      ("RAG Engine",      run_rag),
    "workflow": ("Agent Workflow",  run_workflow),
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

    targets = {args.component: COMPONENTS[args.component]} if args.component else COMPONENTS

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
        print(f"  {label:<30} {status:>10}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
