#!/usr/bin/env python3
"""
StreamForge AI — MinIO Write Throughput Benchmark

Measures write throughput to MinIO (S3-compatible object storage) across
different object sizes and concurrency levels, mirroring the access patterns
used by the feature-sink service in:

    deploy/cdc-flink-minio-demo/feature-sink/feature_to_minio.py

Two execution modes
-------------------
1. **Live MinIO** — if a MinIO instance is reachable (configured via env vars
   or defaults matching deploy/cdc-flink-minio-demo/docker-compose.yml) the
   benchmark writes real objects and measures actual S3 PUT latency.

2. **Local-filesystem simulation** — when MinIO is unavailable the benchmark
   writes to a temporary directory and measures filesystem write throughput,
   providing a reproducible lower-bound reference that runs in CI/CD with no
   external dependencies.

Environment variables (matching feature-sink and prefetch-engine conventions)
-----------------------------------------------------------------------------
  MINIO_ENDPOINT    — default: localhost:9000
  MINIO_ACCESS_KEY  — default: minioadmin
  MINIO_SECRET_KEY  — default: minioadmin
  MINIO_SECURE      — default: false
  MINIO_BUCKET      — default: processed
  MINIO_PREFIX      — default: benchmarks
"""

import io
import json
import os
import random
import statistics
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Callable, Tuple

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# ---------------------------------------------------------------------------
# Payload generators
# ---------------------------------------------------------------------------

def _make_json_payload(size_bytes: int) -> bytes:
    """Generate a realistic feature-sink JSON payload of approximately size_bytes."""
    base = {
        "job_id": "bench-" + str(random.randint(100000, 999999)),
        "window_start": 1700000000000,
        "window_end":   1700000060000,
        "user_id": random.randint(1, 10000),
        "event_count": random.randint(1, 500),
        "sink_timestamp": int(time.time() * 1000),
        "pipeline": "cdc-flink-minio",
    }
    base_bytes = json.dumps(base).encode()
    # Pad with a repeated "padding" field to reach target size
    if len(base_bytes) < size_bytes:
        padding = "x" * (size_bytes - len(base_bytes) - 15)
        base["_pad"] = padding
    return json.dumps(base).encode()


def _make_binary_payload(size_bytes: int) -> bytes:
    rng = random.Random(RANDOM_SEED)
    return bytes(rng.getrandbits(8) for _ in range(size_bytes))


# ---------------------------------------------------------------------------
# Backend abstraction
# ---------------------------------------------------------------------------

class _MinioBackend:
    def __init__(self, client, bucket: str, prefix: str):
        self._client = client
        self._bucket = bucket
        self._prefix = prefix

    def write(self, key: str, data: bytes) -> float:
        t0 = time.perf_counter()
        self._client.put_object(
            self._bucket,
            f"{self._prefix}/{key}",
            io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        return time.perf_counter() - t0

    def cleanup(self, keys: List[str]) -> None:
        from minio.deleteobjects import DeleteObject  # type: ignore
        objects = [DeleteObject(f"{self._prefix}/{k}") for k in keys]
        errors = list(self._client.remove_objects(self._bucket, objects))
        _ = errors  # best-effort cleanup


class _FilesystemBackend:
    def __init__(self, base_dir: Path):
        self._base = base_dir
        self._base.mkdir(parents=True, exist_ok=True)

    def write(self, key: str, data: bytes) -> float:
        path = self._base / key.replace("/", "_")
        t0 = time.perf_counter()
        path.write_bytes(data)
        return time.perf_counter() - t0

    def cleanup(self, keys: List[str]) -> None:
        for key in keys:
            p = self._base / key.replace("/", "_")
            p.unlink(missing_ok=True)


def _resolve_backend(tmp_dir: Path) -> Tuple[object, str]:
    endpoint  = os.environ.get("MINIO_ENDPOINT",   "localhost:9000")
    access    = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret    = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    secure    = os.environ.get("MINIO_SECURE",     "false").lower() == "true"
    bucket    = os.environ.get("MINIO_BUCKET",     "processed")
    prefix    = os.environ.get("MINIO_PREFIX",     "benchmarks")

    try:
        from minio import Minio  # type: ignore
        client = Minio(endpoint, access_key=access, secret_key=secret, secure=secure)
        # Quick connectivity check
        client.list_buckets()
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        return _MinioBackend(client, bucket, prefix), "minio"
    except Exception:
        pass

    return _FilesystemBackend(tmp_dir / "minio_sim"), "filesystem"


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

@dataclass
class MinioScenarioConfig:
    name: str
    object_size_bytes: int
    num_objects: int
    concurrency: int = 1    # parallel writer threads
    repeat: int = 3


@dataclass
class MinioBenchmarkResult:
    scenario: str
    backend: str            # "minio" or "filesystem"
    object_size_bytes: int
    num_objects: int
    concurrency: int
    throughput_ops: float   # objects per second
    throughput_mbs: float   # MB per second
    p50_ms: float           # median PUT latency (ms)
    p99_ms: float           # 99th-percentile PUT latency (ms)
    total_s: float


SCENARIOS: List[MinioScenarioConfig] = [
    MinioScenarioConfig("Tiny objects   (1 KB  × 100, concurrency=1)",
                        object_size_bytes=1_024,        num_objects=100, concurrency=1),
    MinioScenarioConfig("Small objects  (10 KB × 100, concurrency=1)",
                        object_size_bytes=10_240,       num_objects=100, concurrency=1),
    MinioScenarioConfig("Medium objects (100 KB× 50,  concurrency=1)",
                        object_size_bytes=102_400,      num_objects=50,  concurrency=1),
    MinioScenarioConfig("Large objects  (1 MB  × 20,  concurrency=1)",
                        object_size_bytes=1_048_576,    num_objects=20,  concurrency=1),
    MinioScenarioConfig("Concurrent     (10 KB × 100, concurrency=4)",
                        object_size_bytes=10_240,       num_objects=100, concurrency=4),
    MinioScenarioConfig("Concurrent     (100 KB× 50,  concurrency=4)",
                        object_size_bytes=102_400,      num_objects=50,  concurrency=4),
    MinioScenarioConfig("Feature-sink   (2 KB  × 200, concurrency=1)  [realistic]",
                        object_size_bytes=2_048,        num_objects=200, concurrency=1),
]


# ---------------------------------------------------------------------------
# Benchmark execution
# ---------------------------------------------------------------------------

def _run_writes_serial(backend, payloads: List[bytes]) -> List[float]:
    latencies = []
    for i, payload in enumerate(payloads):
        key = f"obj_{i:06d}_{int(time.time()*1000)}.json"
        lat = backend.write(key, payload)
        latencies.append(lat)
    return latencies


def _run_writes_concurrent(backend, payloads: List[bytes],
                           concurrency: int) -> List[float]:
    latencies: List[float] = []
    lock = threading.Lock()
    chunks = [payloads[i::concurrency] for i in range(concurrency)]

    def worker(chunk: List[bytes], thread_id: int) -> None:
        for j, payload in enumerate(chunk):
            key = f"obj_t{thread_id}_{j:06d}_{int(time.time()*1000)}.json"
            lat = backend.write(key, payload)
            with lock:
                latencies.append(lat)

    threads = [threading.Thread(target=worker, args=(chunk, tid))
               for tid, chunk in enumerate(chunks)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return latencies


def run_scenario(cfg: MinioScenarioConfig, backend, backend_name: str,
                 repeat: int = 3) -> MinioBenchmarkResult:
    rng = random.Random(RANDOM_SEED)
    # Use JSON payloads for realistic feature-sink simulation
    payloads = [_make_json_payload(cfg.object_size_bytes) for _ in range(cfg.num_objects)]

    all_lats: List[float] = []
    all_total_s: List[float] = []

    for _ in range(repeat):
        t0 = time.perf_counter()
        if cfg.concurrency > 1:
            lats = _run_writes_concurrent(backend, payloads, cfg.concurrency)
        else:
            lats = _run_writes_serial(backend, payloads)
        total_s = time.perf_counter() - t0
        all_lats.extend(lats)
        all_total_s.append(total_s)

    avg_total_s = statistics.mean(all_total_s)
    sorted_lats_ms = sorted(l * 1000 for l in all_lats)
    total_bytes = cfg.object_size_bytes * cfg.num_objects * repeat

    return MinioBenchmarkResult(
        scenario=cfg.name,
        backend=backend_name,
        object_size_bytes=cfg.object_size_bytes,
        num_objects=cfg.num_objects,
        concurrency=cfg.concurrency,
        throughput_ops=cfg.num_objects / avg_total_s,
        throughput_mbs=total_bytes / avg_total_s / 1e6 / repeat,
        p50_ms=sorted_lats_ms[len(sorted_lats_ms) // 2],
        p99_ms=sorted_lats_ms[int(len(sorted_lats_ms) * 0.99)],
        total_s=avg_total_s,
    )


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def _print_results(results: List[MinioBenchmarkResult], backend_name: str) -> None:
    w = 95
    print("\n" + "=" * w)
    mode = "MinIO (live)" if backend_name == "minio" else "Filesystem simulation"
    print(f"         StreamForge AI: MinIO Write Throughput Results  [{mode}]")
    print("=" * w)
    hdr = (f"{'Scenario':<50} {'ops/s':>7} {'MB/s':>7} "
           f"{'p50 ms':>8} {'p99 ms':>8} {'Total s':>8}")
    print(hdr)
    print("-" * w)
    for r in results:
        print(
            f"{r.scenario:<50} {r.throughput_ops:>7.1f} {r.throughput_mbs:>7.2f}"
            f" {r.p50_ms:>8.2f} {r.p99_ms:>8.2f} {r.total_s:>8.2f}"
        )
    print("=" * w)
    if backend_name == "filesystem":
        print("  NOTE: Running in filesystem-simulation mode (MinIO not reachable).")
        print("        Set MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY to")
        print("        run against a live MinIO instance (e.g. deploy/cdc-flink-minio-demo).")
    print("  ops/s  = PUT objects per second")
    print("  MB/s   = raw write throughput in megabytes per second")
    print("  p50/p99 = PUT latency percentiles in milliseconds")
    print("=" * w)


def run_benchmark() -> List[MinioBenchmarkResult]:
    print("=" * 70)
    print("      StreamForge AI: MinIO Write — Throughput Benchmark")
    print("=" * 70)
    print(f"  Endpoint : {os.environ.get('MINIO_ENDPOINT', 'localhost:9000')}")
    print(f"  Bucket   : {os.environ.get('MINIO_BUCKET', 'processed')}")
    print(f"  Scenarios: {len(SCENARIOS)}\n")
    print("  Detecting backend ...", end=" ", flush=True)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        backend, backend_name = _resolve_backend(tmp_path)
        print(f"using {backend_name.upper()}\n")

        results = []
        for i, cfg in enumerate(SCENARIOS, 1):
            print(f"  [{i}/{len(SCENARIOS)}] {cfg.name} ...", end=" ", flush=True)
            r = run_scenario(cfg, backend, backend_name, repeat=cfg.repeat)
            results.append(r)
            print(f"done  ({r.throughput_ops:.1f} ops/s, {r.throughput_mbs:.2f} MB/s, "
                  f"p99={r.p99_ms:.1f}ms)")

        _print_results(results, backend_name)
    return results


if __name__ == "__main__":
    run_benchmark()
