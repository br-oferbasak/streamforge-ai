# StreamForge AI — Benchmark Suite

Reproducible performance benchmarks covering every major component of the
StreamForge AI data pipeline.

## Quick start

```bash
# All benchmarks (no external services required)
python benchmarks/run_all.py

# Single component
python benchmarks/run_all.py --component cdc
python benchmarks/run_all.py --component flink
python benchmarks/run_all.py --component minio
python benchmarks/run_all.py --component cache_hitrate
python benchmarks/run_all.py --component prefetch
```

Or run any benchmark directly:

```bash
python benchmarks/cdc_ingestion_throughput.py
python benchmarks/flink_job_latency.py
python benchmarks/minio_write_throughput.py
python benchmarks/prefetch_cache_hitrate.py
```

## Benchmarks

### 1. CDC Ingestion Throughput (`cdc_ingestion_throughput.py`)

Measures how fast the CDC ingestion layer can parse and filter Debezium CDC
envelopes — the same JSON format produced by the MySQL connector in
`deploy/cdc-mysql-kafka-debezium/`.

| Metric | Description |
|--------|-------------|
| `Throughput (events/s)` | Total events parsed and filtered per second |
| `INS/s` | INSERT events accepted by the op-filter per second |
| `DLQ%` | Fraction of INSERT events routed to the dead-letter queue (schema drift) |
| `p50 / p99 µs` | Per-event parse latency percentiles |
| `MB/s` | Raw byte processing throughput |

**Scenarios** cover: inserts-only, mixed ops, high-volume (50k events),
schema-drift at 5% and 25%, delete-heavy workloads.

**Live Kafka mode** (optional): set `KAFKA_BOOTSTRAP_SERVERS` to also measure
real consumer throughput against a running broker.

---

### 2. Flink Job Latency (`flink_job_latency.py`)

Replays the four pipeline stages of `CdcUserEventCountJob` in pure Python to
measure per-stage latency and overall throughput:

1. **Deserialisation** — JSON bytes → `CdcEvent`
2. **Filter** — `op == "c"` + schema-version guard
3. **Window aggregation** — tumbling time-window count (mirrors `TumblingEventTimeWindows`)
4. **Serialisation** — `WindowResult` → JSON bytes

| Metric | Description |
|--------|-------------|
| `E2E ms` | Total pipeline latency for the full event stream |
| `Deser ms` | Deserialisation stage time |
| `Win ms` | Window aggregation stage time |
| `Ser ms` | Serialisation stage time |
| `Events/s` | End-to-end processing throughput |

**Scenarios** sweep window sizes (10 s / 30 s / 60 s), stream sizes
(1k / 10k / 50k events), and insert ratios (30% / 90%).

---

### 3. MinIO Write Throughput (`minio_write_throughput.py`)

Measures object write throughput against MinIO, mirroring the access pattern
of `deploy/cdc-flink-minio-demo/feature-sink/feature_to_minio.py`.

| Metric | Description |
|--------|-------------|
| `ops/s` | PUT operations per second |
| `MB/s` | Raw write throughput |
| `p50 / p99 ms` | PUT latency percentiles |

**Two modes** (auto-detected):
- **Live MinIO** — writes real objects when a broker is reachable.
- **Filesystem simulation** — writes to a temp directory for CI/CD runs.

**Environment variables** (matching deploy defaults):

```
MINIO_ENDPOINT    localhost:9000
MINIO_ACCESS_KEY  minioadmin
MINIO_SECRET_KEY  minioadmin
MINIO_SECURE      false
MINIO_BUCKET      processed
MINIO_PREFIX      benchmarks
```

**Scenarios** sweep object sizes (1 KB → 1 MB), object counts, and
concurrency levels (1 and 4 parallel writer threads).

---

### 4. Prefetch Cache Hit-Ratio vs Cold-Start Time (`prefetch_cache_hitrate.py`)

Quantifies the relationship between the prefetch-engine's cache hit-ratio and
ML job cold-start latency, sweeping hit ratios from 0% to 100% across three
dataset sizes.

| Metric | Description |
|--------|-------------|
| `Cold-start ms` | Total time from job start to all files processed |
| `TTFB ms` | Time-to-first-batch: latency to the first record |
| `Speedup` | Cold-start improvement vs 0% hit-ratio baseline |
| `Prefetch ms` | Time spent staging hot files into local cache |
| `Miss penalty ms` | Average remote I/O penalty per cache miss |

**Hit-ratio sweep**: 0%, 25%, 50%, 75%, 100%  
**Datasets**: Small (10 files), Medium (50 files), Large (200 files)

Latency constants match `prefetch-engine/benchmark.py`:
- Remote (MinIO) latency: 50 ms/file
- Local (cache) latency: 2 ms/file

---

### Legacy benchmarks (existing)

| Component | File | Description |
|-----------|------|-------------|
| `prefetch` | `prefetch-engine/benchmark.py` | Multi-scenario prefetch benefit analysis |
| `rag` | `rag-engine/benchmark_rag.py` | RAG engine retrieval benchmarks |
| `workflow` | `agent-workflow/benchmark_workflow.py` | Agent workflow orchestration benchmarks |

## Reproducibility

All new benchmarks use a fixed `RANDOM_SEED = 42`. Given the same Python
version and OS, successive runs should produce byte-identical payloads and
differ only in wall-clock timings (±5% due to OS scheduling noise).

To compare runs across machines, focus on **relative metrics** (speedup,
reduction%, stage fractions) rather than absolute wall-clock times.

## Requirements

The four new benchmarks have **zero extra dependencies** beyond the Python
standard library. The MinIO benchmark optionally uses the `minio` package
(already listed in `prefetch-engine/requirements.txt`) when a live MinIO
instance is available.

```
python >= 3.9
minio >= 7.2.7   # optional, for live MinIO mode only
```
