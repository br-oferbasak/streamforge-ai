# StreamForge AI — Performance Tuning Guide

This document describes every exposed tuning knob for the four subsystems in the
StreamForge CDC pipeline:

```
MySQL → Debezium/Kafka → Flink → MinIO (Iceberg)
```

For each knob you will find: the environment variable name, its default, the lever it
controls, and the tradeoff between the two extremes.

---

## Table of contents

1. [Flink — parallelism](#1-flink--parallelism)
2. [Flink — checkpointing](#2-flink--checkpointing)
3. [Flink — restart strategy](#3-flink--restart-strategy)
4. [Kafka — producer batching & compression](#4-kafka--producer-batching--compression)
5. [MinIO — multipart upload](#5-minio--multipart-upload)
6. [MinIO — prefix sharding](#6-minio--prefix-sharding)
7. [MinIO — sink batching](#7-minio--sink-batching)
8. [Iceberg — write file size](#8-iceberg--write-file-size)
9. [Iceberg — write format & encoding](#9-iceberg--write-format--encoding)
10. [Iceberg — compaction](#10-iceberg--compaction)
11. [Quick-reference table](#11-quick-reference-table)
12. [Recommended profiles](#12-recommended-profiles)

---

## 1. Flink — parallelism

**Code:** [`CdcUserEventCountJob.java`](../stream-processor/src/main/java/ai/streamforge/processor/CdcUserEventCountJob.java)

| Env var | Default | Unit |
|---|---|---|
| `FLINK_PARALLELISM` | `-1` (cluster default) | integer |

Parallelism sets how many parallel task instances each operator runs. In the CDC job
this governs how many partitions the Kafka source reads concurrently and how many
key-groups the windowed aggregation maintains.

| Lower (e.g. `1–2`) | Higher (e.g. `8–32`) |
|---|---|
| Fewer TaskManager slots needed | Better throughput on wide Kafka topics |
| Simpler state layout; smaller checkpoint | More concurrent state; larger checkpoint size |
| Suitable for dev / low-volume topics | Required when source topic has many partitions |
| Watermark synchronization is faster | Watermark alignment across subtasks adds latency |

**Rule of thumb:** set `FLINK_PARALLELISM` equal to the number of partitions in
`KAFKA_SOURCE_TOPIC`. Kafka does not serve the same partition to two consumers in the
same group, so extra parallelism above the partition count wastes slots.

---

## 2. Flink — checkpointing

**Code:** [`CdcUserEventCountJob.java`](../stream-processor/src/main/java/ai/streamforge/processor/CdcUserEventCountJob.java)

| Env var | Default | Unit |
|---|---|---|
| `CHECKPOINT_INTERVAL_MS` | `30000` | ms |
| `CHECKPOINT_TIMEOUT_MS` | `60000` | ms |
| `CHECKPOINT_MIN_PAUSE_MS` | `0` | ms |
| `CHECKPOINT_MAX_CONCURRENT` | `1` | integer |
| `CHECKPOINT_MODE` | `exactly_once` | `exactly_once` \| `at_least_once` |
| `CHECKPOINT_UNALIGNED` | `false` | boolean |

### 2.1 Checkpoint interval

| Short (e.g. 5 000 ms) | Long (e.g. 300 000 ms) |
|---|---|
| Fast recovery — re-plays fewer Kafka messages after failure | Slow recovery — replays more Kafka history |
| High write I/O to state backend (S3/HDFS) | Less I/O pressure |
| More frequent barrier injection — slightly raises latency | Fewer barriers — lower ongoing latency cost |

**Recommendation:** 15 000–60 000 ms for production; shorter during initial load.

### 2.2 Checkpoint timeout

If a checkpoint does not complete within `CHECKPOINT_TIMEOUT_MS` it is aborted. Set
this larger than your expected checkpoint duration (visible on the Flink UI). Repeated
timeouts indicate back-pressure or slow state-backend I/O.

### 2.3 Min pause between checkpoints

`CHECKPOINT_MIN_PAUSE_MS` prevents a new checkpoint from starting immediately after
the previous one finishes. Under heavy back-pressure this stops the job from spending
all its time checkpointing.

| `0` (default) | e.g. `10000` |
|---|---|
| Back-to-back checkpoints possible under back-pressure | Guarantees processing time between checkpoints |
| Can degrade throughput if checkpoints are slow | Predictable overhead per checkpoint cycle |

### 2.4 Exactly-once vs. at-least-once

`CHECKPOINT_MODE=exactly_once` inserts barriers that align across all input channels
before snapshotting. This guarantees no duplicate output but adds a small latency
spike at each barrier.

`at_least_once` skips barrier alignment: records after the barrier may be processed
before the checkpoint completes, which can produce duplicates on recovery but reduces
p99 latency.

| `exactly_once` | `at_least_once` |
|---|---|
| No duplicates; idempotent sink not required | Possible duplicates; sink must be idempotent |
| Barrier alignment pauses subtasks briefly | No alignment pause — lower latency |
| Required when writing to transactional sinks | Suitable when downstream deduplicates (e.g. Iceberg MERGE) |

### 2.5 Unaligned checkpoints

`CHECKPOINT_UNALIGNED=true` stores in-flight records inside the checkpoint snapshot
instead of waiting for barrier alignment. This eliminates alignment stalls under
back-pressure, at the cost of a larger checkpoint size.

| `false` (aligned) | `true` (unaligned) |
|---|---|
| Smaller checkpoint (only state, no in-flight data) | Larger checkpoint (state + buffered records) |
| Suitable when back-pressure is rare | Required when barrier stalls exceed timeout |
| Lower restore overhead | Higher restore overhead (re-applies buffered records) |

> Only available with `CHECKPOINT_MODE=exactly_once`.

---

## 3. Flink — restart strategy

**Code:** [`CdcUserEventCountJob.java`](../stream-processor/src/main/java/ai/streamforge/processor/CdcUserEventCountJob.java)

| Env var | Default | Unit |
|---|---|---|
| `RESTART_ATTEMPTS` | `3` | integer |
| `RESTART_DELAY_MS` | `10000` | ms |

A fixed-delay restart strategy retries the job up to `RESTART_ATTEMPTS` times, waiting
`RESTART_DELAY_MS` between each attempt. If all attempts are exhausted the job fails
permanently.

| Fewer attempts / shorter delay | More attempts / longer delay |
|---|---|
| Fails fast — surfaces persistent errors quickly | More resilient to transient network blips |
| Lower recovery time when bug is real | Risk of cascading retries masking the root cause |

---

## 4. Kafka — producer batching & compression

**Code:** [`CdcUserEventCountJob.java`](../stream-processor/src/main/java/ai/streamforge/processor/CdcUserEventCountJob.java) — `buildKafkaSinkProps()`

These properties apply to both the main `user.event.counts` sink and the DLQ sink.

| Env var | Default | Kafka property |
|---|---|---|
| `KAFKA_SINK_BATCH_SIZE_BYTES` | `16384` (16 KB) | `batch.size` |
| `KAFKA_SINK_LINGER_MS` | `5` | `linger.ms` |
| `KAFKA_SINK_BUFFER_MEMORY_BYTES` | `33554432` (32 MB) | `buffer.memory` |
| `KAFKA_SINK_COMPRESSION` | `none` | `compression.type` |
| `KAFKA_SINK_ACKS` | `all` | `acks` |

### 4.1 Batching (`batch.size` + `linger.ms`)

The Kafka producer accumulates records into a batch before sending. A batch is sent
when it reaches `batch.size` bytes **or** when `linger.ms` milliseconds have elapsed
since the first record was added.

| Small batch + low linger | Large batch + higher linger |
|---|---|
| Low end-to-end latency (near real-time delivery) | High throughput (fewer network round-trips) |
| More producer CPU (many small sends) | Higher per-record latency (records wait in buffer) |
| Suitable for latency-sensitive consumers | Suitable for analytics / batch consumers |

**Recommended for CDC aggregation output:**
`KAFKA_SINK_BATCH_SIZE_BYTES=65536` (64 KB), `KAFKA_SINK_LINGER_MS=20`

### 4.2 Compression

| Algorithm | CPU cost | Compression ratio | Best for |
|---|---|---|---|
| `none` | None | 1× | Low-volume topics or already-compressed payloads |
| `snappy` | Low | ~2× | Default production choice: good ratio, fast |
| `lz4` | Very low | ~2× | Latency-sensitive paths needing compression |
| `zstd` | Medium | ~3–4× | Large messages or expensive network egress |
| `gzip` | High | ~3× | Compatibility with legacy consumers |

JSON CDC events compress well (typically 3–5× with snappy). Enable compression when
the Kafka cluster is network-bound or broker storage is expensive.

### 4.3 Acks

| `acks=all` | `acks=1` | `acks=0` |
|---|---|---|
| All in-sync replicas acknowledge — no data loss on broker failure | Only leader acknowledges — fast but may lose data if leader fails before replication | Fire-and-forget — maximum throughput, no durability guarantee |
| Required for exactly-once with Kafka transactions | Acceptable for DLQ topic if replay is possible | Not suitable for this pipeline |

---

## 5. MinIO — multipart upload

**Code:**
- [`feature_to_minio.py`](../deploy/cdc-flink-minio-demo/feature-sink/feature_to_minio.py)
- [`IcebergSinkFactory.java`](../stream-processor/src/main/java/ai/streamforge/processor/sink/IcebergSinkFactory.java) (S3A settings)

| Env var | Default | Applies to |
|---|---|---|
| `MINIO_PART_SIZE` | `10485760` (10 MB) | Python feature-sink `put_object` |
| `ICEBERG_S3_MULTIPART_SIZE` | `67108864` (64 MB) | S3A Hadoop FS multipart part |
| `ICEBERG_S3_MULTIPART_THRESHOLD` | `67108864` (64 MB) | S3A: minimum object size to trigger multipart |
| `ICEBERG_S3_UPLOAD_THREADS` | `10` | S3A parallel upload threads per task |

Objects smaller than the threshold are uploaded as a single PUT. Objects at or above
the threshold are split into parts of `part_size` bytes, uploaded in parallel.

| Small part size (e.g. 5 MB) | Large part size (e.g. 128 MB) |
|---|---|
| Lower memory per upload thread | Higher memory per upload thread |
| More HTTP round-trips (higher MinIO CPU overhead) | Fewer round-trips (lower overhead) |
| Better for small-to-medium files | Best for large Parquet/ORC data files |

MinIO's hard minimum part size is **5 MB** (same as AWS S3). Do not set below this.

`ICEBERG_S3_UPLOAD_THREADS` controls how many parts are in-flight simultaneously. On
high-bandwidth networks (e.g. 10 GbE within the same data center) increasing this to
`20–40` can double write throughput for large Iceberg data files.

---

## 6. MinIO — prefix sharding

**Code:** [`feature_to_minio.py`](../deploy/cdc-flink-minio-demo/feature-sink/feature_to_minio.py)

| Env var | Default | Unit |
|---|---|---|
| `MINIO_SHARD_DEPTH` | `1` | integer (0–2) |

MinIO (like S3) uses the key prefix to route requests to internal storage nodes. A
flat prefix (all objects under `streamforge/features/`) concentrates requests on a
single "partition" of the key space at high throughput, creating a hot-spot.

| `MINIO_SHARD_DEPTH=0` | `=1` (default) | `=2` |
|---|---|---|
| `<prefix>/<ts>.json` | `<prefix>/2024/01/15/<ts>.json` | `<prefix>/2024/01/<ts>.json` |
| Simple; fine for < 1 000 objects/day | Date-sharded; distributes load across 365 prefixes/year | Hour-sharded; 8 760 prefixes/year |
| Hot-spot risk at high write rates | Recommended for production | Use only for very high write rates |

Depth 1 also enables efficient time-range queries: `mc ls local/processed/streamforge/features/2024/01/15/`

---

## 7. MinIO — sink batching

**Code:** [`feature_to_minio.py`](../deploy/cdc-flink-minio-demo/feature-sink/feature_to_minio.py)

| Env var | Default | Unit |
|---|---|---|
| `SINK_BATCH_SIZE` | `1` | records |
| `SINK_BATCH_TIMEOUT_S` | `5.0` | seconds |
| `KAFKA_FETCH_MAX_BYTES` | `52428800` (50 MB) | bytes |
| `KAFKA_MAX_POLL_RECORDS` | `500` | records |

The sink accumulates records into an in-memory buffer. A PUT is issued when
`SINK_BATCH_SIZE` records are ready **or** `SINK_BATCH_TIMEOUT_S` seconds have elapsed,
whichever comes first. Records are committed to Kafka only after a successful PUT.

| Small batch (1–10 records) | Large batch (100–1 000 records) |
|---|---|
| Low delivery latency | Fewer MinIO PUT requests (lower cost) |
| Many small objects — poor Iceberg scan performance | Larger objects — better compression ratio |
| High MinIO request rate at scale | Lower MinIO request rate |
| Kafka offsets committed frequently | Offset commits only after large PUT |

**For Iceberg workloads:** set `SINK_BATCH_SIZE=500` and `SINK_BATCH_TIMEOUT_S=30` to
produce objects large enough that Iceberg does not need to compact immediately.

`KAFKA_MAX_POLL_RECORDS` limits how many records are fetched from Kafka per poll
loop. Set it ≥ `SINK_BATCH_SIZE` so a batch can be filled in a single poll.

---

## 8. Iceberg — write file size

**Code:** [`IcebergSinkFactory.java`](../stream-processor/src/main/java/ai/streamforge/processor/sink/IcebergSinkFactory.java)

| Env var | Default | Iceberg property |
|---|---|---|
| `ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES` | `134217728` (128 MB) | `write.target-file-size-bytes` |
| `ICEBERG_WRITE_PARALLELISM` | `-1` (Flink default) | `FlinkSink.writeParallelism()` |

The Iceberg writer rolls to a new data file when the current file reaches the target
size. This is the single most important compaction-related knob.

| Small target (e.g. 16 MB) | Large target (e.g. 512 MB) |
|---|---|
| More files — higher S3 PUT cost | Fewer files — lower S3 PUT cost |
| Faster compaction (each file is small) | Slower compaction (large files take longer to rewrite) |
| More metadata overhead in Iceberg catalog | Less metadata |
| Better for low-latency incremental queries | Better for full-scan analytics |

**Rule of thumb:** keep files between **64 MB and 512 MB** for Parquet. Below 64 MB,
the file-listing overhead on query planners (Spark/Trino) dominates. Above 512 MB,
compaction becomes expensive.

The `ICEBERG_WRITE_PARALLELISM` value controls how many Flink subtasks write Iceberg
files concurrently. Setting it lower than `FLINK_PARALLELISM` reduces the number of
simultaneously open files (and therefore simultaneous S3 connections), which helps
keep file sizes large.

---

## 9. Iceberg — write format & encoding

**Code:** [`IcebergSinkFactory.java`](../stream-processor/src/main/java/ai/streamforge/processor/sink/IcebergSinkFactory.java)

| Env var | Default | Iceberg property |
|---|---|---|
| `ICEBERG_WRITE_FORMAT` | `parquet` | `write.format.default` |
| `ICEBERG_WRITE_PARQUET_ROW_GROUP_SIZE_BYTES` | `134217728` (128 MB) | `write.parquet.row-group-size-bytes` |
| `ICEBERG_WRITE_PARQUET_PAGE_SIZE_BYTES` | `1048576` (1 MB) | `write.parquet.page-size-bytes` |

### Format comparison

| Format | Layout | Best for | Notes |
|---|---|---|---|
| `parquet` | Columnar | Analytics, wide tables, high compression | Default; best predicate pushdown |
| `avro` | Row-oriented | Row-level access, schema evolution | Faster appends; larger files |
| `orc` | Columnar | Hive/Trino workloads needing bloom filters | Good for string predicates |

### Parquet row-group size

A Parquet file contains one or more row-groups. The query engine reads one row-group
at a time; smaller row-groups improve selective reads but increase file metadata overhead.

| Small row-group (e.g. 16 MB) | Large row-group (≥ target file size) |
|---|---|
| Fine-grained predicate pushdown | Single row-group per file — minimal overhead |
| More metadata per file | Reader must scan more data for selective predicates |
| Higher memory pressure during write | Lower write memory |

### Parquet page size

Pages are the unit of encoding within a column chunk. Smaller pages improve
predicate recall but add page-header overhead.

Default (1 MB) is appropriate for most workloads. Reduce to 256 KB if queries
frequently access a small fraction of rows with selective predicates.

---

## 10. Iceberg — compaction

Iceberg does not auto-compact by default. The Flink streaming job produces many small
files (one per checkpoint per write parallelism). A separate periodic compaction job
using `RewriteDataFilesAction` is needed to merge them.

Key properties governing compaction behaviour:

| Env var | Default | Meaning |
|---|---|---|
| `ICEBERG_COMPACTION_TARGET_FILE_SIZE` | same as `ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES` | Target size for output files after compaction |
| `ICEBERG_COMPACTION_MIN_INPUT_FILES` | `5` | Min small files required before a group is compacted |

> These are currently used as documentation defaults for the `RewriteDataFilesAction`
> call in your maintenance job. Wire them in when you implement the scheduled
> compaction pipeline.

### Compaction tradeoffs

| Compact aggressively (low min-files, frequent runs) | Compact conservatively (high min-files, infrequent runs) |
|---|---|
| Files stay near target size — good query performance | Files stay small between runs — slower queries |
| Higher Iceberg catalog write amplification | Lower catalog overhead |
| More CPU/S3 I/O used by compaction | Less background resource usage |
| Suitable when fresh data is queried in near real-time | Suitable for batch-only workloads queried once per day |

### S3 prefix sharding and Iceberg

Iceberg stores data files under the warehouse path. On MinIO, all data files for a
table share a common prefix (e.g. `s3a://warehouse/streamforge/user_event_counts/data/`).
At high write rates this single prefix can become a hot-spot. Mitigations:

1. **Partition the table** by a time column (e.g. `window_start_ms` truncated to day).
   Each partition gets its own prefix sub-directory, distributing the key space.
2. **Increase `ICEBERG_S3_UPLOAD_THREADS`** to pipeline concurrent part uploads instead
   of serialising them on the hot prefix.
3. On self-hosted MinIO, deploy multiple drives/nodes and enable erasure coding to
   spread object placement across physical storage.

---

## 11. Quick-reference table

| Category | Env var | Default | Raise when… | Lower when… |
|---|---|---|---|---|
| **Flink** | `FLINK_PARALLELISM` | `-1` | Kafka topic has many partitions | Cluster has few slots |
| **Flink** | `CHECKPOINT_INTERVAL_MS` | `30 000` | Checkpoints are fast; RTO matters | Checkpoint I/O is a bottleneck |
| **Flink** | `CHECKPOINT_TIMEOUT_MS` | `60 000` | Checkpoints routinely time out | — |
| **Flink** | `CHECKPOINT_MIN_PAUSE_MS` | `0` | Checkpoint overhead exceeds 20% of cycle | Latency matters more than overhead |
| **Flink** | `CHECKPOINT_UNALIGNED` | `false` | Barrier stalls cause timeouts | Checkpoint size is already large |
| **Flink** | `RESTART_ATTEMPTS` | `3` | Errors are transient (network blips) | Errors are likely bugs |
| **Kafka** | `KAFKA_SINK_BATCH_SIZE_BYTES` | `16 384` | Throughput is bottleneck | Latency is bottleneck |
| **Kafka** | `KAFKA_SINK_LINGER_MS` | `5` | Batching rate is low | Consumers are latency-sensitive |
| **Kafka** | `KAFKA_SINK_COMPRESSION` | `none` | Network is the bottleneck | CPU is the bottleneck |
| **MinIO** | `MINIO_PART_SIZE` | `10 MB` | Large Parquet files (>128 MB) | Small feature events (<10 MB) |
| **MinIO** | `ICEBERG_S3_UPLOAD_THREADS` | `10` | High-bandwidth intra-DC network | MinIO CPU is saturated |
| **MinIO** | `MINIO_SHARD_DEPTH` | `1` | Very high write rate (>10 K obj/s) | Simple key layout desired |
| **MinIO** | `SINK_BATCH_SIZE` | `1` | PUT cost is high; events are small | Latency matters |
| **MinIO** | `SINK_BATCH_TIMEOUT_S` | `5.0` | Low event rate (batch rarely fills) | Latency matters |
| **Iceberg** | `ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES` | `128 MB` | Files are too small after compaction | Files are too large for selective reads |
| **Iceberg** | `ICEBERG_WRITE_FORMAT` | `parquet` | Analytics on wide tables | Row-level access patterns |
| **Iceberg** | `ICEBERG_WRITE_PARQUET_ROW_GROUP_SIZE_BYTES` | `128 MB` | Full-scan analytics dominate | Highly selective point queries |
| **Iceberg** | `ICEBERG_WRITE_PARALLELISM` | Flink default | Need large files; reduce open file count | High-throughput ingestion |

---

## 12. Recommended profiles

### Low-latency (near real-time dashboard)

```
CHECKPOINT_INTERVAL_MS=10000
CHECKPOINT_MODE=at_least_once
KAFKA_SINK_LINGER_MS=1
KAFKA_SINK_BATCH_SIZE_BYTES=4096
SINK_BATCH_SIZE=1
SINK_BATCH_TIMEOUT_S=1.0
ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES=33554432   # 32 MB — accept more compaction
```

### High-throughput (batch analytics)

```
FLINK_PARALLELISM=16
CHECKPOINT_INTERVAL_MS=60000
CHECKPOINT_UNALIGNED=true
KAFKA_SINK_BATCH_SIZE_BYTES=262144              # 256 KB
KAFKA_SINK_LINGER_MS=50
KAFKA_SINK_COMPRESSION=snappy
SINK_BATCH_SIZE=500
SINK_BATCH_TIMEOUT_S=30.0
MINIO_PART_SIZE=67108864                        # 64 MB
ICEBERG_S3_UPLOAD_THREADS=20
ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES=536870912  # 512 MB
ICEBERG_WRITE_PARALLELISM=4
```

### Cost-optimised (low S3 PUT cost)

```
KAFKA_SINK_COMPRESSION=zstd
SINK_BATCH_SIZE=1000
SINK_BATCH_TIMEOUT_S=60.0
MINIO_SHARD_DEPTH=1
ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES=268435456  # 256 MB
ICEBERG_COMPACTION_MIN_INPUT_FILES=10
```
