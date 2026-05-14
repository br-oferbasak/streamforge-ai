# Iceberg Operations Guide — StreamForge Analytics Store

Iceberg is the primary analytics sink for StreamForge. This document covers the
table's design decisions, how maintenance runs, and how to operate it day-to-day.

---

## Table of contents

1. [Table design](#1-table-design)
2. [Partitioning strategy](#2-partitioning-strategy)
3. [Data-file compaction](#3-data-file-compaction)
4. [Snapshot retention policy](#4-snapshot-retention-policy)
5. [Manifest compaction](#5-manifest-compaction)
6. [Automated maintenance service](#6-automated-maintenance-service)
7. [Manual runbook](#7-manual-runbook)
8. [Querying the table](#8-querying-the-table)
9. [Schema evolution](#9-schema-evolution)

---

## 1. Table design

**Location:** `$ICEBERG_WAREHOUSE/streamforge/user_event_counts/`

**Schema:**

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `user_id` | STRING | NOT NULL | Debezium source `user_id` |
| `event_count` | LONG | NOT NULL | Insert events in the window |
| `window_start_ms` | LONG | NOT NULL | Tumbling window start (epoch ms, UTC) |
| `window_end_ms` | LONG | NOT NULL | Tumbling window end (epoch ms, UTC) |
| `event_date` | STRING | NOT NULL | `YYYY-MM-DD` UTC date of `window_start_ms` |

Field IDs 1–5 are stable. New optional columns must use field IDs ≥ 6 to preserve
backward compatibility with existing data files.

**Default write format:** Parquet (columnar, best for analytics on `event_count` and
`window_start_ms` aggregations).

---

## 2. Partitioning strategy

The table is partitioned by **`identity(event_date)`**.

Each calendar day (UTC) maps to exactly one directory under the table's data path:

```
warehouse/streamforge/user_event_counts/data/
  event_date=2024-01-14/
    00000-0-<uuid>-00001.parquet
    00000-0-<uuid>-00002.parquet
  event_date=2024-01-15/
    00000-0-<uuid>-00001.parquet
```

### Why identity on a string date rather than a timestamp transform?

| Design choice | Reason |
|---|---|
| **Identity on derived string** rather than `day(window_start_ms)` | Avoids a hidden integer-to-date transform that differs between engines; the string value is human-readable in the path and easy to filter with SQL `WHERE event_date BETWEEN '...' AND '...'` |
| **UTC calendar day** | Consistent with Debezium `ts_ms` (always UTC) and avoids DST boundary confusion |
| **Day granularity** rather than hour/month | One day is the natural compaction unit for streaming workloads with < 1 M events/day; reduces partition count compared to hourly |

### Partition pruning

Query engines that support Iceberg partition pruning (Spark, Trino, Flink SQL) will
skip all partitions outside the predicate range. Example:

```sql
-- Trino: scans only the 2024-01-15 partition directory
SELECT user_id, SUM(event_count)
FROM   iceberg.streamforge.user_event_counts
WHERE  event_date = '2024-01-15'
GROUP  BY user_id;
```

Without the `event_date` predicate, a full table scan is required (all partitions).

### Late data

If a Flink window fires late (e.g., `OUT_OF_ORDERNESS_SECONDS` is exceeded), the
record's `event_date` is derived from `window_start_ms` — the window's own clock,
not the wall clock. The record lands in the correct historical partition.
Iceberg's append model means late records do not rewrite existing files.

---

## 3. Data-file compaction

### Why it is needed

The Flink streaming writer creates one data file per checkpoint per write-parallelism
subtask. With default settings (30 s checkpoint, parallelism 2):
- 2 files × 2/min × 60 min × 24 h = **5 760 files per day**

A query planning a single day must list and evaluate all 5 760 files. File-listing
overhead in S3 / MinIO dominates query latency at this scale.

Target state: **8–32 files per day partition** (128 MB each at the 128 MB file target).

### How to run data-file compaction

Data-file compaction requires a compute engine. The recommended approach is a
one-shot Flink batch job using the Iceberg Flink actions:

```bash
# Run from stream-processor directory after mvn package

# Compact one specific partition (e.g. yesterday)
flink run \
  --class org.apache.iceberg.flink.actions.Actions \
  -Diceberg.catalog.type=hadoop \
  -Diceberg.warehouse=s3a://streamforge/warehouse \
  target/stream-processor-*.jar \
  rewrite-data-files \
  --table streamforge.user_event_counts \
  --target-file-size-bytes 134217728 \
  --where "event_date='$(date -u -d 'yesterday' +%Y-%m-%d)'"

# Compact all partitions with files smaller than 64 MB
flink run \
  --class org.apache.iceberg.flink.actions.Actions \
  -Diceberg.catalog.type=hadoop \
  -Diceberg.warehouse=s3a://streamforge/warehouse \
  target/stream-processor-*.jar \
  rewrite-data-files \
  --table streamforge.user_event_counts \
  --target-file-size-bytes 134217728 \
  --min-file-size-bytes 67108864 \
  --max-file-size-bytes 201326592
```

### When to compact

The `IcebergMaintenanceJob` (§6) reports which day partitions contain small files.
A partition needs compaction when:
- It has ≥ 5 files all smaller than 64 MB, **or**
- It has > 50 files of any size (manifest-list overhead)

Suggested schedule: **nightly at 02:00 UTC** for the previous day's partition.

### Compaction tuning

| Parameter | Default | Notes |
|---|---|---|
| `--target-file-size-bytes` | `134217728` (128 MB) | Match `ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES` |
| `--min-file-size-bytes` | `67108864` (64 MB) | Files above this are not considered small |
| `--max-file-size-bytes` | `201326592` (192 MB) | Files above this are not split |
| `--max-concurrent-file-group-rewrites` | `5` | Parallel compaction groups |

---

## 4. Snapshot retention policy

Every Flink checkpoint that writes to Iceberg creates a new snapshot. Default
checkpoint interval is 30 s → 2 snapshots/min → **2 880 snapshots/day**.

Retaining all snapshots indefinitely fills S3 with orphan manifest and delete files.
The retention policy is:

| Setting | Default | Effect |
|---|---|---|
| `SNAPSHOT_MAX_AGE_HOURS` | `168` (7 days) | Expire snapshots older than 7 days |
| `SNAPSHOT_RETAIN_LAST` | `10` | Never expire the 10 most recent snapshots, even if older than the age limit |

These ensure:
- **7-day point-in-time recovery** — any snapshot from the past week can be read via
  `AS OF SNAPSHOT <id>` or `AS OF TIMESTAMP '...'`.
- **Minimum 10 snapshots** — prevents accidental full expiry during low-traffic windows.

### How expiry works

`IcebergMaintenanceJob` calls `table.expireSnapshots()` which:
1. Marks snapshots for deletion (those older than `expiryBeforeMs` and beyond `retainLast`).
2. Deletes the associated manifest files that are no longer referenced by any live snapshot.
3. Deletes position-delete and equality-delete files that are only referenced by expired snapshots.
4. Does **not** delete data files — data files may be shared across snapshots (append-only).

Data files are only removed if **all** snapshots that reference them are expired AND
the data file itself is an orphan (not referenced by any current manifest). Orphan
file cleanup is a separate operation (not implemented in this pipeline; run
`RemoveOrphanFilesAction` if needed).

### Changing the retention window

```bash
# Extend to 30-day retention (for compliance requirements)
SNAPSHOT_MAX_AGE_HOURS=720 SNAPSHOT_RETAIN_LAST=50 \
  docker compose up iceberg-maintenance

# Reduce to 24-hour retention (cost-sensitive low-traffic env)
SNAPSHOT_MAX_AGE_HOURS=24 SNAPSHOT_RETAIN_LAST=5 \
  docker compose up iceberg-maintenance
```

---

## 5. Manifest compaction

Each snapshot appended by the Flink writer adds one new manifest file to the table's
manifest list. After 1 000 checkpoints the manifest list has 1 000 entries; Iceberg
must open and parse all of them to plan a scan.

`IcebergMaintenanceJob` runs `table.rewriteManifests()` which merges all manifest
files into the minimum number of manifests that fit within the target manifest size
(default 8 MB per manifest).

**Impact:** reduces scan-planning time proportionally to the manifest count reduction.
A table with 2 880 manifests/day compacted to ~1 per day reduces planning overhead by
~99% for single-day scans.

Manifest compaction is cheap (read-only on data files; writes only new manifest
files) and runs automatically with each maintenance cycle.

---

## 6. Automated maintenance service

The `iceberg-maintenance` container in `deploy/cdc-flink-minio-demo/docker-compose.yml`
runs `IcebergMaintenanceJob` on a loop:

```
startup → Phase 1 (expire snapshots)
        → Phase 2 (compact manifests)
        → Phase 3 (report small files)
        → sleep MAINTENANCE_INTERVAL_SECONDS
        → repeat
```

**Default interval:** 3 600 s (1 hour).

### Override the interval for development

```bash
MAINTENANCE_INTERVAL_SECONDS=60 docker compose up iceberg-maintenance
```

### Dry-run mode (report only, no commits)

```bash
DRY_RUN=true docker compose up iceberg-maintenance
```

Output example:
```
[maintenance] Running at 2024-01-15T02:00:00Z
MaintenanceResult{
  table=streamforge.user_event_counts,
  snapshotsExpired=2016,
  manifestsRewritten=2016→4,
  dataFiles=5760 (small=5760, 100.0%),
  totalSize=8640.0 MB,
  partitions=1,
  duration=4821ms
}
[WARN] 1 partition(s) have small files. Run data-file compaction — see docs/ICEBERG_OPERATIONS.md §3.
```

---

## 7. Manual runbook

### Trigger maintenance immediately

```bash
docker compose exec iceberg-maintenance \
  java -cp /opt/stream-processor/stream-processor-*.jar \
  ai.streamforge.processor.maintenance.IcebergMaintenanceJob
```

### Expire snapshots only (skip manifest compaction)

Use the Iceberg CLI directly against the table:

```bash
# Via Java (no Flink required)
java -cp stream-processor-*.jar \
  org.apache.iceberg.cli.Main expire-snapshots \
  --catalog-impl org.apache.iceberg.hadoop.HadoopCatalog \
  --warehouse s3a://streamforge/warehouse \
  --table streamforge.user_event_counts \
  --older-than "$(date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%S)"
```

### Roll back to a specific snapshot

```bash
# Find the snapshot ID to roll back to
java -cp stream-processor-*.jar org.apache.iceberg.cli.Main \
  list-snapshots --table streamforge.user_event_counts

# Roll back
java -cp stream-processor-*.jar org.apache.iceberg.cli.Main \
  rollback-to-snapshot \
  --table streamforge.user_event_counts \
  --snapshot-id <SNAPSHOT_ID>
```

### List all partitions

```bash
java -cp stream-processor-*.jar org.apache.iceberg.cli.Main \
  list-partitions \
  --catalog-impl org.apache.iceberg.hadoop.HadoopCatalog \
  --warehouse s3a://streamforge/warehouse \
  --table streamforge.user_event_counts
```

---

## 8. Querying the table

### Flink SQL

```sql
CREATE CATALOG iceberg_catalog WITH (
  'type'        = 'iceberg',
  'catalog-type'= 'hadoop',
  'warehouse'   = 's3a://streamforge/warehouse',
  'fs.s3a.endpoint'          = 'http://minio:9000',
  'fs.s3a.access.key'        = 'minioadmin',
  'fs.s3a.secret.key'        = 'minioadmin',
  'fs.s3a.path.style.access' = 'true'
);

USE CATALOG iceberg_catalog;

-- Time-range scan (partition-pruned)
SELECT   user_id, SUM(event_count) AS total_events
FROM     streamforge.user_event_counts
WHERE    event_date BETWEEN '2024-01-01' AND '2024-01-07'
GROUP BY user_id
ORDER BY total_events DESC
LIMIT    10;

-- Point-in-time read (snapshot travel)
SELECT * FROM streamforge.user_event_counts
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 00:00:00';
```

### PyIceberg (Python analytics)

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("streamforge", **{
    "type": "hadoop",
    "warehouse": "s3a://streamforge/warehouse",
})

table = catalog.load_table("streamforge.user_event_counts")

# Scan a single partition
df = table.scan(
    row_filter="event_date = '2024-01-15'"
).to_arrow().to_pandas()

print(df.groupby("user_id")["event_count"].sum().sort_values(ascending=False).head(10))
```

---

## 9. Schema evolution

Adding a new column to the Iceberg table is safe and backward compatible because
Iceberg tracks field IDs, not column positions.

### Adding an optional column

1. Use field ID ≥ 6 (IDs 1–5 are reserved for the current schema).
2. Update `IcebergSinkFactory.TABLE_SCHEMA` with the new field.
3. Update `IcebergSinkFactory.toRowData()` to populate the new field at position N.
4. The `ensureTable()` call will not re-create the table if it already exists.
   Run an explicit schema update:

```java
Table table = catalog.loadTable(tableId);
table.updateSchema()
     .addColumn("new_column", Types.StringType.get())
     .commit();
```

5. Old data files without the new column will return `null` for that field when read.
   Declare the new field as `optional` in `TABLE_SCHEMA` to avoid validation errors.

### Partition evolution (adding a second partition field)

If you need to partition by an additional field (e.g., `user_id` for high-cardinality
users), use Iceberg's partition evolution:

```java
table.updateSpec()
     .addField(Expressions.bucket("user_id", 32))
     .commit();
```

New writes use the new spec; old partitions remain readable under the old spec.
Query planners handle mixed-spec tables transparently.

> **Do not** rename the `event_date` partition column or change its transform — that
> would break partition pruning for existing query plans and compaction scripts.
