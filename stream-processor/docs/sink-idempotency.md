# Sink idempotency contract

This document describes idempotency guarantees for each sink used by
`CdcUserEventCountJob` and `BackfillJob`, and the steps required to make
re-runs safe.

---

## 1  Kafka sink (`user.event.counts`)

### Delivery guarantee

`KafkaSink` is configured without an explicit `DeliveryGuarantee`, which
defaults to **`AT_LEAST_ONCE`**.  Under normal operation Flink flushes
records on each checkpoint, but on task-manager failure or job restart
without a savepoint, records written since the last successful checkpoint
are replayed and **will be duplicated in the topic**.

### Natural dedup key

Every `UserEventCount` record carries a composite key that uniquely
identifies its aggregation bucket:

```
(userId, windowStartMs, windowEndMs)
```

Two records with the same tuple represent the same logical aggregate for
the same user in the same time window.  Any consumer of this topic **must
deduplicate on this key** before persisting downstream (e.g. upsert into a
database, or a Kafka Streams `reduce` keyed on the tuple).

### Backfill implications

`BackfillJob` writes to the same topic as the live job using a **separate
consumer group** (`flink-cdc-backfill` by default).  If the backfill
interval overlaps with events already processed by the live job, the topic
will contain duplicate records.  Consumers must tolerate this; the dedup
key above is sufficient to collapse them.

### Upgrading to exactly-once

To eliminate duplicates at the Kafka layer, switch to transactional
producers:

```java
KafkaSink.<UserEventCount>builder()
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .setTransactionalIdPrefix("streamforge-uecount-")
    ...
```

**Caveats:**
- Flink checkpointing interval must be shorter than the Kafka transaction
  timeout (`transaction.max.timeout.ms`, default 15 min).
- Consumers must set `isolation.level=read_committed`.
- Exactly-once applies only within a single job run; a backfill re-run
  still produces logical duplicates that must be deduplicated downstream.

---

## 2  Iceberg sink (`streamforge.user_event_counts`)

### Write mode

`IcebergSinkFactory` uses `FlinkSink.forRowData(...).append()`.  Iceberg's
append mode issues an **atomic Iceberg commit per Flink checkpoint**, making
each checkpoint exactly-once within a single job run.  Crash recovery
replays from the last successful checkpoint without double-writing, because
Flink's two-phase commit protocol aborts any uncommitted Iceberg snapshot.

### Re-run / backfill duplicates

A second job run (or a backfill run) over the same time range will append
**new data files** containing the same logical rows.  The Iceberg table will
have duplicate rows after the backfill.

To collapse them, run one of:

**Option A — DELETE + re-insert (simple, small tables):**
```sql
DELETE FROM streamforge.user_event_counts
WHERE window_start_ms >= <backfill_start>
  AND window_end_ms   <= <backfill_end>;
```
Then let the backfill job populate the clean data.

**Option B — MERGE INTO (preferred for large tables):**
```sql
MERGE INTO streamforge.user_event_counts t
USING backfill_staging s
ON t.user_id = s.user_id
   AND t.window_start_ms = s.window_start_ms
   AND t.window_end_ms   = s.window_end_ms
WHEN MATCHED THEN UPDATE SET t.event_count = s.event_count
WHEN NOT MATCHED THEN INSERT *;
```
Write the backfill output to a staging table first, then merge.

**Option C — Iceberg `REPLACE PARTITION`:**
If the table is partitioned by `window_start_ms` (day/hour), use Iceberg's
`ReplacePartitions` API to atomically swap the affected partitions.

### Partition recommendation

Partition the Iceberg table on a truncated window timestamp to make
range-scoped deletes and replacements cheap:

```java
PartitionSpec.builderFor(TABLE_SCHEMA)
    .hour("window_start_ms")
    .build()
```

---

## 3  Dead-letter queue (`cdc.dead.letter`)

The DLQ sink also uses `AT_LEAST_ONCE`.  DLQ records are diagnostic only
and are not deduplicated; consumers must tolerate replays.  The full
original payload is preserved in `DeadLetterEvent.rawPayload`, so re-sent
events are distinguishable by their content.

---

## Summary

| Sink           | Guarantee within one run | On re-run / backfill               | Dedup key                                   |
|----------------|--------------------------|-------------------------------------|---------------------------------------------|
| Kafka topic    | At-least-once            | Duplicate records in topic          | `(userId, windowStartMs, windowEndMs)`       |
| Iceberg table  | Exactly-once per ckpt    | Duplicate rows across Iceberg files | `(user_id, window_start_ms, window_end_ms)` |
| Kafka DLQ      | At-least-once            | Benign duplicates                   | N/A (diagnostic only)                        |
