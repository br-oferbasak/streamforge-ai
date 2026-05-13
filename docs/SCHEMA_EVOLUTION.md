# Schema Evolution Policy — Debezium → Kafka → Flink → Sink

This document defines the compatibility policy for schema changes that flow through
the StreamForge CDC pipeline, explains how each change type is handled end-to-end,
and provides a step-by-step reprocessing runbook for incompatible changes.

---

## 1. Compatibility Policy

### 1.1 Change taxonomy

| Change type | Example DDL | Default compatibility | Flink job action |
|---|---|---|---|
| **ADD COLUMN (nullable)** | `ALTER TABLE t ADD COLUMN session_id VARCHAR(64)` | **FULL** | Null-fill for pre-DDL events; pass through |
| **DROP COLUMN (optional)** | `ALTER TABLE t DROP COLUMN ip_address` | **FORWARD** | Null-fill; pass through |
| **DROP COLUMN (required)** | `ALTER TABLE t DROP COLUMN user_id` | **BREAKING** | Route to DLQ |
| **RENAME COLUMN (registered alias)** | + `COLUMN_ALIASES=new_name=old_name` | **BACKWARD** | Alias resolves; pass through |
| **RENAME COLUMN (no alias)** | No alias registered | **BREAKING** | Route to DLQ |
| **TYPE WIDENING** | `ALTER TABLE t MODIFY created_at BIGINT` | **BACKWARD** | Annotate event; pass through |
| **TYPE NARROWING** | `ALTER TABLE t MODIFY amount INT` | **BREAKING** | Route to DLQ |
| **INCOMPATIBLE TYPE** | `ALTER TABLE t MODIFY ts VARCHAR(32)` | **BREAKING** | Route to DLQ |

### 1.2 Compatibility definitions

- **FULL** — new schema reads old data AND old schema reads new data.  Zero action required.
- **BACKWARD** — new schema reads old data.  Flink job can continue with the current binary.
- **FORWARD** — old schema reads new data.  Unknown fields are silently ignored by `@JsonIgnoreProperties`.
- **BREAKING** — neither direction is safe.  Events land in the dead-letter queue until the issue is remediated.

### 1.3 Required columns

`user_id` is currently the only **required** column.  If it is absent after all alias
normalization, the event is unconditionally routed to the DLQ regardless of op type.

---

## 2. How each change is handled in code

### 2.1 Column add

`SchemaEvolutionHandler.normalizeRow()` null-fills fields absent in the JSON.
`CdcEvent.UserEventRow` has `@JsonIgnoreProperties(ignoreUnknown = true)` so new columns
in future schema versions do not cause parsing failures.

**New columns introduced so far:**

| Column | Version | Handling |
|---|---|---|
| `session_id` | V2 | null-filled in V1 events |
| `ip_address` | V2 | null-filled in V1 events |
| `metadata` | V3 | null-filled in V1/V2 events |

### 2.2 Column drop

`resolveText()` and `resolveTimestamp()` return `null` when neither the canonical name nor
any registered alias is present.  If the missing column is `user_id`, a
`SchemaChange(DROP_COLUMN)` is added to `CdcEvent.detectedChanges` and `SchemaEvolutionFilter`
routes the event to the DLQ.

### 2.3 Column rename

**With a registered alias** (BACKWARD compatible):

```
# At job start, or via savepoint + redeploy:
COLUMN_ALIASES=customer_id=user_id,kind=event_type
```

`ColumnAliasRegistry` resolves `customer_id` → `user_id` transparently.  The main job
continues processing without interruption.

**Without a registered alias** (BREAKING):

`user_id` is absent → `DROP_COLUMN` change detected → event routed to DLQ.
Follow the **Reprocessing Runbook** in §4.

### 2.4 Type widening (BACKWARD compatible)

`TypeCompatibilityChecker.check()` compares the JSON node value against the expected type.
When `created_at` overflows a 32-bit int, a `SchemaChange(WIDEN_TYPE)` is appended to
`detectedChanges`.  The event passes through normally — the value is read as a `long`.
The schema version is promoted to `V3` when this overflow is detected.

### 2.5 Incompatible type change (BREAKING)

When `TypeCompatibilityChecker` detects a `INCOMPATIBLE_TYPE` or `NARROW_TYPE` change,
`SchemaEvolutionFilter` routes the event to the DLQ with the affected column and old/new
type hints.

---

## 3. Dead-letter queue (DLQ) structure

Topic: `cdc.dead.letter` (configurable via `KAFKA_DLQ_TOPIC`)

Each `DeadLetterEvent` carries:

| Field | Purpose |
|---|---|
| `rawPayload` | Original Debezium JSON for replay |
| `errorMessage` | Human-readable description |
| `changeType` | `DROP_COLUMN`, `RENAME_COLUMN`, `INCOMPATIBLE_TYPE`, `NARROW_TYPE` |
| `schemaVersion` | Version detected before rejection (`V1`, `V2`, `V3`, `UNKNOWN`) |
| `affectedColumn` | Column that triggered the rejection |
| `compatibilityLevel` | Always `BREAKING` for DLQ entries |
| `failedAtMs` | Epoch-ms of rejection |

---

## 4. Reprocessing Runbook

### Case A: Column rename (most common)

**Symptoms:** DLQ spike; `changeType=DROP_COLUMN`, `affectedColumn=user_id` (or other required column).

**Remediation:**

```bash
# 1. Identify the new column name from the Debezium schema history topic
#    (or from the DBA who ran the ALTER TABLE).
NEW_COLUMN=customer_id

# 2. Take a savepoint of the running Flink job.
flink savepoint <job-id> s3://your-bucket/savepoints/

# 3. Redeploy the main job with the alias env var set.
#    The alias registry is loaded at open() time — no recompile needed.
COLUMN_ALIASES="${NEW_COLUMN}=user_id" \
  flink run -s <savepoint-path> target/stream-processor-*.jar

# 4. Submit the DlqReprocessingJob (one-shot) to replay DLQ events
#    through the updated alias registry back into the main source topic.
COLUMN_ALIASES="${NEW_COLUMN}=user_id" \
KAFKA_DLQ_TOPIC=cdc.dead.letter \
KAFKA_SOURCE_TOPIC=cdc.streamforge.user_events \
  flink run target/stream-processor-*.jar \
  --class ai.streamforge.processor.reprocessing.DlqReprocessingJob

# 5. Monitor cdc.dead.letter.unresolved — events here could not be recovered.
#    Manual review required for those.
```

### Case B: Type widening (INT → BIGINT)

No action required.  `WIDEN_TYPE` is BACKWARD compatible.
The `detectedChanges` list on the event is available for observability
(emit it to a metrics topic or log it at DEBUG level if needed).

### Case C: Incompatible type change (e.g., timestamp column changed to VARCHAR)

**Symptoms:** DLQ spike; `changeType=INCOMPATIBLE_TYPE`.

This is a source-schema bug.  Reprocessing is only meaningful after the source
is corrected or a coercion transform is introduced upstream.

```bash
# Option 1: Fix the source schema and resume from the pre-DDL savepoint.
flink run -s <pre-ddl-savepoint> target/stream-processor-*.jar

# Option 2: Full replay — reset consumer group offset and rerun from earliest.
#   This reprocesses all history; use only if the window of bad events is small.
kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group flink-cdc-user-event-count --reset-offsets \
  --topic cdc.streamforge.user_events --to-earliest --execute

flink run target/stream-processor-*.jar
```

### Case D: Required column dropped permanently

The source schema must be updated (column restored or renamed).
If the column is permanently removed, update `SchemaEvolutionHandler.normalizeRow()`
to not require it, then redeploy and replay via `DlqReprocessingJob`.

---

## 5. Adding a new required column

1. Add the field to `CdcEvent.UserEventRow` with `@JsonProperty`.
2. Add resolution logic to `SchemaEvolutionHandler.normalizeRow()`.
3. Add a null-check in `SchemaEvolutionFilter.classify()` (analogous to the `user_id` check).
4. Bump `SchemaVersion` if needed.
5. Update the compatibility table in §1.1.

---

## 6. Adding a new alias at deploy time

Add to the `COLUMN_ALIASES` environment variable (no recompile needed):

```
COLUMN_ALIASES=old_col=canonical,another_old=another_canonical
```

The alias registry is built at `DeserializationSchema.open()` time from this env var
plus the built-in aliases.  A savepoint-restore cycle picks up the new aliases without
losing Flink state.

---

## 7. Schema version summary

| Version | Fields | Detected when |
|---|---|---|
| V1 | `user_id`, `event_type`, `created_at` | Core fields present, no V2/V3 fields |
| V2 | + `session_id`, `ip_address` | `session_id` or `ip_address` present |
| V3 | + `metadata` blob, or `created_at` > INT_MAX | `metadata` present, or timestamp overflows int |
| UNKNOWN | No recognizable key field | `after` is null, or no known field found |
