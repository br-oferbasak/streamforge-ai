"""
Kafka → MinIO feature sink.

Tuning knobs (environment variables)
-------------------------------------
Kafka consumer:
  KAFKA_BOOTSTRAP_SERVERS  — broker list, default kafka:9092
  KAFKA_TOPIC              — source topic, default streamforge.features.user_event_counts
  KAFKA_GROUP_ID           — consumer group, default streamforge-feature-minio-sink
  KAFKA_FETCH_MAX_BYTES    — fetch.max.bytes per poll, default 52428800 (50 MB)
                             Larger → fewer round-trips at the cost of memory.
  KAFKA_MAX_POLL_RECORDS   — max.poll.records per poll, default 500
                             Higher values increase throughput; lower values reduce
                             end-to-end latency and rebalance pause time.

Batching / flushing:
  SINK_BATCH_SIZE          — number of records to buffer before a single PUT, default 1
                             Batching amortises per-PUT latency but increases
                             in-flight memory and worst-case delivery delay.
  SINK_BATCH_TIMEOUT_S     — max seconds to hold a partial batch, default 5.0
                             Lower → closer to record-level latency.
                             Higher → better compression ratio and fewer S3 requests.

MinIO / S3:
  MINIO_ENDPOINT           — host:port, default minio:9000
  MINIO_ACCESS_KEY         — default minioadmin
  MINIO_SECRET_KEY         — default minioadmin
  MINIO_BUCKET             — bucket name, default processed
  MINIO_PREFIX             — object key prefix, default streamforge/features
  MINIO_SECURE             — use HTTPS, default false
  MINIO_PART_SIZE          — multipart part size in bytes, default 10485760 (10 MB)
                             Must be ≥ 5 MB (MinIO/AWS minimum). Larger parts improve
                             PUT throughput for big payloads at the cost of memory.
                             Objects smaller than this are uploaded as a single PUT.

Prefix sharding:
  MINIO_SHARD_DEPTH        — number of date-hierarchy levels prepended to the key:
                               0 → <prefix>/<timestamp>.json
                               1 → <prefix>/2024/01/15/<timestamp>.json  (default)
                               2 → <prefix>/2024/01/<timestamp>.json
                             Sharding distributes load across MinIO/S3 key-space
                             prefixes, which matters for high-throughput workloads
                             where a flat prefix becomes a hot-spot.
"""
import json
import os
import time
from datetime import datetime, timezone
from io import BytesIO

from kafka import KafkaConsumer
from minio import Minio


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def build_object_key(prefix: str, shard_depth: int) -> str:
    now = datetime.now(timezone.utc)
    ts = int(time.time() * 1000)
    if shard_depth >= 1:
        date_path = now.strftime("%Y/%m/%d")
        return f"{prefix}/{date_path}/{now.strftime('%H%M%S')}-{ts}.json"
    return f"{prefix}/{now.strftime('%Y%m%d-%H%M%S')}-{ts}.json"


def flush_batch(client: Minio, bucket: str, prefix: str,
                shard_depth: int, part_size: int, records: list) -> None:
    """Write a batch of records as a single JSON-lines object to MinIO."""
    lines = "\n".join(json.dumps(r, separators=(",", ":")) for r in records) + "\n"
    data = lines.encode("utf-8")
    key = build_object_key(prefix, shard_depth)
    client.put_object(
        bucket_name=bucket,
        object_name=key,
        data=BytesIO(data),
        length=len(data),
        content_type="application/json",
        part_size=part_size,
    )
    print(f"[SINK] Wrote {len(records)} record(s) ({len(data)} bytes) → minio://{bucket}/{key}")


def main() -> None:
    kafka_servers   = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic     = env("KAFKA_TOPIC",             "streamforge.features.user_event_counts")
    kafka_group     = env("KAFKA_GROUP_ID",          "streamforge-feature-minio-sink")
    fetch_max_bytes = int(env("KAFKA_FETCH_MAX_BYTES",  "52428800"))
    max_poll_records = int(env("KAFKA_MAX_POLL_RECORDS", "500"))

    minio_endpoint  = env("MINIO_ENDPOINT",   "minio:9000")
    minio_access    = env("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret    = env("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket    = env("MINIO_BUCKET",     "processed")
    minio_prefix    = env("MINIO_PREFIX",     "streamforge/features")
    minio_secure    = env("MINIO_SECURE",     "false").lower() == "true"
    part_size       = int(env("MINIO_PART_SIZE",   "10485760"))  # 10 MB
    shard_depth     = int(env("MINIO_SHARD_DEPTH", "1"))

    batch_size      = int(env("SINK_BATCH_SIZE",    "1"))
    batch_timeout_s = float(env("SINK_BATCH_TIMEOUT_S", "5.0"))

    print(
        f"[SINK] Starting Kafka→MinIO sink  topic={kafka_topic}  endpoint={minio_endpoint}\n"
        f"       batch_size={batch_size}  batch_timeout={batch_timeout_s}s  "
        f"part_size={part_size // 1024 // 1024} MB  shard_depth={shard_depth}"
    )

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_servers],
        group_id=kafka_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # commit only after successful PUT
        fetch_max_bytes=fetch_max_bytes,
        max_poll_records=max_poll_records,
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    minio_client = Minio(
        endpoint=minio_endpoint,
        access_key=minio_access,
        secret_key=minio_secret,
        secure=minio_secure,
    )
    ensure_bucket(minio_client, minio_bucket)

    batch: list = []
    batch_start = time.monotonic()

    for msg in consumer:
        raw_value = msg.value
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            payload = {"raw": raw_value}

        payload["sink_received_at"] = datetime.now(timezone.utc).isoformat()
        batch.append(payload)

        elapsed = time.monotonic() - batch_start
        if len(batch) >= batch_size or elapsed >= batch_timeout_s:
            flush_batch(minio_client, minio_bucket, minio_prefix,
                        shard_depth, part_size, batch)
            consumer.commit()
            batch = []
            batch_start = time.monotonic()


if __name__ == "__main__":
    main()
