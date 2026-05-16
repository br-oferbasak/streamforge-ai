import json
import os
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

from kafka import KafkaConsumer
from minio import Minio

# Optional lineage tracking — enabled when the lineage package is on the path.
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
try:
    from lineage.tracker import lineage_run, kafka_dataset, minio_dataset
    from lineage.emitter import default_emitter as _lineage_default_emitter
    _LINEAGE_ENABLED = True
except ImportError:
    _LINEAGE_ENABLED = False


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def build_object_key(prefix: str) -> str:
    now = datetime.now(timezone.utc)
    return f"{prefix}/{now.strftime('%Y/%m/%d/%H%M%S')}-{int(time.time() * 1000)}.json"


def main() -> None:
    kafka_servers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = env("KAFKA_TOPIC", "streamforge.features.user_event_counts")
    kafka_group = env("KAFKA_GROUP_ID", "streamforge-feature-minio-sink")

    minio_endpoint = env("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = env("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = env("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket = env("MINIO_BUCKET", "processed")
    minio_prefix = env("MINIO_PREFIX", "streamforge/features")

    print(f"[SINK] Starting Kafka->MinIO sink topic={kafka_topic} endpoint={minio_endpoint}")

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_servers],
        group_id=kafka_group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    minio_client = Minio(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )
    ensure_bucket(minio_client, minio_bucket)

    # Emit a START lineage event when the sink begins consuming.
    if _LINEAGE_ENABLED:
        _emitter = _lineage_default_emitter()
        _input_ds = [kafka_dataset(kafka_topic, kafka_servers)]
        _output_ds = [minio_dataset(minio_bucket, minio_prefix, minio_endpoint)]
        _lineage_ctx = lineage_run(
            "streamforge", "feature-sink",
            inputs=_input_ds, outputs=_output_ds,
            emitter=_emitter,
        )
        _lineage_ctx.__enter__()

    try:
        for msg in consumer:
            raw_value = msg.value
            try:
                payload = json.loads(raw_value)
            except json.JSONDecodeError:
                payload = {"raw": raw_value}

            payload["sink_received_at"] = datetime.now(timezone.utc).isoformat()
            data = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
            key = build_object_key(minio_prefix)

            minio_client.put_object(
                bucket_name=minio_bucket,
                object_name=key,
                data=BytesIO(data),
                length=len(data),
                content_type="application/json",
            )

            print(f"[SINK] Wrote feature event to minio://{minio_bucket}/{key}")
    except Exception as exc:
        if _LINEAGE_ENABLED:
            _lineage_ctx.__exit__(type(exc), exc, exc.__traceback__)
        raise
    else:
        if _LINEAGE_ENABLED:
            _lineage_ctx.__exit__(None, None, None)


if __name__ == "__main__":
    main()
