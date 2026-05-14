"""
Kafka → Redis Online Store Writer
===================================
Consumes the Flink feature output topic and keeps the Redis online store
up to date with the latest per-user feature snapshot + rolling history.

Topic consumed:   streamforge.features.user_event_counts
                  (JSON messages produced by CdcUserEventCountJob)

Writes per message
------------------
  HSET  sf:feat:u:{user_id}  event_count … window_start_ms … (+ EXPIRE)
  ZADD  sf:feat:u:{user_id}:hist  score=window_start_ms  member=JSON  (+ EXPIRE)
  ZREMRANGEBYSCORE  — prune history older than 24 h

Run
---
  python kafka_to_online_store.py                  # uses env defaults
  KAFKA_BOOTSTRAP=broker:9092 python kafka_to_online_store.py
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from typing import Optional

from layout_spec import (
    ONLINE_HIST_TTL,
    ONLINE_USER_TTL,
    online_hist_key,
    online_user_key,
    record_to_dict,
)


# ---------------------------------------------------------------------------
# Config (env vars)
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC      = os.environ.get("KAFKA_FEATURE_TOPIC", "streamforge.features.user_event_counts")
KAFKA_GROUP      = os.environ.get("KAFKA_CONSUMER_GROUP", "online-store-writer")
REDIS_HOST       = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT       = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB         = int(os.environ.get("REDIS_DB", "0"))
REDIS_PASSWORD   = os.environ.get("REDIS_PASSWORD") or None

# Keep at most this many history windows per user in the sorted set
MAX_HIST_WINDOWS = int(os.environ.get("MAX_HIST_WINDOWS", "2880"))  # 24h @ 30s windows


# ---------------------------------------------------------------------------
# Redis writer
# ---------------------------------------------------------------------------

class OnlineStoreWriter:
    """Writes one Kafka record into the Redis online store atomically."""

    def __init__(self):
        import redis  # type: ignore
        self._r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
            password=REDIS_PASSWORD, decode_responses=True,
            socket_timeout=2.0,
        )
        self._r.ping()
        print(f"[writer] connected to Redis {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

    def write(self, user_id: int, window_start_ms: int, window_end_ms: int, event_count: int) -> None:
        row = record_to_dict(user_id, window_start_ms, window_end_ms, event_count)
        ukey = online_user_key(user_id)
        hkey = online_hist_key(user_id)

        # Earliest timestamp to keep in history (24 h rolling window)
        cutoff_ms = window_start_ms - ONLINE_HIST_TTL * 1_000

        pipe = self._r.pipeline(transaction=True)

        # 1. Upsert latest snapshot
        pipe.hset(ukey, mapping={
            "event_count":     str(row["event_count"]),
            "window_start_ms": str(row["window_start_ms"]),
            "window_end_ms":   str(row["window_end_ms"]),
            "event_rate":      str(row["event_rate"]),
            "updated_at":      str(time.time()),
        })
        pipe.expire(ukey, ONLINE_USER_TTL)

        # 2. Append to history sorted set (score = window_start_ms)
        pipe.zadd(hkey, {json.dumps(row): window_start_ms})
        pipe.expire(hkey, ONLINE_HIST_TTL)

        # 3. Prune history entries older than 24 h
        pipe.zremrangebyscore(hkey, "-inf", cutoff_ms)

        # 4. Cap the sorted set size to avoid unbounded growth
        pipe.zremrangebyrank(hkey, 0, -(MAX_HIST_WINDOWS + 2))

        pipe.execute()


# ---------------------------------------------------------------------------
# Kafka consumer loop
# ---------------------------------------------------------------------------

def _parse_message(raw_value: bytes) -> Optional[dict]:
    """Parse a Kafka message from CdcUserEventCountJob into a dict."""
    try:
        obj = json.loads(raw_value)
    except (json.JSONDecodeError, TypeError):
        return None

    user_id = obj.get("user_id", obj.get("userId"))
    ws      = obj.get("window_start_ms", obj.get("window_start", obj.get("windowStart")))
    we      = obj.get("window_end_ms",   obj.get("window_end",   obj.get("windowEnd")))
    ec      = obj.get("event_count",     obj.get("eventCount"))

    if None in (user_id, ws, we, ec):
        return None

    return {"user_id": int(user_id), "window_start_ms": int(ws),
            "window_end_ms": int(we), "event_count": int(ec)}


def run(max_messages: Optional[int] = None) -> None:
    """
    Consume Kafka messages and write each one to the online store.

    max_messages: stop after N messages (useful for integration tests).
    """
    try:
        from kafka import KafkaConsumer  # type: ignore
    except ImportError:
        print("[writer] kafka-python not installed; install it with: pip install kafka-python")
        sys.exit(1)

    writer = OnlineStoreWriter()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: b,   # raw bytes; we parse ourselves
        consumer_timeout_ms=5_000,
    )
    print(f"[writer] consuming {KAFKA_TOPIC} from {KAFKA_BOOTSTRAP} (group={KAFKA_GROUP})")

    processed = 0
    errors    = 0

    def _shutdown(sig, frame):
        print(f"\n[writer] shutting down — processed={processed} errors={errors}")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for msg in consumer:
        parsed = _parse_message(msg.value)
        if parsed is None:
            errors += 1
            continue

        try:
            writer.write(**parsed)
            processed += 1
        except Exception as exc:
            errors += 1
            print(f"[writer] warn: Redis write failed: {exc}")

        if processed % 1_000 == 0 and processed > 0:
            print(f"[writer] processed={processed:,} errors={errors}")

        if max_messages and processed >= max_messages:
            break

    consumer.close()
    print(f"[writer] done — processed={processed:,} errors={errors}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()
