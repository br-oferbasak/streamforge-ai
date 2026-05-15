"""
Storage layout constants and path builders for StreamForge feature store.

Two layouts are defined:

  OFFLINE (Batch)
  ───────────────
  MinIO bucket: processed
  streamforge/features/
    YYYY/MM/DD/                ← UTC date partition
      HH/                      ← UTC hour partition
        part-{shard:05d}.jsonl ← NDJSON; rows sorted by window_start_ms
    _manifest/
      YYYY-MM-DD.json          ← daily manifest (see DayManifest)

  ONLINE (Low-latency, Redis)
  ───────────────────────────
  sf:feat:u:{user_id}          → HASH  last window aggregates + metadata
  sf:feat:u:{user_id}:hist     → ZSET  score=window_start_ms, member=JSON payload
  sf:feat:batch:{batch_id}     → STRING JSON array for bulk responses (short TTL)

Key envelope (HASH fields):
  event_count      int
  window_start_ms  int (epoch ms, inclusive)
  window_end_ms    int (epoch ms, exclusive)
  event_rate       float  events/second
  updated_at       float  unix timestamp when this entry was written

TTLs (seconds):
  ONLINE_USER_TTL      7 days   – stale user expiry
  ONLINE_HIST_TTL     24 hours  – rolling window history
  ONLINE_BATCH_TTL    60 s      – ephemeral bulk response cache
"""

from __future__ import annotations

import datetime
from typing import Optional

# ---------------------------------------------------------------------------
# Offline – MinIO / S3 paths
# ---------------------------------------------------------------------------

OFFLINE_BUCKET = "processed"
OFFLINE_PREFIX = "streamforge/features"
OFFLINE_MANIFEST_PREFIX = "streamforge/features/_manifest"


def offline_hour_prefix(dt: datetime.datetime) -> str:
    """Return the MinIO prefix for one UTC hour, e.g. streamforge/features/2026/05/14/09/"""
    return f"{OFFLINE_PREFIX}/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/{dt.hour:02d}/"


def offline_day_prefix(dt: datetime.datetime) -> str:
    """Return the MinIO prefix for one UTC day."""
    return f"{OFFLINE_PREFIX}/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"


def offline_part_key(dt: datetime.datetime, shard: int = 0) -> str:
    """Canonical object key for a shard file inside an hour partition."""
    return f"{offline_hour_prefix(dt)}part-{shard:05d}.jsonl"


def offline_manifest_key(date: datetime.date) -> str:
    return f"{OFFLINE_MANIFEST_PREFIX}/{date.isoformat()}.json"


# ---------------------------------------------------------------------------
# Online – Redis key schema
# ---------------------------------------------------------------------------

ONLINE_USER_TTL = 7 * 86_400       # seconds
ONLINE_HIST_TTL = 24 * 3_600       # seconds
ONLINE_BATCH_TTL = 60              # seconds

_NS = "sf:feat"


def online_user_key(user_id: int | str) -> str:
    """Hash key for the latest feature snapshot of a user."""
    return f"{_NS}:u:{user_id}"


def online_hist_key(user_id: int | str) -> str:
    """Sorted-set key for the per-user window history."""
    return f"{_NS}:u:{user_id}:hist"


def online_batch_key(batch_id: str) -> str:
    """Ephemeral key for a cached bulk-lookup response."""
    return f"{_NS}:batch:{batch_id}"


# ---------------------------------------------------------------------------
# Shared record dict schema (both paths emit the same dict shape)
# ---------------------------------------------------------------------------

def record_to_dict(
    user_id: int,
    window_start_ms: int,
    window_end_ms: int,
    event_count: int,
) -> dict:
    duration_s = (window_end_ms - window_start_ms) / 1000.0
    return {
        "user_id": user_id,
        "window_start_ms": window_start_ms,
        "window_end_ms": window_end_ms,
        "event_count": event_count,
        "event_rate": round(event_count / duration_s, 6) if duration_s > 0 else 0.0,
    }
