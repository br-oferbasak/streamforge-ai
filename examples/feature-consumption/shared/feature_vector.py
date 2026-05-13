"""
Shared FeatureVector dataclass used by both the offline batch reader
and the online low-latency reader.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class FeatureVector:
    """
    One user's aggregated feature snapshot for a single tumbling window.

    Produced by CdcUserEventCountJob (Flink) and stored in two physical
    layouts depending on the access pattern:

      Offline (Iceberg / MinIO raw JSON)
        Organised by event_date partition — fast range scans.

      Online (MinIO serving layout + Redis)
        Organised by user_id shard prefix — fast point lookups.
    """
    user_id: str
    event_count: int
    event_rate: float          # events / second within the window
    window_start_ms: int       # epoch ms (UTC)
    window_end_ms: int         # epoch ms (UTC)
    event_date: str            # YYYY-MM-DD (UTC), partition key in Iceberg
    materialized_at: str = ""  # ISO-8601; populated by the online serving path

    # ── Derived helpers ──────────────────────────────────────────────────────

    @property
    def window_duration_s(self) -> float:
        return (self.window_end_ms - self.window_start_ms) / 1000.0

    @property
    def feature_array(self) -> list[float]:
        """Numeric feature vector ready for ML model input."""
        return [float(self.event_count), self.event_rate, float(self.window_duration_s)]

    # ── Constructors ─────────────────────────────────────────────────────────

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "FeatureVector":
        start = int(d.get("window_start_ms", 0))
        end   = int(d.get("window_end_ms", start + 60_000))
        dur_s = max((end - start) / 1000.0, 1e-6)
        count = int(d.get("event_count", 0))

        # Derive event_date from window_start_ms if not stored explicitly.
        event_date = d.get("event_date") or _ms_to_date(start)

        return cls(
            user_id=str(d["user_id"]),
            event_count=count,
            event_rate=count / dur_s,
            window_start_ms=start,
            window_end_ms=end,
            event_date=event_date,
            materialized_at=d.get("materialized_at", ""),
        )

    @classmethod
    def synthetic(cls, user_id: str, seed: int = 0) -> "FeatureVector":
        """Generate a deterministic synthetic record (no external services needed)."""
        import random
        rng = random.Random(hash(user_id) ^ seed)
        now_ms = int(time.time() * 1000)
        start  = now_ms - (now_ms % 60_000)   # align to minute boundary
        count  = rng.randint(1, 200)
        return cls(
            user_id=user_id,
            event_count=count,
            event_rate=count / 60.0,
            window_start_ms=start,
            window_end_ms=start + 60_000,
            event_date=_ms_to_date(start),
        )

    # ── Serialisation ────────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_json(cls, s: str) -> "FeatureVector":
        return cls.from_dict(json.loads(s))

    # ── MinIO serving-layout key helpers ─────────────────────────────────────

    @property
    def serving_key(self) -> str:
        """
        MinIO object key under the serving layout.
        Pattern: features/serving/latest/{shard}/{user_id}.json
        where shard = first 2 chars of user_id (hex-normalised for safety).
        """
        shard = _shard(self.user_id)
        safe  = self.user_id.replace("/", "_").replace(" ", "_")
        return f"features/serving/latest/{shard}/{safe}.json"

    @staticmethod
    def serving_key_for(user_id: str) -> str:
        shard = _shard(user_id)
        safe  = user_id.replace("/", "_").replace(" ", "_")
        return f"features/serving/latest/{shard}/{safe}.json"

    # ── Redis key helpers ────────────────────────────────────────────────────

    @property
    def redis_key(self) -> str:
        return f"feature:v1:{self.user_id}"

    @staticmethod
    def redis_key_for(user_id: str) -> str:
        return f"feature:v1:{user_id}"


# ── Module-level helpers ─────────────────────────────────────────────────────

def _ms_to_date(epoch_ms: int) -> str:
    """Return a YYYY-MM-DD string (UTC) for an epoch-millisecond timestamp."""
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def _shard(user_id: str) -> str:
    """2-character shard prefix derived from user_id (alphanumeric, lower-cased)."""
    cleaned = "".join(c for c in user_id.lower() if c.isalnum())
    return (cleaned + "xx")[:2]
