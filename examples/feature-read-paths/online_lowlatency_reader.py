"""
Online (Low-latency) Feature Reader
=====================================
Single-key and batch lookups from the Redis online feature store.

Redis key schema
----------------
  sf:feat:u:{user_id}        → HASH  (latest window snapshot)
    Fields:
      event_count      str(int)
      window_start_ms  str(int)
      window_end_ms    str(int)
      event_rate       str(float)
      updated_at       str(float)  unix epoch when written

  sf:feat:u:{user_id}:hist   → ZSET
    score  = window_start_ms (int)
    member = JSON string (same dict as record_to_dict)

Latency targets (local Redis)
------------------------------
  get()         <1 ms   (single HGETALL)
  get_batch()   <2 ms   (pipelined HGETALL × N)
  get_history() <2 ms   (ZRANGEBYSCORE)

Usage
-----
  reader = OnlineFeatureReader()
  feat   = reader.get(user_id=42)          # latest snapshot → dict | None
  batch  = reader.get_batch([1, 42, 99])   # {user_id: dict}
  hist   = reader.get_history(42, since_ms=...) # list[dict] sorted by window
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional, Sequence

from layout_spec import (
    ONLINE_USER_TTL,
    ONLINE_HIST_TTL,
    online_user_key,
    online_hist_key,
    record_to_dict,
)


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def _make_redis():
    """Return a Redis client or None if redis-py is absent / unreachable."""
    try:
        import redis  # type: ignore
        r = redis.Redis(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=int(os.environ.get("REDIS_PORT", "6379")),
            db=int(os.environ.get("REDIS_DB", "0")),
            password=os.environ.get("REDIS_PASSWORD") or None,
            decode_responses=True,
            socket_timeout=1.0,
            socket_connect_timeout=1.0,
        )
        r.ping()
        return r
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

class OnlineFeatureReader:
    """
    Low-latency feature lookups from Redis.

    Falls back to an in-memory stub (InMemoryOnlineStore) when Redis is
    unavailable, so the demo always runs without external services.
    """

    def __init__(self):
        self._redis = _make_redis()
        self._stub: Optional[InMemoryOnlineStore] = None
        if self._redis is None:
            self._stub = InMemoryOnlineStore()

    @property
    def backend(self) -> str:
        return "redis" if self._redis else "in-memory-stub"

    # ------------------------------------------------------------------
    # Single-key lookup  O(1)
    # ------------------------------------------------------------------

    def get(self, user_id: int) -> Optional[dict]:
        """Return the latest feature snapshot for user_id, or None."""
        if self._redis:
            return self._get_redis(user_id)
        return self._stub.get(user_id)  # type: ignore[union-attr]

    def _get_redis(self, user_id: int) -> Optional[dict]:
        key = online_user_key(user_id)
        raw = self._redis.hgetall(key)  # type: ignore[union-attr]
        if not raw:
            return None
        return _hash_to_dict(user_id, raw)

    # ------------------------------------------------------------------
    # Batch lookup  O(N) pipelined — single round-trip
    # ------------------------------------------------------------------

    def get_batch(self, user_ids: Sequence[int]) -> Dict[int, dict]:
        """
        Fetch features for multiple users in one Redis pipeline.

        Returns only the user_ids that were found (missing keys omitted).
        """
        if not user_ids:
            return {}
        if self._redis:
            return self._get_batch_redis(user_ids)
        return self._stub.get_batch(user_ids)  # type: ignore[union-attr]

    def _get_batch_redis(self, user_ids: Sequence[int]) -> Dict[int, dict]:
        pipe = self._redis.pipeline(transaction=False)  # type: ignore[union-attr]
        for uid in user_ids:
            pipe.hgetall(online_user_key(uid))
        results = pipe.execute()
        out: Dict[int, dict] = {}
        for uid, raw in zip(user_ids, results):
            if raw:
                out[uid] = _hash_to_dict(uid, raw)
        return out

    # ------------------------------------------------------------------
    # History lookup  O(log N + M)
    # ------------------------------------------------------------------

    def get_history(
        self,
        user_id: int,
        since_ms: Optional[int] = None,
        until_ms: Optional[int] = None,
        limit: int = 100,
    ) -> List[dict]:
        """
        Return up to `limit` window records for user_id, sorted by window_start_ms.

        since_ms / until_ms are inclusive bounds (epoch milliseconds).
        Defaults to all history stored in Redis (up to ONLINE_HIST_TTL = 24h).
        """
        if self._redis:
            return self._get_history_redis(user_id, since_ms, until_ms, limit)
        return self._stub.get_history(user_id, since_ms, until_ms, limit)  # type: ignore[union-attr]

    def _get_history_redis(
        self,
        user_id: int,
        since_ms: Optional[int],
        until_ms: Optional[int],
        limit: int,
    ) -> List[dict]:
        key = online_hist_key(user_id)
        lo = since_ms if since_ms is not None else "-inf"
        hi = until_ms if until_ms is not None else "+inf"
        # ZRANGEBYSCORE returns members sorted by score (window_start_ms) ascending
        members = self._redis.zrangebyscore(key, lo, hi, start=0, num=limit)  # type: ignore[union-attr]
        records = []
        for m in members:
            try:
                records.append(json.loads(m))
            except (json.JSONDecodeError, TypeError):
                pass
        return records

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return basic store statistics (key count, memory)."""
        if self._redis:
            try:
                info = self._redis.info("memory")  # type: ignore[union-attr]
                dbsize = self._redis.dbsize()  # type: ignore[union-attr]
                return {
                    "backend": "redis",
                    "key_count": dbsize,
                    "used_memory_human": info.get("used_memory_human", "?"),
                    "maxmemory_human": info.get("maxmemory_human", "0B"),
                }
            except Exception as exc:
                return {"backend": "redis", "error": str(exc)}
        return self._stub.stats()  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# In-memory stub (no Redis required)
# ---------------------------------------------------------------------------

class InMemoryOnlineStore:
    """
    Thread-safe in-memory replacement for Redis used in unit tests and demos.

    Populated with synthetic data on first access so callers always get results.
    """

    def __init__(self, n_users: int = 200):
        import random
        rng = random.Random(7)
        now_ms = int(time.time() * 1000)
        window_ms = 30_000
        self._latest: Dict[int, dict] = {}
        self._history: Dict[int, List[dict]] = {}
        for uid in range(1, n_users + 1):
            windows = []
            for w in range(48):  # 24 h of 30-s windows, sampled every 30 min
                ec  = max(1, int(rng.lognormvariate(2.0, 1.2)))
                ws  = now_ms - (w + 1) * window_ms * 60  # ~30 min apart
                row = record_to_dict(uid, ws, ws + window_ms, ec)
                row["updated_at"] = time.time()
                windows.append(row)
            self._history[uid] = sorted(windows, key=lambda r: r["window_start_ms"])
            self._latest[uid] = windows[0]

    def get(self, user_id: int) -> Optional[dict]:
        return self._latest.get(user_id)

    def get_batch(self, user_ids: Sequence[int]) -> Dict[int, dict]:
        return {uid: self._latest[uid] for uid in user_ids if uid in self._latest}

    def get_history(
        self,
        user_id: int,
        since_ms: Optional[int],
        until_ms: Optional[int],
        limit: int,
    ) -> List[dict]:
        rows = self._history.get(user_id, [])
        if since_ms is not None:
            rows = [r for r in rows if r["window_start_ms"] >= since_ms]
        if until_ms is not None:
            rows = [r for r in rows if r["window_start_ms"] <= until_ms]
        return rows[:limit]

    def stats(self) -> dict:
        return {"backend": "in-memory-stub", "key_count": len(self._latest)}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _hash_to_dict(user_id: int, raw: dict) -> dict:
    return {
        "user_id": user_id,
        "event_count": int(raw.get("event_count", 0)),
        "window_start_ms": int(raw.get("window_start_ms", 0)),
        "window_end_ms": int(raw.get("window_end_ms", 0)),
        "event_rate": float(raw.get("event_rate", 0.0)),
        "updated_at": float(raw.get("updated_at", 0.0)),
    }


# ---------------------------------------------------------------------------
# CLI smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    reader = OnlineFeatureReader()
    print(f"[online_reader] backend={reader.backend}")
    print(f"[online_reader] stats={reader.stats()}")

    t0 = time.perf_counter()
    feat = reader.get(user_id=42)
    print(f"[online_reader] get(42)  → {feat}  ({(time.perf_counter()-t0)*1000:.2f} ms)")

    t0 = time.perf_counter()
    batch = reader.get_batch(list(range(1, 51)))
    print(f"[online_reader] get_batch(50 users) → {len(batch)} hits  ({(time.perf_counter()-t0)*1000:.2f} ms)")

    t0 = time.perf_counter()
    hist = reader.get_history(42, limit=10)
    print(f"[online_reader] get_history(42, limit=10) → {len(hist)} windows  ({(time.perf_counter()-t0)*1000:.2f} ms)")
