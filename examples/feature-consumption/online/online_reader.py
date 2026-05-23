"""
Online (low-latency) feature reader.

Physical layout read by this module
------------------------------------
MinIO serving layout (primary):

  s3://processed/features/serving/latest/
    us/user-smith.json          ← shard prefix = user_id[:2], normalised
    us/user-stubbs.json
    jo/john-doe.json
    an/anonymous-99.json

  Each file is a single JSON object — the user's latest feature snapshot:
    {
      "user_id":         "user-smith",
      "event_count":     42,
      "event_rate":      0.7,
      "window_start_ms": 1705276800000,
      "window_end_ms":   1705276860000,
      "event_date":      "2024-01-15",
      "materialized_at": "2024-01-15T02:05:33Z"
    }

Redis cache (L1, optional — sits in front of MinIO):

  Key pattern : feature:v1:{user_id}
  Value       : JSON string (same schema as above)
  TTL         : REDIS_TTL_S (default 3600 s)

  On a cache hit the MinIO GET is skipped entirely (<1 ms vs ~20–50 ms).

Read priority
-------------
  L1 Redis   →  hit: return immediately  miss: try L2
  L2 MinIO   →  hit: backfill L1, return  miss: try L3
  L3 Iceberg →  full table scan (slow, logs WARNING); backfills L1+L2; return
  None       →  user_id genuinely not found

Usage
-----
  from online.online_reader import OnlineFeatureReader

  reader = OnlineFeatureReader()

  # Single user (cache-aside, ~1 ms Redis hit / ~30 ms MinIO miss):
  fv = reader.get("user-smith")
  if fv:
      model.predict([fv.feature_array])

  # Batch lookup (Redis MGET + MinIO for misses — much cheaper than N serial GETs):
  vectors = reader.mget(["user-smith", "user-stubbs", "john-doe"])
  # Returns: {"user-smith": FeatureVector, "user-stubbs": None, ...}

Environment variables
---------------------
  MINIO_ENDPOINT          localhost:9000
  MINIO_ACCESS_KEY        minioadmin
  MINIO_SECRET_KEY        minioadmin
  MINIO_BUCKET            processed
  REDIS_HOST              localhost
  REDIS_PORT              6379
  REDIS_DB                0
  REDIS_TTL_S             3600
  ICEBERG_CATALOG_TYPE    hadoop
  ICEBERG_WAREHOUSE       s3a://streamforge/warehouse
  ICEBERG_DATABASE        streamforge
  ICEBERG_TABLE           user_event_counts
  ICEBERG_S3_ENDPOINT     http://minio:9000
  ICEBERG_S3_ACCESS_KEY   minioadmin
  ICEBERG_S3_SECRET_KEY   minioadmin
"""
from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.feature_vector import FeatureVector


# ── Lookup stats ─────────────────────────────────────────────────────────────

@dataclass
class LookupStats:
    l1_hits: int = 0
    l2_hits: int = 0
    l3_hits: int = 0
    misses:  int = 0
    latencies_ms: list[float] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.l1_hits + self.l2_hits + self.l3_hits + self.misses

    @property
    def p50_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        return s[len(s) // 2]

    @property
    def p99_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        return s[int(len(s) * 0.99)]

    def __str__(self) -> str:
        return (
            f"LookupStats(total={self.total}, "
            f"L1={self.l1_hits}, L2={self.l2_hits}, L3={self.l3_hits}, "
            f"miss={self.misses}, p50={self.p50_ms:.1f}ms, p99={self.p99_ms:.1f}ms)"
        )


# ── OnlineFeatureReader ───────────────────────────────────────────────────────

class OnlineFeatureReader:
    """
    Low-latency feature reader implementing a three-level cache-aside hierarchy.

    All levels are optional at construction — missing libraries or unavailable
    services are silently skipped so the reader degrades gracefully.
    """

    def __init__(self) -> None:
        self._redis  = _connect_redis()    # None if Redis unavailable
        self._minio  = _connect_minio()    # None if minio pkg unavailable
        self._bucket = _env("MINIO_BUCKET", "processed")
        self._ttl    = int(_env("REDIS_TTL_S", "3600"))
        self.stats   = LookupStats()

    # ── Public API ────────────────────────────────────────────────────────────

    def get(self, user_id: str) -> Optional[FeatureVector]:
        """
        Return the latest FeatureVector for user_id, or None if not found.

        Latency targets:
          L1 Redis hit : < 2 ms
          L2 MinIO hit : 10–50 ms
          L3 Iceberg   : seconds (fallback, logs WARNING)
        """
        t0 = time.perf_counter()
        fv = self._l1_get(user_id)

        if fv is None:
            fv = self._l2_get(user_id)
            if fv is not None:
                self._l1_set(fv)          # backfill L1
                self.stats.l2_hits += 1
            else:
                fv = self._l3_get(user_id)
                if fv is not None:
                    self._l1_set(fv)
                    self._l2_set(fv)      # backfill L1 + L2
                    self.stats.l3_hits += 1
                else:
                    self.stats.misses += 1
        else:
            self.stats.l1_hits += 1

        lat_ms = (time.perf_counter() - t0) * 1000
        self.stats.latencies_ms.append(lat_ms)
        return fv

    def mget(self, user_ids: list[str]) -> dict[str, Optional[FeatureVector]]:
        """
        Batch lookup.  Uses Redis MGET for all ids in one round-trip, then
        fetches MinIO objects only for cache misses.

        Returns a dict mapping each user_id to its FeatureVector (or None).
        """
        if not user_ids:
            return {}

        result: dict[str, Optional[FeatureVector]] = {}

        # ── L1: Redis MGET (single round-trip) ───────────────────────────────
        redis_keys = [FeatureVector.redis_key_for(uid) for uid in user_ids]
        redis_hits: dict[str, FeatureVector] = {}

        if self._redis is not None:
            try:
                values = self._redis.mget(redis_keys)
                for uid, raw in zip(user_ids, values):
                    if raw is not None:
                        fv = FeatureVector.from_json(
                            raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                        redis_hits[uid] = fv
                        result[uid] = fv
                        self.stats.l1_hits += 1
            except Exception:
                pass

        missing = [uid for uid in user_ids if uid not in result]

        # ── L2: MinIO parallel GETs for cache misses ─────────────────────────
        if missing and self._minio is not None:
            minio_results = self._l2_mget(missing)
            for uid, fv in minio_results.items():
                result[uid] = fv
                if fv is not None:
                    self._l1_set(fv)
                    self.stats.l2_hits += 1
                    missing = [m for m in missing if m != uid]

        # ── L3: Iceberg for any remaining misses ──────────────────────────────
        still_missing = [uid for uid in user_ids if uid not in result]
        if still_missing:
            for uid in still_missing:
                fv = self._l3_get(uid)
                result[uid] = fv
                if fv is not None:
                    self._l1_set(fv)
                    self._l2_set(fv)
                    self.stats.l3_hits += 1
                else:
                    self.stats.misses += 1

        return result

    def reset_stats(self) -> None:
        self.stats = LookupStats()

    # ── L1: Redis ─────────────────────────────────────────────────────────────

    def _l1_get(self, user_id: str) -> Optional[FeatureVector]:
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(FeatureVector.redis_key_for(user_id))
            if raw is None:
                return None
            return FeatureVector.from_json(
                raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        except Exception:
            return None

    def _l1_set(self, fv: FeatureVector) -> None:
        if self._redis is None:
            return
        try:
            self._redis.setex(fv.redis_key, self._ttl, fv.to_json())
        except Exception:
            pass

    # ── L2: MinIO serving layout ──────────────────────────────────────────────

    def _l2_get(self, user_id: str) -> Optional[FeatureVector]:
        if self._minio is None:
            return None
        key = FeatureVector.serving_key_for(user_id)
        try:
            resp = self._minio.get_object(self._bucket, key)
            raw  = resp.read()
            resp.close()
            return FeatureVector.from_json(raw.decode("utf-8"))
        except Exception:
            return None

    def _l2_mget(self, user_ids: list[str]) -> dict[str, Optional[FeatureVector]]:
        """Fetch multiple objects from MinIO; simple serial implementation."""
        result: dict[str, Optional[FeatureVector]] = {}
        for uid in user_ids:
            result[uid] = self._l2_get(uid)
        return result

    def _l2_set(self, fv: FeatureVector) -> None:
        if self._minio is None:
            return
        import io
        data = fv.to_json().encode("utf-8")
        try:
            self._minio.put_object(
                self._bucket, fv.serving_key,
                io.BytesIO(data), length=len(data),
                content_type="application/json",
            )
        except Exception:
            pass

    # ── L3: Iceberg (slow fallback) ───────────────────────────────────────────

    def _l3_get(self, user_id: str) -> Optional[FeatureVector]:
        print(f"[online_reader] WARNING: L1+L2 miss for {user_id!r} — "
              f"falling back to Iceberg (slow path)", file=sys.stderr)
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError:
            return None

        try:
            catalog_conf = _iceberg_catalog_conf()
            catalog = load_catalog("streamforge", **catalog_conf)
            db      = _env("ICEBERG_DATABASE", "streamforge")
            tbl     = _env("ICEBERG_TABLE", "user_event_counts")
            table   = catalog.load_table(f"{db}.{tbl}")
            scan    = table.scan(row_filter=f"user_id = '{user_id}'")
            arrow   = scan.to_arrow()
            if arrow.num_rows == 0:
                return None
            d = {col: arrow[col][-1].as_py() for col in arrow.schema.names}
            d["user_id"] = user_id
            return FeatureVector.from_dict(d)
        except Exception as exc:
            print(f"[online_reader] Iceberg fallback failed: {exc}", file=sys.stderr)
            return None


# ── Connection helpers ────────────────────────────────────────────────────────

def _connect_redis():
    try:
        import redis as redis_pkg
        client = redis_pkg.Redis(
            host=_env("REDIS_HOST", "localhost"),
            port=int(_env("REDIS_PORT", "6379")),
            db=int(_env("REDIS_DB", "0")),
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        client.ping()
        print("[online_reader] Redis connected", file=sys.stderr)
        return client
    except Exception as exc:
        print(f"[online_reader] Redis unavailable ({exc}) — L1 cache disabled",
              file=sys.stderr)
        return None


def _connect_minio():
    try:
        from minio import Minio
        return Minio(
            endpoint=_env("MINIO_ENDPOINT", "localhost:9000"),
            access_key=_env("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=_env("MINIO_SECRET_KEY", "minioadmin"),
            secure=_env("MINIO_SECURE", "false").lower() == "true",
        )
    except ImportError:
        print("[online_reader] minio package not installed — L2 disabled", file=sys.stderr)
        return None


def _iceberg_catalog_conf() -> dict:
    conf: dict = {"type": _env("ICEBERG_CATALOG_TYPE", "hadoop")}
    warehouse = _env("ICEBERG_WAREHOUSE", "s3a://streamforge/warehouse")
    endpoint  = _env("ICEBERG_S3_ENDPOINT", "")
    if conf["type"] == "rest":
        conf["uri"] = warehouse
    else:
        conf["warehouse"] = warehouse
    if endpoint:
        conf["s3.endpoint"]          = endpoint
        conf["s3.access-key-id"]     = _env("ICEBERG_S3_ACCESS_KEY", "minioadmin")
        conf["s3.secret-access-key"] = _env("ICEBERG_S3_SECRET_KEY", "minioadmin")
        conf["s3.path-style-access"] = "true"
    return conf


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


# ── CLI demo ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Online feature reader demo")
    parser.add_argument("user_ids", nargs="*",
                        default=["user-smith", "user-stubbs", "unknown-user"],
                        help="User IDs to look up")
    parser.add_argument("--batch", action="store_true",
                        help="Use mget() instead of serial get() calls")
    args = parser.parse_args()

    reader = OnlineFeatureReader()

    if args.batch:
        print(f"\nBatch lookup for {len(args.user_ids)} user(s):")
        results = reader.mget(args.user_ids)
        for uid, fv in results.items():
            if fv:
                print(f"  {uid:30s} → count={fv.event_count:4d}  "
                      f"rate={fv.event_rate:.3f}/s  date={fv.event_date}")
            else:
                print(f"  {uid:30s} → NOT FOUND")
    else:
        for uid in args.user_ids:
            t0 = time.perf_counter()
            fv = reader.get(uid)
            ms = (time.perf_counter() - t0) * 1000
            if fv:
                print(f"  {uid:30s} → count={fv.event_count:4d}  "
                      f"rate={fv.event_rate:.3f}/s  [{ms:.1f} ms]")
            else:
                print(f"  {uid:30s} → NOT FOUND  [{ms:.1f} ms]")

    print(f"\n{reader.stats}")
