"""
Materialization job — converts the offline Iceberg/MinIO store into the
online serving layout, and optionally warm-starts the Redis cache.

What it does
------------
1. Read the latest partition(s) from the offline store (Iceberg or raw MinIO JSON).
2. For each user, keep only the record with the highest window_start_ms
   (i.e. the most recent feature snapshot for that user).
3. Write one JSON file per user to the MinIO serving layout:
     {MINIO_BUCKET}/features/serving/latest/{shard}/{user_id}.json
4. Optionally SETEX each record into Redis with a configurable TTL.

Run schedule
------------
Run this job nightly (e.g. 02:00 UTC) after the Iceberg maintenance job
has completed compaction of the previous day's partition.

  # Docker Compose (from examples/feature-consumption/):
  docker compose run --rm materialize

  # Direct:
  python -m online.materialize --date 2024-01-15

  # Custom date range (backfill):
  python -m online.materialize --start 2024-01-01 --end 2024-01-07

Materialization result
----------------------
  MinIO objects written : one per unique user_id found in [start, end]
  Redis keys set        : same (if Redis is reachable and --warm-redis is set)

Object key pattern:
  features/serving/latest/{uid[:2]}/{uid}.json

Example:
  features/serving/latest/us/user-smith.json
  features/serving/latest/jo/john-doe.json

Environment variables
---------------------
  (inherits all variables from batch_reader and online_reader)
  MATERIALIZE_DATES   number of past days to include; default 1 (yesterday only)
  MATERIALIZE_WORKERS number of parallel MinIO PUT threads; default 8
  WARM_REDIS          true/false — whether to backfill Redis; default true
  REDIS_TTL_S         TTL for Redis keys; default 86400 (24 h)
"""
from __future__ import annotations

import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta, timezone
from datetime import datetime as dt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.feature_vector import FeatureVector
from offline.batch_reader import BatchFeatureReader


# ── Result ────────────────────────────────────────────────────────────────────

@dataclass
class MaterializeResult:
    date_range: str
    users_found: int
    objects_written: int
    redis_keys_set: int
    errors: int
    duration_s: float

    def __str__(self) -> str:
        return (
            f"MaterializeResult(dates={self.date_range}, users={self.users_found}, "
            f"minio_writes={self.objects_written}, redis={self.redis_keys_set}, "
            f"errors={self.errors}, duration={self.duration_s:.1f}s)"
        )


# ── Main job ──────────────────────────────────────────────────────────────────

def run(start_date: str, end_date: str, warm_redis: bool = True) -> MaterializeResult:
    """
    Materialise the serving layout for the given date range.

    For each user_id, only the record with the highest window_start_ms is kept
    (latest window wins).  This means:
      - A user active on both 2024-01-14 and 2024-01-15 gets their 2024-01-15 record.
      - A user only active on 2024-01-14 gets that record.
    """
    t0 = time.perf_counter()
    print(f"[materialize] Reading offline store for {start_date} → {end_date}",
          file=sys.stderr)

    # ── Step 1: Read offline store ────────────────────────────────────────────
    reader = BatchFeatureReader()
    df     = reader.read_date_range(start_date, end_date)

    if df.empty:
        print("[materialize] No data found — nothing to materialise", file=sys.stderr)
        return MaterializeResult(
            date_range=f"{start_date}/{end_date}",
            users_found=0, objects_written=0, redis_keys_set=0,
            errors=0, duration_s=time.perf_counter() - t0,
        )

    # ── Step 2: Deduplicate — keep latest window per user ─────────────────────
    latest = (
        df.sort_values("window_start_ms")
          .groupby("user_id", as_index=False)
          .last()
    )
    print(f"[materialize] {len(df):,} rows → {len(latest):,} unique users",
          file=sys.stderr)

    # Convert to FeatureVector list
    now_iso = dt.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    vectors: list[FeatureVector] = []
    for _, row in latest.iterrows():
        fv = FeatureVector.from_dict(row.to_dict())
        fv.materialized_at = now_iso
        vectors.append(fv)

    # ── Step 3: Write to MinIO serving layout ─────────────────────────────────
    minio_client = _connect_minio()
    bucket       = _env("MINIO_BUCKET", "processed")
    workers      = int(_env("MATERIALIZE_WORKERS", "8"))

    written = 0
    errors  = 0

    if minio_client is not None:
        def _put(fv: FeatureVector) -> bool:
            data = fv.to_json().encode("utf-8")
            try:
                minio_client.put_object(
                    bucket, fv.serving_key,
                    io.BytesIO(data), length=len(data),
                    content_type="application/json",
                )
                return True
            except Exception as exc:
                print(f"[materialize] PUT failed for {fv.user_id}: {exc}",
                      file=sys.stderr)
                return False

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_put, fv): fv for fv in vectors}
            for fut in as_completed(futures):
                if fut.result():
                    written += 1
                else:
                    errors += 1

        print(f"[materialize] MinIO: {written} objects written, {errors} errors",
              file=sys.stderr)
    else:
        print("[materialize] MinIO unavailable — skipping object writes", file=sys.stderr)

    # ── Step 4: Warm Redis ────────────────────────────────────────────────────
    redis_set = 0
    if warm_redis:
        redis_client = _connect_redis()
        ttl          = int(_env("REDIS_TTL_S", "86400"))  # 24 h default for materialized data

        if redis_client is not None:
            try:
                pipe = redis_client.pipeline(transaction=False)
                for fv in vectors:
                    pipe.setex(fv.redis_key, ttl, fv.to_json())
                pipe.execute()
                redis_set = len(vectors)
                print(f"[materialize] Redis: {redis_set} keys set (TTL={ttl}s)",
                      file=sys.stderr)
            except Exception as exc:
                print(f"[materialize] Redis pipeline failed: {exc}", file=sys.stderr)
        else:
            print("[materialize] Redis unavailable — skipping cache warm-up",
                  file=sys.stderr)

    result = MaterializeResult(
        date_range=f"{start_date}/{end_date}",
        users_found=len(vectors),
        objects_written=written,
        redis_keys_set=redis_set,
        errors=errors,
        duration_s=time.perf_counter() - t0,
    )
    print(f"[materialize] Done: {result}", file=sys.stderr)
    return result


# ── Connection helpers ────────────────────────────────────────────────────────

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
        return None


def _connect_redis():
    try:
        import redis as redis_pkg
        client = redis_pkg.Redis(
            host=_env("REDIS_HOST", "localhost"),
            port=int(_env("REDIS_PORT", "6379")),
            db=int(_env("REDIS_DB", "0")),
            socket_connect_timeout=2,
        )
        client.ping()
        return client
    except Exception:
        return None


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Materialise online serving layout from offline feature store")
    parser.add_argument("--date",       help="Single date to materialise (YYYY-MM-DD)")
    parser.add_argument("--start",      help="Start date for range (YYYY-MM-DD)")
    parser.add_argument("--end",        help="End date for range (YYYY-MM-DD)")
    parser.add_argument("--days",       type=int, default=1,
                        help="Number of past days to include (default: 1 = yesterday)")
    parser.add_argument("--no-redis",   action="store_true",
                        help="Skip Redis warm-up")
    args = parser.parse_args()

    if args.date:
        start = end = args.date
    elif args.start and args.end:
        start, end = args.start, args.end
    else:
        yesterday = (date.today() - timedelta(days=args.days)).isoformat()
        start = (date.today() - timedelta(days=args.days)).isoformat()
        end   = (date.today() - timedelta(days=1)).isoformat()

    result = run(start, end, warm_redis=not args.no_redis)
    print(result)
    sys.exit(0 if result.errors == 0 else 1)
