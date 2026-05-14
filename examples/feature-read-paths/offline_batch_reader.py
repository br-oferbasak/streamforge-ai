"""
Offline (Batch) Feature Reader
================================
Reads feature records from the date-partitioned MinIO / S3 layout.

Storage layout recap
--------------------
  bucket: processed
  streamforge/features/
    YYYY/MM/DD/HH/part-{shard:05d}.jsonl  ← NDJSON, rows sorted by window_start_ms
    _manifest/YYYY-MM-DD.json              ← daily manifest (fast file discovery)

Two discovery strategies
------------------------
  ManifestReader  – reads the daily manifest JSON first; O(1) network calls per day.
                    Use for scheduled batch jobs where manifests are pre-built.
  PrefixScanner   – lists objects under the date prefix directly.
                    Use when manifests are absent or for ad-hoc exploration.

Typical batch pipeline
----------------------
  reader = OfflineBatchReader()
  records = reader.read_date_range(start=date(2026, 5, 1), end=date(2026, 5, 14))
  # → list[dict]  with keys: user_id, window_start_ms, window_end_ms, event_count, event_rate
"""

from __future__ import annotations

import json
import os
import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Iterator, List, Optional, Sequence

from layout_spec import (
    OFFLINE_BUCKET,
    offline_day_prefix,
    offline_hour_prefix,
    offline_manifest_key,
    record_to_dict,
)


# ---------------------------------------------------------------------------
# Manifest data model
# ---------------------------------------------------------------------------

@dataclass
class FileEntry:
    """One shard file referenced in a daily manifest."""
    key: str            # full MinIO object key
    rows: int           # row count written by the sink
    min_ts: int         # min(window_start_ms) in the file
    max_ts: int         # max(window_start_ms) in the file
    size_bytes: int = 0


@dataclass
class DayManifest:
    """
    JSON manifest produced once per UTC day after all shards are written.

    Schema (saved as _manifest/YYYY-MM-DD.json):
    {
      "date": "2026-05-14",
      "total_rows": 182400,
      "files": [
        {"key": "streamforge/features/2026/05/14/00/part-00000.jsonl",
         "rows": 3600, "min_ts": 1747180800000, "max_ts": 1747184400000,
         "size_bytes": 512000},
        ...
      ]
    }
    """
    date: datetime.date
    total_rows: int
    files: List[FileEntry] = field(default_factory=list)

    def filter_time_range(self, start_ms: int, end_ms: int) -> List[FileEntry]:
        """Return only files whose time range overlaps [start_ms, end_ms)."""
        return [f for f in self.files if f.min_ts < end_ms and f.max_ts >= start_ms]

    @classmethod
    def from_dict(cls, d: dict) -> "DayManifest":
        return cls(
            date=datetime.date.fromisoformat(d["date"]),
            total_rows=d.get("total_rows", 0),
            files=[
                FileEntry(
                    key=f["key"],
                    rows=f.get("rows", 0),
                    min_ts=f.get("min_ts", 0),
                    max_ts=f.get("max_ts", 0),
                    size_bytes=f.get("size_bytes", 0),
                )
                for f in d.get("files", [])
            ],
        )


# ---------------------------------------------------------------------------
# MinIO client helper
# ---------------------------------------------------------------------------

def _make_minio():
    """Return a (Minio, bucket) pair or (None, None)."""
    try:
        from minio import Minio  # type: ignore
        client = Minio(
            os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        )
        client.list_buckets()
        return client, OFFLINE_BUCKET
    except Exception:
        return None, None


# ---------------------------------------------------------------------------
# NDJSON file parsing
# ---------------------------------------------------------------------------

def _parse_ndjson(data: bytes) -> Iterator[dict]:
    for raw in data.splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        uid = obj.get("user_id", obj.get("userId"))
        ws  = obj.get("window_start_ms", obj.get("window_start"))
        we  = obj.get("window_end_ms",   obj.get("window_end"))
        ec  = obj.get("event_count",     obj.get("eventCount"))
        if None in (uid, ws, we, ec):
            continue
        yield record_to_dict(int(uid), int(ws), int(we), int(ec))


# ---------------------------------------------------------------------------
# Reader implementations
# ---------------------------------------------------------------------------

class ManifestReader:
    """
    Fast path: read the daily manifest to discover files, then fetch only
    those whose time range overlaps the requested window.

    Network round-trips per day = 1 (manifest) + len(matching_shards).
    """

    def __init__(self):
        self._client, self._bucket = _make_minio()

    def available(self) -> bool:
        return self._client is not None

    def load_manifest(self, date: datetime.date) -> Optional[DayManifest]:
        if self._client is None:
            return None
        key = offline_manifest_key(date)
        try:
            resp = self._client.get_object(self._bucket, key)
            data = resp.read()
            resp.close()
            return DayManifest.from_dict(json.loads(data))
        except Exception:
            return None

    def read_file(self, key: str) -> List[dict]:
        if self._client is None:
            return []
        try:
            resp = self._client.get_object(self._bucket, key)
            data = resp.read()
            resp.close()
            return list(_parse_ndjson(data))
        except Exception:
            return []

    def read_date_range(
        self,
        start: datetime.date,
        end: datetime.date,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        max_workers: int = 8,
    ) -> List[dict]:
        """
        Read all records whose window_start_ms falls in [start, end) (dates, UTC).

        Optionally narrow further with start_ms / end_ms epoch milliseconds.
        Files are fetched in parallel (max_workers threads).
        """
        if not self.available():
            raise RuntimeError("ManifestReader: MinIO not reachable")

        start_ms = start_ms or int(datetime.datetime.combine(start, datetime.time.min,
                                    tzinfo=datetime.timezone.utc).timestamp() * 1000)
        end_ms   = end_ms   or int(datetime.datetime.combine(end,   datetime.time.max,
                                    tzinfo=datetime.timezone.utc).timestamp() * 1000)

        keys_to_fetch: List[str] = []
        day = start
        while day <= end:
            manifest = self.load_manifest(day)
            if manifest:
                for fe in manifest.filter_time_range(start_ms, end_ms):
                    keys_to_fetch.append(fe.key)
            day += datetime.timedelta(days=1)

        records: List[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self.read_file, k): k for k in keys_to_fetch}
            for fut in as_completed(futures):
                records.extend(fut.result())

        return [r for r in records if start_ms <= r["window_start_ms"] < end_ms]


class PrefixScanner:
    """
    Fallback path: list objects under the date prefix and stream their content.

    No manifest required — works for ad-hoc exploration or when manifests
    haven't been written yet.  Slower for large date ranges.
    """

    def __init__(self):
        self._client, self._bucket = _make_minio()

    def available(self) -> bool:
        return self._client is not None

    def scan_day(self, date: datetime.date) -> Iterator[str]:
        """Yield every object key under the day prefix."""
        if self._client is None:
            return
        prefix = offline_day_prefix(datetime.datetime.combine(date, datetime.time.min))
        try:
            for obj in self._client.list_objects(self._bucket, prefix=prefix, recursive=True):
                if obj.object_name.endswith(".jsonl") or obj.object_name.endswith(".json"):
                    yield obj.object_name
        except Exception:
            return

    def read_key(self, key: str) -> List[dict]:
        if self._client is None:
            return []
        try:
            resp = self._client.get_object(self._bucket, key)
            data = resp.read()
            resp.close()
            return list(_parse_ndjson(data))
        except Exception:
            return []

    def read_date_range(
        self,
        start: datetime.date,
        end: datetime.date,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        max_workers: int = 8,
    ) -> List[dict]:
        if not self.available():
            raise RuntimeError("PrefixScanner: MinIO not reachable")

        start_ms = start_ms or int(datetime.datetime.combine(start, datetime.time.min,
                                    tzinfo=datetime.timezone.utc).timestamp() * 1000)
        end_ms   = end_ms   or int(datetime.datetime.combine(end,   datetime.time.max,
                                    tzinfo=datetime.timezone.utc).timestamp() * 1000)

        all_keys: List[str] = []
        day = start
        while day <= end:
            all_keys.extend(self.scan_day(day))
            day += datetime.timedelta(days=1)

        records: List[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self.read_key, k): k for k in all_keys}
            for fut in as_completed(futures):
                records.extend(fut.result())

        return [r for r in records if start_ms <= r["window_start_ms"] < end_ms]


# ---------------------------------------------------------------------------
# Unified facade
# ---------------------------------------------------------------------------

class OfflineBatchReader:
    """
    Reads feature records from MinIO using manifests when available,
    falling back to prefix scanning otherwise.

    Usage
    -----
    reader = OfflineBatchReader()
    records = reader.read_date_range(
        start=datetime.date(2026, 5, 1),
        end=datetime.date(2026, 5, 14),
    )
    # records: list[dict] → user_id, window_start_ms, window_end_ms, event_count, event_rate
    """

    def __init__(self):
        self._manifest = ManifestReader()
        self._scanner  = PrefixScanner()

    @property
    def is_live(self) -> bool:
        return self._manifest.available() or self._scanner.available()

    def read_date_range(
        self,
        start: datetime.date,
        end: datetime.date,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        max_workers: int = 8,
    ) -> List[dict]:
        if self._manifest.available():
            return self._manifest.read_date_range(start, end, start_ms, end_ms, max_workers)
        if self._scanner.available():
            return self._scanner.read_date_range(start, end, start_ms, end_ms, max_workers)
        return _synthetic_records(2_000)

    def read_today(self) -> List[dict]:
        today = datetime.date.today()
        return self.read_date_range(today, today)


# ---------------------------------------------------------------------------
# Synthetic fallback (no MinIO required)
# ---------------------------------------------------------------------------

def _synthetic_records(n: int = 2_000) -> List[dict]:
    import random
    rng = random.Random(42)
    now_ms = int(time.time() * 1000)
    window_ms = 30_000
    out = []
    for i in range(n):
        ws = now_ms - (i // 10) * window_ms
        uid = rng.randint(1, 200)
        ec  = max(1, int(rng.lognormvariate(2.0, 1.2)))
        out.append(record_to_dict(uid, ws, ws + window_ms, ec))
    return out


# ---------------------------------------------------------------------------
# CLI smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    reader = OfflineBatchReader()
    live = reader.is_live
    print(f"[offline_batch_reader] live={live}")

    t0 = time.perf_counter()
    records = reader.read_date_range(
        start=datetime.date.today() - datetime.timedelta(days=1),
        end=datetime.date.today(),
    )
    elapsed = time.perf_counter() - t0

    print(f"[offline_batch_reader] read {len(records):,} records in {elapsed:.3f}s")
    if records:
        r = records[0]
        print(f"  sample: user_id={r['user_id']} event_count={r['event_count']} "
              f"event_rate={r['event_rate']:.3f}/s")
