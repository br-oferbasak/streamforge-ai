"""
Feature Store — MinIO / Iceberg reader for the ML training pipeline.

Responsibilities
----------------
1. Discover feature files written by the Flink feature-sink service
   (bucket: processed, prefix: streamforge/features/**/*.json).
2. Build a FileStat manifest so the prefetch-engine can score and rank files.
3. Read feature records from the local cache into Python dicts.
4. Fall back to generating synthetic feature data when MinIO is unreachable,
   so the end-to-end example always runs without external services.

Iceberg support
---------------
When ``pyiceberg`` is installed and ICEBERG_CATALOG_URI is set, feature files
are read via the Iceberg REST catalog instead of raw MinIO object listing.
Both paths produce identical FeatureRecord objects.
"""

from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Public data models
# ---------------------------------------------------------------------------

@dataclass
class FeatureRecord:
    """One aggregated feature row produced by the Flink window."""
    user_id: int
    window_start_ms: int
    window_end_ms: int
    event_count: int
    sink_received_at: str = ""

    # Derived convenience helpers
    @property
    def window_duration_s(self) -> float:
        return (self.window_end_ms - self.window_start_ms) / 1000.0

    @property
    def event_rate(self) -> float:
        d = self.window_duration_s
        return self.event_count / d if d > 0 else 0.0


@dataclass
class FileStat:
    """Access statistics for a single feature file — consumed by prefetch-engine."""
    uri: str               # "minio://bucket/key" or "file:///abs/path"
    recent_access_count: int
    last_access_epoch: float

    @property
    def score(self) -> float:
        return float(self.recent_access_count) + 0.000001 * self.last_access_epoch


# ---------------------------------------------------------------------------
# MinIO feature-file discovery
# ---------------------------------------------------------------------------

def _minio_client():
    """Return a configured Minio client or None if env vars are absent."""
    endpoint   = os.environ.get("MINIO_ENDPOINT",   "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    secure     = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    try:
        from minio import Minio  # type: ignore
        client = Minio(endpoint, access_key=access_key,
                       secret_key=secret_key, secure=secure)
        client.list_buckets()   # connectivity check
        return client
    except Exception:
        return None


def list_minio_feature_files(
    bucket: str = "processed",
    prefix: str = "streamforge/features",
    max_files: int = 500,
) -> Tuple[List[FileStat], bool]:
    """
    List feature objects in MinIO and build a FileStat manifest.

    Returns (file_stats, is_live) where is_live=False means synthetic data
    was generated because MinIO was unreachable.
    """
    client = _minio_client()
    if client is None:
        return _synthetic_file_stats(max_files), False

    now = time.time()
    stats: List[FileStat] = []
    try:
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        for i, obj in enumerate(objects):
            if i >= max_files:
                break
            # Heuristic access count: newer objects are accessed more
            age_s = (now - obj.last_modified.timestamp()) if obj.last_modified else 3600
            access_count = max(1, int(100 / (1 + age_s / 3600)))
            stats.append(FileStat(
                uri=f"minio://{bucket}/{obj.object_name}",
                recent_access_count=access_count,
                last_access_epoch=obj.last_modified.timestamp() if obj.last_modified else now,
            ))
    except Exception:
        return _synthetic_file_stats(max_files), False

    if not stats:
        # Bucket exists but no features written yet
        return _synthetic_file_stats(max_files), False

    return stats, True


def _synthetic_file_stats(n: int) -> List[FileStat]:
    """Produce a realistic FileStat manifest without a real MinIO instance."""
    rng = random.Random(42)
    now = time.time()
    stats = []
    for i in range(n):
        age_s = rng.uniform(0, 86400)
        stats.append(FileStat(
            uri=f"synthetic://features/part_{i:05d}.json",
            recent_access_count=rng.randint(1, 120),
            last_access_epoch=now - age_s,
        ))
    return stats


# ---------------------------------------------------------------------------
# Feature record reading
# ---------------------------------------------------------------------------

def read_feature_records_from_cache(
    cache_dir: Path,
    file_stats: List[FileStat],
) -> List[FeatureRecord]:
    """
    Read FeatureRecord objects from files already staged in ``cache_dir``.

    Each file is either:
    - A single JSON object  {"user_id":…, "event_count":…, …}
    - NDJSON (one JSON per line)
    Files that cannot be parsed are skipped with a warning.
    """
    records: List[FeatureRecord] = []
    for fs in file_stats:
        filename = _local_name(fs.uri)
        path = cache_dir / filename
        if not path.exists():
            continue
        records.extend(_parse_file(path))
    return records


def read_synthetic_feature_records(n_records: int = 2000) -> List[FeatureRecord]:
    """
    Generate synthetic FeatureRecords for standalone / offline runs.

    Mimics the distribution produced by the Flink CdcUserEventCountJob:
    - 200 distinct users
    - Event counts drawn from a log-normal distribution (realistic skew)
    - 30-second tumbling windows
    """
    rng = random.Random(42)
    now_ms = int(time.time() * 1000)
    window_ms = 30_000
    records: List[FeatureRecord] = []
    for i in range(n_records):
        user_id = rng.randint(1, 200)
        event_count = max(1, int(rng.lognormvariate(2.0, 1.2)))
        ws = now_ms - (i // 10) * window_ms
        records.append(FeatureRecord(
            user_id=user_id,
            window_start_ms=ws,
            window_end_ms=ws + window_ms,
            event_count=event_count,
        ))
    return records


# ---------------------------------------------------------------------------
# Iceberg reader (optional — requires pyiceberg + catalog)
# ---------------------------------------------------------------------------

class IcebergFeatureReader:
    """
    Read features from an Iceberg table via pyiceberg.

    Table schema mirrors the Flink IcebergSinkFactory output:
      user_id LONG, window_start LONG, window_end LONG, event_count LONG

    Falls back silently to the MinIO/synthetic path if pyiceberg is absent.
    """

    TABLE = os.environ.get("ICEBERG_TABLE", "streamforge.features.user_event_counts")
    CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "")

    def is_available(self) -> bool:
        if not self.CATALOG_URI:
            return False
        try:
            import pyiceberg  # type: ignore  # noqa: F401
            return True
        except ImportError:
            return False

    def read(self, limit: int = 5000) -> List[FeatureRecord]:
        from pyiceberg.catalog import load_catalog  # type: ignore

        catalog = load_catalog("default", **{"uri": self.CATALOG_URI})
        table = catalog.load_table(self.TABLE)
        scan = table.scan(limit=limit)
        records = []
        for batch in scan.to_arrow().to_pydict():
            pass  # process via arrow batches
        # Proper arrow read
        arrow_table = table.scan(limit=limit).to_arrow()
        col = arrow_table.column_names
        for i in range(len(arrow_table)):
            row = {c: arrow_table[c][i].as_py() for c in col}
            records.append(FeatureRecord(
                user_id=row.get("user_id", 0),
                window_start_ms=row.get("window_start", 0),
                window_end_ms=row.get("window_end", 0),
                event_count=row.get("event_count", 0),
            ))
        return records


# ---------------------------------------------------------------------------
# MinIO object download into cache dir  (used by prefetch_warmer)
# ---------------------------------------------------------------------------

def download_to_cache(file_stats: List[FileStat], cache_dir: Path) -> int:
    """
    Download MinIO objects referenced in file_stats into cache_dir.

    Returns the number of files successfully downloaded.
    Used by prefetch_warmer as the real-MinIO path for prefetch_files().
    """
    client = _minio_client()
    if client is None:
        return 0

    cache_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for fs in file_stats:
        if not fs.uri.startswith("minio://"):
            continue
        # Parse "minio://bucket/key"
        rest = fs.uri[len("minio://"):]
        bucket, _, key = rest.partition("/")
        dest = cache_dir / _local_name(fs.uri)
        try:
            client.fget_object(bucket, key, str(dest))
            count += 1
        except Exception as exc:
            print(f"[FEATURE_STORE] warn: failed to download {fs.uri}: {exc}")
    return count


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _local_name(uri: str) -> str:
    """Map any URI to a safe flat filename."""
    return uri.replace("minio://", "").replace("synthetic://", "").replace("/", "_")


def _parse_file(path: Path) -> List[FeatureRecord]:
    records: List[FeatureRecord] = []
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return records

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    for line in lines:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        rec = _dict_to_record(obj)
        if rec is not None:
            records.append(rec)

    # If single-object file, try parsing the whole text as one JSON
    if not records:
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                rec = _dict_to_record(obj)
                if rec is not None:
                    records.append(rec)
        except json.JSONDecodeError:
            pass

    return records


def _dict_to_record(obj: dict) -> Optional[FeatureRecord]:
    try:
        return FeatureRecord(
            user_id=int(obj.get("user_id", obj.get("userId", 0))),
            window_start_ms=int(obj.get("window_start", obj.get("window_start_ms", 0))),
            window_end_ms=int(obj.get("window_end",   obj.get("window_end_ms",   0))),
            event_count=int(obj.get("event_count", obj.get("eventCount", 0))),
            sink_received_at=str(obj.get("sink_received_at", "")),
        )
    except (KeyError, ValueError, TypeError):
        return None
