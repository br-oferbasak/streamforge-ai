"""
Offline (batch) feature reader.

Physical layout read by this module
------------------------------------
Iceberg on MinIO (primary):

  s3a://streamforge/warehouse/streamforge/user_event_counts/data/
    event_date=2024-01-14/
      00000-0-<uuid>-00001.parquet   ← 128 MB target, columnar Parquet
      00000-0-<uuid>-00002.parquet
    event_date=2024-01-15/
      00000-0-<uuid>-00001.parquet

  Partition field : event_date  (identity transform, YYYY-MM-DD UTC)
  File format     : Parquet
  Read via        : PyIceberg → Apache Arrow → pandas DataFrame

MinIO raw JSON (fallback when Iceberg catalog is unavailable):

  s3://processed/streamforge/features/
    2024/01/14/
      153000-1705194000123.json      ← one JSON object per file (feature-sink)
    2024/01/15/
      020000-1705280400456.json

  Format: {"user_id":…, "event_count":…, "window_start_ms":…, "window_end_ms":…}

Backends are auto-detected at construction time.  Pass backend="iceberg" or
backend="minio" to force a specific path.

Usage
-----
  from offline.batch_reader import BatchFeatureReader

  # Date-range scan (Iceberg partition-pruned):
  reader = BatchFeatureReader()
  df = reader.read_date_range("2024-01-01", "2024-01-07")
  # df columns: user_id, event_count, event_rate, window_start_ms,
  #             window_end_ms, event_date

  # ML-ready training split:
  dataset = reader.read_for_training("2024-01-01", "2024-01-07")
  X, y, names = dataset["X"], dataset["y"], dataset["feature_names"]

Environment variables
---------------------
  ICEBERG_CATALOG_TYPE   hadoop (default) | hive | rest
  ICEBERG_WAREHOUSE      s3a://streamforge/warehouse
  ICEBERG_DATABASE       streamforge
  ICEBERG_TABLE          user_event_counts
  ICEBERG_S3_ENDPOINT    http://minio:9000
  ICEBERG_S3_ACCESS_KEY  minioadmin
  ICEBERG_S3_SECRET_KEY  minioadmin
  MINIO_ENDPOINT         localhost:9000
  MINIO_ACCESS_KEY       minioadmin
  MINIO_SECRET_KEY       minioadmin
  MINIO_BUCKET           processed
  MINIO_PREFIX           streamforge/features
"""
from __future__ import annotations

import io
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.feature_vector import FeatureVector, _ms_to_date


# ── Read stats ───────────────────────────────────────────────────────────────

@dataclass
class ReadStats:
    backend: str
    rows: int
    files_scanned: int
    bytes_read: int
    duration_s: float

    def __str__(self) -> str:
        return (
            f"ReadStats(backend={self.backend}, rows={self.rows:,}, "
            f"files={self.files_scanned}, bytes={self.bytes_read / 1e6:.1f} MB, "
            f"duration={self.duration_s:.2f}s)"
        )


# ── BatchFeatureReader ────────────────────────────────────────────────────────

class BatchFeatureReader:
    """
    Date-range reader for the StreamForge user_event_counts feature table.

    Backend selection (in priority order):
      1. Iceberg via PyIceberg  — partition-pruned Parquet scans; best for large ranges
      2. MinIO raw JSON         — list objects by date prefix; no Parquet overhead
      3. Synthetic              — generated locally; zero external dependencies
    """

    def __init__(self, backend: str = "auto") -> None:
        """
        Parameters
        ----------
        backend : "auto" | "iceberg" | "minio" | "synthetic"
            "auto" tries Iceberg, then MinIO, then synthetic.
        """
        self._backend = self._resolve_backend(backend)
        self._stats: ReadStats | None = None

    # ── Public API ────────────────────────────────────────────────────────────

    def read_date_range(self, start_date: str, end_date: str) -> "pd.DataFrame":
        """
        Return all feature rows where event_date is in [start_date, end_date].

        Parameters
        ----------
        start_date, end_date : str
            Inclusive date range in YYYY-MM-DD format (UTC).

        Returns
        -------
        pd.DataFrame
            Columns: user_id, event_count, event_rate, window_start_ms,
                     window_end_ms, event_date
        """
        t0 = time.perf_counter()
        rows: list[FeatureVector]

        if self._backend == "iceberg":
            rows = self._read_iceberg(start_date, end_date)
        elif self._backend == "minio":
            rows = self._read_minio(start_date, end_date)
        else:
            rows = self._read_synthetic(start_date, end_date)

        df = _to_dataframe(rows)
        self._stats = ReadStats(
            backend=self._backend,
            rows=len(df),
            files_scanned=getattr(self, "_last_files", 0),
            bytes_read=getattr(self, "_last_bytes", 0),
            duration_s=time.perf_counter() - t0,
        )
        print(f"[batch_reader] {self._stats}", file=sys.stderr)
        return df

    def read_for_training(
        self,
        start_date: str,
        end_date: str,
        label_thresholds: tuple[int, int] = (10, 50),
    ) -> dict:
        """
        Return an ML-ready dict with feature matrix X, label vector y, and metadata.

        Labels are derived from event_count:
          0 = low    (event_count <  label_thresholds[0])
          1 = medium (event_count >= label_thresholds[0] and < label_thresholds[1])
          2 = high   (event_count >= label_thresholds[1])

        Returns
        -------
        dict with keys:
          "X"             : list[list[float]]  — [[event_count, event_rate, window_s], ...]
          "y"             : list[int]          — activity label
          "feature_names" : list[str]
          "label_names"   : list[str]
          "user_ids"      : list[str]
          "df"            : pd.DataFrame       — full frame for inspection
        """
        df = self.read_date_range(start_date, end_date)
        lo, hi = label_thresholds

        X, y, user_ids = [], [], []
        for _, row in df.iterrows():
            X.append([float(row["event_count"]),
                      float(row["event_rate"]),
                      float(row["window_duration_s"])])
            cnt = int(row["event_count"])
            y.append(0 if cnt < lo else (1 if cnt < hi else 2))
            user_ids.append(row["user_id"])

        return {
            "X": X,
            "y": y,
            "feature_names": ["event_count", "event_rate", "window_duration_s"],
            "label_names": ["low", "medium", "high"],
            "user_ids": user_ids,
            "df": df,
        }

    @property
    def last_stats(self) -> ReadStats | None:
        return self._stats

    # ── Backend: Iceberg via PyIceberg ────────────────────────────────────────

    def _read_iceberg(self, start_date: str, end_date: str) -> list[FeatureVector]:
        """
        Uses PyIceberg to scan the Iceberg table with an event_date range filter.
        The planner prunes partitions outside the range — only relevant Parquet
        files are opened.

        Scan plan:
          table.scan(
              row_filter="event_date >= '2024-01-01' AND event_date <= '2024-01-07'"
          ).to_arrow()
        """
        try:
            from pyiceberg.catalog import load_catalog
            from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual
        except ImportError:
            print("[batch_reader] pyiceberg not installed — falling back to MinIO",
                  file=sys.stderr)
            self._backend = "minio"
            return self._read_minio(start_date, end_date)

        catalog_conf = _iceberg_catalog_conf()
        try:
            catalog = load_catalog("streamforge", **catalog_conf)
            table_name = _env("ICEBERG_TABLE", "user_event_counts")
            db         = _env("ICEBERG_DATABASE", "streamforge")
            table      = catalog.load_table(f"{db}.{table_name}")
        except Exception as exc:
            print(f"[batch_reader] Iceberg catalog unavailable ({exc}) "
                  f"— falling back to MinIO", file=sys.stderr)
            self._backend = "minio"
            return self._read_minio(start_date, end_date)

        row_filter = (
            f"event_date >= '{start_date}' AND event_date <= '{end_date}'"
        )
        scan = table.scan(row_filter=row_filter)

        try:
            arrow_table = scan.to_arrow()
        except Exception as exc:
            print(f"[batch_reader] Iceberg scan failed ({exc}) — falling back to MinIO",
                  file=sys.stderr)
            self._backend = "minio"
            return self._read_minio(start_date, end_date)

        self._last_bytes = arrow_table.nbytes
        self._last_files = scan.plan_files().__class__.__name__ and 0  # approximate

        rows = []
        for batch in arrow_table.to_pydict():
            pass  # iterate columns below

        cols = arrow_table.to_pydict()
        n = len(cols.get("user_id", []))
        self._last_files = n  # file count not easily available; use row count
        for i in range(n):
            rows.append(FeatureVector.from_dict({
                "user_id":         cols["user_id"][i],
                "event_count":     cols["event_count"][i],
                "window_start_ms": cols["window_start_ms"][i],
                "window_end_ms":   cols["window_end_ms"][i],
                "event_date":      cols.get("event_date", [""] * n)[i],
            }))
        return rows

    # ── Backend: MinIO raw JSON ───────────────────────────────────────────────

    def _read_minio(self, start_date: str, end_date: str) -> list[FeatureVector]:
        """
        Lists objects under streamforge/features/YYYY/MM/DD/ for each date in
        [start_date, end_date] and downloads + parses each JSON file.

        Object path pattern:
          {MINIO_PREFIX}/{YYYY}/{MM}/{DD}/{HHmmss}-{epoch_ms}.json
        """
        try:
            from minio import Minio
        except ImportError:
            print("[batch_reader] minio package not installed — using synthetic",
                  file=sys.stderr)
            self._backend = "synthetic"
            return self._read_synthetic(start_date, end_date)

        client = _minio_client()
        bucket = _env("MINIO_BUCKET", "processed")
        prefix = _env("MINIO_PREFIX", "streamforge/features")

        rows: list[FeatureVector] = []
        files = 0
        total_bytes = 0

        for d in _date_range(start_date, end_date):
            day_prefix = f"{prefix}/{d.strftime('%Y/%m/%d')}/"
            try:
                objects = list(client.list_objects(bucket, prefix=day_prefix,
                                                   recursive=True))
            except Exception:
                continue

            for obj in objects:
                if not obj.object_name.endswith(".json"):
                    continue
                try:
                    resp = client.get_object(bucket, obj.object_name)
                    raw  = resp.read()
                    resp.close()
                    total_bytes += len(raw)
                    files += 1
                    _parse_json_blob(raw, rows, fallback_date=d.isoformat())
                except Exception:
                    continue

        self._last_files = files
        self._last_bytes = total_bytes

        if not rows:
            print("[batch_reader] MinIO returned no records — using synthetic",
                  file=sys.stderr)
            self._backend = "synthetic"
            return self._read_synthetic(start_date, end_date)

        return rows

    # ── Backend: Synthetic ───────────────────────────────────────────────────

    def _read_synthetic(self, start_date: str, end_date: str,
                        n_users: int = 50, windows_per_user: int = 24) -> list[FeatureVector]:
        """
        Generates deterministic synthetic records for the date range.
        No external services required — useful for local development and CI.
        """
        import random
        rng = random.Random(42)
        rows: list[FeatureVector] = []
        for d in _date_range(start_date, end_date):
            midnight_ms = int(d.strftime("%s")) * 1000  # approximate
            for u in range(n_users):
                user_id = f"synthetic-user-{u:04d}"
                for w in range(windows_per_user):
                    start_ms = midnight_ms + w * 3_600_000
                    count    = rng.randint(1, 200)
                    rows.append(FeatureVector(
                        user_id=user_id,
                        event_count=count,
                        event_rate=count / 3600.0,
                        window_start_ms=start_ms,
                        window_end_ms=start_ms + 3_600_000,
                        event_date=d.isoformat(),
                    ))
        self._last_files = len(rows)
        self._last_bytes = 0
        return rows

    # ── Backend resolution ────────────────────────────────────────────────────

    @staticmethod
    def _resolve_backend(requested: str) -> str:
        if requested != "auto":
            return requested
        try:
            import pyiceberg  # noqa: F401
            return "iceberg"
        except ImportError:
            pass
        try:
            import minio  # noqa: F401
            return "minio"
        except ImportError:
            pass
        return "synthetic"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_dataframe(rows: list[FeatureVector]) -> "pd.DataFrame":
    try:
        import pandas as pd
    except ImportError:
        raise RuntimeError("pandas is required: pip install pandas")

    if not rows:
        return pd.DataFrame(columns=[
            "user_id", "event_count", "event_rate",
            "window_start_ms", "window_end_ms", "event_date", "window_duration_s",
        ])

    records = [{
        "user_id":          r.user_id,
        "event_count":      r.event_count,
        "event_rate":       r.event_rate,
        "window_start_ms":  r.window_start_ms,
        "window_end_ms":    r.window_end_ms,
        "event_date":       r.event_date,
        "window_duration_s": r.window_duration_s,
    } for r in rows]
    return pd.DataFrame(records)


def _parse_json_blob(raw: bytes, rows: list[FeatureVector], fallback_date: str = "") -> None:
    """Parse either a single JSON object or NDJSON (newline-delimited) bytes."""
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return
    # Try NDJSON first (multiple records per file)
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
            if "user_id" in d:
                if not d.get("event_date"):
                    d["event_date"] = fallback_date
                rows.append(FeatureVector.from_dict(d))
        except json.JSONDecodeError:
            pass


def _date_range(start: str, end: str):
    d = date.fromisoformat(start)
    last = date.fromisoformat(end)
    while d <= last:
        yield d
        d += timedelta(days=1)


def _minio_client():
    from minio import Minio
    return Minio(
        endpoint=_env("MINIO_ENDPOINT", "localhost:9000"),
        access_key=_env("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=_env("MINIO_SECRET_KEY", "minioadmin"),
        secure=_env("MINIO_SECURE", "false").lower() == "true",
    )


def _iceberg_catalog_conf() -> dict:
    conf: dict = {"type": _env("ICEBERG_CATALOG_TYPE", "hadoop")}
    warehouse = _env("ICEBERG_WAREHOUSE", "s3a://streamforge/warehouse")
    endpoint  = _env("ICEBERG_S3_ENDPOINT", "")

    if conf["type"] == "rest":
        conf["uri"] = warehouse
    else:
        conf["warehouse"] = warehouse

    if endpoint:
        conf["s3.endpoint"]              = endpoint
        conf["s3.access-key-id"]         = _env("ICEBERG_S3_ACCESS_KEY", "minioadmin")
        conf["s3.secret-access-key"]     = _env("ICEBERG_S3_SECRET_KEY", "minioadmin")
        conf["s3.path-style-access"]     = "true"

    return conf


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


# ── CLI demo ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Offline batch feature reader demo")
    parser.add_argument("--start",   default="2024-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",     default="2024-01-07", help="End date YYYY-MM-DD")
    parser.add_argument("--backend", default="auto",
                        choices=["auto", "iceberg", "minio", "synthetic"])
    parser.add_argument("--training", action="store_true",
                        help="Print ML training dataset stats instead of raw rows")
    args = parser.parse_args()

    reader = BatchFeatureReader(backend=args.backend)

    if args.training:
        ds = reader.read_for_training(args.start, args.end)
        print(f"\nTraining dataset: {len(ds['X'])} samples, "
              f"classes={ds['label_names']}, features={ds['feature_names']}")
        counts = [0, 0, 0]
        for lbl in ds["y"]:
            counts[lbl] += 1
        for i, name in enumerate(ds["label_names"]):
            print(f"  {name:8s}: {counts[i]:5d} samples ({100*counts[i]/max(len(ds['y']),1):.1f}%)")
    else:
        df = reader.read_date_range(args.start, args.end)
        print(f"\n{df.head(10).to_string(index=False)}")
        print(f"\n{len(df):,} rows, {df['user_id'].nunique():,} unique users")
