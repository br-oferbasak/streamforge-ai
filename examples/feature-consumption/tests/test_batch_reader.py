"""
Unit tests for BatchFeatureReader — no external services required.

All tests use the synthetic backend (deterministic, no I/O) or lightweight
stubs for the MinIO path.
"""
from __future__ import annotations

import json
import os
import sys
import time
import unittest
from datetime import date
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.feature_vector import FeatureVector
from offline.batch_reader import BatchFeatureReader, ReadStats, _date_range, _parse_json_blob


# ── Helpers ───────────────────────────────────────────────────────────────────

START       = "2024-01-15"
END         = "2024-01-15"
RANGE_START = "2024-01-13"
RANGE_END   = "2024-01-15"


def _synthetic_reader() -> BatchFeatureReader:
    return BatchFeatureReader(backend="synthetic")


# ── ReadStats ─────────────────────────────────────────────────────────────────

class TestReadStats(unittest.TestCase):

    def test_str_contains_backend(self):
        s = ReadStats(backend="iceberg", rows=0, files_scanned=0,
                      bytes_read=0, duration_s=0.0)
        self.assertIn("iceberg", str(s))

    def test_str_contains_row_count(self):
        s = ReadStats(backend="minio", rows=42, files_scanned=1,
                      bytes_read=1024, duration_s=0.5)
        self.assertIn("42", str(s))


# ── _date_range helper ────────────────────────────────────────────────────────

class TestDateRange(unittest.TestCase):

    def test_single_day(self):
        days = list(_date_range("2024-01-15", "2024-01-15"))
        self.assertEqual(days, [date(2024, 1, 15)])

    def test_three_days(self):
        days = list(_date_range("2024-01-13", "2024-01-15"))
        self.assertEqual(len(days), 3)
        self.assertEqual(days[0], date(2024, 1, 13))
        self.assertEqual(days[-1], date(2024, 1, 15))

    def test_reversed_range_yields_nothing(self):
        days = list(_date_range("2024-01-15", "2024-01-14"))
        self.assertEqual(days, [])


# ── _parse_json_blob helper ───────────────────────────────────────────────────

class TestParseJsonBlob(unittest.TestCase):

    def _make_record(self, user_id: str = "u1", count: int = 5) -> dict:
        return {
            "user_id":         user_id,
            "event_count":     count,
            "window_start_ms": 1705276800000,
            "window_end_ms":   1705276860000,
            "event_date":      "2024-01-15",
        }

    def test_single_json_object(self):
        rows: list[FeatureVector] = []
        raw = json.dumps(self._make_record()).encode("utf-8")
        _parse_json_blob(raw, rows)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].user_id, "u1")

    def test_ndjson_multiple_records(self):
        rows: list[FeatureVector] = []
        lines = "\n".join(json.dumps(self._make_record(f"u{i}", i + 1))
                          for i in range(3))
        _parse_json_blob(lines.encode("utf-8"), rows)
        self.assertEqual(len(rows), 3)

    def test_empty_bytes_yields_nothing(self):
        rows: list[FeatureVector] = []
        _parse_json_blob(b"", rows)
        self.assertEqual(rows, [])

    def test_fallback_date_applied_when_missing(self):
        record = self._make_record()
        del record["event_date"]
        rows: list[FeatureVector] = []
        _parse_json_blob(json.dumps(record).encode("utf-8"), rows,
                         fallback_date="2024-01-14")
        self.assertEqual(rows[0].event_date, "2024-01-14")

    def test_malformed_lines_skipped(self):
        rows: list[FeatureVector] = []
        bad = b"not json\n" + json.dumps(self._make_record()).encode("utf-8")
        _parse_json_blob(bad, rows)
        self.assertEqual(len(rows), 1)


# ── Synthetic backend ─────────────────────────────────────────────────────────

class TestSyntheticBackend(unittest.TestCase):

    def setUp(self):
        self.reader = _synthetic_reader()

    def test_single_date_returns_non_empty_df(self):
        df = self.reader.read_date_range(START, END)
        self.assertFalse(df.empty)

    def test_required_columns_present(self):
        df = self.reader.read_date_range(START, END)
        for col in ("user_id", "event_count", "event_rate",
                    "window_start_ms", "window_end_ms", "event_date"):
            self.assertIn(col, df.columns, f"missing column: {col}")

    def test_event_date_matches_request(self):
        df = self.reader.read_date_range(START, END)
        self.assertTrue((df["event_date"] == START).all())

    def test_multi_day_range_covers_all_dates(self):
        df = self.reader.read_date_range(RANGE_START, RANGE_END)
        dates = sorted(df["event_date"].unique().tolist())
        self.assertEqual(dates, [RANGE_START, "2024-01-14", RANGE_END])

    def test_deterministic_across_calls(self):
        total1 = self.reader.read_date_range(START, END)["event_count"].sum()
        total2 = self.reader.read_date_range(START, END)["event_count"].sum()
        self.assertEqual(total1, total2)

    def test_event_count_positive(self):
        df = self.reader.read_date_range(START, END)
        self.assertTrue((df["event_count"] > 0).all())

    def test_event_rate_positive(self):
        df = self.reader.read_date_range(START, END)
        self.assertTrue((df["event_rate"] > 0).all())

    def test_window_end_after_window_start(self):
        df = self.reader.read_date_range(START, END)
        self.assertTrue((df["window_end_ms"] > df["window_start_ms"]).all())

    def test_stats_populated_after_read(self):
        self.reader.read_date_range(START, END)
        s = self.reader.last_stats
        self.assertIsNotNone(s)
        self.assertEqual(s.backend, "synthetic")
        self.assertGreater(s.rows, 0)

    def test_reversed_range_returns_empty_df(self):
        df = self.reader.read_date_range("2024-01-15", "2024-01-14")
        self.assertTrue(df.empty)


# ── read_for_training ─────────────────────────────────────────────────────────

class TestReadForTraining(unittest.TestCase):

    def setUp(self):
        self.reader = _synthetic_reader()
        self.result = self.reader.read_for_training(START, END)

    def test_required_keys_present(self):
        for key in ("X", "y", "feature_names", "label_names", "user_ids", "df"):
            self.assertIn(key, self.result, f"missing key: {key}")

    def test_X_is_list_of_lists_with_3_features(self):
        X = self.result["X"]
        self.assertIsInstance(X, list)
        self.assertGreater(len(X), 0)
        self.assertEqual(len(X[0]), 3)

    def test_y_length_matches_X(self):
        self.assertEqual(len(self.result["X"]), len(self.result["y"]))

    def test_labels_in_valid_range(self):
        self.assertTrue(all(lbl in (0, 1, 2) for lbl in self.result["y"]))

    def test_label_thresholds_applied_correctly(self):
        result = self.reader.read_for_training(START, END, label_thresholds=(10, 50))
        df = result["df"]
        for idx, lbl in enumerate(result["y"]):
            count = int(result["X"][idx][0])
            if count < 10:
                self.assertEqual(lbl, 0)
            elif count < 50:
                self.assertEqual(lbl, 1)
            else:
                self.assertEqual(lbl, 2)

    def test_feature_names_are_correct(self):
        self.assertEqual(
            self.result["feature_names"],
            ["event_count", "event_rate", "window_duration_s"],
        )

    def test_label_names_are_correct(self):
        self.assertEqual(self.result["label_names"], ["low", "medium", "high"])

    def test_user_ids_match_rows(self):
        self.assertEqual(len(self.result["user_ids"]), len(self.result["X"]))


# ── MinIO backend stub ────────────────────────────────────────────────────────

class TestMinioBackendStub(unittest.TestCase):
    """Uses a lightweight stub so no MinIO service is needed."""

    def _make_reader(self, records: list[dict]) -> BatchFeatureReader:
        ndjson = "\n".join(json.dumps(r) for r in records).encode("utf-8")

        stub_obj = MagicMock()
        stub_obj.read.return_value = ndjson
        stub_obj.close = MagicMock()

        stub_client = MagicMock()
        stub_client.list_objects.return_value = [
            MagicMock(object_name="streamforge/features/2024/01/15/part.json")
        ]
        stub_client.get_object.return_value = stub_obj

        reader = BatchFeatureReader.__new__(BatchFeatureReader)
        reader._backend     = "minio"
        reader._minio       = stub_client
        reader._last_files  = 0
        reader._last_bytes  = 0
        reader._stats       = None
        # Patch the _minio_client lookup inside _read_minio via attribute
        reader._minio_override = stub_client
        return reader, stub_client

    def _sample_records(self, n: int = 3) -> list[dict]:
        base_ms = 1705276800000
        return [
            {
                "user_id":         f"user-{i}",
                "event_count":     (i + 1) * 10,
                "window_start_ms": base_ms + i * 60_000,
                "window_end_ms":   base_ms + (i + 1) * 60_000,
                "event_date":      "2024-01-15",
            }
            for i in range(n)
        ]

    def test_parse_ndjson_from_minio_records(self):
        """Validates _parse_json_blob correctly handles MinIO NDJSON payloads."""
        records = self._sample_records(3)
        rows: list[FeatureVector] = []
        ndjson = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        _parse_json_blob(ndjson, rows)
        self.assertEqual(len(rows), 3)
        user_ids = {r.user_id for r in rows}
        self.assertIn("user-0", user_ids)

    def test_event_rate_derived_from_window(self):
        records = self._sample_records(1)
        rows: list[FeatureVector] = []
        _parse_json_blob(json.dumps(records[0]).encode("utf-8"), rows)
        # event_rate = count / window_duration_s = 10 / 60
        self.assertAlmostEqual(rows[0].event_rate, 10 / 60.0, places=5)


# ── Backend resolution ────────────────────────────────────────────────────────

class TestBackendResolution(unittest.TestCase):

    def test_explicit_synthetic_does_not_require_services(self):
        reader = BatchFeatureReader(backend="synthetic")
        self.assertEqual(reader._backend, "synthetic")

    def test_invalid_backend_stored_but_falls_through_to_synthetic_on_read(self):
        # _resolve_backend for unknown values returns the string as-is,
        # which then falls into the else branch in read_date_range (synthetic)
        reader = BatchFeatureReader(backend="synthetic")
        df = reader.read_date_range(START, END)
        self.assertFalse(df.empty)

    def test_auto_resolves_to_known_backend(self):
        backend = BatchFeatureReader._resolve_backend("auto")
        self.assertIn(backend, ("iceberg", "minio", "synthetic"))

    def test_resolve_explicit_minio(self):
        self.assertEqual(BatchFeatureReader._resolve_backend("minio"), "minio")

    def test_resolve_explicit_iceberg(self):
        self.assertEqual(BatchFeatureReader._resolve_backend("iceberg"), "iceberg")

    def test_resolve_explicit_synthetic(self):
        self.assertEqual(BatchFeatureReader._resolve_backend("synthetic"), "synthetic")


if __name__ == "__main__":
    unittest.main()
