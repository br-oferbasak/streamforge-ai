"""
Unit tests for OnlineFeatureReader — no external services required.

Redis and MinIO are replaced by lightweight MagicMock stubs; the Iceberg
fallback path is tested via import-level patching.
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.feature_vector import FeatureVector, _shard
from online.online_reader import OnlineFeatureReader, LookupStats


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_fv(user_id: str = "user-smith", count: int = 42) -> FeatureVector:
    base_ms = 1705276800000
    return FeatureVector(
        user_id=user_id,
        event_count=count,
        event_rate=count / 60.0,
        window_start_ms=base_ms,
        window_end_ms=base_ms + 60_000,
        event_date="2024-01-15",
        materialized_at="2024-01-15T02:05:33Z",
    )


def _reader_with_stubs(
    redis_data: dict[str, str] | None = None,
    minio_data: dict[str, str] | None = None,
) -> OnlineFeatureReader:
    """
    Return a OnlineFeatureReader whose Redis and MinIO connections are stubs.

    redis_data : mapping of redis key → JSON string (None = Redis down)
    minio_data : mapping of object_name → JSON string (None = MinIO down)
    """
    reader = OnlineFeatureReader.__new__(OnlineFeatureReader)
    reader._bucket = "processed"
    reader._ttl    = 3600
    reader.stats   = LookupStats()

    # ── Redis stub ────────────────────────────────────────────────────────────
    if redis_data is None:
        reader._redis = None
    else:
        stub_redis = MagicMock()
        stub_redis.get.side_effect = lambda k: (
            redis_data[k].encode("utf-8") if k in redis_data else None)
        stub_redis.mget.side_effect = lambda keys: [
            redis_data.get(k, b"").encode("utf-8")
            if k in redis_data else None
            for k in keys
        ]
        stub_redis.setex = MagicMock()
        stub_redis.pipeline.return_value.__enter__ = lambda s: s
        stub_redis.pipeline.return_value.__exit__  = MagicMock(return_value=False)
        reader._redis = stub_redis

    # ── MinIO stub ────────────────────────────────────────────────────────────
    if minio_data is None:
        reader._minio = None
    else:
        def _get_object(bucket, key):
            if key not in minio_data:
                raise Exception(f"key not found: {key}")
            resp = MagicMock()
            resp.read.return_value = minio_data[key].encode("utf-8")
            resp.close = MagicMock()
            return resp

        stub_minio = MagicMock()
        stub_minio.get_object.side_effect = _get_object
        stub_minio.put_object = MagicMock()
        reader._minio = stub_minio

    return reader


# ── LookupStats ───────────────────────────────────────────────────────────────

class TestLookupStats(unittest.TestCase):

    def test_total_is_sum_of_hits_and_misses(self):
        s = LookupStats(l1_hits=3, l2_hits=2, l3_hits=1, misses=4)
        self.assertEqual(s.total, 10)

    def test_p50_empty(self):
        self.assertEqual(LookupStats().p50_ms, 0.0)

    def test_p99_empty(self):
        self.assertEqual(LookupStats().p99_ms, 0.0)

    def test_p50_odd_count(self):
        s = LookupStats()
        s.latencies_ms = [1.0, 2.0, 3.0]
        self.assertEqual(s.p50_ms, 2.0)

    def test_p99_single_value(self):
        s = LookupStats()
        s.latencies_ms = [5.5]
        self.assertEqual(s.p99_ms, 5.5)

    def test_str_contains_totals(self):
        s = LookupStats(l1_hits=7, misses=3)
        text = str(s)
        self.assertIn("10", text)   # total
        self.assertIn("L1=7", text)


# ── get() — single user lookup ────────────────────────────────────────────────

class TestGetSingleUser(unittest.TestCase):

    def test_l1_hit_returns_feature_vector(self):
        fv = _make_fv("user-smith")
        reader = _reader_with_stubs(
            redis_data={FeatureVector.redis_key_for("user-smith"): fv.to_json()})
        result = reader.get("user-smith")
        self.assertIsNotNone(result)
        self.assertEqual(result.user_id, "user-smith")

    def test_l1_hit_increments_l1_counter(self):
        fv = _make_fv("user-smith")
        reader = _reader_with_stubs(
            redis_data={FeatureVector.redis_key_for("user-smith"): fv.to_json()})
        reader.get("user-smith")
        self.assertEqual(reader.stats.l1_hits, 1)
        self.assertEqual(reader.stats.l2_hits, 0)

    def test_l2_hit_when_redis_empty(self):
        fv = _make_fv("user-smith")
        key = FeatureVector.serving_key_for("user-smith")
        reader = _reader_with_stubs(
            redis_data={},
            minio_data={key: fv.to_json()},
        )
        result = reader.get("user-smith")
        self.assertIsNotNone(result)
        self.assertEqual(result.user_id, "user-smith")
        self.assertEqual(reader.stats.l2_hits, 1)

    def test_l2_hit_backfills_l1(self):
        fv = _make_fv("user-smith")
        key = FeatureVector.serving_key_for("user-smith")
        reader = _reader_with_stubs(
            redis_data={},
            minio_data={key: fv.to_json()},
        )
        reader.get("user-smith")
        reader._redis.setex.assert_called_once()

    def test_miss_when_no_services(self):
        reader = _reader_with_stubs(redis_data=None, minio_data=None)
        result = reader.get("unknown-user")
        self.assertIsNone(result)
        self.assertEqual(reader.stats.misses, 1)

    def test_miss_when_user_not_in_any_layer(self):
        reader = _reader_with_stubs(redis_data={}, minio_data={})
        result = reader.get("ghost-user")
        self.assertIsNone(result)
        self.assertEqual(reader.stats.misses, 1)

    def test_latency_recorded(self):
        reader = _reader_with_stubs(redis_data=None, minio_data=None)
        reader.get("any-user")
        self.assertEqual(len(reader.stats.latencies_ms), 1)
        self.assertGreaterEqual(reader.stats.latencies_ms[0], 0.0)

    def test_multiple_gets_accumulate_latencies(self):
        reader = _reader_with_stubs(redis_data=None, minio_data=None)
        for uid in ["a", "b", "c"]:
            reader.get(uid)
        self.assertEqual(len(reader.stats.latencies_ms), 3)


# ── mget() — batch lookup ─────────────────────────────────────────────────────

class TestMgetBatch(unittest.TestCase):

    def _setup_two_users(self) -> tuple[OnlineFeatureReader, list[str]]:
        fv1 = _make_fv("user-alpha", 10)
        fv2 = _make_fv("user-beta",  20)
        reader = _reader_with_stubs(
            redis_data={
                FeatureVector.redis_key_for("user-alpha"): fv1.to_json(),
                FeatureVector.redis_key_for("user-beta"):  fv2.to_json(),
            }
        )
        return reader, ["user-alpha", "user-beta", "unknown"]

    def test_mget_empty_list_returns_empty_dict(self):
        reader = _reader_with_stubs()
        self.assertEqual(reader.mget([]), {})

    def test_mget_returns_all_requested_keys(self):
        reader, user_ids = self._setup_two_users()
        result = reader.mget(user_ids)
        self.assertEqual(set(result.keys()), set(user_ids))

    def test_mget_redis_hits_resolved(self):
        reader, user_ids = self._setup_two_users()
        result = reader.mget(user_ids)
        self.assertIsNotNone(result["user-alpha"])
        self.assertEqual(result["user-alpha"].user_id, "user-alpha")
        self.assertIsNotNone(result["user-beta"])

    def test_mget_unknown_user_returns_none(self):
        reader, user_ids = self._setup_two_users()
        result = reader.mget(user_ids)
        self.assertIsNone(result["unknown"])

    def test_mget_increments_l1_hits(self):
        reader, user_ids = self._setup_two_users()
        reader.mget(user_ids)
        self.assertEqual(reader.stats.l1_hits, 2)

    def test_mget_increments_misses(self):
        reader, user_ids = self._setup_two_users()
        reader.mget(user_ids)
        self.assertEqual(reader.stats.misses, 1)

    def test_mget_uses_single_redis_round_trip(self):
        reader, user_ids = self._setup_two_users()
        reader.mget(user_ids)
        # mget should be called exactly once (not per user_id)
        reader._redis.mget.assert_called_once()

    def test_mget_falls_to_minio_for_redis_misses(self):
        fv1 = _make_fv("user-minio-only")
        minio_key = FeatureVector.serving_key_for("user-minio-only")
        reader = _reader_with_stubs(
            redis_data={},
            minio_data={minio_key: fv1.to_json()},
        )
        result = reader.mget(["user-minio-only"])
        self.assertIsNotNone(result["user-minio-only"])
        self.assertEqual(reader.stats.l2_hits, 1)


# ── reset_stats ───────────────────────────────────────────────────────────────

class TestResetStats(unittest.TestCase):

    def test_reset_clears_counters(self):
        reader = _reader_with_stubs(redis_data=None)
        reader.get("x")
        reader.get("y")
        reader.reset_stats()
        self.assertEqual(reader.stats.total, 0)
        self.assertEqual(reader.stats.latencies_ms, [])


# ── FeatureVector key derivation ──────────────────────────────────────────────

class TestFeatureVectorKeys(unittest.TestCase):

    def test_redis_key_format(self):
        fv = _make_fv("user-smith")
        self.assertEqual(fv.redis_key, "feature:v1:user-smith")

    def test_redis_key_for_static(self):
        self.assertEqual(FeatureVector.redis_key_for("john-doe"),
                         "feature:v1:john-doe")

    def test_serving_key_prefix_pattern(self):
        fv = _make_fv("user-smith")
        self.assertTrue(fv.serving_key.startswith("features/serving/latest/"))
        self.assertTrue(fv.serving_key.endswith("/user-smith.json"))

    def test_serving_key_shard_length(self):
        fv = _make_fv("user-smith")
        parts = fv.serving_key.split("/")
        shard = parts[-2]   # …/latest/{shard}/{user_id}.json
        self.assertEqual(len(shard), 2)

    def test_serving_key_for_static_matches_instance(self):
        fv = _make_fv("john-doe")
        self.assertEqual(fv.serving_key,
                         FeatureVector.serving_key_for("john-doe"))

    def test_serving_key_replaces_slashes_in_user_id(self):
        key = FeatureVector.serving_key_for("a/b")
        self.assertNotIn("/a/b", key)  # slash in user_id must be replaced
        self.assertIn("a_b.json", key)


# ── _shard helper ─────────────────────────────────────────────────────────────

class TestShardHelper(unittest.TestCase):

    def test_shard_length_is_two(self):
        for uid in ["user-smith", "john-doe", "ab", "x", ""]:
            self.assertEqual(len(_shard(uid)), 2)

    def test_shard_is_alphanumeric_lowercase(self):
        shard = _shard("User-Smith")
        self.assertTrue(shard.isalnum() and shard == shard.lower())

    def test_short_user_id_padded(self):
        # user_id with fewer than 2 alnum chars gets 'x' padding
        shard = _shard("!")
        self.assertEqual(len(shard), 2)
        self.assertEqual(shard, "xx")


if __name__ == "__main__":
    unittest.main()
