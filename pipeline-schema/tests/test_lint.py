"""
Tests for sf_lint — validates that the linter accepts all valid fixtures
and rejects all invalid fixtures with the expected diagnostics.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# Make sf_lint importable without installation
SCHEMA_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SCHEMA_DIR))

import sf_lint  # noqa: E402

SCHEMA_PATH = SCHEMA_DIR / "pipeline.schema.json"
VALID_DIR   = Path(__file__).parent / "fixtures" / "valid"
INVALID_DIR = Path(__file__).parent / "fixtures" / "invalid"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load(path: Path) -> dict:
    import yaml  # type: ignore
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _validate(doc: dict):
    schema = _schema()
    return sf_lint._validate_schema(doc, schema)


def _semantic(doc: dict):
    return sf_lint._semantic_checks(doc)


def _lint(path: Path) -> list[sf_lint.Finding]:
    doc, err = sf_lint._load_yaml(path)
    assert err is None, f"YAML parse error: {err}"
    # Always run both passes — mirrors cmd_lint behaviour.
    findings = _validate(doc)
    findings += _semantic(doc)
    return findings


# ---------------------------------------------------------------------------
# Valid fixtures — must produce zero ERRORs
# ---------------------------------------------------------------------------

VALID_FIXTURES = list(VALID_DIR.glob("*.yaml"))

@pytest.mark.parametrize("path", VALID_FIXTURES, ids=[p.name for p in VALID_FIXTURES])
def test_valid_fixture_passes(path: Path):
    findings = _lint(path)
    errors = [f for f in findings if f.severity == sf_lint.ERROR]
    assert errors == [], (
        f"{path.name} should be valid but got errors:\n"
        + "\n".join(str(f) for f in errors)
    )


# ---------------------------------------------------------------------------
# Invalid fixtures — must produce at least one ERROR
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("filename,expected_fragment", [
    ("missing-version.yaml",          "'version' is a required property"),
    ("missing-source.yaml",           "'source' is a required property"),
    ("bad-name-pattern.yaml",         "does not match"),
    ("bad-source-type.yaml",          "is not valid"),
    ("iceberg-rest-missing-uri.yaml", "catalog_uri"),
    ("duplicate-transform-ids.yaml",  "Duplicate transform id"),
    ("sliding-window-bad-slide.yaml", "slide_seconds"),
    ("dlq-same-as-source.yaml",       "feedback loop"),
])
def test_invalid_fixture_fails(filename: str, expected_fragment: str):
    path = INVALID_DIR / filename
    assert path.exists(), f"Fixture not found: {path}"
    findings = _lint(path)
    errors = [f for f in findings if f.severity == sf_lint.ERROR]
    assert errors, f"{filename} should fail validation but produced no errors."
    messages = " ".join(f.message for f in errors)
    assert expected_fragment.lower() in messages.lower(), (
        f"{filename}: expected fragment '{expected_fragment}' not found in:\n{messages}"
    )


# ---------------------------------------------------------------------------
# Unit tests for specific schema rules
# ---------------------------------------------------------------------------

class TestSchemaEvolutionTransform:
    def _make_pipeline(self, **tx_fields) -> dict:
        return {
            "version": "1",
            "name": "test-pipeline",
            "source": {"type": "kafka_cdc", "topic": "t", "bootstrap_servers": "localhost:9092"},
            "transforms": [{"id": "ev", "type": "schema_evolution", **tx_fields}],
            "sink": {"type": "kafka", "topic": "out", "bootstrap_servers": "localhost:9092"},
        }

    def test_dead_letter_requires_topic(self):
        doc = self._make_pipeline(on_breaking_change="dead_letter")
        # Caught by semantic check (JSON Schema if/then doesn't propagate through allOf+oneOf).
        errors = [f for f in _semantic(doc) if f.severity == sf_lint.ERROR]
        assert any("dead_letter_topic" in f.message or "dead_letter_topic" in f.path for f in errors)

    def test_dead_letter_with_topic_ok(self):
        doc = self._make_pipeline(
            on_breaking_change="dead_letter",
            dead_letter_topic="cdc.dead.letter",
        )
        errors = [f for f in _validate(doc) if f.severity == sf_lint.ERROR]
        assert not errors

    def test_skip_no_topic_needed(self):
        doc = self._make_pipeline(on_breaking_change="skip")
        errors = [f for f in _validate(doc) if f.severity == sf_lint.ERROR]
        assert not errors

    def test_dlq_same_as_source_rejected(self):
        doc = self._make_pipeline(
            on_breaking_change="dead_letter",
            dead_letter_topic="t",   # same as source topic "t"
        )
        # Schema validates; semantic check catches it
        schema_errors = [f for f in _validate(doc) if f.severity == sf_lint.ERROR]
        assert not schema_errors
        sem_errors = [f for f in _semantic(doc) if f.severity == sf_lint.ERROR]
        assert any("feedback" in f.message.lower() for f in sem_errors)


class TestSlidingWindow:
    def _pipeline_with_sliding(self, ws: int, slide: int) -> dict:
        return {
            "version": "1",
            "name": "test-sliding",
            "source": {"type": "kafka_cdc", "topic": "t", "bootstrap_servers": "localhost:9092"},
            "transforms": [{
                "id": "sw",
                "type": "sliding_window",
                "key_by": "user_id",
                "window_size_seconds": ws,
                "slide_seconds": slide,
                "aggregate": "count",
            }],
            "sink": {"type": "kafka", "topic": "out", "bootstrap_servers": "localhost:9092"},
        }

    def test_slide_less_than_window_ok(self):
        doc = self._pipeline_with_sliding(300, 60)
        errors = [f for f in (_validate(doc) + _semantic(doc)) if f.severity == sf_lint.ERROR]
        assert not errors

    def test_slide_equals_window_ok(self):
        doc = self._pipeline_with_sliding(60, 60)
        errors = [f for f in (_validate(doc) + _semantic(doc)) if f.severity == sf_lint.ERROR]
        assert not errors

    def test_slide_greater_than_window_fails(self):
        doc = self._pipeline_with_sliding(60, 120)
        sem = [f for f in _semantic(doc) if f.severity == sf_lint.ERROR]
        assert any("slide_seconds" in f.path for f in sem)


class TestIcebergSink:
    def _pipeline(self, **sink_fields) -> dict:
        return {
            "version": "1",
            "name": "iceberg-test",
            "source": {"type": "kafka_cdc", "topic": "t", "bootstrap_servers": "localhost:9092"},
            "sink": {"type": "iceberg", **sink_fields},
        }

    def test_hadoop_no_uri_required(self):
        doc = self._pipeline(
            catalog_type="hadoop",
            warehouse="file:///tmp/wh",
            database="db",
            table="tbl",
        )
        errors = [f for f in _validate(doc) if f.severity == sf_lint.ERROR]
        assert not errors

    def test_rest_catalog_requires_uri(self):
        doc = self._pipeline(
            catalog_type="rest",
            warehouse="s3a://bucket/wh",
            database="db",
            table="tbl",
        )
        errors = [f for f in (_validate(doc) + _semantic(doc)) if f.severity == sf_lint.ERROR]
        assert errors

    def test_rest_with_uri_ok(self):
        doc = self._pipeline(
            catalog_type="rest",
            catalog_uri="http://catalog:8181",
            warehouse="s3a://bucket/wh",
            database="db",
            table="tbl",
        )
        errors = [f for f in _validate(doc) if f.severity == sf_lint.ERROR]
        assert not errors


class TestDuplicateTransformIds:
    def test_duplicate_ids_caught(self):
        doc = {
            "version": "1",
            "name": "dup-ids",
            "source": {"type": "kafka_cdc", "topic": "t", "bootstrap_servers": "localhost:9092"},
            "transforms": [
                {"id": "step1", "type": "filter", "condition": "op == 'c'"},
                {"id": "step1", "type": "filter", "condition": "op == 'u'"},
            ],
            "sink": {"type": "kafka", "topic": "out", "bootstrap_servers": "localhost:9092"},
        }
        sem = [f for f in _semantic(doc) if f.severity == sf_lint.ERROR]
        assert any("step1" in f.message for f in sem)

    def test_unique_ids_ok(self):
        doc = {
            "version": "1",
            "name": "unique-ids",
            "source": {"type": "kafka_cdc", "topic": "t", "bootstrap_servers": "localhost:9092"},
            "transforms": [
                {"id": "step1", "type": "filter", "condition": "op == 'c'"},
                {"id": "step2", "type": "filter", "condition": "op == 'u'"},
            ],
            "sink": {"type": "kafka", "topic": "out", "bootstrap_servers": "localhost:9092"},
        }
        sem = [f for f in _semantic(doc) if f.severity == sf_lint.ERROR]
        assert not sem


class TestCLI:
    def test_lint_valid_file_exits_zero(self, tmp_path):
        src = VALID_DIR / "minimal.yaml"
        result = sf_lint.main(["lint", str(src), "--no-colour"])
        assert result == 0

    def test_lint_invalid_file_exits_one(self, tmp_path):
        src = INVALID_DIR / "missing-source.yaml"
        result = sf_lint.main(["lint", str(src), "--no-colour"])
        assert result == 1

    def test_lint_nonexistent_file(self, tmp_path):
        result = sf_lint.main(["lint", str(tmp_path / "nope.yaml"), "--no-colour"])
        assert result == 1

    def test_lint_bad_schema_exits_two(self, tmp_path, monkeypatch):
        src = VALID_DIR / "minimal.yaml"
        result = sf_lint.main(["lint", str(src), "--schema", str(tmp_path / "no.json"), "--no-colour"])
        assert result == 2

    def test_lint_multiple_files(self):
        files = [str(p) for p in VALID_FIXTURES]
        result = sf_lint.main(["lint", "--no-colour"] + files)
        assert result == 0
