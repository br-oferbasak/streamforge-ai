#!/usr/bin/env python3
"""
sf_lint — StreamForge Pipeline YAML linter
==========================================

Validates one or more pipeline YAML files against the StreamForge JSON Schema
(pipeline.schema.json).  Also applies a set of semantic checks that go beyond
what JSON Schema alone can express.

Usage
-----
  python sf_lint.py lint pipeline.yaml [pipeline2.yaml ...]
  python sf_lint.py lint examples/*.yaml
  python sf_lint.py lint --schema /custom/path/pipeline.schema.json pipeline.yaml
  python sf_lint.py lint --strict pipeline.yaml        # warnings become errors

Exit codes
----------
  0  All files valid (warnings are printed but do not affect exit code unless --strict)
  1  One or more validation errors
  2  Tool-level error (missing dep, bad schema path, etc.)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterator, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------

def _require(package: str, import_name: Optional[str] = None) -> Any:
    name = import_name or package
    try:
        return __import__(name)
    except ImportError:
        print(f"[sf_lint] error: required package '{package}' is not installed.", file=sys.stderr)
        print(f"         Run: pip install {package}", file=sys.stderr)
        sys.exit(2)


# ---------------------------------------------------------------------------
# Severity levels
# ---------------------------------------------------------------------------

ERROR   = "ERROR"
WARNING = "WARNING"


class Finding:
    __slots__ = ("severity", "path", "message")

    def __init__(self, severity: str, path: str, message: str):
        self.severity = severity
        self.path     = path
        self.message  = message

    def __str__(self) -> str:
        return f"  [{self.severity}] {self.path}: {self.message}"


# ---------------------------------------------------------------------------
# JSON Schema validation
# ---------------------------------------------------------------------------

def _load_schema(schema_path: Path) -> dict:
    try:
        return json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[sf_lint] error: cannot load schema {schema_path}: {exc}", file=sys.stderr)
        sys.exit(2)


def _validate_schema(doc: dict, schema: dict) -> List[Finding]:
    jsonschema = _require("jsonschema")
    validator_cls = jsonschema.Draft7Validator
    validator = validator_cls(schema)
    findings: List[Finding] = []
    for err in sorted(validator.iter_errors(doc), key=lambda e: e.path):
        path = "/" + "/".join(str(p) for p in err.absolute_path) if err.absolute_path else "/"
        findings.append(Finding(ERROR, path, err.message))
    return findings


# ---------------------------------------------------------------------------
# Semantic checks (rules JSON Schema cannot express)
# ---------------------------------------------------------------------------

def _semantic_checks(doc: dict) -> List[Finding]:
    findings: List[Finding] = []

    # Track transform IDs for duplicate detection
    ids_seen: set[str] = set()

    for i, tx in enumerate(doc.get("transforms", [])):
        base = f"/transforms/{i}"
        tid  = tx.get("id", "")

        # Duplicate transform IDs
        if tid in ids_seen:
            findings.append(Finding(ERROR, f"{base}/id", f"Duplicate transform id '{tid}'."))
        ids_seen.add(tid)

        kind = tx.get("type", "")

        # sliding_window: slide must be ≤ window_size
        if kind == "sliding_window":
            ws = tx.get("window_size_seconds", 0)
            sl = tx.get("slide_seconds", 0)
            if sl > ws:
                findings.append(Finding(
                    ERROR, f"{base}/slide_seconds",
                    f"slide_seconds ({sl}) must be ≤ window_size_seconds ({ws})."
                ))

        # schema_evolution: dead_letter_topic checks
        # (JSON Schema if/then doesn't propagate reliably through allOf+oneOf in Draft-07,
        #  so both the presence and the feedback-loop rules live here.)
        if kind == "schema_evolution":
            obc = tx.get("on_breaking_change", "dead_letter")
            if obc == "dead_letter":
                dlq = tx.get("dead_letter_topic", "")
                if not dlq:
                    findings.append(Finding(
                        ERROR, f"{base}/dead_letter_topic",
                        "dead_letter_topic is required when on_breaking_change is 'dead_letter'."
                    ))
                elif dlq == doc.get("source", {}).get("topic", ""):
                    findings.append(Finding(
                        ERROR, f"{base}/dead_letter_topic",
                        "dead_letter_topic must differ from the source topic to avoid feedback loops."
                    ))

        # filter with on_drop=dead_letter requires dead_letter_topic
        if kind == "filter" and tx.get("on_drop") == "dead_letter" and not tx.get("dead_letter_topic"):
            findings.append(Finding(
                ERROR, f"{base}/dead_letter_topic",
                "dead_letter_topic is required when on_drop is 'dead_letter'."
            ))

    # Iceberg rest/hive catalog_uri check (belt-and-suspenders; schema already checks this)
    sink = doc.get("sink", {})
    if sink.get("type") == "iceberg":
        ct = sink.get("catalog_type", "hadoop")
        if ct in ("rest", "hive") and not sink.get("catalog_uri"):
            findings.append(Finding(
                ERROR, "/sink/catalog_uri",
                f"catalog_uri is required when catalog_type is '{ct}'."
            ))

    # Warn if prefetch enabled but sink has no MinIO / S3 source
    prefetch = doc.get("prefetch", {})
    if prefetch.get("enabled", True) and "source" not in prefetch:
        findings.append(Finding(
            WARNING, "/prefetch/source",
            "Prefetch is enabled but no source is defined; "
            "the prefetch engine will fall back to synthetic data."
        ))

    # Warn about unresolved env-var references (${VAR} not in environment)
    for path, value in _walk_strings(doc):
        for var, default in _env_refs(value):
            if var not in os.environ and default is None:
                findings.append(Finding(
                    WARNING, path,
                    f"Environment variable '${{{var}}}' is not set and has no default."
                ))

    return findings


def _walk_strings(obj: Any, path: str = "") -> Iterator[Tuple[str, str]]:
    """Recursively yield (json_pointer, string_value) for every string in the doc."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk_strings(v, f"{path}/{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _walk_strings(v, f"{path}/{i}")
    elif isinstance(obj, str):
        yield path, obj


_ENV_REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _env_refs(value: str) -> Iterator[Tuple[str, Optional[str]]]:
    """Yield (var_name, default_or_None) for every ${VAR} or ${VAR:-default} in value."""
    for m in _ENV_REF_RE.finditer(value):
        yield m.group(1), m.group(2)


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> Tuple[Optional[dict], Optional[str]]:
    yaml = _require("pyyaml", "yaml")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return None, str(exc)
    try:
        return yaml.safe_load(text), None
    except yaml.YAMLError as exc:
        return None, f"YAML parse error: {exc}"


# ---------------------------------------------------------------------------
# Result formatting
# ---------------------------------------------------------------------------

_RESET  = "\033[0m"
_RED    = "\033[31m"
_YELLOW = "\033[33m"
_GREEN  = "\033[32m"
_BOLD   = "\033[1m"

def _coloured(text: str, code: str, use_colour: bool) -> str:
    return f"{code}{text}{_RESET}" if use_colour else text


def _print_result(
    file_path: str,
    findings: List[Finding],
    use_colour: bool,
    show_ok: bool,
) -> Tuple[int, int]:
    errors   = [f for f in findings if f.severity == ERROR]
    warnings = [f for f in findings if f.severity == WARNING]

    if not findings and show_ok:
        icon = _coloured("✓", _GREEN, use_colour)
        print(f"{icon} {file_path}")
        return 0, 0

    icon = _coloured("✗", _RED, use_colour) if errors else _coloured("⚠", _YELLOW, use_colour)
    print(f"{icon} {_coloured(file_path, _BOLD, use_colour)}")
    for f in findings:
        colour = _RED if f.severity == ERROR else _YELLOW
        sev = _coloured(f.severity, colour, use_colour)
        print(f"    [{sev}] {f.path}")
        print(f"           {f.message}")

    return len(errors), len(warnings)


# ---------------------------------------------------------------------------
# Lint command
# ---------------------------------------------------------------------------

def cmd_lint(args: argparse.Namespace) -> int:
    use_colour = sys.stdout.isatty() and not args.no_colour

    schema_path = Path(args.schema)
    if not schema_path.exists():
        print(f"[sf_lint] error: schema not found: {schema_path}", file=sys.stderr)
        return 2

    schema = _load_schema(schema_path)

    total_errors   = 0
    total_warnings = 0
    total_files    = 0

    for pattern in args.files:
        # Allow glob patterns passed as shell-escaped strings
        from pathlib import Path as _P
        import glob as _glob
        paths = _glob.glob(pattern, recursive=True) or [pattern]
        for fp in sorted(paths):
            path = Path(fp)
            if not path.exists():
                print(f"  [ERROR] {fp}: file not found", file=sys.stderr)
                total_errors += 1
                continue

            total_files += 1
            doc, parse_err = _load_yaml(path)
            if parse_err:
                print(f"✗ {_coloured(fp, _BOLD, use_colour)}")
                print(f"    [{_coloured(ERROR, _RED, use_colour)}] /")
                print(f"           {parse_err}")
                total_errors += 1
                continue

            if not isinstance(doc, dict):
                print(f"✗ {fp}")
                print(f"    [ERROR] /: top-level value must be a YAML mapping.")
                total_errors += 1
                continue

            findings: List[Finding] = []
            findings += _validate_schema(doc, schema)
            # Always run semantic checks — they guard themselves against malformed input
            # and surface precise diagnostics even when the JSON Schema produces a generic
            # oneOf/allOf failure message for the same underlying problem.
            findings += _semantic_checks(doc)

            if args.strict:
                for f in findings:
                    f.severity = ERROR  # promote warnings

            err_count, warn_count = _print_result(fp, findings, use_colour, show_ok=True)
            total_errors   += err_count
            total_warnings += warn_count

    # Summary line
    print()
    if total_errors:
        status = _coloured("FAIL", _RED, use_colour)
    elif total_warnings:
        status = _coloured("WARN", _YELLOW, use_colour)
    else:
        status = _coloured("OK", _GREEN, use_colour)

    print(
        f"{status}  "
        f"{total_files} file(s)  "
        f"{total_errors} error(s)  "
        f"{total_warnings} warning(s)"
    )

    if args.strict:
        return 0 if total_errors == 0 else 1
    return 0 if total_errors == 0 else 1


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _default_schema() -> str:
    here = Path(__file__).parent
    return str(here / "pipeline.schema.json")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sf_lint",
        description="StreamForge pipeline YAML linter.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    lint_p = sub.add_parser("lint", help="Validate pipeline YAML file(s).")
    lint_p.add_argument(
        "files", nargs="+", metavar="FILE",
        help="Pipeline YAML file(s) to validate. Glob patterns are accepted.",
    )
    lint_p.add_argument(
        "--schema", default=_default_schema(), metavar="PATH",
        help="Path to pipeline.schema.json (default: adjacent to this script).",
    )
    lint_p.add_argument(
        "--strict", action="store_true",
        help="Treat warnings as errors.",
    )
    lint_p.add_argument(
        "--no-colour", action="store_true",
        help="Disable ANSI colour output.",
    )

    args = parser.parse_args(argv)

    if args.command == "lint":
        return cmd_lint(args)

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
