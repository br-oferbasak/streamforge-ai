#!/usr/bin/env python3
"""
Lineage viewer: reads OpenLineage NDJSON events and renders a markdown report.

Usage
-----
  python lineage/viewer.py [events_file]          # prints to stdout
  python lineage/viewer.py events.ndjson > LINEAGE_REPORT.md
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_events(path: Path) -> List[dict]:
    events: List[dict] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return events


# ---------------------------------------------------------------------------
# Run summary table
# ---------------------------------------------------------------------------

def _build_run_summary(events: List[dict]) -> List[dict]:
    """Collapse START / COMPLETE / FAIL events into one row per run."""
    runs: Dict[str, dict] = {}

    for ev in events:
        rid = ev["run"]["runId"]
        etype = ev["eventType"]
        jid = f"{ev['job']['namespace']}/{ev['job']['name']}"

        if rid not in runs:
            runs[rid] = {
                "run_id": rid,
                "job": jid,
                "status": etype,
                "start_time": None,
                "end_time": None,
                "duration_s": None,
                "inputs": [],
                "outputs": [],
            }

        r = runs[rid]
        if etype == "START":
            r["start_time"] = ev["eventTime"]
            r["inputs"] = [f"{d['namespace']}:{d['name']}" for d in ev.get("inputs", [])]
        elif etype in ("COMPLETE", "FAIL"):
            r["status"] = etype
            r["end_time"] = ev["eventTime"]
            if etype == "COMPLETE":
                r["outputs"] = [f"{d['namespace']}:{d['name']}" for d in ev.get("outputs", [])]

        if r["start_time"] and r["end_time"]:
            try:
                t0 = datetime.fromisoformat(r["start_time"].replace("Z", "+00:00"))
                t1 = datetime.fromisoformat(r["end_time"].replace("Z", "+00:00"))
                r["duration_s"] = round((t1 - t0).total_seconds(), 3)
            except Exception:
                pass

    return sorted(runs.values(), key=lambda x: x.get("start_time") or "")


# ---------------------------------------------------------------------------
# Lineage DAG
# ---------------------------------------------------------------------------

def _build_dag(events: List[dict]) -> Tuple[List[str], Dict[str, dict]]:
    """
    Return (ordered_job_ids, job_details) for ASCII DAG rendering.

    job_details keys: namespace, name, inputs (list of str), outputs (list of str)
    """
    seen: Dict[str, int] = {}  # job_id → insertion order
    details: Dict[str, dict] = {}

    for ev in events:
        jid = f"{ev['job']['namespace']}/{ev['job']['name']}"
        if jid not in seen:
            seen[jid] = len(seen)
            details[jid] = {
                "namespace": ev["job"]["namespace"],
                "name": ev["job"]["name"],
                "inputs": [],
                "outputs": [],
            }
        d = details[jid]
        if ev["eventType"] == "START" and not d["inputs"]:
            d["inputs"] = [f"{x['namespace']} › {x['name']}" for x in ev.get("inputs", [])]
        if ev["eventType"] == "COMPLETE" and not d["outputs"]:
            d["outputs"] = [f"{x['namespace']} › {x['name']}" for x in ev.get("outputs", [])]

    ordered = sorted(seen.keys(), key=lambda k: seen[k])
    return ordered, details


def _render_dag(ordered: List[str], details: Dict[str, dict]) -> str:
    if not ordered:
        return "  *(no lineage events found)*\n"

    lines: List[str] = ["```"]
    shown_datasets: set = set()

    for jid in ordered:
        d = details[jid]
        inputs = d["inputs"]
        outputs = d["outputs"]

        # Show input datasets not already printed as outputs above
        for inp in inputs:
            if inp not in shown_datasets:
                lines.append(f"  [ {inp} ]")
                shown_datasets.add(inp)

        if inputs:
            lines.append("        │")
            lines.append("        ▼")

        label = f"JOB: {d['namespace']}/{d['name']}"
        width = max(len(label) + 4, 38)
        lines.append("  ┌" + "─" * width + "┐")
        lines.append(f"  │  {label:<{width - 2}}│")
        lines.append("  └" + "─" * width + "┘")

        for out in outputs:
            lines.append("        │")
            lines.append("        ▼")
            lines.append(f"  [ {out} ]")
            shown_datasets.add(out)

        lines.append("")

    lines.append("```")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------

_STATUS_ICON = {"COMPLETE": "✅", "FAIL": "❌", "START": "🔄", "ABORT": "⛔"}


def render_markdown(events_path: Path) -> str:
    events = _parse_events(events_path)
    if not events:
        return f"# StreamForge Lineage\n\n*No events found in `{events_path}`*\n"

    runs = _build_run_summary(events)
    ordered, details = _build_dag(events)

    md: List[str] = []
    md.append("# StreamForge Lineage Report")
    md.append(
        f"\n*Generated from `{events_path}` · "
        f"{len(events)} events · {len(runs)} runs · "
        f"{len(ordered)} jobs*\n"
    )

    md.append("## Lineage DAG\n")
    md.append(_render_dag(ordered, details))

    md.append("\n## Run Summary\n")
    md.append("| Job | Run ID | Status | Duration (s) | Inputs | Outputs |")
    md.append("|-----|--------|:------:|:------------:|--------|---------|")

    for r in runs:
        rid_short = r["run_id"][:8] + "…"
        icon = _STATUS_ICON.get(r["status"], r["status"])
        inputs_str = ", ".join(r["inputs"][:2]) + ("…" if len(r["inputs"]) > 2 else "")
        outputs_str = ", ".join(r["outputs"][:2]) + ("…" if len(r["outputs"]) > 2 else "")
        dur = str(r["duration_s"]) if r["duration_s"] is not None else "—"
        md.append(
            f"| `{r['job']}` | `{rid_short}` | {icon} | {dur} | {inputs_str or '—'} | {outputs_str or '—'} |"
        )

    md.append("\n## Dataset Lineage\n")
    md.append("Datasets that cross job boundaries:\n")
    # Find datasets that appear as outputs of one job and inputs of another
    all_outputs: Dict[str, str] = {}  # dataset_label → producer_job
    for jid in ordered:
        for out in details[jid]["outputs"]:
            all_outputs[out] = jid

    found_cross = False
    for jid in ordered:
        for inp in details[jid]["inputs"]:
            if inp in all_outputs:
                producer = all_outputs[inp]
                md.append(f"- `{inp}` : `{producer}` → `{jid}`")
                found_cross = True
    if not found_cross:
        md.append("*(no cross-job datasets detected — run more pipeline stages)*")

    return "\n".join(md) + "\n"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    events_file = argv[0] if argv else "lineage_events.ndjson"
    path = Path(events_file)

    if not path.exists():
        print(f"[viewer] Events file not found: {path}", file=sys.stderr)
        print("[viewer] Run `python lineage/demo.py` to generate sample events first.", file=sys.stderr)
        sys.exit(1)

    print(render_markdown(path))


if __name__ == "__main__":
    main()
