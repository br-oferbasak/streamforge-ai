"""
Lineage event emitters: file (NDJSON), console, and composite fan-out.

Default events file is resolved from STREAMFORGE_LINEAGE_FILE env var,
falling back to lineage_events.ndjson in the current directory.
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path

from .models import LineageEvent

_DEFAULT_FILE = "lineage_events.ndjson"


class LineageEmitter(ABC):
    @abstractmethod
    def emit(self, event: LineageEvent) -> None: ...


class FileEmitter(LineageEmitter):
    """Appends one JSON line per event to an NDJSON file."""

    def __init__(self, path: str | Path = _DEFAULT_FILE) -> None:
        self._path = Path(path)

    def emit(self, event: LineageEvent) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as f:
            f.write(event.to_json() + "\n")


class ConsoleEmitter(LineageEmitter):
    """Prints a one-line summary to stdout for quick visibility."""

    def emit(self, event: LineageEvent) -> None:
        inputs = ",".join(d.name for d in event.inputs) or "—"
        outputs = ",".join(d.name for d in event.outputs) or "—"
        print(
            f"[LINEAGE] {event.eventType:<8} "
            f"job={event.job.namespace}/{event.job.name} "
            f"run={event.run.runId[:8]}… "
            f"in=[{inputs}] out=[{outputs}]"
        )


class CompositeEmitter(LineageEmitter):
    """Fan-out to multiple emitters."""

    def __init__(self, *emitters: LineageEmitter) -> None:
        self._emitters = emitters

    def emit(self, event: LineageEvent) -> None:
        for e in self._emitters:
            e.emit(event)


def default_emitter(events_file: str | None = None) -> LineageEmitter:
    """Return a CompositeEmitter writing to file + console."""
    path = events_file or os.environ.get("STREAMFORGE_LINEAGE_FILE", _DEFAULT_FILE)
    return CompositeEmitter(FileEmitter(path), ConsoleEmitter())
