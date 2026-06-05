"""
OpenLineage-compatible event models for StreamForge.

Schema reference: https://openlineage.io/spec/1-0-5/OpenLineage.json
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import List


@dataclass
class Dataset:
    namespace: str
    name: str
    facets: dict = field(default_factory=dict)


@dataclass
class Job:
    namespace: str
    name: str
    facets: dict = field(default_factory=dict)


@dataclass
class Run:
    runId: str
    facets: dict = field(default_factory=dict)


@dataclass
class LineageEvent:
    """A single OpenLineage event (START, COMPLETE, FAIL, or ABORT)."""

    eventType: str
    eventTime: str  # ISO-8601 UTC
    run: Run
    job: Job
    inputs: List[Dataset]
    outputs: List[Dataset]
    producer: str = "streamforge-ai/lineage"
    schemaURL: str = "https://openlineage.io/spec/1-0-5/OpenLineage.json"

    def to_dict(self) -> dict:
        return {
            "eventType": self.eventType,
            "eventTime": self.eventTime,
            "run": {"runId": self.run.runId, "facets": self.run.facets},
            "job": {
                "namespace": self.job.namespace,
                "name": self.job.name,
                "facets": self.job.facets,
            },
            "inputs": [
                {"namespace": d.namespace, "name": d.name, "facets": d.facets}
                for d in self.inputs
            ],
            "outputs": [
                {"namespace": d.namespace, "name": d.name, "facets": d.facets}
                for d in self.outputs
            ],
            "producer": self.producer,
            "schemaURL": self.schemaURL,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))
