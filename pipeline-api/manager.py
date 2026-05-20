"""
PipelineManager — load pipeline configs from a directory of YAML files,
then start / stop / inspect them as background threads.

No multi-tenant isolation: all pipelines share the same process and
filesystem. Concurrent runs of the same pipeline name are rejected.
"""
from __future__ import annotations

import logging
import sys
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import yaml

# Resolve the agent-workflow package relative to this file's location.
_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT / "agent-workflow" / "src"))

from workflow_engine import WorkflowEngine  # noqa: E402

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Runtime state for a single pipeline execution
# ---------------------------------------------------------------------------

@dataclass
class PipelineRun:
    run_id: str
    pipeline_name: str
    status: str          # running | completed | failed | stopped
    started_at: str
    ended_at: Optional[str] = None
    error: Optional[str] = None
    _thread: Optional[threading.Thread] = field(default=None, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, repr=False)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

class PipelineManager:
    """
    Scans *pipelines_dir* for ``*.yaml`` files on construction and keeps
    them as pipeline definitions.  At most one run per pipeline name is
    tracked at any time.
    """

    def __init__(self, pipelines_dir: str | Path) -> None:
        self._dir = Path(pipelines_dir)
        self._configs: Dict[str, dict] = {}   # name → raw config dict
        self._runs: Dict[str, PipelineRun] = {}  # name → current/last run
        self._lock = threading.Lock()
        self._reload_configs()

    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------

    def _reload_configs(self) -> None:
        for yaml_file in sorted(self._dir.glob("*.yaml")):
            try:
                config = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
            except yaml.YAMLError as exc:
                logger.warning("Skipping %s — YAML parse error: %s", yaml_file, exc)
                continue
            name = config.get("name") or yaml_file.stem
            config.setdefault("name", name)
            config["_source"] = str(yaml_file.relative_to(_REPO_ROOT))
            self._configs[name] = config
            logger.info("Loaded pipeline config: %s from %s", name, yaml_file.name)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_pipelines(self) -> List[dict]:
        with self._lock:
            result = []
            for name, cfg in self._configs.items():
                run = self._runs.get(name)
                result.append({
                    "name": name,
                    "description": cfg.get("description", ""),
                    "config_file": cfg["_source"],
                    "status": self._effective_status(run),
                    "run_id": run.run_id if run else None,
                    "started_at": run.started_at if run else None,
                })
            return result

    def get_pipeline(self, name: str) -> Optional[dict]:
        with self._lock:
            cfg = self._configs.get(name)
            if cfg is None:
                return None
            run = self._runs.get(name)
            info: dict = {
                "name": name,
                "description": cfg.get("description", ""),
                "config_file": cfg["_source"],
                "agents": [a["name"] for a in cfg.get("agents", [])],
                "steps": len(cfg.get("steps", [])),
                "status": self._effective_status(run),
            }
            if run:
                info.update(run.to_dict())
                if run.status == "running" and run._thread:
                    info["agent_statuses"] = {}  # populated by engine post-run
            return info

    def start_pipeline(self, name: str) -> dict:
        with self._lock:
            if name not in self._configs:
                raise KeyError(f"Pipeline '{name}' not found")

            run = self._runs.get(name)
            if run and run.status == "running":
                raise RuntimeError(f"Pipeline '{name}' is already running (run_id={run.run_id})")

            run_id = uuid.uuid4().hex[:8]
            stop_event = threading.Event()
            new_run = PipelineRun(
                run_id=run_id,
                pipeline_name=name,
                status="running",
                started_at=_now(),
                _stop_event=stop_event,
            )
            self._runs[name] = new_run

            config = dict(self._configs[name])  # shallow copy; engine doesn't mutate top-level

        t = threading.Thread(
            target=self._run_pipeline,
            args=(name, config, new_run),
            name=f"pipeline-{name}-{run_id}",
            daemon=True,
        )
        new_run._thread = t
        t.start()

        return {"run_id": run_id, "status": "running", "started_at": new_run.started_at}

    def stop_pipeline(self, name: str, timeout_s: float = 5.0) -> dict:
        with self._lock:
            run = self._runs.get(name)
            if run is None or run.status != "running":
                raise RuntimeError(f"Pipeline '{name}' is not currently running")
            run._stop_event.set()

        # Wait outside the lock so the worker thread can update status.
        if run._thread:
            run._thread.join(timeout=timeout_s)

        with self._lock:
            if run.status == "running":
                # Thread didn't finish within timeout — mark it anyway.
                run.status = "stopped"
                run.ended_at = _now()
            stopped_at = run.ended_at or _now()

        return {"status": run.status, "stopped_at": stopped_at}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_pipeline(self, name: str, config: dict, run: PipelineRun) -> None:
        try:
            engine = WorkflowEngine.from_dict(config, stop_event=run._stop_event)
            success = engine.execute_workflow()
            with self._lock:
                if run._stop_event.is_set():
                    run.status = "stopped"
                else:
                    run.status = "completed" if success else "failed"
                run.ended_at = _now()
        except Exception as exc:
            logger.exception("Pipeline '%s' raised an exception", name)
            with self._lock:
                run.status = "failed"
                run.error = str(exc)
                run.ended_at = _now()

    @staticmethod
    def _effective_status(run: Optional[PipelineRun]) -> str:
        if run is None:
            return "idle"
        # Re-check thread liveness for "running" entries whose thread finished
        # without updating status (shouldn't happen, but guard anyway).
        if run.status == "running" and run._thread and not run._thread.is_alive():
            return "completed"
        return run.status
