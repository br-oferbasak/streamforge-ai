#!/usr/bin/env python3
"""
StreamForge Pipeline API — minimal HTTP server.

Endpoints
---------
  GET  /health                     liveness check
  GET  /pipelines                  list all known pipelines + runtime status
  GET  /pipelines/<name>           detail for a single pipeline
  POST /pipelines/<name>/start     start a pipeline (background thread)
  POST /pipelines/<name>/stop      request graceful stop

Configuration (env vars)
------------------------
  PIPELINE_API_HOST   bind host  (default 0.0.0.0)
  PIPELINE_API_PORT   bind port  (default 8080)
  PIPELINES_DIR       directory of *.yaml configs
                      (default <repo-root>/pipeline-api/pipelines)

No multi-tenant guarantees — all requests share one process.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from flask import Flask, jsonify, request

from manager import PipelineManager

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App + pipeline manager
# ---------------------------------------------------------------------------
app = Flask(__name__)

_DEFAULT_PIPELINES_DIR = Path(__file__).parent / "pipelines"
_pipelines_dir = Path(os.environ.get("PIPELINES_DIR", str(_DEFAULT_PIPELINES_DIR)))
manager = PipelineManager(_pipelines_dir)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return jsonify({"status": "ok", "pipelines_dir": str(_pipelines_dir)})


@app.get("/pipelines")
def list_pipelines():
    return jsonify({"pipelines": manager.list_pipelines()})


@app.get("/pipelines/<name>")
def get_pipeline(name: str):
    info = manager.get_pipeline(name)
    if info is None:
        return jsonify({"error": f"pipeline '{name}' not found"}), 404
    return jsonify(info)


@app.post("/pipelines/<name>/start")
def start_pipeline(name: str):
    try:
        result = manager.start_pipeline(name)
    except KeyError:
        return jsonify({"error": f"pipeline '{name}' not found"}), 404
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 409  # conflict — already running
    return jsonify(result), 202


@app.post("/pipelines/<name>/stop")
def stop_pipeline(name: str):
    try:
        result = manager.stop_pipeline(name)
    except KeyError:
        return jsonify({"error": f"pipeline '{name}' not found"}), 404
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 409
    return jsonify(result)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    host = os.environ.get("PIPELINE_API_HOST", "0.0.0.0")
    port = int(os.environ.get("PIPELINE_API_PORT", "8080"))
    logger.info("Starting Pipeline API on %s:%s", host, port)
    app.run(host=host, port=port)
