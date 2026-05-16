# StreamForge AI — ML Training + Prefetch Engine
# Build context must be the repo root so both prefetch-engine/ and
# examples/ml-training-prefetch/ are available in one layer.
#
#   docker build -f examples/ml-training-prefetch/Dockerfile -t streamforge-ml-trainer .

FROM python:3.11-slim

WORKDIR /app

# ── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ───────────────────────────────────────────────────────
# Copy requirements first to benefit from Docker layer caching
COPY prefetch-engine/requirements.txt /tmp/prefetch-requirements.txt
COPY examples/ml-training-prefetch/requirements.txt /tmp/example-requirements.txt

RUN pip install --no-cache-dir \
    -r /tmp/prefetch-requirements.txt \
    -r /tmp/example-requirements.txt

# ── Application code ──────────────────────────────────────────────────────────
# prefetch-engine source (pipeline imports from it directly)
COPY prefetch-engine/ /app/prefetch-engine/

# The ML training example
COPY examples/ml-training-prefetch/ /app/examples/ml-training-prefetch/

# Make the example importable without an install step
ENV PYTHONPATH=/app/prefetch-engine:/app/examples/ml-training-prefetch

# Default entrypoint — can be overridden by docker compose command
ENTRYPOINT ["python", "examples/ml-training-prefetch/pipeline.py"]
