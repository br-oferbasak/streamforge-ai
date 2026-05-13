# ML Training with Prefetch Cache Warm-up

End-to-end example that wires the StreamForge AI data pipeline to a sample
ML training job, using `prefetch-engine/` to warm the local cache before
training begins.

## Data flow

```
MySQL (binlog)
  │
  ▼  Debezium CDC
Kafka  streamforge.streamforge.customers
  │
  ▼  Flink CdcUserEventCountJob (30-second tumbling windows)
Kafka  streamforge.features.user_event_counts
  │
  ▼  feature-sink (Kafka → MinIO)
MinIO  processed/streamforge/features/YYYY/MM/DD/*.json
  │
  ▼  feature_store.list_minio_feature_files()
FileStat manifest  (uri, recent_access_count, last_access_epoch)
  │
  ▼  prefetch_warmer.warm_cache()   ← prefetch-engine integration
Local cache  /tmp/streamforge-demo/prefetch-cache/ml-training/
  │
  ▼  feature_store.read_feature_records_from_cache()
FeatureRecord list  (user_id, window_start_ms, event_count, …)
  │
  ▼  trainer.train()
Classifier (RandomForest or pure-Python LogisticRegression)
  │
  ▼  MinIO upload
MinIO  processed/streamforge/models/<job_id>/model.pkl
```

## Quick start — no external services

```bash
# From repo root
pip install -r prefetch-engine/requirements.txt
pip install -r examples/ml-training-prefetch/requirements.txt

python examples/ml-training-prefetch/pipeline.py
```

The pipeline auto-detects that MinIO is not reachable, generates 2 000
synthetic feature records, warms a local cache, trains a classifier, and
prints a timing report.

## Against the running demo stack

Start the full `deploy/cdc-flink-minio-demo/` stack, insert rows into MySQL,
submit the Flink job, and then run the ML trainer against live MinIO data:

```bash
# 1 — bring up the demo stack
cd deploy/cdc-flink-minio-demo
docker compose up -d

# 2 — register the Debezium connector
curl -X POST -H 'Content-Type: application/json' \
  --data @connector-config.json http://localhost:8083/connectors

# 3 — submit the Flink job
docker compose exec jobmanager flink run -d \
  /opt/flink/usrlib/stream-processor-0.1.0-SNAPSHOT.jar \
  --bootstrap.servers kafka:9092 \
  --input.topic streamforge.streamforge.customers \
  --output.topic streamforge.features.user_event_counts \
  --window.seconds 30

# 4 — insert some rows (triggers CDC events)
docker compose exec mysql mysql -udebezium -pdebezium streamforge \
  -e "INSERT INTO customers (name, email) VALUES ('Alice', 'alice@example.com')"

# 5 — wait ~30 s for the Flink window to fire, then run training
MINIO_ENDPOINT=localhost:9000 \
MINIO_ACCESS_KEY=minioadmin   \
MINIO_SECRET_KEY=minioadmin   \
python examples/ml-training-prefetch/pipeline.py
```

## Via Docker Compose (ml-trainer service)

```bash
# Full pipeline (extends the demo stack)
docker compose \
  -f deploy/cdc-flink-minio-demo/docker-compose.yml \
  -f examples/ml-training-prefetch/docker-compose.yml \
  up

# Standalone (synthetic features, no upstream pipeline needed)
cd examples/ml-training-prefetch
docker compose --profile standalone up
```

## With Iceberg

When `pyiceberg` is installed and the Iceberg REST catalog is reachable,
the pipeline reads features directly from the Iceberg table instead of
raw MinIO JSON files:

```bash
pip install pyiceberg>=0.6.0

ICEBERG_CATALOG_URI=http://localhost:8181 \
MINIO_ENDPOINT=localhost:9000             \
python examples/ml-training-prefetch/pipeline.py
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO host:port |
| `MINIO_ACCESS_KEY` | `minioadmin` | |
| `MINIO_SECRET_KEY` | `minioadmin` | |
| `MINIO_BUCKET` | `processed` | Feature bucket |
| `MINIO_PREFIX` | `streamforge/features` | Feature object prefix |
| `ICEBERG_CATALOG_URI` | _(unset)_ | REST catalog URL; skipped if empty |
| `ICEBERG_TABLE` | `streamforge.features.user_event_counts` | |
| `PREFETCH_TOP_N` | `50` | Hot files staged before training |
| `PREFETCH_LATENCY_S` | `0.0` | Simulated per-file prefetch RTT (s) |
| `STREAMFORGE_DEMO_DIR` | `/tmp/streamforge-demo` | Base dir for cache + models |
| `STREAMFORGE_JOB_ID` | _(timestamp)_ | Job identifier |
| `N_SYNTHETIC_RECORDS` | `2000` | Synthetic records when offline |
| `METRICS_PUSHGATEWAY_URL` | _(unset)_ | Prometheus push gateway |

## Module structure

```
examples/ml-training-prefetch/
├── pipeline.py          # Main orchestrator — run this
├── feature_store.py     # MinIO / Iceberg feature discovery + reading
├── prefetch_warmer.py   # Bridge: prefetch-engine ↔ feature pipeline
├── trainer.py           # ML dataset prep + model training + MinIO upload
├── Dockerfile           # Image for docker compose
├── docker-compose.yml   # ml-trainer service + standalone MinIO profile
└── requirements.txt     # Optional deps (scikit-learn, pyiceberg)
```

## Dependencies

| Package | Required | Purpose |
|---------|----------|---------|
| `minio` | Yes (in `prefetch-engine/requirements.txt`) | MinIO client |
| `prometheus_client` | Yes (in `prefetch-engine/requirements.txt`) | Metrics |
| `scikit-learn` | Optional | RandomForest model (falls back to pure-Python LR) |
| `pyiceberg` | Optional | Iceberg table reads (falls back to MinIO JSON) |

The example is fully runnable with zero extra installations beyond what
`prefetch-engine/requirements.txt` already requires.
