# StreamForge Control Plane (v0.5)

Lightweight read-only dashboard for monitoring StreamForge AI pipeline services, tailing container logs, and browsing MinIO artifacts.

## Components

| Path | Description |
|------|-------------|
| `api/` | FastAPI service — wraps Docker + MinIO into REST endpoints |
| `ui/` | React + Vite SPA — status grid, log viewer, artifact browser |

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/status` | All service statuses (Docker container state) |
| `GET` | `/api/v1/logs/{service}?tail=N` | Last N lines from a container (`tail` 1–2000) |
| `GET` | `/api/v1/artifacts?bucket=&prefix=&limit=` | Recent MinIO objects, sorted newest-first |

Interactive docs at `http://localhost:8090/docs` when the API is running.

## Quick start

### With Docker Compose (recommended)

Requires the main pipeline stack (`deploy/cdc-flink-minio-demo/`) to be running first.

```bash
cd control-plane
docker compose up --build
```

- UI: http://localhost:3000
- API docs: http://localhost:8090/docs

The API container mounts `/var/run/docker.sock` to inspect running containers.

### Local development

**API**
```bash
cd control-plane/api
pip install -r requirements.txt
uvicorn main:app --reload --port 8090
```

**UI**
```bash
cd control-plane/ui
npm install
npm run dev    # proxies /api/* → localhost:8090
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO S3 endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET` | `processed` | Default bucket to browse |

## Notes

- **Read-only** — v0.5 supports observation only; pipeline start/stop is planned for v0.6.
- The API degrades gracefully when Docker or MinIO is unreachable (returns 503/502 with a message rather than crashing).
- Auto-refreshes every 15 seconds; manual refresh via the header button.
