# Ingestion Gateway

FastAPI microservice that receives CSV uploads, stores them in the shared Airflow input volume, triggers the corresponding Airflow DAG, and provides polling for run status and outputs.

## Prerequisites
- Python 3.11+
- Docker + Docker Compose
- Access to the shared Airflow input volume (mounted at `/shared/input`)

## Configuration
Set environment variables (or copy `.env.example` to `.env` and adjust).  
**Importante:** `AIRFLOW_API_URL` debe incluir `/api/v1` (p. ej. `http://host.docker.internal:8080/api/v1`). Si falta ese sufijo, las llamadas devolverán 404 HTML desde el webserver de Airflow.
```
cp .env.example .env
# then edit values as needed
```

## Run locally
```bash
pip install -r requirements.txt
uvicorn ingestion_gateway.app:app --reload --port 8000
```
Shared input folder must exist; the app will create it if missing.

## Docker
```bash
docker compose -f docker-compose.ingestion-gateway.yml up --build
```
This mounts `../data_input` as `/shared/input` to match Airflow.

## API
### `POST /ingest/part1`
### `POST /ingest/part2`
Multipart form fields:
- `week_year` (int)
- `week_num` (int)
- `notify_email` (email)
- `files` (one or more CSV uploads)

Behavior:
1. Validate CSVs (extension + readable sample).
2. Create `/shared/input/runs/<dag_run_id>/`.
3. Persist uploads inside that folder.
4. Trigger Airflow DAG (`part1_ingestion` or `part2_qbo_export`) with `input_subdir=runs/<dag_run_id>`.
5. Respond `{ run_id, dag_run_id, status: "submitted" }`.

### `GET /poll/{dag_run_id}?dag=part1|part2`
Polls Airflow until success/failed (bounded attempts) and returns run state plus discovered output files at `/shared/input/runs/<dag_run_id>/outputs/*.csv`.

### `GET /health`
Simple liveness check.

## Example requests
```bash
# Submit part1
curl -X POST http://localhost:8000/ingest/part1 \
  -F week_year=2023 -F week_num=40 -F notify_email=user@example.com \
  -F "files=@sample.csv"

# Poll
curl "http://localhost:8000/poll/<dag_run_id>?dag=part1"
```

## Testing
```bash
pytest
```

## Deployment Notes
- Mount the same shared input volume used by Airflow at `/shared/input`.
- The gateway never deletes outputs; Airflow owns cleanup.
- The service only communicates with Airflow via REST, keeping concerns separated.
