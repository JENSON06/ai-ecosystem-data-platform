# AI Ecosystem Data Platform — Runbook

Operational guide for running, monitoring, and recovering the platform.

---

## First-Time Setup

```bash
# 1. Clone the repo
git clone <repo-url>
cd ai-ecosystem-data-platform

# 2. Create your .env from the template
cp .env.example .env
# Edit .env — set GITHUB_TOKEN at minimum

# 3. Build and start all services
docker-compose up --build -d

# 4. Wait ~60 seconds for Airflow to initialise, then verify:
docker-compose ps
```

All services should show `Up`. Open:

| Service | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Airflow | http://localhost:8081 (admin / admin) |
| Dashboard | http://localhost:8501 |

---

## Running the Pipeline

### Via Airflow (normal operations)

1. Open Airflow UI → http://localhost:8081
2. Enable the `ai_platform_daily_pipeline` DAG (toggle on)
3. Trigger manually with the ▶ button, or let it run at 06:00 UTC

### Manually (local dev / debugging)

```bash
# Run each stage individually
python3 ingestion/run_ingestion.py
python3 spark_jobs/run_silver.py
python3 spark_jobs/run_gold.py
python3 postgres/setup_db.py      # first time only
python3 postgres/loader.py
streamlit run dashboards/app.py
```

### Reprocess without re-ingesting

Use the `ai_platform_reprocess` DAG in Airflow (manual trigger only).
This re-runs Silver → Gold → PostgreSQL from existing raw files — useful after fixing a bug in the transform logic.

---

## Health Checks

The `ai_platform_health_check` DAG runs every hour and checks:

- PostgreSQL is reachable
- `fact_executive_kpis` has data no older than 2 days
- Key tables meet minimum row counts

Check results in Airflow UI → DAGs → `ai_platform_health_check`.

---

## Common Failures and Fixes

### Pipeline fails at ingestion with 401 Unauthorized

```
RuntimeError: GitHub API returned 401 Unauthorized
```

**Cause:** `GITHUB_TOKEN` in `.env` is missing or expired.
**Fix:** Generate a new token at https://github.com/settings/tokens and update `.env`.

---

### Spark job fails with `OutOfMemoryError`

**Cause:** `spark.driver.memory` (2g default) is too low for your data volume.
**Fix:** In `spark_jobs/session.py`, increase driver memory:

```python
.config("spark.driver.memory", "4g")
```

---

### PostgreSQL loader fails with `COPY` error

**Cause:** A column type mismatch between the Parquet schema and the PostgreSQL table.
**Fix:**
1. Check the error message for which table and column failed
2. Run `python3 postgres/setup_db.py` to recreate the schema
3. Re-run `python3 postgres/loader.py`

---

### Dashboard shows "Cannot connect to PostgreSQL"

**Cause:** PostgreSQL container is not running, or `POSTGRES_HOST` / `POSTGRES_PORT` in `.env` is wrong.
**Fix:**
```bash
docker-compose ps             # check postgres is Up
docker-compose restart postgres
```

For local dev (outside Docker), PostgreSQL is on port **5433** (mapped in docker-compose.yml).

---

### Airflow shows "Import Error" on a DAG

**Cause:** A Python dependency is missing in the Airflow container.
**Fix:**
```bash
docker-compose build airflow-webserver
docker-compose up -d airflow-webserver airflow-scheduler
```

---

## Teardown

```bash
docker-compose down        # Stop containers, keep PostgreSQL data
docker-compose down -v     # Stop and wipe all data (full reset)
```

---

## Running Tests

```bash
# All non-Spark tests (fast, no JVM)
python3 -m pytest tests/test_ingestion_base.py tests/test_validation.py tests/test_loader.py -v

# All tests including Spark (requires PySpark installed locally)
python3 -m pytest tests/ -v

# Skip Spark tests
python3 -m pytest tests/ -m "not spark" -v
```

---

## Secrets Management

- Never commit `.env` — it is in `.gitignore`
- Use `.env.example` as the template for new contributors
- In production, replace `.env` with a secrets manager (AWS Secrets Manager, HashiCorp Vault)
- Rotate your GitHub token at https://github.com/settings/tokens if it may have been exposed
