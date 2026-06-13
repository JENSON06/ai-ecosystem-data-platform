"""
Health Check DAG — runs every hour

Checks that:
1. PostgreSQL is reachable
2. The latest batch_date in fact_executive_kpis is not stale (> 2 days old)
3. Minimum row counts in key tables

Why a separate health check DAG?
The main pipeline runs once daily. If something breaks PostgreSQL
between runs, you won't know until the next pipeline run fails.
A frequent lightweight check gives you early warning.

This is a common enterprise pattern: separate the data pipeline
from the data quality monitoring.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

default_args = {
    "owner": "data-engineering",
    "retries": 0,               # Health checks should not retry — fail fast
    "email_on_failure": False,
}

# Minimum rows we expect in each key table
MIN_ROW_COUNTS = {
    "fact_github_trends":   50,
    "fact_model_metrics":   10,
    "fact_research_trends":  4,
    "fact_executive_kpis":   1,
}


def check_postgres_connection(**context):
    """Verifies PostgreSQL is reachable and responding."""
    import psycopg2
    from dotenv import load_dotenv
    import os
    load_dotenv(PROJECT_ROOT / ".env", override=True)

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "ai_platform"),
        user=os.getenv("POSTGRES_USER", "platform"),
        password=os.getenv("POSTGRES_PASSWORD", "platform123"),
    )
    cur = conn.cursor()
    cur.execute("SELECT 1")
    conn.close()
    return "PostgreSQL connection OK"


def check_data_freshness(**context):
    """
    Checks that the latest pipeline run is not stale.

    Stale data = fact_executive_kpis has no row for today or yesterday.
    If the pipeline failed silently, this catches it.
    """
    import psycopg2
    from dotenv import load_dotenv
    import os
    from loguru import logger

    load_dotenv(PROJECT_ROOT / ".env", override=True)

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "ai_platform"),
        user=os.getenv("POSTGRES_USER", "platform"),
        password=os.getenv("POSTGRES_PASSWORD", "platform123"),
    )
    cur = conn.cursor()
    cur.execute("SELECT MAX(batch_date) FROM ai_platform.fact_executive_kpis")
    latest = cur.fetchone()[0]
    conn.close()

    if latest is None:
        raise ValueError("No data in fact_executive_kpis — pipeline has never run")

    days_old = (datetime.utcnow().date() - latest).days
    logger.info(f"Latest batch_date: {latest} ({days_old} days ago)")

    if days_old > 2:
        raise ValueError(f"Data is stale: latest batch_date is {latest} ({days_old} days ago)")

    return f"Data freshness OK: latest={latest}"


def check_row_counts(**context):
    """Checks minimum row counts in key tables."""
    import psycopg2
    from dotenv import load_dotenv
    import os
    from loguru import logger

    load_dotenv(PROJECT_ROOT / ".env", override=True)

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "ai_platform"),
        user=os.getenv("POSTGRES_USER", "platform"),
        password=os.getenv("POSTGRES_PASSWORD", "platform123"),
    )
    cur = conn.cursor()
    failed = []

    for table, min_rows in MIN_ROW_COUNTS.items():
        cur.execute(f"SELECT COUNT(*) FROM ai_platform.{table}")
        count = cur.fetchone()[0]
        logger.info(f"  {table}: {count} rows (min: {min_rows})")
        if count < min_rows:
            failed.append(f"{table} has {count} rows, expected >= {min_rows}")

    conn.close()

    if failed:
        raise ValueError(f"Row count checks failed: {failed}")

    return "All row count checks passed"


with DAG(
    dag_id="ai_platform_health_check",
    description="Hourly health check: PostgreSQL connectivity, data freshness, row counts",
    schedule="0 * * * *",      # Every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ai-platform", "monitoring"],
) as dag:

    t_conn = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres_connection,
    )

    t_fresh = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    t_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )

    # All three run in parallel — independent checks
    [t_conn, t_fresh, t_counts]
