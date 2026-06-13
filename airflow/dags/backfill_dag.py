"""
Backfill / Manual Reprocessing DAG

Used when you need to reprocess existing raw data through
Silver → Gold → PostgreSQL without re-fetching from the APIs.

Real-world use case: you find a bug in your Silver cleaning logic.
You fix the bug, then trigger this DAG to reprocess all existing
raw files without wasting API quota on re-ingestion.

This is triggered manually (schedule=None) — never runs automatically.
"""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay_seconds": 60,
}


def reprocess_silver(**context):
    from spark_jobs.run_silver import run_all
    run_all()


def reprocess_gold(**context):
    from spark_jobs.run_gold import run_all
    run_all()


def reload_postgres(**context):
    from postgres.loader import run_all
    run_all()


with DAG(
    dag_id="ai_platform_reprocess",
    description="Reprocess Silver → Gold → PostgreSQL from existing raw data",
    schedule=None,              # Manual trigger only — never runs on a schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ai-platform", "manual", "reprocessing"],
) as dag:

    t_silver = PythonOperator(
        task_id="reprocess_silver",
        python_callable=reprocess_silver,
    )

    t_gold = PythonOperator(
        task_id="reprocess_gold",
        python_callable=reprocess_gold,
    )

    t_postgres = PythonOperator(
        task_id="reload_postgres",
        python_callable=reload_postgres,
    )

    t_silver >> t_gold >> t_postgres
