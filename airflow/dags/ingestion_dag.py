"""
Ingestion DAG — stub for Phase 2.
Full implementation in Phase 7 (Airflow Orchestration).

This DAG runs the ingestion pipeline daily and validates the output.
It's defined now so Airflow picks it up and shows it in the UI,
giving us a place to verify the container is working.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="ai_platform_ingestion",
    description="Ingest data from GitHub, HuggingFace, and arXiv into raw layer",
    schedule_interval="0 6 * * *",   # Daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "phase-2"],
) as dag:

    ingest = BashOperator(
        task_id="run_ingestion",
        bash_command="cd /opt && python -m ingestion.run_ingestion",
    )

    validate = BashOperator(
        task_id="validate_raw_files",
        bash_command=(
            "cd /opt && python -c \""
            "from ingestion.validate import validate_all; "
            "import sys; sys.exit(0 if validate_all('./data_lake/raw') else 1)"
            "\""
        ),
    )

    ingest >> validate
