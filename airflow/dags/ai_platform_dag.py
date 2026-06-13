"""
AI Ecosystem Data Platform — Main Orchestration DAG

Schedule: Daily at 06:00 UTC

Full pipeline flow:
    [ingest_github]     ──┐
    [ingest_huggingface]──┼──► [validate_raw] ──► [silver_github]     ──┐
    [ingest_arxiv]      ──┘                        [silver_huggingface]──┼──► [run_gold] ──► [load_postgres] ──► [pipeline_complete]
                                                   [silver_arxiv]      ──┘

Why this structure?
- Ingestion tasks run in PARALLEL — they hit different APIs with no dependency
- Validate runs AFTER all three ingestions — one bad source fails the whole pipeline early
- Silver tasks run in PARALLEL — each source is independent
- Gold and Postgres run SEQUENTIALLY — Gold needs all Silver done first

Key Airflow concepts used:
  - PythonOperator: runs a Python callable
  - TaskGroup: visually groups related tasks in the UI
  - task_id naming: use snake_case, dots are not allowed
  - depends_on_past=False: each daily run is independent
  - catchup=False: don't backfill missed runs on first deploy
"""

import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Add project root to path so our modules are importable inside Airflow
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ----------------------------------------------------------------
# Default arguments — applied to every task in this DAG
# ----------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,       # Each daily run is independent
    "retries": 2,                   # Retry failed tasks 2 times
    "retry_delay": timedelta(minutes=5),   # Wait 5 min between retries
    "retry_exponential_backoff": True,     # 5min, 10min on retry 1 and 2
    "email_on_failure": False,      # Set to True with SMTP config in production
    "email_on_retry": False,
    "execution_timeout": timedelta(hours=1),  # Kill task if it runs > 1 hour
}

# ----------------------------------------------------------------
# Callable functions — the actual work each task performs
# These are regular Python functions; Airflow just calls them
# ----------------------------------------------------------------

def ingest_github(**context):
    """
    Runs GitHub ingestion.

    context["ds"] is the Airflow execution date string (YYYY-MM-DD).
    In production you'd pass this to the ingester so each day's data
    is labeled with the correct date, even if the job runs late.

    XCom push: stores the output file path so downstream tasks
    can reference it if needed (e.g., for validation).
    """
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env", override=True)

    os.environ["DATA_LAKE_PATH"] = str(PROJECT_ROOT / "data_lake")
    from ingestion.github import GitHubIngester
    ingester = GitHubIngester(raw_base_path=str(PROJECT_ROOT / "data_lake" / "raw"))
    path = ingester.run()
    if not path:
        raise RuntimeError("GitHub ingestion returned 0 records")
    # XCom: push value so downstream tasks can pull it
    context["ti"].xcom_push(key="github_raw_path", value=path)
    return path


def ingest_huggingface(**context):
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env", override=True)

    from ingestion.huggingface import HuggingFaceIngester
    ingester = HuggingFaceIngester(raw_base_path=str(PROJECT_ROOT / "data_lake" / "raw"))
    path = ingester.run()
    if not path:
        raise RuntimeError("HuggingFace ingestion returned 0 records")
    context["ti"].xcom_push(key="hf_raw_path", value=path)
    return path


def ingest_arxiv(**context):
    from ingestion.arxiv_client import ArxivIngester
    ingester = ArxivIngester(raw_base_path=str(PROJECT_ROOT / "data_lake" / "raw"))
    path = ingester.run()
    if not path:
        raise RuntimeError("arXiv ingestion returned 0 records")
    context["ti"].xcom_push(key="arxiv_raw_path", value=path)
    return path


def validate_raw(**context):
    """
    Validates all three raw files exist and meet minimum quality thresholds.

    Why validate as a separate task (not inside each ingester)?
    Separation of concerns: ingestion = fetch + save, validation = quality check.
    If validation fails, Airflow marks this task failed and stops the pipeline.
    The ingestion tasks still show as "success" — useful for debugging
    (you can see the data arrived but failed quality checks).
    """
    from ingestion.validate import validate_all
    raw_base = str(PROJECT_ROOT / "data_lake" / "raw")
    batch_date = context["ds"]   # e.g. "2026-06-12"

    passed = validate_all(raw_base, batch_date)
    if not passed:
        raise ValueError(f"Raw data validation failed for batch_date={batch_date}")
    return f"Validation passed for {batch_date}"


def run_silver_github(**context):
    from spark_jobs.silver.github import run
    run()


def run_silver_huggingface(**context):
    from spark_jobs.silver.huggingface import run
    run()


def run_silver_arxiv(**context):
    from spark_jobs.silver.arxiv import run
    run()


def run_gold_all(**context):
    """
    Runs all four Gold jobs in sequence.

    Why not parallel? Gold jobs share the same SparkSession factory.
    Running them in parallel would create multiple SparkSessions
    competing for the same local CPU/memory resources.
    Sequential is safer and still fast for our data volumes.
    """
    from spark_jobs.gold.github_trends import run as github_gold
    from spark_jobs.gold.ai_models import run as ai_models_gold
    from spark_jobs.gold.research_trends import run as research_gold
    from spark_jobs.gold.executive_metrics import run as executive_gold

    github_gold()
    ai_models_gold()
    research_gold()
    executive_gold()


def load_postgres(**context):
    """
    Loads all Gold Parquet tables into PostgreSQL.

    Why run this as the last step?
    PostgreSQL is the serving layer — the dashboard reads from it.
    We only update PostgreSQL once ALL Gold tables are complete.
    A partial update would give the dashboard inconsistent data
    (e.g., new GitHub trends but old HuggingFace metrics).
    Atomic-style replacement: truncate + insert all tables in one run.
    """
    from postgres.loader import run_all
    run_all()


def notify_complete(**context):
    """
    Final task — logs pipeline completion summary.

    In production this would send a Slack message, email, or
    push a metric to Datadog/CloudWatch. For now it just logs.

    context["ds"] = execution date
    context["dag_run"].run_id = unique run identifier
    """
    from loguru import logger
    batch_date = context["ds"]
    run_id = context["dag_run"].run_id
    logger.info(f"""
    ╔══════════════════════════════════════════╗
    ║  AI Platform Pipeline — COMPLETE         ║
    ║  Batch Date : {batch_date}               ║
    ║  Run ID     : {run_id[:30]}              ║
    ╚══════════════════════════════════════════╝
    """)


# ----------------------------------------------------------------
# DAG Definition
# ----------------------------------------------------------------
with DAG(
    dag_id="ai_platform_daily_pipeline",
    description="End-to-end AI ecosystem data pipeline: ingest → silver → gold → postgres",
    schedule="0 6 * * *",          # Cron: every day at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,                  # Don't backfill historical runs on deploy
    default_args=default_args,
    max_active_runs=1,              # Prevent two daily runs from overlapping
    tags=["ai-platform", "production"],
    doc_md="""
    ## AI Ecosystem Data Platform Pipeline

    Orchestrates the full daily data pipeline:
    1. **Ingest** — fetch from GitHub, HuggingFace, arXiv APIs
    2. **Validate** — quality gate on raw data
    3. **Silver** — clean and standardize with PySpark
    4. **Gold** — compute business metrics with PySpark
    5. **Load** — write Gold tables to PostgreSQL
    """,
) as dag:

    # ── START marker ──────────────────────────────────────────────
    pipeline_start = EmptyOperator(task_id="pipeline_start")

    # ── INGESTION (parallel) ──────────────────────────────────────
    # TaskGroup visually groups these three tasks in the Airflow UI
    # They run in parallel because Airflow's LocalExecutor runs
    # tasks concurrently when they have no dependencies between them
    with TaskGroup("ingestion", tooltip="Fetch data from all three APIs") as ingestion_group:

        t_github = PythonOperator(
            task_id="ingest_github",
            python_callable=ingest_github,
        )

        t_huggingface = PythonOperator(
            task_id="ingest_huggingface",
            python_callable=ingest_huggingface,
        )

        t_arxiv = PythonOperator(
            task_id="ingest_arxiv",
            python_callable=ingest_arxiv,
        )

    # ── VALIDATE (runs after all ingestion tasks) ─────────────────
    t_validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw,
        doc_md="Validates raw files: existence, record count, required fields",
    )

    # ── SILVER (parallel, one per source) ────────────────────────
    with TaskGroup("silver_layer", tooltip="Clean and standardize raw data") as silver_group:

        t_silver_github = PythonOperator(
            task_id="silver_github",
            python_callable=run_silver_github,
        )

        t_silver_hf = PythonOperator(
            task_id="silver_huggingface",
            python_callable=run_silver_huggingface,
        )

        t_silver_arxiv = PythonOperator(
            task_id="silver_arxiv",
            python_callable=run_silver_arxiv,
        )

    # ── GOLD (sequential, all sources) ───────────────────────────
    t_gold = PythonOperator(
        task_id="run_gold_metrics",
        python_callable=run_gold_all,
        doc_md="Computes all business metrics from Silver layer",
    )

    # ── POSTGRES LOAD ─────────────────────────────────────────────
    t_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=load_postgres,
        doc_md="Truncates and reloads all Gold tables in PostgreSQL",
    )

    # ── COMPLETE ──────────────────────────────────────────────────
    t_notify = PythonOperator(
        task_id="notify_complete",
        python_callable=notify_complete,
        trigger_rule="all_success",  # Only notify if everything succeeded
    )

    pipeline_end = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule="none_failed_min_one_success",
    )

    # ── DEPENDENCY CHAIN ─────────────────────────────────────────
    # >> means "then run"
    # [list] means "all of these must succeed before continuing"
    pipeline_start >> ingestion_group >> t_validate >> silver_group >> t_gold >> t_postgres >> t_notify >> pipeline_end
