"""
Ingestion pipeline entry point.

This script is called by:
  1. Airflow (via BashOperator or PythonOperator) in production
  2. Directly via `python -m ingestion.run_ingestion` for local testing

It runs all three ingesters and prints a summary report.
The exit code matters: exit(1) on failure tells Airflow the task failed
and triggers retry/alert logic defined in the DAG.
"""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Explicitly point to .env file using absolute path relative to this file.
# This works regardless of which directory you run python -m from.
_ENV_FILE = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=_ENV_FILE, override=True)

from loguru import logger
from ingestion.github import GitHubIngester
from ingestion.huggingface import HuggingFaceIngester
from ingestion.arxiv_client import ArxivIngester

# Allow overriding the data lake path via environment variable
# Inside Docker: /opt/data_lake
# Local dev: ./data_lake
DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "./data_lake")


def run_all() -> dict:
    """
    Runs all ingesters sequentially.

    Why sequential and not parallel?
    Each ingester hits a different API with its own rate limits.
    Running them in parallel risks getting rate-limited on all three
    simultaneously. Sequential is safer for a daily batch pipeline
    where total runtime doesn't matter much.

    Returns a results dict so Airflow can use XCom to pass
    file paths to downstream Spark tasks.
    """
    ingesters = [
        GitHubIngester(raw_base_path=f"{DATA_LAKE_PATH}/raw"),
        HuggingFaceIngester(raw_base_path=f"{DATA_LAKE_PATH}/raw"),
        ArxivIngester(raw_base_path=f"{DATA_LAKE_PATH}/raw"),
    ]

    results = {}
    failed = []

    for ingester in ingesters:
        try:
            path = ingester.run()
            # Empty string means fetch() returned 0 records — treat as failure
            if not path:
                raise RuntimeError("No records fetched — check API credentials and rate limits")
            results[ingester.source_name] = {"status": "success", "path": path}
        except Exception as e:
            logger.error(f"{ingester.source_name} ingestion failed: {e}")
            results[ingester.source_name] = {"status": "failed", "error": str(e)}
            failed.append(ingester.source_name)

    # Print summary
    logger.info("=" * 50)
    logger.info("INGESTION SUMMARY")
    for source, result in results.items():
        status_icon = "[OK]" if result["status"] == "success" else "[FAILED]"
        logger.info(f"  {status_icon} {source}: {result['status']}")
    logger.info("=" * 50)

    if failed:
        logger.error(f"Failed sources: {failed}")
        sys.exit(1)

    return results


if __name__ == "__main__":
    run_all()
