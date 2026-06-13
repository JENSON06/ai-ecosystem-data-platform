"""
Silver Layer pipeline entry point.

Runs all three silver jobs in sequence.
Called by Airflow after ingestion + validation passes.
"""

import sys
from loguru import logger
from spark_jobs.silver.github import run as github_silver
from spark_jobs.silver.huggingface import run as huggingface_silver
from spark_jobs.silver.arxiv import run as arxiv_silver

JOBS = [
    ("github",      github_silver),
    ("huggingface", huggingface_silver),
    ("arxiv",       arxiv_silver),
]


def run_all():
    failed = []
    for name, job_fn in JOBS:
        try:
            logger.info(f"Starting Silver job: {name}")
            job_fn()
            logger.info(f"[OK] Silver complete: {name}")
        except Exception as e:
            logger.error(f"[FAILED] Silver failed: {name} — {e}")
            failed.append(name)

    logger.info("=" * 50)
    logger.info("SILVER PIPELINE SUMMARY")
    for name, _ in JOBS:
        icon = "[FAILED]" if name in failed else "[OK]"
        logger.info(f"  {icon} {name}")
    logger.info("=" * 50)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    run_all()
