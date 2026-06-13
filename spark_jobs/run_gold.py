"""
Gold Layer pipeline entry point.
Runs all four gold jobs in order — executive metrics last
since it reads from silver (not from other gold tables).
"""

import sys
from loguru import logger
from spark_jobs.gold.github_trends import run as github_gold
from spark_jobs.gold.ai_models import run as ai_models_gold
from spark_jobs.gold.research_trends import run as research_gold
from spark_jobs.gold.executive_metrics import run as executive_gold

JOBS = [
    ("github_trends",     github_gold),
    ("ai_models",         ai_models_gold),
    ("research_trends",   research_gold),
    ("executive_metrics", executive_gold),
]


def run_all():
    failed = []
    for name, job_fn in JOBS:
        try:
            logger.info(f"Starting Gold job: {name}")
            job_fn()
            logger.info(f"[OK] Gold complete: {name}")
        except Exception as e:
            logger.error(f"[FAILED] Gold failed: {name} — {e}")
            failed.append(name)

    logger.info("=" * 50)
    logger.info("GOLD PIPELINE SUMMARY")
    for name, _ in JOBS:
        icon = "[FAILED]" if name in failed else "[OK]"
        logger.info(f"  {icon} {name}")
    logger.info("=" * 50)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    run_all()
