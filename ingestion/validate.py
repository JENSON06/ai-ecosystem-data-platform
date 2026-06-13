"""
Raw layer validation — runs after ingestion, before Spark jobs.

This is the data quality gate between the Landing Zone and the Spark pipeline.

Checks performed:
  1. File exists
  2. File is valid JSON
  3. Record count is above a minimum threshold
  4. Required top-level keys are present
  5. No completely empty records

Why validate before Spark?
Spark jobs are expensive (JVM startup, cluster resources).
Catching bad input here costs milliseconds.
Catching it inside a Spark job costs minutes and produces
cryptic Java stack traces.
"""

import json
from pathlib import Path
from datetime import datetime
from loguru import logger

MINIMUM_RECORD_COUNTS = {
    "github": 50,
    "huggingface": 50,
    "arxiv": 50,
}

REQUIRED_KEYS = {
    "github": ["repo_id", "name", "stars"],
    "huggingface": ["model_id", "author", "downloads"],
    "arxiv": ["paper_id", "title", "published"],
}


def validate_raw_file(source: str, raw_base_path: str, batch_date: str = None) -> bool:
    """
    Validates a raw JSON file for a given source and date.
    Returns True if valid, raises ValueError if not.
    """
    if not batch_date:
        batch_date = datetime.utcnow().strftime("%Y-%m-%d")

    file_path = Path(raw_base_path) / source / f"{source}_{batch_date}.json"

    # Check 1: File existence
    if not file_path.exists():
        raise FileNotFoundError(f"Raw file not found: {file_path}")

    # Check 2: Valid JSON
    try:
        with open(file_path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {file_path}: {e}")

    records = data.get("records", [])

    # Check 3: Minimum record count
    min_count = MINIMUM_RECORD_COUNTS.get(source, 10)
    if len(records) < min_count:
        raise ValueError(
            f"{source} has only {len(records)} records, expected >= {min_count}"
        )

    # Check 4: Required fields present in first record
    required = REQUIRED_KEYS.get(source, [])
    if records and required:
        missing = [k for k in required if k not in records[0]]
        if missing:
            raise ValueError(f"{source} missing required fields: {missing}")

    # Check 5: No fully empty records
    empty_count = sum(1 for r in records if not any(r.values()))
    if empty_count > 0:
        logger.warning(f"{source}: {empty_count} completely empty records found")

    logger.info(f"[OK] {source} validation passed | {len(records)} records | {file_path.name}")
    return True


def validate_all(raw_base_path: str, batch_date: str = None) -> bool:
    """Validates all three sources. Returns True only if all pass."""
    all_passed = True
    for source in ["github", "huggingface", "arxiv"]:
        try:
            validate_raw_file(source, raw_base_path, batch_date)
        except Exception as e:
            logger.error(f"[FAILED] {source} validation failed: {e}")
            all_passed = False
    return all_passed
