"""
Base ingester class.

Every API client inherits from this so retry logic, file saving,
and logging are written once and reused everywhere.

Design pattern: Template Method — the base class defines the skeleton
(fetch → validate → save), subclasses fill in the fetch() detail.
"""

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class BaseIngester(ABC):
    """
    Abstract base class for all API ingesters.

    Why abstract? We enforce that every subclass MUST implement fetch().
    Python raises TypeError at instantiation if fetch() is missing —
    you catch the mistake at class definition, not at 2am during a pipeline run.
    """

    def __init__(self, source_name: str, raw_base_path: str):
        self.source_name = source_name
        self.raw_path = Path(raw_base_path) / source_name
        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.batch_date = datetime.utcnow().strftime("%Y-%m-%d")

        # Configure structured logging — every log line includes source name
        logger.add(
            f"logs/{source_name}_ingestion.log",
            rotation="1 day",
            retention="7 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}",
        )

    @abstractmethod
    def fetch(self) -> list[dict]:
        """
        Subclasses implement this to return a list of raw records.
        Each record is a plain Python dict (JSON-serializable).
        """
        ...

    def save(self, records: list[dict]) -> str:
        """
        Saves records as a single JSON file in the raw layer.

        Why JSON and not CSV?
        APIs return nested data (a GitHub repo has nested owner, topics lists, etc.)
        JSON preserves that structure. CSV would flatten or lose it.
        Spark reads nested JSON natively.

        Why one file per day?
        Appending to a single growing file creates race conditions in parallel pipelines.
        One-file-per-day is safe, easy to reprocess, and maps cleanly to Spark partitions.
        """
        output_path = self.raw_path / f"{self.source_name}_{self.batch_date}.json"

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "source": self.source_name,
                    "batch_date": self.batch_date,
                    "record_count": len(records),
                    "ingested_at": datetime.utcnow().isoformat(),
                    "records": records,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

        logger.info(f"Saved {len(records)} records → {output_path}")
        return str(output_path)

    def run(self) -> str:
        """
        Orchestrates the full extract cycle: fetch → save.
        Returns the path to the saved file.
        """
        logger.info(f"Starting ingestion: {self.source_name} | date={self.batch_date}")
        records = self.fetch()
        if not records:
            logger.warning(f"No records fetched from {self.source_name}")
            return ""
        path = self.save(records)
        logger.info(f"Ingestion complete: {self.source_name} | {len(records)} records")
        return path

    # ------------------------------------------------------------------
    # Shared HTTP helper with retry logic
    # ------------------------------------------------------------------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def _get(self, url: str, params: dict = None, headers: dict = None) -> dict | list:
        """
        Makes a GET request with automatic retries.

        Why tenacity retry?
        APIs are unreliable — timeouts, 429 rate limits, brief 5xx errors.
        Retrying with exponential backoff (2s, 4s, 8s) handles transient
        failures without manual try/except blocks in every client.

        wait_exponential means:
          attempt 1 fails → wait 2s → retry
          attempt 2 fails → wait 4s → retry
          attempt 3 fails → raise exception
        """
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
