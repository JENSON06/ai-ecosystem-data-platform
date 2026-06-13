"""
Base Silver Layer transformer.

Contains shared utilities used by all three silver jobs:
  - Quality report generation
  - Parquet writer with consistent settings
  - Null percentage calculator

Design principle: DRY (Don't Repeat Yourself).
Writing the same parquet save logic in 3 files means 3 places to fix bugs.
"""

from pyspark.sql import DataFrame, functions as F
from loguru import logger


def null_report(df: DataFrame, label: str) -> None:
    """
    Prints the percentage of nulls per column.

    Why percentages instead of counts?
    A count of 50 nulls means nothing without context.
    50 nulls out of 100 rows (50%) is catastrophic.
    50 nulls out of 1,000,000 rows (0.005%) is acceptable.
    """
    total = df.count()
    logger.info(f"\n{'='*50}\nNull Report: {label} (total rows: {total})\n{'='*50}")

    null_counts = df.select([
        F.round(
            F.sum(F.col(c).isNull().cast("int")) / total * 100, 2
        ).alias(c)
        for c in df.columns
    ])
    null_counts.show(truncate=False)


def write_silver(df: DataFrame, output_path: str, partition_col: str = "batch_date") -> None:
    """
    Writes a DataFrame to the Silver Layer as Parquet.

    Key settings explained:

    mode("overwrite") — if today's partition already exists, replace it.
    This makes the job idempotent — safe to re-run without creating duplicates.

    partitionBy(partition_col) — creates subdirectories per date value.
    Spark reads only the relevant partition when filtering by date.

    compression snappy — fast compression/decompression, moderate size reduction.
    Used over gzip because snappy is splittable (gzip is not),
    meaning multiple Spark workers can decompress different chunks in parallel.
    """
    (
        df.write
        .mode("overwrite")
        .partitionBy(partition_col)
        .option("compression", "snappy")
        .parquet(output_path)
    )
    logger.info(f"[OK] Written to Silver: {output_path} | partitioned by {partition_col}")


def log_quality_summary(raw_count: int, silver_count: int, source: str) -> None:
    dropped = raw_count - silver_count
    pct_kept = round(silver_count / raw_count * 100, 1) if raw_count > 0 else 0
    logger.info(
        f"\n{'='*50}\n"
        f"Quality Summary: {source}\n"
        f"  Raw records:    {raw_count}\n"
        f"  Silver records: {silver_count}\n"
        f"  Dropped:        {dropped} ({100 - pct_kept:.1f}%)\n"
        f"  Kept:           {pct_kept}%\n"
        f"{'='*50}"
    )
