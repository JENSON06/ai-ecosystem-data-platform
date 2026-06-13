"""
Base Gold Layer utilities.

Gold tables are written as Parquet with no partitioning by default.
Why no partition on Gold?
Gold tables are small aggregated datasets — a few hundred to a few
thousand rows at most. Partitioning tiny tables adds overhead with
no benefit. The dashboard reads the whole Gold table every time anyway.
"""

from pyspark.sql import DataFrame, functions as F
from loguru import logger


def read_silver(spark, source: str, silver_base: str) -> DataFrame:
    """
    Reads the latest Silver partition.

    In production you'd pass a specific batch_date.
    Here we read all partitions (just one daily run exists)
    and let Spark figure out the schema from the partition folders.
    """
    path = f"{silver_base}/{source}"
    df = spark.read.parquet(path)
    logger.info(f"Read silver/{source}: {df.count()} rows")
    return df


def write_gold(df: DataFrame, output_path: str, table_name: str) -> None:
    """
    Writes a Gold DataFrame to Parquet.

    mode("overwrite") on Gold is safe — Gold is always fully regenerated
    from Silver. There is no incremental Gold update, you always recompute.
    This keeps Gold tables fresh and consistent.
    """
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"[OK] Gold table written: {table_name} → {output_path} ({df.count()} rows)")


def add_gold_metadata(df: DataFrame) -> DataFrame:
    """Adds standard metadata columns to every Gold table."""
    return df.withColumn("gold_created_at", F.current_timestamp())
