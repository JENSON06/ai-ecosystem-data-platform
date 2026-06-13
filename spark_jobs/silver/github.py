"""
GitHub Silver Layer transformation pipeline.

Raw JSON → Cleaned, typed, deduplicated Parquet

Steps:
  1. Read raw JSON and explode records
  2. Cast all columns to correct types
  3. Standardize text fields
  4. Handle nulls with business rules
  5. Deduplicate by repo_id + batch_date
  6. Add data quality flags
  7. Write partitioned Parquet
"""

from pathlib import Path
from pyspark.sql import DataFrame, functions as F
from spark_jobs.session import get_spark_session
from spark_jobs.silver.base import null_report, write_silver, log_quality_summary
from loguru import logger

DATA_LAKE = Path(__file__).parent.parent.parent / "data_lake"
RAW_PATH  = str(DATA_LAKE / "raw"    / "github")
SILVER_PATH = str(DATA_LAKE / "silver" / "github")


def extract(spark) -> DataFrame:
    """
    Read raw JSON and flatten nested structure.

    multiLine=True is required because our JSON is pretty-printed
    across multiple lines — not newline-delimited JSON (NDJSON).
    """
    raw = spark.read.option("multiLine", True).json(f"{RAW_PATH}/*.json")
    raw_count = raw.select(F.explode("records")).count()
    logger.info(f"Raw GitHub records read: {raw_count}")

    return (
        raw
        .select(F.col("batch_date"), F.explode("records").alias("r"))
        .select(
            "batch_date",
            F.col("r.repo_id").alias("repo_id"),
            F.col("r.name").alias("name"),
            F.col("r.full_name").alias("full_name"),
            F.col("r.description").alias("description"),
            F.col("r.stars").alias("stars"),
            F.col("r.forks").alias("forks"),
            F.col("r.watchers").alias("watchers"),
            F.col("r.open_issues").alias("open_issues"),
            F.col("r.language").alias("language"),
            F.col("r.topics").alias("topics"),
            F.col("r.owner_login").alias("owner_login"),
            F.col("r.owner_type").alias("owner_type"),
            F.col("r.created_at").alias("created_at"),
            F.col("r.updated_at").alias("updated_at"),
            F.col("r.is_fork").alias("is_fork"),
            F.col("r.license").alias("license"),
            F.col("r.size_kb").alias("size_kb"),
        )
    )


def cast_types(df: DataFrame) -> DataFrame:
    """
    Enforce correct data types on every column.

    Why explicit casting instead of relying on Spark inference?
    Spark infers types from the first few rows. If the first repo
    has null stars, Spark might infer stars as string. Explicit
    casting guarantees consistent types across all daily runs.

    to_timestamp() parses ISO 8601 strings like "2024-01-15T10:30:00Z"
    into proper timestamp types that support date arithmetic.
    """
    return (
        df
        .withColumn("repo_id",     F.col("repo_id").cast("long"))
        .withColumn("stars",       F.col("stars").cast("long"))
        .withColumn("forks",       F.col("forks").cast("long"))
        .withColumn("watchers",    F.col("watchers").cast("long"))
        .withColumn("open_issues", F.col("open_issues").cast("long"))
        .withColumn("size_kb",     F.col("size_kb").cast("long"))
        .withColumn("is_fork",     F.col("is_fork").cast("boolean"))
        .withColumn("created_at",  F.to_timestamp("created_at"))
        .withColumn("updated_at",  F.to_timestamp("updated_at"))
    )


def clean(df: DataFrame) -> DataFrame:
    """
    Apply business rules to handle nulls and standardize values.

    Business rules explained:
    - stars/forks null → fill with 0 (a null count means 0 activity, not unknown)
    - language null → fill with "Unknown" (preserves the row for analysis)
    - description null → fill with empty string (avoids NullPointerException in string ops)
    - name/full_name null → DROP the row (a repo without a name is unusable)
    - F.trim() removes leading/trailing whitespace from text fields
    - F.lower() on language standardizes "Python" vs "python" vs "PYTHON"

    fillna({'col': value}) fills nulls in specific columns with specific values.
    This is safer than fillna(value) which fills ALL columns with the same value.
    """
    return (
        df
        # Drop rows missing the primary identifier
        .dropna(subset=["repo_id", "full_name"])
        # Fill numeric nulls with 0
        .fillna({"stars": 0, "forks": 0, "watchers": 0, "open_issues": 0, "size_kb": 0})
        # Fill text nulls with meaningful defaults
        .fillna({"language": "Unknown", "description": "", "license": "Unknown"})
        # Standardize text
        .withColumn("full_name",   F.trim(F.col("full_name")))
        .withColumn("name",        F.trim(F.col("name")))
        .withColumn("language",    F.trim(F.lower(F.col("language"))))
        .withColumn("owner_login", F.trim(F.lower(F.col("owner_login"))))
        # Ensure topics is never null — replace null array with empty array
        .withColumn("topics",
            F.when(F.col("topics").isNull(), F.array()).otherwise(F.col("topics"))
        )
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate repos within the same batch.

    Why (repo_id, batch_date) and not just repo_id?
    The same repo can legitimately appear across different batch dates
    (we want to track it daily). But within one batch date, the same
    repo_id appearing twice is a duplicate from overlapping search queries.

    dropDuplicates keeps the first occurrence in the DataFrame.
    The order isn't guaranteed in distributed systems, so we sort first
    to keep the row with the most stars (most complete data).
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy("repo_id", "batch_date").orderBy(F.col("stars").desc())

    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def add_quality_flags(df: DataFrame) -> DataFrame:
    """
    Add metadata columns used downstream for filtering and monitoring.

    is_high_quality: a repo passes quality if it has a name, language,
    and meaningful activity (stars > 0). Gold layer will use this flag
    to filter for reliable metrics.

    topics_count: counts array elements — useful for filtering repos
    with no topic tags vs. richly tagged repos.

    ingested_at: timestamp of when this record entered the Silver layer.
    Critical for debugging data freshness issues.
    """
    return (
        df
        .withColumn("topics_count", F.size(F.col("topics")))
        .withColumn("is_high_quality",
            (F.col("full_name").isNotNull()) &
            (F.col("language") != "unknown") &
            (F.col("stars") > 0)
        )
        .withColumn("silver_ingested_at", F.current_timestamp())
    )


def run():
    spark = get_spark_session("Silver-GitHub")

    # --- Extract ---
    df = extract(spark)
    raw_count = df.count()

    # --- Show null situation before cleaning ---
    null_report(df, "GitHub RAW")

    # --- Transform pipeline ---
    # Each step is a separate function — easy to test individually,
    # easy to add/remove steps without breaking the whole pipeline.
    df = cast_types(df)
    df = clean(df)
    df = deduplicate(df)
    df = add_quality_flags(df)

    silver_count = df.count()
    log_quality_summary(raw_count, silver_count, "GitHub")

    # --- Preview Silver output ---
    print("\nSilver GitHub Sample:")
    df.select(
        "full_name", "stars", "forks", "language",
        "topics_count", "is_high_quality", "batch_date"
    ).orderBy(F.col("stars").desc()).show(10, truncate=False)

    # --- Write ---
    write_silver(df, SILVER_PATH)

    spark.stop()
    logger.info("[OK] GitHub Silver pipeline complete.")


if __name__ == "__main__":
    run()
