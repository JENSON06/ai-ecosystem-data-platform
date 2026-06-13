"""
HuggingFace Silver Layer transformation pipeline.

Raw JSON → Cleaned, typed, deduplicated Parquet

New cleaning concepts introduced:
  - regexp_replace() — remove unwanted characters from strings
  - array_distinct() — remove duplicate values inside an array column
  - Handling deeply nested optional fields
"""

from pathlib import Path
from pyspark.sql import DataFrame, functions as F
from spark_jobs.session import get_spark_session
from spark_jobs.silver.base import null_report, write_silver, log_quality_summary
from loguru import logger

DATA_LAKE   = Path(__file__).parent.parent.parent / "data_lake"
RAW_PATH    = str(DATA_LAKE / "raw"    / "huggingface")
SILVER_PATH = str(DATA_LAKE / "silver" / "huggingface")


def extract(spark) -> DataFrame:
    raw = spark.read.option("multiLine", True).json(f"{RAW_PATH}/*.json")
    logger.info(f"Raw HuggingFace records: {raw.select(F.explode('records')).count()}")

    return (
        raw
        .select(F.col("batch_date"), F.explode("records").alias("r"))
        .select(
            "batch_date",
            F.col("r.model_id").alias("model_id"),
            F.col("r.author").alias("author"),
            F.col("r.model_name").alias("model_name"),
            F.col("r.pipeline_tag").alias("pipeline_tag"),
            F.col("r.downloads").alias("downloads"),
            F.col("r.likes").alias("likes"),
            F.col("r.tags").alias("tags"),
            F.col("r.library_name").alias("library_name"),
            F.col("r.created_at").alias("created_at"),
            F.col("r.last_modified").alias("last_modified"),
            F.col("r.private").alias("private"),
            F.col("r.gated").alias("gated"),
        )
    )


def cast_types(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("downloads",     F.col("downloads").cast("long"))
        .withColumn("likes",         F.col("likes").cast("long"))
        .withColumn("private",       F.col("private").cast("boolean"))
        .withColumn("created_at",    F.to_timestamp("created_at"))
        .withColumn("last_modified", F.to_timestamp("last_modified"))
    )


def clean(df: DataFrame) -> DataFrame:
    """
    HuggingFace-specific cleaning rules.

    model_id is the primary key — drop rows without it.
    downloads/likes null means 0 activity, not unknown.
    library_name null is filled with 'unknown' — many community
    models don't specify a library, that's valid data.

    array_distinct(tags) removes duplicate tag values like
    ["pytorch", "pytorch", "en"] → ["pytorch", "en"].
    This happens because the API sometimes returns repeated tags.

    regexp_replace on author removes special characters that would
    break downstream SQL queries and file path generation.
    """
    return (
        df
        .dropna(subset=["model_id"])
        .fillna({"downloads": 0, "likes": 0})
        .fillna({"library_name": "unknown", "author": "unknown"})
        .withColumn("author",       F.trim(F.lower(F.col("author"))))
        .withColumn("model_name",   F.trim(F.col("model_name")))
        .withColumn("pipeline_tag", F.trim(F.lower(F.col("pipeline_tag"))))
        # Remove duplicate tags within each model's tag array
        .withColumn("tags",
            F.when(F.col("tags").isNull(), F.array())
             .otherwise(F.array_distinct(F.col("tags")))
        )
        # Filter out tags that are region/deploy metadata — not useful for analytics
        # These tags start with "region:", "deploy:", "base_model:" etc.
        .withColumn("clean_tags",
            F.filter(
                F.col("tags"),
                lambda t: ~(
                    t.startswith("region:") |
                    t.startswith("deploy:") |
                    t.startswith("base_model:")
                )
            )
        )
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Keep the highest-download record per model_id per batch_date.

    A model can appear in multiple pipeline_tag queries.
    For example, a model tagged as both text-generation and text-classification
    would appear twice. We keep the record that has the most downloads
    as it's the most up-to-date snapshot from the API.
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy("model_id", "batch_date").orderBy(F.col("downloads").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def add_quality_flags(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("tags_count", F.size("clean_tags"))
        .withColumn("is_high_quality",
            (F.col("model_id").isNotNull()) &
            (F.col("downloads") > 100) &
            (F.col("pipeline_tag").isNotNull())
        )
        .withColumn("silver_ingested_at", F.current_timestamp())
    )


def run():
    spark = get_spark_session("Silver-HuggingFace")

    df = extract(spark)
    raw_count = df.count()

    null_report(df, "HuggingFace RAW")

    df = cast_types(df)
    df = clean(df)
    df = deduplicate(df)
    df = add_quality_flags(df)

    silver_count = df.count()
    log_quality_summary(raw_count, silver_count, "HuggingFace")

    print("\nSilver HuggingFace Sample:")
    df.select(
        "model_id", "author", "pipeline_tag",
        "downloads", "likes", "tags_count", "is_high_quality"
    ).orderBy(F.col("downloads").desc()).show(10, truncate=False)

    write_silver(df, SILVER_PATH)

    spark.stop()
    logger.info("[OK] HuggingFace Silver pipeline complete.")


if __name__ == "__main__":
    run()
