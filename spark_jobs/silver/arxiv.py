"""
arXiv Silver Layer transformation pipeline.

Raw JSON → Cleaned, typed, deduplicated Parquet

New cleaning concepts introduced:
  - concat_ws() — join array elements into a string
  - Derived columns from dates (year, month, quarter)
  - String cleaning on title and summary fields
"""

from pathlib import Path
from pyspark.sql import DataFrame, functions as F
from spark_jobs.session import get_spark_session
from spark_jobs.silver.base import null_report, write_silver, log_quality_summary
from loguru import logger

DATA_LAKE   = Path(__file__).parent.parent.parent / "data_lake"
RAW_PATH    = str(DATA_LAKE / "raw"    / "arxiv")
SILVER_PATH = str(DATA_LAKE / "silver" / "arxiv")


def extract(spark) -> DataFrame:
    raw = spark.read.option("multiLine", True).json(f"{RAW_PATH}/*.json")
    logger.info(f"Raw arXiv records: {raw.select(F.explode('records')).count()}")

    return (
        raw
        .select(F.col("batch_date"), F.explode("records").alias("r"))
        .select(
            "batch_date",
            F.col("r.paper_id").alias("paper_id"),
            F.col("r.title").alias("title"),
            F.col("r.summary").alias("summary"),
            F.col("r.authors").alias("authors"),
            F.col("r.primary_category").alias("primary_category"),
            F.col("r.categories").alias("categories"),
            F.col("r.published").alias("published"),
            F.col("r.updated").alias("updated"),
            F.col("r.doi").alias("doi"),
        )
    )


def cast_types(df: DataFrame) -> DataFrame:
    """
    arXiv dates come as full ISO timestamps: "2026-06-12T00:00:00+00:00"
    We parse them to timestamps then extract date components as separate
    columns. Storing year/month as integers allows fast numeric filtering
    without string parsing at query time.
    """
    return (
        df
        .withColumn("published_ts",   F.to_timestamp("published"))
        .withColumn("updated_ts",     F.to_timestamp("updated"))
        .withColumn("published_date", F.to_date("published_ts"))
        .withColumn("pub_year",       F.year("published_ts").cast("int"))
        .withColumn("pub_month",      F.month("published_ts").cast("int"))
        .withColumn("pub_quarter",
            # F.quarter() returns 1-4 based on month
            F.quarter("published_ts").cast("int")
        )
    )


def clean(df: DataFrame) -> DataFrame:
    """
    arXiv-specific cleaning rules.

    paper_id and title are required — drop rows without either.
    summary null is filled with empty string — some preprints have no abstract yet.

    regexp_replace on title:
    arXiv titles sometimes contain LaTeX markup like \\textbf{word}
    or $\\mathcal{X}$ for math notation. We strip the most common
    patterns to get clean readable titles for dashboards.

    concat_ws(", ", authors) converts the authors array into a comma-
    separated string. This makes it easier to search and display
    while keeping the original array column for analytics.

    author_count derived from array size — useful metric for paper complexity.
    """
    return (
        df
        .dropna(subset=["paper_id", "title"])
        .fillna({"summary": "", "doi": ""})
        # Clean title — remove common LaTeX artifacts
        .withColumn("title",
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.col("title"), r"\\[a-zA-Z]+\{([^}]*)\}", "$1"),
                    r"\$[^$]*\$", ""
                )
            )
        )
        # Standardize primary category to uppercase
        .withColumn("primary_category", F.upper(F.trim(F.col("primary_category"))))
        # Keep categories array clean — distinct values only
        .withColumn("categories",
            F.when(F.col("categories").isNull(), F.array())
             .otherwise(F.array_distinct(F.col("categories")))
        )
        # Derived: author count and authors as string
        .withColumn("author_count",  F.size("authors"))
        .withColumn("authors_str",   F.concat_ws(", ", F.col("authors")))
        # Multi-category flag: papers in >1 category are cross-disciplinary
        .withColumn("is_cross_disciplinary",
            F.size("categories") > 1
        )
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplicate by paper_id — a paper can appear in multiple category queries
    (cs.AI and cs.LG often overlap). Keep the most recently updated version.
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy("paper_id").orderBy(F.col("updated_ts").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def add_quality_flags(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("is_high_quality",
            (F.col("title").isNotNull()) &
            (F.length(F.col("title")) > 10) &
            (F.col("pub_year").isNotNull()) &
            (F.col("author_count") > 0)
        )
        .withColumn("silver_ingested_at", F.current_timestamp())
    )


def run():
    spark = get_spark_session("Silver-arXiv")

    df = extract(spark)
    raw_count = df.count()

    null_report(df, "arXiv RAW")

    df = cast_types(df)
    df = clean(df)
    df = deduplicate(df)
    df = add_quality_flags(df)

    silver_count = df.count()
    log_quality_summary(raw_count, silver_count, "arXiv")

    print("\nSilver arXiv Sample:")
    df.select(
        "title", "primary_category", "pub_year",
        "author_count", "is_cross_disciplinary", "is_high_quality"
    ).show(10, truncate=True)

    write_silver(df, SILVER_PATH)

    spark.stop()
    logger.info("[OK] arXiv Silver pipeline complete.")


if __name__ == "__main__":
    run()
