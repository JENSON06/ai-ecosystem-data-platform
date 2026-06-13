"""
Phase 3 — Spark Fundamentals: arXiv data exploration.

New concepts introduced here:
  1. Date parsing with to_date() and date_format()
  2. String functions: split(), lower(), trim()
  3. array_size() — measuring array columns
  4. Chaining multiple withColumn() calls
"""

from pathlib import Path
from pyspark.sql import functions as F
from spark_jobs.session import get_spark_session


DATA_LAKE_PATH = Path(__file__).parent.parent / "data_lake"
RAW_ARXIV = str(DATA_LAKE_PATH / "raw" / "arxiv")


def run():
    spark = get_spark_session("Phase3-arXiv-Exploration")

    raw_df = spark.read.option("multiLine", True).json(f"{RAW_ARXIV}/*.json")

    papers_df = raw_df.select(
        F.col("batch_date"),
        F.explode("records").alias("paper")
    ).select(
        F.col("batch_date"),
        F.col("paper.paper_id").alias("paper_id"),
        F.col("paper.title").alias("title"),
        F.col("paper.authors").alias("authors"),          # array<string>
        F.col("paper.primary_category").alias("primary_category"),
        F.col("paper.categories").alias("categories"),    # array<string>
        F.col("paper.published").alias("published"),
        F.col("paper.summary").alias("summary"),
    )

    print("\n" + "=" * 60)
    print("STEP 1: Schema")
    print("=" * 60)
    papers_df.printSchema()
    print(f"Total papers: {papers_df.count()}")

    # ----------------------------------------------------------------
    # STEP 2: Date parsing
    #
    # published comes in as a string like "2024-01-15T00:00:00+00:00"
    # to_timestamp() parses it into a proper timestamp type.
    # Then we extract year and month as separate columns for grouping.
    # ----------------------------------------------------------------
    papers_df = papers_df.withColumn(
        "published_ts", F.to_timestamp("published")
    ).withColumn(
        "pub_year", F.year("published_ts")
    ).withColumn(
        "pub_month", F.month("published_ts")
    ).withColumn(
        # array_size() counts elements in an array column
        "author_count", F.size("authors")
    ).withColumn(
        # size() on categories tells us how many categories a paper spans
        "category_count", F.size("categories")
    )

    print("\n" + "=" * 60)
    print("STEP 2: Papers with Parsed Dates")
    print("=" * 60)
    papers_df.select("title", "primary_category", "pub_year", "pub_month", "author_count") \
             .show(10, truncate=True)

    # ----------------------------------------------------------------
    # STEP 3: Publications by category
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 3: Papers Per Primary Category")
    print("=" * 60)
    papers_df.groupBy("primary_category") \
             .agg(
                 F.count("*").alias("paper_count"),
                 F.avg("author_count").alias("avg_authors")
             ) \
             .orderBy(F.col("paper_count").desc()) \
             .show(truncate=False)

    # ----------------------------------------------------------------
    # STEP 4: Prolific authors — explode authors array
    #
    # Each paper has an authors array. explode() gives one row per author.
    # Then count how many papers each author appears in.
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 4: Most Prolific Authors")
    print("=" * 60)
    papers_df.select(F.explode("authors").alias("author"), "primary_category") \
             .groupBy("author") \
             .agg(F.count("*").alias("paper_count")) \
             .orderBy(F.col("paper_count").desc()) \
             .show(10, truncate=False)

    # ----------------------------------------------------------------
    # STEP 5: Monthly publication trend
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 5: Monthly Publication Trend")
    print("=" * 60)
    papers_df.filter(F.col("pub_year").isNotNull()) \
             .groupBy("pub_year", "pub_month", "primary_category") \
             .count() \
             .orderBy("pub_year", "pub_month", F.col("count").desc()) \
             .show(20, truncate=False)

    spark.stop()
    print("\n[OK] arXiv exploration complete.")


if __name__ == "__main__":
    run()
