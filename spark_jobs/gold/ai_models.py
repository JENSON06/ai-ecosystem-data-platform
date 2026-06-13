"""
Gold Layer: AI Models Metrics

Produces 3 Gold tables:
  1. top_models        — top models per pipeline type by downloads
  2. author_leaderboard — top authors/orgs by total downloads + likes
  3. pipeline_summary  — download distribution across pipeline categories

New Spark concepts:
  - pivot() — reshape rows into columns (cross-tab style)
  - ntile() — divide rows into N equal buckets (quartiles, deciles)
  - lag()   — access the value from the previous row in a window
"""

from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_jobs.session import get_spark_session
from spark_jobs.gold.base import read_silver, write_gold, add_gold_metadata
from loguru import logger

DATA_LAKE   = Path(__file__).parent.parent.parent / "data_lake"
SILVER_BASE = str(DATA_LAKE / "silver")
GOLD_BASE   = str(DATA_LAKE / "gold")


def build_top_models(spark, df) -> None:
    """
    Top 5 models per pipeline type, ranked by downloads.

    This uses a window function to rank within each pipeline category.
    The business question: "For each AI task, what are the go-to models?"

    ntile(4) divides all models into 4 equal buckets (quartiles):
      ntile=4 → top 25% of models by downloads (elite tier)
      ntile=3 → 50-75th percentile
      ntile=2 → 25-50th percentile
      ntile=1 → bottom 25%
    This tells you which tier a model belongs to globally.
    """
    df.createOrReplaceTempView("hf_silver")

    top_models = spark.sql("""
        WITH ranked AS (
            SELECT
                model_id,
                author,
                pipeline_tag,
                downloads,
                likes,
                library_name,
                tags_count,
                batch_date,
                DENSE_RANK() OVER (
                    PARTITION BY pipeline_tag
                    ORDER BY downloads DESC
                ) AS rank_in_category,
                DENSE_RANK() OVER (
                    ORDER BY downloads DESC
                ) AS global_rank,
                NTILE(4) OVER (
                    ORDER BY downloads
                ) AS download_quartile
            FROM hf_silver
            WHERE is_high_quality = true
        )
        SELECT * FROM ranked
        WHERE rank_in_category <= 5
        ORDER BY pipeline_tag, rank_in_category
    """)

    print("\nTop 5 Models Per Pipeline Type:")
    top_models.select(
        "rank_in_category", "pipeline_tag", "model_id", "downloads", "download_quartile"
    ).show(20, truncate=False)

    write_gold(add_gold_metadata(top_models), f"{GOLD_BASE}/ai_models/top_models", "top_models")


def build_author_leaderboard(spark, df) -> None:
    """
    Author/organization leaderboard by influence.

    Influence score = log10(total_downloads) * total_likes
    Why log10?
    Downloads span 6 orders of magnitude (100 to 100,000,000+).
    Without log scaling, one author with 100M downloads would dwarf
    everyone else and make the ranking useless. Log10 compresses the
    range while preserving the ordering.

    This is a common technique in analytics — whenever you have
    skewed distributions, log-transform before scoring.
    """
    df.createOrReplaceTempView("hf_silver")

    leaderboard = spark.sql("""
        SELECT
            author,
            COUNT(*)                                    AS model_count,
            SUM(downloads)                              AS total_downloads,
            SUM(likes)                                  AS total_likes,
            MAX(downloads)                              AS top_model_downloads,
            ROUND(AVG(downloads))                       AS avg_downloads,
            ROUND(
                LOG10(SUM(downloads) + 1) * SUM(likes)
            )                                           AS influence_score,
            DENSE_RANK() OVER (
                ORDER BY SUM(downloads) DESC
            )                                           AS download_rank
        FROM hf_silver
        WHERE is_high_quality = true
        GROUP BY author
        ORDER BY total_downloads DESC
        LIMIT 25
    """)

    print("\nAuthor Leaderboard (Top 10):")
    leaderboard.select(
        "download_rank", "author", "model_count",
        "total_downloads", "total_likes", "influence_score"
    ).show(10, truncate=False)

    write_gold(add_gold_metadata(leaderboard), f"{GOLD_BASE}/ai_models/author_leaderboard", "author_leaderboard")


def build_pipeline_summary(df) -> None:
    """
    Downloads and model counts aggregated per pipeline type.

    lag() window function explained:
    lag(total_downloads, 1) returns the total_downloads value from
    the PREVIOUS row in the window order. This lets you compute
    "how does this category compare to the one ranked above it?"

    In a time-series context, lag() is used for:
      growth_rate = (current_value - lag(current_value)) / lag(current_value)
    We'll use that pattern fully in Phase 7 when we have multiple dates.
    For now it shows the concept with category-over-category comparison.
    """
    window = Window.orderBy(F.col("total_downloads").desc())

    summary = (
        df
        .filter(F.col("is_high_quality") == True)
        .groupBy("pipeline_tag")
        .agg(
            F.count("*").alias("model_count"),
            F.sum("downloads").alias("total_downloads"),
            F.sum("likes").alias("total_likes"),
            F.avg("downloads").cast("long").alias("avg_downloads"),
            F.max("downloads").alias("max_downloads"),
        )
        .withColumn("category_rank",
            F.dense_rank().over(window)
        )
        .withColumn("prev_category_downloads",
            # lag(column, offset) — offset=1 means "look 1 row back"
            F.lag("total_downloads", 1).over(window)
        )
        .withColumn("download_share_pct",
            F.round(
                F.col("total_downloads") /
                F.sum("total_downloads").over(Window.rowsBetween(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )) * 100, 2
            )
        )
        .orderBy("category_rank")
    )

    print("\nPipeline Summary:")
    summary.select(
        "category_rank", "pipeline_tag", "model_count",
        "total_downloads", "download_share_pct"
    ).show(truncate=False)

    write_gold(add_gold_metadata(summary), f"{GOLD_BASE}/ai_models/pipeline_summary", "pipeline_summary")


def run():
    spark = get_spark_session("Gold-AI-Models")
    df = read_silver(spark, "huggingface", SILVER_BASE)

    build_top_models(spark, df)
    build_author_leaderboard(spark, df)
    build_pipeline_summary(df)

    spark.stop()
    logger.info("[OK] AI Models Gold pipeline complete.")


if __name__ == "__main__":
    run()
