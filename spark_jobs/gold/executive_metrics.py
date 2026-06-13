"""
Gold Layer: Executive Metrics (KPI Dashboard)

Produces a single Gold table with one row per batch_date containing
all top-level KPIs for the executive dashboard.

This is the "single source of truth" for dashboard headline numbers:
  - Total models tracked
  - Total repos tracked
  - Total papers tracked
  - Most downloaded model
  - Top GitHub repo
  - Most active research category

New Spark concept:
  - union() / unionByName() — stacking DataFrames vertically
  - first() aggregate — get one value from a group
  - Cross-source joins — combining GitHub + HuggingFace + arXiv metrics
"""

from pathlib import Path
from pyspark.sql import DataFrame, functions as F
from spark_jobs.session import get_spark_session
from spark_jobs.gold.base import read_silver, write_gold, add_gold_metadata
from loguru import logger

DATA_LAKE   = Path(__file__).parent.parent.parent / "data_lake"
SILVER_BASE = str(DATA_LAKE / "silver")
GOLD_BASE   = str(DATA_LAKE / "gold")


def compute_kpis(github_df: DataFrame, hf_df: DataFrame, arxiv_df: DataFrame) -> DataFrame:
    """
    Joins aggregate metrics from all three sources into one KPI row per date.

    Why one row per date?
    The dashboard headline section shows single numbers:
    "1,468 repositories | 500 models | 314 papers"
    A pre-aggregated single row is the fastest possible read for a dashboard.
    No groupBy or filter needed at query time — just read and display.

    unionByName() stacks two DataFrames that have the same column names.
    This is used here to build a metrics table from three separate aggregations.
    All three aggregations produce the same schema so they can be unioned.
    """
    batch_date = github_df.select("batch_date").first()[0]

    # --- GitHub KPIs ---
    github_kpis = github_df.filter(F.col("is_high_quality") == True).agg(
        F.count("*").alias("total_repos"),
        F.sum("stars").alias("total_stars"),
        F.sum("forks").alias("total_forks"),
        F.max("stars").alias("max_repo_stars"),
        F.countDistinct("language").alias("unique_languages"),
        F.countDistinct("owner_login").alias("unique_owners"),
    )

    # --- HuggingFace KPIs ---
    hf_kpis = hf_df.filter(F.col("is_high_quality") == True).agg(
        F.count("*").alias("total_models"),
        F.sum("downloads").alias("total_downloads"),
        F.sum("likes").alias("total_likes"),
        F.max("downloads").alias("max_model_downloads"),
        F.countDistinct("author").alias("unique_authors"),
        F.countDistinct("pipeline_tag").alias("unique_pipeline_types"),
    )

    # --- arXiv KPIs ---
    arxiv_kpis = arxiv_df.filter(F.col("is_high_quality") == True).agg(
        F.count("*").alias("total_papers"),
        F.avg("author_count").alias("avg_authors_per_paper"),
        F.countDistinct("primary_category").alias("unique_categories"),
        F.sum(F.col("is_cross_disciplinary").cast("int")).alias("cross_disciplinary_papers"),
    )

    # Get spotlight values: the single "hero" item per category
    top_repo = (
        github_df
        .filter(F.col("is_high_quality") == True)
        .orderBy(F.col("stars").desc())
        .select(F.col("full_name").alias("top_repo_name"), F.col("stars").alias("top_repo_stars"))
        .limit(1)
    )

    top_model = (
        hf_df
        .filter(F.col("is_high_quality") == True)
        .orderBy(F.col("downloads").desc())
        .select(F.col("model_id").alias("top_model_id"), F.col("downloads").alias("top_model_downloads_val"))
        .limit(1)
    )

    top_category = (
        arxiv_df
        .filter(F.col("is_high_quality") == True)
        .groupBy("primary_category")
        .count()
        .orderBy(F.col("count").desc())
        .select(F.col("primary_category").alias("top_research_category"))
        .limit(1)
    )

    # Build the single KPI row using crossJoin
    # crossJoin on single-row DataFrames is safe and efficient —
    # it's just putting all the numbers side by side into one wide row
    kpi_row = (
        github_kpis
        .crossJoin(hf_kpis)
        .crossJoin(arxiv_kpis)
        .crossJoin(top_repo)
        .crossJoin(top_model)
        .crossJoin(top_category)
        .withColumn("batch_date", F.lit(batch_date))
    )

    return kpi_row


def run():
    spark = get_spark_session("Gold-Executive-Metrics")

    github_df = read_silver(spark, "github",      SILVER_BASE)
    hf_df     = read_silver(spark, "huggingface", SILVER_BASE)
    arxiv_df  = read_silver(spark, "arxiv",       SILVER_BASE)

    kpis = compute_kpis(github_df, hf_df, arxiv_df)

    print("\n[KPI] Executive KPI Dashboard:")
    # Show transposed for readability (one metric per line)
    for row in kpis.collect():
        row_dict = row.asDict()
        print("\n" + "=" * 50)
        for k, v in sorted(row_dict.items()):
            if k != "gold_created_at":
                print(f"  {k:<35} {v}")

    write_gold(
        add_gold_metadata(kpis),
        f"{GOLD_BASE}/executive_metrics/kpis",
        "executive_kpis"
    )

    spark.stop()
    logger.info("[OK] Executive Metrics Gold pipeline complete.")


if __name__ == "__main__":
    run()
