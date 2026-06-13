"""
Gold Layer: Research Trends

Produces 3 Gold tables:
  1. category_trends    — paper counts per AI research category
  2. prolific_authors   — most published authors with category breakdown
  3. cross_disciplinary — papers spanning multiple research areas

New Spark concepts:
  - collect_set()  — aggregate distinct values into an array per group
  - array_join()   — convert array column to delimited string
  - concat_ws()    — concatenate multiple columns into one string
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

# Human-readable category names for the dashboard
CATEGORY_LABELS = {
    "CS.AI": "Artificial Intelligence",
    "CS.LG": "Machine Learning",
    "CS.CL": "Computation & Language (NLP)",
    "CS.CV": "Computer Vision",
}


def build_category_trends(spark, df) -> None:
    """
    Publication counts and author statistics per research category.

    collect_set() is like groupBy but instead of counting, it collects
    all distinct values into an array. Here we collect all unique authors
    per category to find "who are the active researchers in Computer Vision?"

    array_join() converts that array back to a comma-separated string
    for easy display in dashboards and PostgreSQL storage.
    """
    df.createOrReplaceTempView("arxiv_silver")

    category_trends = spark.sql("""
        SELECT
            primary_category,
            COUNT(*)                        AS paper_count,
            ROUND(AVG(author_count), 2)     AS avg_authors_per_paper,
            MAX(author_count)               AS max_authors,
            SUM(CASE WHEN is_cross_disciplinary THEN 1 ELSE 0 END)
                                            AS cross_disciplinary_count,
            ROUND(
                SUM(CASE WHEN is_cross_disciplinary THEN 1 ELSE 0 END)
                / COUNT(*) * 100, 1
            )                               AS cross_disciplinary_pct,
            DENSE_RANK() OVER (
                ORDER BY COUNT(*) DESC
            )                               AS activity_rank,
            batch_date
        FROM arxiv_silver
        WHERE is_high_quality = true
        GROUP BY primary_category, batch_date
        ORDER BY paper_count DESC
    """)

    # Add human-readable category names
    label_map = F.create_map(
        *[item for pair in
          [(F.lit(k), F.lit(v)) for k, v in CATEGORY_LABELS.items()]
          for item in pair]
    )
    category_trends = category_trends.withColumn(
        "category_label",
        F.coalesce(label_map[F.col("primary_category")], F.col("primary_category"))
    )

    print("\nResearch Category Trends:")
    category_trends.show(truncate=False)

    write_gold(
        add_gold_metadata(category_trends),
        f"{GOLD_BASE}/research_trends/category_trends",
        "category_trends"
    )


def build_prolific_authors(spark, df) -> None:
    """
    Top 50 most published authors with their category breakdown.

    We explode the authors array to get one row per author per paper,
    then aggregate to find total papers and which categories they publish in.

    collect_set("primary_category") collects all distinct categories
    an author has published in — showing their research breadth.
    """
    # Explode authors array so each author gets their own row
    author_papers = (
        df
        .filter(F.col("is_high_quality") == True)
        .select(
            F.explode("authors").alias("author"),
            "primary_category",
            "pub_year",
            "is_cross_disciplinary",
            "batch_date"
        )
    )

    author_papers.createOrReplaceTempView("author_papers")

    prolific = spark.sql("""
        SELECT
            author,
            COUNT(*)                        AS paper_count,
            COUNT(DISTINCT primary_category) AS category_breadth,
            DENSE_RANK() OVER (
                ORDER BY COUNT(*) DESC
            )                               AS author_rank
        FROM author_papers
        GROUP BY author
        HAVING COUNT(*) >= 2
        ORDER BY paper_count DESC
        LIMIT 50
    """)

    # Add which categories each author has published in
    # collect_set() is an aggregate that builds a set (no duplicates) of values
    author_categories = (
        author_papers
        .groupBy("author")
        .agg(
            F.collect_set("primary_category").alias("categories_published_in")
        )
    )

    # Join the category set back onto the prolific authors table
    # broadcast() is a performance hint: when one DataFrame is small (< a few MB),
    # Spark sends a full copy to every worker instead of shuffling the big table.
    # This avoids a full shuffle join — one of the most expensive Spark operations.
    prolific = prolific.join(
        F.broadcast(author_categories),
        on="author",
        how="left"
    ).withColumn(
        "categories_str",
        F.array_join("categories_published_in", ", ")
    )

    print("\nTop 10 Prolific Authors:")
    prolific.select(
        "author_rank", "author", "paper_count",
        "category_breadth", "categories_str"
    ).show(10, truncate=False)

    write_gold(
        add_gold_metadata(prolific),
        f"{GOLD_BASE}/research_trends/prolific_authors",
        "prolific_authors"
    )


def build_cross_disciplinary(df) -> None:
    """
    Papers that span multiple research areas.

    Cross-disciplinary research is an important signal — papers that
    appear in both CS.AI and CS.CV are likely computer vision + AI fusion
    research (autonomous driving, medical imaging AI, etc.)

    array_sort() sorts the categories array alphabetically so
    ["CS.CV", "CS.AI"] and ["CS.AI", "CS.CV"] are treated as the same
    category combination.

    array_join() then converts it to a string for grouping.
    """
    cross = (
        df
        .filter(
            (F.col("is_high_quality") == True) &
            (F.col("is_cross_disciplinary") == True)
        )
        .withColumn(
            "category_combo",
            F.array_join(F.array_sort("categories"), " + ")
        )
        .groupBy("category_combo")
        .agg(
            F.count("*").alias("paper_count"),
            F.avg("author_count").cast("long").alias("avg_authors"),
        )
        .withColumn("combo_rank",
            F.dense_rank().over(Window.orderBy(F.col("paper_count").desc()))
        )
        .orderBy("combo_rank")
    )

    print("\nCross-Disciplinary Research Combos:")
    cross.show(15, truncate=False)

    write_gold(
        add_gold_metadata(cross),
        f"{GOLD_BASE}/research_trends/cross_disciplinary",
        "cross_disciplinary"
    )


def run():
    spark = get_spark_session("Gold-Research-Trends")
    df = read_silver(spark, "arxiv", SILVER_BASE)

    build_category_trends(spark, df)
    build_prolific_authors(spark, df)
    build_cross_disciplinary(df)

    spark.stop()
    logger.info("[OK] Research Trends Gold pipeline complete.")


if __name__ == "__main__":
    run()
