"""
Gold Layer: GitHub Trends

Produces 3 Gold tables:
  1. top_repos      — top 100 repos ranked by engagement score
  2. language_stats — language distribution with repo counts and avg stars
  3. topic_trends   — most popular topics across all AI repos

New Spark concepts:
  - createOrReplaceTempView() + spark.sql() — SQL-style transformations
  - dense_rank() — gap-free ranking
  - percent_rank() — percentile position of each row (0.0 to 1.0)
  - explode() on topics to count tag frequency
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


def build_top_repos(spark, df) -> None:
    """
    Top 100 repositories ranked by engagement score.

    Engagement score = stars + (forks * 2)
    Forks are weighted 2x because forking means active use,
    not just passive interest like starring.

    dense_rank() vs rank():
      With rank():       1, 1, 3, 4  (skips 2 when there's a tie)
      With dense_rank(): 1, 1, 2, 3  (no gaps — always consecutive)
    dense_rank is better for "top N" lists because rank() can skip numbers,
    making "top 10" actually return fewer or more than 10 results.

    percent_rank() gives the percentile position:
      The top repo gets 1.0, the median repo gets ~0.5, lowest gets 0.0
    This lets the dashboard say "this repo is in the top 5% of all AI repos".
    """

    # Register as SQL temp view so we can query with pure SQL
    # This is equivalent to a CTE in SQL — the view only exists in this session
    df.createOrReplaceTempView("github_silver")

    top_repos = spark.sql("""
        SELECT
            full_name,
            owner_login                              AS owner,
            language,
            stars,
            forks,
            stars + (forks * 2)                     AS engagement_score,
            topics_count,
            license,
            batch_date,
            DENSE_RANK() OVER (
                ORDER BY stars + (forks * 2) DESC
            )                                        AS engagement_rank,
            PERCENT_RANK() OVER (
                ORDER BY stars + (forks * 2)
            )                                        AS engagement_percentile
        FROM github_silver
        WHERE is_high_quality = true
        ORDER BY engagement_score DESC
        LIMIT 100
    """)

    print("\nTop 10 GitHub Repos by Engagement:")
    top_repos.select(
        "engagement_rank", "full_name", "stars", "forks",
        "engagement_score", "engagement_percentile"
    ).show(10, truncate=False)

    write_gold(add_gold_metadata(top_repos), f"{GOLD_BASE}/github_trends/top_repos", "top_repos")


def build_language_stats(spark, df) -> None:
    """
    Language distribution across AI repositories.

    This answers: "What programming languages dominate AI development?"

    We add a cumulative_share column using SUM() as a window function.
    This is a running total — it accumulates as you go down the ranked list.
    When cumulative_share hits 0.8, the languages above that line
    account for 80% of all AI repositories.

    This is the Pareto principle (80/20 rule) applied to languages.
    """
    df.createOrReplaceTempView("github_silver")

    language_stats = spark.sql("""
        WITH lang_counts AS (
            SELECT
                language,
                COUNT(*)        AS repo_count,
                AVG(stars)      AS avg_stars,
                MAX(stars)      AS max_stars,
                SUM(forks)      AS total_forks,
                SUM(stars)      AS total_stars
            FROM github_silver
            WHERE language != 'unknown'
              AND is_high_quality = true
            GROUP BY language
        ),
        ranked AS (
            SELECT *,
                DENSE_RANK() OVER (ORDER BY repo_count DESC) AS language_rank,
                repo_count / SUM(repo_count) OVER ()         AS share_pct
            FROM lang_counts
        )
        SELECT
            *,
            ROUND(SUM(share_pct) OVER (
                ORDER BY repo_count DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 4) AS cumulative_share
        FROM ranked
        ORDER BY repo_count DESC
    """)

    print("\nLanguage Stats:")
    language_stats.show(10, truncate=False)

    write_gold(add_gold_metadata(language_stats), f"{GOLD_BASE}/github_trends/language_stats", "language_stats")


def build_topic_trends(df) -> None:
    """
    Most popular topics across all AI repositories.

    Each repo has a topics array like ["pytorch", "nlp", "transformers"].
    explode() turns that into one row per topic, then we count.

    This is a classic "tag frequency" analysis — very common in
    real-world analytics (hashtag trends, product categories, etc.)
    """
    topic_counts = (
        df
        .filter(F.col("is_high_quality") == True)
        .select(F.explode("topics").alias("topic"), "stars", "batch_date")
        .filter(F.length("topic") > 2)          # filter out noise like "ai", "ml"
        .groupBy("topic")
        .agg(
            F.count("*").alias("repo_count"),
            F.sum("stars").alias("total_stars"),
            F.avg("stars").cast("long").alias("avg_stars_per_repo")
        )
        .withColumn("topic_rank",
            F.dense_rank().over(Window.orderBy(F.col("repo_count").desc()))
        )
        .orderBy("topic_rank")
    )

    print("\nTop 15 Topics:")
    topic_counts.show(15, truncate=False)

    write_gold(add_gold_metadata(topic_counts), f"{GOLD_BASE}/github_trends/topic_trends", "topic_trends")


def build_org_leaderboard(spark, df) -> None:
    """
    Top organizations by total stars across all their repositories.

    Only looks at Organization owner_type — filters out individual users.
    This shows which companies/orgs dominate the AI open-source space.
    """
    df.createOrReplaceTempView("github_silver")

    org_board = spark.sql("""
        SELECT
            owner_login                     AS organization,
            COUNT(*)                        AS repo_count,
            SUM(stars)                      AS total_stars,
            SUM(forks)                      AS total_forks,
            MAX(stars)                      AS top_repo_stars,
            ROUND(AVG(stars))               AS avg_stars,
            DENSE_RANK() OVER (
                ORDER BY SUM(stars) DESC
            )                               AS org_rank
        FROM github_silver
        WHERE owner_type = 'Organization'
          AND is_high_quality = true
        GROUP BY owner_login
        ORDER BY total_stars DESC
        LIMIT 20
    """)

    print("\nTop Organizations:")
    org_board.show(10, truncate=False)

    write_gold(add_gold_metadata(org_board), f"{GOLD_BASE}/github_trends/org_leaderboard", "org_leaderboard")


def run():
    spark = get_spark_session("Gold-GitHub-Trends")
    df = read_silver(spark, "github", SILVER_BASE)

    build_top_repos(spark, df)
    build_language_stats(spark, df)
    build_topic_trends(df)
    build_org_leaderboard(spark, df)

    spark.stop()
    logger.info("[OK] GitHub Gold pipeline complete.")


if __name__ == "__main__":
    run()
