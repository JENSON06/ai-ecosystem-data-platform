"""
Phase 3 — Spark Fundamentals: GitHub data exploration.

This job teaches:
  1. Reading nested JSON into a DataFrame
  2. printSchema() — understanding your data structure
  3. select() — choosing columns
  4. filter() — row-level filtering
  5. withColumn() — adding/transforming columns
  6. groupBy() + agg() — basic aggregations
  7. show() vs collect() — when to use each
  8. explain() — reading the execution plan
"""

from pathlib import Path
from pyspark.sql import functions as F
from spark_jobs.session import get_spark_session


DATA_LAKE_PATH = Path(__file__).parent.parent / "data_lake"
RAW_GITHUB = str(DATA_LAKE_PATH / "raw" / "github")


def run():
    spark = get_spark_session("Phase3-GitHub-Exploration")

    # ----------------------------------------------------------------
    # STEP 1: Read raw JSON
    #
    # spark.read.option("multiLine", True) is required because our JSON
    # file has the entire array on multiple lines (pretty-printed).
    # Without it Spark tries to read each line as a separate JSON object
    # and fails because line 1 is just "{".
    #
    # Spark reads ALL files in the folder matching the pattern.
    # This is powerful — adding tomorrow's file requires zero code changes.
    # ----------------------------------------------------------------
    raw_df = spark.read.option("multiLine", True).json(f"{RAW_GITHUB}/*.json")

    print("\n" + "=" * 60)
    print("STEP 1: Raw JSON Schema")
    print("=" * 60)
    # printSchema() shows the tree structure Spark inferred from your JSON.
    # Notice 'records' is an ARRAY of STRUCT — nested data preserved perfectly.
    raw_df.printSchema()

    # ----------------------------------------------------------------
    # STEP 2: Explode the records array
    #
    # Our JSON envelope looks like: { "source": "...", "records": [...] }
    # The actual repo data is inside the 'records' array.
    #
    # explode() turns one row with an array of 800 items
    # into 800 rows, one per item. This is how you "unwrap" nested arrays.
    #
    # TRANSFORMATION — no data is processed yet, Spark just builds the plan.
    # ----------------------------------------------------------------
    exploded_df = raw_df.select(
        F.col("batch_date"),
        F.explode(F.col("records")).alias("repo")   # each array element becomes a row
    )

    # ----------------------------------------------------------------
    # STEP 3: Flatten the struct into columns
    #
    # After explode(), each row has a 'repo' column that is a STRUCT.
    # repo.name, repo.stars, etc. accesses struct fields.
    # We flatten them into top-level columns for easy querying.
    #
    # TRANSFORMATION — still lazy, no execution yet.
    # ----------------------------------------------------------------
    repos_df = exploded_df.select(
        F.col("batch_date"),
        F.col("repo.repo_id").cast("long").alias("repo_id"),
        F.col("repo.name").alias("name"),
        F.col("repo.full_name").alias("full_name"),
        F.col("repo.stars").cast("long").alias("stars"),
        F.col("repo.forks").cast("long").alias("forks"),
        F.col("repo.language").alias("language"),
        F.col("repo.owner_login").alias("owner"),
        F.col("repo.owner_type").alias("owner_type"),
        F.col("repo.topics").alias("topics"),          # stays as array
        F.col("repo.created_at").alias("created_at"),
        F.col("repo.is_fork").cast("boolean").alias("is_fork"),
        F.col("repo.license").alias("license"),
    )

    print("\n" + "=" * 60)
    print("STEP 2 & 3: Flattened Repos Schema")
    print("=" * 60)
    repos_df.printSchema()

    # ----------------------------------------------------------------
    # STEP 4: show() — first ACTION in this job
    #
    # This is the moment Spark actually executes all transformations above.
    # show() is an ACTION — it triggers the full execution plan.
    # truncate=False shows full column values (no "..." truncation).
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 4: Top 10 Repos by Stars")
    print("=" * 60)
    repos_df.orderBy(F.col("stars").desc()).show(10, truncate=False)

    # ----------------------------------------------------------------
    # STEP 5: filter() — keep only original repos, not forks
    #
    # filter() is equivalent to SQL WHERE clause.
    # It's a TRANSFORMATION — still lazy until an action is called.
    # ----------------------------------------------------------------
    original_repos = repos_df.filter(F.col("is_fork") == False)

    print("\n" + "=" * 60)
    print(f"STEP 5: Record Counts")
    print("=" * 60)
    # count() is an ACTION
    total = repos_df.count()
    originals = original_repos.count()
    print(f"  Total repos:    {total}")
    print(f"  Original repos: {originals}")
    print(f"  Forked repos:   {total - originals}")

    # ----------------------------------------------------------------
    # STEP 6: withColumn() — add a computed column
    #
    # withColumn() adds a new column or replaces an existing one.
    # Here we compute an engagement score: stars + (forks * 2)
    # Forks are weighted higher because forking = active use, not just interest.
    # ----------------------------------------------------------------
    enriched_df = repos_df.withColumn(
        "engagement_score",
        F.col("stars") + (F.col("forks") * 2)
    ).withColumn(
        # F.when() is Spark's if/else — equivalent to SQL CASE WHEN
        "size_category",
        F.when(F.col("stars") >= 10000, "large")
         .when(F.col("stars") >= 1000, "medium")
         .otherwise("small")
    )

    print("\n" + "=" * 60)
    print("STEP 6: Enriched DataFrame with Computed Columns")
    print("=" * 60)
    enriched_df.select("full_name", "stars", "forks", "engagement_score", "size_category") \
               .orderBy(F.col("engagement_score").desc()) \
               .show(10, truncate=False)

    # ----------------------------------------------------------------
    # STEP 7: groupBy() + agg() — language distribution
    #
    # groupBy() groups rows by a column value.
    # agg() computes aggregate functions per group.
    # This is equivalent to SQL: SELECT language, COUNT(*), AVG(stars)
    #                            FROM repos GROUP BY language
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 7: Top Languages by Repo Count")
    print("=" * 60)
    repos_df.filter(F.col("language").isNotNull()) \
            .groupBy("language") \
            .agg(
                F.count("*").alias("repo_count"),
                F.avg("stars").cast("long").alias("avg_stars"),
                F.max("stars").alias("max_stars")
            ) \
            .orderBy(F.col("repo_count").desc()) \
            .show(10, truncate=False)

    # ----------------------------------------------------------------
    # STEP 8: explain() — read the execution plan
    #
    # This shows HOW Spark will execute your transformations.
    # In production you use this to find performance bottlenecks.
    # Look for "FileScan" (reading data) and "HashAggregate" (groupBy).
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 8: Execution Plan (explain)")
    print("=" * 60)
    repos_df.groupBy("language").count().explain()

    spark.stop()
    print("\n[OK] GitHub exploration complete.")


if __name__ == "__main__":
    run()
