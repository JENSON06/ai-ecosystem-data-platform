"""
Phase 3 — Spark Fundamentals: HuggingFace data exploration.

New concepts introduced here:
  1. Working with array columns using explode() on tags
  2. dropDuplicates() — deduplication
  3. isNull() / isNotNull() — null handling
  4. cast() — type conversion
  5. Window functions preview (rank by downloads per pipeline type)
"""

from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_jobs.session import get_spark_session


DATA_LAKE_PATH = Path(__file__).parent.parent / "data_lake"
RAW_HF = str(DATA_LAKE_PATH / "raw" / "huggingface")


def run():
    spark = get_spark_session("Phase3-HuggingFace-Exploration")

    raw_df = spark.read.option("multiLine", True).json(f"{RAW_HF}/*.json")

    # Explode records array into individual model rows
    models_df = raw_df.select(
        F.col("batch_date"),
        F.explode("records").alias("model")
    ).select(
        F.col("batch_date"),
        F.col("model.model_id").alias("model_id"),
        F.col("model.author").alias("author"),
        F.col("model.model_name").alias("model_name"),
        F.col("model.pipeline_tag").alias("pipeline_tag"),
        F.col("model.downloads").cast("long").alias("downloads"),
        F.col("model.likes").cast("long").alias("likes"),
        F.col("model.tags").alias("tags"),              # array<string>
        F.col("model.library_name").alias("library_name"),
        F.col("model.created_at").alias("created_at"),
    )

    print("\n" + "=" * 60)
    print("STEP 1: Schema + Record Count")
    print("=" * 60)
    models_df.printSchema()
    print(f"Total models: {models_df.count()}")

    # ----------------------------------------------------------------
    # STEP 2: Null analysis
    #
    # In real data, nulls are everywhere. Before any transformation
    # you need to know which columns have nulls and how many.
    # F.col().isNull() returns a boolean column — sum() counts Trues.
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 2: Null Count Per Column")
    print("=" * 60)
    models_df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in models_df.columns
    ]).show()

    # ----------------------------------------------------------------
    # STEP 3: dropDuplicates()
    #
    # Our ingester already deduplicates, but Spark DataFrames can still
    # have duplicates if a model appears in multiple pipeline categories.
    # dropDuplicates(['model_id']) keeps the first occurrence.
    # ----------------------------------------------------------------
    deduped_df = models_df.dropDuplicates(["model_id"])
    print(f"\nSTEP 3: After deduplication: {deduped_df.count()} unique models")

    # ----------------------------------------------------------------
    # STEP 4: Top models per pipeline type using Window Functions
    #
    # Window functions compute a value for each row RELATIVE to a group
    # of rows — without collapsing the group into one row like groupBy does.
    #
    # Here: rank each model within its pipeline_tag group by downloads.
    # rank() assigns 1 to the highest downloads in each pipeline group.
    #
    # partitionBy("pipeline_tag") = "restart the ranking for each category"
    # orderBy(downloads.desc())   = "rank by downloads, highest = rank 1"
    # ----------------------------------------------------------------
    window_spec = Window.partitionBy("pipeline_tag").orderBy(F.col("downloads").desc())

    ranked_df = models_df.withColumn("rank", F.rank().over(window_spec))

    print("\n" + "=" * 60)
    print("STEP 4: Top 3 Models Per Pipeline Type (Window Function)")
    print("=" * 60)
    ranked_df.filter(F.col("rank") <= 3) \
             .select("pipeline_tag", "rank", "model_id", "downloads", "likes") \
             .orderBy("pipeline_tag", "rank") \
             .show(30, truncate=False)

    # ----------------------------------------------------------------
    # STEP 5: explode() on the tags array
    #
    # Each model has a tags array like ["pytorch", "transformers", "en"].
    # explode() turns one row with 5 tags into 5 rows, one per tag.
    # Then we count how often each tag appears across all models.
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 5: Most Common Tags Across All Models")
    print("=" * 60)
    models_df.select(F.explode("tags").alias("tag")) \
             .groupBy("tag") \
             .count() \
             .orderBy(F.col("count").desc()) \
             .show(15, truncate=False)

    # ----------------------------------------------------------------
    # STEP 6: Author leaderboard
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 6: Top Authors by Total Downloads")
    print("=" * 60)
    models_df.groupBy("author") \
             .agg(
                 F.count("*").alias("model_count"),
                 F.sum("downloads").alias("total_downloads"),
                 F.sum("likes").alias("total_likes")
             ) \
             .orderBy(F.col("total_downloads").desc()) \
             .show(10, truncate=False)

    spark.stop()
    print("\n[OK] HuggingFace exploration complete.")


if __name__ == "__main__":
    run()
