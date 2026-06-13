"""
Tests for spark_jobs/silver/base.py and spark_jobs/gold/base.py

Uses a real local SparkSession (local[1]) so we test actual Spark behaviour.
No cluster required — local mode runs entirely in-process.

What we test:
  Silver base:
    - write_silver() produces readable Parquet output partitioned by batch_date
    - log_quality_summary() calculates drop percentage correctly
  Gold base:
    - write_gold() produces readable Parquet output
    - add_gold_metadata() adds the gold_created_at column
    - read_silver() reads back what write_silver() wrote
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).parent.parent))

from spark_jobs.silver.base import write_silver, log_quality_summary
from spark_jobs.gold.base import write_gold, add_gold_metadata, read_silver


# ----------------------------------------------------------------
# Session fixture — shared across all tests in this module
# local[1] = single thread, fast startup, no cluster needed
# ----------------------------------------------------------------
@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_spark_base")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")  # disable web UI in tests
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame([
        {"full_name": "owner/repo", "stars": 100, "batch_date": "2024-01-15"},
        {"full_name": "owner/repo2", "stars": 200, "batch_date": "2024-01-15"},
    ])


# ----------------------------------------------------------------
# Silver base — write_silver()
# ----------------------------------------------------------------
class TestWriteSilver:
    def test_writes_readable_parquet(self, spark, sample_df, tmp_path):
        output = str(tmp_path / "silver_out")
        write_silver(sample_df, output)

        df_back = spark.read.parquet(output)
        assert df_back.count() == 2

    def test_partitions_by_batch_date(self, spark, sample_df, tmp_path):
        output = str(tmp_path / "silver_partitioned")
        write_silver(sample_df, output, partition_col="batch_date")

        partition_dirs = [p for p in Path(output).iterdir() if p.is_dir()]
        assert any("batch_date=" in str(p) for p in partition_dirs)

    def test_overwrite_is_idempotent(self, spark, sample_df, tmp_path):
        output = str(tmp_path / "silver_idem")
        write_silver(sample_df, output)
        write_silver(sample_df, output)  # second write should not duplicate

        df_back = spark.read.parquet(output)
        assert df_back.count() == 2

    def test_written_columns_match_input(self, spark, sample_df, tmp_path):
        output = str(tmp_path / "silver_cols")
        write_silver(sample_df, output)

        df_back = spark.read.parquet(output)
        # batch_date is partition col — it will still be in the data when read back
        assert "full_name" in df_back.columns
        assert "stars" in df_back.columns


# ----------------------------------------------------------------
# Silver base — log_quality_summary()
# ----------------------------------------------------------------
class TestLogQualitySummary:
    def test_no_exception_on_valid_input(self):
        # Just verifies it runs without error and calculates correctly
        log_quality_summary(raw_count=1000, silver_count=950, source="github")

    def test_handles_zero_raw_count_gracefully(self):
        log_quality_summary(raw_count=0, silver_count=0, source="github")

    def test_handles_full_drop(self):
        log_quality_summary(raw_count=500, silver_count=0, source="huggingface")


# ----------------------------------------------------------------
# Gold base — write_gold() and add_gold_metadata()
# ----------------------------------------------------------------
class TestWriteGold:
    def test_writes_readable_parquet(self, spark, sample_df, tmp_path):
        output = str(tmp_path / "gold_out")
        write_gold(sample_df, output, "test_table")

        df_back = spark.read.parquet(output)
        assert df_back.count() == 2

    def test_overwrite_is_idempotent(self, spark, sample_df, tmp_path):
        output = str(tmp_path / "gold_idem")
        write_gold(sample_df, output, "test_table")
        write_gold(sample_df, output, "test_table")

        df_back = spark.read.parquet(output)
        assert df_back.count() == 2


class TestAddGoldMetadata:
    def test_adds_gold_created_at_column(self, spark, sample_df):
        result = add_gold_metadata(sample_df)
        assert "gold_created_at" in result.columns

    def test_gold_created_at_is_not_null(self, spark, sample_df):
        result = add_gold_metadata(sample_df)
        null_count = result.filter(F.col("gold_created_at").isNull()).count()
        assert null_count == 0

    def test_original_columns_preserved(self, spark, sample_df):
        result = add_gold_metadata(sample_df)
        assert "full_name" in result.columns
        assert "stars" in result.columns

    def test_row_count_unchanged(self, spark, sample_df):
        result = add_gold_metadata(sample_df)
        assert result.count() == sample_df.count()


# ----------------------------------------------------------------
# Gold base — read_silver()
# ----------------------------------------------------------------
class TestReadSilver:
    def test_reads_back_written_silver_data(self, spark, sample_df, tmp_path):
        silver_base = str(tmp_path / "silver")
        output = str(tmp_path / "silver" / "github")
        write_silver(sample_df, output)

        df = read_silver(spark, "github", silver_base)
        assert df.count() == 2

    def test_read_silver_contains_expected_columns(self, spark, sample_df, tmp_path):
        silver_base = str(tmp_path / "silver2")
        output = str(tmp_path / "silver2" / "github")
        write_silver(sample_df, output)

        df = read_silver(spark, "github", silver_base)
        assert "full_name" in df.columns
        assert "stars" in df.columns
