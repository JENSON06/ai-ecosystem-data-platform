"""
Tests for postgres/loader.py

What we test (pure data-transform functions only — no DB connection needed):
  - clean_for_postgres() normalises column names to lowercase
  - clean_for_postgres() converts batch_date strings to date objects
  - clean_for_postgres() strips timezone info from timestamp columns
  - clean_for_postgres() converts numpy arrays to Python lists
  - clean_for_postgres() drops the gold_created_at column
  - read_parquet() reads a real Parquet file and returns a DataFrame
"""

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import loader and queries directly to bypass postgres/__init__.py
# which eagerly pulls in sqlalchemy and psycopg2
import importlib.util as _ilu

def _load(name, rel_path):
    spec = _ilu.spec_from_file_location(name, Path(__file__).parent.parent / rel_path)
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_loader_mod = _load("postgres.loader", "postgres/loader.py")
clean_for_postgres = _loader_mod.clean_for_postgres
read_parquet = _loader_mod.read_parquet


# ----------------------------------------------------------------
# clean_for_postgres()
# ----------------------------------------------------------------
class TestCleanForPostgres:
    def test_lowercases_column_names(self):
        df = pd.DataFrame({"FULL_NAME": ["repo/a"], "Stars": [100]})
        result = clean_for_postgres(df, "fact_github_trends")
        assert list(result.columns) == ["full_name", "stars"]

    def test_strips_whitespace_from_column_names(self):
        df = pd.DataFrame({" repo_name ": ["x"], "stars ": [1]})
        result = clean_for_postgres(df, "fact_github_trends")
        assert "repo_name" in result.columns
        assert "stars" in result.columns

    def test_converts_batch_date_string_to_date(self):
        df = pd.DataFrame({"batch_date": ["2024-01-15"]})
        result = clean_for_postgres(df, "fact_github_trends")
        assert result["batch_date"].iloc[0] == date(2024, 1, 15)

    def test_strips_utc_timezone_from_timestamp(self):
        df = pd.DataFrame({
            "inserted_at": pd.to_datetime(["2024-01-15T10:00:00"]).tz_localize("UTC")
        })
        result = clean_for_postgres(df, "fact_github_trends")
        assert result["inserted_at"].dt.tz is None

    def test_converts_numpy_array_to_list(self):
        df = pd.DataFrame({"tags": [np.array(["a", "b", "c"])]})
        result = clean_for_postgres(df, "fact_author_metrics")
        val = result["tags"].iloc[0]
        assert isinstance(val, list)
        assert val == ["a", "b", "c"]

    def test_drops_gold_created_at(self):
        df = pd.DataFrame({
            "model_id": ["org/model"],
            "gold_created_at": pd.to_datetime(["2024-01-15"]),
        })
        result = clean_for_postgres(df, "fact_model_metrics")
        assert "gold_created_at" not in result.columns

    def test_no_error_when_gold_created_at_absent(self):
        df = pd.DataFrame({"model_id": ["org/model"], "downloads": [1000]})
        result = clean_for_postgres(df, "fact_model_metrics")
        assert "model_id" in result.columns

    def test_leaves_regular_values_unchanged(self):
        df = pd.DataFrame({"stars": [500], "full_name": ["owner/repo"]})
        result = clean_for_postgres(df, "fact_github_trends")
        assert result["stars"].iloc[0] == 500
        assert result["full_name"].iloc[0] == "owner/repo"


# ----------------------------------------------------------------
# read_parquet()
# ----------------------------------------------------------------
class TestReadParquet:
    def test_reads_parquet_directory(self, tmp_path):
        # Write a small parquet file into a directory (mimics Spark output)
        parquet_dir = tmp_path / "top_repos"
        parquet_dir.mkdir()
        table = pa.table({"full_name": ["owner/repo"], "stars": [1000]})
        pq.write_table(table, parquet_dir / "part-0.parquet")

        df = read_parquet(parquet_dir)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert "full_name" in df.columns

    def test_reads_multiple_part_files(self, tmp_path):
        parquet_dir = tmp_path / "top_repos"
        parquet_dir.mkdir()
        table = pa.table({"stars": [100, 200]})
        pq.write_table(table, parquet_dir / "part-0.parquet")
        pq.write_table(table, parquet_dir / "part-1.parquet")

        df = read_parquet(parquet_dir)
        assert len(df) == 4

    def test_returns_correct_row_count(self, tmp_path):
        parquet_dir = tmp_path / "kpis"
        parquet_dir.mkdir()
        table = pa.table({"total_repos": [500], "total_models": [200]})
        pq.write_table(table, parquet_dir / "part-0.parquet")

        df = read_parquet(parquet_dir)
        assert len(df) == 1
        assert df["total_repos"].iloc[0] == 500
