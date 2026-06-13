"""
PostgreSQL Gold Layer Loader.

Reads each Gold Parquet table and loads it into PostgreSQL
using pandas + psycopg2 (SQLAlchemy engine).

Why pandas instead of Spark JDBC for loading?
Spark JDBC requires a running Spark cluster and the PostgreSQL JDBC jar.
For small Gold tables (< 10K rows), pandas.to_sql() is simpler, faster
to set up, and requires no JVM. We use Spark for heavy transformation
(Silver, Gold computation) and pandas for the final small-table write.

In production with millions of rows you'd use Spark JDBC or COPY FROM STDIN.
For this portfolio project pandas is the right tool — don't over-engineer.

Loading strategy: TRUNCATE then INSERT
Why not upsert (INSERT ON CONFLICT)?
Gold tables are fully recomputed daily. The simplest and most reliable
strategy is to clear the table and reload it completely. This guarantees
consistency — no stale rows, no partial updates.
"""

import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from loguru import logger
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=True)

# ----------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB   = os.getenv("POSTGRES_DB", "ai_platform")
POSTGRES_USER = os.getenv("POSTGRES_USER", "platform")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "platform123")

CONN_STR = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
SCHEMA   = "ai_platform"

DATA_LAKE = Path(__file__).parent.parent / "data_lake"
GOLD_BASE = DATA_LAKE / "gold"

# ----------------------------------------------------------------
# Map: Gold Parquet path → PostgreSQL table name
# Order matters — fact_executive_kpis loads last (depends on all sources)
# ----------------------------------------------------------------
LOAD_MAP = [
    # GitHub
    (GOLD_BASE / "github_trends" / "top_repos",       "fact_github_trends"),
    (GOLD_BASE / "github_trends" / "topic_trends",    "fact_topic_trends"),
    (GOLD_BASE / "github_trends" / "org_leaderboard", "fact_org_leaderboard"),
    # HuggingFace
    (GOLD_BASE / "ai_models" / "top_models",          "fact_model_metrics"),
    (GOLD_BASE / "ai_models" / "author_leaderboard",  "fact_author_leaderboard"),
    (GOLD_BASE / "ai_models" / "pipeline_summary",    "fact_pipeline_summary"),
    # arXiv
    (GOLD_BASE / "research_trends" / "category_trends",   "fact_research_trends"),
    (GOLD_BASE / "research_trends" / "prolific_authors",   "fact_author_metrics"),
    (GOLD_BASE / "research_trends" / "cross_disciplinary", "fact_cross_disciplinary"),
    # Executive KPIs
    (GOLD_BASE / "executive_metrics" / "kpis",        "fact_executive_kpis"),
]


def get_engine():
    """
    Creates a SQLAlchemy engine (connection pool).

    pool_pre_ping=True tests the connection before using it —
    avoids "SSL connection has been closed unexpectedly" errors
    that happen when a connection sits idle for too long.
    """
    return create_engine(CONN_STR, pool_pre_ping=True)


def read_parquet(path: Path) -> pd.DataFrame:
    """
    Reads a Gold Parquet directory into a pandas DataFrame.

    pyarrow.parquet.read_table handles both:
      - Single parquet files
      - Directories with multiple part-*.parquet files
    It returns an Arrow table which we convert to pandas.

    Why not pd.read_parquet() directly?
    pyarrow gives us more control over schema handling and
    handles the Spark-generated _SUCCESS and .crc files gracefully.
    """
    table = pq.read_table(str(path))
    df = table.to_pandas()
    logger.info(f"Read {len(df)} rows from {path.name}")
    return df


def clean_for_postgres(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    # Drop Spark internal columns
    df = df.drop(columns=[c for c in ["__null_dask_index__"] if c in df.columns])

    # Normalize column names
    df.columns = [c.lower().strip() for c in df.columns]

    # Convert batch_date string to actual date
    if "batch_date" in df.columns:
        df["batch_date"] = pd.to_datetime(df["batch_date"]).dt.date

    # Strip timezone from all timestamp columns
    for col in df.select_dtypes(include=["datetime64[ns, UTC]", "datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

    # Convert numpy arrays to Python lists so psycopg2 can adapt them
    import numpy as np
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: x.tolist() if isinstance(x, np.ndarray) else x
            )

    # Drop gold_created_at — not in our schema
    df = df.drop(columns=["gold_created_at"], errors="ignore")

    return df


def load_table(engine, df: pd.DataFrame, table_name: str) -> int:
    """
    Truncates then bulk-inserts using psycopg2 directly.
    Bypasses pandas to_sql to avoid SQLAlchemy version conflicts.
    """
    full_table = f"{SCHEMA}.{table_name}"
    import psycopg2
    import io

    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS,
    )
    cur = conn.cursor()

    # Truncate
    cur.execute(f"TRUNCATE TABLE {full_table} RESTART IDENTITY CASCADE")

    # Convert lists/arrays in any column to strings for PostgreSQL TEXT[]
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: '{' + ','.join(f'"{v}"' for v in x) + '}'
                if isinstance(x, (list, )) else x
            )

    # Use COPY for fast bulk insert via in-memory CSV buffer
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep='')
    buffer.seek(0)

    cur.copy_expert(
        f"COPY {full_table} ({','.join(df.columns)}) FROM STDIN WITH (FORMAT CSV, NULL '')",
        buffer
    )

    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"[OK] Loaded {len(df)} rows → {full_table}")
    return len(df)


def run_all() -> dict:
    """
    Loads all Gold tables into PostgreSQL.
    Returns a summary dict with row counts per table.
    """
    engine = get_engine()
    summary = {}
    failed = []

    logger.info(f"Starting PostgreSQL load | {datetime.utcnow().isoformat()}")
    logger.info(f"Target: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

    for parquet_path, table_name in LOAD_MAP:
        try:
            if not parquet_path.exists():
                logger.warning(f"Gold path not found, skipping: {parquet_path}")
                continue

            df = read_parquet(parquet_path)
            df = clean_for_postgres(df, table_name)
            count = load_table(engine, df, table_name)
            summary[table_name] = {"status": "success", "rows": count}

        except Exception as e:
            logger.error(f"[FAILED] Failed loading {table_name}: {e}")
            summary[table_name] = {"status": "failed", "error": str(e)}
            failed.append(table_name)

    # Print summary
    logger.info("\n" + "=" * 55)
    logger.info("POSTGRESQL LOAD SUMMARY")
    logger.info("=" * 55)
    for table, result in summary.items():
        if result["status"] == "success":
            logger.info(f"  [OK] {table:<35} {result['rows']} rows")
        else:
            logger.error(f"  [FAILED] {table:<35} {result['error']}")
    logger.info("=" * 55)

    if failed:
        raise RuntimeError(f"Failed tables: {failed}")

    return summary


if __name__ == "__main__":
    run_all()
