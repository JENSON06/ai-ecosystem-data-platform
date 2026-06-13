"""
Database setup script — run once before the first pipeline execution.

Creates:
  1. The `platform` user (if not exists)
  2. The `ai_platform` database (if not exists)
  3. All tables from schema.sql

Why a separate setup script?
In production, database setup is a one-time operation done by a DBA
or infrastructure automation (Terraform, Ansible). Mixing it into the
daily pipeline would try to CREATE DATABASE on every run — which fails
if the DB already exists. Separation of concerns keeps the daily
pipeline clean.
"""

import os
import sys
from pathlib import Path
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from loguru import logger
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=True)

# Connect as the default postgres superuser to create DB and user
ADMIN_CONN = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   "postgres",       # always exists
    "user":     os.getenv("POSTGRES_ADMIN_USER", "postgres"),
    "password": os.getenv("POSTGRES_ADMIN_PASSWORD", ""),
}

APP_DB   = os.getenv("POSTGRES_DB",       "ai_platform")
APP_USER = os.getenv("POSTGRES_USER",     "platform")
APP_PASS = os.getenv("POSTGRES_PASSWORD", "platform123")

SCHEMA_FILE = Path(__file__).parent / "schema.sql"


def create_db_and_user():
    """
    Creates the application database and user.
    Uses ISOLATION_LEVEL_AUTOCOMMIT because CREATE DATABASE
    cannot run inside a transaction block in PostgreSQL.
    """
    try:
        conn = psycopg2.connect(**ADMIN_CONN)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Create user if not exists
        cur.execute(f"""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT FROM pg_catalog.pg_roles WHERE rolname = '{APP_USER}'
                ) THEN
                    CREATE USER {APP_USER} WITH PASSWORD '{APP_PASS}';
                END IF;
            END $$;
        """)
        logger.info(f"[OK] User '{APP_USER}' ready")

        # Create database if not exists
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{APP_DB}'")
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {APP_DB} OWNER {APP_USER}")
            logger.info(f"[OK] Database '{APP_DB}' created")
        else:
            logger.info(f"[INFO]  Database '{APP_DB}' already exists")

        cur.close()
        conn.close()
    except psycopg2.OperationalError as e:
        logger.warning(f"Could not connect as admin (postgres user): {e}")
        logger.info("Assuming database and user already exist, continuing...")


def apply_schema():
    """
    Applies schema.sql to the ai_platform database.
    This creates all tables, indexes, and grants.
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=APP_DB,
        user=APP_USER,
        password=APP_PASS,
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    schema_sql = SCHEMA_FILE.read_text()
    cur = conn.cursor()
    cur.execute(schema_sql)
    cur.close()
    conn.close()
    logger.info("[OK] Schema applied successfully")


def verify_tables():
    """
    Verifies that all expected tables were created.
    """
    expected = [
        "dim_repositories", "dim_models",
        "fact_github_trends", "fact_topic_trends", "fact_org_leaderboard",
        "fact_model_metrics", "fact_author_leaderboard", "fact_pipeline_summary",
        "fact_research_trends", "fact_author_metrics", "fact_cross_disciplinary",
        "fact_executive_kpis",
    ]

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=APP_DB,
        user=APP_USER,
        password=APP_PASS,
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'ai_platform'
        ORDER BY table_name
    """)
    existing = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()

    missing = [t for t in expected if t not in existing]
    if missing:
        logger.error(f"Missing tables: {missing}")
        sys.exit(1)

    logger.info(f"[OK] All {len(existing)} tables verified:")
    for t in sorted(existing):
        logger.info(f"   ai_platform.{t}")


if __name__ == "__main__":
    logger.info("Setting up AI Platform database...")
    create_db_and_user()
    apply_schema()
    verify_tables()
    logger.info("[OK] Database setup complete.")
