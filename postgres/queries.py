"""
Reusable query functions for the Streamlit dashboard.

Why centralize queries here instead of writing SQL in the dashboard?
1. The dashboard code stays clean — Python only, no SQL strings
2. Queries are testable independently
3. One place to optimize slow queries
4. Easy to add caching (e.g. @st.cache_data) in the dashboard layer
"""

import os
from pathlib import Path
from functools import lru_cache

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=True)

CONN_STR = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'platform')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'platform123')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'ai_platform')}"
)


@lru_cache(maxsize=1)
def _engine():
    return create_engine(CONN_STR, pool_pre_ping=True)


def _query(sql: str, params: dict = None) -> pd.DataFrame:
    with _engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# ----------------------------------------------------------------
# Executive KPIs
# ----------------------------------------------------------------
def get_latest_kpis() -> pd.Series:
    """Returns the most recent KPI row as a pandas Series."""
    df = _query("""
        SELECT * FROM ai_platform.fact_executive_kpis
        ORDER BY batch_date DESC LIMIT 1
    """)
    return df.iloc[0] if not df.empty else pd.Series()


# ----------------------------------------------------------------
# GitHub queries
# ----------------------------------------------------------------
def get_top_repos(limit: int = 20) -> pd.DataFrame:
    return _query("""
        SELECT full_name, owner, language, stars, forks,
               engagement_score, engagement_rank, topics_count
        FROM ai_platform.fact_github_trends
        ORDER BY engagement_rank ASC
        LIMIT :limit
    """, {"limit": limit})


def get_language_distribution() -> pd.DataFrame:
    return _query("""
        SELECT language, COUNT(*) AS repo_count, SUM(stars) AS total_stars
        FROM ai_platform.fact_github_trends
        WHERE language IS NOT NULL AND language != 'unknown'
        GROUP BY language
        ORDER BY repo_count DESC
    """)


def get_top_topics(limit: int = 20) -> pd.DataFrame:
    return _query("""
        SELECT topic, repo_count, total_stars, avg_stars_per_repo, topic_rank
        FROM ai_platform.fact_topic_trends
        ORDER BY topic_rank ASC
        LIMIT :limit
    """, {"limit": limit})


def get_org_leaderboard(limit: int = 15) -> pd.DataFrame:
    return _query("""
        SELECT organization, repo_count, total_stars, total_forks, org_rank
        FROM ai_platform.fact_org_leaderboard
        ORDER BY org_rank ASC
        LIMIT :limit
    """, {"limit": limit})


# ----------------------------------------------------------------
# HuggingFace queries
# ----------------------------------------------------------------
def get_top_models(pipeline: str = None, limit: int = 10) -> pd.DataFrame:
    if pipeline:
        return _query("""
            SELECT model_id, author, pipeline_tag, downloads, likes,
                   rank_in_category, global_rank
            FROM ai_platform.fact_model_metrics
            WHERE pipeline_tag = :pipeline
            ORDER BY rank_in_category ASC
            LIMIT :limit
        """, {"pipeline": pipeline, "limit": limit})
    return _query("""
        SELECT model_id, author, pipeline_tag, downloads, likes,
               rank_in_category, global_rank
        FROM ai_platform.fact_model_metrics
        ORDER BY global_rank ASC
        LIMIT :limit
    """, {"limit": limit})


def get_pipeline_summary() -> pd.DataFrame:
    return _query("""
        SELECT pipeline_tag, model_count, total_downloads,
               download_share_pct, category_rank
        FROM ai_platform.fact_pipeline_summary
        ORDER BY category_rank ASC
    """)


def get_author_leaderboard(limit: int = 15) -> pd.DataFrame:
    return _query("""
        SELECT author, model_count, total_downloads, total_likes,
               influence_score, download_rank
        FROM ai_platform.fact_author_leaderboard
        ORDER BY download_rank ASC
        LIMIT :limit
    """, {"limit": limit})


# ----------------------------------------------------------------
# arXiv queries
# ----------------------------------------------------------------
def get_research_trends() -> pd.DataFrame:
    return _query("""
        SELECT primary_category, category_label, paper_count,
               avg_authors_per_paper, cross_disciplinary_pct, activity_rank
        FROM ai_platform.fact_research_trends
        ORDER BY activity_rank ASC
    """)


def get_prolific_authors(limit: int = 15) -> pd.DataFrame:
    return _query("""
        SELECT author, paper_count, category_breadth,
               categories_str, author_rank
        FROM ai_platform.fact_author_metrics
        ORDER BY author_rank ASC
        LIMIT :limit
    """, {"limit": limit})


def get_cross_disciplinary(limit: int = 15) -> pd.DataFrame:
    return _query("""
        SELECT category_combo, paper_count, avg_authors, combo_rank
        FROM ai_platform.fact_cross_disciplinary
        ORDER BY combo_rank ASC
        LIMIT :limit
    """, {"limit": limit})
