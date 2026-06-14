"""
AI Ecosystem Data Platform — Streamlit Dashboard (Phase 8)

4 tabs:
  1. Executive KPIs  — headline numbers across all sources
  2. GitHub          — repo trends, language distribution, topics, org rankings
  3. HuggingFace     — model rankings, pipeline share, author leaderboard
  4. arXiv           — research category trends, prolific authors, cross-disciplinary

Data comes exclusively from queries.py — no SQL in this file.
All charts use Plotly for interactive exploration.
@st.cache_data(ttl=300) caches each query for 5 minutes so refreshing
the page doesn't hit PostgreSQL on every interaction.
"""

import sys
from pathlib import Path

# When running locally: add project root so `postgres.queries` is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from postgres import queries

# ----------------------------------------------------------------
# Page config
# ----------------------------------------------------------------
st.set_page_config(
    page_title="AI Ecosystem Platform",
    page_icon="AI",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ----------------------------------------------------------------
# Cached data loaders — TTL 5 minutes
# ----------------------------------------------------------------
@st.cache_data(ttl=300)
def load_kpis():
    return queries.get_latest_kpis()

@st.cache_data(ttl=300)
def load_top_repos(limit=25):
    return queries.get_top_repos(limit)

@st.cache_data(ttl=300)
def load_language_dist():
    return queries.get_language_distribution()

@st.cache_data(ttl=300)
def load_top_topics(limit=20):
    return queries.get_top_topics(limit)

@st.cache_data(ttl=300)
def load_org_leaderboard(limit=15):
    return queries.get_org_leaderboard(limit)

@st.cache_data(ttl=300)
def load_top_models(pipeline=None, limit=10):
    return queries.get_top_models(pipeline, limit)

@st.cache_data(ttl=300)
def load_pipeline_summary():
    return queries.get_pipeline_summary()

@st.cache_data(ttl=300)
def load_author_leaderboard(limit=15):
    return queries.get_author_leaderboard(limit)

@st.cache_data(ttl=300)
def load_research_trends():
    return queries.get_research_trends()

@st.cache_data(ttl=300)
def load_prolific_authors(limit=15):
    return queries.get_prolific_authors(limit)

@st.cache_data(ttl=300)
def load_cross_disciplinary(limit=15):
    return queries.get_cross_disciplinary(limit)


def fmt(n, suffix="") -> str:
    """Format large numbers with K/M suffix for KPI cards."""
    if n is None:
        return "—"
    n = int(n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M{suffix}"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K{suffix}"
    return f"{n}{suffix}"


# ----------------------------------------------------------------
# Try connecting — show friendly error if DB is down
# ----------------------------------------------------------------
try:
    kpis = load_kpis()
    db_ok = not kpis.empty
except Exception as e:
    st.error(f"[WARNING] Cannot connect to PostgreSQL: {e}")
    st.info("Run `python postgres/setup_db.py` then `python postgres/loader.py` to populate data.")
    st.stop()

if not db_ok:
    st.warning("No data found. Run the pipeline first: `python postgres/loader.py`")
    st.stop()

# ----------------------------------------------------------------
# Header
# ----------------------------------------------------------------
st.title("AI Ecosystem Data Platform")
batch_date = kpis.get("batch_date", "—")
st.caption(f"Data as of: **{batch_date}**  ·  Refreshes every 5 minutes")

# ----------------------------------------------------------------
# Tabs
# ----------------------------------------------------------------
tab_exec, tab_github, tab_hf, tab_arxiv = st.tabs([
    "[KPI] Executive KPIs",
    "GitHub GitHub",
    "HuggingFace HuggingFace",
    "arXiv arXiv Research",
])


# ================================================================
# TAB 1 — EXECUTIVE KPIs
# ================================================================
with tab_exec:
    st.subheader("Platform Overview")

    # Row 1 — GitHub
    st.markdown("#### GitHub")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Repositories", fmt(kpis.get("total_repos")))
    c2.metric("Total Stars", fmt(kpis.get("total_stars")))
    c3.metric("Total Forks", fmt(kpis.get("total_forks")))
    c4.metric("Languages", fmt(kpis.get("unique_languages")))
    c5.metric("Organizations", fmt(kpis.get("unique_owners")))

    top_repo = kpis.get("top_repo_name", "—")
    top_repo_stars = fmt(kpis.get("top_repo_stars"))
    st.info(f"Top Top repo: **{top_repo}** — {top_repo_stars} stars")

    st.divider()

    # Row 2 — HuggingFace
    st.markdown("#### HuggingFace")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Models", fmt(kpis.get("total_models")))
    c2.metric("Total Downloads", fmt(kpis.get("total_downloads")))
    c3.metric("Total Likes", fmt(kpis.get("total_likes")))
    c4.metric("Authors", fmt(kpis.get("unique_authors")))
    c5.metric("Pipeline Types", fmt(kpis.get("unique_pipeline_types")))

    top_model = kpis.get("top_model_id", "—")
    top_model_dl = fmt(kpis.get("top_model_downloads_val"))
    st.info(f"Top Top model: **{top_model}** — {top_model_dl} downloads")

    st.divider()

    # Row 3 — arXiv
    st.markdown("#### arXiv Research")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Papers", fmt(kpis.get("total_papers")))
    avg_authors = kpis.get("avg_authors_per_paper")
    c2.metric("Avg Authors / Paper", f"{avg_authors:.1f}" if avg_authors else "—")
    c3.metric("Research Categories", fmt(kpis.get("unique_categories")))
    c4.metric("Cross-Disciplinary", fmt(kpis.get("cross_disciplinary_papers")))

    top_cat = kpis.get("top_research_category", "—")
    st.info(f"Top Most active category: **{top_cat}**")


# ================================================================
# TAB 2 — GITHUB
# ================================================================
with tab_github:

    # ── Top Repos table ──────────────────────────────────────────
    st.subheader("Top Repositories by Engagement")
    repos_df = load_top_repos(25)
    if not repos_df.empty:
        fig = px.bar(
            repos_df.head(20),
            x="engagement_score",
            y="full_name",
            orientation="h",
            color="language",
            hover_data=["stars", "forks", "topics_count"],
            labels={"engagement_score": "Engagement Score", "full_name": "Repository"},
            height=600,
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("View raw data"):
            st.dataframe(
                repos_df[["engagement_rank", "full_name", "owner", "language",
                           "stars", "forks", "engagement_score", "topics_count"]],
                use_container_width=True,
            )

    st.divider()

    col_left, col_right = st.columns(2)

    # ── Language Distribution ─────────────────────────────────────
    with col_left:
        st.subheader("Language Distribution")
        lang_df = load_language_dist()
        if not lang_df.empty:
            top_langs = lang_df.head(10)
            fig = px.pie(
                top_langs,
                names="language",
                values="repo_count",
                hole=0.4,
                hover_data=["total_stars"],
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

    # ── Org Leaderboard ───────────────────────────────────────────
    with col_right:
        st.subheader("Organization Leaderboard")
        org_df = load_org_leaderboard(15)
        if not org_df.empty:
            fig = px.bar(
                org_df,
                x="total_stars",
                y="organization",
                orientation="h",
                color="repo_count",
                color_continuous_scale="Blues",
                labels={"total_stars": "Total Stars", "organization": "Organization"},
                hover_data=["repo_count", "total_forks"],
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Topic Trends ──────────────────────────────────────────────
    st.subheader("Trending Topics")
    topics_df = load_top_topics(20)
    if not topics_df.empty:
        fig = px.treemap(
            topics_df,
            path=["topic"],
            values="repo_count",
            color="total_stars",
            color_continuous_scale="Viridis",
            hover_data=["avg_stars_per_repo", "topic_rank"],
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


# ================================================================
# TAB 3 — HUGGINGFACE
# ================================================================
with tab_hf:

    # ── Pipeline Share ─────────────────────────────────────────────
    st.subheader("Download Share by AI Task Category")
    pipeline_df = load_pipeline_summary()
    if not pipeline_df.empty:
        col_left, col_right = st.columns(2)

        with col_left:
            fig = px.pie(
                pipeline_df,
                names="pipeline_tag",
                values="total_downloads",
                hole=0.4,
                hover_data=["model_count"],
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            fig = px.bar(
                pipeline_df,
                x="pipeline_tag",
                y="model_count",
                color="total_downloads",
                color_continuous_scale="Oranges",
                labels={"pipeline_tag": "Pipeline Type", "model_count": "Model Count"},
            )
            fig.update_layout(xaxis_tickangle=-35)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Top Models ────────────────────────────────────────────────
    st.subheader("Top Models")
    pipeline_options = ["All"] + (
        sorted(pipeline_df["pipeline_tag"].tolist()) if not pipeline_df.empty else []
    )
    selected_pipeline = st.selectbox("Filter by pipeline type", pipeline_options)

    models_df = load_top_models(
        pipeline=None if selected_pipeline == "All" else selected_pipeline,
        limit=20,
    )
    if not models_df.empty:
        fig = px.bar(
            models_df,
            x="downloads",
            y="model_id",
            orientation="h",
            color="pipeline_tag",
            hover_data=["author", "likes", "global_rank"],
            labels={"downloads": "Downloads", "model_id": "Model"},
            height=500,
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Author Leaderboard ────────────────────────────────────────
    st.subheader("Author / Organization Leaderboard")
    author_df = load_author_leaderboard(15)
    if not author_df.empty:
        fig = px.scatter(
            author_df,
            x="total_downloads",
            y="total_likes",
            size="model_count",
            color="influence_score",
            hover_name="author",
            hover_data=["model_count", "download_rank"],
            color_continuous_scale="Plasma",
            labels={"total_downloads": "Total Downloads", "total_likes": "Total Likes"},
            size_max=40,
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("View raw data"):
            st.dataframe(
                author_df[["download_rank", "author", "model_count",
                            "total_downloads", "total_likes", "influence_score"]],
                use_container_width=True,
            )


# ================================================================
# TAB 4 — arXiv RESEARCH
# ================================================================
with tab_arxiv:

    # ── Category Activity ─────────────────────────────────────────
    st.subheader("Research Activity by Category")
    research_df = load_research_trends()
    if not research_df.empty:
        col_left, col_right = st.columns(2)

        with col_left:
            fig = px.bar(
                research_df,
                x="paper_count",
                y="category_label",
                orientation="h",
                color="cross_disciplinary_pct",
                color_continuous_scale="Teal",
                hover_data=["avg_authors_per_paper", "cross_disciplinary_pct"],
                labels={"paper_count": "Paper Count", "category_label": "Category"},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            fig = px.scatter(
                research_df,
                x="paper_count",
                y="cross_disciplinary_pct",
                size="paper_count",
                color="category_label",
                hover_name="category_label",
                hover_data=["avg_authors_per_paper"],
                labels={
                    "paper_count": "Paper Count",
                    "cross_disciplinary_pct": "Cross-Disciplinary %",
                },
                size_max=50,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    col_left, col_right = st.columns(2)

    # ── Prolific Authors ──────────────────────────────────────────
    with col_left:
        st.subheader("Most Prolific Authors")
        prolific_df = load_prolific_authors(15)
        if not prolific_df.empty:
            fig = px.bar(
                prolific_df,
                x="paper_count",
                y="author",
                orientation="h",
                color="category_breadth",
                color_continuous_scale="Magma",
                hover_data=["category_breadth", "categories_str"],
                labels={"paper_count": "Paper Count", "author": "Author"},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    # ── Cross-Disciplinary Research ───────────────────────────────
    with col_right:
        st.subheader("Cross-Disciplinary Research")
        cross_df = load_cross_disciplinary(15)
        if not cross_df.empty:
            fig = px.bar(
                cross_df,
                x="paper_count",
                y="category_combo",
                orientation="h",
                color="avg_authors",
                color_continuous_scale="Cividis",
                hover_data=["avg_authors", "combo_rank"],
                labels={"paper_count": "Paper Count", "category_combo": "Category Combination"},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)
