-- =============================================================
-- AI Ecosystem Data Platform — PostgreSQL Schema
-- Star Schema design for the serving layer
-- =============================================================

-- Drop schema and recreate cleanly on each run
-- In production you'd use migrations (Flyway/Liquibase) instead
DROP SCHEMA IF EXISTS ai_platform CASCADE;
CREATE SCHEMA ai_platform;
SET search_path TO ai_platform;

-- =============================================================
-- DIMENSION TABLES
-- Descriptive, slowly-changing data
-- Named with dim_ prefix — industry convention
-- =============================================================

-- dim_repositories: one row per unique GitHub repository
-- Why a dimension? Repo name/owner/language rarely changes.
-- Facts (stars, forks) change daily and go into fact tables.
CREATE TABLE dim_repositories (
    repo_id         BIGINT          PRIMARY KEY,
    full_name       VARCHAR(255)    NOT NULL,
    owner           VARCHAR(100),
    language        VARCHAR(50),
    license         VARCHAR(50),
    created_at      TIMESTAMP,
    topics_count    INT             DEFAULT 0,
    is_fork         BOOLEAN         DEFAULT FALSE,
    inserted_at     TIMESTAMP       DEFAULT NOW()
);

-- dim_models: one row per unique HuggingFace model
CREATE TABLE dim_models (
    model_id        VARCHAR(255)    PRIMARY KEY,
    author          VARCHAR(100),
    model_name      VARCHAR(255),
    pipeline_tag    VARCHAR(100),
    library_name    VARCHAR(100),
    tags_count      INT             DEFAULT 0,
    created_at      TIMESTAMP,
    inserted_at     TIMESTAMP       DEFAULT NOW()
);

-- =============================================================
-- FACT TABLES
-- Measurable metrics captured daily
-- Named with fact_ prefix — industry convention
-- batch_date is always present: it's the grain (one row per entity per day)
-- =============================================================

-- fact_github_trends: daily snapshot of top GitHub repositories
-- Grain: one row per repo per batch_date
CREATE TABLE fact_github_trends (
    id                  SERIAL          PRIMARY KEY,
    batch_date          DATE            NOT NULL,
    full_name           VARCHAR(255)    NOT NULL,
    owner               VARCHAR(100),
    language            VARCHAR(50),
    stars               BIGINT          DEFAULT 0,
    forks               BIGINT          DEFAULT 0,
    engagement_score    BIGINT          DEFAULT 0,
    engagement_rank     INT,
    engagement_percentile FLOAT,
    topics_count        INT             DEFAULT 0,
    license             VARCHAR(50),
    is_high_quality     BOOLEAN,
    inserted_at         TIMESTAMP       DEFAULT NOW()
);

-- fact_topic_trends: popularity of GitHub topics
-- Grain: one row per topic per batch_date
CREATE TABLE fact_topic_trends (
    id                  SERIAL          PRIMARY KEY,
    batch_date          DATE,
    topic               VARCHAR(100)    NOT NULL,
    repo_count          INT             DEFAULT 0,
    total_stars         BIGINT          DEFAULT 0,
    avg_stars_per_repo  BIGINT          DEFAULT 0,
    topic_rank          INT,
    inserted_at         TIMESTAMP       DEFAULT NOW()
);

-- fact_org_leaderboard: GitHub organizations ranked by total stars
CREATE TABLE fact_org_leaderboard (
    id              SERIAL          PRIMARY KEY,
    batch_date      DATE,
    organization    VARCHAR(100)    NOT NULL,
    repo_count      INT             DEFAULT 0,
    total_stars     BIGINT          DEFAULT 0,
    total_forks     BIGINT          DEFAULT 0,
    top_repo_stars  BIGINT          DEFAULT 0,
    avg_stars       FLOAT,
    org_rank        INT,
    inserted_at     TIMESTAMP       DEFAULT NOW()
);

-- fact_model_metrics: top HuggingFace models per pipeline type
-- Grain: one row per model per pipeline_tag per batch_date
CREATE TABLE fact_model_metrics (
    id                  SERIAL          PRIMARY KEY,
    batch_date          DATE,
    model_id            VARCHAR(255)    NOT NULL,
    author              VARCHAR(100),
    pipeline_tag        VARCHAR(100),
    downloads           BIGINT          DEFAULT 0,
    likes               BIGINT          DEFAULT 0,
    library_name        VARCHAR(100),
    rank_in_category    INT,
    global_rank         INT,
    download_quartile   INT,
    tags_count          INT,
    inserted_at         TIMESTAMP       DEFAULT NOW()
);

-- fact_author_leaderboard: HuggingFace author influence metrics
CREATE TABLE fact_author_leaderboard (
    id                      SERIAL          PRIMARY KEY,
    batch_date              DATE,
    author                  VARCHAR(100)    NOT NULL,
    model_count             INT             DEFAULT 0,
    total_downloads         BIGINT          DEFAULT 0,
    total_likes             BIGINT          DEFAULT 0,
    top_model_downloads     BIGINT          DEFAULT 0,
    avg_downloads           FLOAT,
    influence_score         FLOAT,
    download_rank           INT,
    inserted_at             TIMESTAMP       DEFAULT NOW()
);

-- fact_pipeline_summary: download share per AI task category
CREATE TABLE fact_pipeline_summary (
    id                          SERIAL          PRIMARY KEY,
    batch_date                  DATE,
    pipeline_tag                VARCHAR(100)    NOT NULL,
    model_count                 INT             DEFAULT 0,
    total_downloads             BIGINT          DEFAULT 0,
    total_likes                 BIGINT          DEFAULT 0,
    avg_downloads               BIGINT          DEFAULT 0,
    max_downloads               BIGINT          DEFAULT 0,
    category_rank               INT,
    prev_category_downloads     BIGINT,
    download_share_pct          FLOAT,
    inserted_at                 TIMESTAMP       DEFAULT NOW()
);

-- fact_research_trends: arXiv paper counts per category
CREATE TABLE fact_research_trends (
    id                          SERIAL          PRIMARY KEY,
    batch_date                  DATE,
    primary_category            VARCHAR(20)     NOT NULL,
    category_label              VARCHAR(100),
    paper_count                 INT             DEFAULT 0,
    avg_authors_per_paper       FLOAT,
    max_authors                 INT,
    cross_disciplinary_count    INT             DEFAULT 0,
    cross_disciplinary_pct      FLOAT,
    activity_rank               INT,
    inserted_at                 TIMESTAMP       DEFAULT NOW()
);

-- fact_author_metrics: prolific arXiv authors
CREATE TABLE fact_author_metrics (
    id                          SERIAL          PRIMARY KEY,
    batch_date                  DATE,
    author                      VARCHAR(255)    NOT NULL,
    paper_count                 INT             DEFAULT 0,
    category_breadth            INT             DEFAULT 0,
    categories_published_in     TEXT[],
    categories_str              TEXT,
    author_rank                 INT,
    inserted_at                 TIMESTAMP       DEFAULT NOW()
);

-- fact_cross_disciplinary: research spanning multiple categories
CREATE TABLE fact_cross_disciplinary (
    id              SERIAL          PRIMARY KEY,
    batch_date      DATE,
    category_combo  VARCHAR(255)    NOT NULL,
    paper_count     INT             DEFAULT 0,
    avg_authors     BIGINT,
    combo_rank      INT,
    inserted_at     TIMESTAMP       DEFAULT NOW()
);

-- fact_executive_kpis: one row per daily run — the dashboard headline
-- This is the most important table for the Streamlit dashboard
CREATE TABLE fact_executive_kpis (
    id                          SERIAL          PRIMARY KEY,
    batch_date                  DATE            NOT NULL UNIQUE,
    -- GitHub metrics
    total_repos                 BIGINT,
    total_stars                 BIGINT,
    total_forks                 BIGINT,
    max_repo_stars              BIGINT,
    unique_languages            BIGINT,
    unique_owners               BIGINT,
    top_repo_name               VARCHAR(255),
    top_repo_stars              BIGINT,
    -- HuggingFace metrics
    total_models                BIGINT,
    total_downloads             BIGINT,
    total_likes                 BIGINT,
    max_model_downloads         BIGINT,
    unique_authors              BIGINT,
    unique_pipeline_types       BIGINT,
    top_model_id                VARCHAR(255),
    top_model_downloads_val     BIGINT,
    -- arXiv metrics
    total_papers                BIGINT,
    avg_authors_per_paper       FLOAT,
    unique_categories           BIGINT,
    cross_disciplinary_papers   BIGINT,
    top_research_category       VARCHAR(20),
    inserted_at                 TIMESTAMP       DEFAULT NOW()
);

-- =============================================================
-- INDEXES
-- Add indexes on columns used in WHERE clauses and JOINs
-- Without indexes, every query does a full table scan
-- =============================================================
CREATE INDEX idx_github_batch_date    ON fact_github_trends(batch_date);
CREATE INDEX idx_github_language      ON fact_github_trends(language);
CREATE INDEX idx_github_rank          ON fact_github_trends(engagement_rank);
CREATE INDEX idx_topic_batch_date     ON fact_topic_trends(batch_date);
CREATE INDEX idx_model_pipeline       ON fact_model_metrics(pipeline_tag);
CREATE INDEX idx_model_batch_date     ON fact_model_metrics(batch_date);
CREATE INDEX idx_research_category    ON fact_research_trends(primary_category);
CREATE INDEX idx_kpis_batch_date      ON fact_executive_kpis(batch_date);

-- Grant all permissions to the platform user
GRANT ALL ON SCHEMA ai_platform TO platform;
GRANT ALL ON ALL TABLES IN SCHEMA ai_platform TO platform;
GRANT ALL ON ALL SEQUENCES IN SCHEMA ai_platform TO platform;
