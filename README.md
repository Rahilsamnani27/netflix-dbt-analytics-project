# dbt-databuildtool--masterclass-netflix-project
dbt(databuildtool)-masterclass-netflix-project
🎬 dbt Masterclass — Netflix/MovieLens Data Pipeline

A production-grade data transformation pipeline built with dbt + Snowflake, implementing a full analytics engineering workflow — from raw ingestion to business-ready data marts — with data quality testing, incremental models, snapshots, seeds, and auto-generated documentation.


📌 Table of Contents

Project Overview
Tech Stack
Architecture
Project Structure
Data Layers
Models
Data Quality Tests
Advanced Features
Setup & Installation
Running the Pipeline
Documentation


🎯 Project Overview
This project demonstrates a complete analytics engineering workflow using the MovieLens dataset with a Netflix-style use case. It transforms raw movie ratings, genome scores, and tag data into clean, tested, analytics-ready tables using industry best practices.
What this pipeline produces:
Output TableDescriptiondim_moviesCleaned movie metadata with parsed genre arraysdim_usersUnique users derived from both ratings and tagsdim_genome_tagsTag labels with standardized namingfct_ratingsIncremental fact table of user ratingsfct_genome_scoresMovie-tag relevance scores (filtered & rounded)mart_movie_releasesBusiness mart joining ratings with seed release datessnap_tagsSCD Type 2 snapshot tracking tag changes over time
Key numbers from the final dbt test run: 13 tests, 0 failures, 0 warnings.

🛠 Tech Stack
LayerTechnologyTransformationdbt 1.9.6WarehouseSnowflake (Transient Tables)LanguageSQL, Jinja2Packagesdbt-utils 1.3.0MaterializationView, Table, Incremental, EphemeralTestingSchema tests + Custom singular tests + Custom macrosDocumentationdbt Docs (auto-generated catalog + lineage graph)OrchestrationManual / Schedulable via dbt Cloud or Airflow

🏗 Architecture
RAW LAYER (Snowflake)          STAGING LAYER              DIM / FCT LAYER          MART LAYER
┌──────────────────┐      ┌──────────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  RAW_MOVIES      │─────▶│  src_movies          │───▶│  dim_movies      │    │                  │
│  RAW_RATINGS     │─────▶│  src_ratings (table) │───▶│  dim_users       │───▶│  mart_movie_     │
│  RAW_TAGS        │─────▶│  src_tags (table)    │    │  fct_ratings     │    │  releases        │
│  RAW_GENOME_*    │─────▶│  src_genome_score    │───▶│  fct_genome_     │    │                  │
│  RAW_LINKS       │─────▶│  src_links           │    │  scores          │    └──────────────────┘
└──────────────────┘      └──────────────────────┘    └──────────────────┘
                                                              │
                                                    ┌──────────────────┐
                                                    │  dim_movies_     │  (ephemeral CTE)
                                                    │  with_tags       │
                                                    └──────────────────┘
                                                              │
                                                    ┌──────────────────┐
                                                    │  ep_movie_with_  │
                                                    │  tags            │
                                                    └──────────────────┘

SEEDS                          SNAPSHOTS
┌──────────────────┐      ┌──────────────────┐
│  seed_movie_     │      │  snap_tags        │  ← SCD Type 2
│  release_dates   │      │  (Snowflake       │
└──────────────────┘      │   snapshots)      │
                          └──────────────────┘

📁 Project Structure
netflix/
├── models/
│   ├── staging/              # Raw → cleaned views/tables
│   │   ├── src_movies.sql    # Uses {{ source() }} macro
│   │   ├── src_ratings.sql   # Materialized as TABLE
│   │   ├── src_tags.sql      # Materialized as TABLE
│   │   ├── src_genome_score.sql
│   │   ├── src_genome_tags.sql
│   │   └── src_links.sql
│   ├── dim/                  # Dimension tables
│   │   ├── dim_movies.sql
│   │   ├── dim_users.sql
│   │   ├── dim_genome_tags.sql
│   │   └── dim_movies_with_tags.sql  # Ephemeral CTE
│   ├── fct/                  # Fact tables
│   │   ├── fct_ratings.sql   # Incremental model
│   │   └── fct_genome_scores.sql
│   └── mart/
│       └── mart_movie_releases.sql   # Seed join
├── snapshots/
│   └── snap_tags.sql         # SCD Type 2 snapshot
├── seeds/
│   └── seed_movie_release_dates.csv  # Static reference data
├── tests/
│   └── relevence_score_test.sql      # Custom singular test
├── macros/
│   └── no_nulls_in_columns.sql       # Reusable null-check macro
├── analyses/
│   └── movie_analysis.sql    # Ad-hoc analysis query
├── dbt_project.yml
├── packages.yml              # dbt-utils dependency
└── sources.yml               # Source freshness definitions

📊 Data Layers
Staging Layer
Light transformations only — rename columns, cast types, no business logic. Some staging models are materialized as tables (src_ratings, src_tags) to avoid expensive repeated scans of large raw tables downstream.
sql-- src_ratings.sql
{{ config(materialized = 'table') }}
WITH raw_ratings AS (
  SELECT * FROM MOVIELENS.RAW.RAW_RATINGS
)
SELECT
  userId AS user_id,
  movieId AS movie_id,
  rating,
  TO_TIMESTAMP_LTZ(timestamp) AS rating_timestamp
FROM raw_ratings
Dimension Layer
Clean, business-ready dimension tables. Notable features:

dim_movies: Uses SPLIT(genres, '|') to create a proper genre array, INITCAP(TRIM()) for standardized titles
dim_users: Deduplicates users across two source tables using UNION
dim_movies_with_tags: Ephemeral model — compiled as a CTE inline, no physical table created, used by ep_movie_with_tags

Fact Layer

fct_ratings: Incremental model — on subsequent runs, only inserts new ratings using MAX(rating_timestamp) filter. Uses on_schema_change='fail' to prevent silent schema drift.
fct_genome_scores: Filters out zero-relevance scores and rounds to 4 decimal places for clean analytics

Mart Layer
Business-facing table joining fct_ratings with seed data to flag whether a movie's release date is known or unknown.

🧱 Models
Materialization Strategy
ModelMaterializationReasonsrc_genome_scoreViewSmall reference, low query costsrc_ratingsTableLarge dataset — avoid repeated raw scanssrc_tagsTableSame — supports snapshot and incremental downstreamdim_moviesTableStable, frequently joineddim_movies_with_tagsEphemeralUsed only as a CTE; no storage neededfct_ratingsIncrementalMillions of rows — only process new data each runmart_movie_releasesTableBusiness mart — needs stable materialization
Incremental Model Logic
sql-- fct_ratings.sql
{{ config(materialized = 'incremental', on_schema_change='fail') }}

{% if is_incremental() %}
  AND rating_timestamp > (SELECT MAX(rating_timestamp) FROM {{ this }})
{% endif %}

✅ Data Quality Tests
Schema Tests (schema.yml)
ModelColumnTestsdim_moviesmovie_idnot_nulldim_moviesmovie_titlenot_nulldim_usersuser_idnot_nulldim_genome_tagstag_idnot_nulldim_genome_tagstag_namenot_nullfct_ratingsmovie_idnot_null, relationships → dim_moviesfct_ratingsuser_idnot_nullfct_ratingsratingnot_nullfct_genome_scoresmovie_idnot_nullfct_genome_scorestag_idnot_nullfct_genome_scoresrelevance_scorenot_null
Referential Integrity Test
yaml- relationships:
    to: ref('dim_movies')
    field: movie_id
Ensures every movie_id in fct_ratings exists in dim_movies — catching orphaned records at build time.
Custom Singular Test
sql-- tests/relevence_score_test.sql
SELECT movie_id, tag_id, relevance_score
FROM MOVIELENS.DEV.fct_genome_scores
WHERE relevance_score <= 0
Validates that all relevance scores are strictly positive after filtering in fct_genome_scores.
Custom Macro Test
sql-- macros/no_nulls_in_columns.sql
{% macro no_nulls_in_columns(model) %}
    SELECT * FROM {{ model }} WHERE
    {% for col in adapter.get_columns_in_relation(model) %}
        {{ col.column }} IS NULL OR
    {% endfor %}
    FALSE
{% endmacro %}
Dynamically generates a null check across all columns of any model — no need to enumerate columns manually.
Final test run result: ✅ PASS=13 WARN=0 ERROR=0 SKIP=0 TOTAL=13

🚀 Advanced Features
SCD Type 2 Snapshot
Tracks historical changes to the src_tags table using Snowflake's merge strategy:
sql-- snapshots/snap_tags.sql
{% snapshot snap_tags %}
{{
    config(
        target_schema='snapshots',
        unique_key=['user_id','movie_id','tag'],
        strategy='timestamp',
        updated_at='tag_timestamp',
        invalidate_hard_deletes=True
    )
}}
SELECT
{{ dbt_utils.generate_surrogate_key(['user_id','movie_id','tag']) }} AS row_key,
    ...
FROM {{ ref('src_tags') }}
{% endsnapshot %}
Uses dbt_utils.generate_surrogate_key from the dbt-utils package to create a deterministic surrogate key from composite natural keys.
Seeds
Static CSV data loaded directly into Snowflake:
movie_id,release_date
1,1995-10-20
2,1995-10-21
...
Referenced in mart_movie_releases via {{ ref('seed_movie_release_dates') }}.
Sources
Defined in sources.yml with identifier aliasing — decouples model SQL from raw table names:
yamlsources:
  - name: netflix
    schema: raw
    tables:
      - name: r_movies
        identifier: raw_movies   # actual table name
Ephemeral Model
dim_movies_with_tags is never materialized — it compiles as an inline CTE wherever it's referenced, keeping the warehouse clean while enabling reuse:
sql{{ config(materialized = 'ephemeral') }}
dbt Analyses
Ad-hoc SQL query in analyses/movie_analysis.sql that uses {{ ref() }} to join fct_ratings and dim_movies for top-rated movies — compiled but not executed by dbt run.

⚙️ Setup & Installation
Prerequisites

Python 3.9+
Snowflake account with a MOVIELENS database and RAW schema pre-loaded
YouTube Data API key (if applicable)

Installation
bash# Clone the repo
git clone https://github.com/your-username/dbt-databuildtool-masterclass-netflix-project.git
cd dbt-databuildtool-masterclass-netflix-project

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate          # Mac/Linux
# venv\Scripts\activate           # Windows

# Install dbt-snowflake
pip install dbt-snowflake==1.9.0
Configure dbt Profile
Create ~/.dbt/profiles.yml:
yamlnetflix:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: dbt
      password: <your_password>
      role: TRANSFORM
      database: MOVIELENS
      warehouse: COMPUTE_WH
      schema: DEV
      threads: 4
Snowflake Setup
sqlUSE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

CREATE USER IF NOT EXISTS dbt
  PASSWORD='<your_password>'
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='MOVIELENS.RAW';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM TO USER dbt;

CREATE DATABASE IF NOT EXISTS MOVIELENS;
CREATE SCHEMA IF NOT EXISTS MOVIELENS.RAW;
GRANT ALL ON DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;

▶️ Running the Pipeline
bash# Install dependencies (dbt-utils)
dbt deps

# Load seed data
dbt seed

# Run all models
dbt run

# Run only incremental models
dbt run --select fct_ratings

# Run with full refresh (rebuild incrementals from scratch)
dbt run --full-refresh

# Run data quality tests
dbt test

# Run a specific model + its downstream dependencies
dbt run --select fct_ratings+

# Execute snapshot
dbt snapshot

# Compile all SQL (no execution)
dbt compile
Execution Order
dbt seed
  └── seed_movie_release_dates

dbt run
  ├── src_* (staging views/tables)
  ├── dim_genome_tags, dim_movies, dim_users (dimension tables)
  ├── fct_genome_scores, fct_ratings (fact tables)
  ├── dim_movies_with_tags (ephemeral — compiled inline)
  └── mart_movie_releases (mart table)

dbt snapshot
  └── snap_tags (SCD Type 2)

dbt test
  └── 13 schema + singular + macro tests

📖 Documentation
Generate and serve the dbt docs site with full lineage DAG:
bashdbt docs generate
dbt docs serve
Opens at http://localhost:8080 — includes:

Auto-generated data catalog with column descriptions
Full DAG lineage graph (source → staging → dim/fct → mart)
Compiled SQL for every model
Test coverage per model


📝 Notes

Staging models src_genome_score, src_genome_tags, and src_links use hardcoded MOVIELENS.RAW.* references rather than {{ source() }} — a known improvement opportunity for full source abstraction
src_movies was refactored mid-project to use {{ source('netflix', 'r_movies') }} demonstrating the source abstraction pattern
The unique tests are commented out in schema.yml since the MovieLens dataset contains duplicate entries by design (users can re-rate movies)


🧑‍💻 Author
Built as part of a dbt Masterclass to demonstrate production analytics engineering patterns on real-world movie data.
