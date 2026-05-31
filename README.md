# dbt-databuildtool--masterclass-netflix-project
dbt(databuildtool)-masterclass-netflix-project

div align="center">
# 🎬 dbt × Snowflake — MovieLens Analytics Pipeline
 
**A production-grade analytics engineering project** built with dbt 1.9 + Snowflake.  
Raw movie data → clean, tested, business-ready tables in minutes.
 
![dbt](https://img.shields.io/badge/dbt-1.9.6-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Tests](https://img.shields.io/badge/Tests-13%20Passing-brightgreen?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)
 
</div>
---
 
## 🧭 Overview
 
This pipeline transforms raw [MovieLens](https://grouplens.org/datasets/movielens/) data into analytics-ready tables using a **full dbt feature stack** — incremental models, SCD Type 2 snapshots, custom macros, seeds, sources, and auto-generated documentation — all running on Snowflake.
 
```
RAW (Snowflake)  ──▶  STAGING  ──▶  DIM / FCT  ──▶  MART
raw_movies              src_movies      dim_movies       mart_movie_releases
raw_ratings             src_ratings     dim_users
raw_tags                src_tags        fct_ratings  ←  incremental
raw_genome_*            src_genome_*    fct_genome_scores
                                        snap_tags    ←  SCD Type 2
```
 
---
 
## ⚡ Tech Stack
 
| | Tool |
|---|---|
| **Transform** | dbt 1.9.6 |
| **Warehouse** | Snowflake |
| **Package** | dbt-utils 1.3.0 |
| **Materializations** | View · Table · Incremental · Ephemeral |
| **Testing** | Schema tests · Singular tests · Custom macros |
 
---
 
## 📁 Project Structure
 
```
netflix/
├── models/
│   ├── staging/          # src_movies, src_ratings*, src_tags*, src_genome_*
│   ├── dim/              # dim_movies, dim_users, dim_genome_tags
│   │                     # dim_movies_with_tags  ← ephemeral
│   ├── fct/              # fct_ratings (incremental), fct_genome_scores
│   └── mart/             # mart_movie_releases
├── snapshots/            # snap_tags  ← SCD Type 2
├── seeds/                # seed_movie_release_dates.csv
├── tests/                # relevence_score_test.sql
├── macros/               # no_nulls_in_columns.sql
├── analyses/             # movie_analysis.sql
└── sources.yml           # Source definitions with identifier aliasing
```
> `*` materialized as TABLE to avoid repeated large raw scans
 
---
 
## 🏗 Key Models
 
### Incremental — `fct_ratings`
Only processes **new rows** on each run, preventing full table rebuilds:
```sql
{% if is_incremental() %}
  AND rating_timestamp > (SELECT MAX(rating_timestamp) FROM {{ this }})
{% endif %}
```
> `on_schema_change='fail'` guards against silent schema drift
 
### Ephemeral — `dim_movies_with_tags`
Never materialized in Snowflake — compiled as an **inline CTE** wherever referenced, saving storage while enabling clean reuse.
 
### SCD Type 2 Snapshot — `snap_tags`
Full historical tracking of tag changes using `dbt_utils.generate_surrogate_key`:
```sql
{{ dbt_utils.generate_surrogate_key(['user_id', 'movie_id', 'tag']) }} AS row_key
```
 
---
 
## ✅ Data Quality
 
| Type | Count | Result |
|---|---|---|
| Schema tests (not_null, relationships) | 11 | ✅ Pass |
| Custom singular test | 1 | ✅ Pass |
| Custom macro test (`no_nulls_in_columns`) | 1 | ✅ Pass |
| **Total** | **13** | **✅ 0 failures** |
 
**Referential integrity** — every `movie_id` in `fct_ratings` is validated against `dim_movies`:
```yaml
- relationships:
    to: ref('dim_movies')
    field: movie_id
```
 
**Reusable null-check macro** — dynamically iterates all columns, no hardcoding needed:
```sql
{% for col in adapter.get_columns_in_relation(model) %}
    {{ col.column }} IS NULL OR
{% endfor %}
```
 
---
 
## ⚙️ Setup
 
**1. Install**
```bash
python -m venv venv && source venv/bin/activate
pip install dbt-snowflake==1.9.0
```
 
**2. Configure** `~/.dbt/profiles.yml`
```yaml
netflix:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <account>
      user: dbt
      password: <password>
      role: TRANSFORM
      database: MOVIELENS
      warehouse: COMPUTE_WH
      schema: DEV
```
 
**3. Snowflake prerequisites** — `MOVIELENS` database with `RAW` schema loaded, `TRANSFORM` role, `COMPUTE_WH` warehouse.
 
---
 
## ▶️ Run the Pipeline
 
```bash
dbt deps          # Install dbt-utils
dbt seed          # Load release dates CSV
dbt run           # Build all models
dbt test          # Run all 13 tests
dbt snapshot      # Capture SCD Type 2 changes
dbt docs generate && dbt docs serve   # Open lineage DAG at localhost:8080
```
 
---
 
## 📌 Design Notes
 
- `src_genome_score`, `src_genome_tags`, `src_links` use hardcoded `MOVIELENS.RAW.*` — refactoring to `{{ source() }}` is the next improvement
- `unique` tests are intentionally commented out — MovieLens allows users to re-rate movies, so duplicates are expected by design
- `src_movies` was refactored mid-project to use `{{ source('netflix', 'r_movies') }}`, demonstrating source abstraction
---
 
<div align="center">
Built to demonstrate production analytics engineering patterns with dbt + Snowflake.
</div>
 
