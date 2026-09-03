# Barclays Teradata Migration Demo

> **Demo:** Migrating a Barclays-style Teradata on-premises retail banking data warehouse to dbt Core targeting **Databricks** (primary) and Snowflake (secondary).

This repository demonstrates a realistic migration from a monolithic **Teradata** data warehouse -- typical of large UK retail banks -- to a modern, modular **dbt Core** analytics engineering stack running on **Databricks** (Unity Catalog + Delta Lake). Snowflake is kept as a supported secondary target, and a local **Postgres** target is used for development and CI.

---

## Banking Domain

The demo covers five core banking domains that are representative of a Tier-1 UK retail bank:

| Domain | Description |
|---|---|
| **Customer Management** | KYC onboarding, segmentation (retail / wealth / corporate), SCD Type 2 history |
| **Transaction Processing** | Real-time and batch transaction ingestion across channels (branch, online, ATM) |
| **Credit Risk (Basel III)** | Risk-weighted asset (RWA) calculations, PD/LGD/EAD modelling, regulatory capital |
| **AML / KYC Compliance** | Anti-money-laundering screening, sanctions list matching, suspicious-activity alerts |
| **P&L Reporting** | Daily and monthly profit-and-loss rollups by product, business line, and entity |

---

## Architecture: Before & After

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BEFORE  (Teradata)                           │
│                                                                     │
│  ┌──────────┐   ┌──────────────┐   ┌────────────┐   ┌───────────┐ │
│  │  BTEQ /  │──▶│  Stored      │──▶│  DWH       │──▶│  Mart     │ │
│  │  TPT /   │   │  Procedures  │   │  Tables    │   │  Tables   │ │
│  │  FastLoad│   │  (ETL logic) │   │  (Star     │   │  (Reports)│ │
│  └──────────┘   └──────────────┘   │   Schema)  │   └───────────┘ │
│                                     └────────────┘                  │
│  Single monolithic appliance  ·  Tightly coupled ETL + transforms   │
│  Vendor lock-in (PPI, QUALIFY, SET tables, COLLECT STATS, ...)     │
└─────────────────────────────────────────────────────────────────────┘

                              ▼  Migration  ▼

┌─────────────────────────────────────────────────────────────────────┐
│                    AFTER  (dbt + Databricks / Snowflake)            │
│                                                                     │
│  ┌──────────┐   ┌──────────────┐   ┌────────────┐   ┌───────────┐ │
│  │  Fivetran│   │  dbt Core    │   │  dbt Core  │   │  dbt Core │ │
│  │  / Airbyt│──▶│  staging     │──▶│  intermed. │──▶│  marts    │ │
│  │  / Custom│   │  models      │   │  models    │   │  models   │ │
│  └──────────┘   └──────────────┘   └────────────┘   └───────────┘ │
│                                                                     │
│  Databricks / Snowflake  ·  Version-controlled SQL  ·  CI/CD       │
│  Modular  ·  Testable  ·  Self-documenting  ·  Multi-target        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Teradata Constructs & dbt Equivalents

| Teradata Construct | dbt / Modern Equivalent |
|---|---|
| `SET` / `MULTISET` tables | Standard tables (dedup handled in SQL) |
| `PRIMARY INDEX` | Delta liquid clustering / `ZORDER BY` (Snowflake: clustering keys) |
| `PARTITION BY RANGE_N` (PPI) | Delta `PARTITIONED BY` / liquid clustering (Snowflake: micro-partitioning) |
| `QUALIFY ROW_NUMBER()` | Supported natively in Databricks SQL and Snowflake |
| `ZEROIFNULL` / `NULLIFZERO` | `COALESCE(col, 0)` / `NULLIF(col, 0)` |
| `COLLECT STATISTICS` | `ANALYZE TABLE ... COMPUTE STATISTICS` on Databricks (Snowflake: automatic) |
| `LOCK ROW FOR ACCESS` | Read-committed isolation (default) |
| `CSUM` / `MAVG` / `MDIFF` | `SUM() OVER (ORDER BY ...)` / `AVG() OVER (ROWS ...)` |
| `NORMALIZE ON` (period) | Custom SQL with gap-and-island detection |
| `HASHROW` / `HASHBUCKET` | `md5()` / `hash()` / `xxhash64()` on Databricks |
| Stored Procedures (BTEQ calls) | dbt models + macros + orchestration (Airflow / dbt Cloud) |
| `MERGE INTO` (SCD2) | dbt snapshots (`strategy='check'`) |
| `VOLATILE TABLE` | CTEs or ephemeral models |
| `MERGE INTO` (batch upsert) | Delta `MERGE` via dbt `incremental_strategy: merge` |
| FastLoad / MultiLoad / TPT | Databricks Auto Loader / `COPY INTO` (Snowflake: Snowpipe) |
| BTEQ scripts (.bteq) | dbt run / dbt build with selectors |
| Scheduled jobs (cron / UC4) | dbt Cloud schedules / Airflow DAGs / GitHub Actions |

---

## Repository Structure

```
barclays-teradata-migration-demo/
├── README.md                          # This file
├── teradata/                          # Original Teradata artefacts
│   ├── ddl/                           # Database & table definitions
│   ├── stored_procedures/             # Complex ETL logic
│   ├── macros/                        # Teradata macros
│   ├── bteq/                          # Batch scripts
│   ├── tpt/                           # Teradata Parallel Transporter jobs
│   ├── fastload/                      # FastLoad scripts
│   ├── multiload/                     # MultiLoad scripts
│   └── scheduled_jobs/                # Job sequences
├── dbt_project/                       # Migrated dbt Core project
│   ├── models/staging/                # 1:1 source mirrors
│   ├── models/intermediate/           # Business logic (ex-stored procs)
│   ├── models/marts/                  # Consumption-ready tables
│   ├── macros/                        # Reusable SQL helpers
│   ├── seeds/                         # Reference data CSVs
│   ├── tests/                         # Custom data tests
│   └── snapshots/                     # SCD Type 2 tracking
├── migration_guide/                   # Step-by-step playbooks
├── sample_data/                       # Realistic UK banking CSVs
├── .github/workflows/dbt_ci.yml      # CI pipeline
└── docker-compose.yml                 # Local dev environment
```

---

## Quick Start (Databricks -- primary target)

```bash
# 1. Clone
git clone https://github.com/COG-GTM/brc-teradata-migration-demo.git
cd brc-teradata-migration-demo

# 2. Install dbt with the Databricks adapter
pip install dbt-core dbt-databricks

# 3. Configure the profile (default target is `databricks`)
cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml

# 4. Export Databricks connection settings
export DATABRICKS_HOST='adb-1234567890123456.7.azuredatabricks.net'  # no scheme
export DATABRICKS_HTTP_PATH='/sql/1.0/warehouses/abc123def456'
export DATABRICKS_TOKEN='dapi...'            # or use the OAuth block in the profile
export DATABRICKS_CATALOG='barclays_migration'  # Unity Catalog catalog (optional)
export DATABRICKS_SCHEMA='public'               # optional
export DATABRICKS_THREADS='8'                   # optional

# 5. Build
cd dbt_project
dbt deps
dbt build --target databricks    # seeds + models + snapshots + tests
```

The Unity Catalog `catalog` must already exist and the principal behind
`DATABRICKS_TOKEN` (or the OAuth service principal) needs `USE CATALOG`,
`CREATE SCHEMA`, and `CREATE TABLE` on it.

### Snowflake (secondary target)

```bash
pip install dbt-core dbt-snowflake
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_ROLE=TRANSFORMER SNOWFLAKE_DATABASE=BARCLAYS_MIGRATION SNOWFLAKE_WAREHOUSE=TRANSFORMING
cd dbt_project && dbt deps && dbt build --target snowflake
```

### Local Postgres (development & CI)

Used by `.github/workflows/dbt_ci.yml` so every PR gets a functional
`dbt build` without a warehouse account.

```bash
docker-compose up -d                 # Postgres on :5432, Adminer on :8080
pip install dbt-core dbt-postgres
cd dbt_project && dbt deps && dbt build --target postgres_local
```

`dbt_project.yml` sets `vars.target_platform: "databricks"`; set it to
`"snowflake"` (or override with `--vars 'target_platform: snowflake'`) to emit
Snowflake-flavoured SQL from the `teradata_compat` macros. Databricks-specific
model configs (`file_format: delta`, `incremental_strategy: merge`) are applied
via target-conditional jinja, so the Postgres and Snowflake paths are unchanged.

---

## How Devin Can Help

[Devin](https://devin.ai) is an autonomous AI software engineer that can accelerate every phase of a Teradata-to-dbt migration:

| Migration Phase | How Devin Automates It |
|---|---|
| **SQL Syntax Translation** | Devin reads Teradata DDL and stored procedures, then rewrites them as dbt-compatible SQL targeting Databricks or Snowflake -- handling `QUALIFY`, `ZEROIFNULL`, `CSUM`, date arithmetic, and other Teradata-specific constructs automatically. |
| **Stored Procedure Decomposition** | Devin analyses monolithic stored procedures, identifies discrete transformation steps, and decomposes them into modular dbt models with proper `{{ ref() }}` lineage. |
| **Test Generation** | Devin inspects column semantics and business rules to generate `schema.yml` tests (unique, not_null, accepted_values, relationships) plus custom data tests. |
| **CI/CD Setup** | Devin creates GitHub Actions workflows, docker-compose files, and profile templates so every PR runs `dbt build` automatically. |
| **Incremental Migration Validation** | Devin builds reconciliation queries that compare row counts, aggregates, and sample records between the legacy Teradata output and the new dbt models to ensure data parity. |

---

## Related Resources

- [COG-GTM/Teradata-Utilities-Script](https://github.com/COG-GTM/Teradata-Utilities-Script) -- Additional Teradata utility examples (BTEQ, FastLoad, MultiLoad, TPT)
- [dbt Documentation](https://docs.getdbt.com/)
- [Databricks Migration Guide](https://docs.databricks.com/en/migration/teradata.html)
- [dbt-databricks adapter setup](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)
- [Snowflake Migration Guide](https://docs.snowflake.com/en/user-guide/migration-teradata.html)

---

## License

This repository is provided as a demonstration and educational resource. All data is synthetic and does not represent real Barclays customer information.
