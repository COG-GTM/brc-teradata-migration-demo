# Barclays Teradata Migration Demo

> **Demo:** Migrating a Barclays-style Teradata on-premises retail banking data warehouse to dbt Core targeting Snowflake and Databricks.

This repository demonstrates a realistic migration from a monolithic **Teradata** data warehouse -- typical of large UK retail banks -- to a modern, modular **dbt Core** analytics engineering stack running on **Snowflake** or **Databricks**.

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
│                    AFTER  (dbt + Snowflake / Databricks)            │
│                                                                     │
│  ┌──────────┐   ┌──────────────┐   ┌────────────┐   ┌───────────┐ │
│  │  Fivetran│   │  dbt Core    │   │  dbt Core  │   │  dbt Core │ │
│  │  / Airbyt│──▶│  staging     │──▶│  intermed. │──▶│  marts    │ │
│  │  / Custom│   │  models      │   │  models    │   │  models   │ │
│  └──────────┘   └──────────────┘   └────────────┘   └───────────┘ │
│                                                                     │
│  Snowflake / Databricks  ·  Version-controlled SQL  ·  CI/CD       │
│  Modular  ·  Testable  ·  Self-documenting  ·  Multi-target        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Teradata Constructs & dbt Equivalents

| Teradata Construct | dbt / Modern Equivalent |
|---|---|
| `SET` / `MULTISET` tables | Standard tables (dedup handled in SQL) |
| `PRIMARY INDEX` | Snowflake clustering keys / Databricks Z-ORDER |
| `PARTITION BY RANGE_N` (PPI) | Snowflake automatic micro-partitioning / Delta partitioning |
| `QUALIFY ROW_NUMBER()` | Supported natively in Snowflake; subquery wrapper in Databricks |
| `ZEROIFNULL` / `NULLIFZERO` | `COALESCE(col, 0)` / `NULLIF(col, 0)` |
| `COLLECT STATISTICS` | Snowflake automatic stats / `ANALYZE TABLE` in Databricks |
| `LOCK ROW FOR ACCESS` | Read-committed isolation (default) |
| `CSUM` / `MAVG` / `MDIFF` | `SUM() OVER (ORDER BY ...)` / `AVG() OVER (ROWS ...)` |
| `NORMALIZE ON` (period) | Custom SQL with gap-and-island detection |
| `HASHROW` / `HASHBUCKET` | `MD5()` / `HASH()` |
| Stored Procedures (BTEQ calls) | dbt models + macros + orchestration (Airflow / dbt Cloud) |
| `MERGE INTO` (SCD2) | dbt snapshots (`strategy='check'`) |
| `VOLATILE TABLE` | CTEs or ephemeral models |
| FastLoad / MultiLoad / TPT | Snowpipe / COPY INTO / Databricks Auto Loader |
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

## Quick Start

```bash
# 1. Clone
git clone https://github.com/COG-GTM/barclays-teradata-migration-demo.git
cd barclays-teradata-migration-demo

# 2. Start local Postgres (for dbt development / testing)
docker-compose up -d

# 3. Install dbt
pip install dbt-postgres  # or dbt-snowflake / dbt-databricks

# 4. Run the dbt project
cd dbt_project
cp profiles.yml.example ~/.dbt/profiles.yml   # edit connection details
dbt deps
dbt seed
dbt run
dbt test
```

---

## How Devin Can Help

[Devin](https://devin.ai) is an autonomous AI software engineer that can accelerate every phase of a Teradata-to-dbt migration:

| Migration Phase | How Devin Automates It |
|---|---|
| **SQL Syntax Translation** | Devin reads Teradata DDL and stored procedures, then rewrites them as dbt-compatible SQL targeting Snowflake or Databricks -- handling `QUALIFY`, `ZEROIFNULL`, `CSUM`, date arithmetic, and other Teradata-specific constructs automatically. |
| **Stored Procedure Decomposition** | Devin analyses monolithic stored procedures, identifies discrete transformation steps, and decomposes them into modular dbt models with proper `{{ ref() }}` lineage. |
| **Test Generation** | Devin inspects column semantics and business rules to generate `schema.yml` tests (unique, not_null, accepted_values, relationships) plus custom data tests. |
| **CI/CD Setup** | Devin creates GitHub Actions workflows, docker-compose files, and profile templates so every PR runs `dbt build` automatically. |
| **Incremental Migration Validation** | Devin builds reconciliation queries that compare row counts, aggregates, and sample records between the legacy Teradata output and the new dbt models to ensure data parity. |

---

## Related Resources

- [COG-GTM/Teradata-Utilities-Script](https://github.com/COG-GTM/Teradata-Utilities-Script) -- Additional Teradata utility examples (BTEQ, FastLoad, MultiLoad, TPT)
- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake Migration Guide](https://docs.snowflake.com/en/user-guide/migration-teradata.html)
- [Databricks Migration Guide](https://docs.databricks.com/en/migration/teradata.html)

---

## License

This repository is provided as a demonstration and educational resource. All data is synthetic and does not represent real Barclays customer information.
