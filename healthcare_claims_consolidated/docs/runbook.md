# Deployment Runbook: Unified Banking Data Warehouse

## Overview

This runbook provides step-by-step instructions to deploy the consolidated dbt project and Snowflake orchestration to a new environment. The project unifies transformation logic from three legacy platforms (Teradata, Databricks, Snowflake) into a single dbt-based data model.

---

## Prerequisites

### Software Requirements

| Tool | Version | Purpose |
|------|---------|---------|
| dbt-core | >= 1.7.0 | Transformation framework |
| dbt-snowflake | >= 1.7.0 | Snowflake adapter |
| Python | >= 3.9 | dbt runtime |
| Git | >= 2.30 | Version control |
| Snowflake CLI (snowsql) | >= 1.2 | Orchestration deployment |

### Snowflake Requirements

- **Warehouse**: `TRANSFORM_WH` (X-Small or larger)
- **Database**: `BARCLAYS_UNIFIED` (or target database name)
- **Schemas**: `RAW`, `STG`, `DWH`, `MART`, `COMPLIANCE`
- **Roles**: `TRANSFORM_ROLE` (read on RAW, write on STG/DWH/MART/COMPLIANCE)
- **Service Account**: For scheduled Task execution

### Network Requirements

- Snowflake account connectivity
- Access to S3/Azure Blob/GCS buckets for Snowpipe ingestion
- Git access for dbt package installation

---

## Step 1: Environment Setup

### 1.1 Clone the Repository

```bash
git clone <repo-url>
cd brc-teradata-migration-demo/healthcare_claims_consolidated
```

### 1.2 Install dbt Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install dbt-snowflake>=1.7.0
```

### 1.3 Configure dbt Profile

Create or update `~/.dbt/profiles.yml`:

```yaml
healthcare_claims_consolidated:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: TRANSFORM_ROLE
      warehouse: TRANSFORM_WH
      database: BARCLAYS_UNIFIED
      schema: DWH
      threads: 8
      client_session_keep_alive: true
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: TRANSFORM_ROLE
      warehouse: TRANSFORM_WH
      database: BARCLAYS_UNIFIED_DEV
      schema: DWH
      threads: 4
```

### 1.4 Install dbt Packages

```bash
dbt deps
```

---

## Step 2: Database Preparation

### 2.1 Create Snowflake Infrastructure

Run the following as `SYSADMIN` or equivalent:

```sql
-- Database
CREATE DATABASE IF NOT EXISTS BARCLAYS_UNIFIED;

-- Schemas
CREATE SCHEMA IF NOT EXISTS BARCLAYS_UNIFIED.RAW;
CREATE SCHEMA IF NOT EXISTS BARCLAYS_UNIFIED.STG;
CREATE SCHEMA IF NOT EXISTS BARCLAYS_UNIFIED.DWH;
CREATE SCHEMA IF NOT EXISTS BARCLAYS_UNIFIED.MART;
CREATE SCHEMA IF NOT EXISTS BARCLAYS_UNIFIED.COMPLIANCE;
CREATE SCHEMA IF NOT EXISTS BARCLAYS_UNIFIED.SNAPSHOTS;

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Role and Grants
CREATE ROLE IF NOT EXISTS TRANSFORM_ROLE;
GRANT USAGE ON DATABASE BARCLAYS_UNIFIED TO ROLE TRANSFORM_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE BARCLAYS_UNIFIED TO ROLE TRANSFORM_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA BARCLAYS_UNIFIED.RAW TO ROLE TRANSFORM_ROLE;
GRANT ALL ON SCHEMA BARCLAYS_UNIFIED.STG TO ROLE TRANSFORM_ROLE;
GRANT ALL ON SCHEMA BARCLAYS_UNIFIED.DWH TO ROLE TRANSFORM_ROLE;
GRANT ALL ON SCHEMA BARCLAYS_UNIFIED.MART TO ROLE TRANSFORM_ROLE;
GRANT ALL ON SCHEMA BARCLAYS_UNIFIED.COMPLIANCE TO ROLE TRANSFORM_ROLE;
GRANT ALL ON SCHEMA BARCLAYS_UNIFIED.SNAPSHOTS TO ROLE TRANSFORM_ROLE;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORM_ROLE;
```

### 2.2 Load Raw Data

Ensure raw data from all three legacy platforms is loaded into the `RAW` schema:

```sql
-- Verify raw tables exist
SELECT table_name, row_count
FROM information_schema.tables
WHERE table_schema = 'RAW'
ORDER BY table_name;
```

Expected tables:
- `RAW.TERADATA_CUSTOMER`, `RAW.TERADATA_ACCOUNT`, `RAW.TERADATA_TRANSACTION`, `RAW.TERADATA_MARKET_DATA`, `RAW.TERADATA_COUNTERPARTY`
- `RAW.DATABRICKS_CUSTOMER`, `RAW.DATABRICKS_ACCOUNT`, `RAW.DATABRICKS_TRANSACTION`, `RAW.DATABRICKS_MARKET_DATA`, `RAW.DATABRICKS_COUNTERPARTY`
- `RAW.SNOWFLAKE_CUSTOMER`, `RAW.SNOWFLAKE_ACCOUNT`, `RAW.SNOWFLAKE_TRANSACTION`, `RAW.SNOWFLAKE_MARKET_DATA`, `RAW.SNOWFLAKE_COUNTERPARTY`

---

## Step 3: Initial dbt Build

### 3.1 Validate Configuration

```bash
dbt debug
```

Confirm all checks pass (connection, dependencies, profile).

### 3.2 Load Seeds

```bash
dbt seed
```

This loads reference data (e.g., `dim_date`, `dim_risk_ratings`).

### 3.3 Run Staging Models

```bash
dbt run --select tag:staging
```

This builds all `stg_*` models, resolving column naming and structural drift.

### 3.4 Run Intermediate Models

```bash
dbt run --select tag:intermediate
```

This builds unified cross-platform models and enriched transaction data.

### 3.5 Run Snapshots

```bash
dbt snapshot
```

This initializes SCD Type 2 history tables for customer risk ratings and account status.

### 3.6 Run Mart Models

```bash
dbt run --select tag:mart
```

This builds all finance, risk, and compliance marts.

### 3.7 Full Build (Alternative)

Or run everything at once:

```bash
dbt build
```

---

## Step 4: Validation

### 4.1 Run dbt Tests

```bash
dbt test
```

This runs:
- Schema tests (not_null, unique, accepted_values, relationships)
- Custom reconciliation tests (row counts, aggregates, risk score consistency)
- Edge case tests (null handling, date boundaries, zero-dollar transactions)

### 4.2 Run Reconciliation Queries

```bash
# Row count reconciliation
dbt test --select assert_transaction_count_reconciliation
dbt test --select assert_customer_count_reconciliation

# Aggregate reconciliation
dbt test --select assert_aggregate_reconciliation

# Risk score validation
dbt test --select assert_risk_score_consistency
dbt test --select assert_capital_ratio_bounds
dbt test --select assert_valid_risk_scores
```

### 4.3 Verify Row Counts

```sql
-- Compare unified vs source platform counts
SELECT 'int_unified_customer' as model, count(*) FROM DWH.int_unified_customer
UNION ALL
SELECT 'int_unified_account', count(*) FROM DWH.int_unified_account
UNION ALL
SELECT 'int_unified_transaction', count(*) FROM DWH.int_unified_transaction
UNION ALL
SELECT 'fct_daily_transactions', count(*) FROM MART.fct_daily_transactions
UNION ALL
SELECT 'fct_credit_risk_scores', count(*) FROM MART.fct_credit_risk_scores
UNION ALL
SELECT 'fct_aml_alerts', count(*) FROM COMPLIANCE.fct_aml_alerts;
```

---

## Step 5: Deploy Orchestration

### 5.1 Deploy Helper Procedures

```bash
snowsql -f orchestration/snowflake_tasks/03_helper_procedures.sql
```

### 5.2 Deploy Change Tracking Streams

```bash
snowsql -f orchestration/streams/change_tracking.sql
```

### 5.3 Deploy Snowpipe

```bash
snowsql -f orchestration/snowpipe/raw_data_ingestion.sql
```

### 5.4 Deploy Daily Batch Pipeline

```bash
snowsql -f orchestration/snowflake_tasks/01_daily_batch_pipeline.sql
```

### 5.5 Deploy Monthly Regulatory Pipeline

```bash
snowsql -f orchestration/snowflake_tasks/02_monthly_regulatory_pipeline.sql
```

### 5.6 Verify Task Status

```sql
SHOW TASKS IN SCHEMA BARCLAYS_UNIFIED.DWH;

-- Resume tasks (they are created in suspended state)
ALTER TASK BARCLAYS_UNIFIED.DWH.TASK_DAILY_BATCH_ROOT RESUME;
ALTER TASK BARCLAYS_UNIFIED.DWH.TASK_MONTHLY_REGULATORY_ROOT RESUME;
```

---

## Step 6: Post-Deployment Verification

### 6.1 Monitor First Daily Run

```sql
-- Check task execution history
SELECT *
FROM TABLE(information_schema.task_history(
  scheduled_time_range_start => dateadd('hour', -24, current_timestamp()),
  result_limit => 100
))
ORDER BY scheduled_time DESC;
```

### 6.2 Verify Incremental Models

After the first daily run, verify incremental models are processing correctly:

```sql
-- Check fct_daily_transactions for today's data
SELECT transaction_date, count(*) as txn_count
FROM MART.fct_daily_transactions
WHERE transaction_date >= current_date - 1
GROUP BY transaction_date
ORDER BY transaction_date;
```

### 6.3 Verify Snapshots

```sql
-- Check SCD2 history is being captured
SELECT count(*) as total_versions, count(distinct customer_id) as unique_customers
FROM SNAPSHOTS.snap_customer_risk_rating;
```

---

## Rollback Procedure

If issues are found after deployment:

### Immediate Rollback

```sql
-- Suspend all tasks
ALTER TASK BARCLAYS_UNIFIED.DWH.TASK_DAILY_BATCH_ROOT SUSPEND;
ALTER TASK BARCLAYS_UNIFIED.DWH.TASK_MONTHLY_REGULATORY_ROOT SUSPEND;

-- Pause Snowpipe
ALTER PIPE BARCLAYS_UNIFIED.RAW.PIPE_CUSTOMER_INGESTION SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE BARCLAYS_UNIFIED.RAW.PIPE_TRANSACTION_INGESTION SET PIPE_EXECUTION_PAUSED = TRUE;
```

### Full Rollback

```sql
-- Drop unified schemas (preserves RAW)
DROP SCHEMA IF EXISTS BARCLAYS_UNIFIED.STG CASCADE;
DROP SCHEMA IF EXISTS BARCLAYS_UNIFIED.DWH CASCADE;
DROP SCHEMA IF EXISTS BARCLAYS_UNIFIED.MART CASCADE;
DROP SCHEMA IF EXISTS BARCLAYS_UNIFIED.COMPLIANCE CASCADE;
DROP SCHEMA IF EXISTS BARCLAYS_UNIFIED.SNAPSHOTS CASCADE;
```

---

## Maintenance

### Daily Operations

- Monitor Task execution history for failures
- Review AML alert counts for anomalies
- Check Snowpipe ingestion lag

### Weekly Operations

- Run full `dbt test` suite
- Review snapshot growth
- Check warehouse credit consumption

### Monthly Operations

- Run regulatory capital calculations after month-end close
- Archive old AML alerts
- Review and update risk rating thresholds if needed

---

## Troubleshooting

| Issue | Resolution |
|-------|-----------|
| `dbt deps` fails | Check network connectivity and `packages.yml` syntax |
| Task fails with timeout | Increase warehouse size or add `QUERY_TAG` for monitoring |
| Snowpipe lag > 1 hour | Check S3 event notification configuration |
| Reconciliation test fails | Compare source platform row counts; check for late-arriving data |
| Risk score NaN/NULL | Check for missing customer records in `int_unified_customer` |
| AML alerts spike | Verify structuring threshold parameters in `int_aml_screening_flags` |

---

## Contact

- **Data Engineering**: data-engineering@barclays.internal
- **Risk Analytics**: risk-analytics@barclays.internal
- **Compliance**: compliance-data@barclays.internal
