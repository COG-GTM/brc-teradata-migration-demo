# Healthcare Claims Migration Runbook

## Overview

Step-by-step instructions to deploy the unified dbt project and Snowflake Tasks to a new environment. This runbook consolidates three legacy platforms (Teradata, Databricks, Snowflake) into a single unified data warehouse on Snowflake using dbt and the Tuva Health data model.

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Snowflake Account | Enterprise edition or higher (for Dynamic Data Masking) |
| Snowflake Role | CLAIMS_ETL_ROLE with CREATE TABLE, CREATE VIEW, CREATE TASK, CREATE PIPE |
| Snowflake Warehouse | CLAIMS_WH (X-Small or larger) |
| dbt | v1.7+ (dbt-core or dbt Cloud) |
| dbt-snowflake | v1.7+ adapter |
| Python | 3.9+ (for dbt) |
| Git | For version control |

---

## Step 1: Snowflake Environment Setup

### 1.1 Create Database and Schemas

```sql
-- Connect as ACCOUNTADMIN or SYSADMIN
CREATE DATABASE IF NOT EXISTS CLAIMS_DW;

CREATE SCHEMA IF NOT EXISTS CLAIMS_DW.RAW;
CREATE SCHEMA IF NOT EXISTS CLAIMS_DW.STAGING;
CREATE SCHEMA IF NOT EXISTS CLAIMS_DW.INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS CLAIMS_DW.MARTS;
CREATE SCHEMA IF NOT EXISTS CLAIMS_DW.TUVA_INPUT;
CREATE SCHEMA IF NOT EXISTS CLAIMS_DW.ORCHESTRATION;
```

### 1.2 Create Warehouse

```sql
CREATE WAREHOUSE IF NOT EXISTS CLAIMS_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;
```

### 1.3 Create Role and Grant Permissions

```sql
CREATE ROLE IF NOT EXISTS CLAIMS_ETL_ROLE;
GRANT USAGE ON DATABASE CLAIMS_DW TO ROLE CLAIMS_ETL_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE CLAIMS_DW TO ROLE CLAIMS_ETL_ROLE;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE CLAIMS_DW TO ROLE CLAIMS_ETL_ROLE;
GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE CLAIMS_DW TO ROLE CLAIMS_ETL_ROLE;
GRANT CREATE TASK ON SCHEMA CLAIMS_DW.ORCHESTRATION TO ROLE CLAIMS_ETL_ROLE;
GRANT CREATE PIPE ON SCHEMA CLAIMS_DW.RAW TO ROLE CLAIMS_ETL_ROLE;
GRANT USAGE ON WAREHOUSE CLAIMS_WH TO ROLE CLAIMS_ETL_ROLE;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE CLAIMS_ETL_ROLE;
```

### 1.4 Create Cloud Storage Stage

```sql
-- Replace with your actual cloud storage location
CREATE OR REPLACE STAGE CLAIMS_DW.RAW.CLAIMS_STAGE
    URL = 's3://your-bucket/claims-data/'
    STORAGE_INTEGRATION = your_storage_integration;
```

---

## Step 2: PHI Dynamic Data Masking Setup

### 2.1 Create Masking Policies

```sql
-- SSN masking
CREATE OR REPLACE MASKING POLICY CLAIMS_DW.RAW.SSN_MASK AS (val VARCHAR)
RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_ETL_ROLE', 'CLAIMS_ADMIN_ROLE') THEN val
        ELSE 'XXX-XX-' || RIGHT(val, 4)
    END;

-- Name masking
CREATE OR REPLACE MASKING POLICY CLAIMS_DW.RAW.NAME_MASK AS (val VARCHAR)
RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_ETL_ROLE', 'CLAIMS_ADMIN_ROLE') THEN val
        ELSE SHA2(val, 256)
    END;

-- DOB masking
CREATE OR REPLACE MASKING POLICY CLAIMS_DW.RAW.DOB_MASK AS (val DATE)
RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_ETL_ROLE', 'CLAIMS_ADMIN_ROLE') THEN val
        ELSE DATE_FROM_PARTS(YEAR(val), 1, 1)
    END;
```

### 2.2 Apply Masking Policies to RAW Tables

```sql
ALTER TABLE CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN ssn SET MASKING POLICY CLAIMS_DW.RAW.SSN_MASK;
ALTER TABLE CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN first_name SET MASKING POLICY CLAIMS_DW.RAW.NAME_MASK;
ALTER TABLE CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN last_name SET MASKING POLICY CLAIMS_DW.RAW.NAME_MASK;
ALTER TABLE CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN date_of_birth SET MASKING POLICY CLAIMS_DW.RAW.DOB_MASK;
```

---

## Step 3: dbt Project Setup

### 3.1 Clone Repository

```bash
git clone https://github.com/COG-GTM/brc-teradata-migration-demo.git
cd brc-teradata-migration-demo/healthcare_dbt
```

### 3.2 Set Environment Variables

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
# Or use key-pair authentication:
# export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/rsa_key.p8"
```

### 3.3 Install dbt Dependencies

```bash
pip install dbt-snowflake>=1.7.0
cd healthcare_dbt
dbt deps  # Installs Tuva package
```

### 3.4 Verify Connection

```bash
dbt debug
```

---

## Step 4: Initial Data Load

### 4.1 Create RAW Tables

Run the DDL scripts to create raw tables:

```sql
-- Execute: snowflake/ddl/02_raw_tables.sql
-- This creates RAW_MEMBER_ELIGIBILITY, RAW_MEDICAL_CLAIM, RAW_PHARMACY_CLAIM
```

### 4.2 Load Historical Data

Option A: Bulk load from files
```sql
COPY INTO CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
FROM @CLAIMS_DW.RAW.CLAIMS_STAGE/medical/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

Option B: Load from Teradata export
```sql
-- Use Snowflake-Labs/SC.TeradataExportScripts to export from Teradata
-- Then load the exported files via COPY INTO
```

Option C: Load from Databricks Delta export
```sql
-- Export Delta tables to Parquet, stage in S3/Azure/GCS
-- Then load via COPY INTO
```

---

## Step 5: Run dbt Pipeline

### 5.1 Full Initial Run

```bash
cd healthcare_dbt

# Compile and validate
dbt compile

# Run staging models (maps from raw to unified schema)
dbt run --select staging

# Run intermediate models (ADR dedup, encounter grouping, eligibility dedup)
dbt run --select intermediate

# Run mart models (claim summary, encounter summary, member months)
dbt run --select marts

# Run Tuva input layer models
dbt run --select migration
```

### 5.2 Run Full Test Suite

```bash
# Run all tests (custom validation + Tuva DQ - 614+ tests)
dbt test

# Run only custom validation tests
dbt test --select test_type:singular

# Run specific test
dbt test --select validation_adr_dedup_priority
```

### 5.3 Generate Documentation

```bash
dbt docs generate
dbt docs serve  # Opens documentation site at localhost:8080
```

---

## Step 6: Deploy Orchestration

### 6.1 Create Pipeline Tasks

```sql
-- Execute: orchestration/01_daily_claims_pipeline.sql
-- This creates:
--   TASK_RAW_INGESTION (root, daily 6 AM ET)
--   TASK_STAGING_REFRESH (after ingestion)
--   TASK_ADR_DEDUP (after staging)
--   TASK_ENCOUNTER_GROUPING (after ADR dedup)
--   TASK_MARTS_REFRESH (after encounter grouping)
--   TASK_VALIDATION (after marts)
--   Snowpipes for continuous ingestion

-- Execute: orchestration/02_monthly_enrollment_pipeline.sql
-- This creates:
--   TASK_MONTHLY_ENROLLMENT (1st of month, 2 AM ET)
--   TASK_QUARTERLY_QUALITY (quarterly, 3 AM ET)
```

### 6.2 Create dbt Execution Stored Procedures

The orchestration scripts create `SP_RUN_DBT_MODELS` and `SP_RUN_DBT_TESTS` stored procedures. In production, these should be configured to call:

- **dbt Cloud:** POST to dbt Cloud API to trigger job runs
- **Self-hosted:** Call external function that triggers CI/CD pipeline (e.g., GitHub Actions, GitLab CI)

### 6.3 Enable Tasks

```sql
-- Enable tasks bottom-up (leaf tasks first, root task last)
ALTER TASK CLAIMS_DW.ORCHESTRATION.TASK_VALIDATION RESUME;
ALTER TASK CLAIMS_DW.ORCHESTRATION.TASK_MARTS_REFRESH RESUME;
ALTER TASK CLAIMS_DW.ORCHESTRATION.TASK_ENCOUNTER_GROUPING RESUME;
ALTER TASK CLAIMS_DW.ORCHESTRATION.TASK_ADR_DEDUP RESUME;
ALTER TASK CLAIMS_DW.ORCHESTRATION.TASK_STAGING_REFRESH RESUME;
ALTER TASK CLAIMS_DW.ORCHESTRATION.TASK_RAW_INGESTION RESUME;

ALTER TASK CLAIMS_DW.ORCHESTRATION.TASK_MONTHLY_ENROLLMENT RESUME;
ALTER TASK CLAIMS_DW.ORCHESTRATION.TASK_QUARTERLY_QUALITY RESUME;
```

---

## Step 7: Validation Checklist

After deployment, verify:

- [ ] `dbt debug` connects successfully
- [ ] `dbt run` completes without errors
- [ ] `dbt test` passes all 614+ tests
- [ ] Row count reconciliation: unified counts ≤ platform sum
- [ ] ADR dedup: no DENIED claims retained when ADJUSTED exists
- [ ] Encounter grouping: no overlapping claims in different encounters
- [ ] Financial totals: all amounts ≥ 0
- [ ] PHI masking: non-privileged roles see masked SSN/name/DOB
- [ ] Snowflake Tasks: daily pipeline executes on schedule
- [ ] Snowpipes: auto-ingest processes new files within SLA

---

## Step 8: Legacy Platform Decommission Plan

After validation is complete and the unified pipeline has been running in parallel for a minimum of 30 days:

1. **Week 1-2:** Run unified pipeline in shadow mode (parallel with legacy)
2. **Week 3-4:** Compare outputs daily (row counts, aggregates, financial totals)
3. **Week 5-6:** Route read queries to unified model, keep legacy running
4. **Week 7-8:** Decommission legacy write paths (stop BTEQ jobs, Databricks jobs)
5. **Week 9-12:** Archive legacy platforms, maintain read-only access for audit trail

### Decommission Order

1. **Teradata** (first) — Most drift, most maintenance burden
2. **Databricks** (second) — Good logic but redundant with unified model
3. **Snowflake legacy Tasks/Pipes** (last) — Infrastructure is reused, only legacy scheduling removed

---

## Troubleshooting

### Common Issues

| Issue | Cause | Resolution |
|-------|-------|------------|
| `dbt deps` fails | Network access to dbt Hub | Check firewall/proxy settings |
| `dbt run` timeout | Large data volume | Increase warehouse size (CLAIMS_WH) |
| Task not running | Task suspended | `ALTER TASK ... RESUME` |
| Snowpipe not ingesting | Missing SQS/SNS notification | Check storage integration configuration |
| PHI visible to analysts | Role not mapped to masking policy | Verify `CURRENT_ROLE()` in masking policy |
| Test failures on Tuva DQ | Invalid source data | Check RAW table data quality |

### Monitoring

```sql
-- Check task execution history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;

-- Check pipeline execution log
SELECT * FROM CLAIMS_DW.ORCHESTRATION.PIPELINE_EXECUTION_LOG
ORDER BY started_at DESC
LIMIT 20;

-- Check Snowpipe status
SELECT SYSTEM$PIPE_STATUS('CLAIMS_DW.RAW.PIPE_MEDICAL_CLAIMS');
```

---

## Contact

For questions about this migration:
- **Migration documentation:** `docs/` directory in this repository
- **Drift findings:** `docs/phase3_logic_drift/` and `docs/phase9_documentation/logic_drift_report.json`
- **Risk assessment:** `docs/phase4_risk_assessment/risk_assessment.json`
