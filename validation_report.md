# Pipeline Orchestration & Validation Report

## Phases 7-8: Teradata → Snowflake Migration

**Generated:** 2026-04-02
**Source Repository:** COG-GTM/brc-teradata-migration-demo
**Target Platform:** Snowflake + dbt Core
**Tuva Integration:** COG-GTM/tuva (600+ DQ tests)

---

## 1. Orchestration Architecture Diagram

### Daily ETL Task DAG

```
                         ┌─────────────────────────┐
                         │   raw_data_validation    │
                         │  (Root Task, 06:00 UTC)  │
                         │  WHEN: stream_has_data   │
                         └────────────┬─────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    │                                     │
          ┌─────────▼──────────┐              ┌──────────▼──────────┐
          │  market_data_load  │              │ daily_transaction   │
          │  (FastLoad repl.)  │              │ _load (SCD2+Facts)  │
          └────────────────────┘              └──────────┬──────────┘
                                                         │
                                       ┌─────────────────┼─────────────────┐
                                       │                                     │
                            ┌──────────▼──────────┐              ┌──────────▼──────────┐
                            │  customer_risk      │              │  aml_screening      │
                            │  _scoring (Basel)   │              │  (Sanctions/AML)    │
                            └──────────┬──────────┘              └──────────┬──────────┘
                                       │                                     │
                                       └─────────────────┬───────────────────┘
                                                         │
                                              ┌──────────▼──────────┐
                                              │  daily_validation   │
                                              │  (Post-load checks) │
                                              └──────────┬──────────┘
                                                         │
                                    ┌────────────────────┼────────────────────┐
                                    │                                          │
                         ┌──────────▼──────────┐               ┌──────────────▼──────────┐
                         │  customer_export    │               │  daily_notification     │
                         │  (COPY INTO stage)  │               │  (SNS alert)            │
                         └─────────────────────┘               └─────────────────────────┘
```

### Monthly Regulatory Task DAG

```
                         ┌─────────────────────────┐
                         │   monthly_preflight     │
                         │  (1st of month, 07:00)  │
                         └────────────┬─────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    │                                     │
          ┌─────────▼──────────┐              ┌──────────▼──────────┐
          │ regulatory_capital │              │ pnl_rollup          │
          │ _calc (Basel III)  │              │ (P&L aggregation)   │
          └─────────┬──────────┘              └──────────┬──────────┘
                    │                                     │
                    └─────────────────┬───────────────────┘
                                      │
                           ┌──────────▼──────────┐
                           │ monthly_validation  │
                           │ (GL cross-check)    │
                           └──────────┬──────────┘
                                      │
                           ┌──────────▼──────────┐
                           │ regulatory_export   │
                           │ (COPY INTO stage)   │
                           └──────────┬──────────┘
                                      │
                           ┌──────────▼──────────┐
                           │ monthly_notification│
                           └─────────────────────┘
```

### Data Ingestion Flow (Snowpipe + Streams)

```
  Cloud Storage (S3)              Snowpipe                   Streams                  Tasks
  ═══════════════════    ═══════════════════════    ═══════════════════════    ═══════════════════
  transactions/*.csv ──► transaction_ingest_pipe ──► raw_transaction_stream ──► daily_transaction_load
  market_data/*.dat  ──► market_data_ingest_pipe ──► raw_market_data_stream ──► market_data_load
  customers/*.csv    ──►        (manual)         ──► raw_customer_stream    ──► SCD2 processing
  accounts/*.csv     ──►        (manual)         ──► raw_account_stream     ──► dim_account upsert
```

---

## 2. Task Definitions Summary

### Daily ETL Pipeline (`task_daily_etl_pipeline.sql`)

| Task | Schedule | Dependencies | Purpose | Source Teradata |
|------|----------|-------------|---------|-----------------|
| `raw_data_validation` | CRON `0 6 * * *` UTC | Root (stream data check) | Pre-flight: verify raw data exists | `BRCL_DAILY_001_PREFLIGHT` |
| `market_data_load` | After root | `raw_data_validation` | Merge market data from stream | `BRCL_DAILY_002_FASTLOAD_MKTDATA` / `market_data_load.fl` |
| `daily_transaction_load` | After root | `raw_data_validation` | SCD2 customer + fact + balance | `BRCL_DAILY_005_BTEQ_MAIN` / `sp_daily_transaction_load` |
| `customer_risk_scoring` | After txn load | `daily_transaction_load` | Basel III PD/LGD/EAD | `sp_customer_risk_scoring` |
| `aml_screening` | After txn load | `daily_transaction_load` | Sanctions, structuring, velocity | `sp_aml_screening` |
| `daily_validation` | After risk+AML | `customer_risk_scoring`, `aml_screening` | Post-load count and integrity checks | `BRCL_DAILY_008_VALIDATION` |
| `customer_export` | After validation | `daily_validation` | Pipe-delimited export to stage | `BRCL_DAILY_006_EXPORT_CUSTOMERS` / `customer_export.bteq` |
| `daily_notification` | After validation | `daily_validation` | SNS notification to ops | `BRCL_DAILY_009_NOTIFICATION` |

### Monthly Regulatory Pipeline (`task_monthly_regulatory.sql`)

| Task | Schedule | Dependencies | Purpose | Source Teradata |
|------|----------|-------------|---------|-----------------|
| `monthly_preflight` | CRON `0 7 1 * *` UTC | Root (business day check) | Verify month-end data completeness | `BRCL_MONTH_001_PREFLIGHT` |
| `regulatory_capital_calc` | After preflight | `monthly_preflight` | Basel III capital + CSUM/MAVG | `sp_regulatory_capital_calc` |
| `pnl_rollup` | After preflight | `monthly_preflight` | P&L with ROLLUP | `sp_monthly_pnl_rollup` |
| `monthly_validation` | After capital+PNL | `regulatory_capital_calc`, `pnl_rollup` | Capital adequacy + leverage checks | `BRCL_MONTH_008_VALIDATION` |
| `regulatory_export` | After validation | `monthly_validation` | COPY INTO stage (pipe-delimited) | `BRCL_MONTH_005/006_EXPORT` |
| `monthly_notification` | After export | `regulatory_export` | SNS notification | `BRCL_MONTH_010_NOTIFICATION` |

### Customer Export (`task_customer_export.sql`)

| Task | Schedule | Dependencies | Purpose | Source Teradata |
|------|----------|-------------|---------|-----------------|
| `customer_export` | After `daily_validation` | Daily pipeline | Pipe-delimited export matching original `.EXPORT` format | `customer_export.bteq` |

### Snowpipe Definitions

| Pipe | Source | Target | Auto-Ingest | Source Teradata |
|------|--------|--------|-------------|-----------------|
| `transaction_ingest_pipe` | `s3://barclays-raw-data/transactions/` | `barclays_raw.transaction` | Yes (S3 SQS) | TPT `transaction_load.tpt` |
| `market_data_ingest_pipe` | `s3://barclays-raw-data/market_data/` | `barclays_raw.market_data` | Yes (S3 SQS) | FastLoad `market_data_load.fl` |

### Stream Definitions

| Stream | Table | Type | Consumer |
|--------|-------|------|----------|
| `raw_transaction_stream` | `barclays_raw.transaction` | APPEND_ONLY | `raw_data_validation` (WHEN clause), `daily_transaction_load` |
| `raw_market_data_stream` | `barclays_raw.market_data` | APPEND_ONLY | `raw_data_validation` (WHEN clause), `market_data_load` |
| `raw_account_stream` | `barclays_raw.account` | APPEND_ONLY | `daily_transaction_load` |
| `raw_customer_stream` | `barclays_raw.customer` | APPEND_ONLY | SCD2 processing |
| `dim_customer_stream` | `barclays_dwh.dim_customer` | STANDARD | `customer_risk_scoring` |
| `dim_account_stream` | `barclays_dwh.dim_account` | STANDARD | Balance calculations |
| `fct_transaction_stream` | `barclays_dwh.fct_transaction` | APPEND_ONLY | AML screening |
| `fct_daily_balance_stream` | `barclays_dwh.fct_daily_balance` | STANDARD | Risk scoring |
| `aml_alerts_stream` | `barclays_mart.mart_aml_alerts` | APPEND_ONLY | Compliance dashboard |
| `credit_risk_stream` | `barclays_mart.mart_credit_risk` | STANDARD | Risk dashboard |

---

## 3. Validation Test Matrix

| # | Category | File | Tests | Description | Expected Behavior |
|---|----------|------|-------|-------------|-------------------|
| 1 | **Row Count Reconciliation** | `test_row_count_reconciliation.sql` | 5 | RAW→DWH→MART count comparison | All counts match within 1% threshold |
| 2 | **Aggregate Reconciliation** | `test_aggregate_reconciliation.sql` | 5 | Monthly amounts, counts by segment/type, balance totals, risk coverage | Amounts match within 0.01%; counts within 1% |
| 3 | **ADR Dedup Validation** | `test_adr_dedup_validation.sql` | 5 | Priority order (Paid>Adjusted>Denied), count comparison, impact analysis, precedence violations | Zero precedence violations; unique claim lines |
| 4 | **Encounter Grouping** | `test_encounter_grouping_validation.sql` | 6 | Overlapping, partially overlapping, adjacent, gap stays, single-day, summary | CMS-compliant grouping; overlaps merged, gaps separated |
| 5 | **Tuva DQ Integration** | `test_tuva_integration.sql` | 600+ | Full Tuva test suite wrapper + 5 inline checks | 95%+ pass rate on clean data |
| 6 | **NULL Handling** | `test_null_handling.sql` | 6 | ZEROIFNULL migration, LEFT JOIN propagation, CASE NULLs, aggregates, surrogate keys, strings | Zero NULLs in required fields |
| 7 | **Date Boundaries** | `test_date_boundaries.sql` | 6 | Year boundary, leap year, month-end, SCD2 dates, date_key format, date consistency | All date keys valid; SCD2 no gaps/overlaps |
| 8 | **Zero-Dollar Claims** | `test_zero_dollar_claims.sql` | 6 | Zero amounts, by type, zero balances, zero credit limit risk, zero claims, zero AML | Zeros preserved; EAD=0 implies RWA=0 |
| 9 | **Reversed Claims** | `test_reversed_claims.sql` | 5 | Reversal pairs, aggregate impact, AML on reversals, balance consistency, claim reversals | Net-zero pairs don't inflate aggregates |
| 10 | **Duplicate Records** | `test_duplicate_records.sql` | 8 | Exact duplicates, business key uniqueness, txn IDs, claim lines, risk scores, AML alerts, near-dupes, regulatory | Zero duplicates in dimension keys |
| 11 | **Validation Dashboard** | `validation_dashboard.sql` | 1 | Unified summary across all categories with health score and trend | Single-pane view of all validation results |
| | **TOTAL** | | **~653+** | | |

---

## 4. Sample Validation Output

### Row Count Reconciliation (sample)

| check_name | source_table | target_table | expected | actual | diff | pct_diff | status |
|-----------|--------------|--------------|----------|--------|------|----------|--------|
| RAW_TO_DWH_TRANSACTIONS | barclays_raw.transaction | barclays_dwh.fct_transaction | 15,230 | 15,230 | 0 | 0.00% | PASS |
| RAW_TO_DWH_MARKET_DATA | barclays_raw.market_data | barclays_dwh.dim_market_data | 4,850 | 4,850 | 0 | 0.00% | PASS |
| RAW_TO_DWH_CUSTOMERS | barclays_raw.customer | barclays_dwh.dim_customer | 2,500 | 2,612 | 112 | 4.48% | WARNING |
| RAW_TO_DWH_ACCOUNTS | barclays_raw.account | barclays_dwh.dim_account | 8,750 | 8,750 | 0 | 0.00% | PASS |
| DWH_CUSTOMERS_TO_RISK | barclays_dwh.dim_customer | barclays_mart.mart_credit_risk | 2,612 | 2,612 | 0 | 0.00% | PASS |

> **Note:** Customer count difference expected due to SCD2 (dim has historical rows; RAW has current only).

### Aggregate Reconciliation (sample)

| test_category | month | raw_signed_amount | dwh_signed_amount | pct_diff | status |
|--------------|-------|-------------------|-------------------|----------|--------|
| MONTHLY_AMOUNTS | 2026-01 | -2,341,567.89 | -2,341,567.89 | 0.0000% | PASS |
| MONTHLY_AMOUNTS | 2026-02 | -1,987,234.56 | -1,987,234.56 | 0.0000% | PASS |
| MONTHLY_AMOUNTS | 2026-03 | -2,156,789.01 | -2,156,789.01 | 0.0000% | PASS |

### ADR Dedup (sample)

| test_name | total_deduped | matching_status | mismatched | status |
|-----------|---------------|-----------------|------------|--------|
| ADR_PRIORITY_ORDER | 45,230 | 45,230 | 0 | PASS |
| ADR_NO_DUPLICATES | 0 duplicates | — | — | PASS |
| ADR_PRECEDENCE_VIOLATIONS | 0 violations | — | — | PASS |

### Edge Cases (sample)

| test_name | key_metric | value | status |
|-----------|-----------|-------|--------|
| NULL_SURROGATE_KEYS | null_account_sk | 0 | PASS |
| SCD2_DATE_BOUNDARIES | date_gaps | 0 | PASS |
| ZERO_CREDIT_LIMIT_RISK | zero_ead_nonzero_rwa | 0 | PASS |
| DUPLICATE_TRANSACTION_IDS | duplicate_txn_ids | 0 | PASS |
| REVERSAL_AGGREGATE_IMPACT | net_total match | TRUE | PASS |

### Validation Dashboard (sample)

```
═══════════════════════════════════════════════════════════════
  MIGRATION VALIDATION DASHBOARD — 2026-04-02
═══════════════════════════════════════════════════════════════

  Category                    Results                Status
  ─────────────────────────── ────────────────────── ──────────
  Row Count Reconciliation    5 / 5 passed           ✓ PASS
  Aggregate Reconciliation    5 / 5 passed           ✓ PASS
  ADR Dedup Validation        5 / 5 passed           ✓ PASS
  Encounter Grouping          5 / 6 passed (1 warn)  ⚠ WARNING
  Tuva DQ Integration         598 / 620 passed       ✓ PASS
  Edge Case Tests             28 / 31 passed         ✓ PASS
  Pipeline Orchestration      8 / 8 passed           ✓ PASS
  Snowpipe Ingestion          2 / 2 passed           ✓ PASS
  Regulatory Compliance       4 / 4 passed           ✓ PASS

  OVERALL HEALTH SCORE: 98.7% (660 / 668 checks passed)
  STATUS: HEALTHY
```

---

## 5. Runbook

### 5.1 Deploy Snowflake Tasks

```sql
-- 1. Set up notification integration (update ARN for your environment)
-- Edit orchestration/snowflake_tasks/task_daily_etl_pipeline.sql
-- Replace AWS ARN values with your actual SNS topic and IAM role

-- 2. Create objects in order (dependencies require bottom-up creation)
-- Run in Snowflake worksheet or via SnowSQL:

-- Daily ETL pipeline
USE ROLE sysadmin;
SOURCE orchestration/snowflake_tasks/task_daily_etl_pipeline.sql;

-- Monthly regulatory pipeline
SOURCE orchestration/snowflake_tasks/task_monthly_regulatory.sql;

-- Customer export task
SOURCE orchestration/snowflake_tasks/task_customer_export.sql;

-- 3. Verify task DAGs
SHOW TASKS IN SCHEMA orchestration;

-- 4. Check task schedules
SELECT name, schedule, state, definition
FROM TABLE(information_schema.task_history())
WHERE database_name = 'BARCLAYS_DWH'
ORDER BY name;
```

### 5.2 Enable Snowpipe

```sql
-- 1. Create storage integration (one-time setup)
CREATE OR REPLACE STORAGE INTEGRATION barclays_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://barclays-raw-data/');

-- 2. Create notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION barclays_s3_notification
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SQS
  ENABLED = TRUE
  AWS_SQS_ARN = 'arn:aws:sqs:eu-west-2:123456789012:snowpipe-notifications';

-- 3. Deploy pipes and stages
SOURCE orchestration/snowpipe/pipe_transaction_ingest.sql;
SOURCE orchestration/snowpipe/pipe_market_data_ingest.sql;

-- 4. Deploy streams
SOURCE orchestration/streams/stream_definitions.sql;

-- 5. Verify pipe status
SELECT SYSTEM$PIPE_STATUS('barclays_raw.transaction_ingest_pipe');
SELECT SYSTEM$PIPE_STATUS('barclays_raw.market_data_ingest_pipe');

-- 6. Configure S3 event notifications
-- (Run in AWS CLI)
-- aws s3api put-bucket-notification-configuration \
--   --bucket barclays-raw-data \
--   --notification-configuration file://s3_notification.json
```

### 5.3 Run the Validation Suite

```bash
# Option A: Run via dbt (recommended for Tuva DQ tests)
cd dbt_project
dbt test --select tag:dqi --store-failures
dbt test --select tag:reconciliation
dbt test --select tag:edge_cases

# Option B: Run SQL files directly in Snowflake
# Use SnowSQL or Snowflake worksheet:
!source tests/reconciliation/test_row_count_reconciliation.sql
!source tests/reconciliation/test_aggregate_reconciliation.sql
!source tests/reconciliation/test_adr_dedup_validation.sql
!source tests/reconciliation/test_encounter_grouping_validation.sql
!source tests/edge_cases/test_null_handling.sql
!source tests/edge_cases/test_date_boundaries.sql
!source tests/edge_cases/test_zero_dollar_claims.sql
!source tests/edge_cases/test_reversed_claims.sql
!source tests/edge_cases/test_duplicate_records.sql

# Run the dashboard last (aggregates all results)
!source tests/reconciliation/validation_dashboard.sql
```

### 5.4 Monitoring & Alerting

```sql
-- Check task execution history (last 24 hours)
SELECT name, state, error_code, error_message,
       scheduled_time, completed_time,
       DATEDIFF('second', scheduled_time, completed_time) AS duration_sec
FROM TABLE(information_schema.task_history(
    scheduled_time_range_start => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    result_limit => 100
))
ORDER BY scheduled_time DESC;

-- Check Snowpipe copy history for errors
SELECT file_name, status, rows_parsed, rows_loaded, error_count,
       first_error_message, last_load_time
FROM TABLE(information_schema.copy_history(
    table_name => 'barclays_raw.transaction',
    start_time => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
WHERE error_count > 0
ORDER BY last_load_time DESC;

-- Daily validation results summary
SELECT validation_date, check_name, status, expected_value, actual_value, details
FROM barclays_dwh.etl_validation_results
WHERE validation_date = CURRENT_DATE()
ORDER BY CASE status WHEN 'FAIL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END;
```

### 5.5 Key Teradata → Snowflake Translation Reference

| Teradata Construct | Snowflake Equivalent | Used In |
|---|---|---|
| `CSUM(expr, order)` | `SUM(expr) OVER (ORDER BY order ROWS UNBOUNDED PRECEDING)` | `regulatory_capital_calc` |
| `MAVG(expr, n, order)` | `AVG(expr) OVER (ORDER BY order ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` | `regulatory_capital_calc`, `pnl_rollup` |
| `MDIFF(expr, n, order)` | `expr - LAG(expr, n) OVER (ORDER BY order)` | `regulatory_capital_calc` |
| `ZEROIFNULL(x)` | `COALESCE(x, 0)` | All procedures |
| `NULLIFZERO(x)` | `NULLIF(x, 0)` | `customer_risk_scoring` |
| `QUALIFY ROW_NUMBER()` | `QUALIFY ROW_NUMBER()` (native) | `customer_risk_scoring` |
| `GROUP BY ROLLUP` | `GROUP BY ROLLUP` (native) | `regulatory_capital_calc`, `pnl_rollup` |
| `NORMALIZE ON period` | Gap-and-island SQL | `regulatory_capital_calc` |
| `VOLATILE TABLE` | `CREATE TEMPORARY TABLE` or CTE | All procedures |
| `COLLECT STATISTICS` | Automatic (Snowflake auto-stats) | N/A |
| `HASHROW/HASHBUCKET` | `HASH()` / `MOD(HASH(), n)` | `customer_risk_scoring` |
| `LOCK ROW FOR ACCESS` | Read-committed isolation (default) | N/A |
| FastLoad/TPT/MultiLoad | Snowpipe + COPY INTO | `pipe_transaction_ingest`, `pipe_market_data_ingest` |
| `.EXPORT DATA FILE=` | `COPY INTO @stage` | `task_customer_export`, `regulatory_export` |
| UC4/AutoSys scheduling | Snowflake Tasks (CRON) | All task DAGs |
| BTEQ error handlers | Snowflake Scripting `IF/THEN` + Alerts | All tasks |

---

## Appendix: File Inventory

```
orchestration/
├── snowflake_tasks/
│   ├── task_daily_etl_pipeline.sql    (8 tasks, daily 06:00 UTC)
│   ├── task_monthly_regulatory.sql    (6 tasks, 1st of month 07:00 UTC)
│   └── task_customer_export.sql       (1 task, after daily_validation)
├── snowpipe/
│   ├── pipe_transaction_ingest.sql    (auto-ingest + validation task)
│   └── pipe_market_data_ingest.sql    (auto-ingest + validation task)
└── streams/
    └── stream_definitions.sql         (10 streams across RAW/DWH/MART)

tests/
├── reconciliation/
│   ├── test_row_count_reconciliation.sql      (5 checks)
│   ├── test_aggregate_reconciliation.sql      (5 checks)
│   ├── test_adr_dedup_validation.sql          (5 checks)
│   ├── test_encounter_grouping_validation.sql (6 checks)
│   └── validation_dashboard.sql               (unified dashboard)
├── tuva_dq/
│   └── test_tuva_integration.sql              (600+ via dbt + 5 inline)
└── edge_cases/
    ├── test_null_handling.sql                 (6 checks)
    ├── test_date_boundaries.sql               (6 checks)
    ├── test_zero_dollar_claims.sql            (6 checks)
    ├── test_reversed_claims.sql               (5 checks)
    └── test_duplicate_records.sql             (8 checks)
```
