# Teradata-to-dbt Migration Playbook

## Overview

This playbook provides a step-by-step guide for migrating a Barclays-style Teradata on-premises banking data warehouse to dbt Core targeting Snowflake or Databricks.

---

## Phase 1: Discovery & Assessment

### 1.1 Schema Inventory

Catalogue every object in the Teradata environment:

| Object Type | Count Method | Notes |
|---|---|---|
| Databases | `SELECT DatabaseName FROM DBC.DatabasesV` | Map to dbt schemas |
| Tables | `SELECT TableName FROM DBC.TablesV WHERE TableKind = 'T'` | Identify SET vs MULTISET |
| Views | `SELECT TableName FROM DBC.TablesV WHERE TableKind = 'V'` | Candidates for staging models |
| Stored Procedures | `SELECT SPName FROM DBC.StoredProcsV` | Decompose into dbt models |
| Macros | `SELECT MacroName FROM DBC.MacrosV` | Convert to dbt macros |
| Indexes | `SELECT IndexName FROM DBC.IndicesV` | Review for Snowflake clustering |
| Statistics | `HELP STATISTICS <table>` | Document for performance tuning |

### 1.2 Dependency Mapping

```
-- Find inter-object dependencies
SELECT
    ChildDB, ChildName, ChildKind,
    ParentDB, ParentName, ParentKind
FROM DBC.All_RI_ChildrenV
ORDER BY ParentDB, ParentName;
```

### 1.3 Data Volume Assessment

```
-- Table sizes and row counts
SELECT
    DatabaseName,
    TableName,
    SUM(CurrentPerm) / 1024 / 1024 AS sizeMb,
    SUM(RowCount) AS totalRows
FROM DBC.TableSizeV
GROUP BY 1, 2
ORDER BY 3 DESC;
```

---

## Phase 2: Construct Mapping

### 2.1 Table Migration

| Teradata Construct | dbt/Snowflake Equivalent | dbt/Databricks Equivalent |
|---|---|---|
| `CREATE SET TABLE` | Standard `CREATE TABLE` (Snowflake deduplicates at query time) | Delta table with `MERGE` for dedup |
| `CREATE MULTISET TABLE` | Standard `CREATE TABLE` | Delta table |
| `PRIMARY INDEX (col)` | Cluster key suggestion | `ZORDER BY (col)` |
| `PARTITION BY RANGE_N` | Automatic micro-partitioning | `PARTITIONED BY (col)` |
| `FALLBACK` | Not needed (built-in HA) | Not needed (Delta replication) |
| `JOURNAL` | Time Travel / Fail-safe | Delta log / time travel |
| `COMPRESS` | Automatic compression | Automatic compression |
| `COLLECT STATISTICS` | Not needed (automatic) | `ANALYZE TABLE` |

### 2.2 SQL Construct Migration

| Teradata SQL | Snowflake SQL | Databricks SQL |
|---|---|---|
| `QUALIFY ROW_NUMBER() OVER (...) = 1` | `QUALIFY ROW_NUMBER() OVER (...) = 1` (native support) | Sub-query with `WHERE rn = 1` |
| `ZEROIFNULL(x)` | `COALESCE(x, 0)` or `ZEROIFNULL(x)` | `COALESCE(x, 0)` |
| `NULLIFZERO(x)` | `NULLIF(x, 0)` | `NULLIF(x, 0)` |
| `CSUM(col, order_col)` | `SUM(col) OVER (ORDER BY order_col ROWS UNBOUNDED PRECEDING)` | Same |
| `MAVG(col, n, order_col)` | `AVG(col) OVER (ORDER BY order_col ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` | Same |
| `date1 - date2` (integer days) | `DATEDIFF('day', date2, date1)` | `DATEDIFF(date1, date2)` |
| `NORMALIZE ON` | Manual period merge logic | Manual period merge logic |
| `HASHROW(x)` | `HASH(x)` | `HASH(x)` |
| `LIKE ANY (...)` | Multiple `LIKE` with `OR` | Multiple `LIKE` with `OR` |
| `LOCK ROW FOR ACCESS` | Not needed | Not needed |

### 2.3 Stored Procedure Decomposition

Each Teradata stored procedure maps to one or more dbt models:

```
sp_daily_transaction_load.sql
  ├── stg_customers.sql           (deduplication)
  ├── stg_transactions.sql        (cleansing)
  ├── int_daily_account_balances.sql (balance calc)
  └── fct_daily_transactions.sql  (final mart)

sp_customer_risk_scoring.sql
  ├── int_customer_risk_factors.sql (risk aggregation)
  └── fct_credit_risk_scores.sql    (scoring)

sp_regulatory_capital_calc.sql
  └── fct_regulatory_capital.sql    (Basel III RWA)

sp_aml_screening.sql
  ├── int_aml_screening_flags.sql   (flag generation)
  └── fct_aml_alerts.sql            (alert mart)

sp_monthly_pnl_rollup.sql
  └── fct_monthly_pnl.sql           (P&L rollup)
```

---

## Phase 3: Implementation

### 3.1 Staging Layer

1. Create `_staging__sources.yml` defining raw source tables
2. Create one staging model per source table
3. Apply deduplication, type casting, and cleansing
4. Add data freshness checks

### 3.2 Intermediate Layer

1. Decompose stored procedure logic into CTEs
2. Replace VOLATILE TABLEs with CTEs
3. Replace cursor loops with set-based operations
4. Use `{{ ref() }}` for all model references

### 3.3 Marts Layer

1. Create dimension and fact tables
2. Use incremental materialization where appropriate
3. Add surrogate keys with `dbt_utils.generate_surrogate_key`
4. Define tests in `_models.yml` files

### 3.4 Compatibility Macros

Create reusable macros for common Teradata patterns:
- `qualify_row_number()` - deduplication pattern
- `cast_teradata_date()` - date format conversion
- `zeroifnull()` / `nullifzero()` - null handling

---

## Phase 4: Data Validation

### 4.1 Row Count Reconciliation

Compare row counts between Teradata source and dbt target for every table.

### 4.2 Aggregate Validation

```sql
-- Teradata side
SELECT SUM(amount), COUNT(*), MIN(transaction_date), MAX(transaction_date)
FROM BARCLAYS_DWH.FCT_TRANSACTION;

-- dbt side (same query against Snowflake/Databricks)
SELECT SUM(amount), COUNT(*), MIN(transaction_date), MAX(transaction_date)
FROM {{ ref('fct_daily_transactions') }};
```

### 4.3 Sample Record Comparison

Select random samples and compare field-by-field between source and target.

### 4.4 dbt Tests

- Schema tests: `unique`, `not_null`, `accepted_values`, `relationships`
- Custom tests: `assert_positive_balances`, `assert_valid_risk_scores`
- dbt_expectations: statistical distribution tests

---

## Phase 5: Performance Testing

### 5.1 Benchmark Queries

Run the top-20 most expensive Teradata queries against both platforms and compare:
- Execution time
- Data scanned
- Cost

### 5.2 Incremental Load Testing

Simulate daily incremental loads and measure:
- Model build time
- Warehouse credit consumption (Snowflake) / DBU usage (Databricks)
- Data freshness SLA compliance

---

## Phase 6: Cutover Plan

### 6.1 Pre-Cutover Checklist

- [ ] All dbt models passing in CI
- [ ] Row count reconciliation within tolerance (< 0.01% variance)
- [ ] Aggregate validation within tolerance
- [ ] Performance benchmarks meet SLAs
- [ ] Downstream consumers tested against new platform
- [ ] Rollback procedure documented and tested

### 6.2 Cutover Sequence

1. Final full refresh of all dbt models
2. Run complete validation suite
3. Switch downstream consumers to new platform
4. Monitor for 24 hours
5. Decommission Teradata scheduled jobs
6. Archive Teradata data (retain for 90 days minimum)

### 6.3 Rollback Procedure

If issues are detected within the monitoring window:
1. Revert downstream consumer connections to Teradata
2. Re-enable Teradata scheduled jobs
3. Investigate and resolve dbt issues
4. Re-attempt cutover
