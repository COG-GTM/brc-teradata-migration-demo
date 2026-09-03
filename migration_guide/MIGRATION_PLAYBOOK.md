# Teradata-to-dbt Migration Playbook

## Overview

This playbook is the step-by-step guide for migrating a Barclays-style Teradata
on-premises banking data warehouse to dbt Core.

**Databricks is the primary target platform.** The default `target_platform` var in
`dbt_project.yml` is `databricks`, models are configured for Delta Lake, and the
operational path — Unity Catalog, SQL warehouses, service-principal auth, Workflows — is
described here. Snowflake remains a supported secondary target and is called out only
where the two diverge; Postgres is used for local development and CI.

Companion documents:

- `databricks_runbook.md` — zero to a green `dbt build --target databricks`, with
  environment variables, grants and troubleshooting.
- `teradata_to_databricks_syntax.md` — DDL, type, function and utility mappings, plus a
  Snowflake-vs-Databricks review checklist.
- `testing_strategy.md` — parity testing against Teradata and CI without a live workspace.
- `teradata_to_snowflake_syntax.md` — the secondary-target mapping.

---

## Phase 1: Discovery & Assessment

### 1.1 Schema Inventory

Catalogue every object in the Teradata environment:

| Object Type | Count Method | Notes |
|---|---|---|
| Databases | `SELECT DatabaseName FROM DBC.DatabasesV` | Map to Unity Catalog schemas |
| Tables | `SELECT TableName FROM DBC.TablesV WHERE TableKind = 'T'` | Identify SET vs MULTISET |
| Views | `SELECT TableName FROM DBC.TablesV WHERE TableKind = 'V'` | Candidates for staging models |
| Stored Procedures | `SELECT SPName FROM DBC.StoredProcsV` | Decompose into dbt models |
| Macros | `SELECT MacroName FROM DBC.MacrosV` | Convert to dbt macros |
| Indexes | `SELECT IndexName FROM DBC.IndicesV` | Review for Delta clustering columns |
| Partitioning | `SELECT * FROM DBC.IndexConstraintsV` | PPI definitions drive partition/cluster choice |
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

Volume drives two Databricks decisions: warehouse size for the backfill (runbook §4.2) and
whether a table needs `PARTITIONED BY` at all. Anything under ~10 GB should use liquid
clustering only.

---

## Phase 2: Construct Mapping

### 2.1 Table Migration

| Teradata Construct | dbt/Databricks (primary) | dbt/Snowflake (secondary) |
|---|---|---|
| `CREATE SET TABLE` | Delta table + deduplication in the model | Standard `CREATE TABLE` + dedup |
| `CREATE MULTISET TABLE` | Delta table | Standard `CREATE TABLE` |
| `PRIMARY INDEX (col)` | `CLUSTER BY (col)` (liquid clustering) | Cluster key suggestion |
| `PARTITION BY RANGE_N` | Coarsened `PARTITIONED BY`, usually replaced by clustering | Automatic micro-partitioning |
| `UNIQUE PRIMARY INDEX` | `CLUSTER BY` + dbt `unique` test | Cluster key + `unique` test |
| `FALLBACK` | Not needed (cloud storage durability) | Not needed |
| `JOURNAL` | Delta log; `DESCRIBE HISTORY`, `VERSION AS OF` | Time Travel / Fail-safe |
| `COMPRESS` | Automatic (Parquet encoding) | Automatic |
| `COLLECT STATISTICS` | `ANALYZE TABLE ... COMPUTE STATISTICS` after large loads | Not needed |
| `VOLATILE TABLE` | CTE or ephemeral dbt model | CTE or ephemeral dbt model |

Full mapping, including every physical-design clause: `teradata_to_databricks_syntax.md` §1.

### 2.2 SQL Construct Migration

| Teradata SQL | Databricks SQL (primary) | Snowflake SQL |
|---|---|---|
| `QUALIFY ROW_NUMBER() OVER (...) = 1` | Sub-query with `WHERE rn = 1` (`qualify_row_number()` macro) | Native `QUALIFY` |
| `ZEROIFNULL(x)` | `COALESCE(x, 0)` | `ZEROIFNULL(x)` or `COALESCE` |
| `NULLIFZERO(x)` | `NULLIF(x, 0)` | `NULLIF(x, 0)` |
| `CSUM(col, order_col)` | `SUM(col) OVER (ORDER BY order_col ROWS UNBOUNDED PRECEDING)` | Same |
| `MAVG(col, n, order_col)` | `AVG(col) OVER (ORDER BY order_col ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` | Same |
| `date1 - date2` (integer days) | `DATEDIFF(date1, date2)` | `DATEDIFF('day', date2, date1)` |
| `NORMALIZE ON` | Hand-written gaps-and-islands merge | Hand-written merge |
| `HASHROW(x)` | `HASH(x)` / `XXHASH64(x)` | `HASH(x)` |
| `HASHBUCKET(HASHROW(x))` | `ABS(HASH(x)) % n` (`hash_to_int_bucket()` macro) | Same shape |
| `LIKE ANY (...)` | Multiple `LIKE` with `OR` | Native `LIKE ANY` |
| `LOCK ROW FOR ACCESS` | Not needed | Not needed |
| `UPDATE ... FROM` | `MERGE INTO` | `UPDATE ... FROM` |
| `BEGIN/END TRANSACTION` | Not available — stage then single `MERGE` | Multi-statement transaction |

The four constructs above that are **native in Snowflake but absent in Databricks**
(`QUALIFY`, `LIKE ANY`/`LIKE ALL`, `ZEROIFNULL`/`NULLIFZERO`, `DATEDIFF` argument order)
account for most conversion defects. The full review checklist is
`teradata_to_databricks_syntax.md` §8.

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

Procedural constructs that have no dbt equivalent:

| Procedural construct | Replacement |
|---|---|
| Cursor loop | Set-based `SELECT` over the whole partition |
| `VOLATILE TABLE` scratch space | CTE, or an ephemeral intermediate model |
| `IF`/`WHILE` control flow | Model DAG ordering + `{% if %}` at compile time |
| Explicit commit points | One atomic Delta write per model |
| Error handlers writing to a log table | dbt tests + the Workflow's task failure semantics |

### 2.4 Load Utility Migration

| Teradata utility | Databricks replacement |
|---|---|
| FastLoad | `COPY INTO` from a UC volume or external location |
| MultiLoad | `MERGE INTO` on a Delta table |
| TPT Load | `COPY INTO`, or Auto Loader for continuous arrival |
| TPT Export / FastExport | Write to a volume, or Delta Sharing for Databricks consumers |
| BTEQ scripts | dbt models for the SQL; a Workflow SQL task for the operational steps |
| Error tables (`_ET`/`_UV`) | `badRecordsPath` surfaced as a Delta table |

Details and worked examples: `teradata_to_databricks_syntax.md` §6; the operational
procedure is runbook §8.

---

## Phase 3: Implementation

### 3.1 Target Platform Setup

Before any models are built, stand up the Databricks target as described in the runbook:

1. Unity Catalog: one catalog per environment, fixed schema names
   (`raw`, `seeds`, `staging`, `finance`, `risk`, `compliance`, `snapshots`) — runbook §2.
2. Auth: personal access token for development, OAuth service principal for CI and
   production — runbook §3.
3. Compute: serverless SQL warehouse, 2X-Small for development, sized up only for the
   historical backfill — runbook §4.
4. Confirm with `dbt debug --target databricks` before writing a line of SQL.

### 3.2 Staging Layer

1. Create `_staging__sources.yml` defining raw source tables
2. Create one staging model per source table
3. Apply deduplication (Teradata `SET` semantics), type casting, and cleansing
4. Cast decimals explicitly — do not inherit inferred `DOUBLE` from ingestion
5. Add data freshness checks

### 3.3 Intermediate Layer

1. Decompose stored procedure logic into CTEs
2. Replace VOLATILE TABLEs with CTEs
3. Replace cursor loops with set-based operations
4. Use `{{ ref() }}` for all model references

### 3.4 Marts Layer

1. Create dimension and fact tables
2. Use incremental materialization where appropriate; set `unique_key` so the Delta
   `MERGE` is deterministic
3. Translate the Teradata PI/PPI into `cluster_by` (and `partition_by` only where the
   table is genuinely large)
4. Add surrogate keys with `dbt_utils.generate_surrogate_key`
5. Define tests in `_models.yml` files

### 3.5 Compatibility Macros

Reusable macros for common Teradata patterns live in `dbt_project/macros/teradata_compat/`:

- `qualify_row_number()` — deduplication pattern (Databricks has no `QUALIFY`)
- `cast_teradata_date()` — `FORMAT 'YYYYMMDD'` to Java `yyyyMMdd` conversion
- `hash_to_int_bucket()` — cross-dialect `HASHBUCKET(HASHROW(...))` equivalent

Where a construct cannot be written once, branch on `target.type` inside the macro rather
than duplicating the model.

---

## Phase 4: Data Validation

Validation is specified in detail in `testing_strategy.md`; this is the sequence.

### 4.1 Row Count Reconciliation

Compare row counts between Teradata source and the Databricks target for every table,
per business date. Tolerance: exact match for dimensions, < 0.01% for facts during a
parallel run window.

### 4.2 Aggregate Validation

```sql
-- Teradata side
SELECT SUM(amount), COUNT(*), MIN(transaction_date), MAX(transaction_date)
FROM BARCLAYS_DWH.FCT_TRANSACTION;

-- Databricks side
SELECT SUM(amount), COUNT(*), MIN(transaction_date), MAX(transaction_date)
FROM barclays_migration.finance.fct_daily_transactions;
```

Sum monetary columns as `DECIMAL`, never `DOUBLE` — see the decimal caveats in
`teradata_to_databricks_syntax.md` §2.2.

### 4.3 Checksum Comparison

Row-level `SHA2` checksums over normalised business columns, aggregated per partition.
Platform-native hashes (`HASHROW` vs `HASH`) are **not** comparable across engines.

### 4.4 dbt Tests

- Schema tests: `unique`, `not_null`, `accepted_values`, `relationships`
- Custom tests: `assert_positive_balances`, `assert_valid_risk_scores`
- dbt_expectations: statistical distribution tests

---

## Phase 5: Performance Testing

### 5.1 Benchmark Queries

Run the top-20 most expensive Teradata queries against Databricks and compare:
- Execution time (from `system.query.history` or the warehouse's Query History)
- Bytes scanned / files pruned
- DBU cost

### 5.2 Incremental Load Testing

Simulate daily incremental loads and measure:
- Model build time per model (`dbt run` timings, or `run_results.json`)
- DBU consumption at the chosen warehouse size
- Data freshness SLA compliance

### 5.3 Physical Layout Tuning

Re-check after the first realistic-volume load:
- Are date filters pruning files? (`EXPLAIN FORMATTED`, files-scanned in query history)
- Is `OPTIMIZE` needed on a cadence, and is the file count converging?
- Did an over-eager `PARTITIONED BY` produce many small files? Prefer `CLUSTER BY`.

---

## Phase 6: Parallel Run

Run Teradata and Databricks side by side before cutting over.

### 6.1 Setup

1. Keep the Teradata batch running unchanged on its existing schedule.
2. Feed the same daily extract into the Databricks `raw` schema.
3. Run the dbt job after the Teradata batch completes, against the same business date.
4. Land both sides' reconciliation output in a `parallel_run` schema so drift is queryable
   over time, not just visible in a log.

### 6.2 Daily Comparison

| Check | Scope | Tolerance |
|---|---|---|
| Row counts per table per business date | All marts | Exact |
| Aggregate sums of monetary columns | All facts | Exact to 2 dp |
| Checksum by partition | All marts | Exact |
| Row-level diff of a sampled partition | One mart per day, rotating | Exact |
| Regulatory outputs (capital, AML alerts) | Every run | Exact |

### 6.3 Exit Criteria

Two consecutive weeks with:

- Zero unexplained row-count or checksum differences.
- Every explained difference documented with a signed-off reason (for example, a corrected
  rounding defect in the legacy procedure).
- Build times within the batch window at production volume.
- Downstream consumers validated against the Databricks marts.

---

## Phase 7: Cutover Plan

### 7.1 Pre-Cutover Checklist

- [ ] All dbt models passing in CI
- [ ] Parallel-run exit criteria met (Phase 6.3)
- [ ] Row count reconciliation within tolerance (< 0.01% variance)
- [ ] Aggregate and checksum validation clean
- [ ] Performance benchmarks meet SLAs at production warehouse size
- [ ] Unity Catalog grants reviewed: consumers have `SELECT` only on the mart schemas
- [ ] dbt job runs as a service principal, not a personal token
- [ ] Downstream consumers (BI, regulatory extracts, downstream feeds) tested against
      Databricks
- [ ] Rollback procedure documented and tested

### 7.2 Cutover Sequence

1. Freeze changes to the Teradata procedures.
2. Final full refresh of all dbt models (`dbt build --full-refresh --target databricks_prod`).
3. Run the complete validation suite; record the Delta versions of every mart
   (`DESCRIBE HISTORY`) as the cutover baseline.
4. Repoint downstream consumers to the Databricks marts.
5. Monitor for 24 hours: job success, freshness, test results, query error rates.
6. Disable the Teradata scheduled jobs (disable, do not delete).
7. Archive the Teradata data (retain for 90 days minimum) and keep read-only access for
   the rollback window.

### 7.3 Rollback Procedure

If issues are detected within the monitoring window:

1. Repoint downstream consumers back to Teradata.
2. Re-enable the Teradata scheduled jobs.
3. If a Databricks table was corrupted rather than merely wrong upstream, restore it:
   `RESTORE TABLE <table> TO VERSION AS OF <cutover_version>`.
4. Investigate and resolve the dbt issue; add a test that would have caught it.
5. Re-attempt cutover after a further clean parallel-run week.

### 7.4 Decommission

Only after 90 days of clean production running:

1. Delete the Teradata scheduled job definitions.
2. Release the Teradata licences and hardware.
3. Move the archived extracts to cold storage; keep the schema DDL in this repo under
   `teradata/` as the system of record for the legacy semantics.
