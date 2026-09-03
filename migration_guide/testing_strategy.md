# Testing Strategy for Teradata-to-dbt Migration

## Overview

A robust testing strategy is critical to ensure data integrity when migrating from
Teradata to dbt. **Databricks is the primary target**, so the verification described here
is Databricks-specific: parity against the Teradata extract, decimal-precision drift,
running dbt tests on a SQL warehouse, and keeping CI meaningful without a live workspace.

Three layers, each catching a different class of defect:

| Layer | Catches | Runs where |
|---|---|---|
| Compile-time checks | Snowflake-only syntax, broken refs, bad Jinja | Any machine, no warehouse |
| Functional tests (Postgres CI) | Logic errors, broken joins, failed constraints | GitHub Actions, no cloud credentials |
| Platform parity (Databricks) | Dialect behaviour, decimal drift, physical-layout regressions | A Databricks SQL warehouse |

---

## 1. dbt Schema Tests

### 1.1 Built-in Tests

Apply these to every model in `_models.yml` files:

```yaml
columns:
  - name: customerId
    tests:
      - unique
      - not_null
  - name: kycStatus
    tests:
      - accepted_values:
          values: ['VERIFIED', 'PENDING', 'EXPIRED', 'FAILED']
  - name: accountId
    tests:
      - relationships:
          to: ref('stg_accounts')
          field: accountId
```

Unity Catalog primary/foreign key constraints are informational only (`NOT ENFORCED`), and
Delta does not enforce row uniqueness the way a Teradata `SET` table did. The `unique` and
`relationships` tests are therefore the only thing standing in for constraints that the
legacy platform enforced at write time — they are mandatory on every key column, not
optional.

### 1.2 dbt_expectations Tests

Use the `dbt_expectations` package for statistical and distribution tests:

```yaml
columns:
  - name: amount
    tests:
      - dbt_expectations.expect_column_values_to_be_between:
          min_value: 0
          max_value: 10000000
      - dbt_expectations.expect_column_mean_to_be_between:
          min_value: 100
          max_value: 50000
  - name: transactionDate
    tests:
      - dbt_expectations.expect_column_values_to_be_of_type:
          column_type: date
```

Two Databricks-specific additions:

```yaml
  # VARCHAR(n) becomes unbounded STRING; length limits are no longer enforced.
  - name: customerId
    tests:
      - dbt_expectations.expect_column_value_lengths_to_be_between:
          min_value: 1
          max_value: 10

  # Guard the decimal type itself, not just the values.
  - name: amount
    tests:
      - dbt_expectations.expect_column_values_to_be_of_type:
          column_type: decimal(18,2)
```

---

## 2. Custom Data Tests

### 2.1 Business Rule Tests

Place in `dbt_project/tests/`:

```sql
-- tests/assert_positive_balances.sql
-- Savings accounts should never have negative balances
SELECT accountId, balanceDate, closingBalance
FROM {{ ref('int_daily_account_balances') }} b
INNER JOIN {{ ref('stg_accounts') }} a
    ON b.accountId = a.accountId
WHERE a.accountType = 'SAVINGS'
  AND b.closingBalance < 0
```

```sql
-- tests/assert_valid_risk_scores.sql
-- PD should be between 0 and 1, LGD between 0 and 1
SELECT customerId, probabilityOfDefault, lossGivenDefault
FROM {{ ref('fct_credit_risk_scores') }}
WHERE probabilityOfDefault < 0 OR probabilityOfDefault > 1
   OR lossGivenDefault < 0 OR lossGivenDefault > 1
```

### 2.2 Reconciliation Tests

Create a custom macro for row count reconciliation:

```sql
-- tests/reconcile_transaction_counts.sql
-- Compare expected vs actual transaction counts by date
WITH expectedCounts AS (
    SELECT transactionDate, COUNT(*) AS expectedRows
    FROM {{ source('barclays_raw', 'transaction') }}
    GROUP BY transactionDate
),
actualCounts AS (
    SELECT transactionDate, COUNT(*) AS actualRows
    FROM {{ ref('fct_daily_transactions') }}
    GROUP BY transactionDate
)
SELECT
    e.transactionDate,
    e.expectedRows,
    a.actualRows,
    ABS(e.expectedRows - COALESCE(a.actualRows, 0)) AS variance
FROM expectedCounts e
LEFT JOIN actualCounts a ON e.transactionDate = a.transactionDate
WHERE ABS(e.expectedRows - COALESCE(a.actualRows, 0)) > 0
```

### 2.3 Null-Where-Teradata-Had-Values Tests

Under Databricks' default non-ANSI mode, a decimal overflow returns `NULL` rather than
raising. A silent `NULL` in a monetary column is the highest-risk failure mode in this
migration, so assert against it explicitly on every computed money column:

```sql
-- tests/assert_no_null_monetary_values.sql
SELECT 'fct_daily_transactions' AS model, transactionId AS keyValue
FROM {{ ref('fct_daily_transactions') }}
WHERE amount IS NULL
UNION ALL
SELECT 'fct_monthly_pnl', CAST(reportingMonth AS STRING)
FROM {{ ref('fct_monthly_pnl') }}
WHERE netPnl IS NULL
```

Better still, run the Databricks target with `spark.sql.ansi.enabled = true` so overflow
fails the build instead of producing a null.

---

## 3. Source Freshness Tests

Define in `_staging__sources.yml`:

```yaml
sources:
  - name: barclaysRaw
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: etlLoadedTs
```

Run with: `dbt source freshness`

On Databricks, `loaded_at_field` must be a `TIMESTAMP`, and the session time zone affects
the comparison. Set `spark.sql.session.timeZone = 'UTC'` on the warehouse so freshness
windows do not shift by the local offset (and so they match the Teradata extract's
timestamps during the parallel run).

---

## 4. Snapshot Validation

### 4.1 SCD Type 2 Accuracy

Verify that the `snap_customer_risk_rating` snapshot correctly tracks changes:

```sql
-- Ensure no overlapping validity periods
SELECT customerId, COUNT(*)
FROM {{ ref('snap_customer_risk_rating') }}
WHERE dbtValidTo IS NULL
GROUP BY customerId
HAVING COUNT(*) > 1
```

### 4.2 History Completeness

```sql
-- Every customer should have at least one snapshot record
SELECT c.customerId
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('snap_customer_risk_rating') }} s
    ON c.customerId = s.customerId
WHERE s.customerId IS NULL
```

### 4.3 Snapshot Behaviour on Delta

dbt snapshots use `MERGE INTO` on Databricks. Two failure modes to test for:

- **Duplicate source keys** cause
  `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`. Add a `unique` test on the
  snapshot's `unique_key` against its source model so this fails as a test rather than a
  cryptic runtime error.
- **`check` strategy and type changes.** If a checked column's type changes (for example
  `DECIMAL(18,2)` to `DECIMAL(18,4)`), every row appears changed and the snapshot doubles.
  Assert the row count growth per run is bounded.

---

## 5. Databricks Parity Verification

This section is the core of migration testing: proving the Databricks output equals the
Teradata output.

### 5.1 Row-Count Parity

Extract counts from Teradata per table per business date:

```sql
-- Teradata
SELECT CAST(transaction_date AS DATE) AS business_date, COUNT(*) AS row_count
FROM BARCLAYS_DWH.FCT_TRANSACTION
GROUP BY 1;
```

```sql
-- Databricks
SELECT transaction_date AS business_date, COUNT(*) AS row_count
FROM barclays_migration.finance.fct_daily_transactions
GROUP BY 1;
```

Land the Teradata side in `barclays_migration.parallel_run.td_row_counts` and compare in
SQL, so the history of drift is queryable rather than living in a log file:

```sql
SELECT
    COALESCE(t.business_date, d.business_date) AS business_date,
    t.row_count AS teradata_rows,
    d.row_count AS databricks_rows,
    COALESCE(d.row_count, 0) - COALESCE(t.row_count, 0) AS delta
FROM barclays_migration.parallel_run.td_row_counts t
FULL OUTER JOIN (
    SELECT transaction_date AS business_date, COUNT(*) AS row_count
    FROM barclays_migration.finance.fct_daily_transactions GROUP BY 1
) d ON t.business_date = d.business_date
WHERE COALESCE(t.row_count, 0) <> COALESCE(d.row_count, 0);
```

Tolerance: **exact** for dimensions and for any completed business date. A non-zero delta
on the current date usually means the extract cut mid-day, not a defect — pin the
comparison to closed business dates only.

### 5.2 Checksum Parity

Never compare `HASHROW()` to `HASH()`: the algorithms differ, so equal data produces
different values. Use a platform-neutral checksum over normalised business columns.

Normalisation rules (apply identically on both sides):

1. `TRIM` every character column (Teradata `CHAR` pads; Databricks `STRING` does not).
2. Format decimals to a fixed scale: `CAST(amount AS DECIMAL(18,2))` then to string.
3. Format dates as `yyyy-MM-dd` and timestamps as `yyyy-MM-dd HH:mm:ss` in UTC.
4. Replace `NULL` with a sentinel (`'~'`) so `NULL` and `''` do not collide.
5. Concatenate with a delimiter that cannot appear in the data (`'|'`).

```sql
-- Databricks
SELECT
    transaction_date,
    COUNT(*) AS row_count,
    SUM(CAST(CONV(SUBSTR(SHA2(row_repr, 256), 1, 15), 16, 10) AS DECIMAL(38,0))) AS checksum
FROM (
    SELECT
        transaction_date,
        CONCAT_WS('|',
            COALESCE(TRIM(transaction_id), '~'),
            COALESCE(TRIM(account_id), '~'),
            COALESCE(CAST(CAST(amount AS DECIMAL(18,2)) AS STRING), '~'),
            COALESCE(DATE_FORMAT(transaction_date, 'yyyy-MM-dd'), '~')
        ) AS row_repr
    FROM barclays_migration.finance.fct_daily_transactions
)
GROUP BY transaction_date;
```

```sql
-- Teradata (equivalent shape; SHA-256 via the hash_md5/hash_sha UDF or td_sysfnlib)
SELECT
    transaction_date,
    COUNT(*) AS row_count,
    SUM(CAST(...same digest, decimalised... AS DECIMAL(38,0))) AS checksum
FROM BARCLAYS_DWH.FCT_TRANSACTION
GROUP BY transaction_date;
```

Summing the row digests makes the aggregate order-independent, which matters because
Databricks gives no ordering guarantee across files. An `XOR` aggregate is an alternative
but hides duplicate rows (a duplicated row XORs back out) — prefer `SUM`, and always
compare `COUNT(*)` alongside the checksum.

When a checksum differs, isolate the column by re-running the digest one column at a time;
in practice the offender is almost always a decimal scale or a trailing-blank difference.

### 5.3 Decimal Precision Drift

The most common source of "almost equal" results. Test each of these explicitly:

| Drift source | Symptom | Test |
|---|---|---|
| Division scale promotion | Small differences in ratio columns | Compare `ratio` columns to 6 dp, and assert the column's declared type is the intended `DECIMAL(p,s)` |
| Inferred `DOUBLE` from ingestion | Differences appearing at the 15th significant digit | `information_schema.columns` assertion that no monetary column is `DOUBLE` |
| Non-ANSI overflow | `NULL` where Teradata had a value | §2.3 test; enable ANSI mode |
| `SUM` over `DECIMAL(18,2)` promoting to `DECIMAL(38,2)` | Aggregate overflow to `NULL` at large volume | `not_null` on aggregate columns in the mart |
| Rounding mode | Off-by-0.01 on half values | Compare `ROUND(x, 2)` outputs for a synthetic set of `.005` values |
| Integer division | Truncation lost (`/` returns DOUBLE in Spark) | Grep models for `/` between integer columns; use `DIV` |

Type assertion as a dbt test:

```sql
-- tests/assert_monetary_columns_are_decimal.sql
SELECT table_name, column_name, data_type
FROM {{ target.catalog }}.information_schema.columns
WHERE table_schema IN ('finance', 'risk', 'compliance')
  AND (column_name LIKE '%amount%' OR column_name LIKE '%balance%' OR column_name LIKE '%pnl%')
  AND data_type NOT LIKE 'decimal%'
```

Aggregate comparison should be exact to the declared scale:

```sql
SELECT
    ABS(td.total_amount - dbx.total_amount) AS abs_diff
FROM ... 
-- Pass condition: abs_diff = 0 at DECIMAL(18,2). A "small tolerance" here hides real bugs.
```

### 5.4 Dialect Verification Without a Cluster

These catch Snowflake-only syntax before it reaches a warehouse:

```bash
# 1. Parse: catches refs, Jinja, macro errors. No connection required.
dbt parse

# 2. Compile against the Databricks target and inspect the SQL, not the results.
dbt compile --target databricks

# 3. Grep compiled SQL for constructs Databricks does not support.
grep -riE '\bqualify\b|\blike any\b|\blike all\b|zeroifnull|nullifzero|\biff\(|\bnvl2\(|listagg|within group' \
  target/compiled/barclays_migration/ && echo 'FAIL: Snowflake-only syntax found'
```

The full list of constructs to grep for is the review checklist in
`teradata_to_databricks_syntax.md` §8. Wiring that grep into CI gives most of the value of
a live Databricks connection at none of the cost.

---

## 6. Running dbt Tests on a SQL Warehouse

```bash
dbt build --target databricks              # run + test in DAG order
dbt test  --target databricks --store-failures
```

Practical notes:

- **Store failures.** `--store-failures` writes failing rows to
  `<catalog>.<schema>_dbt_test__audit`, which is the difference between "12 rows failed"
  and being able to query which twelve. Grant the team `SELECT` on that schema.
- **Warehouse sizing.** Tests are many small queries; a 2X-Small serverless warehouse with
  `threads: 8` beats a larger warehouse with `threads: 4`. Test concurrency is bounded by
  the warehouse's query concurrency, not its size.
- **Auto-stop.** A long `dbt build` on a warehouse with a 1-minute auto-stop can hit
  `Invalid SessionHandle`. Use 10 minutes for interactive work.
- **Selection.** `dbt build --select state:modified+ --defer --state ./prod-artifacts`
  builds only what changed and reads the rest from production — the cheapest way to test a
  PR against real data.
- **Severity.** Set `severity: warn` on the freshness and distribution tests during the
  parallel run so drift is visible without blocking the pipeline; keep key and business-rule
  tests at `error`.
- **Cost.** Tag the warehouse and read DBU consumption from `system.billing.usage` so test
  runs are attributable.

---

## 7. CI/CD Testing

### 7.1 CI Without a Databricks Workspace

The GitHub Actions workflow (`.github/workflows/dbt_ci.yml`) runs against a Postgres
service container, not Databricks. This is deliberate: it needs no cloud credentials, no
warehouse cost, and no secret management on forks, and it still catches the majority of
defects (broken refs, logic errors, failed constraints).

What Postgres CI **does** verify:

- The project parses and compiles.
- Every model builds and every test passes on seeded data.
- Snapshots run.
- Documentation generates.

What Postgres CI **cannot** verify:

- Delta-specific configuration (`cluster_by`, `partition_by`, `file_format`, merge behaviour).
- Databricks dialect acceptance (Postgres accepts constructs Databricks rejects, and vice versa).
- Decimal promotion and overflow semantics.
- Unity Catalog three-level naming and grants.
- Physical-layout performance.

Close that gap in CI with two zero-credential steps:

```yaml
- name: Parse against the Databricks target
  working-directory: ./dbt_project
  run: dbt parse --target databricks   # requires dbt-databricks installed; no connection made

- name: Reject Snowflake-only syntax
  working-directory: ./dbt_project
  run: |
    if grep -riE '\bqualify\b|\blike any\b|zeroifnull|nullifzero|\biff\(|listagg' \
        target/compiled/barclays_migration/; then
      echo "Snowflake-only syntax reached the compiled output"; exit 1
    fi
```

`dbt parse` does not open a connection, so it runs on any runner. Anything requiring
execution semantics needs a real warehouse.

### 7.2 Optional Databricks CI Job

Where a workspace is available, add a second job — gated so it does not run on forks:

| Setting | Value |
|---|---|
| Trigger | `pull_request` from the same repository, plus nightly on the default branch |
| Auth | OAuth service principal (runbook §3.2), secrets in GitHub Environments |
| Catalog | `barclays_migration_ci`, dropped and recreated per run, or one schema per PR |
| Command | `dbt build --target databricks_ci --select state:modified+` |
| Warehouse | Serverless 2X-Small, 5-minute auto-stop |
| Failure mode | Non-blocking initially; promote to blocking once flake-free for two weeks |

Keep the Postgres job as the required check. The Databricks job is the deeper, slower
signal, not the gate.

### 7.3 PR-level Validation

Use `dbt build --select state:modified+` to only build and test models changed in the PR.

---

## 8. Test Execution Matrix

| Test Category | Frequency | Tool | Platform | Blocking? |
|---|---|---|---|---|
| `dbt parse` (both targets) | Every PR | dbt | None needed | Yes |
| Snowflake-only syntax grep | Every PR | grep on compiled SQL | None needed | Yes |
| Schema tests (unique, not_null) | Every PR | dbt test | Postgres CI | Yes |
| Accepted values | Every PR | dbt test | Postgres CI | Yes |
| Relationship tests | Every PR | dbt test | Postgres CI | Yes |
| Custom business rule tests | Every PR | dbt test | Postgres CI | Yes |
| Full build on modified models | Every PR (if a workspace exists) | dbt build | Databricks CI | No (initially) |
| Monetary column type assertion | Every PR | dbt test | Databricks | Yes |
| Source freshness | Hourly (production) | dbt source freshness | Databricks | Warning |
| Row count parity vs Teradata | Daily (parallel run) | SQL in `parallel_run` schema | Databricks | Yes |
| Checksum parity vs Teradata | Daily (parallel run) | SQL in `parallel_run` schema | Databricks | Yes |
| Aggregate reconciliation | Daily (parallel run) | Custom SQL | Databricks | Yes |
| Row-level sample diff | Weekly (parallel run) | Script | Databricks | No |
| Decimal drift sweep | Weekly (parallel run) | Custom SQL | Databricks | Yes |
| Performance benchmarks | Weekly | Query history + script | Databricks | No |
| Snapshot accuracy | Daily | dbt test | Databricks | Yes |

---

## 9. Devin-Assisted Testing

Devin can automate many testing tasks:

1. **Generate schema tests** - Analyse column metadata and auto-generate `_models.yml` tests
2. **Create reconciliation queries** - Build source-to-target comparison queries
3. **Identify missing tests** - Scan models and flag untested columns
4. **Fix failing tests** - Diagnose test failures and suggest fixes
5. **Dialect review** - Check models against the Snowflake-vs-Databricks checklist
6. **Performance analysis** - Profile query plans and recommend optimisations
