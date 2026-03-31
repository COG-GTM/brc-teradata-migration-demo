# Testing Strategy for Teradata-to-dbt Migration

## Overview

A robust testing strategy is critical to ensure data integrity when migrating from Teradata to dbt on Snowflake/Databricks. This document outlines the testing approach at every layer of the migration.

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

---

## 5. Performance Testing

### 5.1 Model Build Time Benchmarks

Track build times for each model:

```bash
dbt run --profiles-dir . --target snowflake 2>&1 | tee buildLog.txt
```

Parse the output to extract per-model timing:

```
OK created incremental model finance.fctDailyTransactions .... [SUCCESS 1 in 12.34s]
```

### 5.2 Query Performance Comparison

Run the same analytical queries against both Teradata and the new platform:

```sql
-- Example: Monthly transaction summary
SELECT
    DATE_TRUNC('month', transactionDate) AS month,
    COUNT(*) AS txnCount,
    SUM(amount) AS totalAmount,
    AVG(amount) AS avgAmount
FROM fctDailyTransactions
WHERE transactionDate >= '2024-01-01'
GROUP BY 1
ORDER BY 1;
```

Compare execution time, rows scanned, and cost.

---

## 6. CI/CD Testing

### 6.1 GitHub Actions Workflow

The `.github/workflows/dbt_ci.yml` runs on every PR:

1. `dbt deps` - Install packages
2. `dbt seed` - Load seed data
3. `dbt run` - Build all models
4. `dbt test` - Run all tests
5. `dbt source freshness` - Check source freshness

### 6.2 PR-level Validation

Use `dbt build --select state:modified+` to only build and test models changed in the PR.

---

## 7. Test Execution Matrix

| Test Category | Frequency | Tool | Blocking? |
|---|---|---|---|
| Schema tests (unique, not_null) | Every PR | dbt test | Yes |
| Accepted values | Every PR | dbt test | Yes |
| Relationship tests | Every PR | dbt test | Yes |
| Custom business rule tests | Every PR | dbt test | Yes |
| Source freshness | Hourly (production) | dbt source freshness | Warning |
| Row count reconciliation | Daily (migration) | Custom SQL | Yes |
| Aggregate reconciliation | Daily (migration) | Custom SQL | Yes |
| Sample record comparison | Weekly (migration) | Manual / script | No |
| Performance benchmarks | Weekly | Custom script | No |
| Snapshot accuracy | Daily | dbt test | Yes |

---

## 8. Devin-Assisted Testing

Devin can automate many testing tasks:

1. **Generate schema tests** - Analyse column metadata and auto-generate `_models.yml` tests
2. **Create reconciliation queries** - Build source-to-target comparison queries
3. **Identify missing tests** - Scan models and flag untested columns
4. **Fix failing tests** - Diagnose test failures and suggest fixes
5. **Performance analysis** - Profile query plans and recommend optimisations
