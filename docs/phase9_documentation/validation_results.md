# Phase 8/9: Validation Results

## Overview

This document summarizes the validation test suite built to prove the migrated pipeline produces equivalent (or corrected) results compared to the legacy platforms. The test suite covers six categories as specified in Phase 8.

## Test Suite Summary

| Category | Tests | Status | Notes |
|----------|-------|--------|-------|
| Row Count Reconciliation | 1 | Ready | Validates unified count ≤ sum of platform counts |
| Aggregate Reconciliation | 3 | Ready | Financial totals, member counts, claim counts |
| ADR Dedup Validation | 2 | Ready | DRIFT-001 priority correction verified |
| Encounter Grouping Validation | 1 | Ready | DRIFT-002 gap-and-island algorithm verified |
| Edge Case Tests | 7 | Ready | Nulls, date boundaries, zero-dollar, reversals |
| Tuva DQ Tests | 600+ | Ready | Via `tuva-health/tuva` package dependency |

**Total custom tests: 14**
**Total with Tuva DQ: 614+**

---

## 1. Row Count Reconciliation

**Test:** `healthcare_dbt/tests/validation_row_count_reconciliation.sql`

Validates that the unified model's record count does not exceed the sum of all platform staging counts (which would indicate duplication bugs).

**Expected behavior:**
- Unified medical claim count ≤ (Teradata + Databricks + Snowflake) medical claim count
- Unified eligibility count ≤ (Teradata + Databricks + Snowflake) eligibility count
- Unified pharmacy claim count ≤ (Teradata + Databricks + Snowflake) pharmacy claim count

**Note:** Unified count will be LESS than the sum due to cross-platform deduplication (same claim existing on multiple platforms).

---

## 2. Aggregate Reconciliation

**Test:** `healthcare_dbt/tests/validation_aggregate_reconciliation.sql`

Validates financial and demographic aggregates:

| Check | Assertion |
|-------|-----------|
| Medical paid amounts | Total ≥ 0 (no negative totals) |
| Pharmacy paid amounts | Total ≥ 0 (no negative totals) |
| Member count | Unified unique members ≤ sum of platform unique members |

---

## 3. ADR Dedup Validation (DRIFT-001)

**Test:** `healthcare_dbt/tests/validation_adr_dedup_priority.sql`

Validates that the CMS-correct ADR priority is applied:

| Priority | Status | Score |
|----------|--------|-------|
| 1 (Highest) | PAID | 1 |
| 2 | ADJUSTED | 2 |
| 3 | DENIED | 3 |
| 4 (Lowest) | REVERSED | 4 |

**Specific checks:**
1. No DENIED claim should be retained when an ADJUSTED version of the same claim exists (validates Teradata bug is fixed)
2. No REVERSED claim should be retained when a DENIED version exists (validates Snowflake bug is fixed)

---

## 4. Encounter Grouping Validation (DRIFT-002)

**Test:** `healthcare_dbt/tests/validation_encounter_grouping.sql`

Validates that overlapping date ranges are correctly merged into single encounters:

- Identifies all claim pairs for the same member with overlapping date ranges
- Verifies both claims are assigned to the same encounter
- **FAIL condition:** Overlapping claims in different encounters (would indicate naive GROUP BY was used instead of gap-and-island)

---

## 5. Edge Case Tests

**Test:** `healthcare_dbt/tests/validation_edge_cases.sql`

| # | Check | Severity |
|---|-------|----------|
| 1 | No null claim_id in deduped medical claims | FAIL |
| 2 | No null person_id in deduped medical claims | FAIL |
| 3 | claim_end_date ≥ claim_start_date (when both non-null) | FAIL |
| 4 | Zero-dollar PAID claims flagged for review | WARN |
| 5 | Encounter start_date ≤ end_date | FAIL |
| 6 | No null person_id in eligibility | FAIL |
| 7 | Enrollment start_date ≤ end_date | FAIL |

---

## 6. Tuva Data Quality Tests

The Tuva package (`tuva-health/tuva >= 0.9.0`) includes 600+ data quality tests that automatically run against the input layer models:

- **Eligibility tests:** Valid date ranges, required fields, valid gender/state codes
- **Medical claim tests:** Valid claim types, NPI format, ICD-10 code validation, revenue code validation
- **Pharmacy claim tests:** Valid NDC codes, prescriber NPI format, quantity/days_supply validation
- **Referential integrity:** Claim members exist in eligibility, diagnosis codes exist in terminology

These tests are invoked via `dbt test` and leverage the Tuva package's built-in test definitions.

---

## Drift Resolution Verification Matrix

| Drift ID | Finding | Legacy Wrong | Unified Correct | Test Validates |
|----------|---------|--------------|-----------------|----------------|
| DRIFT-001 | ADR Priority | Teradata, Snowflake | CMS standard (PAID>ADJ>DENIED>REV) | `validation_adr_dedup_priority.sql` |
| DRIFT-002 | Encounter Grouping | Teradata (naive) | Gap-and-island | `validation_encounter_grouping.sql` |
| DRIFT-003 | Diagnosis Storage | N/A (structural) | 25 individual columns | Schema tests in staging YMLs |
| DRIFT-004 | PHI Masking | Teradata (post-mart) | Dynamic Data Masking | Manual verification required |
| DRIFT-005 | Member Dedup | Teradata (non-deterministic) | enrollment_start DESC, timestamp DESC | `int_eligibility_deduped` schema tests |
| DRIFT-006 | Financial Types | Databricks (DOUBLE) | NUMBER(18,2) | Schema tests in staging YMLs |
| DRIFT-007 | Claim Type Encoding | All different | Tuva lowercase standard | `accepted_values` tests in staging YMLs |

---

## Running the Full Test Suite

```bash
# Run all dbt tests (custom + Tuva DQ)
dbt test

# Run only custom validation tests
dbt test --select test_type:singular

# Run only Tuva DQ tests
dbt test --select tuva

# Run specific validation test
dbt test --select validation_adr_dedup_priority
```
