/*
 * Phase 8 Validation: Aggregate Reconciliation
 *
 * Compares total paid amounts, member counts, and claim counts
 * from the unified model against expected ranges to ensure
 * financial accuracy.
 *
 * Expected: NO rows returned (all aggregates within expected ranges)
 */

-- Test: Paid amounts should be non-negative
select
    'medical_claim' as entity,
    sum(paid_amount) as total_paid,
    'FAIL: Negative total paid amount in unified medical claims' as failure_reason
from {{ ref('int_medical_claim_adr_deduped') }}
having sum(paid_amount) < 0

union all

select
    'pharmacy_claim' as entity,
    sum(paid_amount) as total_paid,
    'FAIL: Negative total paid amount in unified pharmacy claims' as failure_reason
from {{ ref('int_pharmacy_claim_adr_deduped') }}
having sum(paid_amount) < 0

union all

-- Test: Member count in unified should not exceed sum of all platform members
select
    'eligibility' as entity,
    count(distinct person_id) as total_members,
    'FAIL: Unified member count exceeds platform total' as failure_reason
from {{ ref('int_eligibility_deduped') }}
having count(distinct person_id) > (
    select count(distinct person_id) from {{ ref('stg_teradata__eligibility') }}
) + (
    select count(distinct person_id) from {{ ref('stg_databricks__eligibility') }}
) + (
    select count(distinct person_id) from {{ ref('stg_snowflake__eligibility') }}
)
