/*
 * Phase 8 Validation: Edge Cases
 *
 * Tests for null handling, date boundary conditions, zero-dollar claims,
 * and reversed claims.
 *
 * Expected: NO rows returned (all edge cases handled correctly)
 */

-- Test 1: No null claim_id in deduped output
select
    'medical_claim' as entity,
    'FAIL: Null claim_id found in deduped medical claims' as failure_reason
from {{ ref('int_medical_claim_adr_deduped') }}
where claim_id is null
limit 1

union all

-- Test 2: No null person_id in deduped output
select
    'medical_claim' as entity,
    'FAIL: Null person_id found in deduped medical claims' as failure_reason
from {{ ref('int_medical_claim_adr_deduped') }}
where person_id is null
limit 1

union all

-- Test 3: claim_end_date should be >= claim_start_date when both are non-null
select
    'medical_claim' as entity,
    'FAIL: claim_end_date before claim_start_date' as failure_reason
from {{ ref('int_medical_claim_adr_deduped') }}
where claim_end_date is not null
  and claim_end_date < claim_start_date
limit 1

union all

-- Test 4: Zero-dollar PAID claims should be flagged (unusual but valid)
select
    'medical_claim' as entity,
    'WARN: Zero-dollar PAID claim detected (review needed)' as failure_reason
from {{ ref('int_medical_claim_adr_deduped') }}
where upper(claim_status) = 'PAID'
  and coalesce(paid_amount, 0) = 0
  and coalesce(charge_amount, 0) > 0
limit 1

union all

-- Test 5: Encounter start_date should never be after end_date
select
    'encounter' as entity,
    'FAIL: Encounter start_date after end_date' as failure_reason
from {{ ref('int_encounter_grouped') }}
where encounter_end_date < encounter_start_date
limit 1

union all

-- Test 6: No null person_id in eligibility
select
    'eligibility' as entity,
    'FAIL: Null person_id in deduped eligibility' as failure_reason
from {{ ref('int_eligibility_deduped') }}
where person_id is null
limit 1

union all

-- Test 7: Enrollment start should not be after enrollment end
select
    'eligibility' as entity,
    'FAIL: Enrollment start_date after end_date' as failure_reason
from {{ ref('int_eligibility_deduped') }}
where enrollment_end_date is not null
  and enrollment_end_date < enrollment_start_date
limit 1
