/*
 * Phase 8 Validation: ADR Dedup Priority Order (DRIFT-001)
 *
 * Validates that the unified model uses the correct CMS ADR priority:
 *   PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4
 *
 * This test confirms that when a claim exists in multiple statuses,
 * the correct status is retained per CMS standards.
 *
 * Expected: NO rows returned (all claims should have correct priority)
 *
 * Legacy platform deviations being validated against:
 *   Teradata:  PAID=1, DENIED=2, ADJUSTED=3, REVERSED=4 (WRONG)
 *   Snowflake: PAID=1, ADJUSTED=2, REVERSED=3, DENIED=4 (WRONG)
 *   Databricks: PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4 (CORRECT)
 */

-- Test: No claim should have a DENIED status retained when an ADJUSTED
-- version of the same claim exists (Teradata bug)
select
    claim_id,
    claim_line_number,
    claim_status,
    adr_priority_score,
    source_platform,
    'FAIL: Denied claim retained when Adjusted version exists' as failure_reason
from {{ ref('int_medical_claim_adr_deduped') }}
where upper(claim_status) = 'DENIED'
  and exists (
    select 1
    from {{ ref('stg_teradata__medical_claim') }} td
    where td.claim_id = {{ ref('int_medical_claim_adr_deduped') }}.claim_id
      and td.claim_line_number = {{ ref('int_medical_claim_adr_deduped') }}.claim_line_number
      and upper(td.claim_status) = 'ADJUSTED'
  )

union all

-- Test: No claim should have REVERSED status retained when DENIED exists
-- (Snowflake bug)
select
    claim_id,
    claim_line_number,
    claim_status,
    adr_priority_score,
    source_platform,
    'FAIL: Reversed claim retained when Denied version exists' as failure_reason
from {{ ref('int_medical_claim_adr_deduped') }}
where upper(claim_status) = 'REVERSED'
  and exists (
    select 1
    from {{ ref('stg_snowflake__medical_claim') }} sf
    where sf.claim_id = {{ ref('int_medical_claim_adr_deduped') }}.claim_id
      and sf.claim_line_number = {{ ref('int_medical_claim_adr_deduped') }}.claim_line_number
      and upper(sf.claim_status) = 'DENIED'
  )
