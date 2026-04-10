/*
 * Phase 8 Validation: Encounter Grouping (DRIFT-002)
 *
 * Validates that the unified model correctly merges overlapping date ranges
 * into single encounters using gap-and-island detection.
 *
 * The Teradata naive GROUP BY approach would produce MORE encounters than
 * the correct algorithm. This test verifies that overlapping claims are
 * properly merged.
 *
 * Expected: NO rows returned (all overlapping claims should be in same encounter)
 */

-- Test: No two claims for the same member with overlapping date ranges
-- should be in different encounters
with claim_pairs as (
    select
        a.person_id,
        a.claim_id as claim_a_id,
        a.claim_start_date as a_start,
        a.claim_end_date as a_end,
        b.claim_id as claim_b_id,
        b.claim_start_date as b_start,
        b.claim_end_date as b_end
    from {{ ref('int_medical_claim_adr_deduped') }} a
    inner join {{ ref('int_medical_claim_adr_deduped') }} b
        on a.person_id = b.person_id
       and a.claim_id < b.claim_id
       -- Overlapping date ranges
       and a.claim_start_date <= coalesce(b.claim_end_date, b.claim_start_date)
       and b.claim_start_date <= coalesce(a.claim_end_date, a.claim_start_date)
    where upper(a.claim_status) in ('PAID', 'ADJUSTED')
      and upper(b.claim_status) in ('PAID', 'ADJUSTED')
),

encounter_assignments as (
    select
        cp.*,
        ea.encounter_id as encounter_a,
        eb.encounter_id as encounter_b
    from claim_pairs cp
    left join {{ ref('int_encounter_grouped') }} ea
        on cp.person_id = ea.person_id
       and cp.a_start >= ea.encounter_start_date
       and cp.a_start <= ea.encounter_end_date
    left join {{ ref('int_encounter_grouped') }} eb
        on cp.person_id = eb.person_id
       and cp.b_start >= eb.encounter_start_date
       and cp.b_start <= eb.encounter_end_date
)

-- Return any overlapping claim pairs that ended up in different encounters
-- (this would indicate the naive Teradata approach was used instead of gap-and-island)
select
    person_id,
    claim_a_id,
    a_start,
    a_end,
    claim_b_id,
    b_start,
    b_end,
    encounter_a,
    encounter_b,
    'FAIL: Overlapping claims assigned to different encounters (naive grouping detected)' as failure_reason
from encounter_assignments
where encounter_a != encounter_b
