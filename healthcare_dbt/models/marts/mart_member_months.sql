/*
 * Mart Model: Member Months with PMPM Metrics
 *
 * Combines member month enrollment with claims data to produce
 * Per Member Per Month (PMPM) cost metrics.
 *
 * Migration source:
 *   - teradata/claims/ddl/04_warehouse_tables.sql (CLAIMS_MART.MART_MEMBER_MONTHS)
 *   - databricks/ddl/04_warehouse_tables.sql (claims_mart.mart_member_months)
 *   - snowflake/ddl/04_warehouse_tables.sql (CLAIMS_DW.MART.MART_MEMBER_MONTHS)
 */

{{ config(
    materialized='table',
    tags=['marts', 'member_months', 'pmpm']
) }}

with member_months as (
    select * from {{ ref('int_member_month_enrollment') }}
),

medical_costs as (
    select
        person_id,
        date_trunc('month', claim_start_date)               as claim_month,
        sum(coalesce(paid_amount, 0))                       as medical_paid_amount,
        count(distinct claim_id)                            as medical_claim_count
    from {{ ref('int_medical_claim_adr_deduped') }}
    where upper(claim_status) in ('PAID', 'ADJUSTED')
    group by 1, 2
),

pharmacy_costs as (
    select
        person_id,
        date_trunc('month', dispensing_date)                as claim_month,
        sum(coalesce(paid_amount, 0))                       as pharmacy_paid_amount,
        count(distinct claim_id)                            as pharmacy_claim_count
    from {{ ref('int_pharmacy_claim_adr_deduped') }}
    where upper(claim_status) in ('PAID', 'ADJUSTED')
    group by 1, 2
),

encounter_counts as (
    select
        person_id,
        date_trunc('month', encounter_start_date)           as encounter_month,
        count(*)                                            as encounter_count,
        sum(case when encounter_type = 'inpatient' then 1 else 0 end)
                                                            as inpatient_count,
        sum(case when encounter_type = 'emergency' then 1 else 0 end)
                                                            as ed_count
    from {{ ref('int_encounter_grouped') }}
    group by 1, 2
)

select
    mm.person_id,
    mm.enrollment_month,
    mm.enrollment_year,
    mm.enrollment_month_num,
    mm.payer,
    mm.plan,
    mm.line_of_business,
    mm.gender,
    mm.member_age,
    mm.state,
    mm.zip_code,

    -- Medical costs
    coalesce(mc.medical_paid_amount, 0)                     as medical_paid_amount,
    coalesce(mc.medical_claim_count, 0)                     as medical_claim_count,

    -- Pharmacy costs
    coalesce(rx.pharmacy_paid_amount, 0)                    as pharmacy_paid_amount,
    coalesce(rx.pharmacy_claim_count, 0)                    as pharmacy_claim_count,

    -- Total costs
    coalesce(mc.medical_paid_amount, 0)
        + coalesce(rx.pharmacy_paid_amount, 0)              as total_paid_amount,

    -- Encounter metrics
    coalesce(ec.encounter_count, 0)                         as encounter_count,
    coalesce(ec.inpatient_count, 0)                         as inpatient_count,
    coalesce(ec.ed_count, 0)                                as ed_count,

    mm.data_source,
    current_timestamp()                                     as etl_load_timestamp

from member_months mm
left join medical_costs mc
    on mm.person_id = mc.person_id
   and mm.enrollment_month = mc.claim_month
left join pharmacy_costs rx
    on mm.person_id = rx.person_id
   and mm.enrollment_month = rx.claim_month
left join encounter_counts ec
    on mm.person_id = ec.person_id
   and mm.enrollment_month = ec.encounter_month
