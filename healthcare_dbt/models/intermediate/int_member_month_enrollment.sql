/*
 * Intermediate Model: Member Month Enrollment
 *
 * Generates one row per member per month of enrollment, used for
 * PMPM calculations and member month denominators.
 *
 * Migration source:
 *   - teradata/claims/stored_procedures/sp_member_month_enrollment.sql
 *     (originally used Teradata EXPAND ON syntax)
 *   - databricks/notebooks/06_member_month_enrollment.py
 *   - snowflake/stored_procedures/sp_member_month_enrollment.sql
 *
 * All three platforms implement equivalent logic; no drift detected.
 * The Teradata EXPAND ON syntax is replaced with a date spine approach.
 */

{{ config(
    materialized='table',
    tags=['intermediate', 'member_month']
) }}

with eligibility as (
    select
        person_id,
        enrollment_start_date,
        coalesce(enrollment_end_date, current_date())       as enrollment_end_date,
        payer,
        plan,
        line_of_business,
        gender,
        birth_date,
        state,
        zip_code,
        data_source
    from {{ ref('int_eligibility_deduped') }}
),

-- Generate a date spine of first-of-month dates
date_spine as (
    select
        dateadd('month', seq4(), '2020-01-01')::date        as month_start_date
    from table(generator(rowcount => 120))  -- 10 years of months
),

member_months as (
    select
        e.person_id,
        ds.month_start_date                                 as enrollment_month,
        last_day(ds.month_start_date)                       as enrollment_month_end,
        extract(year from ds.month_start_date)              as enrollment_year,
        extract(month from ds.month_start_date)             as enrollment_month_num,
        e.payer,
        e.plan,
        e.line_of_business,
        e.gender,
        e.birth_date,
        datediff('year', e.birth_date, ds.month_start_date) as member_age,
        e.state,
        e.zip_code,
        e.data_source
    from eligibility e
    inner join date_spine ds
        on ds.month_start_date >= date_trunc('month', e.enrollment_start_date)
       and ds.month_start_date <= e.enrollment_end_date
)

select * from member_months
