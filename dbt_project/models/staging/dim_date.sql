-- Date dimension model.
-- Migrated from: teradata/ddl/04_warehouse_tables.sql (DIM_DATE)
-- Generates a date spine for business date lookups and time-series analysis.
-- Referenced by: macros/get_business_date.sql

{{
    config(
        materialized='table',
        schema='staging'
    )
}}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}

),

dates as (

    select
        cast(date_day as date) as calendar_date,
        extract(year from date_day) as year_num,
        extract(month from date_day) as month_num,
        extract(day from date_day) as day_num,
        extract(dow from date_day) as day_of_week,

        -- Is it a weekday? (Monday=1 .. Friday=5 in ISO)
        case
            when extract(dow from date_day) in (0, 6) then false
            else true
        end as is_weekday,

        -- Simplified business day logic: weekdays only
        -- In production, this would also exclude UK bank holidays
        case
            when extract(dow from date_day) in (0, 6) then false
            else true
        end as is_business_day

    from date_spine

)

select * from dates
