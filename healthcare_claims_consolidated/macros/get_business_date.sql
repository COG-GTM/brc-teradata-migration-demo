-- Migrated from: teradata/macros/macro_get_business_date.sql
-- Returns the most recent business date before the reference date.
-- In production, this would query a calendar/holiday table.

{% macro get_business_date(reference_date='current_date') %}

    (
        select max(calendar_date)
        from {{ ref('dim_date') }}
        where calendar_date < {{ reference_date }}
          and is_business_day = true
    )

{% endmacro %}
