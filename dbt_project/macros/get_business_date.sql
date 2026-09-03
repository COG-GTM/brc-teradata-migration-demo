-- Migrated from: teradata/macros/macro_get_business_date.sql
-- Returns the most recent business date before the reference date.
-- In production, this would query a calendar/holiday table.
--
-- Platform notes:
--   * The reference date is cast explicitly so a string literal such as
--     '2024-01-31' works on every target. Databricks under ANSI mode does not
--     implicitly compare a date column to a string, so on Databricks the cast
--     goes through to_date().
--   * is_business_day is compared to a boolean literal, which Databricks,
--     Snowflake and Postgres all support.

{% macro get_business_date(reference_date='current_date') %}

    (
        select max(calendar_date)
        from {{ ref('dim_date') }}
        where calendar_date < {{ to_date_expr(reference_date) }}
          and is_business_day = true
    )

{% endmacro %}


-- Cast an arbitrary date expression (column, literal or current_date) to a DATE
-- using dialect-appropriate syntax.

{% macro to_date_expr(date_expression) -%}
    {%- if target.type == 'databricks' -%}
        to_date({{ date_expression }})
    {%- else -%}
        cast({{ date_expression }} as date)
    {%- endif -%}
{%- endmacro %}
