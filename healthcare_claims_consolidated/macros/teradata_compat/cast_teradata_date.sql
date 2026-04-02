-- Teradata Compatibility Macro: Date Format Conversion
--
-- Teradata uses FORMAT 'YYYY-MM-DD' and integer date representation
-- (days since 1900-01-01). This macro handles conversions.
--
-- Teradata integer date: date_col (FORMAT 'YYYYMMDD')(INTEGER)
-- Snowflake equivalent: to_date(to_char(date_col, 'YYYYMMDD'), 'YYYYMMDD')
-- Databricks equivalent: to_date(cast(date_col as string), 'yyyyMMdd')

{% macro cast_teradata_date(column_name, input_format='YYYYMMDD') %}

    {% if target.type == 'snowflake' %}
        to_date(cast({{ column_name }} as varchar), '{{ input_format }}')
    {% elif target.type == 'databricks' %}
        to_date(cast({{ column_name }} as string), '{{ input_format | lower }}')
    {% elif target.type == 'duckdb' %}
        strptime(cast({{ column_name }} as varchar), '{{ input_format }}')::date
    {% else %}
        {# Postgres / default #}
        to_date(cast({{ column_name }} as varchar), '{{ input_format }}')
    {% endif %}

{% endmacro %}


-- Teradata Compatibility: ZEROIFNULL
-- Teradata: ZEROIFNULL(x) -> returns 0 if x is null
-- Standard SQL: coalesce(x, 0)

{% macro zeroifnull(column_name) %}
    coalesce({{ column_name }}, 0)
{% endmacro %}


-- Teradata Compatibility: NULLIFZERO
-- Teradata: NULLIFZERO(x) -> returns null if x is 0
-- Standard SQL: nullif(x, 0)

{% macro nullifzero(column_name) %}
    nullif({{ column_name }}, 0)
{% endmacro %}
