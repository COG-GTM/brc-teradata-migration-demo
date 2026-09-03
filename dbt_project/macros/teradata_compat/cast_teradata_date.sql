-- Teradata Compatibility Macro: Date Format Conversion
--
-- Teradata uses FORMAT 'YYYY-MM-DD' and integer date representation
-- (days since 1900-01-01). This macro handles conversions.
--
-- Teradata integer date: date_col (FORMAT 'YYYYMMDD')(INTEGER)
-- Snowflake equivalent:  to_date(to_char(date_col, 'YYYYMMDD'), 'YYYYMMDD')
-- Databricks equivalent: to_date(try_cast(date_col as string), 'yyyyMMdd')
-- Postgres equivalent:   to_date(cast(date_col as varchar), 'YYYYMMDD')


-- Translate a Teradata FORMAT string into the Java (SimpleDateFormat / Spark
-- datetime pattern) syntax that Databricks expects.
--   YYYY -> yyyy, YY -> yy, DD -> dd, MI -> mm, SS -> ss
--   MM (month) and HH (hour) are already correct in Java patterns.

{% macro teradata_format_to_java(input_format) -%}
    {{- input_format
        | replace('YYYY', 'yyyy')
        | replace('YY', 'yy')
        | replace('DD', 'dd')
        | replace('MI', 'mm')
        | replace('SS', 'ss') -}}
{%- endmacro %}


{% macro cast_teradata_date(column_name, input_format='YYYYMMDD') %}

    {# Use target.type for SQL dialect since it determines which SQL the database understands.
       The target_platform project var is for logical/business decisions only. #}
    {% if target.type == 'snowflake' %}
        to_date(cast({{ column_name }} as varchar), '{{ input_format }}')
    {% elif target.type == 'databricks' %}
        {# Databricks parses with Spark datetime patterns, not Teradata FORMAT strings.
           try_cast keeps the string conversion null-safe under ANSI mode. #}
        to_date(try_cast({{ column_name }} as string), '{{ teradata_format_to_java(input_format) }}')
    {% else %}
        {# Postgres / default #}
        to_date(cast({{ column_name }} as varchar), '{{ input_format }}')
    {% endif %}

{% endmacro %}


-- Teradata Compatibility: ZEROIFNULL
-- Teradata: ZEROIFNULL(x) -> returns 0 if x is null
-- Standard SQL: coalesce(x, 0) -- identical on Snowflake, Databricks and Postgres

{% macro zeroifnull(column_name) %}
    coalesce({{ column_name }}, 0)
{% endmacro %}


-- Teradata Compatibility: NULLIFZERO
-- Teradata: NULLIFZERO(x) -> returns null if x is 0
-- Standard SQL: nullif(x, 0) -- identical on Snowflake, Databricks and Postgres

{% macro nullifzero(column_name) %}
    nullif({{ column_name }}, 0)
{% endmacro %}


-- Teradata Compatibility: CHARACTERS / CHARACTER_LENGTH
-- Teradata: CHARACTERS(x) -> character length
-- Standard SQL: char_length(x) -- Databricks supports char_length as an alias of length

{% macro td_char_length(column_name) %}
    char_length({{ column_name }})
{% endmacro %}
