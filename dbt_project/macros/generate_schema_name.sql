-- Custom schema macro to use schema names directly without prefix.
-- By default dbt prepends the target schema (e.g., public_raw instead of raw).
-- This macro uses the custom schema name directly when specified.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
