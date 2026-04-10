-- Override the default dbt schema naming behaviour.
-- When a custom schema is specified (e.g. +schema: staging), use ONLY that
-- custom schema name instead of prepending the target schema.
-- This ensures seeds land in "raw" (not "public_raw") and models land in
-- "staging", "finance", etc., matching the source definitions.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
