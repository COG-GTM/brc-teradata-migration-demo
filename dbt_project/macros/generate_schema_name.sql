-- Override the default dbt schema naming behaviour.
-- When a custom schema is specified (e.g. +schema: staging), use ONLY that
-- custom schema name instead of prepending the target schema.
-- This ensures seeds land in "raw" (not "public_raw") and models land in
-- "staging", "finance", etc., matching the source definitions.
--
-- Platform note: this is dialect-independent. On Databricks with Unity Catalog
-- the catalog comes from the profile (`catalog:`), and the value returned here
-- is the schema (database) within that catalog, so absolute names such as
-- "raw" resolve to <catalog>.raw.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
