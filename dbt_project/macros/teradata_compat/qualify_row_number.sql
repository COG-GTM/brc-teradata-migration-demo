-- Teradata Compatibility Macro: QUALIFY ROW_NUMBER()
--
-- Teradata supports QUALIFY as a first-class clause:
--   SELECT ... FROM ... QUALIFY ROW_NUMBER() OVER (...) = 1
--
-- Platform decision:
--   * Snowflake  - native QUALIFY.
--   * Databricks - native QUALIFY (Databricks SQL and DBR 10.0+). We emit it by
--     default because it lets Photon push the window filter down instead of
--     materialising a wrapped sub-query. Set the project/target var
--     `databricks_supports_qualify: false` for legacy runtimes (< DBR 10.0),
--     which falls back to the portable sub-query wrapper below.
--   * Postgres / everything else - sub-query wrapper:
--       SELECT * FROM (SELECT ..., ROW_NUMBER() OVER (...) AS rn FROM ...) WHERE rn = 1

{% macro qualify_row_number(source_relation, partition_by, order_by, column_list='*') %}

    {# Use target.type for SQL dialect since it determines which SQL the database understands.
       The target_platform project var is for logical/business decisions only. #}
    {%- set native_qualify = target.type == 'snowflake'
        or (target.type == 'databricks' and var('databricks_supports_qualify', true)) -%}

    {% if native_qualify %}
    (
        select {{ column_list }}
        from {{ source_relation }}
        qualify row_number() over (
            partition by {{ partition_by }}
            order by {{ order_by }}
        ) = 1
    )
    {% else %}
    (
        select {{ column_list }}
        from (
            select
                {{ column_list }},
                row_number() over (
                    partition by {{ partition_by }}
                    order by {{ order_by }}
                ) as _rn
            from {{ source_relation }}
        ) as _deduped
        where _rn = 1
    )
    {% endif %}

{% endmacro %}
