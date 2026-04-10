-- Teradata Compatibility Macro: QUALIFY ROW_NUMBER()
--
-- Teradata supports QUALIFY as a first-class clause:
--   SELECT ... FROM ... QUALIFY ROW_NUMBER() OVER (...) = 1
--
-- Snowflake also supports QUALIFY natively.
-- Databricks and Postgres require a sub-query wrapper:
--   SELECT * FROM (SELECT ..., ROW_NUMBER() OVER (...) AS rn FROM ...) WHERE rn = 1
--
-- This macro generates the deduplication pattern with platform-aware output.

{% macro qualify_row_number(source_relation, partition_by, order_by, column_list='*') %}

    {# Use target.type for SQL dialect. Snowflake supports native QUALIFY; others need sub-query. #}
    {% if target.type == 'snowflake' %}
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
