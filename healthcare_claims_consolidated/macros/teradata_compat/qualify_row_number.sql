-- Teradata Compatibility Macro: QUALIFY ROW_NUMBER()
--
-- Teradata supports QUALIFY as a first-class clause:
--   SELECT ... FROM ... QUALIFY ROW_NUMBER() OVER (...) = 1
--
-- In Snowflake/Databricks, we wrap in a sub-query:
--   SELECT * FROM (SELECT ..., ROW_NUMBER() OVER (...) AS rn FROM ...) WHERE rn = 1
--
-- This macro generates the deduplication pattern.

{% macro qualify_row_number(source_relation, partition_by, order_by, column_list='*') %}

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

{% endmacro %}
