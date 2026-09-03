-- Wrapper macro for surrogate key generation.
-- Uses dbt_utils.generate_surrogate_key under the hood.
-- Provides a consistent interface across the project.
--
-- Platform note: dbt_utils compiles this to md5(concat_ws('-', coalesce(...)))
-- on Snowflake, Databricks and Postgres alike, so no dialect branch is needed
-- and keys are byte-identical across targets during a phased migration.
-- Do not swap in Databricks-native xxhash64/crc32 here: it would break key
-- parity with the legacy Teradata/Snowflake output.

{% macro barclays_surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}
