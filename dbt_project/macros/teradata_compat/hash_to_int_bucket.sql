-- Cross-database macro to convert a hex hash string to an integer bucket.
-- Replaces Teradata HASHBUCKET(HASHROW(col)) MOD n.
--
-- generate_surrogate_key() returns a text MD5 hash; this macro extracts
-- an integer from the first 8 hex characters and applies modulo.
--
-- Platform decision:
--   The MD5 hex prefix is the same string on every warehouse, so decoding it
--   with base conversion keeps bucket assignments identical across Snowflake,
--   Databricks and Postgres. Databricks-native hashes (xxhash64, crc32) would
--   be cheaper but would break that parity during a phased migration, so they
--   are deliberately not used here.
--   On Databricks, conv() returns a string, so it is cast to bigint before the
--   modulo (0xFFFFFFFF fits comfortably in a bigint and is always positive).
--
-- Usage: {{ hash_to_int_bucket(dbt_utils.generate_surrogate_key(['col']), 10) }}

{% macro hash_to_int_bucket(hash_expr, num_buckets) %}

    {# Use target.type for SQL dialect since it determines which SQL the database understands #}
    {% if target.type == 'snowflake' %}
        mod(abs(to_number(substring({{ hash_expr }}, 1, 8), 'XXXXXXXX')), {{ num_buckets }})
    {% elif target.type == 'databricks' %}
        mod(abs(cast(conv(substring({{ hash_expr }}, 1, 8), 16, 10) as bigint)), {{ num_buckets }})
    {% else %}
        {# Postgres / default #}
        mod(abs(('x' || substring({{ hash_expr }}, 1, 8))::bit(32)::int), {{ num_buckets }})
    {% endif %}

{% endmacro %}
