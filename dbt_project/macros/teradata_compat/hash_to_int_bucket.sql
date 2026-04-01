-- Cross-database macro to convert a hex hash string to an integer bucket.
-- generate_surrogate_key() returns a text MD5 hash; this macro extracts
-- an integer from the first 8 hex characters and applies modulo.
--
-- Usage: {{ hash_to_int_bucket(dbt_utils.generate_surrogate_key(['col']), 10) }}

{% macro hash_to_int_bucket(hash_expr, num_buckets) %}

    {% if target.type == 'snowflake' %}
        mod(abs(to_number(substring({{ hash_expr }}, 1, 8), 'XXXXXXXX')), {{ num_buckets }})
    {% elif target.type == 'databricks' %}
        mod(abs(conv(substring({{ hash_expr }}, 1, 8), 16, 10)), {{ num_buckets }})
    {% else %}
        {# Postgres / default #}
        mod(abs(('x' || substring({{ hash_expr }}, 1, 8))::bit(32)::bigint), {{ num_buckets }})
    {% endif %}

{% endmacro %}
