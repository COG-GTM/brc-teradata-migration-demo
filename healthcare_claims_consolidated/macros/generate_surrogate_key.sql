-- Wrapper macro for surrogate key generation.
-- Uses dbt_utils.generate_surrogate_key under the hood.
-- Provides a consistent interface across the project.

{% macro barclays_surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}
