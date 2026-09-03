-- Risk rating reference dimension with calibrated PD/LGD values.
-- Uses seed data from seeds/risk_rating_codes.csv.

{%- set is_databricks = target.type == 'databricks' -%}

{% if is_databricks %}
{{
    config(
        materialized='table',
        file_format='delta',
        tblproperties={
            'delta.autoOptimize.optimizeWrite': 'true',
            'delta.autoOptimize.autoCompact': 'true'
        }
    )
}}
{% endif %}

with ratings as (

    select * from {{ ref('risk_rating_codes') }}

)

select
    risk_rating,
    rating_description,
    -- decimal(18,6) is portable across Databricks, Snowflake and Postgres.
    -- Databricks caps DECIMAL arithmetic at precision 38, so downstream
    -- Basel III products rely on narrow, explicit operand precision here.
    cast(pd_lower_bound as decimal(18, 6)) as pd_lower_bound,
    cast(pd_upper_bound as decimal(18, 6)) as pd_upper_bound,
    cast(lgd_unsecured as decimal(18, 6)) as lgd_unsecured,
    cast(lgd_secured as decimal(18, 6)) as lgd_secured,
    cast(risk_weight_sa as decimal(18, 6)) as risk_weight_sa,
    rating_category

from ratings
