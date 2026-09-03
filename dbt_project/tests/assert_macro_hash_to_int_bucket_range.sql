-- Macro test: hash_to_int_bucket() must return a deterministic bucket in
-- [0, num_buckets) on every target dialect (Snowflake, Databricks, Postgres).
-- Fails if any customer bucket is null or outside the range.

select
    customer_id,
    {{ hash_to_int_bucket(dbt_utils.generate_surrogate_key(['customer_id']), 10) }} as bucket

from {{ ref('stg_customers') }}

where {{ hash_to_int_bucket(dbt_utils.generate_surrogate_key(['customer_id']), 10) }} is null
   or {{ hash_to_int_bucket(dbt_utils.generate_surrogate_key(['customer_id']), 10) }} < 0
   or {{ hash_to_int_bucket(dbt_utils.generate_surrogate_key(['customer_id']), 10) }} >= 10
