-- Macro test: qualify_row_number() must return exactly one row per partition,
-- whether it compiles to a native QUALIFY clause (Snowflake, Databricks) or to
-- the portable sub-query wrapper (Postgres).
-- Fails if any account_id survives deduplication more than once.

with deduped as (

    select * from {{ qualify_row_number(
        source_relation=ref('stg_transactions'),
        partition_by='account_id',
        order_by='transaction_date desc, transaction_id desc',
        column_list='account_id, transaction_id, transaction_date'
    ) }} as _qualified

)

select
    account_id,
    count(*) as row_count

from deduped
group by account_id
having count(*) > 1
