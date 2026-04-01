-- Custom test: Reconciliation check — ensure no transactions are lost
-- between staging and the enriched intermediate model.
-- int_transaction_enriched inner-joins on both stg_accounts and stg_customers,
-- so source_count must mirror both joins to avoid false failures.

with source_count as (
    select count(*) as cnt
    from {{ ref('stg_transactions') }} t
    where exists (
        select 1 from {{ ref('stg_accounts') }} a
        inner join {{ ref('stg_customers') }} c
            on a.customer_id = c.customer_id
        where a.account_id = t.account_id
    )
),

target_count as (
    select count(*) as cnt
    from {{ ref('int_transaction_enriched') }}
)

select
    s.cnt as source_transactions,
    t.cnt as target_transactions,
    s.cnt - t.cnt as difference
from source_count s
cross join target_count t
where s.cnt != t.cnt
