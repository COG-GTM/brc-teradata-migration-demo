-- Custom test: Reconciliation check — ensure no transactions are lost
-- between staging and the enriched intermediate model.
-- Every transaction in stg_transactions should appear in int_transaction_enriched
-- (inner join with accounts filters out orphan transactions).

with source_count as (
    select count(*) as cnt
    from {{ ref('stg_transactions') }} t
    where exists (
        select 1 from {{ ref('stg_accounts') }} a
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
