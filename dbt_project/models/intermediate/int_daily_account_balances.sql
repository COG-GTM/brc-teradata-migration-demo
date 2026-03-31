-- Migrated from: teradata/stored_procedures/sp_daily_transaction_load.sql
--   (FCT_DAILY_BALANCE MERGE section)
-- Teradata constructs replaced:
--   MERGE INTO ... WHEN MATCHED / NOT MATCHED -> dbt incremental
--   ZEROIFNULL(x)                             -> coalesce(x, 0)
--   COLLECT STATISTICS                        -> not needed

{{
    config(
        materialized='incremental',
        unique_key=['account_id', 'balance_date']
    )
}}

with daily_transactions as (

    select
        account_id,
        transaction_date as balance_date,
        sum(case when signed_amount > 0 then signed_amount else 0 end) as total_credits,
        sum(case when signed_amount < 0 then abs(signed_amount) else 0 end) as total_debits,
        sum(signed_amount) as net_movement,
        count(*) as transaction_count

    from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    where transaction_date > (select max(balance_date) from {{ this }})
    {% endif %}

    group by account_id, transaction_date

),

prior_balances as (

    {% if is_incremental() %}
    select
        account_id,
        closing_balance as prior_closing_balance
    from {{ this }}
    where (account_id, balance_date) in (
        select account_id, max(balance_date)
        from {{ this }}
        group by account_id
    )
    {% else %}
    select
        null::varchar as account_id,
        0::numeric as prior_closing_balance
    where 1 = 0
    {% endif %}

)

select
    dt.account_id,
    dt.balance_date,
    coalesce(pb.prior_closing_balance, 0) as opening_balance,
    coalesce(pb.prior_closing_balance, 0) + dt.net_movement as closing_balance,
    dt.total_debits,
    dt.total_credits,
    dt.transaction_count

from daily_transactions dt
left join prior_balances pb
    on dt.account_id = pb.account_id
