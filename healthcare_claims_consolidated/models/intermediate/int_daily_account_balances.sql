-- =============================================================================
-- Migrated from: teradata/stored_procedures/sp_daily_transaction_load.sql
--   (FCT_DAILY_BALANCE MERGE section)
-- Teradata constructs replaced:
--   MERGE INTO ... WHEN MATCHED / NOT MATCHED -> dbt incremental
--   ZEROIFNULL(x)                             -> coalesce(x, 0)
--   COLLECT STATISTICS                        -> not needed
-- Now operates on unified cross-platform transactions.
--
-- Fix: Uses cumulative window sum to correctly cascade balances within
-- a multi-day incremental batch. Each day's opening balance is the prior
-- day's closing balance, even when multiple days arrive in the same run.
-- =============================================================================

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

    from {{ ref('int_unified_transaction') }}

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
        cast(null as {{ dbt.type_string() }}) as account_id,
        cast(0 as numeric) as prior_closing_balance
    where 1 = 0
    {% endif %}

),

-- Compute cumulative net movement within the batch so that multi-day
-- batches correctly cascade: day N+1 opening = day N closing.
daily_with_cumulative as (

    select
        dt.account_id,
        dt.balance_date,
        dt.total_credits,
        dt.total_debits,
        dt.net_movement,
        dt.transaction_count,
        coalesce(pb.prior_closing_balance, 0) as seed_balance,
        sum(dt.net_movement) over (
            partition by dt.account_id
            order by dt.balance_date
            rows between unbounded preceding and current row
        ) as cumulative_net_movement

    from daily_transactions dt
    left join prior_balances pb
        on dt.account_id = pb.account_id

)

select
    account_id,
    balance_date,
    seed_balance + cumulative_net_movement - net_movement as opening_balance,
    seed_balance + cumulative_net_movement as closing_balance,
    total_debits,
    total_credits,
    transaction_count

from daily_with_cumulative
