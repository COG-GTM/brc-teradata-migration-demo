-- =============================================================================
-- Daily balance snapshots fact table.
-- Migrated from: teradata/stored_procedures/sp_daily_transaction_load.sql
--   (FCT_DAILY_BALANCE section)
-- Surfaces the intermediate daily balances as a mart-level fact.
-- =============================================================================

with balances as (

    select * from {{ ref('int_daily_account_balances') }}

),

accounts as (

    select * from {{ ref('int_unified_account') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['b.account_id', 'b.balance_date']) }} as balance_id,
    b.account_id,
    a.customer_id,
    a.account_type,
    a.currency,
    a.branch_code,
    b.balance_date,
    b.opening_balance,
    b.closing_balance,
    b.total_debits,
    b.total_credits,
    b.transaction_count,
    b.closing_balance - b.opening_balance as net_movement,
    current_timestamp as etl_loaded_ts

from balances b
inner join accounts a
    on b.account_id = a.account_id
