-- =============================================================================
-- Edge Case: Balance Consistency
-- Validates that daily balances are mathematically consistent.
-- opening_balance + net_movement should equal closing_balance.
-- =============================================================================

select
    account_id,
    balance_date,
    opening_balance,
    closing_balance,
    total_credits,
    total_debits,
    (closing_balance - opening_balance) as actual_movement,
    (total_credits - total_debits) as expected_movement
from {{ ref('int_daily_account_balances') }}
where abs((closing_balance - opening_balance) - (total_credits - total_debits)) > 0.01
