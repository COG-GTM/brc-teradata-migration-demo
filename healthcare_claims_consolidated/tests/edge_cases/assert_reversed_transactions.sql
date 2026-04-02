-- =============================================================================
-- Edge Case: Reversed Transactions
-- Validates that REVERSAL type transactions are properly handled.
-- Reversals should have the correct sign in signed_amount.
-- =============================================================================

select
    transaction_id,
    transaction_type,
    amount,
    signed_amount
from {{ ref('int_unified_transaction') }}
where transaction_type = 'REVERSAL'
  and signed_amount < 0  -- reversals should be positive (credit back)
