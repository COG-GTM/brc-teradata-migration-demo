-- =============================================================================
-- Edge Case: Zero-Dollar Transactions
-- Validates that zero-dollar transactions are handled correctly.
-- They should exist in the transaction fact but should NOT generate
-- AML structuring alerts.
-- =============================================================================

-- Zero-dollar transactions should not appear in structuring alerts
select
    a.alert_id,
    a.alert_type,
    a.total_amount
from {{ ref('fct_aml_alerts') }} a
where a.alert_type = 'STRUCTURING'
  and a.total_amount = 0
