-- =============================================================================
-- Edge Case: Valid Risk Scores
-- Validates that all risk scores have valid ratings and non-negative metrics.
-- =============================================================================

select
    customer_id,
    risk_rating,
    probability_of_default,
    loss_given_default,
    exposure_at_default,
    risk_weighted_assets,
    expected_loss
from {{ ref('fct_credit_risk_scores') }}
where risk_rating not in ('A', 'B', 'C', 'D', 'E')
   or probability_of_default < 0
   or probability_of_default > 1
   or loss_given_default < 0
   or loss_given_default > 1
   or exposure_at_default < 0
   or risk_weighted_assets < 0
   or expected_loss < 0
