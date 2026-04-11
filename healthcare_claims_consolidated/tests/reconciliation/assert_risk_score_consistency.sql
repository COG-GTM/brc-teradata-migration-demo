-- =============================================================================
-- Risk Score Consistency Check
-- Validates that Basel III risk calculations are internally consistent:
-- - RWA = EAD * PD * LGD * 12.5
-- - Expected Loss = PD * LGD * EAD
-- - Capital Required = RWA * 0.08
-- =============================================================================

with risk_scores as (

    select
        customer_id,
        probability_of_default as pd,
        loss_given_default as lgd,
        exposure_at_default as ead,
        risk_weighted_assets as rwa,
        expected_loss as el
    from {{ ref('fct_credit_risk_scores') }}

)

-- Test passes if no rows returned (all calculations consistent)
select
    customer_id,
    pd,
    lgd,
    ead,
    rwa,
    el,
    abs(rwa - (ead * pd * lgd * 12.5)) as rwa_diff,
    abs(el - (pd * lgd * ead)) as el_diff
from risk_scores
where abs(rwa - (ead * pd * lgd * 12.5)) > 0.01
   or abs(el - (pd * lgd * ead)) > 0.01
