-- =============================================================================
-- Risk rating reference dimension.
-- Maps risk rating codes to their descriptions and PD/LGD parameters.
-- =============================================================================

select
    'A' as risk_rating,
    'Low Risk' as rating_description,
    0.002 as base_pd,
    'Established customers with strong transaction history' as criteria
union all
select 'B', 'Standard Risk', 0.010, 'Default rating for verified customers'
union all
select 'C', 'Elevated Risk', 0.030, 'High volume or young customers'
union all
select 'D', 'High Risk', 0.080, 'New customers or high large-transaction count'
union all
select 'E', 'Very High Risk', 0.150, 'KYC not verified'
