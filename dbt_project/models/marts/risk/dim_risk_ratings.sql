-- Risk rating reference dimension with calibrated PD/LGD values.
-- Uses seed data from seeds/risk_rating_codes.csv.

with ratings as (

    select * from {{ ref('risk_rating_codes') }}

)

select
    risk_rating,
    rating_description,
    cast(pd_lower_bound as numeric(10, 4)) as pd_lower_bound,
    cast(pd_upper_bound as numeric(10, 4)) as pd_upper_bound,
    cast(lgd_unsecured as numeric(10, 4)) as lgd_unsecured,
    cast(lgd_secured as numeric(10, 4)) as lgd_secured,
    cast(risk_weight_sa as numeric(10, 4)) as risk_weight_sa,
    rating_category

from ratings
