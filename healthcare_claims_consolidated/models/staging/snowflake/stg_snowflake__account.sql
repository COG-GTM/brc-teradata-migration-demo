-- =============================================================================
-- Source: Snowflake / HEALTHCARE_RAW / account
-- Migration: account_status -> status (column name drift)
--            opened_date -> open_date (column name drift)
--            closed_date -> close_date (column name drift)
--            ccy -> currency (column name drift)
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('snowflake_raw', 'account') }}

),

standardized as (

    select
        account_id,
        customer_id,
        upper(trim(account_type)) as account_type,
        -- Column naming drift: ccy -> currency
        upper(trim(ccy)) as currency,
        branch_code,
        sort_code,
        -- Column naming drift: account_status -> status
        upper(trim(account_status)) as status,
        -- Column naming drift: opened_date -> open_date
        opened_date as open_date,
        -- Column naming drift: closed_date -> close_date
        closed_date as close_date,
        coalesce(credit_limit, 0) as credit_limit,
        coalesce(overdraft_limit, 0) as overdraft_limit,
        case
            when closed_date is not null then 'CLOSED'
            when upper(trim(account_status)) = 'DORMANT' then 'DORMANT'
            when datediff('day', opened_date, current_date) < 90 then 'NEW'
            else 'ACTIVE'
        end as derived_status,
        datediff('day', opened_date, current_date) as days_since_opening,
        row_number() over (
            partition by account_id
            order by opened_date desc
        ) as row_num

    from source

)

select
    account_id,
    customer_id,
    account_type,
    currency,
    branch_code,
    sort_code,
    status,
    open_date,
    close_date,
    credit_limit,
    overdraft_limit,
    derived_status,
    days_since_opening

from standardized
where row_num = 1
