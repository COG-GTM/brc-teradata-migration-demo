-- =============================================================================
-- Source: Teradata / BARCLAYS_RAW / ACCOUNT (MULTISET table)
-- Migration: QUALIFY ROW_NUMBER() -> subquery with ROW_NUMBER()
--            LOCK ROW FOR ACCESS  -> removed
--            ZEROIFNULL(x)        -> coalesce(x, 0)
--            RANGE_N PPI          -> removed (Snowflake uses micro-partitions)
--            Teradata date arithmetic (CURRENT_DATE - open_date) -> datediff()
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('teradata_raw', 'account') }}

),

deduplicated as (

    select
        account_id,
        customer_id,
        upper(trim(account_type)) as account_type,
        upper(trim(currency)) as currency,
        branch_code,
        sort_code,
        upper(trim(status)) as status,
        open_date,
        close_date,
        coalesce(credit_limit, 0) as credit_limit,
        coalesce(overdraft_limit, 0) as overdraft_limit,
        -- Derived status (replaces Teradata staging view V_ACCOUNT_CURRENT logic)
        case
            when close_date is not null then 'CLOSED'
            when upper(trim(status)) = 'DORMANT' then 'DORMANT'
            when datediff('day', open_date, current_date) < 90 then 'NEW'
            else 'ACTIVE'
        end as derived_status,
        datediff('day', open_date, current_date) as days_since_opening,
        row_number() over (
            partition by account_id
            order by open_date desc
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

from deduplicated
where row_num = 1
