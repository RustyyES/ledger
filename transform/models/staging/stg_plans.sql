{{ config(materialized='view', tags=['staging', 'core']) }}

/*
    Reference pricing.

    Retired plans are KEPT. `legacy_starter` is inactive but still has live
    subscriptions attached, so filtering on `active` here would break the join
    from `fct_subscription_events` and silently drop those customers' MRR.
    "Is this plan sellable" and "does this plan exist" are different questions.
*/

with source as (

    select * from {{ raw_source('plans') }}

),

latest as (

    {{ cdc_latest('source', 'id') }}

)

select
    cast(id as integer)                        as plan_id,
    trim(code)                                 as plan_code,
    cast(monthly_cents as bigint)              as monthly_cents,
    upper(trim(currency))                      as currency_code,
    cast(active as boolean)                    as is_active,

    -- Ordering plans by price rather than by id, because the id reflects
    -- creation order and a plan added later can sit anywhere in the ladder.
    -- Upgrade/downgrade classification depends on this being the price rank.
    row_number() over (order by monthly_cents) as price_rank,

    {{ adapter.quote('_ingested_at') }}        as _ingested_at

from latest
