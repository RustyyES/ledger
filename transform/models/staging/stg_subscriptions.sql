{{ config(materialized='view', tags=['staging', 'core']) }}

with source as (

    select * from {{ raw_source('subscriptions') }}

),

latest as (

    {{ cdc_latest('source', 'id') }}

)

select
    cast(id as varchar)                                       as subscription_id,
    cast(customer_id as varchar)                              as customer_id,
    cast(plan_id as integer)                                  as plan_id,
    lower(trim(status))                                       as subscription_status,

    cast(started_at as timestamp)                             as started_at,
    cast(ended_at as timestamp)                               as ended_at,
    cast(trial_ends_at as timestamp)                          as trial_ends_at,
    cast(updated_at as timestamp)                             as updated_at,

    (lower(trim(status)) in ('trialing', 'active', 'paused')) as is_live,
    (lower(trim(status)) = 'trialing')                        as is_trialing,

    {{ adapter.quote('_op') }}                                as _op,
    {{ adapter.quote('_lsn') }}                               as _lsn,
    {{ adapter.quote('_ingested_at') }}                       as _ingested_at,
    {{ adapter.quote('_source_ts') }}                         as _source_ts

from latest
