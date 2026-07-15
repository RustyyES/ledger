{{ config(materialized='view', tags=['staging', 'finance']) }}

/*
    The subscription lifecycle log. The source of truth for MRR.

    This is the ONE source table that is genuinely append-only: the application
    never updates an event row. That means `cdc_latest` is defensive rather than
    necessary here -- but it stays, because "the application never updates it"
    is an assumption about code that can change, and a dedup on an already-
    unique key costs one window function.

    `_op = 'd'` on this table would mean somebody deleted history, which should
    never happen. `assert_no_deleted_subscription_events` fails the build if it
    does rather than letting the row quietly vanish from MRR.
*/

with source as (

    select * from {{ raw_source('subscription_events') }}

),

latest as (

    {{ cdc_latest('source', 'id') }}

)

select
    cast(id as bigint)                                              as subscription_event_id,
    cast(subscription_id as varchar)                                as subscription_id,
    lower(trim(event_type))                                         as event_type,
    cast(from_plan_id as integer)                                   as from_plan_id,
    cast(to_plan_id as integer)                                     as to_plan_id,

    cast(occurred_at as timestamp)                                  as occurred_at,
    cast(created_at as timestamp)                                   as created_at,

    (lower(trim(event_type)) in ('created', 'resumed'))             as starts_revenue,
    (lower(trim(event_type)) in ('cancelled', 'paused', 'expired')) as stops_revenue,
    (lower(trim(event_type)) in ('upgraded', 'downgraded'))         as changes_revenue,

    {{ adapter.quote('_op') }}                                      as _op,
    {{ adapter.quote('_lsn') }}                                     as _lsn,
    {{ adapter.quote('_ingested_at') }}                             as _ingested_at,
    {{ adapter.quote('_source_ts') }}                               as _source_ts

from latest
