{{ config(materialized='view', tags=['staging', 'finance']) }}

/*
    Refunds -- the late-arriving fact that shapes the whole incremental design.

    `arrival_lag_days` is computed here rather than downstream so that the
    lookback assertion has a single, cheap column to test against. If that
    figure ever exceeds `var('incremental_lookback_days')`, the incremental
    window is too narrow and `assert_no_late_arrival_outside_lookback` fails
    the build -- before the wrong numbers reach anyone.
*/

with source as (

    select * from {{ raw_source('refunds') }}

),

latest as (

    {{ cdc_latest('source', 'id') }}

)

select
    cast(id as varchar)
        as refund_id,
    cast(payment_id as varchar)
        as payment_id,
    cast(amount_cents as bigint)
        as refund_amount_cents,
    nullif(trim(reason), '')
        as refund_reason,

    cast(issued_at as timestamp)
        as issued_at,
    cast(created_at as timestamp)
        as created_at,

    -- How late this refund was, relative to when the pipeline first saw it.
    date_diff('day', cast(created_at as timestamp), {{ adapter.quote('_ingested_at') }})
        as ingestion_lag_days,

    {{ adapter.quote('_op') }}                                                           as _op,
    {{ adapter.quote('_lsn') }}                                                          as _lsn,
    {{ adapter.quote('_ingested_at') }}
        as _ingested_at,
    {{ adapter.quote('_source_ts') }}
        as _source_ts

from latest
