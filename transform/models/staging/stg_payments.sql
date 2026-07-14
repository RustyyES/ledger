{{ config(materialized='view', tags=['staging', 'finance']) }}

/*
    Payments.

    `processed_at` is null while a payment is pending -- legitimately, not as a
    data quality problem. The schema test on this column is therefore
    `not_null_where(condition: "payment_status = 'succeeded'")` rather than a
    blanket `not_null`. A blanket test here would be permanently red, and a
    permanently red test is one nobody reads.
*/

with source as (

    select * from {{ raw_source('payments') }}

),

latest as (

    {{ cdc_latest('source', 'id') }}

)

select
    cast(id as varchar)                 as payment_id,
    cast(order_id as varchar)           as order_id,
    cast(amount_cents as bigint)        as amount_cents,
    lower(trim(method))                 as payment_method,
    lower(trim(status))                 as payment_status,

    cast(processed_at as timestamp)     as processed_at,
    cast(created_at as timestamp)       as created_at,
    cast(updated_at as timestamp)       as updated_at,

    (lower(trim(status)) = 'succeeded') as is_successful,
    (lower(trim(status)) = 'pending')   as is_pending,

    {{ adapter.quote('_op') }}          as _op,
    {{ adapter.quote('_lsn') }}         as _lsn,
    {{ adapter.quote('_ingested_at') }} as _ingested_at,
    {{ adapter.quote('_source_ts') }}   as _source_ts

from latest
