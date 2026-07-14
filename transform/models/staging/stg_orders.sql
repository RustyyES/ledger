{{ config(materialized='view', tags=['staging', 'core']) }}

/*
    Orders, deduplicated and normalised.

    ---------------------------------------------------------------------------
    A DELIBERATE DEVIATION FROM THE SPEC, worth reading.

    The spec's example `stg_orders.sql` resolves `placed_at_local` against
    `customer_timezone` in this model. That requires joining to customers -- and
    the layer contract three sections earlier says staging does "no joins, no
    business logic".

    Both cannot be true. The contract wins, because it is the thing that keeps
    staging cheap to reason about: every staging model is a pure function of
    exactly one source table, so debugging one never requires understanding
    another. The timezone resolution happens in `int_orders__resolved`, which is
    the layer that exists for joins.

    What this model DOES normalise is everything achievable without a join:
    types, trimming, and the legacy status vocabulary.
    ---------------------------------------------------------------------------
*/

with source as (

    select * from {{ raw_source('orders') }}

),

latest as (

    {{ cdc_latest('source', 'id') }}

),

renamed as (

    select
        cast(id as varchar)                 as order_id,
        cast(customer_id as varchar)        as customer_id,
        cast(subscription_id as varchar)    as subscription_id,

        cast(amount_cents as bigint)        as amount_cents,
        upper(trim(currency))               as currency_code,

        -- The mess, normalised. One place, and only this place.
        {{ normalise_order_status('status') }} as order_status,
        trim(status)                        as order_status_raw,
        (lower(trim(status)) not in
            ('paid','complete','completed','pending','cancelled','canceled','refunded')
        )                                   as has_unrecognised_status,

        -- Both timestamp representations pass through untouched. Resolving
        -- them needs the customer's timezone; see the note above.
        cast(placed_at as timestamp)        as placed_at,
        placed_at_local                     as placed_at_local,
        (placed_at is null and placed_at_local is not null) as is_legacy_client_order,

        cast(updated_at as timestamp)       as updated_at,

        -- `channel` arrives with migration 0003 and is NULL for every order
        -- placed before it. `raw_column` resolves the name at COMPILE time
        -- against the actual Parquet schema, substituting a null literal when
        -- the column does not exist yet -- so this model builds before, during
        -- and after the migration without an operator flipping anything.
        -- Coalescing to 'unknown' rather than leaving it null means
        -- `group by order_channel` does not silently drop pre-migration history.
        coalesce(
            nullif(trim(cast({{ raw_column('orders', 'channel') }} as varchar)), ''),
            'unknown'
        )                                   as order_channel,

        {{ adapter.quote('_op') }}          as _op,
        {{ adapter.quote('_lsn') }}         as _lsn,
        {{ adapter.quote('_ingested_at') }} as _ingested_at,
        {{ adapter.quote('_source_ts') }}   as _source_ts

    from latest

)

select * from renamed
