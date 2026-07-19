{{ config(materialized='ephemeral', tags=['intermediate', 'core']) }}

/*
    Orders with their timestamps resolved to UTC and amounts converted to USD.

    This is the model the spec puts in staging. It lives here because both of
    its jobs need a join, and the layer contract says staging does not join:

      * resolving `placed_at_local` needs the customer's IANA timezone;
      * converting to USD needs the FX rate for the order's month.

    ---------------------------------------------------------------------------
    THE CIRCULARITY, and how it is broken.

    Converting to USD needs the order's month. The order's month comes from
    `placed_at_utc`. `placed_at_utc` needs the timezone. So the FX join must
    happen AFTER the timezone resolution, not alongside it -- which is why this
    model is two sequential CTEs rather than one wide join. Getting that order
    wrong puts up to 15% of orders (the legacy-client ones, which have no
    `placed_at`) into the wrong FX month.
    ---------------------------------------------------------------------------
*/

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select
        customer_id,
        timezone,
        has_explicit_timezone
    from {{ ref('stg_customers') }}

),

resolved_time as (

    select
        orders.*,
        customers.timezone                                as customer_timezone,
        customers.has_explicit_timezone,

        {{ resolve_placed_at_utc('orders.placed_at',
                                 'orders.placed_at_local',
                                 'customers.timezone') }} as placed_at_utc,

        -- Marks every row whose UTC timestamp was DERIVED rather than stated.
        -- Two different confidence levels are collapsed into this one flag:
        -- a legacy order with a known timezone (good), and one whose customer
        -- has no explicit timezone so we assumed UTC (could be 14 hours out).
        (orders.placed_at is null)                        as is_timestamp_inferred,
        (
            orders.placed_at is null
            and not customers.has_explicit_timezone
        )                                                 as is_timestamp_low_confidence

    from orders
    left join customers on orders.customer_id = customers.customer_id

),

with_fx as (

    select
        resolved_time.*,
        fx.rate_to_usd,

        {{ to_usd_cents('resolved_time.amount_cents',
                        'resolved_time.currency_code',
                        'fx.rate_to_usd') }}    as amount_usd_cents,

        -- A null rate means the seed does not cover this month. Surfacing it
        -- as a flag rather than letting the conversion silently produce NULL
        -- is what makes `assert_every_non_usd_order_has_an_fx_rate` a real
        -- check instead of a tautology.
        (fx.rate_to_usd is null)             as is_fx_rate_missing

    from resolved_time
    -- The rate for the month the order was PLACED IN. See the seed docs.
    left join {{ ref('seed_fx_rates') }} as fx
        on
            resolved_time.currency_code = fx.currency_code
            and fx.rate_date = cast(date_trunc('month', resolved_time.placed_at_utc) as date)

)

select * from with_fx
