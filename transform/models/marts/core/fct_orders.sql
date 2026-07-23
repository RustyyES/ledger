{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        tags=['marts', 'core', 'fact']
    )
}}

/*
    Order fact. Grain: one row per order.

    ---------------------------------------------------------------------------
    THE INCREMENTAL FILTER.

    Note it is `_ingested_at`, not `placed_at_utc`. Orders are less prone to
    late arrival than payments, but they are not immune: an order's `status`
    changes when it is paid or refunded, and a refund can land fourteen days
    later. Keying on `placed_at_utc` means that status change never enters an
    incremental run, and `fct_orders.order_status` stays 'completed' forever on
    an order that was refunded a fortnight ago.

    `delete+insert` rather than `merge` because the grain is stable and the
    lookback window is small; it avoids a full-table merge scan on every run.
    ---------------------------------------------------------------------------

    THE AS-OF DIMENSION JOIN.

    `customer_key` is resolved as of `placed_at_utc`, NOT as of now. An order
    placed while the customer lived in Egypt keeps pointing at the Egyptian
    version of that customer forever. This is the entire reason dim_customer is
    SCD2, and doing it here means every consumer gets it right for free.
*/

with orders as (

    select * from {{ ref('int_orders__resolved') }}
    where {{ incremental_lookback('_ingested_at') }}

),

customers as (

    select
        customer_key,
        customer_id,
        valid_from,
        valid_to_or_infinity,
        country_code,
        is_deleted
    from {{ ref('dim_customer') }}

),

plans as (

    select
        plan_key,
        plan_id
    from {{ ref('dim_plan') }}

),

subscriptions as (

    select
        subscription_id,
        plan_id
    from {{ ref('stg_subscriptions') }}

),

final as (

    select
        orders.order_id,

        -- as-of, not current. See the header.
        customers.customer_key,
        orders.customer_id,
        plans.plan_key,
        orders.subscription_id,
        {{ date_key('orders.placed_at_utc') }} as date_key,
        cast(orders.placed_at_utc as date)         as order_date,

        orders.order_status,
        orders.order_status_raw,
        orders.order_channel,

        orders.amount_cents,
        orders.currency_code,
        orders.amount_usd_cents,
        orders.rate_to_usd,

        orders.placed_at_utc,
        orders.placed_at_local,
        orders.updated_at,

        -- Provenance flags. An analyst filtering these out is asking a
        -- narrower but more defensible question, and they cannot do that if
        -- the pipeline throws the information away.
        orders.is_legacy_client_order,
        orders.is_timestamp_inferred,
        orders.is_timestamp_low_confidence,
        orders.is_fx_rate_missing,

        -- Retained deliberately: a deleted customer's historical orders are
        -- still revenue that was recognised. Deleting a customer is a privacy
        -- action, not a financial correction. DESIGN.md records this.
        coalesce(customers.is_deleted, false)      as customer_is_deleted,
        customers.country_code                     as customer_country_at_order_time,

        orders.{{ adapter.quote('_ingested_at') }} as _ingested_at

    from orders

    left join customers
        on
            orders.customer_id = customers.customer_id
            and orders.placed_at_utc >= customers.valid_from
            and orders.placed_at_utc < customers.valid_to_or_infinity

    left join subscriptions
        on orders.subscription_id = subscriptions.subscription_id
    left join plans
        on subscriptions.plan_id = plans.plan_id

)

select * from final
