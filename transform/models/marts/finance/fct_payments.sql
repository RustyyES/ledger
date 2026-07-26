{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='delete+insert',
        tags=['marts', 'finance', 'fact']
    )
}}

/*
    Payment fact. Grain: one row per payment.

    ===========================================================================
    THIS MODEL IS THE POINT OF THE WHOLE PROJECT.

    `refunded_amount_cents` is a running total over child rows that arrive up
    to fourteen days AFTER the payment. That single fact invalidates the
    obvious incremental design, and it does so silently.

    ---------------------------------------------------------------------------
    THE WRONG VERSION -- what almost everyone writes first:

{% raw %}
{% if is_incremental() %}
          where processed_at > (select max(processed_at) from {{ this }})
        {% endif %}
{% endraw %}

Walk it through. Today is the 20th. A refund lands today against a payment
    processed on the 8th.

      * The payment's `processed_at` is the 8th.
      * `max(processed_at)` in the table is roughly the 20th.
      * `8th > 20th` is false. The payment is not selected.
      * `refunded_amount_cents` for that payment stays 0.

    It stays 0 FOREVER. No error, no failed test, no null. Net revenue is
    overstated by every late refund the business has ever issued, and the only
    way anyone finds out is a manual reconciliation against the payments table
    months later.

    2.1% of payments get refunded and the delay is uniform over 1-14 days, so
    roughly 2% of all payment rows are permanently wrong under that filter --
    concentrated, of course, in the highest-value orders that customers
    actually bother to dispute.
    ---------------------------------------------------------------------------

    THE RIGHT VERSION -- what is below:

    Filter on `_ingested_at`: when the record entered the PIPELINE, not when the
    event happened in the business. A late refund is by definition recently
    ingested, however old the payment it reverses.

    And critically, `int_payments__with_refunds` defines a payment's
    `_ingested_at` as the LATER of its own and its most recent refund's. Without
    that, a payment whose only change is a new child row still looks untouched
    and the lookback has nothing to match on. The lookback and that `greatest()`
    are one mechanism in two files.

    The window is `var('incremental_lookback_days')` = 15, one day clear of the
    14-day maximum refund delay. `assert_no_late_arrival_outside_lookback.sql`
    fails the build if reality ever exceeds it, because a lookback that is too
    short fails the same silent way the wrong filter does.
    ===========================================================================
*/

with payments as (

    select * from {{ ref('int_payments__with_refunds') }}
    where {{ incremental_lookback('_ingested_at') }}

),

orders as (

    select
        order_id,
        customer_key,
        customer_id,
        date_key,
        order_date,
        currency_code,
        rate_to_usd,
        order_channel
    from {{ ref('fct_orders') }}

),

final as (

    select
        payments.payment_id,
        payments.order_id,
        orders.customer_key,
        orders.customer_id,
        orders.date_key,
        orders.order_date,
        orders.order_channel,

        payments.payment_method,
        payments.payment_status,

        payments.amount_cents
            as gross_amount_cents,
        payments.refunded_amount_cents,
        payments.net_amount_cents,
        payments.refund_count,

        {#- All three conversions use the same currency and rate; only the
            amount differs. Hoisting them makes that obvious and keeps the
            lines readable. -#}
        {%- set ccy = 'orders.currency_code' -%}
        {%- set rate = 'orders.rate_to_usd' %}
        {{ to_usd_cents('payments.amount_cents', ccy, rate) }}
            as gross_amount_usd_cents,
        {{ to_usd_cents('payments.refunded_amount_cents', ccy, rate) }}
            as refunded_amount_usd_cents,
        {{ to_usd_cents('payments.net_amount_cents', ccy, rate) }}
            as net_amount_usd_cents,

        payments.is_successful,
        payments.is_pending,
        payments.is_refunded,
        payments.is_fully_refunded,
        payments.is_partially_refunded,

        payments.processed_at,
        payments.created_at,
        payments.first_refund_issued_at,
        payments.last_refund_issued_at,

        -- Surfaced as a column, not just used internally, so the lookback
        -- assertion is a cheap scan rather than a recomputation, and so an
        -- analyst can see the distribution of arrival lag directly.
        payments.refund_lag_days,

        payments.{{ adapter.quote('_ingested_at') }}
            as _ingested_at

    from payments
    left join orders on payments.order_id = orders.order_id

)

select * from final
