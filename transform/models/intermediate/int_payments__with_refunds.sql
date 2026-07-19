{{ config(materialized='ephemeral', tags=['intermediate', 'finance']) }}

/*
    Payments joined to their refund totals.

    ---------------------------------------------------------------------------
    THIS IS WHERE THE LATE-REFUND PROBLEM BITES.

    A refund arrives 1-14 days after the payment it reverses. That means:

      * `refunded_amount_cents` for a payment is NOT final when the payment is
        first ingested. It can change for a fortnight afterwards.
      * Therefore a payment row must be REPROCESSED when a late refund lands,
        even though nothing about the payment itself changed.
      * Therefore the incremental filter downstream cannot key on the payment's
        own timestamp. It keys on `_ingested_at` -- see `fct_payments`.

    `last_refund_ingested_at` is carried through specifically so the downstream
    incremental model can see recency that comes from a CHILD row. Without it,
    a payment from twelve days ago whose only change is a new refund looks
    untouched, and the lookback has nothing to match on.
    ---------------------------------------------------------------------------
*/

with payments as (

    select * from {{ ref('stg_payments') }}

),

refunds as (

    select * from {{ ref('stg_refunds') }}

),

refund_totals as (

    select
        payment_id,
        sum(refund_amount_cents)                 as refunded_amount_cents,
        count(*)                                 as refund_count,
        min(issued_at)                           as first_refund_issued_at,
        max(issued_at)                           as last_refund_issued_at,
        -- The recency signal that lets the incremental model notice a payment
        -- whose only change came from a child row.
        max({{ adapter.quote('_ingested_at') }}) as last_refund_ingested_at
    from refunds
    group by 1

),

joined as (

    select
        payments.payment_id,
        payments.order_id,
        payments.amount_cents,
        payments.payment_method,
        payments.payment_status,
        payments.processed_at,
        payments.created_at,
        payments.updated_at,
        payments.is_successful,
        payments.is_pending,

        coalesce(refund_totals.refunded_amount_cents, 0)
            as refunded_amount_cents,
        coalesce(refund_totals.refund_count, 0)                                    as refund_count,
        refund_totals.first_refund_issued_at,
        refund_totals.last_refund_issued_at,

        payments.amount_cents
        - coalesce(refund_totals.refunded_amount_cents, 0)
            as net_amount_cents,

        (coalesce(refund_totals.refunded_amount_cents, 0) > 0)                     as is_refunded,
        (
            coalesce(refund_totals.refunded_amount_cents, 0)
            >= payments.amount_cents
        )
            as is_fully_refunded,
        (
            coalesce(refund_totals.refunded_amount_cents, 0) > 0
            and coalesce(refund_totals.refunded_amount_cents, 0)
            < payments.amount_cents
        )
            as is_partially_refunded,

        -- How many days after the payment the LAST refund arrived. This is the
        -- number the lookback has to exceed, and the singular test
        -- `assert_no_late_arrival_outside_lookback` reads it directly.
        date_diff('day', payments.created_at, refund_totals.last_refund_issued_at)
            as refund_lag_days,

        payments.{{ adapter.quote('_op') }}                                        as _op,
        payments.{{ adapter.quote('_lsn') }}                                       as _lsn,
        payments.{{ adapter.quote('_source_ts') }}                                 as _source_ts,

        -- The payment is "recent" if EITHER the payment row or any of its
        -- refunds was recently ingested. This single expression is what makes
        -- the downstream incremental correct.
        greatest(
            payments.{{ adapter.quote('_ingested_at') }},
            coalesce(
                refund_totals.last_refund_ingested_at,
                payments.{{ adapter.quote('_ingested_at') }}
            )
        )                                                                          as _ingested_at

    from payments
    left join refund_totals on payments.payment_id = refund_totals.payment_id

)

select * from joined
