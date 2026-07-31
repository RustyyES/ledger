/*
    A completed order must have a successful payment behind it.

    "Completed" in this business means paid -- `payments.py::create_payment` is
    the only code path that sets it. An order marked completed with no
    successful payment is revenue recognised against cash that never arrived.

    This is also the sharpest test of the STATUS NORMALISATION. The source
    carries 'paid', 'PAID', 'complete' and 'completed' from an unfinished
    migration. If `normalise_order_status` misses a spelling, those orders fall
    into 'unknown' and quietly leave the completed population -- which makes
    revenue DROP rather than error. Conversely, if it over-matches and maps a
    genuinely pending order to completed, that order appears here with no
    payment. Either direction of the normalisation being wrong shows up in this
    one query.

    Refunded orders are excluded: they were completed, then reversed, and their
    payment is correctly present but the order status has moved on.
*/

select
    fct_orders.order_id,
    fct_orders.customer_id,
    fct_orders.order_status,
    fct_orders.order_status_raw,
    fct_orders.amount_cents,
    fct_orders.placed_at_utc,
    count(fct_payments.payment_id) as payment_rows
from {{ ref('fct_orders') }}
left join {{ ref('fct_payments') }}
    on  fct_payments.order_id = fct_orders.order_id
    and fct_payments.is_successful
where fct_orders.order_status = 'completed'
group by 1, 2, 3, 4, 5, 6
having count(fct_payments.payment_id) = 0
