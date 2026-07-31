/*
    Every order must resolve to exactly one customer version, as of its own
    placed_at.

    The as-of join in `fct_orders` is a range join, and range joins fail
    quietly. If no dim_customer version covers an order's `placed_at_utc`, the
    left join yields NULL and the order keeps its place in the fact table while
    disappearing from every aggregate that groups by `customer_key`. Row counts
    reconcile. Revenue by country does not.

    The realistic cause is an order placed BEFORE the first snapshot of its
    customer -- which happens when the snapshot is run after the backfill rather
    than before it, or when a customer's `updated_at` was never touched so no
    initial version was ever recorded.
*/

select
    fct_orders.order_id,
    fct_orders.customer_id,
    fct_orders.placed_at_utc,
    min(dim_customer.valid_from) as earliest_known_version
from {{ ref('fct_orders') }}
left join {{ ref('dim_customer') }}
    on dim_customer.customer_id = fct_orders.customer_id
where fct_orders.customer_key is null
group by 1, 2, 3
