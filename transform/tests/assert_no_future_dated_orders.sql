/*
    No order may be placed in the future.

    This is the test that proves the timezone normalisation actually worked.

    15% of orders arrive as naive local wall-clock with no offset. If that text
    is read as UTC when it was really Asia/Tokyo, the resulting timestamp is up
    to 9 hours ahead of the true instant -- and for an order placed this evening
    in Tokyo, that lands in tomorrow. A handful of future-dated orders is the
    visible symptom of a timezone bug that is silently mis-bucketing EVERY
    legacy-client order by up to 14 hours, most of which fall on the correct day
    and are therefore invisible.

    A small grace window is allowed for clock skew between the application host
    and the warehouse. It is deliberately small: an hour of tolerance would hide
    the single-hour DST errors this is meant to catch.
*/

select
    order_id,
    customer_id,
    placed_at_utc,
    placed_at_local,
    is_legacy_client_order,
    is_timestamp_inferred,
    is_timestamp_low_confidence,
    date_diff('minute', cast({{ warehouse_now() }} as timestamp), placed_at_utc) as minutes_into_the_future
from {{ ref('fct_orders') }}
where placed_at_utc > cast({{ warehouse_now() }} as timestamp) + interval '5 minutes'
