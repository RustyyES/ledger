/*
    No fact may arrive later than the incremental lookback window can see.

    ---------------------------------------------------------------------------
    THIS TEST GUARDS THE ASSUMPTION THE WHOLE INCREMENTAL DESIGN RESTS ON.

    `fct_payments` reprocesses a rolling `incremental_lookback_days` window so
    that late refunds land. That is correct exactly as long as nothing arrives
    later than the window. The moment something does, it is invisible to every
    subsequent run and the affected row is permanently wrong -- silently, with
    no error and no null.

    So the window is not a tuning parameter, it is an assertion about reality,
    and this is the test that holds reality to it.

    It has already earned its place once. The lookback was originally set to 15
    days, reasoning from the load generator's 14-day maximum refund delay. Real
    generated data produced a lag of 16, because `date_diff('day', ...)` counts
    calendar boundaries crossed rather than elapsed 24-hour periods. See the
    derivation in dbt_project.yml; the window is now 21.

    WARN rather than ERROR at the boundary would be wrong here. A late arrival
    that the pipeline cannot see is not a warning, it is data loss.
    ---------------------------------------------------------------------------
*/

select
    payment_id,
    order_id,
    created_at              as payment_created_at,
    last_refund_issued_at,
    refund_lag_days,
    {{ var('incremental_lookback_days') }} as configured_lookback_days,
    refund_lag_days - {{ var('incremental_lookback_days') }} as days_beyond_window
from {{ ref('fct_payments') }}
where refund_lag_days > {{ var('incremental_lookback_days') }}
