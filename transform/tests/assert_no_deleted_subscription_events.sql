/*
    Subscription events are immutable. Nothing may delete one.

    The event log is the source of truth for MRR, and it is append-only by
    design -- the application has no code path that updates or deletes a row.
    A `_op = 'd'` here therefore means someone reached into the database
    directly and removed revenue history.

    Without this test that deletion is invisible: `cdc_latest` would faithfully
    drop the row, MRR would quietly step down, and the warehouse would present
    the new number with exactly as much confidence as the old one.
*/

select
    subscription_event_id,
    subscription_id,
    event_type,
    occurred_at,
    {{ adapter.quote('_op') }} as cdc_operation
from {{ ref('stg_subscription_events') }}
where {{ adapter.quote('_op') }} = 'd'
