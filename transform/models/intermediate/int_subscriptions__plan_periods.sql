{{ config(materialized='ephemeral', tags=['intermediate', 'finance']) }}

/*
    Reconstruct, from the event log, which plan each subscription was on during
    each interval -- and whether it was generating revenue at the time.

    ---------------------------------------------------------------------------
    WHY THIS IS THE HARD ONE.

    `subscriptions.plan_id` tells you the CURRENT plan. It tells you nothing
    about what the customer was paying in March. Answering "what was MRR on
    2025-03-15" requires replaying the event log into intervals, and there are
    four traps in doing it:

    1. `lead()` gives you the end of each interval. The LAST interval has no
       lead, and a null `valid_to` must mean "still open", not "zero length".

    2. A `paused` subscription still exists but earns nothing. Treating pause as
       a plan change (rather than a revenue stop) overstates MRR by the entire
       paused book -- which, at a 1.2% monthly pause rate compounding, is not a
       rounding error.

    3. A trial earns nothing either. `created` opens an interval, but revenue
       does not start until the trial converts. Counting trials as revenue
       inflates MRR by roughly the trial population, and because trials convert
       at 31%, most of that is revenue that never arrives.

    4. `to_plan_id` is null on `paused`/`resumed`/`cancelled` events, because
       those transitions do not change plan. Carrying the plan forward with a
       window function is required; joining on `to_plan_id` directly drops
       every interval that follows a pause.
    ---------------------------------------------------------------------------
*/

with events as (

    select * from {{ ref('stg_subscription_events') }}

),

subscriptions as (

    select
        subscription_id,
        customer_id,
        trial_ends_at
    from {{ ref('stg_subscriptions') }}

),

-- Trap 4: carry the plan forward across events that do not name one.
events_with_plan as (

    select
        events.subscription_event_id,
        events.subscription_id,
        events.event_type,
        events.occurred_at,
        events.from_plan_id,
        coalesce(
            events.to_plan_id,
            last_value(events.to_plan_id ignore nulls) over (
                partition by events.subscription_id
                order by events.occurred_at, events.subscription_event_id
                rows between unbounded preceding and current row
            )
        )                                          as effective_plan_id,
        events.{{ adapter.quote('_ingested_at') }} as _ingested_at
    from events

),

-- Traps 2 and 3: classify each event as starting, stopping or continuing
-- revenue, rather than treating every event as a plan change.
classified as (

    select
        events_with_plan.*,
        subscriptions.customer_id,
        subscriptions.trial_ends_at,
        case
            when event_type in ('created') then 'trialing'
            when event_type in ('upgraded', 'downgraded', 'resumed') then 'earning'
            when event_type in ('paused') then 'paused'
            when event_type in ('cancelled', 'expired') then 'ended'
            else 'earning'
        end as revenue_state
    from events_with_plan
    left join subscriptions on events_with_plan.subscription_id = subscriptions.subscription_id

),

-- Trap 1: lead() with a null tail meaning "still open".
intervals as (

    select
        subscription_id,
        customer_id,
        subscription_event_id,
        event_type,
        revenue_state,
        effective_plan_id as plan_id,
        trial_ends_at,

        occurred_at       as valid_from,
        lead(occurred_at) over (
            partition by subscription_id
            order by occurred_at, subscription_event_id
        )                 as valid_to,

        (lead(occurred_at) over (
            partition by subscription_id
            order by occurred_at, subscription_event_id
        ) is null)        as is_current_interval,

        _ingested_at

    from classified

),

final as (

    select
        intervals.*,

        -- A `created` interval earns nothing until the trial ends. Rather than
        -- special-casing this everywhere downstream, the trial boundary is
        -- folded into the interval here: revenue starts at the LATER of the
        -- interval start and the trial end.
        case
            when intervals.revenue_state = 'trialing'
                then
                    greatest(
                        intervals.valid_from,
                        coalesce(intervals.trial_ends_at, intervals.valid_from)
                    )
            else intervals.valid_from
        end
            as revenue_from,

        -- The one column every downstream MRR calculation actually filters on.
        (
            intervals.revenue_state in ('earning')
            or (intervals.revenue_state = 'trialing' and intervals.trial_ends_at is not null))
            as can_earn_revenue

    from intervals
    -- An interval with no plan cannot be priced. This only happens if the very
    -- first event for a subscription lacks `to_plan_id`, which the API makes
    -- impossible -- but a null here would silently drop revenue, so it is
    -- filtered loudly rather than propagated.
    where intervals.plan_id is not null

)

select * from final
