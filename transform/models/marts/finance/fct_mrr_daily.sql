{{ config(materialized='table', tags=['marts', 'finance', 'metric']) }}

/*
    Daily MRR with mid-month proration. Grain: one row per day.

    ===========================================================================
    THE PRORATION PROBLEM.

    A customer on `basic` ($19/mo) upgrades to `pro` ($49/mo) on 15 February.
    What did they contribute to February MRR?

    The wrong answers, in the order people usually give them:

      1. $49 -- takes the current plan for the whole month. Overstates.
      2. $19 -- takes the plan at the start. Understates.
      3. $34 -- averages the two. Right only when the change lands exactly
         mid-month; wrong every other day, and wrong in a way that looks
         plausible enough to survive review.

    The right answer is calendar-day weighted:

        14/28 x $19  +  14/28 x $49  =  $34.00   (February, 28 days)

    ...which in this example coincidentally matches (3), which is exactly why
    testing proration on a mid-month change proves nothing. The tests use the
    15th of a 31-day month, where the naive average is off by 3.2%.

    Getting there needs three things a single GROUP BY cannot give you:

      1. A DAILY SPINE. Aggregating the event log directly gives you rows on
         days when something happened. MRR is a stock, not a flow -- it has a
         value on every day, including days when nothing happened at all.
      2. PLAN PERIODS from the event log (`int_subscriptions__plan_periods`),
         because `subscriptions.plan_id` only knows about today.
      3. A DAILY RATE, `monthly_cents / days_in_month`, summed across the days
         a plan was actually in force.

    Which is why this is a spine LEFT JOINed to intervals, and not a GROUP BY.
    ===========================================================================
*/

with spine as (

    select
        date_day,
        days_in_month,
        month_start_date,
        year_month
    from {{ ref('dim_date') }}
    where
        date_day
        >= (select min(cast(valid_from as date)) from {{ ref('int_subscriptions__plan_periods') }})
        and date_day <= cast({{ warehouse_now() }} as date)

),

plan_periods as (

    select * from {{ ref('int_subscriptions__plan_periods') }}

),

plans as (

    select
        plan_id,
        plan_code,
        monthly_cents,
        plan_tier
    from {{ ref('dim_plan') }}

),

-- One row per (day, subscription) for every day a subscription was earning.
daily_subscription_revenue as (

    select
        spine.date_day,
        spine.days_in_month,
        spine.year_month,
        plan_periods.subscription_id,
        plan_periods.customer_id,
        plans.plan_id,
        plans.plan_code,
        plans.plan_tier,
        plans.monthly_cents,

        -- Calendar-day proration. NOT monthly_cents / 30.
        plans.monthly_cents * 1.0 / spine.days_in_month as daily_mrr_cents

    from spine
    inner join plan_periods
    -- Revenue starts at `revenue_from`, which already accounts for the
        -- trial boundary; see int_subscriptions__plan_periods.
        on
            spine.date_day >= cast(plan_periods.revenue_from as date)
            and (
                plan_periods.valid_to is null
                or spine.date_day < cast(plan_periods.valid_to as date)
            )
    inner join plans
        on plan_periods.plan_id = plans.plan_id

    -- ----------------------------------------------------------------------
    -- Filter on `can_earn_revenue`, NOT on `revenue_state = 'earning'`.
    --
    -- This originally read `revenue_state = 'earning'` and under-reported MRR
    -- by roughly 50-90% depending on the month. The reason is that the
    -- overwhelming majority of subscriptions only ever emit ONE event --
    -- `created` -- whose revenue_state is 'trialing'. In this dataset that is
    -- 2,000 intervals against 159 'earning' ones. A customer who signs up,
    -- converts, and never changes plan had no 'earning' interval at all and
    -- contributed nothing to MRR for their entire life.
    --
    -- The trial boundary is already handled by `revenue_from`, which starts an
    -- interval at the LATER of its own start and `trial_ends_at`. So a trialing
    -- interval correctly earns nothing during the trial and everything after
    -- it, and a trial that never converts has its interval closed by the
    -- cancellation event before `revenue_from` is ever reached.
    --
    -- Paused and ended intervals remain excluded -- `can_earn_revenue` is false
    -- for both -- so the original protection against overstating the paused
    -- book is intact.
    --
    -- `assert_mrr_reconciles_to_payments` is what caught this, and it is the
    -- only test that could have. Every key was unique, every foreign key
    -- resolved, nothing was null. The number was simply wrong.
    -- ----------------------------------------------------------------------
    where plan_periods.can_earn_revenue

),

daily as (

    select
        date_day,
        year_month,
        days_in_month,

        count(distinct subscription_id)
            as active_subscriptions,
        count(distinct customer_id)
            as paying_customers,
        sum(daily_mrr_cents)
            as daily_mrr_cents,

        -- Monthly-run-rate equivalent: what the book would bill over a full
        -- month at this instant. This is the number people mean by "MRR".
        sum(daily_mrr_cents)
        * max(days_in_month)            as mrr_cents,

        sum(case when plan_tier = 'entry' then daily_mrr_cents else 0 end)
        * max(days_in_month)            as mrr_entry_cents,
        sum(case when plan_tier = 'mid' then daily_mrr_cents else 0 end)
        * max(days_in_month)            as mrr_mid_cents,
        sum(case when plan_tier = 'top' then daily_mrr_cents else 0 end)
        * max(days_in_month)            as mrr_top_cents

    from daily_subscription_revenue
    group by 1, 2, 3

),

with_movement as (

    select
        daily.*,

        lag(daily.mrr_cents) over (order by daily.date_day)                   as prev_mrr_cents,
        daily.mrr_cents - lag(daily.mrr_cents) over (order by daily.date_day) as mrr_change_cents,

        lag(daily.active_subscriptions)
            over (order by daily.date_day)
            as prev_active_subscriptions,
        daily.active_subscriptions
        - lag(daily.active_subscriptions)
            over (order by daily.date_day)
            as subscription_change,

        daily.mrr_cents / nullif(daily.active_subscriptions, 0)               as arpa_cents

    from daily

)

select
    {{ date_key('date_day') }} as date_key,
    with_movement.*
from with_movement
