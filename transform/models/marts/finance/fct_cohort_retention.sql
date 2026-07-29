{{ config(materialized='table', tags=['marts', 'finance', 'metric']) }}

/*
    Cohort retention. Grain: one row per (signup cohort month, period number).

    ===========================================================================
    THE TRAP: RIGHT-CENSORING.

    The March 2025 cohort can be observed at month 12. The March 2026 cohort
    cannot -- month 12 has not happened yet. If you emit a row for it anyway,
    it shows 0% retention, and any average across cohorts at month 12 is
    dragged toward zero by cohorts that simply have not aged.

    That is not a small effect. With 18 months of history and monthly cohorts,
    over half of all (cohort, period) cells are unobservable. A naive
    implementation reports a retention curve that collapses, and the collapse is
    entirely an artefact of the calendar.

    So: `months_observable` is computed per cohort, and rows beyond it are NOT
    emitted. `is_complete_period` marks the boundary explicitly so a consumer
    averaging across cohorts can filter rather than having to know this.
    ===========================================================================
*/

with customers as (

    select
        customer_id,
        created_at,
        cast(date_trunc('month', created_at) as date) as cohort_month
    from {{ ref('stg_customers') }}

),

cohort_sizes as (

    select
        cohort_month,
        count(distinct customer_id)                                           as cohort_size,
        -- How many complete months this cohort can actually be observed for.
        date_diff('month', cohort_month, cast({{ warehouse_now() }} as date))
            as months_observable
    from customers
    group by 1

),

-- A customer is "retained" in a month if they were still earning revenue at
-- any point in it. Revenue, not mere existence: a cancelled customer whose row
-- still exists is churned, and counting them retained is how retention numbers
-- end up flattering.
activity as (

    select distinct
        plan_periods.customer_id,
        cast(date_trunc('month', spine.date_day) as date) as active_month
    from {{ ref('dim_date') }} as spine
    inner join {{ ref('int_subscriptions__plan_periods') }} as plan_periods
        on
            spine.date_day >= cast(plan_periods.revenue_from as date)
            and (
                plan_periods.valid_to is null
                or spine.date_day < cast(plan_periods.valid_to as date)
            )
    where
        plan_periods.can_earn_revenue
        and spine.date_day <= cast({{ warehouse_now() }} as date)

),

cohort_activity as (

    select
        customers.cohort_month,
        date_diff('month', customers.cohort_month, activity.active_month) as period_number,
        count(distinct activity.customer_id)                              as retained_customers
    from customers
    inner join activity on customers.customer_id = activity.customer_id
    where activity.active_month >= customers.cohort_month
    group by 1, 2

),

final as (

    select
        cohort_sizes.cohort_month,
        cohort_activity.period_number,
        cohort_sizes.cohort_size,
        cohort_activity.retained_customers,

        round(
            cohort_activity.retained_customers * 100.0
            / nullif(cohort_sizes.cohort_size, 0), 2
        )                                                                 as retention_pct,

        cohort_sizes.months_observable,
        -- The flag that lets a consumer average across cohorts honestly.
        (cohort_activity.period_number <= cohort_sizes.months_observable)
            as is_complete_period

    from cohort_sizes
    inner join cohort_activity on cohort_sizes.cohort_month = cohort_activity.cohort_month

    -- Right-censoring. Emitting these rows is what makes a retention curve lie.
    where cohort_activity.period_number <= cohort_sizes.months_observable

)

select
    {{ ledger_surrogate_key(['cohort_month', 'period_number']) }} as cohort_period_key,
    final.*
from final
