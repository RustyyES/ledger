{{
    config(
        materialized='incremental',
        unique_key='subscription_event_id',
        incremental_strategy='append',
        tags=['marts', 'finance', 'fact']
    )
}}

/*
    Subscription lifecycle fact. Grain: one row per event. The source for MRR.

    ---------------------------------------------------------------------------
    A BUG THIS MODEL SHIPPED WITH, AND WHY THE STRATEGY IS WHAT IT IS.

    `subscription_events` is genuinely append-only at source, so
    `incremental_strategy='append'` looks obviously correct here -- cheaper than
    delete+insert, and no merge scan. It was the first thing this model used.

    It duplicated every row on the second run.

    The reason is that `append` performs NO deduplication: it inserts whatever
    the SELECT returns, and `unique_key` is ignored entirely. Meanwhile the
    lookback filter deliberately RE-SELECTS a 15-day window of already-loaded
    rows. The two combine into an unconditional double-insert of everything in
    the window -- and because the bulk export gives every historical row the
    same `_ingested_at`, the very first incremental run re-selected the entire
    history and doubled the table.

    So the rule is: `append` is only safe when the incremental filter can never
    re-select a row it has already loaded. A lookback window is precisely a
    filter that re-selects rows on purpose. The two are mutually exclusive, and
    the failure is silent -- no error, just a table with twice the revenue.

    `delete+insert` on `unique_key` makes reprocessing idempotent, which is what
    a lookback requires. `assert_no_duplicate_facts.sql` and the `unique` test
    on `subscription_event_id` both fail loudly if this regresses.
    ---------------------------------------------------------------------------

    The filter is on `_ingested_at`, not `occurred_at`. An event can be
    back-dated by the API (the backfill does exactly that) and `occurred_at`
    therefore is not monotonic with arrival. `_ingested_at` always is.
*/

with events as (

    select * from {{ ref('stg_subscription_events') }}
    where {{ incremental_lookback('_ingested_at') }}

),

subscriptions as (

    select
        subscription_id,
        customer_id
    from {{ ref('stg_subscriptions') }}

),

plans as (

    select
        plan_id,
        plan_key,
        monthly_cents,
        plan_code
    from {{ ref('dim_plan') }}

),

customers as (

    select
        customer_key,
        customer_id,
        valid_from,
        valid_to_or_infinity
    from {{ ref('dim_customer') }}

),

final as (

    select
        events.subscription_event_id,
        events.subscription_id,
        subscriptions.customer_id,
        customers.customer_key,

        events.event_type,
        events.from_plan_id,
        events.to_plan_id,
        from_plan.plan_key                         as from_plan_key,
        to_plan.plan_key                           as to_plan_key,
        from_plan.plan_code                        as from_plan_code,
        to_plan.plan_code                          as to_plan_code,

        coalesce(to_plan.monthly_cents, 0)
        - coalesce(from_plan.monthly_cents, 0)     as mrr_delta_cents,

        events.starts_revenue,
        events.stops_revenue,
        events.changes_revenue,

        events.occurred_at,
        cast(events.occurred_at as date)           as event_date,
        {{ date_key('events.occurred_at') }} as date_key,
        events.created_at,

        events.{{ adapter.quote('_ingested_at') }} as _ingested_at

    from events
    left join subscriptions on events.subscription_id = subscriptions.subscription_id
    left join plans as from_plan on events.from_plan_id = from_plan.plan_id
    left join plans as to_plan on events.to_plan_id = to_plan.plan_id
    left join customers
        on
            subscriptions.customer_id = customers.customer_id
            and events.occurred_at >= customers.valid_from
            and events.occurred_at < customers.valid_to_or_infinity

)

select * from final
