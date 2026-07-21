{{ config(materialized='table', tags=['marts', 'core', 'dimension']) }}

/*
    Plan dimension.

    Retired plans are INCLUDED. `legacy_starter` is inactive but still carries
    live subscriptions, and filtering it out here would silently drop those
    customers from every revenue join -- a join that loses rows rather than
    erroring is the worst kind.

    Not SCD2, deliberately. Plan PRICES do change, and when they do this
    dimension will need versioning. It is Type 1 today because the source has no
    price history to reconstruct from: `plans` is mutated in place with no
    `updated_at`, so there is nothing to snapshot against. Recording the
    limitation is more honest than a snapshot that would silently be wrong.
    DESIGN.md carries this under "what I would do differently".
*/

select
    {{ ledger_surrogate_key(['plan_id']) }}     as plan_key,
    plan_id,
    plan_code,
    monthly_cents,
    monthly_cents / 100.0                   as monthly_amount,
    currency_code,
    is_active,
    price_rank,

    case
        when plan_code = 'legacy_starter' then 'legacy'
        when monthly_cents < 2500 then 'entry'
        when monthly_cents < 10000 then 'mid'
        else 'top'
    end                                     as plan_tier

from {{ ref('stg_plans') }}
