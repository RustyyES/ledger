/*
    MRR accrual reconciled against cash billed. Tolerance 2.5%.

    ===========================================================================
    WHY THIS TOLERANCE IS 2.5% AND NOT 0.5%, AND WHY THAT IS THE HONEST ANSWER.

    The spec asks for this reconciliation at 0.5%. It cannot hold at 0.5%, and
    the reason is an accounting fact rather than a modelling defect:

        A customer billed on 3 November for the month ahead, who cancels on
        13 November, is not refunded. The business BILLED a full month and
        keeps it. The MRR run-rate stops on the 13th.

    So `billed` and `accrued MRR` diverge by exactly the unearned remainder of
    every mid-cycle cancellation. At ~4% monthly churn with cancellations
    uniformly distributed through the cycle, that is a structural ~1-2% gap
    which no amount of correct modelling removes. Measured over a trailing
    six months on this data it is 1.07%.

    There were three ways to respond to that:

      1. Widen the tolerance until it passes and say nothing. This is the
         common choice and it is the worst one: the test still LOOKS like a
         0.5%-grade check to anyone reading the suite, and the number that
         would actually indicate a problem is now inside the bound.

      2. Quietly compare something easier -- successful payments only, or a
         single convenient month -- until 0.5% is reachable. This is worse
         still, because the test now passes while measuring the wrong thing.

      3. Split it in two. Reconcile the thing that CAN be exact at 0.5%
         (`assert_mrr_reconciles_to_subscription_state.sql`, which compares two
         independent derivations of the same run-rate), and keep this one as a
         deliberately looser end-to-end check with the gap explained.

    This is (3). A tolerance is a claim about how much difference is legitimate;
    inflating one to silence a test converts a measurement into a decoration.

    ---------------------------------------------------------------------------
    WHAT THIS TEST STILL EARNS ITS PLACE FOR.

    It is loose, not useless. It is the only check that spans the ENTIRE chain
    -- event log, plan periods, proration, spine, orders, payments -- in one
    number. When MRR was under-reported by 87% because `fct_mrr_daily` filtered
    out every trialing interval, this is the test that showed it, and it would
    have shown it at any tolerance below 80%.

    Cumulative over a trailing six months, not month by month: a monthly
    comparison is dominated by anniversary timing (a subscription billed on the
    30th is nearly all next month's accrual), which is noise rather than signal.
    Accumulating lets that cancel and leaves the real divergence.
*/

with window_bounds as (

    select
        cast(date_trunc('month', cast({{ warehouse_now() }} as date)) as date) as current_month_start,
        cast(date_trunc('month', cast({{ warehouse_now() }} as date))
             - interval '6 months' as date)                                   as window_start

),

-- Revenue EARNED on an accrual basis: the daily run-rate, summed over days.
accrued as (

    select sum(fct_mrr_daily.mrr_cents * 1.0 / fct_mrr_daily.days_in_month) as accrued_cents
    from {{ ref('fct_mrr_daily') }}, window_bounds
    where fct_mrr_daily.date_day >= window_bounds.window_start
      -- The current month is excluded: it is partially accrued by definition
      -- and would drag the comparison every single day.
      and fct_mrr_daily.date_day < window_bounds.current_month_start

),

-- Cash BILLED for subscriptions. All attempts, not just successful ones --
-- MRR is a billing concept, and scoping to `is_successful` would fold the 3.5%
-- payment failure rate into the comparison. Collection rate is its own metric.
billed as (

    select sum(fct_payments.gross_amount_cents) as billed_cents
    from {{ ref('fct_payments') }}
    inner join {{ ref('fct_orders') }} using (order_id), window_bounds
    where fct_orders.subscription_id is not null
      and fct_orders.order_date >= window_bounds.window_start
      and fct_orders.order_date < window_bounds.current_month_start

),

compared as (

    select
        cast(accrued.accrued_cents as bigint)   as accrued_mrr_cents,
        billed.billed_cents,
        cast(accrued.accrued_cents - billed.billed_cents as bigint) as delta_cents,
        abs(accrued.accrued_cents - billed.billed_cents)
            / nullif(billed.billed_cents, 0)    as delta_pct
    from accrued, billed

)

select *
from compared
where delta_pct > {{ var('mrr_billing_tolerance') }}
  and billed_cents > 0
