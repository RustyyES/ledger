# Chapter 14 — Three metrics that need real SQL

> Source: [`transform/models/marts/finance/`](../../transform/models/marts/finance/)

Three metrics that look simple and aren't. Each has a trap that a `GROUP BY`
walks straight into.

---

# 1. MRR with proration

## The question

*"What is our monthly recurring revenue?"*

## Why it's not a SUM

A customer on `basic` (\$19/mo) upgrades to `pro` (\$49/mo) on 15 February. What
did they contribute to February MRR?

The wrong answers, in the order people give them:

1. **\$49** — takes the current plan for the whole month. Overstates.
2. **\$19** — takes the plan at the start. Understates.
3. **\$34** — averages the two. Right *only* when the change lands exactly
   mid-month.

The right answer is calendar-day weighted:

```
14/28 × $19  +  14/28 × $49  =  $34.00     (February, 28 days)
```

...which in this example **coincidentally equals answer 3**. Which is exactly
why testing proration on a mid-month change proves nothing. Use the 15th of a
31-day month and the naive average is off by 3.2%.

## The three things you need

### 1. A daily spine

MRR is a **stock**, not a flow. It has a value on *every* day, including days
when nothing happened.

Aggregate the event log directly and you get rows only on days something
happened. So: start from `dim_date` and join *to* the events, not the other way
round.

### 2. Plan periods reconstructed from the event log

`subscriptions.plan_id` knows today's plan and nothing else. To know what
someone was paying in March, you must replay the event log into intervals.

That's [`int_subscriptions__plan_periods.sql`](../../transform/models/intermediate/int_subscriptions__plan_periods.sql),
and it has **four traps**:

**Trap 1 — `lead()` and the open interval.**
```sql
lead(occurred_at) over (partition by subscription_id order by occurred_at) as valid_to
```
The last interval has no `lead`, so `valid_to` is NULL. That must mean "still
open", not "zero length".

**Trap 2 — pause is not a plan change.**
A paused subscription still exists and earns **nothing**. Treat pause as a plan
change and you overstate MRR by the entire paused book. At 1.2% monthly pause
rate compounding, that isn't a rounding error.

**Trap 3 — a trial earns nothing.**
`created` opens an interval, but revenue doesn't start until the trial converts.
Count trials as revenue and you inflate MRR by the entire trial population — of
which only 31% ever converts.

**Trap 4 — `to_plan_id` is null on pause/resume/cancel.**
Those transitions don't change plan, so the column is null. You must carry the
plan forward:

```sql
coalesce(
    events.to_plan_id,
    last_value(events.to_plan_id ignore nulls) over (
        partition by events.subscription_id
        order by events.occurred_at, events.subscription_event_id
        rows between unbounded preceding and current row
    )
) as effective_plan_id
```

Join on `to_plan_id` directly and you drop every interval following a pause.

### 3. A daily rate

```sql
plans.monthly_cents * 1.0 / spine.days_in_month as daily_mrr_cents
```

Note `days_in_month`, not 30. A customer who upgrades on 15 February contributes
14/28, not 14/30. Finance defines this, and getting it wrong moves reported MRR
by up to 3%.

## The bug: MRR under-reported by 87%

The filter was:

```sql
where plan_periods.revenue_state = 'earning'
```

April 2025 reported **\$199/month** across the entire business. Exactly one
enterprise subscription.

### Why

Intervals get their `revenue_state` from the event that opened them. The
overwhelming majority of subscriptions emit exactly **one** event — `created` —
whose state is `trialing`.

In this dataset: **2,000 trialing intervals against 159 earning ones.**

So a customer who signed up, converted, and never changed plan had **no earning
interval at all** and contributed nothing to MRR for their entire life. Only
customers who had upgraded, downgraded or resumed were counted.

### The fix

```sql
where plan_periods.can_earn_revenue
```

where

```sql
(revenue_state in ('earning')
 or (revenue_state = 'trialing' and trial_ends_at is not null)) as can_earn_revenue
```

The trial boundary is already handled by `revenue_from`, which starts an interval
at the *later* of its own start and `trial_ends_at`. So a trialing interval
correctly earns nothing during the trial and everything after — and a trial that
never converts is closed by its cancellation before `revenue_from` is reached.

Paused and ended intervals stay excluded, so trap 2's protection is intact.

### What found it

No schema test could have. Every key unique, every foreign key resolving,
nothing null. The number was simply wrong.

It was found by `assert_mrr_reconciles_to_subscription_state` — which compares
MRR against a **completely independent** calculation. Chapter 15 is about why
that kind of test is worth more than all the schema tests combined.

---

# 2. Cohort retention

## The question

*"Of customers who signed up in March, how many are still here 6 months later?"*

## The trap: right-censoring

The March 2025 cohort can be observed at month 12. The March 2026 cohort
**cannot** — month 12 hasn't happened yet.

Emit a row for it anyway and it shows **0% retention**. Average across cohorts at
month 12 and the number is dragged toward zero by cohorts that simply haven't
aged.

This is not a small effect. With 18 months of history and monthly cohorts, **over
half** of all (cohort, period) cells are unobservable. A naive implementation
reports a retention curve that collapses — and the collapse is entirely an
artefact of the calendar.

## The fix

```sql
date_diff('month', cohort_month, cast({{ warehouse_now() }} as date)) as months_observable
...
where cohort_activity.period_number <= cohort_sizes.months_observable
```

Rows beyond the observable window are **not emitted**. And a flag makes the
boundary explicit:

```sql
(cohort_activity.period_number <= cohort_sizes.months_observable) as is_complete_period
```

so a consumer averaging across cohorts can filter rather than having to know
this.

The metrics API surfaces it too:

```json
{
  "cells": [...],
  "incomplete_cohorts": ["2026-06-01", "2026-07-01", "2026-08-01"]
}
```

> **Don't emit a value you can't observe.** A missing row is honest. A zero is a
> lie that averages.

## A second subtlety: what counts as "retained"?

```sql
where plan_periods.can_earn_revenue
```

Retained means **still earning revenue**, not merely "the row still exists". A
cancelled customer whose row is still in the database is churned. Counting them
retained is how retention numbers end up flattering.

---

# 3. Rolling revenue with gap fill

## The question

*"What's our 7-day rolling revenue?"*

## The trap, and it's the one interview screens test

The obvious implementation:

```sql
select order_date,
       sum(net) over (order by order_date rows between 6 preceding and current row)
from daily_revenue
```

On data with no zero-order days this looks right.

The moment a day has no orders — Christmas week, an outage, a small market's
first month — **that day has no row**. And `rows between 6 preceding` counts
**ROWS**, not **DAYS**.

So a "7-day" window spanning a 3-day gap silently averages over **10 calendar
days**.

The number isn't obviously wrong. It's just quietly smoothed — and every anomaly
you were trying to detect is exactly what got smoothed away.

## Why `range` isn't enough either

```sql
range between interval '6 days' preceding and current row
```

This fixes the arithmetic in engines that support it. But it still emits **no
row** for the missing day. A chart drawn from it draws a straight line across the
gap instead of a dip to zero — so you can't see the outage at all.

## The fix is structural, not syntactic

`LEFT JOIN` from `dim_date` **first**, so every calendar day exists with a zero.
*Then* apply the window.

```sql
gap_filled as (
    select
        spine.date_day,
        coalesce(daily_actuals.net_revenue_usd_cents, 0) as net_revenue_usd_cents,
        (daily_actuals.order_date is null)               as is_gap_filled_day
    from spine
    left join daily_actuals on daily_actuals.order_date = spine.date_day
),

windowed as (
    select gap_filled.*,
        sum(net_revenue_usd_cents) over (
            order by date_day rows between 6 preceding and current row
        ) as rolling_7d_net_usd_cents
    from gap_filled
)
```

**This is the whole reason `dim_date` is generated rather than derived from the
facts.** A spine built from the data has gaps in exactly the places that matter.

And the flag:

```sql
(daily_actuals.order_date is null) as is_gap_filled_day
```

distinguishes "genuinely zero" from "filled". A day with no orders is real
information; hiding that it was interpolated is not.

## The test

```python
def test_rolling_revenue_returns_every_calendar_day(client, auth):
    points = client.get("/metrics/revenue/rolling?window=7", headers=auth).json()["points"]
    days = [date.fromisoformat(p["date_day"]) for p in points]
    expected = (days[-1] - days[0]).days + 1
    assert len(days) == expected, f"{expected - len(days)} calendar day(s) missing"
```

Counts calendar days between first and last and asserts you got that many rows.
Simple, and it catches the entire class of bug.

---

## The pattern across all three

Each metric has the same shape of trap:

| Metric | Trap | Fix |
|---|---|---|
| MRR | It's a stock, not a flow | Daily spine, not GROUP BY |
| Cohorts | Unobservable cells look like zeros | Don't emit them |
| Rolling | Missing days aren't zero days | LEFT JOIN the spine *first* |

And all three share one root cause:

> **The absence of a row is not the same as a row containing zero — and SQL will
> not tell you which one you have.**

## Try it

```bash
curl -H "X-API-Key: dev-key-change-me" \
  'localhost:8001/metrics/mrr?granularity=month' | jq '.points[-3:]'

curl -H "X-API-Key: dev-key-change-me" \
  'localhost:8001/metrics/cohorts?max_periods=6' | jq '.incomplete_cohorts'
```

---

Next: **[Chapter 15 — Testing data (which is not testing code)](15-testing-data.md)**
