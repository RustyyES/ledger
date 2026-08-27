# Chapter 15 — Testing data (which is not testing code)

> Source: [`transform/tests/`](../../transform/tests/),
> [`transform/models/**/*.yml`](../../transform/models/)

## The difference

Testing **code**: given input X, does the function return Y? You control the
input. It's deterministic. You write a test once and it answers forever.

Testing **data**: you don't control the input. It arrives from a system you
don't own, it changes daily, and "correct" is often a business judgement rather
than a fact.

So the question changes. Not *"does this function work?"* but:

> **"Is this data still shaped the way every model downstream assumes?"**

## The two kinds

### Generic tests — reusable, declared in YAML

```yaml
- name: order_id
  data_tests: [unique, not_null]
- name: order_status
  data_tests:
    - accepted_values:
        arguments:
          values: ["pending", "completed", "cancelled", "refunded"]
```

### Singular tests — a SQL file that must return zero rows

```sql
-- tests/assert_refund_not_exceeding_payment.sql
select payment_id, gross_amount_cents, refunded_amount_cents
from {{ ref('fct_payments') }}
where refunded_amount_cents > gross_amount_cents
```

Any row returned = failure. And crucially, the returned rows **are the
diagnostic** — you get the offending payments, not just a boolean.

This project has 174 tests: 161 generic, 13 singular.

## What generic tests can and cannot do

This is the most important idea in the chapter.

> **Schema tests prove your warehouse is internally CONSISTENT. They cannot
> prove it is CORRECT.**

Consider the MRR bug from Chapter 14 — revenue under-reported by 87%.

| Test | Result |
|---|---|
| `unique(date_day)` | ✅ pass |
| `not_null(mrr_cents)` | ✅ pass |
| `mrr_cents >= 0` | ✅ pass |
| `relationships` on every FK | ✅ pass |
| row counts reconcile | ✅ pass |

Every one green. The number was wrong by a factor of eight.

Generic tests check **shape**. They cannot check **meaning**, because they have
nothing to compare against.

## The test that catches meaning

To check correctness you need a **second, independent** way of computing the
same thing.

```sql
-- assert_mrr_reconciles_to_subscription_state.sql
```

Two paths to the same number:

**Path A — what the warehouse publishes:**
```
stg_subscription_events
  → int_subscriptions__plan_periods   (replay the log into intervals)
  → fct_mrr_daily                     (spine join, daily proration)
```

**Path B — the test:**
```
stg_subscriptions.started_at / trial_ends_at / ended_at / plan_id
  → sum(monthly_cents) for whatever is live on the day
```

They share **no logic**. Path A reconstructs history by replaying an event log.
Path B reads the source's own current-state columns.

If a plan-period interval is dropped, double-counted, or has a boundary computed
wrong, **Path A moves and Path B does not**.

This test found the two biggest defects in the project. Nothing else could have.

> **The most valuable data test compares two independent derivations of the same
> quantity.** Everything else checks shape.

## The window, and why it's short

Path B has a known blind spot: `stg_subscriptions.plan_id` holds only the
*current* plan. The source keeps no plan history — that's the entire reason Path
A exists. So Path B is accurate only where "the current plan" and "the plan in
force then" still coincide, and that decays going back:

```
trailing  7 days   0.25%      ← inside tolerance
trailing 14 days   0.81%
trailing 30 days   3.02%      ← Path B's own drift, not a model error
```

Seven days: long enough to average out boundary jitter (a trial ending at 23:00
lands on different days in each path), short enough that plan drift stays
negligible.

Extending the window wouldn't make the test *stronger*. It would make it fail
for a reason unrelated to the thing being tested. That's a real skill —
recognising when a test's own methodology, not the system, is the limiting
factor.

## Tolerances, and the temptation

Here's a situation you will genuinely face.

The spec asked for MRR to reconcile against **payments** at **0.5%**. It came
out at 1–2%, consistently.

The cause is an accounting fact, not a bug:

> A customer billed on 3 November who cancels on the 13th is **not refunded**.
> The business **billed** a full month and keeps it. The MRR run-rate **stops**
> on the 13th.

So billed and accrued MRR differ by the unearned remainder of every mid-cycle
cancellation — a structural 1–2% that no amount of correct modelling removes.

**Three possible responses:**

**1. Widen the tolerance to 3% and say nothing.**
The common choice, and the worst one. The test still *looks* like a 0.5%-grade
check to anyone reading the suite, and the number that would actually indicate a
problem is now inside the bound. You have converted a measurement into a
decoration.

**2. Quietly compare something easier** — successful payments only, or one
convenient month — until 0.5% is reachable.
Worse. The test now passes while measuring the wrong thing.

**3. Split it in two.**
Reconcile the thing that *can* be exact at 0.5%
(`assert_mrr_reconciles_to_subscription_state`), and keep the end-to-end check
at a tolerance that reflects the real difference, with the gap documented.

We did (3).

> **A tolerance is a claim about how much difference is legitimate. Inflating
> one to silence a test converts a measurement into a decoration.**

Both tests carry the reasoning in their headers, so the next person doesn't have
to rediscover it.

## Scoped tests

A blanket test that's always red is worse than no test — everyone learns to
ignore it, and then they ignore the day it means something.

```yaml
- name: processed_at
  data_tests:
    - not_null_where:
        arguments:
          condition: "payment_status = 'succeeded'"
```

A pending payment legitimately has no `processed_at`. A blanket `not_null` would
fail forever.

Same idea in `fct_payments`:

```yaml
- name: refund_lag_days
  data_tests:
    - between:
        arguments:
          min_value: 0
          max_value: "{{ var('incremental_lookback_days') }}"
```

The bound **reads from the same variable the model uses**, so the test and the
model can never drift apart. This is the test that caught the 15-vs-21-day bug
in Chapter 13.

## The seven business-rule tests

The spec named these, and they're the ones a practitioner writes:

| Test | Catches |
|---|---|
| `assert_mrr_reconciles_to_*` | Any modelling error in the revenue chain |
| `assert_refund_not_exceeding_payment` | Fan-out in the refund join |
| `assert_no_future_dated_orders` | Timezone resolution errors |
| `assert_scd2_no_overlapping_versions` | Both overlap **and** gaps |
| `assert_scd2_exactly_one_current_per_customer` | Both zero **and** two |
| `assert_every_completed_order_has_payment` | Status normalisation, both directions |
| `assert_fct_orders_rowcount_matches_staging` | Row loss **and** fan-out |

Plus six more added while building.

### Two worth reading closely

**`assert_no_future_dated_orders`** looks trivial. It's the test that proves
timezone resolution worked.

15% of orders arrive as naive local wall-clock. Read Asia/Tokyo time as UTC and
you're up to 9 hours ahead — and for an order placed this evening in Tokyo, that
lands *tomorrow*. A handful of future-dated orders is the **visible tip** of a
bug silently mis-bucketing every legacy-client order by up to 14 hours, most of
which land on the right day and are invisible.

The grace window is deliberately tiny (5 minutes). An hour of tolerance would
hide the single-hour DST errors it exists to find.

**`assert_fct_orders_rowcount_matches_staging`** looks like the most trivial
test in the suite. It's the only one that catches two failure modes that leave
everything else green:

- **Row loss** — an inner join where a left join was meant. Every remaining row
  is perfect. There are just fewer of them, and no test that *examines* rows can
  see rows that aren't there.
- **Fan-out** — joining `dim_customer` without an as-of predicate multiplies
  every order by its version count.

The test even reports which:

```sql
case when fact_count.n < staging_count.n
     then 'rows lost -- suspect an inner join'
     else 'rows gained -- suspect a dimension fan-out'
end as likely_cause
```

## `store_failures`

```yaml
tests:
  ledger:
    +store_failures: true
    +schema: test_failures
```

Failing rows are written to a table instead of vanishing with the log. When a
test fails at 2am you can query the failures rather than re-running the build to
see them.

## Source freshness

```yaml
loaded_at_field: _ingested_at
freshness:
  warn_after: {count: 2, period: hour}
  error_after: {count: 6, period: hour}
```

Measured on `_ingested_at`, not a business column. A source can legitimately have
no new **orders** at 4am. What it cannot legitimately have is no new **records
reaching the pipeline**, because the sink writes on a timer.

Note `plans` has freshness explicitly disabled — it changes twice a year, so an
alarm on it would fire permanently and train everyone to ignore the channel.

## Test at the boundary you depend on

Two tests in this project live in a *different service* from the model that needs
them:

- `test_patch_moves_updated_at` (commerce API) — the snapshot's timestamp
  strategy depends on it (Chapter 12).
- `test_refund_delay_stays_inside_the_configured_lookback` (load generator) —
  the warehouse's lookback depends on it (Chapter 13).

Both carry comments explaining what they protect.

> **When a model depends on an assumption about another system, test the
> assumption in that other system.** A comment saying "we assume X" is not a
> control.

## What to test, in priority order

1. **Reconciliation against an independent path.** The only test that finds
   *wrongness*. Expensive to design, worth more than everything else.
2. **Row-count checks between layers.** Catches silent loss and fan-out.
3. **`accepted_values` on every enum-like column.** Absurdly cheap; catches the
   `fiscal_quarter` class of bug instantly.
4. **`relationships` on every foreign key.** Catches orphans, which are silent.
5. **`unique` / `not_null` on keys.** The floor, not the ceiling.
6. **Scoped business rules.** Where the domain knowledge lives.

Most projects do 5 and stop. The value is concentrated in 1 and 2.

## Try it

```bash
cd transform && dbt build
# 196 nodes: 20 models, 2 seeds, 1 snapshot, 174 tests
```

Then break something deliberately and watch which test catches it:

```sql
-- add a sixth status spelling to the generator and rebuild
-- accepted_values on order_status fails immediately
```

---

Part IV done. Next: running it.

Next: **[Chapter 16 — Orchestration](16-orchestration.md)**
