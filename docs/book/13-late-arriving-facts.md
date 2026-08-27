# Chapter 13 — The late-arriving fact

> Source: [`transform/models/marts/finance/fct_payments.sql`](../../transform/models/marts/finance/fct_payments.sql),
> [`macros/cdc.sql`](../../transform/macros/cdc.sql)

This is the most important chapter in the book. If you read one, read this one.

## The setup

A customer pays for something on **8 March**.

On **20 March**, twelve days later, they request a refund. Perfectly normal —
2.1% of payments get refunded, and the delay is uniform between 1 and 14 days.

Your warehouse has a table, `fct_payments`, with one row per payment. It has a
column, `refunded_amount_cents`, which is the total refunded against that
payment.

**Question:** after the refund lands, does that column update?

## The obvious implementation

Rebuilding a 5-million-row table daily is wasteful, so the table is
*incremental*: each run processes only what's new.

Here's what almost everyone writes:

```sql
{{ config(materialized='incremental', unique_key='payment_id') }}

select ...
from {{ ref('stg_payments') }}

{% if is_incremental() %}
  where processed_at > (select max(processed_at) from {{ this }})
{% endif %}
```

In English: *"only process payments newer than the newest one I already have."*

This is sensible-looking, it's what the pattern looks like in every tutorial,
and it is **wrong**.

## Walking it through

Today is 20 March. The refund just landed.

1. The incremental run starts.
2. `select max(processed_at) from fct_payments` → roughly **20 March**.
3. The filter becomes `where processed_at > '2026-03-20'`.
4. Our payment's `processed_at` is **8 March**.
5. `8 March > 20 March` → **false**.
6. **The payment is not selected.**
7. `refunded_amount_cents` stays **0**.

And here's the part that matters:

> **It stays 0 forever.**
>
> The next run has an even later `max(processed_at)`. And the next. That payment
> will never be selected again as long as the table exists.

## Why nobody notices

Go through what would normally catch a bug:

| Check | Result |
|---|---|
| Did anything error? | No. The run succeeded. |
| Is `payment_id` unique? | Yes. |
| Are there nulls? | No. `refunded_amount_cents` is `0`, a perfectly valid number. |
| Do foreign keys resolve? | Yes. |
| Do row counts match staging? | Yes — the row is there, it's just wrong. |
| Does the dashboard render? | Beautifully. |

**Every single test passes.** The number is just too high.

## The scale of it

- 2.1% of payments get refunded
- essentially all of them arrive after the payment's own timestamp

So **roughly 2% of all payment rows are permanently wrong** — and not a random
2%. Refunds concentrate in the high-value orders customers actually bother to
dispute. The error is biased toward your largest transactions.

Net revenue is overstated, permanently, by an amount nobody can see. You find
out when finance reconciles against the payment processor. Months later.

## The reframe

The bug is in what the filter is asking.

`processed_at` answers *"when did this happen in the business?"*

The question you actually need answered is *"what have I not looked at yet?"*

Those are the same question **only if data arrives in the order it happened**.
Refunds break that assumption, and once broken, business-time filtering is
structurally unable to see late data.

So filter on **arrival time**, not event time:

```sql
{% macro incremental_lookback(column='_ingested_at') %}
    {%- if is_incremental() %}
        {{ column }} > (
            select coalesce(max({{ column }}), '1970-01-01'::timestamp)
                   - interval '{{ var("incremental_lookback_days") }} days'
            from {{ this }}
        )
    {%- else %}
        1 = 1
    {%- endif %}
{% endmacro %}
```

`_ingested_at` is *when the record entered the pipeline* — set by the sink from
the Kafka broker timestamp (Chapter 8).

A refund that arrives today has `_ingested_at = today`, **no matter how old the
payment it reverses is**. Late-arriving is recently-ingested, by definition.

That's why Chapter 8 insisted `_ingested_at` be a real, meaningful, deterministic
value rather than an afterthought.

## The second half: making the parent look recent

Here's a subtlety that's easy to miss.

The **refund** row was recently ingested. But we're building `fct_payments`, and
the **payment** row hasn't changed at all. Its own `_ingested_at` is still 8
March.

So even with the right filter, the payment still isn't selected — because
nothing about the *payment* is recent.

The fix lives in the intermediate model:

```sql
-- int_payments__with_refunds.sql
greatest(
    payments._ingested_at,
    coalesce(refund_totals.last_refund_ingested_at, payments._ingested_at)
) as _ingested_at
```

A payment is "recent" if **either** the payment row **or any of its refunds**
was recently ingested.

> The lookback macro and this `greatest()` are **one mechanism split across two
> files**. Either one alone doesn't work. This is worth remembering when you
> read someone else's pipeline: the incremental filter is only half the design.

## Why a lookback *window* at all?

Why not just `where _ingested_at > max(_ingested_at)`?

Because a single run isn't atomic. Records arrive while it's running. A strict
`>` on the maximum leaves a gap — records ingested during the run are newer than
what you read, but you've already advanced past them.

The lookback deliberately **re-reads a window of already-processed data**. It's
cheap (the rows are re-derived, not corrupted — the model is idempotent) and it
closes the gap.

Which means the window has to be **at least as long as the worst arrival delay**.

## Getting the number wrong — my own bug

I set it to **15 days**, reasoning:

> The generator's maximum refund delay is 14 days. 15 gives a day of headroom.

Correct reasoning. Wrong answer.

A test caught it:

```
FAIL 1  between_fct_payments_refund_lag_days__15__0
```

One payment had a lag of **16 days**.

### Why 16

`date_diff('day', a, b)` counts **calendar boundaries crossed**, not elapsed
24-hour periods.

```
payment:  2026-04-17 23:53
refund:   2026-05-03 00:35     ← 15 days and 42 minutes of wall time
date_diff('day', ...) = 16     ← because of where the boundaries fall
```

A payment at 23:53 refunded 14 days and one hour later crosses **sixteen**
day-boundaries.

I had reasoned about the business rule (14 days) and written a filter in
different units (calendar boundaries) without noticing they weren't the same
thing.

### The corrected derivation

```
 14   business maximum refund delay
+ 2   calendar-boundary counting at the edges of a day
+ 5   operational slack: sink backlog, a missed DAG run over a weekend, clock skew
= 21
```

### Why generous rather than tight

The costs are **wildly asymmetric**:

| Too large | Too small |
|---|---|
| A few extra days reprocessed per run | Silent, permanent wrongness |
| Cheap, bounded, visible in the run time | Invisible, on your highest-value rows |

Given that, 21 with five days of operational slack is obviously right. Tuning it
down to save compute would be optimising the cheap side of an asymmetric bet.

### The test that stops it happening again

```sql
-- assert_no_late_arrival_outside_lookback.sql
select payment_id, refund_lag_days,
       {{ var('incremental_lookback_days') }} as configured_lookback_days,
       refund_lag_days - {{ var('incremental_lookback_days') }} as days_beyond_window
from {{ ref('fct_payments') }}
where refund_lag_days > {{ var('incremental_lookback_days') }}
```

The bound reads from the **same variable** the model uses, so the two can never
drift apart.

> **The window is not a tuning parameter. It is an assertion about reality, and
> this is the test that holds reality to it.**

There's a matching test on the generator side too — raise the generator's max
delay past the warehouse's window and `test_refund_delay_stays_inside_the_configured_lookback`
fails, pointing at the dbt variable. The assumption is guarded from both ends.

## `append` is not safe with a lookback

A related bug, in `fct_subscription_events`.

That table's source is genuinely append-only — the app never updates an event
row. So `incremental_strategy='append'` looks obviously correct and is cheaper
than delete+insert.

It **doubled the table** on the second run.

**Why:** `append` performs *no* deduplication — `unique_key` is ignored
entirely. Meanwhile the lookback deliberately re-selects a window of
already-loaded rows. And because the bulk export gives every historical row the
*same* `_ingested_at`, the first incremental run re-selected all of history and
inserted it again.

> **The rule:** `append` is only safe when the incremental filter can never
> re-select a row it has already loaded. A lookback is *precisely* a filter that
> re-selects rows on purpose. The two are mutually exclusive, and the failure is
> silent — no error, just a table with twice the revenue.

`delete+insert` on `unique_key` makes reprocessing idempotent, which is what a
lookback requires.

## The general pattern

This generalises well past refunds:

| Late-arriving fact | Delay |
|---|---|
| Refunds and chargebacks | days to weeks |
| Mobile events from an offline device | hours to days |
| Corrections from an upstream system | unbounded |
| Anything with a human approval step | unbounded |
| Late-reporting third-party partners | days |

**The questions to ask about any incremental model:**

1. Can a row change **after** its business timestamp? If yes, business-time
   filtering is wrong.
2. Can a **child** row change the parent's derived values? If yes, the parent
   needs the child's arrival time.
3. What's the **worst observed** delay — measured, not assumed?
4. Is there a **test** that fails when reality exceeds the window?

## Try it

```bash
make prove-lookback
```

This finds a payment processed ~12 days ago, issues a refund against it **today**,
runs an **incremental** build (not a full refresh — that would prove nothing),
and checks the value changed:

```
==> 1/5  Finding a succeeded payment from ~12 days ago
    payment ddf337ec-... processed 2026-08-16 -- 12 days ago
==> 2/5  Reading the warehouse's current value
    refunded_amount_cents = 0
==> 3/5  Issuing a refund TODAY against a 12-day-old payment
==> 4/5  Running an INCREMENTAL dbt build
==> 5/5  Re-reading the warehouse
    refunded_amount_cents = 577  (was 0)

PASSED: a 12-day-late refund landed in the incremental build.
```

The evidence is written to `results/lookback_proof.json`.

Try it with the naive filter and step 5 reads `0`. That's the whole chapter.

---

Next: **[Chapter 14 — Three metrics that need real SQL](14-three-metrics.md)**
