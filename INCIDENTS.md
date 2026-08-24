# Incidents

Real failures hit while building this, what they looked like, and what changed
because of them.

The entries are written the way a postmortem should be: the *symptom* first,
because that is all you have at the start; then the cause; then what would have
caught it sooner. Several of these were found by tests that already existed,
which is the argument for the tests.

The dangerous pattern running through most of them: **the pipeline did not
error.** It produced a number. The number was wrong.

---

## 001 · MRR under-reported by 87%

**Severity:** critical (silent — would have reached a board deck)
**Found by:** `assert_mrr_reconciles_to_subscription_state`

### Symptom

`fct_mrr_daily` reported MRR of \$199/month across the whole business for
April 2025 — exactly one enterprise subscription. Subscription payments for the
same month totalled \$1,578. Every schema test was green: keys unique, foreign
keys resolving, no nulls, row counts reconciling.

### Cause

`fct_mrr_daily` filtered plan-period intervals on `revenue_state = 'earning'`.

Intervals get their `revenue_state` from the event that opened them. The
overwhelming majority of subscriptions emit exactly **one** event — `created` —
whose state is `trialing`. In this dataset that is 2,000 intervals against 159
`earning` ones.

So a customer who signed up, converted from trial, and never changed plan had
no `earning` interval at all, and contributed **nothing** to MRR for their
entire life. Only customers who had upgraded, downgraded or resumed were
counted.

### Fix

Filter on `can_earn_revenue`, which is true for `earning` *and* for `trialing`
intervals that have a trial end. The trial boundary was already handled by
`revenue_from` — the interval starts earning at the later of its own start and
`trial_ends_at` — so a trial that never converts is closed by its cancellation
before `revenue_from` is reached. Paused and ended intervals remain excluded.

### Why nothing else caught it

Every schema test passed and would have kept passing. `unique`, `not_null` and
`relationships` prove the warehouse is internally *consistent*; they cannot
prove it is *correct*. The only check that could find this was one comparing
two independent derivations of the same quantity.

### Changed

- `fct_mrr_daily` filters on `can_earn_revenue`, with the reasoning in the model.
- The reconciliation test was already there and did its job. It is now also run
  by `quality_dag` every 4 hours, because MRR can break between builds.

---

## 002 · 22% of orders silently dropped from every dimensional aggregate

**Severity:** critical (silent)
**Found by:** `assert_every_order_resolves_a_customer_version`

### Symptom

4,550 of 20,247 orders had a NULL `customer_key` in `fct_orders`. Revenue by
country was missing roughly a fifth of the business. `fct_orders` row counts
reconciled exactly against staging, so
`assert_fct_orders_rowcount_matches_staging` was green.

### Cause

`dbt_valid_from` on a snapshot's **first** version is the moment the snapshot
first *ran*, not when the customer came into existence.

The entire 18-month backfill predates the first snapshot run. So for every
historical customer, version 1 was valid from "this morning", and every order
they placed before that fell outside all versions. The as-of range join in
`fct_orders` matched nothing and produced NULL.

### Why it is worse than it looks

A range join that matches nothing fails **quietly**. The row stays in the fact
table — hence the clean row-count reconciliation — and simply disappears from
anything grouping by `customer_key`. The two checks disagree and neither looks
obviously wrong.

### Fix

`dim_customer` back-dates version 1 to the customer's own `created_at`:

```sql
case when row_number() over (partition by customer_id order by dbt_valid_from) = 1
     then least(dbt_valid_from, created_at)
     else dbt_valid_from
end as valid_from
```

A customer cannot have placed an order before they existed, so this is exactly
the right lower bound rather than an approximation.

### Changed

- `dim_customer` back-dates the first version.
- `assert_every_order_resolves_a_customer_version` added, and it is the reason
  the residual 99-order case below was found immediately afterwards.

---

## 003 · 99 orders placed before their customer existed

**Severity:** high (data generation)
**Found by:** `assert_every_order_resolves_a_customer_version`, immediately after 002

### Symptom

After fixing 002, orphaned orders fell from 4,550 to 99 — not to zero. All 99
had `placed_at_utc` a few hours **before** their customer's `created_at`.

### Cause

In the backfill's one-off order generator, customer eligibility is indexed by
signup **date**, but the order is then given a random **hour** of that day. A
customer who signed up at 18:00 was therefore eligible for an order at 09:00
the same morning.

### Fix

Reject the draw when `when < customer.signup`. One line, plus a comment
explaining that the downstream consequence is a NULL `customer_key` rather than
anything visible.

### Note

This was a bug in the *generator*, not the pipeline — but it produced exactly
the shape of bad data a real upstream produces, and the warehouse test caught it
without knowing which side it came from. That is the argument for asserting
business rules in the warehouse even when the application also enforces them.

---

## 004 · The incremental lookback was too short, by two days

**Severity:** critical (latent — would have silently corrupted refund totals)
**Found by:** `between_fct_payments_refund_lag_days`

### Symptom

One payment had `refund_lag_days = 16` against a configured lookback of 15.

### Cause

The lookback was derived from the business parameter alone: the load generator's
maximum refund delay is 14 days, so 15 "leaves a day of headroom".

It does not, because `date_diff('day', ...)` counts **calendar boundaries
crossed**, not elapsed 24-hour periods. A payment at 23:53 refunded 14 days and
one hour later crosses 16 day-boundaries.

### Impact if it had shipped

Any refund arriving beyond the window is invisible to every subsequent
incremental run. `refunded_amount_cents` for that payment is permanently wrong
and `net_amount_cents` permanently overstated. No error, no null — exactly the
failure mode the lookback exists to prevent, reintroduced by a reasoning error
about the *units* of the window.

### Fix

Window widened to 21, derived rather than guessed:

```
 14   business maximum
+ 2   calendar-boundary counting
+ 5   operational slack (sink backlog, missed DAG run, clock skew)
= 21
```

The test's bound now reads from `var('incremental_lookback_days')` so the two
can never drift apart.

### Changed

- `incremental_lookback_days: 21`, with the derivation in `dbt_project.yml`.
- `assert_no_late_arrival_outside_lookback` fails the build if reality exceeds
  the window again.
- `make prove-lookback` demonstrates a 12-day-late refund landing through an
  incremental build; the evidence is in `results/lookback_proof.json`.

---

## 005 · `fct_subscription_events` doubled on the second run

**Severity:** high
**Found by:** a row-count sanity check while inspecting the marts (before the
`unique` test on that model had been written — which is why it was written)

### Symptom

`fct_subscription_events` held 7,662 rows against 3,831 in the source. Exactly
double, after the second `dbt build`.

### Cause

`incremental_strategy='append'` combined with a lookback filter.

`append` performs **no** deduplication — `unique_key` is ignored entirely —
while the lookback deliberately *re-selects* a window of already-loaded rows.
And because the bulk export gives every historical row the same `_ingested_at`,
the first incremental run re-selected the whole of history and inserted it
again.

`append` had been chosen because `subscription_events` is genuinely append-only
at source, which makes it look obviously safe.

### The general rule

> `append` is safe only when the incremental filter can never re-select a row it
> has already loaded. A lookback window is precisely a filter that re-selects
> rows on purpose.

### Changed

- `delete+insert` on `unique_key`, with the failure documented in the model.
- The `unique` test on `subscription_event_id` — absent at the time — added.
  The marts schema file had not been written yet, which is exactly why the bug
  survived to be found by hand.

---

## 006 · Every `date_key` foreign key was orphaned

**Severity:** critical (would have emptied every metric joined through `dim_date`)
**Found by:** `relationships_fct_orders_date_key__ref_dim_date_`

### Symptom

All 20,247 `date_key` values in `fct_orders` failed the relationship test
against `dim_date`. The same calendar day produced two different hashes:

```
dim_date    2026-05-11  ->  36d5e5634b8a6457469ba5a320b4f30a
fct_orders  2026-05-11  ->  e1fc1478943112a42717cec0f7dacbef
```

### Cause

`dim_date` hashed its spine column directly; `fct_orders` hashed
`cast(placed_at_utc as date)`. Both "obviously" produce the key for the same
day — but the spine column was a `TIMESTAMP`, so `dbt_utils` stringified it as
`'2026-05-11 00:00:00'` while the fact stringified `'2026-05-11'`.

### Fix

A single `date_key(expr)` macro that casts to `date` before hashing, used
everywhere. The bug existed because one concept had two implementations.

### Changed

- `date_key()` macro added; all five call sites use it.
- The lesson is recorded in the macro itself, not just here: *a key defined in
  two places is a key with two definitions.*

---

## 007 · `fiscal_quarter` had twelve distinct values

**Severity:** low (loud — would have been noticed immediately in use)
**Found by:** `accepted_values` on `dim_date.fiscal_quarter`

### Symptom

`fiscal_quarter` contained `1.0, 1.333, 1.667, 2.0, 2.333, ...` — twelve values
where four were expected.

### Cause

`(month - 2 + 12) % 12 / 3 + 1`. In both DuckDB and Snowflake, `/` on integers
is **float** division.

### Fix

Explicit `cast(floor(... / 3) as integer) + 1`.

### Why it is here despite being trivial

It is the cheapest possible illustration of what `accepted_values` is *for*. The
column was never null, never duplicated, and every value was plausible in
isolation. Only an enumeration of what the column is allowed to contain catches
this, and it caught it on the first run after the test was written.

---

## 008 · The load generator was about to take Prometheus down

**Severity:** medium (would have been an outage under sustained load)
**Found by:** reading the generator's own summary output

### Symptom

The live generator's per-endpoint stats read:

```
ok:/orders/09c56913-a776-480d-9302-cfd6d2573f11/payments   1
ok:/orders/0dc62a26-1798-42cc-8406-8f19d779b47c/payments   1
... one line per order ...
```

Those same strings were the `endpoint` label on a Prometheus counter.

### Cause

The generator labelled its metrics with the **concrete** request path. One time
series per order, forever. At 8,000 orders/day that is millions of series, and
it takes the Prometheus instance with it.

### Why it is worth recording

The commerce API's middleware gets this right — it reads the matched route off
the ASGI scope, and `test_metrics_labels_use_templated_routes_not_concrete_ids`
enforces it. The generator is an HTTP *client*, has no route to read, and the
same mistake was made two hundred lines away from the comment warning about it.

### Fix

A `template_path()` helper that collapses UUIDs back to `/{id}`, applied to
every label and stats key.

### Changed

- `template_path()` in the generator.
- The equivalent test now exists on the metrics API too
  (`test_prometheus_metrics_use_templated_routes`), because the same mistake is
  available on every service that emits metrics.

---

## 009 · The backfill produced future-dated orders

**Severity:** medium (would have masked a real timezone bug)
**Found by:** `assert_no_future_dated_orders`

### Symptom

39 orders had `placed_at_utc` up to 15 hours in the future.

### Cause

The backfill picks a day from a weighted distribution that includes *today*,
then assigns a random hour 0–23. Generating at 07:44 therefore produced orders
"today at 22:39".

### Why a trivial-looking bug got a fix rather than a wider test bound

`assert_no_future_dated_orders` exists to catch **timezone** errors: 15% of
orders arrive as naive local wall-clock, and reading Asia/Tokyo time as UTC
pushes evening orders into tomorrow. A handful of future-dated rows is the
visible tip of a bug that is silently mis-bucketing *every* legacy-client order
by up to 14 hours.

A generator that trips this test for an unrelated reason makes the real signal
unreadable. The correct response was to fix the generator, not to widen the
tolerance.

### Changed

- The backfill clamps every generated timestamp to the generation instant.
- The test keeps a 5-minute grace window for clock skew — deliberately small,
  since an hour of tolerance would hide the single-hour DST errors it exists to
  find.

---

## 010 · `stg_orders` could not build across a schema migration

**Severity:** medium (would have broken the build on migration day)
**Found by:** running `dbt build` before applying migration 0003

### Symptom

```
Binder Error: Referenced column "channel" not found in FROM clause!
```

### Cause

`stg_orders` referenced `orders.channel`, added by migration 0003. Before that
migration is applied, no Parquet file has the column — and referencing a
nonexistent column is a **binder** error, not a cast error, so `try_cast` does
not help. `union_by_name` only resolves it if at least one file has the column.

The transitional state is the one that actually breaks things: while the prefix
contains files both with and without the column.

### Fix

A `raw_column()` macro that reads the Parquet schema at **compile** time and
substitutes a `null` literal when the column is absent. The model then builds
before, during and after the migration with no operator action.

### Rejected alternative

Gating the model behind a var an operator flips on migration day. It works right
up until somebody forgets, and then it fails at 02:00 on a Saturday.

### Changed

- `raw_column()` and `raw_columns()` macros, with the results cached per compile.
- `make schema-change` applies the migration against the running stack so the
  whole path can be exercised deliberately rather than discovered.

---

## Patterns worth extracting

Ten incidents, and they fall into four groups.

**1. The pipeline does not error; it produces a wrong number.** 001, 002, 004,
005, 006. Not one of these raised an exception. Schema tests prove internal
consistency and cannot prove correctness — only a check comparing two
independent derivations can. That is what
`assert_mrr_reconciles_to_subscription_state` is, and it earned its cost twice.

**2. A join that loses rows is worse than one that errors.** 002 and 006 both
left row counts reconciling perfectly while the data quietly left the building.
Row-count checks and relationship tests catch different halves of this, and both
are needed.

**3. Reasoning about a parameter is not the same as measuring it.** 004 and 007.
"14 days plus headroom" and "divide by 3" were both correct reasoning about the
wrong units. Derive bounds from measurement, then assert the measurement.

**4. The same mistake is available in every service.** 008. The metric-label
cardinality bug was documented in a comment two hundred lines from where it was
then made. A comment is not a control; a test is.
