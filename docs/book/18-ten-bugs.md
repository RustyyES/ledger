# Chapter 18 — Ten bugs

Ten real defects hit while building this. The full postmortems are in
[`INCIDENTS.md`](../../INCIDENTS.md); this chapter is about what they *teach*.

Read the count first:

> **Bugs that raised an exception: 0**
> **Bugs that produced a wrong number: 10**

That ratio is the single most important fact in this book.

---

## The catalogue

### 1. MRR under-reported by 87%

**Symptom:** April 2025 reported \$199/month across the whole business. One
enterprise subscription.

**Cause:** filtered on `revenue_state = 'earning'`, which excluded every
subscription whose only event was `created` — 2,000 intervals against 159.

**Found by:** the reconciliation test comparing two independent derivations.
Nothing else could have.

**Lesson:** *the majority case is easy to exclude by accident when you filter on
a derived category.* Ask "how many rows does this filter keep?" before trusting
it.

---

### 2. 22% of orders silently orphaned

**Symptom:** 4,550 of 20,247 orders had a NULL `customer_key`. Revenue by country
was missing a fifth of the business. **Row counts reconciled perfectly.**

**Cause:** the SCD2 snapshot's first version was valid from when the snapshot
first *ran*, not from when the customer existed. Everything predating the first
snapshot fell outside all versions.

**Lesson:** *a range join that matches nothing fails silently.* The row stays in
the table and disappears from every aggregate. Always test that a range join
resolves for every row.

---

### 3. 99 orders placed before their customer existed

**Symptom:** after fixing #2, orphans went 4,550 → 99. Not zero.

**Cause:** a bug in the **data generator** — customers were eligible by signup
*date*, but orders got a random *hour*, so a customer who signed up at 18:00 was
eligible for a 09:00 order the same morning.

**Lesson:** *a warehouse test caught a bug in the source system without knowing
which side it came from.* That's the argument for asserting business rules in the
warehouse even when the app also enforces them.

---

### 4. The lookback was too short, by two days

**Symptom:** one payment with a refund lag of 16 days against a 15-day window.

**Cause:** reasoning from the business parameter (14-day max delay) while the
filter measured in different units — `date_diff('day', ...)` counts calendar
boundaries, not elapsed hours.

**Lesson:** *reasoning about a parameter is not the same as measuring it.*
Derive bounds from measurement, then write a test that asserts the measurement.

---

### 5. `fct_subscription_events` doubled

**Symptom:** 7,662 rows against 3,831 in the source. Exactly double, on the
second run.

**Cause:** `incremental_strategy='append'` combined with a lookback window.
`append` performs no deduplication; the lookback deliberately re-selects rows.

**Lesson:** *`append` is only safe when the filter can never re-select a loaded
row.* A lookback is precisely such a filter. They're mutually exclusive.

**Bonus lesson:** the `unique` test on that model didn't exist yet. That's why
it survived to be found by hand.

---

### 6. Every `date_key` foreign key orphaned

**Symptom:** all 20,247 relationship tests failed. Same date, two different
hashes.

**Cause:** one model hashed a `TIMESTAMP`, another hashed a `DATE`. The
stringification differed.

**Lesson:** *a key defined in two places is a key with two definitions.* One
macro, one cast, one answer.

---

### 7. `fiscal_quarter` had twelve values

**Symptom:** `1.0, 1.333, 1.667, 2.0, ...`

**Cause:** `/` on integers is float division in both DuckDB and Snowflake.

**Lesson:** *`accepted_values` is absurdly cheap and catches an entire class of
bug.* Every value was non-null, non-duplicate and individually plausible. Only an
enumeration of what a column is *allowed* to contain finds this.

---

### 8. The load generator was about to take Prometheus down

**Symptom:** per-endpoint stats reading one line per order UUID — and those
strings were metric labels.

**Cause:** labelled metrics with the concrete request path instead of the route
template. One time series per order.

**Lesson:** *a comment is not a control.* The commerce API's middleware has this
right, with a test. The generator made the same mistake two hundred lines from
the comment warning about it.

---

### 9. The backfill generated future-dated orders

**Symptom:** 39 orders up to 15 hours in the future.

**Cause:** the generator picked *today* as a candidate day, then assigned a
random hour 0–23.

**Lesson:** *fix the generator, don't widen the test.*
`assert_no_future_dated_orders` exists to catch **timezone** bugs. A generator
that trips it for an unrelated reason makes the real signal unreadable.

---

### 10. `stg_orders` couldn't build across a migration

**Symptom:** `Binder Error: Referenced column "channel" not found`.

**Cause:** the model referenced a column added by a migration that hadn't been
applied. Referencing a nonexistent column is a *binder* error — `try_cast`
doesn't help.

**Lesson:** *the transitional state is the one that breaks things.* Not "before"
or "after" the migration, but *during*, when the folder contains files both with
and without the column.

---

## The four patterns

### Pattern 1 — the pipeline doesn't error, it produces a wrong number

**Bugs 1, 2, 4, 5, 6.**

Not one raised an exception. This is the defining characteristic of data bugs
and it's why data testing is a different discipline from code testing.

> Schema tests prove your warehouse is internally **consistent**. They cannot
> prove it's **correct**. Only a check comparing two independent derivations can.

That's what `assert_mrr_reconciles_to_subscription_state` is, and it paid for
itself twice.

### Pattern 2 — a join that loses rows is worse than one that errors

**Bugs 2 and 6.**

Both left row counts reconciling perfectly while data quietly left the building.

- **Loss** — an inner join where a left join was meant. No test that *examines*
  rows can see rows that aren't there.
- **Orphaning** — the row is present, the foreign key resolves to nothing, and it
  vanishes from every dimensional aggregate.

Row-count checks and relationship tests catch different halves. You need both.

### Pattern 3 — reasoning about a parameter ≠ measuring it

**Bugs 4 and 7.**

"14 days plus headroom" and "divide by 3" were both correct reasoning applied in
the wrong units.

> Derive bounds from measurement. Then assert the measurement, with the test's
> bound reading from the same variable the code uses, so they can't drift.

### Pattern 4 — the same mistake is available everywhere

**Bug 8.**

The metric-label bug was *documented in a comment* two hundred lines from where
it was then made.

> A comment is not a control. A test is. And if a mistake is possible in one
> service, write the test in all of them.

---

## What this means for how you work

Three habits, in order of value:

**1. For every model, ask "what would a wrong answer look like?"**
Not "would this error" — it won't. Would it be too big? Too small? Missing a
category? Then write the test that catches *that*.

**2. Find a second path to the same number.**
The reconciliation test is expensive to design and worth more than every schema
test combined. If you can't think of a second path, that's a signal about how
well you understand the metric.

**3. When a test fails for a correct reason, fix the comparison — never the
bound.**
Bug 9 and the MRR tolerance question are both instances. Widening a bound to
silence a test converts a measurement into a decoration.

---

Next: **[Chapter 19 — Jenga: what breaks if you pull this out](19-jenga.md)**
