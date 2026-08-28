# Chapter 19 — Jenga: what breaks if you pull this out

The most useful question you can ask about any piece of a system:

> **"What breaks if I delete this?"**

If the answer is "nothing", it shouldn't be there. If it's "I don't know", you
don't understand it yet.

This chapter answers it for every significant piece. Each entry says what
happens, **how you'd find out**, and how long that would take — because the
scariest ones are the pieces whose absence you'd never notice.

---

## Source system

### `Idempotency-Key` on POST

**Breaks:** the load generator's timeout retries create duplicate orders.
Revenue in the warehouse is overstated by roughly the timeout rate.

**How you'd find out:** finance reconciles against the payment processor.
**When:** months.

**Why it's invisible:** the duplicates have different primary keys and different
timestamps. Every uniqueness test passes. Two legitimate identical orders and two
duplicated ones look exactly the same.

---

### `updated_at` moving on every mutation

**Breaks:** the dbt snapshot's `timestamp` strategy never sees the change, so
that customer version is **never recorded**.

**How you'd find out:** a customer relocates and history doesn't reflect it —
but only if you happen to look at that customer.
**When:** possibly never.

**Guarded by:** `test_patch_moves_updated_at` in the *commerce API* suite, with
a comment explaining it protects a warehouse model.

---

### `REPLICA IDENTITY FULL`

**Breaks:** deletes arrive with only the primary key. You cannot tell what was
deleted, or a soft delete from a hard one.

**How you'd find out:** when you try to write a model that needs the before-image
and discover it's empty.
**When:** immediately, but only once you need it.

---

### The load generator's *shape*

**Breaks:** nothing visibly. Everything still runs.

**What's actually lost:** seasonality models can't be validated (everything is
flat), cohort retention is meaningless (no cohort structure), the incremental
lookback is untested (nothing arrives late), SCD2 is decoration (nobody
relocates).

**How you'd find out:** you don't. Your tests still pass — they've just stopped
testing anything.
**When:** in production, on real data.

> **This is the most dangerous entry in the chapter.** Every other item breaks
> something visible. This one hollows out your entire test suite while leaving it
> green.

**Guarded by:** the 32 tests in `services/loadgen/tests/`, which assert on the
shape itself.

---

## Ingestion

### Write-then-commit ordering (2 lines)

**Breaks:** data loss on **every** crash. Records between the commit and the
write are gone.

**How you'd find out:** row counts drift after an unclean restart.
**When:** you'd probably attribute it to something else for a long time.

**The whole guarantee is these two lines in this order.**

---

### `(partition, offset)` deduplication

**Breaks:** duplicate rows after any restart.

**How you'd find out:** `assert_fct_orders_rowcount_matches_staging` fails.
**When:** next build. **Good** — this one is loud.

---

### Broker timestamp for `_ingested_at`

**Breaks:** byte-identical replay becomes impossible by construction. And more
seriously, the incremental lookback loses its meaning — `_ingested_at` no longer
reliably orders arrivals.

**How you'd find out:** `make prove-idempotency` fails.
**When:** immediately, *if you run it*.

---

### The sort before writing

**Breaks:** replay produces different bytes — **sometimes**.

**How you'd find out:** intermittent proof failures.
**When:** eventually, confusingly.

> Worse than a consistent failure, because it *sometimes works* and gets
> attributed to flakiness.

---

### Deletes written as rows

**Breaks:** the warehouse can never distinguish a soft delete from a hard one,
and the "do deleted customers keep their orders?" decision gets baked into the
transport layer irreversibly.

**How you'd find out:** when someone asks a question the raw layer can no longer
answer.
**When:** and by then you'd have to re-extract from the source.

---

### The time-based flush

**Breaks:** quiet tables never land. Their data sits in memory indefinitely and
is lost on restart.

**How you'd find out:** freshness alerts on the quiet tables.
**When:** hours.

---

### The graceful drain on SIGTERM

**Breaks:** up to 60 seconds of consumed records discarded on **every deploy**.

**How you'd find out:** you wouldn't directly — the offsets weren't committed, so
they'd be re-consumed. But there's a window where the data isn't in the warehouse
and nobody knows.

---

### The schema guard

**Breaks:** an incompatible type change silently coerces. Every number built on
that column is quietly wrong.

**How you'd find out:** manual reconciliation.
**When:** months.

**Also breaks:** one bad table takes down ingestion for all seven.

---

### The schema registry file surviving restarts

**Breaks:** the guard treats every table as newly-seen and **silently accepts a
change it would otherwise have rejected**.

**How you'd find out:** you don't. The guard is now decorative.
**When:** never.

> Subtle and nasty: the mechanism still runs, still logs, still looks healthy.
> It has just stopped having an opinion.

---

### Not committing offsets on a halt

**Breaks:** the backlog that arrived during the incident is discarded. When a
human resolves the schema question, the data is gone.

**How you'd find out:** a gap in the raw layer.
**When:** during the post-incident review, when it's too late.

---

## Modelling

### The layer contract (no joins in staging)

**Breaks:** nothing immediately. Models still build.

**What's lost:** every staging model stops being independently debuggable.
Debugging one now requires understanding several.

**How you'd find out:** the first time a number is wrong and you can't isolate
where.
**When:** gradually, as an increase in how long everything takes.

---

### `else 'unknown'` in the status normalisation

**Breaks:** a new legacy spelling silently creates a sixth status. Revenue drops
out of the "completed" bucket into one nobody looks at.

**How you'd find out:** revenue appears to fall.
**When:** whenever someone investigates a dip — and they'd look at the business
first, not the pipeline.

**Guarded by:** the `accepted_values` test, which fails immediately instead.

---

### `dim_date` being generated rather than derived

**Breaks:** the spine has gaps on zero-order days. Rolling windows silently span
more calendar days than they claim. Every anomaly gets smoothed away.

**How you'd find out:** `test_rolling_revenue_returns_every_calendar_day`.
**When:** immediately, *if that test exists*.

---

### SCD2 on `dim_customer`

**Breaks:** historical revenue by country silently restates whenever anyone
relocates. The same report run twice returns different numbers.

**How you'd find out:** someone notices last year's numbers moved.
**When:** and you won't be able to explain it, because the old value doesn't
exist anywhere.

---

### The as-of join in `fct_orders`

**Breaks:** either every fact fans out by version count (revenue multiplies), or
`customer_key` is NULL and facts vanish from aggregates.

**How you'd find out:** the fan-out is caught by `unique(order_id)`. The NULL is
caught by `assert_every_order_resolves_a_customer_version` — which exists
*because* this happened.

---

### The `_ingested_at` incremental filter

**Breaks:** ~2% of payment rows are permanently wrong, concentrated in the
highest-value transactions.

**How you'd find out:** manual reconciliation.
**When:** months. **This is the flagship silent failure.**

---

### The `greatest()` in `int_payments__with_refunds`

**Breaks:** the same thing — even with the right filter, a payment whose only
change is a new child row still looks untouched.

> The filter and this `greatest()` are **one mechanism in two files**. Removing
> either produces the identical silent failure. This is the entry most likely to
> be lost in a refactor, because the two halves don't look related.

---

### `delete+insert` instead of `append`

**Breaks:** the table doubles on the second run.

**How you'd find out:** `unique` on the primary key.
**When:** next build. Loud, thankfully.

---

### The reconciliation tests

**Breaks:** nothing visibly. Every schema test still passes.

**What's lost:** the only check capable of finding *wrongness* rather than
*malformedness*.

**How you'd find out:** you don't. Bugs 1 and 2 in Chapter 18 would both still
be in the codebase.

> Second most dangerous entry after the generator shape. Both are cases where
> removing something leaves the suite green and hollow.

---

## Orchestration

### Snapshot before models

**Breaks:** today's facts are attributed against yesterday's dimension versions.
Every customer who changed country today gets their old country for a full day.

**How you'd find out:** a one-day-lagged discrepancy that self-heals.
**When:** essentially never — it looks like normal lag.

---

### `DBT_RUN_AS_OF`

**Breaks:** backfills are not reproducible. Re-running a past date builds it
against today's cutoff.

**How you'd find out:** `make backfill-proof` fails.
**When:** immediately if run; otherwise when two reports built either side of a
backfill disagree.

---

### `mode="reschedule"` on sensors

**Breaks:** each sensor holds a worker slot for its whole wait. Enough of them
deadlock the pool.

**How you'd find out:** the scheduler stops making progress.
**When:** under load, at the worst time.

---

### `max_retry_delay`

**Breaks:** exponential backoff reaches 40 minutes by the third retry. The DAG
blows through its 90-minute SLA while technically still running.

**How you'd find out:** SLA misses with no failed tasks.
**When:** confusingly.

---

### `max_active_runs=1`

**Breaks:** two concurrent dbt runs against one DuckDB file corrupt it. Against
Snowflake they race on the same incremental tables.

**How you'd find out:** corruption, or non-deterministic results.
**When:** during a backfill, which is when concurrency actually happens.

---

## Serving

### Templated route labels

**Breaks:** one Prometheus time series per order. Millions of series.

**How you'd find out:** Prometheus falls over.
**When:** days to weeks, and it takes your monitoring with it exactly when you
need it.

---

### The staleness 503

**Breaks:** the API serves stale numbers that look current.

**How you'd find out:** somebody makes a decision on data that predates the thing
they're deciding about.
**When:** unknowable.

---

### The read-only mount

**Breaks:** the serving layer *can* write to the warehouse. Nothing does today.

**How you'd find out:** the first time somebody adds a "quick fix" endpoint.
**When:** and then it's a convention, not a guarantee.

---

### A new DuckDB connection per query

**Breaks:** a long-lived connection holds a file lock and **blocks the dbt
rebuild**. The API prevents the warehouse it serves from being refreshed.

**How you'd find out:** builds hang.
**When:** and it's unpleasant to diagnose, because both sides look healthy.

---

## The five most dangerous removals

Ranked by *how long until you'd notice*:

| Rank | Remove | Notice after |
|---|---|---|
| 1 | The load generator's shape | Never — suite goes hollow |
| 2 | The reconciliation tests | Never — suite goes hollow |
| 3 | The schema registry surviving restart | Never — guard goes decorative |
| 4 | The `_ingested_at` filter (or its `greatest()`) | Months |
| 5 | Idempotency keys | Months |

Notice the pattern: **the most dangerous things to remove are the ones whose
absence produces no symptom.** Not the ones that break loudly.

That's the whole chapter in one line, and it's the difference between a system
you can operate and one that's quietly lying to you.

---

Next: **[Chapter 20 — Why this tool and not that one](20-tool-choices.md)**
