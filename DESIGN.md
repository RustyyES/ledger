# Design decisions

Anyone can wire these tools together. The decisions are the evidence of
judgement, so this document records them — including the ones that turned out
to be wrong first, because how a decision was corrected is more informative
than the decision itself.

Each section states the choice, the alternative that was rejected, and what it
would cost to be wrong.

---

## 1. Bulk-load then stream, instead of a Debezium snapshot

**Decision.** History is exported straight from Postgres to Parquet inside one
`REPEATABLE READ` transaction. CDC starts from that transaction's LSN.
Debezium's `snapshot.mode` is `no_data`.

**Rejected.** `snapshot.mode: initial`, which streams every existing row
through Kafka before capturing changes.

**Why.** For 5M orders the snapshot is slow, holds a replication slot open for
its duration, and teaches nothing that streaming changes does not. Every
practitioner I have watched do this at scale has bulk-loaded and then attached
the stream.

**The part that is easy to get wrong.** The handoff has exactly one correct
ordering:

```
1. BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ
2. capture pg_current_wal_lsn()          <- inside the transaction
3. export every table from that snapshot
4. create the replication slot AT that LSN
5. start the connector
```

Create the slot *before* the snapshot and every row changed in between is
counted twice. The sink's `(partition, offset)` dedup absorbs the duplicates,
but the row-count reconciliation lies until the overlap clears.

Create it *after* the export finishes and every change made **during** the
export is lost — permanently, silently, and undetectably, because nothing
downstream can notice the absence of a row it never saw.

`REPEATABLE READ` rather than `READ COMMITTED` for the same reason at a smaller
scale: under `READ COMMITTED` each table gets its own snapshot, and an order
exported before its payment leaves a foreign key the warehouse cannot resolve.

`scripts/setup_cdc.sh` encodes the ordering, with the reasoning in comments,
because this is the sharpest edge in the entire pipeline.

---

## 2. The incremental lookback is 21 days, and it was 15 first

**Decision.** Incremental models filter on `_ingested_at` — when a record
reached the *pipeline* — with a 21-day lookback window.

**Rejected.** Filtering on the business timestamp (`placed_at`, `processed_at`),
which is what almost everyone writes first.

**Why the business timestamp is wrong.** Walk it through. Today is the 20th. A
refund arrives today against a payment processed on the 8th.

```sql
-- the wrong version
{% if is_incremental() %}
  where processed_at > (select max(processed_at) from {{ this }})
{% endif %}
```

* the payment's `processed_at` is the 8th
* `max(processed_at)` in the table is roughly the 20th
* `8th > 20th` is false — the payment is not selected
* `refunded_amount_cents` stays 0

It stays 0 **forever**. No error, no null, no failing test. At a 2.1% refund
rate uniformly spread over 1–14 days, roughly 2% of payment rows are
permanently wrong — concentrated, naturally, in the high-value orders customers
bother to dispute.

`_ingested_at` fixes it because a late-arriving fact is *recently ingested*
however old the event is.

**Why the window is 21 and not 15.** This is the interesting part, because 15
was the original value and it was wrong.

The reasoning that produced 15 was: the load generator's maximum refund delay
is 14 days, so 15 leaves a day of headroom. That is wrong, and the mistake is
subtle:

> `date_diff('day', ...)` counts **calendar boundaries crossed**, not elapsed
> 24-hour periods. A payment at 23:53 refunded 14 days and one hour later
> crosses **16** day-boundaries.

A build against real generated data produced a measured lag of 16 days, and
`between_fct_payments_refund_lag_days` caught it. The window is now derived
rather than guessed:

```
 14   business maximum refund delay
+ 2   calendar-boundary counting at the edges of a day
+ 5   operational slack: sink backlog, a missed DAG run over a weekend, clock skew
= 21
```

**Cost of being wrong in each direction.** Too large: a few extra days
reprocessed per run — cheap, bounded, and visible in the run time. Too small:
silent, permanent wrongness on exactly the rows that matter most. These are not
symmetric, so the window is generous.

`assert_no_late_arrival_outside_lookback.sql` fails the build if reality ever
exceeds it again. `make prove-lookback` demonstrates a 12-day-late refund
landing through an *incremental* build (`results/lookback_proof.json`).

---

## 3. SCD Type 2 on `dim_customer`

**Decision.** Customers are snapshotted with dbt's `timestamp` strategy;
`fct_orders` resolves `customer_key` **as of** `placed_at_utc`.

**Rejected.** A Type 1 dimension holding current state.

**Why.** A customer in Egypt moves to Germany. Their `country_code` changes in
place — Postgres keeps no history. Without a snapshot, every order they ever
placed is now attributed to Germany, and last year's revenue-by-country report
silently changes. Run the same report twice, six months apart, and the numbers
differ with no code change and no bug to point at.

That is not hypothetical here: the load generator relocates ~3% of customers a
year specifically so this model has work to do and the tests can prove it does
it.

**What it costs.** A dimension you can no longer join on `customer_id` alone —
doing so multiplies every fact by the customer's version count. The mitigation
is that `fct_orders` resolves the as-of key once, so consumers join on
`customer_key` and get the historically correct version without needing to know
any of this.

**Timestamp strategy, not check.** `check` diffs a column list on every run: it
costs a full-table comparison and cannot see a change that was reverted between
two runs. `timestamp` relies on `updated_at` moving on *every* mutation — an
assumption about the application code, and a load-bearing one. A route that
forgets to touch `updated_at` produces a version that is never recorded,
silently. `test_patch_moves_updated_at` in the commerce API holds up that end
of the contract.

**A bug this shipped with.** `dbt_valid_from` on version 1 is when the snapshot
*first ran*, not when the customer came into existence. Every order predating
the first snapshot fell outside all versions and got a NULL `customer_key` —
4,550 of 20,247 orders, 22% of revenue, silently dropped from every dimensional
aggregate while remaining in the fact table. Row counts still reconciled.
`dim_customer` now back-dates version 1 to the customer's `created_at`, and
`assert_every_order_resolves_a_customer_version` fails if it ever regresses.

---

## 4. Money is an integer number of cents

**Decision.** Every monetary column is `integer` cents. No floats, no
`numeric`, anywhere in the pipeline.

**Rejected.** `numeric(12,2)`, or floating point.

**Why not float.** `0.1 + 0.2 != 0.3`. Sum a hundred thousand float amounts and
the result depends on the order they were summed in — which in a parallel query
engine is not deterministic. Two runs of the same aggregate can disagree in the
last cent, and there is no way to say which is right.

**Why not `numeric`.** It is *correct*, and it is the right answer in a
warehouse that handles multiple currencies with different minor units. It costs
more storage, arithmetic is slower, and — the practical reason here — it
crosses the CDC boundary badly: Debezium's default `decimal.handling.mode`
encodes it as base64 `BigDecimal` bytes that every consumer must decode using
the scale from the schema. `decimal.handling.mode: string` avoids that, at
which point every model is casting strings to decimals.

Integers cross every boundary — Postgres, Debezium, JSON, Parquet, DuckDB,
Snowflake — without a single conversion decision.

**What it costs.** No sub-cent values, so a business that prices per-1000-impressions
at $0.0003 could not use this schema. And a currency with three minor units
(Bahraini dinar, Tunisian dinar) needs a per-currency scale factor, which this
schema does not carry. Both are real limitations and neither applies to this
business. The schema would need a `minor_unit_scale` column on a currency
dimension to lift the second.

---

## 5. Deterministic file names, and no UUID in the path

**Decision.** Raw Parquet is laid out as:

```
{table}/_ingested_date={YYYY-MM-DD}/part-{kafka_partition:03d}-{offset_block:09d}.parquet
where offset_block = kafka_offset // 100_000
```

**Rejected.** The spec's `{table}-{ts}-{uuid}.parquet`.

**Why.** The spec also requires that "re-running any partition produces
byte-identical Parquet". A UUID in the name makes that impossible by
construction: a replay *adds a second file* rather than replacing the first, so
the partition silently doubles.

Naming from the batch's own min/max offset is not enough either, and the reason
is subtle: two replays covering **overlapping but different** ranges (0–599 and
300–899) produce two different filenames, both land, and offsets 300–599 now
exist twice. Only an exact-range replay would have been safe.

A layout derived independently of any batch means a given offset always maps to
exactly one file, so replay of any subset, superset or overlap converges on the
same bytes.

**The other three conditions for byte-identity**, all easy to miss:

1. **Deterministic row order.** Rows are sorted by `_kafka_offset` before
   writing. Kafka guarantees order *within* a partition, not across a
   multi-partition poll, so arrival order varies run to run.
2. **`_ingested_at` is the Kafka broker timestamp, not `now()`.** This is the
   one people miss. With wall-clock time, every replay writes different bytes
   and the guarantee is impossible.
3. **Fixed writer settings.** `write_statistics=False` is not an optimisation:
   min/max stats are stable but the *page layout* carrying them shifts with
   dictionary state. `store_schema=False` suppresses a serialised-schema blob
   whose contents vary across pyarrow patch releases.

`make prove-idempotency` writes `results/idempotency_proof.json`, covering
same-order replay, shuffled-arrival replay, and overlapping replay.

---

## 6. zstd, not snappy

**Decision.** `compression=zstd, level=3`.

**Why.** Measured on this data: **3.09x** versus uncompressed, against roughly
1.9x for snappy. These files are written once and read on every dbt run,
several times a day, for as long as the history is retained — the read side
dominates by orders of magnitude, and zstd's decode speed is close enough to
snappy's that DuckDB does not notice.

Level 3 rather than the default 1, or a higher level: 3 is where the
ratio/CPU curve bends on this data. Level 9 bought another 4% for 3x the write
CPU, which the flush budget does not have.

---

## 7. A hand-written sink, not Kafka Connect

**Decision.** The sink is a Python consumer.

**Why.** Every interesting decision in a CDC sink is a *policy* decision that a
Connect config hides behind a property name:

* **when to commit** — after the object-store write, never before. This single
  ordering is what makes crash recovery lossless. Committing first loses data
  on every crash.
* **what a delete means** — written as a row with `_op='d'` carrying the
  before-image. Nothing is ever removed from raw. Deciding what a delete
  *means* is the warehouse's job.
* **what to do about a schema change** — see below.
* **what "idempotent" means** — see §5.

`enable.auto.commit: false` is not a tuning knob here; it is what makes the
write-then-commit ordering expressible at all.

---

## 8. The schema guard rejects rather than coerces, and halts one table

**Decision.** Four classes, four responses:

| change | response |
|---|---|
| additive (new nullable column) | accept, log INFO, audit |
| widening (int32→int64, f32→f64) | accept, log WARN, audit |
| column dropped | accept, backfill null, WARN, alert |
| narrowing or type change | **reject the batch**, DLQ, alert, **halt that table only** |

**Why an allowlist of safe widenings, not a denylist of unsafe ones.** The cost
of wrongly allowing a change is silent data loss; the cost of wrongly rejecting
one is a five-minute human decision. Those are not comparable, so anything not
explicitly known to be lossless is incompatible.

**Why halt one table and not the sink.** Halting everything because `refunds`
changed shape stops `orders` ingesting too, turning a one-table problem into a
platform outage. `test_incompatible_change_halts_only_the_offending_table`
covers exactly this.

**Why a dropped column is retained as null.** Physically removing it means
files in the same `_ingested_date=` prefix have different shapes, and every
engine reading that prefix has to guess. The null costs a few bytes under zstd
and keeps the partition rectangular.

**Why the offsets are not committed on a halt.** The backlog stays in Kafka, so
once a human decides what the change means, the data is still there to replay.
Committing would discard it.

**Why a file, not Confluent Schema Registry.** The registry solves a
producer-coordination problem this pipeline does not have — there is exactly
one producer. What is actually needed is a durable record of "what did this
table look like last time", which is a file. It is mounted on a volume because
losing it makes the guard treat every table as newly-seen, which silently
accepts a change it would otherwise have rejected.

---

## 9. Staging does not join, even though the spec's example does

**Decision.** `stg_orders` normalises the legacy status vocabulary and casts
types. It does **not** resolve `placed_at_local` against the customer's
timezone. That happens in `int_orders__resolved`.

**Why.** The spec's layer contract says staging does "no joins, no business
logic", and its own example `stg_orders.sql` then joins to customers for the
timezone. Both cannot hold. The contract wins, because it is what keeps every
staging model a pure function of exactly one source table — debugging one never
requires understanding another.

**A circularity in the intermediate layer worth knowing about.** Converting to
USD needs the order's month; the month comes from `placed_at_utc`;
`placed_at_utc` needs the timezone. So the FX join must happen *after* timezone
resolution, not alongside it — which is why `int_orders__resolved` is two
sequential CTEs rather than one wide join. Getting that order wrong puts the
15% of orders with no `placed_at` into the wrong FX month.

---

## 10. FX rates are monthly closes, applied as of the order month

**Decision.** `seed_fx_rates` holds one rate per currency per month; conversion
uses the rate for the month the order was **placed in**.

**Why not today's rate.** Last quarter's revenue would change every morning.
Finance notices, and trusts you less afterwards.

**Why monthly, not daily.** Finance restates non-USD revenue at the month's
closing rate. A daily rate would imply a precision the company does not
actually use, and would make two people reconciling the same month disagree by
rounding.

**Why there is an explicit USD→USD row at 1.0.** So the join never
special-cases the reporting currency, and a missing rate is unambiguously an
error rather than an implicit identity.
`assert_every_non_usd_order_has_an_fx_rate` can then be a strict check rather
than a judgement call — which matters, because `sum()` skips nulls silently and
~20% of the book is non-USD.

---

## 11. Deleted customers keep their orders

**Decision.** A soft-deleted customer is retained in `stg_customers` and
`dim_customer`, their historical orders stay in `fct_orders`, and they are
excluded from active-customer counts.

**Why.** Deleting a customer is a **privacy** action, not a **financial** one.
Revenue that was recognised stays recognised. Removing their orders would
restate historical revenue every time somebody exercises a right to erasure,
and the restatement would be invisible.

This is why `stg_customers` is the one staging model that does not end with
`where _op != 'd'` — the exception the layer contract asks to be justified
explicitly.

**What a stricter regime would require.** Under a genuine erasure obligation
you cannot keep the email and name. The right shape is *pseudonymisation*:
retain the row and the surrogate key, null the direct identifiers, keep the
aggregates. That is a schema change to `dim_customer` plus a scheduled job, and
it is not implemented here.

---

## 12. `append` is not safe with a lookback

**Decision.** Every incremental mart uses `delete+insert` on a `unique_key`.

**Rejected.** `incremental_strategy='append'` on `fct_subscription_events`,
which looks obviously correct because the source is genuinely append-only.

**What happened.** It duplicated the entire table on the second run. `append`
performs **no** deduplication — `unique_key` is ignored entirely — while the
lookback filter deliberately *re-selects* a window of already-loaded rows. And
because the bulk export gives every historical row the same `_ingested_at`, the
first incremental run re-selected all of history and doubled it.

**The rule.** `append` is only safe when the incremental filter can never
re-select a row it has already loaded. A lookback window is precisely a filter
that re-selects rows on purpose. The two are mutually exclusive, and the
failure is silent — no error, just a table with twice the revenue.

---

## 13. Two MRR reconciliations, two tolerances

**Decision.** `assert_mrr_reconciles_to_subscription_state` at **0.5%**, and
`assert_mrr_reconciles_to_payments` at **2.5%**.

**Why not one test at 0.5%,** which is what the spec asks for: it cannot hold
against billing, for an accounting reason rather than a modelling one.

> A customer billed on 3 November who cancels on the 13th is not refunded. The
> business **billed** a full month and keeps it. The MRR run-rate **stops** on
> the 13th.

So billed and accrued MRR diverge by the unearned remainder of every mid-cycle
cancellation — a structural 1–2% that no amount of correct modelling removes.

There were three possible responses:

1. Widen the tolerance until it passes and say nothing. **The worst option**:
   the test still *looks* like a 0.5%-grade check, and the number that would
   indicate a real problem is now inside the bound.
2. Quietly compare something easier until 0.5% is reachable. Worse still — the
   test passes while measuring the wrong thing.
3. Split it: reconcile the thing that *can* be exact at 0.5%, and keep the
   end-to-end check at a tolerance that reflects the real difference.

This is (3). A tolerance is a claim about how much difference is legitimate;
inflating one to silence a test converts a measurement into a decoration.

The tight test compares two genuinely independent derivations of the same
run-rate — replaying the event log versus reading the source's current-state
columns. They share no logic, which is what makes agreement meaningful.

**A bug it caught.** MRR was under-reported by **87%**, because `fct_mrr_daily`
filtered on `revenue_state = 'earning'` and thereby excluded every subscription
whose only event was `created` — which is most of them (2,000 intervals against
159). No schema test could have found it: every key was unique, every foreign
key resolved, nothing was null. The number was simply wrong.

---

## 14. Where the numbers are computed

**Decision.** The metrics API computes nothing. Every figure it returns is
already a column in a mart.

**Why.** A metric computed in the API is a metric the dbt tests do not cover,
the lineage graph does not show, and that disagrees with the warehouse the
first time somebody changes one and not the other. The API's job is
translation — SQL to HTTP — plus auth, caching and pagination.

The one exception is a custom rolling window, which is computed over the
**already gap-filled** series from `fct_revenue_rolling`. Computing it from raw
daily revenue would reproduce the exact bug that model exists to avoid.

---

## 15. Things deliberately not done

**Kubernetes.** Docker Compose is correct at this scope. K8s adds a week and
demonstrates nothing that is not already demonstrated.

**A cloud warehouse by default.** DuckDB locally. The dbt project targets
Snowflake via `--target prod` and the SQL is written to compile on both (which
is why `days_in_month` and `raw_source` are macros), but the default path needs
no account.

**Multi-region, HA, autoscaling.** One Postgres, one Kafka broker, one sink.
Out of scope, and stated as a choice rather than left as an omission.

**Real payment processing.** Simulated status transitions only.

**A front-end.** The metrics API is the deliverable; the Streamlit page is an
operations dashboard, not a BI tool.

---

## 16. What I would do differently at 100x

At 500M orders and 50M customers, the following stop working — roughly in the
order they break.

**DuckDB → Snowflake or BigQuery, immediately.** A single-file embedded engine
on one machine is the first hard wall. The dbt project already targets
Snowflake; what changes is `dim_customer` needing clustering on
`(customer_id, valid_from)` and the facts needing partitioning on `order_date`.

**The sink becomes the bottleneck before the warehouse does.** One Python
consumer at 50k records/batch tops out around 40–60k records/second. At 100x
that is not enough. In order of what I would try:

1. Partition the source topics by table and run one consumer group per table —
   the schema guard already isolates tables, so this is a deployment change
   rather than a redesign.
2. Move the Parquet write off the consumer thread into a bounded writer pool.
3. Only then consider Flink or Spark Structured Streaming — and accept losing
   the explicit control over commit ordering that §7 depends on.

**`delete+insert` becomes too expensive.** Rewriting a 21-day window of a
500M-row fact on every run is hours of work to update a few thousand rows. The
answer is real partitioning: `insert_overwrite` on date partitions, so only
touched partitions are rewritten. That requires the warehouse to support
partition-level replacement, which is the concrete reason to leave DuckDB.

**The lookback becomes the wrong mechanism.** A 21-day reprocessing window over
500M rows is enormous compared to the handful of rows that actually changed.
The right shape is a **change-log-driven** rebuild: the sink emits the set of
`payment_id`s touched in each batch, and the incremental model reprocesses
exactly that set. The lookback is a proxy for "what changed" that is correct
and cheap at this scale and correct and ruinous at 100x.

**Sorting the whole batch in memory stops fitting.** `_dedupe_by_offset` and
the pre-write sort are O(batch) in memory. At larger batch sizes this needs an
external merge, or smaller batches and more files — which then needs compaction,
which is a whole subsystem this project does not have.

**One Postgres, one replication slot, one publication.** At 100x the write
volume the slot's WAL retention becomes a genuine operational risk, and
`max_slot_wal_keep_size` starts actually firing. The heartbeat mitigates the
idle case but not the volume case. That points at multiple publications by
table group, and eventually at sharding the source.

**Secrets.** `infra/debezium/secrets.properties` is committed with development
credentials. At any real scale that file is rendered at deploy time from a
secret manager and never exists in git. The `${file:...}` indirection is
already in place precisely so that swap is a deployment change, not a code
change.

**Testing against a fixture stops being enough.** The CI fixture is 0.4% scale.
It keeps all six messiness patterns, which is what makes it useful, but it
cannot catch a query plan that degrades at volume. That needs a scheduled
large-scale run against a production-sized clone — a nightly job, not a PR gate.
