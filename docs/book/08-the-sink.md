# Chapter 8 — The sink, and what "exactly once" really means

> Source: [`services/cdc-sink/sink.py`](../../services/cdc-sink/sink.py),
> [`storage.py`](../../services/cdc-sink/storage.py)

The sink reads messages from Kafka and writes Parquet files. That's a hundred
lines of obvious code and about eight hundred lines of "what happens when it
crashes".

## Why we wrote it instead of configuring Kafka Connect

Kafka Connect has an off-the-shelf S3 sink. One config file, done.

We wrote our own because every interesting decision in a CDC sink is a **policy
decision** that a config file hides behind a property name:

- *When do you commit your position?* (This one is everything.)
- *What does a delete mean?*
- *What do you do when the schema changes?*
- *What does "idempotent" actually mean here?*

If you can't answer those, you don't understand your pipeline — you understand a
config file. So we own the code.

## Delivery guarantees, honestly

You'll see "at-least-once", "at-most-once" and "exactly-once" everywhere. Here's
what they mean in practice.

**At-most-once** — record your position *before* doing the work. Crash in
between, the record is skipped. **You lose data.** Never acceptable for money.

**At-least-once** — do the work *first*, record your position after. Crash in
between, you redo the work on restart. **You may duplicate.** Recoverable, if
the work is idempotent.

**Exactly-once** — usually a lie, or a very expensive distributed transaction.

Our approach: **at-least-once delivery, made effectively-exactly-once by making
the write idempotent.** Duplicates are allowed to happen, and are harmless.

## The most important ordering in the codebase

```python
# 1. write to object storage
results = self.writer.write_batch(table, conformed)

# 2. ONLY NOW commit the Kafka offsets
self._commit()
```

That order is the whole guarantee.

- Crash **between** them → offsets not committed → restart re-reads the same
  records → writes the same file → same bytes. Harmless.
- Crash **before** the write → nothing committed, nothing lost.

Reverse them and you lose data on every crash. It's two lines and it is the
difference between a pipeline you can trust and one you can't.

## Byte-identical replay

The claim: **re-running any partition produces byte-identical Parquet.**

Why bother? Because it converts "did the retry corrupt anything?" from a
question requiring investigation into a `sha256sum` comparison. And because a
system where replay is safe is a system you can operate confidently — you can
always just run it again.

Four things must be true, and three are easy to miss.

### 1. Deterministic row order

```python
batch = batch.sort_by([("_kafka_offset", "ascending")])
```

Kafka guarantees order *within a partition*, not across a multi-partition poll.
Arrival order varies between runs. Without the sort, the same records produce
different files.

### 2. `_ingested_at` is the broker timestamp, not `now()`

**This is the one people miss.** Every row gets metadata columns, including
`_ingested_at` — when this record entered the pipeline.

If that's `datetime.now()`, every replay writes different bytes and the guarantee
is impossible *by construction*.

So it's the **Kafka broker timestamp**, which is stored in the message and
identical on every replay:

```python
ingested_at=datetime.fromtimestamp(broker_ts_ms / 1000, tz=timezone.utc)
```

Still semantically correct — it *is* when the record entered the pipeline — and
now deterministic. This column becomes critically important in Chapter 13.

### 3. Deterministic filenames — and this took two attempts

The spec suggested `{table}-{timestamp}-{uuid}.parquet`.

**Attempt 1 — with a UUID.** Broken immediately. A replay *adds a second file*
rather than replacing the first, so the partition silently doubles.

**Attempt 2 — name from the batch's own min/max offset.** Better, and still
wrong. Two replays covering *overlapping but different* ranges — offsets 0–599
and 300–899 — produce two different filenames. Both land. Offsets 300–599 now
exist twice. Only an *exact-range* replay was safe.

**Attempt 3 — a layout derived independently of any batch:**

```
{table}/_ingested_date=2026-08-29/part-{kafka_partition:03d}-{offset_block:09d}.parquet
                                       where offset_block = offset // 100_000
```

Now a given offset *always* maps to exactly one file, whatever batch it arrives
in. Replay of any subset, superset or overlap converges on the same bytes.

The general lesson is worth more than the specific fix:

> "Idempotent" is not a property you can assert. It's a property you have to
> *test*, including the awkward cases — and overlapping replay is the awkward
> case that the obvious implementations fail.

### 4. Fixed writer settings

```python
pq.write_table(
    batch, sink,
    compression="zstd", compression_level=3,
    write_statistics=False,     # ← not an optimisation
    store_schema=False,         # ← also not an optimisation
    ...
)
```

`write_statistics=False`: min/max stats are stable, but the *page layout*
carrying them shifts with dictionary state — enough to break a byte comparison.

`store_schema=False`: suppresses a serialised-schema blob whose contents vary
across pyarrow patch releases.

## Deduplication

The key is `(kafka_partition, kafka_offset)`.

**Why not just offset?** Offsets are unique *per partition*, not globally.
Partition 0 offset 5 and partition 1 offset 5 are different records. Dedupe on
offset alone and you silently drop real data.

**Why keep the LAST occurrence, not the first?**

```python
for i, pair in enumerate(zip(partitions, offsets)):
    last_index[pair] = i        # later writes win
```

On a consumer-group rebalance the same offset can be redelivered. The later
delivery is the one whose surrounding state we committed against.

## Deletes are rows, not deletions

```python
row = body.get("after") if op != "d" else body.get("before")
```

A delete is written as a **row** with `_op = 'd'`, carrying the before-image.
Nothing is ever removed from raw.

**Why:** the raw layer is a log of *what the source said*. Deciding what a delete
*means* is a business question — Chapter 4 decided that deleted customers keep
their orders — and business questions belong in the warehouse, not in the
transport layer.

If the sink deleted rows, that decision would be baked in irreversibly, and
you'd have to re-extract from the source to change your mind.

## Batching

```python
batch_max_records: 50_000
batch_max_seconds: 60
```

Flush at whichever comes first.

**Why both?** Records-only means a quiet table never flushes — its data sits in
memory indefinitely and is lost on restart. Time-only means a busy table
accumulates unbounded memory between flushes.

Together they bound both latency and memory.

## Crash recovery, tested two ways

The distinction matters and it's easy to test the wrong one.

**SIGTERM (graceful).** The process gets to run its `finally` block, which
flushes whatever is buffered. Without that drain, a clean shutdown discards up
to 60 seconds of records.

**SIGKILL (hard).** No `finally`. Buffered records vanish with the process.

The first version of the crash test used `KeyboardInterrupt` — which still runs
`finally`. It was accidentally testing the graceful path while claiming to test
the hard one. Now there are two tests:

```python
def test_hard_kill_mid_batch_then_restart_loses_nothing_and_duplicates_nothing():
    # drive the loop by hand and ABANDON the object -- no finally
def test_graceful_shutdown_drains_the_buffer():
    # run() with a stop signal -- finally runs
```

Recovery from a hard kill depends on **two properties working together**:
offsets committed only after a successful write, *and* dedup on
`(partition, offset)`. Neither alone is sufficient.

## Why zstd and not snappy

Measured on this data: **3.09x** compression versus uncompressed. Snappy manages
roughly 1.9x.

These files are written once and read on every dbt run, several times a day, for
as long as history is retained. The read side dominates by orders of magnitude,
and zstd's decode speed is close enough to snappy's that DuckDB doesn't notice.

Level 3 rather than 1 or 9: 3 is where the ratio/CPU curve bends on this data.
Level 9 bought another 4% for triple the write CPU, which the flush budget
doesn't have.

## What breaks if you remove things

| Remove | What breaks |
|---|---|
| Write-then-commit ordering | Data loss on every crash |
| `(partition, offset)` dedup | Duplicate rows after any restart |
| Broker timestamp for `_ingested_at` | Byte-identical replay becomes impossible |
| The sort before writing | Same, non-deterministically — worse, because it *sometimes* works |
| Deletes-as-rows | The warehouse can never distinguish soft from hard deletes |
| The time-based flush | Quiet tables never land |
| The graceful drain | Up to 60s of records lost on every deploy |

## Try it

```bash
cd services/cdc-sink && pytest -v          # 48 tests
make prove-idempotency                     # writes results/idempotency_proof.json
```

The proof covers same-order replay, **shuffled-arrival** replay and
**overlapping** replay. The middle one is the one that catches a missing sort;
the last is the one that catches a bad filename scheme.

---

Next: **[Chapter 9 — Surviving a schema change](09-schema-guard.md)**
