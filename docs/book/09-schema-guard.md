# Chapter 9 — Surviving a schema change

> Source: [`services/cdc-sink/schema_guard.py`](../../services/cdc-sink/schema_guard.py)

## The failure this prevents

A developer changes a column type. `amount_cents` goes from an integer to a
string — maybe they needed to store `"1200.50"`, maybe an ORM migration did it
sideways.

Your sink receives the new shape. What happens?

**Naive answer: whatever pyarrow infers.** Today's batch has strings, so today's
Parquet file has a string column. Yesterday's file has an integer column. Both
sit in the same folder.

Then the warehouse reads the folder. It sees two different types for the same
column, picks one, and **silently nulls whatever won't cast**.

No error. No warning. Every aggregate on that column is now quietly wrong. You
find out months later when someone reconciles by hand.

> **Silent coercion is the single most common way a warehouse starts lying.**

## The policy

Four classes of change, four responses:

| Change | Example | Response |
|---|---|---|
| **Additive** | new nullable column | accept · log INFO · audit |
| **Widening** | `int32` → `int64` | accept · log WARN · audit |
| **Dropped** | column disappears | accept · backfill null · WARN · alert |
| **Incompatible** | `int64` → `int32`, `int` → `string` | **reject** · DLQ · alert · **halt that table** |

Every one of these choices has a reason.

## Why an allowlist, not a denylist

```python
SAFE_WIDENINGS = {
    ("int8", "int16"), ("int8", "int32"), ("int32", "int64"),
    ("float", "double"), ("string", "large_string"),
    ...
}
```

Anything not explicitly listed as lossless is **incompatible**.

The reasoning is an asymmetry:

- **Wrongly allowing** a change → silent data loss, discovered months later,
  possibly unrecoverable.
- **Wrongly rejecting** a change → a five-minute human decision.

Those costs aren't remotely comparable, so the default is "refuse".

Write it as a denylist and every type combination nobody thought of sails
through. Which, given the number of type combinations, is most of them.

## Why halt one table and not the sink

```python
self.halted.add(table)     # not: self.stop()
```

If `refunds` changes shape incompatibly, `refunds` stops. `orders`, `payments`
and `customers` keep flowing.

**Why:** halting everything converts a one-table problem into a platform outage.
Someone gets paged at 3am for something that could have waited until Tuesday.

There's a test that specifically guards this:

```python
def test_incompatible_change_halts_only_the_offending_table(guard):
    ...
    assert guard.is_halted("payments")
    assert not guard.is_halted("orders"), "an unrelated table was halted"
```

## Why the offsets are NOT committed on a halt

```python
if self.guard.is_halted(table):
    return          # no commit -- the backlog stays in Kafka
```

Subtle and important. When a table halts, we deliberately **do not** commit its
Kafka offsets.

**Why:** the data stays in Kafka. Once a human decides what the schema change
means, the backlog is still there to replay. Commit, and you've thrown away
every record that arrived during the incident.

The cost is that consumer lag grows while halted — which is *correct*, because
growing lag is exactly the signal that says "something needs a human".

## Why a dropped column is kept as null

```python
else:
    fields.append(pa.field(f.name, f.type, nullable=True))   # dropped → keep
```

The column vanishes from the source. We keep it in the schema, filled with
nulls.

**Why:** physically removing it means files in the same `_ingested_date=` folder
have *different shapes*. Every engine reading that folder then has to guess how
to reconcile them, and different engines guess differently.

The null costs a few bytes under zstd and keeps the partition rectangular.
That's a very good trade.

## Why a file, not a Schema Registry

Confluent Schema Registry is the standard answer. We used a JSON file.

**Reasoning:** the Registry solves a *producer coordination* problem — many
producers writing the same topic, needing agreement. We have exactly one
producer. What we actually need is a durable record of "what did this table look
like last time", which is a file.

Adding a service means another thing to run, back up, monitor and reason about.

**But** — the file is mounted on a Docker volume, deliberately:

```yaml
volumes:
  - sink-state:/var/lib/ledger
```

Lose that file and the guard treats every table as newly-seen, which means it
**silently accepts a change it would otherwise have rejected**. The registry
being durable is load-bearing.

It's also written atomically:

```python
tmp.write_text(...)
tmp.replace(self.path)     # atomic rename
```

A torn registry file is unrecoverable — you can't tell which half is current.

## What happens to a rejected batch

Three things:

1. **Written to a dead-letter queue** as JSON, including a sample of the records
   and the exact offset range. You can inspect exactly what arrived.
2. **The table halts**, so no more batches are attempted.
3. **An alert fires** (`sink_halted_tables > 0` in Prometheus).

And critically — the last-known-good schema is **not** overwritten:

```python
def test_rejected_schema_is_not_written_to_the_registry(guard):
    ...
    assert guard.registry.get("payments").field("v").type.equals(pa.int64())
```

If a rejected batch poisoned the registry, the *next* batch would compare
against the bad shape and be accepted. The guard would have permanently
defeated itself.

## Seeing it work

Migration 0003 adds `orders.channel`. It is deliberately **not** applied by
`make up` — you apply it by hand, against the running stack:

```bash
make schema-change
```

Then watch:

```bash
make logs SERVICE=cdc-sink | grep schema_
```

You'll see the guard classify it as ADDITIVE, accept it, and write an audit row.

There's a wrinkle worth knowing about. The commerce API's `Order` model does
**not** declare `channel`. The running application doesn't know the column
exists.

That's deliberate, and it's the more demanding test: Debezium starts emitting
`channel` on every change event regardless of whether the app knows about it,
because the column is in the *table*. This is exactly what a real migration
looks like — the DDL lands ahead of the deploy that uses it.

## The bug this caused downstream

The staging model referenced `orders.channel`. Before the migration, no Parquet
file has that column, and referencing a nonexistent column is a **binder error**:

```
Binder Error: Referenced column "channel" not found in FROM clause!
```

`try_cast` doesn't help — the failure is name resolution, not casting.

The fix reads the Parquet schema at *compile* time:

```jinja
{% macro raw_column(table_name, column_name, fallback='null') %}
    {%- if column_name in raw_columns(table_name) -%}
        {{ adapter.quote(column_name) }}
    {%- else -%}
        {{ fallback }}
    {%- endif -%}
{% endmacro %}
```

So the model builds **before, during and after** the migration with no operator
action. The "during" case is the one that actually breaks things — when the
folder contains files both with and without the column.

**The alternative we rejected:** gate the model behind a variable someone flips
on migration day. Works right up until somebody forgets, and then it fails at
2am on a Saturday.

## What breaks without the guard

- An incompatible change silently corrupts a column, and every number built on
  it, indefinitely.
- One bad table takes down ingestion for all seven.
- A dropped column makes the partition non-rectangular and every reader guesses.
- Nobody has any record of when the schema changed, so the investigation starts
  from zero.

## Try it

```bash
cd services/cdc-sink && pytest tests/test_schema_guard.py -v
```

Sixteen tests. `test_incompatible_change_halts_only_the_offending_table` is the
one that encodes the operational judgement rather than just the mechanics.

---

Part III done. Next: turning raw data into answers — the longest part.

Next: **[Chapter 10 — Staging, and why the layer contract matters](10-staging.md)**
