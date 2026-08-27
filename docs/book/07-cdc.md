# Chapter 7 — Change data capture from first principles

## The question

*How do you find out what changed in a database?*

Four answers exist. Three are bad. Understanding why is worth more than
memorising which tool to use.

## Approach 1: copy everything, every time

```sql
SELECT * FROM orders;
```

**Works?** Yes.
**Scales?** No. Five million rows every hour, competing with your checkout page.
**Detects deletes?** Yes, by comparing to what you had before — but you must
hold the entire previous copy to do the comparison.

Fine for a 10,000-row lookup table. Ruinous for anything that grows.

## Approach 2: a high-water mark

```sql
SELECT * FROM orders WHERE updated_at > :last_run;
```

This is what most people write, and for a lot of systems it's genuinely fine.

**Three ways it breaks:**

**Deletes are invisible.** You cannot `SELECT` a row that isn't there. A deleted
row stays in your warehouse forever and nothing will ever tell you.

**It depends on the app never forgetting.** Every write path must update
`updated_at`. Miss one — a bulk `UPDATE` in a migration, an admin tool, a
`ON CONFLICT DO UPDATE` that doesn't touch it — and those rows are invisible
forever. You are relying on a convention held by a team that doesn't know you
exist.

**Timestamp ties at the boundary.** Two rows written in the same millisecond,
your job reads one and the clock ticks. Use `>` and you miss the second one
permanently. Use `>=` and you reprocess forever.

**You only see the final state.** A row updated three times between runs gives
you one row. If you needed the intermediate states — and MRR does — they're gone.

## Approach 3: triggers

Put a trigger on each table that writes to an audit table.

**Works?** Yes, and it catches deletes.
**Cost:** every write now does two writes. You have added latency to the
checkout page to serve the analytics team. The app team will, correctly, say no.

## Approach 4: read the database's own log

Here's the insight.

Postgres already writes down every change. It has to — it's how it recovers from
a crash. Before any row is modified, the change goes into the **write-ahead log**
(WAL): *"in transaction 4471, table `orders`, row with id X changed from A to
B."*

That log is a perfect, complete, ordered record of every change ever made. It
already exists. It costs nothing extra to produce.

**Change Data Capture** is: read that log.

```
     app writes                Postgres writes             we read
     a row          ───────►   it to the WAL    ───────►   the WAL
                               (it does this
                                anyway)
```

**What this gives you:**

- Every change, including deletes
- Every *intermediate* state, not just the final one
- Exact ordering, guaranteed by the database
- **Zero additional load on the application** — reading a log file is not
  competing with your checkout page
- No dependency on the app maintaining any convention

That last point is the big one. CDC works even if the app team never updates
`updated_at`, never emits an event, and doesn't know you exist.

## The moving parts

Four things, and the names are less scary than they sound:

```
Postgres  ──►  Debezium  ──►  Kafka  ──►  our sink  ──►  Parquet files
   │              │             │            │
   │              │             │            └─ our code (Ch. 8)
   │              │             └─ a durable queue
   │              └─ translates WAL into JSON
   └─ produces the WAL
```

### Postgres: logical replication

The raw WAL is a binary format about *disk pages*, not rows. Useless directly.
So you turn on **logical replication**:

```
wal_level = logical
```

Postgres now additionally emits a row-level stream: "row X in table Y changed
from A to B". Two more concepts:

- A **publication** — which tables to stream. We list our seven explicitly.
- A **replication slot** — a bookmark. Postgres retains WAL until the consumer
  confirms it has read past that point.

> **A slot is a loaded gun.** If the consumer stops and the slot isn't dropped,
> Postgres keeps every WAL segment since that position — *forever*. The disk
> fills and the database stops. This is one of the most common serious
> production incidents in CDC. We defend against it twice: `max_slot_wal_keep_size = 4GB`
> caps the damage, and a heartbeat keeps the slot moving even when our tables
> are idle. More on the heartbeat below.

### Debezium: WAL → JSON

Debezium connects as a replication client and turns each change into a JSON
message:

```json
{
  "op": "u",
  "before": {"id": "abc", "status": "pending"},
  "after":  {"id": "abc", "status": "completed"},
  "source": {"table": "orders", "lsn": 123456789, "ts_ms": 1741000000000}
}
```

`op` is `c` (create), `u` (update), `d` (delete), or `r` (read — a snapshot row).

`before` is only populated because of `REPLICA IDENTITY FULL` from Chapter 4.
Without it, `before` on an update is just the primary key.

### Kafka: a durable buffer

Kafka is a log you can replay. Messages go in, get retained, and consumers track
their own position.

**Why not have Debezium write files directly?** Because then Debezium's failure
is your failure, its retry policy is your retry policy, and you cannot replay.
With Kafka in the middle, our sink can crash, restart, and re-read from its last
committed position. Chapter 8 depends entirely on this.

We use **Redpanda**, which speaks Kafka's protocol but is one binary with no
ZooKeeper. Same API, a tenth of the operational weight, which is the right trade
for a single-node local stack.

## The connector settings that matter

From [`infra/debezium/connector.json`](../../infra/debezium/connector.json).
Every one of these has a reason.

### `"snapshot.mode": "no_data"`

The most consequential line in the file.

Debezium's default (`initial`) streams every existing row through Kafka before
capturing changes. For 5M orders that's slow, holds the slot open for the
duration, and teaches nothing.

Instead: export history straight from Postgres to Parquet, and start CDC from
the exact moment that export was taken. This is the **bulk-load-then-stream**
pattern, and getting the handoff right is genuinely subtle.

> **The one correct ordering:**
> ```
> 1. BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ
> 2. capture pg_current_wal_lsn()          ← inside the transaction
> 3. export every table from that snapshot
> 4. create the replication slot AT that LSN
> 5. start the connector
> ```
>
> **Slot before snapshot** → every row changed in between is counted twice.
> Deduplication absorbs it, but your row-count reconciliation lies until the
> overlap clears.
>
> **Slot after export** → every change made *during* the export is lost.
> Permanently, silently, undetectably — nothing downstream can notice the
> absence of a row it never saw.

`REPEATABLE READ` matters for the same reason at smaller scale: under `READ
COMMITTED` each table gets its own snapshot, so an order exported before its
payment leaves a foreign key that can never resolve.

This is all encoded in [`scripts/setup_cdc.sh`](../../scripts/setup_cdc.sh) with
the reasoning in comments, because it's the sharpest edge in the whole pipeline.

### `"heartbeat.interval.ms": "10000"`

Prevents the outage described above.

The slot only advances when the connector confirms an LSN. If *our* seven tables
are quiet but the rest of the database is busy, the slot never advances and WAL
accumulates. The heartbeat writes to a dummy table on a timer, producing a change
the connector must acknowledge, which advances the slot.

The heartbeat table is deliberately excluded from the publication — it's
plumbing, not data.

### `"publication.autocreate.mode": "disabled"`

Debezium's default creates a publication over *every* table — including
`idempotency_keys`, which is service plumbing that would arrive in the warehouse
looking like a business fact. We create the publication ourselves with exactly
seven tables.

### `"tombstones.on.delete": "false"`

A tombstone is an extra null-valued message Kafka's log compaction uses to erase
a key. We don't compact, and our sink keeps deletes as rows. Tombstones would be
noise the sink skips on every delete.

### `"decimal.handling.mode": "string"`

Debezium's default encodes decimals as base64 `BigDecimal` bytes. `double`
silently loses precision on money. `string` is exact and readable.

There are no `numeric` columns today (money is integer cents — Chapter 4). This
is set so that the day someone adds one, it doesn't arrive as base64.

### `"errors.tolerance": "none"`

Fail on a bad record rather than skipping it. A skipped CDC record is a row the
warehouse will never learn about, and nothing downstream can detect its absence.
Stopping is recoverable; silent loss is not.

## What breaks without CDC

Go back to the high-water mark and here's what you lose:

- **Deletes.** Gone. Undetectably.
- **Intermediate states.** A subscription that went `created → upgraded →
  cancelled` between runs looks like it went straight to `cancelled`. MRR is
  wrong and there's no way to know.
- **Independence from the app team.** You now need every write path to maintain
  a convention.
- **Ordering guarantees.** You get the state at read time, not the sequence of
  changes.

## Try it

```bash
make verify-cdc
```

Inserts a customer through the API and polls object storage until it appears —
typically under 60 seconds. That's the whole chapter in one command: a row
written by an app that has never heard of a warehouse, arriving in one, without
the app doing anything.

---

Next: **[Chapter 8 — The sink, and what "exactly once" really means](08-the-sink.md)**
