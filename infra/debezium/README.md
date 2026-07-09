# Debezium connector configuration

Every non-obvious setting in `connector.json`, and why it is what it is.

### `"snapshot.mode": "no_data"`

**The most consequential line in the file.** Debezium's default is `initial`,
which streams every existing row through Kafka before it starts capturing
changes. For 5M orders that is slow, it holds a replication slot open for the
duration, and it teaches nothing that streaming the changes does not.

Instead: `services/cdc-sink/bulk_export.py` exports the history straight from
Postgres to Parquet inside one `REPEATABLE READ` transaction, records the LSN
of that snapshot, and CDC starts from exactly that LSN.

The sequencing is not optional:

```
1. BEGIN REPEATABLE READ; capture pg_current_wal_lsn()
2. export every table from that one snapshot
3. create the replication slot AT that LSN
4. start the connector
```

Creating the slot *before* the snapshot double-counts every row changed in
between — dedup absorbs it, but the row-count reconciliation lies until the
overlap clears. Creating it *after* loses every change made during the export,
permanently and silently. `scripts/setup_cdc.sh` does this in the right order.

### `"publication.autocreate.mode": "disabled"`

Debezium's default (`all_tables`) creates a publication covering every table in
the database, including `idempotency_keys` — service plumbing that would then
be streamed into the warehouse as if it were a business fact. The publication
is created explicitly in `setup_cdc.sh` with exactly the seven tables we want.

### `"tombstones.on.delete": "false"`

A tombstone is a null-valued message Kafka's log compaction uses to erase a
key. We do not compact, and the sink writes deletes as rows with `_op='d'`
rather than removing anything. Tombstones would be pure noise the sink has to
skip on every delete.

### `"decimal.handling.mode": "string"`

The default (`precise`) encodes decimals as base64 `java.math.BigDecimal`
bytes, which every consumer then has to decode with the scale from the schema.
`double` silently loses precision on money. `string` is exact and readable —
and the sink casts it once, in one place.

There are no `numeric` columns in this schema today (money is `integer` cents,
deliberately). This is set so that the day somebody adds one, it does not
arrive as base64.

### `"time.precision.mode": "connect"`

Debezium's default emits microseconds since epoch as `int64`. `connect` uses
Kafka Connect's logical types, which pyarrow maps to real timestamps without a
hand-written conversion — and a hand-written epoch conversion is where the
timezone bugs live.

### `"heartbeat.interval.ms": "10000"` and `heartbeat.action.query`

**This prevents an outage that is genuinely hard to diagnose.**

A replication slot only advances when the connector confirms an LSN. If the
tables in `table.include.list` are quiet while *other* tables in the database
are busy, the slot never advances, and Postgres retains every WAL segment since
the slot's position. The disk fills. The database stops.

The heartbeat writes to a dummy table on an interval, which produces a change
the connector must acknowledge, which advances the slot. Ten seconds is cheap
insurance against an unbounded disk.

The heartbeat table is deliberately excluded from `table.include.list` — it is
plumbing, not data.

### `"errors.tolerance": "none"`

Fail on a bad record rather than skipping it. A skipped CDC record is a row the
warehouse will never learn about, and nothing downstream can detect its absence.
Stopping is recoverable; silent loss is not.

### Credentials via `${file:...}`

The connector config is committed. The credentials are not: Connect resolves
`${file:/etc/kafka-connect/secrets.properties:db_user}` at runtime from a file
mounted by compose. Posting a connector config with an inline password writes
that password into Connect's own config topic, in plaintext, forever.
