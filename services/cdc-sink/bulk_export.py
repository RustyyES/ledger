"""One-time bulk export: Postgres -> Parquet, bypassing Kafka entirely.

Why this exists
---------------
Debezium's initial snapshot is explicitly out of scope. Pushing 5M rows through
Kafka to get them into the warehouse is slow, it holds a replication slot open
for the duration, and it teaches nothing that streaming the *changes* does not
already teach. Bulk-load-then-stream is what a practitioner does.

The sequencing is the part that has to be right:

    1. Open a REPEATABLE READ transaction and capture its snapshot.
    2. Record `pg_current_wal_lsn()` inside that transaction.
    3. Export every table from that one consistent snapshot.
    4. Create the replication slot AT that LSN, and start Debezium there.

Doing (4) before (1) double-counts every row that changed in between. Doing it
after the export finishes *loses* every row that changed during the export. The
transaction snapshot is what makes the handoff exact, and it is the single
reason this file is more than a `COPY TO`.

Output shape
------------
Identical to what the CDC sink writes, so the warehouse reads one union across
both. Bulk rows are marked `_op='r'` (read/snapshot, Debezium's own convention)
and carry `_kafka_partition = -1` so they can never collide with a real Kafka
(partition, offset) pair in the dedup key.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg
import pyarrow as pa
import structlog
from storage import LocalStore, PartitionWriter, S3Store

log = structlog.get_logger("bulk_export")

#: Bulk rows live in their own partition space so their synthetic offsets can
#: never be mistaken for Kafka offsets.
BULK_PARTITION = -1

EXPORT_TABLES = [
    "plans",
    "customers",
    "subscriptions",
    "subscription_events",
    "orders",
    "payments",
    "refunds",
]

#: Chunk size for server-side cursor reads. Large enough that the round-trip
#: cost is amortised, small enough that one chunk fits comfortably in memory
#: even for the widest table.
FETCH_SIZE = 100_000


@dataclass
class ExportManifest:
    """Written next to the data. Read by `setup_cdc.sh` to place the slot."""

    lsn: str
    snapshot_at: str
    tables: dict[str, int]
    scale_note: str = ""

    def to_json(self) -> str:
        return json.dumps(
            {
                "lsn": self.lsn,
                "snapshot_at": self.snapshot_at,
                "tables": self.tables,
                "note": self.scale_note,
                "contract": (
                    "CDC must start from this LSN. Starting later loses every "
                    "change made after the snapshot; starting earlier "
                    "double-counts, which dedup absorbs but which makes the "
                    "row-count reconciliation lie until the overlap clears."
                ),
            },
            indent=2,
        )


def _arrow_type_for(pg_type: str) -> pa.DataType:
    """Map Postgres type names onto the Arrow types the sink would produce.

    Keeping these aligned matters: if the bulk export writes `amount_cents` as
    int64 and CDC writes int32, the schema guard sees a narrowing on the first
    streamed batch and halts the table.
    """
    mapping = {
        "uuid": pa.string(),
        "text": pa.string(),
        "varchar": pa.string(),
        "bpchar": pa.string(),
        "char": pa.string(),
        "int2": pa.int32(),
        "int4": pa.int32(),
        "int8": pa.int64(),
        "bool": pa.bool_(),
        "timestamptz": pa.timestamp("us", tz="UTC"),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "date": pa.date32(),
        "jsonb": pa.string(),
        "json": pa.string(),
        "numeric": pa.string(),  # decimals as strings, matching Debezium's
        # decimal.handling.mode=string
        "float4": pa.float32(),
        "float8": pa.float64(),
    }
    return mapping.get(pg_type, pa.string())


class BulkExporter:
    def __init__(self, writer: PartitionWriter) -> None:
        self.writer = writer

    def export(self, conn: psycopg.Connection, tables: list[str]) -> ExportManifest:
        # REPEATABLE READ so every table is read from ONE consistent point.
        # READ COMMITTED would give each table its own snapshot, and an order
        # exported before its payment would be a foreign key the warehouse
        # cannot resolve.
        conn.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        with conn.cursor() as cur:
            cur.execute("SELECT pg_current_wal_lsn()::text, now()")
            lsn, snapshot_at = cur.fetchone()

        log.info("snapshot_opened", lsn=lsn, at=str(snapshot_at))
        counts: dict[str, int] = {}
        try:
            for table in tables:
                counts[table] = self._export_table(conn, table, snapshot_at)
        finally:
            conn.rollback()  # read-only; release the snapshot promptly

        manifest = ExportManifest(
            lsn=lsn,
            snapshot_at=snapshot_at.isoformat(),
            tables=counts,
        )
        self.writer.store.write("_manifest/bulk_export.json", manifest.to_json().encode())
        log.info("export_complete", lsn=lsn, rows=sum(counts.values()), tables=counts)
        return manifest

    def _export_table(self, conn: psycopg.Connection, table: str, snapshot_at: datetime) -> int:
        columns = self._columns(conn, table)
        schema = pa.schema(
            [pa.field(name, _arrow_type_for(pg_type)) for name, pg_type in columns]
            + [
                pa.field("_op", pa.string()),
                pa.field("_lsn", pa.int64()),
                pa.field("_kafka_offset", pa.int64()),
                pa.field("_kafka_partition", pa.int32()),
                pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
                pa.field("_source_ts", pa.timestamp("us", tz="UTC")),
            ]
        )

        total = 0
        # Named cursor => server-side, so a 5M-row table is streamed rather
        # than materialised in the client. Without `name=` psycopg buffers the
        # entire result set and the export OOMs somewhere past a million rows.
        with conn.cursor(name=f"bulk_{table}") as cur:
            cur.itersize = FETCH_SIZE
            collist = ", ".join(f'"{c}"' for c, _ in columns)
            # Deterministic order so a re-export produces the same offsets and
            # therefore the same bytes.
            cur.execute(f"SELECT {collist} FROM {table} ORDER BY 1")

            chunk: list[tuple] = []
            for row in cur:
                chunk.append(row)
                if len(chunk) >= FETCH_SIZE:
                    total += self._write_chunk(table, columns, schema, chunk, total, snapshot_at)
                    chunk = []
            if chunk:
                total += self._write_chunk(table, columns, schema, chunk, total, snapshot_at)

        log.info("table_exported", table=table, rows=total)
        return total

    def _write_chunk(
        self,
        table: str,
        columns: list[tuple[str, str]],
        schema: pa.Schema,
        chunk: list[tuple],
        offset_base: int,
        snapshot_at: datetime,
    ) -> int:
        data: dict[str, list] = {name: [] for name, _ in columns}
        for row in chunk:
            for (name, _), value in zip(columns, row, strict=False):
                data[name].append(_coerce(value))

        n = len(chunk)
        data["_op"] = ["r"] * n  # Debezium's snapshot marker
        data["_lsn"] = [None] * n
        data["_kafka_offset"] = list(range(offset_base, offset_base + n))
        data["_kafka_partition"] = [BULK_PARTITION] * n
        # One fixed timestamp for the whole export, not now() per chunk: a
        # re-export must produce identical bytes, and it also means the entire
        # bulk load lands in exactly one `_ingested_date` partition.
        data["_ingested_at"] = [snapshot_at] * n
        data["_source_ts"] = [snapshot_at] * n

        arrays = [
            pa.array(data[f.name], type=f.type) if f.name in data else pa.nulls(n, type=f.type)
            for f in schema
        ]
        self.writer.write_batch(table, pa.Table.from_arrays(arrays, schema=schema))
        return n

    @staticmethod
    def _columns(conn: psycopg.Connection, table: str) -> list[tuple[str, str]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname, t.typname
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_type t ON t.oid = a.atttypid
                WHERE c.relname = %s AND n.nspname = 'public'
                  AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                (table,),
            )
            return list(cur.fetchall())


def _coerce(value):
    """Normalise Python values into what pyarrow expects."""
    import decimal
    import uuid as _uuid

    if isinstance(value, _uuid.UUID):
        return str(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, dict | list):
        return json.dumps(value, sort_keys=True)
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bulk export Postgres to Parquet.")
    parser.add_argument("--dsn", default=os.environ.get("COMMERCE_DATABASE_URL", ""))
    parser.add_argument("--local-path", default=None)
    parser.add_argument("--bucket", default=os.environ.get("SINK_S3_BUCKET", "ledger-raw"))
    parser.add_argument("--endpoint-url", default=os.environ.get("SINK_S3_ENDPOINT_URL"))
    parser.add_argument("--tables", nargs="*", default=EXPORT_TABLES)
    args = parser.parse_args(argv)

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.stdlib.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ]
    )

    if not args.dsn:
        print("error: --dsn or COMMERCE_DATABASE_URL required", file=sys.stderr)
        return 2

    store = (
        LocalStore(args.local_path)
        if args.local_path
        else S3Store(
            args.bucket,
            endpoint_url=args.endpoint_url,
            access_key=os.environ.get("SINK_S3_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("SINK_S3_SECRET_KEY", "minioadmin"),
        )
    )
    exporter = BulkExporter(PartitionWriter(store))

    dsn = args.dsn.replace("postgresql+psycopg://", "postgresql://")
    with psycopg.connect(dsn, autocommit=True) as conn:
        manifest = exporter.export(conn, args.tables)

    print(manifest.to_json())
    print(
        f"\nCDC must now start from LSN {manifest.lsn}.\n"
        f"`make setup-cdc` reads _manifest/bulk_export.json and does this for you.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
