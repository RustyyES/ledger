"""Kafka -> Parquet sink.

Written as a plain consumer rather than configured through Kafka Connect. The
spec is explicit about wanting to own this code, and the reason is that every
interesting decision in a CDC sink -- when to commit, what to do with a delete,
how to react to a schema change, what "idempotent" actually means -- is a
policy decision that a Connect config file hides behind a property name.

Delivery semantics
------------------
At-least-once from Kafka, made effectively-exactly-once by the storage layer.
The order is: consume -> batch -> WRITE to object store -> THEN commit offsets.
If the process dies between the write and the commit, the next run re-consumes
that range and rewrites the same file with the same bytes (see storage.py). If
it dies before the write, nothing was committed and nothing was lost.

Committing before the write would be the other way round, and would lose data
on every crash. This ordering is the single most important line in the file.

Deletes
-------
A Debezium delete arrives with `after: null`. It is written as a ROW with
`_op='d'`, carrying the `before` image so the warehouse can see what was
deleted. Nothing is ever physically removed from the raw layer -- raw is an
append-only log of what the source said, and deciding what a delete *means* is
the warehouse's job, not the sink's.
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

import pyarrow as pa
import structlog
from config import SinkSettings, get_settings
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from schema_guard import SchemaGuard, SchemaRegistry, conform
from storage import LocalStore, PartitionWriter, S3Store

log = structlog.get_logger("sink")

RECORDS = Counter("sink_records_total", "CDC records processed", ["table", "op"])
BATCHES = Counter("sink_batches_total", "Batches flushed", ["table", "outcome"])
FLUSH_SECONDS = Histogram(
    "sink_flush_duration_seconds",
    "Time to serialise and write a batch",
    ["table"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)
CONSUMER_LAG = Gauge(
    "sink_consumer_lag_records",
    "High-watermark minus committed offset",
    ["topic", "partition"],
)
DLQ_RECORDS = Counter("sink_dlq_records_total", "Records diverted to the DLQ", ["table", "reason"])
HALTED_TABLES = Gauge("sink_halted_tables", "Tables whose consumer is halted by the schema guard")
LAST_FLUSH = Gauge(
    "sink_last_flush_timestamp_seconds", "Unix time of the last successful flush", ["table"]
)


class Consumer(Protocol):
    """The slice of confluent_kafka.Consumer this sink actually uses.

    Narrowed to a Protocol so the batching, guard and write paths can be tested
    without a broker. Testing a sink only through a live Kafka is how sinks end
    up with untested failure paths.
    """

    def poll(self, timeout: float) -> Any: ...
    def commit(self, asynchronous: bool = ...) -> Any: ...
    def assignment(self) -> list: ...
    def get_watermark_offsets(
        self, partition, timeout: float = ..., cached: bool = ...
    ) -> tuple[int, int]: ...
    def committed(self, partitions: list, timeout: float = ...) -> list: ...
    def close(self) -> None: ...


# --------------------------------------------------------------------------- #
# Debezium envelope
# --------------------------------------------------------------------------- #


@dataclass
class CdcRecord:
    table: str
    op: str
    payload: dict[str, Any]
    lsn: int | None
    kafka_offset: int
    kafka_partition: int
    ingested_at: datetime
    source_ts: datetime | None


def parse_message(
    raw_value: bytes, topic: str, partition: int, offset: int, broker_ts_ms: int
) -> CdcRecord | None:
    """Decode one Debezium change event.

    Returns None for a tombstone (null value), which Debezium emits after a
    delete when `tombstones.on.delete` is true. We configure it false, but a
    sink that crashes on an unexpected tombstone is a sink that pages you at
    03:00 the first time someone flips that setting.
    """
    if not raw_value:
        return None

    envelope = json.loads(raw_value)
    # Debezium wraps in {"schema": ..., "payload": ...} unless schemas are
    # disabled. Tolerate both, because the converter setting is exactly the kind
    # of thing that gets changed without telling the sink owner.
    body = envelope.get("payload", envelope)

    op = body.get("op")
    if op is None:
        return None

    source = body.get("source") or {}
    table = source.get("table") or topic.rsplit(".", 1)[-1]

    # `after` for c/u/r, `before` for d. A delete carries the last known state,
    # which is the only thing that makes the delete useful downstream.
    row = body.get("after") if op != "d" else body.get("before")
    if row is None:
        row = body.get("before") or {}

    source_ts_ms = source.get("ts_ms") or body.get("ts_ms")
    return CdcRecord(
        table=table,
        op=op,
        payload=dict(row),
        lsn=source.get("lsn"),
        kafka_offset=offset,
        kafka_partition=partition,
        # Broker timestamp, NOT wall clock. See storage.py condition 2 -- this
        # is what makes replay byte-identical.
        ingested_at=datetime.fromtimestamp(broker_ts_ms / 1000, tz=UTC),
        source_ts=(datetime.fromtimestamp(source_ts_ms / 1000, tz=UTC) if source_ts_ms else None),
    )


def records_to_table(records: list[CdcRecord]) -> pa.Table:
    """Build one Arrow table from a homogeneous list of records.

    Column order is sorted so that two batches carrying the same columns in a
    different order produce the same schema -- otherwise the schema guard sees
    a spurious change on every rebalance.
    """
    if not records:
        return pa.table({})

    columns: set[str] = set()
    for r in records:
        columns.update(r.payload)
    ordered = sorted(columns)

    data: dict[str, list[Any]] = {c: [] for c in ordered}
    for r in records:
        for c in ordered:
            data[c].append(r.payload.get(c))

    data["_op"] = [r.op for r in records]
    data["_lsn"] = [r.lsn for r in records]
    data["_kafka_offset"] = [r.kafka_offset for r in records]
    data["_kafka_partition"] = [r.kafka_partition for r in records]
    data["_ingested_at"] = [r.ingested_at for r in records]
    data["_source_ts"] = [r.source_ts for r in records]
    return pa.table(data)


# --------------------------------------------------------------------------- #
# Batching
# --------------------------------------------------------------------------- #


@dataclass
class TableBatch:
    table: str
    records: list[CdcRecord] = field(default_factory=list)
    opened_at: float = field(default_factory=time.monotonic)

    def add(self, record: CdcRecord) -> None:
        self.records.append(record)

    def should_flush(self, max_records: int, max_seconds: float) -> bool:
        if not self.records:
            return False
        return (
            len(self.records) >= max_records or (time.monotonic() - self.opened_at) >= max_seconds
        )

    def reset(self) -> None:
        self.records = []
        self.opened_at = time.monotonic()


# --------------------------------------------------------------------------- #
# The sink
# --------------------------------------------------------------------------- #


class CdcSink:
    def __init__(
        self,
        settings: SinkSettings,
        *,
        consumer: Consumer | None = None,
        writer: PartitionWriter | None = None,
        guard: SchemaGuard | None = None,
        registry_path: str = "/var/lib/ledger/schema_registry.json",
    ) -> None:
        self.settings = settings
        self.consumer = consumer
        self.writer = writer or PartitionWriter(
            _build_store(settings),
            compression=settings.compression,
            compression_level=settings.compression_level,
            row_group_size=settings.row_group_size,
        )
        self.guard = guard or SchemaGuard(SchemaRegistry(registry_path))
        self.batches: dict[str, TableBatch] = {}
        self.schema_audit: list[dict[str, Any]] = []
        self._stop = False
        self._last_lag_poll = 0.0

    # -- lifecycle ---------------------------------------------------------- #

    def request_stop(self, *_: Any) -> None:
        log.info("shutdown_requested")
        self._stop = True

    def run(self) -> int:
        if self.consumer is None:
            self.consumer = _build_consumer(self.settings)

        log.info(
            "sink_started",
            topics=self.settings.topics,
            batch_max_records=self.settings.batch_max_records,
            batch_max_seconds=self.settings.batch_max_seconds,
        )
        try:
            while not self._stop:
                msg = self.consumer.poll(self.settings.poll_timeout_s)
                if msg is not None:
                    self._handle_message(msg)
                self._flush_due()
                self._poll_lag()
        finally:
            # Flush whatever is buffered before dying. Without this, a clean
            # SIGTERM discards up to 60 seconds of records -- which the offsets
            # were never committed for, so they would be re-consumed, but only
            # after an operator noticed the gap.
            log.info(
                "draining_before_exit",
                buffered={t: len(b.records) for t, b in self.batches.items()},
            )
            self._flush_all()
            if self.consumer is not None:
                self.consumer.close()
        return 0

    # -- message handling --------------------------------------------------- #

    def _handle_message(self, msg: Any) -> None:
        if msg.error() is not None:
            log.warning("kafka_message_error", error=str(msg.error()))
            return

        topic = msg.topic()
        table = topic.rsplit(".", 1)[-1]

        if self.guard.is_halted(table):
            # Deliberately do NOT commit. The offsets stay put so that once an
            # operator resolves the schema problem and calls `resume`, the
            # backlog is still there to be replayed.
            return

        try:
            record = parse_message(
                msg.value(),
                topic,
                msg.partition(),
                msg.offset(),
                msg.timestamp()[1] if msg.timestamp() else int(time.time() * 1000),
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            self._to_dlq(table, msg, reason="unparseable", detail=str(exc))
            return

        if record is None:
            return

        RECORDS.labels(table, record.op).inc()
        self.batches.setdefault(table, TableBatch(table)).add(record)

        if len(self.batches[table].records) >= self.settings.batch_max_records:
            self._flush_table(table)

    # -- flushing ----------------------------------------------------------- #

    def _flush_due(self) -> None:
        for table in list(self.batches):
            if self.batches[table].should_flush(
                self.settings.batch_max_records, self.settings.batch_max_seconds
            ):
                self._flush_table(table)

    def _flush_all(self) -> None:
        for table in list(self.batches):
            if self.batches[table].records:
                self._flush_table(table)

    def _flush_table(self, table: str) -> None:
        batch = self.batches.get(table)
        if batch is None or not batch.records:
            return

        arrow = records_to_table(batch.records)
        verdict = self.guard.inspect(table, arrow.schema)

        for change in verdict.changes:
            self.schema_audit.append(change.to_row())

        if not verdict.accepted:
            self._batch_to_dlq(
                table, batch, reason="incompatible_schema", detail=verdict.reason or ""
            )
            BATCHES.labels(table, "rejected").inc()
            HALTED_TABLES.set(len(self.guard.halted))
            batch.reset()
            # Offsets are NOT committed. See _handle_message.
            return

        assert verdict.reconciled_schema is not None
        conformed = conform(arrow, verdict.reconciled_schema)

        try:
            with FLUSH_SECONDS.labels(table).time():
                results = self.writer.write_batch(table, conformed)
        except Exception as exc:
            log.exception("flush_failed", table=table, error=str(exc))
            BATCHES.labels(table, "error").inc()
            # Do not reset and do not commit -- retry on the next tick with the
            # records still buffered. If this keeps failing the batch grows and
            # consumer lag climbs, which is exactly the signal an operator needs.
            return

        # ---- ONLY NOW is it safe to commit. -------------------------------- #
        self._commit()

        BATCHES.labels(table, "ok").inc()
        LAST_FLUSH.labels(table).set(time.time())
        log.info(
            "batch_flushed",
            table=table,
            records=len(batch.records),
            files=len(results),
            rows=sum(r.rows for r in results),
            bytes=sum(r.bytes_written for r in results),
            deduplicated=sum(r.deduplicated for r in results),
        )
        self._write_schema_audit()
        batch.reset()

    def _commit(self) -> None:
        if self.consumer is None:
            return
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as exc:  # pragma: no cover - broker-dependent
            log.error("commit_failed", error=str(exc))

    # -- DLQ ---------------------------------------------------------------- #

    def _to_dlq(self, table: str, msg: Any, *, reason: str, detail: str) -> None:
        DLQ_RECORDS.labels(table, reason).inc()
        key = (
            f"{self.settings.dlq_prefix}/{table}/"
            f"{datetime.now(UTC).date().isoformat()}/"
            f"{msg.partition():03d}-{msg.offset():015d}.json"
        )
        self.writer.store.write(
            key,
            json.dumps(
                {
                    "table": table,
                    "reason": reason,
                    "detail": detail,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "raw": (msg.value() or b"").decode("utf-8", errors="replace"),
                    "diverted_at": datetime.now(UTC).isoformat(),
                },
                indent=2,
            ).encode(),
        )
        log.error("record_to_dlq", table=table, reason=reason, key=key, detail=detail)

    def _batch_to_dlq(self, table: str, batch: TableBatch, *, reason: str, detail: str) -> None:
        DLQ_RECORDS.labels(table, reason).inc(len(batch.records))
        first, last = batch.records[0], batch.records[-1]
        key = (
            f"{self.settings.dlq_prefix}/{table}/"
            f"{datetime.now(UTC).date().isoformat()}/"
            f"batch-{first.kafka_partition:03d}-{first.kafka_offset:015d}"
            f"-{last.kafka_offset:015d}.json"
        )
        self.writer.store.write(
            key,
            json.dumps(
                {
                    "table": table,
                    "reason": reason,
                    "detail": detail,
                    "records": len(batch.records),
                    "offset_range": [first.kafka_offset, last.kafka_offset],
                    "sample": [r.payload for r in batch.records[:5]],
                    "diverted_at": datetime.now(UTC).isoformat(),
                },
                indent=2,
                default=str,
            ).encode(),
        )
        log.error(
            "batch_to_dlq",
            table=table,
            reason=reason,
            key=key,
            records=len(batch.records),
            detail=detail,
            action="this table's consumer is halted; other tables continue",
        )

    def _write_schema_audit(self) -> None:
        if not self.schema_audit:
            return
        key = (
            f"{self.settings.schema_audit_table}/"
            f"_ingested_date={datetime.now(UTC).date().isoformat()}/"
            f"changes-{int(time.time())}.json"
        )
        self.writer.store.write(key, json.dumps(self.schema_audit, indent=2, default=str).encode())
        self.schema_audit = []

    # -- observability ------------------------------------------------------ #

    def _poll_lag(self) -> None:
        """Expose consumer lag as a Prometheus gauge.

        Polled on an interval rather than per message: `get_watermark_offsets`
        is a broker round-trip, and calling it per record turns a metric into a
        bottleneck.
        """
        now = time.monotonic()
        if now - self._last_lag_poll < self.settings.lag_poll_seconds:
            return
        self._last_lag_poll = now
        if self.consumer is None:
            return
        try:
            assignment = self.consumer.assignment()
            if not assignment:
                return
            committed = self.consumer.committed(assignment, timeout=5.0)
            for tp in committed:
                _low, high = self.consumer.get_watermark_offsets(tp, timeout=5.0, cached=False)
                position = tp.offset if tp.offset and tp.offset >= 0 else 0
                CONSUMER_LAG.labels(tp.topic, str(tp.partition)).set(max(0, high - position))
        except Exception as exc:  # pragma: no cover - broker-dependent
            log.warning("lag_poll_failed", error=str(exc))


# --------------------------------------------------------------------------- #
# wiring
# --------------------------------------------------------------------------- #


def _build_store(settings: SinkSettings):
    if settings.local_path:
        return LocalStore(settings.local_path)
    return S3Store(
        settings.s3_bucket,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        region=settings.s3_region,
    )


def _build_consumer(settings: SinkSettings) -> Consumer:
    from confluent_kafka import Consumer as KafkaConsumer

    consumer = KafkaConsumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.consumer_group,
            "auto.offset.reset": settings.auto_offset_reset,
            # Manual commit is not optional here -- it is what makes the
            # write-then-commit ordering possible.
            "enable.auto.commit": False,
            "max.poll.interval.ms": 900_000,  # a 50k-record flush can be slow
            "session.timeout.ms": 45_000,
            "fetch.min.bytes": 1024,
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe(settings.topics)
    return consumer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Debezium -> Parquet CDC sink.")
    parser.add_argument(
        "--local-path", default=None, help="Write to a local directory instead of S3."
    )
    parser.add_argument("--registry", default="/var/lib/ledger/schema_registry.json")
    parser.add_argument("--metrics-port", type=int, default=None)
    args = parser.parse_args(argv)

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.stdlib.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )

    settings = get_settings()
    if args.local_path:
        settings = settings.model_copy(update={"local_path": args.local_path})

    port = args.metrics_port or settings.metrics_port
    try:
        start_http_server(port)
        log.info("metrics_server_started", port=port)
    except OSError as exc:
        log.warning("metrics_port_unavailable", port=port, error=str(exc))

    sink = CdcSink(settings, registry_path=args.registry)
    signal.signal(signal.SIGTERM, sink.request_stop)
    signal.signal(signal.SIGINT, sink.request_stop)
    return sink.run()


if __name__ == "__main__":
    raise SystemExit(main())
