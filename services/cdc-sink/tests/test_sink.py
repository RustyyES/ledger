"""Sink behaviour, including the crash-recovery guarantee.

A fake consumer stands in for Kafka. That is deliberate: the failure paths that
matter (crash between write and commit, incompatible schema mid-stream,
rebalance redelivery) are all but impossible to trigger reliably against a live
broker, so testing only through one leaves them untested.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest
from config import SinkSettings
from schema_guard import SchemaGuard, SchemaRegistry
from sink import CdcSink, parse_message, records_to_table
from storage import LocalStore, PartitionWriter

BROKER_TS = int(datetime(2025, 3, 1, 10, 0, tzinfo=UTC).timestamp() * 1000)


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #


class FakeMessage:
    def __init__(self, topic, partition, offset, value, ts=BROKER_TS, error=None):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._value, self._ts, self._error = value, ts, error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def timestamp(self):
        return (1, self._ts)

    def error(self):
        return self._error


class FakeConsumer:
    """Replays a scripted list of messages, and records commits.

    `crash_after` raises once the given number of messages have been polled,
    simulating a hard kill in the middle of a batch.
    """

    def __init__(self, messages, *, crash_after: int | None = None):
        self.messages = list(messages)
        self.index = 0
        self.commits: list[int] = []
        self.crash_after = crash_after
        self.closed = False
        #: Set by `build_sink`. The real sink runs forever by design, so the
        #: fake has to be the thing that decides the stream has ended.
        self.on_exhausted = lambda: None

    def poll(self, timeout: float):
        if self.crash_after is not None and self.index >= self.crash_after:
            raise KeyboardInterrupt("simulated hard kill")
        if self.index >= len(self.messages):
            self.on_exhausted()
            return None
        msg = self.messages[self.index]
        self.index += 1
        return msg

    def commit(self, asynchronous: bool = False):
        self.commits.append(self.index)

    def assignment(self):
        return []

    def committed(self, partitions, timeout=5.0):
        return []

    def get_watermark_offsets(self, partition, timeout=5.0, cached=False):
        return (0, 0)

    def close(self):
        self.closed = True


def debezium_event(op: str, row: dict, *, table: str = "orders", lsn: int = 1000) -> bytes:
    body = {
        "op": op,
        "before": row if op == "d" else None,
        "after": None if op == "d" else row,
        "source": {"table": table, "lsn": lsn, "ts_ms": BROKER_TS - 250, "db": "ledger"},
        "ts_ms": BROKER_TS,
    }
    return json.dumps({"payload": body}).encode()


def order_row(i: int) -> dict:
    return {
        "id": f"order-{i:06d}",
        "customer_id": f"cust-{i % 50:04d}",
        "amount_cents": 1000 + i,
        "currency": "USD",
        "status": "completed",
    }


@pytest.fixture
def settings(tmp_path) -> SinkSettings:
    return SinkSettings(
        local_path=str(tmp_path / "raw"),
        batch_max_records=100,
        batch_max_seconds=3600,  # time-based flush disabled unless asked for
        tables=["orders"],
    )


def build_sink(settings, messages, tmp_path, *, crash_after=None) -> tuple[CdcSink, FakeConsumer]:
    consumer = FakeConsumer(messages, crash_after=crash_after)
    sink = CdcSink(
        settings,
        consumer=consumer,
        writer=PartitionWriter(LocalStore(settings.local_path)),
        guard=SchemaGuard(SchemaRegistry(tmp_path / "registry.json")),
    )
    consumer.on_exhausted = sink.request_stop
    return sink, consumer


# --------------------------------------------------------------------------- #
# envelope parsing
# --------------------------------------------------------------------------- #


def test_parses_a_create_event():
    rec = parse_message(debezium_event("c", order_row(1)), "ledger.public.orders", 0, 7, BROKER_TS)
    assert rec.op == "c"
    assert rec.table == "orders"
    assert rec.kafka_offset == 7
    assert rec.payload["id"] == "order-000001"
    assert rec.ingested_at == datetime(2025, 3, 1, 10, 0, tzinfo=UTC)


def test_delete_carries_the_before_image():
    """A delete with no before-image is useless to the warehouse."""
    rec = parse_message(debezium_event("d", order_row(2)), "ledger.public.orders", 0, 1, BROKER_TS)
    assert rec.op == "d"
    assert rec.payload["id"] == "order-000002", "delete lost its before image"


def test_tombstone_is_ignored_not_fatal():
    assert parse_message(b"", "ledger.public.orders", 0, 1, BROKER_TS) is None
    assert parse_message(None, "ledger.public.orders", 0, 1, BROKER_TS) is None


def test_envelope_without_schema_wrapper_is_accepted():
    """Tolerating both converter settings is cheap; crashing on one is not."""
    body = json.dumps(
        {
            "op": "c",
            "after": order_row(3),
            "source": {"table": "orders", "lsn": 5, "ts_ms": BROKER_TS},
        }
    ).encode()
    rec = parse_message(body, "ledger.public.orders", 0, 3, BROKER_TS)
    assert rec is not None and rec.payload["id"] == "order-000003"


def test_ingested_at_is_the_broker_timestamp_not_wall_clock():
    """The whole replay guarantee rests on this."""
    a = parse_message(debezium_event("c", order_row(1)), "ledger.public.orders", 0, 1, BROKER_TS)
    b = parse_message(debezium_event("c", order_row(1)), "ledger.public.orders", 0, 1, BROKER_TS)
    assert a.ingested_at == b.ingested_at


def test_records_to_table_sorts_columns_deterministically():
    """Otherwise a rebalance looks like a schema change on every batch."""
    r1 = parse_message(
        json.dumps(
            {"op": "c", "after": {"b": 1, "a": 2}, "source": {"table": "t", "ts_ms": BROKER_TS}}
        ).encode(),
        "ledger.public.t",
        0,
        1,
        BROKER_TS,
    )
    r2 = parse_message(
        json.dumps(
            {"op": "c", "after": {"a": 3, "b": 4}, "source": {"table": "t", "ts_ms": BROKER_TS}}
        ).encode(),
        "ledger.public.t",
        0,
        2,
        BROKER_TS,
    )
    assert records_to_table([r1]).schema.names == records_to_table([r2]).schema.names


# --------------------------------------------------------------------------- #
# batching and commit ordering
# --------------------------------------------------------------------------- #


def test_batch_flushes_at_the_record_threshold(settings, tmp_path):
    msgs = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(250)
    ]
    sink, consumer = build_sink(settings, msgs, tmp_path)
    sink.run()

    table = sink.writer.read_partition("orders", date(2025, 3, 1))
    assert table.num_rows == 250
    assert consumer.commits, "offsets were never committed"


def test_commit_happens_only_after_a_successful_write(settings, tmp_path, monkeypatch):
    """The single most important ordering in the sink."""
    msgs = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(100)
    ]
    sink, consumer = build_sink(settings, msgs, tmp_path)

    def boom(*_a, **_k):
        raise OSError("object store unavailable")

    monkeypatch.setattr(sink.writer, "write_batch", boom)
    sink.run()

    assert consumer.commits == [], "committed offsets for a batch that was never written"


def test_records_survive_a_failed_flush_and_are_retried(settings, tmp_path, monkeypatch):
    msgs = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(100)
    ]
    sink, consumer = build_sink(settings, msgs, tmp_path)

    calls = {"n": 0}
    real = sink.writer.write_batch

    def flaky(table, batch):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("transient")
        return real(table, batch)

    monkeypatch.setattr(sink.writer, "write_batch", flaky)
    sink.run()

    # The drain on exit retries, so nothing is lost.
    assert sink.writer.read_partition("orders", date(2025, 3, 1)).num_rows == 100


# --------------------------------------------------------------------------- #
# crash recovery -- the Stage 2 exit criterion
# --------------------------------------------------------------------------- #


def test_hard_kill_mid_batch_then_restart_loses_nothing_and_duplicates_nothing(settings, tmp_path):
    """Kill the sink mid-batch, restart, verify no duplicates and no loss.

    This models SIGKILL, not SIGTERM: the process vanishes, so `run()`'s
    `finally` drain never happens and whatever was buffered is gone. The loop
    is therefore driven by hand and the object simply abandoned -- calling
    `run()` here would exercise the graceful path and quietly test the wrong
    thing (see `test_graceful_shutdown_drains_the_buffer`).

    Recovery rests on two properties working together: offsets are committed
    only after a successful write, and the storage layer dedupes on
    (partition, offset). Neither alone is sufficient.
    """
    msgs = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(300)
    ]

    # --- run 1: hard kill after 150 messages, mid-batch ---
    sink1, _ = build_sink(settings, msgs, tmp_path)
    for msg in msgs[:150]:
        sink1._handle_message(msg)
        sink1._flush_due()
    # Process dies here. No drain, no commit for the buffered 50.
    del sink1

    reader = PartitionWriter(LocalStore(settings.local_path))
    after_crash = reader.read_partition("orders", date(2025, 3, 1))
    assert (
        after_crash.num_rows == 100
    ), "expected only the completed 100-record batch to have been durably written"

    # --- run 2: restart. Offsets were never committed past 100, so a real
    # consumer re-reads from there; re-reading from 0 is the harsher case. ---
    sink2, _ = build_sink(settings, msgs, tmp_path)
    sink2.run()

    final = sink2.writer.read_partition("orders", date(2025, 3, 1))
    offsets = final.column("_kafka_offset").to_pylist()

    assert len(offsets) == 300, f"lost records: got {len(offsets)}"
    assert len(set(offsets)) == 300, "duplicate records after replay"
    assert sorted(offsets) == list(range(300))


def test_graceful_shutdown_drains_the_buffer(settings, tmp_path):
    """SIGTERM must not discard up to a full batch of already-consumed records."""
    msgs = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(150)
    ]
    sink, _ = build_sink(settings, msgs, tmp_path)
    sink.run()

    table = sink.writer.read_partition("orders", date(2025, 3, 1))
    assert table.num_rows == 150, "the partial batch was dropped on shutdown"


def test_replay_after_crash_is_byte_identical(settings, tmp_path):
    msgs = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(200)
    ]
    sink1, _ = build_sink(settings, msgs, tmp_path)
    sink1.run()
    before = sink1.writer.partition_checksums("orders", date(2025, 3, 1))

    sink2, _ = build_sink(settings, msgs, tmp_path)
    sink2.run()
    after = sink2.writer.partition_checksums("orders", date(2025, 3, 1))

    assert after == before


# --------------------------------------------------------------------------- #
# schema guard integration
# --------------------------------------------------------------------------- #


def test_additive_column_flows_through_without_stopping_ingestion(settings, tmp_path):
    """Migration 0003 arriving live."""
    old = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(100)
    ]
    new = [
        FakeMessage(
            "ledger.public.orders",
            0,
            100 + i,
            debezium_event("c", order_row(100 + i) | {"channel": "mobile"}),
        )
        for i in range(100)
    ]
    sink, _ = build_sink(settings, old + new, tmp_path)
    sink.run()

    table = sink.writer.read_partition("orders", date(2025, 3, 1))
    assert table.num_rows == 200
    assert "channel" in table.column_names
    channels = table.column("channel").to_pylist()
    assert channels.count(None) == 100, "historical rows should be null for the new column"
    assert channels.count("mobile") == 100


def test_incompatible_change_halts_one_table_and_dlqs_the_batch(tmp_path):
    settings = SinkSettings(
        local_path=str(tmp_path / "raw"),
        batch_max_records=50,
        batch_max_seconds=3600,
        tables=["orders", "payments"],
    )
    good_orders = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(50)
    ]
    baseline_payments = [
        FakeMessage(
            "ledger.public.payments",
            0,
            i,
            debezium_event("c", {"id": f"p{i}", "amount_cents": i}, table="payments"),
        )
        for i in range(50)
    ]
    # amount_cents flips int -> string. Narrowing/incompatible.
    broken_payments = [
        FakeMessage(
            "ledger.public.payments",
            0,
            50 + i,
            debezium_event(
                "c", {"id": f"p{50+i}", "amount_cents": "not-a-number"}, table="payments"
            ),
        )
        for i in range(50)
    ]
    more_orders = [
        FakeMessage("ledger.public.orders", 0, 50 + i, debezium_event("c", order_row(50 + i)))
        for i in range(50)
    ]

    sink, _ = build_sink(
        settings, baseline_payments + good_orders + broken_payments + more_orders, tmp_path
    )
    sink.run()

    assert sink.guard.is_halted("payments")
    assert not sink.guard.is_halted("orders"), "an unrelated table was halted"

    # orders kept ingesting straight through the payments incident.
    orders_table = sink.writer.read_partition("orders", date(2025, 3, 1))
    assert orders_table.num_rows == 100

    # payments kept its good batch and DLQ'd the bad one.
    payments_table = sink.writer.read_partition("payments", date(2025, 3, 1))
    assert payments_table.num_rows == 50

    dlq = list((tmp_path / "raw" / "_dlq" / "payments").rglob("*.json"))
    assert dlq, "rejected batch was not written to the DLQ"
    body = json.loads(dlq[0].read_text())
    assert body["reason"] == "incompatible_schema"
    assert body["records"] == 50


def test_unparseable_message_goes_to_dlq_without_stopping_the_stream(settings, tmp_path):
    msgs = [
        FakeMessage("ledger.public.orders", 0, 0, b"{not json at all"),
        *[
            FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
            for i in range(1, 101)
        ],
    ]
    sink, _ = build_sink(settings, msgs, tmp_path)
    sink.run()

    assert sink.writer.read_partition("orders", date(2025, 3, 1)).num_rows == 100
    dlq = list((tmp_path / "raw" / "_dlq" / "orders").rglob("*.json"))
    assert dlq and json.loads(dlq[0].read_text())["reason"] == "unparseable"


def test_deletes_are_retained_as_rows(settings, tmp_path):
    msgs = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(50)
    ] + [
        FakeMessage("ledger.public.orders", 0, 50 + i, debezium_event("d", order_row(i)))
        for i in range(50)
    ]
    sink, _ = build_sink(settings, msgs, tmp_path)
    sink.run()

    table = sink.writer.read_partition("orders", date(2025, 3, 1))
    ops = table.column("_op").to_pylist()
    assert ops.count("d") == 50
    assert table.num_rows == 100, "deletes removed rows instead of appending them"


def test_metadata_columns_are_present_on_every_row(settings, tmp_path):
    msgs = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(100)
    ]
    sink, _ = build_sink(settings, msgs, tmp_path)
    sink.run()
    table = sink.writer.read_partition("orders", date(2025, 3, 1))
    for col in ("_op", "_lsn", "_kafka_offset", "_kafka_partition", "_ingested_at", "_source_ts"):
        assert col in table.column_names, f"missing metadata column {col}"
        assert table.column(col).null_count < table.num_rows


def test_batch_flushes_on_the_time_threshold(tmp_path):
    settings = SinkSettings(
        local_path=str(tmp_path / "raw"),
        batch_max_records=10_000,
        batch_max_seconds=0.0001,
        tables=["orders"],
    )
    msgs = [
        FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
        for i in range(20)
    ]
    sink, _ = build_sink(settings, msgs, tmp_path)
    sink.run()
    assert sink.writer.read_partition("orders", date(2025, 3, 1)).num_rows == 20


def test_kafka_error_message_is_logged_not_fatal(settings, tmp_path):
    msgs = [
        FakeMessage("ledger.public.orders", 0, 0, None, error="broker transport failure"),
        *[
            FakeMessage("ledger.public.orders", 0, i, debezium_event("c", order_row(i)))
            for i in range(1, 101)
        ],
    ]
    sink, _ = build_sink(settings, msgs, tmp_path)
    assert sink.run() == 0
    assert sink.writer.read_partition("orders", date(2025, 3, 1)).num_rows == 100
