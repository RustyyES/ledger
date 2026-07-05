"""The byte-identical replay guarantee, and the ways it can be broken.

Every test here corresponds to one of the four conditions listed in
`storage.py`'s module docstring. They are written as *failing* scenarios first
(shuffled order, wall-clock timestamps) so a regression in the writer shows up
as a specific broken condition rather than a vague hash mismatch.
"""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest
from storage import LocalStore, PartitionWriter, _dedupe_by_offset

BASE_TS = datetime(2025, 3, 1, 9, 0, tzinfo=UTC)


def make_batch(n: int = 500, start_offset: int = 0, *, shuffle: bool = False) -> pa.Table:
    rows = []
    for i in range(n):
        off = start_offset + i
        rows.append(
            {
                "id": f"id-{off:08d}",
                "amount_cents": (off * 37) % 100_000,
                "status": ["pending", "completed", "refunded"][off % 3],
                "_op": ["c", "u", "d", "r"][off % 4],
                "_lsn": 100_000 + off,
                "_kafka_offset": off,
                "_kafka_partition": off % 3,
                # Broker timestamp, NOT now(). This is what makes replay stable.
                "_ingested_at": BASE_TS + timedelta(seconds=off),
                "_source_ts": BASE_TS + timedelta(seconds=off) - timedelta(milliseconds=250),
            }
        )
    if shuffle:
        random.Random(1234).shuffle(rows)
    return pa.Table.from_pylist(rows)


@pytest.fixture
def writer(tmp_path) -> PartitionWriter:
    return PartitionWriter(LocalStore(tmp_path / "raw"))


def test_write_produces_one_file_per_ingested_date(writer):
    results = writer.write_batch("orders", make_batch(500))
    assert len(results) >= 1
    assert all("_ingested_date=" in r.key for r in results)
    assert sum(r.rows for r in results) == 500


def test_replaying_a_dropped_partition_is_byte_identical(writer, tmp_path):
    """The headline guarantee: drop, replay, diff the bytes."""
    batch = make_batch(2_000)
    writer.write_batch("orders", batch)
    day = date(2025, 3, 1)
    before = writer.partition_checksums("orders", day)
    assert before, "nothing was written"

    writer.drop_partition("orders", day)
    assert writer.partition_checksums("orders", day) == {}

    writer.write_batch("orders", batch)
    after = writer.partition_checksums("orders", day)

    assert after == before, "replay produced different bytes"


def test_replay_is_stable_under_shuffled_arrival_order(writer):
    """Condition 1: rows must be sorted by offset before writing.

    A multi-partition Kafka poll returns rows in arrival order, which varies
    between runs. Without the sort this test fails and the guarantee is a lie.
    """
    ordered = make_batch(1_500, shuffle=False)
    shuffled = make_batch(1_500, shuffle=True)
    day = date(2025, 3, 1)

    writer.write_batch("orders", ordered)
    from_ordered = writer.partition_checksums("orders", day)

    writer.drop_partition("orders", day)
    writer.write_batch("orders", shuffled)
    from_shuffled = writer.partition_checksums("orders", day)

    assert from_shuffled == from_ordered


def test_files_are_laid_out_by_partition_and_offset_block(writer):
    """The fixed layout: a given offset always maps to exactly one file."""
    writer.write_batch("orders", make_batch(300))
    keys = writer.store.list(writer.partition_prefix("orders", date(2025, 3, 1)))
    assert all("/part-" in k and k.endswith(".parquet") for k in keys)
    # 3 kafka partitions, all offsets inside block 0.
    assert sorted(k.rsplit("/", 1)[1] for k in keys) == [
        "part-000-000000000.parquet",
        "part-001-000000000.parquet",
        "part-002-000000000.parquet",
    ]


def test_filename_is_deterministic_not_uuid_based(writer):
    """Condition 3: a uuid in the name means replay doubles the partition."""
    batch = make_batch(300)
    first = writer.write_batch("orders", batch)
    second = writer.write_batch("orders", batch)
    assert [r.key for r in first] == [r.key for r in second]

    day = date(2025, 3, 1)
    files = writer.store.list(writer.partition_prefix("orders", day))
    assert len(files) == len(first), "replay created additional files"


def test_replay_does_not_duplicate_rows(writer):
    batch = make_batch(1_000)
    writer.write_batch("orders", batch)
    writer.write_batch("orders", batch)
    table = writer.read_partition("orders", date(2025, 3, 1))
    offsets = table.column("_kafka_offset").to_pylist()
    assert len(offsets) == len(set(offsets)) == 1_000


def test_overlapping_replay_merges_rather_than_duplicating(writer):
    """Not just exact-range replay -- an overlapping range must be safe too."""
    writer.write_batch("orders", make_batch(600, start_offset=0))
    writer.write_batch("orders", make_batch(600, start_offset=300))
    table = writer.read_partition("orders", date(2025, 3, 1))
    offsets = sorted(table.column("_kafka_offset").to_pylist())
    assert offsets == list(range(900))
    assert len(offsets) == len(set(offsets))


def test_within_batch_duplicates_keep_the_last_delivery(writer):
    """A rebalance redelivers offsets; the later delivery wins."""
    rows = [
        {
            "id": "a",
            "_op": "c",
            "_lsn": 1,
            "_kafka_offset": 5,
            "_kafka_partition": 0,
            "_ingested_at": BASE_TS,
            "_source_ts": BASE_TS,
            "status": "first",
        },
        {
            "id": "a",
            "_op": "u",
            "_lsn": 2,
            "_kafka_offset": 5,
            "_kafka_partition": 0,
            "_ingested_at": BASE_TS,
            "_source_ts": BASE_TS,
            "status": "second",
        },
    ]
    deduped, removed = _dedupe_by_offset(pa.Table.from_pylist(rows))
    assert removed == 1
    assert deduped.column("status").to_pylist() == ["second"]


def test_same_offset_in_different_kafka_partitions_is_not_a_duplicate(writer):
    """Offsets are unique per partition, not globally. Deduping on offset
    alone would silently drop real rows."""
    rows = [
        {
            "id": "a",
            "_op": "c",
            "_lsn": 1,
            "_kafka_offset": 5,
            "_kafka_partition": 0,
            "_ingested_at": BASE_TS,
            "_source_ts": BASE_TS,
        },
        {
            "id": "b",
            "_op": "c",
            "_lsn": 2,
            "_kafka_offset": 5,
            "_kafka_partition": 1,
            "_ingested_at": BASE_TS,
            "_source_ts": BASE_TS,
        },
    ]
    deduped, removed = _dedupe_by_offset(pa.Table.from_pylist(rows))
    assert removed == 0
    assert deduped.num_rows == 2


def test_batch_spanning_midnight_splits_into_two_partitions(writer):
    batch = make_batch(200, start_offset=0)
    # Push the tail of the batch over midnight.
    ingested = [
        BASE_TS + timedelta(seconds=i) if i < 100 else BASE_TS + timedelta(days=1)
        for i in range(200)
    ]
    batch = batch.set_column(
        batch.schema.get_field_index("_ingested_at"),
        "_ingested_at",
        pa.array(ingested, type=pa.timestamp("us", tz="UTC")),
    )
    results = writer.write_batch("orders", batch)
    # Files split by (date, kafka_partition, offset_block), so the count is
    # dates x partitions. What matters is that BOTH dates are represented and
    # no row landed in the wrong one.
    assert {r.key.split("_ingested_date=")[1][:10] for r in results} == {"2025-03-01", "2025-03-02"}
    assert sum(r.rows for r in results) == 200
    day_two = writer.read_partition("orders", date(2025, 3, 2))
    assert day_two.num_rows == 100


def test_deletes_are_written_as_rows_never_removed(writer):
    batch = make_batch(400)
    writer.write_batch("orders", batch)
    table = writer.read_partition("orders", date(2025, 3, 1))
    ops = set(table.column("_op").to_pylist())
    assert "d" in ops, "delete rows were dropped instead of retained"
    assert table.num_rows == 400


def test_missing_required_metadata_column_is_an_error(writer):
    bad = pa.table({"id": ["a"], "_ingested_at": [BASE_TS]})
    with pytest.raises(ValueError, match="_kafka_offset"):
        writer.write_batch("orders", bad)


def test_compression_is_zstd_and_actually_smaller_than_uncompressed(tmp_path):
    batch = make_batch(20_000)
    zstd = PartitionWriter(LocalStore(tmp_path / "z"), compression="zstd")
    none = PartitionWriter(LocalStore(tmp_path / "n"), compression="none")
    z = sum(r.bytes_written for r in zstd.write_batch("orders", batch))
    n = sum(r.bytes_written for r in none.write_batch("orders", batch))
    assert z < n
    print(f"\n  zstd {z:,}B vs uncompressed {n:,}B -> {n / z:.2f}x")
