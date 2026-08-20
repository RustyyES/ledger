#!/usr/bin/env python
"""Prove the sink's byte-identical replay guarantee, and commit the evidence.

    make prove-idempotency

What it does:

    1. Hash every Parquet file in a partition.
    2. DELETE the partition.
    3. Replay the same records through the writer.
    4. Hash again, and diff.

A claim like "re-running any partition produces byte-identical Parquet" is
worth nothing unasserted -- it is exactly the kind of property that is true
when written and quietly false three commits later, because breaking it
requires no error and produces no symptom until somebody diffs two files.

The output goes to `results/idempotency_proof.json`, which is committed. A
reviewer can recompute it; that is the point.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "cdc-sink"))

import pyarrow as pa
from storage import LocalStore, PartitionWriter, S3Store

BASE_TS = datetime(2025, 3, 1, 9, 0, tzinfo=UTC)


def synthetic_batch(rows: int, *, shuffle_seed: int | None = None) -> pa.Table:
    """A batch shaped exactly like real CDC output."""
    import random

    records = []
    for i in range(rows):
        records.append(
            {
                "id": f"order-{i:09d}",
                "customer_id": f"cust-{i % 997:06d}",
                "amount_cents": (i * 37) % 250_000,
                "currency": ["USD", "EUR", "GBP"][i % 3],
                "status": ["pending", "completed", "refunded", "paid"][i % 4],
                "_op": ["c", "u", "d", "r"][i % 4],
                "_lsn": 1_000_000 + i,
                "_kafka_offset": i,
                "_kafka_partition": i % 3,
                # Broker timestamp, never wall clock. This is condition 2 of the
                # guarantee: if _ingested_at were now(), replay could not possibly
                # produce identical bytes.
                "_ingested_at": BASE_TS + timedelta(seconds=i % 3600),
                "_source_ts": BASE_TS + timedelta(seconds=i % 3600) - timedelta(milliseconds=250),
            }
        )
    if shuffle_seed is not None:
        # Kafka guarantees order WITHIN a partition, not across a multi-partition
        # poll. Arrival order therefore varies run to run, and the writer must
        # sort before writing or the guarantee is a coincidence.
        random.Random(shuffle_seed).shuffle(records)
    return pa.Table.from_pylist(records)


def run(writer: PartitionWriter, *, rows: int, day: date) -> dict:
    report: dict = {
        "generated_at": datetime.now(UTC).isoformat(),
        "rows": rows,
        "partition_date": day.isoformat(),
        "steps": [],
    }

    # --- 1. initial write ---------------------------------------------------
    writer.drop_partition("orders", day)
    writer.write_batch("orders", synthetic_batch(rows))
    first = writer.partition_checksums("orders", day)
    report["steps"].append({"step": "initial_write", "files": len(first)})
    if not first:
        raise SystemExit("nothing was written -- the proof cannot run")

    # --- 2. drop and replay in the SAME order -------------------------------
    writer.drop_partition("orders", day)
    assert writer.partition_checksums("orders", day) == {}, "drop_partition left files behind"
    writer.write_batch("orders", synthetic_batch(rows))
    replayed = writer.partition_checksums("orders", day)
    report["steps"].append(
        {
            "step": "replay_same_order",
            "identical": replayed == first,
            "files": len(replayed),
        }
    )

    # --- 3. replay in SHUFFLED arrival order ---------------------------------
    writer.drop_partition("orders", day)
    writer.write_batch("orders", synthetic_batch(rows, shuffle_seed=99))
    shuffled = writer.partition_checksums("orders", day)
    report["steps"].append(
        {
            "step": "replay_shuffled_arrival_order",
            "identical": shuffled == first,
            "files": len(shuffled),
        }
    )

    # --- 4. OVERLAPPING replay ----------------------------------------------
    # The case a naive filename scheme gets wrong: two replays covering
    # different-but-overlapping ranges must converge, not duplicate.
    writer.drop_partition("orders", day)
    writer.write_batch("orders", synthetic_batch(rows))
    writer.write_batch("orders", synthetic_batch(rows // 2))
    overlapped = writer.partition_checksums("orders", day)
    table = writer.read_partition("orders", day)
    offsets = table.column("_kafka_offset").to_pylist()
    report["steps"].append(
        {
            "step": "overlapping_replay",
            "identical": overlapped == first,
            "rows_after": table.num_rows,
            "duplicate_offsets": len(offsets) - len(set(offsets)),
        }
    )

    report["checksums"] = {Path(k).name: v for k, v in sorted(first.items())}
    report["all_identical"] = (
        all(s.get("identical", True) for s in report["steps"])
        and report["steps"][-1]["duplicate_offsets"] == 0
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--local-path", default=None, help="Use a local directory instead of S3.")
    parser.add_argument("--out", default="results/idempotency_proof.json")
    args = parser.parse_args()

    store = (
        LocalStore(args.local_path)
        if args.local_path
        else S3Store(
            os.environ.get("S3_BUCKET", "ledger-raw"),
            endpoint_url=os.environ.get("SINK_S3_ENDPOINT_URL", "http://localhost:9000"),
            access_key=os.environ.get("SINK_S3_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("SINK_S3_SECRET_KEY", "minioadmin"),
        )
    )
    report = run(PartitionWriter(store), rows=args.rows, day=date(2025, 3, 1))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")

    print(f"\n{'step':34} {'identical':>10}")
    print("-" * 46)
    for step in report["steps"]:
        marker = step.get("identical")
        print(f"{step['step']:34} {'--' if marker is None else ('YES' if marker else 'NO'):>10}")
    print(f"\nchecksums for {len(report['checksums'])} file(s) written to {out}")

    if not report["all_identical"]:
        print("\nFAILED: replay is not byte-identical.", file=sys.stderr)
        return 1
    print("\nPASSED: every replay produced byte-identical Parquet.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
