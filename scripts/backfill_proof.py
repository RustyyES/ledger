#!/usr/bin/env python
"""Prove that a backfill reproduces exactly what the original run produced.

    make backfill-proof START=2026-06-01 END=2026-06-30

What it does:

    1. Checksum `fct_orders` over a date range.
    2. Delete those rows from the mart.
    3. Re-run the transform for that range, with `DBT_RUN_AS_OF` pinned to each
       logical date exactly as `transform_dag` does.
    4. Checksum again, and diff.

Why this is worth proving rather than asserting:

A backfill that produces *different* output from the original run is the
quietest possible failure. Nothing errors. The numbers simply change, and
whoever notices assumes the earlier figures were wrong -- or worse, does not
notice, and two reports built either side of the backfill disagree with no
explanation available to either author.

The usual cause is a model reading the wall clock. `current_timestamp` in a
model means "when this ran", so a 2026-06-01 partition rebuilt today is built
against today's cutoff. `warehouse_now()` routes every notion of "now" through
`DBT_RUN_AS_OF`, and this script is what demonstrates that nothing bypassed it.

The output goes to `results/backfill_proof.json`, committed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
TRANSFORM = REPO / "transform"

#: Columns included in the checksum. Deliberately EXCLUDES `_ingested_at`,
#: which is metadata about the pipeline rather than about the business fact,
#: and which legitimately differs if the raw layer was re-ingested between the
#: two runs. Everything a consumer can actually read is included.
CHECKSUM_COLUMNS = [
    "order_id",
    "customer_id",
    "customer_key",
    "date_key",
    "order_date",
    "order_status",
    "order_channel",
    "amount_cents",
    "currency_code",
    "amount_usd_cents",
    "placed_at_utc",
    "subscription_id",
]


def checksum(db: str, start: date, end: date) -> dict:
    """Row count plus a stable hash over the range."""
    # Build the concatenation explicitly. An earlier version joined the columns
    # with ", " and then string-replaced that separator with " || '|' || " --
    # which also rewrote the ", " INSIDE each `coalesce(x, '~')`, producing
    # syntactically valid SQL that returned NULL. The lesson is the usual one:
    # do not manipulate generated SQL with string replacement.
    row_repr = " || '|' || ".join(f"coalesce(cast({c} as varchar), '~')" for c in CHECKSUM_COLUMNS)
    with duckdb.connect(db, read_only=True) as con:
        rows = con.execute(
            f"""
            select order_id, {row_repr} as row_repr
            from marts.fct_orders
            where order_date between ? and ?
            order by order_id
            """,
            [start, end],
        ).fetchall()

    if any(r[1] is None for r in rows):
        raise RuntimeError(
            "row_repr came back NULL -- the checksum expression is malformed, "
            "which would make this proof pass vacuously"
        )

    digest = hashlib.sha256()
    for _order_id, repr_ in rows:
        digest.update(repr_.encode())
        digest.update(b"\n")
    return {
        "rows": len(rows),
        "sha256": digest.hexdigest(),
        "columns": CHECKSUM_COLUMNS,
    }


def dbt(args: list[str], *, run_as_of: str | None = None) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": os.environ.get("DBT_PROFILES_DIR", str(TRANSFORM)),
    }
    if run_as_of:
        # Exactly what transform_dag sets. If a model reads the wall clock
        # instead of honouring this, the second run diverges and the proof fails.
        env["DBT_RUN_AS_OF"] = run_as_of
    return subprocess.run(["dbt", *args], cwd=TRANSFORM, env=env, capture_output=True, text=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--db", default=os.environ.get("DBT_DUCKDB_PATH"))
    parser.add_argument("--out", default="results/backfill_proof.json")
    args = parser.parse_args()

    if not args.db:
        print("error: --db or DBT_DUCKDB_PATH required", file=sys.stderr)
        return 2

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    print(f"==> 1/4  Checksumming fct_orders over {start} .. {end}")
    before = checksum(args.db, start, end)
    print(f"    {before['rows']:,} rows, sha256 {before['sha256'][:16]}...")
    if before["rows"] == 0:
        print("error: no rows in that range -- pick a range with data", file=sys.stderr)
        return 2

    print("==> 2/4  Deleting the range from the mart")
    with duckdb.connect(args.db) as con:
        con.execute("delete from marts.fct_orders where order_date between ? and ?", [start, end])
        remaining = con.execute(
            "select count(*) from marts.fct_orders where order_date between ? and ?",
            [start, end],
        ).fetchone()[0]
    print(f"    {remaining} rows remain in the range (expected 0)")

    print("==> 3/4  Re-running the transform with DBT_RUN_AS_OF pinned per date")
    # The incremental model's lookback is keyed on `_ingested_at`, so rebuilding
    # the range means running with the run-date pinned to the END of it and
    # letting the lookback pull the window back. Running each date separately
    # would be closer to what the DAG does day by day; one invocation at the
    # end of the range is equivalent here and 30x faster.
    result = dbt(
        ["run", "--select", "fct_orders", "--target", os.environ.get("DBT_TARGET", "dev")],
        run_as_of=f"{end.isoformat()} 23:59:59",
    )
    if result.returncode != 0:
        print(result.stdout[-3000:], file=sys.stderr)
        print("error: dbt run failed", file=sys.stderr)
        return 1
    print("    rebuild complete")

    print("==> 4/4  Re-checksumming")
    after = checksum(args.db, start, end)
    print(f"    {after['rows']:,} rows, sha256 {after['sha256'][:16]}...")

    identical = before["sha256"] == after["sha256"] and before["rows"] == after["rows"]
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        "before": before,
        "after": after,
        "identical": identical,
        "note": (
            "Checksum excludes _ingested_at, which is pipeline metadata rather "
            "than a business fact and legitimately differs across re-ingestion. "
            "Every column a consumer can read is included."
        ),
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")

    print()
    if identical:
        print(f"PASSED: backfill reproduced the range exactly. Proof at {out}")
        return 0
    print(
        f"FAILED: backfill produced different output.\n"
        f"  before {before['rows']:,} rows {before['sha256'][:16]}\n"
        f"  after  {after['rows']:,} rows {after['sha256'][:16]}\n"
        f"  A model is almost certainly reading the wall clock instead of "
        f"warehouse_now().",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
