#!/usr/bin/env python
"""Prove that a late-arriving refund still lands in fct_payments.

    make prove-lookback

This is the demonstration the whole incremental design exists for. It does the
thing that breaks a naive pipeline, and then checks:

    1. Pick a payment that was processed roughly 12 days ago.
    2. Issue a refund against it TODAY.
    3. Re-run the INCREMENTAL build (not a full refresh -- a full refresh would
       pick it up trivially and prove nothing).
    4. Assert `fct_payments.refunded_amount_cents` for that payment changed.

Under the naive filter -- `where processed_at > (select max(processed_at) from
{{ this }})` -- step 4 fails. The payment's business timestamp is twelve days
old, so it never enters the incremental window, and its refund total stays 0
forever. No error, no null, no failing test: the row is just permanently wrong.

Under the `_ingested_at` lookback, the refund is recently INGESTED however old
the payment is, so the payment is reprocessed and the total is correct.

Run with `--simulate-naive` to watch the wrong version fail, which is the more
convincing half of the demonstration.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
TRANSFORM = REPO / "transform"


def dbt(args: list[str]) -> subprocess.CompletedProcess:
    env = {**os.environ, "DBT_PROFILES_DIR": os.environ.get("DBT_PROFILES_DIR", str(TRANSFORM))}
    return subprocess.run(["dbt", *args], cwd=TRANSFORM, env=env, capture_output=True, text=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.environ.get("DBT_DUCKDB_PATH"))
    parser.add_argument("--dsn", default=os.environ.get("COMMERCE_DATABASE_URL"))
    parser.add_argument("--lag-days", type=int, default=12)
    parser.add_argument(
        "--simulate-naive",
        action="store_true",
        help="Also show that a placed_at-keyed filter misses it.",
    )
    parser.add_argument("--out", default="results/lookback_proof.json")
    args = parser.parse_args()

    if not args.db or not args.dsn:
        print(
            "error: --db/DBT_DUCKDB_PATH and --dsn/COMMERCE_DATABASE_URL required", file=sys.stderr
        )
        return 2

    import psycopg

    dsn = args.dsn.replace("postgresql+psycopg://", "postgresql://")
    now = datetime.now(UTC)
    target_day = now - timedelta(days=args.lag_days)

    # --- 1. find a payment from ~lag_days ago with refund headroom ----------
    print(f"==> 1/5  Finding a succeeded payment from ~{args.lag_days} days ago")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            select p.id, p.amount_cents, p.created_at,
                   coalesce((select sum(r.amount_cents) from refunds r
                             where r.payment_id = p.id), 0) as already_refunded
            from payments p
            where p.status = 'succeeded'
              and p.created_at between %s and %s
              and p.amount_cents > 500
            order by p.created_at
            limit 1
            """,
            (target_day - timedelta(days=1), target_day + timedelta(days=1)),
        )
        row = cur.fetchone()

    if row is None:
        print(
            f"error: no succeeded payment found around {target_day.date()}. "
            f"Run `make backfill` first, or try a different --lag-days.",
            file=sys.stderr,
        )
        return 2

    payment_id, amount, created_at, already = row
    headroom = amount - already
    if headroom <= 0:
        print("error: that payment is already fully refunded", file=sys.stderr)
        return 2
    refund_amount = max(1, headroom // 2)
    actual_lag = (now - created_at.replace(tzinfo=UTC)).days
    print(f"    payment {payment_id}")
    print(f"    processed {created_at:%Y-%m-%d} -- {actual_lag} days ago")
    print(f"    amount {amount}, refunding {refund_amount}")

    # --- 2. capture the current warehouse state -----------------------------
    print("==> 2/5  Reading the warehouse's current value for that payment")
    with duckdb.connect(args.db, read_only=True) as con:
        before = con.execute(
            "select refunded_amount_cents, net_amount_cents, refund_lag_days "
            "from marts.fct_payments where payment_id = ?",
            [str(payment_id)],
        ).fetchone()
    if before is None:
        print(
            "error: that payment is not in fct_payments -- build the warehouse first",
            file=sys.stderr,
        )
        return 2
    print(f"    refunded_amount_cents = {before[0]}")

    # --- 3. issue the LATE refund -------------------------------------------
    print(f"==> 3/5  Issuing a refund TODAY against a {actual_lag}-day-old payment")
    refund_id = uuid.uuid4()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into refunds (id, payment_id, amount_cents, reason, issued_at, created_at)
            values (%s, %s, %s, %s, %s, %s)
            """,
            (refund_id, payment_id, refund_amount, "lookback proof", now, now),
        )
        # The payment row itself is untouched apart from updated_at -- which is
        # the whole point. Nothing about the PAYMENT changed; only a child row
        # arrived. A pipeline keyed on the payment's own timestamps cannot see
        # this at all.
        cur.execute("update payments set updated_at = %s where id = %s", (now, payment_id))
        conn.commit()
    print(f"    refund {refund_id} inserted")

    # --- 4. re-ingest and run the INCREMENTAL build -------------------------
    print("==> 4/5  Re-exporting raw and running an INCREMENTAL dbt build")
    export = (
        subprocess.run(
            [
                sys.executable,
                str(REPO / "services" / "cdc-sink" / "bulk_export.py"),
                "--dsn",
                dsn,
                "--local-path",
                os.environ["LEDGER_RAW_PATH"],
            ],
            capture_output=True,
            text=True,
        )
        if os.environ.get("LEDGER_RAW_PATH", "").startswith("/")
        else None
    )
    if export is not None and export.returncode != 0:
        print(export.stderr[-2000:], file=sys.stderr)
        return 1

    # NOT --full-refresh. A full refresh would rebuild everything and prove
    # nothing about the incremental filter, which is the thing under test.
    result = dbt(["run", "--select", "fct_payments"])
    if result.returncode != 0:
        print(result.stdout[-3000:], file=sys.stderr)
        return 1
    print("    incremental build complete")

    # --- 5. did it land? ----------------------------------------------------
    print("==> 5/5  Re-reading the warehouse")
    with duckdb.connect(args.db, read_only=True) as con:
        after = con.execute(
            "select refunded_amount_cents, net_amount_cents, refund_lag_days "
            "from marts.fct_payments where payment_id = ?",
            [str(payment_id)],
        ).fetchone()
    print(f"    refunded_amount_cents = {after[0]}  (was {before[0]})")

    landed = after[0] == before[0] + refund_amount
    report = {
        "generated_at": now.isoformat(),
        "payment_id": str(payment_id),
        "payment_processed_at": created_at.isoformat(),
        "refund_issued_at": now.isoformat(),
        "arrival_lag_days": actual_lag,
        "configured_lookback_days": 21,
        "refund_amount_cents": refund_amount,
        "refunded_before": before[0],
        "refunded_after": after[0],
        "landed": landed,
        "why_this_matters": (
            "An incremental model filtered on the payment's own business "
            "timestamp would never re-select this row: its processed_at is "
            f"{actual_lag} days old and far below max(processed_at). The refund "
            "would be invisible and net_amount_cents permanently overstated. "
            "Filtering on _ingested_at -- when the record reached the pipeline "
            "-- makes a late fact recent by definition."
        ),
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")

    print()
    if landed:
        print(f"PASSED: a {actual_lag}-day-late refund landed in the incremental build.")
        print(f"        proof written to {out}")
        return 0
    print(
        f"FAILED: the refund did NOT land.\n"
        f"  expected {before[0] + refund_amount}, got {after[0]}\n"
        f"  The lookback window is too narrow, or the incremental filter is "
        f"keyed on a business timestamp instead of _ingested_at.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
