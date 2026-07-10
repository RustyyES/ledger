#!/usr/bin/env python
"""Count rows (or match one id) in a raw Parquet prefix.

Exists so the Makefile does not have to embed Python in a recipe. Inline
heredocs in a Makefile are a tab-significance trap and they cannot be tested;
this can be run directly.

    python scripts/parquet_count.py orders
    python scripts/parquet_count.py customers --id <uuid>   # exit 0 if found
    python scripts/parquet_count.py orders --distinct id
"""

from __future__ import annotations

import argparse
import os
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table")
    parser.add_argument("--id", default=None, help="Exit 0 if this id is present, 1 if not.")
    parser.add_argument(
        "--distinct", default=None, help="Count distinct values of this column instead."
    )
    parser.add_argument("--path", default=os.environ.get("LEDGER_RAW_PATH", "s3://ledger-raw"))
    args = parser.parse_args()

    import duckdb

    con = duckdb.connect()
    if args.path.startswith("s3://"):
        # MinIO speaks S3 but is not AWS; DuckDB has to be told.
        con.execute("install httpfs; load httpfs;")
        endpoint = os.environ.get("SINK_S3_ENDPOINT_URL", "http://minio:9000")
        con.execute(f"set s3_endpoint='{endpoint.replace('http://', '').replace('https://', '')}'")
        con.execute(f"set s3_access_key_id='{os.environ.get('SINK_S3_ACCESS_KEY', 'minioadmin')}'")
        con.execute(
            f"set s3_secret_access_key='{os.environ.get('SINK_S3_SECRET_KEY', 'minioadmin')}'"
        )
        con.execute("set s3_use_ssl=false; set s3_url_style='path';")

    glob = f"{args.path.rstrip('/')}/{args.table}/**/*.parquet"
    try:
        if args.id:
            n = con.execute(
                f"select count(*) from read_parquet('{glob}', union_by_name=true) where id = ?",
                [args.id],
            ).fetchone()[0]
            print(n)
            return 0 if n else 1
        column = f"distinct {args.distinct}" if args.distinct else "*"
        n = con.execute(
            f"select count({column}) from read_parquet('{glob}', union_by_name=true)"
        ).fetchone()[0]
        print(n)
        return 0
    except duckdb.Error as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
