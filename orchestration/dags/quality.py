"""quality_dag -- every 4 hours. Does the warehouse still deserve to be trusted?

Separate from `transform_dag` on purpose. If quality checks live inside the
build, they only run when the build runs, and the interesting failures are
exactly the ones that happen BETWEEN builds: a source that stopped producing at
03:00, a sink that fell behind overnight, a row count that drifted after
somebody ran a manual backfill.

Every check writes a metric rather than only raising. A quality DAG that just
fails tells you something is wrong now; one that records a series tells you when
it started, which is the question actually being asked at 03:00.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from common import DEFAULT_ARGS, TAGS, alert_on_failure, warehouse_env

DBT_DIR = os.environ.get("LEDGER_DBT_DIR", "/opt/ledger/transform")
METRICS_DATASET = Dataset("duckdb://ledger/quality_metrics")

#: Tables whose freshness is meaningful. `plans` is excluded: it changes twice a
#: year, so a freshness alarm on it would fire permanently and train everyone to
#: ignore the channel.
FRESHNESS_TABLES = [
    "customers",
    "subscriptions",
    "subscription_events",
    "orders",
    "payments",
    "refunds",
]

FRESHNESS_WARN_HOURS = 2
FRESHNESS_ERROR_HOURS = 6
ROWCOUNT_DRIFT_THRESHOLD = 0.02  # 2%


@dag(
    dag_id="quality_dag",
    description="Freshness, row-count drift and reconciliation checks.",
    schedule="0 */4 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,  # "is the data fresh RIGHT NOW" has no history to backfill
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 2},
    on_failure_callback=alert_on_failure,
    tags=[*TAGS, "quality"],
    doc_md=__doc__,
)
def quality_dag():
    def _warehouse():
        import duckdb

        env = warehouse_env()
        return duckdb.connect(env["DBT_DUCKDB_PATH"], read_only=True)

    @task(task_id="freshness_check")
    def freshness_check() -> dict:
        """How long since each source table last produced a record.

        Measured on `_ingested_at` -- when the record reached the PIPELINE --
        not on a business timestamp. A table can legitimately have no new orders
        at 04:00; what it cannot legitimately have is no new records reaching
        the warehouse, because CDC emits on change and the sink writes on a
        timer.
        """
        results: dict[str, float] = {}
        stale: list[str] = []
        with _warehouse() as con:
            for table in FRESHNESS_TABLES:
                row = con.execute(f"select max(_ingested_at) from staging.stg_{table}").fetchone()
                if not row or row[0] is None:
                    results[table] = float("inf")
                    stale.append(f"{table} (no rows at all)")
                    continue
                last = row[0]
                if last.tzinfo is None:
                    last = last.replace(tzinfo=UTC)
                hours = (datetime.now(UTC) - last).total_seconds() / 3600
                results[table] = round(hours, 2)
                if hours > FRESHNESS_ERROR_HOURS:
                    stale.append(f"{table} ({hours:.1f}h)")

        print(json.dumps({"freshness_hours": results}, default=str))
        if stale:
            raise AirflowFailException(f"stale beyond {FRESHNESS_ERROR_HOURS}h: {', '.join(stale)}")
        warn = [t for t, h in results.items() if h > FRESHNESS_WARN_HOURS]
        if warn:
            print(f"WARN approaching staleness: {warn}")
        return results

    @task(task_id="rowcount_drift_check")
    def rowcount_drift_check() -> dict:
        """Staging vs marts row counts, per table.

        This is the check that catches a join that started dropping rows. It
        cannot catch a join that started dropping rows from the very first run
        -- that is what `assert_fct_orders_rowcount_matches_staging` is for --
        but it catches the far more common case of a model that was fine and
        then regressed.
        """
        pairs = [
            ("stg_orders", "fct_orders"),
            ("stg_payments", "fct_payments"),
            ("stg_subscription_events", "fct_subscription_events"),
        ]
        drift: dict[str, dict] = {}
        offenders: list[str] = []
        with _warehouse() as con:
            for staging_model, mart in pairs:
                s = con.execute(f"select count(*) from staging.{staging_model}").fetchone()[0]
                m = con.execute(f"select count(*) from marts.{mart}").fetchone()[0]
                delta = abs(s - m) / s if s else 0.0
                drift[mart] = {"staging": s, "mart": m, "drift_pct": round(delta * 100, 3)}
                if delta > ROWCOUNT_DRIFT_THRESHOLD:
                    offenders.append(f"{mart}: {s:,} vs {m:,} ({delta:.2%})")

        print(json.dumps({"rowcount_drift": drift}))
        if offenders:
            raise AirflowFailException("row count drift beyond 2%: " + "; ".join(offenders))
        return drift

    @task(task_id="reconciliation_check")
    def reconciliation_check() -> dict:
        """Re-run the reconciliation tests on their own cadence.

        These are the same singular tests `dbt test` runs during the build. They
        run again here because they are the checks whose result can change
        WITHOUT a build: a late refund landing outside the lookback, or a manual
        write to the source, both break reconciliation between builds.
        """
        import subprocess

        env = {**os.environ, **warehouse_env()}
        selected = (
            "assert_mrr_reconciles_to_subscription_state,"
            "assert_mrr_reconciles_to_payments,"
            "assert_refund_not_exceeding_payment,"
            "assert_no_late_arrival_outside_lookback"
        )
        proc = subprocess.run(
            [
                "dbt",
                "test",
                "--select",
                selected,
                "--target",
                env["DBT_TARGET"],
                "--profiles-dir",
                env["DBT_PROFILES_DIR"],
            ],
            cwd=DBT_DIR,
            env=env,
            capture_output=True,
            text=True,
        )
        print(proc.stdout[-4000:])
        if proc.returncode != 0:
            raise AirflowFailException(
                "reconciliation failed between builds -- the warehouse and the "
                "source no longer agree. Tail of dbt output:\n" + proc.stdout[-2000:]
            )
        return {"returncode": proc.returncode, "selected": selected}

    @task(task_id="dbt_test_pass_rate")
    def dbt_test_pass_rate() -> dict:
        """Read the last build's run_results.json for the dashboard.

        Skips rather than fails when the artifact is absent: a missing artifact
        means no build has happened yet, which is a fact about the schedule and
        not a data quality problem.
        """
        path = os.path.join(DBT_DIR, "target", "run_results.json")
        if not os.path.exists(path):
            raise AirflowSkipException(f"{path} not found -- no build has run yet")

        with open(path) as fh:
            payload = json.load(fh)
        results = payload.get("results", [])
        tests = [r for r in results if r.get("unique_id", "").startswith("test.")]
        passed = sum(1 for r in tests if r.get("status") == "pass")
        summary = {
            "tests_total": len(tests),
            "tests_passed": passed,
            "pass_rate_pct": round(passed * 100 / len(tests), 2) if tests else None,
            "generated_at": payload.get("metadata", {}).get("generated_at"),
        }
        print(json.dumps(summary))
        return summary

    @task(task_id="publish_metrics", outlets=[METRICS_DATASET])
    def publish_metrics(freshness: dict, drift: dict, recon: dict, tests: dict) -> str:
        """Append one observation to the quality series the dashboard reads.

        Appends rather than overwrites, because the value of this table is the
        TREND. A dashboard that only shows the current state cannot answer
        "when did this start", which is the first question every time.
        """
        import duckdb

        env = warehouse_env()
        observed_at = datetime.now(UTC)
        with duckdb.connect(env["DBT_DUCKDB_PATH"]) as con:
            con.execute("create schema if not exists ops")
            con.execute("""
                create table if not exists ops.quality_observations (
                    observed_at timestamp,
                    metric      varchar,
                    subject     varchar,
                    value       double,
                    payload     varchar
                )
            """)
            rows = []
            for table, hours in freshness.items():
                rows.append(
                    (
                        observed_at,
                        "freshness_hours",
                        table,
                        None if hours == float("inf") else hours,
                        None,
                    )
                )
            for mart, d in drift.items():
                rows.append(
                    (observed_at, "rowcount_drift_pct", mart, d["drift_pct"], json.dumps(d))
                )
            if tests.get("pass_rate_pct") is not None:
                rows.append(
                    (
                        observed_at,
                        "dbt_test_pass_rate",
                        "all",
                        tests["pass_rate_pct"],
                        json.dumps(tests),
                    )
                )
            rows.append(
                (
                    observed_at,
                    "reconciliation_ok",
                    "all",
                    1.0 if recon.get("returncode") == 0 else 0.0,
                    None,
                )
            )
            con.executemany("insert into ops.quality_observations values (?, ?, ?, ?, ?)", rows)
        print(f"published {len(rows)} observations")
        return f"{len(rows)} observations"

    fresh = freshness_check()
    drift = rowcount_drift_check()
    recon = reconciliation_check()
    tests = dbt_test_pass_rate()

    # Sequential, not parallel: they all read the same DuckDB file, and DuckDB
    # takes an exclusive lock for the write in publish_metrics. On Snowflake
    # these would fan out.
    fresh >> drift >> recon >> tests
    publish_metrics(fresh, drift, recon, tests)


quality_dag()
