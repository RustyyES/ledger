"""transform_dag -- daily 02:00 UTC. Runs the dbt project.

Triggered by `ingest_dag` publishing its datasets, AND on a daily schedule. Both,
deliberately: the dataset trigger makes it responsive to real data landing, and
the schedule guarantees a run even if ingestion is silent, so a stalled sink
shows up as a failing transform rather than as a DAG that simply never fires.
A pipeline whose failure mode is silence is a pipeline nobody notices breaking.

CATCHUP IS ON HERE. That is the opposite of `ingest_dag`, and the reason is
that a transform run for 2025-03-01 is meaningful and reproducible: `DBT_RUN_AS_OF`
pins the warehouse's notion of "now" to the logical date, so re-running a past
day produces what the original run produced. `make backfill-proof` demonstrates
exactly that with checksums.

SLA: 90 minutes, with a callback that actually fires. See `alert_on_sla_miss`.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from common import DEFAULT_ARGS, TAGS, alert_on_failure, alert_on_sla_miss, warehouse_env

DBT_DIR = os.environ.get("LEDGER_DBT_DIR", "/opt/ledger/transform")

RAW_DATASETS = [
    Dataset(f"s3://ledger-raw/{t}")
    for t in (
        "customers",
        "plans",
        "subscriptions",
        "subscription_events",
        "orders",
        "payments",
        "refunds",
    )
]

MART_DATASET = Dataset("duckdb://ledger/marts")


def dbt_command(
    name: str, subcommand: str, *, sla_minutes: int | None = None, extra: str = ""
) -> BashOperator:
    """One dbt invocation as a Bash task.

    `--no-write-json` is omitted on purpose: the run artifacts are what
    `quality_dag` reads to report test pass rates, so they must be written.

    `DBT_RUN_AS_OF` is set from the LOGICAL date, never from wall-clock time.
    That single environment variable is what makes a backfill reproducible: a
    model that calls `current_timestamp` cannot produce the same output twice,
    and `warehouse_now()` in the dbt project routes every "now" through this.
    """
    return BashOperator(
        task_id=name,
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt {subcommand} "
            f"--target ${{DBT_TARGET}} "
            f"--profiles-dir ${{DBT_PROFILES_DIR}} "
            f"{extra}"
        ),
        env={
            **warehouse_env(),
            # `{{ ds }}` is the logical date of THIS run, not today.
            "DBT_RUN_AS_OF": "{{ ds }} 23:59:59",
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        },
        append_env=False,  # explicit env only; nothing leaks in from the worker
        sla=timedelta(minutes=sla_minutes) if sla_minutes else None,
        retries=2,
        execution_timeout=timedelta(minutes=60),
    )


@dag(
    dag_id="transform_dag",
    description="Build and test the dbt project.",
    # Both a schedule and dataset triggers. See the module docstring.
    schedule=[*RAW_DATASETS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # ON, and provably so: `make prove-catchup` clears a week of runs and
    # watches them backfill in order.
    catchup=True,
    # Serialised. Two concurrent dbt runs against one DuckDB file corrupt it,
    # and against Snowflake they race on the same incremental tables.
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "sla": timedelta(minutes=90)},
    on_failure_callback=alert_on_failure,
    sla_miss_callback=alert_on_sla_miss,
    tags=[*TAGS, "transform", "dbt"],
    doc_md=__doc__,
)
def transform_dag():
    @task(task_id="assert_dbt_deps_installed")
    def assert_deps() -> str:
        """Fail fast if `dbt deps` was never run.

        Without this the first dbt task fails with a compilation error about an
        unknown macro, which sends whoever is on call looking for a bug in the
        SQL rather than a missing package directory.
        """
        packages = os.path.join(DBT_DIR, "dbt_packages")
        if not os.path.isdir(packages) or not os.listdir(packages):
            raise RuntimeError(
                f"{packages} is empty -- run `dbt deps` in the image build, "
                f"not at runtime. Installing packages on every DAG run makes "
                f"the pipeline depend on the dbt hub being up."
            )
        return packages

    seed = dbt_command("dbt_seed", "seed", extra="--select seed_fx_rates seed_holidays")

    # SNAPSHOT BEFORE RUN. This ordering is load-bearing.
    #
    # The snapshot captures customer state as of NOW. `dim_customer` reads the
    # snapshot, and `fct_orders` resolves its as-of foreign key against
    # `dim_customer`. Running the snapshot AFTER the models would mean today's
    # facts are attributed against yesterday's dimension versions -- every
    # customer who changed country today is silently attributed to their old
    # country for a full day.
    snapshot = dbt_command("dbt_snapshot", "snapshot")

    run_staging = dbt_command("dbt_run_staging", "run", extra="--select staging")
    run_intermediate = dbt_command("dbt_run_intermediate", "run", extra="--select intermediate")
    run_marts = dbt_command("dbt_run_marts", "run", extra="--select marts", sla_minutes=75)

    # Tests run LAST and are not `trigger_rule=all_done`. If a model failed to
    # build, its tests are meaningless -- running them anyway produces a wall of
    # "relation does not exist" errors that buries the one real failure.
    test = dbt_command("dbt_test", "test")

    @task(task_id="publish_docs", trigger_rule=TriggerRule.ALL_SUCCESS)
    def publish_docs() -> str:
        """Regenerate the lineage graph only when the build was clean.

        Publishing docs for a broken build is worse than not publishing: it puts
        a browsable, authoritative-looking graph in front of people describing a
        warehouse that did not build.
        """
        import subprocess

        env = {**os.environ, **warehouse_env()}
        subprocess.run(
            [
                "dbt",
                "docs",
                "generate",
                "--target",
                env["DBT_TARGET"],
                "--profiles-dir",
                env["DBT_PROFILES_DIR"],
            ],
            cwd=DBT_DIR,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        return f"{DBT_DIR}/target/index.html"

    @task(task_id="emit_mart_dataset", outlets=[MART_DATASET])
    def emit_mart_dataset() -> str:
        """Signals downstream consumers (metrics API cache, dashboard)."""
        return "marts refreshed"

    (
        assert_deps()
        >> seed
        >> snapshot
        >> run_staging
        >> run_intermediate
        >> run_marts
        >> test
        >> publish_docs()
        >> emit_mart_dataset()
    )


transform_dag()
