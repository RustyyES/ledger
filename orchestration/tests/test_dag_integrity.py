"""DAG integrity tests.

These run in CI on every pull request. Their job is to catch, at review time,
the class of DAG defects that otherwise only appear in production at 02:00 --
and to make the spec's non-negotiable rules mechanically enforced rather than
enforced by whoever happens to review the PR.

A DAG that fails to import does not show up as an error in the Airflow UI. It
shows up as a DAG that is simply *absent*, which is the single most confusing
failure mode the scheduler has.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_DIR))
os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow-test-home")

from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


EXPECTED_DAGS = {"ingest_dag", "transform_dag", "quality_dag"}


def test_all_dags_import_without_error(dagbag):
    assert not dagbag.import_errors, (
        "DAG import failures -- these appear in Airflow as MISSING DAGs, not as "
        f"errors:\n{dagbag.import_errors}"
    )


def test_expected_dags_are_present(dagbag):
    assert EXPECTED_DAGS.issubset(
        set(dagbag.dag_ids)
    ), f"missing: {EXPECTED_DAGS - set(dagbag.dag_ids)}"


def test_no_dag_has_cycles(dagbag):
    from airflow.utils.dag_cycle_tester import check_cycle

    for dag in dagbag.dags.values():
        check_cycle(dag)


# --------------------------------------------------------------------------- #
# The spec's non-negotiables, enforced mechanically.
# --------------------------------------------------------------------------- #


def test_no_sleep_anywhere_in_the_dags():
    """`time.sleep()` in a DAG occupies a worker slot doing nothing.

    Under a pool of N workers, N sleeping tasks are a deadlock. Every wait in
    this project is a Sensor in reschedule mode, which releases the slot between
    pokes. This is the clearest junior/senior tell in Airflow code and the
    reason it is a test rather than a convention.
    """
    import ast

    # Parsed, not grepped. A regex over source text cannot tell a real call
    # from the sentence in common.py's docstring explaining why not to make
    # one -- and a check that flags its own documentation gets muted, at which
    # point it catches nothing. The AST sees calls and only calls.
    offenders = []
    for path in DAGS_DIR.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = None
            if isinstance(func, ast.Attribute):
                name = func.attr
                module = getattr(func.value, "id", None)
                if name == "sleep" and module in ("time", "asyncio"):
                    offenders.append(f"{path.name}:{node.lineno}: {module}.sleep(...)")
            elif isinstance(func, ast.Name) and func.id == "sleep":
                offenders.append(f"{path.name}:{node.lineno}: sleep(...)")
    assert not offenders, (
        "sleep() found in DAG code -- use a Sensor in reschedule mode:\n" + "\n".join(offenders)
    )


def test_every_task_has_retries_and_backoff(dagbag):
    """Every task retries; sensors are allowed to retry less.

    A sensor that has already polled for thirty minutes has established that
    the thing it is waiting for is not coming. Retrying it three more times
    turns a 30-minute failure into a two-hour one and delays the alert by
    exactly as long. Ordinary tasks get 3 retries; sensors get 1, deliberately.
    """
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            # `type(task).__name__` is "MappedOperator" for anything built with
            # `.expand()`, which hides the underlying class. `operator_name`
            # reports the real operator for both mapped and unmapped tasks.
            operator = getattr(task, "operator_name", None) or type(task).__name__
            is_sensor = "sensor" in operator.lower() or "sensor" in task.task_id
            minimum = 1 if is_sensor else 2
            assert task.retries >= minimum, (
                f"{dag.dag_id}.{task.task_id} has retries={task.retries}, " f"expected >= {minimum}"
            )
            if task.retries > 1:
                assert task.retry_delay is not None, (
                    f"{dag.dag_id}.{task.task_id} retries with no delay -- "
                    "it will hammer a struggling dependency"
                )


def test_retry_backoff_is_capped(dagbag):
    """Exponential backoff without a ceiling blows through the SLA.

    Doubling from 5 minutes reaches 40 minutes by the third retry; a DAG with a
    90-minute SLA misses it while still technically 'running'.
    """
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            if getattr(task, "retry_exponential_backoff", False):
                assert task.max_retry_delay is not None, (
                    f"{dag.dag_id}.{task.task_id} uses exponential backoff with "
                    "no max_retry_delay"
                )


def test_every_task_has_an_execution_timeout(dagbag):
    """A task with no timeout can hold its slot forever."""
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert (
                task.execution_timeout is not None
            ), f"{dag.dag_id}.{task.task_id} has no execution_timeout"


def test_transform_dag_has_catchup_enabled(dagbag):
    """Explicitly required by the spec, and provable via `make prove-catchup`."""
    assert dagbag.dags["transform_dag"].catchup is True


def test_ingest_and_quality_do_not_catch_up(dagbag):
    """Backfilling 'is the data fresh right now' is meaningless."""
    assert dagbag.dags["ingest_dag"].catchup is False
    assert dagbag.dags["quality_dag"].catchup is False


def test_transform_dag_has_a_ninety_minute_sla_and_a_callback(dagbag):
    dag = dagbag.dags["transform_dag"]
    assert (
        dag.sla_miss_callback is not None
    ), "an SLA with no callback is a comment, not a commitment"
    slas = [t.sla for t in dag.tasks if t.sla is not None]
    assert slas, "no task carries an SLA"
    from datetime import timedelta

    assert timedelta(minutes=90) in slas or any(s <= timedelta(minutes=90) for s in slas)


def test_dags_are_serialised_where_they_share_state(dagbag):
    """Two concurrent dbt runs against one DuckDB file corrupt it."""
    for dag_id in ("transform_dag", "ingest_dag", "quality_dag"):
        assert (
            dagbag.dags[dag_id].max_active_runs == 1
        ), f"{dag_id} allows concurrent runs against shared state"


def test_no_credentials_are_literal_in_dag_files():
    """Credentials belong in Connections and Variables, not in git history.

    Looks for assignment of a plausible secret to a literal. Deliberately not a
    generic entropy check: those produce false positives on every hash in a
    comment, get muted, and then catch nothing.
    """
    pattern = re.compile(
        r"""(password|passwd|secret|api[_-]?key|access[_-]?key|token|dsn)"""
        r"""\s*[:=]\s*["'][^"'{}\s]{8,}["']""",
        re.IGNORECASE,
    )
    offenders = []
    for path in DAGS_DIR.rglob("*.py"):
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if pattern.search(line) and "Variable.get" not in line and "environ" not in line:
                offenders.append(f"{path.name}:{lineno}: {line.strip()}")
    assert not offenders, "literal credentials in DAG files:\n" + "\n".join(offenders)


def test_dynamic_task_mapping_is_used_for_per_table_work():
    """The spec requires `.expand()` rather than a parse-time for-loop.

    A for-loop that generates one task per table bakes the table list into the
    DAG structure: adding a table becomes a code change and a redeploy, and the
    DAG cannot react to what the upstream task actually discovered.
    """
    source = (DAGS_DIR / "ingest.py").read_text()
    assert (
        ".expand(" in source or ".expand_kwargs(" in source
    ), "ingest_dag does not use dynamic task mapping"


def test_every_dag_has_documentation(dagbag):
    for dag in dagbag.dags.values():
        assert dag.doc_md, f"{dag.dag_id} has no doc_md"
        assert len(dag.doc_md) > 200, (
            f"{dag.dag_id}'s doc_md is a stub -- explain WHY it is scheduled "
            "the way it is, not what it does"
        )


def test_every_dag_has_a_failure_callback(dagbag):
    for dag in dagbag.dags.values():
        assert dag.on_failure_callback is not None, f"{dag.dag_id} fails silently"


def test_every_dag_is_tagged(dagbag):
    for dag in dagbag.dags.values():
        assert "ledger" in dag.tags, f"{dag.dag_id} is not tagged for filtering"


def test_transform_dag_is_dataset_triggered(dagbag):
    """It should react to data landing, not fire optimistically on a clock.

    Checked via the timetable rather than a `dataset_triggers` attribute --
    that attribute does not exist on DAG in 2.11, and asserting on a missing
    attribute produces a test that always fails for the wrong reason.
    """
    dag = dagbag.dags["transform_dag"]
    assert type(dag.timetable).__name__ == "DatasetTriggeredTimetable", (
        f"transform_dag uses {type(dag.timetable).__name__}; it should be "
        "triggered by the raw datasets ingest_dag publishes"
    )
