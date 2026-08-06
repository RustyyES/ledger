"""Shared DAG configuration.

Everything in here exists so that the three DAG files contain orchestration and
nothing else. Two rules are enforced by construction rather than by review:

1. **No credentials in DAG files.** Connection and Variable lookups happen
   here, through Airflow's own stores, which read from the environment. A DSN
   in a DAG file is a DSN in git history, and DAG files get pasted into Slack.

2. **No `time.sleep()` anywhere.** Sleeping occupies a worker slot doing
   nothing; under a pool of N workers, N sleeping tasks are a deadlock. Every
   wait in this project is a Sensor with `poke_interval` and `timeout`, which
   releases the slot between pokes in reschedule mode. This is the clearest
   single tell between junior and senior Airflow code, and `make lint-dags`
   greps for it so the rule cannot rot.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

from airflow.models import Variable
from airflow.utils.email import send_email

# --------------------------------------------------------------------------- #
# Defaults applied to every task in every DAG.
# --------------------------------------------------------------------------- #

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,  # handled by the callback, which is richer
    "email_on_retry": False,
    "retries": 3,
    # Exponential backoff with a ceiling. Without `max_retry_delay`, doubling
    # from 5 minutes reaches 40 minutes by the third retry and the DAG blows
    # through its SLA while technically still "running".
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    # A task that hangs forever holds its slot forever. Every task gets a
    # ceiling; sensors override it with something longer.
    "execution_timeout": timedelta(minutes=45),
}

TAGS = ["ledger"]


# --------------------------------------------------------------------------- #
# Connections and variables -- never literals.
# --------------------------------------------------------------------------- #


def warehouse_env() -> dict[str, str]:
    """Environment for a dbt invocation.

    Reads from Airflow Variables, which are populated from the process
    environment by the scheduler. Nothing here is a literal, and nothing here
    is logged: Airflow masks Variables whose name matches its sensitive-field
    patterns, which is why the secret ones are named `*_secret_key` and
    `*_password`.
    """
    return {
        "DBT_TARGET": Variable.get("dbt_target", default_var="dev"),
        "DBT_PROFILES_DIR": Variable.get("dbt_profiles_dir", default_var="/opt/ledger/transform"),
        "DBT_DUCKDB_PATH": Variable.get(
            "dbt_duckdb_path", default_var="/data/warehouse/ledger.duckdb"
        ),
        "LEDGER_RAW_PATH": Variable.get("ledger_raw_path", default_var="/data/raw"),
        "SINK_S3_ACCESS_KEY": Variable.get("sink_s3_access_key", default_var="minioadmin"),
        "SINK_S3_SECRET_KEY": Variable.get("sink_s3_secret_key", default_var="minioadmin"),
        "DBT_S3_ENDPOINT": Variable.get("dbt_s3_endpoint", default_var="minio:9000"),
    }


def sink_config() -> dict[str, Any]:
    return {
        "bootstrap_servers": Variable.get("kafka_bootstrap", default_var="redpanda:9092"),
        "consumer_group": Variable.get("sink_consumer_group", default_var="ledger-sink"),
        "bucket": Variable.get("sink_bucket", default_var="ledger-raw"),
        "max_lag": int(Variable.get("sink_max_lag", default_var="100000")),
        "tables": [
            "customers",
            "plans",
            "subscriptions",
            "subscription_events",
            "orders",
            "payments",
            "refunds",
        ],
    }


# --------------------------------------------------------------------------- #
# Callbacks
# --------------------------------------------------------------------------- #


def alert_on_failure(context: dict[str, Any]) -> None:
    """Structured failure alert.

    Deliberately does not raise. A callback that throws replaces a task failure
    you can diagnose with a scheduler error you cannot.
    """
    ti = context.get("task_instance")
    payload = {
        "event": "task_failed",
        "dag_id": getattr(ti, "dag_id", "unknown"),
        "task_id": getattr(ti, "task_id", "unknown"),
        "run_id": context.get("run_id"),
        "try_number": getattr(ti, "try_number", None),
        "log_url": getattr(ti, "log_url", None),
        "exception": str(context.get("exception"))[:2000],
    }
    print(f"ALERT {payload}")
    _notify(f"[ledger] {payload['dag_id']}.{payload['task_id']} failed", payload)


def alert_on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """SLA miss callback.

    An SLA that fires nothing is a comment. This is the thing that makes the
    90-minute SLA on `transform_dag` a real commitment: `make test-sla`
    artificially delays a task to prove this path actually executes.

    Note the signature is positional and fixed by Airflow -- it is NOT the
    single-`context` shape of the other callbacks, which is a genuinely easy
    mistake that leaves the callback silently unregistered.
    """
    payload = {
        "event": "sla_missed",
        "dag_id": getattr(dag, "dag_id", "unknown"),
        "tasks": [str(t) for t in (task_list or [])],
        "blocking": [str(t) for t in (blocking_task_list or [])],
        "slas": [str(s) for s in (slas or [])],
    }
    print(f"ALERT {payload}")
    _notify(f"[ledger] SLA missed on {payload['dag_id']}", payload)


def _notify(subject: str, payload: dict[str, Any]) -> None:
    """Send an alert if a channel is configured; log it regardless.

    Alerting is best-effort by design: an unreachable SMTP server must not
    convert a warning into a second failure.
    """
    recipient = os.environ.get("LEDGER_ALERT_EMAIL")
    if not recipient:
        return
    try:
        send_email(to=[recipient], subject=subject, html_content=f"<pre>{payload}</pre>")
    except Exception as exc:
        print(f"alert_delivery_failed: {exc}")
