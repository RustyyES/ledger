"""ingest_dag -- hourly. Kafka -> Parquet partition landing and registration.

Runs hourly because the sink is a continuously-running service; this DAG does
not DO the ingestion, it supervises it. That distinction matters: an ingestion
DAG that owns the consumer can only ingest while the DAG is running, which
turns a streaming pipeline into an hourly batch one with extra steps.

What it actually does:
  * fails loudly if consumer lag is running away;
  * asks the sink to seal the current partition;
  * WAITS for the partition to appear in object storage;
  * registers what landed, so downstream has a manifest to reconcile against.

The wait is a Sensor in `reschedule` mode, not a sleep. In reschedule mode the
worker slot is RELEASED between pokes, so a hundred waiting partitions occupy
zero slots instead of a hundred. With `poke` mode (the default) each sensor
holds its slot for the whole wait, and enough of them deadlock the pool.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.sensors.python import PythonSensor
from common import DEFAULT_ARGS, TAGS, alert_on_failure, sink_config


def _topic_lag(config: dict, topic: str) -> int:
    """Committed-offset lag for one topic, summed across partitions.

    Uses a throwaway consumer in the SINK'S consumer group so that
    `committed()` returns the sink's real position. Creating it with
    `enable.auto.commit=False` and never subscribing means it can read the
    group's offsets without joining the group and triggering a rebalance --
    a rebalance every hour would stall the actual sink for seconds at a time.
    """
    from confluent_kafka import Consumer, TopicPartition

    probe = Consumer(
        {
            "bootstrap.servers": config["bootstrap_servers"],
            "group.id": config["consumer_group"],
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = probe.list_topics(topic, timeout=20)
        partitions = list(metadata.topics[topic].partitions)
        committed = probe.committed([TopicPartition(topic, p) for p in partitions], timeout=20)
        total = 0
        for tp in committed:
            _low, high = probe.get_watermark_offsets(tp, timeout=20, cached=False)
            position = tp.offset if tp.offset and tp.offset >= 0 else 0
            total += max(0, high - position)
        return total
    finally:
        probe.close()


#: Datasets are how `transform_dag` learns that new data landed, instead of
#: being scheduled optimistically and hoping. One per table so a single stalled
#: table does not block the rest.
RAW_DATASETS = {
    table: Dataset(f"s3://ledger-raw/{table}")
    for table in (
        "customers",
        "plans",
        "subscriptions",
        "subscription_events",
        "orders",
        "payments",
        "refunds",
    )
}


@dag(
    dag_id="ingest_dag",
    description="Seal and register CDC Parquet partitions, hourly.",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # Catchup OFF here, deliberately -- and this is the opposite of the choice
    # made in transform_dag. Re-running a past hour of INGESTION is meaningless:
    # the sink has already moved on, the offsets are committed, and the
    # partitions exist. Backfilling this DAG would do nothing but burn slots.
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=alert_on_failure,
    tags=[*TAGS, "ingestion"],
    doc_md=__doc__,
)
def ingest_dag():
    @task(task_id="check_kafka_lag")
    def check_kafka_lag() -> dict:
        """Fail the run if consumer lag is beyond recovery.

        Lag is not a warning here. A sink that is 100k records behind is a sink
        that will still be behind when the transform DAG runs, and the marts it
        builds will silently be stale rather than wrong -- which is worse,
        because nothing looks broken.
        """
        from confluent_kafka.admin import AdminClient

        config = sink_config()
        admin = AdminClient({"bootstrap.servers": config["bootstrap_servers"]})
        metadata = admin.list_topics(timeout=20)

        lags: dict[str, int] = {}
        for table in config["tables"]:
            topic = f"ledger.public.{table}"
            if topic not in metadata.topics:
                # A topic that does not exist yet is normal on a cold start; a
                # topic that vanishes is not. Both surface as lag 0 here and are
                # caught by the freshness check in quality_dag, which is the
                # right place for "is this table alive at all".
                lags[table] = 0
                continue
            lags[table] = _topic_lag(config, topic)

        worst = max(lags.values()) if lags else 0
        if worst > config["max_lag"]:
            raise AirflowFailException(
                f"consumer lag {worst:,} exceeds threshold {config['max_lag']:,}. "
                f"Per-table lag: {lags}. The sink is not keeping up; do not "
                f"build marts on top of this."
            )
        return lags

    @task(task_id="flush_sink_partition")
    def flush_sink_partition(lags: dict) -> list[dict]:
        """Ask the sink to seal its current batch for each table.

        Idempotent: sealing an already-sealed partition is a no-op, so clearing
        and re-running this task is safe. That is a requirement, not a nicety --
        every task in this project must survive being cleared.
        """
        import urllib.request

        config = sink_config()
        results = []
        for table in config["tables"]:
            request = urllib.request.Request(f"http://cdc-sink:8080/flush/{table}", method="POST")
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    body = json.loads(response.read())
            except Exception as exc:
                # A sink that cannot be reached is a real failure, but reporting
                # it per-table tells you whether it is one table or the service.
                body = {"table": table, "error": str(exc), "flushed": False}
            results.append({"table": table, "lag": lags.get(table, 0), **body})
        return results

    @task(task_id="expected_partitions")
    def expected_partitions(flushed: list[dict]) -> list[dict]:
        """Compute the object-store prefix each table should have written to.

        The shape of these dicts IS the sensor's kwargs -- `.expand(op_kwargs=)`
        unpacks each one directly -- so the keys here and `_partition_exists`'s
        signature are a contract. Adding a key without adding a parameter is a
        TypeError at map time, per mapped task, which is a confusing way to
        find out.
        """
        day = datetime.now(UTC).date().isoformat()
        return [
            {"table": item["table"], "prefix": f"{item['table']}/_ingested_date={day}/"}
            for item in flushed
        ]

    def _partition_exists(table: str, prefix: str) -> bool:
        from storage import S3Store

        config = sink_config()
        store = S3Store(
            config["bucket"],
            endpoint_url=__import__("os").environ.get("SINK_S3_ENDPOINT_URL"),
            access_key=__import__("os").environ.get("SINK_S3_ACCESS_KEY"),
            secret_key=__import__("os").environ.get("SINK_S3_SECRET_KEY"),
        )
        keys = store.list(prefix)
        print(f"{table}: {len(keys)} object(s) under {prefix}")
        return len(keys) > 0

    @task(task_id="register_partition_metadata")
    def register_partition_metadata(partitions: list[dict]) -> dict:
        """Record what landed, so quality_dag has something to reconcile to."""
        from storage import S3Store

        config = sink_config()
        store = S3Store(
            config["bucket"],
            endpoint_url=__import__("os").environ.get("SINK_S3_ENDPOINT_URL"),
            access_key=__import__("os").environ.get("SINK_S3_ACCESS_KEY"),
            secret_key=__import__("os").environ.get("SINK_S3_SECRET_KEY"),
        )
        manifest = {
            "registered_at": datetime.now(UTC).isoformat(),
            "partitions": [{**p, "objects": len(store.list(p["prefix"]))} for p in partitions],
        }
        # Deterministic key: re-running the task overwrites rather than appends,
        # which is what makes the task idempotent.
        key = f"_manifest/ingest/{datetime.now(UTC):%Y-%m-%dT%H}.json"
        store.write(key, json.dumps(manifest, indent=2).encode())
        print(f"registered {len(manifest['partitions'])} partitions at {key}")
        return manifest

    lags = check_kafka_lag()
    flushed = flush_sink_partition(lags)
    partitions = expected_partitions(flushed)

    # Dynamic task mapping: ONE sensor per table, generated at runtime from the
    # upstream result. The alternative -- a Python for-loop over a hard-coded
    # table list at parse time -- bakes the table list into the DAG structure,
    # so adding a table becomes a code change plus a redeploy, and the DAG
    # cannot react to what the upstream task actually found.
    verify = PythonSensor.partial(
        task_id="verify_partition_written",
        python_callable=_partition_exists,
        # RESCHEDULE, not poke. See the module docstring.
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
        # A missing partition at this point means the sink is wedged. Failing
        # is right; retrying the sensor would just wait another 30 minutes.
        soft_fail=False,
        retries=1,
        execution_timeout=timedelta(minutes=35),
    ).expand(op_kwargs=partitions)

    registered = register_partition_metadata(partitions)
    verify >> registered

    # Emitting the datasets is what triggers transform_dag. Placed last, after
    # verification, so a failed ingestion cannot trigger a transform.
    @task(task_id="publish_datasets", outlets=list(RAW_DATASETS.values()))
    def publish_datasets(manifest: dict) -> int:
        count = sum(p["objects"] for p in manifest["partitions"])
        print(f"published {len(manifest['partitions'])} datasets, {count} objects")
        return count

    publish_datasets(registered)


ingest_dag()
