# Chapter 16 — Orchestration

> Source: [`orchestration/dags/`](../../orchestration/dags/)

## What orchestration is for

You have pieces that must run in order, on a schedule, and recover from failure.
That's it. Everything else Airflow does is in service of those three things.

Could you use cron? For a linear chain, yes. What cron doesn't give you:

- **dependencies** — "run this only if that succeeded"
- **retries** with backoff
- **backfill** — "re-run last Tuesday"
- **visibility** — which task failed, with logs
- **concurrency control** — "never two of these at once"

Airflow gives you all five. It's heavy, and Chapter 20 covers when it isn't
worth it.

## DAG in one sentence

A **D**irected **A**cyclic **G**raph: tasks with dependencies, no cycles.

```
check_lag → flush → verify → register → publish
```

Airflow runs it on a schedule, one *run* per scheduled interval.

## Three DAGs, deliberately separate

| DAG | Schedule | Job |
|---|---|---|
| `ingest_dag` | hourly | seal and register Parquet partitions |
| `transform_dag` | daily 02:00 + dataset-triggered | run dbt |
| `quality_dag` | every 4 hours | is the warehouse still trustworthy? |

**Why not one DAG?** Because they have different failure meanings and different
cadences. Quality checks that only run when the build runs miss the interesting
failures — a source that stopped producing at 3am, a sink that fell behind
overnight. Those happen *between* builds.

## The rule that separates junior from senior Airflow code

> **Never `time.sleep()` in a task.**

Sleeping occupies a worker slot doing nothing. With a pool of N workers, N
sleeping tasks are a **deadlock** — nothing else can run.

Use a **Sensor** in `reschedule` mode:

```python
verify = PythonSensor.partial(
    task_id="verify_partition_written",
    python_callable=_partition_exists,
    mode="reschedule",      # ← releases the worker slot between checks
    poke_interval=60,
    timeout=60 * 30,
)
```

In `reschedule` mode the task **releases its slot** between pokes and Airflow
wakes it later. A hundred waiting sensors occupy zero slots.

In `poke` mode (the default!) each sensor holds its slot for the entire wait.
Enough of them and the pool deadlocks.

This is enforced, not just documented:

```python
def test_no_sleep_anywhere_in_the_dags():
    """Parsed, not grepped."""
    import ast
    for path in DAGS_DIR.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call): ...
```

**Why AST rather than grep?** The first version was a regex, and it flagged the
sentence in `common.py`'s docstring *explaining why not to use sleep*. A check
that flags its own documentation gets muted, and then it catches nothing. The
AST sees calls and only calls.

## Catchup: on for one DAG, off for two

```python
# transform_dag
catchup=True

# ingest_dag, quality_dag
catchup=False
```

**Why the difference?**

Re-running a past day's **transform** is meaningful: `DBT_RUN_AS_OF` pins the
warehouse's notion of "now" to the logical date, so it reproduces what the
original run produced.

Re-running a past hour of **ingestion** is meaningless. The sink has already
moved on, offsets are committed, partitions exist. Backfilling would burn slots
achieving nothing.

Same for quality — "is the data fresh *right now*" has no history to backfill.

> **`catchup=True` is right when a past run is reproducible and meaningful.
> Otherwise it's a way to accidentally launch 400 runs on your first deploy.**

## Making backfill actually reproducible

```python
env={
    **warehouse_env(),
    "DBT_RUN_AS_OF": "{{ ds }} 23:59:59",     # ← the LOGICAL date, not today
}
```

Every notion of "now" in the dbt project routes through this:

```sql
{% macro warehouse_now() %}
    {%- if var('run_as_of') -%}
        cast('{{ var("run_as_of") }}' as timestamp)
    {%- else -%}
        current_timestamp
    {%- endif -%}
{% endmacro %}
```

**Why this matters:** a model calling `current_timestamp` directly cannot be
backfilled. Re-running 2026-06-01 today builds it against *today's* cutoff, so
the output differs from the original run. Numbers change with no code change and
no explanation.

And it's proven, not asserted:

```bash
make backfill-proof START=2026-06-01 END=2026-06-30
```

```
==> 1/4  Checksumming fct_orders over 2026-06-01 .. 2026-06-30
    1,409 rows, sha256 1ec442cc57d6f229...
==> 2/4  Deleting the range from the mart
==> 3/4  Re-running with DBT_RUN_AS_OF pinned
==> 4/4  Re-checksumming
    1,409 rows, sha256 1ec442cc57d6f229...

PASSED: backfill reproduced the range exactly.
```

The checksum deliberately **excludes `_ingested_at`** — that's pipeline metadata
rather than a business fact, and it legitimately differs if raw was re-ingested.
Everything a consumer can read is included.

## Dataset triggers

```python
schedule=[Dataset("s3://ledger-raw/orders"), Dataset("s3://ledger-raw/payments"), ...]
```

`transform_dag` runs when `ingest_dag` **publishes** those datasets — not on a
blind timer hoping data arrived.

```python
@task(task_id="publish_datasets", outlets=list(RAW_DATASETS.values()))
def publish_datasets(manifest): ...
```

Placed **last**, after verification, so a failed ingestion cannot trigger a
transform.

## Task ordering that carries meaning

```python
assert_deps() >> seed >> snapshot >> run_staging >> run_intermediate
             >> run_marts >> test >> publish_docs() >> emit_mart_dataset()
```

**Snapshot before run** is load-bearing.

The snapshot captures customer state as of *now*. `dim_customer` reads it, and
`fct_orders` resolves its as-of key against `dim_customer`. Snapshot *after* the
models and today's facts are attributed against yesterday's dimension versions —
every customer who changed country today is silently attributed to their old
country for a full day.

**Tests last, and not `trigger_rule=all_done`.** If a model failed to build, its
tests are meaningless — running them anyway produces a wall of "relation does not
exist" errors that buries the one real failure.

**Docs only on success.** Publishing docs for a broken build puts a browsable,
authoritative-looking lineage graph in front of people describing a warehouse
that didn't build.

## Retries and their ceiling

```python
"retries": 3,
"retry_delay": timedelta(minutes=5),
"retry_exponential_backoff": True,
"max_retry_delay": timedelta(minutes=20),   # ← the important one
```

Without `max_retry_delay`, doubling from 5 minutes reaches 40 by the third
retry — and the DAG blows through its 90-minute SLA while technically still
"running".

Sensors deliberately get **fewer** retries:

```python
retries=1,
```

A sensor that has already polled for thirty minutes has established that the
thing isn't coming. Retrying three more times turns a 30-minute failure into a
two-hour one and delays the alert by exactly as long.

The integrity test encodes that judgement:

```python
operator = getattr(task, "operator_name", None) or type(task).__name__
is_sensor = "sensor" in operator.lower() or "sensor" in task.task_id
minimum = 1 if is_sensor else 2
```

(`type(task).__name__` is `"MappedOperator"` for anything built with `.expand()`,
which hides the real class — a small trap worth knowing.)

## SLA with a callback that actually fires

```python
default_args={**DEFAULT_ARGS, "sla": timedelta(minutes=90)},
sla_miss_callback=alert_on_sla_miss,
```

> **An SLA with no callback is a comment, not a commitment.**

One trap: the SLA callback's signature is **positional and fixed** by Airflow —
`(dag, task_list, blocking_task_list, slas, blocking_tis)` — *not* the
single-`context` shape of the other callbacks. Get it wrong and the callback is
silently never registered.

## Dynamic task mapping

```python
verify = PythonSensor.partial(
    task_id="verify_partition_written",
    python_callable=_partition_exists,
    ...
).expand(op_kwargs=partitions)
```

One sensor per table, generated **at runtime** from an upstream task's result.

**Why not a Python `for` loop over a hard-coded list?** Because that bakes the
table list into the DAG *structure* at parse time. Adding a table becomes a code
change plus a redeploy, and the DAG can't react to what the upstream task
actually found.

## No credentials in DAG files

```python
def warehouse_env() -> dict[str, str]:
    return {
        "DBT_TARGET": Variable.get("dbt_target", default_var="dev"),
        "SINK_S3_SECRET_KEY": Variable.get("sink_s3_secret_key", ...),
    }
```

Everything through Airflow Variables, populated from the environment.

Enforced:

```python
def test_no_credentials_are_literal_in_dag_files():
    pattern = re.compile(
        r"(password|secret|api[_-]?key|token|dsn)\s*[:=]\s*[\"'][^\"'{}\s]{8,}[\"']",
        re.IGNORECASE)
```

Deliberately not a generic entropy check — those produce false positives on
every hash in a comment, get muted, and then catch nothing.

## Task idempotency

Every task must survive being cleared and re-run. Concretely:

```python
key = f"_manifest/ingest/{datetime.now(timezone.utc):%Y-%m-%dT%H}.json"
store.write(key, ...)
```

A **deterministic** key, so re-running overwrites rather than appends.

## Lag polling done right

```python
now = time.monotonic()
if now - self._last_lag_poll < self.settings.lag_poll_seconds:
    return
```

`get_watermark_offsets` is a broker round-trip. Calling it per message turns a
metric into a bottleneck. Polled on an interval instead.

Similarly, the lag probe uses a throwaway consumer that never *subscribes* — it
reads the group's committed offsets without joining the group, because joining
triggers a rebalance and an hourly rebalance stalls the real sink for seconds at
a time.

## The 17 integrity tests

They run in CI on every PR and turn the spec's non-negotiables into mechanical
checks:

- every DAG imports (a DAG that fails to import shows in Airflow as **absent**,
  not as an error — the most confusing failure the scheduler has)
- no cycles
- no `sleep()`
- every task has retries, backoff ceiling, execution timeout
- `catchup` correct per DAG
- `max_active_runs=1` where state is shared
- no literal credentials
- dynamic mapping used
- every DAG documented and tagged
- SLA callback registered

## Try it

```bash
cd orchestration && pytest tests/ -v          # 17 tests
make prove-catchup                            # clear a week, watch it backfill
make test-sla                                 # force an SLA miss
```

---

Next: **[Chapter 17 — Serving](17-serving.md)**
