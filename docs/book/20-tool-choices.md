# Chapter 20 — Why this tool and not that one

Every technology choice, the alternatives, and when the alternative is right.

The most useful column is the last one. A tool choice with no "when the other is
better" is a preference, not a decision.

---

## Postgres, not MySQL / MongoDB

**Chose:** PostgreSQL 16.

**Why:** logical replication is first-class and well-documented, and Debezium's
Postgres connector is the most mature of the set. `REPLICA IDENTITY FULL` gives
before-images for free.

**MySQL** would have worked — binlog-based CDC is arguably *simpler* than
Postgres's replication slots, and there's no slot-fills-the-disk failure mode.
Postgres wins on the richer type system and better JSON support.

**MongoDB** would have been actively worse for teaching: schema-per-document
means "the schema changed" is not an event, so Chapter 9 evaporates.

**When MySQL is better:** if you already run it. The slot management in Chapter 7
is a genuine operational burden that binlog CDC doesn't have.

---

## FastAPI, not Django / Flask

**Chose:** FastAPI.

**Why:** Pydantic validation is declarative and produces good errors, OpenAPI
docs come free, and async matters for the load generator's concurrency.

**Django** brings an ORM, admin, migrations and auth in one — genuinely more
productive for a real commerce app. It's heavier than needed for 15 endpoints,
and its ORM is more opinionated than SQLAlchemy where we wanted explicit control
over the schema.

**Flask** is fine and would need Marshmallow plus hand-written OpenAPI.

**When Django is better:** an actual product. The admin alone justifies it.

---

## Debezium, not a custom WAL reader / triggers / polling

**Chose:** Debezium.

**Why:** WAL parsing is genuinely hard and Debezium has solved it — snapshot
handling, schema changes, connector restarts, offset management.

**A custom reader** using `wal2json` is educational and a permanent maintenance
burden. Not the interesting part of this project.

**Triggers** work and catch deletes, but every write becomes two writes. You've
added latency to checkout to serve analytics. The app team will say no, correctly.

**Polling `updated_at`** is what most teams actually do. It's fine until you need
deletes or intermediate states. Chapter 7 details exactly where it breaks.

**When polling is better:** genuinely, quite often. If you don't need deletes,
don't need intermediate states, and the source reliably maintains `updated_at`,
polling is simpler and has no replication slot to babysit. **Don't reach for CDC
by default** — reach for it when you can name which of those three you need.

---

## Redpanda, not Kafka / Pulsar / no queue

**Chose:** Redpanda.

**Why:** Kafka's API, one binary, no ZooKeeper/KRaft configuration. For a
single-node local stack that's a tenth of the operational weight for the same
protocol.

**Kafka** is the default in production and has a bigger ecosystem.
Redpanda speaks its protocol, so switching is a connection-string change.

**Pulsar** has genuinely nice multi-tenancy and tiered storage. Smaller
ecosystem; Debezium support is less mature.

**No queue at all** — Debezium writing files directly — is the interesting
alternative. It fails because then Debezium's failure is your failure, its retry
policy is yours, and **you cannot replay**. Everything in Chapter 8 depends on
being able to re-read from a durable log.

**When no queue is better:** low-volume, single-consumer, and you can tolerate
re-snapshotting on failure. Then a queue is a service you're running for nothing.

---

## A hand-written sink, not Kafka Connect S3

**Chose:** our own consumer.

**Why:** every interesting decision in a sink is a **policy** decision a config
file hides behind a property name — when to commit, what a delete means, what to
do about a schema change, what "idempotent" means.

**Kafka Connect** is less code and battle-tested. It also makes the schema guard
in Chapter 9 impossible without writing a custom SMT, at which point you're
writing code anyway with less control.

**When Connect is better:** when your policies are the default policies. If you
don't need custom schema handling and at-least-once with dedup is fine, use it
and save 800 lines.

---

## Parquet, not CSV / JSON / Avro / Iceberg

**Chose:** Parquet.

**Why:** columnar (read three columns of thirty and pay for three), compresses
extremely well on repetitive data, embeds its schema, and every engine reads it.

**CSV** has no types. Everything is a string. Your `amount_cents` is `"1900"` and
someone will eventually parse it as a float.

**JSON** has types but is enormous and row-oriented — you pay for every column on
every read.

**Avro** is row-oriented with excellent schema evolution. Better for the
*message* format, worse for the *storage* format, because analytical queries are
columnar.

**Iceberg / Delta Lake** add a metadata layer over Parquet giving ACID
transactions, time travel and schema evolution. **This is genuinely the better
choice at scale** and we didn't use it because it adds a concept before you
understand the problem it solves.

**When Iceberg is better:** more than one writer, or you need transactional
partition replacement, or you want time travel without keeping every version
yourself. Chapter 21 says more.

---

## MinIO, not real S3 / a local filesystem

**Chose:** MinIO locally, S3 API throughout.

**Why:** the code is identical against MinIO and AWS. `make up` needs no cloud
account and costs nothing.

**A plain filesystem** is simpler, and the sink supports it (`--local-path`, used
by CI). It doesn't teach you that object storage has no rename, no append and
eventual-consistency semantics — which shape the design.

---

## DuckDB, not Snowflake / BigQuery / Postgres-as-warehouse

**Chose:** DuckDB locally, Snowflake as a target.

**Why:** DuckDB is a single file, an embedded library, no account, no cost, and
genuinely fast on this volume. It reads Parquet directly from S3. Twenty million
rows on a laptop is comfortable.

**Snowflake / BigQuery** are what you'd use in production and are what
`--target prod` points at. Both need an account and a credit card, which would
make "clone and run" impossible.

**Postgres as a warehouse** is what many small companies do, and it works to a
surprising scale. It's row-oriented, so analytical scans are much slower, and
you're competing with your OLTP workload.

**When Snowflake is better:** the first hard wall. See Chapter 21.

---

## dbt, not raw SQL scripts / Spark / Dagster models

**Chose:** dbt.

**Why:** `ref()` gives you a dependency graph for free — build order, parallelism
and lineage all fall out of it. Tests live beside models. Everything is version
controlled SQL, reviewable by people who understand the business.

**Raw SQL scripts** work and you immediately reinvent dependency ordering,
templating and testing — badly.

**Spark** is the right answer for genuinely large data or non-SQL
transformations. It's a heavier programming model, and at this volume the JVM
startup costs more than the query.

**When Spark is better:** data that doesn't fit a warehouse, or transformations
that aren't expressible in SQL (ML feature engineering, complex text processing).

---

## Airflow, not Dagster / Prefect / cron / dbt Cloud

**Chose:** Airflow 2.

**Why:** it's the industry default, so the concepts transfer, and the operational
patterns (sensors, backfill, catchup, SLAs) are the ones you'll meet elsewhere.

**Dagster** is genuinely better designed for data specifically — assets rather
than tasks, better typing, better local development. Smaller ecosystem and fewer
people know it. Airflow's dataset-triggered scheduling (used here) is a partial
convergence on Dagster's model.

**Prefect** has the nicest Python ergonomics of the three.

**cron** genuinely suffices for a linear chain. What you lose: dependency
management, retries with backoff, backfill, visibility, concurrency control.

**dbt Cloud** would handle the transform DAG and nothing else. You'd still need
an orchestrator for ingestion and quality.

**When Dagster is better:** greenfield, and you value correctness of the model
over hiring familiarity. Honestly a defensible choice for a new team.

**When cron is better:** genuinely, if you have one linear job. Airflow is a lot
of machinery.

---

## A JSON file, not Confluent Schema Registry

**Chose:** a file on a Docker volume.

**Why:** the Registry solves a *producer coordination* problem — many producers
on one topic needing agreement. We have exactly one producer. What's actually
needed is a durable record of "what did this table look like last time", which is
a file.

**When the Registry is better:** more than one producer, or consumers in
languages that benefit from generated classes, or you want Avro schema evolution
enforced at the broker.

---

## Streamlit, not Metabase / Superset / Grafana

**Chose:** Streamlit.

**Why:** the quality dashboard is code, so it's version controlled, reviewable
and deployable with the rest. It queries DuckDB directly.

**Metabase / Superset** are better for *business* dashboards where non-engineers
build their own. This is an *operations* dashboard with a fixed set of panels
that engineers maintain.

**Grafana** is better for pure time-series and is arguably a better fit for the
freshness panels. It's a worse fit for the tabular row-count deltas and schema
change log.

**When Metabase is better:** the moment a non-engineer needs to build a chart.

---

## Prometheus, not OpenTelemetry / Datadog

**Chose:** Prometheus with a scrape endpoint per service.

**Why:** pull-based, no agent, trivially local, and the alert rules live in the
repo.

**OpenTelemetry** is where the industry is going and is better for traces. For
pure metrics it's more machinery.

**Datadog** is better in almost every way and costs money.

---

## Docker Compose, not Kubernetes

**Chose:** Compose.

**Why:** ten services on one machine. Compose expresses that in 300 readable
lines. Kubernetes would need manifests, a local cluster, and an ingress, to run
the same containers.

**When Kubernetes is better:** when you need more than one machine, or rolling
deploys, or autoscaling. None of which apply to a laptop.

The spec explicitly called this out, and it's right: reaching for K8s here adds a
week and demonstrates nothing.

---

## The meta-point

Notice how many of these say **"the alternative is better when..."** and mean it:

- polling instead of CDC — often right
- Kafka Connect instead of a custom sink — right if your policies are the defaults
- Iceberg instead of raw Parquet — right at scale
- Dagster instead of Airflow — defensible for a new team
- cron instead of an orchestrator — right for one linear job
- Metabase instead of Streamlit — right the moment non-engineers need charts

> **A technology choice that has no "when the other is better" is not a decision.
> It's a preference you haven't examined.**

The interview question isn't "why did you use Kafka?" It's **"when wouldn't
you?"** — and if you can't answer that, you didn't choose it, you defaulted to
it.

---

Next: **[Chapter 21 — At 100x](21-at-100x.md)**
