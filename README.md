# Ledger

**A production ELT platform with a live transactional system underneath it.**

Most portfolio data pipelines load a static CSV into a warehouse. That skips the
single most important boundary in the job — the one between a running OLTP
system and the analytics layer that reads from it. Every real problem a data
engineer has originates at that boundary: schemas change without warning, rows
get soft-deleted, refunds arrive three days after the order they reverse,
timestamps come in three timezones.

Ledger puts a real commerce application underneath the pipeline so those
problems are real rather than simulated. A FastAPI service writes to Postgres,
Debezium streams the WAL into Kafka, a hand-written sink lands Parquet, dbt
builds a dimensional model on top, Airflow orchestrates it, and a metrics API
serves MRR that reconciles back to the payments table.

```
make up
```

Twenty minutes later: a commerce API generating load, a warehouse being
populated by CDC, dbt models building on a schedule, and MRR you can query.

<!-- Screenshot: the quality dashboard at http://localhost:8501 after ~72h of
     accumulated history. Take it once the stack has been running long enough
     for the freshness and pass-rate panels to have a real series. -->

---

## What is actually here

```
┌─────────────────────────────────────────────────────────────┐
│  SOURCE SYSTEM                                              │
│  FastAPI commerce service ──► Postgres 16 (OLTP)            │
│  load generator ──► HTTP against the API                    │
└──────────────────────────────┬──────────────────────────────┘
                               │ logical replication (pgoutput)
                               ▼
┌─────────────────────────────────────────────────────────────┐
│  INGESTION                                                  │
│  Debezium ──► Redpanda ──► Python sink ──► Parquet on MinIO │
│  partitioned by _ingested_date, zstd, byte-identical replay │
└──────────────────────────────┬──────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────┐
│  WAREHOUSE                                                  │
│  DuckDB (local) / Snowflake (--target prod)                 │
│  raw ──► staging ──► intermediate ──► marts        [dbt]    │
└──────────────────────────────┬──────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────┐
│  SERVING                                                    │
│  FastAPI metrics API ──► cached mart queries                │
│  Streamlit quality dashboard · Prometheus + alert rules     │
└─────────────────────────────────────────────────────────────┘

Airflow runs three DAGs: ingest (hourly), transform (daily, dataset-triggered),
quality (every 4 hours).
```

| | |
|---|---|
| **Services** | 4 (commerce API, load generator, CDC sink, metrics API) |
| **dbt project** | 20 models · 1 snapshot · 2 seeds · 7 sources · **174 tests** |
| **of which** | 13 singular business-rule tests, 161 generic |
| **Python tests** | 65 commerce API · 48 CDC sink · 43 metrics API · 32 load generator · 17 DAG integrity |
| **Endpoints** | 15 transactional · 6 analytical |
| **Coverage** | 94% overall on the commerce API, 96–100% on every router |

---

## The five problems this exists to solve

Each one is a deliberate property of the source data, not an accident, and each
forces a real modelling decision. `make messiness` verifies they are all still
present — if the generator stops producing one, the test covering it becomes
vacuous.

### 1. Refunds arrive up to two weeks late

The single most instructive constraint in the project. An incremental model
keyed on the business timestamp never re-selects a payment whose refund landed
twelve days later, so its refund total stays wrong **forever** — with no error,
no null, and no failing test.

The fix is a lookback keyed on `_ingested_at` (when the record reached the
*pipeline*), not on `processed_at`. → [`fct_payments`](transform/models/marts/finance/fct_payments.sql),
[DESIGN §2](DESIGN.md#2-the-incremental-lookback-is-21-days-and-it-was-15-first)

```bash
make prove-lookback     # issues a 12-day-late refund, proves it lands
```

### 2. Customers relocate, and history must not follow them

A customer in Egypt moves to Germany. Without SCD2, every order they ever placed
is retroactively attributed to Germany, and last year's revenue-by-country
report silently changes. → [`dim_customer`](transform/models/marts/core/dim_customer.sql),
[DESIGN §3](DESIGN.md#3-scd-type-2-on-dim_customer)

### 3. `orders.status` is `paid`, `PAID`, `complete` and `completed`

The residue of an unfinished data migration. Normalised in exactly one place,
with an `accepted_values` test that fails if a sixth spelling ever appears.
→ [`normalise_order_status`](transform/macros/business_logic.sql)

### 4. 15% of orders have no `placed_at`

A legacy mobile client writes naive local wall-clock into `placed_at_local` with
no offset. Resolving it needs the customer's IANA timezone — which is a join,
which is why it happens in the intermediate layer and not in staging.
→ [`int_orders__resolved`](transform/models/intermediate/int_orders__resolved.sql)

### 5. The source schema changes while the pipeline is running

```bash
make schema-change      # applies migration 0003 against the LIVE database
```

The sink's schema guard classifies it as ADDITIVE, accepts it, and audits it.
An *incompatible* change (a narrowing, a type flip) rejects the batch, diverts
it to a DLQ, and halts **that table's** consumer only — not the sink.
→ [`schema_guard.py`](services/cdc-sink/schema_guard.py), [DESIGN §8](DESIGN.md#8-the-schema-guard-rejects-rather-than-coerces-and-halts-one-table)

---

## Claims, and the commands that verify them

Nothing here is asserted without a way to check it.

| Claim | Command | Evidence |
|---|---|---|
| Replaying a partition is byte-identical | `make prove-idempotency` | [`results/idempotency_proof.json`](results/idempotency_proof.json) |
| A 12-day-late refund still lands | `make prove-lookback` | [`results/lookback_proof.json`](results/lookback_proof.json) |
| A backfill reproduces a range exactly | `make backfill-proof` | [`results/backfill_proof.json`](results/backfill_proof.json) |
| A SIGKILL loses nothing and duplicates nothing | `make chaos-kill-sink` | — |
| `transform_dag` catches up correctly | `make prove-catchup` | — |
| The SLA callback actually fires | `make test-sla` | — |
| A row reaches Parquet within 60s | `make verify-cdc` | — |
| All six messiness patterns present | `make messiness` | — |

---

## Quick tour

```bash
make up                     # everything, ~20 min (most of it the backfill)
make urls                   # where each service lives

# Query the warehouse through the API
curl -H "X-API-Key: dev-key-change-me" \
     'localhost:8001/metrics/mrr?granularity=month' | jq

# Watch CDC work
make verify-cdc             # insert a row, find it in Parquet
make lag                    # consumer lag

# Break something on purpose
make schema-change          # migrate the source underneath the pipeline
make chaos-kill-sink        # SIGKILL the sink mid-batch

make test                   # every suite
make proofs                 # every proof
```

Scale is a knob. `make up SCALE=1.0` produces the full 5M orders / 500k
customers over 18 months; the default 0.02 keeps the same *shape* — weekend
dips, a bimodal daily curve, a Black Friday spike, cohort churn — in about 90
seconds.

---

## The book

There is a complete, ground-up explanation of this project in
**[`docs/book/`](docs/book/README.md)** — 21 chapters, ~5,700 lines, written for
someone who knows Python and SQL and has never built a pipeline.

It explains every decision, every rejected alternative, and every bug. Read it
[as a single page](https://claude.ai/code/artifact/798a5711-8d5a-48bf-b353-bd779c4b9c8e),
start with the [table of contents](docs/book/README.md), or jump to the three
chapters that matter most:

- **[Ch. 13 — The late-arriving fact](docs/book/13-late-arriving-facts.md)** —
  why the obvious incremental filter is permanently, silently wrong
- **[Ch. 18 — Ten bugs](docs/book/18-ten-bugs.md)** — ten real defects, zero of
  which raised an exception
- **[Ch. 19 — Jenga](docs/book/19-jenga.md)** — what breaks if you remove each
  piece, and how long until you would notice

---

## Reading this repository

If you have twenty minutes and want the parts that matter:

1. **[`DESIGN.md`](DESIGN.md)** — sixteen decisions, each with the alternative
   that was rejected and what it costs to be wrong. §2 (the lookback), §3
   (SCD2) and §13 (the two MRR tolerances) are the load-bearing ones.
2. **[`INCIDENTS.md`](INCIDENTS.md)** — ten real failures hit while building
   this. Every one of them produced a *wrong number* rather than an error.
3. **[`fct_payments.sql`](transform/models/marts/finance/fct_payments.sql)** —
   the hardest model, with the wrong version written out in the header so the
   difference is visible.
4. **[`schema_guard.py`](services/cdc-sink/schema_guard.py)** — the four
   change classes and why the response to each is what it is.
5. **[`storage.py`](services/cdc-sink/storage.py)** — the four conditions that
   have to hold for byte-identical replay, three of which are easy to miss.

---

## Layout

```
ledger/
├── services/
│   ├── commerce-api/     FastAPI OLTP service · 15 endpoints · idempotency · Alembic
│   ├── loadgen/          behaviour simulator (backfill + live) · not a data faker
│   ├── cdc-sink/         Kafka → Parquet · schema guard · byte-identical replay
│   └── metrics-api/      read-only serving · auth · TTL cache · cursor pagination
├── transform/            dbt: staging → intermediate → marts, 171 tests
├── orchestration/        3 Airflow DAGs + integrity tests
├── infra/                Debezium, Postgres init, Prometheus, dashboard
├── scripts/              CDC setup + the proof scripts
├── results/              committed evidence for the claims above
├── DESIGN.md             why every decision is what it is
└── INCIDENTS.md          what went wrong and what changed
```

---

## Requirements

Docker with Compose v2, ~10GB free disk, ~6GB RAM. `make preflight` checks.

Everything else runs in containers. Local Python is only needed to run the test
suites outside Docker.

---

## Configuration

Copy `.env.example` to `.env`. Every credential is environment-driven; nothing
is hard-coded, and `test_no_credentials_are_literal_in_dag_files` enforces that
for the DAGs.

The one exception is `infra/debezium/secrets.properties`, which is committed
with development credentials. It exists so the connector config can reference
`${file:...}` instead of inlining a password — an inline password is written
into Kafka Connect's own config topic in plaintext, permanently. In a real
deployment that file is rendered from a secret manager at deploy time.

---

## About this repository

This is a **reference implementation** — a worked example of the design in
`LEDGER_SPEC.md`, built to be read and studied rather than deployed. It was
produced with AI assistance and is published as a learning reference.

The parts worth your time are the ones that carry judgement rather than code:
[`DESIGN.md`](DESIGN.md) for why each decision went the way it did,
[`INCIDENTS.md`](INCIDENTS.md) for the ten bugs found while building it, and
[`docs/book/`](docs/book/README.md) for the full explanation.

[`docs/SCHEDULE.md`](docs/SCHEDULE.md) breaks the commit history into a
phase-by-phase effort estimate, if you want to build it yourself.

## Status

Code-complete and verified: every test suite passes, every proof runs, the dbt
project builds green from scratch and incrementally.

Two exit criteria are wall-clock rather than code and are yours to run:

- **72 hours of continuously green DAGs.** The DAGs are correct and their
  integrity tests pass; nobody has yet left them running for three days.
- **p95 < 200ms under sustained load.** Measured at ~2ms on cached endpoints
  against a warehouse of this size, but not under a real concurrent load test.
