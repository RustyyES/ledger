# Build schedule

The commit history is spaced to reflect **estimated effort**, not the wall-clock
time it actually took to produce this repository. If you rebuild the project
yourself, this is a pacing reference: roughly where you should be, and roughly
how long each phase takes at a part-time study pace.

The assumption is ~2.5h on a weeknight and ~5h at a weekend — about 13 hours a
week. That turns the spec's ~130 hours into ten calendar weeks.

| Phase | Days | Est. hours | What lands |
|---|---|---|---|
| Foundations | 1–5 | 8 | compose, Postgres with logical replication, Makefile |
| Commerce API | 5–14 | 26 | 15 endpoints, idempotency, migrations, 65 tests |
| Load generator | 15–20 | 16 | behavioural profile, backfill, live mode, 32 tests |
| CDC + sink | 22–30 | 22 | schema guard, Parquet writer, replay proof, 48 tests |
| dbt modelling | 32–55 | 40 | 20 models, SCD2, the lookback, 174 tests |
| Orchestration | 57–61 | 12 | three DAGs, 17 integrity tests |
| Serving | 63–69 | 14 | metrics API, dashboard, full compose |
| Proofs, CI, docs | 71–79 | 16 | three proofs, CI pipeline, DESIGN/INCIDENTS |

**Total: ~154 hours over 11 weeks.**

That is above the spec's 130-hour estimate, and deliberately so — the spec
costs the code, not the debugging. Roughly one commit in seven here is a bug
fix, and those cost more than the feature that caused them.

## Where the time actually goes

The phases are not evenly hard.

**dbt is 40 hours and it is the one that will hurt.** Not because the SQL is
long — most models are under 100 lines — but because the bugs are silent. You
will spend an afternoon on `fct_mrr_daily` returning a number that is wrong by
87% with every test passing. Budget for that. It is the point of the exercise.

**The commerce API is 26 hours and it is the easy 26.** It is ordinary web
development. If you are behind here, you will be much further behind later.

**The CDC phase looks scary and isn't.** Most of the 22 hours is reading
documentation about replication slots. The code is not hard.

## Checkpoints

If you are pacing against this, the useful checks:

- **End of week 2** — `make backfill` produces data and `make messiness` passes.
  If not, everything downstream is untested.
- **End of week 4** — a row inserted through the API appears in Parquet within
  60 seconds.
- **End of week 7** — `dbt build` is green *and* you can explain why the
  incremental filter uses `_ingested_at`. The second half matters more.
- **End of week 10** — `make proofs` passes.

## If you fall behind

Cut in this order, and know what you are giving up:

1. **The dashboard** (~6h). Costs you nothing conceptually.
2. **Prometheus and the alert rules** (~4h). Same.
3. **The metrics API** (~14h). It is ordinary web development again; you have
   already demonstrated that in phase 2.
4. **Airflow** (~12h). Painful to cut — orchestration is a real skill — but you
   can run dbt from a shell script and still learn the modelling.

Do **not** cut: the load generator's behavioural shape, the schema guard, SCD2,
or the incremental lookback. Those four are the project.
