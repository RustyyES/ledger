# The Ledger Book

A complete explanation of one data pipeline — every decision, every rejected
alternative, and every bug.

Written for someone who knows Python and SQL and has never built a pipeline.
Starts from "what is a data pipeline" and ends at "what breaks at 100x".

**~29,600 words. Roughly two and a half hours cover to cover.**

📖 **[Read it as a single page](https://claude.ai/code/artifact/798a5711-8d5a-48bf-b353-bd779c4b9c8e)** — same content, rendered with a chapter rail and light/dark themes.

Regenerate that page after editing any chapter:

```bash
python docs/book/build_html.py
```

---

## Table of contents

### Front matter
- **[Preface — how to read this](00-preface.md)** · reading orders, conventions

### Part I — Foundations
*No code. The ideas and the vocabulary.*

1. **[What we're actually building](01-what-we-are-building.md)**
   OLTP vs warehouse · the five problems · what "done" looks like
2. **[What a pipeline actually is](02-what-a-pipeline-is.md)**
   ETL vs ELT · the four layers · facts and dimensions
3. **[Why we built the app first](03-why-build-the-app.md)**
   The decision that shapes everything else

### Part II — The source system
4. **[A schema that's realistically awkward](04-the-schema.md)**
   Seven tables · six deliberate messes · why money is an integer
5. **[Idempotency, or how a retry doubles your revenue](05-idempotency.md)**
   The key, the canonical hash, and the race resolved at COMMIT
6. **[Generating data that has a shape](06-the-load-generator.md)**
   Faker vs simulator · why flat data hollows out your test suite

### Part III — Getting data out
7. **[Change data capture from first principles](07-cdc.md)**
   Four approaches, three of them bad · the slot that fills your disk
8. **[The sink, and what "exactly once" really means](08-the-sink.md)**
   Write-then-commit · byte-identical replay in four conditions
9. **[Surviving a schema change](09-schema-guard.md)**
   Four change classes · why halting one table beats halting the sink

### Part IV — Modelling
*The longest part.*

10. **[Staging, and why the layer contract matters](10-staging.md)**
    dbt in two paragraphs · normalising the mess · where the spec was wrong
11. **[Star schemas from scratch](11-star-schemas.md)**
    Grain · surrogate keys · why `dim_date` is generated
12. **[Slowly changing dimensions](12-scd2.md)**
    The time-travel problem · the bug that orphaned 22% of orders
13. **[The late-arriving fact](13-late-arriving-facts.md)** ⭐
    **The most important chapter.** Why the obvious incremental filter is
    permanently, silently wrong
14. **[Three metrics that need real SQL](14-three-metrics.md)**
    MRR proration · right-censored cohorts · the gap-fill trap
15. **[Testing data (which is not testing code)](15-testing-data.md)**
    Why schema tests can't find wrongness · tolerances and the temptation

### Part V — Running it
16. **[Orchestration](16-orchestration.md)**
    No `sleep()` · catchup · reproducible backfills
17. **[Serving](17-serving.md)**
    401 vs 403 · cursor pagination · the cardinality trap

### Part VI — Judgement
18. **[Ten bugs](18-ten-bugs.md)** ⭐
    Ten defects. Zero exceptions. Four patterns.
19. **[Jenga: what breaks if you pull this out](19-jenga.md)** ⭐
    Every component, what its absence costs, and how long until you'd notice
20. **[Why this tool and not that one](20-tool-choices.md)**
    Every choice, every alternative, and when the alternative is right
21. **[At 100x](21-at-100x.md)**
    What breaks, in order · and what doesn't

---

## Suggested reading orders

**Two hours** — Chapters 1, 2, 13, 18.
Chapter 13 is the heart of the project; Chapter 18 is the evidence for why it's
shaped that way.

**Learning the craft** — straight through. After each part, build that part
yourself before reading the next one, then diff. Slow, and the only way it
sticks.

**Evaluating the ideas for work** — Chapters 19, 20, 21.
What each piece protects you from, every tool choice against its alternatives,
and what stops working at scale.

**Preparing for an interview** — Chapters 13, 18, 19.
These three are where the questions come from.

---

## The one idea

> **Bugs in this project that raised an exception: 0.**
> **Bugs that produced a wrong number: 10.**

In application code a bug announces itself — a stack trace, a 500, a red test.
In data work the common bug is a `LEFT JOIN` that silently drops 22% of your
rows, or an incremental filter that never re-reads a row it needed to update.

Everything runs. The dashboard renders. The number is just wrong, and stays
wrong until somebody reconciles by hand months later.

Every technique in this book exists to make that failure mode visible.
