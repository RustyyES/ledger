# Chapter 3 — Why we built the app first

This is the decision that shapes everything else, so it gets its own chapter.

## What almost every tutorial does

> "Download `orders.csv`. Load it into your warehouse. Congratulations, you've
> built a data pipeline."

You have not. You've done the easy 10%.

Here's what a static CSV file cannot ever do:

- change while you're reading it
- delete a row
- get a new column on a Tuesday
- contain a refund that arrives after you already processed the payment
- contain four spellings of the same status because of an abandoned migration
- have a customer who moved country last March

Every one of those is a normal Tuesday for a real pipeline. A CSV has none of
them, so a pipeline built against a CSV has never been tested against the things
that actually break pipelines.

## The alternative we rejected: simulate the mess

We could have generated a messy CSV. Sprinkle in some `PAID`s, add some late
refunds, done. Much less work.

We didn't, for three reasons.

**First, you only simulate the problems you already know about.** The whole
point of a learning project is meeting problems you *haven't* thought of. A
generator can only produce mess its author anticipated. A live system produces
mess by existing.

That's not theoretical here. Three of the ten bugs in Chapter 18 came from the
interaction between components — an order generated at 09:00 for a customer who
signed up at 18:00 the same day; a snapshot taken after a backfill instead of
before. No CSV would have produced either.

**Second, you can't feel the boundary from one side.** The interesting thing
about the app/warehouse boundary is that it's a *negotiation between two teams
with different incentives*. The app team wants a fast checkout. You want stable
history. Those conflict. Building both sides is the only way to feel why the app
team's perfectly reasonable choices are so annoying for you.

**Third, some problems only exist in motion.** "The schema changed while the
pipeline was running" is not a data property. It's an *event*, and you can only
have it if there's something running to have it happen to.

```bash
make schema-change   # adds a column to the live database, mid-flight
```

That command is only possible because there's a live database.

## What "the app doesn't know about you" means in practice

The commerce API in this project has **no idea a warehouse exists**. That is
enforced by omission, and it's worth listing what's absent:

- no analytics events
- no "data team" columns
- no outbox table, no change feed, no webhooks
- no batch-export endpoint

If the pipeline needs something, the pipeline takes it from the database's
write-ahead log — the same log Postgres uses for crash recovery. The app is not
consulted and does not cooperate. That's realistic: in most companies, asking
the product team to emit events for you is a quarter-long negotiation, and the
answer is often no.

There is exactly one exception, and it's honest about itself: the
`customers.timezone` column exists partly because the warehouse needs it to
resolve local timestamps. A real app would plausibly have it anyway (for sending
emails at sensible hours), which is why it was acceptable.

## What this bought us

Concretely, having a live source system is what makes these possible:

| Only possible with a live source | Command |
|---|---|
| Change the schema underneath a running pipeline | `make schema-change` |
| Kill the ingestion process mid-write | `make chaos-kill-sink` |
| Insert a row and time how long it takes to arrive | `make verify-cdc` |
| Issue a refund *today* against a payment from 12 days ago | `make prove-lookback` |

Each of those is a test you simply cannot write against a file.

## The cost, stated honestly

This decision roughly **doubled the size of the project**. The commerce API is
~3,200 lines. The load generator is another ~2,000. That's a third of the
codebase spent on something that is not, strictly speaking, the data pipeline.

If your goal is to learn dbt specifically, that's a bad trade — go do the CSV
tutorial. If your goal is to understand why pipelines are hard, it's the only
trade worth making.

## What breaks if you remove it

Swap the live source for a static file and here's what silently stops being
tested:

- **The CDC layer becomes pointless.** No changes to capture. Chapters 7–9 evaporate.
- **SCD2 becomes decoration.** Nobody ever relocates, so every customer has one
  version and the whole time-travel apparatus proves nothing. (Ch. 12)
- **The incremental lookback is untested.** All the data arrives at once, so
  nothing is ever late. This is the big one — Chapter 13's entire subject
  disappears. (Ch. 13)
- **The schema guard never fires.** A file's schema doesn't change. (Ch. 9)
- **Crash recovery is untestable.** There's no process to kill. (Ch. 8)

Roughly half this book requires the source to be alive.

## The general principle

> **Build the thing that produces your problem, not just the thing that solves it.**

This generalises past data engineering. If you're writing a rate limiter, write
the client that hammers it. If you're writing a cache, write the workload that
thrashes it. You will find bugs in the interaction that you would never find in
either piece alone.

---

Part I is done. You now have the vocabulary and the reasoning. Part II builds
the source system.

Next: **[Chapter 4 — A schema that's realistically awkward](04-the-schema.md)**
