# Chapter 2 — What a pipeline actually is

Still no code. This chapter builds the vocabulary you need for everything else.

## The simplest possible pipeline

Here's a data pipeline:

```python
rows = source_database.query("SELECT * FROM orders")
warehouse.insert("orders", rows)
```

That's it. Genuinely. Read from one place, write to another. Everything in this
project is an answer to the question *"and what happens when that isn't good
enough?"*

Let's break it, one step at a time.

## Break 1: the table is too big

`SELECT * FROM orders` on five million rows takes minutes, holds a database
connection open, and competes with the checkout page for resources. Run it
every hour and you have a self-inflicted performance problem.

**The obvious fix:** only fetch what's new.

```python
rows = source.query("SELECT * FROM orders WHERE updated_at > :last_run")
```

This is called an **incremental load**, and it is where most of the interesting
bugs in this book live. Keep this line in mind — we will come back to it in
Chapter 13 and discover it is subtly, silently wrong.

## Break 2: rows change after you've copied them

You copy an order on Monday with status `pending`. On Tuesday it becomes
`completed`. Your warehouse still says `pending`.

The `updated_at` filter above handles this — as long as the app faithfully
updates `updated_at` on every single change. Chapter 12 shows what happens when
it doesn't.

## Break 3: rows get deleted

A row disappears from the source. Your `WHERE updated_at > ...` query will never
tell you, because you can't select a row that isn't there. Your warehouse now
contains a customer that no longer exists, and no query you write will ever
notice.

This is the first genuinely hard problem, and it's what motivates the technology
in Chapter 7.

## Break 4: you need history the source doesn't keep

The customer moves from Egypt to Germany. The source has one `country_code`
column; it now says `DE`. Egypt is gone.

No amount of clever querying recovers it. If you didn't capture the change *as
it happened*, that fact is lost. This is Chapter 12.

## ETL vs ELT — a distinction worth two minutes

You'll see both. The letters are Extract, Transform, Load, in different orders.

**ETL** — transform the data *before* it lands in the warehouse:

```
source ──► [clean it up in Python] ──► warehouse
```

**ELT** — land the raw data first, transform it *inside* the warehouse:

```
source ──► warehouse (raw) ──► [clean it up in SQL] ──► warehouse (clean)
```

Ledger is **ELT**, and here's the practical reason:

> In ETL, if your transformation has a bug, the bad data is all you have. The
> original is gone. You have to re-extract from the source — which may have
> changed, or may not let you go back that far.
>
> In ELT, the raw data is still sitting there. Fix the SQL, re-run, done.

The raw layer in this project is *append-only and never modified*. It's the
receipts. Everything downstream can be rebuilt from it at any time. That
property is worth its storage cost many times over, and it's the reason
Chapter 11's rebuild-from-scratch is a five-minute operation rather than a
week-long re-extraction.

There's a second reason: modern warehouses are very good at SQL and there are
far more people who can read SQL than can read someone else's Python
transformation script. Putting the business logic in SQL, in version control,
with tests, makes it reviewable by the people who actually understand the
business.

## The four layers

Ledger's data moves through four named stages. You'll see these words
constantly.

```
   raw   ──►   staging   ──►   intermediate   ──►   marts
```

**raw** — exactly what the source said, unmodified. Ugly. `paid` and `PAID` both
present. Deleted rows still here. *Never edited.*

**staging** — one model per source table. Rename columns to business names, fix
types, normalise the mess. **No joins.** One staging model = one source table,
always. This rule is load-bearing and Chapter 10 explains why.

**intermediate** — where joins and shared logic live. Not for public
consumption; nothing outside the project should read these.

**marts** — the finished product. This is what people query. It is a *public
contract*: once someone builds a dashboard on `fct_orders`, changing its shape
breaks their work.

Why bother with four layers instead of one big query? Three reasons, in order of
how much they'll matter to you:

1. **Debugging.** When a number is wrong, you can look at each layer and ask
   "was it right here?" With one 400-line query you can only ask "is it wrong?"
2. **Reuse.** Fifteen models need "the cleaned-up order status". Defining it once
   means it can't drift.
3. **Testing.** You can test each layer's assumptions separately.

## Facts and dimensions

One more pair of words and we're done with vocabulary.

Warehouse tables come in two flavours.

**A fact table** records *things that happened*. One row per event. They're long
and thin, they grow forever, and they're mostly numbers and foreign keys.

> `fct_orders` — one row per order.
> `fct_payments` — one row per payment.

**A dimension table** records *things that are*. One row per entity. They're
short and wide, they grow slowly, and they're mostly descriptive text.

> `dim_customer` — one row per customer.
> `dim_plan` — one row per plan.
> `dim_date` — one row per calendar day.

The naming convention (`fct_` / `dim_`) is universal enough that using it makes
your project immediately legible to anyone who's worked in this field.

Why split them at all? Because "what did we sell?" and "who did we sell it to?"
change at completely different rates. You get five thousand orders a day and
maybe three plan changes a year. Storing the plan name on every order row would
mean five thousand copies of the string `"pro"` per day — and if the plan gets
renamed, five thousand rows a day to update.

Facts point at dimensions with a **foreign key**. Chapter 11 covers this
properly.

## Where this project's pieces fit

Now the architecture diagram means something:

```
┌─────────────────────────────────────────────────────────┐
│  SOURCE                                                 │
│  FastAPI + Postgres          ← "the app that runs it"   │
│  load generator              ← a robot buying things    │
└──────────────────┬──────────────────────────────────────┘
                   │  Debezium reads the write-ahead log
                   │  Kafka carries the changes           ← Ch. 7, 8
                   ▼
┌─────────────────────────────────────────────────────────┐
│  RAW                                                    │
│  Parquet files on object storage                        │
│  append-only, never edited                    ← Ch. 8, 9│
└──────────────────┬──────────────────────────────────────┘
                   │  dbt                                 ← Ch. 10–15
                   ▼
┌─────────────────────────────────────────────────────────┐
│  staging → intermediate → marts                         │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  SERVING: a read-only HTTP API                ← Ch. 17  │
└─────────────────────────────────────────────────────────┘

        Airflow runs all of this on a schedule ← Ch. 16
```

## The one habit worth forming

Every time you meet a piece of this project, ask:

> **"What breaks if I delete this?"**

If the answer is "nothing", it shouldn't be there. If the answer is "I don't
know", you don't understand it yet.

Chapter 19 answers that question for every major component. It's the chapter I'd
read second, after Chapter 13.

---

Next: **[Chapter 3 — Why we built the app first](03-why-build-the-app.md)**
