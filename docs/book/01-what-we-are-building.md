# Chapter 1 — What we're actually building

Let's start with no jargon at all.

## A company, described plainly

Imagine a small software business. Customers sign up, pick a monthly plan
(\$19, \$49 or \$199), and get billed every month. They can upgrade, downgrade,
pause, or cancel. They also buy one-off things. Sometimes a payment fails.
Sometimes they ask for a refund, and the refund arrives a week or two after the
original payment.

Two completely different kinds of software exist inside that business.

**The first kind runs the business.** When a customer clicks "subscribe", some
code writes a row into a database. When they pay, another row. This software
cares about *one customer at a time*, and it cares about being fast and correct
right now. Nobody asks it "what was our revenue last March?" — it wouldn't know
how to answer, and asking would slow down the checkout page for everyone else.

This is called an **OLTP** system. *Online Transaction Processing.* You can
ignore the acronym; just remember: **the app that runs the business.**

**The second kind answers questions about the business.** "What's our monthly
recurring revenue?" "Do customers who sign up in December stay longer than
customers who sign up in June?" "Which country is growing fastest?" This
software cares about *millions of rows at a time* and doesn't mind taking ten
seconds to answer.

This is called an **OLAP** system, or more usefully, **the warehouse**.

## The gap between them

Here's the whole problem in one sentence:

> The data lives in the first system, and the questions get asked of the second.

So something has to move data from one to the other. That something is a **data
pipeline**, and building one is what this project is about.

That sounds simple. It is not, and the reason it isn't is the subject of this
book.

## Why it isn't simple

The app was not built for you. It was built to run the business. It makes
choices that are perfectly sensible for its job and actively hostile to yours:

- It **overwrites** data. When a customer moves from Egypt to Germany, the app
  changes `country_code` from `EG` to `DE`. The old value is gone. Forever. But
  your revenue-by-country report for *last year* should still say Egypt — the
  money really was earned while they lived there.

- It **doesn't delete things properly.** Deleting a customer row would break
  every order that points at it, so the app sets a `deleted_at` timestamp and
  leaves the row there. Now every single query you write has to remember that.

- It **carries scars.** Two years ago someone migrated the order statuses and
  didn't finish. So `orders.status` contains `paid`, `PAID`, `complete` *and*
  `completed`, all meaning the same thing. Nobody will fix it, because fixing it
  risks breaking the checkout page and the checkout page makes money.

- Its **data arrives out of order.** A refund issued today reverses a payment
  from twelve days ago. The refund is new; the thing it changes is old.

- Its **schema changes without telling you.** A developer adds a column on
  Tuesday. Nobody informs the data team, because in most companies nobody knows
  the data team needs to be informed.

Every one of those is a real problem with a real solution, and each solution has
a way of being subtly wrong.

## What "Ledger" is

Ledger is a complete, working version of both halves:

```
  ┌────────────────────────────────────┐
  │  THE APP THAT RUNS THE BUSINESS    │
  │                                    │
  │  a web API + a Postgres database   │
  │  + a robot pretending to be        │
  │    thousands of customers          │
  └────────────────┬───────────────────┘
                   │
                   │  ← the pipeline lives here
                   │
  ┌────────────────▼───────────────────┐
  │  THE WAREHOUSE                     │
  │                                    │
  │  organised for questions,          │
  │  not for transactions              │
  └────────────────┬───────────────────┘
                   │
  ┌────────────────▼───────────────────┐
  │  AN API THAT ANSWERS QUESTIONS     │
  └────────────────────────────────────┘
```

Most tutorials skip the top box. They hand you a CSV file and say "load this
into a warehouse". That skips the entire interesting part, because *every*
problem listed above comes from the top box being a live system rather than a
frozen file.

So we built the top box too. That is the single most important decision in this
project, and Chapter 3 is about why.

## The five problems, and where they're solved

Everything in this book comes back to five properties of the source data. They
are **deliberate**. The data generator is written to produce them on purpose,
because a pipeline that has never met them is a pipeline that hasn't been
tested.

| # | The problem | Where it's solved |
|---|---|---|
| 1 | Refunds arrive up to two weeks late | Ch. 13 — *the hardest chapter* |
| 2 | Customers relocate; history must not follow them | Ch. 12 |
| 3 | `status` is `paid` / `PAID` / `complete` / `completed` | Ch. 10 |
| 4 | 15% of orders have no timestamp, just local wall-clock text | Ch. 10 |
| 5 | The schema changes while the pipeline is running | Ch. 9 |

If you only ever remember one of these, make it **number 1**. It is the one
that breaks pipelines silently, it's the one interviewers ask about, and it is
the reason half the design decisions in this project are what they are.

## What "done" looks like

You can run one command:

```bash
make up
```

Twenty minutes later, on a laptop, you have:

- a commerce API with fake customers continuously buying things
- their every change streaming into a warehouse within a minute
- a dimensional model rebuilt on a schedule
- an API that returns monthly recurring revenue
- and — the part that matters — **that MRR figure reconciles, to within half a
  percent, against a completely independent calculation from the payments
  table.**

That last point is the entire game. Anyone can move data. Moving it and then
*proving* the result is right is the job.

## The shape of the rest of this book

- **Part I** (Ch. 1–3): the ideas. No code yet.
- **Part II** (Ch. 4–6): building the source system.
- **Part III** (Ch. 7–9): getting data out of it without losing any.
- **Part IV** (Ch. 10–15): turning raw data into answers. The longest part.
- **Part V** (Ch. 16–17): running it on a schedule and serving it.
- **Part VI** (Ch. 18–21): judgement. Bugs, trade-offs, and what breaks at scale.

---

**Try it now**, if the stack is up — this is the whole project in one command:

```bash
curl -H "X-API-Key: dev-key-change-me" \
     'localhost:8001/metrics/mrr?granularity=month' | jq '.points[-1]'
```

That number travelled from a `POST /orders` call, through a database's
write-ahead log, through Kafka, into Parquet files, through five layers of SQL,
and back out as JSON. The next twenty chapters are about every step of that
journey and why each one is shaped the way it is.

---

Next: **[Chapter 2 — What a pipeline actually is](02-what-a-pipeline-is.md)**
