# Chapter 4 — A schema that's realistically awkward

> Source: [`services/commerce-api/app/models.py`](../../services/commerce-api/app/models.py)
> and [`alembic/versions/0001_initial_schema.py`](../../services/commerce-api/alembic/versions/0001_initial_schema.py)

## The seven tables

```
customers ──┬── subscriptions ──── subscription_events
            │        │
            └── orders ──── payments ──── refunds
                     │
                   plans
```

- **customers** — who they are
- **plans** — the price list (4 rows, changes twice a year)
- **subscriptions** — who is on which plan right now
- **subscription_events** — the *log* of every change to a subscription
- **orders** — something was bought
- **payments** — money was attempted
- **refunds** — money was given back

## Why `subscriptions` AND `subscription_events`

This looks redundant. It isn't, and understanding why is most of Chapter 14.

`subscriptions` tells you the **current state**: this customer is on `pro`
today. It's what the app needs to decide whether to let them in.

`subscription_events` tells you the **history**: created on the 3rd, upgraded on
the 19th, paused in June, resumed in July. Every row immutable; nothing is ever
updated or deleted.

Ask "what was our revenue in March?" and only the second table can answer.
`subscriptions.plan_id` knows today's plan and nothing else.

This pattern — a mutable state table beside an append-only event log — is
extremely common and worth recognising on sight. When you find both, the event
log is almost always the one you want.

## The deliberately awkward parts

This is where the schema stops being a nice clean textbook example. **Every one
of these is on purpose.**

### 1. `orders.placed_at` is nullable

```python
placed_at:       Mapped[datetime | None]  # nullable!
placed_at_local: Mapped[str | None]       # "2025-03-01 14:30:00", no timezone
```

with a constraint that at least one must be present:

```sql
CHECK (placed_at IS NOT NULL OR placed_at_local IS NOT NULL)
```

**The story:** there's an old mobile app. It writes the customer's local
wall-clock time as *text*, with no timezone offset, into `placed_at_local`, and
leaves `placed_at` empty. 15% of orders arrive this way. Nobody will fix the
mobile app because it still works and rewriting it is a quarter of engineering
time.

**What it forces on you:** to know when an order happened, you need the
customer's timezone — which lives on a *different table*. That's a join, and it
turns out that where you put that join matters a lot. Chapter 10.

> **A note on honesty.** The original spec said `placed_at timestamptz NOT NULL`
> *and* described 15% of rows having only local time. Those two statements
> cannot both be true. We made the column nullable and wrote down why. When a
> spec contradicts itself, pick the interpretation that produces the more
> realistic system and record the decision — don't silently pick one.

### 2. `orders.status` is free text, not an enum

The table contains `paid`, `PAID`, `complete`, `Completed` and `completed`. All
five mean the same thing.

**Why not an enum?** Because a Postgres `ENUM` would have *rejected* those
values at write time — and then the mess wouldn't exist, and the cleanup
wouldn't need to happen, and you wouldn't learn anything. The absence of an enum
is exactly why real tables look like this.

**Who wrote which:** the *live API* only ever writes `completed`. Only the
historical backfill writes the legacy spellings, and only for orders older than
nine months. That's what a half-finished migration actually looks like — recent
data clean, old data not.

### 3. Customers are soft-deleted

```python
deleted_at: Mapped[datetime | None]
```

The row is never removed. `DELETE /customers/{id}` sets a timestamp.

**Why:** hard-deleting would break the foreign key from every order they ever
placed. Every app does this.

**What it forces on you:** a decision you cannot avoid — *do a deleted
customer's orders still count as revenue?* This project says **yes**. Deleting a
customer is a privacy action, not a financial one. The money really was earned.

That decision is written down in `DESIGN.md` §11, and it's the right kind of
decision to write down: two competent engineers could disagree, so the reasoning
matters more than the answer.

### 4. `payments.processed_at` is null while pending

A payment that hasn't completed has no processing time. Obviously.

**What it forces on you:** you cannot write `not_null` on that column in the
warehouse. If you do, the test fails forever, and a test that always fails is a
test everyone learns to ignore. You need a *scoped* test:

```yaml
- not_null_where:
    arguments:
      condition: "payment_status = 'succeeded'"
```

Chapter 15 goes into why this distinction matters so much.

### 5. Refunds arrive 1–14 days after their payment

Not a column — a *behaviour*, produced by the load generator.

This is the single most consequential property of the whole dataset. Chapter 13
is entirely about it.

### 6. 20% of customers transact in EUR or GBP

**What it forces on you:** you cannot just `SUM(amount_cents)`. You need
conversion, and — the part people get wrong — you need the exchange rate *from
the month the order was placed*, not today's rate. Otherwise last quarter's
revenue changes every morning.

## Two decisions that aren't awkward, just right

### Money is an integer number of cents

Never a float. Never `numeric`. `amount_cents INTEGER`.

**Why not float:** `0.1 + 0.2 != 0.3`. Sum a hundred thousand floats and the
answer depends on the order they were added in — which, in a parallel query
engine, isn't deterministic. Two runs of the same report can disagree in the
last cent and there's no way to say which is right.

**Why not `numeric`:** it's genuinely correct, and it's the right answer for a
system handling many currencies. Here it loses on a practical point: it crosses
the CDC boundary badly. Debezium encodes decimals as base64-encoded
`BigDecimal` bytes by default, which every consumer then has to decode using the
scale from the schema.

Integers cross every boundary — Postgres, Debezium, JSON, Parquet, DuckDB,
Snowflake — with zero conversion decisions. That's worth a lot.

**The honest limitation:** this breaks for sub-cent pricing (ad impressions at
\$0.0003) and for currencies with three decimal places (Bahraini dinar). Neither
applies here. Both would need a real fix.

### `REPLICA IDENTITY FULL`

One line at the end of the first migration:

```sql
ALTER TABLE customers REPLICA IDENTITY FULL;
```

**What it does:** tells Postgres to write the *entire old row* into the
write-ahead log on an update or delete, not just the primary key.

**Why you need it:** without it, a `DELETE` reaches your pipeline as "row with
id X is gone" and nothing else. You can't tell what was deleted, and you can't
tell a soft delete from a hard one.

**What it costs:** more WAL volume. Every update writes the whole before-image.
That's a real cost at scale and it's a deliberate trade — noted in `DESIGN.md`.

Note `subscription_events` does *not* get this. It's append-only; there are no
updates or deletes to capture before-images for.

## Try it

```bash
make messiness
```

That runs [`scripts/verify_messiness.sql`](../../scripts/verify_messiness.sql),
which counts every deliberate pattern. Every count must be greater than zero.

**Why that check exists:** if the generator ever stops producing one of these,
every downstream test covering it silently becomes *vacuous* — it still passes,
by testing nothing. That's the worst state a test can be in, and it's invisible
unless you check.

---

Next: **[Chapter 5 — Idempotency, or how a retry doubles your revenue](05-idempotency.md)**
