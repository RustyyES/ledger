# Chapter 11 — Star schemas from scratch

> Source: [`transform/models/marts/`](../../transform/models/marts/)

Assume you've never designed a warehouse table. This chapter builds up the idea
from the problem it solves.

## The problem: one big table doesn't work

Naive approach — put everything on the order row:

```
orders
├── order_id
├── amount_cents
├── customer_name           ← repeated on every order
├── customer_email          ← repeated
├── customer_country        ← repeated
├── plan_name               ← repeated
├── plan_price              ← repeated
└── ...
```

Three problems:

1. **Storage.** The string `"pro"` stored five thousand times a day.
2. **Updates.** Rename a plan and you rewrite five thousand rows per day of
   history.
3. **The killer** — a customer moves country. Update all their orders? Then last
   year's revenue-by-country changes. Don't update? Then the current view is
   wrong. **There is no right answer** with this shape.

## The split

Separate **things that happened** from **things that are**:

```
        dim_customer                     dim_plan
             │                               │
             └──────────┐         ┌──────────┘
                        ▼         ▼
                      fct_orders
                        ▲     ▲
             ┌──────────┘     └──────────┐
        dim_date                    (dim_channel, ...)
```

That shape — a fact in the middle, dimensions radiating out — is a **star
schema**. Hence the name.

**Facts** — one row per event. Long, thin, grows forever. Mostly numbers and
foreign keys.

**Dimensions** — one row per entity. Short, wide, grows slowly. Mostly
descriptive text.

## The grain — say it out loud

Before writing any fact table, finish this sentence:

> "One row in this table is exactly one ______."

- `fct_orders` — one row per **order**
- `fct_payments` — one row per **payment**
- `fct_mrr_daily` — one row per **day**

If you can't finish the sentence in one noun, your table is wrong. A table at
mixed grain — some rows per order, some per line item — is impossible to
aggregate correctly, and the bug shows up as numbers that are *nearly* right.

Every model in this project states its grain in the first line of its docstring.
That's not decoration; it's the thing you check first when a number is wrong.

## Surrogate keys

A **natural key** comes from the source: `customer_id`, a UUID.

A **surrogate key** is one the warehouse invents: `customer_key`, a hash.

Why bother inventing one? For `dim_plan` it's arguably ceremony. For
`dim_customer` it's essential, and Chapter 12 explains why: that table has
**multiple rows per customer**, so `customer_id` doesn't identify a row.

The surrogate is a hash of the natural key *plus whatever makes the row unique*:

```sql
{{ ledger_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_key
```

### A bug worth internalising

`dim_date` hashed its spine column directly. `fct_orders` hashed
`cast(placed_at_utc as date)`. Both "obviously" produce the key for the same day.

They didn't:

```
dim_date    2026-05-11  ->  36d5e5634b8a6457469ba5a320b4f30a
fct_orders  2026-05-11  ->  e1fc1478943112a42717cec0f7dacbef
```

The spine column was a `TIMESTAMP`, so it stringified as `'2026-05-11 00:00:00'`
while the fact stringified `'2026-05-11'`. **All 20,247 foreign keys were
orphaned.** Every metric joined through `dim_date` would have returned empty.

The fix wasn't "remember to cast". It was to make one definition:

```sql
{% macro date_key(date_expression) %}
    {{ dbt_utils.generate_surrogate_key(['cast(' ~ date_expression ~ ' as date)']) }}
{% endmacro %}
```

> **A key defined in two places is a key with two definitions.**

Caught by a `relationships` test. Chapter 15 covers why that test type earns its
keep.

## `dim_date`: generated, not derived

This looks like the most boring table in the warehouse. It isn't.

**Why generate it** rather than `select distinct order_date from fct_orders`?

Because a derived spine has **gaps** — precisely on the days with no orders.
Which are exactly the days you most need to see as zero rather than missing.

Chapter 14's rolling-revenue model depends entirely on having a complete spine.
Without it, a 7-day window silently spans 10 calendar days whenever there's a
gap, and every anomaly you were trying to detect gets smoothed away.

It also carries things the facts don't know:

```sql
{{ days_in_month('date_day') }} as days_in_month,   -- for MRR proration
(extract(dayofweek from date_day) in (0, 6)) as is_weekend,
fiscal_year, fiscal_quarter, fiscal_month,
is_holiday, holiday_name
```

The fiscal year starts in **February** — arbitrary, but deliberate. A fiscal
calendar that happens to equal the Gregorian one lets an off-by-one hide
forever.

### The `/` bug

```sql
((month - 2 + 12) % 12) / 3 + 1 as fiscal_quarter
```

Looks fine. Produced **twelve** distinct values: `1.0, 1.333, 1.667, 2.0, ...`

In both DuckDB and Snowflake, `/` on integers is **float division**.

```sql
cast(floor(((month - 2 + 12) % 12) / 3) as integer) + 1
```

Caught instantly by `accepted_values: [1,2,3,4]`. Every value was non-null,
non-duplicate and individually plausible — only an enumeration of what the
column is *allowed* to contain finds this. It's the cheapest possible
illustration of why that test type exists.

## `fct_orders`: the as-of join

The interesting part:

```sql
left join customers
    on  customers.customer_id = orders.customer_id
    and orders.placed_at_utc >= customers.valid_from
    and orders.placed_at_utc <  customers.valid_to_or_infinity
```

Not just "join on customer_id" — join on customer_id **and the time window that
contains this order**.

That's a **range join**, and it's what makes historical attribution stable. An
order placed while the customer lived in Egypt points at the Egyptian version of
that customer, forever, no matter how many times they move afterwards.

Chapter 12 is entirely about the dimension that makes this possible.

> **Range joins fail quietly.** If no version covers the timestamp, you get NULL
> rather than an error. The row stays in the fact table and disappears from
> every dimensional aggregate. That was bug #2 — 22% of orders — and row counts
> reconciled perfectly the whole time.

## Provenance columns

`fct_orders` carries several columns that aren't business data:

```sql
orders.is_legacy_client_order,
orders.is_timestamp_inferred,
orders.is_timestamp_low_confidence,
orders.is_fx_rate_missing,
coalesce(customers.is_deleted, false) as customer_is_deleted,
```

**Why keep them?** Because an analyst who filters `where not
is_timestamp_low_confidence` is asking a narrower but **more defensible**
question — and they can't do that if the pipeline threw the information away.

This is a habit worth forming. When you make an assumption, carry a flag saying
where you made it.

## Incremental strategy

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}
```

`delete+insert` — for the rows in scope, delete then re-insert.

**Why not `merge`?** Merge scans the whole table to find matches. Our grain is
stable and the reprocessing window is small, so delete+insert avoids a
full-table scan every run.

**Why not `append`?** Because it silently duplicated the entire table. That's
Chapter 13 and it's the next chapter but one.

## Why `dim_plan` is Type 1, stated honestly

`dim_customer` tracks history. `dim_plan` doesn't. Inconsistent?

No — it's a limitation, written down:

> Not SCD2, deliberately. Plan PRICES do change, and when they do this dimension
> will need versioning. It is Type 1 today because the source has no price
> history to reconstruct from: `plans` is mutated in place with no `updated_at`,
> so there is nothing to snapshot against. Recording the limitation is more
> honest than a snapshot that would silently be wrong.

If plan prices change, historical MRR restates. That's a real bug waiting to
happen, and it's in `DESIGN.md` under "what I would do differently" rather than
hidden.

> **Write down what your model can't do.** A known limitation is a design
> decision. An unknown one is a bug with a delay fuse.

## Try it

```bash
cd transform && dbt docs generate && dbt docs serve
```

Opens a browsable lineage graph. Click `fct_orders` and you can see every model
it depends on and everything that depends on it — which is the fastest way to
understand a dbt project you didn't write.

---

Next: **[Chapter 12 — Slowly changing dimensions, or the time-travel problem](12-scd2.md)**
