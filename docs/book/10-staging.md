# Chapter 10 — Staging, and why the layer contract matters

> Source: [`transform/models/staging/`](../../transform/models/staging/)

Welcome to dbt. Two paragraphs of what it is, then the interesting part.

## What dbt is, briefly

dbt lets you write a `SELECT` statement in a `.sql` file and have it become a
table or view. That's it. You write:

```sql
-- models/staging/stg_plans.sql
select id as plan_id, code as plan_code from raw.plans
```

and dbt runs `CREATE VIEW staging.stg_plans AS (that select)`.

Two things make it more than a script runner:

**`ref()`** builds a dependency graph.

```sql
select * from {{ ref('stg_orders') }}
```

dbt now knows this model depends on `stg_orders`, and will build them in the
right order, in parallel where it can.

**Tests live next to models**, in YAML:

```yaml
- name: order_id
  data_tests: [unique, not_null]
```

That's most of dbt. The rest of this part is about *what to write*, not how.

## The four layers, and their contracts

```
raw  ──►  staging  ──►  intermediate  ──►  marts
```

| Layer | Materialised as | Rules |
|---|---|---|
| `staging` | view | One model per source table. Rename, cast, normalise. **No joins.** |
| `intermediate` | ephemeral | Joins and shared logic. Not public. |
| `marts` | incremental table | Star schema. **This is the public contract.** |

The contracts matter more than the layer names.

### Why "one staging model per source table, no joins"?

Because it makes every staging model a **pure function of exactly one source
table**. Debugging one never requires understanding another.

That sounds like an aesthetic preference. It isn't. When a number is wrong, your
first question is "where did it go wrong?" With this rule, you can check each
staging model in isolation — is `stg_orders` a faithful, cleaned-up view of the
`orders` source? Yes or no, answerable in thirty seconds.

Break the rule and staging models start depending on each other, and the answer
becomes "well, it depends on whether `stg_customers` was right".

### Why views for staging, tables for marts?

**Views** are just stored queries — no storage, always current, but recomputed
on every read. Right for staging: they're thin, they're read by a handful of
downstream models, and you always want the freshest raw data.

**Tables** are materialised. Right for marts: they're read by dashboards and
APIs many times, and recomputing a 20-model dependency chain per dashboard load
is not viable.

**Ephemeral** (intermediate) means dbt inlines the SQL as a CTE into whatever
references it. No object is created at all. Right here because these models
aren't part of the public contract — materialising them would cost storage to
publish an interface we explicitly don't offer.

## The mess, normalised

Here's the status cleanup, in one macro, used in exactly one place:

```sql
{% macro normalise_order_status(column) %}
    case lower(trim({{ column }}))
        when 'paid'       then 'completed'
        when 'complete'   then 'completed'
        when 'completed'  then 'completed'
        when 'pending'    then 'pending'
        when 'cancelled'  then 'cancelled'
        when 'canceled'   then 'cancelled'
        when 'refunded'   then 'refunded'
        else 'unknown'
    end
{% endmacro %}
```

Two design points.

**Why a macro rather than inline SQL?** Because "what counts as a completed
order" is a *business rule*, and business rules need exactly one definition.
Inline it in three models and they will drift — someone will add a spelling to
one and not the others, and two dashboards will disagree with no obvious cause.

**Why `else 'unknown'` rather than passing the value through?** This one is
subtle and it's the more important choice.

Pass it through, and a new legacy spelling silently creates a *sixth status*.
Every `group by order_status` now splits on it. Revenue quietly drops out of the
"completed" bucket and appears in a bucket nobody is looking at.

Map it to `'unknown'` and it's **loud** — the `accepted_values` test fails
immediately:

```yaml
- name: order_status
  data_tests:
    - accepted_values:
        arguments:
          values: ["pending", "completed", "cancelled", "refunded"]
```

> **The general principle: fail loudly on the unexpected, don't pass it
> through.** Unknown data flowing silently into your aggregates is the enemy.

There's a companion flag too:

```sql
(lower(trim(status)) not in (...)) as has_unrecognised_status
```

tested to always be false. Belt and braces, on the thing that would be silent.

## Deduplicating CDC

Raw contains *every version* of every row — one per change, plus the original
snapshot. Staging needs the current one.

```sql
{% macro cdc_latest(relation_alias, key_column, include_deletes=false) %}
    select * from (
        select {{ relation_alias }}.*,
            row_number() over (
                partition by {{ relation_alias }}.{{ key_column }}
                order by {{ cdc_version_order('desc') }}
            ) as _version_rank
        from {{ relation_alias }}
    )
    where _version_rank = 1
    {%- if not include_deletes %}
      and {{ adapter.quote('_op') }} != 'd'
    {%- endif %}
{% endmacro %}
```

The ordering is the interesting part:

```sql
coalesce(_lsn, 0) desc,
_source_ts desc nulls last,
_kafka_partition desc,
_kafka_offset desc
```

**Why `coalesce(_lsn, 0)`?** Two populations share this data. Bulk-export rows
have `_lsn = NULL`; CDC rows have a real LSN. Coalescing null to 0 puts every
bulk row *below* every CDC row — correct, because the snapshot is by
construction the oldest state we have.

**Why LSN and not `_source_ts`?** LSN is Postgres's authoritative commit order.
`_source_ts` has millisecond resolution and *ties* on a busy table. Ordering by
a column with ties gives you a non-deterministic answer.

**Why not `_kafka_offset` as primary?** It's only ordered within one Kafka
partition. Across partitions it means nothing.

## The one model that keeps deletes

Every staging model ends with `where _op != 'd'` — except one:

```sql
-- stg_customers.sql
{{ cdc_latest('source', 'id', include_deletes=true) }}
```

**Why:** a soft-deleted customer is still the customer who placed every order in
their history. Drop them here and those orders orphan — their `customer_key`
becomes null and they vanish from every dimensional aggregate while remaining in
the fact table. (That's bug #2 in Chapter 18, from a different cause but the same
symptom.)

The delete becomes a *flag* instead, and the warehouse decides what it means.

The layer contract says exceptions need explicit justification, and the
justification is a comment at the top of the model. That's the right place for
it — findable by whoever next wonders why this model is different.

## Where the spec was wrong, and what we did

The original spec's example `stg_orders.sql` resolved local timestamps against
the customer's timezone:

```sql
coalesce(placed_at, (placed_at_local::timestamp at time zone customer_timezone))
```

That requires **joining to customers** — and the layer contract three sections
earlier says staging does no joins.

Both cannot be true.

We chose the contract, and moved the resolution to `int_orders__resolved`. The
reasoning: the contract is what keeps every staging model independently
debuggable, and that property is worth more than following an example.

> **When a spec contradicts itself, pick the interpretation that produces the
> better system, and write down that you did.** Silently picking one leaves the
> next reader wondering whether you noticed.

## The circularity in the intermediate layer

Moving the timezone resolution revealed something genuinely tricky.

- Converting to USD needs the order's **month**.
- The month comes from `placed_at_utc`.
- `placed_at_utc` needs the **timezone**.

So the FX join must happen **after** the timezone resolution, not alongside it.
That's why `int_orders__resolved` is two sequential CTEs rather than one wide
join:

```sql
resolved_time as (
    select ..., {{ resolve_placed_at_utc(...) }} as placed_at_utc
    from orders left join customers using (customer_id)
),
with_fx as (
    select ..., {{ to_usd_cents(...) }} as amount_usd_cents
    from resolved_time
    left join {{ ref('seed_fx_rates') }} as fx
        on fx.rate_date = cast(date_trunc('month', resolved_time.placed_at_utc) as date)
)
```

Get the order wrong and the 15% of orders with no `placed_at` land in the wrong
FX month. The error is small, plausible, and essentially undetectable by
inspection.

## Timezone resolution, and the honest limitation

```sql
{% macro resolve_placed_at_utc(placed_at, placed_at_local, timezone_col) %}
    coalesce(
        {{ placed_at }},
        case
            when {{ placed_at_local }} is null then null
            when {{ timezone_col }} is null
                then cast({{ placed_at_local }} as timestamp) at time zone 'UTC'
            else cast({{ placed_at_local }} as timestamp) at time zone {{ timezone_col }}
        end
    )
{% endmacro %}
```

Two edge cases a naive version gets wrong:

**DST spring-forward.** A local time inside the skipped hour doesn't exist.
DuckDB and Snowflake both resolve it forward rather than erroring — which is
what we want, but it should be a deliberate choice rather than an accident.

**Unknown timezone.** Falling back to UTC is **wrong** by up to 14 hours. We do
it anyway, because the alternative is dropping the order. But every such row is
flagged:

```sql
(orders.placed_at is null) as is_timestamp_inferred,
(orders.placed_at is null and not customers.has_explicit_timezone)
    as is_timestamp_low_confidence
```

So an analyst can exclude them, and `assert_no_future_dated_orders` catches the
worst cases.

> **When you have to make a lossy assumption, mark the rows you made it on.**
> The alternative is a dataset where good and guessed values are
> indistinguishable.

## FX: the rate as of *when*?

The rate that applies is the one for the **month the order was placed in**.

Use today's rate and last quarter's revenue changes every morning. Finance
notices, and trusts you less afterwards.

**Why monthly rather than daily?** Finance restates non-USD revenue at the
month's closing rate. A daily rate implies a precision the business doesn't
actually use, and makes two people reconciling the same month disagree by
rounding.

**Why is there an explicit USD → USD row at 1.0?** So the join never
special-cases the reporting currency, and a missing rate is unambiguously an
error rather than an implicit identity. That lets
`assert_every_non_usd_order_has_an_fx_rate` be a strict check.

That test matters because `SUM()` **skips nulls silently**. A missing rate makes
`amount_usd_cents` null, the sum omits it, and total revenue comes back smaller
than the truth — looking entirely reasonable. About 20% of the book is non-USD.

## Try it

```bash
cd transform && dbt build --select staging
```

73 nodes: 7 models and 66 tests.

---

Next: **[Chapter 11 — Star schemas from scratch](11-star-schemas.md)**
