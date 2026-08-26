# Chapter 6 — Generating data that has a shape

> Source: [`services/loadgen/profiles.py`](../../services/loadgen/profiles.py),
> [`backfill.py`](../../services/loadgen/backfill.py),
> [`generator.py`](../../services/loadgen/generator.py)

## Faker vs simulator

Here's the fake-data code most projects write:

```python
for i in range(5_000_000):
    orders.append({
        "customer_id": random.choice(customer_ids),
        "amount_cents": random.randint(100, 50000),
        "placed_at": random_date_in_last_18_months(),
    })
```

Five million rows. Looks like a lot of data.

Now ask it a question:

- *"Do we sell more on weekends?"* — No. Exactly the same.
- *"When during the day do people buy?"* — Uniformly. All 24 hours identical.
- *"Do newer cohorts churn faster?"* — There are no cohorts.
- *"What happened on Black Friday?"* — Nothing.

Every analytical question returns a **flat line**. Which means every model you
build on it is untested, because a model that computes the wrong seasonal
adjustment produces a flat line too, and you can't tell the difference.

> **The principle.** Synthetic data must have *structure* that your models are
> supposed to find. Otherwise a model that finds nothing looks identical to a
> model that works.

## What structure we built in

Every constant lives in [`profiles.py`](../../services/loadgen/profiles.py) with
a comment explaining what it forces downstream.

### Time-of-day: bimodal

```python
_RAW_HOUR_CURVE = [
    0.20, 0.12, 0.08, 0.06, 0.06, 0.10,   # 00-05  overnight trough
    0.25, 0.55, 0.90, 1.05, 1.10, 1.35,   # 06-11  morning ramp
    1.70, 1.55, 1.20, 1.10, 1.15, 1.45,   # 12-17  lunch peak, afternoon dip
    1.95, 2.20, 2.05, 1.60, 1.00, 0.50,   # 18-23  evening peak
]
```

Two peaks — lunch and evening — with a real overnight trough. Measured on
generated data: **peak hour 19:00, trough 03:00**, a ratio over 10:1.

**Why it matters beyond looking realistic:** a freshness dashboard on flat data
never shows a quiet period, so you never find out how your pipeline behaves when
the source goes quiet. Pipelines break at the *edges* of the daily cycle — the
3am lull is when a sensor times out waiting for data that isn't coming.

### Weekends are quieter

```python
WEEKEND_MULTIPLIER = 0.65
```

Measured on the generated data: weekday ~3,100 orders, weekend ~2,100. Ratio
0.68 against a 0.65 target — the small drift comes from the growth ramp
interacting with which days fall on weekends.

### Black Friday

```python
BLACK_FRIDAY_SPIKE = 4.2
```

Computed properly — fourth Thursday of November, plus a day — so it lands on the
right date every year. Cyber Monday is 2.4x. The week after Christmas drops to
0.45x.

**What this forces:** any model that computes a "normal" baseline has to cope
with a 4.2x outlier. Rolling averages, anomaly detection and forecasts all
behave differently in its presence. On flat data none of that is exercised.

### Cohort churn: two effects at once

```python
MONTHLY_CHURN_BY_TENURE = {
    0: 0.115, 1: 0.082, 2: 0.061, 3: 0.048, ...
}
COHORT_RECENCY_PENALTY = 0.0035

def churn_probability(tenure_months, cohort_index):
    base = MONTHLY_CHURN_BY_TENURE.get(tenure_months, CHURN_FLOOR)
    return min(0.45, base + cohort_index * COHORT_RECENCY_PENALTY)
```

Two things are modelled simultaneously:

1. **Within a cohort, churn decays with tenure.** Survivors are stickier — the
   people still here at month 12 are not the same population that arrived.
2. **Across cohorts, newer ones churn faster.** This is the normal signature of
   a company that broadened its acquisition channels over time.

The second is what makes a retention heatmap *interesting*. Without it every
cohort row looks the same and the chart proves nothing.

### Growth

```python
ANNUAL_GROWTH_RATE = 1.45
```

Without this, every monthly cohort is the same size, and cohort analysis on
equal-sized cohorts tells you very little.

## Two modes, and why they're different code

### Backfill — writes straight to Postgres

18 months of history, up to 5M orders. **Bypasses the API entirely.** Three
reasons:

1. **Speed.** 5M orders through FastAPI + SQLAlchemy is hours. Through `COPY`
   it's minutes.
2. **Back-dating.** The API stamps `created_at` with `now()`. History needs its
   own timestamps — and an API that lets a client set them is a security hole,
   not a feature.
3. **Legacy values.** We need `paid`/`PAID`/`complete` rows that the current
   application would never write. The only truthful way to produce a
   half-finished migration is to write it as one.

### Live mode — goes through the HTTP API

Continuous, realistic traffic. **Deliberately does not bypass the API**, because
the point is to exercise validation, idempotency, the state machine and the
connection pool — and to produce write-ahead log entries exactly the way
production would.

Two details worth copying:

**The Idempotency-Key is generated once, outside the retry loop.**

```python
headers = {}
if idempotent and method == "POST":
    headers["Idempotency-Key"] = f"lg-{uuid.uuid4()}"   # ← once

for attempt in range(self.profile.max_retries):
    ...                                                 # ← retries reuse it
```

Move that inside the loop and every retry becomes a *duplicate*, not a retry —
which is precisely the bug Chapter 5 exists to prevent, reintroduced by the
client.

**Arrivals are Poisson, not evenly spaced.**

```python
await asyncio.sleep(self.rng.expovariate(rate / 60.0))
```

A fixed `sleep(5)` produces suspiciously smooth traffic that never tests the
sink's batching under burst. Exponential gaps produce clumps, which is what real
arrivals look like.

### Late refunds are scheduled, not immediate

```python
heapq.heappush(self.refund_queue, ScheduledRefund(
    due_at=now + timedelta(days=self.rng.randint(1, 14)),
    payment_id=payment["id"],
    ...
))
```

A priority queue of refunds due in the future. A generator left running for a
fortnight issues refunds against orders it created a fortnight ago.

**This is what makes Chapter 13 testable.** Without genuinely late-arriving
facts, the entire incremental design is untested and a broken version would
look identical to a working one.

## Determinism

```python
python backfill.py --scale 1.0 --seed 20250106
```

Same seed plus same scale produces byte-identical data. That's what makes the
backfill checksum proof in Chapter 16 meaningful — if the data changed between
runs, you couldn't tell a pipeline bug from a data difference.

## The scale knob

```bash
make backfill SCALE=1.0     # 5M orders, 500k customers, ~12 min
make backfill SCALE=0.02    # same SHAPE, ~90 seconds
```

The important word is **shape**. At 0.02 you still get weekend dips, the bimodal
curve, Black Friday, cohort decay and all six messiness patterns. Only the row
counts shrink.

That's what makes it usable as a CI fixture. CI runs at 0.004 and still catches
modelling bugs, because the properties the tests check are all still present.

## What breaks if you use a naive faker

- Seasonality models can't be validated — everything is flat.
- Cohort retention is meaningless — no cohort structure.
- The incremental lookback is untested — nothing arrives late.
- SCD2 is decoration — nobody relocates.
- Timezone handling is untested — no naive local timestamps.
- The status normalisation is untested — one spelling.

You'd have five million rows and roughly zero test coverage of the things that
matter.

## Try it

```bash
cd services/loadgen && pytest tests/ -v
```

32 tests, and they're worth reading as a group. They assert on the *shape* —
that the curve is bimodal, that weekends are quieter, that churn decays, that
newer cohorts churn faster. If someone flattens the generator, these fail
immediately rather than silently hollowing out every downstream test.

My favourite is `test_refund_delay_stays_inside_the_configured_lookback`, which
asserts a property of the *generator* against a constant in the *warehouse* —
raise the max refund delay past the lookback and this test tells you before the
warehouse starts silently losing refunds.

---

Part II done. Next: getting the data out.

Next: **[Chapter 7 — Change data capture from first principles](07-cdc.md)**
