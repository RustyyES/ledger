# Chapter 17 — Serving

> Source: [`services/metrics-api/`](../../services/metrics-api/)

The warehouse has the answers. Now somebody needs to read them without being
given a SQL client and a database password.

## The rule: the API computes nothing

Every number the metrics API returns is **already a column in a mart**.

**Why this is a hard rule:** a metric computed in the API is a metric that

- the dbt tests do not cover
- the lineage graph does not show
- and that disagrees with the warehouse the first time somebody changes one and
  not the other

The API's job is **translation** — SQL to HTTP — plus auth, caching and
pagination.

There's exactly one exception, and it proves the rule: a custom rolling window
is computed in the API, but over the **already gap-filled** series from
`fct_revenue_rolling`. Computing it from raw daily revenue would reproduce the
exact bug that model exists to avoid (Chapter 14).

## 401 vs 403 — a real distinction

```python
if x_api_key is None:
    raise HTTPException(401, ..., headers={"WWW-Authenticate": "ApiKey"})
...
if not matched:
    raise HTTPException(403, ...)
```

- **401 Unauthorized** — "I don't know who you are." No credential presented.
- **403 Forbidden** — "I know who you are and the answer is still no."

These are different instructions to a client library. 401 means *attach a
credential and retry*. 403 means *stop retrying, this key will never work*.

Return 403 for both and a missing-header bug looks like a permissions problem,
sending whoever's debugging to the wrong team.

## Constant-time comparison

```python
matched = False
for candidate in settings.valid_api_keys:
    if hmac.compare_digest(x_api_key, candidate):
        matched = True
```

Two details:

**`hmac.compare_digest` not `==`.** String equality short-circuits on the first
differing byte, so response time leaks the key's prefix. It's a small leak and
an entirely avoidable one.

**No `break`.** Deliberately compares against *all* keys, so timing doesn't
reveal *which* key matched.

**Why a set of keys rather than one?** Rotating a shared secret with one slot
means a window where either the old or the new key is rejected. Two slots: add
the new key, migrate callers, remove the old one. No downtime.

## Freshness, and refusing to serve

Every response carries:

```
X-Data-Freshness: 0.32h
X-Data-Last-Updated: 2026-08-29T00:48:22Z
```

And past a threshold, the API **refuses**:

```python
if hours > settings.staleness_threshold_hours:
    raise HTTPException(503, detail={"code": "data_stale", ...})
```

> **Serving stale data silently is worse than serving none.** A six-hour-old
> dashboard looks *exactly* like a current one. Nobody checks. Decisions get made
> on numbers that predate the thing being decided about.

A 503 is loud. Someone notices immediately.

Freshness is read from the marts themselves — `max(_ingested_at)` in
`fct_payments` — rather than from a status table somebody has to remember to
update. The newest fact the warehouse has actually absorbed is what a consumer
asking "how current is this" actually wants to know.

### A performance detail worth copying

Freshness is checked on **every** request — twice, in fact. Each check opened a
DuckDB connection costing ~37ms. That's 75ms of a 200ms p95 budget spent
re-answering a question whose answer changes once a day.

Memoised for 10 seconds:

```python
_FRESHNESS_TTL_SECONDS = 10.0
```

But with a subtlety — the cached entry stores the *timestamp*, and the **age is
recomputed** on every read:

```python
last_seen = value[0]
age = (datetime.now(timezone.utc) - last_seen).total_seconds() / 3600
return last_seen, age
```

Cache the *age* and the reported freshness freezes for the TTL — which is
exactly the lie this header exists to prevent.

Result: **2.1ms** average on cached endpoints, against a 200ms target.

## Caching, and making it visible

```python
def cached(response: Response, key: str, producer: Callable[[], T]) -> T:
    hit = cache.get(key)
    if hit is not None:
        response.headers["X-Cache"] = "HIT"
        return cast(T, hit)
    value = producer()
    cache.set(key, value)
    response.headers["X-Cache"] = "MISS"
    return value
```

5-minute TTL. Every response says whether it was cached.

**Why in-process rather than Redis?** Redis is right for more than one replica.
It's wrong here: it adds a service, a failure mode and a serialisation format to
save a query taking 8ms against a local file.

The `X-Cache` header makes the behaviour observable either way, so swapping the
implementation later changes nothing a client can see. That's the point of
exposing it.

**A typing note.** `cached` is generic in the producer's return type. Before it
was annotated, it collapsed to `Any` — which let a plain `str` reach a
`Literal["day","week","month"]` field undetected. mypy caught it once the
generic was added. Small, and exactly the kind of thing type annotations are
actually for.

## Cursor pagination

The customer timeline can be thousands of events. It's paginated — with a
**cursor**, not an offset.

### Why not offset

```sql
LIMIT 50 OFFSET 5000
```

Two problems:

**Cost.** The database scans and discards 5,000 rows to return 50. Page 100
costs a hundred times page 1.

**Correctness.** The result set isn't stable. A refund landing between two
requests shifts every subsequent row by one — so the reader sees a duplicate or
**misses an event entirely**. And this endpoint is exactly the kind someone pages
through while data is arriving.

### The keyset cursor

```sql
where (occurred_at, reference_id) < (?, ?)
order by occurred_at desc, reference_id desc
limit ?
```

Encode the sort key of the last row seen. The next page uses the index, costs
the same on page 100 as page 1, and cannot skip or duplicate under concurrent
writes.

**Why `reference_id` in the key?** It's a tiebreaker, not decoration. Several
events share a timestamp to the microsecond — an order and its payment are
written in one transaction. A cursor on `occurred_at` alone would either loop
forever or skip the tied rows.

**Why a row-value comparison?** `(a, b) < (?, ?)` is equivalent to
`a < ? OR (a = ? AND b < ?)` — and the second form is the one people get subtly
wrong.

### Detecting "is there more"

```python
sql, params = build_timeline_query(decoded, customer_id, page_size)  # asks for page_size + 1
rows = query(sql, params)
has_more = len(rows) > page_size
rows = rows[:page_size]
```

Fetch one extra. Don't return it — it only tells you whether another page
exists. Avoids both an empty final page and a `total_count` requiring a second
expensive query.

### The cursor is opaque, not secret

```python
base64.urlsafe_b64encode(json.dumps({"t": ..., "r": ...}).encode())
```

Base64 signals "don't construct this yourself". It is **not** a security
boundary — it's trivially decodable — so it carries only the sort key, never
anything the caller isn't already entitled to see.

## Metric labels: the cardinality trap

```python
def _route_of(request: Request) -> str:
    route = request.scope.get("route")
    return getattr(route, "path", None) or "unmatched"
```

The **templated** path — `/orders/{order_id}/payments` — never the concrete one.

> Labelling a metric with `/orders/9f2c.../payments` creates **one time series
> per order**. At 8,000 orders/day that's millions of series, and it takes
> Prometheus down long before it tells you anything.

This is the single most common way a first metrics integration causes an
incident.

And it happened here anyway. The commerce API's middleware gets it right and has
a test enforcing it. The **load generator**, two hundred lines from that comment,
labelled its metrics with concrete paths:

```
ok:/orders/09c56913-a776-480d-9302-cfd6d2573f11/payments   1
ok:/orders/0dc62a26-1798-42cc-8406-8f19d779b47c/payments   1
... one series per order ...
```

A generator is an HTTP *client* — it has no ASGI scope to read the route from —
so it needs to normalise by hand:

```python
_UUID_RE = re.compile(r"/[0-9a-fA-F]{8}-...")
def template_path(path: str) -> str:
    return _UUID_RE.sub("/{id}", path)
```

> **A comment is not a control. A test is.** The same mistake is available on
> every service that emits metrics, so the test now exists on all of them.

## Status codes that mean something

| Code | When |
|---|---|
| 400 | malformed date range — `from` after `to` |
| 401 | no API key |
| 403 | invalid API key |
| 404 | unknown customer |
| 422 | invalid parameter (bad granularity, out-of-range window) |
| 503 | warehouse stale, or mid-rebuild |

A reversed date range is a **400**, not an empty result:

```python
if self.date_from > self.date_to:
    raise ValueError(f"'from' ({self.date_from}) is after 'to' ({self.date_to})")
```

Silently returning zero rows for a nonsensical range gives a dashboard a
confident, empty chart instead of an error.

That last row matters too:

```python
@app.exception_handler(duckdb.Error)
async def _warehouse_error(request, exc):
    return JSONResponse(status_code=503, ...)
```

dbt swaps tables during a build; a query landing in that window sees a missing
relation. That's **transient and retryable**, and saying so is the difference
between a client retry and a page.

## The read-only mount

```yaml
metrics-api:
  volumes:
    - warehouse-data:/data/warehouse:ro     # ← read-only
```

The serving layer must never write to the warehouse. Making that a **filesystem
guarantee** is stronger than making it a code convention.

Also, a new connection per query rather than a long-lived one — DuckDB takes a
file lock, and holding a connection across the dbt rebuild would *block the
rebuild*. The API would prevent the warehouse it serves from being refreshed,
which is an unpleasant deadlock to diagnose because everything looks healthy on
both sides.

## The dashboard

Streamlit, and it's an **operations** dashboard, not a BI one. It answers *"can I
trust the numbers right now?"*, not *"how is revenue doing?"*

That distinction drives every panel: each shows a **series over time** rather
than a current value, because the question an on-call engineer actually has is
*"when did this start?"* — and a single number cannot answer it.

The most useful panel plots refund arrival lag against the configured lookback,
with a red line at 21 days. Anything at or beyond the line is a fact the pipeline
can no longer see.

## Try it

```bash
curl -i -H "X-API-Key: dev-key-change-me" localhost:8001/metrics/mrr | head -20
curl -i localhost:8001/metrics/mrr                            # 401
curl -i -H "X-API-Key: wrong" localhost:8001/metrics/mrr      # 403
```

Watch `X-Cache` go `MISS` then `HIT` on a repeat.

---

Part V done. Next: judgement.

Next: **[Chapter 18 — Ten bugs](18-ten-bugs.md)**
