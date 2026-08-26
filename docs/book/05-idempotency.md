# Chapter 5 — Idempotency, or how a retry doubles your revenue

> Source: [`services/commerce-api/app/idempotency.py`](../../services/commerce-api/app/idempotency.py)

## The problem, in one story

A customer clicks "Buy". Your API receives the request, writes the order,
charges the card — and then the network drops before the response gets back.

The customer's phone sees a timeout. It retries, exactly as it should.

Your API receives a *second* identical request. It has no way to know it's a
retry. It writes a second order and charges the card again.

The customer has been billed twice.

## Why this is a data engineering chapter

Because the same failure at a different layer is *much* worse.

The load generator in this project runs unattended for weeks. It retries on
timeout — any sensible client does. Every retry that creates a duplicate order
puts a duplicate row in Postgres, which flows through CDC into the warehouse,
which inflates revenue.

And here's the part that makes it a *data* problem rather than an app problem:
**nothing downstream can tell the difference.** Two orders, two distinct UUIDs,
two timestamps a second apart, same customer, same amount. That's not obviously
wrong — customers do sometimes buy the same thing twice. Every uniqueness test
passes. The revenue number is just too high.

You cannot fix duplicates in the warehouse if you can't identify them. You have
to prevent them at the source.

## The solution: a key the client generates

The client generates a unique key per *logical operation* and sends it as a
header:

```
POST /orders
Idempotency-Key: 7f3a9b2e-...
```

On a retry, the client sends **the same key**. The server recognises it and
returns the original response instead of doing the work again.

Three cases:

| Situation | Response |
|---|---|
| New key | Do the work. Store the response. |
| Same key, same body | Return the stored response. Don't redo the work. |
| Same key, **different** body | **422.** This is a client bug. |

That third case is worth pausing on. If a client reuses a key with different
content, something is broken on their side — maybe a key counter reset. Silently
returning the old response would hide it. Returning an error surfaces it while
it's still cheap to fix.

## The subtle parts

### The key is hashed against a *canonical* body

```python
canonical = json.dumps(json.loads(body), sort_keys=True, separators=(",", ":"))
return hashlib.sha256(canonical.encode()).hexdigest()
```

**Why not just hash the raw bytes?** Because HTTP libraries reorder JSON keys
between retries. Genuinely — some serialise from a dict whose iteration order
isn't stable. Hash the raw bytes and an honest retry looks like a different
request, gets a 422, and your client is now broken by your own safety mechanism.

Re-serialising with sorted keys makes `{"a":1,"b":2}` and `{"b":2,"a":1}` the
same request, which is what a human means by "the same request".

### Keys are scoped per endpoint

The stored key is `("POST /orders", "abc123")`, not just `"abc123"`.

**Why:** clients often use a simple counter or a per-session UUID. Without
scoping, the same key on `/customers` and `/orders` would collide, and the
second call would get the first call's response — a customer object returned
from an order endpoint. Scoping makes collisions impossible across endpoints.

### The route *template*, not the path

```python
route = request.scope.get("route")
route_path = getattr(route, "path", None) or request.url.path
```

`/orders/{order_id}/payments`, not `/orders/9f2c.../payments`.

**Why:** using the concrete path means every order gets its own key namespace,
which is harmless but pointless. Using the template keeps it one endpoint. (The
same reasoning appears in a much more dangerous form in Chapter 17 — using
concrete paths as *metric labels* takes Prometheus down. It's bug #8 in
Chapter 18.)

### The race, and how it's resolved

Two identical requests arrive at the same instant, on different workers. Both
check for an existing key. Both find nothing. Both proceed.

The obvious fix is a lock on the key. We didn't do that — it serialises unrelated
traffic through one lock table.

Instead: **let both do the work, and let the database's primary key arbitrate at
COMMIT.**

```python
def commit_or_replay(self, status_code, body):
    self.remember(status_code, body)      # add the key row
    try:
        self.session.commit()             # ← both racers commit here
    except IntegrityError as exc:
        if not _is_idempotency_pk_violation(exc):
            raise
        self.session.rollback()           # loser rolls back EVERYTHING
        row = <fetch the winner's stored response>
        raise ReplayedResponse(row.response_status, row.response_body)
    return body
```

The critical detail: **the domain writes and the idempotency row are in the same
transaction.** So when the loser rolls back, the order it created disappears
too. There is no window where a duplicate order exists without a matching key
row to catch it.

Get that wrong — commit the order, then insert the key — and you have both a
duplicate order *and* a mechanism that thinks it prevented one.

## What we rejected

**A lock on the key.** Correct, but it serialises through one hot table and adds
a deadlock surface. The optimistic approach costs one wasted transaction on a
rare race.

**A cache instead of a table.** Redis with a TTL is a common choice. It fails
here because the cache and the database can disagree: the cache write can succeed
while the transaction rolls back, or vice versa. Putting the key in the *same
transaction as the data* is what makes the guarantee airtight.

**Doing nothing and deduplicating in the warehouse.** This is what a lot of teams
actually do, and it's why this chapter exists. You can't deduplicate what you
can't identify. Two legitimate identical orders and two duplicated ones look
exactly the same by the time they reach you.

## What breaks if you remove it

- The load generator's retries create duplicate orders.
- Revenue in the warehouse is overstated by roughly the timeout rate.
- No test anywhere catches it — every uniqueness constraint still holds, because
  the duplicates have different primary keys.
- You discover it when finance reconciles against the payment processor, months
  later.

## Try it

```bash
cd services/commerce-api
pytest tests/test_idempotency.py -v
```

The one to read is `test_replay_is_insensitive_to_key_order_in_the_body` — it's
the non-obvious one, and it's the one that would have bitten in production.

---

Next: **[Chapter 6 — Generating data that has a shape](06-the-load-generator.md)**
