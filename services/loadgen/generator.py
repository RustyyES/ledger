"""Live traffic generator: continuous HTTP load against the commerce API.

Unlike `backfill.py`, this one goes through the API. That is the point -- it
exercises validation, idempotency, the state machine and the connection pool,
and it produces WAL exactly the way production would. If the sink can keep up
with this, it can keep up.

Design notes worth reading if you are using this as a reference:

* **It retries with the same Idempotency-Key.** A retry that generates a fresh
  key is not a retry, it is a duplicate, and it is the single most common way a
  load generator quietly poisons a warehouse with double-counted revenue.

* **It follows the hour-of-day curve.** Flat synthetic traffic makes freshness
  dashboards lie: everything looks healthy at 04:00 because the pipeline is
  never actually idle. Real pipelines break at the edges of the diurnal cycle.

* **It schedules late refunds.** Refunds are queued 1-14 days out and issued
  when due, which means a long-running instance produces genuinely late-arriving
  facts -- the exact condition the incremental lookback exists to survive.

* **It never crashes on a 4xx.** A validation failure is data about the API, not
  a reason to stop. It is counted, logged and moved past. Only a sustained
  failure rate above the circuit-breaker threshold stops the run.

Run:
    python generator.py --api http://localhost:8000
    python generator.py --api http://localhost:8000 --duration 300 --rate 40
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import heapq
import logging
import os
import random
import signal
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import profiles as P
import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = structlog.get_logger("loadgen")

REQUESTS = Counter("loadgen_requests_total", "Requests issued", ["endpoint", "outcome"])
LATENCY = Histogram(
    "loadgen_request_seconds",
    "Request latency as the client sees it",
    ["endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0),
)
PENDING_REFUNDS = Gauge("loadgen_pending_refunds", "Refunds scheduled but not yet due")
ACTIVE_CUSTOMERS = Gauge("loadgen_known_customers", "Customers this instance knows about")


_UUID_RE = __import__("re").compile(
    r"/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


def template_path(path: str) -> str:
    """Collapse concrete ids back into the route template.

    Labelling a metric (or a stats key) with `/orders/9f2c.../payments` creates
    one time series per order. At this generator's volume that is millions of
    series, and it takes Prometheus down long before it tells you anything.
    The API middleware avoids this by reading the matched route off the ASGI
    scope; a client has no scope to read, so it has to normalise by hand.
    """
    return _UUID_RE.sub("/{id}", path)


@dataclass(order=True)
class ScheduledRefund:
    """A refund queued to fire days after its payment.

    This is what makes the generator produce late-arriving facts. `due_at` is in
    wall-clock time, so a generator left running for a fortnight issues refunds
    against orders it created a fortnight ago -- which is precisely the case
    that breaks an incremental model keyed on the business timestamp.
    """

    due_at: datetime
    payment_id: str = field(compare=False)
    amount_cents: int = field(compare=False)
    reason: str | None = field(compare=False, default=None)


@dataclass
class KnownCustomer:
    id: str
    currency: str
    timezone: str
    subscription_id: str | None = None
    plan_code: str | None = None
    sub_status: str | None = None


class CircuitBreaker:
    """Stops the run if the API is comprehensively broken.

    Deliberately tolerant: it trips on a sustained *5xx/transport* failure rate,
    not on 4xx. A generator that halts because it sent one malformed request is
    useless for a two-week soak.
    """

    def __init__(self, threshold: float = 0.5, window: int = 200) -> None:
        self.threshold = threshold
        self.window = window
        self.outcomes: list[bool] = []

    def record(self, ok: bool) -> None:
        self.outcomes.append(ok)
        if len(self.outcomes) > self.window:
            self.outcomes.pop(0)

    @property
    def tripped(self) -> bool:
        if len(self.outcomes) < self.window:
            return False
        failure_rate = 1 - (sum(self.outcomes) / len(self.outcomes))
        return failure_rate > self.threshold


class LiveGenerator:
    def __init__(self, api: str, profile: P.LiveModeProfile, *, seed: int | None = None) -> None:
        self.api = api.rstrip("/")
        self.profile = profile
        self.rng = random.Random(seed)
        self.customers: list[KnownCustomer] = []
        self.refund_queue: list[ScheduledRefund] = []
        self.stats: dict[str, int] = defaultdict(int)
        self.breaker = CircuitBreaker()
        self.plans: list[str] = []
        self._stop = asyncio.Event()

    # -- HTTP --------------------------------------------------------------- #

    async def _request(
        self,
        client: httpx.AsyncClient,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        idempotent: bool = True,
    ) -> tuple[int, Any]:
        """Issue one request with bounded retries.

        The Idempotency-Key is generated ONCE, outside the retry loop. Moving it
        inside would turn every retry into a duplicate write.
        """
        headers: dict[str, str] = {}
        if idempotent and method == "POST":
            headers["Idempotency-Key"] = f"lg-{uuid.uuid4()}"
        label = template_path(path)

        delay = 0.25
        last_status, last_body = 0, None
        for attempt in range(self.profile.max_retries):
            try:
                with LATENCY.labels(label).time():
                    resp = await client.request(
                        method,
                        f"{self.api}{path}",
                        json=json,
                        headers=headers,
                        timeout=self.profile.request_timeout_s,
                    )
                last_status = resp.status_code
                try:
                    last_body = resp.json()
                except Exception:
                    last_body = None

                if resp.status_code < 500:
                    # 4xx is a real answer, not a transport failure. Do not retry.
                    outcome = "ok" if resp.status_code < 400 else "client_error"
                    REQUESTS.labels(label, outcome).inc()
                    self.breaker.record(True)
                    self.stats[f"{outcome}:{label}"] += 1
                    return resp.status_code, last_body

                REQUESTS.labels(label, "server_error").inc()
                self.breaker.record(False)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                REQUESTS.labels(label, "transport_error").inc()
                self.breaker.record(False)
                last_status, last_body = 0, {"transport_error": str(exc)}
                log.warning("transport_error", path=path, attempt=attempt, error=str(exc))

            if attempt < self.profile.max_retries - 1:
                await asyncio.sleep(delay + self.rng.random() * delay)  # jittered backoff
                delay *= 2

        self.stats[f"failed:{label}"] += 1
        return last_status, last_body

    # -- behaviours --------------------------------------------------------- #

    async def bootstrap(self, client: httpx.AsyncClient) -> None:
        status, body = await self._request(client, "GET", "/plans")
        if status != 200 or not body:
            raise RuntimeError(f"cannot read /plans from {self.api} (status {status})")
        self.plans = [p["code"] for p in body]
        log.info("bootstrapped", plans=self.plans)

    async def signup(self, client: httpx.AsyncClient) -> None:
        country = self.rng.choices(list(P.COUNTRY_WEIGHTS), list(P.COUNTRY_WEIGHTS.values()))[0]
        first = self.rng.choice(
            ["Ada", "Omar", "Yuki", "Ingrid", "Mateo", "Nadia", "Kofi", "Sofia"]
        )
        last = self.rng.choice(["Hopper", "Haddad", "Nakamura", "Rossi", "Okafor", "Liskov"])
        tz = P.COUNTRY_TIMEZONE.get(country, "UTC")

        status, body = await self._request(
            client,
            "POST",
            "/customers",
            json={
                "email": f"{first.lower()}.{last.lower()}.{uuid.uuid4().hex[:10]}@example.com",
                "name": f"{first} {last}",
                "country_code": country,
                "timezone": tz,
            },
        )
        if status != 201 or not body:
            return

        currency = P.COUNTRY_CURRENCY.get(country, "USD")
        if self.rng.random() > 0.85:
            currency = "USD"
        customer = KnownCustomer(id=body["id"], currency=currency, timezone=tz)
        self.customers.append(customer)
        ACTIVE_CUSTOMERS.set(len(self.customers))

        # Most signups immediately start a trial.
        if self.rng.random() < 0.78 and self.plans:
            plan = self.rng.choices(list(P.PLAN_WEIGHTS), list(P.PLAN_WEIGHTS.values()))[0]
            s, b = await self._request(
                client,
                "POST",
                "/subscriptions",
                json={"customer_id": customer.id, "plan_code": plan, "start_trial": True},
            )
            if s == 201 and b:
                customer.subscription_id = b["id"]
                customer.plan_code = plan
                customer.sub_status = b["status"]

    async def place_order(self, client: httpx.AsyncClient) -> None:
        if not self.customers:
            return
        customer = self.rng.choice(self.customers)
        amount = max(199, int(__import__("math").exp(self.rng.gauss(3.35, 0.85)) * 100))
        if customer.currency != "USD":
            amount = int(amount * {"EUR": 0.92, "GBP": 0.79}[customer.currency])

        now = datetime.now(UTC)
        payload: dict[str, Any] = {
            "customer_id": customer.id,
            "amount_cents": amount,
            "currency": customer.currency,
        }
        # 15% arrive from the legacy mobile client with a naive local time.
        if self.rng.random() < P.NAIVE_TIMESTAMP_SHARE:
            from zoneinfo import ZoneInfo

            payload["placed_at_local"] = now.astimezone(ZoneInfo(customer.timezone)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        else:
            payload["placed_at"] = now.isoformat()

        status, order = await self._request(client, "POST", "/orders", json=payload)
        if status != 201 or not order:
            return

        method = self.rng.choices(
            list(P.PAYMENT_METHOD_WEIGHTS), list(P.PAYMENT_METHOD_WEIGHTS.values())
        )[0]
        roll = self.rng.random()
        force = (
            "failed"
            if roll < P.PAYMENT_FAILURE_RATE
            else "pending"
            if roll < P.PAYMENT_FAILURE_RATE + P.PAYMENT_PENDING_RATE
            else "succeeded"
        )
        s, payment = await self._request(
            client,
            "POST",
            f"/orders/{order['id']}/payments",
            json={"amount_cents": amount, "method": method, "force_status": force},
        )
        if s != 201 or not payment or force != "succeeded":
            return

        # Schedule a genuinely late refund.
        if self.rng.random() < P.REFUND_RATE:
            delay_days = self.rng.randint(*P.REFUND_DELAY_DAYS)
            partial = self.rng.random() < P.PARTIAL_REFUND_SHARE
            heapq.heappush(
                self.refund_queue,
                ScheduledRefund(
                    due_at=now + timedelta(days=delay_days, seconds=self.rng.randrange(86_400)),
                    payment_id=payment["id"],
                    amount_cents=(
                        max(1, int(amount * self.rng.uniform(0.15, 0.8))) if partial else amount
                    ),
                    reason=self.rng.choice(
                        ["customer request", "duplicate charge", "goodwill", None]
                    ),
                ),
            )
            PENDING_REFUNDS.set(len(self.refund_queue))

    async def issue_due_refunds(self, client: httpx.AsyncClient) -> None:
        now = datetime.now(UTC)
        while self.refund_queue and self.refund_queue[0].due_at <= now:
            item = heapq.heappop(self.refund_queue)
            body: dict[str, Any] = {"amount_cents": item.amount_cents}
            if item.reason:
                body["reason"] = item.reason
            await self._request(client, "POST", f"/payments/{item.payment_id}/refunds", json=body)
        PENDING_REFUNDS.set(len(self.refund_queue))

    async def churn_and_change(self, client: httpx.AsyncClient) -> None:
        """Plan changes, pauses and cancellations on live subscriptions."""
        live = [c for c in self.customers if c.subscription_id and c.sub_status != "cancelled"]
        if not live:
            return
        customer = self.rng.choice(live)
        roll = self.rng.random()

        if roll < 0.35 and self.plans:
            options = [p for p in P.PLAN_WEIGHTS if p != customer.plan_code]
            if options:
                new_plan = self.rng.choice(options)
                s, b = await self._request(
                    client,
                    "PATCH",
                    f"/subscriptions/{customer.subscription_id}/plan",
                    json={"plan_code": new_plan},
                    idempotent=False,
                )
                if s == 200 and b:
                    customer.plan_code = new_plan
                    customer.sub_status = b["status"]
        elif roll < 0.55:
            s, b = await self._request(
                client, "POST", f"/subscriptions/{customer.subscription_id}/pause", json={}
            )
            if s == 200 and b:
                customer.sub_status = b["status"]
        elif roll < 0.75 and customer.sub_status == "paused":
            s, b = await self._request(
                client, "POST", f"/subscriptions/{customer.subscription_id}/resume", json={}
            )
            if s == 200 and b:
                customer.sub_status = b["status"]
        else:
            s, b = await self._request(
                client,
                "POST",
                f"/subscriptions/{customer.subscription_id}/cancel",
                json={"reason": "load generator churn"},
            )
            if s == 200 and b:
                customer.sub_status = "cancelled"

    async def relocate(self, client: httpx.AsyncClient) -> None:
        """Move a customer to another country.

        Small in volume, large in consequence: this is the mutation that makes
        SCD2 on `dim_customer` do observable work. Without relocations every
        customer has exactly one version and the snapshot proves nothing.
        """
        if not self.customers:
            return
        customer = self.rng.choice(self.customers)
        new_country = self.rng.choices(list(P.COUNTRY_WEIGHTS), list(P.COUNTRY_WEIGHTS.values()))[0]
        await self._request(
            client,
            "PATCH",
            f"/customers/{customer.id}",
            json={
                "country_code": new_country,
                "timezone": P.COUNTRY_TIMEZONE.get(new_country, "UTC"),
            },
            idempotent=False,
        )
        customer.timezone = P.COUNTRY_TIMEZONE.get(new_country, "UTC")

    # -- scheduling --------------------------------------------------------- #

    def _current_rate(self, base_per_minute: float) -> float:
        if not self.profile.follow_hour_curve:
            return base_per_minute
        return base_per_minute * P.hour_weight(datetime.now(UTC).hour)

    async def _pace(self, per_minute: float) -> None:
        """Poisson-distributed inter-arrival delay.

        Exponential gaps rather than a fixed sleep, because real arrivals are
        Poisson and a fixed cadence produces a suspiciously smooth WAL that
        never tests the sink's batching under burst.
        """
        rate = max(0.01, self._current_rate(per_minute))
        await asyncio.sleep(self.rng.expovariate(rate / 60.0))

    async def _order_loop(self, client: httpx.AsyncClient) -> None:
        while not self._stop.is_set():
            await self._pace(self.profile.orders_per_minute / self.profile.concurrency)
            if self._stop.is_set():
                break
            await self.place_order(client)

    async def _signup_loop(self, client: httpx.AsyncClient) -> None:
        while not self._stop.is_set():
            await self._pace(self.profile.signups_per_minute)
            if self._stop.is_set():
                break
            await self.signup(client)

    async def _lifecycle_loop(self, client: httpx.AsyncClient) -> None:
        while not self._stop.is_set():
            await self._pace(self.profile.signups_per_minute * 0.8)
            if self._stop.is_set():
                break
            if self.rng.random() < P.RELOCATION_RATE_ANNUAL * 12:
                await self.relocate(client)
            else:
                await self.churn_and_change(client)

    async def _refund_loop(self, client: httpx.AsyncClient) -> None:
        while not self._stop.is_set():
            await self.issue_due_refunds(client)
            await asyncio.sleep(10)

    async def _watchdog(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(15)
            if self.breaker.tripped:
                log.error("circuit_breaker_tripped", stats=dict(self.stats))
                self._stop.set()

    async def run(self, duration_s: float | None = None) -> int:
        limits = httpx.Limits(
            max_connections=self.profile.concurrency * 2,
            max_keepalive_connections=self.profile.concurrency,
        )
        async with httpx.AsyncClient(limits=limits) as client:
            await self.bootstrap(client)

            # Seed a starting population so orders have somewhere to land.
            await asyncio.gather(*(self.signup(client) for _ in range(20)))

            tasks = [
                asyncio.create_task(self._order_loop(client))
                for _ in range(self.profile.concurrency)
            ]
            tasks += [
                asyncio.create_task(self._signup_loop(client)),
                asyncio.create_task(self._lifecycle_loop(client)),
                asyncio.create_task(self._refund_loop(client)),
                asyncio.create_task(self._watchdog()),
            ]

            if duration_s:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=duration_s)
                except TimeoutError:
                    self._stop.set()
            else:
                await self._stop.wait()

            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        return 1 if self.breaker.tripped else 0

    def report(self) -> None:
        print("\nLive generator summary", file=sys.stderr)
        for key in sorted(self.stats):
            print(f"  {key:48} {self.stats[key]:>8,}", file=sys.stderr)
        print(f"  {'customers known':48} {len(self.customers):>8,}", file=sys.stderr)
        print(f"  {'refunds still scheduled':48} {len(self.refund_queue):>8,}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Live traffic generator.")
    parser.add_argument("--api", default=os.environ.get("LOADGEN_API_URL", "http://localhost:8000"))
    parser.add_argument(
        "--duration", type=float, default=None, help="Seconds to run. Omit to run until SIGTERM."
    )
    parser.add_argument("--rate", type=float, default=None, help="Override orders per minute.")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--metrics-port", type=int, default=int(os.environ.get("LOADGEN_METRICS_PORT", 9102))
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.stdlib.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    profile = P.LiveModeProfile()
    if args.rate:
        profile = P.LiveModeProfile(
            orders_per_minute=args.rate,
            signups_per_minute=max(0.2, args.rate / 8),
            concurrency=profile.concurrency,
        )

    try:
        start_http_server(args.metrics_port)
    except OSError as exc:
        log.warning("metrics_port_unavailable", port=args.metrics_port, error=str(exc))

    gen = LiveGenerator(args.api, profile, seed=args.seed)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGTERM, signal.SIGINT):
        # Not available on Windows; the generator still runs, it just cannot be
        # stopped gracefully there.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, gen._stop.set)

    try:
        code = loop.run_until_complete(gen.run(args.duration))
    finally:
        gen.report()
        loop.close()
    return code


if __name__ == "__main__":
    raise SystemExit(main())
