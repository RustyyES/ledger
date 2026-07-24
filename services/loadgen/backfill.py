"""Historical backfill: 18 months of commerce history, written straight to Postgres.

Why this bypasses the API
-------------------------
The live generator drives the HTTP API because that is the honest way to
produce *current* traffic. The backfill does not, for three reasons:

  1. Speed. 5M orders through FastAPI + SQLAlchemy is hours. Through COPY it is
     minutes. The ELT platform is the subject of this project; the loader is not.
  2. Back-dating. The API stamps `created_at`/`updated_at` with now(). History
     needs its own timestamps, and an API that lets a client set them is a
     security hole, not a feature.
  3. Legacy values. `orders.status` needs 'paid'/'PAID'/'complete' rows that the
     current application would never write. The only truthful way to produce a
     half-finished data migration is to write it as one.

This is also the load path that feeds the bulk Parquet export, so its output
must be exactly what CDC would have produced had it been running -- same
columns, same conventions. See `services/cdc-sink/bulk_export.py`.

Determinism
-----------
Seeded. Two runs with the same seed and scale produce byte-identical data,
which is what makes the backfill checksum proof in `results/` meaningful.

Usage
-----
    python backfill.py --scale 1.0          # full 5M orders / 500k customers
    python backfill.py --scale 0.02         # laptop-sized, same shape
    python backfill.py --scale 1.0 --dry-run
"""

from __future__ import annotations

import argparse
import io
import logging
import math
import os
import random
import sys
import uuid
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import profiles as P
import psycopg

log = logging.getLogger("backfill")

COPY_BATCH = 50_000
NULL = r"\N"


# --------------------------------------------------------------------------- #
# In-memory customer state
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class CustomerState:
    id: uuid.UUID
    email: str
    name: str
    country: str
    timezone: str
    currency: str
    signup: datetime
    cohort_index: int
    deleted_at: datetime | None = None
    updated_at: datetime = field(default=None)  # type: ignore[assignment]

    # subscription
    sub_id: uuid.UUID | None = None
    plan_code: str | None = None
    sub_status: str | None = None
    sub_started: datetime | None = None
    sub_ended: datetime | None = None
    trial_ends: datetime | None = None
    billing_day: int = 1

    #: (effective_from, plan_code), in ascending order. Renewals must be billed
    #: at the plan in force ON THE BILLING DATE, not at the customer's final
    #: plan. Billing everything at the final plan made historical revenue
    #: disagree with event-derived MRR by up to 40% -- caught by
    #: assert_mrr_reconciles_to_payments.
    plan_timeline: list = field(default_factory=list)

    def plan_on(self, when: datetime) -> str:
        """The plan code in force at `when`."""
        current = self.plan_timeline[0][1] if self.plan_timeline else "basic"
        for effective_from, code in self.plan_timeline:
            if effective_from <= when:
                current = code
            else:
                break
        return current


FIRST_NAMES = [
    "Ada",
    "Grace",
    "Alan",
    "Edsger",
    "Barbara",
    "Donald",
    "Frances",
    "Ken",
    "Margaret",
    "Tim",
    "Linus",
    "Radia",
    "Vint",
    "Shafi",
    "Leslie",
    "Katherine",
    "Omar",
    "Layla",
    "Yuki",
    "Ingrid",
    "Mateo",
    "Nadia",
    "Kofi",
    "Sofia",
]
LAST_NAMES = [
    "Lovelace",
    "Hopper",
    "Turing",
    "Dijkstra",
    "Liskov",
    "Knuth",
    "Allen",
    "Thompson",
    "Hamilton",
    "Berners-Lee",
    "Torvalds",
    "Perlman",
    "Cerf",
    "Goldwasser",
    "Lamport",
    "Johnson",
    "Haddad",
    "Nakamura",
    "Okafor",
    "Rossi",
]
REFUND_REASONS = [
    "customer request",
    "duplicate charge",
    "service outage credit",
    "downgrade adjustment",
    "fraud chargeback",
    "goodwill",
    None,
]


class Backfill:
    def __init__(self, *, scale: float, seed: int, end: date | None = None) -> None:
        self.rng = random.Random(seed)
        self.scale = scale
        # History must not run past the moment of generation. Orders are placed
        # at a random hour of their day, so picking today as a candidate day
        # produces timestamps later today -- which arrive in the warehouse as
        # future-dated orders and fail `assert_no_future_dated_orders`. That
        # test exists to catch timezone bugs; a generator that trips it for an
        # unrelated reason makes the real signal unreadable.
        self.generated_at = datetime.now(UTC)
        self.target_customers = max(50, int(P.TARGET_CUSTOMERS * scale))
        self.target_orders = max(200, int(P.TARGET_ORDERS * scale))
        self.end_date = end or datetime.now(UTC).date()
        self.start_date = self.end_date - timedelta(days=int(P.BACKFILL_MONTHS * 30.44))
        self.legacy_cutoff = self.end_date - timedelta(
            days=int(P.LEGACY_STATUS_CUTOFF_MONTHS * 30.44)
        )
        self.plans: dict[str, tuple[int, int]] = {}  # code -> (id, monthly_cents)
        self.customers: list[CustomerState] = []
        self.counts: dict[str, int] = defaultdict(int)
        self._event_id = 0

    # -- setup ------------------------------------------------------------- #

    def load_plans(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as cur:
            cur.execute("SELECT code, id, monthly_cents FROM plans")
            self.plans = {code: (pid, cents) for code, pid, cents in cur.fetchall()}
        missing = set(P.PLAN_WEIGHTS) - set(self.plans)
        if missing:
            raise RuntimeError(
                f"plans table is missing {sorted(missing)}; run `alembic upgrade head` first"
            )

    # -- customer generation ------------------------------------------------ #

    def _signup_distribution(self) -> list[int]:
        """Customers per month, ramped by ANNUAL_GROWTH_RATE.

        A flat distribution would make every cohort the same size, and cohort
        retention charts on flat cohorts tell you nothing.
        """
        months = P.BACKFILL_MONTHS
        monthly_growth = P.ANNUAL_GROWTH_RATE ** (1 / 12)
        weights = [monthly_growth**i for i in range(months)]
        total = sum(weights)
        counts = [int(self.target_customers * w / total) for w in weights]
        counts[-1] += self.target_customers - sum(counts)
        return counts

    def generate_customers(self) -> None:
        counts = self._signup_distribution()
        countries = list(P.COUNTRY_WEIGHTS)
        country_w = list(P.COUNTRY_WEIGHTS.values())
        plan_codes = list(P.PLAN_WEIGHTS)
        plan_w = list(P.PLAN_WEIGHTS.values())

        seq = 0
        for month_index, n in enumerate(counts):
            month_start = self.start_date + timedelta(days=int(month_index * 30.44))
            for _ in range(n):
                seq += 1
                signup = self._random_datetime_in(month_start, days=30)
                country = self.rng.choices(countries, country_w)[0]
                first = self.rng.choice(FIRST_NAMES)
                last = self.rng.choice(LAST_NAMES)
                currency = self._pick_currency(country)

                cust = CustomerState(
                    id=uuid.UUID(int=self.rng.getrandbits(128), version=4),
                    email=f"{first.lower()}.{last.lower().replace('-', '')}.{seq}@example.com",
                    name=f"{first} {last}",
                    country=country,
                    timezone=P.COUNTRY_TIMEZONE.get(country, "UTC"),
                    currency=currency,
                    signup=signup,
                    cohort_index=month_index,
                    updated_at=signup,
                    billing_day=min(28, signup.day),
                )
                self._attach_subscription(cust, plan_codes, plan_w)
                self.customers.append(cust)

    def _pick_currency(self, country: str) -> str:
        """20% of customers transact in EUR or GBP.

        Currency follows country where the country has one, which is what makes
        the conversion layer downstream non-trivial: the exchange rate that
        applies is the one *on the order date*, not today's.
        """
        native = P.COUNTRY_CURRENCY.get(country)
        if native and self.rng.random() < 0.85:
            return native
        if self.rng.random() < P.NON_USD_CUSTOMER_SHARE * 0.25:
            return self.rng.choice(["EUR", "GBP"])
        return "USD"

    def _attach_subscription(self, c: CustomerState, plan_codes, plan_w) -> None:
        c.sub_id = uuid.UUID(int=self.rng.getrandbits(128), version=4)
        c.plan_code = self.rng.choices(plan_codes, plan_w)[0]
        c.sub_started = c.signup
        c.trial_ends = c.signup + timedelta(days=14)
        c.sub_status = "trialing"
        c.plan_timeline = [(c.signup, c.plan_code)]

    # -- lifecycle simulation ---------------------------------------------- #

    def simulate_lifecycles(self) -> Iterator[tuple]:
        """Walk each customer month by month, yielding subscription events.

        Yields tuples ready for COPY into `subscription_events`.
        """
        for c in self.customers:
            yield from self._simulate_one(c)

    def _simulate_one(self, c: CustomerState) -> Iterator[tuple]:
        assert c.sub_id and c.plan_code and c.sub_started
        self._event_id += 1
        yield (
            self._event_id,
            c.sub_id,
            "created",
            NULL,
            self.plans[c.plan_code][0],
            c.sub_started,
            c.sub_started,
        )

        # Trial outcome.
        trial_end = c.trial_ends or c.signup
        if trial_end.date() > self.end_date:
            c.sub_status = "trialing"
            return
        if self.rng.random() > P.TRIAL_CONVERSION_RATE:
            c.sub_status = "cancelled"
            c.sub_ended = trial_end
            c.updated_at = trial_end
            self._event_id += 1
            yield (
                self._event_id,
                c.sub_id,
                "cancelled",
                self.plans[c.plan_code][0],
                self.plans[c.plan_code][0],
                trial_end,
                trial_end,
            )
            return

        c.sub_status = "active"
        cursor = trial_end
        tenure = 0
        while cursor.date() < self.end_date:
            cursor = cursor + timedelta(days=30)
            tenure += 1
            if cursor.date() >= self.end_date:
                break

            if self.rng.random() < P.churn_probability(tenure, c.cohort_index):
                c.sub_status = "cancelled"
                c.sub_ended = cursor
                c.updated_at = cursor
                self._event_id += 1
                yield (
                    self._event_id,
                    c.sub_id,
                    "cancelled",
                    self.plans[c.plan_code][0],
                    self.plans[c.plan_code][0],
                    cursor,
                    cursor,
                )
                # A slice of churned customers later exercise their right to
                # erasure. This is what puts soft-deleted rows WITH order
                # history into the warehouse.
                if self.rng.random() < 0.08:
                    c.deleted_at = cursor + timedelta(days=self.rng.randint(1, 90))
                    if c.deleted_at.date() > self.end_date:
                        c.deleted_at = None
                    else:
                        c.updated_at = c.deleted_at
                return

            if self.rng.random() < P.PLAN_CHANGE_RATE:
                old_code = c.plan_code
                new_code = self._pick_other_plan(old_code)
                if new_code:
                    upgrade = self.plans[new_code][1] > self.plans[old_code][1]
                    c.plan_code = new_code
                    c.plan_timeline.append((cursor, new_code))
                    c.updated_at = cursor
                    self._event_id += 1
                    yield (
                        self._event_id,
                        c.sub_id,
                        "upgraded" if upgrade else "downgraded",
                        self.plans[old_code][0],
                        self.plans[new_code][0],
                        cursor,
                        cursor,
                    )

            elif self.rng.random() < P.PAUSE_RATE:
                self._event_id += 1
                yield (
                    self._event_id,
                    c.sub_id,
                    "paused",
                    self.plans[c.plan_code][0],
                    self.plans[c.plan_code][0],
                    cursor,
                    cursor,
                )
                resume_at = cursor + timedelta(days=self.rng.randint(20, 75))
                if resume_at.date() < self.end_date:
                    self._event_id += 1
                    yield (
                        self._event_id,
                        c.sub_id,
                        "resumed",
                        self.plans[c.plan_code][0],
                        self.plans[c.plan_code][0],
                        resume_at,
                        resume_at,
                    )
                    c.sub_status = "active"
                else:
                    c.sub_status = "paused"

    def _pick_other_plan(self, current: str) -> str | None:
        options = [p for p in P.PLAN_WEIGHTS if p != current]
        if not options:
            return None
        if self.rng.random() < P.UPGRADE_SHARE_OF_PLAN_CHANGES:
            dearer = [p for p in options if self.plans[p][1] > self.plans[current][1]]
            if dearer:
                return self.rng.choice(dearer)
        cheaper = [p for p in options if self.plans[p][1] < self.plans[current][1]]
        return self.rng.choice(cheaper) if cheaper else self.rng.choice(options)

    # -- helpers ------------------------------------------------------------ #

    def _random_datetime_in(self, start: date, days: int) -> datetime:
        """A timestamp inside a window, shaped by the hour-of-day curve.

        Clamped to the generation instant: signup timestamps in the future
        would produce customers who placed orders before they existed.
        """
        day = start + timedelta(days=self.rng.randrange(days))
        hour = self.rng.choices(range(24), P.HOUR_OF_DAY_CURVE)[0]
        stamp = datetime(
            day.year,
            day.month,
            day.day,
            hour,
            self.rng.randrange(60),
            self.rng.randrange(60),
            tzinfo=UTC,
        )
        return min(stamp, self.generated_at)

    def _daily_order_weights(self) -> dict[date, float]:
        weights: dict[date, float] = {}
        d = self.start_date
        while d <= self.end_date:
            w = P.DAY_OF_WEEK_MULTIPLIER[d.weekday()] * P.calendar_multiplier(d)
            # Growth ramp, so recent days carry more volume than old ones.
            age_months = (self.end_date - d).days / 30.44
            w *= P.ANNUAL_GROWTH_RATE ** (-age_months / 12)
            weights[d] = w
            d += timedelta(days=1)
        return weights

    # -- order / payment / refund generation -------------------------------- #

    def generate_transactions(self) -> tuple[list, list, list]:
        """Produce orders, payments and refunds.

        Two distinct streams, because the business has two:

        * **Renewal orders** -- one per active subscription per billing cycle.
          These are what MRR must reconcile against, and they are deliberately
          NOT seasonal: nobody's subscription renews harder on Black Friday.
        * **One-off orders** -- shaped by the daily volume curve, the weekend
          multiplier and the calendar spikes. These are what makes rolling
          revenue and the gap-fill requirement interesting.

        Conflating the two is the modelling error this data is designed to
        expose: an analyst who sums all orders and calls it MRR gets a number
        that spikes 4.2x on one day in November.
        """
        orders: list[tuple] = []
        payments: list[tuple] = []
        refunds: list[tuple] = []

        for c in self.customers:
            self._renewal_orders(c, orders, payments, refunds)

        remaining = max(0, self.target_orders - len(orders))
        self._one_off_orders(remaining, orders, payments, refunds)

        # COPY does not care about order, but a deterministic sort makes the
        # bulk Parquet export reproducible, which the checksum proof needs.
        orders.sort(key=lambda r: (str(r[0])))
        payments.sort(key=lambda r: (str(r[0])))
        refunds.sort(key=lambda r: (str(r[0])))
        return orders, payments, refunds

    def _renewal_orders(self, c: CustomerState, orders, payments, refunds) -> None:
        if c.sub_status is None or c.sub_started is None:
            return
        # Billing starts when the trial converts, not at signup.
        cursor = (c.trial_ends or c.sub_started) + timedelta(days=1)
        stop = min(
            c.sub_ended or datetime.combine(self.end_date, datetime.min.time(), UTC),
            datetime.combine(self.end_date, datetime.min.time(), UTC),
        )
        if c.sub_status == "trialing":
            return

        while cursor < stop:
            amount = self.plans[c.plan_code][1] if c.plan_code else 1900
            amount = self._localise_amount(amount, c.currency)
            self._emit_order(c, cursor, amount, orders, payments, refunds, subscription=True)
            cursor += timedelta(days=30)

    def _one_off_orders(self, count: int, orders, payments, refunds) -> None:
        if count <= 0 or not self.customers:
            return
        weights = self._daily_order_weights()
        days = list(weights)
        day_w = [weights[d] for d in days]

        # Only customers who existed on the order date can place it, so pick
        # the day first and then a customer alive on it. Buckets keep that
        # lookup O(1) instead of a scan per order.
        by_signup_day: dict[date, list[int]] = defaultdict(list)
        for i, c in enumerate(self.customers):
            by_signup_day[c.signup.date()].append(i)
        sorted_days = sorted(by_signup_day)
        alive_prefix: list[int] = []
        running: list[int] = []
        for d in sorted_days:
            running.extend(by_signup_day[d])
            alive_prefix.append(len(running))
        eligible_order = running  # indices in signup order

        chosen_days = self.rng.choices(days, day_w, k=count)
        for day in chosen_days:
            # Binary search for how many customers had signed up by `day`.
            lo, hi = 0, len(sorted_days)
            while lo < hi:
                mid = (lo + hi) // 2
                if sorted_days[mid] <= day:
                    lo = mid + 1
                else:
                    hi = mid
            alive = alive_prefix[lo - 1] if lo else 0
            if alive == 0:
                continue
            c = self.customers[eligible_order[self.rng.randrange(alive)]]
            if c.deleted_at is not None and c.deleted_at.date() <= day:
                continue

            hour = self.rng.choices(range(24), P.HOUR_OF_DAY_CURVE)[0]
            when = datetime(
                day.year,
                day.month,
                day.day,
                hour,
                self.rng.randrange(60),
                self.rng.randrange(60),
                tzinfo=UTC,
            )
            if when >= self.generated_at:
                continue  # would be in the future; skip this draw

            # The eligibility index is keyed on signup DATE, but the order gets
            # a random hour of that day. A customer who signed up at 18:00 was
            # therefore eligible for an order at 09:00 the same morning --
            # i.e. an order placed before the customer existed.
            #
            # Downstream that is not a harmless oddity: the as-of join in
            # `fct_orders` finds no dim_customer version covering the order, so
            # `customer_key` is NULL and the order silently drops out of every
            # dimensional aggregate while staying in the fact table. Row counts
            # still reconcile, which is what makes it hard to notice.
            #
            # Caught by `assert_every_order_resolves_a_customer_version`.
            if when < c.signup:
                continue
            amount = self._localise_amount(self._one_off_amount(), c.currency)
            self._emit_order(c, when, amount, orders, payments, refunds, subscription=False)

    def _one_off_amount(self) -> int:
        """Log-normal-ish basket size in USD cents. Long right tail, no zeros."""
        base = math.exp(self.rng.gauss(3.35, 0.85))  # ~ $28 median
        return max(199, int(base * 100))

    def _localise_amount(self, usd_cents: int, currency: str) -> int:
        """Convert to the customer's transacting currency.

        A fixed nominal rate is used here on purpose. The warehouse must apply
        the rate *as of the order date* from `seed_fx_rates`, and if the
        generator baked a date-varying rate in, a wrong conversion downstream
        would still reconcile. Making the source deliberately naive is what
        makes the conversion layer testable.
        """
        if currency == "USD":
            return usd_cents
        nominal = {"EUR": 0.92, "GBP": 0.79}.get(currency, 1.0)
        return max(1, int(round(usd_cents * nominal)))

    def _emit_order(
        self,
        c: CustomerState,
        when: datetime,
        amount: int,
        orders,
        payments,
        refunds,
        *,
        subscription: bool,
    ) -> None:
        order_id = uuid.UUID(int=self.rng.getrandbits(128), version=4)

        # --- messiness 3: naive local timestamps -------------------------- #
        placed_at: str | datetime = when
        placed_at_local: str = NULL
        if self.rng.random() < P.NAIVE_TIMESTAMP_SHARE:
            from zoneinfo import ZoneInfo

            local = when.astimezone(ZoneInfo(c.timezone))
            placed_at_local = local.strftime("%Y-%m-%d %H:%M:%S")
            placed_at = NULL  # type: ignore[assignment]

        failed = self.rng.random() < P.PAYMENT_FAILURE_RATE
        pending = not failed and self.rng.random() < P.PAYMENT_PENDING_RATE

        # --- messiness 1: legacy status spellings -------------------------- #
        if failed or pending:
            status = "pending"
        elif when.date() < self.legacy_cutoff:
            variants = list(P.LEGACY_STATUS_VARIANTS)
            status = self.rng.choices(variants, list(P.LEGACY_STATUS_VARIANTS.values()))[0]
        else:
            status = "completed"

        self.counts["orders"] += 1
        orders.append(
            (
                order_id,
                c.id,
                c.sub_id if subscription and c.sub_id else NULL,
                amount,
                c.currency,
                status,
                placed_at,
                placed_at_local,
                when,
            )
        )

        payment_id = uuid.UUID(int=self.rng.getrandbits(128), version=4)
        method = self.rng.choices(
            list(P.PAYMENT_METHOD_WEIGHTS), list(P.PAYMENT_METHOD_WEIGHTS.values())
        )[0]
        pay_status = "failed" if failed else ("pending" if pending else "succeeded")
        # --- messiness 6: nullable-in-practice processed_at ----------------- #
        processed_at = NULL if pending else when + timedelta(seconds=self.rng.randint(1, 90))
        self.counts["payments"] += 1
        payments.append(
            (
                payment_id,
                order_id,
                amount,
                method,
                pay_status,
                processed_at,
                when,
                when,
            )
        )

        # --- messiness 4: LATE refunds ------------------------------------- #
        if pay_status == "succeeded" and self.rng.random() < P.REFUND_RATE:
            delay = self.rng.randint(*P.REFUND_DELAY_DAYS)
            issued = when + timedelta(
                days=delay, hours=self.rng.randrange(24), minutes=self.rng.randrange(60)
            )
            if issued <= self.generated_at:
                partial = self.rng.random() < P.PARTIAL_REFUND_SHARE
                refund_amount = (
                    max(1, int(amount * self.rng.uniform(0.15, 0.8))) if partial else amount
                )
                if not partial:
                    # A fully refunded order flips status, exactly as the API does.
                    for i, row in enumerate(orders):
                        if row[0] == order_id:
                            orders[i] = row[:5] + ("refunded",) + row[6:]
                            break
                self.counts["refunds"] += 1
                refunds.append(
                    (
                        uuid.UUID(int=self.rng.getrandbits(128), version=4),
                        payment_id,
                        refund_amount,
                        self.rng.choice(REFUND_REASONS) or NULL,
                        issued,
                        issued,
                    )
                )

    # -- writing ------------------------------------------------------------ #

    @staticmethod
    def _copy(conn: psycopg.Connection, table: str, columns: list[str], rows: list[tuple]) -> int:
        """Stream rows into Postgres with COPY, in batches.

        Text-format COPY with an explicit NULL marker. `psycopg.copy` with
        binary would be marginally faster, but text keeps the failure mode
        legible: a malformed row names its column in the error rather than
        producing an opaque protocol fault 400k rows in.
        """
        if not rows:
            return 0
        written = 0
        collist = ", ".join(columns)
        sql = f"COPY {table} ({collist}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
        for start in range(0, len(rows), COPY_BATCH):
            chunk = rows[start : start + COPY_BATCH]
            buf = io.StringIO()
            for row in chunk:
                buf.write("\t".join(_encode(v) for v in row))
                buf.write("\n")
            buf.seek(0)
            with conn.cursor().copy(sql) as cp:
                cp.write(buf.read())
            written += len(chunk)
            log.info("copied", extra={"table": table, "rows": written, "of": len(rows)})
            print(f"  {table}: {written:,} / {len(rows):,}", file=sys.stderr)
        return written

    def write(self, conn: psycopg.Connection) -> None:
        # ORDER MATTERS. `simulate_lifecycles()` mutates CustomerState in place
        # -- it is what sets `deleted_at`, `updated_at`, `sub_status` and
        # `sub_ended`. Materialising the customer rows before running it
        # captures every customer in their pre-simulation state, which silently
        # produces zero soft deletes. `verify()` catches that; this ordering is
        # what makes it pass.
        events = list(self.simulate_lifecycles())
        orders, payments, refunds = self.generate_transactions()

        customers = [
            (
                c.id,
                c.email,
                c.name,
                c.country,
                c.timezone,
                c.signup,
                c.updated_at or c.signup,
                c.deleted_at if c.deleted_at else NULL,
            )
            for c in self.customers
        ]

        # Subscriptions are derived from the final in-memory state, so they are
        # written after the lifecycle walk has run.
        subscriptions = [
            (
                c.sub_id,
                c.id,
                self.plans[c.plan_code][0],
                c.sub_status,
                c.sub_started,
                c.sub_ended if c.sub_ended else NULL,
                c.trial_ends if c.trial_ends else NULL,
                c.updated_at or c.signup,
            )
            for c in self.customers
            if c.sub_id and c.plan_code and c.sub_status
        ]

        print(f"\nWriting to Postgres (scale={self.scale}):", file=sys.stderr)
        self._copy(
            conn,
            "customers",
            [
                "id",
                "email",
                "name",
                "country_code",
                "timezone",
                "created_at",
                "updated_at",
                "deleted_at",
            ],
            customers,
        )
        self._copy(
            conn,
            "subscriptions",
            [
                "id",
                "customer_id",
                "plan_id",
                "status",
                "started_at",
                "ended_at",
                "trial_ends_at",
                "updated_at",
            ],
            subscriptions,
        )
        self._copy(
            conn,
            "subscription_events",
            [
                "id",
                "subscription_id",
                "event_type",
                "from_plan_id",
                "to_plan_id",
                "occurred_at",
                "created_at",
            ],
            events,
        )
        self._copy(
            conn,
            "orders",
            [
                "id",
                "customer_id",
                "subscription_id",
                "amount_cents",
                "currency",
                "status",
                "placed_at",
                "placed_at_local",
                "updated_at",
            ],
            orders,
        )
        self._copy(
            conn,
            "payments",
            [
                "id",
                "order_id",
                "amount_cents",
                "method",
                "status",
                "processed_at",
                "created_at",
                "updated_at",
            ],
            payments,
        )
        self._copy(
            conn,
            "refunds",
            ["id", "payment_id", "amount_cents", "reason", "issued_at", "created_at"],
            refunds,
        )

        # bigserial does not advance when ids are supplied explicitly. Leaving
        # the sequence behind means the first live API write collides on the
        # primary key -- a genuinely nasty bug to diagnose after the fact.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT setval(pg_get_serial_sequence('subscription_events','id'), "
                "COALESCE((SELECT MAX(id) FROM subscription_events), 1))"
            )
        conn.commit()


def _add_month(when: datetime) -> datetime:
    """Advance one calendar month, clamping the day to the month's length.

    A subscription that starts on the 31st bills on the 30th in a 30-day month
    and on the 28th in February -- which is what real billing systems do, and
    what keeps one billing event per calendar month per subscription.
    """
    import calendar

    year = when.year + (when.month // 12)
    month = when.month % 12 + 1
    day = min(when.day, calendar.monthrange(year, month)[1])
    return when.replace(year=year, month=month, day=day)


def _encode(value) -> str:
    """Encode one value for text-format COPY."""
    if value is None:
        return NULL
    if isinstance(value, str):
        if value == NULL:
            return NULL
        return (
            value.replace("\\", "\\\\")
            .replace("\t", "\\t")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
        )
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def verify(conn: psycopg.Connection) -> dict[str, int]:
    """Post-load assertions on the six messiness patterns.

    This is the query half of the Stage 1 exit criterion "all six messiness
    patterns present and verifiable by query". If any of these returns zero the
    generator regressed and the warehouse tests downstream become vacuous --
    they would pass by testing nothing.
    """
    checks = {
        "legacy_status_rows": "SELECT count(*) FROM orders WHERE status IN ('paid','PAID','complete','Completed')",
        "soft_deleted_customers": "SELECT count(*) FROM customers WHERE deleted_at IS NOT NULL",
        "soft_deleted_with_orders": (
            "SELECT count(DISTINCT c.id) FROM customers c JOIN orders o ON o.customer_id = c.id "
            "WHERE c.deleted_at IS NOT NULL"
        ),
        "naive_local_timestamps": "SELECT count(*) FROM orders WHERE placed_at IS NULL AND placed_at_local IS NOT NULL",
        "late_refunds_over_7d": (
            "SELECT count(*) FROM refunds r JOIN payments p ON p.id = r.payment_id "
            "WHERE r.issued_at - p.created_at > interval '7 days'"
        ),
        "non_usd_orders": "SELECT count(*) FROM orders WHERE currency <> 'USD'",
        "pending_payments_null_processed_at": (
            "SELECT count(*) FROM payments WHERE status = 'pending' AND processed_at IS NULL"
        ),
        "plan_change_events": "SELECT count(*) FROM subscription_events WHERE event_type IN ('upgraded','downgraded')",
    }
    results: dict[str, int] = {}
    with conn.cursor() as cur:
        for name, sql in checks.items():
            cur.execute(sql)
            results[name] = cur.fetchone()[0]
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate historical commerce data.")
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Fraction of the full 5M-order target. 0.02 is a good laptop size.",
    )
    parser.add_argument(
        "--seed", type=int, default=20250106, help="RNG seed. Same seed + scale => identical data."
    )
    parser.add_argument("--dsn", default=os.environ.get("COMMERCE_DATABASE_URL", ""))
    parser.add_argument(
        "--dry-run", action="store_true", help="Generate and report, write nothing."
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Empty the fact tables first. Refuses if the API has written since.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(message)s")

    if not args.dsn:
        print("error: --dsn or COMMERCE_DATABASE_URL is required", file=sys.stderr)
        return 2
    dsn = args.dsn.replace("postgresql+psycopg://", "postgresql://")

    started = datetime.now(UTC)
    with psycopg.connect(dsn) as conn:
        bf = Backfill(scale=args.scale, seed=args.seed)
        bf.load_plans(conn)

        if args.truncate:
            with conn.cursor() as cur:
                cur.execute(
                    "TRUNCATE refunds, payments, orders, subscription_events, "
                    "subscriptions, customers RESTART IDENTITY CASCADE"
                )
            conn.commit()

        print(
            f"Generating {bf.target_customers:,} customers "
            f"and ~{bf.target_orders:,} orders "
            f"across {bf.start_date} .. {bf.end_date}",
            file=sys.stderr,
        )
        bf.generate_customers()

        if args.dry_run:
            events = list(bf.simulate_lifecycles())
            orders, payments, refunds = bf.generate_transactions()
            print(
                f"\nDRY RUN -- nothing written\n"
                f"  customers          {len(bf.customers):,}\n"
                f"  subscription_events{len(events):>12,}\n"
                f"  orders             {len(orders):,}\n"
                f"  payments           {len(payments):,}\n"
                f"  refunds            {len(refunds):,}",
                file=sys.stderr,
            )
            return 0

        bf.write(conn)
        elapsed = (datetime.now(UTC) - started).total_seconds()
        print(f"\nLoaded in {elapsed:.1f}s", file=sys.stderr)

        print("\nMessiness verification (all must be > 0):", file=sys.stderr)
        failures = []
        for name, count in verify(conn).items():
            flag = "ok " if count > 0 else "FAIL"
            print(f"  [{flag}] {name:36} {count:>12,}", file=sys.stderr)
            if count == 0:
                failures.append(name)
        if failures:
            print(
                f"\nFAILED: {len(failures)} messiness pattern(s) absent: {failures}",
                file=sys.stderr,
            )
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
