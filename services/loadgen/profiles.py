"""Behavioural parameters for the traffic simulator.

This file is the difference between a data *faker* and a data *generator*. A
faker gives you 5M rows of uniform noise, and every analytical question you ask
of it returns a flat line -- which means none of the models downstream are
being tested against anything. The numbers here produce data with structure:
weekday/weekend shape, a bimodal daily curve, cohorts that decay at different
rates, a Black Friday spike, and refunds that arrive days after the payment
they reverse.

Everything is overridable by environment variable so a laptop run can use a
tenth of the volume without editing code.

Sources for the shapes (not the exact values, which are invented):
  * Bimodal daily curve   -- lunch and evening peaks, standard for consumer
                             subscription commerce.
  * Cohort churn decay    -- newer cohorts churning faster is the normal
                             signature of a company that has broadened its
                             acquisition channels over time. It is also the
                             thing a retention model must be able to SHOW,
                             which is why it is baked in rather than random.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


# --------------------------------------------------------------------------- #
# Volume
# --------------------------------------------------------------------------- #

DAILY_VOLUME_BASE: int = _env_int("LOADGEN_DAILY_VOLUME_BASE", 8_000)
WEEKEND_MULTIPLIER: float = _env_float("LOADGEN_WEEKEND_MULTIPLIER", 0.65)
BLACK_FRIDAY_SPIKE: float = _env_float("LOADGEN_BLACK_FRIDAY_SPIKE", 4.2)

TARGET_CUSTOMERS: int = _env_int("LOADGEN_TARGET_CUSTOMERS", 500_000)
TARGET_ORDERS: int = _env_int("LOADGEN_TARGET_ORDERS", 5_000_000)
BACKFILL_MONTHS: int = _env_int("LOADGEN_BACKFILL_MONTHS", 18)

# Hour-of-day weights, index 0..23. Bimodal: a lunch bump and a larger evening
# peak, with a genuine overnight trough. Normalised at import.
_RAW_HOUR_CURVE: list[float] = [
    0.20,
    0.12,
    0.08,
    0.06,
    0.06,
    0.10,  # 00-05 overnight trough
    0.25,
    0.55,
    0.90,
    1.05,
    1.10,
    1.35,  # 06-11 morning ramp
    1.70,
    1.55,
    1.20,
    1.10,
    1.15,
    1.45,  # 12-17 lunch peak, afternoon dip
    1.95,
    2.20,
    2.05,
    1.60,
    1.00,
    0.50,  # 18-23 evening peak
]
_curve_total = sum(_RAW_HOUR_CURVE)
HOUR_OF_DAY_CURVE: list[float] = [w / _curve_total for w in _RAW_HOUR_CURVE]

# Day-of-week multipliers, Monday=0. Friday is slightly elevated; the weekend
# drop is the WEEKEND_MULTIPLIER applied to Sat/Sun.
DAY_OF_WEEK_MULTIPLIER: list[float] = [
    1.05,
    1.02,
    1.00,
    1.03,
    1.12,
    WEEKEND_MULTIPLIER,
    WEEKEND_MULTIPLIER * 0.92,
]

# Compound annual growth, applied as a smooth ramp across the backfill window.
# Without it every cohort is the same size and cohort analysis is uninteresting.
ANNUAL_GROWTH_RATE: float = _env_float("LOADGEN_ANNUAL_GROWTH_RATE", 1.45)


# --------------------------------------------------------------------------- #
# Calendar effects
# --------------------------------------------------------------------------- #


def _black_friday(year: int) -> date:
    """Fourth Thursday of November, plus one day."""
    import calendar

    thursdays = [
        d
        for week in calendar.Calendar().monthdatescalendar(year, 11)
        for d in week
        if d.month == 11 and d.weekday() == calendar.THURSDAY
    ]
    return thursdays[3] + __import__("datetime").timedelta(days=1)


def calendar_multiplier(day: date) -> float:
    """Volume multiplier for calendar effects on a given date."""
    if day == _black_friday(day.year):
        return BLACK_FRIDAY_SPIKE
    if day == _black_friday(day.year) + __import__("datetime").timedelta(days=3):
        return 2.4  # Cyber Monday
    if day.month == 12 and 20 <= day.day <= 24:
        return 1.6  # pre-Christmas
    if day.month == 12 and 25 <= day.day <= 31:
        return 0.45  # dead week
    if day.month == 1 and day.day <= 3:
        return 0.55
    return 1.0


# --------------------------------------------------------------------------- #
# Customer behaviour
# --------------------------------------------------------------------------- #

TRIAL_CONVERSION_RATE: float = _env_float("LOADGEN_TRIAL_CONVERSION_RATE", 0.31)
PLAN_CHANGE_RATE: float = _env_float("LOADGEN_PLAN_CHANGE_RATE", 0.04)  # monthly
UPGRADE_SHARE_OF_PLAN_CHANGES: float = _env_float("LOADGEN_UPGRADE_SHARE", 0.62)
PAUSE_RATE: float = _env_float("LOADGEN_PAUSE_RATE", 0.012)  # monthly

# Monthly churn by signup-cohort age in months. Two things are modelled at once:
#   * within a cohort, churn decays with tenure (the survivors are stickier);
#   * across cohorts, newer ones churn faster (see module docstring).
# Key = months since signup. The cohort-recency penalty is applied on top.
MONTHLY_CHURN_BY_TENURE: dict[int, float] = {
    0: 0.115,
    1: 0.082,
    2: 0.061,
    3: 0.048,
    4: 0.041,
    5: 0.036,
    6: 0.032,
    7: 0.029,
    8: 0.027,
    9: 0.025,
    10: 0.024,
    11: 0.023,
}
CHURN_FLOOR: float = 0.019  # asymptote past 12 months
COHORT_RECENCY_PENALTY: float = _env_float("LOADGEN_COHORT_RECENCY_PENALTY", 0.0035)


def churn_probability(tenure_months: int, cohort_index: int) -> float:
    """Monthly churn hazard for a customer.

    `cohort_index` is months since the very first cohort; higher means newer.
    """
    base = MONTHLY_CHURN_BY_TENURE.get(tenure_months, CHURN_FLOOR)
    return min(0.45, base + cohort_index * COHORT_RECENCY_PENALTY)


# --------------------------------------------------------------------------- #
# Money
# --------------------------------------------------------------------------- #

REFUND_RATE: float = _env_float("LOADGEN_REFUND_RATE", 0.021)
REFUND_DELAY_DAYS: tuple[int, int] = (
    _env_int("LOADGEN_REFUND_DELAY_MIN", 1),
    _env_int("LOADGEN_REFUND_DELAY_MAX", 14),
)
PARTIAL_REFUND_SHARE: float = _env_float("LOADGEN_PARTIAL_REFUND_SHARE", 0.28)
PAYMENT_FAILURE_RATE: float = _env_float("LOADGEN_PAYMENT_FAILURE_RATE", 0.035)
PAYMENT_PENDING_RATE: float = _env_float("LOADGEN_PAYMENT_PENDING_RATE", 0.018)

PAYMENT_METHOD_WEIGHTS: dict[str, float] = {
    "card": 0.71,
    "paypal": 0.16,
    "apple_pay": 0.09,
    "bank_transfer": 0.04,
}

PLAN_WEIGHTS: dict[str, float] = {"basic": 0.58, "pro": 0.33, "enterprise": 0.09}


# --------------------------------------------------------------------------- #
# The deliberate mess
#
# Each constant below maps to one row of the messiness table in the spec. They
# are requirements, not bugs, and the warehouse tests assert they are present.
# --------------------------------------------------------------------------- #

#: Share of orders written with a naive local timestamp instead of `placed_at`.
NAIVE_TIMESTAMP_SHARE: float = _env_float("LOADGEN_NAIVE_TIMESTAMP_SHARE", 0.15)

#: Share of customers who transact in a non-USD currency.
NON_USD_CUSTOMER_SHARE: float = _env_float("LOADGEN_NON_USD_SHARE", 0.20)

#: Legacy `orders.status` spellings, only ever written by the BACKFILL for
#: orders older than LEGACY_STATUS_CUTOFF_MONTHS. The live API always writes
#: the canonical value -- which is exactly what a half-finished data migration
#: looks like in a real table.
LEGACY_STATUS_VARIANTS: dict[str, float] = {
    "paid": 0.42,
    "PAID": 0.11,
    "complete": 0.29,
    "Completed": 0.18,
}
LEGACY_STATUS_CUTOFF_MONTHS: int = _env_int("LOADGEN_LEGACY_STATUS_CUTOFF_MONTHS", 9)

COUNTRY_WEIGHTS: dict[str, float] = {
    "US": 0.34,
    "GB": 0.11,
    "DE": 0.09,
    "FR": 0.06,
    "EG": 0.06,
    "IN": 0.07,
    "BR": 0.05,
    "CA": 0.05,
    "AU": 0.04,
    "NL": 0.03,
    "ES": 0.03,
    "IT": 0.03,
    "JP": 0.02,
    "SE": 0.01,
    "PL": 0.01,
}

COUNTRY_TIMEZONE: dict[str, str] = {
    "US": "America/New_York",
    "GB": "Europe/London",
    "DE": "Europe/Berlin",
    "FR": "Europe/Paris",
    "EG": "Africa/Cairo",
    "IN": "Asia/Kolkata",
    "BR": "America/Sao_Paulo",
    "CA": "America/Toronto",
    "AU": "Australia/Sydney",
    "NL": "Europe/Amsterdam",
    "ES": "Europe/Madrid",
    "IT": "Europe/Rome",
    "JP": "Asia/Tokyo",
    "SE": "Europe/Stockholm",
    "PL": "Europe/Warsaw",
}

COUNTRY_CURRENCY: dict[str, str] = {
    "GB": "GBP",
    "DE": "EUR",
    "FR": "EUR",
    "NL": "EUR",
    "ES": "EUR",
    "IT": "EUR",
}

#: A small number of customers relocate. This is what makes SCD2 on
#: `dim_customer` do observable work rather than being decoration: without
#: relocations, every version is identical and the model proves nothing.
RELOCATION_RATE_ANNUAL: float = _env_float("LOADGEN_RELOCATION_RATE", 0.03)


@dataclass(frozen=True)
class LiveModeProfile:
    """Rates for the continuously-running live generator.

    Every default is a `default_factory`, not a computed default. A bare
    `x: int = _env_int(...)` is evaluated ONCE, when the class body executes at
    import time -- so a test that sets the environment variable and then
    constructs the dataclass gets the value captured at import, not the one it
    just set. `default_factory` defers the read to construction, which is both
    what a reader expects and what makes the profile testable.
    """

    orders_per_minute: float = field(
        default_factory=lambda: _env_float("LOADGEN_ORDERS_PER_MINUTE", 12.0)
    )
    signups_per_minute: float = field(
        default_factory=lambda: _env_float("LOADGEN_SIGNUPS_PER_MINUTE", 1.5)
    )
    concurrency: int = field(default_factory=lambda: _env_int("LOADGEN_CONCURRENCY", 8))
    request_timeout_s: float = field(
        default_factory=lambda: _env_float("LOADGEN_REQUEST_TIMEOUT_S", 10.0)
    )
    max_retries: int = field(default_factory=lambda: _env_int("LOADGEN_MAX_RETRIES", 3))
    #: Live traffic follows the same hour-of-day curve, scaled so the mean
    #: matches `orders_per_minute`. Without this the "live" data is flat and
    #: freshness dashboards look wrong at 04:00.
    follow_hour_curve: bool = field(
        default_factory=lambda: os.environ.get("LOADGEN_FOLLOW_CURVE", "1") == "1"
    )
    weights: dict[str, float] = field(default_factory=lambda: dict(PLAN_WEIGHTS))


def hour_weight(hour: int) -> float:
    """Multiplier relative to a flat day. 1.0 == the daily mean."""
    return HOUR_OF_DAY_CURVE[hour % 24] * 24.0
