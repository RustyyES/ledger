"""Response contracts for the metrics API."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class DateRange(BaseModel):
    """Shared query-parameter validation.

    A reversed range (`from` after `to`) is a 400, not an empty result. Silently
    returning zero rows for a nonsensical range is how a dashboard ends up
    showing a confident, empty chart instead of an error.
    """

    date_from: date
    date_to: date

    @model_validator(mode="after")
    def _ordered(self) -> DateRange:
        if self.date_from > self.date_to:
            raise ValueError(f"'from' ({self.date_from}) is after 'to' ({self.date_to})")
        if (self.date_to - self.date_from).days > 1830:  # ~5 years
            raise ValueError("range exceeds the 5-year maximum")
        return self


class MrrPoint(BaseModel):
    period: str
    period_start: date
    mrr_cents: int
    mrr: float = Field(description="MRR in whole currency units.")
    active_subscriptions: int
    paying_customers: int
    arpa_cents: int | None
    mrr_change_cents: int | None


class MrrResponse(BaseModel):
    granularity: Literal["day", "week", "month"]
    date_from: date
    date_to: date
    points: list[MrrPoint]
    total_points: int


class CohortCell(BaseModel):
    cohort_month: date
    period_number: int
    cohort_size: int
    retained_customers: int
    retention_pct: float
    is_complete_period: bool


class CohortResponse(BaseModel):
    cohort_from: date
    cohort_to: date
    max_periods: int
    cells: list[CohortCell]
    #: Cohorts too young to be observed at every requested period. Surfaced
    #: rather than silently omitted, so a consumer averaging across cohorts
    #: knows the matrix is ragged.
    incomplete_cohorts: list[date]


class RollingPoint(BaseModel):
    date_day: date
    order_count: int
    net_revenue_usd_cents: int
    rolling_net_usd_cents: int
    rolling_avg_net_usd_cents: float
    wow_change_pct: float | None
    is_gap_filled_day: bool


class RollingResponse(BaseModel):
    window: int
    date_from: date
    date_to: date
    points: list[RollingPoint]


class TimelineEvent(BaseModel):
    occurred_at: datetime
    event_category: Literal["order", "payment", "refund", "subscription"]
    event_type: str
    reference_id: str
    amount_cents: int | None
    currency_code: str | None
    detail: str | None


class TimelineResponse(BaseModel):
    customer_id: str
    events: list[TimelineEvent]
    #: Opaque cursor for the next page. Null means the end.
    next_cursor: str | None
    has_more: bool


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded", "stale"]
    service: str
    version: str
    warehouse: Literal["up", "down"]
    data_freshness_hours: float | None
    last_data_at: datetime | None
