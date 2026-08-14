"""Metrics API -- the serving layer over the warehouse marts.

Read-only. It publishes what dbt built and adds nothing to it: no metric is
computed here that is not already a column in a mart. That is a deliberate
boundary. A metric computed in the API is a metric the dbt tests do not cover,
that the lineage graph does not show, and that quietly disagrees with the
warehouse the first time somebody changes one and not the other.

Every response carries `X-Data-Freshness`, and the API returns 503 rather than
serving numbers that are stale beyond the threshold. Serving stale data
silently is worse than serving none: a six-hour-old dashboard looks exactly
like a current one.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import date, timedelta
from typing import Literal, TypeVar, cast

import duckdb
import structlog
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, Response, status
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from app.config import get_settings
from app.schemas import (
    CohortCell,
    CohortResponse,
    DateRange,
    HealthResponse,
    MrrPoint,
    MrrResponse,
    RollingPoint,
    RollingResponse,
    TimelineEvent,
    TimelineResponse,
)
from app.security import require_api_key
from app.timeline import build_timeline_query, decode_cursor, encode_cursor
from app.warehouse import cache, data_freshness, query

log = structlog.get_logger("metrics-api")
settings = get_settings()
VERSION = "1.0.0"

REQUESTS = Counter("metrics_api_requests_total", "Requests", ["route", "status"])
LATENCY = Histogram(
    "metrics_api_request_duration_seconds",
    "Latency",
    ["route"],
    buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0),
)
CACHE_EVENTS = Counter("metrics_api_cache_total", "Cache outcomes", ["outcome"])


@asynccontextmanager
async def lifespan(_app: FastAPI):
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.stdlib.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )
    last, hours = data_freshness()
    log.info("startup", warehouse=settings.warehouse_path, freshness_hours=hours)
    yield


app = FastAPI(
    title="Ledger Metrics API",
    version=VERSION,
    description=(
        "Read-only serving layer over the Ledger warehouse.\n\n"
        "**Authentication:** every endpoint except `/health` and `/metrics` "
        "requires an `X-API-Key` header. Missing key -> 401, invalid key -> 403.\n\n"
        "**Freshness:** every response carries `X-Data-Freshness` (hours since "
        "the newest fact). If the warehouse is stale beyond the configured "
        "threshold the API returns 503 rather than serving numbers that look "
        "current and are not."
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def instrument(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    route = getattr(request.scope.get("route"), "path", "unmatched")
    elapsed = time.perf_counter() - started
    REQUESTS.labels(route, str(response.status_code)).inc()
    LATENCY.labels(route).observe(elapsed)
    response.headers["X-Response-Time-Ms"] = f"{elapsed * 1000:.2f}"
    return response


def _freshness_headers(response: Response) -> float | None:
    last, hours = data_freshness()
    response.headers["X-Data-Freshness"] = "unknown" if hours is None else f"{hours:.2f}h"
    if last is not None:
        response.headers["X-Data-Last-Updated"] = last.isoformat()
    return hours


def guard_freshness(response: Response) -> float | None:
    """503 when the warehouse is too far behind to be worth serving."""
    hours = _freshness_headers(response)
    if hours is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "warehouse_empty", "message": "the warehouse has no data yet"},
        )
    if hours > settings.staleness_threshold_hours:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "data_stale",
                "message": (
                    f"data is {hours:.1f}h old, beyond the "
                    f"{settings.staleness_threshold_hours}h threshold"
                ),
            },
        )
    return hours


def parse_range(
    date_from: date | None, date_to: date | None, *, default_days: int = 90
) -> DateRange:
    """Validate the range, turning a bad one into a 400 rather than empty rows."""
    resolved_to = date_to or date.today()
    resolved_from = date_from or (resolved_to - timedelta(days=default_days))
    try:
        return DateRange(date_from=resolved_from, date_to=resolved_to)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "invalid_date_range", "message": str(exc)},
        ) from exc


T = TypeVar("T")


def cached(response: Response, key: str, producer: Callable[[], T]) -> T:
    """Serve from cache when possible; always set X-Cache.

    Generic in the producer's return type so callers keep their real response
    type instead of collapsing to Any -- which is what let a `str` reach a
    `Literal[...]` field undetected before this was annotated.
    """
    hit = cache.get(key)
    if hit is not None:
        response.headers["X-Cache"] = "HIT"
        CACHE_EVENTS.labels("hit").inc()
        return cast(T, hit)
    value: T = producer()
    cache.set(key, value)
    response.headers["X-Cache"] = "MISS"
    CACHE_EVENTS.labels("miss").inc()
    return value


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #

GRANULARITY_SQL = {
    "day": "date_day",
    "week": "cast(date_trunc('week',  date_day) as date)",
    "month": "cast(date_trunc('month', date_day) as date)",
}


@app.get("/metrics/mrr", response_model=MrrResponse, tags=["metrics"])
def get_mrr(
    response: Response,
    date_from: date | None = Query(default=None, alias="from"),
    date_to: date | None = Query(default=None, alias="to"),
    granularity: str = Query(default="day", pattern="^(day|week|month)$"),
    _key: str = Depends(require_api_key),
) -> MrrResponse:
    guard_freshness(response)
    rng = parse_range(date_from, date_to)

    def produce() -> MrrResponse:
        bucket = GRANULARITY_SQL[granularity]
        # Averaged within the bucket, not summed. MRR is a STOCK -- a value that
        # exists on each day -- so summing 30 daily MRR figures gives you 30x
        # the monthly run rate, which is the single most common way this metric
        # is misreported.
        rows = query(
            f"""
            select
                {bucket}                            as period_start,
                avg(mrr_cents)                      as mrr_cents,
                avg(active_subscriptions)           as active_subscriptions,
                avg(paying_customers)               as paying_customers,
                avg(arpa_cents)                     as arpa_cents,
                sum(mrr_change_cents)               as mrr_change_cents
            from marts.fct_mrr_daily
            where date_day between ? and ?
            group by 1
            order by 1
            """,
            [rng.date_from, rng.date_to],
        )
        points = [
            MrrPoint(
                period=str(r["period_start"]),
                period_start=r["period_start"],
                mrr_cents=int(r["mrr_cents"] or 0),
                mrr=round(float(r["mrr_cents"] or 0) / 100, 2),
                active_subscriptions=int(r["active_subscriptions"] or 0),
                paying_customers=int(r["paying_customers"] or 0),
                arpa_cents=int(r["arpa_cents"]) if r["arpa_cents"] is not None else None,
                mrr_change_cents=int(r["mrr_change_cents"])
                if r["mrr_change_cents"] is not None
                else None,
            )
            for r in rows
        ]
        # The Query pattern already constrains this to the three values; the
        # cast tells the type checker what the regex guarantees.
        return MrrResponse(
            granularity=cast(Literal["day", "week", "month"], granularity),
            date_from=rng.date_from,
            date_to=rng.date_to,
            points=points,
            total_points=len(points),
        )

    return cached(response, cache.key("mrr", rng.date_from, rng.date_to, granularity), produce)


@app.get("/metrics/cohorts", response_model=CohortResponse, tags=["metrics"])
def get_cohorts(
    response: Response,
    cohort_from: date | None = Query(default=None),
    cohort_to: date | None = Query(default=None),
    max_periods: int = Query(default=12, ge=1, le=36),
    _key: str = Depends(require_api_key),
) -> CohortResponse:
    guard_freshness(response)
    rng = parse_range(cohort_from, cohort_to, default_days=540)

    def produce() -> CohortResponse:
        rows = query(
            """
            select cohort_month, period_number, cohort_size, retained_customers,
                   retention_pct, is_complete_period, months_observable
            from marts.fct_cohort_retention
            where cohort_month between ? and ?
              and period_number <= ?
            order by cohort_month, period_number
            """,
            [rng.date_from, rng.date_to, max_periods],
        )
        cells = [
            CohortCell(
                cohort_month=r["cohort_month"],
                period_number=int(r["period_number"]),
                cohort_size=int(r["cohort_size"]),
                retained_customers=int(r["retained_customers"]),
                retention_pct=float(r["retention_pct"]),
                is_complete_period=bool(r["is_complete_period"]),
            )
            for r in rows
        ]
        # A cohort younger than `max_periods` cannot be observed at every
        # requested period. Reporting which ones lets a consumer average
        # honestly instead of averaging in structural zeroes.
        incomplete = sorted(
            {r["cohort_month"] for r in rows if int(r["months_observable"]) < max_periods}
        )
        return CohortResponse(
            cohort_from=rng.date_from,
            cohort_to=rng.date_to,
            max_periods=max_periods,
            cells=cells,
            incomplete_cohorts=incomplete,
        )

    return cached(response, cache.key("cohorts", rng.date_from, rng.date_to, max_periods), produce)


@app.get("/metrics/revenue/rolling", response_model=RollingResponse, tags=["metrics"])
def get_rolling_revenue(
    response: Response,
    window: int = Query(default=7, ge=1, le=90),
    date_from: date | None = Query(default=None, alias="from"),
    date_to: date | None = Query(default=None, alias="to"),
    _key: str = Depends(require_api_key),
) -> RollingResponse:
    guard_freshness(response)
    rng = parse_range(date_from, date_to)

    def produce() -> RollingResponse:
        # The 7 and 28 day windows are precomputed in the mart. Any other window
        # is computed here over the ALREADY GAP-FILLED series -- which is the
        # part that matters. Computing it from raw daily revenue would reproduce
        # the exact bug fct_revenue_rolling exists to avoid, because a window
        # over rows is not a window over days when days are missing.
        rows = query(
            """
            select date_day, order_count, net_revenue_usd_cents,
                   rolling_7d_net_usd_cents, rolling_28d_net_usd_cents,
                   wow_change_pct, is_gap_filled_day,
                   sum(net_revenue_usd_cents) over (
                       order by date_day rows between ? preceding and current row
                   ) as rolling_custom_cents,
                   avg(net_revenue_usd_cents) over (
                       order by date_day rows between ? preceding and current row
                   ) as rolling_custom_avg
            from marts.fct_revenue_rolling
            where date_day between ? and ?
            order by date_day
            """,
            [window - 1, window - 1, rng.date_from, rng.date_to],
        )
        points = [
            RollingPoint(
                date_day=r["date_day"],
                order_count=int(r["order_count"]),
                net_revenue_usd_cents=int(r["net_revenue_usd_cents"]),
                rolling_net_usd_cents=int(r["rolling_custom_cents"] or 0),
                rolling_avg_net_usd_cents=round(float(r["rolling_custom_avg"] or 0), 2),
                wow_change_pct=float(r["wow_change_pct"])
                if r["wow_change_pct"] is not None
                else None,
                is_gap_filled_day=bool(r["is_gap_filled_day"]),
            )
            for r in rows
        ]
        return RollingResponse(
            window=window, date_from=rng.date_from, date_to=rng.date_to, points=points
        )

    return cached(response, cache.key("rolling", window, rng.date_from, rng.date_to), produce)


@app.get("/customers/{customer_id}/timeline", response_model=TimelineResponse, tags=["customers"])
def get_customer_timeline(
    response: Response,
    customer_id: str = Path(min_length=1, max_length=64),
    cursor: str | None = Query(default=None, description="Opaque cursor from a previous page."),
    limit: int = Query(default=None, ge=1, le=200),
    _key: str = Depends(require_api_key),
) -> TimelineResponse:
    guard_freshness(response)
    page_size = limit or settings.default_page_size

    exists = query("select 1 from marts.dim_customer where customer_id = ? limit 1", [customer_id])
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "customer_not_found", "message": f"no customer with id {customer_id}"},
        )

    decoded = None
    if cursor:
        try:
            decoded = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "invalid_cursor", "message": str(exc)},
            ) from exc

    sql, params = build_timeline_query(decoded, customer_id, page_size)
    rows = query(sql, params)

    # We asked for page_size + 1. The extra row is not returned -- it only tells
    # us whether a further page exists, which is how you avoid an empty final
    # page and a `total_count` that requires a second, expensive query.
    has_more = len(rows) > page_size
    rows = rows[:page_size]

    events = [
        TimelineEvent(
            occurred_at=r["occurred_at"],
            event_category=r["event_category"],
            event_type=r["event_type"],
            reference_id=str(r["reference_id"]),
            amount_cents=int(r["amount_cents"]) if r["amount_cents"] is not None else None,
            currency_code=r["currency_code"],
            detail=r["detail"],
        )
        for r in rows
    ]
    next_cursor = (
        encode_cursor(events[-1].occurred_at, events[-1].reference_id)
        if events and has_more
        else None
    )
    response.headers["X-Cache"] = "BYPASS"  # per-customer, rarely re-read
    return TimelineResponse(
        customer_id=customer_id,
        events=events,
        next_cursor=next_cursor,
        has_more=has_more,
    )


# --------------------------------------------------------------------------- #
# Ops
# --------------------------------------------------------------------------- #


@app.get("/health", response_model=HealthResponse, tags=["ops"])
def health(response: Response) -> HealthResponse:
    """Unauthenticated on purpose -- an orchestrator's probe has no API key."""
    try:
        query("select 1 as ok")
        warehouse_up = True
    except duckdb.Error:
        warehouse_up = False

    last, hours = data_freshness()
    response.headers["X-Data-Freshness"] = "unknown" if hours is None else f"{hours:.2f}h"

    state: Literal["ok", "degraded", "stale"]
    if not warehouse_up:
        state, code = "degraded", status.HTTP_503_SERVICE_UNAVAILABLE
    elif hours is not None and hours > settings.staleness_threshold_hours:
        state, code = "stale", status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        state, code = "ok", status.HTTP_200_OK
    response.status_code = code

    return HealthResponse(
        status=state,
        service=settings.service_name,
        version=VERSION,
        warehouse="up" if warehouse_up else "down",
        data_freshness_hours=round(hours, 2) if hours is not None else None,
        last_data_at=last,
    )


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.exception_handler(duckdb.Error)
async def _warehouse_error(request: Request, exc: duckdb.Error) -> JSONResponse:
    """A warehouse mid-rebuild is a 503, not a 500.

    dbt swaps tables during a build; a query landing in that window sees a
    missing relation. That is transient and retryable, and telling the client
    so is the difference between a retry and a page.
    """
    log.warning("warehouse_error", path=request.url.path, error=str(exc))
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": {"code": "warehouse_unavailable", "message": str(exc)[:400]}},
    )
