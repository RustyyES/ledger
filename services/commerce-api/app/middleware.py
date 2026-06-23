"""Request-scoped instrumentation: correlation id, timing, Prometheus."""

from __future__ import annotations

import time
import uuid

import structlog
from prometheus_client import Counter, Histogram
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

log = structlog.get_logger(__name__)

REQUESTS = Counter(
    "commerce_http_requests_total",
    "HTTP requests handled by the commerce API",
    labelnames=("method", "route", "status"),
)
LATENCY = Histogram(
    "commerce_http_request_duration_seconds",
    "Request latency",
    labelnames=("method", "route"),
    # Buckets chosen around the SLO (p95 < 200ms), not the library default,
    # which wastes resolution above 1s where we have no traffic.
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 5.0),
)


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = request_id
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)

        started = time.perf_counter()
        try:
            response: Response = await call_next(request)
        except Exception:
            elapsed = time.perf_counter() - started
            route = _route_of(request)
            REQUESTS.labels(request.method, route, "500").inc()
            LATENCY.labels(request.method, route).observe(elapsed)
            log.exception(
                "request_failed",
                method=request.method,
                route=route,
                duration_ms=round(elapsed * 1000, 2),
            )
            raise

        elapsed = time.perf_counter() - started
        route = _route_of(request)
        REQUESTS.labels(request.method, route, str(response.status_code)).inc()
        LATENCY.labels(request.method, route).observe(elapsed)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time-Ms"] = f"{elapsed * 1000:.2f}"

        log.info(
            "request",
            method=request.method,
            route=route,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(elapsed * 1000, 2),
        )
        return response


def _route_of(request: Request) -> str:
    """Templated path, never the concrete one.

    Labelling metrics with `/orders/9f2c...` instead of `/orders/{order_id}`
    produces one time series per order and takes Prometheus down. This is the
    single most common way a first metrics integration causes an incident.
    """
    route = request.scope.get("route")
    return getattr(route, "path", None) or "unmatched"
