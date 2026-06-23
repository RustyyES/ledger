"""Commerce API -- the OLTP source system.

This service exists to be a realistic *upstream*. It is a normal transactional
application: it does not know a warehouse exists, it does not emit analytics
events, and it makes none of the accommodations an analytics team would ask
for. Everything the pipeline needs, the pipeline has to take from the WAL.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Response
from fastapi.openapi.utils import get_openapi
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.config import get_settings
from app.db import healthcheck
from app.errors import install_error_handlers
from app.idempotency import install_replay_handler
from app.logging_config import configure_logging
from app.middleware import RequestContextMiddleware
from app.routers import customers, health, orders, payments, plans, subscriptions

log = structlog.get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    configure_logging(settings.log_level, json_output=settings.env != "local")
    log.info(
        "startup",
        service=settings.service_name,
        env=settings.env,
        database_reachable=healthcheck(),
    )
    yield
    log.info("shutdown", service=settings.service_name)


app = FastAPI(
    title="Ledger Commerce API",
    version="1.0.0",
    description=(
        "Transactional commerce service backing the Ledger platform.\n\n"
        "**All POST endpoints require an `Idempotency-Key` header.** Replaying "
        "a key with an identical body returns the original response and sets "
        "`Idempotency-Replayed: true`; replaying it with a different body is a "
        "422."
    ),
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(RequestContextMiddleware)
install_error_handlers(app)
install_replay_handler(app)

app.include_router(health.router)
app.include_router(customers.router)
app.include_router(plans.router)
app.include_router(subscriptions.router)
app.include_router(orders.router)
app.include_router(payments.order_payments)
app.include_router(payments.payment_refunds)


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def custom_openapi() -> dict:
    """Document the idempotency header once, on every POST, automatically."""
    if app.openapi_schema:
        return app.openapi_schema

    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    header = {
        "name": "Idempotency-Key",
        "in": "header",
        "required": True,
        "description": "Client-generated unique key, 8-255 chars. Safe to retry.",
        "schema": {"type": "string", "minLength": 8, "maxLength": 255},
    }
    for path_item in schema.get("paths", {}).values():
        if "post" in path_item:
            path_item["post"].setdefault("parameters", []).append(header)
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi  # type: ignore[method-assign]
