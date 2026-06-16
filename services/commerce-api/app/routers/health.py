"""Liveness and readiness.

`/health` is deliberately cheap: it touches the connection pool and nothing
else. A health check that runs a real query against a 5M-row table is how you
turn a slow database into an outage, because the orchestrator starts killing
healthy containers exactly when they are least able to spare the capacity.
"""

from __future__ import annotations

from fastapi import APIRouter, Response, status

from app.config import get_settings
from app.db import healthcheck
from app.schemas import HealthOut

router = APIRouter(tags=["ops"])

VERSION = "1.0.0"


@router.get("/health", response_model=HealthOut)
def health(response: Response) -> HealthOut:
    db_up = healthcheck()
    if not db_up:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return HealthOut(
        status="ok" if db_up else "degraded",
        service=get_settings().service_name,
        version=VERSION,
        database="up" if db_up else "down",
    )
