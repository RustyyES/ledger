"""Uniform error envelope.

Every non-2xx response from this service has the same shape:

    {"error": {"code": "...", "message": "...", "field": "..."}, "request_id": "..."}

That uniformity is not cosmetic. The load generator runs unattended for weeks;
when it logs a failure we need the failing field and a request id we can grep
the application log for, without a human reading a stack trace.
"""

from __future__ import annotations

import structlog
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

log = structlog.get_logger(__name__)


class DomainError(Exception):
    """Base for expected, client-visible failures."""

    status_code: int = status.HTTP_400_BAD_REQUEST
    code: str = "domain_error"

    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.field = field


class NotFound(DomainError):
    status_code = status.HTTP_404_NOT_FOUND
    code = "not_found"


class Conflict(DomainError):
    """The request is well-formed but the resource is in the wrong state."""

    status_code = status.HTTP_409_CONFLICT
    code = "conflict"


class UnprocessableState(DomainError):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    code = "unprocessable"


class IdempotencyConflict(DomainError):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    code = "idempotency_key_reused_with_different_body"


class MissingIdempotencyKey(DomainError):
    status_code = status.HTTP_400_BAD_REQUEST
    code = "idempotency_key_required"


def _envelope(request: Request, *, code: str, message: str, field: str | None = None) -> dict:
    return {
        "error": {"code": code, "message": message, "field": field},
        "request_id": getattr(request.state, "request_id", "unknown"),
    }


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def _domain(request: Request, exc: DomainError) -> JSONResponse:
        log.info(
            "domain_error",
            code=exc.code,
            message=exc.message,
            path=request.url.path,
            request_id=getattr(request.state, "request_id", None),
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=_envelope(request, code=exc.code, message=exc.message, field=exc.field),
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Surface the *first* offending field explicitly. FastAPI's default
        # body is a list of dicts, which is fine for humans and useless for a
        # machine client deciding what to retry.
        errors = exc.errors()
        first = errors[0] if errors else {}
        loc = [str(p) for p in first.get("loc", []) if p not in ("body", "query", "path")]
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                **_envelope(
                    request,
                    code="validation_error",
                    message=first.get("msg", "request validation failed"),
                    field=".".join(loc) or None,
                ),
                "details": [
                    {
                        "field": ".".join(
                            str(p) for p in e.get("loc", []) if p not in ("body", "query", "path")
                        ),
                        "message": e.get("msg", ""),
                        "type": e.get("type", ""),
                    }
                    for e in errors
                ],
            },
        )

    @app.exception_handler(IntegrityError)
    async def _integrity(request: Request, exc: IntegrityError) -> JSONResponse:
        # A unique/FK/check violation that slipped past validation. Almost
        # always a race (two concurrent signups on the same email) rather than
        # a bug, so 409 rather than 500.
        constraint = getattr(getattr(exc.orig, "diag", None), "constraint_name", None)
        log.warning("integrity_error", constraint=constraint, path=request.url.path)
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=_envelope(
                request,
                code="constraint_violation",
                message=f"database constraint violated: {constraint or 'unknown'}",
            ),
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled_error", path=request.url.path, error=str(exc))
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_envelope(
                request, code="internal_error", message="an unexpected error occurred"
            ),
        )
