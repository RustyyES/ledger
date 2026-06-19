"""Idempotency-Key support for every mutating endpoint.

Contract
--------
* `Idempotency-Key` header is required on POST (configurable off for tests).
* Replay with the same key + same body  -> the stored response, verbatim,
  with `Idempotency-Replayed: true`.
* Replay with the same key + different body -> 422. This is a client bug and
  silently returning the old response would hide it.
* Keys expire after `idempotency_ttl_hours`.

Concurrency
-----------
Two requests with the same key can race. Rather than taking an advisory lock on
the key (which serialises unrelated traffic through one lock table) we let both
do the work and rely on the primary key to arbitrate at COMMIT: the loser gets
an IntegrityError, rolls back its own writes, re-reads the winner's stored
response and replays it. The domain writes and the idempotency row are in the
SAME transaction, so the loser's rollback is total -- there is no window where
a duplicate order survives without a duplicate key row to match it.

That property is the whole point, and it is why `remember()` must be called
before the route commits, not after.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import Depends, Header, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import get_db
from app.errors import IdempotencyConflict, MissingIdempotencyKey
from app.models import IdempotencyKey


class ReplayedResponse(Exception):
    """Raised to short-circuit a route with a previously stored response."""

    def __init__(self, status_code: int, body: dict[str, Any]) -> None:
        super().__init__("idempotent replay")
        self.status_code = status_code
        self.body = body


def _hash_body(body: bytes) -> str:
    """Hash the *canonical* form of the body.

    Re-serialising through json with sorted keys means a client that reorders
    its JSON fields between retries -- which HTTP libraries genuinely do -- is
    still recognised as sending the same request.
    """
    if not body:
        return hashlib.sha256(b"").hexdigest()
    try:
        canonical = json.dumps(json.loads(body), sort_keys=True, separators=(",", ":")).encode()
    except (json.JSONDecodeError, UnicodeDecodeError):
        canonical = body
    return hashlib.sha256(canonical).hexdigest()


class IdempotencyGuard:
    """Per-request handle. Obtained via the `idempotency` dependency."""

    def __init__(
        self,
        session: Session,
        key: str | None,
        endpoint: str,
        request_hash: str,
        response: Response,
    ) -> None:
        self.session = session
        self.key = key
        self.endpoint = endpoint
        self.request_hash = request_hash
        self._response = response

    # -- read path ----------------------------------------------------------
    def check_replay(self) -> None:
        """Raise ReplayedResponse if this exact request was already served."""
        if self.key is None:
            return
        settings = get_settings()
        cutoff = datetime.now(UTC) - timedelta(hours=settings.idempotency_ttl_hours)
        row = self.session.execute(
            select(IdempotencyKey).where(
                IdempotencyKey.key == self.key,
                IdempotencyKey.endpoint == self.endpoint,
                IdempotencyKey.created_at >= cutoff,
            )
        ).scalar_one_or_none()

        if row is None:
            return
        if row.request_hash != self.request_hash:
            raise IdempotencyConflict(
                "Idempotency-Key was already used for this endpoint with a "
                "different request body",
                field="Idempotency-Key",
            )
        raise ReplayedResponse(row.response_status, row.response_body)

    # -- write path ---------------------------------------------------------
    def remember(self, status_code: int, body: Any) -> None:
        """Persist the response. MUST be called before the route commits."""
        if self.key is None:
            return
        self.session.add(
            IdempotencyKey(
                key=self.key,
                endpoint=self.endpoint,
                request_hash=self.request_hash,
                response_status=status_code,
                response_body=jsonable(body),
            )
        )

    def commit_or_replay(self, status_code: int, body: Any) -> Any:
        """Commit the transaction, resolving a concurrent-duplicate race.

        Returns the body to serve -- normally the caller's own, or the winner's
        stored body if we lost the race.
        """
        self.remember(status_code, body)
        try:
            self.session.commit()
        except IntegrityError as exc:
            if not _is_idempotency_pk_violation(exc):
                raise
            # We lost the race. Our domain writes are gone with the rollback.
            self.session.rollback()
            row = self.session.execute(
                select(IdempotencyKey).where(
                    IdempotencyKey.key == self.key,
                    IdempotencyKey.endpoint == self.endpoint,
                )
            ).scalar_one_or_none()
            if row is None:  # pragma: no cover - would mean the PK lied
                raise
            raise ReplayedResponse(row.response_status, row.response_body) from exc
        return body

    def mark_fresh(self) -> None:
        self._response.headers["Idempotency-Replayed"] = "false"


def _is_idempotency_pk_violation(exc: IntegrityError) -> bool:
    constraint = getattr(getattr(exc.orig, "diag", None), "constraint_name", "") or ""
    return "idempotency" in constraint.lower()


def jsonable(obj: Any) -> Any:
    """Minimal JSON coercion for storage in JSONB."""
    from fastapi.encoders import jsonable_encoder

    return jsonable_encoder(obj)


async def idempotency(
    request: Request,
    response: Response,
    session: Session = Depends(get_db),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> IdempotencyGuard:
    settings = get_settings()
    if settings.require_idempotency_key and request.method == "POST" and not idempotency_key:
        raise MissingIdempotencyKey(
            "POST requests require an Idempotency-Key header",
            field="Idempotency-Key",
        )
    if idempotency_key is not None and not (8 <= len(idempotency_key) <= 255):
        raise MissingIdempotencyKey(
            "Idempotency-Key must be between 8 and 255 characters",
            field="Idempotency-Key",
        )

    body = await request.body()

    # The TEMPLATED route, falling back to the concrete path. Scoping keys per
    # route template means a client's own counter cannot collide across
    # endpoints, and the template (not the path) keeps `/orders/{id}/payments`
    # one endpoint rather than one per order.
    route = request.scope.get("route")
    route_path = getattr(route, "path", None) or request.url.path

    guard = IdempotencyGuard(
        session=session,
        key=idempotency_key,
        endpoint=f"{request.method} {route_path}",
        request_hash=_hash_body(body),
        response=response,
    )
    guard.check_replay()
    guard.mark_fresh()
    return guard


def install_replay_handler(app) -> None:
    @app.exception_handler(ReplayedResponse)
    async def _replay(_request: Request, exc: ReplayedResponse) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.body,
            headers={"Idempotency-Replayed": "true"},
        )


def purge_expired(session: Session) -> int:
    """Housekeeping, called by the quality DAG. Returns rows removed."""
    settings = get_settings()
    cutoff = datetime.now(UTC) - timedelta(hours=settings.idempotency_ttl_hours)
    result = session.execute(delete(IdempotencyKey).where(IdempotencyKey.created_at < cutoff))
    session.commit()
    # `rowcount` lives on CursorResult, which is what a DELETE returns; the
    # generic Result type does not declare it.
    return getattr(result, "rowcount", 0) or 0
