"""API key authentication.

The spec asks for 401 on missing and 403 on invalid, which is worth a note
because the two are routinely conflated:

    401 Unauthorized  -- "I do not know who you are." No credential presented.
    403 Forbidden     -- "I know who you are and the answer is still no."

Distinguishing them matters to a client library: 401 means "attach a
credential and retry", 403 means "stop retrying, this key will never work".
Returning 403 for both makes a missing-header bug look like a permissions
problem and sends whoever is debugging to the wrong team.
"""

from __future__ import annotations

import hmac

from fastapi import Header, HTTPException, Request, status

from app.config import get_settings


async def require_api_key(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    settings = get_settings()

    if x_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "api_key_required", "message": "X-API-Key header is required"},
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # Constant-time comparison against every valid key. `==` on a secret leaks
    # its prefix through timing; it is a small leak and an entirely avoidable
    # one. Comparing against ALL keys rather than short-circuiting on the first
    # match keeps the timing independent of which key was used.
    matched = False
    for candidate in settings.valid_api_keys:
        if hmac.compare_digest(x_api_key, candidate):
            matched = True

    if not matched:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"code": "api_key_invalid", "message": "the supplied API key is not valid"},
        )

    request.state.api_key_suffix = x_api_key[-4:]
    return x_api_key
