"""Database engine, session lifecycle and transactional helpers."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings

_settings = get_settings()

engine: Engine = create_engine(
    _settings.sqlalchemy_url,
    pool_size=_settings.db_pool_size,
    max_overflow=_settings.db_max_overflow,
    pool_timeout=_settings.db_pool_timeout_s,
    pool_pre_ping=True,  # survives Postgres restarts / connection reaping
    future=True,
)


@event.listens_for(engine, "connect")
def _set_connection_defaults(dbapi_conn, _record) -> None:
    """Bound every statement and pin the session timezone to UTC.

    Pinning UTC matters more than it looks: the load generator deliberately
    writes some naive local timestamps, and we do not want the server's
    TimeZone GUC silently reinterpreting them differently per container.
    """
    with dbapi_conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = {_settings.db_statement_timeout_ms}")
        cur.execute("SET timezone = 'UTC'")
        cur.execute("SET idle_in_transaction_session_timeout = 30000")


SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,  # so response serialisation after commit does not re-query
    future=True,
)


def get_db() -> Iterator[Session]:
    """FastAPI dependency. One session per request, always closed.

    Commit is the route's responsibility, not this dependency's: routes that
    write need to commit *before* building the response so the idempotency
    record and the domain rows land in the same transaction.
    """
    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextlib.contextmanager
def session_scope() -> Iterator[Session]:
    """Transactional scope for non-request callers (CLI, tests, loadgen)."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def healthcheck() -> bool:
    """Cheap liveness probe against the pool."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
