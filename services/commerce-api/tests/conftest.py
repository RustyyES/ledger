"""Test fixtures.

These tests run against a real Postgres, never SQLite. The schema uses JSONB,
partial indexes, `SELECT ... FOR UPDATE` and REPLICA IDENTITY; a SQLite stand-in
would pass while testing something that is not the system. `make test` and CI
both provide the database.

Isolation is per-test via an outer transaction that is rolled back, rather than
by recreating the schema each time. That keeps the suite fast enough to run on
every save and, more importantly, means a test that forgets to clean up cannot
leak into the next one.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest

os.environ.setdefault(
    "COMMERCE_DATABASE_URL",
    os.environ.get(
        "COMMERCE_TEST_DATABASE_URL",
        "postgresql+psycopg://ledger:ledger@localhost:5433/ledger_test",
    ),
)
os.environ.setdefault("COMMERCE_ENV", "test")
os.environ.setdefault("COMMERCE_LOG_LEVEL", "WARNING")

from app.db import engine, get_db
from app.main import app
from app.models import Base, Plan
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session

PLAN_FIXTURES = [
    ("legacy_starter", 900, False),
    ("basic", 1900, True),
    ("pro", 4900, True),
    ("enterprise", 19900, True),
]


@pytest.fixture(scope="session", autouse=True)
def _schema() -> Iterator[None]:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        for table in ("customers", "subscriptions", "orders", "payments", "refunds"):
            conn.execute(text(f"ALTER TABLE {table} REPLICA IDENTITY FULL"))
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture
def session(_schema) -> Iterator[Session]:
    """A session bound to a transaction that is always rolled back."""
    connection = engine.connect()
    transaction = connection.begin()
    db = Session(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint")
    try:
        yield db
    finally:
        db.close()
        transaction.rollback()
        connection.close()


@pytest.fixture
def plans(session: Session) -> dict[str, Plan]:
    created: dict[str, Plan] = {}
    for code, cents, active in PLAN_FIXTURES:
        plan = Plan(code=code, monthly_cents=cents, currency="USD", active=active)
        session.add(plan)
        created[code] = plan
    session.flush()
    return created


@pytest.fixture
def client(session: Session, plans: dict[str, Plan]) -> Iterator[TestClient]:
    """TestClient whose requests share the test's rolled-back transaction."""

    def _override() -> Iterator[Session]:
        yield session

    app.dependency_overrides[get_db] = _override
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def idem() -> IdemFactory:
    return IdemFactory()


class IdemFactory:
    """Generates fresh Idempotency-Key headers, or reuses one on demand."""

    def fresh(self) -> dict[str, str]:
        return {"Idempotency-Key": f"test-{uuid.uuid4()}"}

    def fixed(self, key: str) -> dict[str, str]:
        return {"Idempotency-Key": key}


@pytest.fixture
def customer(client, idem) -> dict:
    resp = client.post(
        "/customers",
        json={
            "email": f"{uuid.uuid4().hex[:12]}@example.com",
            "name": "Test Customer",
            "country_code": "EG",
            "timezone": "Africa/Cairo",
        },
        headers=idem.fresh(),
    )
    assert resp.status_code == 201, resp.text
    return resp.json()
