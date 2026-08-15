"""Metrics API fixtures.

Tests run against a REAL DuckDB warehouse built by dbt, not against mocks. The
whole job of this service is translating warehouse SQL into HTTP, so a mocked
warehouse tests the translation of a fiction. `make test-metrics` builds a
small warehouse first; CI does the same.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

WAREHOUSE = os.environ.get(
    "METRICS_WAREHOUSE_PATH",
    str(Path(__file__).resolve().parents[3] / "ci_warehouse.duckdb"),
)
os.environ["METRICS_WAREHOUSE_PATH"] = WAREHOUSE
os.environ.setdefault("METRICS_API_KEYS", "test-key-1,test-key-2")
# Generous: the fixture warehouse is built once and then sits still, so its
# newest fact ages during the run. A tight threshold would make the suite fail
# by the clock rather than by any defect.
os.environ.setdefault("METRICS_STALENESS_THRESHOLD_HOURS", "87600")

from app.main import app  # noqa: E402
from app.warehouse import cache, reset_freshness_cache  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _require_warehouse():
    if not Path(WAREHOUSE).exists():
        pytest.skip(
            f"no warehouse at {WAREHOUSE}; run `make warehouse` or set " "METRICS_WAREHOUSE_PATH"
        )


@pytest.fixture
def client() -> TestClient:
    cache.clear()  # a cached response from a previous test is a false pass
    reset_freshness_cache()
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def auth() -> dict[str, str]:
    return {"X-API-Key": "test-key-1"}


@pytest.fixture
def a_customer_id(client, auth) -> str:
    import duckdb

    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        row = con.execute("select customer_id from marts.fct_orders limit 1").fetchone()
    assert row, "warehouse has no orders"
    return row[0]
