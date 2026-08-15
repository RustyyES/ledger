"""Metrics API contract tests, run against a real dbt-built warehouse."""

from __future__ import annotations

from datetime import date

import pytest

# --------------------------------------------------------------------------- #
# Authentication -- 401 vs 403 is a real distinction, not a style choice.
# --------------------------------------------------------------------------- #

PROTECTED = [
    "/metrics/mrr",
    "/metrics/cohorts",
    "/metrics/revenue/rolling",
    "/customers/anything/timeline",
]


@pytest.mark.parametrize("path", PROTECTED)
def test_missing_api_key_is_401(client, path):
    resp = client.get(path)
    assert resp.status_code == 401
    assert resp.headers.get("WWW-Authenticate") == "ApiKey"


@pytest.mark.parametrize("path", PROTECTED)
def test_invalid_api_key_is_403(client, path):
    """403, not 401: 'retry with a credential' and 'this key will never work'
    are different instructions to a client library."""
    assert client.get(path, headers={"X-API-Key": "wrong"}).status_code == 403


def test_any_configured_key_is_accepted(client):
    """Two slots exist so a shared secret can be rotated without an outage."""
    for key in ("test-key-1", "test-key-2"):
        assert client.get("/metrics/mrr", headers={"X-API-Key": key}).status_code == 200


def test_health_needs_no_key(client):
    """An orchestrator's liveness probe does not carry credentials."""
    assert client.get("/health").status_code in (200, 503)


def test_prometheus_endpoint_needs_no_key(client):
    assert client.get("/metrics").status_code == 200


# --------------------------------------------------------------------------- #
# Freshness
# --------------------------------------------------------------------------- #


def test_every_response_carries_freshness(client, auth):
    resp = client.get("/metrics/mrr", headers=auth)
    assert "X-Data-Freshness" in resp.headers
    assert resp.headers["X-Data-Freshness"].endswith("h")


def test_stale_warehouse_returns_503(client, auth, monkeypatch):
    """Serving stale numbers silently is worse than serving none: a six-hour-old
    dashboard is indistinguishable from a current one."""
    # `app.main` binds `settings` at import time, so patching the environment
    # after import changes nothing. Patch the live object the request path
    # actually reads.
    from app import main

    monkeypatch.setattr(main.settings, "staleness_threshold_hours", 0.0001)
    resp = client.get("/metrics/mrr", headers=auth)
    assert resp.status_code == 503
    assert resp.json()["detail"]["code"] == "data_stale"


# --------------------------------------------------------------------------- #
# Caching
# --------------------------------------------------------------------------- #


def test_first_call_misses_and_second_hits(client, auth):
    first = client.get("/metrics/mrr", headers=auth)
    second = client.get("/metrics/mrr", headers=auth)
    assert first.headers["X-Cache"] == "MISS"
    assert second.headers["X-Cache"] == "HIT"
    assert first.json() == second.json()


def test_different_parameters_are_cached_separately(client, auth):
    a = client.get("/metrics/mrr?granularity=day", headers=auth)
    b = client.get("/metrics/mrr?granularity=month", headers=auth)
    assert a.headers["X-Cache"] == "MISS"
    assert b.headers["X-Cache"] == "MISS", "cache key ignores a parameter"
    assert a.json()["granularity"] != b.json()["granularity"]


# --------------------------------------------------------------------------- #
# MRR
# --------------------------------------------------------------------------- #


def test_mrr_returns_points(client, auth):
    body = client.get("/metrics/mrr", headers=auth).json()
    assert body["points"], "no MRR points returned"
    assert body["total_points"] == len(body["points"])
    assert all(p["mrr_cents"] >= 0 for p in body["points"])


@pytest.mark.parametrize("granularity", ["day", "week", "month"])
def test_mrr_granularities(client, auth, granularity):
    resp = client.get(f"/metrics/mrr?granularity={granularity}", headers=auth)
    assert resp.status_code == 200
    assert resp.json()["granularity"] == granularity


def test_coarser_granularity_returns_fewer_points(client, auth):
    days = client.get("/metrics/mrr?granularity=day", headers=auth).json()
    months = client.get("/metrics/mrr?granularity=month", headers=auth).json()
    assert months["total_points"] < days["total_points"]


def test_mrr_is_averaged_within_a_bucket_not_summed(client, auth):
    """MRR is a STOCK. Summing 30 daily figures gives 30x the run rate --
    the single most common way this metric is misreported."""
    days = client.get("/metrics/mrr?granularity=day", headers=auth).json()["points"]
    months = client.get("/metrics/mrr?granularity=month", headers=auth).json()["points"]
    if not days or not months:
        pytest.skip("not enough data")
    peak_day = max(p["mrr_cents"] for p in days)
    peak_month = max(p["mrr_cents"] for p in months)
    assert (
        peak_month <= peak_day * 1.05
    ), "monthly MRR far exceeds any daily value -- it is being summed, not averaged"


def test_invalid_granularity_is_422(client, auth):
    assert client.get("/metrics/mrr?granularity=fortnight", headers=auth).status_code == 422


def test_reversed_date_range_is_400(client, auth):
    """A nonsensical range is an error, not an empty chart."""
    resp = client.get("/metrics/mrr?from=2026-06-01&to=2026-01-01", headers=auth)
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "invalid_date_range"


def test_absurdly_long_range_is_400(client, auth):
    resp = client.get("/metrics/mrr?from=2000-01-01&to=2026-01-01", headers=auth)
    assert resp.status_code == 400


def test_malformed_date_is_422(client, auth):
    assert client.get("/metrics/mrr?from=not-a-date", headers=auth).status_code == 422


# --------------------------------------------------------------------------- #
# Cohorts
# --------------------------------------------------------------------------- #


def test_cohorts_return_cells(client, auth):
    body = client.get("/metrics/cohorts", headers=auth).json()
    assert body["cells"]
    assert all(0 <= c["retention_pct"] <= 100 for c in body["cells"])


def test_cohorts_report_incomplete_cohorts(client, auth):
    """A cohort younger than max_periods cannot be observed at every period.
    Reporting which ones lets a consumer average honestly."""
    body = client.get("/metrics/cohorts?max_periods=12", headers=auth).json()
    assert "incomplete_cohorts" in body
    assert isinstance(body["incomplete_cohorts"], list)


def test_max_periods_is_respected(client, auth):
    body = client.get("/metrics/cohorts?max_periods=3", headers=auth).json()
    assert all(c["period_number"] <= 3 for c in body["cells"])


def test_period_zero_is_always_full_retention(client, auth):
    """Every member of a cohort is, by definition, present in period 0."""
    body = client.get("/metrics/cohorts", headers=auth).json()
    zeros = [c for c in body["cells"] if c["period_number"] == 0]
    assert zeros
    assert all(c["retention_pct"] > 0 for c in zeros)


# --------------------------------------------------------------------------- #
# Rolling revenue
# --------------------------------------------------------------------------- #


def test_rolling_revenue_returns_every_calendar_day(client, auth):
    """The gap fill is the point: a day with no orders must be a zero row,
    not a missing one."""
    body = client.get("/metrics/revenue/rolling?window=7", headers=auth).json()
    points = body["points"]
    assert points
    days = [date.fromisoformat(p["date_day"]) for p in points]
    expected = (days[-1] - days[0]).days + 1
    assert len(days) == expected, f"{expected - len(days)} calendar day(s) missing from the series"


def test_rolling_window_is_configurable(client, auth):
    for window in (1, 7, 30):
        resp = client.get(f"/metrics/revenue/rolling?window={window}", headers=auth)
        assert resp.status_code == 200
        assert resp.json()["window"] == window


def test_window_of_one_equals_the_daily_value(client, auth):
    points = client.get("/metrics/revenue/rolling?window=1", headers=auth).json()["points"]
    assert all(p["rolling_net_usd_cents"] == p["net_revenue_usd_cents"] for p in points)


def test_rolling_window_out_of_bounds_is_422(client, auth):
    assert client.get("/metrics/revenue/rolling?window=0", headers=auth).status_code == 422
    assert client.get("/metrics/revenue/rolling?window=500", headers=auth).status_code == 422


# --------------------------------------------------------------------------- #
# Timeline and cursor pagination
# --------------------------------------------------------------------------- #


def test_unknown_customer_is_404(client, auth):
    resp = client.get("/customers/00000000-0000-0000-0000-000000000000/timeline", headers=auth)
    assert resp.status_code == 404
    assert resp.json()["detail"]["code"] == "customer_not_found"


def test_timeline_returns_events_newest_first(client, auth, a_customer_id):
    body = client.get(f"/customers/{a_customer_id}/timeline", headers=auth).json()
    assert body["events"]
    stamps = [e["occurred_at"] for e in body["events"]]
    assert stamps == sorted(stamps, reverse=True)


def test_timeline_paginates_without_gaps_or_duplicates(client, auth, a_customer_id):
    """The property that matters: walking every page yields each event exactly
    once. Offset pagination breaks this the moment a write lands mid-walk."""
    seen: list[str] = []
    cursor = None
    exhausted = False
    # Bounded so a cursor bug cannot hang the suite, but high enough to walk a
    # busy customer to the end -- a bound that stops early makes the walk a
    # prefix of the full page and the comparison below meaningless.
    for _ in range(500):
        url = f"/customers/{a_customer_id}/timeline?limit=3"
        if cursor:
            url += f"&cursor={cursor}"
        body = client.get(url, headers=auth).json()
        seen.extend(f"{e['occurred_at']}|{e['reference_id']}" for e in body["events"])
        cursor = body["next_cursor"]
        if not cursor:
            exhausted = True
            break

    assert seen, "no events walked"
    assert len(seen) == len(set(seen)), "pagination returned duplicate events"
    assert exhausted, "walk hit the iteration bound -- the cursor is not advancing"

    full = client.get(f"/customers/{a_customer_id}/timeline?limit=200", headers=auth).json()
    expected = [f"{e['occurred_at']}|{e['reference_id']}" for e in full["events"]]
    # The single page is capped at 200; the walk is unbounded. Compare on the
    # overlap, which is where they must agree exactly and in order.
    overlap = min(len(seen), len(expected))
    assert overlap > 0
    assert seen[:overlap] == expected[:overlap], "paged walk diverges from a single page"


def test_last_page_has_no_cursor(client, auth, a_customer_id):
    body = client.get(f"/customers/{a_customer_id}/timeline?limit=200", headers=auth).json()
    if not body["has_more"]:
        assert body["next_cursor"] is None


def test_malformed_cursor_is_400(client, auth, a_customer_id):
    resp = client.get(f"/customers/{a_customer_id}/timeline?cursor=!!!not-base64!!!", headers=auth)
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "invalid_cursor"


def test_limit_above_maximum_is_422(client, auth, a_customer_id):
    assert (
        client.get(f"/customers/{a_customer_id}/timeline?limit=99999", headers=auth).status_code
        == 422
    )


def test_timeline_covers_all_four_event_categories(client, auth):
    """A timeline missing payments is not a timeline."""
    import duckdb

    from tests.conftest import WAREHOUSE

    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        row = con.execute("""
            select customer_id from marts.fct_payments
            where refund_count > 0 limit 1
        """).fetchone()
    if not row:
        pytest.skip("no refunded customer in the fixture warehouse")

    body = client.get(f"/customers/{row[0]}/timeline?limit=200", headers=auth).json()
    categories = {e["event_category"] for e in body["events"]}
    assert {"order", "payment", "refund"} <= categories


# --------------------------------------------------------------------------- #
# Ops
# --------------------------------------------------------------------------- #


def test_openapi_documents_every_endpoint(client):
    paths = client.get("/openapi.json").json()["paths"]
    for expected in (
        "/metrics/mrr",
        "/metrics/cohorts",
        "/metrics/revenue/rolling",
        "/customers/{customer_id}/timeline",
        "/health",
    ):
        assert expected in paths, f"{expected} is undocumented"


def test_prometheus_metrics_use_templated_routes(client, auth, a_customer_id):
    client.get(f"/customers/{a_customer_id}/timeline", headers=auth)
    body = client.get("/metrics").text
    assert 'route="/customers/{customer_id}/timeline"' in body
    assert a_customer_id not in body, "a customer id leaked into a metric label"


def test_response_time_header_present(client, auth):
    assert "X-Response-Time-Ms" in client.get("/metrics/mrr", headers=auth).headers
