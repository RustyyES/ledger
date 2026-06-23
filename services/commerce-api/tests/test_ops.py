"""Health, metrics and the OpenAPI contract."""

from __future__ import annotations


def test_health_reports_database_up(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["database"] == "up"


def test_every_response_carries_a_request_id(client):
    resp = client.get("/health")
    assert resp.headers["X-Request-ID"]
    assert resp.headers["X-Response-Time-Ms"]


def test_supplied_request_id_is_echoed_back(client):
    resp = client.get("/health", headers={"X-Request-ID": "trace-me-123"})
    assert resp.headers["X-Request-ID"] == "trace-me-123"


def test_openapi_documents_all_fifteen_endpoints(client):
    schema = client.get("/openapi.json").json()
    operations = {
        (method.upper(), path)
        for path, item in schema["paths"].items()
        for method in item
        if method in {"get", "post", "patch", "delete"}
    }
    expected = {
        ("POST", "/customers"),
        ("GET", "/customers/{customer_id}"),
        ("PATCH", "/customers/{customer_id}"),
        ("DELETE", "/customers/{customer_id}"),
        ("GET", "/plans"),
        ("POST", "/subscriptions"),
        ("PATCH", "/subscriptions/{subscription_id}/plan"),
        ("POST", "/subscriptions/{subscription_id}/cancel"),
        ("POST", "/subscriptions/{subscription_id}/pause"),
        ("POST", "/subscriptions/{subscription_id}/resume"),
        ("POST", "/orders"),
        ("GET", "/orders/{order_id}"),
        ("POST", "/orders/{order_id}/payments"),
        ("POST", "/payments/{payment_id}/refunds"),
        ("GET", "/health"),
    }
    missing = expected - operations
    assert not missing, f"missing documented endpoints: {sorted(missing)}"
    assert len(expected) == 15


def test_openapi_marks_idempotency_key_required_on_every_post(client):
    schema = client.get("/openapi.json").json()
    for path, item in schema["paths"].items():
        if "post" not in item:
            continue
        names = {p["name"] for p in item["post"].get("parameters", [])}
        assert "Idempotency-Key" in names, f"POST {path} does not document the header"


def test_metrics_endpoint_exposes_prometheus_text(client):
    client.get("/health")
    body = client.get("/metrics").text
    assert "commerce_http_requests_total" in body
    assert "commerce_http_request_duration_seconds" in body


def test_metrics_labels_use_templated_routes_not_concrete_ids(client, customer):
    """A label per order id is how a metrics integration takes Prometheus down."""
    client.get(f"/customers/{customer['id']}")
    body = client.get("/metrics").text
    assert 'route="/customers/{customer_id}"' in body
    assert customer["id"] not in body


def test_plans_hides_retired_plans_by_default(client):
    codes = {p["code"] for p in client.get("/plans").json()}
    assert "legacy_starter" not in codes
    all_codes = {p["code"] for p in client.get("/plans", params={"include_inactive": True}).json()}
    assert "legacy_starter" in all_codes
