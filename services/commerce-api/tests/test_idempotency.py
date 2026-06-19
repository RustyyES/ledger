"""The idempotency contract.

These are the tests that matter most in this file: the load generator retries
on timeout, and without a correct implementation those retries silently double
revenue in the warehouse.
"""

from __future__ import annotations

import uuid


def _payload() -> dict:
    return {
        "email": f"{uuid.uuid4().hex[:12]}@example.com",
        "name": "Retry Subject",
        "country_code": "US",
    }


def test_missing_key_on_post_is_400(client):
    resp = client.post("/customers", json=_payload())
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "idempotency_key_required"


def test_replay_with_same_body_returns_the_stored_response(client, idem):
    headers = idem.fixed("replay-key-aaaaaaaa")
    payload = _payload()

    first = client.post("/customers", json=payload, headers=headers)
    assert first.status_code == 201
    assert first.headers["Idempotency-Replayed"] == "false"

    second = client.post("/customers", json=payload, headers=headers)
    assert second.status_code == 201
    assert second.headers["Idempotency-Replayed"] == "true"
    assert second.json()["id"] == first.json()["id"], "replay created a second row"


def test_replay_is_insensitive_to_key_order_in_the_body(client, idem):
    """Real HTTP clients reorder JSON keys between retries."""
    headers = idem.fixed("reorder-key-bbbbbb")
    email = f"{uuid.uuid4().hex[:12]}@example.com"

    first = client.post(
        "/customers",
        json={"email": email, "name": "Order A", "country_code": "US"},
        headers=headers,
    )
    second = client.post(
        "/customers",
        json={"country_code": "US", "name": "Order A", "email": email},
        headers=headers,
    )
    assert second.status_code == first.status_code
    assert second.json()["id"] == first.json()["id"]


def test_same_key_different_body_is_422(client, idem):
    headers = idem.fixed("conflict-key-cccccc")
    client.post("/customers", json=_payload(), headers=headers)
    resp = client.post("/customers", json=_payload(), headers=headers)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "idempotency_key_reused_with_different_body"


def test_same_key_on_a_different_endpoint_is_independent(client, idem, customer):
    """Keys are scoped per endpoint, so a client's counter cannot collide."""
    headers = idem.fixed("shared-key-dddddddd")
    a = client.post("/customers", json=_payload(), headers=headers)
    b = client.post(
        "/orders",
        json={
            "customer_id": customer["id"],
            "amount_cents": 1900,
            "currency": "USD",
            "placed_at": "2025-03-01T10:00:00Z",
        },
        headers=headers,
    )
    assert a.status_code == 201
    assert b.status_code == 201


def test_short_key_is_rejected(client):
    resp = client.post("/customers", json=_payload(), headers={"Idempotency-Key": "tiny"})
    assert resp.status_code == 400
