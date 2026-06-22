"""Orders, payments, refunds -- and the invariants the warehouse tests mirror.

`assert_refund_not_exceeding_payment.sql` in the dbt project is the warehouse
side of `test_refund_cannot_exceed_payment` here. If the API stops enforcing
it, the dbt test is the thing that catches it -- and vice versa. Both exist on
purpose: one prevents the bad row, the other proves none got in.
"""

from __future__ import annotations

import uuid

import pytest


def _order(client, idem, customer, **overrides) -> dict:
    payload = {
        "customer_id": customer["id"],
        "amount_cents": 4900,
        "currency": "USD",
        "placed_at": "2025-03-01T12:00:00Z",
    }
    payload.update(overrides)
    resp = client.post("/orders", json=payload, headers=idem.fresh())
    assert resp.status_code == 201, resp.text
    return resp.json()


def _pay(client, idem, order, **overrides) -> object:
    payload = {"amount_cents": order["amount_cents"], "method": "card"}
    payload.update(overrides)
    return client.post(f"/orders/{order['id']}/payments", json=payload, headers=idem.fresh())


def test_order_opens_pending(client, idem, customer):
    assert _order(client, idem, customer)["status"] == "pending"


def test_order_accepts_naive_local_time_instead_of_placed_at(client, idem, customer):
    """The legacy mobile client path. 15% of production traffic looks like this."""
    order = _order(client, idem, customer, placed_at=None, placed_at_local="2025-03-01 14:30:00")
    assert order["placed_at"] is None
    assert order["placed_at_local"] == "2025-03-01 14:30:00"


def test_order_with_neither_timestamp_is_422(client, idem, customer):
    resp = client.post(
        "/orders",
        json={"customer_id": customer["id"], "amount_cents": 100, "currency": "USD"},
        headers=idem.fresh(),
    )
    assert resp.status_code == 422


def test_placed_at_local_must_be_naive(client, idem, customer):
    resp = client.post(
        "/orders",
        json={
            "customer_id": customer["id"],
            "amount_cents": 100,
            "currency": "USD",
            "placed_at_local": "2025-03-01T14:30:00+02:00",
        },
        headers=idem.fresh(),
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["field"] == "placed_at_local"


@pytest.mark.parametrize("currency", ["JPY", "BTC", "usdt"])
def test_unsupported_currency_is_422(client, idem, customer, currency):
    resp = client.post(
        "/orders",
        json={
            "customer_id": customer["id"],
            "amount_cents": 100,
            "currency": currency,
            "placed_at": "2025-03-01T12:00:00Z",
        },
        headers=idem.fresh(),
    )
    assert resp.status_code == 422


def test_deleted_customer_cannot_place_new_orders(client, idem, customer):
    client.delete(f"/customers/{customer['id']}")
    resp = client.post(
        "/orders",
        json={
            "customer_id": customer["id"],
            "amount_cents": 100,
            "currency": "USD",
            "placed_at": "2025-03-01T12:00:00Z",
        },
        headers=idem.fresh(),
    )
    assert resp.status_code == 422


def test_successful_payment_completes_the_order(client, idem, customer):
    order = _order(client, idem, customer)
    resp = _pay(client, idem, order)
    assert resp.status_code == 201
    assert resp.json()["status"] == "succeeded"
    assert resp.json()["processed_at"] is not None
    assert client.get(f"/orders/{order['id']}").json()["status"] == "completed"


def test_pending_payment_has_no_processed_at(client, idem, customer):
    """The nullable-in-practice column. A blanket not_null test downstream is wrong."""
    order = _order(client, idem, customer)
    body = _pay(client, idem, order, force_status="pending").json()
    assert body["status"] == "pending"
    assert body["processed_at"] is None
    assert client.get(f"/orders/{order['id']}").json()["status"] == "pending"


def test_failed_payment_leaves_the_order_pending(client, idem, customer):
    order = _order(client, idem, customer)
    assert _pay(client, idem, order, force_status="failed").json()["status"] == "failed"
    assert client.get(f"/orders/{order['id']}").json()["status"] == "pending"


def test_partial_payment_is_rejected(client, idem, customer):
    order = _order(client, idem, customer)
    resp = _pay(client, idem, order, amount_cents=order["amount_cents"] - 1)
    assert resp.status_code == 422
    assert resp.json()["error"]["field"] == "amount_cents"


def test_double_payment_is_rejected(client, idem, customer):
    order = _order(client, idem, customer)
    assert _pay(client, idem, order).status_code == 201
    assert _pay(client, idem, order).status_code == 422


def test_full_refund_flips_the_order_to_refunded(client, idem, customer):
    order = _order(client, idem, customer)
    payment = _pay(client, idem, order).json()
    resp = client.post(
        f"/payments/{payment['id']}/refunds",
        json={"amount_cents": payment["amount_cents"], "reason": "customer request"},
        headers=idem.fresh(),
    )
    assert resp.status_code == 201
    assert client.get(f"/orders/{order['id']}").json()["status"] == "refunded"


def test_partial_refund_leaves_the_order_completed(client, idem, customer):
    order = _order(client, idem, customer)
    payment = _pay(client, idem, order).json()
    client.post(
        f"/payments/{payment['id']}/refunds",
        json={"amount_cents": 1000},
        headers=idem.fresh(),
    )
    assert client.get(f"/orders/{order['id']}").json()["status"] == "completed"


def test_refund_cannot_exceed_payment_across_multiple_calls(client, idem, customer):
    order = _order(client, idem, customer)
    payment = _pay(client, idem, order).json()
    half = payment["amount_cents"] // 2

    assert (
        client.post(
            f"/payments/{payment['id']}/refunds", json={"amount_cents": half}, headers=idem.fresh()
        ).status_code
        == 201
    )
    # Second refund of more than the remainder must fail on the running total,
    # not on the individual amount.
    resp = client.post(
        f"/payments/{payment['id']}/refunds",
        json={"amount_cents": payment["amount_cents"]},
        headers=idem.fresh(),
    )
    assert resp.status_code == 422
    assert "already refunded" in resp.json()["error"]["message"]


def test_cannot_refund_a_failed_payment(client, idem, customer):
    order = _order(client, idem, customer)
    payment = _pay(client, idem, order, force_status="failed").json()
    resp = client.post(
        f"/payments/{payment['id']}/refunds", json={"amount_cents": 100}, headers=idem.fresh()
    )
    assert resp.status_code == 422


def test_late_refund_keeps_its_backdated_issued_at(client, idem, customer):
    """This is the shape `make prove-lookback` uses against the warehouse."""
    order = _order(client, idem, customer)
    payment = _pay(client, idem, order).json()
    resp = client.post(
        f"/payments/{payment['id']}/refunds",
        json={"amount_cents": 500, "issued_at": "2025-03-13T09:00:00Z"},
        headers=idem.fresh(),
    )
    assert resp.status_code == 201
    assert resp.json()["issued_at"].startswith("2025-03-13")


def test_refund_on_unknown_payment_is_404(client, idem):
    resp = client.post(
        f"/payments/{uuid.uuid4()}/refunds", json={"amount_cents": 100}, headers=idem.fresh()
    )
    assert resp.status_code == 404


def test_zero_amount_refund_is_422(client, idem, customer):
    order = _order(client, idem, customer)
    payment = _pay(client, idem, order).json()
    resp = client.post(
        f"/payments/{payment['id']}/refunds", json={"amount_cents": 0}, headers=idem.fresh()
    )
    assert resp.status_code == 422


def test_order_can_be_linked_to_the_customers_own_subscription(client, idem, customer):
    sub = client.post(
        "/subscriptions",
        json={"customer_id": customer["id"], "plan_code": "pro", "start_trial": False},
        headers=idem.fresh(),
    ).json()
    order = _order(client, idem, customer, subscription_id=sub["id"])
    assert order["subscription_id"] == sub["id"]


def test_order_linked_to_someone_elses_subscription_is_422(client, idem, customer):
    """Cross-customer linkage would corrupt every per-customer aggregate."""
    other = client.post(
        "/customers",
        json={
            "email": f"{uuid.uuid4().hex[:10]}@example.com",
            "name": "Other",
            "country_code": "US",
        },
        headers=idem.fresh(),
    ).json()
    their_sub = client.post(
        "/subscriptions",
        json={"customer_id": other["id"], "plan_code": "basic", "start_trial": False},
        headers=idem.fresh(),
    ).json()

    resp = client.post(
        "/orders",
        json={
            "customer_id": customer["id"],
            "subscription_id": their_sub["id"],
            "amount_cents": 1900,
            "currency": "USD",
            "placed_at": "2025-03-01T12:00:00Z",
        },
        headers=idem.fresh(),
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["field"] == "subscription_id"


def test_order_with_unknown_subscription_is_404(client, idem, customer):
    resp = client.post(
        "/orders",
        json={
            "customer_id": customer["id"],
            "subscription_id": str(uuid.uuid4()),
            "amount_cents": 1900,
            "currency": "USD",
            "placed_at": "2025-03-01T12:00:00Z",
        },
        headers=idem.fresh(),
    )
    assert resp.status_code == 404


def test_order_for_unknown_customer_is_404(client, idem):
    resp = client.post(
        "/orders",
        json={
            "customer_id": str(uuid.uuid4()),
            "amount_cents": 100,
            "currency": "USD",
            "placed_at": "2025-03-01T12:00:00Z",
        },
        headers=idem.fresh(),
    )
    assert resp.status_code == 404


def test_unknown_order_is_404(client):
    assert client.get(f"/orders/{uuid.uuid4()}").status_code == 404


def test_payment_on_unknown_order_is_404(client, idem):
    resp = client.post(
        f"/orders/{uuid.uuid4()}/payments",
        json={"amount_cents": 100, "method": "card"},
        headers=idem.fresh(),
    )
    assert resp.status_code == 404
