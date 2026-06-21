"""Subscription state machine and the events it must emit.

Every assertion about `subscription_events` here is really an assertion about
MRR: a transition that fails to emit an event produces revenue that the
warehouse can never reconstruct.
"""

from __future__ import annotations

import uuid

from app.models import SubscriptionEvent
from sqlalchemy import select


def _events(session, subscription_id) -> list[SubscriptionEvent]:
    return list(
        session.execute(
            select(SubscriptionEvent)
            .where(SubscriptionEvent.subscription_id == uuid.UUID(str(subscription_id)))
            .order_by(SubscriptionEvent.id)
        )
        .scalars()
        .all()
    )


def _subscribe(client, idem, customer, plan_code="basic", trial=True):
    return client.post(
        "/subscriptions",
        json={"customer_id": customer["id"], "plan_code": plan_code, "start_trial": trial},
        headers=idem.fresh(),
    )


def test_create_opens_in_trialing_and_emits_created(client, idem, customer, session):
    resp = _subscribe(client, idem, customer)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["status"] == "trialing"
    assert body["trial_ends_at"] is not None

    events = _events(session, body["id"])
    assert [e.event_type for e in events] == ["created"]
    assert events[0].from_plan_id is None


def test_create_without_trial_opens_active(client, idem, customer):
    body = _subscribe(client, idem, customer, trial=False).json()
    assert body["status"] == "active"
    assert body["trial_ends_at"] is None


def test_second_live_subscription_is_409(client, idem, customer):
    assert _subscribe(client, idem, customer).status_code == 201
    resp = _subscribe(client, idem, customer, plan_code="pro")
    assert resp.status_code == 409


def test_retired_plan_cannot_be_subscribed_to(client, idem, customer):
    resp = _subscribe(client, idem, customer, plan_code="legacy_starter")
    assert resp.status_code == 422
    assert resp.json()["error"]["field"] == "plan_code"


def test_upgrade_emits_upgraded_with_both_plan_ids(client, idem, customer, session):
    sub = _subscribe(client, idem, customer, plan_code="basic").json()
    resp = client.patch(f"/subscriptions/{sub['id']}/plan", json={"plan_code": "pro"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "active", "changing plan converts a trial"

    events = _events(session, sub["id"])
    assert [e.event_type for e in events] == ["created", "upgraded"]
    change = events[-1]
    assert change.from_plan_id is not None
    assert change.to_plan_id != change.from_plan_id


def test_downgrade_is_classified_by_price_not_by_direction_of_the_call(
    client, idem, customer, session
):
    sub = _subscribe(client, idem, customer, plan_code="enterprise", trial=False).json()
    client.patch(f"/subscriptions/{sub['id']}/plan", json={"plan_code": "basic"})
    assert _events(session, sub["id"])[-1].event_type == "downgraded"


def test_changing_to_the_same_plan_is_409(client, idem, customer):
    sub = _subscribe(client, idem, customer, plan_code="pro").json()
    resp = client.patch(f"/subscriptions/{sub['id']}/plan", json={"plan_code": "pro"})
    assert resp.status_code == 409


def test_pause_then_resume_round_trips(client, idem, customer, session):
    sub = _subscribe(client, idem, customer, trial=False).json()
    assert (
        client.post(f"/subscriptions/{sub['id']}/pause", headers=idem.fresh()).json()["status"]
        == "paused"
    )
    assert (
        client.post(f"/subscriptions/{sub['id']}/resume", headers=idem.fresh()).json()["status"]
        == "active"
    )
    assert [e.event_type for e in _events(session, sub["id"])] == ["created", "paused", "resumed"]


def test_resume_on_an_active_subscription_is_409(client, idem, customer):
    sub = _subscribe(client, idem, customer, trial=False).json()
    resp = client.post(f"/subscriptions/{sub['id']}/resume", headers=idem.fresh())
    assert resp.status_code == 409


def test_cancel_sets_ended_at_and_is_terminal(client, idem, customer):
    sub = _subscribe(client, idem, customer, trial=False).json()
    cancelled = client.post(
        f"/subscriptions/{sub['id']}/cancel", json={"reason": "too expensive"}, headers=idem.fresh()
    ).json()
    assert cancelled["status"] == "cancelled"
    assert cancelled["ended_at"] is not None

    # Terminal: no transition out.
    assert client.post(f"/subscriptions/{sub['id']}/pause", headers=idem.fresh()).status_code == 409
    assert (
        client.patch(f"/subscriptions/{sub['id']}/plan", json={"plan_code": "pro"}).status_code
        == 409
    )


def test_cancel_is_idempotent_under_replay(client, idem, customer, session):
    sub = _subscribe(client, idem, customer, trial=False).json()
    headers = idem.fixed("cancel-once-eeeeeee")
    first = client.post(f"/subscriptions/{sub['id']}/cancel", json={}, headers=headers)
    second = client.post(f"/subscriptions/{sub['id']}/cancel", json={}, headers=headers)
    assert first.status_code == 200 and second.status_code == 200
    cancels = [e for e in _events(session, sub["id"]) if e.event_type == "cancelled"]
    assert len(cancels) == 1, "replay emitted a duplicate cancellation event"


def test_subscription_for_deleted_customer_is_404(client, idem, customer):
    client.delete(f"/customers/{customer['id']}")
    assert _subscribe(client, idem, customer).status_code == 404
