"""Subscription lifecycle.

Every state transition writes two things atomically: the new state on
`subscriptions`, and an immutable row on `subscription_events`. The events
table is the source of truth for MRR, so a transition that updates state
without emitting an event is a revenue bug, not a cosmetic one. That is why
`_transition` is the only way state changes in this module -- there is no path
that touches `subscription.status` directly.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.errors import Conflict, NotFound, UnprocessableState
from app.idempotency import IdempotencyGuard, idempotency
from app.models import Customer, Plan, Subscription, SubscriptionEvent
from app.schemas import (
    SubscriptionCancel,
    SubscriptionCreate,
    SubscriptionOut,
    SubscriptionPlanChange,
)

router = APIRouter(prefix="/subscriptions", tags=["subscriptions"])

TRIAL_DAYS = 14

# Legal transitions. Anything not listed is a 409.
ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "trialing": {"active", "cancelled", "paused"},
    "active": {"paused", "cancelled", "active"},  # active->active = plan change
    "paused": {"active", "cancelled"},
    "cancelled": set(),  # terminal
    "expired": set(),  # terminal
}


def _load_subscription(session: Session, subscription_id: uuid.UUID) -> Subscription:
    sub = session.execute(
        select(Subscription).where(Subscription.id == subscription_id).with_for_update()
    ).scalar_one_or_none()
    if sub is None:
        raise NotFound(f"subscription {subscription_id} does not exist")
    return sub


def _load_plan(session: Session, plan_code: str) -> Plan:
    plan = session.execute(select(Plan).where(Plan.code == plan_code)).scalar_one_or_none()
    if plan is None:
        raise NotFound(f"plan {plan_code!r} does not exist", field="plan_code")
    return plan


def _transition(
    session: Session,
    sub: Subscription,
    *,
    to_status: str,
    event_type: str,
    occurred_at: datetime,
    to_plan_id: int | None = None,
) -> SubscriptionEvent:
    """The single chokepoint for subscription state change."""
    if to_status not in ALLOWED_TRANSITIONS.get(sub.status, set()):
        raise Conflict(f"cannot move subscription from {sub.status!r} to {to_status!r}")

    from_plan_id = sub.plan_id
    sub.status = to_status
    if to_plan_id is not None:
        sub.plan_id = to_plan_id
    if to_status == "cancelled":
        sub.ended_at = occurred_at
    sub.updated_at = datetime.now(UTC)

    event = SubscriptionEvent(
        subscription_id=sub.id,
        event_type=event_type,
        from_plan_id=from_plan_id,
        to_plan_id=to_plan_id if to_plan_id is not None else sub.plan_id,
        occurred_at=occurred_at,
        created_at=datetime.now(UTC),
    )
    session.add(event)
    return event


@router.post("", response_model=SubscriptionOut, status_code=status.HTTP_201_CREATED)
def create_subscription(
    payload: SubscriptionCreate,
    guard: IdempotencyGuard = Depends(idempotency),
    session: Session = Depends(get_db),
) -> SubscriptionOut:
    customer = session.execute(
        select(Customer).where(Customer.id == payload.customer_id)
    ).scalar_one_or_none()
    if customer is None or customer.deleted_at is not None:
        raise NotFound(f"customer {payload.customer_id} does not exist", field="customer_id")

    plan = _load_plan(session, payload.plan_code)
    if not plan.active:
        raise UnprocessableState(
            f"plan {plan.code!r} is retired and cannot be subscribed to",
            field="plan_code",
        )

    # One live subscription per customer. The load generator relies on this
    # being enforced rather than assumed.
    live = (
        session.execute(
            select(Subscription).where(
                Subscription.customer_id == customer.id,
                Subscription.status.in_(("trialing", "active", "paused")),
            )
        )
        .scalars()
        .first()
    )
    if live is not None:
        raise Conflict(
            f"customer already has a live subscription ({live.id})",
            field="customer_id",
        )

    started_at = payload.started_at or datetime.now(UTC)
    sub = Subscription(
        id=uuid.uuid4(),
        customer_id=customer.id,
        plan_id=plan.id,
        status="trialing" if payload.start_trial else "active",
        started_at=started_at,
        trial_ends_at=(started_at + timedelta(days=TRIAL_DAYS) if payload.start_trial else None),
        updated_at=datetime.now(UTC),
    )
    session.add(sub)
    session.flush()

    session.add(
        SubscriptionEvent(
            subscription_id=sub.id,
            event_type="created",
            from_plan_id=None,
            to_plan_id=plan.id,
            occurred_at=started_at,
            created_at=datetime.now(UTC),
        )
    )
    session.flush()

    body = SubscriptionOut.model_validate(sub)
    guard.commit_or_replay(status.HTTP_201_CREATED, body)
    return body


@router.patch("/{subscription_id}/plan", response_model=SubscriptionOut)
def change_plan(
    subscription_id: uuid.UUID,
    payload: SubscriptionPlanChange,
    session: Session = Depends(get_db),
) -> SubscriptionOut:
    """Upgrade or downgrade. Emits `upgraded` or `downgraded` by price delta."""
    sub = _load_subscription(session, subscription_id)
    new_plan = _load_plan(session, payload.plan_code)

    if new_plan.id == sub.plan_id:
        raise Conflict("subscription is already on that plan", field="plan_code")
    if not new_plan.active:
        raise UnprocessableState(f"plan {new_plan.code!r} is retired", field="plan_code")

    current_plan = session.get(Plan, sub.plan_id)
    assert current_plan is not None  # FK guarantees this
    event_type = "upgraded" if new_plan.monthly_cents > current_plan.monthly_cents else "downgraded"

    # A trialing subscription that changes plan converts to active: the
    # customer has made a purchasing decision.
    target_status = "active"
    _transition(
        session,
        sub,
        to_status=target_status,
        event_type=event_type,
        occurred_at=payload.occurred_at or datetime.now(UTC),
        to_plan_id=new_plan.id,
    )
    session.flush()
    body = SubscriptionOut.model_validate(sub)
    session.commit()
    return body


@router.post("/{subscription_id}/cancel", response_model=SubscriptionOut)
def cancel_subscription(
    subscription_id: uuid.UUID,
    payload: SubscriptionCancel,
    guard: IdempotencyGuard = Depends(idempotency),
    session: Session = Depends(get_db),
) -> SubscriptionOut:
    sub = _load_subscription(session, subscription_id)
    if sub.status == "cancelled":
        raise Conflict("subscription is already cancelled")

    _transition(
        session,
        sub,
        to_status="cancelled",
        event_type="cancelled",
        occurred_at=payload.occurred_at or datetime.now(UTC),
    )
    session.flush()
    body = SubscriptionOut.model_validate(sub)
    guard.commit_or_replay(status.HTTP_200_OK, body)
    return body


@router.post("/{subscription_id}/pause", response_model=SubscriptionOut)
def pause_subscription(
    subscription_id: uuid.UUID,
    guard: IdempotencyGuard = Depends(idempotency),
    session: Session = Depends(get_db),
) -> SubscriptionOut:
    sub = _load_subscription(session, subscription_id)
    _transition(
        session,
        sub,
        to_status="paused",
        event_type="paused",
        occurred_at=datetime.now(UTC),
    )
    session.flush()
    body = SubscriptionOut.model_validate(sub)
    guard.commit_or_replay(status.HTTP_200_OK, body)
    return body


@router.post("/{subscription_id}/resume", response_model=SubscriptionOut)
def resume_subscription(
    subscription_id: uuid.UUID,
    guard: IdempotencyGuard = Depends(idempotency),
    session: Session = Depends(get_db),
) -> SubscriptionOut:
    sub = _load_subscription(session, subscription_id)
    if sub.status != "paused":
        raise Conflict(f"only a paused subscription can resume (is {sub.status!r})")
    _transition(
        session,
        sub,
        to_status="active",
        event_type="resumed",
        occurred_at=datetime.now(UTC),
    )
    session.flush()
    body = SubscriptionOut.model_validate(sub)
    guard.commit_or_replay(status.HTTP_200_OK, body)
    return body
