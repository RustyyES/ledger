from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.errors import NotFound, UnprocessableState
from app.idempotency import IdempotencyGuard, idempotency
from app.models import Customer, Order, Subscription
from app.schemas import OrderCreate, OrderOut

router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("", response_model=OrderOut, status_code=status.HTTP_201_CREATED)
def create_order(
    payload: OrderCreate,
    guard: IdempotencyGuard = Depends(idempotency),
    session: Session = Depends(get_db),
) -> OrderOut:
    customer = session.execute(
        select(Customer).where(Customer.id == payload.customer_id)
    ).scalar_one_or_none()
    if customer is None:
        raise NotFound(f"customer {payload.customer_id} does not exist", field="customer_id")
    if customer.deleted_at is not None:
        # Deleted customers cannot place NEW orders, but their historical
        # orders are untouched. Those are different questions and the warehouse
        # must not conflate them.
        raise UnprocessableState(
            "customer is deleted and cannot place new orders", field="customer_id"
        )

    if payload.subscription_id is not None:
        sub = session.execute(
            select(Subscription).where(Subscription.id == payload.subscription_id)
        ).scalar_one_or_none()
        if sub is None:
            raise NotFound(
                f"subscription {payload.subscription_id} does not exist",
                field="subscription_id",
            )
        if sub.customer_id != customer.id:
            raise UnprocessableState(
                "subscription does not belong to that customer",
                field="subscription_id",
            )

    order = Order(
        id=uuid.uuid4(),
        customer_id=customer.id,
        subscription_id=payload.subscription_id,
        amount_cents=payload.amount_cents,
        currency=payload.currency,
        status="pending",
        placed_at=payload.placed_at,
        placed_at_local=payload.placed_at_local,
        updated_at=datetime.now(UTC),
    )
    session.add(order)
    session.flush()

    body = OrderOut.model_validate(order)
    guard.commit_or_replay(status.HTTP_201_CREATED, body)
    return body


@router.get("/{order_id}", response_model=OrderOut)
def get_order(order_id: uuid.UUID, session: Session = Depends(get_db)) -> Order:
    order = session.execute(select(Order).where(Order.id == order_id)).scalar_one_or_none()
    if order is None:
        raise NotFound(f"order {order_id} does not exist")
    return order
