"""Payments and refunds.

Two routers live here because a refund is not an aggregate root -- it only
exists relative to a payment, and the invariant that matters
(sum(refunds) <= payment.amount_cents) can only be enforced with the payment
row locked. Splitting them into separate modules would have separated the
invariant from the lock that protects it.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.errors import NotFound, UnprocessableState
from app.idempotency import IdempotencyGuard, idempotency
from app.models import Order, Payment, Refund
from app.schemas import PaymentCreate, PaymentOut, RefundCreate, RefundOut

order_payments = APIRouter(prefix="/orders", tags=["payments"])
payment_refunds = APIRouter(prefix="/payments", tags=["payments"])

# Terminal-ish order states the pipeline normalises to 'completed'. The API
# writes the canonical value; only pre-migration history carries the variants.
COMPLETED = "completed"


@order_payments.post(
    "/{order_id}/payments",
    response_model=PaymentOut,
    status_code=status.HTTP_201_CREATED,
)
def create_payment(
    order_id: uuid.UUID,
    payload: PaymentCreate,
    guard: IdempotencyGuard = Depends(idempotency),
    session: Session = Depends(get_db),
) -> PaymentOut:
    order = session.execute(
        select(Order).where(Order.id == order_id).with_for_update()
    ).scalar_one_or_none()
    if order is None:
        raise NotFound(f"order {order_id} does not exist")

    if payload.amount_cents != order.amount_cents:
        raise UnprocessableState(
            f"payment amount {payload.amount_cents} does not match order "
            f"amount {order.amount_cents}; partial payment is not supported",
            field="amount_cents",
        )

    already_succeeded = session.execute(
        select(func.count())
        .select_from(Payment)
        .where(Payment.order_id == order.id, Payment.status == "succeeded")
    ).scalar_one()
    if already_succeeded:
        raise UnprocessableState("order is already paid")

    now = datetime.now(UTC)
    resolved_status = payload.force_status or "succeeded"
    # A pending payment has no processed_at. This is the nullable-in-practice
    # column that makes a blanket not_null test in the warehouse wrong.
    processed_at = None if resolved_status == "pending" else (payload.processed_at or now)

    payment = Payment(
        id=uuid.uuid4(),
        order_id=order.id,
        amount_cents=payload.amount_cents,
        method=payload.method,
        status=resolved_status,
        processed_at=processed_at,
        created_at=now,
        updated_at=now,
    )
    session.add(payment)

    if resolved_status == "succeeded":
        order.status = COMPLETED
        order.updated_at = now
    session.flush()

    body = PaymentOut.model_validate(payment)
    guard.commit_or_replay(status.HTTP_201_CREATED, body)
    return body


@payment_refunds.post(
    "/{payment_id}/refunds",
    response_model=RefundOut,
    status_code=status.HTTP_201_CREATED,
)
def create_refund(
    payment_id: uuid.UUID,
    payload: RefundCreate,
    guard: IdempotencyGuard = Depends(idempotency),
    session: Session = Depends(get_db),
) -> RefundOut:
    """Issue a refund. Arrives 1-14 days after the payment in normal traffic.

    The `issued_at` override is what lets the load generator -- and the
    incremental-lookback proof in `make prove-lookback` -- create a refund that
    is genuinely late relative to the order it reverses.
    """
    payment = session.execute(
        select(Payment).where(Payment.id == payment_id).with_for_update()
    ).scalar_one_or_none()
    if payment is None:
        raise NotFound(f"payment {payment_id} does not exist")
    if payment.status != "succeeded":
        raise UnprocessableState(f"cannot refund a payment with status {payment.status!r}")

    already_refunded = session.execute(
        select(func.coalesce(func.sum(Refund.amount_cents), 0)).where(
            Refund.payment_id == payment.id
        )
    ).scalar_one()
    if already_refunded + payload.amount_cents > payment.amount_cents:
        raise UnprocessableState(
            f"refund would exceed payment: {already_refunded} already refunded "
            f"of {payment.amount_cents}",
            field="amount_cents",
        )

    now = datetime.now(UTC)
    refund = Refund(
        id=uuid.uuid4(),
        payment_id=payment.id,
        amount_cents=payload.amount_cents,
        reason=payload.reason,
        issued_at=payload.issued_at or now,
        created_at=now,
    )
    session.add(refund)

    # Fully refunded flips the order; partial refunds deliberately do not.
    if already_refunded + payload.amount_cents == payment.amount_cents:
        order = session.get(Order, payment.order_id)
        if order is not None:
            order.status = "refunded"
            order.updated_at = now
    payment.updated_at = now
    session.flush()

    body = RefundOut.model_validate(refund)
    guard.commit_or_replay(status.HTTP_201_CREATED, body)
    return body
