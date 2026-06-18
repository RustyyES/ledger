from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.errors import Conflict, NotFound
from app.idempotency import IdempotencyGuard, idempotency
from app.models import Customer
from app.schemas import CustomerCreate, CustomerOut, CustomerUpdate

router = APIRouter(prefix="/customers", tags=["customers"])


def _load(session: Session, customer_id: uuid.UUID, *, include_deleted: bool = False) -> Customer:
    stmt = select(Customer).where(Customer.id == customer_id)
    customer = session.execute(stmt).scalar_one_or_none()
    if customer is None:
        raise NotFound(f"customer {customer_id} does not exist")
    if customer.deleted_at is not None and not include_deleted:
        # 404, not 403. A soft-deleted customer should be indistinguishable
        # from a nonexistent one to an ordinary caller.
        raise NotFound(f"customer {customer_id} does not exist")
    return customer


@router.post("", response_model=CustomerOut, status_code=status.HTTP_201_CREATED)
def create_customer(
    payload: CustomerCreate,
    guard: IdempotencyGuard = Depends(idempotency),
    session: Session = Depends(get_db),
) -> CustomerOut:
    existing = session.execute(
        select(Customer).where(Customer.email == payload.email)
    ).scalar_one_or_none()
    if existing is not None:
        raise Conflict(f"email {payload.email} is already registered", field="email")

    now = datetime.now(UTC)
    customer = Customer(
        id=uuid.uuid4(),
        email=str(payload.email),
        name=payload.name,
        country_code=payload.country_code,
        timezone=payload.timezone,
        created_at=now,
        updated_at=now,
    )
    session.add(customer)
    session.flush()

    body = CustomerOut.model_validate(customer)
    guard.commit_or_replay(status.HTTP_201_CREATED, body)
    return body


@router.get("/{customer_id}", response_model=CustomerOut)
def get_customer(
    customer_id: uuid.UUID,
    include_deleted: bool = Query(
        default=False,
        description="Internal/ops flag. The warehouse reconciliation job uses "
        "it to confirm a soft-deleted row is still present in the source.",
    ),
    session: Session = Depends(get_db),
) -> Customer:
    return _load(session, customer_id, include_deleted=include_deleted)


@router.patch("/{customer_id}", response_model=CustomerOut)
def update_customer(
    customer_id: uuid.UUID,
    payload: CustomerUpdate,
    session: Session = Depends(get_db),
) -> CustomerOut:
    customer = _load(session, customer_id)

    # Only assign fields the client actually sent. This is what makes
    # `{"country_code": "DE"}` a country change and not a wipe of `name`.
    changes = payload.model_dump(exclude_unset=True)
    for field, value in changes.items():
        setattr(customer, field, value)

    # Every mutation moves updated_at. The dbt snapshot's timestamp strategy
    # depends on this being true without exception -- a missed touch here is an
    # SCD2 version that silently never gets recorded.
    customer.updated_at = datetime.now(UTC)
    session.flush()
    body = CustomerOut.model_validate(customer)
    session.commit()
    return body


@router.delete("/{customer_id}", response_model=CustomerOut)
def soft_delete_customer(
    customer_id: uuid.UUID,
    session: Session = Depends(get_db),
) -> CustomerOut:
    """Soft delete. The row stays, `deleted_at` is stamped.

    Their orders are deliberately left alone. Revenue that was recognised
    stays recognised; deleting a customer is a privacy action, not a financial
    correction. `DESIGN.md` records that decision and the warehouse honours it.
    """
    # include_deleted=True so an already-deleted row is found rather than 404ing.
    # DELETE is idempotent here by design: the load generator retries on
    # timeout, and a retry after a successful delete must not be logged as a
    # failure. RFC 9110 requires the same *effect*, not the same status code,
    # so returning the current state is both correct and operationally kinder.
    customer = _load(session, customer_id, include_deleted=True)
    if customer.deleted_at is not None:
        return CustomerOut.model_validate(customer)

    now = datetime.now(UTC)
    customer.deleted_at = now
    customer.updated_at = now
    session.flush()
    body = CustomerOut.model_validate(customer)
    session.commit()
    return body
