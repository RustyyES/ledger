"""SQLAlchemy models for the commerce OLTP database.

This is deliberately a schema an *application* team would write, not one an
analyst would want. Read `docs/DESIGN.md#source-schema` for why each awkward
edge is here on purpose. Three of them matter enough to call out at the top:

1. `orders.placed_at` is NULLABLE. A legacy mobile client writes wall-clock
   local time into `orders.placed_at_local` (text, no offset) and leaves
   `placed_at` null. A CHECK constraint guarantees at least one is present.
   Downstream this forces a real timezone-normalisation step in staging.

2. `orders.status` is free text, not an enum. Historical rows carry 'paid',
   'PAID' and 'complete' from a pre-migration era; current code writes
   'completed'. An enum would have made this clean, which is exactly why the
   real system does not have one.

3. Customers are soft-deleted. `deleted_at` is set, rows are never removed,
   and their orders stay behind. The warehouse has to decide what that means.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def _uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


# --------------------------------------------------------------------------- #
# Canonical vocabularies.
#
# These live in Python, not in a Postgres ENUM, on purpose. A Postgres enum
# would reject the legacy values below at write time, which would erase the
# very mess the pipeline exists to clean up. The application validates against
# CANONICAL_*; the LEGACY_* sets document what is already in the table.
# --------------------------------------------------------------------------- #

CANONICAL_ORDER_STATUS = frozenset({"pending", "completed", "cancelled", "refunded"})
LEGACY_ORDER_STATUS = frozenset({"paid", "PAID", "complete", "Completed"})
ALL_ORDER_STATUS = CANONICAL_ORDER_STATUS | LEGACY_ORDER_STATUS

CANONICAL_SUBSCRIPTION_STATUS = frozenset({"trialing", "active", "paused", "cancelled", "expired"})
CANONICAL_PAYMENT_STATUS = frozenset({"pending", "succeeded", "failed"})
CANONICAL_PAYMENT_METHOD = frozenset({"card", "paypal", "bank_transfer", "apple_pay"})
CANONICAL_EVENT_TYPE = frozenset(
    {"created", "upgraded", "downgraded", "paused", "resumed", "cancelled", "expired"}
)


class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[uuid.UUID] = _uuid_pk()
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    country_code: Mapped[str] = mapped_column(String(2), nullable=False)
    # Needed by the warehouse to resolve `placed_at_local` into UTC. IANA name,
    # e.g. 'Africa/Cairo'. Set at signup from the client, never back-filled for
    # rows created before it existed -- hence the default rather than NOT NULL
    # with no default.
    timezone: Mapped[str] = mapped_column(Text, nullable=False, server_default="UTC")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # Soft delete. Rows are NEVER physically removed.
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    subscriptions: Mapped[list[Subscription]] = relationship(back_populates="customer")
    orders: Mapped[list[Order]] = relationship(back_populates="customer")

    __table_args__ = (
        CheckConstraint("char_length(country_code) = 2", name="ck_customers_country_len"),
        CheckConstraint("position('@' in email) > 1", name="ck_customers_email_shape"),
        Index("ix_customers_created_at", "created_at"),
        Index("ix_customers_updated_at", "updated_at"),
        # Partial index: the overwhelmingly common query is "live customers".
        Index("ix_customers_active", "id", postgresql_where=(deleted_at.is_(None))),
    )


class Plan(Base):
    __tablename__ = "plans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    monthly_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default="USD")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")

    __table_args__ = (CheckConstraint("monthly_cents >= 0", name="ck_plans_monthly_cents_nonneg"),)


class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[uuid.UUID] = _uuid_pk()
    customer_id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("customers.id"), nullable=False
    )
    plan_id: Mapped[int] = mapped_column(Integer, ForeignKey("plans.id"), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    trial_ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    customer: Mapped[Customer] = relationship(back_populates="subscriptions")
    events: Mapped[list[SubscriptionEvent]] = relationship(
        back_populates="subscription", order_by="SubscriptionEvent.occurred_at"
    )

    __table_args__ = (
        CheckConstraint(
            "ended_at IS NULL OR ended_at >= started_at",
            name="ck_subscriptions_period_ordered",
        ),
        Index("ix_subscriptions_customer_id", "customer_id"),
        Index("ix_subscriptions_updated_at", "updated_at"),
        Index("ix_subscriptions_status", "status"),
    )


class SubscriptionEvent(Base):
    """Append-only. The source of truth for MRR.

    Note there is no `updated_at`: events are immutable, so CDC only ever sees
    inserts on this table. That is a useful contrast for the warehouse -- this
    is the one source table where an append-only incremental strategy is
    actually safe.
    """

    __tablename__ = "subscription_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    subscription_id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("subscriptions.id"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    from_plan_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("plans.id"), nullable=True)
    to_plan_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("plans.id"), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    subscription: Mapped[Subscription] = relationship(back_populates="events")

    __table_args__ = (
        Index("ix_sub_events_subscription_id", "subscription_id"),
        Index("ix_sub_events_occurred_at", "occurred_at"),
        Index("ix_sub_events_created_at", "created_at"),
    )


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = _uuid_pk()
    customer_id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("customers.id"), nullable=False
    )
    subscription_id: Mapped[uuid.UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("subscriptions.id"), nullable=True
    )
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)

    # See module docstring, point 1. Nullable on purpose.
    placed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    placed_at_local: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Naive local wall-clock from the legacy mobile client, "
        "'YYYY-MM-DD HH:MM:SS', no offset. Resolve against customers.timezone.",
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    customer: Mapped[Customer] = relationship(back_populates="orders")
    payments: Mapped[list[Payment]] = relationship(back_populates="order")

    __table_args__ = (
        CheckConstraint("amount_cents >= 0", name="ck_orders_amount_nonneg"),
        CheckConstraint(
            "placed_at IS NOT NULL OR placed_at_local IS NOT NULL",
            name="ck_orders_placed_at_present",
        ),
        Index("ix_orders_customer_id", "customer_id"),
        Index("ix_orders_placed_at", "placed_at"),
        Index("ix_orders_updated_at", "updated_at"),
        Index("ix_orders_status", "status"),
    )


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = _uuid_pk()
    order_id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("orders.id"), nullable=False
    )
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    method: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    # Null while pending. This is why a blanket not_null test on the warehouse
    # column is wrong and a scoped one is right.
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    order: Mapped[Order] = relationship(back_populates="payments")
    refunds: Mapped[list[Refund]] = relationship(back_populates="payment")

    __table_args__ = (
        CheckConstraint("amount_cents >= 0", name="ck_payments_amount_nonneg"),
        CheckConstraint(
            "status <> 'succeeded' OR processed_at IS NOT NULL",
            name="ck_payments_succeeded_has_processed_at",
        ),
        Index("ix_payments_order_id", "order_id"),
        Index("ix_payments_updated_at", "updated_at"),
        Index("ix_payments_processed_at", "processed_at"),
    )


class Refund(Base):
    """Issued 1-14 days after the payment it reverses.

    That lag is the single most instructive constraint in the whole project:
    it is what makes an incremental model keyed on the business timestamp
    silently and permanently wrong.
    """

    __tablename__ = "refunds"

    id: Mapped[uuid.UUID] = _uuid_pk()
    payment_id: Mapped[uuid.UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("payments.id"), nullable=False
    )
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    payment: Mapped[Payment] = relationship(back_populates="refunds")

    __table_args__ = (
        CheckConstraint("amount_cents > 0", name="ck_refunds_amount_positive"),
        Index("ix_refunds_payment_id", "payment_id"),
        Index("ix_refunds_issued_at", "issued_at"),
        Index("ix_refunds_created_at", "created_at"),
    )


class IdempotencyKey(Base):
    """Server-side replay protection for every POST.

    Stores the hash of the request body alongside the serialised response. A
    replay with the same key and the same body returns the stored response; a
    replay with the same key and a *different* body is a client bug and gets
    422. This table is intentionally excluded from CDC -- it is service
    plumbing, not a business fact.
    """

    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    endpoint: Mapped[str] = mapped_column(Text, primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    response_status: Mapped[int] = mapped_column(Integer, nullable=False)
    response_body: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("key", "endpoint", name="uq_idempotency_key_endpoint"),
        Index("ix_idempotency_created_at", "created_at"),
    )
