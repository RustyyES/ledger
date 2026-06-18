"""Pydantic request/response contracts.

Validation lives here and only here. Routers assume anything that reaches them
has already been shape-checked; they enforce *state* rules (does this
subscription exist, is it already cancelled) which validation cannot know.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

# NOTE: the pattern is case-INSENSITIVE even though the stored value is
# upper-case. Pydantic v2 applies `pattern` BEFORE `to_upper`, so a
# `^[A-Z]{2}$` pattern rejects the very lowercase input that `to_upper` exists
# to normalise. Tested by `test_create_customer_returns_201_and_echoes_fields`.
CountryCode = Annotated[
    str, StringConstraints(min_length=2, max_length=2, to_upper=True, pattern=r"^[A-Za-z]{2}$")
]
CurrencyCode = Annotated[
    str, StringConstraints(min_length=3, max_length=3, to_upper=True, pattern=r"^[A-Za-z]{3}$")
]

SUPPORTED_CURRENCIES = frozenset({"USD", "EUR", "GBP"})


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# --------------------------------------------------------------------------- #
# Customers
# --------------------------------------------------------------------------- #


class CustomerCreate(BaseModel):
    email: EmailStr
    name: Annotated[str, StringConstraints(min_length=1, max_length=200, strip_whitespace=True)]
    country_code: CountryCode
    timezone: str = Field(
        default="UTC",
        description="IANA timezone name. Validated against the system tz database.",
    )

    @field_validator("timezone")
    @classmethod
    def _tz_must_exist(cls, v: str) -> str:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
            raise ValueError(f"unknown IANA timezone: {v!r}") from exc
        return v


class CustomerUpdate(BaseModel):
    """PATCH semantics: every field optional, but at least one required.

    `None` and 'absent' are genuinely different here, which is why we inspect
    `model_fields_set` rather than testing for None.
    """

    name: Annotated[str, StringConstraints(min_length=1, max_length=200)] | None = None
    country_code: CountryCode | None = None
    timezone: str | None = None

    @model_validator(mode="after")
    def _at_least_one_field(self) -> CustomerUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH body must contain at least one field")
        return self


class CustomerOut(ORMModel):
    id: uuid.UUID
    email: str
    name: str
    country_code: str
    timezone: str
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None


# --------------------------------------------------------------------------- #
# Plans
# --------------------------------------------------------------------------- #


class PlanOut(ORMModel):
    id: int
    code: str
    monthly_cents: int
    currency: str
    active: bool


# --------------------------------------------------------------------------- #
# Subscriptions
# --------------------------------------------------------------------------- #


class SubscriptionCreate(BaseModel):
    customer_id: uuid.UUID
    plan_code: str
    start_trial: bool = Field(
        default=True,
        description="When true the subscription opens in 'trialing' with a "
        "14-day trial; otherwise it opens 'active' and bills immediately.",
    )
    started_at: datetime | None = Field(
        default=None,
        description="Back-dating hook used by the historical backfill. "
        "Rejected in production-like envs by the router, not here.",
    )


class SubscriptionPlanChange(BaseModel):
    plan_code: str
    occurred_at: datetime | None = None


class SubscriptionCancel(BaseModel):
    reason: Annotated[str, StringConstraints(max_length=500)] | None = None
    occurred_at: datetime | None = None


class SubscriptionOut(ORMModel):
    id: uuid.UUID
    customer_id: uuid.UUID
    plan_id: int
    status: str
    started_at: datetime
    ended_at: datetime | None
    trial_ends_at: datetime | None
    updated_at: datetime


class SubscriptionEventOut(ORMModel):
    id: int
    subscription_id: uuid.UUID
    event_type: str
    from_plan_id: int | None
    to_plan_id: int | None
    occurred_at: datetime
    created_at: datetime


# --------------------------------------------------------------------------- #
# Orders
# --------------------------------------------------------------------------- #


class OrderCreate(BaseModel):
    customer_id: uuid.UUID
    subscription_id: uuid.UUID | None = None
    amount_cents: int = Field(..., ge=0, le=100_000_000)
    currency: CurrencyCode
    placed_at: datetime | None = None
    placed_at_local: str | None = Field(
        default=None,
        description="Legacy mobile client field: naive 'YYYY-MM-DD HH:MM:SS'.",
    )

    @field_validator("currency")
    @classmethod
    def _supported(cls, v: str) -> str:
        if v not in SUPPORTED_CURRENCIES:
            raise ValueError(
                f"unsupported currency {v!r}; expected one of {sorted(SUPPORTED_CURRENCIES)}"
            )
        return v

    @field_validator("placed_at_local")
    @classmethod
    def _naive_shape(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            parsed = datetime.fromisoformat(v)
        except ValueError as exc:
            raise ValueError("placed_at_local must be ISO 'YYYY-MM-DD HH:MM:SS'") from exc
        if parsed.tzinfo is not None:
            raise ValueError("placed_at_local must be naive (no offset)")
        return v

    @model_validator(mode="after")
    def _one_timestamp_present(self) -> OrderCreate:
        if self.placed_at is None and self.placed_at_local is None:
            raise ValueError("one of placed_at or placed_at_local is required")
        return self


class OrderOut(ORMModel):
    id: uuid.UUID
    customer_id: uuid.UUID
    subscription_id: uuid.UUID | None
    amount_cents: int
    currency: str
    status: str
    placed_at: datetime | None
    placed_at_local: str | None
    updated_at: datetime


# --------------------------------------------------------------------------- #
# Payments and refunds
# --------------------------------------------------------------------------- #


class PaymentCreate(BaseModel):
    amount_cents: int = Field(..., ge=0, le=100_000_000)
    method: Literal["card", "paypal", "bank_transfer", "apple_pay"]
    # Simulation hook: lets the load generator drive the failure rate rather
    # than relying on chance. Absent means the service decides.
    force_status: Literal["pending", "succeeded", "failed"] | None = None
    processed_at: datetime | None = None


class PaymentOut(ORMModel):
    id: uuid.UUID
    order_id: uuid.UUID
    amount_cents: int
    method: str
    status: str
    processed_at: datetime | None
    created_at: datetime
    updated_at: datetime


class RefundCreate(BaseModel):
    amount_cents: int = Field(..., gt=0, le=100_000_000)
    reason: Annotated[str, StringConstraints(max_length=500)] | None = None
    issued_at: datetime | None = None


class RefundOut(ORMModel):
    id: uuid.UUID
    payment_id: uuid.UUID
    amount_cents: int
    reason: str | None
    issued_at: datetime
    created_at: datetime


# --------------------------------------------------------------------------- #
# Envelope types
# --------------------------------------------------------------------------- #


class HealthOut(BaseModel):
    status: Literal["ok", "degraded"]
    service: str
    version: str
    database: Literal["up", "down"]


class ErrorDetail(BaseModel):
    code: str
    message: str
    field: str | None = None


class ErrorOut(BaseModel):
    error: ErrorDetail
    request_id: str
