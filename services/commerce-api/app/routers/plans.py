from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Plan
from app.schemas import PlanOut

router = APIRouter(prefix="/plans", tags=["plans"])


@router.get("", response_model=list[PlanOut])
def list_plans(
    include_inactive: bool = Query(
        default=False,
        description="Retired plans still have live subscriptions attached, so "
        "the warehouse needs them even though the storefront does not.",
    ),
    session: Session = Depends(get_db),
) -> list[Plan]:
    stmt = select(Plan).order_by(Plan.monthly_cents)
    if not include_inactive:
        stmt = stmt.where(Plan.active.is_(True))
    return list(session.execute(stmt).scalars().all())
