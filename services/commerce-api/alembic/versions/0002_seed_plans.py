"""seed reference plans

Revision ID: 0002
Revises: 0001
Create Date: 2025-01-06

Plans are reference data, not user data: there are four of them, the pricing
team changes them twice a year, and every environment needs identical values or
MRR is not comparable across them. That makes a migration the right home for
them rather than a fixture or a seed script.

`legacy_starter` is inactive and intentionally so -- it has live subscriptions
attached from before it was retired, which is why `GET /plans` needs an
`include_inactive` flag and why `dim_plan` cannot filter on `active`.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

PLANS = [
    {"code": "legacy_starter", "monthly_cents": 900, "currency": "USD", "active": False},
    {"code": "basic", "monthly_cents": 1900, "currency": "USD", "active": True},
    {"code": "pro", "monthly_cents": 4900, "currency": "USD", "active": True},
    {"code": "enterprise", "monthly_cents": 19900, "currency": "USD", "active": True},
]


def upgrade() -> None:
    plans = sa.table(
        "plans",
        sa.column("code", sa.Text),
        sa.column("monthly_cents", sa.Integer),
        sa.column("currency", sa.String),
        sa.column("active", sa.Boolean),
    )
    op.bulk_insert(plans, PLANS)


def downgrade() -> None:
    codes = ", ".join(f"'{p['code']}'" for p in PLANS)
    op.execute(f"DELETE FROM plans WHERE code IN ({codes})")
