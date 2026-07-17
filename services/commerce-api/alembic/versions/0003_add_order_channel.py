"""add orders.channel -- the mid-project schema change

Revision ID: 0003
Revises: 0002
Create Date: 2025-02-17

This migration exists to be applied *while the pipeline is running*, not at
setup. Run it with `make schema-change` once CDC has been streaming for a
while, then watch:

  1. Debezium picks up the new column on the next change to `orders`.
  2. `schema_guard.py` classifies it as ADDITIVE, accepts the batch, logs at
     INFO and writes a row to `_schema_changes`.
  3. The Parquet for `orders` gains a nullable `channel` column. Older
     partitions do not have it, so the warehouse must tolerate a union of two
     shapes.
  4. `stg_orders` reads it with a defaulting expression so the model does not
     break on the historical files.

Adding a nullable column with no default is deliberately the *safe* shape --
it takes no table rewrite and no long lock even on 5M rows. A NOT NULL column
with a default would have been the interesting failure on older Postgres; on
16 it is also cheap, but the nullable form is what the pipeline should be
proven against first.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "orders",
        sa.Column(
            "channel",
            sa.Text(),
            nullable=True,
            comment="Acquisition channel: web | mobile | api. Added 2025-02-17; "
            "NULL for every order placed before that date.",
        ),
    )
    # Deliberately NOT backfilled. A backfill would rewrite 5M rows, flood the
    # WAL, and hand the CDC sink 5M synthetic updates that mean nothing. The
    # warehouse coalesces NULL to 'unknown' instead -- one line of SQL against
    # an hour of write amplification.


def downgrade() -> None:
    op.drop_column("orders", "channel")


# Note on the application side: `app.models.Order` deliberately does NOT declare
# `channel`. The running API predates this column and neither reads nor writes
# it -- which is the normal shape of a real migration, where the DDL lands ahead
# of the deploy that uses it. It is also the more demanding test for the
# pipeline: Debezium starts emitting `channel` on every subsequent change event
# regardless of whether the application knows the column exists. `make
# schema-change` populates it for recently-updated rows so the warehouse sees
# both the NULL history and real values.
