"""Initial tables: payments and outbox_events

Revision ID: 001
Revises:
Create Date: 2026-04-20

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    payment_status = postgresql.ENUM(
        "pending", "succeeded", "failed", name="payment_status", create_type=False
    )
    payment_status.create(op.get_bind(), checkfirst=True)

    outbox_event_status = postgresql.ENUM(
        "pending", "published", "failed", name="outbox_event_status", create_type=False
    )
    outbox_event_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "payments",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
        sa.Column(
            "status",
            payment_status,
            nullable=False,
            server_default="pending",
        ),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
    )

    op.create_table(
        "outbox_events",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("aggregate_id", sa.UUID(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "status",
            outbox_event_status,
            nullable=False,
            server_default="pending",
        ),
        sa.Column("retries", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_index(
        "ix_outbox_events_status_created",
        "outbox_events",
        ["status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_events_status_created", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_table("payments")

    op.execute("DROP TYPE IF EXISTS outbox_event_status")
    op.execute("DROP TYPE IF EXISTS payment_status")
