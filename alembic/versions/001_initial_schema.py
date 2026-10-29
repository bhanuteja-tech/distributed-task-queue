"""Initial schema for tasks, outbox events, and ledger tables.

Revision ID: 001_initial_schema
Revises: 
Create Date: 2026-10-29 18:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create tasks table
    op.create_table(
        "tasks",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("task_type", sa.String(length=128), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="PENDING"),
        sa.Column("priority", sa.String(length=16), nullable=False, server_default="DEFAULT"),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("lease_worker_id", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_index("ix_tasks_task_type", "tasks", ["task_type"])
    op.create_index("ix_tasks_status", "tasks", ["status"])
    op.create_index("ix_tasks_idempotency_key", "tasks", ["idempotency_key"], unique=True)
    op.create_index("ix_tasks_lease_expires_at", "tasks", ["lease_expires_at"])
    op.create_index("ix_tasks_retry_at", "tasks", ["retry_at"])
    op.create_index("ix_tasks_created_at", "tasks", ["created_at"])
    op.create_index("ix_tasks_status_created_at", "tasks", ["status", "created_at"])
    op.create_index("ix_tasks_lease_reclamation", "tasks", ["status", "lease_expires_at"])

    # 2. Create outbox_events table
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("task_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False, server_default="TASK_CREATED"),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("routing_key", sa.String(length=128), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publish_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_index("ix_outbox_events_task_id", "outbox_events", ["task_id"])
    op.create_index("ix_outbox_events_created_at", "outbox_events", ["created_at"])
    op.create_index("ix_outbox_events_published_at", "outbox_events", ["published_at"])
    op.create_index("ix_outbox_events_locked_until", "outbox_events", ["locked_until"])
    op.create_index("ix_outbox_pending", "outbox_events", ["published_at", "locked_until", "created_at"])

    # 3. Create accounts table
    op.create_table(
        "accounts",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("balance", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    # 4. Create ledger_entries table (business idempotency)
    op.create_table(
        "ledger_entries",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("account_id", sa.String(length=64), nullable=False),
        sa.Column("transaction_id", sa.String(length=128), nullable=False),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("operation", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_index("ix_ledger_entries_account_id", "ledger_entries", ["account_id"])
    op.create_index("ix_ledger_entries_transaction_id", "ledger_entries", ["transaction_id"], unique=True)
    op.create_index("ix_ledger_account_tx", "ledger_entries", ["account_id", "transaction_id"])


def downgrade() -> None:
    op.drop_table("ledger_entries")
    op.drop_table("accounts")
    op.drop_table("outbox_events")
    op.drop_table("tasks")

