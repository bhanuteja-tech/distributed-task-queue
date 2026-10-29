"""Ledger and Account models for demonstrating business-level idempotency.

These models demonstrate how repeated execution or redelivery of the same task
does not produce duplicate side-effects (e.g., duplicate charges or double balance increments).
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    DateTime,
    Float,
    Index,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from taskqueue.models.base import Base, utc_now


class Account(Base):
    """Account model tracking balance."""

    __tablename__ = "accounts"

    id: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        doc="Account identifier (e.g. 'acc-123')",
    )
    balance: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=0.0,
        doc="Current financial balance",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
    )


class LedgerEntry(Base):
    """Ledger transaction record enforcing business-level idempotency.

    The unique constraint on transaction_id guarantees that even if a worker executes
    the same task repeatedly (e.g. due to network timeout right before ack), the transaction
    cannot be inserted twice, preventing double-crediting or duplicate side-effects.
    """

    __tablename__ = "ledger_entries"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
    )
    account_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
    )
    transaction_id: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        unique=True,
        index=True,
        doc="Unique transaction key providing business-level idempotency",
    )
    amount: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )
    operation: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        doc="CREDIT or DEBIT",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )

    __table_args__ = (
        Index("ix_ledger_account_tx", "account_id", "transaction_id"),
    )
