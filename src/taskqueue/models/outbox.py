"""Transactional Outbox ORM model.

Ensures atomic persistence of tasks alongside their outgoing broker messages.
The outbox publisher worker polls unpublished events and guarantees at-least-once
delivery to RabbitMQ with publisher confirms.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from taskqueue.models.base import Base, utc_now


class OutboxEvent(Base):
    """Represents a message pending publication to the message broker."""

    __tablename__ = "outbox_events"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        doc="Unique outbox event identifier",
    )
    task_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        index=True,
        doc="Referenced task ID",
    )
    event_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default="TASK_CREATED",
        doc="Type of event: TASK_CREATED, TASK_RETRY, etc.",
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        doc="Broker message payload (task_id, task_type, priority, etc.)",
    )
    routing_key: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        doc="RabbitMQ routing key (e.g. tasks.high, tasks.default, tasks.low)",
    )
    priority: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=5,
        doc="RabbitMQ message priority level (1-10)",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        index=True,
        doc="Timestamp event was written in the transaction",
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
        doc="Timestamp broker confirmed durable receipt",
    )
    publish_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of times outbox publisher attempted publishing",
    )
    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc="Last error message if broker publish failed",
    )
    locked_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
        doc="Distributed lock timestamp to prevent multiple publishers claiming same event",
    )

    __table_args__ = (
        Index("ix_outbox_pending", "published_at", "locked_until", "created_at"),
    )
