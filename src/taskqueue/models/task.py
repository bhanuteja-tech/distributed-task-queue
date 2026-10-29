"""Task ORM model, State Machine, and Transition Rules.

Encapsulates task persistence, states, priorities, lifecycle timestamps,
lease tracking, and validation of allowed state transitions.
"""

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from taskqueue.exceptions import StateTransitionError
from taskqueue.models.base import Base, utc_now


class TaskStatus(str, enum.Enum):
    """Explicit lifecycle states for a task."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


class TaskPriority(str, enum.Enum):
    """Priority levels for task routing."""

    LOW = "LOW"
    DEFAULT = "DEFAULT"
    HIGH = "HIGH"

    @property
    def numeric_level(self) -> int:
        """Map to RabbitMQ priority level (1 to 10)."""
        mapping = {
            TaskPriority.LOW: 1,
            TaskPriority.DEFAULT: 5,
            TaskPriority.HIGH: 10,
        }
        return mapping[self]


# Allowed state transitions: {CurrentState: {AllowedTargetStates}}
ALLOWED_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING: {TaskStatus.RUNNING, TaskStatus.FAILED},
    TaskStatus.RUNNING: {
        TaskStatus.SUCCEEDED,
        TaskStatus.RETRYING,
        TaskStatus.FAILED,
        TaskStatus.DEAD_LETTER,
        TaskStatus.RUNNING,  # Lease heartbeats keep it RUNNING
    },
    TaskStatus.RETRYING: {
        TaskStatus.PENDING,
        TaskStatus.RUNNING,
        TaskStatus.FAILED,
        TaskStatus.DEAD_LETTER,
    },
    TaskStatus.FAILED: {TaskStatus.PENDING},  # Only via explicit retry API
    TaskStatus.DEAD_LETTER: {TaskStatus.PENDING},  # Only via explicit operator retry
    TaskStatus.SUCCEEDED: set(),  # Terminal state: cannot transition out
}


class Task(Base):
    """Task database entity representing durable asynchronous units of work."""

    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        doc="Unique UUID primary key",
    )
    task_type: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        index=True,
        doc="Registered handler type identifier",
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        doc="Input parameters serialized as JSON",
    )
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, name="task_status_enum", native_enum=False),
        nullable=False,
        default=TaskStatus.PENDING,
        index=True,
        doc="Current lifecycle state",
    )
    priority: Mapped[TaskPriority] = mapped_column(
        Enum(TaskPriority, name="task_priority_enum", native_enum=False),
        nullable=False,
        default=TaskPriority.DEFAULT,
        doc="Execution priority level",
    )
    idempotency_key: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        unique=True,
        index=True,
        doc="Client-supplied key to prevent duplicate creation",
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        doc="Number of times execution has been attempted",
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3,
        doc="Maximum permitted attempts before dead lettering",
    )
    result: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        doc="Structured output on successful execution",
    )
    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc="Error details or stack summary on failure",
    )
    version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        doc="Optimistic locking counter for concurrent updates",
    )

    # Lease & worker tracking for crash recovery
    lease_worker_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        doc="Identifier of the worker currently processing this task",
    )
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
        doc="Timestamp after which task is considered abandoned and eligible for reclamation",
    )
    retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
        doc="Timestamp before which retrying task should not be picked up",
    )

    # Lifecycle Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        index=True,
        doc="Time task was accepted by the API",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
        doc="Time task record was last modified",
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Time execution first began",
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Time task reached a terminal state (SUCCEEDED or FAILED)",
    )

    __table_args__ = (
        Index("ix_tasks_status_created_at", "status", "created_at"),
        Index("ix_tasks_lease_reclamation", "status", "lease_expires_at"),
    )

    def __init__(self, **kwargs: Any) -> None:
        if "status" not in kwargs:
            kwargs["status"] = TaskStatus.PENDING
        if "version" not in kwargs:
            kwargs["version"] = 1
        super().__init__(**kwargs)

    def transition_to(self, target_state: TaskStatus) -> None:
        """Validate and apply a state transition.

        Raises StateTransitionError if the requested transition is illegal according
        to ALLOWED_TRANSITIONS.
        """
        current_status = self.status or TaskStatus.PENDING
        if target_state not in ALLOWED_TRANSITIONS.get(current_status, set()):
            raise StateTransitionError(
                current_state=current_status.value,
                target_state=target_state.value,
                task_id=self.id,
            )

        now = utc_now()
        self.status = target_state
        self.updated_at = now
        self.version = (self.version or 0) + 1

        if target_state == TaskStatus.RUNNING and not self.started_at:
            self.started_at = now

        if target_state in (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.DEAD_LETTER):
            self.completed_at = now
            self.lease_expires_at = None
            self.lease_worker_id = None
