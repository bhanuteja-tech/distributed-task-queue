"""Pydantic schemas for Task API request validation and response serialization."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from taskqueue.models.task import TaskPriority, TaskStatus


class TaskCreate(BaseModel):
    """Payload to submit a new asynchronous task."""

    model_config = ConfigDict(extra="forbid")

    task_type: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Identifier of registered task handler",
        examples=["report_generation", "ledger_batch_processor", "word_count_aggregation"],
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Structured input arguments passed to the task handler",
        examples=[{"report_type": "monthly_revenue", "date_range": "2026-Q1"}],
    )
    priority: TaskPriority = Field(
        default=TaskPriority.DEFAULT,
        description="Execution priority (LOW, DEFAULT, HIGH)",
    )
    idempotency_key: str | None = Field(
        default=None,
        max_length=255,
        description="Optional client-provided unique key to prevent duplicate creation",
        examples=["sub-order-98741-attempt-1"],
    )
    max_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum retry attempts on retryable failures before dead-lettering",
    )


class TaskResponse(BaseModel):
    """Detailed task status and execution metadata representation."""

    model_config = ConfigDict(from_attributes=True)

    id: str = Field(..., description="Unique task identifier")
    task_type: str = Field(..., description="Registered handler name")
    payload: dict[str, Any] = Field(..., description="Task input parameters")
    status: TaskStatus = Field(..., description="Current lifecycle state")
    priority: TaskPriority = Field(..., description="Routing priority")
    idempotency_key: str | None = Field(None, description="Idempotency key if supplied")
    attempt_count: int = Field(..., description="Execution attempts completed")
    max_attempts: int = Field(..., description="Maximum allowed attempts")
    result: dict[str, Any] | None = Field(None, description="Result on success")
    error_message: str | None = Field(None, description="Error reason on failure")
    version: int = Field(..., description="Optimistic locking revision number")
    created_at: datetime = Field(..., description="Submission timestamp")
    updated_at: datetime = Field(..., description="Last state modification timestamp")
    started_at: datetime | None = Field(None, description="First processing timestamp")
    completed_at: datetime | None = Field(None, description="Terminal state timestamp")


class TaskListResponse(BaseModel):
    """Paginated collection of tasks."""

    items: list[TaskResponse]
    total: int = Field(..., description="Total matching tasks in database")
    limit: int = Field(..., description="Items requested per page")
    offset: int = Field(..., description="Current offset")
    has_next: bool = Field(..., description="Whether subsequent items exist")


class TaskRetryRequest(BaseModel):
    """Optional configuration when requesting a manual retry of a failed task."""

    reason: str | None = Field(
        default=None,
        description="Operator explanation for triggering manual retry",
    )

