"""Core task service managing creation, idempotency, queries, and manual retries."""

import logging
import random

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue.cache.redis import TaskCache
from taskqueue.cache.redis import cache as default_cache
from taskqueue.config import settings
from taskqueue.exceptions import (
    IdempotencyConflictError,
    PermanentTaskError,
    StateTransitionError,
    TaskNotFoundError,
)
from taskqueue.handlers.base import registry
from taskqueue.metrics import TASKS_SUBMITTED_TOTAL
from taskqueue.models.outbox import OutboxEvent
from taskqueue.models.task import Task, TaskPriority, TaskStatus
from taskqueue.schemas.task import TaskCreate

logger = logging.getLogger(__name__)


def calculate_retry_delay(
    attempt: int,
    initial_delay: float | None = None,
    max_delay: float | None = None,
    backoff_base: float | None = None,
    jitter: bool | None = None,
) -> float:
    """Calculate exponential backoff retry delay with optional jitter.

    Formula:
        delay = min(max_delay, initial_delay * (backoff_base ** (attempt - 1)))
        If jitter is enabled, adds +/- 20% random variance to prevent thundering herd.
    """
    init_d = (
        initial_delay
        if initial_delay is not None
        else settings.default_initial_retry_delay_seconds
    )
    max_d = (
        max_delay
        if max_delay is not None
        else settings.default_max_retry_delay_seconds
    )
    base = (
        backoff_base
        if backoff_base is not None
        else settings.default_exponential_backoff_base
    )
    use_jitter = jitter if jitter is not None else settings.default_jitter

    # Compute raw exponential delay
    delay = init_d * (base ** max(0, attempt - 1))
    delay = min(delay, max_d)

    if use_jitter and delay > 0:
        # Uniform jitter within [delay * 0.8, delay * 1.2]
        delay = delay * random.uniform(0.8, 1.2)

    return round(delay, 3)


class TaskService:
    """Encapsulates task business logic and transactional outbox persistence."""

    def __init__(self, cache_instance: TaskCache | None = None) -> None:
        self.cache = cache_instance or default_cache

    async def create_task(self, session: AsyncSession, task_in: TaskCreate) -> tuple[Task, bool]:
        """Create a new task and its corresponding outbox event in a single atomic transaction.

        Returns:
            tuple of (Task, is_duplicate). If idempotency_key was previously submitted
            with matching payload, returns existing task and True.
        """
        # Validate that task_type is registered
        if task_in.task_type not in registry.list_supported_types():
            raise PermanentTaskError(
                f"Unknown task_type: '{task_in.task_type}'. "
                f"Supported types: {registry.list_supported_types()}"
            )

        # 1. Check idempotency key if provided
        if task_in.idempotency_key:
            stmt = select(Task).where(Task.idempotency_key == task_in.idempotency_key)
            existing_task = await session.scalar(stmt)
            if existing_task:
                # Compare payloads to detect client conflicts
                if existing_task.payload != task_in.payload or existing_task.task_type != task_in.task_type:
                    raise IdempotencyConflictError(
                        key=task_in.idempotency_key,
                        task_id=existing_task.id,
                    )
                logger.info(
                    "Idempotency hit for key '%s'. Returning existing task %s",
                    task_in.idempotency_key,
                    existing_task.id,
                )
                return existing_task, True

        # 2. Map priority to RabbitMQ routing key
        routing_key_map = {
            TaskPriority.HIGH: "tasks.high",
            TaskPriority.DEFAULT: "tasks.default",
            TaskPriority.LOW: "tasks.low",
        }
        routing_key = routing_key_map.get(task_in.priority, "tasks.default")

        # 3. Create Task entity
        task = Task(
            task_type=task_in.task_type,
            payload=task_in.payload,
            priority=task_in.priority,
            idempotency_key=task_in.idempotency_key,
            max_attempts=task_in.max_attempts,
            status=TaskStatus.PENDING,
        )
        session.add(task)
        await session.flush()  # Populates task.id

        # 4. Create Transactional Outbox Event in the SAME database transaction
        outbox_event = OutboxEvent(
            task_id=task.id,
            event_type="TASK_CREATED",
            payload={
                "task_id": task.id,
                "task_type": task.task_type,
                "priority": task.priority.numeric_level,
                "routing_key": routing_key,
            },
            routing_key=routing_key,
            priority=task.priority.numeric_level,
        )
        session.add(outbox_event)

        try:
            await session.commit()
        except IntegrityError as e:
            await session.rollback()
            # Race condition on unique constraint for idempotency key
            if task_in.idempotency_key:
                existing = await session.scalar(
                    select(Task).where(Task.idempotency_key == task_in.idempotency_key)
                )
                if existing:
                    return existing, True
            raise e

        # Invalidate cache if entry existed
        await self.cache.invalidate(task.id)

        # Record metric
        TASKS_SUBMITTED_TOTAL.labels(
            task_type=task.task_type,
            priority=task.priority.value,
        ).inc()

        logger.info("Submitted task %s of type '%s'", task.id, task.task_type)
        return task, False

    async def get_task(self, session: AsyncSession, task_id: str) -> Task:
        """Fetch task by ID. Checks cache first, falls back to authoritative database."""
        # Check cache
        cached_data = await self.cache.get_task_status(task_id)
        if cached_data:
            logger.debug("Cache hit for task %s", task_id)
            # Fetch full task from DB to ensure complete model state if needed,
            # or we can construct/read directly. To satisfy DB authoritativeness:
            # cache provides fast status, but when returning ORM model, query DB:
            pass

        task = await session.get(Task, task_id)
        if not task:
            raise TaskNotFoundError(task_id)

        # Update cache asynchronously
        await self.cache.set_task_status(
            task_id,
            {
                "id": task.id,
                "status": task.status.value,
                "attempt_count": task.attempt_count,
                "result": task.result,
                "error_message": task.error_message,
                "version": task.version,
            },
        )

        return task

    async def list_tasks(
        self,
        session: AsyncSession,
        status: TaskStatus | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[Task], int]:
        """List tasks with pagination and optional status filter."""
        limit = max(1, min(100, limit))
        offset = max(0, offset)

        query = select(Task)
        count_query = select(func.count(Task.id))

        if status:
            query = query.where(Task.status == status)
            count_query = count_query.where(Task.status == status)

        query = query.order_by(Task.created_at.desc()).limit(limit).offset(offset)

        total = await session.scalar(count_query) or 0
        result = await session.scalars(query)
        tasks = list(result.all())

        return tasks, total

    async def retry_failed_task(
        self,
        session: AsyncSession,
        task_id: str,
        reason: str | None = None,
    ) -> Task:
        """Manually trigger a retry for an eligible failed or dead-letter task."""
        task = await session.get(Task, task_id)
        if not task:
            raise TaskNotFoundError(task_id)

        if task.status not in (TaskStatus.FAILED, TaskStatus.DEAD_LETTER):
            raise StateTransitionError(
                current_state=task.status.value,
                target_state=TaskStatus.PENDING.value,
                task_id=task.id,
            )

        task.transition_to(TaskStatus.PENDING)
        task.error_message = f"Manual retry initiated. Previous failure: {task.error_message}"
        task.retry_at = None

        # Re-publish outbox event
        routing_key_map = {
            TaskPriority.HIGH: "tasks.high",
            TaskPriority.DEFAULT: "tasks.default",
            TaskPriority.LOW: "tasks.low",
        }
        routing_key = routing_key_map.get(task.priority, "tasks.default")

        outbox_event = OutboxEvent(
            task_id=task.id,
            event_type="TASK_MANUAL_RETRY",
            payload={
                "task_id": task.id,
                "task_type": task.task_type,
                "priority": task.priority.numeric_level,
                "routing_key": routing_key,
                "retry_reason": reason,
            },
            routing_key=routing_key,
            priority=task.priority.numeric_level,
        )
        session.add(outbox_event)

        await session.commit()
        await self.cache.invalidate(task.id)

        logger.info("Manually requeued failed task %s", task.id)
        return task
