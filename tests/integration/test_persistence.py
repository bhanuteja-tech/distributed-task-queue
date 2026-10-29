"""Integration tests for database persistence, relationships, and queries."""

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue.models.outbox import OutboxEvent
from taskqueue.models.task import TaskPriority
from taskqueue.schemas.task import TaskCreate
from taskqueue.services.task_service import TaskService


@pytest.mark.asyncio
async def test_task_and_outbox_persistence_in_single_transaction(
    db_session: AsyncSession,
    task_service: TaskService,
) -> None:
    task_in = TaskCreate(
        task_type="report_generation",
        payload={"report_type": "sales_q1"},
        priority=TaskPriority.HIGH,
    )

    task, is_duplicate = await task_service.create_task(db_session, task_in)
    assert not is_duplicate
    assert task.id is not None

    # Query outbox table directly
    stmt = select(OutboxEvent).where(OutboxEvent.task_id == task.id)
    outbox_event = await db_session.scalar(stmt)

    assert outbox_event is not None
    assert outbox_event.task_id == task.id
    assert outbox_event.routing_key == "tasks.high"
    assert outbox_event.priority == 10
    assert outbox_event.published_at is None
