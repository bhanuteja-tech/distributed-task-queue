"""Integration tests for Outbox Publisher reliability and confirms."""

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskqueue.broker.memory import InMemoryBroker
from taskqueue.models.outbox import OutboxEvent
from taskqueue.outbox.publisher import OutboxPublisher
from taskqueue.schemas.task import TaskCreate
from taskqueue.services.task_service import TaskService


@pytest.mark.asyncio
async def test_outbox_publisher_publishes_and_marks_confirmed(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
    task_service: TaskService,
) -> None:
    # 1. Create task with outbox event
    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(task_type="report_generation", payload={"report_type": "daily"}),
        )
        task_id = task.id

    # 2. Run Outbox Publisher
    publisher = OutboxPublisher(
        session_factory=test_session_factory,
        broker=memory_broker,
        batch_size=10,
    )
    published_count = await publisher.publish_pending_events()
    assert published_count == 1

    # 3. Verify outbox table has published_at timestamp
    async with test_session_factory() as session:
        ev = await session.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task_id))
        assert ev is not None
        assert ev.published_at is not None

    # 4. Verify message landed in broker
    assert len(memory_broker.published_messages) == 1
    assert memory_broker.published_messages[0]["task_id"] == task_id


@pytest.mark.asyncio
async def test_outbox_publisher_tolerates_broker_failure(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
    task_service: TaskService,
) -> None:
    # 1. Create task
    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(task_type="report_generation", payload={"report_type": "daily"}),
        )
        task_id = task.id

    # 2. Simulate broker publish failure
    memory_broker.simulate_publish_failure = True
    publisher = OutboxPublisher(
        session_factory=test_session_factory,
        broker=memory_broker,
        batch_size=10,
    )

    published_count = await publisher.publish_pending_events()
    assert published_count == 0

    # 3. Verify event remains UNPUBLISHED with last_error recorded
    async with test_session_factory() as session:
        ev = await session.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task_id))
        assert ev is not None
        assert ev.published_at is None
        assert ev.last_error is not None
        assert "Simulated broker" in ev.last_error

    # 4. Restore broker and retry publishing
    memory_broker.simulate_publish_failure = False
    published_count = await publisher.publish_pending_events()
    assert published_count == 1

    async with test_session_factory() as session:
        ev = await session.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task_id))
        assert ev is not None
        assert ev.published_at is not None

