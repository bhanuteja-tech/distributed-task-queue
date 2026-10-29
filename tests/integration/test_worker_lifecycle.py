"""Integration tests for end-to-end worker execution lifecycle."""

import asyncio

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskqueue.broker.memory import InMemoryBroker
from taskqueue.models.task import Task, TaskPriority, TaskStatus
from taskqueue.outbox.publisher import OutboxPublisher
from taskqueue.schemas.task import TaskCreate
from taskqueue.services.task_service import TaskService
from taskqueue.services.worker_service import Worker


@pytest.mark.asyncio
async def test_end_to_end_worker_processing(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
    task_service: TaskService,
) -> None:
    # 1. Submit task
    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="word_count_aggregation",
                payload={"text": "hello distributed queue hello systems"},
                priority=TaskPriority.HIGH,
            ),
        )
        task_id = task.id

    # 2. Publish outbox message to broker
    publisher = OutboxPublisher(
        session_factory=test_session_factory,
        broker=memory_broker,
    )
    await publisher.publish_pending_events()

    # 3. Start worker
    worker = Worker(
        broker=memory_broker,
        session_factory=test_session_factory,
        worker_id="test-worker-1",
        concurrency=2,
    )
    await worker.start()

    # 4. Await task completion
    completed = False
    for _ in range(30):
        async with test_session_factory() as session:
            t = await session.get(Task, task_id)
            if t and t.status == TaskStatus.SUCCEEDED:
                completed = True
                assert t.result is not None
                assert t.result["total_words"] == 5
                assert t.result["unique_words"] == 4
                assert t.completed_at is not None
                break
        await asyncio.sleep(0.05)

    await worker.stop()
    assert completed, "Task failed to reach SUCCEEDED state within timeout"
