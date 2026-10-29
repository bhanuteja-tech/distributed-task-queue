"""Failure tests: Worker termination and abandoned task lease recovery."""

from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskqueue.broker.interface import BrokerMessage
from taskqueue.broker.memory import InMemoryBroker
from taskqueue.models.base import utc_now
from taskqueue.models.task import Task, TaskStatus
from taskqueue.outbox.publisher import OutboxPublisher
from taskqueue.outbox.reaper import LeaseReaper
from taskqueue.schemas.task import TaskCreate
from taskqueue.services.task_service import TaskService
from taskqueue.services.worker_service import Worker


async def _async_noop() -> None:
    pass


@pytest.mark.asyncio
async def test_reaper_reclaims_abandoned_task_from_crashed_worker(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
    task_service: TaskService,
) -> None:
    # 1. Create task
    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="report_generation",
                payload={"report_type": "sales"},
                max_attempts=3,
            ),
        )
        task_id = task.id

    # Publish the initial event so outbox queue is clear
    publisher = OutboxPublisher(
        session_factory=test_session_factory,
        broker=memory_broker,
    )
    await publisher.publish_pending_events()

    # 2. Simulate worker claiming task and then crashing:
    async with test_session_factory() as session:
        t = await session.get(Task, task_id)
        t.transition_to(TaskStatus.RUNNING)
        t.lease_worker_id = "crashed-worker-node-42"
        t.lease_expires_at = utc_now() - timedelta(seconds=10)
        t.attempt_count = 1
        await session.commit()

    # 3. Run LeaseReaper
    reaper = LeaseReaper(session_factory=test_session_factory)
    reclaimed_count = await reaper.reclaim_abandoned_tasks()
    assert reclaimed_count == 1

    # 4. Verify task state changed to RETRYING and lease cleared
    async with test_session_factory() as session:
        t = await session.get(Task, task_id)
        assert t.status == TaskStatus.RETRYING
        assert t.lease_worker_id is None
        assert t.lease_expires_at is None
        assert "crashed-worker-node-42" in t.error_message

    # 5. Outbox publisher publishes the recovery message
    published = await publisher.publish_pending_events()
    assert published == 1

    # 6. A healthy worker picks up the reclaimed task and completes it
    healthy_worker = Worker(
        broker=memory_broker,
        session_factory=test_session_factory,
        worker_id="healthy-worker-99",
    )
    msg = BrokerMessage(
        task_id=task_id,
        body={"task_id": task_id, "task_type": "report_generation"},
        priority=5,
        routing_key="tasks.default",
        _ack_callback=_async_noop,
    )
    await healthy_worker.process_task(msg)

    async with test_session_factory() as session:
        t = await session.get(Task, task_id)
        assert t.status == TaskStatus.SUCCEEDED
        assert t.result is not None
