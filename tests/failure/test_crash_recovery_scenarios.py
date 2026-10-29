"""Tests for critical distributed failure and recovery scenarios."""

from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskqueue.broker.interface import BrokerMessage
from taskqueue.broker.memory import InMemoryBroker
from taskqueue.models.base import utc_now
from taskqueue.models.task import Task, TaskStatus
from taskqueue.outbox.publisher import OutboxPublisher
from taskqueue.schemas.task import TaskCreate
from taskqueue.services.task_service import TaskService
from taskqueue.services.worker_service import Worker


@pytest.mark.asyncio
async def test_scenario_1_api_commits_but_broker_offline(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
    task_service: TaskService,
) -> None:
    """Scenario 1: API commits task to PostgreSQL, but broker publish fails.

    The Transactional Outbox Pattern ensures the message is stored atomically in PostgreSQL.
    When the broker recovers, the Outbox Publisher reliably delivers it.
    """
    # 1. Broker is temporarily offline
    memory_broker.simulate_broker_down = True

    # API creates task in DB without attempting direct broker send
    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="report_generation",
                payload={"report_type": "offline_test"},
            ),
        )
        task_id = task.id

    # 2. Broker comes back online
    memory_broker.simulate_broker_down = False
    await memory_broker.connect()

    # 3. Outbox Publisher runs and recovers pending event
    publisher = OutboxPublisher(
        session_factory=test_session_factory,
        broker=memory_broker,
    )
    published = await publisher.publish_pending_events()
    assert published == 1

    # 4. Message is now in the queue
    assert len(memory_broker.published_messages) == 1
    assert memory_broker.published_messages[0]["task_id"] == task_id


@pytest.mark.asyncio
async def test_scenario_8_two_workers_encounter_same_task(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
    task_service: TaskService,
) -> None:
    """Scenario 8: Two workers encounter the same logical task concurrently.

    Worker A acquires the lease. Worker B attempts to process the task, detects
    an active lease from Worker A, and re-queues the message without conflicting.
    """
    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="report_generation",
                payload={"report_type": "concurrent_test", "simulated_delay_seconds": 0.2},
            ),
        )
        task_id = task.id

        # Worker A claims the task with an active 60s lease
        task.transition_to(TaskStatus.RUNNING)
        task.lease_worker_id = "worker-alpha"
        task.lease_expires_at = utc_now() + timedelta(seconds=60)
        await session.commit()

    # Worker B receives the duplicate message
    worker_b = Worker(
        broker=memory_broker,
        session_factory=test_session_factory,
        worker_id="worker-bravo",
    )

    msg_requeued = False

    async def _nack_callback(requeue: bool) -> None:
        nonlocal msg_requeued
        if requeue:
            msg_requeued = True

    msg = BrokerMessage(
        task_id=task_id,
        body={"task_id": task_id, "task_type": "report_generation"},
        priority=5,
        routing_key="tasks.default",
        _nack_callback=_nack_callback,
    )

    await worker_b.process_task(msg)

    # Worker B must not run the task; it must nack with requeue=True
    assert msg_requeued is True


@pytest.mark.asyncio
async def test_scenario_10_retry_persisted_across_application_restart(
    test_session_factory: async_sessionmaker[AsyncSession],
    task_service: TaskService,
) -> None:
    """Scenario 10: A retry is scheduled and the application restarts before execution.

    The retry timestamp and state are persisted in PostgreSQL, surviving a full process restart.
    """
    # 1. Create and set task to RETRYING
    retry_time = utc_now() + timedelta(seconds=15)
    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(task_type="report_generation", payload={"report_type": "restart_test"}),
        )
        task_id = task.id

        task.transition_to(TaskStatus.RUNNING)
        task.transition_to(TaskStatus.RETRYING)
        task.retry_at = retry_time
        task.attempt_count = 1
        await session.commit()

    # 2. Simulate complete restart: new session, inspect persisted state
    async with test_session_factory() as new_session:
        restored_task = await new_session.get(Task, task_id)
        assert restored_task is not None
        assert restored_task.status == TaskStatus.RETRYING
        assert restored_task.attempt_count == 1
        assert restored_task.retry_at is not None
