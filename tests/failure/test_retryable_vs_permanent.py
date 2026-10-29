"""Failure tests: Distinguishing retryable transient errors vs permanent errors."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskqueue.broker.interface import BrokerMessage
from taskqueue.broker.memory import InMemoryBroker
from taskqueue.models.task import Task, TaskStatus
from taskqueue.schemas.task import TaskCreate
from taskqueue.services.task_service import TaskService
from taskqueue.services.worker_service import Worker


async def _async_noop() -> None:
    pass


async def _async_noop_nack(requeue: bool) -> None:
    pass


@pytest.mark.asyncio
async def test_retryable_error_schedules_retry(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
    task_service: TaskService,
) -> None:
    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="report_generation",
                payload={"report_type": "sales", "simulate_error": "transient"},
                max_attempts=3,
            ),
        )
        task_id = task.id

    worker = Worker(
        broker=memory_broker,
        session_factory=test_session_factory,
        worker_id="worker-retry-1",
    )

    msg = BrokerMessage(
        task_id=task_id,
        body={"task_id": task_id, "task_type": "report_generation"},
        priority=5,
        routing_key="tasks.default",
        _ack_callback=_async_noop,
    )
    await worker.process_task(msg)

    async with test_session_factory() as session:
        t = await session.get(Task, task_id)
        assert t.status == TaskStatus.RETRYING
        assert t.attempt_count == 1
        assert t.retry_at is not None
        assert "Transient" in t.error_message


@pytest.mark.asyncio
async def test_retryable_error_exhausts_max_attempts_to_dead_letter(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
    task_service: TaskService,
) -> None:
    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="report_generation",
                payload={"report_type": "sales", "simulate_error": "transient"},
                max_attempts=1,
            ),
        )
        task_id = task.id

    worker = Worker(
        broker=memory_broker,
        session_factory=test_session_factory,
        worker_id="worker-deadletter-1",
    )

    dead_lettered_in_broker = False

    async def _nack_callback(requeue: bool) -> None:
        nonlocal dead_lettered_in_broker
        if not requeue:
            dead_lettered_in_broker = True

    msg = BrokerMessage(
        task_id=task_id,
        body={"task_id": task_id, "task_type": "report_generation"},
        priority=5,
        routing_key="tasks.default",
        _nack_callback=_nack_callback,
    )
    await worker.process_task(msg)

    assert dead_lettered_in_broker is True

    async with test_session_factory() as session:
        t = await session.get(Task, task_id)
        assert t.status == TaskStatus.DEAD_LETTER
        assert t.attempt_count == 1
        assert "exhausted" in t.error_message.lower()


@pytest.mark.asyncio
async def test_permanent_error_fails_immediately_without_retrying(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
    task_service: TaskService,
) -> None:
    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="report_generation",
                payload={"report_type": "sales", "simulate_error": "permanent"},
                max_attempts=5,
            ),
        )
        task_id = task.id

    worker = Worker(
        broker=memory_broker,
        session_factory=test_session_factory,
        worker_id="worker-perm-1",
    )

    msg = BrokerMessage(
        task_id=task_id,
        body={"task_id": task_id, "task_type": "report_generation"},
        priority=5,
        routing_key="tasks.default",
        _nack_callback=_async_noop_nack,
    )
    await worker.process_task(msg)

    async with test_session_factory() as session:
        t = await session.get(Task, task_id)
        assert t.status == TaskStatus.FAILED
        assert t.attempt_count == 1
        assert "Permanent failure" in t.error_message
