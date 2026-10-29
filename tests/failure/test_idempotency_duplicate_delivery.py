"""Failure tests: Idempotency and duplicate message delivery prevention.

Tests that deliberately delivering the same task message more than once
does NOT duplicate business side-effects (e.g. account balance crediting).
"""

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskqueue.broker.interface import BrokerMessage
from taskqueue.broker.memory import InMemoryBroker
from taskqueue.models.ledger import Account, LedgerEntry
from taskqueue.models.task import Task, TaskStatus
from taskqueue.schemas.task import TaskCreate
from taskqueue.services.task_service import TaskService
from taskqueue.services.worker_service import Worker


@pytest.mark.asyncio
async def test_duplicate_message_delivery_does_not_duplicate_business_effect(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
    task_service: TaskService,
) -> None:
    # 1. Create a task that credits an account with $250.00
    account_id = "acc-idempotent-test"
    tx_id = "tx-unique-credit-999"

    async with test_session_factory() as session:
        task, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="ledger_batch_processor",
                payload={
                    "account_id": account_id,
                    "transactions": [
                        {"transaction_id": tx_id, "amount": 250.0, "operation": "CREDIT"}
                    ],
                },
            ),
        )
        task_id = task.id

    worker = Worker(
        broker=memory_broker,
        session_factory=test_session_factory,
        worker_id="worker-node-1",
    )

    # 2. Simulate First Delivery from RabbitMQ
    first_msg_acked = False

    async def _ack1() -> None:
        nonlocal first_msg_acked
        first_msg_acked = True

    msg1 = BrokerMessage(
        task_id=task_id,
        body={"task_id": task_id, "task_type": "ledger_batch_processor"},
        priority=5,
        routing_key="tasks.default",
        _ack_callback=_ack1,
    )

    await worker.process_task(msg1)

    assert first_msg_acked is True

    # Verify task is SUCCEEDED and balance is $250.00
    async with test_session_factory() as session:
        t = await session.get(Task, task_id)
        assert t.status == TaskStatus.SUCCEEDED

        acc = await session.get(Account, account_id)
        assert acc.balance == 250.0

        entries = list(
            (await session.scalars(select(LedgerEntry).where(LedgerEntry.account_id == account_id))).all()
        )
        assert len(entries) == 1

    # 3. Deliberately simulate RabbitMQ duplicate delivery of the SAME task message
    second_msg_acked = False

    async def _ack2() -> None:
        nonlocal second_msg_acked
        second_msg_acked = True

    msg2 = BrokerMessage(
        task_id=task_id,
        body={"task_id": task_id, "task_type": "ledger_batch_processor"},
        priority=5,
        routing_key="tasks.default",
        redelivered=True,
        _ack_callback=_ack2,
    )

    await worker.process_task(msg2)

    # The worker must acknowledge the duplicate message immediately without re-executing
    assert second_msg_acked is True

    # 4. Verify account was NOT credited twice!
    async with test_session_factory() as session:
        acc = await session.get(Account, account_id)
        assert acc.balance == 250.0, f"Expected 250.0 but found {acc.balance} (Double credit detected!)"

        entries = list(
            (await session.scalars(select(LedgerEntry).where(LedgerEntry.account_id == account_id))).all()
        )
        assert len(entries) == 1, "Duplicate ledger entry created!"

