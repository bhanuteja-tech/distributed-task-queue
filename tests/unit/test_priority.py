"""Unit tests for task priorities and priority queue ordering."""

import pytest

from taskqueue.broker.memory import InMemoryBroker
from taskqueue.models.task import TaskPriority


def test_priority_numeric_levels() -> None:
    assert TaskPriority.LOW.numeric_level == 1
    assert TaskPriority.DEFAULT.numeric_level == 5
    assert TaskPriority.HIGH.numeric_level == 10


@pytest.mark.asyncio
async def test_priority_queue_ordering(memory_broker: InMemoryBroker) -> None:
    # Publish low priority task first, then high priority task
    await memory_broker.publish(
        routing_key="tasks.low",
        payload={"task_id": "low-1"},
        priority=TaskPriority.LOW.numeric_level,
    )
    await memory_broker.publish(
        routing_key="tasks.high",
        payload={"task_id": "high-1"},
        priority=TaskPriority.HIGH.numeric_level,
    )
    await memory_broker.publish(
        routing_key="tasks.default",
        payload={"task_id": "med-1"},
        priority=TaskPriority.DEFAULT.numeric_level,
    )

    received_order: list[str] = []

    async def _test_handler(msg) -> None:
        received_order.append(msg.task_id)
        await msg.ack()

    # Start consuming and allow consumer to drain messages
    await memory_broker.start_consuming(
        queue_name="taskqueue.tasks",
        handler=_test_handler,
    )

    import asyncio
    for _ in range(20):
        if len(received_order) == 3:
            break
        await asyncio.sleep(0.05)

    await memory_broker.stop_consuming()

    # The high priority message (10) must be processed first,
    # followed by default (5), then low (1)!
    assert received_order == ["high-1", "med-1", "low-1"]

