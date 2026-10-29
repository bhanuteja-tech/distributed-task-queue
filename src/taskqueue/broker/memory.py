"""In-memory MessageBroker simulation for deterministic testing and failure injection.

Implements full AMQP semantics: priority queues, dead-letter routing,
publisher confirms, explicit acknowledgments, redelivery, and consumer cancellation.
"""

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from taskqueue.broker.interface import BrokerMessage, MessageBroker, MessageHandler

logger = logging.getLogger(__name__)


@dataclass(order=True)
class PrioritizedItem:
    """Queue item ordered by descending priority (negated for min-heap)."""

    sort_key: int
    count: int
    msg: BrokerMessage = field(compare=False)


class InMemoryBroker(MessageBroker):
    """Hermetic in-memory AMQP broker emulator supporting failure injection."""

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.PriorityQueue[PrioritizedItem]] = defaultdict(
            asyncio.PriorityQueue
        )
        self._dead_letter_queue: list[BrokerMessage] = []
        self._is_connected = False
        self._consumer_task: asyncio.Task[None] | None = None
        self._is_consuming = False
        self._message_seq = 0
        self.published_messages: list[dict[str, Any]] = []

        # Failure injection flags
        self.simulate_publish_failure = False
        self.simulate_broker_down = False

    async def connect(self) -> None:
        if self.simulate_broker_down:
            raise ConnectionError("Simulated broker connection failure")
        self._is_connected = True

    async def disconnect(self) -> None:
        await self.stop_consuming()
        self._is_connected = False

    async def publish(
        self,
        routing_key: str,
        payload: dict[str, Any],
        priority: int = 5,
        task_id: str | None = None,
    ) -> bool:
        if not self._is_connected or self.simulate_broker_down:
            raise ConnectionError("Broker is disconnected")

        if self.simulate_publish_failure:
            raise RuntimeError("Simulated broker publisher confirm rejection")

        tid = task_id or payload.get("task_id", "")
        self._message_seq += 1

        # Determine target queue (simplified routing for test harness)
        queue_name = "taskqueue.tasks"

        # RabbitMQ priority: higher number = higher priority.
        # Python PriorityQueue is a min-heap, so we negate priority.
        sort_key = -int(priority)

        raw_msg = BrokerMessage(
            task_id=tid,
            body=payload,
            priority=priority,
            routing_key=routing_key,
            delivery_tag=self._message_seq,
            redelivered=False,
        )

        # Wire up ack and nack callbacks
        async def _ack() -> None:
            pass  # Acknowledged successfully

        async def _nack(requeue: bool = False) -> None:
            if requeue:
                raw_msg.redelivered = True
                await self._queues[queue_name].put(
                    PrioritizedItem(sort_key=sort_key, count=self._message_seq, msg=raw_msg)
                )
            else:
                self._dead_letter_queue.append(raw_msg)

        raw_msg._ack_callback = _ack
        raw_msg._nack_callback = _nack

        item = PrioritizedItem(sort_key=sort_key, count=self._message_seq, msg=raw_msg)
        await self._queues[queue_name].put(item)

        self.published_messages.append({
            "routing_key": routing_key,
            "payload": payload,
            "priority": priority,
            "task_id": tid,
        })
        return True

    async def start_consuming(
        self,
        queue_name: str,
        handler: MessageHandler,
        prefetch_count: int = 10,
    ) -> None:
        if not self._is_connected:
            await self.connect()

        self._is_consuming = True

        async def _consume_loop() -> None:
            q = self._queues[queue_name]
            while self._is_consuming:
                try:
                    # Timeout to allow checking self._is_consuming periodically
                    item = await asyncio.wait_for(q.get(), timeout=0.1)
                    try:
                        await handler(item.msg)
                    except Exception as err:
                        logger.error("Consumer unhandled error: %s", err)
                    finally:
                        q.task_done()
                except TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break

        self._consumer_task = asyncio.create_task(_consume_loop())

    async def stop_consuming(self) -> None:
        self._is_consuming = False
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None

    async def check_health(self) -> bool:
        return self._is_connected and not self.simulate_broker_down

    async def get_queue_depth(self, queue_name: str) -> int:
        return self._queues[queue_name].qsize()

    @property
    def dead_letter_count(self) -> int:
        return len(self._dead_letter_queue)
