"""Transactional Outbox Publisher.

Periodically queries unpublished outbox events from PostgreSQL and publishes them
to RabbitMQ with publisher confirms. Marks records as published ONLY after the broker
has confirmed receipt.
"""

import asyncio
import logging
from datetime import timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskqueue.broker.interface import MessageBroker
from taskqueue.config import settings
from taskqueue.metrics import OUTBOX_PUBLISH_FAILURES_TOTAL, OUTBOX_PUBLISHED_TOTAL
from taskqueue.models.base import utc_now
from taskqueue.models.outbox import OutboxEvent

logger = logging.getLogger(__name__)


class OutboxPublisher:
    """Asynchronous outbox worker ensuring at-least-once message delivery from DB to broker."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: MessageBroker,
        batch_size: int | None = None,
        poll_interval: float | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.broker = broker
        self.batch_size = batch_size or settings.outbox_batch_size
        self.poll_interval = poll_interval or settings.outbox_poll_interval_seconds
        self._is_running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the background publishing loop."""
        self._is_running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Outbox publisher started (poll_interval=%.2fs)", self.poll_interval)

    async def stop(self) -> None:
        """Stop publishing gracefully."""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Outbox publisher stopped")

    async def publish_pending_events(self) -> int:
        """Fetch and publish a batch of pending outbox events.

        Returns:
            Number of events successfully published to the message broker.
        """
        now = utc_now()
        lock_expiry = now + timedelta(seconds=settings.outbox_lock_timeout_seconds)
        events_to_publish: list[dict[str, Any]] = []

        # 1. Claim pending events under lock
        async with self.session_factory() as session:
            stmt = (
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .where(
                    (OutboxEvent.locked_until.is_(None)) | (OutboxEvent.locked_until < now)
                )
                .order_by(OutboxEvent.created_at.asc())
                .limit(self.batch_size)
            )
            result = await session.scalars(stmt)
            claimed = list(result.all())

            for event in claimed:
                event.locked_until = lock_expiry
                event.publish_attempts += 1
                events_to_publish.append({
                    "id": event.id,
                    "task_id": event.task_id,
                    "routing_key": event.routing_key,
                    "priority": event.priority,
                    "payload": event.payload,
                    "task_type": event.payload.get("task_type", "unknown"),
                })

            if claimed:
                await session.commit()

        if not events_to_publish:
            return 0

        published_count = 0

        # 2. Publish to broker with publisher confirms
        for item in events_to_publish:
            event_id = item["id"]
            task_id = item["task_id"]
            routing_key = item["routing_key"]
            priority = item["priority"]
            payload = item["payload"]
            task_type = item["task_type"]

            try:
                # Publish with broker confirmation
                confirmed = await self.broker.publish(
                    routing_key=routing_key,
                    payload=payload,
                    priority=priority,
                    task_id=task_id,
                )

                if confirmed:
                    # Mark published in DB
                    async with self.session_factory() as session:
                        ev = await session.get(OutboxEvent, event_id)
                        if ev:
                            ev.published_at = utc_now()
                            ev.locked_until = None
                            await session.commit()

                    published_count += 1
                    OUTBOX_PUBLISHED_TOTAL.labels(task_type=task_type).inc()
                    logger.debug("Published outbox event for task %s", task_id)
                else:
                    raise RuntimeError("Broker did not confirm message")

            except Exception as e:
                logger.warning("Failed to publish outbox event for task %s: %s", task_id, e)
                OUTBOX_PUBLISH_FAILURES_TOTAL.labels(task_type=task_type).inc()
                # Release lock so it can be retried on next poll
                try:
                    async with self.session_factory() as session:
                        ev = await session.get(OutboxEvent, event_id)
                        if ev:
                            ev.locked_until = None
                            ev.last_error = str(e)
                            await session.commit()
                except Exception:
                    pass

        return published_count

    async def _run_loop(self) -> None:
        while self._is_running:
            try:
                published = await self.publish_pending_events()
                # Back off if queue was empty
                if published == 0:
                    await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as err:
                logger.error("Error in outbox publisher loop: %s", err)
                await asyncio.sleep(self.poll_interval)
