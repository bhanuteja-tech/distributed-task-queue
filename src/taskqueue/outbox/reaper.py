"""Lease Recovery Reaper for abandoned RUNNING tasks.

Scans for tasks whose worker lease has expired (e.g. because worker died from SIGKILL,
OOM, or network partition). Reclaims them safely by re-queuing or dead-lettering.
"""

import asyncio

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskqueue.config import settings
from taskqueue.logging import get_logger
from taskqueue.models.base import utc_now
from taskqueue.models.outbox import OutboxEvent
from taskqueue.models.task import Task, TaskStatus

logger = get_logger(__name__)


class LeaseReaper:
    """Detects and reclaims abandoned tasks with expired worker leases."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        reaper_interval: float | None = None,
        batch_size: int | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.reaper_interval = reaper_interval or settings.reaper_interval_seconds
        self.batch_size = batch_size or settings.reaper_batch_size
        self._is_running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start background reaper loop."""
        self._is_running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Lease reaper started (interval=%.2fs)", self.reaper_interval)

    async def stop(self) -> None:
        """Stop reaper gracefully."""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Lease reaper stopped")

    async def reclaim_abandoned_tasks(self) -> int:
        """Find and recover tasks in RUNNING state whose lease expired.

        Returns:
            Number of abandoned tasks reclaimed.
        """
        now = utc_now()
        reclaimed_count = 0

        async with self.session_factory() as session:
            # Query RUNNING tasks with expired leases
            stmt = (
                select(Task)
                .where(Task.status == TaskStatus.RUNNING)
                .where(Task.lease_expires_at.is_not(None))
                .where(Task.lease_expires_at < now)
                .limit(self.batch_size)
            )
            result = await session.scalars(stmt)
            abandoned_tasks = list(result.all())

            for task in abandoned_tasks:
                dead_worker = task.lease_worker_id
                logger.warning(
                    "Reclaiming abandoned task %s (worker: %s, expired: %s, attempts: %d/%d)",
                    task.id,
                    dead_worker,
                    task.lease_expires_at,
                    task.attempt_count,
                    task.max_attempts,
                )

                if task.attempt_count >= task.max_attempts:
                    # Exhausted attempts
                    task.transition_to(TaskStatus.DEAD_LETTER)
                    task.error_message = (
                        f"Task lease expired for worker {dead_worker} "
                        f"and max attempts ({task.max_attempts}) reached"
                    )
                else:
                    # Transition to RETRYING / PENDING and create an OutboxEvent for re-publication
                    task.transition_to(TaskStatus.RETRYING)
                    task.error_message = f"Reclaimed from unresponsive worker {dead_worker}"
                    task.lease_worker_id = None
                    task.lease_expires_at = None

                    routing_key_map = {
                        "HIGH": "tasks.high",
                        "DEFAULT": "tasks.default",
                        "LOW": "tasks.low",
                    }
                    routing_key = routing_key_map.get(task.priority.value, "tasks.default")

                    outbox_event = OutboxEvent(
                        task_id=task.id,
                        event_type="TASK_RECLAIMED",
                        payload={
                            "task_id": task.id,
                            "task_type": task.task_type,
                            "priority": task.priority.numeric_level,
                            "routing_key": routing_key,
                            "reclaimed_from": dead_worker,
                        },
                        routing_key=routing_key,
                        priority=task.priority.numeric_level,
                    )
                    session.add(outbox_event)

                reclaimed_count += 1

            if abandoned_tasks:
                await session.commit()

        return reclaimed_count

    async def _run_loop(self) -> None:
        while self._is_running:
            try:
                await self.reclaim_abandoned_tasks()
                await asyncio.sleep(self.reaper_interval)
            except asyncio.CancelledError:
                break
            except Exception as err:
                logger.error("Error in lease reaper loop: %s", err)
                await asyncio.sleep(self.reaper_interval)
