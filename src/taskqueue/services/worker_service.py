"""Worker execution engine managing task claiming, state transitions, leases, and graceful shutdown."""

import asyncio
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskqueue.broker.interface import BrokerMessage, MessageBroker
from taskqueue.cache.redis import TaskCache
from taskqueue.cache.redis import cache as default_cache
from taskqueue.config import settings
from taskqueue.exceptions import (
    PermanentTaskError,
    RetryableTaskError,
    StateTransitionError,
)
from taskqueue.handlers.base import BaseTaskHandler, registry
from taskqueue.logging import correlation_task_id, correlation_worker_id
from taskqueue.metrics import (
    ACTIVE_WORKERS_GAUGE,
    RUNNING_TASKS_GAUGE,
    TASK_END_TO_END_DURATION_SECONDS,
    TASK_EXECUTION_DURATION_SECONDS,
    TASKS_COMPLETED_TOTAL,
    TASKS_DEAD_LETTERED_TOTAL,
    TASKS_RETRIED_TOTAL,
)
from taskqueue.models.base import ensure_utc, utc_now
from taskqueue.models.outbox import OutboxEvent
from taskqueue.models.task import Task, TaskStatus
from taskqueue.services.task_service import calculate_retry_delay

logger = logging.getLogger(__name__)


class Worker:
    """Independent worker consuming and executing tasks with bounded concurrency and crash safety."""

    def __init__(
        self,
        broker: MessageBroker,
        session_factory: async_sessionmaker[AsyncSession],
        worker_id: str | None = None,
        concurrency: int | None = None,
        cache_instance: TaskCache | None = None,
    ) -> None:
        self.broker = broker
        self.session_factory = session_factory
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.concurrency = concurrency or settings.worker_concurrency
        self.cache = cache_instance or default_cache

        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._active_tasks: set[asyncio.Task[Any]] = set()
        self._is_running = False
        self._is_stopping = False

    async def start(self, queue_name: str | None = None) -> None:
        """Start consuming tasks from the message broker."""
        self._is_running = True
        self._is_stopping = False
        correlation_worker_id.set(self.worker_id)
        ACTIVE_WORKERS_GAUGE.inc()

        q_name = queue_name or settings.rabbitmq_queue_prefix
        logger.info(
            "Worker %s starting with concurrency=%d, prefetch=%d",
            self.worker_id,
            self.concurrency,
            settings.worker_prefetch_count,
        )

        await self.broker.start_consuming(
            queue_name=q_name,
            handler=self.handle_message,
            prefetch_count=settings.worker_prefetch_count,
        )

    async def stop(self) -> None:
        """Gracefully shut down the worker, allowing in-flight tasks to complete."""
        if self._is_stopping or not self._is_running:
            return

        self._is_stopping = True
        logger.info("Worker %s initiating graceful shutdown...", self.worker_id)

        # 1. Stop consuming new messages from the broker
        try:
            await self.broker.stop_consuming()
        except Exception as e:
            logger.warning("Error stopping consumer: %s", e)

        # 2. Await currently running tasks within timeout
        if self._active_tasks:
            logger.info(
                "Worker %s waiting for %d active tasks to complete...",
                self.worker_id,
                len(self._active_tasks),
            )
            done, pending = await asyncio.wait(
                self._active_tasks,
                timeout=settings.worker_shutdown_timeout_seconds,
            )
            if pending:
                logger.warning(
                    "Worker %s shutdown timeout exceeded; cancelling %d pending tasks",
                    self.worker_id,
                    len(pending),
                )
                for t in pending:
                    t.cancel()

        ACTIVE_WORKERS_GAUGE.dec()
        self._is_running = False
        logger.info("Worker %s stopped cleanly", self.worker_id)

    async def handle_message(self, msg: BrokerMessage) -> None:
        """Entrypoint for incoming broker messages under bounded concurrency."""
        if self._is_stopping:
            # Requeue message if worker is actively shutting down
            await msg.nack(requeue=True)
            return

        # Acquire concurrency semaphore before scheduling
        await self._semaphore.acquire()

        task = asyncio.create_task(self._process_message_wrapper(msg))
        self._active_tasks.add(task)
        RUNNING_TASKS_GAUGE.inc()

        def _cleanup(t: asyncio.Task[Any]) -> None:
            self._active_tasks.discard(t)
            self._semaphore.release()
            RUNNING_TASKS_GAUGE.dec()

        task.add_done_callback(_cleanup)

    async def _process_message_wrapper(self, msg: BrokerMessage) -> None:
        correlation_worker_id.set(self.worker_id)
        correlation_task_id.set(msg.task_id)
        try:
            await self.process_task(msg)
        except Exception as e:
            logger.exception("Unexpected error processing task %s: %s", msg.task_id, e)
            try:
                await msg.nack(requeue=False)
            except Exception:
                pass

    async def process_task(self, msg: BrokerMessage) -> None:
        """Core execution pipeline: claim, execute handler with heartbeat, persist outcome, and ack."""
        task_id = msg.task_id
        start_time = time.perf_counter()

        # Step 1: Claim task and inspect current state
        async with self.session_factory() as session:
            task = await session.get(Task, task_id)
            if not task:
                logger.warning("Task %s not found in database. Dead-lettering message.", task_id)
                await msg.nack(requeue=False)
                return

            # Idempotency / Duplicate Delivery Guard:
            # If task is already SUCCEEDED, acknowledge message and skip execution.
            if task.status == TaskStatus.SUCCEEDED:
                logger.info("Task %s is already SUCCEEDED. Acknowledging duplicate delivery.", task_id)
                await msg.ack()
                return

            # If task is currently claimed by another active worker with valid lease
            now = utc_now()
            lease_exp = ensure_utc(task.lease_expires_at)
            if (
                task.status == TaskStatus.RUNNING
                and task.lease_worker_id != self.worker_id
                and lease_exp
                and lease_exp > now
            ):
                logger.warning(
                    "Task %s is actively claimed by %s (lease expires %s). Re-queuing.",
                    task_id,
                    task.lease_worker_id,
                    task.lease_expires_at,
                )
                await msg.nack(requeue=True)
                return

            # Claim the task
            task.lease_worker_id = self.worker_id
            task.lease_expires_at = now + timedelta(seconds=settings.worker_lease_duration_seconds)
            task.attempt_count += 1

            try:
                task.transition_to(TaskStatus.RUNNING)
            except StateTransitionError as err:
                logger.error("State transition error claiming task %s: %s", task_id, err)
                await msg.nack(requeue=False)
                return

            await session.commit()
            task_type = task.task_type
            payload = dict(task.payload)
            attempt_count = task.attempt_count
            max_attempts = task.max_attempts
            created_at = task.created_at

        # Step 2: Spawn lease heartbeat background task
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(task_id, heartbeat_stop)
        )

        # Step 3: Execute task handler
        execution_error: Exception | None = None
        result: dict[str, Any] | None = None

        try:
            handler: BaseTaskHandler = registry.get(task_type)
            async with self.session_factory() as handler_session:
                result = await handler.execute(task_id, payload, handler_session)
                await handler_session.commit()
        except Exception as e:
            execution_error = e
        finally:
            # Stop lease heartbeat
            heartbeat_stop.set()
            await heartbeat_task

        duration = time.perf_counter() - start_time
        TASK_EXECUTION_DURATION_SECONDS.labels(task_type=task_type).observe(duration)

        # Step 4: Handle outcomes and persist state
        if execution_error is None:
            await self._handle_success(msg, task_id, task_type, result, created_at)
        elif isinstance(execution_error, PermanentTaskError):
            await self._handle_permanent_failure(msg, task_id, task_type, execution_error)
        else:
            await self._handle_retryable_failure(
                msg,
                task_id,
                task_type,
                execution_error,
                attempt_count,
                max_attempts,
            )

    async def _handle_success(
        self,
        msg: BrokerMessage,
        task_id: str,
        task_type: str,
        result: dict[str, Any] | None,
        created_at: datetime,
    ) -> None:
        """Persist SUCCEEDED state in PostgreSQL before acknowledging RabbitMQ."""
        async with self.session_factory() as session:
            task = await session.get(Task, task_id)
            if task:
                task.result = result
                task.error_message = None
                task.transition_to(TaskStatus.SUCCEEDED)
                await session.commit()

        # Invalidate cache
        await self.cache.invalidate(task_id)

        # Acknowledge message only AFTER durable state transition is committed
        await msg.ack()

        TASKS_COMPLETED_TOTAL.labels(task_type=task_type, status="SUCCEEDED").inc()
        if created_at:
            e2e_duration = (utc_now() - ensure_utc(created_at)).total_seconds()
            TASK_END_TO_END_DURATION_SECONDS.labels(task_type=task_type).observe(e2e_duration)

        logger.info("Task %s completed successfully", task_id)

    async def _handle_permanent_failure(
        self,
        msg: BrokerMessage,
        task_id: str,
        task_type: str,
        err: Exception,
    ) -> None:
        """Handle non-retryable error: fail immediately without exhausting remaining attempts."""
        async with self.session_factory() as session:
            task = await session.get(Task, task_id)
            if task:
                task.error_message = f"Permanent failure: {err}"
                task.transition_to(TaskStatus.FAILED)
                await session.commit()

        await self.cache.invalidate(task_id)
        # Nack without requeue routes to Dead-Letter Exchange (DLX)
        await msg.nack(requeue=False)

        TASKS_COMPLETED_TOTAL.labels(task_type=task_type, status="FAILED").inc()
        logger.error("Task %s permanently failed: %s", task_id, err)

    async def _handle_retryable_failure(
        self,
        msg: BrokerMessage,
        task_id: str,
        task_type: str,
        err: Exception,
        attempt_count: int,
        max_attempts: int,
    ) -> None:
        """Handle transient error: apply exponential backoff or dead-letter if max attempts reached."""
        if attempt_count < max_attempts:
            # Calculate backoff delay
            retry_delay = calculate_retry_delay(attempt_count)
            if isinstance(err, RetryableTaskError) and err.retry_delay_seconds:
                retry_delay = err.retry_delay_seconds

            retry_at = utc_now() + timedelta(seconds=retry_delay)

            async with self.session_factory() as session:
                task = await session.get(Task, task_id)
                if task:
                    task.error_message = f"Attempt {attempt_count}/{max_attempts} failed: {err}"
                    task.retry_at = retry_at
                    task.transition_to(TaskStatus.RETRYING)

                    # Create outbox event for re-publishing when retry_at is reached
                    routing_key_map = {
                        "HIGH": "tasks.high",
                        "DEFAULT": "tasks.default",
                        "LOW": "tasks.low",
                    }
                    routing_key = routing_key_map.get(task.priority.value, "tasks.default")

                    outbox_event = OutboxEvent(
                        task_id=task.id,
                        event_type="TASK_RETRY",
                        payload={
                            "task_id": task.id,
                            "task_type": task.task_type,
                            "priority": task.priority.numeric_level,
                            "routing_key": routing_key,
                            "attempt": attempt_count + 1,
                        },
                        routing_key=routing_key,
                        priority=task.priority.numeric_level,
                    )
                    session.add(outbox_event)
                    await session.commit()

            await self.cache.invalidate(task_id)
            # Acknowledge current message from broker so it does not hold the prefetch slot;
            # the outbox publisher will dispatch the retried task
            await msg.ack()

            TASKS_RETRIED_TOTAL.labels(task_type=task_type).inc()
            logger.warning(
                "Task %s failed (attempt %d/%d). Scheduled retry in %.2fs. Error: %s",
                task_id,
                attempt_count,
                max_attempts,
                retry_delay,
                err,
            )
        else:
            # Max attempts exhausted: transition to DEAD_LETTER
            async with self.session_factory() as session:
                task = await session.get(Task, task_id)
                if task:
                    task.error_message = f"Max attempts ({max_attempts}) exhausted. Last error: {err}"
                    task.transition_to(TaskStatus.DEAD_LETTER)
                    await session.commit()

            await self.cache.invalidate(task_id)
            await msg.nack(requeue=False)

            TASKS_DEAD_LETTERED_TOTAL.labels(task_type=task_type, reason="max_attempts_exceeded").inc()
            logger.error("Task %s exhausted max attempts (%d). Moved to DEAD_LETTER.", task_id, max_attempts)

    async def _heartbeat_loop(self, task_id: str, stop_event: asyncio.Event) -> None:
        """Periodically extend task lease while worker is running."""
        interval = settings.worker_heartbeat_interval_seconds
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                break
            except TimeoutError:
                # Interval elapsed; refresh lease
                try:
                    async with self.session_factory() as session:
                        task = await session.get(Task, task_id)
                        if task and task.status == TaskStatus.RUNNING and task.lease_worker_id == self.worker_id:
                            task.lease_expires_at = utc_now() + timedelta(
                                seconds=settings.worker_lease_duration_seconds
                            )
                            await session.commit()
                            logger.debug("Refreshed lease for task %s", task_id)
                except Exception as e:
                    logger.warning("Failed to refresh lease for task %s: %s", task_id, e)
