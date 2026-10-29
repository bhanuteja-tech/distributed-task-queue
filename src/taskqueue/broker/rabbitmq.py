"""RabbitMQ implementation of MessageBroker using aio-pika.

Configures durable exchanges, priority queues, consumer prefetch, publisher confirms,
and dead-letter exchange (DLX) routing.
"""

import asyncio
import json
import logging
from typing import Any

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    DeliveryMode,
)

from taskqueue.broker.interface import BrokerMessage, MessageBroker, MessageHandler
from taskqueue.config import settings

logger = logging.getLogger(__name__)


class RabbitMQBroker(MessageBroker):
    """RabbitMQ client managing channels, topology, publisher confirms, and consumers."""

    def __init__(self, url: str | None = None) -> None:
        self.url = url or settings.rabbitmq_url
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._exchange: AbstractExchange | None = None
        self._dlx_exchange: AbstractExchange | None = None
        self._consumer_tag: str | None = None
        self._active_queue: AbstractQueue | None = None
        self._is_connected = False
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Establish AMQP connection with retries and declare core topology."""
        async with self._lock:
            if self._is_connected and self._connection and not self._connection.is_closed:
                return

            retries = settings.rabbitmq_connection_retries
            delay = settings.rabbitmq_retry_interval_seconds

            for attempt in range(1, retries + 1):
                try:
                    logger.info("Connecting to RabbitMQ at %s (attempt %d/%d)", self.url, attempt, retries)
                    self._connection = await aio_pika.connect_robust(self.url)
                    self._channel = await self._connection.channel(publisher_confirms=settings.rabbitmq_publisher_confirms)
                    await self._setup_topology()
                    self._is_connected = True
                    logger.info("Successfully connected to RabbitMQ and declared topology")
                    return
                except Exception as err:
                    logger.warning("RabbitMQ connection attempt %d failed: %s", attempt, err)
                    if attempt == retries:
                        raise ConnectionError(f"Failed to connect to RabbitMQ after {retries} attempts: {err}") from err
                    await asyncio.sleep(delay)

    async def _setup_topology(self) -> None:
        """Declare durable exchanges, queues, DLX, and routing bindings."""
        if not self._channel:
            raise RuntimeError("Channel is not open")

        # 1. Dead Letter Exchange & Queue
        self._dlx_exchange = await self._channel.declare_exchange(
            settings.rabbitmq_dead_letter_exchange,
            type=aio_pika.ExchangeType.FANOUT,
            durable=True,
        )
        dlq = await self._channel.declare_queue(
            settings.rabbitmq_dead_letter_queue,
            durable=True,
        )
        await dlq.bind(self._dlx_exchange)

        # 2. Main Direct Exchange
        self._exchange = await self._channel.declare_exchange(
            settings.rabbitmq_exchange,
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        # 3. Main Task Queue with Priority support and Dead-Lettering
        queue_args = {
            "x-max-priority": 10,
            "x-dead-letter-exchange": settings.rabbitmq_dead_letter_exchange,
        }
        main_queue = await self._channel.declare_queue(
            settings.rabbitmq_queue_prefix,
            durable=True,
            arguments=queue_args,
        )

        # Bind priorities to main exchange
        for routing_key in ("tasks.high", "tasks.default", "tasks.low"):
            await main_queue.bind(self._exchange, routing_key=routing_key)

        self._active_queue = main_queue

    async def disconnect(self) -> None:
        """Gracefully close channel and connection."""
        async with self._lock:
            if self._channel and not self._channel.is_closed:
                await self._channel.close()
            if self._connection and not self._connection.is_closed:
                await self._connection.close()
            self._is_connected = False
            logger.info("RabbitMQ connection closed cleanly")

    async def publish(
        self,
        routing_key: str,
        payload: dict[str, Any],
        priority: int = 5,
        task_id: str | None = None,
    ) -> bool:
        """Publish a persistent message with publisher confirmation."""
        if not self._channel or self._channel.is_closed:
            await self.connect()

        if not self._exchange:
            raise RuntimeError("Exchange not initialized")

        body_bytes = json.dumps(payload).encode("utf-8")
        message = aio_pika.Message(
            body=body_bytes,
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=max(1, min(10, priority)),
            correlation_id=task_id,
            content_type="application/json",
        )

        # Publisher confirm: publish returns Confirmation when confirms are enabled
        await self._exchange.publish(message, routing_key=routing_key)
        return True

    async def start_consuming(
        self,
        queue_name: str,
        handler: MessageHandler,
        prefetch_count: int = 10,
    ) -> None:
        """Subscribe to queue with explicit QoS prefetch and manual ack wrapping."""
        if not self._channel or self._channel.is_closed:
            await self.connect()

        if not self._channel:
            raise RuntimeError("Cannot start consumer: channel not open")

        # Set QoS consumer prefetch to limit unacknowledged work
        await self._channel.set_qos(prefetch_count=prefetch_count)

        queue = await self._channel.get_queue(queue_name)

        async def _message_wrapper(incoming: AbstractIncomingMessage) -> None:
            # We do NOT use "async with incoming.process():" because we need explicit,
            # deliberate control over acknowledgment timing. Acks are sent ONLY
            # after PostgreSQL transaction commits.
            try:
                body = json.loads(incoming.body.decode("utf-8"))
            except Exception as e:
                logger.error("Failed to parse message JSON: %s. Dead-lettering.", e)
                await incoming.reject(requeue=False)
                return

            task_id = incoming.correlation_id or body.get("task_id", "")

            broker_msg = BrokerMessage(
                task_id=task_id,
                body=body,
                priority=incoming.priority or 5,
                routing_key=incoming.routing_key or "",
                delivery_tag=incoming.delivery_tag,
                redelivered=incoming.redelivered,
                _ack_callback=incoming.ack,
                _nack_callback=lambda requeue: incoming.reject(requeue=requeue),
            )

            await handler(broker_msg)

        self._consumer_tag = await queue.consume(_message_wrapper, no_ack=False)
        logger.info("Started consuming from '%s' with prefetch=%d", queue_name, prefetch_count)

    async def stop_consuming(self) -> None:
        """Cancel consumer subscription."""
        if self._channel and self._consumer_tag:
            try:
                queue = await self._channel.get_queue(settings.rabbitmq_queue_prefix)
                await queue.cancel(self._consumer_tag)
                logger.info("Cancelled consumer tag %s", self._consumer_tag)
            except Exception as e:
                logger.warning("Error cancelling consumer: %s", e)
            finally:
                self._consumer_tag = None

    async def check_health(self) -> bool:
        """Return True if connection is alive and channel is open."""
        return bool(
            self._is_connected
            and self._connection
            and not self._connection.is_closed
            and self._channel
            and not self._channel.is_closed
        )

    async def get_queue_depth(self, queue_name: str) -> int:
        """Query queue message count."""
        if not self._channel or self._channel.is_closed:
            return 0
        try:
            queue = await self._channel.declare_queue(queue_name, passive=True)
            if queue.declaration_result:
                return queue.declaration_result.message_count
            return 0
        except Exception:
            return 0

