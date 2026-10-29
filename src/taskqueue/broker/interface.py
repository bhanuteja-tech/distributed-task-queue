"""Abstract Message Broker interface and message abstractions."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any


@dataclass
class BrokerMessage:
    """Standardized message envelope received by consumers."""

    task_id: str
    body: dict[str, Any]
    priority: int
    routing_key: str
    delivery_tag: Any = None
    redelivered: bool = False
    _ack_callback: Callable[[], Coroutine[Any, Any, None]] | None = None
    _nack_callback: Callable[[bool], Coroutine[Any, Any, None]] | None = None

    async def ack(self) -> None:
        """Acknowledge message processing success to the broker."""
        if self._ack_callback:
            await self._ack_callback()

    async def nack(self, requeue: bool = False) -> None:
        """Reject message, optionally re-queuing or forwarding to dead-letter exchange."""
        if self._nack_callback:
            await self._nack_callback(requeue)


MessageHandler = Callable[[BrokerMessage], Coroutine[Any, Any, None]]


class MessageBroker(ABC):
    """Abstract interface defining required message broker capabilities."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the broker."""
        raise NotImplementedError

    @abstractmethod
    async def disconnect(self) -> None:
        """Close broker connection cleanly."""
        raise NotImplementedError

    @abstractmethod
    async def publish(
        self,
        routing_key: str,
        payload: dict[str, Any],
        priority: int = 5,
        task_id: str | None = None,
    ) -> bool:
        """Publish a message with publisher confirmation.

        Returns:
            True if broker confirmed receipt; False or raises on failure.
        """
        raise NotImplementedError

    @abstractmethod
    async def start_consuming(
        self,
        queue_name: str,
        handler: MessageHandler,
        prefetch_count: int = 10,
    ) -> None:
        """Subscribe to a queue with explicit consumer prefetch."""
        raise NotImplementedError

    @abstractmethod
    async def stop_consuming(self) -> None:
        """Stop accepting new messages from the broker."""
        raise NotImplementedError

    @abstractmethod
    async def check_health(self) -> bool:
        """Verify broker connectivity and readiness."""
        raise NotImplementedError

    @abstractmethod
    async def get_queue_depth(self, queue_name: str) -> int:
        """Return the current number of pending messages in the queue."""
        raise NotImplementedError

