"""Base interface and central registry for Task Handlers.

All task executions are dispatched through this registry. Handlers must inherit
from BaseTaskHandler and implement execute().
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue.exceptions import PermanentTaskError

logger = logging.getLogger(__name__)


class BaseTaskHandler(ABC):
    """Abstract base class for all task handlers."""

    task_type: str

    @abstractmethod
    async def execute(
        self,
        task_id: str,
        payload: dict[str, Any],
        session: AsyncSession,
    ) -> dict[str, Any]:
        """Execute the task's business logic.

        Args:
            task_id: Unique task identifier.
            payload: Input payload parameters.
            session: Active SQLAlchemy database session for transactional side effects.

        Returns:
            Structured dict containing the execution result.

        Raises:
            RetryableTaskError: When an issue is temporary (e.g. timeout, lock contention).
            PermanentTaskError: When payload is malformed or business rule is violated.
        """
        raise NotImplementedError


class HandlerRegistry:
    """Thread-safe registry mapping task_type string keys to handler instances."""

    def __init__(self) -> None:
        self._handlers: dict[str, BaseTaskHandler] = {}

    def register(self, handler: BaseTaskHandler) -> None:
        """Register a handler instance for its task_type."""
        if not handler.task_type:
            raise ValueError("Handler must specify a non-empty task_type")
        self._handlers[handler.task_type] = handler
        logger.info("Registered task handler: '%s'", handler.task_type)

    def get(self, task_type: str) -> BaseTaskHandler:
        """Retrieve handler for task_type.

        Raises:
            PermanentTaskError: If the task_type is unrecognized.
        """
        handler = self._handlers.get(task_type)
        if not handler:
            raise PermanentTaskError(f"No handler registered for task type: '{task_type}'")
        return handler

    def list_supported_types(self) -> list[str]:
        """Return all registered task type identifiers."""
        return sorted(list(self._handlers.keys()))


# Global handler registry instance
registry = HandlerRegistry()
