"""FastAPI dependencies for database sessions, services, and brokers."""

from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue.broker.interface import MessageBroker
from taskqueue.broker.rabbitmq import RabbitMQBroker
from taskqueue.cache.redis import TaskCache
from taskqueue.cache.redis import cache as default_cache
from taskqueue.database import get_db_session
from taskqueue.services.task_service import TaskService

# Singleton instances for API lifecycle
_broker_instance: MessageBroker | None = None


def get_broker() -> MessageBroker:
    """Return shared broker instance for API process."""
    global _broker_instance
    if _broker_instance is None:
        _broker_instance = RabbitMQBroker()
    return _broker_instance


def set_broker_instance(broker: MessageBroker) -> None:
    """Override broker instance (used in test harness)."""
    global _broker_instance
    _broker_instance = broker


def get_task_service(
    cache: Annotated[TaskCache, Depends(lambda: default_cache)],
) -> TaskService:
    """Dependency injecting TaskService."""
    return TaskService(cache_instance=cache)


DbSession = Annotated[AsyncSession, Depends(get_db_session)]
Service = Annotated[TaskService, Depends(get_task_service)]
Broker = Annotated[MessageBroker, Depends(get_broker)]
