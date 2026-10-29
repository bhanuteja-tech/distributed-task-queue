"""Pytest fixtures for unit, API, integration, and failure recovery tests."""

from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from taskqueue.api.app import create_app
from taskqueue.api.dependencies import get_db_session, set_broker_instance
from taskqueue.broker.memory import InMemoryBroker
from taskqueue.cache.redis import TaskCache
from taskqueue.database import Base
from taskqueue.services.task_service import TaskService


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def test_engine() -> AsyncGenerator[AsyncEngine, None]:
    """Provide an isolated in-memory async SQLite engine for tests."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    await engine.dispose()


@pytest.fixture
async def test_session_factory(
    test_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """Provide an async sessionmaker bound to the test engine."""
    return async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


@pytest.fixture
async def db_session(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """Provide an isolated database session per test with automatic rollback."""
    async with test_session_factory() as session:
        yield session
        await session.rollback()


@pytest.fixture
async def memory_broker() -> AsyncGenerator[InMemoryBroker, None]:
    """Provide an active in-memory message broker."""
    broker = InMemoryBroker()
    await broker.connect()
    yield broker
    await broker.disconnect()


@pytest.fixture
def mock_cache() -> TaskCache:
    """Provide a disabled/isolated cache instance."""
    return TaskCache(enabled=False)


@pytest.fixture
def task_service(mock_cache: TaskCache) -> TaskService:
    """Provide a TaskService wired with mock cache."""
    return TaskService(cache_instance=mock_cache)


@pytest.fixture
async def client(
    test_session_factory: async_sessionmaker[AsyncSession],
    memory_broker: InMemoryBroker,
) -> AsyncGenerator[AsyncClient, None]:
    """HTTPX AsyncClient configured with test database session and in-memory broker."""
    app = create_app()

    async def _override_get_db_session() -> AsyncGenerator[AsyncSession, None]:
        async with test_session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = _override_get_db_session
    set_broker_instance(memory_broker)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac

    app.dependency_overrides.clear()
