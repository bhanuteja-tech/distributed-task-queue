"""Database engine, session factory, and connection lifecycle."""

import logging
from collections.abc import AsyncGenerator
from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from taskqueue.config import settings
from taskqueue.models.base import Base

logger = logging.getLogger(__name__)


def create_engine(url: str | None = None, **kwargs: Any) -> AsyncEngine:
    """Create and configure an asynchronous SQLAlchemy engine."""
    db_url = url or settings.database_url
    engine_kwargs: dict[str, Any] = {
        "echo": False,
        "future": True,
    }

    if db_url.startswith("sqlite"):
        # SQLite async options for local development and tests
        engine_kwargs["connect_args"] = {"check_same_thread": False}
    else:
        # PostgreSQL pool configuration
        engine_kwargs["pool_size"] = kwargs.get("pool_size", settings.database_pool_size)
        engine_kwargs["max_overflow"] = kwargs.get(
            "max_overflow", settings.database_max_overflow
        )
        engine_kwargs["pool_pre_ping"] = True

    engine_kwargs.update(kwargs)
    return create_async_engine(db_url, **engine_kwargs)


# Global engine and sessionmaker
engine: AsyncEngine = create_engine()
async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency yielding an isolated database session."""
    async with async_session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db(engine_instance: AsyncEngine | None = None) -> None:
    """Create all schema tables. Used for testing or initial bootstrap."""
    eng = engine_instance or engine
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database schema initialized successfully.")


async def close_db(engine_instance: AsyncEngine | None = None) -> None:
    """Dispose of the database connection pool on shutdown."""
    eng = engine_instance or engine
    await eng.dispose()
    logger.info("Database connection pool closed.")
