"""Integration tests for Redis cache behavior, invalidation, and fallback."""

import pytest

from taskqueue.cache.redis import TaskCache


class MockRedisClient:
    """In-memory mock mimicking redis.asyncio.Redis."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.is_connected = True

    async def ping(self) -> bool:
        if not self.is_connected:
            raise ConnectionError("Redis is down")
        return True

    async def get(self, key: str) -> str | None:
        if not self.is_connected:
            raise ConnectionError("Redis is down")
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        if not self.is_connected:
            raise ConnectionError("Redis is down")
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        if not self.is_connected:
            raise ConnectionError("Redis is down")
        return 1 if self.store.pop(key, None) else 0

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_cache_hit_miss_and_invalidation() -> None:
    cache = TaskCache(enabled=True)
    mock_redis = MockRedisClient()
    cache._client = mock_redis  # inject mock client

    task_id = "task-cache-123"

    # 1. Cache Miss
    data = await cache.get_task_status(task_id)
    assert data is None

    # 2. Cache Set
    test_status = {"id": task_id, "status": "RUNNING", "attempt_count": 1}
    await cache.set_task_status(task_id, test_status, ttl_seconds=60)

    # 3. Cache Hit
    cached = await cache.get_task_status(task_id)
    assert cached is not None
    assert cached["status"] == "RUNNING"

    # 4. Invalidation
    await cache.invalidate(task_id)
    assert await cache.get_task_status(task_id) is None


@pytest.mark.asyncio
async def test_cache_graceful_degradation_when_offline() -> None:
    cache = TaskCache(enabled=True)
    mock_redis = MockRedisClient()
    cache._client = mock_redis

    # Simulate Redis connection failure
    mock_redis.is_connected = False

    # Should not raise exception, returns None gracefully
    data = await cache.get_task_status("task-123")
    assert data is None

    # Set status should not raise exception
    await cache.set_task_status("task-123", {"status": "SUCCEEDED"})

    # Health check reflects offline status
    assert await cache.check_health() is False

