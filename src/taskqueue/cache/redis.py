"""Redis Cache client providing read-through status caching with graceful fallback.

PostgreSQL remains authoritative. If Redis is disabled, offline, or times out,
all operations fall back to the primary database transparently.
"""

import json
import logging
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from taskqueue.config import settings

logger = logging.getLogger(__name__)


class TaskCache:
    """Read-through status cache backed by Redis with graceful degradation."""

    def __init__(self, url: str | None = None, enabled: bool | None = None) -> None:
        self.enabled = settings.redis_enabled if enabled is None else enabled
        self.url = url or settings.redis_url
        self.ttl = settings.redis_cache_ttl_seconds
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        """Initialize Redis connection if caching is enabled."""
        if not self.enabled:
            return

        try:
            self._client = aioredis.from_url(
                self.url,
                socket_timeout=settings.redis_socket_timeout_seconds,
                decode_responses=True,
            )
            await self._client.ping()
            logger.info("Connected to Redis cache at %s", self.url)
        except Exception as e:
            logger.warning("Failed to connect to Redis cache (%s). Operating in degraded mode.", e)
            self._client = None

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def _cache_key(self, task_id: str) -> str:
        return f"task:{task_id}:status"

    async def get_task_status(self, task_id: str) -> dict[str, Any] | None:
        """Retrieve cached task state if available. Returns None on cache miss or error."""
        if not self.enabled or not self._client:
            return None

        try:
            val = await self._client.get(self._cache_key(task_id))
            if val:
                return json.loads(val)
        except RedisError as e:
            logger.warning("Redis read error for task %s: %s", task_id, e)
        except Exception as e:
            logger.warning("Unexpected error reading from Redis: %s", e)

        return None

    async def set_task_status(
        self,
        task_id: str,
        data: dict[str, Any],
        ttl_seconds: int | None = None,
    ) -> None:
        """Cache task status with expiration TTL."""
        if not self.enabled or not self._client:
            return

        try:
            ttl = ttl_seconds or self.ttl
            payload = json.dumps(data, default=str)
            await self._client.set(self._cache_key(task_id), payload, ex=ttl)
        except RedisError as e:
            logger.warning("Redis write error for task %s: %s", task_id, e)
        except Exception as e:
            logger.warning("Unexpected error writing to Redis: %s", e)

    async def invalidate(self, task_id: str) -> None:
        """Invalidate cached status when task transitions to a new state."""
        if not self.enabled or not self._client:
            return

        try:
            await self._client.delete(self._cache_key(task_id))
        except RedisError as e:
            logger.warning("Redis delete error for task %s: %s", task_id, e)
        except Exception as e:
            logger.warning("Unexpected error deleting from Redis: %s", e)

    async def check_health(self) -> bool:
        """Check Redis connectivity."""
        if not self.enabled:
            return True  # Considered healthy if deliberately disabled

        if not self._client:
            return False

        try:
            return bool(await self._client.ping())
        except Exception:
            return False


# Global default cache instance
cache = TaskCache()

