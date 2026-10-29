"""Configuration management using Pydantic Settings.

Reads configuration from environment variables or a .env file.
Settings are strongly typed and validated upon instantiation.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with defaults suitable for local development."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # General
    environment: str = Field(default="development", description="Runtime environment")
    log_level: str = Field(default="INFO", description="Logging level")
    app_name: str = Field(default="distributed-task-queue", description="Application name")

    # API Server
    api_host: str = Field(default="0.0.0.0", description="API host address")
    api_port: int = Field(default=8000, description="API port")
    api_workers: int = Field(default=1, description="API server worker count")

    # Database (PostgreSQL default, overridable to sqlite+aiosqlite for testing)
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/taskqueue",
        description="SQLAlchemy async database connection URL",
    )
    database_pool_size: int = Field(default=10, description="Connection pool size")
    database_max_overflow: int = Field(default=20, description="Max overflow connections")
    database_timeout_seconds: float = Field(default=15.0, description="Query timeout")

    # RabbitMQ Broker
    rabbitmq_url: str = Field(
        default="amqp://guest:guest@localhost:5672/",
        description="AMQP connection string",
    )
    rabbitmq_exchange: str = Field(
        default="taskqueue.direct",
        description="Main direct exchange name",
    )
    rabbitmq_queue_prefix: str = Field(
        default="taskqueue.tasks",
        description="Prefix for task queues",
    )
    rabbitmq_dead_letter_exchange: str = Field(
        default="taskqueue.dlx",
        description="Dead-letter exchange name",
    )
    rabbitmq_dead_letter_queue: str = Field(
        default="taskqueue.tasks.dlq",
        description="Dead-letter queue name",
    )
    rabbitmq_publisher_confirms: bool = Field(
        default=True,
        description="Enable broker publisher confirms for outbox reliability",
    )
    rabbitmq_connection_retries: int = Field(
        default=5,
        description="Max connection retry attempts on startup",
    )
    rabbitmq_retry_interval_seconds: float = Field(
        default=2.0,
        description="Delay between broker connection retries",
    )

    # Redis (Optional read-through status cache)
    redis_enabled: bool = Field(
        default=False,
        description="Toggle Redis caching layer. Database remains authoritative.",
    )
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL",
    )
    redis_cache_ttl_seconds: int = Field(
        default=300,
        description="TTL for cached task records in seconds",
    )
    redis_socket_timeout_seconds: float = Field(
        default=2.0,
        description="Socket timeout for Redis operations",
    )

    # Worker Settings
    worker_concurrency: int = Field(
        default=5,
        description="Maximum concurrent asyncio tasks per worker process",
    )
    worker_prefetch_count: int = Field(
        default=10,
        description="RabbitMQ consumer prefetch limit (unacknowledged messages)",
    )
    worker_lease_duration_seconds: int = Field(
        default=60,
        description="Duration of task ownership lease before considering it abandoned",
    )
    worker_heartbeat_interval_seconds: int = Field(
        default=15,
        description="Interval at which worker heartbeats and extends its task lease",
    )
    worker_shutdown_timeout_seconds: float = Field(
        default=15.0,
        description="Grace period for active tasks to complete on SIGINT/SIGTERM",
    )

    # Outbox Publisher & Lease Reaper
    outbox_poll_interval_seconds: float = Field(
        default=1.0,
        description="Polling interval for unpublished outbox events",
    )
    outbox_batch_size: int = Field(
        default=50,
        description="Maximum outbox events to fetch per batch",
    )
    outbox_lock_timeout_seconds: int = Field(
        default=30,
        description="Lease duration on outbox event claiming",
    )
    reaper_interval_seconds: float = Field(
        default=10.0,
        description="Interval for checking and recovering expired task leases",
    )
    reaper_batch_size: int = Field(
        default=50,
        description="Batch size for reclaiming abandoned tasks",
    )

    # Retry Policy Defaults
    default_max_attempts: int = Field(
        default=3,
        description="Default max execution attempts before dead-lettering",
    )
    default_initial_retry_delay_seconds: float = Field(
        default=2.0,
        description="Initial delay before first retry",
    )
    default_max_retry_delay_seconds: float = Field(
        default=60.0,
        description="Cap on exponential backoff delay",
    )
    default_exponential_backoff_base: float = Field(
        default=2.0,
        description="Exponential backoff multiplier base",
    )
    default_jitter: bool = Field(
        default=True,
        description="Apply randomized jitter to avoid thundering herd on retries",
    )


# Global settings singleton
settings = Settings()

