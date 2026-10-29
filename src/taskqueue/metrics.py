"""Prometheus metrics registry and telemetry collection for the distributed task queue.

Tracks task submission, processing duration, state transitions, retries,
and dead-letter counts.
"""

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# Custom registry to avoid default Python process bloat if desired
REGISTRY = CollectorRegistry()

# Counters
TASKS_SUBMITTED_TOTAL = Counter(
    "taskqueue_tasks_submitted_total",
    "Total number of tasks submitted to the API",
    ["task_type", "priority"],
    registry=REGISTRY,
)

TASKS_COMPLETED_TOTAL = Counter(
    "taskqueue_tasks_completed_total",
    "Total number of tasks executed to terminal or retry state",
    ["task_type", "status"],
    registry=REGISTRY,
)

TASKS_RETRIED_TOTAL = Counter(
    "taskqueue_tasks_retried_total",
    "Total number of task retry attempts scheduled",
    ["task_type"],
    registry=REGISTRY,
)

TASKS_DEAD_LETTERED_TOTAL = Counter(
    "taskqueue_tasks_dead_lettered_total",
    "Total number of tasks routed to dead-letter queue",
    ["task_type", "reason"],
    registry=REGISTRY,
)

OUTBOX_PUBLISHED_TOTAL = Counter(
    "taskqueue_outbox_published_total",
    "Total number of outbox events published to the message broker",
    ["task_type"],
    registry=REGISTRY,
)

OUTBOX_PUBLISH_FAILURES_TOTAL = Counter(
    "taskqueue_outbox_publish_failures_total",
    "Total number of outbox message publish failures",
    ["task_type"],
    registry=REGISTRY,
)

# Histograms
TASK_EXECUTION_DURATION_SECONDS = Histogram(
    "taskqueue_task_execution_duration_seconds",
    "Time spent running task handler in seconds",
    ["task_type"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
    registry=REGISTRY,
)

TASK_END_TO_END_DURATION_SECONDS = Histogram(
    "taskqueue_task_end_to_end_duration_seconds",
    "Time from task creation to completion in seconds",
    ["task_type"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
    registry=REGISTRY,
)

# Gauges
ACTIVE_WORKERS_GAUGE = Gauge(
    "taskqueue_active_workers",
    "Current number of registered active workers",
    registry=REGISTRY,
)

RUNNING_TASKS_GAUGE = Gauge(
    "taskqueue_running_tasks",
    "Current number of tasks currently executing in workers",
    registry=REGISTRY,
)

QUEUE_BACKLOG_GAUGE = Gauge(
    "taskqueue_queue_backlog",
    "Estimated pending queue depth",
    ["priority"],
    registry=REGISTRY,
)


def export_metrics() -> tuple[bytes, str]:
    """Generate Prometheus-formatted metrics bytes and content type."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST

