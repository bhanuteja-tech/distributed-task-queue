# 08 - Observability, Logging, and Metrics

## 1. What Problem This Module Solves
In a distributed architecture with multiple asynchronous processes (API, outbox publishers, lease reapers, workers, message brokers), finding bugs without unified correlation IDs and telemetry is nearly impossible.

## 2. Logs vs. Metrics vs. Health Checks

| Signal | Purpose | Typical Tool | In This Architecture |
|---|---|---|---|
| **Logs** | High-cardinality discrete event details (what happened on this specific task?). | Structured JSON, ELK, Datadog | `src/taskqueue/logging.py`: Single-line JSON logs containing `task_id`, `request_id`, `worker_id`, timestamp, and stack traces. |
| **Metrics** | Aggregated statistical numerical trends over time (what is the p95 latency across all tasks?). | Prometheus, Grafana | `src/taskqueue/metrics.py`: Exposes standard `/metrics` endpoint with Histograms, Counters, and Gauges. |
| **Health Checks** | Binary liveness and readiness state of the application and external dependencies. | Kubernetes, Consul, AWS ALB | `src/taskqueue/api/routes/health.py`: `/health/live` (process alive) and `/health/ready` (DB, RabbitMQ, and Redis connectivity). |

## 3. Distributed Tracing via Correlation Context
Python's `contextvars` module provides coroutine-safe contextual tracking. When an HTTP request enters the system or a worker starts processing a task:
1. `CorrelationIdMiddleware` extracts or generates `X-Request-ID` and sets `correlation_request_id.set(req_id)`.
2. Worker sets `correlation_task_id.set(msg.task_id)`.
3. Every subsequent log record emitted anywhere in that async call stack automatically formats into:
   ```json
   {
     "timestamp": "2026-10-29T18:50:00.123456Z",
     "level": "INFO",
     "logger": "taskqueue.services.worker_service",
     "message": "Task completed successfully",
     "task_id": "77c89afb-8b83-4642-a40c-9df4bb292ac2",
     "worker_id": "worker-node-1"
   }
   ```
4. Engineers can filter entire distributed workflows in Datadog or CloudWatch with `task_id: "77c89afb-8b83-4642-a40c-9df4bb292ac2"`.

## 4. Key Metrics Exposed on `/metrics`
- `taskqueue_tasks_submitted_total`: Counter by `task_type` and `priority`.
- `taskqueue_tasks_completed_total`: Counter by `task_type` and `status` (`SUCCEEDED`, `FAILED`).
- `taskqueue_tasks_retried_total`: Counter tracking retry events by `task_type`.
- `taskqueue_tasks_dead_lettered_total`: Counter tracking exhausted tasks moved to DLQ.
- `taskqueue_outbox_published_total` / `taskqueue_outbox_publish_failures_total`: Monitors transactional outbox throughput and broker publish errors.
- `taskqueue_task_execution_duration_seconds`: Histogram measuring handler run duration in seconds.
- `taskqueue_task_end_to_end_duration_seconds`: Histogram measuring duration from initial API submission to final completion.
- `taskqueue_active_workers`: Gauge tracking active worker processes.
- `taskqueue_running_tasks`: Gauge tracking tasks currently executing in the worker pool.

## 5. Relevant Tests
- [tests/api/test_health_metrics.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/tests/api/test_health_metrics.py): Verifies `/health/live`, `/health/ready`, and Prometheus `/metrics` exposition.

