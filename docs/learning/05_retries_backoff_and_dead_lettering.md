# 05 - Retries, Backoff, and Dead-Lettering

## 1. What Problem This Module Solves
Not all failures are equal. Distributed systems face two distinct classes of errors:
1. **Transient Failures**: Network glitches, temporary database lock contention, rate limits, or microservice restarts. These usually resolve within seconds.
2. **Permanent Failures**: Malformed JSON payloads, missing parameters, invalid database IDs, or business constraint violations. Retrying these is futile and wastes CPU and broker bandwidth.

Naively retrying every failure in a tight loop exhausts worker concurrency and creates a **Thundering Herd** problem that can knock down recovering databases.

## 2. Retry Policy Design

### A. Retryable vs. Permanent Error Classification
The task queue defines an explicit exception hierarchy in [src/taskqueue/exceptions.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/exceptions.py):
- `RetryableTaskError`: Indicates a transient failure. The system transitions the task to `RETRYING` and schedules a delayed retry if `attempt_count < max_attempts`.
- `PermanentTaskError`: Indicates a fatal non-retryable error. The system bypasses all remaining retry attempts and transitions the task directly to `FAILED`.

### B. Exponential Backoff with Jitter
The delay before attempt $k$ is calculated in [src/taskqueue/services/task_service.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/services/task_service.py):

$$\text{delay} = \min\left(\text{max\_delay}, \text{initial\_delay} \times \text{base}^{k-1}\right) \times \text{uniform}(0.8, 1.2)$$

- **Exponential Expansion**: Gives downstream services breathing room to recover.
- **Jitter ($\pm 20\%$)**: Randomizes retry timing across concurrent failing tasks, preventing synchronized burst spikes (the thundering herd) from repeatedly slamming the recovering service.
- **Cap (`max_delay`)**: Prevents delays from growing into hours or days.

### C. Non-Blocking Delayed Retries
A common anti-pattern in inexperienced implementations is to do:
```python
# ANTI-PATTERN: DO NOT DO THIS
await asyncio.sleep(retry_delay)  # Locks a worker thread!
```
Doing `sleep()` inside a worker monopolizes a worker concurrency slot. If 5 workers encounter rate limits and sleep for 60 seconds, the entire worker pool is frozen and cannot process other healthy tasks.

**Our Implementation**:
1. Worker marks the task in PostgreSQL with `status = RETRYING` and `retry_at = now() + delay`.
2. Worker commits the transaction, acknowledges the original broker message (`await msg.ack()`), and frees up its concurrency slot immediately!
3. An `OutboxEvent` for the retry is scheduled to be published when `retry_at` arrives.

## 3. Dead-Letter Handling (DLQ)
When a task exhausts `max_attempts`:
1. PostgreSQL transitions the task to `status = DEAD_LETTER`.
2. The worker sends `await msg.nack(requeue=False)` to RabbitMQ.
3. RabbitMQ's Dead-Letter Exchange (`taskqueue.dlx`) catches the rejected message and routes it to `taskqueue.tasks.dlq`.
4. The task remains safely parked for operator inspection or manual retry via `POST /tasks/{id}/retry`.

## 4. Relevant Tests
- [tests/unit/test_retry_policy.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/tests/unit/test_retry_policy.py): Verifies exponential math, max delay bounds, and jitter dispersion.
- [tests/failure/test_retryable_vs_permanent.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/tests/failure/test_retryable_vs_permanent.py): Verifies that permanent errors fail immediately on attempt 1, while retryable errors back off and dead-letter only when max attempts are exhausted.

