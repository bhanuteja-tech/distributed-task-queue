# Glossary of Distributed Systems & Task Queue Terms

### At-Least-Once Delivery
A messaging guarantee where every message is delivered to a consumer at least once, but under network partitions, timeouts, or worker crashes, duplicate deliveries may occur. Requires consumers to be idempotent.

### At-Most-Once Delivery
A messaging guarantee where messages are never delivered more than once, but messages may be lost if a process crashes before handling them. (e.g., auto-acknowledging messages upon receipt).

### Backpressure
A mechanism that limits the rate at which a system accepts or buffers work when downstream consumers cannot keep up, preventing memory exhaustion and cascading failure (e.g., RabbitMQ consumer prefetch QoS).

### Consumer Prefetch (QoS)
A setting in AMQP that tells the message broker how many unacknowledged messages a worker process may hold at any given time. Prevents one worker from monopolizing the queue.

### Dead-Letter Queue (DLQ)
A specialized queue where unprocessable, malformed, or repeatedly failing messages are routed after exhausting retry policies, allowing operators to inspect and debug failures without blocking the main pipeline.

### Dual-Write Problem
An architectural anti-pattern where an application writes to two independent storage systems (e.g., a relational database and a message broker) without distributed transactions, leading to inconsistency when one of the writes fails.

### Exponential Backoff
An algorithm that increases the delay between consecutive retry attempts exponentially (e.g., 2s, 4s, 8s, 16s), allowing overloaded downstream services time to recover.

### Heartbeat
A recurring ping sent by a background process to signal that it is alive. Used by workers to extend task leases and keep them from being reclaimed by the reaper.

### Idempotency
A mathematical property where performing an operation multiple times produces the exact same result and side-effects as performing it once ($f(f(x)) = f(x)$).

### Idempotency Key
A unique client-generated token (often a UUID) sent with an API request to ensure that duplicate requests (e.g., from network retries) are recognized and processed only once.

### Jitter
Random variance introduced into retry backoff calculations to avoid the "thundering herd" problem, where multiple failing tasks retry at the exact same instant.

### Optimistic Concurrency Control (OCC)
A concurrency control technique that assumes multiple transactions can complete without interfering. Before committing, it verifies that no other transaction has modified the record (typically via a `version` number column).

### Publisher Confirms
An AMQP protocol extension where the message broker responds with an asynchronous confirmation (ACK) only after the published message has been safely persisted to durable storage or mirrored across a cluster.

### Task Lease
A time-bound claim of ownership over a task by a worker. If the worker crashes and fails to renew the lease before expiration, the task is considered abandoned and eligible for reclamation.

### Thundering Herd Problem
A failure scenario where a large number of waiting processes or clients wake up simultaneously to retry or query a resource, overwhelming the resource and causing repeated cascading failure.

### Transactional Outbox Pattern
A distributed systems design pattern where events destined for a message broker are persisted in a database table within the same transaction as the business entity, and subsequently published by a separate background process.

