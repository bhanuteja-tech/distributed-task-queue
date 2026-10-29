# The Comprehensive Guide to the Distributed Task Queue

Welcome! If you are learning distributed systems, preparing for backend engineering interviews, or trying to understand how production-grade asynchronous architectures work, this guide is written specifically for you.

---

## 1. The Core Concept: Why Does This Project Exist?

### The Real-World Problem: The Restaurant Analogy
Imagine walking into a restaurant. You sit down and tell the waiter: *"I'd like a slow-roasted gourmet pizza."*
- **Scenario A (Synchronous / Blocking API)**:
  The waiter stands at your table, freezes, walks into the kitchen, waits 45 minutes while the pizza bakes in the oven, and only then hands you the pizza and bill. While waiting at your table, the waiter cannot take orders from any other customers. If 5 customers arrive, the restaurant halts. If the oven loses power at minute 44, the waiter drops the pizza and you get an HTTP 504 Gateway Timeout.
- **Scenario B (Asynchronous Distributed Task Queue - This Project)**:
  The waiter takes your order, writes it on a numbered ticket, stamps it in the restaurant ledger, hands you ticket `#42`, and says: *"Your order has been accepted! You can check status anytime with ticket #42."* The waiter is immediately free to serve the next customer. Behind the kitchen doors, independent chefs (workers) pick up tickets from a ticket carousel (RabbitMQ), cook the dishes, and write the finished status in the database.

### When Do You Need a Task Queue in Real Life?
- **Sending emails or SMS** (e.g., password reset, order confirmation).
- **Generating PDF reports or Excel exports** (e.g., aggregating 100,000 database rows).
- **Payment processing & webhook delivery** (e.g., Stripe webhooks, banking ledgers).
- **Image & video processing** (e.g., resizing avatars, video transcoding).
- **Periodic batch data pipelines** (e.g., syncing inventory every hour).

---

## 2. "What Is What?" — Tour of Every Component & Why It Is Used

Here is a breakdown of every technology, folder, and file in this repository:

### The Technology Stack

| Technology | Role | Why It Was Chosen Over Alternatives |
|---|---|---|
| **Python 3.12** | Core Programming Language | High performance `asyncio` event loop, modern typing syntax (`X \| Y`), widely used across backend and data infrastructure. |
| **FastAPI** | REST API Framework | Native async support, high throughput, automatic OpenAPI documentation, and seamless integration with Pydantic v2. |
| **PostgreSQL** | Authoritative Primary Database | Relational integrity, ACID transactions (crucial for outbox events and ledger idempotency), row-level locking. |
| **RabbitMQ** | Message Broker | Robust AMQP protocol, explicit manual acks, consumer prefetch (QoS), native priority queues, and dead-letter exchanges (DLX). |
| **`aio-pika`** | RabbitMQ Async Client | Maintained Python library for asynchronous AMQP communication with publisher confirms. |
| **Redis** | Ephemeral Read Cache | In-memory key-value store used strictly to cache task status queries, reducing load on PostgreSQL without becoming a point of data loss. |
| **SQLAlchemy 2.0 (Async)** | Database ORM & Access | Industry-standard Python ORM with native `asyncpg` asynchronous drivers and clean repository abstractions. |
| **Alembic** | Database Schema Migrations | Safe, version-controlled database schema evolution that runs before services boot. |
| **Docker & Docker Compose** | Containerization | Complete reproducible environment that spins up the database, broker, cache, API, workers, and outbox daemons with one command. |
| **Pytest & HTTPX** | Automated Testing | Fast asynchronous test execution with high test isolation and deterministic failure injection. |

---

### The Codebase Tour (Folder by Folder)

```
Distributed_sysstem_project/
├── src/taskqueue/
│   ├── config.py              <-- Settings loaded from environment variables
│   ├── database.py            <-- Database engine & sessionmaker (PostgreSQL / SQLite)
│   ├── logging.py             <-- JSON structured logging with correlation IDs
│   ├── metrics.py             <-- Prometheus telemetry (latency histograms & counters)
│   ├── exceptions.py          <-- Retryable vs. Permanent error definitions
│   │
│   ├── models/                <-- Database tables (SQLAlchemy)
│   │   ├── task.py            <-- The Task table and State Machine rules
│   │   ├── outbox.py          <-- The OutboxEvent table for reliable delivery
│   │   └── ledger.py          <-- Account & LedgerEntry tables for idempotency demo
│   │
│   ├── broker/                <-- Message Queue communication
│   │   ├── interface.py       <-- Abstract Broker contract
│   │   ├── rabbitmq.py        <-- Real RabbitMQ client using aio-pika
│   │   └── memory.py          <-- In-memory AMQP simulator for fast tests
│   │
│   ├── handlers/              <-- What the worker actually does
│   │   ├── base.py            <-- Handler interface and registry
│   │   ├── report.py          <-- Simulated analytical report generator
│   │   ├── ledger.py          <-- Financial ledger batch processor
│   │   └── word_count.py      <-- Deterministic text frequency analyzer
│   │
│   ├── services/              <-- Business logic
│   │   ├── task_service.py    <-- Task creation, queries, idempotency, backoff math
│   │   └── worker_service.py  <-- Worker consumption loop, claiming, heartbeats, acks
│   │
│   ├── outbox/                <-- Distributed reliability daemons
│   │   ├── publisher.py       <-- Reads DB outbox and sends to RabbitMQ with confirms
│   │   └── reaper.py          <-- Finds tasks from crashed workers and reclaims them
│   │
│   ├── cache/
│   │   └── redis.py           <-- Fast read-through status cache with fallback
│   │
│   ├── api/                   <-- HTTP REST API
│   │   ├── app.py             <-- FastAPI application & Request-ID middleware
│   │   └── routes/            <-- /tasks, /health, /metrics endpoints
│   │
│   ├── worker/cli.py          <-- CLI command to run an independent worker
│   └── outbox_worker/cli.py   <-- CLI command to run publisher + reaper daemon
│
├── tests/                     <-- 40 automated tests covering all failure modes
├── benchmarks/                <-- Performance benchmark scripts measuring throughput
└── docs/learning/             <-- Deep-dive study guides for system design interviews
```

---

## 3. How to Run This Project

You can run this project in two ways:
- **Option 1: Complete Stack with Docker Compose (Recommended)**
- **Option 2: Standalone Local Virtual Environment**

---

### Option 1: Running with Docker Compose (Everything in 1 Command)

This is the fastest and most realistic way to run the full distributed system. It starts PostgreSQL, RabbitMQ (with management UI), Redis, migrations, API, Worker, and the Outbox Publisher daemon.

#### Step 1: Copy the Environment Template
Open your terminal in the project directory:
```bash
cp .env.example .env
```

#### Step 2: Build and Launch All Containers
```bash
docker compose up --build -d
```

#### Step 3: Verify Everything Is Running
```bash
docker compose ps
```
You should see all 6 services running healthy:
- `taskqueue_postgres` (Port 5432)
- `taskqueue_rabbitmq` (Port 5672, Management UI on 15672)
- `taskqueue_redis` (Port 6379)
- `taskqueue_api` (Port 8000)
- `taskqueue_worker` (Independent worker process)
- `taskqueue_outbox_publisher` (Outbox publisher + lease reaper)

#### Step 4: Open the Web Dashboards
- **FastAPI Interactive Docs**: Open your browser at [http://localhost:8000/docs](http://localhost:8000/docs)
- **RabbitMQ Management Dashboard**: Open [http://localhost:15672](http://localhost:15672) (Username: `guest`, Password: `guest`)
- **Prometheus Metrics**: Open [http://localhost:8000/metrics](http://localhost:8000/metrics)
- **Health Check**: Open [http://localhost:8000/health/ready](http://localhost:8000/health/ready)

#### Step 5: Shutting Down
```bash
docker compose down
```

---

### Option 2: Running Locally in Python (Without Docker)

If you want to run or debug Python code directly on your machine:

#### Step 1: Activate Virtual Environment
```powershell
# Windows PowerShell:
.venv\Scripts\activate

# macOS / Linux:
source .venv/bin/activate
```

#### Step 2: Run All 40 Automated Tests
```bash
pytest -v
```
All 40 unit, API, integration, and failure recovery tests run in ~1.2 seconds using an isolated in-memory engine and broker.

#### Step 3: Run the Performance Benchmark Suite
```bash
python benchmarks/run_benchmark.py
```
This measures real task submission latency, worker completion throughput, p50/p95 latency distributions, and controlled worker crash recovery.

#### Step 4: Run Code Quality & Lint Checks
```bash
ruff check src tests benchmarks
```

---

## 4. Hands-on Tutorial: Submit & Observe a Task

Let's submit tasks and see the distributed mechanics in action!

### Experiment 1: Submit a Standard Task

Open your terminal (or use the interactive Swagger UI at [http://localhost:8000/docs](http://localhost:8000/docs)):

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "report_generation",
    "payload": {
      "report_type": "monthly_revenue_summary",
      "date_range": "2026-Q1"
    },
    "priority": "HIGH"
  }'
```

**What Happens Behind the Scenes:**
1. FastAPI validates the payload against Pydantic models.
2. In **one atomic PostgreSQL transaction**, the API inserts a row in `tasks` with `status: PENDING` and an event in `outbox_events`.
3. The API immediately responds with `201 Created` and your `task_id` (e.g., `"77c89afb-8b83-4642-a40c-9df4bb292ac2"`).
4. Within 500ms, the `OutboxPublisher` background process picks up the outbox event and publishes it to RabbitMQ on routing key `tasks.high`.
5. RabbitMQ assigns priority `10` and delivers the message to the `worker`.
6. The worker claims the task, transitions it to `RUNNING`, sets a 60s lease, and begins generating the report.
7. Upon completion, the worker updates the database to `status: SUCCEEDED`, stores the analytical result, invalidates the Redis cache, and sends an `ACK` to RabbitMQ.

Now query the task status:
```bash
curl http://localhost:8000/tasks/<YOUR_TASK_ID>
```
You will see `status: "SUCCEEDED"` with the generated summary metrics!

---

### Experiment 2: Test Idempotency (Preventing Duplicate Work)

Submit a task with an `idempotency_key`:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "report_generation",
    "payload": {"report_type": "annual_summary"},
    "idempotency_key": "order-req-9999"
  }'
```
You get `201 Created`.

Now send the **exact same command a second time** (simulating a mobile app retrying after a poor network connection):
```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "report_generation",
    "payload": {"report_type": "annual_summary"},
    "idempotency_key": "order-req-9999"
  }'
```
**Result**: The server returns `200 OK` with the **existing task ID**. It does **not** create a new task or execute the report a second time!

---

### Experiment 3: See Business-Level Financial Idempotency

Submit a batch to the `ledger_batch_processor` handler:
```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "ledger_batch_processor",
    "payload": {
      "account_id": "acc-checking-01",
      "transactions": [
        {"transaction_id": "tx-salary-101", "amount": 1000.0, "operation": "CREDIT"}
      ]
    }
  }'
```
The account balance becomes `$1000.00`.
Even if RabbitMQ encounters a network partition and delivers this message **twice**, the handler checks the database constraint on `transaction_id = "tx-salary-101"`. The second execution detects that the transaction has already been journaled, skips it, and leaves the balance at exactly `$1000.00`, preventing duplicate credits!

---

### Experiment 4: Trigger an Automatic Retry (Transient Error)

Submit a task instructing the handler to simulate a temporary database timeout:
```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "report_generation",
    "payload": {
      "report_type": "sales",
      "simulate_error": "transient"
    },
    "max_attempts": 3
  }'
```
1. The worker catches `RetryableTaskError`.
2. The worker increments `attempt_count` to 1.
3. Because attempts ($1$) < max ($3$), the worker calculates exponential backoff:
   $$\text{delay} = 2.0 \times 2^0 \times \text{jitter} \approx 2.1\text{ seconds}$$
4. The worker transitions the task to `status: RETRYING` and schedules `retry_at`.
5. When the timer arrives, the task is re-delivered. If it fails 3 times, it transitions to `DEAD_LETTER` and moves to the Dead-Letter Queue.

---

### Experiment 5: Trigger a Permanent Failure (Immediate Rejection)

Submit a task with an unrecoverable bug:
```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "report_generation",
    "payload": {
      "report_type": "sales",
      "simulate_error": "permanent"
    },
    "max_attempts": 5
  }'
```
**Result**: The system recognizes `PermanentTaskError`. Retrying would be futile, so it **immediately transitions to `FAILED` on attempt 1**, bypassing remaining attempts and saving worker capacity.

---

## 5. System Design Interview Cheat Sheet

When discussing this project in software engineering interviews, here are the key architectural topics and where they are implemented:

| Interview Question | How This System Solves It | Key Source File |
|---|---|---|
| *"How do you prevent data loss if the message broker crashes after an API write?"* | **Transactional Outbox Pattern**: The API commits the task and an outbox event in the same ACID database transaction. An outbox daemon publishes to RabbitMQ using **Publisher Confirms**. | [`src/taskqueue/outbox/publisher.py`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/outbox/publisher.py) |
| *"What happens if a worker dies mid-task (`SIGKILL` / OOM)?"* | **Task Leases & Heartbeats**: Workers claim a 60-second lease and refresh it every 15s via heartbeat. If a worker dies, heartbeats stop. The `LeaseReaper` daemon detects the expired lease and safely re-queues the task. | [`src/taskqueue/outbox/reaper.py`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/outbox/reaper.py) |
| *"Can you guarantee exactly-once execution?"* | **No universal exactly-once exists across distributed boundaries**. We provide **at-least-once delivery** from RabbitMQ, paired with **three-tier idempotency** (submission keys, terminal state guards, and database transaction unique constraints) to ensure *effectively exactly-once business results*. | [`src/taskqueue/handlers/ledger.py`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/handlers/ledger.py) |
| *"How do you prevent a failing service from being overwhelmed by retries?"* | **Exponential Backoff with Jitter**: Delay increases exponentially ($2\text{s}, 4\text{s}, 8\text{s}$) with $\pm 20\%$ randomized variance to avoid the "thundering herd" problem. Delayed retries are non-blocking. | [`src/taskqueue/services/task_service.py`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/services/task_service.py) |
| *"Why not just put all task state in Redis?"* | **PostgreSQL is the durable source of truth**. Redis is an in-memory ephemeral cache. If Redis restarts or is disabled, the system continues running with zero data loss by reading directly from PostgreSQL. | [`src/taskqueue/cache/redis.py`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/cache/redis.py) |
| *"How do urgent tasks jump ahead of batch jobs?"* | **RabbitMQ Priority Queues (`x-max-priority=10`)**: High-priority tasks (priority 10) are dequeued before default (5) or low (1) priority tasks. | [`src/taskqueue/broker/rabbitmq.py`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/broker/rabbitmq.py) |

---

## 6. Educational Reading Order

If you want to read through the companion study guides line by line, follow this order:
1. [`docs/learning/01_architecture_overview.md`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/docs/learning/01_architecture_overview.md) — Big-picture system diagram and component boundaries.
2. [`docs/learning/02_task_lifecycle_and_state_machine.md`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/docs/learning/02_task_lifecycle_and_state_machine.md) — States, transitions, and ownership rules.
3. [`docs/learning/03_transactional_outbox_pattern.md`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/docs/learning/03_transactional_outbox_pattern.md) — Solving the dual-write problem.
4. [`docs/learning/04_idempotency_and_duplicate_delivery.md`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/docs/learning/04_idempotency_and_duplicate_delivery.md) — Why at-least-once requires idempotency.
5. [`docs/learning/05_retries_backoff_and_dead_lettering.md`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/docs/learning/05_retries_backoff_and_dead_lettering.md) — Handling transient vs permanent errors.
6. [`docs/learning/06_worker_leases_and_crash_recovery.md`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/docs/learning/06_worker_leases_and_crash_recovery.md) — Recovering abandoned tasks.
7. [`docs/learning/07_rabbitmq_topology_and_priority_routing.md`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/docs/learning/07_rabbitmq_topology_and_priority_routing.md) — AMQP exchanges, prefetch QoS, and DLX.
8. [`docs/learning/08_observability_and_metrics.md`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/docs/learning/08_observability_and_metrics.md) — Prometheus metrics, health checks, and JSON logging.
9. [`docs/learning/glossary.md`](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/docs/learning/glossary.md) — Distributed systems vocabulary definitions.

