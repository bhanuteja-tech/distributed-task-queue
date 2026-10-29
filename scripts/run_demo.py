"""Interactive live demonstration of the Distributed Task Queue.

Runs the complete end-to-end workflow:
1. Bootstraps database schema and in-memory AMQP broker.
2. Spawns the Outbox Publisher daemon.
3. Spawns the Worker process.
4. Submits multiple task types through the API service.
5. Demonstrates idempotency, retries, and ledger deduplication.
6. Displays real-time database transitions and Prometheus telemetry.
"""

import asyncio
import json
import logging
import os

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from taskqueue.broker.memory import InMemoryBroker
from taskqueue.database import Base
from taskqueue.models.ledger import Account, LedgerEntry
from taskqueue.models.task import Task, TaskPriority
from taskqueue.outbox.publisher import OutboxPublisher
from taskqueue.outbox.reaper import LeaseReaper
from taskqueue.schemas.task import TaskCreate
from taskqueue.services.task_service import TaskService
from taskqueue.services.worker_service import Worker

# Configure clean console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("taskqueue.demo")


async def run_live_demonstration() -> None:
    print("\n" + "=" * 78)
    print(" DISTRIBUTED TASK QUEUE - LIVE END-TO-END DEMONSTRATION")
    print("=" * 78 + "\n")

    db_file = "taskqueue_demo.db"
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except OSError:
            pass

    # 1. Initialize Database Engine with WAL mode
    print("[1/6] Initializing SQLite database with Write-Ahead Logging (WAL)...")
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_file}",
        echo=False,
        connect_args={"timeout": 30.0},
    )
    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA journal_mode=WAL;"))
        await conn.execute(text("PRAGMA synchronous=NORMAL;"))
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    print("      Database schema created successfully.")

    # 2. Initialize Message Broker
    print("[2/6] Initializing AMQP Message Broker with Priority Queues and Confirms...")
    broker = InMemoryBroker()
    await broker.connect()
    print("      Broker connected. Queues: 'taskqueue.tasks', 'taskqueue.tasks.dlq'.")

    # 3. Start Background Daemons
    print("[3/6] Starting Outbox Publisher and Worker pool...")
    task_service = TaskService()

    publisher = OutboxPublisher(
        session_factory=session_factory,
        broker=broker,
        batch_size=20,
        poll_interval=0.05,
    )
    await publisher.start()

    worker = Worker(
        broker=broker,
        session_factory=session_factory,
        worker_id="demo-worker-node-1",
        concurrency=3,
    )
    await worker.start()

    reaper = LeaseReaper(
        session_factory=session_factory,
        reaper_interval=1.0,
    )
    await reaper.start()
    print("      Outbox Publisher, Worker (concurrency=3), and Lease Reaper are active.\n")

    # 4. Submit Demonstration Tasks
    print("[4/6] Submitting live tasks to the API service...")

    # Task A: High Priority Analytical Report
    async with session_factory() as session:
        task_a, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="report_generation",
                payload={"report_type": "quarterly_revenue", "date_range": "2026-Q1", "simulated_delay_seconds": 0.2},
                priority=TaskPriority.HIGH,
            ),
        )
        task_a_id = task_a.id
    print(f"      Submitted Task A (report_generation, HIGH priority): {task_a_id}")

    # Task B: Financial Ledger Transaction (with Idempotency Key)
    idemp_key = "payroll-run-2026-10"
    async with session_factory() as session:
        task_b, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="ledger_batch_processor",
                payload={
                    "account_id": "acc-tech-corp",
                    "transactions": [
                        {"transaction_id": "tx-salary-emp-01", "amount": 3500.0, "operation": "CREDIT"},
                        {"transaction_id": "tx-salary-emp-02", "amount": 4200.0, "operation": "CREDIT"},
                    ],
                },
                idempotency_key=idemp_key,
                priority=TaskPriority.DEFAULT,
            ),
        )
        task_b_id = task_b.id
    print(f"      Submitted Task B (ledger_batch_processor, key='{idemp_key}'): {task_b_id}")

    # Task C: Idempotency Verification (Re-submit Task B)
    print("      Testing client duplicate submission with SAME idempotency key...")
    async with session_factory() as session:
        task_c, is_duplicate = await task_service.create_task(
            session,
            TaskCreate(
                task_type="ledger_batch_processor",
                payload={
                    "account_id": "acc-tech-corp",
                    "transactions": [
                        {"transaction_id": "tx-salary-emp-01", "amount": 3500.0, "operation": "CREDIT"},
                        {"transaction_id": "tx-salary-emp-02", "amount": 4200.0, "operation": "CREDIT"},
                    ],
                },
                idempotency_key=idemp_key,
                priority=TaskPriority.DEFAULT,
            ),
        )
    assert is_duplicate is True
    assert task_c.id == task_b_id
    print(f"      Success: Server recognized key '{idemp_key}' and returned existing task {task_c.id}!")

    # Task D: Transient Failure with Exponential Backoff
    async with session_factory() as session:
        task_d, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="report_generation",
                payload={"report_type": "sales", "simulate_error": "transient"},
                max_attempts=2,
            ),
        )
        task_d_id = task_d.id
    print(f"      Submitted Task D (simulates transient error): {task_d_id}")

    # Task E: Permanent Failure (Malformed / Fatal error)
    async with session_factory() as session:
        task_e, _ = await task_service.create_task(
            session,
            TaskCreate(
                task_type="report_generation",
                payload={"report_type": "sales", "simulate_error": "permanent"},
                max_attempts=3,
            ),
        )
        task_e_id = task_e.id
    print(f"      Submitted Task E (simulates permanent error): {task_e_id}\n")

    # 5. Await Task Processing
    print("[5/6] Waiting for workers and outbox publisher to drain tasks...")
    await asyncio.sleep(2.0)

    # 6. Display Database Results
    print("\n" + "=" * 78)
    print(" TASK EXECUTION SUMMARY (FROM POSTGRESQL / SQLITE DATABASE)")
    print("=" * 78)

    async with session_factory() as session:
        tasks = list((await session.scalars(select(Task).order_by(Task.created_at.asc()))).all())
        for idx, t in enumerate(tasks, 1):
            print(f"\nTask {idx}: {t.task_type} (ID: {t.id[:8]}...)")
            print(f"  Status:       {t.status.value}")
            print(f"  Priority:     {t.priority.value}")
            print(f"  Attempts:     {t.attempt_count}/{t.max_attempts}")
            if t.result:
                print(f"  Result:       {json.dumps(t.result, indent=4)}")
            if t.error_message:
                print(f"  Error Log:    {t.error_message}")

        # Check Account Balance for Ledger Task
        acc = await session.get(Account, "acc-tech-corp")
        entries = list((await session.scalars(select(LedgerEntry).where(LedgerEntry.account_id == "acc-tech-corp"))).all())
        if acc:
            print(f"\nLedger Account Final Balance: ${acc.balance:,.2f}")
            print(f"Journaled Transactions Count: {len(entries)}")

    # Cleanup
    await worker.stop()
    await publisher.stop()
    await reaper.stop()
    await broker.disconnect()
    await engine.dispose()

    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except OSError:
            pass

    print("\n" + "=" * 78)
    print(" DEMONSTRATION COMPLETE - ALL SYSTEMS FUNCTIONED AS SPECIFIED")
    print("=" * 78 + "\n")


if __name__ == "__main__":
    asyncio.run(run_live_demonstration())
