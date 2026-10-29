# 06 - Worker Leases and Crash Recovery

## 1. What Problem This Module Solves: Abandoned In-Flight Tasks
Consider what happens when a worker container running a task is killed abruptly (e.g. `SIGKILL`, Linux OOM killer, node termination on spot instances, or power outage):
- The task was marked `RUNNING` in PostgreSQL.
- The worker died before finishing or updating the database.
- The task is now stranded in `RUNNING` forever. A naive queue would never know whether the worker is still working on it or died hours ago.

Resetting any task simply because it has been in `RUNNING` for a while is dangerous: a legitimate, heavy report might take 5 minutes to generate. If the queue blindly resets it after 2 minutes, two workers will run the task simultaneously!

## 2. The Task Lease and Heartbeat Architecture
Our architecture solves this through cooperative **Task Leases** combined with **Background Heartbeats**:

```mermaid
sequenceDiagram
    autonumber
    participant Worker as Worker Process
    participant DB as PostgreSQL
    participant Reaper as Lease Reaper Daemon
    
    Worker->>DB: Claim task: status=RUNNING, lease_expires_at = NOW() + 60s
    DB-->>Worker: Lease Granted
    
    par Handler Execution
        Worker->>Worker: Run long task handler (e.g. 90s)
    and Background Heartbeat Loop (every 15s)
        Worker->>DB: UPDATE tasks SET lease_expires_at = NOW() + 60s
        Note over Worker,DB: Keeps lease alive while worker makes progress
    end
    
    alt Worker Dies Suddenly (OOM / SIGKILL)
        Note over Worker: Worker crashes; heartbeats stop!
        Note over DB: Time passes... lease_expires_at is now in the past
        Reaper->>DB: SELECT * FROM tasks WHERE status='RUNNING' AND lease_expires_at < NOW()
        Reaper->>DB: Reclaim task: status=RETRYING, add outbox event to re-queue
        Reaper-->>DB: COMMIT
    end
```

## 3. How Leases and Heartbeats Work in Detail
1. **Task Claiming**: When a worker claims a task, it sets:
   - `lease_worker_id = worker_uuid`
   - `lease_expires_at = utc_now() + timedelta(seconds=60)`
   - `status = RUNNING`
2. **Heartbeat Loop**: In [src/taskqueue/services/worker_service.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/services/worker_service.py), a concurrent `asyncio.Task` fires every 15 seconds. It updates `lease_expires_at = utc_now() + 60s` as long as the worker remains healthy.
   - **Benefit**: Tasks that take 30 seconds or 10 minutes never expire prematurely as long as the worker is alive.
3. **Lease Reaper**: In [src/taskqueue/outbox/reaper.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/outbox/reaper.py), an independent daemon queries:
   ```sql
   SELECT * FROM tasks 
   WHERE status = 'RUNNING' 
     AND lease_expires_at < NOW()
   LIMIT 50;
   ```
   If a task is returned, it is guaranteed that the worker crashed (because its heartbeats ceased). The reaper safely transitions it to `RETRYING` (or `DEAD_LETTER` if attempts are exhausted) and triggers outbox re-publication.

## 4. Concurrency Guard Between Workers
If Worker Bravo receives an AMQP message for a task while Worker Alpha holds an unexpired lease:
- Worker Bravo checks `lease_expires_at > utc_now()`.
- Finding an active lease, Worker Bravo logs a warning and rejects the message with `requeue=True` (`await msg.nack(requeue=True)`).
- **Result**: No duplicate execution occurs.

## 5. Relevant Tests
- [tests/failure/test_lease_recovery.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/tests/failure/test_lease_recovery.py): Simulates a crashed worker with an expired lease, verifies the reaper reclaims it, and confirms a healthy worker picks it up and finishes it.
- [tests/failure/test_crash_recovery_scenarios.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/tests/failure/test_crash_recovery_scenarios.py): Tests concurrent worker lease detection.

