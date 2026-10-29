# 07 - RabbitMQ Topology and Priority Routing

## 1. What Problem This Module Solves
In production systems, some tasks are time-critical (e.g. sending password reset emails or processing user-facing checkout transactions) while others can tolerate delay (e.g. weekly backup analytics or monthly batch reports). 

Without priority routing and proper broker topology, low-priority batch workloads create queue backlogs that delay urgent tasks for hours.

## 2. RabbitMQ Topology Design

```mermaid
flowchart LR
    Outbox[Outbox Publisher] -->|Direct Routing Key: tasks.high / tasks.default / tasks.low| DirectExchange["Direct Exchange: taskqueue.direct"]
    
    subgraph Queues ["RabbitMQ Broker Topology"]
        DirectExchange -->|Priority 10| TaskQueue["Durable Queue: taskqueue.tasks (x-max-priority=10)"]
        DirectExchange -->|Priority 5| TaskQueue
        DirectExchange -->|Priority 1| TaskQueue
        
        TaskQueue -.->|On Rejected / NACK(requeue=false)| DLX["Fanout DLX: taskqueue.dlx"]
        DLX --> DLQ["Durable Queue: taskqueue.tasks.dlq"]
    end
    
    TaskQueue -->|Consumer Prefetch = 10| Worker1[Worker Process 1]
    TaskQueue -->|Consumer Prefetch = 10| Worker2[Worker Process 2]
```

## 3. Core AMQP Configuration Details

### A. Durable Queues and Persistent Messages
- **Durable Exchanges & Queues**: `durable=True` ensures queue definitions survive broker restarts.
- **Persistent Messages**: `delivery_mode = DeliveryMode.PERSISTENT` writes messages to the Mnesia disk store on the broker.

### B. Priority Queues (`x-max-priority`)
- When creating `taskqueue.tasks`, we configure `arguments={"x-max-priority": 10}`.
- Messages carry integer priorities:
  - `HIGH`: Priority 10
  - `DEFAULT`: Priority 5
  - `LOW`: Priority 1
- RabbitMQ prioritizes higher-priority messages inside the internal queue data structure. If a burst of 1,000 low-priority tasks is queued, incoming high-priority tasks jump ahead of them.

### C. Starvation Considerations (Interview Trade-Off)
Strict priority queues introduce the risk of **low-priority starvation**: if high-priority tasks arrive continuously at or above the worker pool's consumption capacity, low-priority tasks will never be processed.
- **Mitigation strategies**:
  1. Dedicated worker pools with separate queues (`high_priority_queue` vs `low_priority_queue`) with dedicated worker allocations.
  2. Dynamic priority aging (promoting older low-priority tasks over time).

### D. Consumer Prefetch (QoS)
- Configured via `await channel.set_qos(prefetch_count=10)`.
- Without prefetch limits, RabbitMQ will push *all* available messages to the first connected worker, starving other workers and exhausting worker memory.
- A bounded prefetch ensures work is distributed evenly across all active worker containers.

### E. Manual Explicit Acknowledgements
- `no_ack=False`: We strictly disallow auto-acking.
- Messages are only acknowledged (`await msg.ack()`) **after** the PostgreSQL transaction recording the task's terminal state or retry has successfully committed.

## 4. Relevant Tests
- [tests/unit/test_priority.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/tests/unit/test_priority.py): Verifies priority numeric mapping and that higher-priority messages are consumed first.
- [tests/integration/test_worker_lifecycle.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/tests/integration/test_worker_lifecycle.py): Tests manual acknowledgements and lifecycle handling.

