# AltQueue Architecture

**Status:** original first-release architecture target, with a runnable durable MVP now implemented. The sections below describe the target design, not a claim that every optimization or release criterion has shipped.

**Related documents:** [README](../README.md) · [Project plan](PLAN.md) · [Implementation status](IMPLEMENTATION.md) · [API](API.md)

### Current implementation differences

The current engine uses one mutex to serialize state transitions and synchronized Pebble batches per request. Job metadata records include state, sequence, retry policy and lease fields; immutable payloads are stored separately. Runtime indexes and deadline heaps are reconstructed from those records. Payloads remain resident in bounded memory. Separate durable ready/lease index keyspaces, disk-only backlogs and cross-request group commit described as targets below are not implemented. The [implementation status](IMPLEMENTATION.md) document is authoritative about shipped behavior and remaining gaps.

## 1. Scope and assumptions

AltQueue is a durable, single-node task queue written in Go. Producers submit opaque job payloads to named queues. Workers competing on a queue lease jobs, execute application code, and acknowledge completion.

The initial system assumes:

- One active broker process owns one local data directory.
- Storage is persistent local disk supported by Pebble.
- The filesystem and hardware honor synchronization requests.
- Workers and network connections may fail at any time.
- Application handlers can tolerate or prevent duplicate side effects.
- Broker wall-clock behavior is sufficiently stable for configured delays and leases.

It does not initially provide replication, automatic failover, replayable completed-job history, broadcast subscriptions, or exactly-once execution. Network filesystems and shared-disk multi-writer operation are not part of the initial deployment model.

## 2. Delivery and durability contract

### At-least-once processing

A job remains owned by the broker until a valid acknowledgement commits. If a worker disappears, fails, or loses its lease, the job can be retried according to its policy. Retry limits may ultimately move it to dead-letter state rather than retry forever.

Duplicate execution is possible even when only one current lease exists. A worker may continue executing after its lease expires, or may perform an external side effect and fail before acknowledging. Lease tokens protect broker state; they cannot fence writes to an unrelated external system.

### Durability boundary

A mutating request succeeds only after the relevant Pebble write batch has committed with synchronization enabled:

- **Publish:** payload, job record, and initial state index are durable.
- **Fetch:** the job's new lease and attempt count are durable before delivery.
- **Ack:** the removal of the active job is durable before success.
- **Nack or expiry:** the next ready, delayed, or dead-letter state is durable.
- **Extend lease:** the new deadline and deadline index are durable.

This is local durability, not replication. Loss of the node's persistent disk can lose acknowledged jobs.

### Unknown outcomes

A connection loss, timeout, or canceled request does not prove that a mutation failed. The mutation may have committed before the response was lost. In particular, repeating a publish can create a second job because publish deduplication is outside the initial scope.

The client must not assume that a request identifier provides deduplication. SDK retry defaults must preserve this distinction.

### Ordering

Ready jobs are selected in broker-assigned ready-sequence order within a queue, subject to bounded scheduling. This is best-effort FIFO dispatch, not a guarantee of start, finish, or side-effect order. Concurrent consumers, retries, delayed jobs, and lease expiry can reorder processing. There is no global ordering across queues.

## 3. Components

```text
Producers                                      Workers
    |                                              |
    +------------------- gRPC ---------------------+
                             |
                             v
               Transport and validation
               authentication, request limits
                             |
                             v
                    Admission control
                             |
                             v
              Bounded mutation coordinator <----- Scheduler
              ordered transitions and batches      deadlines
                             |
                             v
                     Pebble storage
              job records + state indexes
                             |
                             v
                        Local disk

HTTP operational endpoints: health, readiness, metrics
CLI and Go SDK: clients of the public API
```

### Transport

The public job API uses gRPC and Protobuf. The initial worker model is pull-based fetch with bounded long polling; server-pushed delivery is not required.

Transport code handles request decoding, authentication, deadlines, and response encoding. It must not independently mutate queue state.

### Queue engine

The engine owns legal state transitions, queue policies, lease validation, and admission decisions. Its core rules should be testable without gRPC or real time.

### Mutation coordinator

Initially, one coordinator serializes state-changing commands and combines compatible commands into bounded durable batches. This favors simple correctness and group commit over premature sharding.

Network handling, payload validation, metrics, and suitable read operations may run concurrently. The coordinator must not block on worker execution, long-poll waits, or network response writes.

### Scheduler

The scheduler finds due delayed jobs and expired leases through ordered indexes, then submits transitions through the same coordinator. It processes bounded batches and yields between queues so a retry storm does not monopolize the engine.

The scheduler does not bypass lease checks or atomic storage transitions.

## 4. Domain model

### Queue

A queue has:

- An immutable internal ID and a unique user-facing name.
- A default retry policy and lease duration.
- Limits for stored job count, logical payload bytes, and in-flight jobs.
- Creation metadata and a configuration version.

First-release queue deletion is allowed only when ready, delayed, leased, and dead-letter job counts are all zero. Forced deletion and cancellation of running work are deferred.

Retry policy is captured on each job when it is published. Future queue configuration changes should not silently rewrite the policy of existing jobs.

### Job

A logical job record includes:

| Field | Purpose |
| --- | --- |
| `id` | Opaque, broker-generated job identity |
| `queue_id` | Owning queue |
| `payload` | Opaque bytes; never interpreted by the broker |
| `metadata` | Bounded application metadata |
| `created_at` | Broker acceptance timestamp |
| `state` | Ready, delayed, leased, or dead |
| `available_at` | Earliest eligible time for delayed work |
| `attempts` | Number of committed lease acquisitions |
| `max_attempts` | Total allowed attempts, including the first |
| `retry_policy` | Backoff parameters captured at publish |
| `lease_token` | Unique token for the current delivery, if leased |
| `lease_deadline` | Persisted expiry time, if leased |
| `last_error` | Bounded diagnostic information, not unlimited logs |

Completed is a logical terminal outcome, not a retained job state in the initial storage model. Acknowledgement removes the payload and live indexes. Long-term completion history is out of scope.

### Lease

Each successful fetch creates a new unpredictable, collision-resistant lease token and increments `attempts`. A lease is identified by queue, job ID, and token.

Acknowledge, negative acknowledge, and extension require the current token and an unexpired deadline. The coordinator checks the deadline even if the scheduler has not yet processed expiry. A missing job or old token must not mutate another delivery.

Repeating an acknowledgement after completion returns a missing/stale delivery result; retained acknowledgement receipts and exact replay of prior responses are not initially provided. A lost acknowledgement response can therefore leave the client uncertain, even though the completed job stays removed.

## 5. Job state machine

```text
                          scheduled time reached
                 +--------------------------------+
                 |                                v
publish -----> DELAYED                          READY <------ publish
                 ^                                |
                 |                                | durable fetch
                 |                                v
                 +--- retry with backoff ------ LEASED
                                                  |
                       +--------------------------+--------------------+
                       |                          |                    |
                       | valid ack                | retry allowed      | exhausted /
                       v                          v                    | explicit reject
                  COMPLETED                READY or DELAYED            v
                (record removed)                                     DEAD
                                                                       |
                                                                       | manual requeue
                                                                       v
                                                                     READY
```

| Event | Required transition |
| --- | --- |
| Immediate publish | Create record and ready index |
| Delayed publish | Create record and delayed index |
| Due delay | Remove delayed index; create ready index |
| Fetch | Remove ready index; set lease; increment attempts; create lease-expiry index |
| Valid ack | Remove job and its lease-expiry index |
| Retryable nack or lease expiry | Remove lease index; create ready/delayed index, or dead-letter if exhausted |
| Explicit permanent rejection | Remove lease index; create dead-letter index |
| Valid extension | Replace lease deadline and its index |
| Manual dead-letter requeue | Remove dead index; reset attempts and lease fields; create ready index |

For an existing job, exactly one state index must match its stored state. There must never be both a ready index and an active lease index for the same job.

A single serialized transition wins races such as acknowledge versus expiry or extension versus expiry. A command must revalidate current state when it executes, not rely on an earlier network-layer check.

### Retry policy

An attempt is consumed when a lease commits, not when a worker reports failure. This ensures abandoned deliveries count toward the limit.

For retry number `n`, starting at one after the first failed attempt:

```text
upper_bound = min(max_backoff, base_backoff * 2^(n - 1))
retry_delay = random duration between 0 and upper_bound
```

Implement this with overflow-safe arithmetic. The chosen next-available timestamp is persisted so a restart does not recompute jitter.

Jobs that reach `max_attempts` move to dead-letter state after failure or expiry. An explicit permanent rejection can dead-letter earlier. Dead-letter jobs retain their payload and count toward queue storage limits until explicitly requeued or a separately specified retention mechanism removes them.

## 6. Storage design

### Engine choice

Use Pebble initially for its write-ahead log, atomic batches, recovery, and ordered iteration. Do not implement an independent WAL alongside it.

The storage boundary exposes queue-oriented operations and snapshots, not arbitrary key/value access to transport handlers. The data directory has an exclusive lock; a second broker must refuse to open it.

### Logical keyspaces

The following keys are illustrative, not a finalized binary encoding:

```text
meta/schema-version
meta/next-ready-sequence
queue/<queue-id>
queue-name/<name>                                  -> queue-id
job/<queue-id>/<job-id>                            -> record + payload
ready/<queue-id>/<sequence>/<job-id>               -> job reference
delayed/<deadline>/<queue-id>/<job-id>              -> job reference
lease/<deadline>/<queue-id>/<job-id>               -> token/reference
dead/<queue-id>/<dead-at>/<job-id>                 -> job reference
```

Ordered numeric fields need a sortable encoding. Names and identifiers need unambiguous encoding rather than raw delimiter concatenation.

Payloads live in job records rather than being copied into every index. Fetch and scheduling read a bounded working set; they do not hydrate all payloads at startup. Persisted sequence allocation must not reuse a live ready-sequence key after recovery.

Queue usage accounting is updated with state changes where required for admission correctness. Operational metrics may be cached, but approximate metrics must not be used to bypass hard limits.

### Atomic batches and group commit

For each committed transition, update the job record, old/new indexes, sequence metadata, and relevant accounting in one atomic Pebble batch.

The coordinator may combine independent requests in one commit, subject to maximum operation count, encoded bytes, and batching delay. Commands in the same batch must see earlier staged transitions: two concurrent fetches cannot both select a job merely because the earlier lease is not committed yet. Maintain a staged batch view or avoid grouping conflicting operations until that support exists.

Processing order:

1. Validate the command against committed state plus earlier staged transitions.
2. Prepare all record and index changes.
3. Commit the batch with synchronization enabled.
4. Publish committed in-memory changes and notify waiters.
5. Return responses to successful requests.

No success response, lease delivery, or ready-job notification may escape before the commit succeeds. If a storage failure leaves commit outcome uncertain, stop accepting mutations, mark the broker unready, and recover from disk before resuming. Do not continue using potentially divergent caches.

A client cancellation before admission may prevent work. Once a command is admitted, cancellation is not an undo mechanism.

### Memory policy

Bound request queues, payload bytes awaiting commit, ready references, scheduler batches, and long-poll waiters. Size limits must count bytes as well as object counts.

Pebble caches and compaction buffers also contribute to RSS. A bounded application cache alone does not imply a fixed total process memory footprint; storage-engine budgets must be configured and measured.

## 7. API shape

The following names define intended behavior, not finalized Protobuf messages.

| Operation | Semantics |
| --- | --- |
| `CreateQueue` | Create a named queue with validated policy and limits |
| `GetQueue`, `ListQueues` | Inspect queue configuration with bounded pagination |
| `DeleteQueue` | Delete only an empty queue |
| `Publish` | Durably create one job |
| `PublishBatch` | All-or-nothing creation of a bounded batch in one queue |
| `Fetch` | Lease up to a bounded count and payload-byte budget, optionally long polling |
| `Ack`, `AckBatch` | Complete deliveries using their current lease tokens |
| `Nack` | Retry according to policy, or permanently reject |
| `ExtendLease` | Persist a validated extension for a current, unexpired lease |
| `GetQueueStats` | Return documented queue counts and operational estimates |
| `ListDeadJobs` | Page through bounded dead-letter metadata |
| `RequeueDeadJob` | Explicitly reset and requeue a dead-letter job |

### Fetch and long polling

Fetch accepts a queue, maximum jobs, maximum payload bytes, wait timeout, and lease duration. The server validates all values against configured bounds. Each delivery returns the job ID, payload, attempt number, token, and deadline.

An empty long poll waits outside the mutation coordinator. Registration and availability checks must avoid lost wakeups, and the coordinator rechecks availability when servicing it. Ready notifications happen only after commit. Timed-out or canceled waiters release their resources.

A fetch byte limit must be able to accommodate the queue's maximum single-job delivery size, including bounded envelope overhead; otherwise reject the request rather than repeatedly skipping an oversized head job.

### Batch behavior

`PublishBatch` is atomic within one queue: validation or capacity failure rejects the entire batch. Cross-queue atomic publishing is deferred.

`AckBatch` returns per-item results. Invalid or expired deliveries do not invalidate unrelated valid items. Successful transitions may share one durable commit; return no successful per-item result before that commit completes. Duplicate delivery entries within a batch must have explicitly tested behavior and cannot complete a job twice.

### Errors

Distinguish invalid arguments, missing queues/jobs, stale or expired leases, capacity exhaustion, authentication/authorization failures, and temporary unavailability. Avoid exposing internal storage details in public errors.

Retries are operation-dependent. In particular, a timeout on publish is not a safe indication to blindly retry, and an acknowledgement retry may find that the job already completed.

## 8. Concurrency, fairness, and backpressure

Begin with a single ordered mutation coordinator. Its throughput ceiling is a benchmark question, not an assumption that more goroutines will solve it.

- Keep request decoding and network writes outside the mutation loop.
- Apply maximum command counts, bytes, and time per scheduling turn.
- Bound per-queue and global in-flight deliveries.
- Limit admitted publish bytes before allocating or queuing large internal work.
- Use fair selection among active queues and waiting consumers.
- Avoid a persistent goroutine per queued job.
- Perform expensive diagnostics and logging outside the critical path.

Reject requests with a documented capacity error when limits are exceeded. Existing jobs must remain recoverable; overload must not silently discard them.

Logical payload limits do not capture physical storage use. Reserve disk headroom for the WAL, indexes, compaction, and cleanup transitions. Under disk pressure, throttle new publishing early enough to preserve room for acknowledgements and recovery. Never promise that this can overcome a completely full or failing disk.

If profiling demonstrates coordinator contention, consider queue-based ownership or storage partitioning in a later design revision. Sharding changes batching, fairness, and cross-queue management and must not be introduced implicitly.

## 9. Startup, recovery, and shutdown

### Startup

1. Validate configuration and acquire exclusive storage ownership.
2. Open Pebble and allow storage-engine recovery.
3. Validate schema version and required metadata; reject unsupported versions.
4. Restore bounded queue metadata, accounting, and scheduler cursors.
5. Validate required invariants and initialize deadline processing.
6. Start serving and report ready only when normal operations are safe.

Unexpired persisted leases remain leased after restart. Do not immediately requeue all in-flight work while workers may still be running. Expired leases are processed in bounded batches through normal retry/dead-letter transitions; API lease checks reject expired tokens even before that background cleanup finishes.

Storage corruption must cause a clear failure, not automatic deletion of the data directory.

### Graceful shutdown

- Mark the broker unready and stop admitting new work.
- Release or terminate long polls with a retriable shutdown result.
- Drain already admitted mutations within a configured deadline.
- Complete required durable commits and close storage cleanly.
- Preserve outstanding leases on disk; workers may reconnect before expiry after restart.

A shutdown timeout or crash is handled by the same recovery rules. Shutdown does not imply successful completion of jobs currently executing in workers.

### Clock behavior

Persist deadlines as UTC timestamps for restart recovery, and use monotonic timing for process-local waits where possible. Inject the clock in tests. Forward wall-clock jumps can make jobs due or leases expire early; backward jumps can delay them. Document time synchronization requirements rather than claiming immunity to clock changes.

## 10. Observability and security

### Logs and metrics

Use structured logging through `log/slog`. Include bounded request, queue, and job identifiers where useful; avoid logging payloads, credentials, or arbitrary unbounded metadata.

Planned metrics include:

- Publish, fetch, acknowledge, and completed-job rates.
- Request and durable-commit latency distributions.
- Ready, delayed, leased, and dead-letter counts.
- Queue wait time, retries, lease expirations, and stale-token errors.
- Admission rejections, scheduler lag, and long-poll waiters.
- Memory, disk usage, storage errors, and compaction activity.

Do not use job IDs, request IDs, or arbitrary customer metadata as metric labels. Queue labels must be configurable or bounded to control cardinality.

### Operational endpoints

- Liveness indicates whether the process is alive; it is not a durability guarantee.
- Readiness indicates recovery is complete and the engine can safely accept its configured workload.
- Metrics are exposed over an operational HTTP listener with deployment-appropriate access restrictions.

Readiness becomes false during shutdown and fatal storage failure. Detailed queue inspection belongs in authenticated management APIs, not public health responses.

### Security baseline

Before untrusted-network use, implement authentication for broker and management operations, TLS or a documented trusted TLS termination boundary, request-size limits, and protected operational endpoints. Local development should bind to loopback by default rather than expose an unauthenticated public listener.

Fine-grained multi-tenant authorization and per-tenant isolation are separate future requirements. An initial shared credential does not establish tenant isolation.

## 11. Backup and availability

A single broker is a single availability domain. Process restart recovery is not high availability, and local durability is not a backup.

Use a supported consistent Pebble checkpoint/backup mechanism. Do not copy live database files arbitrarily and assume the result is restorable. Backup, restore, and schema compatibility must be tested before the first operational release.

Restoring an older backup can reintroduce previously completed jobs and cannot recover jobs published after the backup. Workers still need idempotency.

Replication and failover require a separate design covering ownership, consensus, quorum durability, and split-brain prevention. They cannot be safely added by placing several independent brokers behind a load balancer.

## 12. Correctness and performance validation

Required invariants include:

1. Each stored job has exactly one matching state index.
2. Only one current lease token exists per job.
3. An expired or superseded token cannot mutate a delivery.
4. Every success response follows the operation's durable commit.
5. Queue counts and hard-limit accounting agree with committed transitions.
6. Acknowledged completion removes the active job across restart.
7. Dead-lettering retains payloads until an explicit supported action removes them.
8. No bounded API operation requires loading the entire backlog into memory.

Test races, random transition sequences, commit failures, and abrupt process termination. A reference state machine and controllable clock should drive deterministic tests before storage optimization.

Performance results must distinguish durable publishing from end-to-end completed jobs and include tail latency, memory, backlog size, and compaction effects. The measurement matrix and release criteria are in the [project plan](PLAN.md).

## 13. Deferred architectural decisions

The following need explicit later design work rather than speculative abstractions now:

- Publisher deduplication windows, keys, and storage cost.
- Completed-job retention and audit history.
- Per-queue execution shards or separate storage partitions.
- Custom append-only segments if Pebble is a measured bottleneck.
- Replication, leader election, and automatic failover.
- Additional SDKs, push delivery, routing, or replayable streams.

The current architecture should remain small enough to validate its guarantees before any of these are added.
