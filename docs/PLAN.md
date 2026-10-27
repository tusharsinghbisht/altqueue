# AltQueue Project Plan

**Status:** a runnable single-node durable MVP is implemented. This document retains the original release roadmap; not every acceptance criterion is complete. See [implementation status](IMPLEMENTATION.md) for current behavior and limitations.

**Related documents:** [README](../README.md) · [Architecture](ARCHITECTURE.md) · [Running](RUNNING.md)

### Current milestone summary

| Milestone | Current status |
| --- | --- |
| M0 | Module, contract, fake clocks and CI configuration exist; remote CI still needs validation |
| M1 | Engine, batch operations, long polling and gRPC implemented |
| M2 | Synchronized Pebble commits, recovery, checkpoints and abrupt-exit tests implemented; cross-request group commit and disk-only backlogs remain |
| M3 | Delays, bounded deadline scheduling, retries, dead-letter inspection and replay implemented |
| M4 | Admission controls and local benchmark exist; profiling, durable network benchmarks and soak testing remain |
| M5 | Go SDK, renewing workers, CLI, examples and run documentation implemented |
| M6 | TLS/token auth, metrics, shutdown and checkpoint restore implemented; WSL2 Docker build/runtime smoke verified; disk-headroom handling, security review, migration/release policy and broader deployment validation remain |

## 1. Product goal

Build a high-throughput, durable task queue in Go with straightforward worker APIs and explicit failure semantics.

The first release targets background-job workloads: webhook delivery, email, report generation, and asynchronous backend processing. It is a competing-consumer queue: one job is leased to one worker at a time, but failures can cause repeated execution.

The first release is intentionally single-node. It is not a replacement for every capability of RabbitMQ, BullMQ, or Kafka.

### Success means

- An acknowledged publish survives a broker process crash, assuming healthy storage that honors synchronization requests.
- Jobs follow a documented, tested lifecycle without disappearing during state transitions.
- Expired leases permit redelivery; stale workers cannot acknowledge a newer delivery.
- Memory and in-flight work remain bounded when producers outrun consumers.
- Throughput and tail latency can be reproduced from a documented benchmark workload.
- A developer can run a broker, publish a job, and process it using the Go SDK and examples.

No messages-per-second target is a product promise until hardware, payloads, batching, durability, and workload have been specified and measured.

## 2. Scope

### First durable release

| Area | Included |
| --- | --- |
| Queue management | Create, inspect, list, and delete empty queues |
| Publishing | Single and bounded batch publishing to one queue |
| Consumption | Bounded fetch, long polling, and competing workers |
| Delivery | Acknowledgement, negative acknowledgement, lease extension |
| Recovery | Durable job state, persisted leases, restart recovery |
| Scheduling | One-time delayed jobs and delayed retries |
| Failures | Attempt limits, retry backoff with jitter, dead-lettering |
| Dead jobs | Bounded inspection and explicit manual requeue |
| Limits | Payload, batch, queue capacity, in-flight, and request limits |
| Client tools | Go SDK, CLI, and runnable examples |
| Operations | Logs, metrics, health, readiness, graceful shutdown |
| Security | Authentication, TLS, and operational guidance before untrusted-network use |

### Deferred

- Replication, clustering, failover, and partition reassignment.
- Replayable event streams and independent consumer-group offsets.
- Protocol compatibility with existing brokers.
- Complex routing, broadcast subscriptions, and exchanges.
- Workflow graphs, cron scheduling, and job priorities.
- Completed-job history and arbitrary payload search.
- Publisher deduplication and publish-time idempotency keys.
- A dashboard and SDKs beyond Go.
- Exactly-once execution.

Publisher deduplication may be reconsidered after the first durable release. Until then, retrying a publish with an unknown outcome can create another job.

## 3. Working principles

1. **Define semantics before optimizing.** Every operation needs a durability boundary and failure behavior.
2. **Implement vertical slices.** Prefer a small end-to-end feature over broad scaffolding.
3. **One durable mode initially.** Do not inflate performance numbers by silently relaxing synchronization.
4. **Use an existing storage engine.** Pebble provides the initial WAL, atomic batches, and recovery.
5. **Bound every hot-path buffer.** Include bytes as well as item counts in limits.
6. **Use evidence for complexity.** Add sharding or custom storage only after profiling identifies a bottleneck.
7. **Treat retries as normal.** Duplicate execution is part of the delivery contract.
8. **Separate planned and implemented behavior.** Documentation and examples must not imply unavailable features.

## 4. Milestones

### M0 — Contract and repository foundation

**Deliverables**

- Confirm the task-queue model and first-release scope.
- Define job states, lease semantics, retry behavior, and API error categories.
- Specify queue limits and batch atomicity.
- Initialize the Go module using a supported Go release and the chosen repository path.
- Add formatting, test, vet, and race-detector checks to CI on supported runners.
- Establish a clock abstraction and test fixtures.

**Exit criteria**

- State transitions and failure cases in the architecture document are accepted.
- The project builds and tests in CI.
- No unimplemented feature is described as available.

### M1 — In-memory queue prototype

**Deliverables**

- Queue creation and basic configuration.
- Publish, batch publish, fetch, acknowledge, and negative acknowledge.
- Unique delivery lease tokens, deadlines, and lease extension.
- Bounded long polling and cancellation.
- An in-memory engine exercised through a minimal gRPC API.

**Exit criteria**

- Multiple workers cannot hold the same current lease.
- Expired and superseded lease tokens cannot complete a job.
- Tests advance a fake clock rather than sleep for lease deadlines.
- Queue, fetch, and request limits are enforced.
- The prototype is explicitly documented as non-durable.

### M2 — Durable queue core

**Deliverables**

- Pebble-backed job records and state indexes.
- A serialized mutation coordinator with bounded group commits.
- Durable publish, lease acquisition, acknowledgement, and lease extension.
- Storage schema versioning and exclusive ownership of the data directory.
- Recovery and startup readiness checks.
- Process-crash and storage-error test harnesses.

**Exit criteria**

- Every acknowledged publish is recoverable after abrupt process termination unless it was subsequently completed or explicitly removed.
- A job-state transition and its index changes commit atomically.
- A fetch response is sent only after its lease is durable.
- An acknowledged completion does not reappear after restart.
- Unexpired leases survive restart; expired leases are eventually retried or dead-lettered.
- Storage errors make the broker fail closed rather than acknowledge uncommitted writes.

### M3 — Scheduling and failure handling

**Deliverables**

- Delayed jobs and deadline-indexed scheduling.
- Retry policy with capped exponential backoff and jitter.
- Attempt limits and bounded lease-expiry processing.
- Dead-letter inspection and manual requeue.
- Validation of retry, delay, and lease configuration.

**Exit criteria**

- Delayed jobs are not made ready before their scheduled time according to the broker clock.
- Every successful lease acquisition increments the attempt count exactly once.
- Retry deadlines are persisted and survive restart.
- Jobs that exhaust attempts are retained in dead-letter state.
- Manual requeue is an explicit operation that resets attempts and preserves the payload.
- Scheduled and dead-letter backlogs do not require loading all payloads into memory.

### M4 — Throughput and resource controls

**Deliverables**

- Tuned batching with explicit maximum size and wait time.
- Admission control by queue depth, stored payload bytes, in-flight work, and outstanding requests.
- Bounded ready-job caches and scheduler batches.
- CPU, allocation, lock-contention, and storage profiling.
- Reproducible publish-only and end-to-end benchmarks.

**Exit criteria**

- Backlogs can grow on disk without proportional growth in resident payload memory.
- Overload produces documented throttling or rejection instead of unbounded buffers.
- One busy queue cannot indefinitely starve other queues.
- Benchmarks include durable commit settings and p95/p99 latency.
- Any concurrency or sharding change has a measured benefit and preserves correctness tests.

### M5 — Developer experience

**Deliverables**

- Go SDK with timeouts, batch helpers, lease-extension support, and conservative retry behavior.
- CLI for queue management, publishing, inspection, and dead-letter operations.
- Producer and worker examples with idempotent-processing guidance.
- Configuration reference and actual build/run instructions.

**Exit criteria**

- A new developer can complete a publish/process/ack walkthrough.
- SDK retries do not silently imply publish deduplication.
- Cancellation, ambiguous request outcomes, and stale leases are documented.
- Examples compile and run in automated checks.

### M6 — Operational hardening and first release

**Deliverables**

- Authentication and TLS for remote access; safe local-development defaults.
- Prometheus metrics, structured logging, health, and readiness endpoints.
- Graceful shutdown, disk-pressure handling, and clear storage-failure behavior.
- Consistent-backup and restore procedures using supported storage-engine facilities.
- Compatibility policy for API and on-disk schema changes.
- Sustained-load, restart, and recovery tests.

**Exit criteria**

- The broker is not ready while recovering or after a fatal storage failure.
- Shutdown does not acknowledge uncommitted work or erase outstanding leases.
- Restore is tested, and its possibility of reprocessing older work is documented.
- A sustained-load test checks memory, disk growth, compaction, and recovery time.
- Release documentation includes measured limits and the single-node availability caveat.

## 5. Proposed implementation layout

Create directories when their implementation begins, rather than adding empty packages upfront.

```text
altqueue/
├── README.md
├── go.mod
├── cmd/
│   ├── altqueued/             # Broker executable
│   └── altqueue/              # CLI
├── api/
│   └── proto/                # Protobuf API definitions
├── internal/
│   ├── broker/               # Lifecycle, mutation coordination, admission
│   ├── queue/                # Job state machine and queue policy
│   ├── storage/              # Pebble records, indexes, recovery
│   ├── transport/            # gRPC and operational HTTP endpoints
│   ├── config/
│   └── observability/
├── sdk/
│   └── go/                   # Public client package
├── tests/
│   └── integration/
├── benchmarks/
├── examples/
└── docs/
    ├── PLAN.md
    └── ARCHITECTURE.md
```

Keep domain transitions independent of transport details. Keep the storage interface small and queue-oriented; do not build a general database abstraction or plugin system.

## 6. Test strategy

| Test type | Required coverage |
| --- | --- |
| Unit | Validation, retry calculations, limits, lease-token checks |
| State machine | Every legal transition and rejection of invalid transitions |
| Property/fuzz | Random command sequences preserve job/index invariants |
| Concurrency | Racing fetches, ack versus expiry, extension versus expiry |
| Integration | gRPC API, Pebble persistence, restart, SDK behavior |
| Fault injection | Commit failures, abrupt termination, full disk, canceled requests |
| Recovery | Mixed ready/delayed/leased/dead jobs and persisted deadlines |
| Load/soak | Growing backlog, slow consumers, hot queues, compaction |
| Security | Authentication failures, oversized requests, protected admin endpoints |

A process-kill test is not proof of power-loss durability. Storage guarantees also depend on the filesystem and hardware honoring synchronization requests.

## 7. Benchmark plan

Begin with representative payloads such as 128 B, 1 KiB, and 16 KiB; batch sizes of 1, 32, and 128; and increasing producer and worker concurrency. Keep all test cases within configured payload and batch limits.

Measure separately:

- Durable publish throughput and acknowledgement latency.
- Completed jobs per second, including fetch and acknowledgement traffic.
- Queue wait time and end-to-end completion latency.
- CPU, allocations, RSS, disk bandwidth, disk space, and compaction impact.
- Performance with no backlog, a large ready backlog, delayed jobs, and many leases.
- Overload behavior and recovery time after an unclean restart.

Every published result must record the commit, Go version, OS, CPU, RAM, disk/filesystem, payload and metadata sizes, queue count, client concurrency, batch sizes, lease policy, synchronization settings, warmup, and test duration. In-memory prototype results must not be presented as durable-broker results.

## 8. Risks and responses

| Risk | Response |
| --- | --- |
| Scope expands into three different broker models | Keep a written first-release boundary; defer streams and clustering |
| Incorrect job/index updates lose or duplicate ownership | Atomic transitions, invariant tests, crash testing |
| Fsync or compaction limits throughput | Batching, profiling, bounded backlogs; no hidden durability relaxation |
| Retry storms overwhelm the broker | Jitter, retry caps, bounded scheduler work, admission control |
| A hot queue starves others | Fair bounded scheduling; profile before introducing shards |
| Physical disk usage exceeds logical payload accounting | Disk headroom checks; account for WAL, indexes, and compaction |
| Lost responses lead clients to repeat operations | Document unknown outcomes and application-level idempotency |
| Clock changes affect schedules and leases | Injectable clock, operational guidance, explicit clock assumptions |
| Single-node failure makes the service unavailable | Document the limit; test backups; design HA separately later |

## 9. Decisions to validate during implementation

These are not blockers for the documentation stage:

- Default and maximum payload sizes, batch sizes, lease durations, and queue limits.
- Retry defaults, dead-letter retention policy, and minimum free-disk threshold.
- Initial authentication mechanism and supported deployment platforms.
- Repository/module path, license, and supported Go version.
- Target hardware and workload for the first performance baseline.

Until an automatic dead-letter retention policy is explicitly specified, dead-letter jobs remain stored and count toward queue capacity. Do not silently discard them under disk pressure.

## 10. Immediate next step

Run the durable broker on representative hardware, record local and network baselines, and prioritize bounded-working-set storage and cross-request group commit from measured bottlenecks. Complete the production-readiness gaps listed in [implementation status](IMPLEMENTATION.md) before declaring the original first release complete. Clustering, a custom WAL and a dashboard remain out of scope.
