# Implementation status

AltQueue now has a runnable **single-node durable MVP**. This is not a declaration that every production-readiness requirement in the original plan is complete.

## Delivered

- Existing in-memory API preserved, including `CreateQueue`, `PublishBatch`, and `FetchBatch`.
- Per-queue job count, payload-byte and in-flight limits; additional process-wide ceilings.
- Persistent queue configuration and per-job records in Pebble.
- Synchronized atomic batches before successful publication, delivery, acknowledgement, retry, extension, requeue, or queue deletion.
- Recovered ready ordering, payload accounting, delayed deadlines, and unexpired leases.
- Fail-closed storage errors: readiness becomes false and normal engine operations stop until restart.
- A deadline heap with one entry per leased/delayed job; heartbeats update entries rather than accumulating stale timers.
- Bounded scheduler turns; delayed jobs; capped exponential retry backoff with full jitter; dead-lettering; manual replay.
- gRPC/Protobuf API with checked-in bindings, bounded long polling, message limits, admission control, shared-token authentication and optional TLS.
- Per-item batch acknowledgements; repeated valid receipts are treated as stale after the first accepted item.
- CLI, public Go SDK, bounded worker concurrency and automatic heartbeat support.
- Structured broker logs, aggregate Prometheus metrics, health/readiness, signal-driven graceful shutdown.
- Consistent local checkpoints, restore documentation, CI configuration, Dockerfile and examples.

## Storage implementation versus original design

The current engine serializes transitions using its mutex. Each request commits its own Pebble atomic batch. The durable keyspaces are `meta/schema`, `meta/sequence`, `queue/<encoded-name>`, `job/<encoded-name>/<job-id>`, and `payload/<encoded-name>/<job-id>`.

A job metadata record includes its state, ready sequence, captured retry policy, and lease fields. Immutable payloads have separate raw-byte records, written at publication and deleted at completion; fetching and heartbeats do not rewrite payloads. Runtime ready indexes, deadline heaps and counters are reconstructed and validated on open. There are no separate durable ready/lease index records to drift away from the job record.

Queue names are encoded unambiguously. Schema version 1 is accepted; unsupported schema versions fail to open. No automatic migration or repair that deletes data is attempted.

**All unfinished payloads remain resident in memory.** This deliberately builds on the original in-memory engine and provides a practical baseline, but does not implement the original disk-backlog scaling goal. Job count and payload ceilings bound admitted state, not total process RSS.

Cross-request group commit is not implemented. Batch requests share a commit, but independent requests do not. The mutex and synchronous persistence are expected bottlenecks and must be measured before claims about high throughput.

## Focused verification

Existing engine tests are retained. Added acceptance coverage is intentionally small and scenario-oriented:

- Durable lifecycle across restart, including delayed work, retry exhaustion, dead-letter replay, queue deletion, and checkpoint restore.
- Injected commit failure and fail-closed recovery without uncommitted jobs.
- Payload-byte and in-flight accounting.
- Abrupt child-process exit without closing storage: acknowledged completion stays removed and unfinished leases recover.
- Real TCP gRPC with TLS and authentication, delayed long polling, retries, batch acknowledgement, metrics protection and shutdown readiness.

A process-exit test is **not proof of power-loss durability**. Filesystems and storage hardware must honor synchronization requests. Existing concurrency and race-detector tests do not replace sustained adversarial load tests.

## Docker validation in WSL2

The existing Dockerfile was built and smoke-tested using Docker Engine 29.6.1 in WSL2 Ubuntu 26.04, on Linux amd64. The resulting local image was approximately 25.2 MiB; size will vary with base-image updates.

The container run verified non-root execution, writable named-volume ownership, rejection of unsafe default remote listeners, token authentication, publish/fetch/ack, preservation of unexpired leases across restart, dead-letter replay, recovery of delayed work after SIGKILL and container replacement, protected metrics, and graceful SIGTERM shutdown with exit code 0.

Temporary smoke containers and their volume were removed. This validates the local container workflow, not production load behavior or every deployment platform. WSL instructions are in [running and operations](RUNNING.md#windows-with-wsl2).

## Still required before a production release

1. Disk-only or bounded-working-set payload handling for large backlogs.
2. Profiling, cross-request group commit, throughput/latency baselines and sustained-load testing.
3. Explicit disk-headroom admission and full-disk/compaction failure exercises.
4. More recovery invariant validation, fuzzing, and fault injection around ambiguous commit outcomes.
5. Authentication lifecycle/rotation, deployment-specific authorization, and security review.
6. Metrics for commit latency, scheduler lag, queue wait time and storage/compaction pressure.
7. API compatibility and storage migration policy, release packaging and a selected project license.
8. CI execution on remote runners and validation on deployment environments beyond the local WSL2 Docker smoke run.

Clustering, replication, Kafka-style streams, publisher deduplication and exactly-once execution are outside this MVP, not features silently implied by it.

## Practical next priorities

Run the broker on representative hardware, record the lifecycle baseline, then measure durable batched publish/fetch/ack over the network. Use those measurements to decide whether group commit, payload paging, or transport work should come first. Do not use the memory-only benchmark as a durable throughput claim.

[README](../README.md) · [Roadmap](PLAN.md) · [Architecture](ARCHITECTURE.md)
