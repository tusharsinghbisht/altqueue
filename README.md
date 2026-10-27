# AltQueue

A single-node durable task queue written in Go, with gRPC, a Go SDK, and a CLI.

**Status: runnable MVP, not a production-hardened RabbitMQ/Kafka replacement.** The broker implements persistent jobs, competing workers, leases, retries, delayed jobs, dead-lettering, limits, and restart recovery. Performance targets have not been established or independently validated.

## Quick start

Requires the Go version specified in `go.mod` and a local writable disk. Protobuf bindings are checked in; `protoc` is not needed to build or run.

Start the broker:

```sh
go run ./cmd/altqueued
```

In another terminal:

```sh
go run ./cmd/altqueue create emails
go run ./cmd/altqueue publish emails "hello" "another job"
go run ./cmd/altqueue work emails
```

The demo `work` command prints and acknowledges each job. For application handlers and automatic lease renewal, use the [Go SDK worker example](examples/worker/main.go).

Data is stored in `./data`. Restarting the broker reopens that directory. Only one broker may own it at a time.

Default listeners are loopback-only:

- gRPC: `127.0.0.1:7331`
- HTTP health/readiness/metrics: `127.0.0.1:7332`

## Features implemented

- Named queues with independent stored-job, payload-byte, in-flight, and retry limits.
- Atomic batch publishing and bounded FIFO batch fetching.
- Explicit acknowledgements, per-item batch acknowledgements, and negative acknowledgements.
- Lease tokens, lease extension, expiry recovery, and stale-worker rejection.
- Delayed publication, capped exponential retry backoff with full jitter, and attempt limits.
- Dead-letter inspection and manual requeue.
- Pebble atomic batches with synchronized commits before mutation success responses.
- Restart recovery, storage schema validation, exclusive directory ownership, and local checkpoints.
- Bounded, cancelable long polling and periodic deadline scheduling.
- gRPC/Protobuf API, Go client, heartbeat-enabled workers, and CLI.
- TLS, shared-token authentication, request admission controls, metrics, and graceful shutdown.

## Delivery contract

**At least once, not exactly once.** Workers must make external side effects idempotent. A lease token prevents an old worker from changing broker ownership, but cannot stop that worker from writing to an unrelated database or service.

- Successful publication means a synchronized local storage commit, not successful processing.
- Fetch commits the lease before returning work.
- Ack commits completion before success; completed payloads are removed rather than retained for replay.
- Lost responses leave an unknown outcome. Retrying publication can create another job; publisher deduplication is not implemented.
- Concurrent processing, retries, and delays do not guarantee completion ordering.
- Local durability does not protect against loss of the node's disk.

## Current limits and caveats

The durable MVP keeps job payloads **in memory as well as on disk**. Queue limits apply to all unfinished states, including dead-letter jobs. It does not yet provide a disk-only backlog or cross-request group commit.

Defaults per queue:

| Setting | Default |
| --- | --- |
| Stored jobs | 100,000 |
| Stored payload bytes | 64 MiB |
| In-flight jobs | 1,000, capped at the job limit when defaulted |
| Total delivery attempts | 5 |
| Retry base | 0: immediate retry |
| Maximum retry backoff | 1 minute |

Process-wide ceilings: 128 queues, 1,000,000 stored jobs, and 1 GiB of stored payload bytes. These are logical admission limits, **not an RSS guarantee**; Go objects, request copies, Pebble, and compaction also consume memory.

Requests allow up to 120 jobs, 1 MiB per job, and 4 MiB of combined job payload. The scheduler handles bounded deadline batches every 100 ms. Network durations use milliseconds.

There is no clustering, replication, event replay, AMQP/Kafka compatibility, priority scheduling, or multi-tenant authorization. Disk-headroom admission, sustained-load validation, and independent performance measurements remain release work. See [implementation status](docs/IMPLEMENTATION.md).

## Examples

```sh
# Custom queue limits and delayed retries
go run ./cmd/altqueue create -max-jobs 1000 -max-bytes 10485760 -max-in-flight 10 -max-attempts 5 -retry-base 1s notifications

# Delayed publication
go run ./cmd/altqueue publish -delay 5s notifications "send later"

# Lease jobs without automatically acknowledging them
go run ./cmd/altqueue fetch -limit 10 -lease 30s -wait 5s notifications

# Inspect queues and dead-letter jobs
go run ./cmd/altqueue queues
go run ./cmd/altqueue stats notifications
go run ./cmd/altqueue dead notifications
```

CLI flags go **before** positional arguments. JSON payload fields in CLI output are base64 encoded. Fetch output includes the job ID and token required by `ack`, `nack`, and `extend`.

## Security

Local development can run without a token or TLS. Do not expose those defaults publicly. Remote listeners require **both TLS and `ALTQUEUE_TOKEN`**, unless an explicit unsafe development override is supplied. When configured, the token protects gRPC and metrics; health/readiness return only basic status.

See [running and operations](docs/RUNNING.md) for authentication, TLS, backups, Docker, and shutdown.

## Build and verify

```sh
go build ./cmd/altqueued ./cmd/altqueue
go test ./...
go vet ./...
go test -race ./...
go test ./benchmarks -run '^$' -bench BenchmarkRoundTrip -benchmem
```

Tests remain beside source where they need private state; the end-to-end broker test is under `tests/integration/`. New coverage focuses on durable lifecycle, failure recovery, and end-to-end behavior rather than repeating every permutation.

## Documentation

- [Running and operations](docs/RUNNING.md)
- [API and Go SDK](docs/API.md)
- [Implementation status and limitations](docs/IMPLEMENTATION.md)
- [Architecture and original design target](docs/ARCHITECTURE.md)
- [Project roadmap](docs/PLAN.md)

## Layout

```text
api/proto/          Protocol definitions
api/gen/            Checked-in generated Go bindings
cmd/altqueued/      Broker
cmd/altqueue/       CLI
internal/queue/     Engine, resource controls, scheduler, persistence
internal/transport/ gRPC, authentication, health and metrics
sdk/go/             Public Go client and worker helper
examples/           Producer and worker
benchmarks/         Local lifecycle baseline
tests/integration/  TLS/authenticated end-to-end scenario
docs/               Design, usage and operational documentation
```
