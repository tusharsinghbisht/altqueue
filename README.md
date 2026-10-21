# AltQueue

A planned high-throughput, durable work queue written in Go.

AltQueue is intended for background work such as webhook delivery, email processing, report generation, and asynchronous application tasks. The initial design combines task-queue features associated with BullMQ with explicit delivery acknowledgements associated with message brokers such as RabbitMQ.

**Status: design stage.** This repository currently contains documentation only. The broker, SDK, CLI, and features described below are not implemented, and there are no installation or run commands yet.

## Project direction

The first release will be a **single-node, persistent task queue**, not a distributed event-streaming platform.

The goal is to make a focused system correct, observable, and measurable before adding clustering or advanced routing. High throughput is a design objective, not a benchmark claim.

| System | Primary model | Relationship to AltQueue |
| --- | --- | --- |
| BullMQ | Background jobs, retries, and scheduling | Inspiration for the worker experience |
| RabbitMQ | Message delivery, acknowledgements, and routing | Inspiration for delivery semantics; no AMQP compatibility planned initially |
| Kafka | Retained event logs and independent consumer offsets | A different model; replayable streams are outside the initial scope |

In the initial model, workers on the same queue compete for jobs. A successful acknowledgement removes the job from active storage. AltQueue will not retain every completed job for replay or broadcast each job to independent subscriber groups.

## Planned first-release capabilities

- Named queues and configurable queue limits.
- Single and batched publishing.
- Worker fetch with long polling and bounded batches.
- Explicit acknowledgement, negative acknowledgement, and lease extension.
- Visibility leases that make abandoned jobs eligible for redelivery.
- Delayed jobs, retry backoff with jitter, and attempt limits.
- Dead-letter storage, inspection, and manual requeue.
- Durable storage and process-restart recovery.
- A Go SDK and command-line client.
- Health endpoints, structured logs, and Prometheus metrics.
- Authentication and TLS support before deployment on untrusted networks.

## Delivery contract

The planned durable mode has the following contract:

- **At-least-once delivery:** a job may execute more than once.
- **Durable publish acknowledgement:** success is returned only after the job is committed to local durable storage.
- **Leased processing:** a fetched job is temporarily reserved, not permanently removed.
- **Explicit completion:** an acknowledgement with a valid, unexpired lease completes the job.
- **No strict execution ordering:** concurrent workers, delays, retries, and lease expiry can reorder work.
- **Bounded resource use:** overload must produce backpressure rather than unbounded memory growth.

A durable publish does not mean that a worker has processed the job. Local durability also does not provide availability or protection against loss of the broker's disk.

Workers must make external side effects idempotent where possible. AltQueue cannot atomically combine a queue acknowledgement with an application's database update, payment, or HTTP request. Exactly-once execution is not promised.

See the [architecture document](docs/ARCHITECTURE.md) for the precise state transitions, failure behavior, and durability boundary.

## Planned architecture

```text
Producers                         Workers
    |                                |
    +---------------+----------------+
                    | gRPC / Protobuf
                    v
          API, validation, and limits
                    |
                    v
          Queue engine and scheduler
                    |
                    v
          Batched durable state changes
                    |
                    v
             Pebble on local disk

HTTP: health, readiness, and metrics
```

The proposed implementation uses Go, gRPC, Protobuf, Pebble, `log/slog`, and Prometheus instrumentation. These are design decisions, not existing dependencies in this repository.

## Initial non-goals

- Multi-node replication, automatic failover, or distributed consensus.
- Kafka-style retention, offsets, and event replay.
- AMQP, Kafka, Redis, or BullMQ protocol compatibility.
- Complex exchanges, topic routing, or fan-out subscriptions.
- Job dependencies, workflows, cron schedules, or priority queues.
- A web dashboard or clients for every language.
- Exactly-once processing guarantees.

## Documentation

- [Project plan](docs/PLAN.md): scope, milestones, acceptance criteria, and risks.
- [Architecture](docs/ARCHITECTURE.md): data model, storage, APIs, concurrency, recovery, and operations.

## Repository layout

Current layout:

```text
altqueue/
├── README.md
└── docs/
    ├── PLAN.md
    └── ARCHITECTURE.md
```

The proposed implementation layout is recorded in the project plan. No application scaffolding is included yet.

## Next milestone

Document the delivery contract in executable state-machine tests, then build an in-memory publish → fetch → acknowledge prototype with deterministic lease-expiry tests. Add persistence before making durability or production-readiness claims.
