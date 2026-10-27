# API and Go SDK

The versioned API is [queue.proto](../api/proto/altqueue/v1/queue.proto). Generated bindings under `api/gen/altqueue/v1` are checked in. The protocol is experimental until a compatibility policy is published.

## Methods

| RPC | Behavior |
| --- | --- |
| `CreateQueue` | Create with defaults when config is absent, or with supplied limits |
| `GetQueue` | Configuration and state counts |
| `ListQueues` | Lexical name pagination: `after`, `limit` (1-128) |
| `DeleteQueue` | Delete an empty queue only |
| `Publish` | Atomic single-queue batch; optional initial delay |
| `Fetch` | Bounded leased batch; optional long poll |
| `Ack` | Complete one current delivery |
| `AckBatch` | Commit valid entries and return per-item status |
| `Nack` | Retry according to captured policy, or permanently reject |
| `ExtendLease` | Extend a current, unexpired lease without shortening it |
| `ListDeadJobs` | Bounded dead-letter payload inspection with lexical job-ID pagination |
| `RequeueDeadJob` | Reset attempts and return a dead-letter job to ready |

`GetQueue` includes total jobs, ready/delayed/in-flight/dead counts and stored payload bytes; these are committed engine state, not approximate metrics.

## Limits and errors

- Queue names: 1-64 ASCII letters, digits, `.`, `_`, `-`.
- Batch count: 1-120; maximum single payload: 1 MiB; combined payload budget: 4 MiB.
- Fetch may return fewer jobs due to backlog, payload budget or in-flight capacity. FIFO means it stops at the next job that cannot fit rather than skipping it.
- Network durations use milliseconds. Lease windows: positive, at most 24 hours; long polls: at most 30 seconds; initial delay: at most 365 days.
- Empty immediate fetch and an elapsed server-side long poll return an empty delivery list. A caller's canceled or expired context returns a context status.
- Per-queue defaults and process ceilings are listed in the [README](../README.md).

| gRPC code | Meaning |
| --- | --- |
| `InvalidArgument` | Invalid limits, configuration, payload sizes or duration |
| `AlreadyExists` | Queue name already exists |
| `NotFound` | Queue or required dead-letter job does not exist |
| `ResourceExhausted` | Queue/global capacity, in-flight, request-stream or long-poll limit |
| `FailedPrecondition` | Stale/expired lease or attempted deletion of a nonempty queue |
| `Unauthenticated` | Missing or invalid configured token |
| `Unavailable` | Shutdown or failed storage |
| `Canceled` / `DeadlineExceeded` | Caller cancellation/deadline |

`AckBatch` reports invalid/missing/stale receipts per item; a duplicate accepted receipt succeeds only once. A storage failure fails the RPC rather than returning successful per-item acknowledgements.

A non-success network result does not prove a mutation was not committed. There is no publisher deduplication. Handlers must make external side effects idempotent.

## Go client

Import the public SDK; applications do not import `internal/queue`:

```go
import (
    "context"
    "time"
    altqueue "github.com/tusharsinghbisht/altqueue/sdk/go"
)

ctx := context.Background()
client, err := altqueue.Dial(ctx, "127.0.0.1:7331", altqueue.Options{
    Insecure: true, // Loopback development only; TLS is the SDK default.
})
if err != nil { /* handle */ }
defer client.Close()

_, err = client.CreateQueue(ctx, "jobs", nil)
// Handle AlreadyExists if appropriate for your application's startup policy.
ids, err := client.Publish(ctx, "jobs", [][]byte{[]byte("hello")}, 0)
_ = ids
_ = err
```

Client options support a bearer token, a verified `tls.Config`, and a default request timeout (35 seconds). Explicit context deadlines take precedence. The SDK disables application-level retries for ambiguous mutations. For advanced use, `client.API` exposes the generated gRPC client.

Use TLS by leaving `Insecure` false and supplying roots/server name in `Options.TLS` if required. Never set `InsecureSkipVerify`.

## Worker helper

```go
err = client.RunWorker(ctx, "jobs", altqueue.WorkerOptions{
    Concurrency: 4,
    LeaseDuration: 30 * time.Second,
    Wait: 5 * time.Second,
}, func(ctx context.Context, delivery *altqueue.Delivery) error {
    // Honor ctx, validate payloads, and use an application idempotency key.
    // nil acknowledges; a returned error requests retry.
    return process(ctx, delivery.Payload)
})
```

The helper renews the lease every third of the configured window. Heartbeat RPCs have their own deadline; heartbeat failure cancels the handler context and does not acknowledge the possibly stale delivery. Worker lease windows must be at least three seconds. Handlers that ignore cancellation cannot be forcibly stopped by the SDK.

Fetch-side temporary unavailability/capacity errors are retried with a bounded pause. Acknowledgement ambiguity or lost lease stops the worker and is returned to the application rather than silently executing a potentially unsafe completion retry. Handler panics are not recovered; outstanding work will be redelivered after expiry.

See the compiling [producer](../examples/producer/main.go) and [worker](../examples/worker/main.go) examples.

## Regenerate bindings

Only needed when changing the protocol. Install `protoc` and these pinned Go plugins on PATH:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.9
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
protoc -I api/proto --go_out=. --go_opt=module=github.com/tusharsinghbisht/altqueue --go-grpc_out=. --go-grpc_opt=module=github.com/tusharsinghbisht/altqueue api/proto/altqueue/v1/queue.proto
```

Checked-in code was generated with protoc 29.3. Review protocol/schema changes separately from implementation changes; changing generated files by hand is not supported.
