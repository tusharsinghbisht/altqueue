package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	altqueue "github.com/tusharsinghbisht/altqueue/sdk/go"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	client, err := altqueue.Dial(ctx, "127.0.0.1:7331", altqueue.Options{Insecure: true, Token: os.Getenv("ALTQUEUE_TOKEN")})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	err = client.RunWorker(ctx, "examples", altqueue.WorkerOptions{Concurrency: 4}, func(ctx context.Context, d *altqueue.Delivery) error {
		// Real handlers must honor ctx and deduplicate external side effects.
		// RunWorker renews the lease, acknowledges nil, and retries returned errors.
		log.Printf("processing job=%s attempt=%d payload=%s", d.JobId, d.Attempt, d.Payload)
		return ctx.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
