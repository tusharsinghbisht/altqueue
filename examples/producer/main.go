package main

import (
	"context"
	"log"
	"os"
	"time"

	altqueue "github.com/tusharsinghbisht/altqueue/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Plaintext is explicitly enabled only for this loopback development example.
	client, err := altqueue.Dial(ctx, "127.0.0.1:7331", altqueue.Options{Insecure: true, Token: os.Getenv("ALTQUEUE_TOKEN")})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	if _, err := client.CreateQueue(ctx, "examples", nil); err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatal(err)
	}
	ids, err := client.Publish(ctx, "examples", [][]byte{[]byte(`{"task":"email","idempotency_key":"example-1"}`)}, 0)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("published %v", ids)
}
