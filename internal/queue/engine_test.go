package queue

import (
	"errors"
	"testing"
	"time"
)

func TestCreateQueue(t *testing.T) {
	engine := NewEngine(nil)

	err := engine.CreateQueue("emails")
	if err != nil {
		t.Fatalf("CreateQueue() returned an error: %v", err)
	}
}

func TestCreateQueueRejectsDuplicates(t *testing.T) {
	engine := NewEngine(nil)

	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatalf("first CreateQueue() failed; %v", err)
	}

	err := engine.CreateQueue("emails")

	if !errors.Is(err, ErrQueueExists) {
		t.Fatalf("expected ErrQueueExists, got %v", err)
	}
}

func TestPublishStoresJob(t *testing.T) {
	engine := NewEngine(nil)

	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatal(err)
	}
	jobID, err := engine.Publish("emails", []byte("hello"))
	if err != nil {
		t.Fatalf("Publish() failed: %v", err)
	}
	if jobID == "" {
		t.Fatalf("expected a non-empty job ID")
	}

	q := engine.queues["emails"]

	job, exists := q.jobs[jobID]

	if !exists || job == nil {
		t.Fatal("published job was not stored")
	}

	if string(job.Payload) != "hello" {
		t.Fatalf("unexpected payload: %q", job.Payload)
	}
	if job.Attempts != 0 {
		t.Fatalf("expected zero attempts, got %d", job.Attempts)
	}
	if len(q.ready) != 1 || q.ready[0] != jobID {
		t.Fatalf("unexpected ready queue: %v", q.ready)
	}
}

func TestPublishRejectsUnknownQueue(t *testing.T) {
	engine := NewEngine(nil)

	jobID, err := engine.Publish("missing", []byte("hello"))

	if !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got %v", err)
	}

	if jobID != "" {
		t.Fatalf("expected an empty job ID, got %q", jobID)
	}
}

func TestPublishCopiesPayload(t *testing.T) {
	engine := NewEngine(nil)

	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatal(err)
	}

	payload := []byte("hello")

	jobID, err := engine.Publish("emails", payload)
	if err != nil {
		t.Fatal(err)
	}

	payload[0] = 'X'
	stored := engine.queues["emails"].jobs[jobID]

	if string(stored.Payload) != "hello" {
		t.Fatalf("Stored payload changed: %q", stored.Payload)
	}
}

func TestFetchCreatesLease(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	engine := NewEngine(func() time.Time {
		return now
	})

	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatal(err)
	}

	jobID, err := engine.Publish("emails", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	delivery, err := engine.Fetch("emails", 30*time.Second)
	if err != nil {
		t.Fatalf("Fetch() failed: %v", err)
	}

	if delivery.JobID != jobID {
		t.Fatalf("expected job %q, got %q", jobID, delivery.JobID)
	}

	if string(delivery.Payload) != "hello" {
		t.Fatalf("unexpected payload: %q", delivery.Payload)
	}

	if delivery.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", delivery.Attempt)
	}

	if delivery.LeaseToken == "" {
		t.Fatal("expected a lease token")
	}

	expectedDeadline := now.Add(30 * time.Second)
	if !delivery.LeaseDeadline.Equal(expectedDeadline) {
		t.Fatalf(
			"expected deadline %v, got %v",
			expectedDeadline,
			delivery.LeaseDeadline,
		)
	}

	// The leased job must not be offered again.
	_, err = engine.Fetch("emails", 30*time.Second)
	if !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty, got %v", err)
	}

	// Fetch must retain the unfinished job.
	if _, exists := engine.queues["emails"].jobs[jobID]; !exists {
		t.Fatal("Fetch removed the stored job")
	}
}
func TestAckCompletesJob(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	engine := NewEngine(func() time.Time {
		return now
	})

	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatal(err)
	}

	jobID, err := engine.Publish("emails", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	delivery, err := engine.Fetch("emails", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	err = engine.Ack("emails", jobID, delivery.LeaseToken)
	if err != nil {
		t.Fatalf("Ack() failed: %v", err)
	}

	q := engine.queues["emails"]

	if _, exists := q.jobs[jobID]; exists {
		t.Fatal("completed job is still stored")
	}

	if _, exists := q.leases[jobID]; exists {
		t.Fatal("completed job still has a lease")
	}

	_, err = engine.Fetch("emails", 30*time.Second)
	if !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty, got %v", err)
	}
}

func TestExpiredLeaseCanBeRedelivered(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	engine := NewEngine(func() time.Time {
		return now
	})

	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatal(err)
	}

	jobID, err := engine.Publish("emails", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	first, err := engine.Fetch("emails", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Expiry includes the exact deadline.
	now = first.LeaseDeadline

	recovered, err := engine.RequeueExpired("emails")
	if err != nil {
		t.Fatal(err)
	}

	if recovered != 1 {
		t.Fatalf("expected 1 recovered job, got %d", recovered)
	}

	second, err := engine.Fetch("emails", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if second.JobID != jobID {
		t.Fatalf("expected job %q, got %q", jobID, second.JobID)
	}

	if second.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %d", second.Attempt)
	}

	if second.LeaseToken == first.LeaseToken {
		t.Fatal("redelivery reused the old lease token")
	}

	if string(second.Payload) != "hello" {
		t.Fatalf("unexpected payload: %q", second.Payload)
	}

	// The old worker cannot complete the new delivery.
	err = engine.Ack("emails", jobID, first.LeaseToken)
	if !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("expected ErrInvalidLease, got %v", err)
	}

	// The current worker still can.
	if err := engine.Ack("emails", jobID, second.LeaseToken); err != nil {
		t.Fatalf("current delivery could not acknowledge: %v", err)
	}
}
func TestExtendLeaseKeepsJobReserved(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	engine := NewEngine(func() time.Time {
		return now
	})

	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatal(err)
	}

	if _, err := engine.Publish("emails", []byte("hello")); err != nil {
		t.Fatal(err)
	}

	delivery, err := engine.Fetch("emails", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	now = now.Add(20 * time.Second)

	deadline, err := engine.ExtendLease(
		"emails",
		delivery.JobID,
		delivery.LeaseToken,
		30*time.Second,
	)
	if err != nil {
		t.Fatalf("ExtendLease() failed: %v", err)
	}

	expected := now.Add(30 * time.Second)
	if !deadline.Equal(expected) {
		t.Fatalf("expected deadline %v, got %v", expected, deadline)
	}

	// The original deadline must no longer expire this lease.
	now = delivery.LeaseDeadline

	recovered, err := engine.RequeueExpired("emails")
	if err != nil {
		t.Fatal(err)
	}

	if recovered != 0 {
		t.Fatalf("expected no recovered jobs, got %d", recovered)
	}

	// The same token remains valid past the original deadline.
	err = engine.Ack("emails", delivery.JobID, delivery.LeaseToken)
	if err != nil {
		t.Fatalf("Ack() after extension failed: %v", err)
	}
}

func TestNackMakesJobAvailableAgain(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	engine := NewEngine(func() time.Time {
		return now
	})

	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatal(err)
	}

	jobID, err := engine.Publish("emails", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	first, err := engine.Fetch("emails", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	err = engine.Nack("emails", jobID, first.LeaseToken)
	if err != nil {
		t.Fatalf("Nack() failed: %v", err)
	}

	// Nack itself must not consume another attempt.
	job := engine.queues["emails"].jobs[jobID]
	if job == nil {
		t.Fatal("Nack removed the job")
	}

	if job.Attempts != 1 {
		t.Fatalf("expected 1 attempt before refetch, got %d", job.Attempts)
	}

	second, err := engine.Fetch("emails", 30*time.Second)
	if err != nil {
		t.Fatalf("job was not available after Nack: %v", err)
	}

	if second.JobID != jobID {
		t.Fatalf("expected job %q, got %q", jobID, second.JobID)
	}

	if second.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %d", second.Attempt)
	}

	if second.LeaseToken == first.LeaseToken {
		t.Fatal("redelivery reused the old lease token")
	}

	if string(second.Payload) != "hello" {
		t.Fatalf("unexpected payload: %q", second.Payload)
	}

	// The previous worker cannot release the new delivery.
	err = engine.Nack("emails", jobID, first.LeaseToken)
	if !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("expected ErrInvalidLease, got %v", err)
	}

	// The valid delivery must remain usable.
	if err := engine.Ack("emails", jobID, second.LeaseToken); err != nil {
		t.Fatalf("current delivery could not acknowledge: %v", err)
	}
}

func TestConcurrentFetchLeasesJobOnce(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	engine := NewEngine(func() time.Time {
		return now
	})

	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatal(err)
	}

	jobID, err := engine.Publish("emails", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		delivery Delivery
		err      error
	}

	const workers = 32

	start := make(chan struct{})
	results := make(chan result, workers)

	for i := 0; i < workers; i++ {
		go func() {
			// Wait until all workers have been launched.
			<-start

			delivery, err := engine.Fetch("emails", time.Minute)

			results <- result{
				delivery: delivery,
				err:      err,
			}
		}()
	}

	// Release all workers.
	close(start)

	successes := 0

	for i := 0; i < workers; i++ {
		result := <-results

		switch {
		case result.err == nil:
			successes++

			if result.delivery.JobID != jobID {
				t.Fatalf(
					"expected job %q, got %q",
					jobID,
					result.delivery.JobID,
				)
			}

			if result.delivery.Attempt != 1 {
				t.Fatalf(
					"expected attempt 1, got %d",
					result.delivery.Attempt,
				)
			}

		case errors.Is(result.err, ErrQueueEmpty):
			// Expected for workers that didn't acquire the job.

		default:
			t.Fatalf("unexpected Fetch error: %v", result.err)
		}
	}

	if successes != 1 {
		t.Fatalf("expected exactly 1 successful fetch, got %d", successes)
	}
}
