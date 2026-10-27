package queue

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestCreateQueueWithConfigRejectsNonPositiveCapacity(t *testing.T) {
	for _, tt := range []struct {
		name    string
		maxJobs int
	}{
		{name: "zero", maxJobs: 0},
		{name: "negative", maxJobs: -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			engine := newBatchTestEngine(t)
			before := snapshotBatchTestQueue(t, engine)

			err := engine.CreateQueueWithConfig("invalid", QueueConfig{MaxJobs: tt.maxJobs})
			if !errors.Is(err, ErrInvalidQueueConfig) {
				t.Fatalf("expected ErrInvalidQueueConfig, got %v", err)
			}
			if len(engine.queues) != 1 {
				t.Fatal("invalid configuration created a queue")
			}
			if after := snapshotBatchTestQueue(t, engine); !reflect.DeepEqual(before, after) {
				t.Fatal("invalid creation changed an existing queue")
			}
			if err := engine.CreateQueueWithConfig("invalid", QueueConfig{MaxJobs: 1}); err != nil {
				t.Fatalf("invalid creation reserved the queue name: %v", err)
			}
		})
	}
}

func TestCreateQueueWithConfigCopiesConfigAndRejectsDuplicates(t *testing.T) {
	engine := NewEngine(nil)
	config := QueueConfig{MaxJobs: 1}
	if err := engine.CreateQueueWithConfig("emails", config); err != nil {
		t.Fatal(err)
	}

	// Changing the caller's value must not increase the existing queue's capacity.
	config.MaxJobs = 10
	if _, err := engine.Publish("emails", []byte("existing")); err != nil {
		t.Fatal(err)
	}
	before := snapshotBatchTestQueue(t, engine)

	err := engine.CreateQueueWithConfig("emails", config)
	if !errors.Is(err, ErrQueueExists) {
		t.Fatalf("expected ErrQueueExists, got %v", err)
	}
	if after := snapshotBatchTestQueue(t, engine); !reflect.DeepEqual(before, after) {
		t.Fatal("duplicate creation changed an existing queue")
	}
	if engine.queues["emails"].config.MaxJobs != 1 {
		t.Fatal("caller mutation or duplicate creation changed the saved configuration")
	}
	if _, err := engine.Publish("emails", []byte("extra")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected original capacity to remain enforced, got %v", err)
	}
}

func TestCreateQueueUsesDefaultCapacity(t *testing.T) {
	engine := newBatchTestEngine(t)
	if got := engine.queues["emails"].config.MaxJobs; got != 100_000 {
		t.Fatalf("expected default capacity 100000, got %d", got)
	}
}

func TestQueueCapacityAcceptsExactLimitAndRejectsExtraJob(t *testing.T) {
	engine := newLimitedQueueTestEngine(t, 2)
	ids, err := engine.PublishBatch("emails", [][]byte{[]byte("one"), []byte("two")})
	if err != nil || len(ids) != 2 {
		t.Fatalf("expected 2 accepted jobs, got %d IDs, error %v", len(ids), err)
	}
	before := snapshotBatchTestQueue(t, engine)
	if len(before.jobs) != 2 || !reflect.DeepEqual(before.ready, ids) {
		t.Fatal("exact-capacity batch was not stored correctly")
	}

	id, err := engine.Publish("emails", []byte("three"))
	if !errors.Is(err, ErrQueueFull) || id != "" {
		t.Fatalf("expected no ID and ErrQueueFull, got ID %q, error %v", id, err)
	}
	if after := snapshotBatchTestQueue(t, engine); !reflect.DeepEqual(before, after) {
		t.Fatal("rejected publish changed a full queue")
	}
}

func TestQueueCapacityRejectsEntireBatchWhenOnlyOneSlotRemains(t *testing.T) {
	engine := newLimitedQueueTestEngine(t, 3)
	if _, err := engine.Publish("emails", []byte("leased")); err != nil {
		t.Fatal(err)
	}
	leased, err := engine.Fetch("emails", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish("emails", []byte("ready")); err != nil {
		t.Fatal(err)
	}
	before := snapshotBatchTestQueue(t, engine)

	ids, err := engine.PublishBatch("emails", [][]byte{[]byte("new one"), []byte("new two")})
	if !errors.Is(err, ErrQueueFull) || len(ids) != 0 {
		t.Fatalf("expected no IDs and ErrQueueFull, got IDs %v, error %v", ids, err)
	}
	if after := snapshotBatchTestQueue(t, engine); !reflect.DeepEqual(before, after) {
		t.Fatal("rejected batch changed ready or leased work")
	}

	// Failure must neither consume the remaining slot nor invalidate existing leases.
	if _, err := engine.Publish("emails", []byte("fits")); err != nil {
		t.Fatalf("rejected batch consumed the remaining slot: %v", err)
	}
	if err := engine.Ack("emails", leased.JobID, leased.LeaseToken); err != nil {
		t.Fatalf("rejected batch invalidated the existing lease: %v", err)
	}
}

func TestQueueCapacityCountsLeasedAndRetriedJobs(t *testing.T) {
	tests := []struct {
		name   string
		action func(t *testing.T, engine *Engine, now *time.Time, delivery Delivery)
	}{
		{name: "fetch retains capacity"},
		{
			name: "lease extension retains capacity",
			action: func(t *testing.T, engine *Engine, now *time.Time, delivery Delivery) {
				t.Helper()
				if _, err := engine.ExtendLease("emails", delivery.JobID, delivery.LeaseToken, 2*time.Minute); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "nack retains capacity",
			action: func(t *testing.T, engine *Engine, now *time.Time, delivery Delivery) {
				t.Helper()
				if err := engine.Nack("emails", delivery.JobID, delivery.LeaseToken); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "unprocessed expired lease retains capacity",
			action: func(t *testing.T, engine *Engine, now *time.Time, delivery Delivery) {
				*now = delivery.LeaseDeadline
			},
		},
		{
			name: "recovered expired lease retains capacity",
			action: func(t *testing.T, engine *Engine, now *time.Time, delivery Delivery) {
				t.Helper()
				*now = delivery.LeaseDeadline
				count, err := engine.RequeueExpired("emails")
				if err != nil || count != 1 {
					t.Fatalf("expected 1 recovered job, got %d, error %v", count, err)
				}
			},
		},
		{
			name: "invalid acknowledgement retains capacity",
			action: func(t *testing.T, engine *Engine, now *time.Time, delivery Delivery) {
				t.Helper()
				err := engine.Ack("emails", delivery.JobID, "wrong token")
				if !errors.Is(err, ErrInvalidLease) {
					t.Fatalf("expected ErrInvalidLease, got %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			engine := NewEngine(func() time.Time { return now })
			if err := engine.CreateQueueWithConfig("emails", QueueConfig{MaxJobs: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err := engine.Publish("emails", []byte("existing")); err != nil {
				t.Fatal(err)
			}
			delivery, err := engine.Fetch("emails", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if tt.action != nil {
				tt.action(t, engine, &now, delivery)
			}
			before := snapshotBatchTestQueue(t, engine)

			if _, err := engine.Publish("emails", []byte("extra")); !errors.Is(err, ErrQueueFull) {
				t.Fatalf("expected ErrQueueFull while original job remains unfinished, got %v", err)
			}
			if after := snapshotBatchTestQueue(t, engine); !reflect.DeepEqual(before, after) {
				t.Fatal("rejected publish changed existing work")
			}
		})
	}
}

func TestQueueCapacityAckFreesExactlyOneSlot(t *testing.T) {
	engine := newLimitedQueueTestEngine(t, 2)
	if _, err := engine.PublishBatch("emails", [][]byte{[]byte("one"), []byte("two")}); err != nil {
		t.Fatal(err)
	}
	delivery, err := engine.Fetch("emails", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Ack("emails", delivery.JobID, delivery.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish("emails", []byte("replacement")); err != nil {
		t.Fatalf("valid acknowledgement did not free capacity: %v", err)
	}

	// Repeating the acknowledgement must not free another slot.
	if err := engine.Ack("emails", delivery.JobID, delivery.LeaseToken); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("expected ErrInvalidLease on repeated acknowledgement, got %v", err)
	}
	if _, err := engine.Publish("emails", []byte("extra")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected exactly one slot to be freed, got %v", err)
	}
}

func TestQueueCapacityIsIndependentPerQueue(t *testing.T) {
	engine := newLimitedQueueTestEngine(t, 1)
	if err := engine.CreateQueueWithConfig("reports", QueueConfig{MaxJobs: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish("emails", []byte("email")); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish("emails", []byte("extra")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected emails queue to be full, got %v", err)
	}
	ids, err := engine.PublishBatch("reports", [][]byte{[]byte("one"), []byte("two")})
	if err != nil || len(ids) != 2 {
		t.Fatalf("full emails queue affected reports queue: IDs %v, error %v", ids, err)
	}
	if _, err := engine.Publish("reports", []byte("extra")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected reports queue to be full at its own limit, got %v", err)
	}

	delivery, err := engine.Fetch("emails", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Ack("emails", delivery.JobID, delivery.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish("emails", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish("reports", []byte("still full")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("freeing emails capacity affected reports capacity: %v", err)
	}
}

func TestConcurrentPublishersCannotExceedQueueCapacity(t *testing.T) {
	const capacity, workers = 12, 32
	engine := newLimitedQueueTestEngine(t, capacity)
	type result struct {
		ids       []string
		requested int
		err       error
	}
	start := make(chan struct{})
	results := make(chan result, workers)

	for i := 0; i < workers; i++ {
		go func(single bool) {
			<-start
			if single {
				id, err := engine.Publish("emails", []byte("single"))
				var ids []string
				if id != "" {
					ids = []string{id}
				}
				results <- result{ids: ids, requested: 1, err: err}
				return
			}
			ids, err := engine.PublishBatch("emails", [][]byte{[]byte("batch one"), []byte("batch two")})
			results <- result{ids: ids, requested: 2, err: err}
		}(i%2 == 0)
	}
	close(start)

	accepted := make(map[string]bool)
	for i := 0; i < workers; i++ {
		result := <-results
		if result.err != nil {
			if !errors.Is(result.err, ErrQueueFull) || len(result.ids) != 0 {
				t.Fatalf("unexpected rejected publish result: %+v", result)
			}
			continue
		}
		if len(result.ids) != result.requested {
			t.Fatalf("batch was only partially published: %+v", result)
		}
		for _, id := range result.ids {
			if id == "" || accepted[id] {
				t.Fatalf("empty or duplicate published ID: %q", id)
			}
			accepted[id] = true
		}
	}

	// There are more single-job requests than slots, so a final spare slot
	// cannot be left unused solely because two-job batches no longer fit.
	if len(accepted) != capacity {
		t.Fatalf("expected exactly %d accepted jobs, got %d", capacity, len(accepted))
	}
	state := snapshotBatchTestQueue(t, engine)
	if len(state.jobs) != capacity || len(state.ready) != capacity || len(state.leases) != 0 {
		t.Fatalf("unexpected queue state: jobs=%d ready=%d leases=%d", len(state.jobs), len(state.ready), len(state.leases))
	}
	for _, id := range state.ready {
		if !accepted[id] {
			t.Fatalf("stored job %q was not returned by a successful publish", id)
		}
		delete(accepted, id)
	}
	if len(accepted) != 0 {
		t.Fatal("successfully published IDs are missing from the ready queue")
	}
}

func newLimitedQueueTestEngine(t *testing.T, maxJobs int) *Engine {
	t.Helper()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	engine := NewEngine(func() time.Time { return now })
	if err := engine.CreateQueueWithConfig("emails", QueueConfig{MaxJobs: maxJobs}); err != nil {
		t.Fatal(err)
	}
	return engine
}
