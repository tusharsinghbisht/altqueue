package queue

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestPublishBatchPreservesOrder(t *testing.T) {
	engine := newBatchTestEngine(t)

	// A batch must append to existing ready work, not replace or overtake it.
	existingID, err := engine.Publish("emails", []byte("existing"))
	if err != nil {
		t.Fatal(err)
	}

	payloads := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	ids, err := engine.PublishBatch("emails", payloads)
	if err != nil {
		t.Fatalf("PublishBatch() failed: %v", err)
	}
	if len(ids) != len(payloads) {
		t.Fatalf("expected %d IDs, got %d", len(payloads), len(ids))
	}

	wantIDs := append([]string{existingID}, ids...)
	wantPayloads := append([][]byte{[]byte("existing")}, payloads...)
	beforeFetch := snapshotBatchTestQueue(t, engine)

	if !reflect.DeepEqual(beforeFetch.ready, wantIDs) {
		t.Fatalf("expected ready IDs %v, got %v", wantIDs, beforeFetch.ready)
	}
	if len(beforeFetch.jobs) != len(wantIDs) {
		t.Fatalf("expected %d stored jobs, got %d", len(wantIDs), len(beforeFetch.jobs))
	}
	if len(beforeFetch.leases) != 0 {
		t.Fatal("publishing unexpectedly created leases")
	}

	seen := make(map[string]bool, len(wantIDs))
	for i, id := range wantIDs {
		if id == "" || seen[id] {
			t.Fatalf("expected nonempty, distinct IDs; got %v", wantIDs)
		}
		seen[id] = true

		job, exists := beforeFetch.jobs[id]
		if !exists || job.ID != id || job.Attempts != 0 {
			t.Fatalf("job %q was not stored with its ID and zero attempts", id)
		}

		delivery, err := engine.Fetch("emails", time.Minute)
		if err != nil {
			t.Fatalf("Fetch() for job %d failed: %v", i, err)
		}
		if delivery.JobID != id {
			t.Fatalf("delivery %d: expected ID %q, got %q", i, id, delivery.JobID)
		}
		if !bytes.Equal(delivery.Payload, wantPayloads[i]) {
			t.Fatalf("delivery %d: expected payload %q, got %q", i, wantPayloads[i], delivery.Payload)
		}
		if delivery.Attempt != 1 {
			t.Fatalf("delivery %d: expected attempt 1, got %d", i, delivery.Attempt)
		}
	}

	if _, err := engine.Fetch("emails", time.Minute); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty after fetching all jobs, got %v", err)
	}
}

func TestPublishBatchCopiesPayloads(t *testing.T) {
	engine := newBatchTestEngine(t)
	want := [][]byte{[]byte("first"), []byte("second")}
	payloads := [][]byte{bytes.Clone(want[0]), bytes.Clone(want[1])}

	ids, err := engine.PublishBatch("emails", payloads)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != len(want) {
		t.Fatalf("expected %d IDs, got %d", len(want), len(ids))
	}

	// Neither modifying input bytes nor replacing an input slice may change jobs.
	for _, payload := range payloads {
		payload[0] = 'X'
	}
	payloads[0] = []byte("replacement")

	for i, id := range ids {
		delivery, err := engine.Fetch("emails", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if delivery.JobID != id || !bytes.Equal(delivery.Payload, want[i]) {
			t.Fatalf("delivery %d changed after input mutation: %+v", i, delivery)
		}

		// A returned delivery must not expose the stored payload either.
		delivery.Payload[0] = 'Y'
		stored := snapshotBatchTestQueue(t, engine).jobs[id]
		if !bytes.Equal(stored.Payload, want[i]) {
			t.Fatalf("job %q changed after delivery mutation: %q", id, stored.Payload)
		}
	}
}

func TestPublishBatchRejectsInvalidInputWithoutMutation(t *testing.T) {
	tests := []struct {
		name      string
		queueName string
		payloads  func() [][]byte
		wantErr   error
	}{
		{
			name:      "unknown queue",
			queueName: "missing",
			payloads:  func() [][]byte { return [][]byte{[]byte("valid")} },
			wantErr:   ErrQueueNotFound,
		},
		{
			name:     "nil batch",
			payloads: func() [][]byte { return nil },
			wantErr:  ErrEmptyBatch,
		},
		{
			name:     "empty batch",
			payloads: func() [][]byte { return [][]byte{} },
			wantErr:  ErrEmptyBatch,
		},
		{
			name:     "too many jobs",
			payloads: func() [][]byte { return make([][]byte, maxBatchJobs+1) },
			wantErr:  ErrBatchTooLarge,
		},
		{
			name: "oversized payload after valid payload",
			payloads: func() [][]byte {
				return [][]byte{[]byte("valid"), make([]byte, maxJobBytes+1)}
			},
			wantErr: ErrPayloadTooLarge,
		},
		{
			name: "combined bytes exceed limit by one",
			payloads: func() [][]byte {
				return batchTestPayloadsWithTotal(maxBatchBytes + 1)
			},
			wantErr: ErrBatchTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newBatchTestEngine(t)

			// Preserve both leased and ready work across every rejected request.
			leasedID, err := engine.Publish("emails", []byte("already leased"))
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := engine.Fetch("emails", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := engine.Publish("emails", []byte("already ready")); err != nil {
				t.Fatal(err)
			}

			before := snapshotBatchTestQueue(t, engine)
			queueName := tt.queueName
			if queueName == "" {
				queueName = "emails"
			}

			ids, err := engine.PublishBatch(queueName, tt.payloads())
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected %v, got %v", tt.wantErr, err)
			}
			if len(ids) != 0 {
				t.Fatalf("rejected batch returned job IDs: %v", ids)
			}

			after := snapshotBatchTestQueue(t, engine)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected batch changed queue state:\nbefore: %+v\nafter: %+v", before, after)
			}

			engine.mu.Lock()
			queueCount := len(engine.queues)
			engine.mu.Unlock()
			if queueCount != 1 {
				t.Fatalf("rejected batch changed queue count: got %d", queueCount)
			}

			// Existing lease ownership must remain usable, not merely look intact.
			if err := engine.Ack("emails", leasedID, delivery.LeaseToken); err != nil {
				t.Fatalf("rejected batch invalidated an existing lease: %v", err)
			}
		})
	}
}

func TestPublishBatchAcceptsBoundaryValues(t *testing.T) {
	tests := []struct {
		name     string
		payloads func() [][]byte
	}{
		{
			name: "exact job count limit",
			payloads: func() [][]byte {
				return make([][]byte, maxBatchJobs)
			},
		},
		{
			name: "exact individual payload limit",
			payloads: func() [][]byte {
				return [][]byte{bytes.Repeat([]byte("x"), maxJobBytes)}
			},
		},
		{
			name: "exact combined byte limit",
			payloads: func() [][]byte {
				return batchTestPayloadsWithTotal(maxBatchBytes)
			},
		},
		{
			name: "nil and empty individual payloads",
			payloads: func() [][]byte {
				return [][]byte{nil, {}, []byte("nonempty")}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newBatchTestEngine(t)
			payloads := tt.payloads()

			ids, err := engine.PublishBatch("emails", payloads)
			if err != nil {
				t.Fatalf("valid boundary batch was rejected: %v", err)
			}
			if len(ids) != len(payloads) {
				t.Fatalf("expected %d IDs, got %d", len(payloads), len(ids))
			}

			seen := make(map[string]bool, len(ids))
			for i, id := range ids {
				if id == "" || seen[id] {
					t.Fatalf("job %d has an empty or duplicate ID: %q", i, id)
				}
				seen[id] = true

				delivery, err := engine.Fetch("emails", time.Minute)
				if err != nil {
					t.Fatalf("Fetch() for job %d failed: %v", i, err)
				}
				if delivery.JobID != id || !bytes.Equal(delivery.Payload, payloads[i]) {
					t.Fatalf("job %d: fetched ID or payload does not match input", i)
				}
			}

			if _, err := engine.Fetch("emails", time.Minute); !errors.Is(err, ErrQueueEmpty) {
				t.Fatalf("expected ErrQueueEmpty after batch, got %v", err)
			}
		})
	}
}

func TestPublishRejectsOversizedPayload(t *testing.T) {
	engine := newBatchTestEngine(t)
	before := snapshotBatchTestQueue(t, engine)

	id, err := engine.Publish("emails", make([]byte, maxJobBytes+1))
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
	if id != "" {
		t.Fatalf("rejected publish returned an ID: %q", id)
	}
	if after := snapshotBatchTestQueue(t, engine); !reflect.DeepEqual(before, after) {
		t.Fatal("rejected single publish changed queue state")
	}
}

func newBatchTestEngine(t *testing.T) *Engine {
	t.Helper()

	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	engine := NewEngine(func() time.Time { return now })
	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatal(err)
	}
	return engine
}

// Split the total into individually valid payloads to isolate the batch byte limit.
func batchTestPayloadsWithTotal(total int) [][]byte {
	var payloads [][]byte
	for total > 0 {
		size := min(total, maxJobBytes)
		payloads = append(payloads, bytes.Repeat([]byte("x"), size))
		total -= size
	}
	return payloads
}

type batchTestQueueSnapshot struct {
	jobs   map[string]Job
	ready  []string
	leases map[string]jobLease
}

// Deep-copy payloads so an accidental in-place mutation cannot alter the snapshot.
func snapshotBatchTestQueue(t *testing.T, engine *Engine) batchTestQueueSnapshot {
	t.Helper()
	engine.mu.Lock()
	defer engine.mu.Unlock()

	q, exists := engine.queues["emails"]
	if !exists {
		t.Fatal("expected emails queue to exist")
	}

	snapshot := batchTestQueueSnapshot{
		jobs:   make(map[string]Job, len(q.jobs)),
		ready:  append([]string(nil), q.ready...),
		leases: make(map[string]jobLease, len(q.leases)),
	}
	for id, job := range q.jobs {
		if job == nil {
			t.Fatalf("job %q has a nil record", id)
		}
		jobCopy := *job
		jobCopy.Payload = bytes.Clone(job.Payload)
		snapshot.jobs[id] = jobCopy
	}
	for id, lease := range q.leases {
		snapshot.leases[id] = lease
	}
	return snapshot
}
