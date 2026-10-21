package queue

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestFetchBatchPreservesOrderAndLeaseState(t *testing.T) {
	engine := newBatchTestEngine(t)
	payloads := [][]byte{[]byte("first"), []byte("second"), []byte("third"), []byte("fourth")}
	ids, err := engine.PublishBatch("emails", payloads)
	if err != nil {
		t.Fatal(err)
	}

	deliveries, err := engine.FetchBatch("emails", 3, time.Minute)
	if err != nil {
		t.Fatalf("FetchBatch() failed: %v", err)
	}
	if len(deliveries) != 3 {
		t.Fatalf("expected 3 deliveries, got %d", len(deliveries))
	}

	state := snapshotBatchTestQueue(t, engine)
	if len(state.jobs) != len(ids) || len(state.leases) != len(deliveries) {
		t.Fatalf("fetch must retain all jobs and lease only selected jobs: jobs=%d leases=%d", len(state.jobs), len(state.leases))
	}
	if !reflect.DeepEqual(state.ready, ids[3:]) {
		t.Fatalf("expected remaining ready IDs %v, got %v", ids[3:], state.ready)
	}

	seenTokens := make(map[string]bool)
	for i, delivery := range deliveries {
		if delivery.JobID != ids[i] || !bytes.Equal(delivery.Payload, payloads[i]) {
			t.Fatalf("delivery %d does not match the published job", i)
		}
		if delivery.Attempt != 1 || state.jobs[delivery.JobID].Attempts != 1 {
			t.Fatalf("job %q did not consume exactly one attempt", delivery.JobID)
		}
		if delivery.LeaseToken == "" || seenTokens[delivery.LeaseToken] {
			t.Fatal("expected distinct, nonempty lease tokens")
		}
		seenTokens[delivery.LeaseToken] = true

		lease, exists := state.leases[delivery.JobID]
		if !exists || lease.token != delivery.LeaseToken || !lease.deadline.Equal(delivery.LeaseDeadline) {
			t.Fatalf("delivery %q does not match its stored lease", delivery.JobID)
		}
	}
	if state.jobs[ids[3]].Attempts != 0 {
		t.Fatal("fetch changed the attempt count of an unselected job")
	}

	// Previously leased jobs must not be selected again.
	remainder, err := engine.FetchBatch("emails", 3, time.Minute)
	if err != nil || len(remainder) != 1 {
		t.Fatalf("expected one remaining delivery, got %d, error %v", len(remainder), err)
	}
	if remainder[0].JobID != ids[3] || remainder[0].Attempt != 1 {
		t.Fatalf("unexpected remaining delivery: %+v", remainder[0])
	}
	if _, err := engine.FetchBatch("emails", 3, time.Minute); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("expected ErrQueueEmpty, got %v", err)
	}

	// Each returned token must work independently with the existing Ack API.
	for _, delivery := range append(deliveries, remainder...) {
		if err := engine.Ack("emails", delivery.JobID, delivery.LeaseToken); err != nil {
			t.Fatalf("Ack() for %q failed: %v", delivery.JobID, err)
		}
	}
	if state := snapshotBatchTestQueue(t, engine); len(state.jobs) != 0 || len(state.leases) != 0 {
		t.Fatal("acknowledged batch jobs remain stored")
	}
}

func TestFetchBatchRespectsByteLimitWithoutSkipping(t *testing.T) {
	tests := []struct {
		name       string
		firstBytes int
	}{
		{name: "exact byte limit", firstBytes: maxBatchBytes},
		{name: "leave gap rather than skip large head job", firstBytes: maxBatchBytes - maxJobBytes/2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newBatchTestEngine(t)
			payloads := batchTestPayloadsWithTotal(tt.firstBytes)
			wantCount := len(payloads)
			// In the second case, the final tiny job could fit the gap, but FIFO
			// requires leaving it behind the next full-sized job.
			payloads = append(payloads, make([]byte, maxJobBytes), []byte("tail"))

			ids := make([]string, 0, len(payloads))
			for _, payload := range payloads {
				id, err := engine.Publish("emails", payload)
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}

			deliveries, err := engine.FetchBatch("emails", len(ids), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if len(deliveries) != wantCount {
				t.Fatalf("expected %d deliveries, got %d", wantCount, len(deliveries))
			}
			totalBytes := 0
			for i, delivery := range deliveries {
				if delivery.JobID != ids[i] || !bytes.Equal(delivery.Payload, payloads[i]) {
					t.Fatalf("delivery %d violated FIFO or changed payload", i)
				}
				totalBytes += len(delivery.Payload)
			}
			if totalBytes != tt.firstBytes || totalBytes > maxBatchBytes {
				t.Fatalf("expected %d payload bytes, got %d", tt.firstBytes, totalBytes)
			}

			state := snapshotBatchTestQueue(t, engine)
			if !reflect.DeepEqual(state.ready, ids[wantCount:]) {
				t.Fatalf("expected ready IDs %v, got %v", ids[wantCount:], state.ready)
			}
			for _, id := range ids[wantCount:] {
				if state.jobs[id].Attempts != 0 {
					t.Fatalf("unselected job %q consumed an attempt", id)
				}
				if _, exists := state.leases[id]; exists {
					t.Fatalf("unselected job %q was leased", id)
				}
			}

			remainder, err := engine.FetchBatch("emails", len(ids), time.Minute)
			if err != nil || len(remainder) != 2 {
				t.Fatalf("expected 2 remaining deliveries, got %d, error %v", len(remainder), err)
			}
			for i, delivery := range remainder {
				if delivery.JobID != ids[wantCount+i] {
					t.Fatal("remaining jobs were reordered")
				}
			}
		})
	}
}

func TestFetchBatchRejectsInvalidInputWithoutMutation(t *testing.T) {
	tests := []struct {
		name      string
		queueName string
		limit     int
		duration  time.Duration
		wantErr   error
	}{
		{name: "zero limit", limit: 0, duration: time.Minute, wantErr: ErrInvalidFetchLimit},
		{name: "negative limit", limit: -1, duration: time.Minute, wantErr: ErrInvalidFetchLimit},
		{name: "excessive limit", limit: maxBatchJobs + 1, duration: time.Minute, wantErr: ErrInvalidFetchLimit},
		{name: "zero duration", limit: 1, duration: 0, wantErr: ErrInvalidLeaseDuration},
		{name: "negative duration", limit: 1, duration: -time.Nanosecond, wantErr: ErrInvalidLeaseDuration},
		{name: "unknown queue", queueName: "missing", limit: 1, duration: time.Minute, wantErr: ErrQueueNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newBatchTestEngine(t)
			if _, err := engine.Publish("emails", []byte("leased")); err != nil {
				t.Fatal(err)
			}
			if _, err := engine.Fetch("emails", time.Minute); err != nil {
				t.Fatal(err)
			}
			if _, err := engine.Publish("emails", []byte("ready")); err != nil {
				t.Fatal(err)
			}
			before := snapshotBatchTestQueue(t, engine)
			queueName := tt.queueName
			if queueName == "" {
				queueName = "emails"
			}

			deliveries, err := engine.FetchBatch(queueName, tt.limit, tt.duration)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected %v, got %v", tt.wantErr, err)
			}
			if len(deliveries) != 0 {
				t.Fatalf("rejected fetch returned %d deliveries", len(deliveries))
			}
			if after := snapshotBatchTestQueue(t, engine); !reflect.DeepEqual(before, after) {
				t.Fatal("rejected fetch changed jobs, attempts, ready order, or leases")
			}
		})
	}
}

func TestFetchBatchEmptyQueue(t *testing.T) {
	engine := newBatchTestEngine(t)
	before := snapshotBatchTestQueue(t, engine)

	deliveries, err := engine.FetchBatch("emails", 1, time.Minute)
	if !errors.Is(err, ErrQueueEmpty) || len(deliveries) != 0 {
		t.Fatalf("expected no deliveries and ErrQueueEmpty, got %d deliveries, error %v", len(deliveries), err)
	}
	if after := snapshotBatchTestQueue(t, engine); !reflect.DeepEqual(before, after) {
		t.Fatal("empty fetch changed queue state")
	}
}

func TestFetchBatchCopiesPayloads(t *testing.T) {
	engine := newBatchTestEngine(t)
	shared := []byte("shared")
	ids, err := engine.PublishBatch("emails", [][]byte{shared, shared})
	if err != nil {
		t.Fatal(err)
	}

	deliveries, err := engine.FetchBatch("emails", 2, time.Minute)
	if err != nil || len(deliveries) != 2 {
		t.Fatalf("expected 2 deliveries, got %d, error %v", len(deliveries), err)
	}
	deliveries[0].Payload[0] = 'X'
	if string(deliveries[1].Payload) != "shared" {
		t.Fatal("deliveries share mutable payload storage")
	}
	deliveries[1].Payload[0] = 'Y'

	state := snapshotBatchTestQueue(t, engine)
	for _, id := range ids {
		if string(state.jobs[id].Payload) != "shared" {
			t.Fatalf("delivery mutation changed stored job %q", id)
		}
	}
}

func TestFetchBatchAcceptsBoundaryValues(t *testing.T) {
	tests := []struct {
		name     string
		payloads [][]byte
		limit    int
	}{
		{name: "maximum count", payloads: make([][]byte, maxBatchJobs), limit: maxBatchJobs},
		{name: "fewer ready than requested", payloads: [][]byte{[]byte("one"), []byte("two")}, limit: maxBatchJobs},
		{name: "nil and empty payloads", payloads: [][]byte{nil, {}, []byte("three")}, limit: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newBatchTestEngine(t)
			ids, err := engine.PublishBatch("emails", tt.payloads)
			if err != nil {
				t.Fatal(err)
			}
			deliveries, err := engine.FetchBatch("emails", tt.limit, time.Nanosecond)
			if err != nil {
				t.Fatalf("valid fetch was rejected: %v", err)
			}
			if len(deliveries) != len(ids) {
				t.Fatalf("expected %d deliveries, got %d", len(ids), len(deliveries))
			}
			for i, delivery := range deliveries {
				if delivery.JobID != ids[i] || delivery.Attempt != 1 || !bytes.Equal(delivery.Payload, tt.payloads[i]) {
					t.Fatalf("delivery %d does not match its published job", i)
				}
			}
		})
	}
}

func TestFetchBatchUsesSingleClockReading(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clockCalls := 0
	engine := NewEngine(func() time.Time {
		clockCalls++
		return now.Add(time.Duration(clockCalls) * time.Second)
	})
	if err := engine.CreateQueue("emails"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.PublishBatch("emails", [][]byte{[]byte("one"), []byte("two")}); err != nil {
		t.Fatal(err)
	}

	deliveries, err := engine.FetchBatch("emails", 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 2 || clockCalls != 1 {
		t.Fatalf("expected 2 deliveries using 1 clock reading, got %d deliveries and %d readings", len(deliveries), clockCalls)
	}
	wantDeadline := now.Add(time.Second + time.Minute)
	for _, delivery := range deliveries {
		if !delivery.LeaseDeadline.Equal(wantDeadline) {
			t.Fatalf("expected deadline %v, got %v", wantDeadline, delivery.LeaseDeadline)
		}
	}
}

func TestConcurrentFetchAndFetchBatchDoNotDuplicateJobs(t *testing.T) {
	engine := newBatchTestEngine(t)
	const jobCount, workers, batchLimit = 64, 32, 4
	payloads := make([][]byte, jobCount)
	for i := range payloads {
		payloads[i] = []byte{byte(i)}
	}
	ids, err := engine.PublishBatch("emails", payloads)
	if err != nil {
		t.Fatal(err)
	}
	knownIDs := make(map[string]bool, len(ids))
	for _, id := range ids {
		knownIDs[id] = true
	}

	type result struct {
		deliveries []Delivery
		limit      int
		err        error
	}
	start := make(chan struct{})
	results := make(chan result, workers)
	for i := 0; i < workers; i++ {
		go func(single bool) {
			<-start
			if single {
				delivery, err := engine.Fetch("emails", time.Minute)
				var deliveries []Delivery
				if err == nil {
					deliveries = []Delivery{delivery}
				}
				results <- result{deliveries: deliveries, limit: 1, err: err}
				return
			}
			deliveries, err := engine.FetchBatch("emails", batchLimit, time.Minute)
			results <- result{deliveries: deliveries, limit: batchLimit, err: err}
		}(i%2 == 0)
	}
	close(start)

	seenIDs := make(map[string]bool)
	seenTokens := make(map[string]bool)
	for i := 0; i < workers; i++ {
		result := <-results
		if result.err != nil {
			if !errors.Is(result.err, ErrQueueEmpty) || len(result.deliveries) != 0 {
				t.Fatalf("unexpected concurrent fetch result: %+v", result)
			}
			continue
		}
		if len(result.deliveries) == 0 || len(result.deliveries) > result.limit {
			t.Fatalf("fetch violated its count limit: got %d, limit %d", len(result.deliveries), result.limit)
		}
		for _, delivery := range result.deliveries {
			if !knownIDs[delivery.JobID] || seenIDs[delivery.JobID] {
				t.Fatalf("unknown or multiply leased job: %q", delivery.JobID)
			}
			if delivery.LeaseToken == "" || seenTokens[delivery.LeaseToken] {
				t.Fatal("empty or duplicate lease token")
			}
			if delivery.Attempt != 1 {
				t.Fatalf("expected attempt 1, got %d", delivery.Attempt)
			}
			seenIDs[delivery.JobID] = true
			seenTokens[delivery.LeaseToken] = true
		}
	}
	if len(seenIDs) != jobCount {
		t.Fatalf("expected all %d jobs to be leased, got %d", jobCount, len(seenIDs))
	}

	state := snapshotBatchTestQueue(t, engine)
	if len(state.jobs) != jobCount || len(state.leases) != jobCount || len(state.ready) != 0 {
		t.Fatalf("unexpected final state: jobs=%d leases=%d ready=%d", len(state.jobs), len(state.leases), len(state.ready))
	}
}
