package queue

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// One lifecycle test covers durable delays, retries, dead-letter retention,
// manual replay, backup restore and queue deletion rather than duplicating each API test.
func TestDurableLifecycleAndCheckpoint(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "db")
	e, err := Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if err := e.CreateQueueWithConfig("jobs", QueueConfig{MaxJobs: 3, MaxPayloadBytes: 64, MaxInFlight: 1, MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	ids, err := e.PublishBatch("jobs", [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	ds, err := e.FetchBatch("jobs", 2, time.Second)
	if err != nil || len(ds) != 1 {
		t.Fatalf("in-flight limit: %v %v", ds, err)
	}
	if _, err := e.Fetch("jobs", time.Second); !errors.Is(err, ErrInFlightLimit) {
		t.Fatalf("expected in-flight limit, got %v", err)
	}
	if err := e.Ack("jobs", ds[0].JobID, ds[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		d, err := e.Fetch("jobs", time.Second)
		if err != nil || d.Attempt != attempt {
			t.Fatalf("retry %d: %+v %v", attempt, d, err)
		}
		if err := e.Nack("jobs", d.JobID, d.LeaseToken); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.PublishDelayed("jobs", [][]byte{[]byte("c")}, time.Second); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "checkpoint")
	if err := e.Checkpoint(backup); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	info, err := e.GetQueue("jobs")
	if err != nil || info.Dead != 1 || info.Delayed != 1 || info.PayloadBytes != 2 {
		t.Fatalf("recovered state: %+v %v", info, err)
	}
	dead, err := e.ListDead("jobs", "", 10)
	if err != nil || len(dead) != 1 || dead[0].ID != ids[1] || string(dead[0].Payload) != "b" {
		t.Fatalf("dead jobs: %+v %v", dead, err)
	}
	if err := e.RequeueDead("jobs", ids[1]); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if n, err := e.RequeueExpired("jobs"); err != nil || n != 1 {
		t.Fatalf("due jobs: %d %v", n, err)
	}
	for _, want := range []string{"b", "c"} {
		d, err := e.Fetch("jobs", time.Second)
		if err != nil || string(d.Payload) != want || d.Attempt != 1 {
			t.Fatalf("replayed delivery: %+v %v", d, err)
		}
		if err := e.Ack("jobs", d.JobID, d.LeaseToken); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.DeleteQueue("jobs"); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if qs, err := e.ListQueues("", 10); err != nil || len(qs) != 0 {
		t.Fatalf("deleted queue recovered: %+v %v", qs, err)
	}
	restored, err := Open(backup, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	info, err = restored.GetQueue("jobs")
	if err != nil || info.Jobs != 2 || info.Dead != 1 || info.Delayed != 1 {
		t.Fatalf("backup changed with live database: %+v %v", info, err)
	}
}

func TestStorageFailureFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	e, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.CreateQueue("jobs"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Publish("jobs", []byte("kept")); err != nil {
		t.Fatal(err)
	}
	e.store.commit = func(*pebble.Batch) error { return errors.New("injected disk failure") }
	if _, err := e.Publish("jobs", []byte("not committed")); !errors.Is(err, ErrStorage) {
		t.Fatalf("expected storage error, got %v", err)
	}
	if e.Ready() {
		t.Fatal("faulted engine remained ready")
	}
	if _, err := e.Fetch("jobs", time.Second); !errors.Is(err, ErrStorage) {
		t.Fatalf("faulted engine served work: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	d, err := recovered.Fetch("jobs", time.Second)
	if err != nil || string(d.Payload) != "kept" {
		t.Fatalf("committed work lost: %+v %v", d, err)
	}
	if _, err := recovered.Fetch("jobs", time.Second); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("uncommitted job appeared: %v", err)
	}
}

func TestResourceAccounting(t *testing.T) {
	e := NewEngine(nil)
	defer e.Close()
	if err := e.CreateQueueWithConfig("jobs", QueueConfig{MaxJobs: 3, MaxPayloadBytes: 3, MaxInFlight: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.PublishBatch("jobs", [][]byte{[]byte("ab"), []byte("cd")}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("oversized batch accepted: %v", err)
	}
	if info, _ := e.GetQueue("jobs"); info.Jobs != 0 || info.PayloadBytes != 0 {
		t.Fatal("rejected batch changed accounting")
	}
	if _, err := e.PublishBatch("jobs", [][]byte{[]byte("ab"), []byte("c")}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Publish("jobs", []byte("x")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("byte cap not enforced: %v", err)
	}
	ds, err := e.FetchBatch("jobs", 2, time.Second)
	if err != nil || len(ds) != 1 {
		t.Fatalf("in-flight cap: %v %v", ds, err)
	}
	if err := e.Ack("jobs", ds[0].JobID, ds[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Publish("jobs", []byte("de")); err != nil {
		t.Fatalf("ack did not free bytes: %v", err)
	}
	if info, _ := e.GetQueue("jobs"); info.PayloadBytes != 3 || info.Jobs != 2 {
		t.Fatalf("incorrect accounting: %+v", info)
	}
}

// Exit the child without closing Pebble: this exercises WAL recovery, not just
// graceful Close/Open. It is not a simulation of hardware power-loss behavior.
func TestAbruptProcessRecovery(t *testing.T) {
	const env = "ALTQUEUE_CRASH_TEST_DIR"
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if path := os.Getenv(env); path != "" {
		e, err := Open(path, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		if err := e.CreateQueue("jobs"); err != nil {
			t.Fatal(err)
		}
		if _, err := e.PublishBatch("jobs", [][]byte{[]byte("completed"), []byte("unfinished")}); err != nil {
			t.Fatal(err)
		}
		d, err := e.Fetch("jobs", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Ack("jobs", d.JobID, d.LeaseToken); err != nil {
			t.Fatal(err)
		}
		if _, err := e.Fetch("jobs", time.Second); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	path := filepath.Join(t.TempDir(), "db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestAbruptProcessRecovery$")
	cmd.Env = append(os.Environ(), env+"="+path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child failed: %v\n%s", err, output)
	}
	now = now.Add(time.Second)
	e, err := Open(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if n, err := e.RequeueExpired("jobs"); err != nil || n != 1 {
		t.Fatalf("lease recovery: %d %v", n, err)
	}
	d, err := e.Fetch("jobs", time.Second)
	if err != nil || string(d.Payload) != "unfinished" || d.Attempt != 2 {
		t.Fatalf("incorrect recovered delivery: %+v %v", d, err)
	}
	if _, err := e.Fetch("jobs", time.Second); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("completed job reappeared: %v", err)
	}
}
