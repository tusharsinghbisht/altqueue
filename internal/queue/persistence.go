package queue

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

type diskStore struct {
	db     *pebble.DB
	commit func(*pebble.Batch) error
}

func queueKey(name string) string {
	return "queue/" + base64.RawURLEncoding.EncodeToString([]byte(name))
}
func jobKey(name, id string) string {
	return "job/" + base64.RawURLEncoding.EncodeToString([]byte(name)) + "/" + id
}
func payloadKey(name, id string) string {
	return "payload/" + base64.RawURLEncoding.EncodeToString([]byte(name)) + "/" + id
}

// Open recovers a locally durable engine. Payloads remain resident in memory and
// are subject to per-queue and global admission limits. The data directory is exclusive.
func Open(path string, now func() time.Time) (*Engine, error) {
	if path == "" {
		return nil, ErrInvalidArgument
	}
	cache := pebble.NewCache(32 << 20)
	defer cache.Unref()
	db, err := pebble.Open(path, &pebble.Options{Cache: cache, MemTableSize: 8 << 20, MaxConcurrentCompactions: func() int { return 1 }})
	if err != nil {
		return nil, err
	}
	e := NewEngine(now)
	e.store = &diskStore{db: db, commit: func(batch *pebble.Batch) error { return batch.Commit(pebble.Sync) }}
	if err := e.recover(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("recover queue: %w", err)
	}
	return e, nil
}
func (e *Engine) recover() error {
	db := e.store.db
	value, closer, err := db.Get([]byte("meta/schema"))
	if errors.Is(err, pebble.ErrNotFound) {
		iter, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		nonempty := iter.First()
		iterErr := iter.Error()
		_ = iter.Close()
		if iterErr != nil {
			return iterErr
		}
		if nonempty {
			return errors.New("missing schema in nonempty database")
		}
		batch := db.NewBatch()
		defer batch.Close()
		if err := batch.Set([]byte("meta/schema"), []byte("1"), nil); err != nil {
			return err
		}
		if err := batch.Set([]byte("meta/sequence"), []byte("0"), nil); err != nil {
			return err
		}
		return batch.Commit(pebble.Sync)
	}
	if err != nil {
		return err
	}
	supported := bytes.Equal(value, []byte("1"))
	_ = closer.Close()
	if !supported {
		return errors.New("unsupported storage schema")
	}
	value, closer, err = db.Get([]byte("meta/sequence"))
	if err != nil {
		return err
	}
	e.seq, err = strconv.ParseUint(string(value), 10, 64)
	_ = closer.Close()
	if err != nil {
		return err
	}
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("queue/"), UpperBound: []byte("queue0")})
	if err != nil {
		return err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		nameBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(string(iter.Key()), "queue/"))
		if err != nil {
			_ = iter.Close()
			return err
		}
		name := string(nameBytes)
		var config QueueConfig
		if err := json.Unmarshal(iter.Value(), &config); err != nil {
			_ = iter.Close()
			return err
		}
		config, err = normalizeConfig(config)
		if err != nil || !queueNamePattern.MatchString(name) || len(e.queues) >= maxQueues {
			_ = iter.Close()
			return errors.New("invalid persisted queue configuration")
		}
		e.queues[name] = newQueue(config)
	}
	err = iter.Error()
	_ = iter.Close()
	if err != nil {
		return err
	}
	iter, err = db.NewIter(&pebble.IterOptions{LowerBound: []byte("job/"), UpperBound: []byte("job0")})
	if err != nil {
		return err
	}
	defer iter.Close()
	sequences := map[uint64]bool{}
	for iter.First(); iter.Valid(); iter.Next() {
		parts := strings.Split(string(iter.Key()), "/")
		if len(parts) != 3 {
			return errors.New("invalid job key")
		}
		nameBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return err
		}
		q := e.queues[string(nameBytes)]
		var job Job
		if err := json.Unmarshal(iter.Value(), &job); err != nil {
			return err
		}
		payload, payloadCloser, err := db.Get([]byte(payloadKey(string(nameBytes), parts[2])))
		if err != nil {
			return fmt.Errorf("missing payload: %w", err)
		}
		if len(payload) > maxJobBytes {
			_ = payloadCloser.Close()
			return ErrPayloadTooLarge
		}
		job.Payload = append([]byte(nil), payload...)
		_ = payloadCloser.Close()
		if q == nil || job.ID != parts[2] || job.ID == "" || len(job.Payload) > maxJobBytes || job.Attempts < 0 || job.MaxAttempts <= 0 || job.MaxAttempts > 1000 || job.Attempts > job.MaxAttempts || job.RetryBase < 0 || job.RetryMax < job.RetryBase || job.RetryMax > 24*time.Hour || job.Sequence == 0 || job.Sequence > e.seq || sequences[job.Sequence] {
			return errors.New("invalid persisted job")
		}
		switch job.State {
		case "ready", "dead":
		case "leased":
			if job.LeaseToken == "" || job.LeaseDeadline.IsZero() {
				return errors.New("invalid persisted lease")
			}
		case "delayed":
			if job.AvailableAt.IsZero() {
				return errors.New("invalid delayed job")
			}
		default:
			return errors.New("unknown job state")
		}
		sequences[job.Sequence] = true
		e.applyLocked(q, []*Job{&job}, nil)
		if len(q.jobs) > q.config.MaxJobs || q.payloadBytes > q.config.MaxPayloadBytes || len(q.leases) > q.config.MaxInFlight || e.totalJobs > maxTotalJobs || e.totalBytes > maxTotalPayloadBytes {
			return errors.New("recovered data exceeds resource limits")
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	for _, q := range e.queues {
		sort.Slice(q.ready, func(i, j int) bool { return q.jobs[q.ready[i]].Sequence < q.jobs[q.ready[j]].Sequence })
	}
	return nil
}
func (e *Engine) persistLocked(name string, config *QueueConfig, jobs []*Job, deletes []string, deleteQueue bool, sequence uint64) error {
	if e.store == nil {
		return nil
	}
	batch := e.store.db.NewBatch()
	defer batch.Close()
	setJSON := func(key string, v any) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return batch.Set([]byte(key), data, nil)
	}
	if config != nil {
		if err := setJSON(queueKey(name), config); err != nil {
			return e.storageFailure(err)
		}
	}
	for _, job := range jobs {
		metadata := *job
		metadata.Payload = nil
		if err := setJSON(jobKey(name, job.ID), &metadata); err != nil {
			return e.storageFailure(err)
		}
		// Payloads are immutable: write once, not on every fetch or heartbeat.
		q := e.queues[name]
		if q == nil || q.jobs[job.ID] == nil {
			if err := batch.Set([]byte(payloadKey(name, job.ID)), job.Payload, nil); err != nil {
				return e.storageFailure(err)
			}
		}
	}
	for _, id := range deletes {
		if err := batch.Delete([]byte(jobKey(name, id)), nil); err != nil {
			return e.storageFailure(err)
		}
		if err := batch.Delete([]byte(payloadKey(name, id)), nil); err != nil {
			return e.storageFailure(err)
		}
	}
	if deleteQueue {
		if err := batch.Delete([]byte(queueKey(name)), nil); err != nil {
			return e.storageFailure(err)
		}
	}
	if err := batch.Set([]byte("meta/sequence"), []byte(strconv.FormatUint(sequence, 10)), nil); err != nil {
		return e.storageFailure(err)
	}
	if err := e.store.commit(batch); err != nil {
		return e.storageFailure(err)
	}
	return nil
}

// Checkpoint creates a consistent local backup in a destination that must not exist.
func (e *Engine) Checkpoint(path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return err
	}
	if e.store == nil {
		return ErrInvalidArgument
	}
	return e.store.db.Checkpoint(path, pebble.WithFlushedWAL())
}
