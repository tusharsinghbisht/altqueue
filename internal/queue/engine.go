package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"regexp"
	"sort"
	"sync"
	"time"
)

const (
	maxBatchJobs         = 120
	maxJobBytes          = 1 << 20
	maxBatchBytes        = 4 << 20
	defaultMaxQueueJobs  = 100_000
	maxQueues            = 128
	maxTotalJobs         = 1_000_000
	maxTotalPayloadBytes = 1 << 30
)

var queueNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

type queueState struct {
	config       QueueConfig
	jobs         map[string]*Job
	ready        []string
	leases       map[string]jobLease
	delayed      map[string]time.Time
	dead         map[string]bool
	payloadBytes int64
	deadlines    deadlineQueue
	notify       chan struct{}
}
type jobLease struct {
	token    string
	deadline time.Time
}
type Engine struct {
	mu         sync.Mutex
	queues     map[string]*queueState
	now        func() time.Time
	store      *diskStore
	seq        uint64
	totalJobs  int
	totalBytes int64
	closed     bool
	fault      error
}

func NewEngine(now func() time.Time) *Engine {
	if now == nil {
		now = time.Now
	}
	return &Engine{queues: make(map[string]*queueState), now: now}
}
func newQueue(config QueueConfig) *queueState {
	return &queueState{config: config, jobs: make(map[string]*Job), leases: make(map[string]jobLease), delayed: make(map[string]time.Time), dead: make(map[string]bool), notify: make(chan struct{})}
}
func normalizeConfig(c QueueConfig) (QueueConfig, error) {
	d := DefaultQueueConfig()
	if c.MaxJobs <= 0 || c.MaxPayloadBytes < 0 || c.MaxInFlight < 0 || c.MaxAttempts < 0 || c.RetryBase < 0 || c.RetryMax < 0 {
		return c, ErrInvalidQueueConfig
	}
	if c.MaxPayloadBytes == 0 {
		c.MaxPayloadBytes = d.MaxPayloadBytes
	}
	if c.MaxInFlight == 0 {
		c.MaxInFlight = min(d.MaxInFlight, c.MaxJobs)
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = d.MaxAttempts
	}
	if c.RetryMax == 0 {
		c.RetryMax = d.RetryMax
	}
	if c.MaxAttempts > 1000 || c.RetryMax > 24*time.Hour || c.RetryBase > c.RetryMax {
		return c, ErrInvalidQueueConfig
	}
	return c, nil
}
func (e *Engine) statusLocked() error {
	if e.closed {
		return ErrClosed
	}
	return e.fault
}
func (e *Engine) Ready() bool        { e.mu.Lock(); defer e.mu.Unlock(); return e.statusLocked() == nil }
func (e *Engine) wake(q *queueState) { close(q.notify); q.notify = make(chan struct{}) }
func (e *Engine) nextSequence(seq uint64) (uint64, error) {
	if seq == math.MaxUint64 {
		return seq, ErrQueueFull
	}
	return seq + 1, nil
}
func randomID() string { var b [16]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }

func (e *Engine) CreateQueue(name string) error {
	return e.CreateQueueWithConfig(name, DefaultQueueConfig())
}
func (e *Engine) CreateQueueWithConfig(name string, config QueueConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return err
	}
	if !queueNamePattern.MatchString(name) {
		return ErrInvalidQueueName
	}
	config, err := normalizeConfig(config)
	if err != nil {
		return err
	}
	if _, ok := e.queues[name]; ok {
		return ErrQueueExists
	}
	if len(e.queues) >= maxQueues {
		return ErrQueueFull
	}
	if err := e.persistLocked(name, &config, nil, nil, false, e.seq); err != nil {
		return err
	}
	e.queues[name] = newQueue(config)
	return nil
}
func (e *Engine) Publish(name string, payload []byte) (string, error) {
	ids, err := e.PublishBatch(name, [][]byte{payload})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}
func (e *Engine) PublishBatch(name string, payloads [][]byte) ([]string, error) {
	return e.PublishDelayed(name, payloads, 0)
}
func (e *Engine) PublishDelayed(name string, payloads [][]byte, delay time.Duration) ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return nil, err
	}
	q, ok := e.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	if delay < 0 || delay > 365*24*time.Hour {
		return nil, ErrInvalidArgument
	}
	if len(payloads) == 0 {
		return nil, ErrEmptyBatch
	}
	if len(payloads) > maxBatchJobs {
		return nil, ErrBatchTooLarge
	}
	size := 0
	for _, p := range payloads {
		if len(p) > maxJobBytes {
			return nil, ErrPayloadTooLarge
		}
		size += len(p)
	}
	if size > maxBatchBytes {
		return nil, ErrBatchTooLarge
	}
	if len(payloads) > q.config.MaxJobs-len(q.jobs) || len(payloads) > maxTotalJobs-e.totalJobs || int64(size) > q.config.MaxPayloadBytes-q.payloadBytes || int64(size) > maxTotalPayloadBytes-e.totalBytes {
		return nil, ErrQueueFull
	}
	now, seq := e.now(), e.seq
	jobs := make([]*Job, 0, len(payloads))
	ids := make([]string, 0, len(payloads))
	for _, p := range payloads {
		var err error
		seq, err = e.nextSequence(seq)
		if err != nil {
			return nil, err
		}
		payload := append([]byte(nil), p...)
		job := &Job{ID: randomID(), Payload: payload, State: "ready", CreatedAt: now, Sequence: seq, MaxAttempts: q.config.MaxAttempts, RetryBase: q.config.RetryBase, RetryMax: q.config.RetryMax}
		if delay > 0 {
			job.State = "delayed"
			job.AvailableAt = now.Add(delay)
		}
		jobs = append(jobs, job)
		ids = append(ids, job.ID)
	}
	if err := e.persistLocked(name, nil, jobs, nil, false, seq); err != nil {
		return nil, err
	}
	e.seq = seq
	e.applyLocked(q, jobs, nil)
	return ids, nil
}
func (e *Engine) Fetch(name string, duration time.Duration) (Delivery, error) {
	ds, err := e.FetchBatch(name, 1, duration)
	if err != nil {
		return Delivery{}, err
	}
	return ds[0], nil
}

// FetchBatch leases a FIFO prefix bounded by count, payload bytes and in-flight capacity.
func (e *Engine) FetchBatch(name string, limit int, duration time.Duration) ([]Delivery, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > maxBatchJobs {
		return nil, ErrInvalidFetchLimit
	}
	if duration <= 0 || duration > 24*time.Hour {
		return nil, ErrInvalidLeaseDuration
	}
	q, ok := e.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	if len(q.ready) == 0 {
		return nil, ErrQueueEmpty
	}
	available := q.config.MaxInFlight - len(q.leases)
	if available <= 0 {
		return nil, ErrInFlightLimit
	}
	jobs := make([]*Job, 0, min(limit, len(q.ready), available))
	ds := make([]Delivery, 0, cap(jobs))
	size := 0
	for _, id := range q.ready[:min(limit, len(q.ready), available)] {
		old := q.jobs[id]
		if len(old.Payload) > maxBatchBytes-size {
			break
		}
		size += len(old.Payload)
		job := *old
		job.State = "leased"
		job.Attempts++
		job.LeaseToken = randomID()
		jobs = append(jobs, &job)
		ds = append(ds, Delivery{JobID: id, Payload: append([]byte(nil), job.Payload...), Attempt: job.Attempts, LeaseToken: job.LeaseToken})
	}
	deadline := e.now().Add(duration)
	for i, job := range jobs {
		job.LeaseDeadline = deadline
		ds[i].LeaseDeadline = deadline
	}
	if err := e.persistLocked(name, nil, jobs, nil, false, e.seq); err != nil {
		return nil, err
	}
	e.applyLocked(q, jobs, nil)
	clear(q.ready[:len(jobs)])
	q.ready = q.ready[len(jobs):]
	return ds, nil
}

// FetchWait captures the notification channel before checking availability to avoid lost wakeups.
func (e *Engine) FetchWait(ctx context.Context, name string, limit int, lease, wait time.Duration) ([]Delivery, error) {
	if wait < 0 || wait > 30*time.Second {
		return nil, ErrInvalidArgument
	}
	if wait == 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return e.FetchBatch(name, limit, lease)
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		e.mu.Lock()
		err := e.statusLocked()
		q := e.queues[name]
		var ch <-chan struct{}
		if q != nil {
			ch = q.notify
		}
		e.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if q == nil {
			return nil, ErrQueueNotFound
		}
		ds, err := e.FetchBatch(name, limit, lease)
		if !errorsIsEmpty(err) {
			return ds, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}
func errorsIsEmpty(err error) bool { return err == ErrQueueEmpty || err == ErrInFlightLimit }
func validLease(q *queueState, id, token string, now time.Time) bool {
	l, ok := q.leases[id]
	return ok && l.token == token && now.Before(l.deadline)
}
func (e *Engine) Ack(name, id, token string) error {
	results, err := e.AckBatch(name, []Receipt{{id, token}})
	if err != nil {
		return err
	}
	return results[0]
}

// AckBatch commits valid entries together and reports stale/duplicate receipts per item.
func (e *Engine) AckBatch(name string, receipts []Receipt) ([]error, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return nil, err
	}
	if len(receipts) == 0 || len(receipts) > maxBatchJobs {
		return nil, ErrInvalidArgument
	}
	q, ok := e.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	now := e.now()
	results := make([]error, len(receipts))
	ids := make([]string, 0, len(receipts))
	seen := map[string]bool{}
	for i, r := range receipts {
		if seen[r.JobID] || !validLease(q, r.JobID, r.LeaseToken, now) {
			results[i] = ErrInvalidLease
			continue
		}
		seen[r.JobID] = true
		ids = append(ids, r.JobID)
	}
	if len(ids) > 0 {
		if err := e.persistLocked(name, nil, nil, ids, false, e.seq); err != nil {
			return nil, err
		}
		e.applyLocked(q, nil, ids)
	}
	return results, nil
}
func (e *Engine) Nack(name, id, token string) error { return e.Reject(name, id, token, false, "") }
func (e *Engine) Reject(name, id, token string, permanent bool, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return err
	}
	if len(reason) > 1024 {
		return ErrInvalidArgument
	}
	q, ok := e.queues[name]
	if !ok {
		return ErrQueueNotFound
	}
	now := e.now()
	if !validLease(q, id, token, now) {
		return ErrInvalidLease
	}
	job, seq, err := e.retry(q.jobs[id], now, permanent, reason, e.seq)
	if err != nil {
		return err
	}
	if err := e.persistLocked(name, nil, []*Job{job}, nil, false, seq); err != nil {
		return err
	}
	e.seq = seq
	e.applyLocked(q, []*Job{job}, nil)
	return nil
}
func (e *Engine) retry(old *Job, now time.Time, permanent bool, reason string, seq uint64) (*Job, uint64, error) {
	job := *old
	job.LeaseToken = ""
	job.LeaseDeadline = time.Time{}
	job.LastError = reason
	job.AvailableAt = time.Time{}
	if permanent || job.Attempts >= job.MaxAttempts {
		job.State = "dead"
		return &job, seq, nil
	}
	var err error
	seq, err = e.nextSequence(seq)
	if err != nil {
		return nil, seq, err
	}
	job.Sequence = seq
	job.State = "ready"
	if job.RetryBase > 0 {
		upper := job.RetryBase
		for n := 1; n < job.Attempts && upper < job.RetryMax; n++ {
			if upper > job.RetryMax/2 {
				upper = job.RetryMax
			} else {
				upper *= 2
			}
		}
		upper = min(upper, job.RetryMax)
		delay := time.Duration(mathrand.Int64N(int64(upper) + 1))
		if delay > 0 {
			job.State = "delayed"
			job.AvailableAt = now.Add(delay)
		}
	}
	return &job, seq, nil
}
func (e *Engine) ExtendLease(name, id, token string, duration time.Duration) (time.Time, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return time.Time{}, err
	}
	if duration <= 0 || duration > 24*time.Hour {
		return time.Time{}, ErrInvalidLeaseDuration
	}
	q, ok := e.queues[name]
	if !ok {
		return time.Time{}, ErrQueueNotFound
	}
	now := e.now()
	if !validLease(q, id, token, now) {
		return time.Time{}, ErrInvalidLease
	}
	job := *q.jobs[id]
	candidate := now.Add(duration)
	if candidate.After(job.LeaseDeadline) {
		job.LeaseDeadline = candidate
	}
	if err := e.persistLocked(name, nil, []*Job{&job}, nil, false, e.seq); err != nil {
		return time.Time{}, err
	}
	e.applyLocked(q, []*Job{&job}, nil)
	return job.LeaseDeadline, nil
}

// RequeueExpired processes at most one bounded scheduler batch, including due delayed jobs.
func (e *Engine) RequeueExpired(name string) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return 0, err
	}
	q, ok := e.queues[name]
	if !ok {
		return 0, ErrQueueNotFound
	}
	return e.advanceLocked(name, q)
}
func (e *Engine) advanceLocked(name string, q *queueState) (int, error) {
	now, seq := e.now(), e.seq
	ids := q.deadlines.due(now, maxBatchJobs)
	jobs := make([]*Job, 0, len(ids))
	payloadBytes := 0
	for _, id := range ids {
		old := q.jobs[id]
		if len(old.Payload) > maxBatchBytes-payloadBytes {
			break
		}
		payloadBytes += len(old.Payload)
		var job *Job
		var err error
		if old.State == "leased" {
			job, seq, err = e.retry(old, now, false, "lease expired", seq)
		} else {
			copyJob := *old
			copyJob.State = "ready"
			seq, err = e.nextSequence(seq)
			copyJob.Sequence = seq
			copyJob.AvailableAt = time.Time{}
			job = &copyJob
		}
		if err != nil {
			return 0, err
		}
		jobs = append(jobs, job)
	}
	if len(jobs) == 0 {
		return 0, nil
	}
	if err := e.persistLocked(name, nil, jobs, nil, false, seq); err != nil {
		return 0, err
	}
	e.seq = seq
	e.applyLocked(q, jobs, nil)
	return len(jobs), nil
}
func (e *Engine) Run(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			e.mu.Lock()
			if err := e.statusLocked(); err != nil {
				e.mu.Unlock()
				return err
			}
			names := make([]string, 0, len(e.queues))
			for name := range e.queues {
				names = append(names, name)
			}
			sort.Strings(names)
			var err error
			for _, name := range names {
				if _, err = e.advanceLocked(name, e.queues[name]); err != nil {
					break
				}
			}
			e.mu.Unlock()
			if err != nil {
				return err
			}
		}
	}
}
func (e *Engine) applyLocked(q *queueState, jobs []*Job, deletes []string) {
	for _, id := range deletes {
		old := q.jobs[id]
		if old != nil {
			q.payloadBytes -= int64(len(old.Payload))
			e.totalBytes -= int64(len(old.Payload))
			e.totalJobs--
		}
		delete(q.jobs, id)
		delete(q.leases, id)
		delete(q.delayed, id)
		delete(q.dead, id)
		q.deadlines.remove(id)
	}
	for _, job := range jobs {
		old := q.jobs[job.ID]
		if old == nil {
			q.payloadBytes += int64(len(job.Payload))
			e.totalBytes += int64(len(job.Payload))
			e.totalJobs++
		}
		delete(q.leases, job.ID)
		delete(q.delayed, job.ID)
		delete(q.dead, job.ID)
		q.deadlines.remove(job.ID)
		if job.State == "ready" && (old == nil || old.State != "ready") {
			q.ready = append(q.ready, job.ID)
		}
		if job.State == "leased" {
			q.leases[job.ID] = jobLease{job.LeaseToken, job.LeaseDeadline}
			q.deadlines.set(job.ID, job.LeaseDeadline)
		}
		if job.State == "delayed" {
			q.delayed[job.ID] = job.AvailableAt
			q.deadlines.set(job.ID, job.AvailableAt)
		}
		if job.State == "dead" {
			q.dead[job.ID] = true
		}
		q.jobs[job.ID] = job
	}
	e.wake(q)
}
func info(name string, q *queueState) QueueInfo {
	return QueueInfo{Name: name, Config: q.config, Jobs: len(q.jobs), Ready: len(q.ready), Delayed: len(q.delayed), InFlight: len(q.leases), Dead: len(q.dead), PayloadBytes: q.payloadBytes}
}
func (e *Engine) GetQueue(name string) (QueueInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return QueueInfo{}, err
	}
	q := e.queues[name]
	if q == nil {
		return QueueInfo{}, ErrQueueNotFound
	}
	return info(name, q), nil
}
func (e *Engine) ListQueues(after string, limit int) ([]QueueInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 128 {
		return nil, ErrInvalidArgument
	}
	names := []string{}
	for name := range e.queues {
		if name > after {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	names = names[:min(limit, len(names))]
	out := make([]QueueInfo, 0, len(names))
	for _, name := range names {
		out = append(out, info(name, e.queues[name]))
	}
	return out, nil
}
func (e *Engine) DeleteQueue(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return err
	}
	q := e.queues[name]
	if q == nil {
		return ErrQueueNotFound
	}
	if len(q.jobs) > 0 {
		return ErrQueueNotEmpty
	}
	if err := e.persistLocked(name, nil, nil, nil, true, e.seq); err != nil {
		return err
	}
	e.wake(q)
	delete(e.queues, name)
	return nil
}
func (e *Engine) ListDead(name, after string, limit int) ([]Job, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return nil, err
	}
	q := e.queues[name]
	if q == nil {
		return nil, ErrQueueNotFound
	}
	if limit <= 0 || limit > maxBatchJobs {
		return nil, ErrInvalidArgument
	}
	ids := []string{}
	for id := range q.dead {
		if id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	out := make([]Job, 0, min(limit, len(ids)))
	size := 0
	for _, id := range ids[:min(limit, len(ids))] {
		job := *q.jobs[id]
		if len(job.Payload) > maxBatchBytes-size {
			break
		}
		size += len(job.Payload)
		job.Payload = append([]byte(nil), job.Payload...)
		out = append(out, job)
	}
	return out, nil
}
func (e *Engine) RequeueDead(name, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.statusLocked(); err != nil {
		return err
	}
	q := e.queues[name]
	if q == nil {
		return ErrQueueNotFound
	}
	if !q.dead[id] {
		return ErrJobNotFound
	}
	job := *q.jobs[id]
	seq, err := e.nextSequence(e.seq)
	if err != nil {
		return err
	}
	job.State = "ready"
	job.Attempts = 0
	job.LastError = ""
	job.Sequence = seq
	if err := e.persistLocked(name, nil, []*Job{&job}, nil, false, seq); err != nil {
		return err
	}
	e.seq = seq
	e.applyLocked(q, []*Job{&job}, nil)
	return nil
}
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	for _, q := range e.queues {
		e.wake(q)
	}
	if e.store != nil {
		return e.store.db.Close()
	}
	return nil
}
func (e *Engine) storageFailure(err error) error {
	e.fault = fmt.Errorf("%w: %v", ErrStorage, err)
	for _, q := range e.queues {
		e.wake(q)
	}
	return e.fault
}
