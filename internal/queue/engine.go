package queue

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

const (
	maxBatchJobs  = 120
	maxJobBytes   = 1 << 20
	maxBatchBytes = 4 << 20
)

type queueState struct {
	jobs   map[string]*Job
	ready  []string
	leases map[string]jobLease
}

type Engine struct {
	mu     sync.Mutex
	queues map[string]*queueState
	now    func() time.Time
}

type jobLease struct {
	token    string
	deadline time.Time
}

func NewEngine(now func() time.Time) *Engine {
	if now == nil {
		now = time.Now
	}

	return &Engine{
		queues: make(map[string]*queueState),
		now:    now,
	}
}

func (e *Engine) CreateQueue(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.queues[name]; exists {
		return ErrQueueExists
	}

	e.queues[name] = &queueState{
		jobs:   make(map[string]*Job),
		leases: make(map[string]jobLease),
	}

	return nil
}

func (e *Engine) Publish(queueName string, payload []byte) (string, error) {
	ids, err := e.PublishBatch(queueName, [][]byte{payload})
	if err != nil {
		return "", err
	}

	return ids[0], nil
}

func (e *Engine) Fetch(
	queueName string,
	leaseDuration time.Duration,
) (Delivery, error) {
	deliveries, err := e.FetchBatch(queueName, 1, leaseDuration)
	if err != nil {
		return Delivery{}, err
	}

	return deliveries[0], nil
}

// FetchBatch leases a FIFO prefix of ready jobs, bounded by limit and
// maxBatchBytes of payload. Limit must be between 1 and maxBatchJobs.
// It returns ErrQueueEmpty when no ready jobs are available.
func (e *Engine) FetchBatch(
	queueName string,
	limit int,
	leaseDuration time.Duration,
) ([]Delivery, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if limit <= 0 || limit > maxBatchJobs {
		return nil, ErrInvalidFetchLimit
	}
	if leaseDuration <= 0 {
		return nil, ErrInvalidLeaseDuration
	}

	q, exists := e.queues[queueName]
	if !exists {
		return nil, ErrQueueNotFound
	}
	if len(q.ready) == 0 {
		return nil, ErrQueueEmpty
	}

	count, payloadBytes := 0, 0
	for _, jobID := range q.ready[:min(limit, len(q.ready))] {
		size := len(q.jobs[jobID].Payload)
		if size > maxBatchBytes-payloadBytes {
			// Preserve FIFO: leave this job and all following jobs ready.
			break
		}
		payloadBytes += size
		count++
	}

	// Prepare every delivery before changing ownership or attempt counts.
	deliveries := make([]Delivery, 0, count)
	for _, jobID := range q.ready[:count] {
		job := q.jobs[jobID]
		var rawToken [16]byte
		if _, err := rand.Read(rawToken[:]); err != nil {
			return nil, err
		}

		payload := make([]byte, len(job.Payload))
		copy(payload, job.Payload)
		deliveries = append(deliveries, Delivery{
			JobID:      jobID,
			Payload:    payload,
			Attempt:    job.Attempts + 1,
			LeaseToken: hex.EncodeToString(rawToken[:]),
		})
	}

	// Start all leases from one clock reading after payload preparation.
	deadline := e.now().Add(leaseDuration)
	for i := range deliveries {
		delivery := &deliveries[i]
		delivery.LeaseDeadline = deadline
		q.jobs[delivery.JobID].Attempts = delivery.Attempt
		q.leases[delivery.JobID] = jobLease{
			token:    delivery.LeaseToken,
			deadline: deadline,
		}
	}

	clear(q.ready[:count])
	q.ready = q.ready[count:]
	return deliveries, nil
}

func (e *Engine) Ack(
	queueName string,
	jobID string,
	leaseToken string,
) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	q, queueExists := e.queues[queueName]
	if !queueExists {
		return ErrQueueNotFound
	}

	lease, leaseExists := q.leases[jobID]
	if !leaseExists || lease.token != leaseToken || !e.now().Before(lease.deadline) {
		return ErrInvalidLease
	}

	delete(q.leases, jobID)
	delete(q.jobs, jobID)

	return nil
}

func (e *Engine) RequeueExpired(queueName string) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	q, queueExists := e.queues[queueName]
	if !queueExists {
		return 0, ErrQueueNotFound
	}
	count := 0
	now := e.now()
	for jobID, lease := range q.leases {
		expired := !now.Before(lease.deadline)
		if expired {
			delete(q.leases, jobID)
			q.ready = append(q.ready, jobID)
			count += 1
		}
	}

	return count, nil
}

func (e *Engine) ExtendLease(
	queueName string,
	jobID string,
	leaseToken string,
	duration time.Duration,
) (time.Time, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if duration <= 0 {
		return time.Time{}, ErrInvalidLeaseDuration
	}

	q, queueExists := e.queues[queueName]
	if !queueExists {
		return time.Time{}, ErrQueueNotFound
	}

	lease, leaseExists := q.leases[jobID]
	if !leaseExists || lease.token != leaseToken {
		return time.Time{}, ErrInvalidLease
	}

	now := e.now()
	if !now.Before(lease.deadline) {
		return time.Time{}, ErrInvalidLease
	}

	deadline := lease.deadline
	newDuration := now.Add(duration)
	if newDuration.After(lease.deadline) {
		deadline = newDuration
	}

	q.leases[jobID] = jobLease{
		token:    leaseToken,
		deadline: deadline,
	}

	return deadline, nil
}

func (e *Engine) Nack(
	queueName string,
	jobID string,
	leaseToken string,
) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	q, queueExists := e.queues[queueName]
	if !queueExists {
		return ErrQueueNotFound
	}

	lease, leaseExists := q.leases[jobID]
	if !leaseExists || lease.token != leaseToken || !e.now().Before(lease.deadline) {
		return ErrInvalidLease
	}

	delete(q.leases, jobID)
	q.ready = append(q.ready, jobID)

	return nil
}

func (e *Engine) PublishBatch(
	queueName string,
	payloads [][]byte,
) ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	q, queueExists := e.queues[queueName]

	if !queueExists {
		return []string{}, ErrQueueNotFound
	}
	if len(payloads) == 0 {
		return []string{}, ErrEmptyBatch
	}
	if len(payloads) > maxBatchJobs {
		return []string{}, ErrBatchTooLarge
	}

	batchSize := 0
	for i := 0; i < len(payloads); i++ {
		if len(payloads[i]) > maxJobBytes {
			return []string{}, ErrPayloadTooLarge
		}
		batchSize += len(payloads[i])
	}

	if batchSize > maxBatchBytes {
		return []string{}, ErrBatchTooLarge
	}

	jobIDs := make([]string, 0, len(payloads))
	preparedJobs := make([]*Job, 0, len(payloads))

	for i := 0; i < len(payloads); i++ {
		var rawID [16]byte

		if _, err := rand.Read(rawID[:]); err != nil {
			return []string{}, err
		}

		jobID := hex.EncodeToString(rawID[:])

		payloadCopy := make([]byte, len(payloads[i]))
		copy(payloadCopy, payloads[i])
		job := &Job{
			ID:       jobID,
			Payload:  payloadCopy,
			Attempts: 0,
		}

		preparedJobs = append(preparedJobs, job)
		jobIDs = append(jobIDs, jobID)
	}

	for _, job := range preparedJobs {
		q.jobs[job.ID] = job
		q.ready = append(q.ready, job.ID)
	}

	return jobIDs, nil
}
