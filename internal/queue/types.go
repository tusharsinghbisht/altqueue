package queue

import (
	"errors"
	"time"
)

var (
	ErrQueueExists          = errors.New("queue already exists")
	ErrQueueNotFound        = errors.New("queue not found")
	ErrQueueEmpty           = errors.New("queue has no ready jobs")
	ErrInvalidFetchLimit    = errors.New("fetch limit is outside the allowed range")
	ErrInvalidLeaseDuration = errors.New("lease duration must be postive")
	ErrInvalidLease         = errors.New("lease is missing, expired or invalid")
	ErrEmptyBatch           = errors.New("batch must contain at least one job")
	ErrBatchTooLarge        = errors.New("batch exceeds request limits")
	ErrPayloadTooLarge      = errors.New("payload exceeds size limit")
)

// the stored unit of work
type Job struct {
	ID       string
	Payload  []byte
	Attempts int
}

// a temporary assignment of that work to a worker
// same job can have several deliveries over lifetime, but each delivery needs a new lease token.
type Delivery struct {
	JobID         string
	Payload       []byte
	Attempt       int
	LeaseToken    string
	LeaseDeadline time.Time
}
