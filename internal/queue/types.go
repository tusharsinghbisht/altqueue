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
	ErrInvalidLeaseDuration = errors.New("lease duration must be positive and at most 24 hours")
	ErrInvalidLease         = errors.New("lease is missing, expired or invalid")
	ErrEmptyBatch           = errors.New("batch must contain at least one job")
	ErrBatchTooLarge        = errors.New("batch exceeds request limits")
	ErrPayloadTooLarge      = errors.New("payload exceeds size limit")
	ErrInvalidQueueConfig   = errors.New("invalid queue configuration")
	ErrInvalidQueueName     = errors.New("queue name must be 1-64 ASCII letters, digits, dots, underscores or hyphens")
	ErrQueueFull            = errors.New("queue capacity exceeded")
	ErrInFlightLimit        = errors.New("queue in-flight limit reached")
	ErrQueueNotEmpty        = errors.New("queue is not empty")
	ErrJobNotFound          = errors.New("job not found in the required state")
	ErrInvalidArgument      = errors.New("invalid argument")
	ErrClosed               = errors.New("engine is closed")
	ErrStorage              = errors.New("storage unavailable; restart required")
)

// QueueConfig limits include ready, delayed, leased and dead-letter jobs.
// MaxJobs must be positive. Other zero fields select defaults. RetryBase may be zero.
type QueueConfig struct {
	MaxJobs         int
	MaxPayloadBytes int64
	MaxInFlight     int
	MaxAttempts     int
	RetryBase       time.Duration
	RetryMax        time.Duration
}

func DefaultQueueConfig() QueueConfig {
	return QueueConfig{MaxJobs: 100_000, MaxPayloadBytes: 64 << 20, MaxInFlight: 1_000, MaxAttempts: 5, RetryMax: time.Minute}
}

// Job is an unfinished job. Retry policy is captured at publication.
type Job struct {
	ID            string
	Payload       []byte
	Attempts      int
	State         string
	CreatedAt     time.Time
	AvailableAt   time.Time
	Sequence      uint64
	MaxAttempts   int
	RetryBase     time.Duration
	RetryMax      time.Duration
	LastError     string
	LeaseToken    string
	LeaseDeadline time.Time
}

type Delivery struct {
	JobID         string
	Payload       []byte
	Attempt       int
	LeaseToken    string
	LeaseDeadline time.Time
}

type QueueInfo struct {
	Name         string
	Config       QueueConfig
	Jobs         int
	Ready        int
	Delayed      int
	InFlight     int
	Dead         int
	PayloadBytes int64
}

type Receipt struct{ JobID, LeaseToken string }
