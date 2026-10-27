package altqueue

import (
	"context"
	"fmt"
	"time"
	"unicode/utf8"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WorkerOptions struct {
	Concurrency   int
	LeaseDuration time.Duration
	Wait          time.Duration
}

// RunWorker processes jobs until cancellation or an unrecoverable delivery error.
// Handlers must honor context cancellation and make external side effects idempotent.
// A handler error negatively acknowledges the job; nil acknowledges it. A lost
// heartbeat cancels that handler and never acknowledges its potentially stale delivery.
func (c *Client) RunWorker(ctx context.Context, name string, opts WorkerOptions, handler func(context.Context, *Delivery) error) error {
	if opts.Concurrency == 0 {
		opts.Concurrency = 4
	}
	if opts.LeaseDuration == 0 {
		opts.LeaseDuration = 30 * time.Second
	}
	if opts.Wait == 0 {
		opts.Wait = 5 * time.Second
	}
	if opts.Concurrency < 1 || opts.Concurrency > 64 || opts.LeaseDuration < 3*time.Second || opts.LeaseDuration > 24*time.Hour || opts.Wait < time.Millisecond || opts.Wait > 30*time.Second || handler == nil {
		return fmt.Errorf("invalid worker options")
	}
	group, ctx := errgroup.WithContext(ctx)
	for i := 0; i < opts.Concurrency; i++ {
		group.Go(func() error {
			for {
				ds, err := c.Fetch(ctx, name, 1, opts.LeaseDuration, opts.Wait)
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err != nil {
					if status.Code(err) == codes.ResourceExhausted || status.Code(err) == codes.Unavailable {
						timer := time.NewTimer(time.Second)
						select {
						case <-ctx.Done():
							timer.Stop()
							return ctx.Err()
						case <-timer.C:
						}
						continue
					}
					return err
				}
				for _, d := range ds {
					if err := c.process(ctx, name, d, opts.LeaseDuration, handler); err != nil {
						return err
					}
				}
			}
		})
	}
	return group.Wait()
}
func (c *Client) process(ctx context.Context, name string, d *Delivery, lease time.Duration, handler func(context.Context, *Delivery) error) error {
	// Keep ownership credentials separate from the value passed to application code.
	leaseRef := &Delivery{JobId: d.JobId, LeaseToken: d.LeaseToken}
	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	done := make(chan struct{})
	heartbeatErr := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				rpcCtx, cancel := context.WithTimeout(heartbeatCtx, lease/3)
				_, err := c.ExtendLease(rpcCtx, name, leaseRef, lease)
				cancel()
				if err != nil {
					if heartbeatCtx.Err() == nil {
						heartbeatErr <- err
						cancelJob()
					}
					return
				}
			}
		}
	}()
	handlerErr := handler(jobCtx, d)
	stopHeartbeat()
	<-done
	select {
	case err := <-heartbeatErr:
		return fmt.Errorf("lease heartbeat failed: %w", err)
	default:
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if handlerErr == nil {
		return c.Ack(ctx, name, leaseRef)
	}
	reason := handlerErr.Error()
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	for !utf8.ValidString(reason) {
		reason = reason[:len(reason)-1]
	}
	return c.Nack(ctx, name, leaseRef, false, reason)
}
