// Package altqueue is the Go client for the AltQueue gRPC API.
// Mutating requests are not automatically retried: a timeout can have an unknown outcome.
package altqueue

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	pb "github.com/tusharsinghbisht/altqueue/api/gen/altqueue/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type QueueConfig = pb.QueueConfig
type QueueInfo = pb.QueueInfo
type Delivery = pb.Delivery
type AckResult = pb.AckResult

type Options struct {
	Token string
	// Insecure explicitly enables plaintext. Use only on trusted local networks.
	Insecure bool
	TLS      *tls.Config
	Timeout  time.Duration
}
type Client struct {
	conn *grpc.ClientConn
	API  pb.QueueServiceClient
}

func Dial(ctx context.Context, address string, opts Options) (*Client, error) {
	if opts.Timeout <= 0 {
		opts.Timeout = 35 * time.Second
	}
	var creds credentials.TransportCredentials
	if opts.Insecure {
		if opts.TLS != nil {
			return nil, fmt.Errorf("TLS and Insecure are mutually exclusive")
		}
		creds = insecure.NewCredentials()
	} else {
		config := &tls.Config{MinVersion: tls.VersionTLS12}
		if opts.TLS != nil {
			config = opts.TLS.Clone()
			if config.MinVersion < tls.VersionTLS12 {
				config.MinVersion = tls.VersionTLS12
			}
		}
		creds = credentials.NewTLS(config)
	}
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
			defer cancel()
		}
		if opts.Token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+opts.Token)
		}
		return invoke(ctx, method, req, reply, cc, options...)
	}
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, address, grpc.WithBlock(), grpc.WithTransportCredentials(creds), grpc.WithDisableRetry(), grpc.WithUnaryInterceptor(interceptor), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(6<<20), grpc.MaxCallSendMsgSize(6<<20)))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, API: pb.NewQueueServiceClient(conn)}, nil
}
func (c *Client) Close() error { return c.conn.Close() }
func (c *Client) CreateQueue(ctx context.Context, name string, config *QueueConfig) (*QueueInfo, error) {
	return c.API.CreateQueue(ctx, &pb.CreateQueueRequest{Queue: name, Config: config})
}
func (c *Client) GetQueue(ctx context.Context, name string) (*QueueInfo, error) {
	return c.API.GetQueue(ctx, &pb.QueueRequest{Queue: name})
}
func (c *Client) ListQueues(ctx context.Context, after string, limit int32) (*pb.ListQueuesResponse, error) {
	return c.API.ListQueues(ctx, &pb.ListQueuesRequest{After: after, Limit: limit})
}
func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	_, err := c.API.DeleteQueue(ctx, &pb.QueueRequest{Queue: name})
	return err
}
func ms(d, max time.Duration, positive bool) (int64, error) {
	if d < 0 || d > max || (positive && d < time.Millisecond) {
		return 0, fmt.Errorf("duration outside supported millisecond range")
	}
	return d.Milliseconds(), nil
}
func (c *Client) Publish(ctx context.Context, name string, payloads [][]byte, delay time.Duration) ([]string, error) {
	m, err := ms(delay, 365*24*time.Hour, false)
	if err != nil {
		return nil, err
	}
	r, err := c.API.Publish(ctx, &pb.PublishRequest{Queue: name, Payloads: payloads, DelayMs: m})
	if err != nil {
		return nil, err
	}
	return r.JobIds, nil
}
func (c *Client) Fetch(ctx context.Context, name string, limit int32, lease, wait time.Duration) ([]*Delivery, error) {
	l, err := ms(lease, 24*time.Hour, true)
	if err != nil {
		return nil, err
	}
	w, err := ms(wait, 30*time.Second, false)
	if err != nil {
		return nil, err
	}
	r, err := c.API.Fetch(ctx, &pb.FetchRequest{Queue: name, Limit: limit, LeaseMs: l, WaitMs: w})
	if err != nil {
		return nil, err
	}
	return r.Deliveries, nil
}
func ref(d *Delivery) *pb.Receipt {
	if d == nil {
		return nil
	}
	return &pb.Receipt{JobId: d.JobId, LeaseToken: d.LeaseToken}
}
func (c *Client) Ack(ctx context.Context, name string, d *Delivery) error {
	_, err := c.API.Ack(ctx, &pb.AckRequest{Queue: name, Receipt: ref(d)})
	return err
}
func (c *Client) AckBatch(ctx context.Context, name string, ds []*Delivery) ([]*AckResult, error) {
	refs := make([]*pb.Receipt, 0, len(ds))
	for _, d := range ds {
		refs = append(refs, ref(d))
	}
	r, err := c.API.AckBatch(ctx, &pb.AckBatchRequest{Queue: name, Receipts: refs})
	if err != nil {
		return nil, err
	}
	return r.Results, nil
}
func (c *Client) Nack(ctx context.Context, name string, d *Delivery, permanent bool, reason string) error {
	_, err := c.API.Nack(ctx, &pb.NackRequest{Queue: name, Receipt: ref(d), Permanent: permanent, Reason: reason})
	return err
}
func (c *Client) ExtendLease(ctx context.Context, name string, d *Delivery, duration time.Duration) (time.Time, error) {
	m, err := ms(duration, 24*time.Hour, true)
	if err != nil {
		return time.Time{}, err
	}
	r, err := c.API.ExtendLease(ctx, &pb.ExtendLeaseRequest{Queue: name, Receipt: ref(d), DurationMs: m})
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(r.LeaseDeadlineUnixMs), nil
}
func (c *Client) ListDead(ctx context.Context, name, after string, limit int32) (*pb.ListDeadResponse, error) {
	return c.API.ListDeadJobs(ctx, &pb.ListDeadRequest{Queue: name, After: after, Limit: limit})
}
func (c *Client) RequeueDead(ctx context.Context, name, id string) error {
	_, err := c.API.RequeueDeadJob(ctx, &pb.RequeueDeadRequest{Queue: name, JobId: id})
	return err
}
