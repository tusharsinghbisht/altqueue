package transport

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	pb "github.com/tusharsinghbisht/altqueue/api/gen/altqueue/v1"
	"github.com/tusharsinghbisht/altqueue/internal/queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

type Options struct {
	Token       string
	TLS         *tls.Config
	MaxRequests int
}
type Server struct {
	pb.UnimplementedQueueServiceServer
	GRPC      *grpc.Server
	Registry  *prometheus.Registry
	engine    *queue.Engine
	token     string
	accepting atomic.Bool
	stop      context.Context
	cancel    context.CancelFunc
}

func New(engine *queue.Engine, opts Options) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{engine: engine, token: opts.Token, stop: ctx, cancel: cancel, Registry: prometheus.NewRegistry()}
	s.accepting.Store(true)
	if opts.MaxRequests <= 0 {
		opts.MaxRequests = 128
	}
	slots := make(chan struct{}, opts.MaxRequests)
	decodeSlots := make(chan struct{}, opts.MaxRequests)
	pollSlots := make(chan struct{}, max(1, opts.MaxRequests/2))
	// Gate streams before protobuf decoding as well as handlers after decoding.
	gate := func(ctx context.Context, _ *tap.Info) (context.Context, error) {
		if !s.accepting.Load() {
			return nil, status.Error(codes.Unavailable, "broker is stopping")
		}
		if s.token != "" {
			md, _ := metadata.FromIncomingContext(ctx)
			values := md.Get("authorization")
			if len(values) != 1 || !s.authorized(values[0]) {
				return nil, status.Error(codes.Unauthenticated, "invalid credentials")
			}
		}
		select {
		case decodeSlots <- struct{}{}:
			context.AfterFunc(ctx, func() { <-decodeSlots })
			return ctx, nil
		default:
			return nil, status.Error(codes.ResourceExhausted, "too many request streams")
		}
	}
	calls := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "altqueue_rpc_total", Help: "Completed RPCs by method and status."}, []string{"method", "code"})
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "altqueue_rpc_seconds", Help: "RPC latency including storage commits.", Buckets: prometheus.DefBuckets}, []string{"method"})
	s.Registry.MustRegister(calls, latency, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	for _, state := range []string{"ready", "delayed", "in_flight", "dead"} {
		state := state
		s.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "altqueue_jobs_" + state, Help: "Current jobs in this state across queues."}, func() float64 {
			infos, err := engine.ListQueues("", 128)
			if err != nil {
				return 0
			}
			n := 0
			for _, q := range infos {
				switch state {
				case "ready":
					n += q.Ready
				case "delayed":
					n += q.Delayed
				case "in_flight":
					n += q.InFlight
				case "dead":
					n += q.Dead
				}
			}
			return float64(n)
		}))
	}
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		start := time.Now()
		defer func() {
			calls.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
			latency.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		}()
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			return nil, status.Error(codes.ResourceExhausted, "too many outstanding requests")
		}
		if !s.accepting.Load() || !engine.Ready() {
			return nil, status.Error(codes.Unavailable, "broker is not ready")
		}
		if fetch, ok := req.(*pb.FetchRequest); ok && fetch.WaitMs > 0 {
			select {
			case pollSlots <- struct{}{}:
				defer func() { <-pollSlots }()
			default:
				return nil, status.Error(codes.ResourceExhausted, "too many long polls")
			}
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		stop := context.AfterFunc(s.stop, cancel)
		defer stop()
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		return handler(ctx, req)
	}
	options := []grpc.ServerOption{grpc.InTapHandle(gate), grpc.UnaryInterceptor(interceptor), grpc.MaxRecvMsgSize(6 << 20), grpc.MaxSendMsgSize(6 << 20), grpc.MaxConcurrentStreams(128), grpc.MaxHeaderListSize(16 << 10), grpc.ConnectionTimeout(5 * time.Second)}
	if opts.TLS != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(opts.TLS)))
	}
	s.GRPC = grpc.NewServer(options...)
	pb.RegisterQueueServiceServer(s.GRPC, s)
	return s
}
func (s *Server) authorized(header string) bool {
	return subtle.ConstantTimeCompare([]byte(header), []byte("Bearer "+s.token)) == 1
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !s.accepting.Load() || !s.engine.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	metrics := promhttp.HandlerFor(s.Registry, promhttp.HandlerOpts{Timeout: 5 * time.Second})
	mux.Handle("GET /metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.token != "" && !s.authorized(r.Header.Get("Authorization")) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		metrics.ServeHTTP(w, r)
	}))
	return mux
}
func (s *Server) Shutdown(ctx context.Context) {
	s.accepting.Store(false)
	s.cancel()
	done := make(chan struct{})
	go func() { s.GRPC.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		s.GRPC.Stop()
		<-done
	}
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	case errors.Is(err, queue.ErrQueueExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, queue.ErrQueueNotFound), errors.Is(err, queue.ErrJobNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, queue.ErrQueueFull), errors.Is(err, queue.ErrInFlightLimit):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, queue.ErrInvalidLease), errors.Is(err, queue.ErrQueueNotEmpty):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, queue.ErrInvalidArgument), errors.Is(err, queue.ErrInvalidQueueConfig), errors.Is(err, queue.ErrInvalidQueueName), errors.Is(err, queue.ErrInvalidFetchLimit), errors.Is(err, queue.ErrInvalidLeaseDuration), errors.Is(err, queue.ErrEmptyBatch), errors.Is(err, queue.ErrBatchTooLarge), errors.Is(err, queue.ErrPayloadTooLarge):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, queue.ErrClosed), errors.Is(err, queue.ErrStorage):
		return status.Error(codes.Unavailable, "storage unavailable")
	default:
		return status.Error(codes.Internal, "internal server error")
	}
}
func milliseconds(value int64, max time.Duration) (time.Duration, error) {
	if value < 0 || value > int64(max/time.Millisecond) {
		return 0, queue.ErrInvalidArgument
	}
	return time.Duration(value) * time.Millisecond, nil
}
func configPB(c queue.QueueConfig) *pb.QueueConfig {
	return &pb.QueueConfig{MaxJobs: int32(c.MaxJobs), MaxPayloadBytes: c.MaxPayloadBytes, MaxInFlight: int32(c.MaxInFlight), MaxAttempts: int32(c.MaxAttempts), RetryBaseMs: c.RetryBase.Milliseconds(), RetryMaxMs: c.RetryMax.Milliseconds()}
}
func infoPB(q queue.QueueInfo) *pb.QueueInfo {
	return &pb.QueueInfo{Name: q.Name, Config: configPB(q.Config), Jobs: int64(q.Jobs), Ready: int64(q.Ready), Delayed: int64(q.Delayed), InFlight: int64(q.InFlight), Dead: int64(q.Dead), PayloadBytes: q.PayloadBytes}
}
func (s *Server) CreateQueue(ctx context.Context, r *pb.CreateQueueRequest) (*pb.QueueInfo, error) {
	var err error
	if r.Config == nil {
		err = s.engine.CreateQueue(r.Queue)
	} else {
		base, e := milliseconds(r.Config.RetryBaseMs, 24*time.Hour)
		if e != nil {
			return nil, rpcError(e)
		}
		max, e := milliseconds(r.Config.RetryMaxMs, 24*time.Hour)
		if e != nil {
			return nil, rpcError(e)
		}
		err = s.engine.CreateQueueWithConfig(r.Queue, queue.QueueConfig{MaxJobs: int(r.Config.MaxJobs), MaxPayloadBytes: r.Config.MaxPayloadBytes, MaxInFlight: int(r.Config.MaxInFlight), MaxAttempts: int(r.Config.MaxAttempts), RetryBase: base, RetryMax: max})
	}
	if err != nil {
		return nil, rpcError(err)
	}
	q, err := s.engine.GetQueue(r.Queue)
	return infoPB(q), rpcError(err)
}
func (s *Server) GetQueue(ctx context.Context, r *pb.QueueRequest) (*pb.QueueInfo, error) {
	q, err := s.engine.GetQueue(r.Queue)
	if err != nil {
		return nil, rpcError(err)
	}
	return infoPB(q), nil
}
func (s *Server) ListQueues(ctx context.Context, r *pb.ListQueuesRequest) (*pb.ListQueuesResponse, error) {
	limit := int(r.Limit)
	if limit == 0 {
		limit = 100
	}
	qs, err := s.engine.ListQueues(r.After, limit)
	if err != nil {
		return nil, rpcError(err)
	}
	out := &pb.ListQueuesResponse{}
	for _, q := range qs {
		out.Queues = append(out.Queues, infoPB(q))
	}
	if len(qs) == limit {
		out.NextAfter = qs[len(qs)-1].Name
	}
	return out, nil
}
func (s *Server) DeleteQueue(ctx context.Context, r *pb.QueueRequest) (*pb.Empty, error) {
	return &pb.Empty{}, rpcError(s.engine.DeleteQueue(r.Queue))
}
func (s *Server) Publish(ctx context.Context, r *pb.PublishRequest) (*pb.PublishResponse, error) {
	delay, err := milliseconds(r.DelayMs, 365*24*time.Hour)
	if err != nil {
		return nil, rpcError(err)
	}
	ids, err := s.engine.PublishDelayed(r.Queue, r.Payloads, delay)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.PublishResponse{JobIds: ids}, nil
}
func (s *Server) Fetch(ctx context.Context, r *pb.FetchRequest) (*pb.FetchResponse, error) {
	lease, err := milliseconds(r.LeaseMs, 24*time.Hour)
	if err != nil {
		return nil, rpcError(err)
	}
	wait, err := milliseconds(r.WaitMs, 30*time.Second)
	if err != nil {
		return nil, rpcError(err)
	}
	ds, err := s.engine.FetchWait(ctx, r.Queue, int(r.Limit), lease, wait)
	if errors.Is(err, queue.ErrQueueEmpty) || (errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil) {
		return &pb.FetchResponse{}, nil
	}
	if err != nil {
		return nil, rpcError(err)
	}
	out := &pb.FetchResponse{}
	for _, d := range ds {
		out.Deliveries = append(out.Deliveries, &pb.Delivery{JobId: d.JobID, Payload: d.Payload, Attempt: int32(d.Attempt), LeaseToken: d.LeaseToken, LeaseDeadlineUnixMs: d.LeaseDeadline.UnixMilli()})
	}
	return out, nil
}
func receipt(r *pb.Receipt) (queue.Receipt, error) {
	if r == nil || r.JobId == "" || r.LeaseToken == "" {
		return queue.Receipt{}, queue.ErrInvalidArgument
	}
	return queue.Receipt{JobID: r.JobId, LeaseToken: r.LeaseToken}, nil
}
func (s *Server) Ack(ctx context.Context, r *pb.AckRequest) (*pb.Empty, error) {
	ref, err := receipt(r.Receipt)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.Empty{}, rpcError(s.engine.Ack(r.Queue, ref.JobID, ref.LeaseToken))
}
func (s *Server) AckBatch(ctx context.Context, r *pb.AckBatchRequest) (*pb.AckBatchResponse, error) {
	refs := make([]queue.Receipt, 0, len(r.Receipts))
	for _, ref := range r.Receipts {
		if ref == nil {
			refs = append(refs, queue.Receipt{})
		} else {
			refs = append(refs, queue.Receipt{JobID: ref.JobId, LeaseToken: ref.LeaseToken})
		}
	}
	results, err := s.engine.AckBatch(r.Queue, refs)
	if err != nil {
		return nil, rpcError(err)
	}
	out := &pb.AckBatchResponse{}
	for i, err := range results {
		result := &pb.AckResult{JobId: refs[i].JobID}
		if err != nil {
			result.ErrorCode = status.Code(rpcError(err)).String()
		}
		out.Results = append(out.Results, result)
	}
	return out, nil
}
func (s *Server) Nack(ctx context.Context, r *pb.NackRequest) (*pb.Empty, error) {
	ref, err := receipt(r.Receipt)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.Empty{}, rpcError(s.engine.Reject(r.Queue, ref.JobID, ref.LeaseToken, r.Permanent, r.Reason))
}
func (s *Server) ExtendLease(ctx context.Context, r *pb.ExtendLeaseRequest) (*pb.ExtendLeaseResponse, error) {
	ref, err := receipt(r.Receipt)
	if err != nil {
		return nil, rpcError(err)
	}
	duration, err := milliseconds(r.DurationMs, 24*time.Hour)
	if err != nil {
		return nil, rpcError(err)
	}
	deadline, err := s.engine.ExtendLease(r.Queue, ref.JobID, ref.LeaseToken, duration)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.ExtendLeaseResponse{LeaseDeadlineUnixMs: deadline.UnixMilli()}, nil
}
func (s *Server) ListDeadJobs(ctx context.Context, r *pb.ListDeadRequest) (*pb.ListDeadResponse, error) {
	limit := int(r.Limit)
	if limit == 0 {
		limit = 100
	}
	jobs, err := s.engine.ListDead(r.Queue, r.After, limit)
	if err != nil {
		return nil, rpcError(err)
	}
	out := &pb.ListDeadResponse{}
	for _, job := range jobs {
		out.Jobs = append(out.Jobs, &pb.DeadJob{JobId: job.ID, Payload: job.Payload, Attempts: int32(job.Attempts), LastError: job.LastError, CreatedUnixMs: job.CreatedAt.UnixMilli()})
	}
	if len(jobs) > 0 {
		out.NextAfter = jobs[len(jobs)-1].ID
	}
	return out, nil
}
func (s *Server) RequeueDeadJob(ctx context.Context, r *pb.RequeueDeadRequest) (*pb.Empty, error) {
	return &pb.Empty{}, rpcError(s.engine.RequeueDead(r.Queue, strings.TrimSpace(r.JobId)))
}
