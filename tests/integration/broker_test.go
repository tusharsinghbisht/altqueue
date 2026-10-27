package integration

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tusharsinghbisht/altqueue/internal/queue"
	"github.com/tusharsinghbisht/altqueue/internal/transport"
	altqueue "github.com/tusharsinghbisht/altqueue/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// One end-to-end scenario covers TLS, authentication, delayed long polling,
// retries, batched acknowledgement, management, metrics and shutdown readiness.
func TestBrokerEndToEnd(t *testing.T) {
	engine, err := queue.Open(filepath.Join(t.TempDir(), "db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	serverTLS, clientTLS := testTLS(t)
	const token = "integration-secret-at-least-32-bytes"
	server := transport.New(engine, transport.Options{Token: token, TLS: serverTLS})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() { _ = server.GRPC.Serve(listener) }()
	go func() { _ = engine.Run(ctx) }()
	defer func() {
		shutdown, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		server.Shutdown(shutdown)
	}()

	bad, err := altqueue.Dial(ctx, listener.Addr().String(), altqueue.Options{TLS: clientTLS})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.CreateQueue(ctx, "jobs", nil); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated RPC accepted: %v", err)
	}
	_ = bad.Close()
	client, err := altqueue.Dial(ctx, listener.Addr().String(), altqueue.Options{TLS: clientTLS, Token: token})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.CreateQueue(ctx, "jobs", &altqueue.QueueConfig{MaxJobs: 3, MaxPayloadBytes: 1024, MaxInFlight: 1, MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	ids, err := client.Publish(ctx, "jobs", [][]byte{[]byte("hello")}, 10*time.Millisecond)
	if err != nil || len(ids) != 1 {
		t.Fatalf("publish: %v %v", ids, err)
	}
	ds, err := client.Fetch(ctx, "jobs", 2, time.Second, 2*time.Second)
	if err != nil || len(ds) != 1 || string(ds[0].Payload) != "hello" {
		t.Fatalf("delayed long poll: %+v %v", ds, err)
	}
	deadline, err := client.ExtendLease(ctx, "jobs", ds[0], time.Second)
	if err != nil || deadline.UnixMilli() < ds[0].LeaseDeadlineUnixMs {
		t.Fatalf("lease extension: %v %v", deadline, err)
	}
	if err := client.Nack(ctx, "jobs", ds[0], false, "retry once"); err != nil {
		t.Fatal(err)
	}
	ds, err = client.Fetch(ctx, "jobs", 1, time.Second, 0)
	if err != nil || len(ds) != 1 || ds[0].Attempt != 2 {
		t.Fatalf("retry: %+v %v", ds, err)
	}
	results, err := client.AckBatch(ctx, "jobs", []*altqueue.Delivery{ds[0], ds[0], nil})
	if err != nil || len(results) != 3 {
		t.Fatalf("batch ack: %+v %v", results, err)
	}
	if results[0].ErrorCode != "" || results[1].ErrorCode != "FailedPrecondition" || results[2].ErrorCode != "FailedPrecondition" {
		t.Fatalf("incorrect per-item results: %+v", results)
	}
	if info, err := client.GetQueue(ctx, "jobs"); err != nil || info.Jobs != 0 || info.PayloadBytes != 0 {
		t.Fatalf("completed queue: %+v %v", info, err)
	}
	// Process beyond the original lease deadline, exercising real SDK heartbeats.
	if _, err := client.Publish(ctx, "jobs", [][]byte{[]byte("long-running")}, 0); err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- client.RunWorker(workerCtx, "jobs", altqueue.WorkerOptions{Concurrency: 1, LeaseDuration: 3 * time.Second, Wait: time.Second}, func(ctx context.Context, d *altqueue.Delivery) error {
			// Mutating the application value must not change heartbeat/ack credentials.
			d.LeaseToken = "application-modified"
			timer := time.NewTimer(4 * time.Second)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		})
	}()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
waiting:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("worker did not finish before deadline")
		case err := <-workerDone:
			t.Fatalf("worker stopped before completing work: %v", err)
		case <-poll.C:
			info, err := client.GetQueue(ctx, "jobs")
			if err != nil {
				t.Fatal(err)
			}
			if info.Jobs == 0 {
				break waiting
			}
		}
	}
	stopWorker()
	if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	if err := client.DeleteQueue(ctx, "jobs"); err != nil {
		t.Fatal(err)
	}

	admin := httptest.NewServer(server.Handler())
	defer admin.Close()
	response, err := http.Get(admin.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatal("metrics were not protected")
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, admin.URL+"/metrics", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(data), "altqueue_rpc_total") {
		t.Fatalf("metrics unavailable: %d %v", response.StatusCode, err)
	}
	shutdown, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	server.Shutdown(shutdown)
	response, err = http.Get(admin.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("shutting-down broker remained ready")
	}
}

func testTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "AltQueue test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}, &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
}
