package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tusharsinghbisht/altqueue/internal/queue"
	"github.com/tusharsinghbisht/altqueue/internal/transport"
	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("broker stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	data := flag.String("data", "data", "Pebble data directory")
	address := flag.String("listen", "127.0.0.1:7331", "gRPC TCP address")
	admin := flag.String("http", "127.0.0.1:7332", "health and metrics address")
	certFile := flag.String("tls-cert", "", "PEM server certificate")
	keyFile := flag.String("tls-key", "", "PEM server private key")
	allowRemote := flag.Bool("allow-insecure-remote", false, "explicitly allow unsafe remote development listeners")
	backup := flag.String("backup-to", "", "create a checkpoint at this new path and exit (broker must be stopped)")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	token := os.Getenv("ALTQUEUE_TOKEN")
	if token != "" && len(token) < 16 {
		return fmt.Errorf("ALTQUEUE_TOKEN must contain at least 16 bytes")
	}
	var tlsConfig *tls.Config
	if (*certFile == "") != (*keyFile == "") {
		return fmt.Errorf("both -tls-cert and -tls-key are required")
	}
	if *certFile != "" {
		pair, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			return err
		}
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
	}
	for _, addr := range []string{*address, *admin} {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}
		ip := net.ParseIP(host)
		local := host == "localhost" || (ip != nil && ip.IsLoopback())
		if !local && (tlsConfig == nil || token == "") && !*allowRemote {
			return fmt.Errorf("remote listener %q requires TLS and ALTQUEUE_TOKEN; use -allow-insecure-remote only for development", addr)
		}
	}
	engine, err := queue.Open(*data, nil)
	if err != nil {
		return err
	}
	defer engine.Close()
	if *backup != "" {
		return engine.Checkpoint(*backup)
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return err
	}
	defer listener.Close()
	httpListener, err := net.Listen("tcp", *admin)
	if err != nil {
		return err
	}
	defer httpListener.Close()
	server := transport.New(engine, transport.Options{Token: token, TLS: tlsConfig, MaxRequests: 128})
	httpServer := &http.Server{Handler: server.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	if tlsConfig != nil {
		httpServer.TLSConfig = tlsConfig.Clone()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	errorsCh := make(chan error, 3)
	go func() { errorsCh <- server.GRPC.Serve(netutil.LimitListener(listener, 128)) }()
	go func() {
		limited := netutil.LimitListener(httpListener, 128)
		if tlsConfig != nil {
			errorsCh <- httpServer.ServeTLS(limited, *certFile, *keyFile)
		} else {
			errorsCh <- httpServer.Serve(limited)
		}
	}()
	go func() { errorsCh <- engine.Run(ctx) }()
	slog.Info("AltQueue ready", "grpc", listener.Addr().String(), "http", httpListener.Addr().String(), "data", *data, "tls", tlsConfig != nil, "authenticated", token != "")
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errorsCh:
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	server.Shutdown(shutdownCtx)
	_ = httpServer.Shutdown(shutdownCtx)
	if errors.Is(runErr, http.ErrServerClosed) || errors.Is(runErr, grpc.ErrServerStopped) {
		return nil
	}
	return runErr
}
