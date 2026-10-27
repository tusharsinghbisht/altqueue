package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	altqueue "github.com/tusharsinghbisht/altqueue/sdk/go"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "altqueue:", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string) error {
	global := flag.NewFlagSet("altqueue", flag.ContinueOnError)
	addr := global.String("addr", "127.0.0.1:7331", "broker address")
	useTLS := global.Bool("tls", false, "enable TLS using system trust roots")
	caFile := global.String("ca", "", "PEM root CA (also enables TLS)")
	serverName := global.String("server-name", "", "TLS server name override")
	global.Usage = func() {
		fmt.Fprintln(global.Output(), "Usage: altqueue [connection flags] COMMAND [command flags] QUEUE [arguments]\nCommands: create queues stats delete publish fetch ack nack extend dead requeue work")
		global.PrintDefaults()
	}
	if err := global.Parse(args); err != nil {
		return err
	}
	args = global.Args()
	if len(args) == 0 {
		global.Usage()
		return fmt.Errorf("command required")
	}
	command := args[0]
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	var maxJobs, maxInFlight, maxAttempts, limit int
	var maxBytes int64
	var delay, lease, wait, base, maxBackoff time.Duration
	var permanent bool
	var reason, after string
	switch command {
	case "create":
		fs.IntVar(&maxJobs, "max-jobs", 100_000, "stored job limit")
		fs.Int64Var(&maxBytes, "max-bytes", 64<<20, "stored payload byte limit")
		fs.IntVar(&maxInFlight, "max-in-flight", 1000, "in-flight job limit")
		fs.IntVar(&maxAttempts, "max-attempts", 5, "total delivery attempts")
		fs.DurationVar(&base, "retry-base", 0, "retry backoff base; zero retries immediately")
		fs.DurationVar(&maxBackoff, "retry-max", time.Minute, "maximum retry backoff")
	case "publish":
		fs.DurationVar(&delay, "delay", 0, "initial delay")
	case "fetch":
		fs.IntVar(&limit, "limit", 1, "maximum jobs")
		fs.DurationVar(&lease, "lease", 30*time.Second, "visibility lease")
		fs.DurationVar(&wait, "wait", 0, "long-poll wait, at most 30s")
	case "nack":
		fs.BoolVar(&permanent, "permanent", false, "dead-letter instead of retry")
		fs.StringVar(&reason, "reason", "", "bounded failure description")
	case "extend":
		fs.DurationVar(&lease, "duration", 30*time.Second, "desired remaining lease window")
	case "dead", "queues":
		fs.StringVar(&after, "after", "", "pagination cursor")
		fs.IntVar(&limit, "limit", 100, "page size")
	case "stats", "delete", "ack", "requeue", "work":
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	args = fs.Args()
	need := 1
	switch command {
	case "queues":
		need = 0
	case "ack", "nack", "extend":
		need = 3
	case "requeue":
		need = 2
	case "publish":
		need = 2
	}
	if (command == "publish" && len(args) < need) || (command != "publish" && len(args) != need) {
		return fmt.Errorf("%s requires %d positional arguments (flags come before the queue)", command, need)
	}
	opts := altqueue.Options{Token: os.Getenv("ALTQUEUE_TOKEN"), Insecure: !*useTLS && *caFile == ""}
	if !opts.Insecure {
		opts.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: *serverName}
		if *caFile != "" {
			data, err := os.ReadFile(*caFile)
			if err != nil {
				return err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(data) {
				return fmt.Errorf("CA file contains no certificates")
			}
			opts.TLS.RootCAs = pool
		}
	}
	client, err := altqueue.Dial(ctx, *addr, opts)
	if err != nil {
		return err
	}
	defer client.Close()
	print := func(v any) error {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(v)
	}
	name := ""
	if len(args) > 0 {
		name = args[0]
	}
	delivery := func() *altqueue.Delivery { return &altqueue.Delivery{JobId: args[1], LeaseToken: args[2]} }
	switch command {
	case "create":
		if maxJobs <= 0 || maxJobs > 1_000_000 || maxInFlight < 0 || maxInFlight > 1_000_000 || maxAttempts < 0 || maxAttempts > 1000 || base < 0 || maxBackoff < 0 || base > 24*time.Hour || maxBackoff > 24*time.Hour {
			return fmt.Errorf("invalid queue configuration")
		}
		q, err := client.CreateQueue(ctx, name, &altqueue.QueueConfig{MaxJobs: int32(maxJobs), MaxPayloadBytes: maxBytes, MaxInFlight: int32(maxInFlight), MaxAttempts: int32(maxAttempts), RetryBaseMs: base.Milliseconds(), RetryMaxMs: maxBackoff.Milliseconds()})
		if err != nil {
			return err
		}
		return print(q)
	case "queues":
		if limit < 1 || limit > 128 {
			return fmt.Errorf("limit must be 1-128")
		}
		qs, err := client.ListQueues(ctx, after, int32(limit))
		if err != nil {
			return err
		}
		return print(qs)
	case "stats":
		q, err := client.GetQueue(ctx, name)
		if err != nil {
			return err
		}
		return print(q)
	case "delete":
		return client.DeleteQueue(ctx, name)
	case "publish":
		payloads := make([][]byte, 0, len(args)-1)
		for _, arg := range args[1:] {
			payloads = append(payloads, []byte(arg))
		}
		ids, err := client.Publish(ctx, name, payloads, delay)
		if err != nil {
			return err
		}
		return print(ids)
	case "fetch":
		if limit < 1 || limit > 120 {
			return fmt.Errorf("limit must be 1-120")
		}
		ds, err := client.Fetch(ctx, name, int32(limit), lease, wait)
		if err != nil {
			return err
		}
		return print(ds)
	case "ack":
		return client.Ack(ctx, name, delivery())
	case "nack":
		return client.Nack(ctx, name, delivery(), permanent, reason)
	case "extend":
		deadline, err := client.ExtendLease(ctx, name, delivery(), lease)
		if err != nil {
			return err
		}
		return print(deadline)
	case "dead":
		if limit < 1 || limit > 120 {
			return fmt.Errorf("limit must be 1-120")
		}
		jobs, err := client.ListDead(ctx, name, after, int32(limit))
		if err != nil {
			return err
		}
		return print(jobs)
	case "requeue":
		return client.RequeueDead(ctx, name, args[1])
	case "work":
		for {
			ds, err := client.Fetch(ctx, name, 1, 30*time.Second, 5*time.Second)
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				return err
			}
			for _, d := range ds {
				if _, err := fmt.Fprintf(os.Stdout, "%s %s\n", d.JobId, d.Payload); err != nil {
					return err
				}
				if err := client.Ack(ctx, name, d); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
