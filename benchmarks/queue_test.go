package benchmarks

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/tusharsinghbisht/altqueue/internal/queue"
)

// Compare an in-memory lifecycle with three synchronized durable transitions.
// This is a local baseline, not a network throughput or production capacity claim.
func BenchmarkRoundTrip(b *testing.B) {
	for _, durable := range []bool{false, true} {
		name := "memory"
		if durable {
			name = "durable"
		}
		b.Run(name, func(b *testing.B) {
			engine := queue.NewEngine(nil)
			if durable {
				var err error
				engine, err = queue.Open(filepath.Join(b.TempDir(), "db"), nil)
				if err != nil {
					b.Fatal(err)
				}
			}
			defer engine.Close()
			if err := engine.CreateQueueWithConfig("bench", queue.QueueConfig{MaxJobs: 1}); err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, 1024)
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := engine.Publish("bench", payload); err != nil {
					b.Fatal(err)
				}
				d, err := engine.Fetch("bench", time.Minute)
				if err != nil {
					b.Fatal(err)
				}
				if err := engine.Ack("bench", d.JobID, d.LeaseToken); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
