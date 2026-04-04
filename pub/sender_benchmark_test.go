package pub

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/udhos/pulsix/pulsix"
)

const benchmarkMessageCount = 100_000

type benchmarkStorage struct{}

func (benchmarkStorage) PutObject(_ context.Context, _ string, r io.Reader, _ int64) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

func (benchmarkStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

// BenchmarkSenderInject100k measures enqueueing 100k messages into Sender.
// It uses message-count thresholds to flush in larger chunks and a mock backend.
func BenchmarkSenderInject100k(b *testing.B) {
	ctx := context.Background()
	msg := pulsix.Message{Data: []byte("x")}

	b.ReportAllocs()

	for b.Loop() {
		sender := NewSender(SendOptions{
			Options: Options{
				Storage: benchmarkStorage{},
				Prefix:  "bench",
			},
			FlushThresholdAge:      time.Hour,
			FlushThresholdMessages: 10_000,
			FlushThresholdBytes:    50 * 1024 * 1024,
		})

		for range benchmarkMessageCount {
			if _, err := sender.Send(ctx, msg); err != nil {
				b.Fatalf("send failed: %v", err)
			}
		}

		if err := sender.Close(ctx); err != nil {
			b.Fatalf("close failed: %v", err)
		}
	}
}

// BenchmarkSenderInject100kConcurrent8 measures injecting 100k messages using
// 8 concurrent goroutines into a single shared Sender.
func BenchmarkSenderInject100kConcurrent8(b *testing.B) {
	const workers = 8

	ctx := context.Background()
	msg := pulsix.Message{Data: []byte("x")}

	b.ReportAllocs()

	for b.Loop() {
		sender := NewSender(SendOptions{
			Options: Options{
				Storage: benchmarkStorage{},
				Prefix:  "bench",
			},
			FlushThresholdAge:      time.Hour,
			FlushThresholdMessages: 10_000,
			FlushThresholdBytes:    50 * 1024 * 1024,
		})

		base := benchmarkMessageCount / workers
		rest := benchmarkMessageCount % workers

		var wg sync.WaitGroup
		wg.Add(workers)
		for w := range workers {
			count := base
			if w == workers-1 {
				count += rest
			}

			go func(n int) {
				defer wg.Done()
				for range n {
					if _, err := sender.Send(ctx, msg); err != nil {
						b.Errorf("send failed: %v", err)
						return
					}
				}
			}(count)
		}

		wg.Wait()

		if err := sender.Close(ctx); err != nil {
			b.Fatalf("close failed: %v", err)
		}
	}
}

// BenchmarkSenderTimeBasedFlush measures a workload where flush is triggered
// primarily by FlushThresholdAge (timer), not by message/byte thresholds.
func BenchmarkSenderTimeBasedFlush(b *testing.B) {
	ctx := context.Background()
	msg := pulsix.Message{Data: []byte("x")}

	b.ReportAllocs()

	for b.Loop() {
		sender := NewSender(SendOptions{
			Options: Options{
				Storage: benchmarkStorage{},
				Prefix:  "bench",
			},
			FlushThresholdAge:      5 * time.Millisecond,
			FlushThresholdMessages: benchmarkMessageCount * 10,
			FlushThresholdBytes:    1 << 40, // 1TB effectively disables size-based flush
		})

		for range 1000 {
			if _, err := sender.Send(ctx, msg); err != nil {
				b.Fatalf("send failed: %v", err)
			}
		}

		select {
		case ack := <-sender.AckChan():
			_ = ack // Successful ack received
		case <-time.After(2 * time.Second):
			b.Fatal("timed out waiting for time-based flush ack")
		}

		if err := sender.Close(ctx); err != nil {
			b.Fatalf("close failed: %v", err)
		}
	}
}
