package pulsix_test

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/udhos/pulsix/inject"
	"github.com/udhos/pulsix/pub"
	"github.com/udhos/pulsix/stream"
	"github.com/udhos/pulsix/streaminject"
)

// benchStorage is a dummy storage that discards all data.
type benchStorage struct{}

func (s *benchStorage) PutObject(_ context.Context, _ string, r io.Reader, _ int64) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

func (s *benchStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("GetObject not implemented in benchStorage")
}

func TestBenchmarkInject(t *testing.T) {
	sizes := []int{1000, 10000}
	messages := 100000 // Fixed number of messages to process

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Legacy-Payload%d", size), func(t *testing.T) {
			storage := &benchStorage{}
			sender := pub.NewSender(pub.SendOptions{
				Options: pub.Options{
					Storage:        storage,
					Prefix:         "bench",
					GenerateIDFunc: func() string { return "" },
				},
				FlushThresholdAge:   1 * time.Second,
				FlushThresholdBytes: 100 * 1024 * 1024,
				AckChannelSize:      20000,
			})

			var wg sync.WaitGroup
			wg.Add(messages)
			injector := inject.New(sender, func(_ string) {
				wg.Done()
			}, 20000)

			go injector.Run()

			payload := make([]byte, size)
			start := time.Now()

			for i := range messages {
				injector.C <- inject.InjectMessage{
					Receipt: strconv.Itoa(i),
					Data:    payload,
				}
			}

			close(injector.C)
			wg.Wait()
			sender.Close() // Safe to close after acks

			elapsed := time.Since(start)
			throughput := float64(messages) / elapsed.Seconds()
			t.Logf("Legacy-Payload%d: %d messages in %v (%.2f msg/s)", size, messages, elapsed, throughput)
		})

		t.Run(fmt.Sprintf("Stream-Payload%d", size), func(t *testing.T) {
			storage := &benchStorage{}
			var wg sync.WaitGroup
			wg.Add(messages)

			opts := streaminject.Options{
				StreamOptions: stream.Options{
					Storage:               storage,
					Prefix:                "bench",
					GenerateIDFunc:        func() string { return "" },
					FlushThresholdAge:     1 * time.Second,
					FlushThresholdBytes:   100 * 1024 * 1024,
					FlushThresholdSilence: 0,
					InboxSize:             1024,
				},
				BufferSize: 20000,
				OnAck: func(_ string) {
					wg.Done()
				},
			}

			injector := streaminject.New(opts)
			go injector.Run()

			payload := make([]byte, size)
			start := time.Now()

			for i := range messages {
				injector.C <- streaminject.InjectMessage{
					Receipt: strconv.Itoa(i),
					Data:    payload,
				}
			}

			close(injector.C)
			wg.Wait()

			elapsed := time.Since(start)
			throughput := float64(messages) / elapsed.Seconds()
			t.Logf("Stream-Payload%d: %d messages in %v (%.2f msg/s)", size, messages, elapsed, throughput)
		})
	}
}
