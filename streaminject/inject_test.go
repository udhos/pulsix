package streaminject

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/udhos/pulsix/stream"
)

type testStorage struct {
	mu            sync.Mutex
	failRemaining int
}

func (s *testStorage) PutObject(_ context.Context, _ string, r io.Reader, _ int64) error {
	s.mu.Lock()
	fail := s.failRemaining > 0
	if fail {
		s.failRemaining--
	}
	s.mu.Unlock()

	if fail {
		// Mock HTTP client closing body on error
		if closer, ok := r.(io.Closer); ok {
			_ = closer.Close()
		}
		return fmt.Errorf("forced storage error")
	}

	_, _ = io.Copy(io.Discard, r)
	return nil
}

func (s *testStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func newTestOptions(storage *testStorage) Options {
	return Options{
		StreamOptions: stream.Options{
			Storage:               storage,
			Prefix:                "streaminject-test",
			GenerateIDFunc:        func() string { return "test-id" },
			FlushThresholdAge:     20 * time.Millisecond,
			FlushThresholdBytes:   1024,
			FlushThresholdSilence: 20 * time.Millisecond,
		},
		BufferSize: 8,
	}
}

func TestRunAcksReceipts(t *testing.T) {
	storage := &testStorage{}
	opts := newTestOptions(storage)

	var (
		mu   sync.Mutex
		got  []string
		want = []string{"r1", "r2", "r3"}
	)

	opts.OnAck = func(receipt string) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, receipt)
	}

	inj := New(opts)

	runErr := make(chan error, 1)
	go func() {
		runErr <- inj.Run()
	}()

	for _, r := range want {
		inj.C <- InjectMessage{Receipt: r, Data: []byte("payload-" + r)}
	}
	close(inj.C)

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting Run()")
	}

	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("callback count mismatch: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("receipt mismatch at %d: got=%q want=%q", i, got[i], want[i])
		}
	}
}

func TestRunRetriesAfterAckError(t *testing.T) {
	storage := &testStorage{failRemaining: 2}
	opts := newTestOptions(storage)

	ackCount := 0
	var mu sync.Mutex
	opts.OnAck = func(_ string) {
		mu.Lock()
		ackCount++
		mu.Unlock()
	}

	inj := New(opts)

	runErr := make(chan error, 1)
	go func() {
		runErr <- inj.Run()
	}()

	inj.C <- InjectMessage{Receipt: "retry-1", Data: []byte("hello")}
	close(inj.C)

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting Run() after recovery")
	}

	mu.Lock()
	defer mu.Unlock()
	if ackCount != 1 {
		t.Fatalf("expected one ack callback after retry, got=%d", ackCount)
	}
}
