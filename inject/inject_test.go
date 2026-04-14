package inject

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/udhos/pulsix/pub"
)

type testStorage struct {
	mu            sync.Mutex
	failRemaining int
}

func (s *testStorage) PutObject(_ context.Context, _ string, _ io.Reader, _ int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failRemaining > 0 {
		s.failRemaining--
		return fmt.Errorf("forced storage error")
	}
	return nil
}

func (s *testStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func newTestSender(storage *testStorage) *pub.Sender {
	return pub.NewSender(pub.SendOptions{
		Options: pub.Options{
			Storage: storage,
			Prefix:  "inject-test",
			GenerateIDFunc: func() string {
				return "inject-fixed-id"
			},
		},
		FlushThresholdAge:   20 * time.Millisecond,
		FlushThresholdBytes: 1024,
		HardFailDeadline:    100 * time.Millisecond,
	})
}

func TestRunAcksReceipts(t *testing.T) {
	sender := newTestSender(&testStorage{})
	defer sender.Close()

	var (
		mu   sync.Mutex
		got  []string
		want = []string{"r1", "r2", "r3"}
	)

	inj := New(sender, func(receipt string) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, receipt)
	}, 8)

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
	storage := &testStorage{failRemaining: 50}
	sender := newTestSender(storage)
	defer sender.Close()

	ackCount := 0
	var mu sync.Mutex
	inj := New(sender, func(_ string) {
		mu.Lock()
		ackCount++
		mu.Unlock()
	}, 4)

	runErr := make(chan error, 1)
	go func() {
		runErr <- inj.Run()
	}()

	inj.C <- InjectMessage{Receipt: "retry-1", Data: []byte("hello")}
	close(inj.C)

	// Let sender hit hard-fail boundary at least once, then recover backend.
	time.Sleep(300 * time.Millisecond)
	storage.mu.Lock()
	storage.failRemaining = 0
	storage.mu.Unlock()

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

func TestRunFailsIfSenderClosedEarly(t *testing.T) {
	sender := newTestSender(&testStorage{})
	inj := New(sender, nil, 2)

	runErr := make(chan error, 1)
	go func() {
		runErr <- inj.Run()
	}()

	inj.C <- InjectMessage{Receipt: "x", Data: []byte("x")}
	sender.Close()
	close(inj.C)

	select {
	case err := <-runErr:
		if err == nil {
			t.Fatal("expected error when sender is closed before injector drains")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting Run() error")
	}
}
