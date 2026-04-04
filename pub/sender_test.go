package pub

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/udhos/pulsix/pulsix"
)

func newTestSender(t *testing.T) (*Sender, *mockStorage) {
	t.Helper()
	store := &mockStorage{}
	sender := NewSender(SendOptions{
		Options: Options{
			Storage: store,
			Prefix:  "test",
			GenerateIDFunc: func() string {
				return "FIXED_ID_FOR_TESTING_1234567"
			},
		},
		FlushThresholdAge:      100 * time.Millisecond,
		FlushThresholdMessages: 10_000,
		FlushThresholdBytes:    50 * 1024 * 1024,
	})
	return sender, store
}

func TestSender_SendAndAck(t *testing.T) {
	sender, _ := newTestSender(t)

	id1, err := sender.Send(context.Background(), pulsix.Message{Data: []byte("hello")})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	id2, err := sender.Send(context.Background(), pulsix.Message{Data: []byte("world")})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if id1 >= id2 {
		t.Errorf("expected id1 < id2, got %d >= %d", id1, id2)
	}

	// Wait for time-based flush
	ack := <-sender.AckChan()
	if ack.AckedUpTo != id2 {
		t.Errorf("expected AckedUpTo=%d, got %d", id2, ack.AckedUpTo)
	}

	sender.Close(context.Background())
}

func TestSender_IDsAreMonotonic(t *testing.T) {
	sender, _ := newTestSender(t)
	defer sender.Close(context.Background())

	const n = 100
	ids := make([]uint64, n)
	for i := range ids {
		id, err := sender.Send(context.Background(), pulsix.Message{Data: []byte("x")})
		if err != nil {
			t.Fatalf("Send failed: %v", err)
		}
		ids[i] = id
	}

	for i := 1; i < n; i++ {
		if ids[i] <= ids[i-1] {
			t.Errorf("ID not monotonic at index %d: %d <= %d", i, ids[i], ids[i-1])
		}
	}
}

func TestSender_FlushThresholdMessages(t *testing.T) {
	store := &mockStorage{}
	sender := NewSender(SendOptions{
		Options: Options{
			Storage:        store,
			Prefix:         "test",
			GenerateIDFunc: func() string { return "FIXED_ID_FOR_TESTING_1234567" },
		},
		FlushThresholdAge:      10 * time.Second, // long age so only message count triggers
		FlushThresholdMessages: 3,
		FlushThresholdBytes:    50 * 1024 * 1024,
	})
	defer sender.Close(context.Background())

	for i := range 3 {
		_, err := sender.Send(context.Background(), pulsix.Message{Data: []byte{byte(i)}})
		if err != nil {
			t.Fatalf("Send failed: %v", err)
		}
	}

	select {
	case ack := <-sender.AckChan():
		if ack.AckedUpTo != 2 {
			t.Errorf("expected AckedUpTo=2, got %d", ack.AckedUpTo)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ack after message threshold")
	}
}

func TestSender_CloseFlushesRemaining(t *testing.T) {
	sender, _ := newTestSender(t)

	id, err := sender.Send(context.Background(), pulsix.Message{Data: []byte("flush-on-close")})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	sender.Close(context.Background())

	// After Close, ackChan is closed. Drain it and look for the ack.
	found := false
	for ack := range sender.AckChan() {
		if ack.AckedUpTo >= id {
			found = true
		}
	}
	if !found {
		t.Errorf("did not receive ack for message sent before Close")
	}
}

func TestSender_SendAfterClose(t *testing.T) {
	sender, _ := newTestSender(t)
	sender.Close(context.Background())

	_, err := sender.Send(context.Background(), pulsix.Message{Data: []byte("late")})
	if err == nil {
		t.Error("expected error when sending after Close, got nil")
	}
}

func TestSender_HardFailAfterRetries(t *testing.T) {
	// Mock storage that always fails
	failingStore := &mockFailingStorage{failCount: 999}

	sender := NewSender(SendOptions{
		Options: Options{
			Storage: failingStore,
			Prefix:  "test",
			GenerateIDFunc: func() string {
				return "FIXED_ID_FOR_TESTING_1234567"
			},
		},
		FlushThresholdAge:      50 * time.Millisecond, // short age for quick retry attempts
		FlushThresholdMessages: 10,
		FlushThresholdBytes:    50 * 1024 * 1024,
		HardFailDeadline:       250 * time.Millisecond,
	})

	// Send messages to trigger flush
	_, err := sender.Send(context.Background(), pulsix.Message{Data: []byte("msg1")})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	_, err = sender.Send(context.Background(), pulsix.Message{Data: []byte("msg2")})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Wait for retries to exhaust and hard fail to trigger.
	time.Sleep(700 * time.Millisecond)

	// Wait for terminal boundary reported via AckChan.
	var reportedErr error
	deadline := time.After(2 * time.Second)
	for reportedErr == nil {
		select {
		case ack := <-sender.AckChan():
			if ack.Err == nil {
				continue
			}
			reportedErr = ack.Err
		case <-deadline:
			t.Fatal("timed out waiting for terminal boundary ack")
		}
	}

	if !errors.Is(reportedErr, errMockStorageFailed) {
		t.Fatalf("expected storage failure on Ack.Err, got %v", reportedErr)
	}

	t.Logf("Terminal boundary ack error: %v", reportedErr)

	if err := sender.Close(context.Background()); err != nil {
		t.Fatalf("expected Close without terminal error, got %v", err)
	}
}

func TestSender_ReuseAfterHardFail(t *testing.T) {
	store := &mockRecoveringStorage{failing: true}

	sender := NewSender(SendOptions{
		Options: Options{
			Storage: store,
			Prefix:  "test",
			GenerateIDFunc: func() string {
				return "FIXED_ID_FOR_TESTING_1234567"
			},
		},
		FlushThresholdAge:      50 * time.Millisecond,
		FlushThresholdMessages: 10,
		FlushThresholdBytes:    50 * 1024 * 1024,
		HardFailDeadline:       250 * time.Millisecond,
	})
	defer sender.Close(context.Background())

	// Phase 1: force a hard fail window.
	if _, err := sender.Send(context.Background(), pulsix.Message{Data: []byte("will-fail-1")}); err != nil {
		t.Fatalf("first send failed unexpectedly: %v", err)
	}
	if _, err := sender.Send(context.Background(), pulsix.Message{Data: []byte("will-fail-2")}); err != nil {
		t.Fatalf("second send failed unexpectedly: %v", err)
	}

	// Wait for retry deadline to elapse so sender enters hard-fail boundary.
	time.Sleep(700 * time.Millisecond)

	// Phase 2: recover backend and reuse same sender instance.
	store.SetFailing(false)

	recoveryID, err := sender.Send(context.Background(), pulsix.Message{Data: []byte("after-hard-fail")})
	if err != nil {
		t.Fatalf("expected send to succeed after hard fail on same sender, got: %v", err)
	}

	deadline := time.After(3 * time.Second)
	for {
		select {
		case ack := <-sender.AckChan():
			if ack.AckedUpTo >= recoveryID {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for recovery send ack after reusing sender post hard fail")
		}
	}
}

// mockFailingStorage is a mock storage that always fails writes
type mockFailingStorage struct {
	failCount int
}

func (m *mockFailingStorage) PutObject(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return errMockStorageFailed
}

func (m *mockFailingStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, errMockStorageFailed
}

var errMockStorageFailed = &mockError{msg: "mock storage failed"}

type mockError struct {
	msg string
}

func (e *mockError) Error() string { return e.msg }

type mockRecoveringStorage struct {
	mu      sync.Mutex
	failing bool
}

func (m *mockRecoveringStorage) SetFailing(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failing = v
}

func (m *mockRecoveringStorage) PutObject(_ context.Context, _ string, _ io.Reader, _ int64) error {
	m.mu.Lock()
	failing := m.failing
	m.mu.Unlock()
	if failing {
		return errMockStorageFailed
	}
	return nil
}

func (m *mockRecoveringStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, errMockStorageFailed
}
