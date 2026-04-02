package sub

import (
	"context"
	"io"
	"strings"
	"testing"
)

type MockNotification struct {
	Key     string
	Deleted bool
}

func (m *MockNotification) GetKey() string                 { return m.Key }
func (m *MockNotification) Delete(_ context.Context) error { m.Deleted = true; return nil }

type MockQueue struct {
	Notifications []Notification
}

func (m *MockQueue) ReceiveNotifications(_ context.Context) ([]Notification, error) {
	return m.Notifications, nil
}

type MockStorage struct {
	Data map[string]string
}

func (m *MockStorage) PutObject(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return nil
}
func (m *MockStorage) GetObject(_ context.Context, key string) (io.ReadCloser, error) {
	content, ok := m.Data[key]
	if !ok {
		return nil, io.EOF
	}
	return io.NopCloser(strings.NewReader(content)), nil
}

func TestSub_ReceiveAndLifecycle(t *testing.T) {
	// 1. Setup
	key := "test/path/batch.pulsix"
	content := "PULSIX-SIZE:5\nhello"

	// We create the pointer here so we can track it later
	notif1 := &MockNotification{Key: key}

	mockQueue := &MockQueue{
		Notifications: []Notification{notif1},
	}
	mockStorage := &MockStorage{
		Data: map[string]string{key: content},
	}

	subscriber := New(Options{
		Queue:   mockQueue,
		Storage: mockStorage,
	})

	// 2. Execute Receive
	batches, err := subscriber.Receive(context.Background())
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}

	batch := batches[0]

	// 3. Verify Key
	if batch.notification.GetKey() != key {
		t.Errorf("expected key %s, got %s", key, batch.notification.GetKey())
	}

	// 4. CRITICAL CHECK: Verify it is NOT deleted yet
	if notif1.Deleted {
		t.Error("notification was deleted before Done() was called")
	}

	// 5. Verify Lifecycle (Done)
	if err := batch.Done(); err != nil {
		t.Errorf("Done failed: %v", err)
	}

	// 6. FINAL CHECK: Verify it IS deleted now
	if !notif1.Deleted {
		t.Error("expected notification to be deleted after Done()")
	}
}

func TestSub_FullStream(t *testing.T) {
	key := "test.batch"
	// Two messages: "hello" (5) and "world" (5)
	content := "PULSIX-SIZE:5\nhelloPULSIX-SIZE:5\nworld"

	mockQueue := &MockQueue{Notifications: []Notification{&MockNotification{Key: key}}}
	mockStorage := &MockStorage{Data: map[string]string{key: content}}
	subscriber := New(Options{Queue: mockQueue, Storage: mockStorage})

	batches, _ := subscriber.Receive(context.Background())
	batch := batches[0]

	// First message
	if !batch.Next() {
		t.Fatalf("expected first message, got error: %v", batch.Error())
	}
	if string(batch.Message()) != "hello" {
		t.Errorf("expected hello, got %s", batch.Message())
	}

	// Second message
	if !batch.Next() {
		t.Fatalf("expected second message, got error: %v", batch.Error())
	}
	if string(batch.Message()) != "world" {
		t.Errorf("expected world, got %s", batch.Message())
	}

	// End of stream
	if batch.Next() {
		t.Error("expected end of stream, but Next() returned true")
	}

	batch.Done()
}
