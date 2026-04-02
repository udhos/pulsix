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
	key := "test/path/batch.pulsix"

	// d(1) + :(1) + 5(1) + :(1) + hello(5) = 9 bytes
	const content = "p1:9:d:5:hello"

	notif1 := &MockNotification{Key: key}
	mockQueue := &MockQueue{Notifications: []Notification{notif1}}
	mockStorage := &MockStorage{Data: map[string]string{key: content}}

	subscriber := New(Options{
		Queue:   mockQueue,
		Storage: mockStorage,
	})

	batches, err := subscriber.Receive(context.Background())
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	batch := batches[0]
	if !batch.Next() {
		t.Fatalf("Next() failed: %v", batch.Error())
	}

	if string(batch.Message()) != "hello" {
		t.Errorf("expected hello, got %s", batch.Message())
	}

	if err := batch.Done(); err != nil {
		t.Errorf("Done failed: %v", err)
	}

	if !notif1.Deleted {
		t.Error("expected notification to be deleted after Done()")
	}
}

func TestSub_FullStream(t *testing.T) {
	key := "test.batch"

	// Two records in p1 format
	content := "p1:9:d:5:hellop1:9:d:5:world"

	mockQueue := &MockQueue{Notifications: []Notification{&MockNotification{Key: key}}}
	mockStorage := &MockStorage{Data: map[string]string{key: content}}
	subscriber := New(Options{Queue: mockQueue, Storage: mockStorage})

	batches, _ := subscriber.Receive(context.Background())
	batch := batches[0]

	if !batch.Next() || string(batch.Message()) != "hello" {
		t.Errorf("failed to read first message")
	}

	if !batch.Next() || string(batch.Message()) != "world" {
		t.Errorf("failed to read second message")
	}

	if batch.Next() {
		t.Error("expected end of stream")
	}

	batch.Done()
}
