package pub

import (
	"context"
	"io"
	"strings"
	"testing"
)

// MockStorage implements pulsix.Storage for testing purposes.
type MockStorage struct {
	CapturedKey     string
	CapturedContent []byte
	CapturedSize    int64
}

func (m *MockStorage) PutObject(_ context.Context, key string, r io.Reader, contentLength int64) error {
	m.CapturedKey = key
	m.CapturedSize = contentLength
	// Read the entire stream to verify the content
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.CapturedContent = data
	return nil
}

func (m *MockStorage) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, nil // Not used in pub tests
}

func TestSendBatch(t *testing.T) {
	mock := &MockStorage{}
	publisher := New(Options{
		Storage: mock,
		Prefix:  "test-events",
	})

	messages := [][]byte{
		[]byte("hello"),
		[]byte("pulsix"),
	}

	err := publisher.SendBatch(context.Background(), messages)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	if !strings.HasPrefix(mock.CapturedKey, "test-events/") {
		t.Errorf("expected key prefix 'test-events/', got %s", mock.CapturedKey)
	}

	expectedContent := "\nPULSIX-SIZE:5\nhello\nPULSIX-SIZE:6\npulsix"
	if string(mock.CapturedContent) != expectedContent {
		t.Errorf("expected content %q, got %q", expectedContent, string(mock.CapturedContent))
	}

	// Verify total size calculation (41 bytes)
	if mock.CapturedSize != int64(len(expectedContent)) {
		t.Errorf("expected reported size %d, got %d", len(expectedContent), mock.CapturedSize)
	}
}

func TestSendBatchEmpty(t *testing.T) {
	publisher := New(Options{Storage: &MockStorage{}})
	err := publisher.SendBatch(context.Background(), [][]byte{})

	if err != ErrEmptyMessages {
		t.Errorf("expected ErrEmptyMessages, got %v", err)
	}
}
